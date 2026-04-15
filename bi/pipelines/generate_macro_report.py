"""
マクロレポート自動生成スクリプト

前日との差分を検出し、新着記事があれば Claude API で sonnet_macro.md を生成する。
新着なしの場合は exit code 2 を返す（CI での skip 判定に使う）。

使い方:
  python generate_macro_report.py             # 今日のレポートを生成
  python generate_macro_report.py --date 2026-04-05  # 日付指定
  python generate_macro_report.py --force     # 新着なしでも強制生成

exit codes:
  0  正常生成
  1  エラー（API失敗・ファイル未存在等）
  2  新着記事なし（skip）
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import yfinance as yf
from dotenv import load_dotenv

REPO_ROOT  = Path(__file__).resolve().parents[2]
MARKET_DIR = REPO_ROOT / "market" / "daily"
MACRO_DIR  = MARKET_DIR / "macro"
AGENTS_DIR = REPO_ROOT / "agents"
_ENV_PATH  = Path(__file__).resolve().parent / ".env"

EXIT_OK   = 0
EXIT_ERR  = 1
EXIT_SKIP = 2

# yfinance ティッカー
SNAPSHOT_TICKERS = {
    "日経平均":  "^N225",
    "S&P500":   "^GSPC",
    "ドル円":    "USDJPY=X",
    "金(Gold)": "GC=F",
    "BTC":      "BTC-USD",
    "米10年債":  "^TNX",
    "VIX":      "^VIX",
}


# ---------------------------------------------------------------------------
# 市況スナップショット
# ---------------------------------------------------------------------------

def get_market_snapshot() -> str:
    lines = ["| 指標 | 水準 | 前日比 | 備考 |", "|------|------|--------|------|"]
    for name, ticker in SNAPSHOT_TICKERS.items():
        try:
            info = yf.Ticker(ticker).fast_info
            close = info.last_price
            prev  = info.previous_close
            if close is not None and prev is not None and prev != 0:
                chg = close - prev
                pct = chg / prev * 100
                comment = ""
                if name == "VIX":
                    if close >= 30:
                        comment = "⚠️ 恐怖ゾーン"
                    elif close <= 15:
                        comment = "楽観ゾーン"
                lines.append(f"| {name} | {close:,.2f} | {chg:+,.2f} ({pct:+.2f}%) | {comment} |")
            else:
                lines.append(f"| {name} | 取得不可 | ─ | ─ |")
        except Exception as e:
            lines.append(f"| {name} | 取得不可 | ─ | {e} |")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 新着記事の検出
# ---------------------------------------------------------------------------

def extract_urls(content: str) -> set[str]:
    """時系列インデックスセクションからURLを抽出"""
    urls: set[str] = set()
    in_timeline = False
    for line in content.splitlines():
        if "時系列インデックス" in line:
            in_timeline = True
        elif in_timeline and line.startswith("## ") and "時系列" not in line:
            break
        elif in_timeline:
            for url in re.findall(r"https?://[^\s\)\]]+", line):
                urls.add(url)
    return urls


def count_new_articles(today: str, yesterday: str | None) -> int:
    today_urls = extract_urls(today)
    if not yesterday:
        return len(today_urls)
    return len(today_urls - extract_urls(yesterday))


# ---------------------------------------------------------------------------
# プロンプト構築
# ---------------------------------------------------------------------------

def build_prompt(
    today_raw: str,
    yesterday_report: str | None,
    snapshot: str,
    target_date: str,
    finnhub_raw: str | None = None,
    deep_research: str | None = None,
) -> str:
    agent_spec = (AGENTS_DIR / "macro_analyst.md").read_text(encoding="utf-8")

    delta_section = ""
    if yesterday_report:
        # 前日レポートの冒頭2500字を差分コンテキストとして渡す
        preview = yesterday_report[:2500].rstrip()
        delta_section = f"""
---
## 前日レポート（差分参照用）
以下は前日（{target_date}の前日）のレポート冒頭です。
**今日のレポートでは「前日から変わった点・新しい動き」を重点的に書いてください。**
変化がないトピックは1〜2行で簡潔にまとめ、新規・変化ありのトピックを深堀りしてください。

{preview}
---
"""

    finnhub_section = ""
    if finnhub_raw:
        finnhub_section = f"""
---
## グローバルニュース・経済カレンダー（Finnhub）
以下は Reuters/Bloomberg 等のグローバルニュースと今後の経済指標カレンダーです。
日本語のニュースと組み合わせて、マクロ環境を総合的に分析してください。
英語のニュース見出し・要約は内容を理解した上で日本語で分析に反映してください。

{finnhub_raw}
---
"""

    deep_research_section = ""
    if deep_research:
        deep_research_section = f"""
---
## Deep Research 定性分析（外部入力）
以下は Perplexity 等の Deep Research による詳細調査結果です。
定量データ・一次情報を積極的に活用し、レポートの各テーマセクションに反映してください。

{deep_research}
---
"""

    return f"""\
あなたは Mizuki Fund のマクロ経済アナリストです。
以下の情報をもとに本日（{target_date}）のマクロレポートを生成してください。

## エージェント仕様（必ず遵守）
{agent_spec}

## 本日の市況スナップショット（yfinance 取得）
{snapshot}
{delta_section}{finnhub_section}
## 本日のニュース生データ（{target_date}_news_raw.md 全文）
{today_raw}
{deep_research_section}
---
上記情報をもとに agents/macro_analyst.md の仕様に従い、
`{target_date}_sonnet_macro.md` として出力するレポートを日本語で生成してください。
マークダウン形式で出力し、コードブロックで囲まないこと。

## ⚠️ 必須出力ルール（絶対に省略禁止）

レポートの**最後**に、必ず以下のフォーマットで「Deep Research 候補」セクションを出力すること。
このセクションは**省略不可・「なし」の場合もその旨を明記**すること。
候補が思いつかない場合でも「Deep Research 候補なし（本日は全テーマ解像度十分）」と書くこと。

```
## 📌 Deep Research 候補

- [ ] 〇〇について（理由: △△が不明確なため）
- [ ] 〇〇について（理由: △△の影響度を定量化したい）
```

上記フォーマットを守り、「このレポートで重要だが解像度が足りない」「掘り下げると投資判断が変わりうる」論点を3〜5件リストアップすること。
"""


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv(_ENV_PATH)

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--force", action="store_true", help="新着なしでも強制生成")
    parser.add_argument("--snapshot-only", action="store_true", help="市況スナップショットだけ取得して表示（API不要）")
    args = parser.parse_args()
    target_date: str = args.date

    # スナップショットのみモード（Claude Code手動生成時に使う）
    if args.snapshot_only:
        print("市況データ取得中 (yfinance)...")
        print(get_market_snapshot())
        sys.exit(EXIT_OK)

    # news_raw.md を読み込む
    raw_path = MARKET_DIR / f"{target_date}_news_raw.md"
    if not raw_path.exists():
        print(f"[ERROR] {raw_path.name} が存在しません。fetch_rss.py を先に実行してください。", file=sys.stderr)
        sys.exit(EXIT_ERR)

    today_raw = raw_path.read_text(encoding="utf-8")

    # 前日ファイルを読み込む
    yesterday_str = (date.fromisoformat(target_date) - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday_raw_path    = MARKET_DIR / f"{yesterday_str}_news_raw.md"
    yesterday_report_path = MACRO_DIR / f"{yesterday_str}.md"

    yesterday_raw    = yesterday_raw_path.read_text(encoding="utf-8")    if yesterday_raw_path.exists()    else None
    yesterday_report = yesterday_report_path.read_text(encoding="utf-8") if yesterday_report_path.exists() else None

    # 新着記事数チェック
    new_count = count_new_articles(today_raw, yesterday_raw)
    print(f"新着記事数: {new_count}")

    if new_count == 0 and not args.force:
        print("[SKIP] 新着記事なし")
        sys.exit(EXIT_SKIP)

    # Finnhub raw データを読み込む（任意・存在しなくてもスキップ）
    finnhub_path = MARKET_DIR / f"{target_date}_finnhub_raw.md"
    finnhub_raw: str | None = None
    if finnhub_path.exists():
        finnhub_raw = finnhub_path.read_text(encoding="utf-8")
        print(f"Finnhub データあり: {finnhub_path.name} ({len(finnhub_raw):,} 文字)")
    else:
        print(f"Finnhub データなし（{finnhub_path.name}）- fetch_finnhub.py を先に実行するとグローバルニュースが追加されます")

    # Deep Research データを読み込む（任意・存在しなくてもスキップ）
    deep_research_path = MACRO_DIR / f"{target_date}_deep_research.md"
    deep_research: str | None = None
    if deep_research_path.exists():
        deep_research = deep_research_path.read_text(encoding="utf-8")
        print(f"Deep Research データあり: {deep_research_path.name} ({len(deep_research):,} 文字)")
    else:
        print(f"Deep Research なし({deep_research_path.name}) -- Perplexity 結果をこのパスに保存すると自動統合されます")

    # 市況スナップショット取得
    print("市況データ取得中 (yfinance)...")
    snapshot = get_market_snapshot()

    # Claude API 呼び出し
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        print("[ERROR] ANTHROPIC_API_KEY が未設定です", file=sys.stderr)
        sys.exit(EXIT_ERR)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
    except ImportError:
        print("[ERROR] anthropic パッケージが未インストールです: pip install anthropic", file=sys.stderr)
        sys.exit(EXIT_ERR)

    prompt = build_prompt(today_raw, yesterday_report, snapshot, target_date, finnhub_raw, deep_research)
    print("Claude API 呼び出し中 (claude-sonnet-4-6)...")

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        print(f"[ERROR] Claude API エラー: {e}", file=sys.stderr)
        sys.exit(EXIT_ERR)

    report_text = message.content[0].text

    # 出力
    MACRO_DIR.mkdir(parents=True, exist_ok=True)
    output_path = MACRO_DIR / f"{target_date}.md"
    output_path.write_text(report_text, encoding="utf-8")

    in_tok  = message.usage.input_tokens
    out_tok = message.usage.output_tokens
    cost_usd = in_tok / 1_000_000 * 3.0 + out_tok / 1_000_000 * 15.0
    print(f"✅ 生成完了: {output_path.name}")
    print(f"   tokens: input={in_tok:,}  output={out_tok:,}  推定コスト: ${cost_usd:.4f}")


if __name__ == "__main__":
    main()
