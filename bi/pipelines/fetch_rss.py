"""
RSS パイプライン — フィード取得・フィルタ・サマリー出力

使い方:
  python fetch_rss.py                        # 全フィード（trigger + macro）
  python fetch_rss.py --category trigger     # トリガー用のみ
  python fetch_rss.py --category macro       # マクロ用のみ（Haiku要約）
  python fetch_rss.py --category supplement  # 補完フィード
  python fetch_rss.py --days 3               # 直近3日分（デフォルト1日）

出力: market/daily/YYYY-MM-DD_news.md
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import yaml
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# パス設定
# ---------------------------------------------------------------------------
REPO_ROOT   = Path(__file__).resolve().parents[2]
CONFIG_PATH = Path(__file__).resolve().parent / "ops" / "rss_config.yaml"
OUTPUT_DIR  = REPO_ROOT / "market" / "daily"
SEEN_PATH   = OUTPUT_DIR / ".rss_seen.txt"   # 既出アイテムのハッシュキャッシュ
PORTFOLIO_PATH = REPO_ROOT / "portfolio" / "positions.md"
_ENV_PATH   = Path(__file__).resolve().parent / ".env"

JST = timezone(timedelta(hours=9))


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _hash(title: str, link: str) -> str:
    return hashlib.md5(f"{title}|{link}".encode()).hexdigest()


def load_seen() -> set[str]:
    if SEEN_PATH.exists():
        return set(SEEN_PATH.read_text(encoding="utf-8").splitlines())
    return set()


def save_seen(seen: set[str]) -> None:
    SEEN_PATH.write_text("\n".join(sorted(seen)), encoding="utf-8")


def parse_entry_dt(entry: dict) -> datetime | None:
    for field in ("published_parsed", "updated_parsed", "created_parsed"):
        val = entry.get(field)
        if val:
            try:
                return datetime(*val[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def is_recent(entry: dict, days: int) -> bool:
    dt = parse_entry_dt(entry)
    if dt is None:
        return True  # 日付不明は含める
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
    return dt >= cutoff


def entry_to_dict(entry: dict) -> dict:
    return {
        "title": entry.get("title", "").strip(),
        "link":  entry.get("link", "").strip(),
        "summary": re.sub(r"<[^>]+>", "", entry.get("summary", "")).strip()[:300],
        "published": entry.get("published", ""),
    }


# ---------------------------------------------------------------------------
# ポジション銘柄コード・銘柄名の読み込み
# ---------------------------------------------------------------------------

def load_portfolio() -> list[dict]:
    """positions.md のテーブルから {code, name} を抽出する。"""
    if not PORTFOLIO_PATH.exists():
        return []
    text = PORTFOLIO_PATH.read_text(encoding="utf-8")
    stocks = []
    for line in text.splitlines():
        # | 7256 | 河西工業 | ... 形式
        m = re.match(r"\|\s*(\d{4})\s*\|\s*([^|]+)\|", line)
        if m:
            stocks.append({"code": m.group(1).strip(), "name": m.group(2).strip()})
    return stocks


# ---------------------------------------------------------------------------
# フィード取得
# ---------------------------------------------------------------------------

def fetch_feed(url: str, days: int) -> list[dict]:
    """feedparser でフィード取得。直近 days 日以内のアイテムを返す。"""
    try:
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries:
            if is_recent(entry, days):
                items.append(entry_to_dict(entry))
        return items
    except Exception as e:
        print(f"  [ERROR] {url}: {e}")
        return []


def fetch_per_stock(url_tpl: str, stocks: list[dict], days: int) -> list[tuple[str, list[dict]]]:
    """銘柄ごとにフィードを取得。(label, items) のリストを返す。"""
    results = []
    for s in stocks:
        url = url_tpl.replace("{code}", s["code"])
        print(f"  取得: {s['name']}（{s['code']}）")
        items = fetch_feed(url, days)
        if items:
            results.append((f"{s['name']}（{s['code']}）", items))
        time.sleep(0.5)
    return results


def fetch_keyword(url_tpl: str, stocks: list[dict], days: int) -> list[tuple[str, list[dict]]]:
    """銘柄名キーワードでフィードを取得。"""
    results = []
    for s in stocks:
        import urllib.parse
        keyword = urllib.parse.quote(s["name"])
        url = url_tpl.replace("{keyword}", keyword)
        print(f"  取得: {s['name']} キーワード検索")
        items = fetch_feed(url, days)
        if items:
            results.append((f"{s['name']} ニュース", items))
        time.sleep(1.0)
    return results


# ---------------------------------------------------------------------------
# Haiku 要約
# ---------------------------------------------------------------------------

def haiku_summarize_items(items: list[dict], feed_name: str) -> str:
    """Haiku で複数アイテムを一括要約。APIキーなし時はスキップ。"""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        items_text = "\n".join(
            f"- {it['published'][:10]} {it['title']}: {it['summary'][:200]}"
            for it in items[:15]
        )
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{
                "role": "user",
                "content": (
                    f"以下は「{feed_name}」の最新記事一覧です。"
                    "投資・金融政策の観点で重要なポイントを3〜5行で要約してください。\n\n"
                    + items_text
                ),
            }],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        return f"[Haiku要約失敗: {e}]"


# ---------------------------------------------------------------------------
# Markdown 出力
# ---------------------------------------------------------------------------

def build_markdown(
    today_str: str,
    trigger_sections: list[tuple[str, list[dict]]],
    macro_sections: list[tuple[str, list[dict], str]],
    days: int,
) -> str:
    lines = [
        f"# 日次ニュースサマリー {today_str}",
        f"",
        f"- **生成日時**: {datetime.now(JST).strftime('%Y-%m-%d %H:%M')} JST",
        f"- **対象期間**: 直近{days}日",
        f"",
    ]

    # トリガー（LLMなし）
    if trigger_sections:
        lines += ["## トリガー情報（ポジション銘柄・関連ニュース）", ""]
        for label, items in trigger_sections:
            lines += [f"### {label}", ""]
            for it in items[:10]:
                date_str = it["published"][:10] if it["published"] else ""
                lines.append(f"- **{date_str}** [{it['title']}]({it['link']})")
                if it["summary"]:
                    lines.append(f"  > {it['summary'][:150]}")
            lines.append("")

    # マクロ（Haiku要約あり）
    if macro_sections:
        lines += ["## マクロ情報（日銀・財務省・DIR等）", ""]
        for label, items, summary in macro_sections:
            lines += [f"### {label}", ""]
            if summary:
                lines += ["**AI要約（Haiku）:**", "", summary, ""]
            lines += ["**記事一覧:**", ""]
            for it in items[:8]:
                date_str = it["published"][:10] if it["published"] else ""
                lines.append(f"- **{date_str}** [{it['title']}]({it['link']})")
            lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv(_ENV_PATH)

    parser = argparse.ArgumentParser(description="RSS フィード取得・サマリー生成")
    parser.add_argument("--category", choices=["trigger", "macro", "supplement", "all"],
                        default="all", help="実行カテゴリ（デフォルト: all）")
    parser.add_argument("--days", type=int, default=1, help="直近N日分（デフォルト1）")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today_str = datetime.now(JST).strftime("%Y-%m-%d")
    out_path = OUTPUT_DIR / f"{today_str}_news.md"

    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    feeds = config.get("feeds", {})
    stocks = load_portfolio()
    print(f"ポジション銘柄: {[s['code'] for s in stocks]}")

    seen = load_seen()
    trigger_sections: list[tuple[str, list[dict]]] = []
    macro_sections: list[tuple[str, list[dict], str]] = []

    for feed_name, cfg in feeds.items():
        category = cfg.get("category", "trigger")
        use_llm  = cfg.get("llm", False)
        mode     = cfg.get("mode", "static")
        url      = cfg.get("url", "")

        run_cat = args.category
        if run_cat != "all" and category != run_cat:
            continue

        print(f"\n[{feed_name}] mode={mode} category={category}")

        if mode == "per_stock":
            results = fetch_per_stock(url, stocks, args.days)
            for label, items in results:
                # 重複除去
                new_items = [it for it in items if _hash(it["title"], it["link"]) not in seen]
                seen.update(_hash(it["title"], it["link"]) for it in new_items)
                if new_items:
                    trigger_sections.append((label, new_items))

        elif mode == "keyword":
            results = fetch_keyword(url, stocks, args.days)
            for label, items in results:
                new_items = [it for it in items if _hash(it["title"], it["link"]) not in seen]
                seen.update(_hash(it["title"], it["link"]) for it in new_items)
                if new_items:
                    trigger_sections.append((label, new_items))

        elif mode == "static":
            items = fetch_feed(url, args.days)
            new_items = [it for it in items if _hash(it["title"], it["link"]) not in seen]
            seen.update(_hash(it["title"], it["link"]) for it in new_items)

            if not new_items:
                print(f"  → 新着なし")
                continue

            print(f"  → {len(new_items)}件")
            if category == "macro":
                summary = ""
                if use_llm:
                    print(f"  Haiku 要約中...")
                    summary = haiku_summarize_items(new_items, feed_name)
                macro_sections.append((feed_name, new_items, summary))
            elif category == "trigger":
                trigger_sections.append((feed_name, new_items))

    save_seen(seen)

    if not trigger_sections and not macro_sections:
        print("\n新着アイテムなし。")
        return

    md = build_markdown(today_str, trigger_sections, macro_sections, args.days)
    out_path.write_text(md, encoding="utf-8")
    print(f"\n出力: {out_path}")


if __name__ == "__main__":
    main()
