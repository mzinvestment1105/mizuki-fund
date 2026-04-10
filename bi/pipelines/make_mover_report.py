"""
動意銘柄レポート 生データ生成
==============================
当日の値動きが大きかった銘柄を抽出し、TDNet適時開示・セクターコンテキストと
組み合わせた生データMarkdownを出力する。

Claudeがこのファイルを読んで「なぜ動いたか」を推論してレポートを生成する。

出力: market/daily/YYYY-MM-DD_movers_raw.md

実行:
  cd bi/pipelines
  python make_mover_report.py
  python make_mover_report.py --date 2026-04-11   # 特定日
  python make_mover_report.py --top 20 --bottom 10 --min-cap 100  # パラメータ指定

環境変数:
  JQUANTS_API_KEY  必須
"""

from __future__ import annotations

import argparse
import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from dotenv import load_dotenv

from jq_client_utils import (
    fetch_paginated_v2,
    normalize_code_4,
)

# ---------------------------------------------------------------------------
# パス定義
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
OUTPUTS_DIR = BASE_DIR / ".." / "outputs"
MARKET_DAILY_DIR = BASE_DIR / ".." / ".." / "market" / "daily"
SCREENING_MASTER_PATH = OUTPUTS_DIR / "screening_master.parquet"
SECTOR_STOCK_PATH = OUTPUTS_DIR / "sector_stock_weekly.parquet"
SECTOR_AGG_PATH = OUTPUTS_DIR / "sector_weekly.parquet"

# ---------------------------------------------------------------------------
# デフォルト設定
# ---------------------------------------------------------------------------
DEFAULT_TOP_N = 15
DEFAULT_BOTTOM_N = 5
DEFAULT_MIN_CAP_OKU = 50       # 時価総額下限 億円
DEFAULT_TDNET_DAYS = 30        # TDNet取得期間
TDNET_PDF_MAX_CHARS = 2000     # PDF本文の最大文字数
REQUEST_SLEEP = 0.5

_TDNET_ATOM_URL = "https://webapi.yanoshin.jp/webapi/tdnet/list/{code}.atom"
_NS = {"a": "http://purl.org/atom/ns#"}


# ---------------------------------------------------------------------------
# Step 1: 日次価格を全銘柄一括取得（2コール）
# ---------------------------------------------------------------------------

def fetch_daily_all(client, target_date: date) -> pd.DataFrame:
    """指定日の全銘柄OHLCV（/equities/bars/daily?date=YYYY-MM-DD）を取得してDataFrameで返す。"""
    rows = fetch_paginated_v2(
        client,
        "/equities/bars/daily",
        params={"date": target_date.strftime("%Y-%m-%d")},
        sleep_seconds=1.0,
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # カラム正規化
    col_map = {}
    for col in df.columns:
        cl = col.lower()
        if cl in ("open", "o"):       col_map[col] = "Open"
        elif cl in ("high", "h"):     col_map[col] = "High"
        elif cl in ("low", "l"):      col_map[col] = "Low"
        elif cl in ("close", "c"):    col_map[col] = "Close"
        elif cl in ("volume", "v", "vo"): col_map[col] = "Volume"
        elif cl == "code":            col_map[col] = "Code"
        elif cl == "date":            col_map[col] = "Date"
    df = df.rename(columns=col_map)
    df["Code"] = df["Code"].astype(str).str[:4]
    if "Close" in df.columns:
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
    return df


def resolve_trading_days(client, target_date: date, lookback: int = 14) -> tuple[date, date]:
    """target_date を含む直近2営業日を返す（today, prev）。"""
    found: list[date] = []
    for i in range(lookback):
        d = target_date - timedelta(days=i)
        rows = fetch_paginated_v2(
            client,
            "/equities/bars/daily",
            params={"date": d.strftime("%Y-%m-%d")},
            sleep_seconds=0.5,
        )
        if rows:
            found.append(d)
            if len(found) == 2:
                break
    if len(found) < 2:
        raise RuntimeError(f"直近2営業日が見つかりません（{target_date} から {lookback} 日さかのぼった）")
    return found[0], found[1]


# ---------------------------------------------------------------------------
# Step 2: リターン計算 & モバー抽出
# ---------------------------------------------------------------------------

def compute_movers(
    today_df: pd.DataFrame,
    prev_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    top_n: int,
    bottom_n: int,
    min_cap_oku: float,
) -> pd.DataFrame:
    """日次リターンを計算し、上位・下位銘柄を返す。"""
    t = today_df[["Code", "Close", "Volume"]].rename(columns={"Close": "Close_T", "Volume": "Volume_T"})
    p = prev_df[["Code", "Close"]].rename(columns={"Close": "Close_P"})
    df = t.merge(p, on="Code", how="inner")
    df["DailyReturn"] = (df["Close_T"] / df["Close_P"] - 1) * 100
    df = df.dropna(subset=["DailyReturn", "Close_T", "Close_P"])

    # screening_master から銘柄情報を結合
    meta_cols = ["Code", "CompanyName", "Sector17CodeName", "MarketCap"]
    avail = [c for c in meta_cols if c in master_df.columns]
    meta = master_df[avail].copy()
    meta["Code"] = meta["Code"].astype(str).str[:4]

    df = df.merge(meta, on="Code", how="left")

    # 時価総額フィルタ（億円換算）
    if "MarketCap" in df.columns:
        df["MarketCapOku"] = pd.to_numeric(df["MarketCap"], errors="coerce") / 1e8
        df = df[df["MarketCapOku"] >= min_cap_oku]

    df = df.drop_duplicates("Code")

    top = df.nlargest(top_n, "DailyReturn")
    bottom = df.nsmallest(bottom_n, "DailyReturn")
    movers = pd.concat([top, bottom]).drop_duplicates("Code")
    return movers


# ---------------------------------------------------------------------------
# Step 3: TDNet取得
# ---------------------------------------------------------------------------

def fetch_tdnet_atom(code4: str) -> tuple[list[dict], str]:
    url = _TDNET_ATOM_URL.format(code=code4)
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as e:
        return [], f"[取得失敗: {e}]"

    entries = []
    company_name = ""
    for entry in root.findall("a:entry", _NS):
        def _text(tag: str) -> str:
            el = entry.find(tag, _NS)
            return el.text.strip() if el is not None and el.text else ""

        published_str = _text("a:issued") or _text("a:created") or _text("a:modified")
        link_el = entry.find("a:link[@rel='alternate']", _NS)
        link_href = link_el.get("href", "") if link_el is not None else ""
        pdf_url = ""
        if "rd.php?" in link_href:
            pdf_url = link_href.split("rd.php?", 1)[1]
        elif link_href.endswith(".pdf"):
            pdf_url = link_href

        raw_title = _text("a:title")
        if ":" in raw_title and not company_name:
            company_name = raw_title.split(":", 1)[0].strip()
        title = raw_title.split(":", 1)[1].strip() if ":" in raw_title else raw_title

        entries.append({
            "title": title,
            "published": published_str,
            "summary": _text("a:summary"),
            "pdf_url": pdf_url,
        })
    return entries, company_name


def filter_by_days(entries: list[dict], days: int) -> list[dict]:
    cutoff = datetime.now().astimezone() - timedelta(days=days)
    result = []
    for e in entries:
        try:
            dt = datetime.fromisoformat(e["published"].replace("Z", "+00:00"))
            if dt >= cutoff:
                result.append(e)
        except Exception:
            result.append(e)
    return result


def fetch_pdf_text(pdf_url: str, max_chars: int = TDNET_PDF_MAX_CHARS) -> str:
    if not pdf_url:
        return ""
    try:
        from io import BytesIO, StringIO
        from pdfminer.high_level import extract_text_to_fp
        from pdfminer.layout import LAParams
        resp = requests.get(pdf_url, timeout=20)
        resp.raise_for_status()
        out = StringIO()
        extract_text_to_fp(BytesIO(resp.content), out, laparams=LAParams(), output_type="text", codec=None)
        text = out.getvalue()
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text[:max_chars]
    except Exception as e:
        return f"[PDF取得失敗: {e}]"


def fetch_tdnet_batch(codes: list[str], days: int) -> dict[str, dict]:
    """複数コードのTDNetを取得してdict[code -> {entries, company_name}]で返す。"""
    result = {}
    total = len(codes)
    for i, code in enumerate(codes, 1):
        code4 = normalize_code_4(code)
        print(f"  TDNet [{i}/{total}] {code4} ...")
        entries, company_name = fetch_tdnet_atom(code4)
        entries = filter_by_days(entries, days)
        # PDF本文取得（開示件数が多い場合は最初の3件のみ）
        for j, e in enumerate(entries[:3]):
            if e.get("pdf_url"):
                e["pdf_text"] = fetch_pdf_text(e["pdf_url"])
                time.sleep(REQUEST_SLEEP)
            else:
                e["pdf_text"] = ""
        for e in entries[3:]:
            e["pdf_text"] = ""
        result[code4] = {"entries": entries, "company_name": company_name}
        time.sleep(REQUEST_SLEEP)
    return result


# ---------------------------------------------------------------------------
# Step 3b: Yahoo Finance ニュース & 掲示板スクレイピング
# ---------------------------------------------------------------------------

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

# ナビゲーション・UI テキストの除外パターン
_BBS_NOISE = re.compile(
    r"(JavaScript|ポートフォリオ|ランキング|ログイン|VIP倶楽部|掲示板マイページ"
    r"|前のページ|変更点|リニューアル|報告$|^返信|投資の参考|^はい\d|^いいえ\d"
    r"|^No\.\d|^\d{4}/\d+/\d+|NISA|カードローン|証券会社|不動産投資"
    r"|投資信託|FX・為替|米国株|日本株トップ|マイアカウント|検索やさしい)"
)
# 実際の投稿っぽい文字を含む（文末表現）
_BBS_POST_LIKE = re.compile(r"[。！？ねよわだます]")


def fetch_yahoo_news(code4: str, max_items: int = 8) -> list[dict]:
    """Yahoo Finance Japan の銘柄別ニュースページから直近記事を取得する。"""
    from bs4 import BeautifulSoup
    url = f"https://finance.yahoo.co.jp/quote/{code4}.T/news"
    try:
        r = requests.get(url, headers=_YAHOO_HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        items = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if "/news/detail/" not in href:
                continue
            text = a.get_text(strip=True)
            if not text or len(text) < 10:
                continue
            # テキスト末尾の「日付＋ソース」を切り出す
            # 例: "記事タイトル3/10株探ニュース" → title="記事タイトル", date="3/10", source="株探ニュース"
            m = re.search(r"(\d+/\d+)([^\d].{1,20})$", text)
            if m:
                date_str = m.group(1)
                source = m.group(2).strip()
                title = text[: m.start()].strip()
            else:
                date_str, source, title = "", "", text
            if not title:
                continue
            items.append({"title": title, "date": date_str, "source": source})
            if len(items) >= max_items:
                break
        return items
    except Exception as e:
        return [{"title": f"[取得失敗: {e}]", "date": "", "source": ""}]


def fetch_yahoo_bbs(code4: str, max_posts: int = 8) -> dict:
    """Yahoo Finance Japan の銘柄掲示板から直近投稿とセンチメントを取得する。"""
    from bs4 import BeautifulSoup
    url = f"https://finance.yahoo.co.jp/quote/{code4}.T/forum"
    try:
        r = requests.get(url, headers=_YAHOO_HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # センチメント（みんなの評価）
        sentiment = ""
        for el in soup.find_all(string=re.compile(r"強く買いたい.*%")):
            m = re.search(r"強く買いたい\s*([\d.]+)%.*?強く売りたい\s*([\d.]+)%", str(el))
            if m:
                sentiment = f"強く買いたい{m.group(1)}% / 強く売りたい{m.group(2)}%"
            break

        # 投稿本文を抽出
        seen = set()
        posts = []
        for el in soup.find_all(["p", "span", "div"]):
            text = el.get_text(strip=True)
            if len(text) < 25 or len(text) > 350:
                continue
            if _BBS_NOISE.search(text):
                continue
            if not _BBS_POST_LIKE.search(text):
                continue
            # 重複除去（先頭50文字で判定）
            key = text[:50]
            if key in seen:
                continue
            seen.add(key)
            posts.append(text)
            if len(posts) >= max_posts:
                break

        return {"sentiment": sentiment, "posts": posts}
    except Exception as e:
        return {"sentiment": "", "posts": [f"[取得失敗: {e}]"]}


def fetch_yahoo_batch(codes: list[str]) -> dict[str, dict]:
    """複数コードのYahoo情報をまとめて取得する。"""
    result = {}
    total = len(codes)
    for i, code in enumerate(codes, 1):
        code4 = normalize_code_4(code)
        print(f"  Yahoo [{i}/{total}] {code4} ...")
        news = fetch_yahoo_news(code4)
        bbs = fetch_yahoo_bbs(code4)
        result[code4] = {"news": news, "bbs": bbs}
        time.sleep(REQUEST_SLEEP)
    return result


# ---------------------------------------------------------------------------
# Step 4: セクターコンテキスト取得
# ---------------------------------------------------------------------------

def get_sector_context(today_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    """今日の全銘柄リターンからセクター平均を計算する。"""
    if "Code" not in master_df.columns or "Sector17CodeName" not in master_df.columns:
        return pd.DataFrame()
    meta = master_df[["Code", "Sector17CodeName", "MarketCapClose"]].copy()
    meta["Code"] = meta["Code"].astype(str).str[:4]

    t = today_df[["Code", "Close"]].copy()
    t["Code"] = t["Code"].astype(str).str[:4]
    # 前日比はここでは出せないので、sector_weekly.parquetがあれば使う
    # なければスキップ
    return pd.DataFrame()


def load_sector_weekly_context() -> dict[str, float]:
    """sector_weekly.parquetからセクター別直近週次リターン（Return_W01）を返す。"""
    if not SECTOR_AGG_PATH.exists():
        return {}
    try:
        df = pd.read_parquet(SECTOR_AGG_PATH)
        if "Sector17CodeName" in df.columns and "Return_W01" in df.columns:
            return dict(zip(df["Sector17CodeName"], df["Return_W01"]))
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Step 5: マクロレポート取得
# ---------------------------------------------------------------------------

def load_latest_macro_report() -> str:
    """直近のsonnet_macroレポートの冒頭部分を取得する。"""
    if not MARKET_DAILY_DIR.exists():
        return ""
    files = sorted(MARKET_DAILY_DIR.glob("*_sonnet_macro.md"), reverse=True)
    if not files:
        return ""
    content = files[0].read_text(encoding="utf-8")
    # 冒頭2000文字（市況スナップショットと重要テーマの要旨）
    return content[:2000]


# ---------------------------------------------------------------------------
# Step 6: Markdown生成
# ---------------------------------------------------------------------------

def _append_mover_sources(
    lines: list[str],
    code4: str,
    tdnet_data: dict[str, dict],
    yahoo_data: dict[str, dict],
) -> None:
    """TDNet・Yahooニュース・掲示板を銘柄ごとにまとめてlinesに追記する。"""
    # TDNet
    tdnet = tdnet_data.get(code4, {})
    entries = tdnet.get("entries", [])
    if entries:
        lines.append(f"**TDNet適時開示（直近{DEFAULT_TDNET_DAYS}日: {len(entries)}件）:**")
        lines.append("")
        for e in entries:
            lines.append(f"- {e['published'][:10]}　{e['title']}")
            if e.get("pdf_text"):
                text_preview = e["pdf_text"][:600].replace("\n", " ").strip()
                lines.append(f"  > {text_preview}")
        lines.append("")
    else:
        lines.append(f"**TDNet適時開示（直近{DEFAULT_TDNET_DAYS}日）:** なし")
        lines.append("")

    # Yahoo Finance ニュース
    yahoo = yahoo_data.get(code4, {})
    news = yahoo.get("news", [])
    if news:
        lines.append(f"**Yahooニュース（直近{len(news)}件）:**")
        lines.append("")
        for n in news:
            date_src = f"({n['date']} {n['source']})" if n.get("date") else ""
            lines.append(f"- {n['title']} {date_src}".strip())
        lines.append("")
    else:
        lines.append("**Yahooニュース:** なし")
        lines.append("")

    # Yahoo掲示板
    bbs = yahoo.get("bbs", {})
    sentiment = bbs.get("sentiment", "")
    posts = bbs.get("posts", [])
    if sentiment or posts:
        lines.append("**Yahoo掲示板:**")
        if sentiment:
            lines.append(f"- みんなの評価: {sentiment}")
        lines.append("")
        for p in posts:
            lines.append(f"> {p}")
        lines.append("")
    else:
        lines.append("**Yahoo掲示板:** なし")
        lines.append("")


def build_raw_report(
    movers: pd.DataFrame,
    tdnet_data: dict[str, dict],
    yahoo_data: dict[str, dict],
    sector_weekly: dict[str, float],
    macro_snippet: str,
    today: date,
    prev: date,
    top_n: int,
    bottom_n: int,
) -> str:
    lines = [
        f"# 動意銘柄レポート 生データ ({today.strftime('%Y-%m-%d')})",
        f"",
        f"> yfinance + JQuants + TDNet から自動取得。Claude が「なぜ動いたか」を推論してレポートを生成する。",
        f"- **生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M')} JST",
        f"- **価格比較**: {prev.strftime('%Y-%m-%d')} → {today.strftime('%Y-%m-%d')}（前営業日比）",
        f"- **TDNet対象期間**: 直近{DEFAULT_TDNET_DAYS}日",
        f"",
    ]

    top_movers = movers[movers["DailyReturn"] >= 0].sort_values("DailyReturn", ascending=False).head(top_n)
    bottom_movers = movers[movers["DailyReturn"] < 0].sort_values("DailyReturn").head(bottom_n)

    # --- 上昇銘柄 ---
    lines += [f"## 上昇銘柄 Top {len(top_movers)}", f""]
    for _, row in top_movers.iterrows():
        code4 = normalize_code_4(row["Code"])
        name = row.get("CompanyName", code4)
        ret = row["DailyReturn"]
        close = row["Close_T"]
        sector = row.get("Sector17CodeName", "不明")
        cap = row.get("MarketCapOku", None)
        cap_str = f"{cap:.0f}億" if pd.notna(cap) else "時価総額不明"
        vol = row.get("Volume_T", None)
        vol_str = f"{vol/1e4:.0f}万株" if pd.notna(vol) else "出来高不明"

        # セクター週次リターン
        sector_ret = sector_weekly.get(sector, None)
        sector_ctx = f"セクター週次リターン: {sector_ret:+.1f}%" if sector_ret is not None else "セクター週次データなし"

        lines += [
            f"### {code4} {name}　{ret:+.1f}%",
            f"",
            f"- 終値: {close:,.0f}円　出来高: {vol_str}　時価総額: {cap_str}",
            f"- セクター: {sector}　{sector_ctx}",
            f"",
        ]

        _append_mover_sources(lines, code4, tdnet_data, yahoo_data)

    # --- 下落銘柄 ---
    lines += [f"## 下落銘柄 Bottom {len(bottom_movers)}", f""]
    for _, row in bottom_movers.iterrows():
        code4 = normalize_code_4(row["Code"])
        name = row.get("CompanyName", code4)
        ret = row["DailyReturn"]
        close = row["Close_T"]
        sector = row.get("Sector17CodeName", "不明")
        cap = row.get("MarketCapOku", None)
        cap_str = f"{cap:.0f}億" if pd.notna(cap) else "時価総額不明"
        vol = row.get("Volume_T", None)
        vol_str = f"{vol/1e4:.0f}万株" if pd.notna(vol) else "出来高不明"

        sector_ret = sector_weekly.get(sector, None)
        sector_ctx = f"セクター週次リターン: {sector_ret:+.1f}%" if sector_ret is not None else "セクター週次データなし"

        lines += [
            f"### {code4} {name}　{ret:+.1f}%",
            f"",
            f"- 終値: {close:,.0f}円　出来高: {vol_str}　時価総額: {cap_str}",
            f"- セクター: {sector}　{sector_ctx}",
            f"",
        ]

        _append_mover_sources(lines, code4, tdnet_data, yahoo_data)

    # --- マクロコンテキスト ---
    if macro_snippet:
        lines += [
            f"## マクロコンテキスト（直近レポート冒頋）",
            f"",
            f"```",
            macro_snippet,
            f"```",
            f"",
        ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    _ENV_PATH = BASE_DIR / ".env"
    load_dotenv(_ENV_PATH)

    parser = argparse.ArgumentParser(description="動意銘柄レポート 生データ生成")
    parser.add_argument("--date", default=None, help="対象日（YYYY-MM-DD）。省略時は本日")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N, help=f"上昇銘柄件数（デフォルト{DEFAULT_TOP_N}）")
    parser.add_argument("--bottom", type=int, default=DEFAULT_BOTTOM_N, help=f"下落銘柄件数（デフォルト{DEFAULT_BOTTOM_N}）")
    parser.add_argument("--min-cap", type=float, default=DEFAULT_MIN_CAP_OKU, help=f"時価総額下限・億円（デフォルト{DEFAULT_MIN_CAP_OKU}）")
    parser.add_argument("--no-pdf", action="store_true", help="TDNet PDF本文取得をスキップ")
    args = parser.parse_args()

    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise ValueError("JQUANTS_API_KEY が未設定です")

    import jquantsapi
    client = jquantsapi.ClientV2(api_key=api_key)

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    print(f"対象日: {target_date}")

    # --- 価格取得 ---
    print("直近2営業日を確認中...")
    today_dt, prev_dt = resolve_trading_days(client, target_date)
    print(f"  本日営業日: {today_dt}　前営業日: {prev_dt}")

    print(f"価格取得: {today_dt} ...")
    today_df = fetch_daily_all(client, today_dt)
    print(f"  {today_dt}: {len(today_df)} 銘柄")

    print(f"価格取得: {prev_dt} ...")
    prev_df = fetch_daily_all(client, prev_dt)
    print(f"  {prev_dt}: {len(prev_df)} 銘柄")

    # --- screening_master読み込み ---
    if not SCREENING_MASTER_PATH.exists():
        raise FileNotFoundError(f"screening_master.parquet が見つかりません: {SCREENING_MASTER_PATH}")
    master_df = pd.read_parquet(SCREENING_MASTER_PATH)
    master_df["Code"] = master_df["Code"].astype(str).str[:4]
    print(f"screening_master: {len(master_df)} 銘柄")

    # --- モバー抽出 ---
    print(f"リターン計算・モバー抽出（上位{args.top}、下位{args.bottom}、時価総額{args.min_cap}億以上）...")
    movers = compute_movers(
        today_df, prev_df, master_df,
        top_n=args.top,
        bottom_n=args.bottom,
        min_cap_oku=args.min_cap,
    )
    print(f"  抽出銘柄: {len(movers)} 件")

    # リターン分布を表示
    top_ret = movers[movers["DailyReturn"] >= 0]["DailyReturn"]
    bottom_ret = movers[movers["DailyReturn"] < 0]["DailyReturn"]
    if not top_ret.empty:
        print(f"  上昇: 最大{top_ret.max():.1f}% 〜 最小{top_ret.min():.1f}%")
    if not bottom_ret.empty:
        print(f"  下落: 最大{bottom_ret.min():.1f}% 〜 最小{bottom_ret.max():.1f}%")

    # --- TDNet取得 ---
    codes = movers["Code"].astype(str).str[:4].tolist()
    if args.no_pdf:
        print("TDNet取得（PDFスキップ）...")
        tdnet_data = {}
        for code in codes:
            entries, company_name = fetch_tdnet_atom(normalize_code_4(code))
            entries = filter_by_days(entries, DEFAULT_TDNET_DAYS)
            for e in entries:
                e["pdf_text"] = ""
            tdnet_data[normalize_code_4(code)] = {"entries": entries, "company_name": company_name}
    else:
        print("TDNet取得（PDF本文あり）...")
        tdnet_data = fetch_tdnet_batch(codes, DEFAULT_TDNET_DAYS)

    # --- Yahoo Finance ニュース & 掲示板 ---
    print("Yahoo Finance ニュース・掲示板取得...")
    yahoo_data = fetch_yahoo_batch(codes)

    # --- セクターコンテキスト ---
    sector_weekly = load_sector_weekly_context()
    if sector_weekly:
        print(f"セクター週次リターン: {len(sector_weekly)} セクター")
    else:
        print("sector_weekly.parquet なし（スキップ）")

    # --- マクロレポート ---
    macro_snippet = load_latest_macro_report()
    if macro_snippet:
        print("マクロレポート: 読み込み済み")

    # --- Markdown生成 ---
    report_md = build_raw_report(
        movers=movers,
        tdnet_data=tdnet_data,
        yahoo_data=yahoo_data,
        sector_weekly=sector_weekly,
        macro_snippet=macro_snippet,
        today=today_dt,
        prev=prev_dt,
        top_n=args.top,
        bottom_n=args.bottom,
    )

    # --- 出力 ---
    MARKET_DAILY_DIR.mkdir(parents=True, exist_ok=True)
    out_path = MARKET_DAILY_DIR / f"{today_dt.strftime('%Y-%m-%d')}_movers_raw.md"
    out_path.write_text(report_md, encoding="utf-8")
    print(f"\n出力: {out_path}")
    print(f"次のステップ: Claude Code にこのファイルを読ませて「なぜ動いたか」レポートを生成してください。")


if __name__ == "__main__":
    main()
