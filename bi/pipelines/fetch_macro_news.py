"""
yfinance からマクロ指標のニュースを取得して market/daily/{date}_news_raw.md に保存する。

使い方:
  python fetch_macro_news.py

出力: market/daily/{YYYY-MM-DD}_news_raw.md
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import yfinance as yf

OUTPUT_DIR = Path("../../market/daily")

TICKERS = {
    "S&P500 (^GSPC)": "^GSPC",
    "日経平均 (^N225)": "^N225",
    "ダウ (^DJI)": "^DJI",
    "ドル円 (USDJPY=X)": "USDJPY=X",
}


def fetch_news(ticker: str) -> list[dict]:
    t = yf.Ticker(ticker)
    news = t.news or []
    results = []
    for n in news:
        c = n.get("content", {})
        title = c.get("title", "").strip()
        summary = c.get("summary", "").replace("\xa0", " ").strip()
        pub = c.get("pubDate", "")[:10]
        url = c.get("canonicalUrl", {}).get("url", "")
        if title:
            results.append({"date": pub, "title": title, "summary": summary, "url": url})
    return results


def build_markdown(today: str) -> str:
    lines = [
        f"# マクロニュース 生データ ({today})",
        f"",
        f"> yfinance から自動取得。Claudeに読ませてサマリー・レポートを生成する。",
        f"",
    ]

    for label, ticker in TICKERS.items():
        lines.append(f"## {label}")
        lines.append("")
        items = fetch_news(ticker)
        if not items:
            lines.append("*取得なし*")
        else:
            for item in items:
                lines.append(f"### [{item['date']}] {item['title']}")
                if item["summary"]:
                    lines.append(f"{item['summary']}")
                if item["url"]:
                    lines.append(f"URL: {item['url']}")
                lines.append("")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    today = date.today().strftime("%Y-%m-%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{today}_news_raw.md"

    print(f"ニュース取得中...")
    md = build_markdown(today)
    out_path.write_text(md, encoding="utf-8")

    size_kb = out_path.stat().st_size / 1024
    print(f"保存完了: {out_path}  ({size_kb:.1f} KB)")
    print(f"-> このファイルをClaude Codeに渡してマクロレポートを依頼してください。")


if __name__ == "__main__":
    main()
