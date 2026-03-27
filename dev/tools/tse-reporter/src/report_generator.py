"""レポート生成モジュール"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import config
from .web_researcher import ResearchResult


# --------------------------------------------------------------------------- #
# Markdown テンプレート
# --------------------------------------------------------------------------- #

_MD_HEADER = """# 東証株価変動レポート

**生成日時**: {generated_at}
**対象日**: {target_date}
**集計対象**: 前日比 ±{threshold:.0f}% 以上の銘柄

---
"""

_MD_STOCK_SECTION = """## {index}. {name}（{code}）

| 項目 | 値 |
|------|-----|
| 証券コード | {code} |
| 前日比 | **{change_rate:+.2f}%** {arrow} |
| 調査日 | {target_date} |

### 変動理由の概要

{summary}

{sources_section}

---
"""

_MD_SOURCES = """### 参照ソース

{sources}
"""

# --------------------------------------------------------------------------- #
# HTML テンプレート
# --------------------------------------------------------------------------- #

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>東証株価変動レポート - {target_date}</title>
  <style>
    body {{ font-family: "Helvetica Neue", Arial, "Hiragino Kaku Gothic ProN", sans-serif;
            max-width: 960px; margin: 0 auto; padding: 2rem; color: #333; }}
    h1 {{ color: #1a1a2e; border-bottom: 3px solid #e94560; padding-bottom: .5rem; }}
    h2 {{ color: #16213e; margin-top: 2rem; }}
    h3 {{ color: #0f3460; }}
    .meta {{ background: #f8f9fa; padding: 1rem; border-radius: 8px; margin-bottom: 2rem; }}
    .stock-card {{ border: 1px solid #dee2e6; border-radius: 8px; padding: 1.5rem;
                   margin-bottom: 1.5rem; }}
    .up   {{ color: #c0392b; font-weight: bold; }}
    .down {{ color: #2980b9; font-weight: bold; }}
    table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
    th, td {{ border: 1px solid #dee2e6; padding: .5rem 1rem; text-align: left; }}
    th {{ background: #f1f3f5; }}
    .sources {{ font-size: .85rem; color: #666; }}
    .sources a {{ color: #0f3460; }}
    pre {{ background: #f8f9fa; padding: 1rem; border-radius: 4px; overflow-x: auto;
           white-space: pre-wrap; }}
  </style>
</head>
<body>
  <h1>東証株価変動レポート</h1>
  <div class="meta">
    <p><strong>生成日時</strong>: {generated_at}</p>
    <p><strong>対象日</strong>: {target_date}</p>
    <p><strong>集計対象</strong>: 前日比 ±{threshold:.0f}% 以上 — {count} 銘柄</p>
  </div>
  {stocks_html}
</body>
</html>
"""

_HTML_STOCK_CARD = """
  <div class="stock-card">
    <h2>{index}. {name}（{code}）</h2>
    <table>
      <tr><th>証券コード</th><td>{code}</td></tr>
      <tr><th>前日比</th><td class="{cls}">{change_rate:+.2f}%</td></tr>
      <tr><th>調査日</th><td>{target_date}</td></tr>
    </table>
    <h3>変動理由の概要</h3>
    <pre>{summary}</pre>
    {sources_html}
  </div>
"""


# --------------------------------------------------------------------------- #
# ReportGenerator
# --------------------------------------------------------------------------- #

class ReportGenerator:
    """調査結果からMarkdown / HTMLレポートを生成する"""

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def generate_screenshot_report(
        self,
        stock_info,          # ScreenshotAnalyzer.StockInfo
        research: ResearchResult,
        output_path: Optional[Path] = None,
    ) -> Path:
        """
        スクリーンショット解析 + Web 調査結果からレポートを生成する（機能1）。
        """
        target_date = datetime.now().strftime("%Y-%m-%d")
        results = [research]

        return self._write_report(
            results=results,
            target_date=target_date,
            threshold=abs(research.change_rate),
            output_path=output_path,
            title_suffix="_screenshot",
        )

    def generate_screener_report(
        self,
        results: list[ResearchResult],
        target_date: str,
        output_path: Optional[Path] = None,
    ) -> Path:
        """
        J-Quants スクリーナー結果からレポートを生成する（機能2）。
        """
        return self._write_report(
            results=results,
            target_date=target_date,
            threshold=config.PRICE_CHANGE_THRESHOLD,
            output_path=output_path,
            title_suffix="_screener",
        )

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _write_report(
        self,
        results: list[ResearchResult],
        target_date: str,
        threshold: float,
        output_path: Optional[Path],
        title_suffix: str,
    ) -> Path:
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fmt = config.REPORT_FORMAT.lower()

        if output_path is None:
            filename = f"report_{target_date.replace('-', '')}{title_suffix}.{fmt if fmt == 'html' else 'md'}"
            output_path = config.REPORT_OUTPUT_DIR / filename

        if fmt == "html":
            content = self._render_html(results, target_date, generated_at, threshold)
        else:
            content = self._render_markdown(results, target_date, generated_at, threshold)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        return output_path

    def _render_markdown(
        self,
        results: list[ResearchResult],
        target_date: str,
        generated_at: str,
        threshold: float,
    ) -> str:
        parts = [
            _MD_HEADER.format(
                generated_at=generated_at,
                target_date=target_date,
                threshold=threshold,
            )
        ]

        for i, r in enumerate(results, 1):
            arrow = "🔴" if r.change_rate > 0 else "🔵"
            sources_section = ""
            if r.sources:
                source_lines = "\n".join(f"- {url}" for url in r.sources)
                sources_section = _MD_SOURCES.format(sources=source_lines)

            parts.append(
                _MD_STOCK_SECTION.format(
                    index=i,
                    code=r.code,
                    name=r.name,
                    change_rate=r.change_rate,
                    arrow=arrow,
                    target_date=target_date,
                    summary=r.summary,
                    sources_section=sources_section,
                )
            )

        return "\n".join(parts)

    def _render_html(
        self,
        results: list[ResearchResult],
        target_date: str,
        generated_at: str,
        threshold: float,
    ) -> str:
        cards = []
        for i, r in enumerate(results, 1):
            cls = "up" if r.change_rate > 0 else "down"
            sources_html = ""
            if r.sources:
                links = "".join(
                    f'<li><a href="{url}" target="_blank">{url}</a></li>'
                    for url in r.sources
                )
                sources_html = f'<div class="sources"><h3>参照ソース</h3><ul>{links}</ul></div>'

            cards.append(
                _HTML_STOCK_CARD.format(
                    index=i,
                    code=r.code,
                    name=r.name,
                    change_rate=r.change_rate,
                    cls=cls,
                    target_date=target_date,
                    summary=r.summary,
                    sources_html=sources_html,
                )
            )

        return _HTML_TEMPLATE.format(
            generated_at=generated_at,
            target_date=target_date,
            threshold=threshold,
            count=len(results),
            stocks_html="\n".join(cards),
        )
