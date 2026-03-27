"""Google検索結果をローカルLLMに貼り付けやすい Markdown に整形する（LLM API は呼ばない）"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from .google_cse_client import SearchHit
from .jquants_client import DailyQuote


@dataclass
class QueryBundle:
    label: str
    query: str
    hits: list[SearchHit]
    error: str | None = None


def _hits_to_markdown(hits: list[SearchHit]) -> str:
    lines: list[str] = []
    for i, h in enumerate(hits, 1):
        lines.append(f"{i}. **{h.title}**  ")
        lines.append(f"   - URL: {h.link}")
        if h.snippet:
            lines.append(f"   - 抜粋: {h.snippet}")
        lines.append("")
    if not lines:
        lines.append("（検索結果なし）")
        lines.append("")
    return "\n".join(lines)


def build_stock_section(
    rank: int,
    side: Literal["上昇", "下落"],
    quote: DailyQuote,
    bundles: list[QueryBundle],
) -> str:
    name = quote.name or quote.code
    rate = quote.change_rate or 0.0
    parts = [
        f"## {rank}. {name}（{quote.code}） {rate:+.2f}% 【{side}】",
        "",
    ]
    for b in bundles:
        parts.append(f"### 検索: {b.label}")
        parts.append(f"`{b.query}`")
        parts.append("")
        if b.error:
            parts.append(f"**検索エラー**: {b.error}")
            parts.append("")
        parts.append(_hits_to_markdown(b.hits))
        parts.append("---")
        parts.append("")
    return "\n".join(parts)


def build_document_header(
    target_date: str,
    cse_calls: int,
    stock_count: int,
    diagnostic_note: str = "",
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if diagnostic_note.strip():
        diag_block = f"\n{diagnostic_note.strip()}\n\n---\n"
    else:
        diag_block = "\n\n---\n"
    return f"""# ローカルLLM用マテリアル（プロトタイプ）

**生成日時**: {now}
**株価の基準日**: {target_date}
**Google Custom Search 呼び出し回数（目安）**: {cse_calls} 回（1日無料枠 100 回以内に注意）
**銘柄数**: {stock_count}
{diag_block}
## このファイルの使い方

1. 下の「**一括依頼用プロンプト（テンプレ）**」をコピーする。
2. 続く「銘柄ごとの検索結果」セクションごと、またはファイル全体を **Ollama / LM Studio 等のローカルLLM** に貼り付ける。
3. LLMには **1〜2行の要約** と **参照したソース名** の出力を依頼する（クラウドAPIキー不要）。

---

## 一括依頼用プロンプト（テンプレ）

以下の【銘柄ブロック】それぞれについて、東証の株価が記載のとおり変動した**主な理由**を、**日本語で1〜2行**に要約してください。可能な範囲で **公式IR・ニュース・Yahoo掲示板・ブログ** など、検索結果に現れた情報源の名前を括弧や「—」のあとに添えてください。根拠が乏しい場合はその旨を書いてください。

"""


def build_footer_prompt() -> str:
    return """
---

## （LLM向け）出力フォーマット例

```
上昇トップ / 下落トップの該当銘柄について:

1. 銘柄名（コード） ±X.X%
   → 要約1〜2行（ソース名）
```

"""


def assemble_full_report(
    target_date: str,
    sections: list[str],
    cse_calls: int,
    stock_count: int,
    diagnostic_note: str = "",
) -> str:
    header = build_document_header(
        target_date=target_date,
        cse_calls=cse_calls,
        stock_count=stock_count,
        diagnostic_note=diagnostic_note,
    )
    body = "\n".join(sections)
    return header + "\n" + body + build_footer_prompt()
