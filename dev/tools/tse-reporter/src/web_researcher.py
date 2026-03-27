"""Web検索による株価変動理由調査モジュール

Google Custom Search API で検索し、Claude Code CLI で分析する（Anthropic API クレジット不要）。
"""

from dataclasses import dataclass, field
from typing import Optional

from .claude_cli import run_claude
from .config import config
from .brave_search_client import BraveSearchClient, SearchHit


@dataclass
class ResearchResult:
    """調査結果"""
    code: str
    name: str
    change_rate: float
    summary: str                      # 変動理由の要約（Markdown）
    sources: list[str] = field(default_factory=list)


_ANALYSIS_PROMPT_TEMPLATE = """あなたは東証上場企業の株価変動を調査する金融アナリストです。
以下の検索結果をもとに、指定された銘柄の株価変動理由を分析してください。

## 銘柄情報
- 証券コード: {code}
- 銘柄名: {name}
- 前日比: {change_rate:+.1f}%（{direction}）
- 対象日: {date}

## 検索結果（情報源）

{search_context}

---

上記の検索結果をもとに、以下の形式でMarkdownを出力してください。

## 銘柄名
（正式な銘柄名を記載）

## 変動理由サマリー
（200字程度で変動理由を簡潔にまとめる）

## 詳細分析

### 公式IR・開示情報
（決算、業績予想修正、M&A、自社株買い等のIR情報）

### ニュース・報道
（日経、Bloomberg、Reuters等のニュース）

### 市場・SNSの反応
（Yahoo!ファイナンス掲示板、X（旧Twitter）等の反応）

### セクター・マクロ要因
（同業他社や市場全体の動向）

情報が見つからない項目は「情報なし」と記載してください。"""


def _format_hits(label: str, query: str, hits: list[SearchHit], error: Optional[str]) -> str:
    lines = [f"### {label}（クエリ: {query}）", ""]
    if error:
        lines.append(f"**検索エラー**: {error}")
    elif not hits:
        lines.append("（検索結果なし）")
    else:
        for i, h in enumerate(hits, 1):
            lines.append(f"{i}. **{h.title}**")
            lines.append(f"   - URL: {h.link}")
            if h.snippet:
                lines.append(f"   - 抜粋: {h.snippet}")
            lines.append("")
    return "\n".join(lines)


class WebResearcher:
    """Google CSE で検索し、Claude CLI で株価変動理由を分析する"""

    def __init__(self):
        self._cse = BraveSearchClient()

    def research(
        self,
        code: str,
        change_rate: float,
        target_date: Optional[str] = None,
        name: Optional[str] = None,
    ) -> ResearchResult:
        """
        単一銘柄の変動理由を Google 検索 + Claude で調査する。
        1銘柄あたり Google CSE を 2 回呼び出す（無料枠：100回/日）。
        """
        display_name = name or code
        direction = "上昇" if change_rate > 0 else "下落"
        date_str = target_date or "直近"

        # 2クエリで検索
        q_reason = f"{display_name} 株価 {direction} 理由 {target_date or ''}".strip()
        q_ir = f"{display_name} IR ニュース"

        hits1, err1 = self._cse.search(q_reason, num=8)
        hits2, err2 = self._cse.search(q_ir, num=8)

        # 参照 URL 収集（重複除去）
        sources = list(dict.fromkeys(
            [h.link for h in hits1 if h.link] + [h.link for h in hits2 if h.link]
        ))

        # 検索結果をテキストコンテキストに整形
        search_context = "\n\n".join([
            _format_hits("株価変動の理由・日付", q_reason, hits1, err1),
            _format_hits("IR・ニュース", q_ir, hits2, err2),
        ])

        prompt = _ANALYSIS_PROMPT_TEMPLATE.format(
            code=code,
            name=display_name,
            change_rate=change_rate,
            direction=direction,
            date=date_str,
            search_context=search_context,
        )

        summary = run_claude(prompt)

        # 銘柄名を本文から抽出（## 銘柄名 セクション直後の非空行）
        detected_name = display_name
        lines = summary.splitlines()
        for i, line in enumerate(lines):
            if "銘柄名" in line and line.startswith("#"):
                for j in range(i + 1, min(i + 4, len(lines))):
                    candidate = lines[j].strip().lstrip("#").strip()
                    if candidate and candidate != "（正式な銘柄名を記載）":
                        detected_name = candidate
                        break
                break

        return ResearchResult(
            code=code,
            name=detected_name,
            change_rate=change_rate,
            summary=summary,
            sources=sources,
        )
