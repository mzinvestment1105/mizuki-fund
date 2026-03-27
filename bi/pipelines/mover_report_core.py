"""
Web 検索（Tavily）＋ LLM（OpenAI）で銘柄ごとの「材料」レポート用コンテキスト生成。
"""

from __future__ import annotations

import base64
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import requests

from jq_client_utils import normalize_code_4

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # type: ignore[misc, assignment]

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_a: Any, **_k: Any) -> bool:
        return False


def _maybe_load_dotenv() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")


def tavily_search(query: str, *, api_key: str, max_results: int = 5) -> list[dict[str, str]]:
    r = requests.post(
        "https://api.tavily.com/search",
        json={
            "api_key": api_key,
            "query": query,
            "search_depth": "basic",
            "max_results": max_results,
            "include_answer": False,
        },
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    out: list[dict[str, str]] = []
    for it in data.get("results") or []:
        if not isinstance(it, dict):
            continue
        out.append(
            {
                "title": str(it.get("title") or ""),
                "url": str(it.get("url") or ""),
                "content": str(it.get("content") or it.get("snippet") or ""),
            }
        )
    return out


def gather_search_context(
    *,
    company_name: str,
    code: str,
    as_of: date,
    tavily_api_key: str,
    queries_extra: list[str] | None = None,
) -> str:
    name = (company_name or "").strip() or f"銘柄{code}"
    queries = [
        f"{name} {code} 株価 材料 {as_of:%Y年%m月%d日}",
        f"{name} 適時開示 IR",
        f"{name} 株 {as_of:%Y-%m-%d} ニュース",
    ]
    if queries_extra:
        queries.extend(queries_extra)

    chunks: list[str] = []
    seen_url: set[str] = set()
    for q in queries:
        try:
            hits = tavily_search(q, api_key=tavily_api_key, max_results=4)
        except requests.RequestException as e:
            chunks.append(f"（検索エラー: {q!r} → {e}）\n")
            continue
        for h in hits:
            u = h["url"]
            if u in seen_url:
                continue
            seen_url.add(u)
            chunks.append(f"### {h['title']}\nURL: {u}\n{h['content'][:1200]}\n")

    if not chunks:
        return "（検索結果が取得できませんでした。APIキー・ネットワーク・クエリを確認してください。）"
    return "\n".join(chunks)


SYSTEM_PROMPT = """あなたは日本株のリサーチアシスタントです。
与えられた検索スニペットのみを根拠に、読み手が非専門家でも理解できる日本語で短くまとめます。

必ず守ること:
- 検索スニペットに無いことは「不明」「推測」と明記し、断定しない。
- 掲示板・SNS・噂の可能性がある情報は、その不確実性を明記する。
- 各セクション末尾に「参考URL」を箇条書きで列挙する（スニペットに含まれる URL のみ）。
"""


def llm_mover_section(
    *,
    company_name: str,
    code: str,
    as_of: date,
    search_context: str,
    model: str,
    extra_user_hint: str = "",
) -> str:
    if OpenAI is None:
        raise RuntimeError("openai パッケージがインストールされていません。")
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY が未設定です。")

    client = OpenAI(api_key=api_key)
    name = (company_name or "").strip() or f"銘柄{code}"
    user = f"""対象日: {as_of.isoformat()}
銘柄コード: {code}
銘柄名: {name}

以下は Web 検索で得たスニペットです（出典は URL 行を参照）。

---
{search_context}
---
"""
    if extra_user_hint.strip():
        user += f"\n（画面・画像から読み取った補足）\n{extra_user_hint.strip()}\n"

    user += """
以下の Markdown 見出し構成で出力してください（見出しレベルは ## と ### のみ）:

## （銘柄名）(コード)

### 当日の株価動きの整理
- 前日比・方向は入力データに基づき1〜2文（検索に無ければ「不明」）

### 考えられる要因（仮説）
- 箇条書き 2〜5 個。各項目に根拠の強さ（強/中/弱）を付ける

### 不確実性・注意
- 取りこぼし、遅延、誤報の可能性

### 参考URL
- 箇条書き（タイトル — URL）
"""

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        temperature=0.3,
    )
    choice = resp.choices[0].message.content
    return (choice or "").strip()


def build_markdown_report(
    rows: list[dict[str, Any]],
    *,
    as_of: date,
    tavily_api_key: str,
    model: str,
    extra_hints: dict[str, str] | None = None,
) -> str:
    """
    rows: {"Code", "CompanyName"?, "return"?, "direction"?, "_vision_notes"?} のリスト
    extra_hints: code4 -> 画像等からの補足テキスト（行ごとの _vision_notes より優先されない。両方渡す場合はマージ）
    """
    extra_hints = dict(extra_hints or {})
    parts: list[str] = [
        f"# 値幅銘柄 材料メモ ({as_of.isoformat()})\n",
        "※ 自動生成・検索スニペットベース。投資判断は自己責任で行ってください。\n",
    ]
    for row in rows:
        code = normalize_code_4(row.get("Code", ""))
        if not code:
            continue
        name = str(row.get("CompanyName", "") or "")
        ret = row.get("return")
        direction = str(row.get("direction", "") or "")
        scanner_hint = ""
        if ret is not None:
            try:
                scanner_hint = f"スキャナー算出の前日比: {float(ret) * 100:.2f}% 方向: {direction}\n"
            except (TypeError, ValueError):
                scanner_hint = ""

        vision_notes = str(row.get("_vision_notes") or "").strip()
        merged_extra = "\n".join(
            x
            for x in (extra_hints.get(code, "").strip(), vision_notes)
            if x
        )

        ctx = gather_search_context(
            company_name=name,
            code=code,
            as_of=as_of,
            tavily_api_key=tavily_api_key,
        )
        if scanner_hint:
            ctx = scanner_hint + "\n" + ctx

        section = llm_mover_section(
            company_name=name,
            code=code,
            as_of=as_of,
            search_context=ctx,
            model=model,
            extra_user_hint=merged_extra,
        )
        parts.append("\n---\n\n")
        parts.append(section)
        parts.append("\n")
    return "".join(parts)


def vision_extract_screenshot(
    image_bytes: bytes,
    *,
    mime: str,
    model: str,
) -> dict[str, Any]:
    """画像から銘柄・文脈を JSON で抽出する。"""
    if OpenAI is None:
        raise RuntimeError("openai パッケージがインストールされていません。")
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY が未設定です。")

    b64 = base64.standard_b64encode(image_bytes).decode("ascii")
    url = f"data:{mime};base64,{b64}"

    client = OpenAI(api_key=api_key)
    schema_hint = """{
  "items": [ {"code": "4桁または省略", "name": "銘柄名", "notes": "画面上の短いメモ"} ],
  "visible_date_hint": "YYYY-MM-DD または null",
  "headline_hint": "見出しがあれば"
}"""
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "この画像は株式アプリ・ニュース・掲示板のスクリーンショットの可能性があります。"
                            "日本上場株について、読み取れる銘柄コード（4桁）と銘柄名、日付、見出しを抽出してください。"
                            "推測は notes に区別して書いてください。JSON のみを返答し、前後に説明文を付けないでください。\n"
                            f"スキーマ例: {schema_hint}"
                        ),
                    },
                    {"type": "image_url", "image_url": {"url": url}},
                ],
            }
        ],
        temperature=0.1,
    )
    raw = (resp.choices[0].message.content or "").strip()
    raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Vision の応答を JSON として解釈できません: {e}\n---\n{raw[:800]}") from e


def vision_result_to_rows(vision: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    """vision_extract_screenshot の戻りから movers 行と as_of ヒント文字列。"""
    items = vision.get("items") or []
    rows: list[dict[str, Any]] = []
    if isinstance(items, list):
        for it in items:
            if not isinstance(it, dict):
                continue
            code = normalize_code_4(str(it.get("code") or ""))
            if len(code) != 4 or not code.isdigit():
                continue
            name = str(it.get("name") or "").strip()
            notes = str(it.get("notes") or "").strip()
            row: dict[str, Any] = {"Code": code, "CompanyName": name}
            if notes:
                row["_vision_notes"] = notes
            rows.append(row)

    hint = vision.get("visible_date_hint")
    hint_s = str(hint).strip() if hint else ""
    headline = str(vision.get("headline_hint") or "").strip()
    if headline:
        for row in rows:
            prev = str(row.get("_vision_notes") or "").strip()
            row["_vision_notes"] = "\n".join(
                x for x in (f"画面上の見出し: {headline}", prev) if x
            )
    return rows, hint_s


def parse_as_of_date(s: str | None, fallback: date) -> date:
    if not s or not str(s).strip():
        return fallback
    try:
        return date.fromisoformat(str(s).strip()[:10])
    except ValueError:
        return fallback
