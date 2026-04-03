"""
EDINET ZIP 内の PDF から有報の主要セクション（事業内容・リスク・MD&A・セグメント）を抽出。
pdfminer.six を使用（日本語 PDF に強い）。
"""

from __future__ import annotations

import re
import zipfile
from io import BytesIO, StringIO

try:
    from pdfminer.high_level import extract_text_to_fp
    from pdfminer.layout import LAParams
    _PDFMINER_AVAILABLE = True
except ImportError:
    _PDFMINER_AVAILABLE = False


# 有報のセクション見出しパターン（正規表現）
# 各タプル: (section_key, header_pattern)
_SECTION_HEADERS: list[tuple[str, str]] = [
    ("business_overview",  r"第1\s*[　 ]*企業の概況|事業の概要"),
    ("business_detail",    r"第2\s*[　 ]*事業の状況|主要な事業内容"),
    ("risk_factors",       r"事業等のリスク"),
    ("mda",                r"経営者による財政状態.*?の状況の分析|MD&A"),
    ("segment",            r"セグメント情報|セグメントの概要|報告セグメント"),
    ("shareholder",        r"大株主の状況|株式の状況"),
]

# セクション最大文字数（コスト節約のため上限を設ける）
_SECTION_MAX_CHARS = 8_000


def _find_pdf_in_zip(zip_bytes: BytesIO) -> bytes | None:
    """ZIP から最大の PDF ファイルを返す（有報本文）。"""
    zip_bytes.seek(0)
    with zipfile.ZipFile(zip_bytes) as zf:
        pdf_files = [n for n in zf.namelist() if n.lower().endswith(".pdf")]
        if not pdf_files:
            return None
        pdf_files.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
        return zf.read(pdf_files[0])


def _extract_full_text(pdf_bytes: bytes) -> str:
    """PDF バイト列から全テキストを抽出する。"""
    if not _PDFMINER_AVAILABLE:
        raise ImportError("pdfminer.six が必要です: pip install pdfminer.six")
    out = StringIO()
    extract_text_to_fp(
        BytesIO(pdf_bytes),
        out,
        laparams=LAParams(line_margin=0.3),
        output_type="text",
        codec="utf-8",
    )
    return out.getvalue()


def _split_sections(text: str) -> dict[str, str]:
    """
    全テキストからセクションヘッダーを検出し、各セクションのテキストを返す。
    見つからないセクションは空文字。
    """
    result: dict[str, str] = {key: "" for key, _ in _SECTION_HEADERS}

    # 各ヘッダーの位置を検索
    positions: list[tuple[int, str, int]] = []  # (pos, key, header_end)
    for key, pattern in _SECTION_HEADERS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            positions.append((m.start(), key, m.end()))

    positions.sort(key=lambda x: x[0])

    for i, (start, key, header_end) in enumerate(positions):
        # 次のセクションの開始位置まで（または上限まで）
        end = positions[i + 1][0] if i + 1 < len(positions) else len(text)
        section_text = text[header_end:end].strip()
        result[key] = section_text[:_SECTION_MAX_CHARS]

    return result


def extract_sections_from_zip(zip_bytes: BytesIO) -> dict[str, str]:
    """
    EDINET PDF ZIP を受け取り、有報の主要セクションテキストを dict で返す。
    キー: business_overview, business_detail, risk_factors, mda, segment, shareholder
    """
    pdf_bytes = _find_pdf_in_zip(zip_bytes)
    if pdf_bytes is None:
        return {"error": "PDF not found in ZIP"}

    try:
        full_text = _extract_full_text(pdf_bytes)
    except Exception as e:
        return {"error": f"PDF extraction failed: {e}"}

    sections = _split_sections(full_text)
    found = [k for k, v in sections.items() if v]
    print(f"PDF sections extracted: {found}")
    return sections


def extract_sections_from_bytes(data: BytesIO) -> dict[str, str]:
    """
    EDINET type=2 の直接 PDF バイト列（ZIP でない）から主要セクションを抽出する。
    キー: business_overview, business_detail, risk_factors, mda, segment, shareholder
    """
    data.seek(0)
    pdf_bytes = data.read()

    try:
        full_text = _extract_full_text(pdf_bytes)
    except Exception as e:
        return {"error": f"PDF extraction failed: {e}"}

    sections = _split_sections(full_text)
    found = [k for k, v in sections.items() if v]
    print(f"PDF sections extracted: {found}")
    return sections
