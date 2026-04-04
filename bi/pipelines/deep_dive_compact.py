"""
EDINET deep_dive.py が出力した {code}_{date}_data.md を圧縮する。

財務テーブル（XBRL）はそのまま保持。
PDFセクションは「数値・箇条書き・重要語を含む行」だけ残してトリミングする。

使い方:
  python deep_dive_compact.py --code 7256

出力: research/stocks/{code}_{date}_data_compact.md
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from jq_client_utils import normalize_code_4

OUTPUT_DIR = Path("../../research/stocks")

# --- 重要行の判定ルール ---

# 数値を含む正規表現（億, 百万, 万円, %, pt, 倍, 件, 人, 年, 月, etc.）
_NUM_PATTERN = re.compile(
    r"[\d,\.]+\s*(?:%|％|億|百万|万円|円|倍|件|人|pt|ポイント|年度|年|月|期|回|社|店)"
)

# 箇条書き行（日本語・英語）
_BULLET_PATTERN = re.compile(
    r"^[\s　]*[・●■□▶▷◆◇①②③④⑤⑥⑦⑧⑨⑩\-\*\+]\s*\S"
)

# 重要キーワード（財務・ビジネス用語）
_KEY_TERMS = re.compile(
    r"売上|利益|損失|赤字|黒字|改善|計画|目標|リスク|戦略|主要|セグメント|"
    r"海外|国内|シェア|競合|コスト削減|構造改革|増収|減収|増益|減益|"
    r"配当|自己株|M&A|上方修正|下方修正|通期|中計|KTA|トヨタ|ホンダ"
)

# Markdown 見出し
_HEADING_PATTERN = re.compile(r"^#{1,4}\s")

# 表の行（| で始まる）
_TABLE_PATTERN = re.compile(r"^\|")

# セクション内で保持する最大行数
_MAX_LINES_PER_SECTION = 60


def _is_important_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if _HEADING_PATTERN.match(stripped):
        return True
    if _TABLE_PATTERN.match(stripped):
        return True
    if _BULLET_PATTERN.match(line):
        return True
    if _NUM_PATTERN.search(stripped):
        return True
    if _KEY_TERMS.search(stripped):
        return True
    return False


def _compact_section(lines: list[str]) -> list[str]:
    """セクション内の行を重要行だけに絞る（最大 _MAX_LINES_PER_SECTION 行）。"""
    result = []
    prev_blank = False
    for line in lines:
        if not line.strip():
            if not prev_blank and result:
                result.append("")
            prev_blank = True
            continue
        prev_blank = False
        if _is_important_line(line):
            result.append(line.rstrip())
    return result[:_MAX_LINES_PER_SECTION]


def compact_data_md(src: Path) -> str:
    text = src.read_text(encoding="utf-8")
    lines = text.splitlines()

    output: list[str] = []
    in_pdf_section = False
    section_buf: list[str] = []
    section_header = ""

    def flush_section():
        nonlocal section_buf, section_header
        if section_header:
            output.append(section_header)
            output.append("")
            compacted = _compact_section(section_buf)
            output.extend(compacted)
            output.append("")
        section_buf = []
        section_header = ""

    # 財務テーブルブロックの終わりを示すセクション見出し（PDFセクション開始）
    PDF_SECTION_START = re.compile(r"^## (事業の概要|事業の状況|事業等のリスク|MD&A|セグメント情報|大株主の状況)")

    for line in lines:
        if PDF_SECTION_START.match(line):
            # 前のセクションをフラッシュ
            flush_section()
            in_pdf_section = True
            section_header = line
            continue

        if in_pdf_section:
            # 次の ## 見出し（別PDFセクション）が来たらフラッシュ
            if re.match(r"^## ", line) and not PDF_SECTION_START.match(line):
                flush_section()
                in_pdf_section = False
                output.append(line)
            else:
                section_buf.append(line)
        else:
            output.append(line)

    flush_section()  # 最後のセクション

    return "\n".join(output)


def find_latest_data_md(code: str) -> Path:
    candidates = sorted(
        OUTPUT_DIR.glob(f"{code}_*_data.md"),
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"{code}_*_data.md が {OUTPUT_DIR} に見つかりません。")
    return candidates[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="deep_dive data.md を圧縮して compact 版を生成")
    parser.add_argument("--code", required=True, help="証券コード（4桁）例: 7256")
    parser.add_argument("--input", default=None, help="入力ファイルパスを直接指定（省略時は自動検索）")
    args = parser.parse_args()

    code = normalize_code_4(args.code)
    src = Path(args.input) if args.input else find_latest_data_md(code)
    print(f"入力: {src}  ({src.stat().st_size / 1024:.1f} KB)")

    compacted = compact_data_md(src)

    # 出力ファイル名: 入力と同じ日付部分を引き継ぐ
    out_name = src.stem + "_compact.md"
    out_path = src.parent / out_name
    out_path.write_text(compacted, encoding="utf-8")

    orig_kb = src.stat().st_size / 1024
    comp_kb = out_path.stat().st_size / 1024
    ratio = comp_kb / orig_kb * 100
    print(f"保存完了: {out_path}")
    print(f"  圧縮: {orig_kb:.1f} KB → {comp_kb:.1f} KB ({ratio:.0f}%)")


if __name__ == "__main__":
    main()
