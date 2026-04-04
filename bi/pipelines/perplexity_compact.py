"""
Perplexity レポート MD を圧縮する。

戦略:
  - References セクションを全削除（脚注は情報密度がゼロ）
  - 脚注参照 [^n] をテキストから除去
  - セクションごとに文字数上限を設けてトリム

使い方:
  python perplexity_compact.py --code 7256

対象ファイル: research/stocks/{code}_*_perplexity_*.md（複数あれば全て処理）
出力: research/stocks/{code}_{date}_perplexity_compact.md（1ファイルに統合）
"""

from __future__ import annotations

import argparse
import re
from datetime import date
from pathlib import Path

from jq_client_utils import normalize_code_4

OUTPUT_DIR = Path("../../research/stocks")

# セクション見出し（## レベル）ごとの最大文字数
# 重要度が低いセクションは短く
_SECTION_LIMITS: dict[str, int] = {
    "references": 0,          # 完全スキップ
    "類似": 800,               # 類似事例は短く
    "top3": 600,               # 他銘柄推薦は最小限
}
_DEFAULT_LIMIT = 1_500        # デフォルト上限（約300〜400トークン相当）

# 脚注参照パターン [^1] [^2] など
_FOOTNOTE_REF = re.compile(r"\[\^[\w\d]+\]")

# ## 見出しパターン
_H2_PATTERN = re.compile(r"^## ")


def _get_limit(heading: str) -> int:
    lower = heading.lower()
    for key, limit in _SECTION_LIMITS.items():
        if key in lower:
            return limit
    return _DEFAULT_LIMIT


def _clean_line(line: str) -> str:
    return _FOOTNOTE_REF.sub("", line)


def compact_perplexity_md(src: Path) -> list[str]:
    text = src.read_text(encoding="utf-8")
    lines = text.splitlines()

    output: list[str] = []
    section_buf: list[str] = []
    current_heading = ""
    current_limit = _DEFAULT_LIMIT

    def flush():
        nonlocal section_buf, current_heading, current_limit
        if current_limit == 0:
            # スキップ対象セクション
            section_buf = []
            current_heading = ""
            return
        if current_heading:
            # セクション内容を char 上限でトリム
            body = "\n".join(_clean_line(l) for l in section_buf).strip()
            if len(body) > current_limit:
                body = body[:current_limit].rsplit("\n", 1)[0] + "\n…(省略)"
            output.append(current_heading)
            output.append("")
            if body:
                output.append(body)
            output.append("")
        section_buf = []
        current_heading = ""
        current_limit = _DEFAULT_LIMIT

    for line in lines:
        if _H2_PATTERN.match(line):
            flush()
            current_heading = _clean_line(line).rstrip()
            current_limit = _get_limit(line)
        else:
            section_buf.append(line)

    flush()

    # ファイル先頭（## より前のブロック: タイトル・要確認など）は別途処理済み
    return output


def find_perplexity_files(code: str) -> list[Path]:
    files = sorted(
        f for f in OUTPUT_DIR.glob(f"{code}_*_perplexity_*.md")
        if not f.stem.endswith("_compact")
    )
    if not files:
        raise FileNotFoundError(f"{code}_*_perplexity_*.md が {OUTPUT_DIR} に見つかりません。")
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description="Perplexity レポート MD を圧縮して統合")
    parser.add_argument("--code", required=True, help="証券コード（4桁）例: 7256")
    args = parser.parse_args()

    code = normalize_code_4(args.code)
    files = find_perplexity_files(code)
    print(f"対象ファイル: {len(files)} 件")

    all_lines: list[str] = []
    for f in files:
        size_kb = f.stat().st_size / 1024
        print(f"  処理中: {f.name}  ({size_kb:.1f} KB)")
        all_lines.append(f"# ═══ {f.stem} ═══")
        all_lines.append("")
        all_lines.extend(compact_perplexity_md(f))
        all_lines.append("")

    today_str = date.today().strftime("%Y-%m-%d")
    out_path = OUTPUT_DIR / f"{code}_{today_str}_perplexity_compact.md"
    out_path.write_text("\n".join(all_lines), encoding="utf-8")

    orig_total_kb = sum(f.stat().st_size for f in files) / 1024
    comp_kb = out_path.stat().st_size / 1024
    ratio = comp_kb / orig_total_kb * 100
    print(f"保存完了: {out_path}")
    print(f"  圧縮: {orig_total_kb:.1f} KB → {comp_kb:.1f} KB ({ratio:.0f}%)")


if __name__ == "__main__":
    main()
