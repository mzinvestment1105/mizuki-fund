"""
Deep Dive 用の圧縮済みファイルを 1 つの Markdown に統合する。

実行すると自動で以下のスクリプトも呼び出す:
  1. extract_stock_snapshot.py → snapshot.yaml
  2. deep_dive_compact.py     → data_compact.md
  3. perplexity_compact.py    → perplexity_compact.md

最後にそれらを結合した {code}_{date}_assembled.md を出力する。

使い方:
  python assemble_prompt.py --code 7256

  # 圧縮ステップをスキップして既存ファイルを使う場合:
  python assemble_prompt.py --code 7256 --no-rerun
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

from jq_client_utils import normalize_code_4

OUTPUT_DIR = Path("../../research/stocks")
HERE = Path(__file__).parent


def _run(script: str, args: list[str]) -> None:
    cmd = [sys.executable, str(HERE / script)] + args
    print(f"  -> {script} {' '.join(args)}")
    result = subprocess.run(cmd, capture_output=True)
    # デコードは両エンコーディングでフォールバック
    for enc in ("utf-8", "cp932", "latin-1"):
        try:
            stdout = result.stdout.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        stdout = result.stdout.decode("latin-1")
    if stdout.strip():
        for line in stdout.strip().splitlines():
            print(f"     {line.encode('utf-8', errors='replace').decode('utf-8', errors='replace')}")
    if result.returncode != 0:
        raise RuntimeError(f"{script} が失敗しました（exit {result.returncode}）")


def find_latest(pattern: str) -> Path | None:
    candidates = sorted(OUTPUT_DIR.glob(pattern), reverse=True)
    return candidates[0] if candidates else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Deep Dive 用圧縮ファイルを統合して assembled.md を生成")
    parser.add_argument("--code", required=True, help="証券コード（4桁）例: 7256")
    parser.add_argument("--no-rerun", action="store_true", help="圧縮スクリプトを再実行せず既存ファイルを使う")
    args = parser.parse_args()

    code = normalize_code_4(args.code)
    today_str = date.today().strftime("%Y-%m-%d")

    if not args.no_rerun:
        print("[1/3] スナップショット生成...")
        _run("extract_stock_snapshot.py", ["--code", code])

        print("[2/3] EDINET data.md 圧縮...")
        _run("deep_dive_compact.py", ["--code", code])

        print("[3/3] Perplexity MD 圧縮・統合...")
        _run("perplexity_compact.py", ["--code", code])

    # 各ファイルを収集
    snapshot_path = OUTPUT_DIR / f"{code}_snapshot.yaml"
    data_compact_path = find_latest(f"{code}_*_data_compact.md")
    perplexity_compact_path = find_latest(f"{code}_*_perplexity_compact.md")

    missing = []
    if not snapshot_path.exists():
        missing.append(str(snapshot_path))
    if data_compact_path is None:
        missing.append(f"{code}_*_data_compact.md")
    if perplexity_compact_path is None:
        missing.append(f"{code}_*_perplexity_compact.md")
    if missing:
        raise FileNotFoundError(f"以下のファイルが見つかりません: {missing}")

    # --- 統合 ---
    parts: list[str] = []

    parts.append(f"# {code} Deep Dive アセンブル済みデータ（{today_str}）")
    parts.append("")
    parts.append("> このファイルは assemble_prompt.py が自動生成したものです。")
    parts.append("> 以下のデータを基に Deep Dive レポートを作成してください。")
    parts.append("")
    parts.append("---")
    parts.append("")

    # 1. スナップショット
    parts.append("## ■ ETLスナップショット（screening_master）")
    parts.append("")
    parts.append("```yaml")
    parts.append(snapshot_path.read_text(encoding="utf-8").strip())
    parts.append("```")
    parts.append("")
    parts.append("---")
    parts.append("")

    # 2. EDINET 圧縮データ
    parts.append("## ■ EDINETデータ（有報・圧縮済み）")
    parts.append("")
    parts.append(data_compact_path.read_text(encoding="utf-8").strip())
    parts.append("")
    parts.append("---")
    parts.append("")

    # 3. Perplexity 圧縮レポート
    parts.append("## ■ Perplexityリサーチ（圧縮済み）")
    parts.append("")
    parts.append(perplexity_compact_path.read_text(encoding="utf-8").strip())
    parts.append("")

    assembled = "\n".join(parts)

    out_path = OUTPUT_DIR / f"{code}_{today_str}_assembled.md"
    out_path.write_text(assembled, encoding="utf-8")

    # サイズ表示
    total_input_kb = (
        snapshot_path.stat().st_size
        + data_compact_path.stat().st_size
        + perplexity_compact_path.stat().st_size
    ) / 1024
    out_kb = out_path.stat().st_size / 1024

    print(f"\n[OK] 保存完了: {out_path}")
    print(f"  合計インプットサイズ: {total_input_kb:.1f} KB")
    print(f"  assembled.md:        {out_kb:.1f} KB")
    print(f"\n-> このファイルを Claude Code に渡して Deep Dive レポートを依頼してください。")


if __name__ == "__main__":
    main()
