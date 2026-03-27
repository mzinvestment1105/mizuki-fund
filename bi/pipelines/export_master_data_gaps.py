"""
既存の screening_master*.parquet から、財務主要指標がすべて欠損している銘柄だけを抽出する。

例:
  python export_master_data_gaps.py
  python export_master_data_gaps.py --input data/processed/screening_master_limit100.parquet --excel
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from update_statements import CRITICAL_COLS

DEFAULT_INPUT = Path("data") / "processed" / "screening_master.parquet"


def main() -> None:
    p = argparse.ArgumentParser(description="master parquet から財務クリティカル全欠損銘柄を抽出")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="入力 screening_master*.parquet")
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="出力 parquet（省略時は入力名に _data_gaps を付与）",
    )
    p.add_argument("--excel", action="store_true", help=".xlsx も出力")
    args = p.parse_args()

    inp = args.input
    if not inp.exists():
        raise SystemExit(f"入力がありません: {inp}")

    master = pd.read_parquet(inp)
    crit = list(CRITICAL_COLS)
    for c in crit:
        if c not in master.columns:
            master[c] = pd.NA

    mask = master[crit].isna().all(axis=1)
    gaps = master.loc[mask].copy()
    if len(gaps):
        gaps["gap_reason"] = "from_parquet_critical_all_na"

    outp = args.output
    if outp is None:
        outp = inp.with_name(f"{inp.stem}_data_gaps.parquet")

    outp.parent.mkdir(parents=True, exist_ok=True)
    gaps.to_parquet(outp, index=False)
    print(f"wrote {len(gaps)} rows -> {outp}")

    if args.excel:
        try:
            x = outp.with_suffix(".xlsx")
            gaps.to_excel(x, index=False)
            print(f"excel: {x}")
        except ImportError:
            raise SystemExit("Excel 出力には openpyxl が必要です: pip install openpyxl") from None


if __name__ == "__main__":
    main()
