from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _as_float(v: object) -> float:
    x = pd.to_numeric(v, errors="coerce")
    if pd.isna(x):
        return float("nan")
    return float(x)


def main() -> None:
    p = argparse.ArgumentParser(description="screening_master の最小健全性チェック")
    p.add_argument(
        "--input",
        type=Path,
        default=Path("..") / "outputs" / "screening_master.parquet",
        help="検証対象 parquet",
    )
    args = p.parse_args()

    if not args.input.exists():
        raise SystemExit(f"[NG] parquet not found: {args.input}")

    df = pd.read_parquet(args.input)
    if df.empty:
        raise SystemExit("[NG] output is empty")
    if "Code" not in df.columns:
        raise SystemExit("[NG] missing Code column")

    required_meta = ["ETLRunId", "ETLStartedAtUTC", "ETLStartedAtJST"]
    for c in required_meta:
        if c not in df.columns:
            raise SystemExit(f"[NG] missing metadata column: {c}")
        if df[c].astype(str).str.strip().eq("").all():
            raise SystemExit(f"[NG] metadata column is all empty: {c}")

    # 監視銘柄の回帰チェック（ズレ再発を止める）
    checks = [
        ("130A", "NetSales_NextYear_Forecast", 113_000_000.0),
        ("1332", "Profit_LatestYear_Actual", 25_381_000_000.0),
    ]
    code_s = df["Code"].astype(str).str[:4]
    for code, col, expected in checks:
        if col not in df.columns:
            raise SystemExit(f"[NG] missing column: {col}")
        sub = df.loc[code_s.eq(code), col]
        if sub.empty:
            raise SystemExit(f"[NG] code not found: {code}")
        got = _as_float(sub.iloc[0])
        if pd.isna(got):
            raise SystemExit(f"[NG] {code} {col} is NaN")
        if abs(got - expected) > 0.5:
            raise SystemExit(
                f"[NG] {code} {col} mismatch got={got:.0f} expected={expected:.0f}"
            )

    print(f"[OK] validate_screening_master rows={len(df)} path={args.input}")


if __name__ == "__main__":
    main()
