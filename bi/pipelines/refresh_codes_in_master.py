from __future__ import annotations

import argparse
import os
from pathlib import Path

import jquantsapi
import pandas as pd

from jq_client_utils import fetch_paginated_v2
from update_statements import STATEMENT_NUMERIC_COLS, aggregate_fins_summary_df, fins_summary_code_variants


def _apply_trailing_valuation_ratios(df: pd.DataFrame) -> None:
    """make_screening_master_v2 と同じ式で PER / PBR / ROE を上書き（列があれば）。"""
    need = ("MarketCap", "Profit_LatestYear_Actual", "Equity_LatestFY")
    if not all(c in df.columns for c in need):
        return
    _mc_val = pd.to_numeric(df["MarketCap"], errors="coerce")
    _np_lt = pd.to_numeric(df["Profit_LatestYear_Actual"], errors="coerce")
    _eq_lt = pd.to_numeric(df["Equity_LatestFY"], errors="coerce")
    _ok_per = _np_lt.notna() & (_np_lt > 0) & _mc_val.notna()
    _ok_pbr = _eq_lt.notna() & (_eq_lt > 0) & _mc_val.notna()
    _ok_roe = _eq_lt.notna() & (_eq_lt != 0) & _np_lt.notna()
    df["PER_Trailing"] = (_mc_val / _np_lt).where(_ok_per)
    df["PBR_Trailing"] = (_mc_val / _eq_lt).where(_ok_pbr)
    df["ROE_LatestYear"] = (_np_lt / _eq_lt).where(_ok_roe)


def _fetch_rows_for_code(client: jquantsapi.ClientV2, code4: str) -> list[dict]:
    for code_try in fins_summary_code_variants(code4):
        rows = fetch_paginated_v2(client, "/fins/summary", params={"code": code_try}, sleep_seconds=1.2)
        if rows:
            return rows
    return []


def main() -> None:
    p = argparse.ArgumentParser(description="screening_master の指定銘柄だけ財務列を再計算して上書き")
    p.add_argument("--codes", nargs="+", required=True, help="例: --codes 130A 1332")
    p.add_argument(
        "--master",
        default=str(Path("..") / "outputs" / "screening_master.parquet"),
        help="更新対象 parquet パス",
    )
    args = p.parse_args()

    key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not key:
        raise ValueError("JQUANTS_API_KEY が未設定です。")

    master_path = Path(args.master)
    if not master_path.exists():
        raise FileNotFoundError(f"master parquet が見つかりません: {master_path}")

    codes = [str(c).strip()[:4] for c in args.codes if str(c).strip()]
    if not codes:
        raise ValueError("codes が空です。")

    client = jquantsapi.ClientV2(api_key=key)
    master = pd.read_parquet(master_path)
    if "Code" not in master.columns:
        raise ValueError("master parquet に Code 列がありません。")
    master = master.copy()
    master["Code"] = master["Code"].astype(str).str[:4]

    updated = 0
    for code4 in codes:
        rows = _fetch_rows_for_code(client, code4)
        if not rows:
            print(f"[skip] {code4}: /fins/summary 0 rows")
            continue
        fin_df = pd.DataFrame.from_records(rows)
        ser, err = aggregate_fins_summary_df(fin_df)
        if err is not None or ser is None:
            print(f"[skip] {code4}: {err or 'aggregate failed'}")
            continue

        mask = master["Code"].eq(code4)
        if not mask.any():
            print(f"[skip] {code4}: master にコードがありません")
            continue

        for col in STATEMENT_NUMERIC_COLS:
            if col in master.columns:
                master.loc[mask, col] = ser.get(col, pd.NA)
        updated += 1
        print(f"[ok] {code4} updated")

    _apply_trailing_valuation_ratios(master)

    tmp = master_path.with_suffix(master_path.suffix + ".tmp")
    master.to_parquet(tmp, index=False)
    tmp.replace(master_path)
    print(f"saved: {master_path} updated_codes={updated}/{len(codes)}")


if __name__ == "__main__":
    main()
