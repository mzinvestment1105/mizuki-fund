"""
Debug helper: fetch J-Quants /fins/summary for a few codes and print
the aggregated statement outputs (no full ETL).

Usage (PowerShell):
  cd bi/pipelines
  python .\\debug_fins_pick.py --codes 1332 1301 130A 7203 7256
"""

from __future__ import annotations

import argparse
import os
import sys

import jquantsapi
import pandas as pd

from jq_client_utils import fetch_paginated_v2
from update_statements import aggregate_fins_summary_df, fins_summary_code_variants


def _require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"{name} is not set")
    return v


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--codes", nargs="+", required=True, help="e.g. 1332 130A 7203")
    args = p.parse_args()

    key = _require_env("JQUANTS_API_KEY")
    client = jquantsapi.ClientV2(key)

    rows_out: list[dict] = []
    for code4 in args.codes:
        code4 = str(code4).strip()
        used = ""
        rows: list[dict] = []
        for v in fins_summary_code_variants(code4):
            rows = fetch_paginated_v2(client, "/fins/summary", params={"code": v}, sleep_seconds=1.2)
            if rows:
                used = v
                break
        if not rows:
            rows_out.append({"code": code4, "variant_used": "", "error": "0 rows"})
            continue

        df = pd.DataFrame(rows)
        ser, err = aggregate_fins_summary_df(df)
        if err or not ser:
            rows_out.append({"code": code4, "variant_used": used, "error": err or "unknown"})
            continue

        row = {"code": code4, "variant_used": used, "error": ""}
        keys = [
            "NetSales_PriorYear_Actual",
            "NetSales_LatestYear_Actual",
            "NetSales_NextYear_Forecast",
            "OperatingProfit_PriorYear_Actual",
            "OperatingProfit_LatestYear_Actual",
            "OperatingProfit_NextYear_Forecast",
            "Profit_PriorYear_Actual",
            "Profit_LatestYear_Actual",
            "Profit_NextYear_Forecast",
            "YFinance_Supplemented",
            "_jq_fye_prior",
            "_jq_fye_latest",
        ]
        for k in keys:
            row[k] = ser.get(k, pd.NA)
        rows_out.append(row)

    out = pd.DataFrame(rows_out)
    pd.set_option("display.width", 240)
    pd.set_option("display.max_columns", 100)
    print(out.to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[Interrupted]", file=sys.stderr)
        raise

