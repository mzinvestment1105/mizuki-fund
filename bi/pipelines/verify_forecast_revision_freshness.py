"""
旧ロジック(strictのみ) vs 新ロジック(欠損NxtFYEn救済あり) の比較検証。

使い方:
  python verify_forecast_revision_freshness.py --limit 300
  python verify_forecast_revision_freshness.py --full
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import jquantsapi
import pandas as pd
from dotenv import load_dotenv

from make_screening_master_v2 import _fetch_fins_summary_rows_for_code
from update_statements import aggregate_fins_summary_df


def _fmt_yen(v: object) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "NA"
    return f"{x/1e8:.1f}億"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=300, help="検証対象銘柄数（--full優先）")
    parser.add_argument("--full", action="store_true", help="全銘柄を検証する")
    parser.add_argument("--sleep", type=float, default=0.35, help="API間隔秒")
    args = parser.parse_args()

    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(env_path)

    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY が .env に必要です。")

    src = pd.read_parquet(Path("..") / "outputs" / "screening_master.parquet")
    all_codes = src["Code"].astype(str).str[:4].drop_duplicates().tolist()
    codes = all_codes if args.full else all_codes[: args.limit]

    cli = jquantsapi.ClientV2(api_key=api_key)

    changes: list[dict[str, object]] = []
    stale_risks: list[dict[str, object]] = []
    scanned = 0
    relax_candidates = 0
    relax_candidates_codes: set[str] = set()
    total = len(codes)
    for i, code4 in enumerate(codes, start=1):
        rows = _fetch_fins_summary_rows_for_code(cli, code4, sleep_seconds=args.sleep)
        if not rows:
            continue
        fin_df = pd.DataFrame.from_records(rows)

        # 新ロジックの救済対象になりうる銘柄数を把握
        if "DiscDate" in fin_df.columns:
            w = fin_df.copy()
            w["DiscDate"] = pd.to_datetime(w["DiscDate"], errors="coerce")
            w = w.sort_values("DiscDate", ascending=True, kind="mergesort")
            if not w.empty:
                d_last = w["DiscDate"].max()
                latest = w.loc[w["DiscDate"] == d_last].copy()
                if not latest.empty:
                    doc = latest.get("DocType", pd.Series([], dtype=str)).astype(str)
                    has_rev = doc.str.contains("EarnForecastRevision", case=False, na=False).any()
                    if has_rev:
                        nxt = pd.to_datetime(latest.get("NxtFYEn"), errors="coerce")
                        nxt_missing = nxt.isna().any()
                        nx_cols = [c for c in ["NxFSales", "NxFNCSales", "NxFOP", "NxFNCOP", "NxFNp", "NxFNP", "NxFNCNP"] if c in latest.columns]
                        has_nx = False
                        for c in nx_cols:
                            if pd.to_numeric(latest[c], errors="coerce").notna().any():
                                has_nx = True
                                break
                        if nxt_missing and has_nx:
                            relax_candidates += 1
                            relax_candidates_codes.add(code4)

        os.environ["NX_FORECAST_RELAXED_REVISION"] = "0"
        old_ser, old_err = aggregate_fins_summary_df(fin_df)
        os.environ["NX_FORECAST_RELAXED_REVISION"] = "1"
        new_ser, new_err = aggregate_fins_summary_df(fin_df)

        if old_err or new_err or old_ser is None or new_ser is None:
            continue

        scanned += 1
        for col in (
            "NetSales_NextYear_Forecast",
            "OperatingProfit_NextYear_Forecast",
            "Profit_NextYear_Forecast",
        ):
            ov = old_ser.get(col, pd.NA)
            nv = new_ser.get(col, pd.NA)
            old_na = pd.isna(ov)
            new_na = pd.isna(nv)
            if old_na and not new_na:
                changes.append(
                    {
                        "Code": code4,
                        "Column": col,
                        "ChangeType": "NA->Value",
                        "Old": ov,
                        "New": nv,
                    }
                )

        # 期間依存でない鮮度検証:
        # 「最新の業績予想修正(EarnForecastRevision)の値」と最終採用値がズレていないかを直接確認。
        w2 = fin_df.copy()
        if "DiscDate" in w2.columns:
            w2["DiscDate"] = pd.to_datetime(w2["DiscDate"], errors="coerce")
        if "DocType" in w2.columns:
            doc2 = w2["DocType"].astype(str)
            rev = w2.loc[doc2.str.contains("EarnForecastRevision", case=False, na=False)].copy()
            if not rev.empty:
                rev = rev.sort_values("DiscDate", ascending=True, kind="mergesort")

                mapping = [
                    ("NetSales_NextYear_Forecast", ["NxFSales", "NxFNCSales"]),
                    ("OperatingProfit_NextYear_Forecast", ["NxFOP", "NxFNCOP"]),
                    ("Profit_NextYear_Forecast", ["NxFNp", "NxFNP", "NxFNCNP"]),
                ]
                for out_col, cand_cols in mapping:
                    latest_val = pd.NA
                    latest_dd = pd.NaT
                    for _, rr in rev.iterrows():
                        vv = pd.NA
                        for cc in cand_cols:
                            if cc in rr.index:
                                t = pd.to_numeric(rr.get(cc), errors="coerce")
                                if pd.notna(t):
                                    vv = t
                                    break
                        if pd.isna(vv):
                            continue
                        dd = pd.to_datetime(rr.get("DiscDate"), errors="coerce")
                        if pd.isna(dd):
                            continue
                        if pd.isna(latest_dd) or dd > latest_dd:
                            latest_dd = dd
                            latest_val = vv

                    if pd.isna(latest_val):
                        continue
                    final_v = new_ser.get(out_col, pd.NA)
                    if pd.notna(final_v) and float(final_v) != float(latest_val):
                        stale_risks.append(
                            {
                                "Code": code4,
                                "Column": out_col,
                                "LatestRevisionDiscDate": latest_dd,
                                "LatestRevisionValue": latest_val,
                                "FinalSelectedValue": final_v,
                                "GapType": "LatestRevisionMismatch",
                            }
                        )
            elif (not old_na) and (not new_na) and float(ov) != float(nv):
                changes.append(
                    {
                        "Code": code4,
                        "Column": col,
                        "ChangeType": "ValueUpdated",
                        "Old": ov,
                        "New": nv,
                    }
                )

        if i == 1 or i % 100 == 0 or i == total:
            print(
                f"progress {i}/{total} scanned={scanned} "
                f"changed={len(changes)} stale={len(stale_risks)}",
                flush=True,
            )

    out_df = pd.DataFrame(changes)
    stale_df = pd.DataFrame(stale_risks)
    out_path = Path("..") / "outputs" / "forecast_revision_freshness_check.csv"
    stale_path = Path("..") / "outputs" / "forecast_revision_stale_risk.csv"
    if not out_df.empty:
        out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    if not stale_df.empty:
        stale_df.to_csv(stale_path, index=False, encoding="utf-8-sig")

    print(
        f"scanned={scanned}, changed_rows={len(out_df)}, "
        f"relax_candidates={relax_candidates}, stale_risk_rows={len(stale_df)}"
    )
    if not out_df.empty:
        print(f"output={out_path}")
    if not stale_df.empty:
        print(f"stale_output={stale_path}")
    if out_df.empty and stale_df.empty:
        print("変更銘柄なし（旧新差分なし・鮮度ミスマッチなし）")
        return
    print("\n=== サンプル(先頭20件) ===")
    for _, r in out_df.head(20).iterrows():
        print(
            f"{r['Code']} {r['Column']} {r['ChangeType']} "
            f"{_fmt_yen(r['Old'])} -> {_fmt_yen(r['New'])}"
        )


if __name__ == "__main__":
    main()

