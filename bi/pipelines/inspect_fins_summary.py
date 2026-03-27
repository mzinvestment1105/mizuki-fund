"""
J-Quants /fins/summary を指定銘柄で取得し、集約結果と FY 行の Sales 有無を表示する（欠損調査用）。

  python inspect_fins_summary.py 130A
  python inspect_fins_summary.py 7203

要: JQUANTS_API_KEY（.env 可）
"""

from __future__ import annotations

import argparse
import os
import sys

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import jquantsapi
import pandas as pd

from jq_client_utils import fetch_paginated_v2
from update_statements import aggregate_fins_summary_df, fins_summary_code_variants


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("code", help="4桁銘柄（例: 130A, 7203）")
    args = p.parse_args()
    code4 = str(args.code).strip()
    key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not key:
        print("JQUANTS_API_KEY が未設定です。", file=sys.stderr)
        sys.exit(1)

    client = jquantsapi.ClientV2(key)
    rows: list[dict] = []
    for v in fins_summary_code_variants(code4):
        rows = fetch_paginated_v2(client, "/fins/summary", params={"code": v}, sleep_seconds=1.2)
        if rows:
            print(f"取得: code={v!r} rows={len(rows)}\n")
            break
    if not rows:
        print("0 件（variants すべて空）")
        sys.exit(2)

    df = pd.DataFrame(rows)
    # スタンダードプランでも /fins/summary は銘柄ごとに開示件数が違う（プランではなくデータの有無）
    if "DiscDate" in df.columns and not df.empty:
        dd = pd.to_datetime(df["DiscDate"], errors="coerce")
        print(
            f"/fins/summary: 全 {len(df)} 行, 開示日 DiscDate {dd.min()} ～ {dd.max()} "
            f"（この code で返る範囲＝API が保持する当該銘柄のサマリ履歴）\n"
        )
    ser, err = aggregate_fins_summary_df(df)
    print("aggregate_fins_summary_df:", "err=" + repr(err) if err else "ok")
    if ser:
        for k, v in ser.items():
            print(f"  {k}: {v}")

    if "CurPerType" not in df.columns:
        return

    qf = df[df["CurPerType"].astype(str).str.upper().str.strip().isin(["4Q", "FY"])].copy()
    if not qf.empty and "CurFYEn" in qf.columns:
        qf = qf.copy()
        qf["_dd"] = pd.to_datetime(qf["DiscDate"], errors="coerce")
        print("\n4Q/FY 行の診断（CurFYEn ごと・DiscDate 昇順、株探通期と突き合わせ用）:")
        for cen in sorted(qf["CurFYEn"].dropna().unique()):
            sub = qf.loc[qf["CurFYEn"] == cen].sort_values("_dd", kind="mergesort")
            print(f"  --- CurFYEn={cen} ---")
            for _, r in sub.iterrows():
                cpt = str(r.get("CurPerType", "")).strip()
                doc = str(r.get("DocType", ""))[:42]
                s = r.get("Sales", "")
                op = r.get("OP", "")
                npv = r.get("NP", "")
                print(
                    f"    DiscDate={r.get('DiscDate')}  {cpt:>2}  "
                    f"Sales={repr(s)[:14]} OP={repr(op)[:12]} NP={repr(npv)[:12]}  {doc}"
                )

    fy = df[df["CurPerType"].astype(str).str.upper().str.strip() == "FY"].copy()
    print("\nFY 行の Sales / NCSales（空は API 上ブランク）:")
    for _, r in fy.iterrows():
        doc = str(r.get("DocType", ""))[:50]
        s = r.get("Sales", "")
        ns = r.get("NCSales", "")
        op = r.get("OP", "")
        print(
            f"  DiscDate={r.get('DiscDate')} CurFYEn={r.get('CurFYEn')} "
            f"Sales={repr(s)[:20]} NCSales={repr(ns)[:20]} OP={repr(op)[:16]} … {doc}"
        )

    # 典型欠損: 売上だけ空で OP はある
    bad = fy[
        (fy["Sales"].astype(str).str.strip().isin(["", "nan", "None"]) | fy["Sales"].isna())
        & ~(fy["OP"].astype(str).str.strip().isin(["", "nan", "None"]) | fy["OP"].isna())
    ]
    if not bad.empty:
        print(
            "\n※ 次の FY 行は API 上 Sales が空だが OP/NP はある → "
            "NetSales_LatestYear_Actual が欠損になりやすい:"
        )
        print(bad[["DiscDate", "CurFYEn", "DocType"]].to_string(index=False))


if __name__ == "__main__":
    main()
