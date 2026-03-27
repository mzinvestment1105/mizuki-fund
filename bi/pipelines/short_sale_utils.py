"""
空売り /markets/short-sale-report 集計用。

標準ロジック（make_screening_master_v2 / update_short_positions と同一）:
- 直近約1か月分（SHORT_SALE_LOOKBACK_DAYS、既定30日）の開示を取得し1つの母集団に結合。
- DiscDate → CalcDate → _QueryDiscDate の順でソートし、各 (Code, inst_key) について
  **最後に更新があった行（tail(1)）**だけ残す。
- 銘柄ごとにその株数を **すべて合算**。
"""

from __future__ import annotations

import pandas as pd

QUERY_DISC_DATE_COL = "_QueryDiscDate"


def _as_str_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df[col].fillna("").astype(str)


def fix_degenerate_inst_keys(
    df: pd.DataFrame, *, inst_key_col: str = "inst_key", code_col: str = "Code"
) -> pd.DataFrame:
    out = df.copy()
    if inst_key_col not in out.columns:
        return out
    raw = out[inst_key_col].fillna("").astype(str)
    collapsed = raw.str.replace("|", "", regex=False).str.strip() == ""
    rownum = out.groupby(code_col, sort=False).cumcount().astype(str)
    anon = "__anon__|" + rownum
    out[inst_key_col] = raw.where(~collapsed, anon)
    return out


def aggregate_short_sale_monthly_pool(
    ss_df: pd.DataFrame,
    *,
    inst_col: str = "DiscretionaryInvestmentContractorName",
    shares_col: str = "ShortPositionsInSharesNumber",
    ratio_col: str = "ShortPositionsToSharesOutstandingRatio",
    query_disc_col: str = QUERY_DISC_DATE_COL,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if ss_df.empty:
        empty = pd.DataFrame(columns=["Code", inst_col, shares_col, ratio_col, "inst_key"])
        empty_tot = pd.DataFrame(columns=["Code", shares_col])
        empty_ratio = pd.DataFrame(columns=["Code", ratio_col])
        return empty, empty_tot, empty_ratio

    work = ss_df.copy()
    for k in ["DiscDate", "CalcDate"]:
        if k in work.columns:
            work[k] = pd.to_datetime(work[k], errors="coerce")

    sort_keys = [k for k in ["DiscDate", "CalcDate"] if k in work.columns]
    if query_disc_col in work.columns:
        work["_short_sale_qd_ord"] = pd.to_datetime(work[query_disc_col], errors="coerce")
        sort_keys.append("_short_sale_qd_ord")

    if sort_keys:
        work = work.sort_values(sort_keys, kind="mergesort")

    if "SSName" in work.columns:
        work[inst_col] = work[inst_col].where(
            work[inst_col].notna()
            & ~work[inst_col].astype(str).str.strip().isin(["", "-"]),
            work["SSName"],
        )

    work["inst_key"] = (
        _as_str_series(work, inst_col)
        + "|"
        + _as_str_series(work, "SSAddr")
        + "|"
        + _as_str_series(work, "FundName")
    )
    work = fix_degenerate_inst_keys(work)
    inst_dedup = work.groupby(["Code", "inst_key"], as_index=False).tail(1)
    inst_dedup = inst_dedup.drop(columns=["_short_sale_qd_ord"], errors="ignore")

    total_shares = (
        inst_dedup.groupby("Code")[shares_col]
        .sum(min_count=1)
        .rename(shares_col)
        .reset_index()
    )
    ratio_max = (
        inst_dedup.groupby("Code")[ratio_col]
        .max()
        .rename(ratio_col)
        .reset_index()
    )
    return inst_dedup, total_shares, ratio_max
