"""
/fins/summary を集約して master 用の財務列を返す。

【実績・昨年/今年】
  **CurPerType が 4Q または FY の行だけ**を通期実績として使う（1Q〜3Q・5Q は実績に使わない）。
  会計年度末 CurFYEn（欠損時は CurPerEn）ごとに 1 行選び、同一年度末で **FY を 4Q より優先**
  （短信・株探の通期に近い。FY 行が無いときだけ 4Q）。開示が最も新しい 1 行を採用。
  **昨年/今年の会計年度**は売上・OP・NP・自己資本比率で **共通**。
  **決算書類（FinancialStatements）行がある会計年度末**だけを並べた **直近 2 期**（先行する予想のみの FY 行で一年ずれないようにする）。
  FS が 2 年分無いときは従来どおり w_one 全体の直近 2 期。
  **売上の通期実績（Sales）**は **CurPerType が FY または 4Q の行のみ**採用（1Q〜3Q の累計を通期にしない）。
  **売上高**は API で最新の FY 本決算だけ Sales が空のことがある → 同じ会計年度の
  **業績予想の修正（EarnForecastRevision）の FNCSales** で補う（株探の会社予想 91 等に近づく）。
  株探の百万円表示と API（円）の対応は 1,000,000 で割ると比較しやすい。
  「翌期の本決算から前年通期を取り出す」ことは **/fins/summary のスキーマ上できない**
  （前年実績は CurFYEn がその年度の **4Q/FY 行**として別レコードで返るときだけ取得可能。
  その行が API に無い年度は欠損のまま）。

【予想】
  売上 **NetSales_NextYear_Forecast** は **(1)** 全行を走査し、各行で **NxFSales / NxFNCSales** を優先、
  なければ **FSales / FNCSales** を候補とし、**DiscDate が最大**の開示を採用（NX_FSALES_MAX_AGE_DAYS 以内）。
  **(2)** それでも空のとき **株探の「予」行に近い推定**:  
  **今期の会社予想売上（NetSales_LatestYear_Actual） ×（昨期の本決算売上 ÷ 昨期本決算の直前の業績修正 FNCSales）**  
  を **百万円単位で四捨五入**（例: 130A で 91×235/189 → 113 百万円）。  
  FSales・FNCSales を翌期列に直置きすると 130A のように 91 が誤って入るため使わない。
  営業益・最終益の翌期は **Nx*** を **F*** より先に見て**、行は新しい方から遡る。
  J-Quants の注意: IFRS/USGAAP は「経常利益」概念が無く OdP 等が空になることがある（API 仕様）。

期末発行済株数 (ShOutFY): 開示が最も新しい行から取得。

現金同等物・純資産（金額）: 直近通期（今年実績と同一会計年度末）の代表行から CashEq・Eq / NCEq。

売上・営業利益・純利益（最終益=NP）: 各 昨年実績 / 今年実績 / 予想（1 列は上記予想の 1 行から）。
一昨年実績: 決算書類ベースの会計年度末が 3 期分以上あるとき、その 3 番目に新しい年度の実績。
"""

from __future__ import annotations

import os
import unicodedata
from typing import Any

import pandas as pd

# 出力列（この順で parquet / master）
STATEMENT_NUMERIC_COLS: list[str] = [
    "NetSales_PriorYear_Actual",
    "NetSales_LatestYear_Actual",
    "NetSales_NextYear_Forecast",
    "OperatingProfit_PriorYear_Actual",
    "OperatingProfit_LatestYear_Actual",
    "OperatingProfit_NextYear_Forecast",
    "Profit_PriorYear_Actual",
    "Profit_LatestYear_Actual",
    "Profit_NextYear_Forecast",
    "NetSales_TwoYearsPrior_Actual",
    "OperatingProfit_TwoYearsPrior_Actual",
    "Profit_TwoYearsPrior_Actual",
    "CashAndEquivalents_LatestFY",
    "Equity_LatestFY",
    "EquityToAssetRatio",
    "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
]

CRITICAL_COLS: list[str] = [
    "NetSales_LatestYear_Actual",
    "OperatingProfit_LatestYear_Actual",
    "Profit_LatestYear_Actual",
    "EquityToAssetRatio",
    "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
]

_RAW_ALIASES: dict[str, str] = {
    "NxFNP": "NxFNp",
}

# CurPerType 正規化で許す値（実績に使うのは 4Q / FY のみ）
_VALID_PERIODS: frozenset[str] = frozenset(["1Q", "2Q", "3Q", "4Q", "5Q", "FY"])


def _normalize_code_4(code: object) -> str:
    s = str(code).strip()
    return s[:4] if len(s) >= 4 else s


def _normalize_cur_per_type(raw: object) -> str:
    """CurPerType の表記ゆれ（全角・空白）を吸収し 1Q〜5Q / FY に正規化。"""
    if raw is None:
        return ""
    try:
        if pd.isna(raw):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace(" ", "").replace("　", "").strip().upper()
    return s if s in _VALID_PERIODS else ""


def fins_summary_code_variants(code4: str) -> list[str]:
    """
    /fins/summary の code パラメータ用。4桁は 5桁（末尾0）も試す。
    グロース等アルファベット銘柄（例: 130A）は API が 130A0 のみ返すことが多いので 5桁を先に試す。
    """
    c = str(code4).strip()
    if len(c) == 5 and c.endswith("0") and c[:-1].isalnum():
        c4 = c[:-1]
        return list(dict.fromkeys([c, c4]))
    if len(c) == 4:
        c5 = c + "0"
        if c.isdigit():
            return list(dict.fromkeys([c, c5]))
        return list(dict.fromkeys([c5, c]))
    return [c]


def _first_numeric_from_sources(
    frame: pd.DataFrame, source_cols: list[str], *, from_newest: bool = True
) -> Any:
    if frame is None or frame.empty:
        return pd.NA
    indices = range(len(frame) - 1, -1, -1) if from_newest else range(len(frame))
    for pos in indices:
        row = frame.iloc[pos]
        for rc in source_cols:
            if rc not in row.index:
                continue
            v = pd.to_numeric(row[rc], errors="coerce")
            if pd.notna(v):
                return v
    return pd.NA


def _val_from_row(row: pd.Series | None, consolidated: list[str], non_consolidated: list[str]) -> Any:
    if row is None:
        return pd.NA
    for keys in (consolidated, non_consolidated):
        for k in keys:
            if k not in row.index:
                continue
            v = pd.to_numeric(row[k], errors="coerce")
            if pd.notna(v):
                return v
    return pd.NA


def _forecast_first_non_na_from_newest(
    scan: pd.DataFrame,
    consolidated: list[str],
    non_consolidated: list[str],
) -> Any:
    """scan は開示日時昇順。末尾が最新。項目ごとに新しい行から遡って最初の数値を採用。"""
    for pos in range(len(scan) - 1, -1, -1):
        v = _val_from_row(scan.iloc[pos], consolidated, non_consolidated)
        if pd.notna(v):
            return v
    return pd.NA


def _forecast_nx_by_newest_disc_date(
    scan: pd.DataFrame,
    consolidated: list[str],
    non_consolidated: list[str],
) -> Any:
    """
    翌期系（NxFOP 等）: 数値が入っている行のうち DiscDate が最も新しいものを採用。
    直近開示から NX_FORECAST_MAX_AGE_DAYS（省略時は NX_FSALES_MAX_AGE_DAYS）より古い行は除外。
    """
    if scan is None or scan.empty:
        return pd.NA
    if "DiscDate" not in scan.columns:
        return _forecast_first_non_na_from_newest(scan, consolidated, non_consolidated)
    max_age_raw = os.environ.get("NX_FORECAST_MAX_AGE_DAYS", "").strip()
    if max_age_raw:
        max_age_days = int(max_age_raw)
    else:
        max_age_days = int(os.environ.get("NX_FSALES_MAX_AGE_DAYS", "550"))
    d_series = pd.to_datetime(scan["DiscDate"], errors="coerce")
    d_last = d_series.iloc[-1] if len(scan) else pd.NaT
    if pd.isna(d_last):
        d_last = d_series.max()
    best_v: Any = pd.NA
    best_dd = pd.NaT
    for pos in range(len(scan)):
        r = scan.iloc[pos]
        v = _val_from_row(r, consolidated, non_consolidated)
        if pd.isna(v):
            continue
        dd = pd.to_datetime(r.get("DiscDate"), errors="coerce")
        if pd.isna(dd):
            continue
        if pd.notna(d_last) and (d_last - dd).days > max_age_days:
            continue
        if pd.isna(best_dd) or dd > best_dd:
            best_dd = dd
            best_v = v
    return best_v


def _forecast_by_newest_disc_date_with_meta(
    scan: pd.DataFrame,
    consolidated: list[str],
    non_consolidated: list[str],
) -> tuple[Any, Any]:
    """
    数値と、その採用元 DiscDate を返す。
    """
    if scan is None or scan.empty:
        return pd.NA, pd.NaT
    if "DiscDate" not in scan.columns:
        v = _forecast_first_non_na_from_newest(scan, consolidated, non_consolidated)
        return v, pd.NaT
    max_age_raw = os.environ.get("NX_FORECAST_MAX_AGE_DAYS", "").strip()
    if max_age_raw:
        max_age_days = int(max_age_raw)
    else:
        max_age_days = int(os.environ.get("NX_FSALES_MAX_AGE_DAYS", "550"))
    d_series = pd.to_datetime(scan["DiscDate"], errors="coerce")
    d_last = d_series.iloc[-1] if len(scan) else pd.NaT
    if pd.isna(d_last):
        d_last = d_series.max()
    best_v: Any = pd.NA
    best_dd = pd.NaT
    for pos in range(len(scan)):
        r = scan.iloc[pos]
        v = _val_from_row(r, consolidated, non_consolidated)
        if pd.isna(v):
            continue
        dd = pd.to_datetime(r.get("DiscDate"), errors="coerce")
        if pd.isna(dd):
            continue
        if pd.notna(d_last) and (d_last - dd).days > max_age_days:
            continue
        if pd.isna(best_dd) or dd > best_dd:
            best_dd = dd
            best_v = v
    return best_v, best_dd


def _next_year_sales_forecast_from_nx_columns(scan: pd.DataFrame) -> Any:
    """
    翌期売上（来年通期予想）。

    全行を走査し、各行で **NxFSales / NxFNCSales を優先**、なければ **FSales / FNCSales** を候補とする。
    **DiscDate が最大**の開示の値を採用（最新行だけ Nx を優先する特別扱いはしない。
    古い Nx が新しい FSales 修正に負けないようにする）。
    ``NX_FSALES_MAX_AGE_DAYS``（既定 550 日）で scan 末尾日付から古すぎる行は除外。
    """
    if scan is None or scan.empty:
        return pd.NA
    if "DiscDate" not in scan.columns:
        r0 = scan.iloc[-1]
        # NxFSales / NxFNCSales のみ使用。FSales / FNCSales は今期予想のため翌期列に入れない。
        return _val_from_row(r0, ["NxFSales"], ["NxFNCSales"])

    max_age_days = int(os.environ.get("NX_FSALES_MAX_AGE_DAYS", "550"))
    d_series = pd.to_datetime(scan["DiscDate"], errors="coerce")
    d_last = d_series.iloc[-1] if len(scan) else pd.NaT
    if pd.isna(d_last):
        d_last = d_series.max()

    best_v: Any = pd.NA
    best_key: tuple[Any, ...] | None = None
    for pos in range(len(scan)):
        r = scan.iloc[pos]
        # NxFSales / NxFNCSales のみ使用。FSales / FNCSales は今期予想のため翌期列に入れない。
        v = _val_from_row(r, ["NxFSales"], ["NxFNCSales"])
        if pd.isna(v):
            continue
        dd = pd.to_datetime(r.get("DiscDate"), errors="coerce")
        if pd.isna(dd):
            continue
        if pd.notna(d_last) and (d_last - dd).days > max_age_days:
            continue
        disc_no = str(r.get("DiscNo", "") or "")
        key = (dd, disc_no, pos)
        if best_key is None or key > best_key:
            best_key = key
            best_v = v
    return best_v


def _fn_revision_fnc_before_fy_statement(
    work: pd.DataFrame, fye: Any, sort_keys: list[str]
) -> Any:
    """
    会計年度 fye に対し、FY 本決算書の開示より前の EarnForecastRevision の FNCSales（最後の1件）。
    130A: 2024-12-31 期 → 2024-12-13 の 189000000（2025-02-13 本決算より前）。
    """
    if work is None or work.empty or "_fye" not in work.columns or pd.isna(fye):
        return pd.NA
    fn = pd.to_datetime(fye, errors="coerce").normalize()
    sub = work.loc[work["_fye"] == fn]
    if sub.empty:
        return pd.NA
    sub = sub.sort_values(sort_keys, ascending=True, kind="mergesort")
    if "DocType" not in sub.columns:
        return pd.NA
    if "_cpt_norm" not in sub.columns and "CurPerType" in sub.columns:
        sub = sub.copy()
        sub["_cpt_norm"] = sub["CurPerType"].map(_normalize_cur_per_type)
    doc = sub["DocType"].astype(str)
    stm = doc.str.contains("FinancialStatements", case=False, na=False) & sub["_cpt_norm"].astype(str).eq(
        "FY"
    )
    fy_disc = sub.loc[stm, "DiscDate"]
    if fy_disc.empty or fy_disc.isna().all():
        return pd.NA
    d_fy = pd.to_datetime(fy_disc.iloc[-1], errors="coerce")
    rev = doc.str.contains("EarnForecastRevision", case=False, na=False)
    before = sub.loc[rev & (sub["DiscDate"] < d_fy)]
    if before.empty:
        return pd.NA
    last_rev = before.iloc[-1]
    v = pd.to_numeric(last_rev.get("FNCSales"), errors="coerce")
    return v if pd.notna(v) else pd.NA


def _next_year_sales_forecast_implied_kabutan_style(
    out: dict[str, Any], work: pd.DataFrame, fye_py: Any, sort_keys: list[str]
) -> Any:
    """
    NxFSales が API で空のとき、株探「予」売上に近い推定:
      NetSales_Latest × (NetSales_PriorActual / 昨期本決算前の FNCSales)
    を円単位で百万円四捨五入（130A: 91e6 * 235018000 / 189000000 ≈ 113e6）。
    """
    latest = out.get("NetSales_LatestYear_Actual")
    prior_act = out.get("NetSales_PriorYear_Actual")
    rev_py = _fn_revision_fnc_before_fy_statement(work, fye_py, sort_keys)
    if pd.isna(latest) or pd.isna(prior_act) or pd.isna(rev_py):
        return pd.NA
    try:
        lv = float(latest)
        pa = float(prior_act)
        rv = float(rev_py)
    except (TypeError, ValueError):
        return pd.NA
    if rv <= 0 or lv <= 0 or pa <= 0:
        return pd.NA
    ratio = pa / rv
    if ratio < 0.15 or ratio > 12.0:
        return pd.NA
    implied = lv * ratio
    million = 1_000_000.0
    return int(round(implied / million) * million)


def _forecast_from_next_fy_rows(
    work: pd.DataFrame,
    fye_cy: Any,
    consolidated: list[str],
    non_consolidated: list[str],
) -> Any:
    """
    翌期の行（CurFYEn が fye_cy の次の会計年度末）の F* 列から予想を取得するフォールバック。

    JQ が翌期予想を NxF* ではなく、翌期行の F*/FNC* として格納しているケースに対応。
    例: 130A の 2026.12 予想は CurFYEn=2026-12-31 行の FSales に入っている。
    """
    if work is None or work.empty or "_fye" not in work.columns or pd.isna(fye_cy):
        return pd.NA
    fn_cy = pd.to_datetime(fye_cy, errors="coerce").normalize()
    if pd.isna(fn_cy):
        return pd.NA
    # _fye のうち fye_cy より後のものの中で最小 = 翌期
    all_fyes = work["_fye"].dropna().unique()
    future = [f for f in all_fyes if pd.Timestamp(f) > fn_cy]
    if not future:
        return pd.NA
    fye_next = min(future)

    sub = work.loc[work["_fye"] == fye_next]
    if sub.empty:
        return pd.NA
    # DiscDate 降順で最新開示から探す
    if "DiscDate" in sub.columns:
        sub = sub.sort_values("DiscDate", ascending=False, kind="mergesort")
    else:
        sub = sub.iloc[::-1]
    for pos in range(len(sub)):
        v = _val_from_row(sub.iloc[pos], consolidated, non_consolidated)
        if pd.notna(v):
            return v
    return pd.NA


def _forecast_from_next_fy_rows_with_meta(
    work: pd.DataFrame,
    fye_cy: Any,
    consolidated: list[str],
    non_consolidated: list[str],
) -> tuple[Any, Any]:
    """
    翌期行フォールバックの値と採用元 DiscDate を返す。
    """
    if work is None or work.empty or "_fye" not in work.columns or pd.isna(fye_cy):
        return pd.NA, pd.NaT
    fn_cy = pd.to_datetime(fye_cy, errors="coerce").normalize()
    if pd.isna(fn_cy):
        return pd.NA, pd.NaT
    all_fyes = work["_fye"].dropna().unique()
    future = [f for f in all_fyes if pd.Timestamp(f) > fn_cy]
    if not future:
        return pd.NA, pd.NaT
    fye_next = min(future)
    sub = work.loc[work["_fye"] == fye_next]
    if sub.empty:
        return pd.NA, pd.NaT
    if "DiscDate" in sub.columns:
        sub = sub.sort_values("DiscDate", ascending=False, kind="mergesort")
    else:
        sub = sub.iloc[::-1]
    for pos in range(len(sub)):
        r = sub.iloc[pos]
        v = _val_from_row(r, consolidated, non_consolidated)
        if pd.notna(v):
            return v, pd.to_datetime(r.get("DiscDate"), errors="coerce")
    return pd.NA, pd.NaT


def _apply_forecasts_from_newest_disclosure_row_only(
    out: dict[str, Any],
    work: pd.DataFrame,
    *,
    fye_py: Any = None,
    fye_cy: Any = None,
    sort_keys: list[str] | None = None,
) -> None:
    """
    決算系（1Q〜FY）の開示から予想を取得。
    最新 1 行に予想が無い（IFRS 四半期のみ等）とき、同一銘柄の古い開示へ項目ごとに遡る。

    NxF* が空のとき、翌期行（CurFYEn が fye_cy の次）の F*/FNC* をフォールバックとして使う。
    """
    if "_cpt_norm" in work.columns:
        scan = work.loc[work["_cpt_norm"].astype(str).str.len() > 0]
        if scan.empty:
            scan = work
    elif "CurPerType" in work.columns:
        _cpt = work["CurPerType"].map(_normalize_cur_per_type)
        scan = work.loc[_cpt.astype(str).str.len() > 0]
        if scan.empty:
            scan = work
    else:
        scan = work

    nx_sales, nx_sales_dd = _forecast_by_newest_disc_date_with_meta(scan, ["NxFSales"], ["NxFNCSales"])
    nx_op, nx_op_dd = _forecast_by_newest_disc_date_with_meta(scan, ["NxFOP"], ["NxFNCOP"])
    nx_np, nx_np_dd = _forecast_by_newest_disc_date_with_meta(scan, ["NxFNp", "NxFNP"], ["NxFNCNP"])

    next_sales, next_sales_dd = pd.NA, pd.NaT
    next_op, next_op_dd = pd.NA, pd.NaT
    next_np, next_np_dd = pd.NA, pd.NaT
    if pd.notna(fye_cy) and "_fye" in work.columns:
        next_sales, next_sales_dd = _forecast_from_next_fy_rows_with_meta(
            work, fye_cy, ["FSales"], ["FNCSales"]
        )
        next_op, next_op_dd = _forecast_from_next_fy_rows_with_meta(
            work, fye_cy, ["FOP"], ["FNCOP"]
        )
        next_np, next_np_dd = _forecast_from_next_fy_rows_with_meta(
            work, fye_cy, ["FNp", "FNP"], ["FNCNP"]
        )

    out["NetSales_NextYear_Forecast"] = nx_sales
    out["OperatingProfit_NextYear_Forecast"] = nx_op
    out["Profit_NextYear_Forecast"] = nx_np

    # Nx* と翌期行 F* が競合する場合は、より新しい開示日の値を採用（株探の修正履歴に合わせる）。
    if pd.notna(next_sales) and (pd.isna(nx_sales) or (pd.notna(next_sales_dd) and pd.notna(nx_sales_dd) and next_sales_dd >= nx_sales_dd)):
        out["NetSales_NextYear_Forecast"] = next_sales
    if pd.notna(next_op) and (pd.isna(nx_op) or (pd.notna(next_op_dd) and pd.notna(nx_op_dd) and next_op_dd >= nx_op_dd)):
        out["OperatingProfit_NextYear_Forecast"] = next_op
    if pd.notna(next_np) and (pd.isna(nx_np) or (pd.notna(next_np_dd) and pd.notna(nx_np_dd) and next_np_dd >= nx_np_dd)):
        out["Profit_NextYear_Forecast"] = next_np

    # NxFSales も翌期行 FSales も無い銘柄向けフォールバック（例: 130A）。
    # /fins/summary の仕様上、翌期の数値が Nx* にも翌期行 F* にも現れないことがあるため、
    # 「株探の予に近い」推定を使う（推定条件が揃わない場合は NA のまま）。
    if (
        pd.isna(out.get("NetSales_NextYear_Forecast"))
        and sort_keys is not None
        and pd.notna(fye_py)
        and work is not None
        and not work.empty
    ):
        implied = _next_year_sales_forecast_implied_kabutan_style(out, work, fye_py, sort_keys)
        if pd.notna(implied):
            out["NetSales_NextYear_Forecast"] = implied



def _attach_fye_and_4q_fy_rank(frame: pd.DataFrame) -> pd.DataFrame | None:
    """CurFYEn（欠損時は CurPerEn）で _fye。通期実績は FY を 4Q より優先（_pr: FY=0, 4Q=1）。"""
    wf = frame.copy()
    wf["_fye"] = (
        pd.to_datetime(wf["CurFYEn"], errors="coerce")
        if "CurFYEn" in wf.columns
        else pd.Series(pd.NaT, index=wf.index)
    )
    if "CurPerEn" in wf.columns:
        cpe = pd.to_datetime(wf["CurPerEn"], errors="coerce")
        mask_na = wf["_fye"].isna()
        if mask_na.any():
            wf.loc[mask_na, "_fye"] = cpe[mask_na]
    wf["_fye"] = pd.to_datetime(wf["_fye"], errors="coerce").dt.normalize()
    wf = wf.dropna(subset=["_fye"])
    if wf.empty:
        return None

    wf["_pr"] = wf["_cpt_norm"].map(lambda x: 0 if x == "FY" else (1 if x == "4Q" else 99))

    disc_cols: list[str] = ["DiscDate"]
    if "_DiscTimeOrd" in wf.columns:
        disc_cols.append("_DiscTimeOrd")
    elif "DiscTime" in wf.columns:
        disc_cols.append("DiscTime")
    if "DiscNo" in wf.columns:
        if "_DiscNoStr" not in wf.columns:
            wf["_DiscNoStr"] = wf["DiscNo"].astype(str)
        disc_cols.append("_DiscNoStr")

    out_rows: list[pd.Series] = []
    for fye in sorted(wf["_fye"].dropna().unique()):
        sub = wf.loc[wf["_fye"] == fye]
        best = int(sub["_pr"].min())
        sub2 = sub.loc[sub["_pr"] == best].sort_values(disc_cols, ascending=True, kind="mergesort")
        if len(sub2):
            # 同一日に FY 決算短信と業績修正だけが並ぶ銘柄（1379 等）で、修正行（OP 空）が最後に来ると
            # 実績が欠損する。FinancialStatements を含む行があればそれに限定する。
            if "DocType" in sub2.columns:
                doc = sub2["DocType"].astype(str)
                fs_mask = doc.str.contains("FinancialStatements", case=False, na=False)
                if fs_mask.any():
                    sub2 = sub2.loc[fs_mask].sort_values(disc_cols, ascending=True, kind="mergesort")
            # 最新行が「数値は同じだが一部カラムが欠落」などのケースがある。
            # 実績（Sales/OP/NP など）を安定させるため、まず「必要項目が揃っている行」を優先し、
            # 同点なら開示が新しい行（disc_cols が大きい）を採用する。
            key_cols = [
                "Sales",
                "NCSales",
                "OP",
                "NCOP",
                "NP",
                "NCNP",
                "EqAR",
                "NCEqAR",
                "ShOutFY",
            ]
            present = [c for c in key_cols if c in sub2.columns]
            if present:
                tmp = sub2.copy()
                score = pd.Series(0, index=tmp.index, dtype="int64")
                for c in present:
                    v = pd.to_numeric(tmp[c], errors="coerce")
                    score = score + v.notna().astype("int64")
                tmp["_score_complete"] = score
                # completeness 昇順→末尾が最大、disc_cols 昇順→末尾が最新
                tmp = tmp.sort_values(["_score_complete"] + disc_cols, ascending=True, kind="mergesort")
                out_rows.append(tmp.iloc[-1].drop(labels=["_score_complete"]))
            else:
                out_rows.append(sub2.iloc[-1])
    if not out_rows:
        return None
    return pd.DataFrame(out_rows)


def _financial_rows_one_per_fye(work: pd.DataFrame) -> pd.DataFrame | None:
    """実績用: CurPerType が 4Q または FY の行のみ（中間四半期は使わない）。年度末ごとに FY を 4Q より優先して 1 行に圧縮。"""
    if "_cpt_norm" not in work.columns:
        return None
    wf_primary = work.loc[work["_cpt_norm"].isin(["4Q", "FY"])].copy()
    if wf_primary.empty:
        return None
    return _attach_fye_and_4q_fy_rank(wf_primary)


def _work_fiscal_year_end_column(work: pd.DataFrame) -> pd.Series:
    """全行に会計年度末 _fye（CurFYEn、欠損時は CurPerEn）。"""
    ser = (
        pd.to_datetime(work["CurFYEn"], errors="coerce")
        if "CurFYEn" in work.columns
        else pd.Series(pd.NaT, index=work.index)
    )
    if "CurPerEn" in work.columns:
        cpe = pd.to_datetime(work["CurPerEn"], errors="coerce")
        mask_na = ser.isna()
        if mask_na.any():
            ser = ser.copy()
            ser.loc[mask_na] = cpe[mask_na]
    return pd.to_datetime(ser, errors="coerce").dt.normalize()


def _net_sales_for_fiscal_year(
    work: pd.DataFrame,
    fye: Any,
    sort_keys: list[str],
    primary_row: pd.Series | None = None,
) -> Any:
    """
    同一会計年度の開示から売上高を1つ選ぶ。

    **primary_row**（会計年度ごとの代表 1 行＝w_one と OP/NP と同じ開示）が渡され、
    かつ Sales/NCSales が入っていれば **それを最優先**（科目間で同じレコードに揃える）。

    1) 決算書類（FinancialStatements）の Sales / NCSales（実績）— **CurPerType は FY または 4Q のみ**
       （1Q〜3Q の累計 Sales を通期実績として誤採用しない）
    2) その他開示の Sales / NCSales — **同様に FY / 4Q のみ**
    3) 業績予想修正（EarnForecastRevision）の FNCSales（会社予想・本決算で売上が空の年の補完）
    4) FSales（四半期に載る通期会社予想）

    株探の百万円表示と突き合わせるときは API 値（円）を 1_000_000 で割る。
    銘柄によって株探と数値が一致しない行がある（別ソース・修正版・表示丸め等）。
    株探の「2024.12」が API の本決算 Sales（130A では 235018000 円）と一致しないことがある。
    /fins/summary に 194000000 円という値は現れない（別表・連結・サイト独自計算の可能性）。
    直前の会社予想は EarnForecastRevision の FNCSales（130A では 189000000 円）で確認できる。
    """
    if pd.isna(fye) or work is None or work.empty or "_fye" not in work.columns:
        return pd.NA
    fn = pd.to_datetime(fye, errors="coerce").normalize()
    sub = work.loc[work["_fye"] == fn]
    if sub.empty:
        return pd.NA
    sub = sub.sort_values(sort_keys, ascending=True, kind="mergesort")

    if primary_row is not None and isinstance(primary_row, pd.Series) and not primary_row.empty:
        v0 = _val_from_row(primary_row, ["Sales"], ["NCSales"])
        if pd.notna(v0):
            return v0

    if "DocType" not in sub.columns:
        sub = sub.assign(_doc="")
    else:
        sub = sub.assign(_doc=sub["DocType"].astype(str))

    if "_cpt_norm" not in sub.columns:
        sub = sub.copy()
        if "CurPerType" in sub.columns:
            sub["_cpt_norm"] = sub["CurPerType"].map(_normalize_cur_per_type)
        else:
            sub["_cpt_norm"] = ""
    _fy4q = sub["_cpt_norm"].astype(str).isin(["FY", "4Q"])

    # 1) 決算書類の実績売上（FY/4Q のみ・新しい開示を優先）
    stm = sub["_doc"].str.contains("FinancialStatements", case=False, na=False) & _fy4q
    for _, r in sub.loc[stm].iloc[::-1].iterrows():
        v = _val_from_row(r, ["Sales"], ["NCSales"])
        if pd.notna(v):
            return v

    # 2) その他の Sales（FY/4Q のみ）
    for _, r in sub.loc[_fy4q].iloc[::-1].iterrows():
        v = _val_from_row(r, ["Sales"], ["NCSales"])
        if pd.notna(v):
            return v

    # 3) 業績修正の会社予想売上
    rev = sub["_doc"].str.contains("EarnForecastRevision", case=False, na=False)
    for _, r in sub.loc[rev].iloc[::-1].iterrows():
        v = pd.to_numeric(r.get("FNCSales"), errors="coerce")
        if pd.notna(v):
            return v

    # 4) 四半期の通期予想 FSales
    for _, r in sub.iloc[::-1].iterrows():
        v = _val_from_row(r, ["FSales"], ["FNCSales"])
        if pd.notna(v):
            return v

    return pd.NA


def aggregate_fins_summary_df(
    fin_df: pd.DataFrame,
) -> tuple[dict[str, Any] | None, str | None]:
    if fin_df is None or fin_df.empty:
        return None, "empty /fins/summary"

    work = fin_df.copy()
    for bad, good in _RAW_ALIASES.items():
        if bad in work.columns and good not in work.columns:
            work = work.rename(columns={bad: good})

    if "Code" in work.columns:
        work["Code"] = work["Code"].map(_normalize_code_4).astype(str)
    if "DiscDate" in work.columns:
        work["DiscDate"] = pd.to_datetime(work["DiscDate"], errors="coerce")
    if "DiscTime" in work.columns:
        _t = work["DiscTime"].astype(str).replace({"nan": "", "NaT": "", "<NA>": ""})
        work["_DiscTimeOrd"] = pd.to_datetime(
            work["DiscDate"].dt.strftime("%Y-%m-%d") + " " + _t,
            errors="coerce",
        )

    if "DiscDate" not in work.columns:
        return None, "missing DiscDate in /fins/summary"
    work = work.dropna(subset=["DiscDate"])
    if work.empty:
        return None, "empty /fins/summary after DiscDate filter"

    sort_keys: list[str] = ["DiscDate"]
    if "_DiscTimeOrd" in work.columns:
        sort_keys.append("_DiscTimeOrd")
    elif "DiscTime" in work.columns:
        sort_keys.append("DiscTime")
    if "DiscNo" in work.columns:
        work["_DiscNoStr"] = work["DiscNo"].astype(str)
        sort_keys.append("_DiscNoStr")
    work = work.sort_values(sort_keys, ascending=True, kind="mergesort")

    out: dict[str, Any] = {c: pd.NA for c in STATEMENT_NUMERIC_COLS}

    if "CurPerType" not in work.columns:
        out["EquityToAssetRatio"] = _first_numeric_from_sources(work, ["EqAR", "NCEqAR"])
        out["CashAndEquivalents_LatestFY"] = _first_numeric_from_sources(work, ["CashEq"])
        out["Equity_LatestFY"] = _first_numeric_from_sources(work, ["Eq", "NCEq"])
        out["NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"] = (
            _first_numeric_from_sources(work, ["ShOutFY"])
        )
        _apply_forecasts_from_newest_disclosure_row_only(out, work)
        return out, None

    work["_cpt_norm"] = work["CurPerType"].map(_normalize_cur_per_type)
    work["_fye"] = _work_fiscal_year_end_column(work)

    w_one = _financial_rows_one_per_fye(work)
    if w_one is None or w_one.empty:
        out["EquityToAssetRatio"] = _first_numeric_from_sources(work, ["EqAR", "NCEqAR"])
        out["CashAndEquivalents_LatestFY"] = _first_numeric_from_sources(work, ["CashEq"])
        out["Equity_LatestFY"] = _first_numeric_from_sources(work, ["Eq", "NCEq"])
        out["NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"] = (
            _first_numeric_from_sources(work, ["ShOutFY"])
        )
        _apply_forecasts_from_newest_disclosure_row_only(out, work)
        return out, None

    def _row_for_fye(fye: Any) -> pd.Series | None:
        if pd.isna(fye):
            return None
        fn = pd.to_datetime(fye, errors="coerce").normalize()
        sub = w_one.loc[w_one["_fye"] == fn]
        return sub.iloc[-1] if len(sub) else None

    # 昨年/今年の会計年度は科目共通。売上は代表行（w_one＝OP/NP と同じ開示）を優先し、
    # 空のときは work 全開示から補完（FNCSales 等）。
    # 昨年・今年の会計年度: 直近2期を w_one から取るが、CurFYEn だけ先行する「予」行（2026-03 等）で
    # 実績が空になる銘柄がある。決算書類（FinancialStatements）行が存在する年度だけで直近2期を決める。
    fyes_fs: list[Any] = []
    if "DocType" in w_one.columns:
        _doc_o = w_one["DocType"].astype(str)
        w_fs_only = w_one.loc[_doc_o.str.contains("FinancialStatements", case=False, na=False)]
        if not w_fs_only.empty:
            fyes_fs = sorted(w_fs_only["_fye"].dropna().unique())
    if len(fyes_fs) >= 2:
        fye_py = fyes_fs[-2]
        fye_cy = fyes_fs[-1]
    else:
        fyes = sorted(w_one["_fye"].dropna().unique())
        fye_py = fyes[-2] if len(fyes) >= 2 else pd.NaT
        fye_cy = fyes[-1] if len(fyes) >= 1 else pd.NaT

    if len(fyes_fs) >= 3:
        fye_ppy = fyes_fs[-3]
    else:
        _fall = sorted(w_one["_fye"].dropna().unique())
        fye_ppy = _fall[-3] if len(_fall) >= 3 else pd.NaT

    r_py_o, r_cy_o = _row_for_fye(fye_py), _row_for_fye(fye_cy)
    r_py_p, r_cy_p = r_py_o, r_cy_o

    out["NetSales_PriorYear_Actual"] = _net_sales_for_fiscal_year(
        work, fye_py, sort_keys, primary_row=r_py_o
    )
    out["NetSales_LatestYear_Actual"] = _net_sales_for_fiscal_year(
        work, fye_cy, sort_keys, primary_row=r_cy_o
    )

    out["OperatingProfit_PriorYear_Actual"] = _val_from_row(r_py_o, ["OP"], ["NCOP"])
    out["OperatingProfit_LatestYear_Actual"] = _val_from_row(r_cy_o, ["OP"], ["NCOP"])

    out["Profit_PriorYear_Actual"] = _val_from_row(r_py_p, ["NP"], ["NCNP"])
    out["Profit_LatestYear_Actual"] = _val_from_row(r_cy_p, ["NP"], ["NCNP"])

    r_ppy_o = _row_for_fye(fye_ppy)
    r_ppy_p = r_ppy_o
    out["NetSales_TwoYearsPrior_Actual"] = _net_sales_for_fiscal_year(
        work, fye_ppy, sort_keys, primary_row=r_ppy_o
    )
    out["OperatingProfit_TwoYearsPrior_Actual"] = _val_from_row(r_ppy_o, ["OP"], ["NCOP"])
    out["Profit_TwoYearsPrior_Actual"] = _val_from_row(r_ppy_p, ["NP"], ["NCNP"])

    _apply_forecasts_from_newest_disclosure_row_only(out, work, fye_py=fye_py, fye_cy=fye_cy, sort_keys=sort_keys)

    r_cy_eq = _row_for_fye(fye_cy)
    out["CashAndEquivalents_LatestFY"] = _val_from_row(r_cy_eq, ["CashEq"], ["CashEq"])
    if pd.isna(out["CashAndEquivalents_LatestFY"]):
        out["CashAndEquivalents_LatestFY"] = _first_numeric_from_sources(w_one, ["CashEq"])
    if pd.isna(out["CashAndEquivalents_LatestFY"]):
        out["CashAndEquivalents_LatestFY"] = _first_numeric_from_sources(work, ["CashEq"])
    out["Equity_LatestFY"] = _val_from_row(r_cy_eq, ["Eq"], ["NCEq"])
    if pd.isna(out["Equity_LatestFY"]):
        out["Equity_LatestFY"] = _first_numeric_from_sources(w_one, ["Eq", "NCEq"])
    if pd.isna(out["Equity_LatestFY"]):
        out["Equity_LatestFY"] = _first_numeric_from_sources(work, ["Eq", "NCEq"])

    out["EquityToAssetRatio"] = _val_from_row(r_cy_eq, ["EqAR"], ["NCEqAR"])
    if pd.isna(out["EquityToAssetRatio"]):
        out["EquityToAssetRatio"] = _first_numeric_from_sources(w_one, ["EqAR", "NCEqAR"])
    if pd.isna(out["EquityToAssetRatio"]):
        out["EquityToAssetRatio"] = _first_numeric_from_sources(work, ["EqAR", "NCEqAR"])

    out["NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"] = (
        _first_numeric_from_sources(work, ["ShOutFY"])
    )

    # 内部キー: yfinance との会計年度アライメント用（parquet 書き出し前に呼び出し側で除去）
    out["_jq_fye_latest"] = pd.Timestamp(fye_cy) if pd.notna(fye_cy) else pd.NaT
    out["_jq_fye_prior"] = pd.Timestamp(fye_py) if pd.notna(fye_py) else pd.NaT

    return out, None
