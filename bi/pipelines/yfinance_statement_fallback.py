"""
J-Quants /fins/summary の履歴が薄い銘柄向けに、Yahoo Finance (yfinance) の年次損益・貸借対照表から
master 用の財務列を埋める。

- 銘柄コードは ``{Code4}.T``（yfinance_utils.jpx_code_to_yahoo_symbol と同じ）。
- 開示が十分ある銘柄では呼ばない想定（make_screening_master_v2 側で判定）。

注意:
  Yahoo の数値は連結・会計基準が J-Quants 非連結と異なることがある。
  「薄い」ときは直近2期の *年次* 列を Latest / Prior に対応させる（JQ の「今期予想」ベースの
  Latest 売上より、年次実績に揃うことが多い）。
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from update_statements import STATEMENT_NUMERIC_COLS
from yfinance_utils import jpx_code_to_yahoo_symbol


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)).strip())
    except (TypeError, ValueError):
        return default


def is_jquants_fins_history_thin(fin_df: pd.DataFrame | None) -> bool:
    """
    /fins/summary を code のみで取った DataFrame から「履歴が薄い」か判定。

    既定（環境変数で上書き可）:
      - 総行数 <= JQ_FINS_THIN_MAX_TOTAL_ROWS (20)
      - または FY 行数 < JQ_FINS_THIN_MIN_FY_ROWS (6)
    """
    if fin_df is None or fin_df.empty:
        return True
    max_rows = _env_int("JQ_FINS_THIN_MAX_TOTAL_ROWS", 20)
    min_fy = _env_int("JQ_FINS_THIN_MIN_FY_ROWS", 6)
    n = len(fin_df)
    fy_n = 0
    if "CurPerType" in fin_df.columns:
        fy_n = int((fin_df["CurPerType"].astype(str).str.upper().str.strip() == "FY").sum())
    if n <= max_rows:
        return True
    if fy_n > 0 and fy_n < min_fy:
        return True
    return False


def _pick_row(inc: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    idx_set = set(str(x) for x in inc.index)
    for c in candidates:
        if c in idx_set:
            return c
    for ix in inc.index:
        s = str(ix).strip().lower()
        for c in candidates:
            if c.lower() == s:
                return str(ix)
    return None


def build_statement_dict_from_yfinance(code4: str) -> dict[str, Any] | None:
    """
    yfinance の年次 income_stmt / balance_sheet / info から STATEMENT_NUMERIC_COLS 相当を構築。

    Returns:
        取得できた項目のみ埋めた dict。Ticker が無効・データ空なら None。
    """
    try:
        import yfinance as yf
    except ImportError:
        return None

    sym = jpx_code_to_yahoo_symbol(code4)
    try:
        t = yf.Ticker(sym)
        inc = t.income_stmt
        if inc is None or inc.empty:
            inc = t.financials
    except Exception:
        return None

    if inc is None or inc.empty or len(inc.columns) < 2:
        return None

    rev_key = _pick_row(inc, ("Total Revenue", "Operating Revenue", "TotalRevenue"))
    op_key = _pick_row(inc, ("Operating Income", "OperatingIncome"))
    ni_key = _pick_row(
        inc,
        (
            "Net Income",
            "Net Income Common Stockholders",
            "Net Income Including Noncontrolling Interests",
            "NetIncome",
        ),
    )
    if not rev_key or not op_key or not ni_key:
        return None

    # Yahoo 年次は「確定した直近の通期」まで（例: 2024-12 列）。翌通期（2025-12）は未確定のためここでは埋めない。
    # 株探の「昨年=直近確定通期」「今年=進行中通期」に合わせ、Prior のみ c0 を使う（130A で 360/194 の一年ズレを防ぐ）。
    cols = sorted(inc.columns, reverse=True)
    c0 = inc[cols[0]]
    if len(cols) < 1:
        return None

    def _v(ser: pd.Series, key: str) -> Any:
        if key not in inc.index:
            return pd.NA
        try:
            x = ser.loc[key]
        except Exception:
            return pd.NA
        n = pd.to_numeric(x, errors="coerce")
        return n if pd.notna(n) else pd.NA

    out: dict[str, Any] = {c: pd.NA for c in STATEMENT_NUMERIC_COLS}

    out["NetSales_PriorYear_Actual"] = _v(c0, rev_key)
    out["OperatingProfit_PriorYear_Actual"] = _v(c0, op_key)
    out["Profit_PriorYear_Actual"] = _v(c0, ni_key)
    # Latest は J-Quants（進行期の会社予想・本決算）に任せる

    # 貸借対照表: 自己資本比率（最新期）
    try:
        bal = t.balance_sheet
        if bal is not None and not bal.empty:
            bcols = sorted(bal.columns, reverse=True)
            bc = bal[bcols[0]]
            eq_keys = (
                "Stockholders Equity",
                "Common Stock Equity",
                "Total Equity Gross Minority Interest",
            )
            ta_key = "Total Assets"
            eq_k = _pick_row(bal, eq_keys)
            if eq_k and ta_key in bal.index:
                eq = pd.to_numeric(bc.loc[eq_k], errors="coerce")
                ta = pd.to_numeric(bc.loc[ta_key], errors="coerce")
                if pd.notna(eq) and pd.notna(ta) and float(ta) != 0:
                    out["EquityToAssetRatio"] = float(eq) / float(ta)
    except Exception:
        pass

    # 発行済株式数（概算）
    try:
        inf = t.info
        if isinstance(inf, dict):
            sh = inf.get("sharesOutstanding")
            n = pd.to_numeric(sh, errors="coerce")
            if pd.notna(n):
                out["NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"] = int(n)
    except Exception:
        pass

    # 予想は Yahoo 年次には無いことが多い → 触らない（呼び出し側で J-Quants をマージ）

    return out


def merge_jquants_with_yfinance_thin(
    jq: dict[str, Any],
    yahoo: dict[str, Any] | None,
    *,
    prefer_yahoo_actuals: bool = True,
) -> dict[str, Any]:
    """
    jq: aggregate_fins_summary_df の結果。yahoo: build_statement_dict_from_yfinance の結果。

    - **昨年**（Prior）の売上・OP・利益: Yahoo 年次の「直近確定通期」（薄い銘柄で株探の前年列に近い）
    - **今年**（Latest）の売上・OP・利益: **J-Quants 優先**（進行期の会社予想・FY 本決算。Yahoo は上書きしない）
    - 自己資本比率・株数: Yahoo で補完（欠損時のみ）
    - 予想列: jq を維持
    """
    if yahoo is None:
        return jq
    out = dict(jq)
    prior_keys = [
        "NetSales_PriorYear_Actual",
        "OperatingProfit_PriorYear_Actual",
        "Profit_PriorYear_Actual",
    ]
    latest_keys = [
        "NetSales_LatestYear_Actual",
        "OperatingProfit_LatestYear_Actual",
        "Profit_LatestYear_Actual",
    ]
    other_keys = [
        "EquityToAssetRatio",
        "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
    ]
    forecast_keys = [
        "NetSales_NextYear_Forecast",
        "OperatingProfit_NextYear_Forecast",
        "Profit_NextYear_Forecast",
    ]
    for k in prior_keys:
        yv = yahoo.get(k)
        if prefer_yahoo_actuals and pd.notna(yv):
            out[k] = yv
        elif pd.isna(out.get(k)) and pd.notna(yv):
            out[k] = yv
    for k in latest_keys:
        # Latest は J-Quants を維持（Yahoo の年次は確定分のみのため一年ズレる）
        jv = out.get(k)
        if pd.isna(jv):
            yv = yahoo.get(k)
            if pd.notna(yv):
                out[k] = yv
    for k in other_keys:
        yv = yahoo.get(k)
        jv = out.get(k)
        if prefer_yahoo_actuals and pd.notna(yv):
            out[k] = yv
        elif pd.isna(jv) and pd.notna(yv):
            out[k] = yv
    for k in forecast_keys:
        if k in out and pd.isna(out[k]):
            yv = yahoo.get(k)
            if pd.notna(yv):
                out[k] = yv
    return out
