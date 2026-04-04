"""
screening_master.parquet から指定銘柄のスナップショットを YAML 出力する。

使い方:
  python extract_stock_snapshot.py --code 7256

出力: research/stocks/{code}_snapshot.yaml
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from jq_client_utils import normalize_code_4

PARQUET_PATH = Path("../../bi/outputs/screening_master.parquet")
OUTPUT_DIR = Path("../../research/stocks")


def _fmt_oku(val: float | None) -> str | None:
    """百万円単位の値を億円に変換して返す。"""
    if val is None or pd.isna(val):
        return None
    return f"{val / 1e8:.1f}億円"


def _fmt_ratio(val: float | None, pct: bool = False) -> str | None:
    if val is None or pd.isna(val):
        return None
    if pct:
        return f"{val * 100:.1f}%"
    return f"{val:.4f}"


def build_snapshot(row: pd.Series) -> dict:
    shares = row.get("NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock")
    return {
        "code": str(row["Code"]),
        "company": row.get("CompanyName"),
        "market": row.get("MarketCodeName"),
        "sector17": row.get("Sector17CodeName"),
        "sector33": row.get("Sector33CodeName"),
        "fiscal_year": str(row.get("FiscalYear", "")),
        "fiscal_quarter": str(row.get("FiscalQuarter", "")),
        "announcement_date": str(row.get("AnnouncementDate", ""))[:10],
        "price": {
            "close": float(row["Close"]) if not pd.isna(row.get("Close", float("nan"))) else None,
            "market_cap": _fmt_oku(row.get("MarketCap")),
        },
        "financials": {
            "net_sales": {
                "prior_actual": _fmt_oku(row.get("NetSales_PriorYear_Actual")),
                "latest_actual": _fmt_oku(row.get("NetSales_LatestYear_Actual")),
                "next_forecast": _fmt_oku(row.get("NetSales_NextYear_Forecast")),
            },
            "operating_profit": {
                "prior_actual": _fmt_oku(row.get("OperatingProfit_PriorYear_Actual")),
                "latest_actual": _fmt_oku(row.get("OperatingProfit_LatestYear_Actual")),
                "next_forecast": _fmt_oku(row.get("OperatingProfit_NextYear_Forecast")),
            },
            "net_income": {
                "prior_actual": _fmt_oku(row.get("Profit_PriorYear_Actual")),
                "latest_actual": _fmt_oku(row.get("Profit_LatestYear_Actual")),
                "next_forecast": _fmt_oku(row.get("Profit_NextYear_Forecast")),
            },
            "equity_ratio": _fmt_ratio(row.get("EquityToAssetRatio"), pct=True),
        },
        "shares_outstanding": int(shares) if shares and not pd.isna(shares) else None,
        "demand_supply": {
            "avg_daily_volume_5d": int(row["AvgDailyVolume5d"]) if not pd.isna(row.get("AvgDailyVolume5d", float("nan"))) else None,
            "long_margin_volume": int(row["LongMarginTradeVolume"]) if not pd.isna(row.get("LongMarginTradeVolume", float("nan"))) else None,
            "short_margin_volume": int(row["ShortMarginTradeVolume"]) if not pd.isna(row.get("ShortMarginTradeVolume", float("nan"))) else None,
            "short_ratio_to_outstanding": _fmt_ratio(row.get("ShortPositionsToSharesOutstandingRatio"), pct=True),
            "discretionary_investor": row.get("DiscretionaryInvestmentContractorName"),
        },
        "yfinance_supplemented": bool(row.get("YFinance_Supplemented", False)),
        "snapshot_date": date.today().isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="screening_master から銘柄スナップショットを YAML 出力")
    parser.add_argument("--code", required=True, help="証券コード（4桁）例: 7256")
    parser.add_argument("--parquet", default=str(PARQUET_PATH), help="parquet ファイルパス")
    args = parser.parse_args()

    code = normalize_code_4(args.code)
    df = pd.read_parquet(args.parquet)
    rows = df[df["Code"] == code]
    if rows.empty:
        raise ValueError(f"{code} が screening_master に見つかりません。")

    row = rows.iloc[0]
    snapshot = build_snapshot(row)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{code}_snapshot.yaml"
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(snapshot, f, allow_unicode=True, sort_keys=False, default_flow_style=False)

    print(f"保存完了: {out_path}")
    # サイズ確認
    size_kb = out_path.stat().st_size / 1024
    print(f"  サイズ: {size_kb:.1f} KB")


if __name__ == "__main__":
    main()
