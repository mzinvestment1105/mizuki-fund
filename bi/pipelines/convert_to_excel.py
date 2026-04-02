"""screening_master parquet → Excel（列の一部削除・日本語ヘッダ）"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path

import pandas as pd

try:
    from openpyxl.utils import get_column_letter
    from openpyxl import load_workbook

    _HAVE_OPENPYXL = True
except ImportError:  # pragma: no cover
    _HAVE_OPENPYXL = False

INPUT_PATH = Path("..") / "outputs" / "screening_master.parquet"
OUTPUT_PATH = Path("..") / "outputs" / "screening_master.xlsx"

DROP_COLS = [
    "DiscretionaryInvestmentContractorName",
    "ShortPositionsToSharesOutstandingRatio",
    "FiscalQuarter",
]

JP_HEADERS: dict[str, str] = {
    "Code": "銘柄コード",
    "CompanyName": "銘柄名",
    "MarketCodeName": "市場",
    "Sector17CodeName": "セクター17",
    "Sector33CodeName": "セクター33",
    "Close": "終値",
    "MarketCap": "時価総額",
    "YFinanceMarketCap": "時価総額_Yahoo",
    "YFinanceSharesOutstanding": "発行済株式数_Yahoo",
    "NetSales_PriorYear_Actual": "売上高_昨年通期実績",
    "NetSales_LatestYear_Actual": "売上高_今年通期実績",
    "NetSales_NextYear_Forecast": "売上高_来年通期予想",
    "OperatingProfit_PriorYear_Actual": "営業利益_昨年通期実績",
    "OperatingProfit_LatestYear_Actual": "営業利益_今年通期実績",
    "OperatingProfit_NextYear_Forecast": "営業利益_来年通期予想",
    "Profit_PriorYear_Actual": "最終益_昨年通期実績",
    "Profit_LatestYear_Actual": "最終益_今年通期実績",
    "Profit_NextYear_Forecast": "最終益_来年通期予想",
    "EquityToAssetRatio": "自己資本比率",
    "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock": "期末発行株式数（自己株含む）",
    "ShortMarginTradeVolume": "信用売り残",
    "LongMarginTradeVolume": "信用買い残",
    "AvgDailyVolume5d": "出来高_5日平均",
    "ShortPositionsInSharesNumber": "空売り残高（株数）",
    "AnnouncementDate": "決算発表予定日",
    "FiscalYear": "会計年度",
    "YFinance_Supplemented": "YF補完",
    "ETLRunId": "ETL実行ID",
    "ETLStartedAtUTC": "ETL開始時刻(UTC)",
    "ETLStartedAtJST": "ETL開始時刻(JST)",
}

# Excel 表示用（科学記数法・#### 緩和）
_NUM_FMT_INT = "#,##0"
_NUM_FMT_FLOAT = "#,##0.00"
_NUM_FMT_RATIO = "0.000"
_NUM_FMT_PERCENT_1DP = "0.0%"
_MIN_WIDTH_BY_JP_HEADER: dict[str, float] = {
    "決算発表予定日": 14,
    "会計年度": 12,
    "銘柄名": 28,
}


def _apply_excel_display_formats(path: Path, header_row: int = 1) -> None:
    """openpyxl で列幅・数値書式を付与（to_excel 直後のシートを想定）。"""
    if not _HAVE_OPENPYXL:
        return
    wb = load_workbook(path)
    ws = wb.active
    if ws.max_row < header_row or ws.max_column < 1:
        wb.save(path)
        return

    # 1行目固定（ヘッダ固定）
    ws.freeze_panes = f"A{header_row + 1}"

    headers: dict[str, int] = {}
    for col_idx in range(1, ws.max_column + 1):
        cell = ws.cell(header_row, col_idx)
        if cell.value is not None:
            headers[str(cell.value)] = col_idx

    money_jp = {
        "終値",
        "時価総額",
        "時価総額_Yahoo",
        "発行済株式数_Yahoo",
        "売上高_昨年通期実績",
        "売上高_今年通期実績",
        "売上高_来年通期予想",
        "営業利益_昨年通期実績",
        "営業利益_今年通期実績",
        "営業利益_来年通期予想",
        "最終益_昨年通期実績",
        "最終益_今年通期実績",
        "最終益_来年通期予想",
        "期末発行株式数（自己株含む）",
        "信用売り残",
        "信用買い残",
        "空売り残高（株数）",
        "出来高_5日平均",
    }

    # Q列（17列目）だけ % 表示（要望: 0.0%）
    percent_col_idx = 17
    if ws.max_column >= percent_col_idx:
        for r in range(header_row + 1, ws.max_row + 1):
            cell = ws.cell(r, percent_col_idx)
            if cell.value is not None and cell.value != "":
                cell.number_format = _NUM_FMT_PERCENT_1DP

        q_letter = get_column_letter(percent_col_idx)
        cur_q = ws.column_dimensions[q_letter].width
        if cur_q is None or cur_q < 12:
            ws.column_dimensions[q_letter].width = 12

    for jp, cidx in headers.items():
        letter = get_column_letter(cidx)
        if jp in money_jp:
            fmt = _NUM_FMT_INT
            for r in range(header_row + 1, ws.max_row + 1):
                cell = ws.cell(r, cidx)
                if cell.value is not None and cell.value != "":
                    cell.number_format = fmt
        elif jp == "自己資本比率":
            for r in range(header_row + 1, ws.max_row + 1):
                cell = ws.cell(r, cidx)
                if cell.value is not None and cell.value != "":
                    cell.number_format = _NUM_FMT_RATIO

        # 可能な範囲で列幅を自動調整（#### が出にくくする）
        # 文字数ベースで上限を設け、極端に広くならないようにする
        max_len = 0
        scan_rows = min(ws.max_row, header_row + 200)
        for r in range(header_row, scan_rows + 1):
            v = ws.cell(r, cidx).value
            if v is None:
                continue
            # 日付は YYYY-MM-DD 程度を想定
            if isinstance(v, (datetime, date)):
                s = v.strftime("%Y-%m-%d")
            else:
                s = str(v)
            if len(s) > max_len:
                max_len = len(s)

        wch = _MIN_WIDTH_BY_JP_HEADER.get(jp)
        if wch is not None:
            ws.column_dimensions[letter].width = max(wch, ws.column_dimensions[letter].width or 0, min(60, max_len + 2))
        else:
            # デフォルト幅が狭いと日付が #### になるため最低幅
            cur = ws.column_dimensions[letter].width
            if cur is None or cur < 10:
                ws.column_dimensions[letter].width = max(12, min(60, max_len + 2))
            else:
                ws.column_dimensions[letter].width = max(cur, min(60, max_len + 2))

    # 「全数値カンマ」: 明らかに数値列の列に書式をつける（Q列と日付は除外）
    for cidx in range(1, ws.max_column + 1):
        if cidx == percent_col_idx:
            continue

        # 見出しで日付っぽい列を除外（例: 決算発表予定日）
        header_val = ws.cell(header_row, cidx).value
        header_s = str(header_val) if header_val is not None else ""
        if "日" in header_s or "Date" in header_s:
            continue

        nonnull = 0
        numeric = 0
        any_float = False
        for r in range(header_row + 1, ws.max_row + 1):
            v = ws.cell(r, cidx).value
            if v is None or v == "":
                continue
            nonnull += 1
            if isinstance(v, bool):
                continue
            if isinstance(v, (datetime, date)):
                # 日付は除外
                numeric = 0
                nonnull = 0
                break
            if isinstance(v, (int, float)):
                numeric += 1
                if isinstance(v, float) and abs(v - int(v)) > 1e-9:
                    any_float = True

        if nonnull == 0:
            continue
        if numeric / nonnull < 0.8:
            continue

        fmt = _NUM_FMT_FLOAT if any_float else _NUM_FMT_INT
        for r in range(header_row + 1, ws.max_row + 1):
            cell = ws.cell(r, cidx)
            if cell.value is not None and cell.value != "":
                # 既に別書式を付けている列（例: 自己資本比率）は上書きしない
                if cell.number_format not in ("General", "0", "0.0", "0.00"):
                    continue
                cell.number_format = fmt

    wb.save(path)


def parquet_to_excel(
    inp: Path, outp: Path, *, max_rows: int | None = None
) -> tuple[Path, pd.DataFrame]:
    if not inp.exists():
        raise FileNotFoundError(f"input parquet not found: {inp}")

    df = pd.read_parquet(inp)
    if max_rows is not None and max_rows > 0:
        df = df.head(int(max_rows))
    outp.parent.mkdir(parents=True, exist_ok=True)

    for _col in ("ShortMarginTradeVolume", "LongMarginTradeVolume"):
        if _col in df.columns:
            df[_col] = pd.to_numeric(df[_col], errors="coerce")
    if "AvgDailyVolume5d" in df.columns:
        df["AvgDailyVolume5d"] = pd.to_numeric(df["AvgDailyVolume5d"], errors="coerce")

    drop = [c for c in DROP_COLS if c in df.columns]
    if drop:
        df = df.drop(columns=drop)

    rename = {k: v for k, v in JP_HEADERS.items() if k in df.columns}
    df = df.rename(columns=rename)

    # 予想列が NaN（予想未開示）の場合は「予想無し」と表示する
    _forecast_cols_jp = ["売上高_来年通期予想", "営業利益_来年通期予想", "最終益_来年通期予想"]
    for _fc in _forecast_cols_jp:
        if _fc in df.columns:
            df[_fc] = df[_fc].where(df[_fc].notna(), other="予想無し")

    try:
        df.to_excel(outp, index=False)
    except PermissionError as e:
        raise PermissionError(
            f"Excel ファイルに書けません（開いたままの Excel を閉じてください）: {outp}"
        ) from e
    try:
        _apply_excel_display_formats(outp)
    except Exception:
        # 表示調整は補助。失敗しても parquet は有効。
        pass
    return outp, df


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, default=INPUT_PATH)
    p.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = p.parse_args()
    out, _ = parquet_to_excel(args.input, args.output)
    print(out)


if __name__ == "__main__":
    main()
