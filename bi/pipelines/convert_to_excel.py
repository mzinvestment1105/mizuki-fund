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
    "NetSales_TwoYearsPrior_Actual": "売上高_一昨年通期実績",
    "OperatingProfit_TwoYearsPrior_Actual": "営業利益_一昨年通期実績",
    "Profit_TwoYearsPrior_Actual": "最終益_一昨年通期実績",
    "CashAndEquivalents_LatestFY": "現金及び現金同等物_直近期末",
    "Equity_LatestFY": "純資産額_直近期末",
    "PER_Trailing": "PER_実績ベース",
    "PBR_Trailing": "PBR_実績ベース",
    "ROE_LatestYear": "ROE_今期実績",
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
# #,##0 はロケールの千桁区切り（日本語環境ではカンマ）
_NUM_FMT_INT = "#,##0"
_NUM_FMT_FLOAT = "#,##0.00"
# セル値は 0〜1（例: 0.352 → 35.20%）。既に 35.2 のような百分率の数値は 100 で割ってから保存する。
_NUM_FMT_PERCENT = "0.00%"
_MIN_WIDTH_BY_JP_HEADER: dict[str, float] = {
    "決算発表予定日": 14,
    "会計年度": 12,
    "銘柄名": 28,
}
# 大金額・カンマ付きで #### にならないよう最低幅（文字相当）
_MIN_WIDTH_NUMERIC = 14
_MIN_WIDTH_MONEY = 16
_MAX_COL_WIDTH = 55


def _estimate_display_chars(v: object, *, prefer_float: bool = False) -> int:
    """セル表示のおおよその文字数（列幅の目安）。千桁カンマ付きを想定。"""
    if v is None or v == "":
        return 0
    if isinstance(v, bool):
        return 5
    if isinstance(v, (datetime, date)):
        return 12
    if isinstance(v, str):
        return len(v)
    if isinstance(v, (int, float)):
        try:
            fv = float(v)
            if fv != fv:  # nan
                return 0
            if abs(fv) >= 1e15:
                return 14
            if prefer_float or (isinstance(v, float) and abs(fv - round(fv)) > 1e-6):
                return len(f"{fv:,.2f}")
            return len(f"{int(round(fv)):,}")
        except (OverflowError, ValueError):
            return len(str(v))
    return len(str(v))


def _equity_ratio_as_excel_fraction(raw: object) -> float | None:
    """自己資本比率を Excel の % 書式用（0〜1）にそろえる。"""
    try:
        fv = float(raw)
    except (TypeError, ValueError):
        return None
    if fv != fv:  # nan
        return None
    if fv > 1.0 + 1e-6:
        return fv / 100.0
    return fv


def _estimate_percent_display_chars(v: object) -> int:
    fr = _equity_ratio_as_excel_fraction(v)
    if fr is None:
        return 0
    return max(len(f"{fr * 100:.2f}%"), 7)


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
        "売上高_一昨年通期実績",
        "営業利益_一昨年通期実績",
        "最終益_一昨年通期実績",
        "現金及び現金同等物_直近期末",
        "純資産額_直近期末",
        "期末発行株式数（自己株含む）",
        "信用売り残",
        "信用買い残",
        "空売り残高（株数）",
        "出来高_5日平均",
    }
    ratio_metric_jp = {
        "PER_実績ベース",
        "PBR_実績ベース",
        "ROE_今期実績",
    }

    for jp, cidx in headers.items():
        letter = get_column_letter(cidx)
        if jp in money_jp:
            fmt = _NUM_FMT_INT
            for r in range(header_row + 1, ws.max_row + 1):
                cell = ws.cell(r, cidx)
                if cell.value is not None and cell.value != "":
                    cell.number_format = fmt
        elif jp in ratio_metric_jp:
            for r in range(header_row + 1, ws.max_row + 1):
                cell = ws.cell(r, cidx)
                if cell.value is not None and cell.value != "":
                    cell.number_format = _NUM_FMT_FLOAT
        elif jp == "自己資本比率":
            for r in range(header_row + 1, ws.max_row + 1):
                cell = ws.cell(r, cidx)
                if cell.value is not None and cell.value != "":
                    fr = _equity_ratio_as_excel_fraction(cell.value)
                    if fr is not None:
                        cell.value = fr
                    cell.number_format = _NUM_FMT_PERCENT

    # 銘柄コードやIDは整数でもカンマ付けしない（見た目が崩れるため）
    _skip_auto_numeric_headers = {"銘柄コード", "ETL実行ID"}

    # 「全数値カンマ」: 上記以外の数値主体列（日付・テキスト列は除外）
    for cidx in range(1, ws.max_column + 1):
        header_val = ws.cell(header_row, cidx).value
        header_s = str(header_val) if header_val is not None else ""
        if header_s in _skip_auto_numeric_headers:
            continue
        if "日" in header_s or "Date" in header_s or "時刻" in header_s:
            continue
        if header_s in money_jp or header_s in ratio_metric_jp or header_s == "自己資本比率":
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
                numeric = 0
                nonnull = 0
                break
            if isinstance(v, (int, float)):
                numeric += 1
                if isinstance(v, float) and abs(v - int(v)) > 1e-9:
                    any_float = True
            elif isinstance(v, str) and v != "予想無し":
                # 銘柄名などと混在する列は数値列扱いしない
                numeric = 0
                nonnull = 0
                break

        if nonnull == 0:
            continue
        if numeric / nonnull < 0.8:
            continue

        fmt = _NUM_FMT_FLOAT if any_float else _NUM_FMT_INT
        for r in range(header_row + 1, ws.max_row + 1):
            cell = ws.cell(r, cidx)
            if cell.value is not None and cell.value != "":
                if cell.number_format not in ("General", "0", "0.0", "0.00"):
                    continue
                cell.number_format = fmt

    # 列幅: 全データ行を走査し #### 回避（ヘッダ長・カンマ付き表示幅の大きい方）
    for jp, cidx in headers.items():
        letter = get_column_letter(cidx)
        hdr_w = len(jp) + 3
        max_cell = hdr_w
        floor = _MIN_WIDTH_BY_JP_HEADER.get(jp, 10)
        if jp in money_jp:
            floor = max(floor, _MIN_WIDTH_MONEY)
        elif jp in ratio_metric_jp or jp == "自己資本比率":
            floor = max(floor, _MIN_WIDTH_NUMERIC)

        prefer_float = jp in ratio_metric_jp
        is_eq_ratio = jp == "自己資本比率"

        for r in range(header_row + 1, ws.max_row + 1):
            v = ws.cell(r, cidx).value
            if v is None or v == "":
                continue
            if is_eq_ratio:
                wch = _estimate_percent_display_chars(v)
            else:
                wch = _estimate_display_chars(v, prefer_float=prefer_float)
            if wch > max_cell:
                max_cell = wch

        target = min(_MAX_COL_WIDTH, max(max_cell + 2, floor, hdr_w))
        ws.column_dimensions[letter].width = float(target)

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
