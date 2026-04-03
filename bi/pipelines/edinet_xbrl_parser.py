"""
EDINET type=5 ZIP 内の XBRL_TO_CSV/*.csv から CF・連結財務サマリーを構造化して返す。

EDINET v2 API の type=5 は XBRL_TO_CSV 形式（UTF-16 TSV）を返す。
列: 要素ID / 項目名 / コンテキストID / 相対年度 / 連結・個別 / 期間・時点 / ユニットID / 単位 / 値
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from io import BytesIO
from typing import Any


# 取得したいCF・財務サマリー要素のローカル名パターン（部分一致、大文字小文字無視）
# IFRS・J-GAAP 両対応で複数候補を列挙
_CF_PATTERNS: list[tuple[str, list[str]]] = [
    ("operating_cf",  ["NetCashProvidedByUsedInOperatingActivities", "CashFlowsFromUsedInOperatingActivities"]),
    ("investing_cf",  ["NetCashProvidedByUsedInInvestingActivities", "CashFlowsFromUsedInInvestingActivities"]),
    ("financing_cf",  ["NetCashProvidedByUsedInFinancingActivities", "CashFlowsFromUsedInFinancingActivities"]),
    ("cash_end",      ["CashAndCashEquivalentsAtEndOfPeriod", "CashAndCashEquivalents"]),
]

_SUMMARY_PATTERNS: list[tuple[str, list[str]]] = [
    ("net_sales",          ["NetSales", "OperatingRevenues", "Revenue"]),
    ("operating_profit",   ["OperatingIncomeLoss", "OperatingProfit", "ProfitFromOperations"]),
    ("ordinary_profit",    ["OrdinaryIncomeLoss", "OrdinaryIncome"]),
    ("net_income",         ["ProfitLossAttributableToOwnersOfParent", "NetIncomeLoss", "ProfitLoss"]),
    ("total_assets",       ["Assets"]),
    ("equity",             ["NetAssets", "Equity"]),
    ("equity_ratio",       ["EquityToAssetRatio"]),
    ("eps",                ["BasicEarningsLossPerShare", "BasicEarningsPerShare"]),
    ("shares_outstanding", ["NumberOfIssuedAndOutstandingShares"]),
]


def _read_xbrl_csv(zip_bytes: BytesIO) -> tuple[list[dict[str, str]], str | None]:
    """
    XBRL_TO_CSV 内の jpcrp*.csv を読んで行リストと期末日を返す。
    期末日はファイル名の YYYY-MM-DD から抽出。
    """
    zip_bytes.seek(0)
    with zipfile.ZipFile(zip_bytes) as zf:
        targets = [n for n in zf.namelist() if "jpcrp" in n.lower() and n.endswith(".csv")]
        if not targets:
            return [], None
        target = targets[0]
        raw = zf.read(target)

    # ファイル名から期末日を抽出 (例: ..._2025-03-31_...)
    period_end: str | None = None
    m = re.search(r"_(\d{4}-\d{2}-\d{2})_", target)
    if m:
        period_end = m.group(1)

    text = raw.decode("utf-16")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t", quotechar='"')
    return list(reader), period_end


def _extract_values_from_csv(
    rows: list[dict[str, str]],
    patterns: list[tuple[str, list[str]]],
) -> dict[str, float | None]:
    """
    CSV 行リストから指定パターンに一致する当期・連結優先の値を抽出。
    """
    result: dict[str, float | None] = {key: None for key, _ in patterns}

    for key, pattern_list in patterns:
        candidates: list[tuple[bool, bool, str]] = []
        for row in rows:
            elem_id = row.get("要素ID", "")
            local = elem_id.split(":")[-1] if ":" in elem_id else elem_id
            val = row.get("値", "").strip()
            if not val:
                continue

            matched = any(p.lower() in local.lower() for p in pattern_list)
            if not matched:
                continue

            is_current = row.get("相対年度", "") == "当期"
            is_consolidated = row.get("連結・個別", "") == "連結"
            candidates.append((is_current, is_consolidated, val))

        if not candidates:
            continue
        # 当期 > 連結 の優先順でソート
        candidates.sort(key=lambda x: (not x[0], not x[1]))
        try:
            result[key] = float(candidates[0][2].replace(",", ""))
        except ValueError:
            result[key] = None

    return result


def parse_xbrl_zip(zip_bytes: BytesIO) -> dict[str, Any]:
    """
    EDINET XBRL ZIP（type=5）を受け取り、CF・財務サマリーを dict で返す。
    取得できなかった項目は None。
    """
    try:
        rows, period_end = _read_xbrl_csv(zip_bytes)
    except Exception as e:
        return {"error": f"CSV read error: {e}"}

    if not rows:
        return {"error": "XBRL CSV not found in ZIP"}

    cf = _extract_values_from_csv(rows, _CF_PATTERNS)
    summary = _extract_values_from_csv(rows, _SUMMARY_PATTERNS)

    return {
        "period_end": period_end,
        "cashflow": cf,
        "summary": summary,
    }
