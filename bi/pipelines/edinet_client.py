"""
EDINET API v2 クライアント。
書類一覧の検索・ZIP/PDFダウンロード。
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from io import BytesIO
from typing import Any

import requests


EDINET_API_BASE = "https://api.edinet-fsa.go.jp/api/v2"

# docTypeCode（主要なもの）
DOC_TYPE_ANNUAL = "120"         # 有価証券報告書
DOC_TYPE_QUARTERLY = "140"      # 四半期報告書
DOC_TYPE_LARGE_HOLDING = "030"  # 大量保有報告書

# type パラメータ（ダウンロード種別）
FILE_TYPE_PDF = 2
FILE_TYPE_XBRL = 5


def _get(url: str, params: dict[str, Any], *, api_key: str, timeout: int = 90) -> requests.Response:
    p = dict(params)
    p["Subscription-Key"] = api_key
    resp = requests.get(url, params=p, timeout=timeout)
    resp.raise_for_status()
    return resp


def get_document_list(target_date: date, api_key: str) -> list[dict[str, Any]]:
    """指定日に提出された書類一覧を返す。"""
    url = f"{EDINET_API_BASE}/documents.json"
    resp = _get(url, {"date": target_date.isoformat(), "type": 2}, api_key=api_key)
    return resp.json().get("results") or []


def find_latest_filing(
    sec_code: str,
    api_key: str,
    *,
    doc_type_codes: list[str] | None = None,
    lookback_days: int = 400,
    sleep_seconds: float = 0.5,
) -> dict[str, Any] | None:
    """
    証券コードから最新の有報（または指定書類種別）のメタデータを返す。
    直近 lookback_days 日分を新しい順に検索。
    """
    if doc_type_codes is None:
        doc_type_codes = [DOC_TYPE_ANNUAL]

    code4 = str(sec_code).strip()[:4]
    code5 = code4 + "0"  # EDINET は5桁で格納していることがある

    today = date.today()
    for i in range(lookback_days):
        d = today - timedelta(days=i)
        try:
            docs = get_document_list(d, api_key)
        except requests.HTTPError as e:
            print(f"EDINET API error ({d}): {e}")
            time.sleep(sleep_seconds * 4)
            continue

        for doc in docs:
            sc = str(doc.get("secCode") or "").strip()
            dtc = str(doc.get("docTypeCode") or "").strip()
            withdrawn = str(doc.get("withdrawalStatus") or "0").strip()
            if sc not in (code4, code5):
                continue
            if dtc not in doc_type_codes:
                continue
            if withdrawn == "1":
                continue
            return doc

        time.sleep(sleep_seconds)

    return None


def download_document(doc_id: str, api_key: str, *, file_type: int) -> BytesIO:
    """書類ファイルをダウンロードして BytesIO で返す。"""
    url = f"{EDINET_API_BASE}/documents/{doc_id}"
    resp = _get(url, {"type": file_type}, api_key=api_key, timeout=120)
    return BytesIO(resp.content)
