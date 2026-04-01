"""
Debug: verify what value J-Quants /fins/details exposes for 130A around FY 2024-12.

We purposely do NOT "hunt other columns" in summary; we inspect the underlying
XBRL-derived FS keys in /fins/details for the same disclosure number, to see
what the source says for sales/revenue items.
"""

from __future__ import annotations

import os
import re
import sys

import jquantsapi
import pandas as pd

from jq_client_utils import get_json_with_429_backoff


def main() -> None:
    key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not key:
        raise SystemExit("JQUANTS_API_KEY is not set")

    client = jquantsapi.ClientV2(key)
    url = f"{client.JQUANTS_API_BASE}/fins/details"

    # FY disclosure date for 130A0 in /fins/summary was 2025-02-13 (DiscNo 20250212571016)
    payload = get_json_with_429_backoff(
        client,
        url,
        {"code": "130A0", "date": "2025-02-13"},
        max_attempts=3,
    )
    data = payload.get("data") or []
    print("batch_rows:", len(data))
    if not data:
        raise SystemExit("no data")

    df = pd.DataFrame(data)
    if "DiscNo" not in df.columns:
        raise SystemExit("missing DiscNo")

    disc_no = "20250212571016"
    sub = df[df["DiscNo"].astype(str).eq(disc_no)].copy()
    print("match_rows:", len(sub))
    if sub.empty:
        # Show what DiscNo are present to debug.
        print("available DiscNo:", sorted(df["DiscNo"].astype(str).unique().tolist())[:20])
        raise SystemExit("target DiscNo not found in this batch")

    r = sub.iloc[0]
    fs = r.get("FS") or {}
    print("DiscNo:", r.get("DiscNo"))
    print("DocType:", r.get("DocType"))

    keys = [k for k in fs.keys() if re.search(r"(sales|revenue)", k, re.IGNORECASE)]
    print("matched_keys:", len(keys))

    for k in sorted(keys):
        v = fs.get(k)
        n = pd.to_numeric(v, errors="coerce")
        if pd.notna(n):
            print(f"{k}: {int(n)}")

    print("\n-- amendment/meta flags --")
    for k in [
        "XBRL amendment flag, DEI",
        "Report amendment flag, DEI",
        "Amendment flag, DEI",
        "Type of current period, DEI",
        "Current fiscal year end date, DEI",
        "Comparative period end date, DEI",
        "Accounting standards, DEI",
        "Whether consolidated financial statements are prepared, DEI",
    ]:
        if k in fs:
            print(f"{k}: {fs[k]}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[Interrupted]", file=sys.stderr)
        raise

