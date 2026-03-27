"""
J-Quants v2 の日次バーから、プライム・スタンダード・グロース銘柄の前日比 ±threshold を抽出する。
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

import jquantsapi
import pandas as pd

from jq_client_utils import (
    fetch_paginated_v2,
    latest_trading_day_date_v2,
    normalize_code_4,
    previous_trading_day_date_v2,
)

# make_screening_master_v2 と同じユニバース
UNIVERSE_MARKET_NAMES = {"プライム", "スタンダード", "グロース"}

PROCESSED_DIR = Path("data") / "processed"


def _load_universe_master(client: jquantsapi.ClientV2, master_date: date) -> pd.DataFrame:
    dstr = master_date.strftime("%Y-%m-%d")
    rows = fetch_paginated_v2(client, "/equities/master", params={"date": dstr})
    df = pd.DataFrame.from_records(rows)
    if df.empty:
        return pd.DataFrame(
            columns=["Code", "CompanyName", "MarketCodeName", "Sector17CodeName", "Sector33CodeName"]
        )
    df = df.copy()
    df["Code"] = df["Code"].map(normalize_code_4).astype(str)
    df = df.rename(
        columns={
            "CoName": "CompanyName",
            "MktNm": "MarketCodeName",
            "S17Nm": "Sector17CodeName",
            "S33Nm": "Sector33CodeName",
        }
    )
    keep = ["Code", "CompanyName", "MarketCodeName", "Sector17CodeName", "Sector33CodeName"]
    df = df[[c for c in keep if c in df.columns]].copy()
    df = df[df["MarketCodeName"].isin(UNIVERSE_MARKET_NAMES)]
    return df.drop_duplicates("Code").reset_index(drop=True)


def _bars_close_df(client: jquantsapi.ClientV2, d: date) -> pd.DataFrame:
    dstr = d.strftime("%Y-%m-%d")
    rows = fetch_paginated_v2(client, "/equities/bars/daily", params={"date": dstr})
    px = pd.DataFrame.from_records(rows)
    if px.empty:
        return pd.DataFrame(columns=["Code", "Close"])
    px = px.copy()
    px["Code"] = px["Code"].map(normalize_code_4).astype(str)
    if "C" not in px.columns:
        raise ValueError("equities/bars/daily に終値列 C がありません")
    px["Close"] = pd.to_numeric(px["C"], errors="coerce")
    return px[["Code", "Close"]].drop_duplicates("Code", keep="last")


def scan_movers(
    client: jquantsapi.ClientV2,
    *,
    as_of: date | None = None,
    threshold: float = 0.10,
) -> tuple[pd.DataFrame, date, date]:
    """
    Returns: (movers_df, T, T_prev)
    """
    if as_of is None:
        t = latest_trading_day_date_v2(client)
    else:
        t = as_of
    t_prev = previous_trading_day_date_v2(client, before=t)

    universe = _load_universe_master(client, t)
    bars_t = _bars_close_df(client, t)
    bars_prev = _bars_close_df(client, t_prev)

    merged = universe.merge(bars_t, on="Code", how="inner", suffixes=("", "_T"))
    merged = merged.merge(
        bars_prev.rename(columns={"Close": "Close_prev"}),
        on="Code",
        how="inner",
    )
    merged = merged[merged["Close_prev"].notna() & (merged["Close_prev"] > 0)]
    merged = merged[merged["Close"].notna()]
    merged["return"] = merged["Close"] / merged["Close_prev"] - 1.0
    movers = merged[merged["return"].abs() >= threshold].copy()
    movers["direction"] = movers["return"].map(lambda r: "up" if r >= 0 else "down")
    movers = movers.sort_values("return", key=lambda s: s.abs(), ascending=False)
    return movers.reset_index(drop=True), t, t_prev


def main() -> None:
    p = argparse.ArgumentParser(description="日次 ±threshold 銘柄を J-Quants から抽出")
    p.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="基準営業日 YYYY-MM-DD（省略時は API から最新営業日）",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.10,
        help="絶対値の騰落率しきい値（既定 0.10 = 10%%）",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=PROCESSED_DIR,
        help="CSV/JSON の出力先（既定 data/processed）",
    )
    args = p.parse_args()

    print(
        "\n[scan_daily_movers] これから行うこと:\n"
        "  1) J-Quants API に接続し、基準日と前営業日の株価（全銘柄分の日次バー）を取得します。\n"
        "  2) プライム・スタンダード・グロースだけに絞り、前日比がしきい値以上の銘柄を列挙します。\n"
        "  3) data/processed に CSV と JSON を保存します。\n"
        "（API の利用制限に達すると待ち時間が長くなることがあります。）\n"
    )

    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("JQUANTS_API_KEY が未設定です。")

    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    client = jquantsapi.ClientV2(api_key=api_key)
    df, t, t_prev = scan_movers(client, as_of=as_of, threshold=args.threshold)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"movers_{t.isoformat()}"
    csv_path = args.out_dir / f"{stem}.csv"
    json_path = args.out_dir / f"{stem}.json"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    payload = {
        "as_of": t.isoformat(),
        "prev_day": t_prev.isoformat(),
        "threshold": args.threshold,
        "count": len(df),
        "movers": json.loads(df.to_json(orient="records", force_ascii=False)),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"T={t} T_prev={t_prev} movers={len(df)} threshold={args.threshold}")
    print(f"saved: {csv_path}")
    print(f"saved: {json_path}")


if __name__ == "__main__":
    main()
