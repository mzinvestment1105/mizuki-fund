"""
セクター週次レポート ETL
========================
出力:
  bi/outputs/sector_stock_weekly.parquet  … 銘柄別生データ（deep dive 用）
  bi/outputs/sector_weekly.parquet        … Sector17 集計

価格キャッシュ:
  bi/data/raw/sector_prices.parquet       … 全銘柄日次終値（増分更新）

実行:
  cd bi/pipelines
  python make_sector_report.py [--limit-codes N] [--skip-price-fetch]

環境変数:
  JQUANTS_API_KEY  … 必須
"""

from __future__ import annotations

import argparse
import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from jq_client_utils import (
    fetch_paginated_v2,
    normalize_code_4,
)

# ---------------------------------------------------------------------------
# パス定義
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
OUTPUTS_DIR = BASE_DIR / ".." / "outputs"
DATA_RAW_DIR = BASE_DIR / ".." / "data" / "raw"
SCREENING_MASTER_PATH = OUTPUTS_DIR / "screening_master.parquet"
PRICE_CACHE_PATH = DATA_RAW_DIR / "sector_prices.parquet"
OUT_STOCK_PATH = OUTPUTS_DIR / "sector_stock_weekly.parquet"
OUT_SECTOR_PATH = OUTPUTS_DIR / "sector_weekly.parquet"

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
LOOKBACK_YEARS = 3          # 価格キャッシュの取得範囲
WEEKLY_SLOTS = 8            # 週次スロット数（W01=最新週）
SNAPSHOT_LABELS = ["3M", "6M", "1Y", "2Y", "3Y"]
SNAPSHOT_DAYS   = [63,   126,  252,  504,  756]   # 営業日近似
REQUEST_SLEEP   = 1.2       # API リクエスト間隔(秒)


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _last_friday(d: date) -> date:
    """d 以前で最も近い金曜日（d が金曜なら d 自身）。"""
    offset = (d.weekday() - 4) % 7
    return d - timedelta(days=offset)


def _prior_friday(d: date, n: int) -> date:
    """_last_friday(d) の n 週前の金曜日。"""
    fri = _last_friday(d)
    return fri - timedelta(weeks=n)


def _nearest_close(prices: pd.DataFrame, target: date, tolerance_days: int = 7) -> pd.Series | None:
    """target 日に最も近い（前方向）終値行を返す。なければ None。"""
    sub = prices[prices["Date"] <= pd.Timestamp(target)]
    if sub.empty:
        return None
    row = sub.iloc[-1]
    if (target - row["Date"].date()).days > tolerance_days:
        return None
    return row


# ---------------------------------------------------------------------------
# Step 1: 価格キャッシュ（増分更新）
# ---------------------------------------------------------------------------

def _load_price_cache() -> pd.DataFrame:
    if PRICE_CACHE_PATH.exists():
        df = pd.read_parquet(PRICE_CACHE_PATH)
        df["Date"] = pd.to_datetime(df["Date"])
        df["Code"] = df["Code"].astype("string")
        return df
    return pd.DataFrame(columns=["Date", "Code", "C"])


def _save_price_cache(df: pd.DataFrame) -> None:
    PRICE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PRICE_CACHE_PATH, index=False)


def fetch_price_history(
    codes: list[str],
    *,
    limit_codes: int = 0,
) -> pd.DataFrame:
    """
    JQuants から全銘柄の日次終値を取得し、キャッシュに増分追記して返す。
    既存キャッシュより新しい日付のみ API 取得する。
    """
    import jquantsapi

    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise ValueError("JQUANTS_API_KEY が未設定です")
    client = jquantsapi.ClientV2(api_key=api_key)

    if limit_codes > 0:
        codes = codes[:limit_codes]

    cache = _load_price_cache()
    fetch_from = date.today() - timedelta(days=LOOKBACK_YEARS * 365 + 30)
    if not cache.empty and "Date" in cache.columns:
        cached_max = cache["Date"].max().date()
        if cached_max >= fetch_from:
            # キャッシュが既にある場合は翌日から増分取得
            fetch_from = cached_max + timedelta(days=1)

    today = date.today()
    if fetch_from > today:
        print(f"価格キャッシュは最新（{fetch_from} > {today}）、スキップ")
        return cache

    print(f"価格取得: {fetch_from} 〜 {today}、対象銘柄 {len(codes)} 件")
    frames: list[pd.DataFrame] = []
    failures: list[str] = []
    total = len(codes)

    for i, code in enumerate(codes, 1):
        try:
            time.sleep(REQUEST_SLEEP)
            rows = fetch_paginated_v2(
                client,
                "/equities/bars/daily",
                params={
                    "code": code,
                    "date_from": fetch_from.strftime("%Y-%m-%d"),
                    "date_to": today.strftime("%Y-%m-%d"),
                },
                sleep_seconds=0,  # 上でsleepしているのでここは0
            )
            if not rows:
                continue
            df = pd.DataFrame(rows)
            if "Date" not in df.columns or "C" not in df.columns:
                # カラム名が違う場合の吸収
                col_map = {}
                for col in df.columns:
                    if col.lower() in ("close", "c"):
                        col_map[col] = "C"
                    if col.lower() == "date":
                        col_map[col] = "Date"
                df = df.rename(columns=col_map)
            if "C" not in df.columns:
                continue
            df["Code"] = code
            df["Date"] = pd.to_datetime(df["Date"])
            df["C"] = pd.to_numeric(df["C"], errors="coerce")
            frames.append(df[["Date", "Code", "C"]])
        except Exception as e:
            failures.append(f"{code}: {e}")

        if i == 1 or i % 100 == 0 or i == total:
            print(f"  {i}/{total} (ok={len(frames)} fail={len(failures)})")

    if failures:
        print(f"失敗 {len(failures)} 件（先頭5）: {failures[:5]}")

    if frames:
        new_data = pd.concat(frames, ignore_index=True)
        new_data["Code"] = new_data["Code"].astype("string")
        combined = pd.concat([cache, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Date", "Code"]).sort_values(["Code", "Date"]).reset_index(drop=True)
        _save_price_cache(combined)
        print(f"価格キャッシュ保存: {PRICE_CACHE_PATH} ({len(combined)} 行)")
        return combined
    else:
        print("新規価格データなし、キャッシュをそのまま使用")
        return cache


# ---------------------------------------------------------------------------
# Step 2: 週次終値・リターン計算（銘柄別）
# ---------------------------------------------------------------------------

def compute_stock_returns(
    prices: pd.DataFrame,
    today: date,
) -> pd.DataFrame:
    """
    銘柄ごとに週次終値スナップショットとリターンを計算。
    prices: Date(datetime64), Code(string), C(float)
    """
    prices = prices.copy()
    prices["Date"] = pd.to_datetime(prices["Date"])
    prices = prices.sort_values(["Code", "Date"])

    results: list[dict] = []
    codes = prices["Code"].unique()
    total = len(codes)

    for i, code in enumerate(codes, 1):
        sub = prices[prices["Code"] == code].set_index("Date")["C"]
        if sub.empty:
            continue

        row: dict = {"Code": str(code)}

        # 最新終値
        latest_close = sub.iloc[-1]
        row["Close_Latest"] = latest_close

        # 週次スロット W01〜W08（W01=最新週の週末）
        for w in range(1, WEEKLY_SLOTS + 1):
            target = _prior_friday(today, w - 1)
            # target 以前で最も近い日の終値
            sub_before = sub[sub.index <= pd.Timestamp(target)]
            if sub_before.empty:
                row[f"Close_W{w:02d}"] = float("nan")
            else:
                row[f"Close_W{w:02d}"] = sub_before.iloc[-1]

        # 週次リターン（W01 = 直近の1週リターン = W01終値/W02終値 - 1）
        for w in range(1, WEEKLY_SLOTS + 1):
            c_cur = row.get(f"Close_W{w:02d}", float("nan"))
            c_prev = row.get(f"Close_W{w+1:02d}", float("nan"))
            if pd.notna(c_cur) and pd.notna(c_prev) and c_prev != 0:
                row[f"Return_W{w:02d}"] = c_cur / c_prev - 1
            else:
                row[f"Return_W{w:02d}"] = float("nan")

        # スナップショット終値・リターン（3M/6M/1Y/2Y/3Y）
        for label, bdays in zip(SNAPSHOT_LABELS, SNAPSHOT_DAYS):
            target = today - timedelta(days=int(bdays * 365 / 252))
            sub_before = sub[sub.index <= pd.Timestamp(target)]
            if sub_before.empty:
                row[f"Close_{label}"] = float("nan")
                row[f"Return_{label}"] = float("nan")
            else:
                snap_close = sub_before.iloc[-1]
                row[f"Close_{label}"] = snap_close
                if snap_close != 0 and pd.notna(snap_close) and pd.notna(latest_close):
                    row[f"Return_{label}"] = latest_close / snap_close - 1
                else:
                    row[f"Return_{label}"] = float("nan")

        results.append(row)

        if i % 500 == 0 or i == total:
            print(f"  リターン計算: {i}/{total}")

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Step 3: スクリーニングマスターと結合
# ---------------------------------------------------------------------------

def build_stock_table(
    returns_df: pd.DataFrame,
    master_df: pd.DataFrame,
    today: date,
    etl_run_id: str,
    etl_started_jst: str,
) -> pd.DataFrame:
    """
    returns_df（リターン計算結果）と screening_master を結合して
    sector_stock_weekly の全カラムを構築。
    """
    returns_df["Code"] = returns_df["Code"].astype("string")
    master_df = master_df.copy()
    master_df["Code"] = master_df["Code"].astype("string").str.strip()

    # ETL メタ除去（後で上書き）
    drop_meta = ["ETLRunId", "ETLStartedAtUTC", "ETLStartedAtJST"]
    master_df = master_df.drop(columns=[c for c in drop_meta if c in master_df.columns])

    merged = returns_df.merge(master_df, on="Code", how="left")

    # 時価総額ウェイト（セクター内）
    merged["MarketCap"] = pd.to_numeric(merged.get("MarketCap", pd.Series(dtype=float)), errors="coerce")
    sector_mcap = merged.groupby("Sector17CodeName")["MarketCap"].transform("sum")
    merged["MarketCap_Weight"] = merged["MarketCap"] / sector_mcap

    # 信用倍率（最新週）
    long_latest = pd.to_numeric(merged.get("LongMargin_WkSeq01", pd.Series(dtype=float)), errors="coerce")
    short_latest = pd.to_numeric(merged.get("ShortMargin_WkSeq01", pd.Series(dtype=float)), errors="coerce")
    merged["ShortMargin_Latest"] = short_latest
    merged["LongMargin_Latest"] = long_latest
    with pd.option_context("mode.chained_assignment", None):
        merged["MarginRatio"] = long_latest / short_latest.replace(0, float("nan"))

    # セクター内リターン順位（Return_W01 基準）
    merged["Return_Rank_InSector"] = (
        merged.groupby("Sector17CodeName")["Return_W01"]
        .rank(ascending=False, method="min", na_option="bottom")
        .astype("Int64")
    )

    # メタ
    merged["AsOf"] = today.isoformat()
    merged["ETLRunId"] = etl_run_id
    merged["ETLStartedAtJST"] = etl_started_jst

    return merged.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 4: セクター集計
# ---------------------------------------------------------------------------

def build_sector_table(
    stock_df: pd.DataFrame,
    today: date,
    etl_run_id: str,
    etl_started_jst: str,
) -> pd.DataFrame:
    """sector_stock_weekly から Sector17 集計を作る。"""

    sectors = stock_df["Sector17CodeName"].dropna().unique()
    rows: list[dict] = []

    for sector in sorted(sectors):
        sg = stock_df[stock_df["Sector17CodeName"] == sector].copy()
        row: dict = {"Sector17CodeName": sector}

        row["StockCount"] = len(sg)
        row["MarketCap_Total"] = sg["MarketCap"].sum(skipna=True)

        # 時価総額加重平均リターン
        w = sg["MarketCap_Weight"].fillna(0)
        w_sum = w.sum()

        for wk in range(1, WEEKLY_SLOTS + 1):
            col = f"Return_W{wk:02d}"
            if col in sg.columns:
                valid = sg[col].notna() & (w > 0)
                if valid.any() and w_sum > 0:
                    row[col] = (sg.loc[valid, col] * w[valid]).sum() / w[valid].sum()
                else:
                    row[col] = float("nan")

        for label in SNAPSHOT_LABELS:
            col = f"Return_{label}"
            if col in sg.columns:
                valid = sg[col].notna() & (w > 0)
                if valid.any():
                    row[col] = (sg.loc[valid, col] * w[valid]).sum() / w[valid].sum()
                else:
                    row[col] = float("nan")

        # 時価総額加重平均バリュエーション
        for val_col, out_col in [
            ("PER_Trailing", "PER_WAvg"),
            ("PBR_Trailing", "PBR_WAvg"),
            ("ROE_LatestYear", "ROE_WAvg"),
        ]:
            if val_col in sg.columns:
                sg[val_col] = pd.to_numeric(sg[val_col], errors="coerce")
                valid = sg[val_col].notna() & (w > 0)
                if valid.any():
                    row[out_col] = (sg.loc[valid, val_col] * w[valid]).sum() / w[valid].sum()
                else:
                    row[out_col] = float("nan")

        # 上位3・下位3銘柄（Return_W01 基準、1ヶ月近似）
        if "Return_W01" in sg.columns and "Return_W04" in sg.columns:
            # 1ヶ月リターン = 4週累積
            sg["_Return_1M"] = (
                (1 + sg["Return_W01"].fillna(0))
                * (1 + sg["Return_W02"].fillna(0))
                * (1 + sg["Return_W03"].fillna(0))
                * (1 + sg["Return_W04"].fillna(0))
                - 1
            )
        else:
            sg["_Return_1M"] = sg.get("Return_W01", float("nan"))

        def _fmt(r: pd.Series) -> str:
            parts = []
            for _, s in r.iterrows():
                pct = f"{s['_Return_1M']*100:.1f}%" if pd.notna(s.get("_Return_1M")) else "N/A"
                name = str(s.get("CompanyName", s["Code"]))[:10]
                parts.append(f"{s['Code']} {name}({pct})")
            return " / ".join(parts)

        sorted_asc = sg.dropna(subset=["_Return_1M"]).sort_values("_Return_1M")
        sorted_desc = sorted_asc[::-1]
        row["Top3_Return_1M"] = _fmt(sorted_desc.head(3))
        row["Bottom3_Return_1M"] = _fmt(sorted_asc.head(3))

        # 時価総額上位3銘柄
        top3_mcap = sg.dropna(subset=["MarketCap"]).nlargest(3, "MarketCap")
        row["Top3_MarketCap"] = " / ".join(
            f"{r['Code']} {str(r.get('CompanyName', ''))[:10]}"
            for _, r in top3_mcap.iterrows()
        )

        # 上位5銘柄の時価総額集中度
        top5_mcap = sg.dropna(subset=["MarketCap"]).nlargest(5, "MarketCap")["MarketCap"].sum()
        row["MarketCap_Ratio_Top5"] = top5_mcap / row["MarketCap_Total"] if row["MarketCap_Total"] > 0 else float("nan")

        row["AsOf"] = today.isoformat()
        row["ETLRunId"] = etl_run_id
        row["ETLStartedAtJST"] = etl_started_jst
        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit-codes", type=int, default=0, help="テスト用: 取得銘柄数上限")
    parser.add_argument("--skip-price-fetch", action="store_true", help="価格APIをスキップしてキャッシュのみ使用")
    args = parser.parse_args()

    etl_run_id = str(uuid.uuid4())
    jst = timezone(timedelta(hours=9))
    started_at = datetime.now(jst)
    etl_started_jst = started_at.strftime("%Y-%m-%d %H:%M:%S JST")
    today = date.today()

    print(f"=== セクター週次レポート ETL ===")
    print(f"実行日: {today}  RunId: {etl_run_id}")

    # --- ユニバース読み込み（screening_master から全銘柄取得）---
    master_df = pd.read_parquet(SCREENING_MASTER_PATH)
    master_df["Code"] = master_df["Code"].astype("string").str.strip().str[:4]
    codes = master_df["Code"].dropna().unique().tolist()
    print(f"ユニバース: {len(codes)} 銘柄（全市場）")

    # --- 価格キャッシュ取得 ---
    if args.skip_price_fetch:
        print("価格取得スキップ、キャッシュ読み込み")
        prices = _load_price_cache()
    else:
        prices = fetch_price_history(codes, limit_codes=args.limit_codes)

    if prices.empty:
        raise RuntimeError(
            "価格データがありません。\n"
            "初回は --skip-price-fetch なしで実行してください（全銘柄取得に数時間かかります）。\n"
            f"キャッシュ保存先: {PRICE_CACHE_PATH}"
        )

    prices["Code"] = prices["Code"].astype("string").str.strip().str[:4]
    print(f"価格データ: {len(prices)} 行、銘柄 {prices['Code'].nunique()} 件")

    # --- リターン計算 ---
    print("リターン計算中...")
    returns_df = compute_stock_returns(prices, today)
    print(f"リターン計算完了: {len(returns_df)} 銘柄")

    print(f"スクリーニングマスター: {len(master_df)} 行")

    # --- 銘柄テーブル構築 ---
    print("銘柄テーブル構築中...")
    stock_df = build_stock_table(returns_df, master_df, today, etl_run_id, etl_started_jst)

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    stock_df.to_parquet(OUT_STOCK_PATH, index=False)
    print(f"保存: {OUT_STOCK_PATH} ({len(stock_df)} 行, {len(stock_df.columns)} カラム)")

    # --- セクター集計 ---
    print("セクター集計中...")
    sector_df = build_sector_table(stock_df, today, etl_run_id, etl_started_jst)

    sector_df.to_parquet(OUT_SECTOR_PATH, index=False)
    print(f"保存: {OUT_SECTOR_PATH} ({len(sector_df)} 行, {len(sector_df.columns)} カラム)")

    # --- サマリー表示 ---
    print("\n=== セクター集計サマリー ===")
    display_cols = ["Sector17CodeName", "StockCount", "Return_W01", "Return_1Y", "PER_WAvg", "PBR_WAvg", "ROE_WAvg"]
    display_cols = [c for c in display_cols if c in sector_df.columns]
    print(sector_df[display_cols].to_string(index=False))
    print("\n完了！")


if __name__ == "__main__":
    main()
