import argparse
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import jquantsapi
import pandas as pd

from jq_client_utils import (
    fetch_paginated_v2 as _fetch_paginated_v2,
    latest_trading_day_date_v2 as _latest_trading_day_date_v2,
    previous_trading_day_date_v2 as _previous_trading_day_date_v2,
    normalize_code_4 as _normalize_code_4,
)
from short_sale_utils import (
    QUERY_DISC_DATE_COL,
    aggregate_short_sale_monthly_pool,
)
from update_statements import (
    CRITICAL_COLS,
    STATEMENT_NUMERIC_COLS,
    aggregate_fins_summary_df,
    fins_summary_code_variants,
)
from yfinance_statement_fallback import (
    build_statement_dict_from_yfinance,
    is_jquants_fins_history_thin,
    merge_jquants_with_yfinance_thin,
)
from yfinance_utils import fetch_yfinance_market_snapshot


OUTPUT_PATH = Path("..") / "outputs" / "screening_master.parquet"
TEST_OUTPUT_PATH = Path("..") / "outputs" / "screening_master_test.parquet"
TEST_EXCEL_PATH = Path("..") / "outputs" / "screening_master_test.xlsx"
YFINANCE_AUDIT_PATH = Path("..") / "outputs" / "yfinance_audit.parquet"

UNIVERSE_MARKET_NAMES = {"プライム", "スタンダード", "グロース"}


def _to_numeric_df(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _gap_reason_series(api_err: pd.Series, field_note: pd.Series) -> pd.Series:
    """財務欠損行の分類（API失敗 / 部分欠損メモあり / ログなし）。"""
    ae = api_err.astype(object).where(api_err.notna(), "")
    fn = field_note.astype(object).where(field_note.notna(), "")
    ae_s = ae.astype(str).str.strip()
    fn_s = fn.astype(str).str.strip()
    out = pd.Series("fins_critical_all_na_no_api_log", index=api_err.index, dtype=object)
    out = out.mask(ae_s.ne(""), "fins_summary_api_fail")
    out = out.mask(ae_s.eq("") & fn_s.ne(""), "fins_summary_field_issue_or_empty_aggregate")
    return out


def _build_fins_data_gaps_df(
    master: pd.DataFrame,
    stmt_failures: list[tuple[str, str]],
    stmt_field_issues: list[tuple[str, str]],
) -> pd.DataFrame:
    """
    CRITICAL_COLS（売上・OP・純利益・自己資本比率・期末発行済株数）がすべて欠損の行だけ抽出。
    /fins/summary 失敗・部分欠損のメッセージを付与（財務だけ空の銘柄の一覧用）。
    """
    crit = list(CRITICAL_COLS)
    m = master.copy()
    for c in crit:
        if c not in m.columns:
            m[c] = pd.NA
    mask = m[crit].isna().all(axis=1)
    out = m.loc[mask].copy()
    if out.empty:
        return out

    fmap = {str(code): msg for code, msg in stmt_failures}
    imap = {str(code): msg for code, msg in stmt_field_issues}
    cstr = out["Code"].astype(str)
    out["fins_summary_api_error"] = cstr.map(fmap)
    out["fins_summary_field_note"] = cstr.map(imap)
    out["gap_reason"] = _gap_reason_series(out["fins_summary_api_error"], out["fins_summary_field_note"])

    front = [
        "Code",
        "CompanyName",
        "MarketCodeName",
        "Sector17CodeName",
        "Sector33CodeName",
        "gap_reason",
        "fins_summary_api_error",
        "fins_summary_field_note",
        "Close",
    ]
    front = [c for c in front if c in out.columns]
    rest = [c for c in out.columns if c not in front]
    return out[front + rest].sort_values("Code", kind="mergesort").reset_index(drop=True)


def _short_sale_institution_names_concat(g: pd.DataFrame, inst_col: str) -> str:
    """機関名を重複なく、出現順で「、」連結（空・「-」は除外）。"""
    parts: list[str] = []
    seen: set[str] = set()
    for raw in g[inst_col].tolist():
        s = str(raw).strip() if raw is not None else ""
        if not s or s in ("-", "nan", "None"):
            continue
        if s not in seen:
            seen.add(s)
            parts.append(s)
    return "、".join(parts)


def _last_friday(d: date) -> date:
    days_since_friday = (d.weekday() - 4) % 7
    return d - timedelta(days=days_since_friday)


def _fetch_fins_summary_rows_for_code(
    client: Any,
    code4: str,
    *,
    sleep_seconds: float = 1.2,
) -> list[Any]:
    """
    /fins/summary を **code パラメータのみ**で取得（4桁 / 5桁末尾0 の順）。

    日付さかのぼり（code+date・全市場 date）は **行わない**（遅い・429 のため削除）。
    code で 0 件の銘柄は空リスト → 集約は欠損のまま。
    """
    variants = fins_summary_code_variants(code4)
    for code_try in variants:
        rows = _fetch_paginated_v2(
            client,
            "/fins/summary",
            params={"code": code_try},
            sleep_seconds=sleep_seconds,
        )
        if rows:
            return rows
    return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="スクリーニング用 master parquet 生成（J-Quants v2 API）"
    )
    parser.add_argument(
        "--code",
        type=str,
        default="",
        help="4桁銘柄だけ処理（検証用）。出力は screening_master_test.parquet（例: --code 1414）。"
        " 未指定かつ --limit も無いときだけ環境変数 SCREENING_TEST_CODE を参照。",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="universe を銘柄コード昇順の先頭 N 件だけ処理（時間短縮）。出力は screening_master_limit{N}.parquet。"
        " --code と併用不可。例: --limit 100",
    )
    parser.add_argument(
        "--no-excel",
        action="store_true",
        help="Excel（.xlsx）を出さない。省略時は parquet と同名の .xlsx も出力する（要 openpyxl）。",
    )
    parser.add_argument(
        "--excel",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--no-data-gaps",
        action="store_true",
        help="財務欠損銘柄の別ファイル（*_data_gaps.parquet）を出さない。",
    )
    parser.add_argument(
        "--yfinance",
        action="store_true",
        help="yfinance で marketCap / sharesOutstanding を取得し列を追加。"
        " 値がある銘柄は MarketCap・期末発行済株数を Yahoo 値で上書き（J-Quants 計算は欠損時のみ）。"
        " 要: pip install yfinance。間隔は環境変数 YFINANCE_SLEEP（秒、既定0.35）。",
    )
    parser.add_argument(
        "--no-yfinance-statements",
        action="store_true",
        help="J-Quants /fins/summary の行数が少ない銘柄でも、Yahoo 年次損益へフォールバックしない。"
        " 省略時は yfinance が入っていれば自動（YFINANCE_STATEMENT_FALLBACK=0 で無効化）。",
    )
    args = parser.parse_args()
    etl_started_at_utc = datetime.now(timezone.utc).replace(microsecond=0)
    jst = timezone(timedelta(hours=9))
    etl_started_at_jst = etl_started_at_utc.astimezone(jst)
    etl_started_at_utc_str = etl_started_at_utc.isoformat()
    etl_started_at_jst_str = etl_started_at_jst.isoformat()
    etl_run_id = etl_started_at_jst.strftime("%Y%m%d-%H%M%S")
    want_excel = not args.no_excel
    _yf_stmt_env = os.environ.get("YFINANCE_STATEMENT_FALLBACK", "1").strip().lower()
    use_yfinance_statement_fallback = (not args.no_yfinance_statements) and _yf_stmt_env not in (
        "0",
        "false",
        "no",
        "off",
    )

    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not api_key:
        raise ValueError("JQUANTS_API_KEY が未設定です。")

    limit_n = max(0, int(args.limit))
    # --limit のときは環境変数 SCREENING_TEST_CODE を無視（残っていると単一銘柄と衝突する）
    test_code_raw = (args.code or "").strip()
    if limit_n > 0 and test_code_raw:
        raise ValueError("--code と --limit は同時に指定できません。")
    if not test_code_raw and limit_n == 0:
        test_code_raw = os.environ.get("SCREENING_TEST_CODE", "").strip()
    test_code_norm = _normalize_code_4(test_code_raw) if test_code_raw else ""

    if test_code_norm:
        out_path = TEST_OUTPUT_PATH
    elif limit_n > 0:
        out_path = OUTPUT_PATH.with_name(f"screening_master_limit{limit_n}.parquet")
    else:
        out_path = OUTPUT_PATH

    client = jquantsapi.ClientV2(api_key=api_key)

    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    # 1) listed/info (v2: equities/master)
    print(f"listed: date={today_str}")
    eq_master_rows = _fetch_paginated_v2(
        client, "/equities/master", params={"date": today_str}, sleep_seconds=1.2
    )
    eq_master_df = pd.DataFrame.from_records(eq_master_rows)

    if eq_master_df.empty:
        universe_df = pd.DataFrame(
            columns=["Code", "CompanyName", "MarketCodeName", "Sector17CodeName", "Sector33CodeName"]
        )
    else:
        eq_master_df = eq_master_df.copy()
        eq_master_df["Code"] = eq_master_df["Code"].map(_normalize_code_4).astype(str)
        eq_master_df = eq_master_df.rename(
            columns={
                "CoName": "CompanyName",
                "MktNm": "MarketCodeName",
                "S17Nm": "Sector17CodeName",
                "S33Nm": "Sector33CodeName",
            }
        )
        universe_df = eq_master_df[
            ["Code", "CompanyName", "MarketCodeName", "Sector17CodeName", "Sector33CodeName"]
        ].copy()
        universe_df = universe_df[universe_df["MarketCodeName"].isin(UNIVERSE_MARKET_NAMES)]
        universe_df = universe_df.drop_duplicates("Code").reset_index(drop=True)

    codes = universe_df["Code"].dropna().astype(str).unique().tolist()
    if test_code_norm:
        u_before = len(universe_df)
        universe_df = universe_df.loc[universe_df["Code"].astype(str) == test_code_norm].copy()
        if universe_df.empty:
            raise ValueError(
                f"--code / SCREENING_TEST_CODE={test_code_raw!r} は universe に存在しません "
                f"(正規化後={test_code_norm!r}, master件数={u_before})。"
            )
        codes = [test_code_norm]
        print(f"単一銘柄モード: {test_code_norm} のみ（出力: {out_path}）")
    elif limit_n > 0:
        universe_df = (
            universe_df.sort_values("Code", kind="mergesort").head(limit_n).reset_index(drop=True)
        )
        codes = universe_df["Code"].dropna().astype(str).tolist()
        print(f"--limit {limit_n}: 先頭 {len(codes)} 銘柄のみ処理（出力: {out_path}）")

    print(f"universe codes: {len(codes)}")

    # 1b) short-sale-report（財務・信用枠より先）
    # --limit などで先に /fins/summary を大量に叩いた直後に空売りを取ると 429 等でページが欠け、
    # プール行数が減って株数が小さく出ることがある。universe 確定後すぐ全市場を取得し、
    # 集計後に codes_set だけ残す（銘柄別の合算は独立のため数値は一致する）。
    short_sale_back_days = max(0, int(os.environ.get("SHORT_SALE_LOOKBACK_DAYS", "30")))
    ss_sleep = float(os.environ.get("SHORT_SALE_SLEEP", "1.2"))
    codes_set = set(universe_df["Code"].astype(str)) if not universe_df.empty else set()
    ss_frames: list[pd.DataFrame] = []
    for i in range(0, short_sale_back_days + 1):
        d_scan = today - timedelta(days=i)
        disc_date = d_scan.strftime("%Y-%m-%d")
        ss_rows = _fetch_paginated_v2(
            client,
            "/markets/short-sale-report",
            params={"disc_date": disc_date},
            sleep_seconds=ss_sleep,
        )
        if not ss_rows:
            continue
        tmp = pd.DataFrame.from_records(ss_rows)
        if tmp.empty:
            continue
        tmp = tmp.copy()
        tmp["Code"] = tmp["Code"].map(_normalize_code_4).astype(str)
        tmp[QUERY_DISC_DATE_COL] = disc_date
        ss_frames.append(tmp)
        n_u = int(tmp["Code"].isin(codes_set).sum()) if codes_set else len(tmp)
        print(f"short_sale-report: disc_date={disc_date} rows_all={len(tmp)} rows_in_universe={n_u}")

    if not ss_frames:
        ss_df = pd.DataFrame(
            columns=[
                "Code",
                "DiscretionaryInvestmentContractorName",
                "ShortPositionsToSharesOutstandingRatio",
                "ShortPositionsInSharesNumber",
            ]
        )
    else:
        ss_df = pd.concat(ss_frames, ignore_index=True)
        ss_df = ss_df.copy()
        ss_df["Code"] = ss_df["Code"].map(_normalize_code_4).astype(str)
        ss_df = ss_df.rename(
            columns={
                "DICName": "DiscretionaryInvestmentContractorName",
                "ShrtPosToSO": "ShortPositionsToSharesOutstandingRatio",
                "ShrtPosShares": "ShortPositionsInSharesNumber",
            }
        )
        ss_df = _to_numeric_df(
            ss_df,
            ["ShortPositionsToSharesOutstandingRatio", "ShortPositionsInSharesNumber"],
        )
        print(
            f"short_sale: pool rows={len(ss_df)} lookback_days={short_sale_back_days} "
            f"(全市場→各 inst_key 最新1行→銘柄合算→universe のみ出力)"
        )

        inst_col = "DiscretionaryInvestmentContractorName"
        shares_col = "ShortPositionsInSharesNumber"
        ratio_col = "ShortPositionsToSharesOutstandingRatio"

        inst_dedup, total_shares, ratio_max = aggregate_short_sale_monthly_pool(ss_df)
        if codes_set:
            _cs = codes_set
            inst_dedup = inst_dedup[inst_dedup["Code"].astype(str).isin(_cs)]
            total_shares = total_shares[total_shares["Code"].astype(str).isin(_cs)]
            ratio_max = ratio_max[ratio_max["Code"].astype(str).isin(_cs)]

        names_rows: list[dict[str, Any]] = []
        for code, g in inst_dedup.groupby("Code", sort=False):
            names_rows.append(
                {
                    "Code": code,
                    "DiscretionaryInvestmentContractorName": _short_sale_institution_names_concat(g, inst_col),
                }
            )
        names_df = (
            pd.DataFrame(names_rows)
            if names_rows
            else pd.DataFrame(columns=["Code", "DiscretionaryInvestmentContractorName"])
        )

        ss_df = total_shares.merge(names_df, on="Code", how="left").merge(ratio_max, on="Code", how="left")

        ss_df = ss_df[
            [
                "Code",
                "DiscretionaryInvestmentContractorName",
                "ShortPositionsToSharesOutstandingRatio",
                "ShortPositionsInSharesNumber",
            ]
        ].copy()

    # 2) fins/statements (v2: fins/summary) latest per Code
    statement_latest_rows: list[pd.DataFrame] = []
    stmt_failures: list[tuple[str, str]] = []
    stmt_field_issues: list[tuple[str, str]] = []
    yf_audit_rows: list[dict] = []  # Yahoo Finance 補完監査ログ
    _fins_sleep = float(os.environ.get("FINS_SUMMARY_FALLBACK_SLEEP", "1.2"))
    print(
        "fins/summary: code のみ取得（日付さかのぼりなし）"
        + (
            " | 薄い銘柄→Yahoo年次損益で補完（要 yfinance / YFINANCE_STATEMENT_FALLBACK=0 で無効）"
            if use_yfinance_statement_fallback
            else ""
        )
    )

    for i, code4 in enumerate(codes, start=1):
        try:
            # 銘柄ごとに /fins/summary を叩くため 429 になりやすい → 間隔を長めに
            rows = _fetch_fins_summary_rows_for_code(
                client, code4, sleep_seconds=_fins_sleep
            )
            fin_df = pd.DataFrame.from_records(rows)
            ser, agg_err = aggregate_fins_summary_df(fin_df)
            if agg_err is not None or ser is None:
                raise RuntimeError(agg_err or "empty /fins/summary")

            _is_thin = use_yfinance_statement_fallback and is_jquants_fins_history_thin(fin_df)
            _yf_fetched = False
            _yf_used = False
            _ser_before_yf: dict | None = None
            _ydict: dict | None = None
            if _is_thin:
                try:
                    _ydict = build_statement_dict_from_yfinance(code4)
                    _yf_fetched = True
                    if _ydict is not None:
                        _ser_before_yf = dict(ser)
                        ser = merge_jquants_with_yfinance_thin(
                            ser, _ydict,
                            jq_fye_latest=ser.get("_jq_fye_latest"),
                        )
                        _yf_used = True
                except ImportError:
                    pass
                except Exception:
                    pass
                if _ydict is not None:
                    _ys = float(os.environ.get("YFINANCE_SLEEP", "0.35"))
                    if _ys > 0:
                        time.sleep(_ys)
                # 監査レコード: thin 銘柄は全件記録
                _audit: dict = {
                    "Code": code4,
                    "JQ_Thin": True,
                    "YFinance_Fetched": _yf_fetched,
                    "YFinance_Used": _yf_used,
                    "JQ_TotalRows": len(fin_df) if fin_df is not None else 0,
                }
                for _k in STATEMENT_NUMERIC_COLS:
                    _jv = (_ser_before_yf or ser).get(_k)
                    _yv = _ydict.get(_k) if _ydict else None
                    _fv = ser.get(_k)
                    _audit[f"{_k}_JQ"] = _jv
                    _audit[f"{_k}_YF"] = _yv
                    _audit[f"{_k}_Final"] = _fv
                    if pd.isna(_fv):
                        _src = "NONE"
                    elif _yf_used and _ydict and pd.notna(_yv) and (pd.isna(_jv) or _fv == _yv):
                        _src = "YF"
                    else:
                        _src = "JQ"
                    _audit[f"{_k}_Source"] = _src
                yf_audit_rows.append(_audit)

            # 開示日はマージ用に raw の最大値（列集約後も「直近の開示」に近い）
            if "DiscDate" in fin_df.columns:
                dd = pd.to_datetime(fin_df["DiscDate"], errors="coerce")
                disc_out = dd.max() if dd.notna().any() else pd.NaT
            else:
                disc_out = pd.NaT

            # 内部キー（_jq_fye_*, _yf_* 等）を除去してから出力行を作成
            for _internal_key in list(ser.keys()):
                if _internal_key.startswith("_jq_fye_") or _internal_key.startswith("_yf_"):
                    del ser[_internal_key]

            one_row = pd.DataFrame(
                [{"Code": code4, **{c: ser[c] for c in STATEMENT_NUMERIC_COLS}, "DiscDate": disc_out, "YFinance_Supplemented": _yf_used}]
            )
            statement_latest_rows.append(one_row)

            # Log missing required numeric fields for easier debugging.
            nan_fields = [
                c
                for c in STATEMENT_NUMERIC_COLS
                if pd.isna(ser[c])
            ]
            critical_cols = set(CRITICAL_COLS)
            if nan_fields and any(c in critical_cols for c in nan_fields):
                shown = nan_fields[:10]
                suffix = "..." if len(nan_fields) > len(shown) else ""
                stmt_field_issues.append(
                    (code4, f"missing {len(nan_fields)} fields: {','.join(shown)}{suffix}")
                )
        except KeyboardInterrupt:
            print("\n[中断]")
            raise
        except Exception as e:
            stmt_failures.append((code4, f"{type(e).__name__}: {e}"))
        finally:
            if test_code_norm or i == 1 or i % 50 == 0 or i == len(codes):
                print(
                    f"statements progress: {i}/{len(codes)} "
                    f"(ok={len(statement_latest_rows)} fail={len(stmt_failures)})"
                )

    statements_df = (
        pd.concat(statement_latest_rows, ignore_index=True)
        if statement_latest_rows
        else pd.DataFrame(columns=["Code"] + STATEMENT_NUMERIC_COLS + ["DiscDate"])
    )
    if not statements_df.empty:
        statements_df = statements_df.drop_duplicates("Code")
        if "DiscDate" in statements_df.columns:
            statements_df["DiscDate"] = pd.to_datetime(statements_df["DiscDate"], errors="coerce")
            statements_df = statements_df.rename(columns={"DiscDate": "StatementDisclosedDate"})
        else:
            statements_df["StatementDisclosedDate"] = pd.NaT
    else:
        statements_df["StatementDisclosedDate"] = pd.NaT

    # 3) /fins/announcement (v2: equities/earnings-calendar)
    print("announcement: (next business day)")
    ann_rows = _fetch_paginated_v2(client, "/equities/earnings-calendar", params={})
    ann_df = pd.DataFrame.from_records(ann_rows)
    if ann_df.empty:
        ann_df = pd.DataFrame(columns=["Code", "AnnouncementDate", "FiscalQuarter", "FiscalYear"])
    else:
        ann_df = ann_df.copy()
        ann_df["Code"] = ann_df["Code"].map(_normalize_code_4).astype(str)
        ann_df["Date"] = pd.to_datetime(ann_df["Date"], errors="coerce")
        ann_df = ann_df.rename(columns={"Date": "AnnouncementDate", "FQ": "FiscalQuarter", "FY": "FiscalYear"})
        ann_df = ann_df[["Code", "AnnouncementDate", "FiscalQuarter", "FiscalYear"]].copy()
        ann_df = ann_df.drop_duplicates("Code", keep="last")

    # 4) weekly_margin_interest (v2: markets/margin-interest)
    # 直近金曜を週次アンカーに、欠損は当日から最大 N 日さかのぼり。
    # デフォルト 8 週 ≒2 か月分の買残履歴（playbook: トレンド確認用）。MARGIN_INTEREST_LOOKBACK_WEEKS で変更可。
    margin_weeks = max(1, int(os.environ.get("MARGIN_INTEREST_LOOKBACK_WEEKS", "8")))
    margin_day_fallback = max(0, int(os.environ.get("MARGIN_INTEREST_DAY_FALLBACK", "2")))
    margin_verbose = os.environ.get("SCREENING_VERBOSE_MARGIN", "").strip().lower() in ("1", "true", "yes")
    print(
        f"weekly_margin_interest: weeks={margin_weeks} day_fallback={margin_day_fallback} "
        f"(増やす: MARGIN_INTEREST_* / 週ごと詳細: SCREENING_VERBOSE_MARGIN=1)"
    )
    _m_sleep = float(os.environ.get("MARGIN_INTEREST_SLEEP", "1.5"))
    fridays: list[date] = []
    d_fr = _last_friday(today)
    for _ in range(margin_weeks):
        fridays.append(d_fr)
        d_fr = d_fr - timedelta(days=7)

    # 週ごとのデータを収集し、後でピボット（W1=最新〜WN=N週前）
    wm_all_weeks: list[pd.DataFrame] = []
    for i, frd in enumerate(fridays):
        # 週次の基準日は「通常金曜」だが祝日等で空の週がある → 同一週内で最大N日さかのぼって取得
        wm_rows: list[dict[str, Any]] = []
        chosen_str = ""
        for delta in range(margin_day_fallback + 1):
            d_try = frd - timedelta(days=delta)
            ds = d_try.strftime("%Y-%m-%d")
            cand = _fetch_paginated_v2(
                client,
                "/markets/margin-interest",
                params={"date": ds},
                sleep_seconds=_m_sleep,
            )
            if cand:
                wm_rows = cand
                chosen_str = ds
                break
        if not wm_rows:
            print(
                f"weekly_margin_interest: week {i + 1}/{margin_weeks} anchor={frd}: "
                f"empty ({margin_day_fallback}d fallback exhausted)"
            )
            continue
        anchor_s = frd.strftime("%Y-%m-%d")
        if margin_verbose:
            if chosen_str != anchor_s:
                _cd = date.fromisoformat(chosen_str)
                print(
                    f"weekly_margin_interest: week {i + 1}/{margin_weeks} date={chosen_str} "
                    f"(anchor {anchor_s}, -{(frd - _cd).days}d)"
                )
            else:
                print(f"weekly_margin_interest: week {i + 1}/{margin_weeks} date={chosen_str}")

        chunk = pd.DataFrame.from_records(wm_rows)
        if chunk.empty:
            continue
        chunk = chunk.copy()
        # 5桁のまま保持してから重複解消する（先に4桁化すると 94340/94345/94346 のような
        # 別銘柄が同一視され、0/0 行が優先されることがある）
        chunk["CodeRaw"] = chunk["Code"].astype(str)
        # 同一 CodeRaw に IssType 別の複数行: 1→2→3 の順で、かつ ShrtVol/LongVol が埋まっている行を優先
        did_iss_dedup = False
        if "IssType" in chunk.columns:
            it = pd.to_numeric(chunk["IssType"], errors="coerce")
            rank = pd.Series(99, index=chunk.index, dtype="int64")
            rank = rank.where(~it.eq(1), 0)
            rank = rank.where(~it.eq(2), 1)
            rank = rank.where(~it.eq(3), 2)
            sv0 = pd.to_numeric(chunk["ShrtVol"], errors="coerce")
            lv0 = pd.to_numeric(chunk["LongVol"], errors="coerce")
            vol_score = sv0.notna().astype("int8") + lv0.notna().astype("int8")
            chunk = (
                chunk.assign(_iss_rank=rank, _vol_score=vol_score)
                .sort_values(["CodeRaw", "_iss_rank", "_vol_score"], ascending=[True, True, False])
                .drop(columns=["_iss_rank", "_vol_score"])
            )
            chunk = chunk.drop_duplicates("CodeRaw", keep="first")
            did_iss_dedup = True
        chunk = chunk.rename(
            columns={"ShrtVol": "ShortMarginTradeVolume", "LongVol": "LongMarginTradeVolume"}
        )
        chunk = _to_numeric_df(chunk, ["ShortMarginTradeVolume", "LongMarginTradeVolume"])
        chunk["Code"] = chunk["CodeRaw"].map(_normalize_code_4).astype(str)
        chunk = chunk[["Code", "ShortMarginTradeVolume", "LongMarginTradeVolume"]].copy()
        if not did_iss_dedup:
            pass
        s_num = pd.to_numeric(chunk["ShortMarginTradeVolume"], errors="coerce")
        l_num = pd.to_numeric(chunk["LongMarginTradeVolume"], errors="coerce")
        chunk = (
            chunk.assign(
                _vol_score=(
                    s_num.notna().astype("int8")
                    + l_num.notna().astype("int8")
                    + s_num.fillna(0).ne(0).astype("int8")
                    + l_num.fillna(0).ne(0).astype("int8")
                ),
                _vol_abs=(s_num.abs().fillna(0) + l_num.abs().fillna(0)),
            )
            .sort_values(["Code", "_vol_score", "_vol_abs"], ascending=[True, False, False])
            .drop_duplicates("Code", keep="first")
            .drop(columns=["_vol_score", "_vol_abs"])
        )
        chunk["_week_idx"] = i  # 0=最新週（直近金曜系）, 1=1週前, ...
        wm_all_weeks.append(chunk)

    # 週次を結合: Short は各 Code の最新週（最小 _week_idx）行。Long は 8 週分を列展開
    # LongMargin_WkSeq01=最古 … WkSeq08=直近（week_idx 0）。欠損週は NA。
    wm_df = pd.DataFrame(columns=["Code", "ShortMarginTradeVolume", "LongMarginTradeVolume"])
    if wm_all_weeks:
        all_long = pd.concat(wm_all_weeks, ignore_index=True)

        idx_latest = all_long.groupby("Code", sort=False)["_week_idx"].idxmin()
        latest = (
            all_long.loc[idx_latest, ["Code", "ShortMarginTradeVolume", "LongMarginTradeVolume"]]
            .drop_duplicates("Code")
            .reset_index(drop=True)
        )

        _pivot_src = all_long[["Code", "_week_idx", "LongMarginTradeVolume"]].drop_duplicates(
            ["Code", "_week_idx"]
        )
        pivot = _pivot_src.pivot(
            index="Code", columns="_week_idx", values="LongMarginTradeVolume"
        )
        pivot = pivot.reset_index()

        wm_df = latest[["Code", "ShortMarginTradeVolume"]].copy()
        long_seq_cols: list[str] = []
        for seq in range(1, margin_weeks + 1):
            wi = margin_weeks - seq
            col = f"LongMargin_WkSeq{seq:02d}"
            long_seq_cols.append(col)
            if wi in pivot.columns:
                sub = pivot[["Code", wi]].rename(columns={wi: col})
                wm_df = wm_df.merge(sub, on="Code", how="outer")
            else:
                wm_df[col] = pd.NA

        _seq08 = "LongMargin_WkSeq08"
        _fb = latest.set_index("Code")["LongMarginTradeVolume"]
        wm_df["LongMarginTradeVolume"] = pd.to_numeric(wm_df[_seq08], errors="coerce")
        wm_df["LongMarginTradeVolume"] = wm_df["LongMarginTradeVolume"].fillna(wm_df["Code"].map(_fb))

        _s = wm_df["ShortMarginTradeVolume"].notna().sum()
        _l = wm_df["LongMarginTradeVolume"].notna().sum()
        n_weeks_got = len(wm_all_weeks)
        print(
            f"margin_interest 集計後: rows={len(wm_df)} weeks_fetched={n_weeks_got} "
            f"Short非欠損={int(_s)} Long非欠損={int(_l)} (買残8週列: {long_seq_cols})"
        )
        if not universe_df.empty:
            uc = set(universe_df["Code"].astype(str))
            sub = wm_df.loc[wm_df["Code"].astype(str).isin(uc)]
            nu = len(uc)
            su = int(sub["ShortMarginTradeVolume"].notna().sum())
            lu = int(sub["LongMarginTradeVolume"].notna().sum())
            if nu > 0:
                print(
                    f"  universe({nu}銘柄)に対し Short埋まり={su} ({100.0 * su / nu:.1f}%) "
                    f"Long埋まり={lu} ({100.0 * lu / nu:.1f}%)"
                )

    # 6) daily_quotes (v2: equities/bars/daily)
    trading_day_latest = _latest_trading_day_date_v2(client)
    trading_days: list[date] = [trading_day_latest]
    d_prev = trading_day_latest
    # 直近5営業日（latest + 前4つ）
    for _ in range(4):
        d_prev = _previous_trading_day_date_v2(client, before=d_prev, max_back_days=14)
        trading_days.append(d_prev)

    # latest day: Close 用（従来どおり 1日分のみ）
    trading_str_latest = trading_day_latest.strftime("%Y-%m-%d")
    print(f"daily_quotes: latest date={trading_str_latest}")
    latest_rows = _fetch_paginated_v2(
        client,
        "/equities/bars/daily",
        params={"date": trading_str_latest},
        sleep_seconds=1.2,
    )
    px_latest_df = pd.DataFrame.from_records(latest_rows)
    # Vo / Close ともに Code は 4桁正規化した値で揃える
    if not px_latest_df.empty and "Code" in px_latest_df.columns:
        px_latest_df = px_latest_df.copy()
        px_latest_df["Code"] = px_latest_df["Code"].map(_normalize_code_4).astype(str)
    if px_latest_df.empty or "C" not in px_latest_df.columns:
        px_close_df = pd.DataFrame(columns=["Code", "Close"])
    else:
        px_latest_df = px_latest_df.copy()
        px_latest_df = px_latest_df.rename(columns={"C": "Close"})
        px_latest_df = _to_numeric_df(px_latest_df, ["Close"])
        px_close_df = px_latest_df[["Code", "Close"]].copy()
        px_close_df = px_close_df.drop_duplicates("Code", keep="last")

    # 直近5営業日の出来高（Vo）の平均
    vo_frames: list[pd.DataFrame] = []
    # latest day は Close 用に取得済みレスポンスから Vo も流用する（API呼び出し回数削減）
    if not px_latest_df.empty and "Vo" in px_latest_df.columns:
        df_latest_vo = px_latest_df.copy()
        df_latest_vo = _to_numeric_df(df_latest_vo, ["Vo"])
        df_latest_vo = df_latest_vo[["Code", "Vo"]].copy()
        df_latest_vo = df_latest_vo.drop_duplicates("Code", keep="last")
        vo_frames.append(df_latest_vo)

    for d_scan in trading_days[1:]:
        ds = d_scan.strftime("%Y-%m-%d")
        rows = _fetch_paginated_v2(
            client,
            "/equities/bars/daily",
            params={"date": ds},
            sleep_seconds=1.2,
        )
        df_day = pd.DataFrame.from_records(rows)
        if df_day.empty:
            continue
        if "Vo" not in df_day.columns:
            raise ValueError("equities/bars/daily missing Vo (volume) column")
        df_day = df_day.copy()
        df_day["Code"] = df_day["Code"].map(_normalize_code_4).astype(str)
        df_day = _to_numeric_df(df_day, ["Vo"])
        df_day = df_day[["Code", "Vo"]].copy()
        # 1日内で 4桁コードに統一したときの重複を先に解消（Close と同じ思想）
        df_day = df_day.drop_duplicates("Code", keep="last")
        vo_frames.append(df_day)

    if vo_frames:
        vo_all = pd.concat(vo_frames, ignore_index=True)
        avg_df = (
            vo_all.groupby("Code", as_index=False)["Vo"]
            .mean()
            .rename(columns={"Vo": "AvgDailyVolume5d"})
        )
    else:
        avg_df = pd.DataFrame(columns=["Code", "AvgDailyVolume5d"])

    # Merge: Close + AvgDailyVolume5d
    px_df = px_close_df.merge(avg_df, on="Code", how="left")

    # Left join: start from universe
    master = universe_df.merge(statements_df, on="Code", how="left")
    master = master.merge(px_df, on="Code", how="left")
    master["MarketCap"] = (
        master["Close"]
        * master["NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"]
    )

    sh_out = "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"
    if args.yfinance:
        yf_sleep = float(os.environ.get("YFINANCE_SLEEP", "0.35"))
        print(f"yfinance: {len(codes)} 銘柄 (sleep={yf_sleep}s between tickers)")
        yf_df = fetch_yfinance_market_snapshot(codes, sleep_seconds=yf_sleep)
        master = master.merge(yf_df, on="Code", how="left")
        _mc_jq = pd.to_numeric(master["MarketCap"], errors="coerce")
        _mc_yf = pd.to_numeric(master["YFinanceMarketCap"], errors="coerce")
        master["MarketCap"] = _mc_yf.where(_mc_yf.notna(), _mc_jq)
        _sh_jq = pd.to_numeric(master[sh_out], errors="coerce")
        _sh_yf = pd.to_numeric(master["YFinanceSharesOutstanding"], errors="coerce")
        master[sh_out] = _sh_yf.where(_sh_yf.notna(), _sh_jq)
        _n_m = int(_mc_yf.notna().sum())
        _n_s = int(_sh_yf.notna().sum())
        print(f"yfinance: MarketCap 取得 {_n_m}/{len(codes)} 株数 {_n_s}/{len(codes)}")

    master = master.merge(wm_df, on="Code", how="left")

    master = master.merge(ss_df, on="Code", how="left")
    # 合算空売り株数 ÷ 期末発行済株式数 で比率を一貫させる（機関別比率の max は合算と整合しないため）
    if sh_out in master.columns and "ShortPositionsInSharesNumber" in master.columns:
        sh = pd.to_numeric(master[sh_out], errors="coerce")
        shortn = pd.to_numeric(master["ShortPositionsInSharesNumber"], errors="coerce")
        ok = sh.notna() & (sh > 0) & shortn.notna()
        master.loc[ok, "ShortPositionsToSharesOutstandingRatio"] = shortn[ok] / sh[ok]
    master = master.merge(ann_df, on="Code", how="left")
    # Earnings-calendar は「翌営業日の予定」しか返さないため空欄が多い。
    # /fins/summary の開示日(DiscDate)を fallback として使う。
    if "AnnouncementDate" in master.columns and "StatementDisclosedDate" in master.columns:
        master["AnnouncementDate"] = pd.to_datetime(master["AnnouncementDate"], errors="coerce")
        master["StatementDisclosedDate"] = pd.to_datetime(master["StatementDisclosedDate"], errors="coerce")
        master["AnnouncementDate"] = master["AnnouncementDate"].fillna(master["StatementDisclosedDate"])
    # helper column is not part of final output; it will be ignored by required_final_cols
    master["MarketCap"] = pd.to_numeric(master["MarketCap"], errors="coerce")
    _mc_val = master["MarketCap"]
    _np_lt = pd.to_numeric(master["Profit_LatestYear_Actual"], errors="coerce")
    _eq_lt = pd.to_numeric(master["Equity_LatestFY"], errors="coerce")
    _ok_per = _np_lt.notna() & (_np_lt > 0) & _mc_val.notna()
    _ok_pbr = _eq_lt.notna() & (_eq_lt > 0) & _mc_val.notna()
    _ok_roe = _eq_lt.notna() & (_eq_lt != 0) & _np_lt.notna()
    master["PER_Trailing"] = (_mc_val / _np_lt).where(_ok_per)
    master["PBR_Trailing"] = (_mc_val / _eq_lt).where(_ok_pbr)
    master["ROE_LatestYear"] = (_np_lt / _eq_lt).where(_ok_roe)
    # 出力監査用: このデータがいつの ETL 実行で作られたかを全行に明示
    master["ETLRunId"] = etl_run_id
    master["ETLStartedAtUTC"] = etl_started_at_utc_str
    master["ETLStartedAtJST"] = etl_started_at_jst_str

    required_final_cols = [
        "Code",
        "CompanyName",
        "MarketCodeName",
        "Sector17CodeName",
        "Sector33CodeName",
        "Close",
        "MarketCap",
    ]
    if args.yfinance:
        required_final_cols.extend(["YFinanceMarketCap", "YFinanceSharesOutstanding"])
    required_final_cols.extend(
        [
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
        "PER_Trailing",
        "PBR_Trailing",
        "ROE_LatestYear",
        "EquityToAssetRatio",
        "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
        "ShortMarginTradeVolume",
        "LongMarginTradeVolume",
        "LongMargin_WkSeq01",
        "LongMargin_WkSeq02",
        "LongMargin_WkSeq03",
        "LongMargin_WkSeq04",
        "LongMargin_WkSeq05",
        "LongMargin_WkSeq06",
        "LongMargin_WkSeq07",
        "LongMargin_WkSeq08",
        "DiscretionaryInvestmentContractorName",
        "ShortPositionsToSharesOutstandingRatio",
        "ShortPositionsInSharesNumber",
        "AvgDailyVolume5d",
        "AnnouncementDate",
        "FiscalQuarter",
        "FiscalYear",
        "YFinance_Supplemented",
        "ETLRunId",
        "ETLStartedAtUTC",
        "ETLStartedAtJST",
        ]
    )

    for c in required_final_cols:
        if c not in master.columns:
            master[c] = pd.NA

    master = master[required_final_cols].copy()
    master = master.sort_values("Code").reset_index(drop=True)

    _nr = len(master)
    if _nr:
        _fs = pd.to_numeric(master["ShortMarginTradeVolume"], errors="coerce").notna().sum()
        _fl = pd.to_numeric(master["LongMarginTradeVolume"], errors="coerce").notna().sum()
        print(
            f"master 信用枠（結合後）: Short埋まり={int(_fs)}/{_nr} ({100.0 * _fs / _nr:.1f}%) "
            f"Long埋まり={int(_fl)}/{_nr} ({100.0 * _fl / _nr:.1f}%)"
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Streamlit が同時に読み込む可能性があるため、一時ファイルへ書き出してから rename（原子置換）
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    master.to_parquet(tmp_path, index=False)
    tmp_path.replace(out_path)

    print(f"saved: {out_path} rows={len(master)} cols={len(master.columns)}")
    print(
        f"etl metadata: run_id={etl_run_id} "
        f"started_utc={etl_started_at_utc_str} started_jst={etl_started_at_jst_str}"
    )
    print(master.head(5))

    # Yahoo Finance 補完監査ログ出力
    if yf_audit_rows:
        audit_path = out_path.parent / "yfinance_audit.parquet"
        audit_df = pd.DataFrame(yf_audit_rows)
        # CompanyName をメイン出力からマージ
        if "CompanyName" in master.columns:
            audit_df = audit_df.merge(master[["Code", "CompanyName"]], on="Code", how="left")
            cols = ["Code", "CompanyName", "JQ_Thin", "YFinance_Fetched", "YFinance_Used", "JQ_TotalRows"]
            for _k in STATEMENT_NUMERIC_COLS:
                for _s in ("Final", "JQ", "YF", "Source"):
                    _c = f"{_k}_{_s}"
                    if _c in audit_df.columns and _c not in cols:
                        cols.append(_c)
            cols += [c for c in audit_df.columns if c not in cols]
            audit_df = audit_df[cols]
        audit_df.to_parquet(audit_path, index=False)
        _n_thin = len(audit_df)
        _n_yf_used = int(audit_df["YFinance_Used"].sum())
        _n_yf_fail = int(audit_df["YFinance_Fetched"].sum()) - _n_yf_used
        print(
            f"yfinance_audit: thin={_n_thin} yf_used={_n_yf_used} yf_fetch_failed={_n_yf_fail}"
            f" -> {audit_path}"
        )
        if want_excel:
            audit_xlsx = audit_path.with_suffix(".xlsx")
            try:
                audit_df.to_excel(audit_xlsx, index=False)
                print(f"yfinance_audit excel: {audit_xlsx}")
            except (ImportError, PermissionError) as _e:
                print(f"yfinance_audit excel skip: {_e}")
    else:
        print("yfinance_audit: thin 銘柄なし（全銘柄 J-Quants データ十分）")

    if not args.no_data_gaps:
        gaps_path = out_path.with_name(f"{out_path.stem}_data_gaps.parquet")
        gaps_df = _build_fins_data_gaps_df(master, stmt_failures, stmt_field_issues)
        gaps_df.to_parquet(gaps_path, index=False)
        print(f"data_gaps: {len(gaps_df)} rows -> {gaps_path}")
        if want_excel:
            gx = gaps_path.with_suffix(".xlsx")
            try:
                gaps_df.to_excel(gx, index=False)
                print(f"data_gaps excel: {gx}")
            except ImportError:
                print("data_gaps: Excel 出力は openpyxl が必要です（pip install openpyxl）")
            except PermissionError:
                print(
                    f"data_gaps excel: 書き込み不可（{gx} を Excel で開いていませんか？閉じて再実行）"
                )

    if want_excel:
        from convert_to_excel import parquet_to_excel

        xlsx_path = out_path.with_suffix(".xlsx")
        try:
            xout, _xf = parquet_to_excel(out_path, xlsx_path)
            print(f"excel: {xout}")
        except PermissionError:
            print(
                f"Excel 出力をスキップしました: {xlsx_path} がロックされています。\n"
                "  → 当該 .xlsx を閉じてから再実行するか、別名で保存: "
                f"python convert_to_excel.py --input {out_path} --output data/processed/tmp_master.xlsx"
            )

    if stmt_failures:
        head_n = min(20, len(stmt_failures))
        print(f"statement failures: {len(stmt_failures)} showing {head_n}")
        for code4, msg in stmt_failures[:head_n]:
            print(f"- {code4}: {msg}")
    if stmt_field_issues:
        head_n = min(20, len(stmt_field_issues))
        print(f"statement field issues: {len(stmt_field_issues)} showing {head_n}")
        for code4, msg in stmt_field_issues[:head_n]:
            print(f"- {code4}: {msg}")


if __name__ == "__main__":
    main()

