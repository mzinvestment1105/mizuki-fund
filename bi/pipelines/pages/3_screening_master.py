"""スクリーニングマスター閲覧・Excelエクスポートページ"""
from __future__ import annotations

import io
import json
import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st

_REPO_ROOT = Path(__file__).resolve().parents[3]

def _git_pull() -> None:
    """リモートの最新 parquet を取得する（サイレント）。"""
    try:
        subprocess.run(
            ["git", "pull", "--ff-only"],
            cwd=_REPO_ROOT,
            capture_output=True,
            timeout=30,
        )
    except Exception:
        pass

def _save_conditions(raw: dict) -> None:
    """現在の入力値をJSONに保存する。"""
    SAVED_CONDITIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SAVED_CONDITIONS_PATH, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)


def _load_saved_conditions() -> dict:
    """保存済み条件をJSONから読み込む。なければ空dict。"""
    try:
        with open(SAVED_CONDITIONS_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# convert_to_excel の関数を再利用
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from convert_to_excel import (
    JP_HEADERS,
    DERIVED_JP_HEADERS,
    SCREENING_TABLE_COLUMNS,
    DROP_COLS,
    _add_screening_derived_columns,
    parquet_to_excel,
)

DATA_PATH = Path(__file__).resolve().parents[2] / "outputs" / "screening_master.parquet"
EXCEL_PATH = Path(__file__).resolve().parents[2] / "outputs" / "screening_master.xlsx"
SAVED_CONDITIONS_PATH = Path(__file__).resolve().parents[2] / "outputs" / "screening_conditions_saved.json"

# 数値フォーマット用
_PERCENT_COLS = {
    "売上高_CAGR2年", "営業利益_CAGR2年", "最終益_CAGR2年",
    "売上高_成長率Y1", "売上高_成長率Y2",
    "営業利益_成長率Y1", "営業利益_成長率Y2",
    "最終益_成長率Y1", "最終益_成長率Y2",
    "売上高_予想対実績伸び率", "営業利益_予想対実績伸び率", "最終益_予想対実績伸び率",
    "自己資本比率", "ROE_実績ベース",
    "信用買い-発行済比率", "信用買い-出来高倍率",
}
_RATIO_COLS = {
    "PER_実績ベース", "PBR_実績ベース",
    "機関空売り_時価総額比", "現金同等物_時価総額比",
}
_INT_COLS = {
    "時価総額", "終値",
    "売上高_一昨年通期実績", "売上高_昨年通期実績", "売上高_今年通期実績", "売上高_来年通期予想",
    "営業利益_一昨年通期実績", "営業利益_昨年通期実績", "営業利益_今年通期実績", "営業利益_来年通期予想",
    "最終益_一昨年通期実績", "最終益_昨年通期実績", "最終益_今年通期実績", "最終益_来年通期予想",
    "現金及び現金同等物_直近期末", "純資産額_直近期末",
    "出来高_5日平均", "売買代金_5日平均",
    "発行株式総数",
    "信用買い残", "信用売り残",
    "信用買い残_週次01_最古", "信用買い残_週次02", "信用買い残_週次03",
    "信用買い残_週次04", "信用買い残_週次05", "信用買い残_週次06",
    "信用買い残_週次07", "信用買い残_週次08_直近",
}

# スクリーニング条件入力の単位変換: 入力値 × multiplier = データ側の値
_SCREENING_UNIT: dict[str, tuple[float, str]] = {
    # 時価総額: 1億単位で入力 (100 → 100億 = 10,000,000,000)
    "時価総額": (1e8, "億円"),
    # 売上高・営業利益・最終益: 百万単位で入力 (100 → 1億 = 100,000,000)
    "売上高_一昨年通期実績": (1e6, "百万円"),
    "売上高_昨年通期実績":   (1e6, "百万円"),
    "売上高_今年通期実績":   (1e6, "百万円"),
    "売上高_来年通期予想":   (1e6, "百万円"),
    "営業利益_一昨年通期実績": (1e6, "百万円"),
    "営業利益_昨年通期実績":   (1e6, "百万円"),
    "営業利益_今年通期実績":   (1e6, "百万円"),
    "営業利益_来年通期予想":   (1e6, "百万円"),
    "最終益_一昨年通期実績": (1e6, "百万円"),
    "最終益_昨年通期実績":   (1e6, "百万円"),
    "最終益_今年通期実績":   (1e6, "百万円"),
    "最終益_来年通期予想":   (1e6, "百万円"),
}


@st.cache_data(show_spinner="parquet読み込み中…", ttl=300)
def _load(_mtime: float) -> pd.DataFrame:
    df = pd.read_parquet(DATA_PATH)
    # 不要列削除
    df = df.drop(columns=[c for c in DROP_COLS if c in df.columns])
    # 週次・ブロック列を数値化
    for c in df.columns:
        if any(c.startswith(p) for p in ("LongMargin_WkSeq", "ShortMargin_WkSeq",
                                          "ShortSale_WkSeq", "VolAvg5d_BlkSeq",
                                          "ValAvg5d_BlkSeq")):
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # 派生列追加
    df = _add_screening_derived_columns(df)
    # 日本語リネーム
    rename = {k: v for k, v in JP_HEADERS.items() if k in df.columns}
    rename.update({k: v for k, v in DERIVED_JP_HEADERS.items() if k in df.columns})
    df = df.rename(columns=rename)
    # 予想列のNA→"予想無し"
    for c in ["売上高_来年通期予想", "営業利益_来年通期予想", "最終益_来年通期予想"]:
        if c in df.columns:
            df[c] = df[c].where(df[c].notna(), other="予想無し")
    return df


def _get_mtime() -> float:
    try:
        return DATA_PATH.stat().st_mtime
    except OSError:
        return 0.0


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    """表示用に数値フォーマット（Streamlit用）"""
    fmt = df.copy()
    for col in fmt.columns:
        if col in _PERCENT_COLS:
            fmt[col] = pd.to_numeric(fmt[col], errors="coerce").map(
                lambda v: f"{v*100:.1f}%" if pd.notna(v) else ""
            )
        elif col in _RATIO_COLS:
            fmt[col] = pd.to_numeric(fmt[col], errors="coerce").map(
                lambda v: f"{v:.2f}" if pd.notna(v) else ""
            )
        elif col in _INT_COLS:
            fmt[col] = pd.to_numeric(fmt[col], errors="coerce").map(
                lambda v: f"{int(v):,}" if pd.notna(v) else ""
            )
    return fmt


def main() -> None:
    st.markdown("""
    <style>
    .block-container { padding-left: 0.5rem !important; padding-right: 0.5rem !important; max-width: 100% !important; }
    </style>
    """, unsafe_allow_html=True)

    st.title("📊 スクリーニングマスター")

    if not DATA_PATH.exists():
        st.error(f"データが見つかりません: {DATA_PATH}")
        return

    _git_pull()
    mtime = _get_mtime()
    df = _load(mtime)

    # ── ヘッダー情報 ───────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("銘柄数", f"{len(df):,}")
    with c2:
        st.metric("列数", len(df.columns))
    with c3:
        import datetime, time as _time
        updated = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
        st.metric("データ更新", updated)

    st.divider()

    # ── スクリーニング条件 ─────────────────────────────────────
    def _parse_text(s: str) -> float | None:
        s = s.replace(",", "").strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None

    # 保存済み条件をセッション開始時に一度だけ読み込んでsession_stateに注入
    if "conditions_loaded" not in st.session_state:
        st.session_state["conditions_loaded"] = True
        saved = _load_saved_conditions()
        for jp_col, vals in saved.items():
            lo_val = vals.get("lo")
            hi_val = vals.get("hi")
            if lo_val is not None:
                st.session_state[f"lo_{jp_col}"] = lo_val
            if hi_val is not None:
                st.session_state[f"hi_{jp_col}"] = hi_val

    with st.expander("🔍 スクリーニング条件", expanded=True):
        raw_inputs: dict[str, dict] = {}
        conditions: dict[str, tuple[float | None, float | None]] = {}

        # 左列：バリュエーション・需給系 / 右列：成長率系（グループを崩さない）
        _LEFT_COLS = {
            "時価総額", "PER_実績ベース", "PBR_実績ベース", "ROE_実績ベース",
            "自己資本比率", "信用買い-発行済比率", "信用買い-出来高倍率",
            "機関空売り_時価総額比", "現金同等物_時価総額比",
        }
        left_items  = [(jp, m) for jp, m in SCREENING_TABLE_COLUMNS if jp in df.columns and jp in _LEFT_COLS]
        right_items = [(jp, m) for jp, m in SCREENING_TABLE_COLUMNS if jp in df.columns and jp not in _LEFT_COLS]

        def _render_condition(container, jp_col: str, memo: str) -> tuple:
            is_pct = jp_col in _PERCENT_COLS
            is_int = jp_col in _INT_COLS
            unit_mult, unit_label = _SCREENING_UNIT.get(jp_col, (None, ""))

            if is_pct:
                ph_lo, ph_hi, unit_disp = "下限(%)", "上限(%)", "%"
            elif unit_label:
                ph_lo, ph_hi, unit_disp = "下限", "上限", unit_label
            else:
                ph_lo, ph_hi, unit_disp = "下限", "上限", ""

            with container:
                c0, c1, c2, c3 = st.columns([3, 1.2, 1.2, 1])
                c0.caption(f"**{jp_col}**")

                if is_int:
                    lo_raw = c1.text_input(f"下限##{jp_col}", value="",
                                           key=f"lo_{jp_col}", label_visibility="collapsed",
                                           placeholder=ph_lo)
                    hi_raw = c2.text_input(f"上限##{jp_col}", value="",
                                           key=f"hi_{jp_col}", label_visibility="collapsed",
                                           placeholder=ph_hi)
                    lo = _parse_text(lo_raw)
                    hi = _parse_text(hi_raw)
                    mult = unit_mult or 1.0
                    note_parts = []
                    if lo is not None:
                        note_parts.append(f"≥{lo * mult:,.0f}")
                    if hi is not None:
                        note_parts.append(f"≤{hi * mult:,.0f}")
                    c3.caption((" / ".join(note_parts)) if note_parts else unit_disp)
                elif is_pct:
                    lo = c1.number_input(f"下限##{jp_col}", value=None, format="%.2f",
                                         key=f"lo_{jp_col}", label_visibility="collapsed",
                                         placeholder=ph_lo)
                    hi = c2.number_input(f"上限##{jp_col}", value=None, format="%.2f",
                                         key=f"hi_{jp_col}", label_visibility="collapsed",
                                         placeholder=ph_hi)
                    c3.caption(unit_disp)
                else:
                    lo = c1.number_input(f"下限##{jp_col}", value=None, format="%g",
                                         key=f"lo_{jp_col}", label_visibility="collapsed",
                                         placeholder=ph_lo)
                    hi = c2.number_input(f"上限##{jp_col}", value=None, format="%g",
                                         key=f"hi_{jp_col}", label_visibility="collapsed",
                                         placeholder=ph_hi)
                    c3.caption(unit_disp)

            return lo, hi, is_pct, unit_mult

        col_l, col_r = st.columns(2)

        for jp_col, memo in left_items:
            lo, hi, is_pct, unit_mult = _render_condition(col_l, jp_col, memo)
            if lo is not None or hi is not None:
                def _adj(v, _pct=is_pct, _mult=unit_mult):
                    if v is None: return None
                    if _pct: return v / 100
                    if _mult is not None: return v * _mult
                    return v
                conditions[jp_col] = (_adj(lo), _adj(hi))
            raw_inputs[jp_col] = {"lo": lo, "hi": hi}

        for jp_col, memo in right_items:
            lo, hi, is_pct, unit_mult = _render_condition(col_r, jp_col, memo)
            if lo is not None or hi is not None:
                def _adj(v, _pct=is_pct, _mult=unit_mult):
                    if v is None: return None
                    if _pct: return v / 100
                    if _mult is not None: return v * _mult
                    return v
                conditions[jp_col] = (_adj(lo), _adj(hi))
            raw_inputs[jp_col] = {"lo": lo, "hi": hi}

        _save_conditions(raw_inputs)

    # ── フィルタ適用 ───────────────────────────────────────────
    df_filtered = df.copy()
    for col, (lo, hi) in conditions.items():
        series = pd.to_numeric(df_filtered[col], errors="coerce")
        if lo is not None:
            df_filtered = df_filtered[series >= lo]
        if hi is not None:
            df_filtered = df_filtered[series <= hi]

    # ── テキスト検索 ───────────────────────────────────────────
    q = st.text_input("銘柄名 / コード 検索", placeholder="例: トヨタ　or　7203")
    if q:
        mask = (
            df_filtered.get("銘柄名", pd.Series(dtype=str)).astype(str).str.contains(q, na=False)
            | df_filtered.get("銘柄コード", pd.Series(dtype=str)).astype(str).str.contains(q, na=False)
        )
        df_filtered = df_filtered[mask]

    # ── ソート ────────────────────────────────────────────────
    sort_options = [
        "時価総額", "終値",
        "PER_実績ベース", "PBR_実績ベース", "ROE_実績ベース",
        "信用買い-発行済比率", "信用買い-出来高倍率",
        "機関空売り_時価総額比",
        "売上高_CAGR2年", "営業利益_CAGR2年", "最終益_CAGR2年",
        "売上高_成長率Y1", "売上高_成長率Y2",
        "営業利益_成長率Y1", "営業利益_成長率Y2",
        "最終益_成長率Y1", "最終益_成長率Y2",
        "売上高_予想対実績伸び率", "営業利益_予想対実績伸び率", "最終益_予想対実績伸び率",
        "現金同等物_時価総額比", "自己資本比率",
        "売買代金_5日平均",
    ]
    sort_options = [c for c in sort_options if c in df_filtered.columns]
    col_sort, col_asc = st.columns([3, 1])
    sort_col = col_sort.selectbox("並べ替え", sort_options, index=0)
    ascending = col_asc.checkbox("昇順", value=False)

    if sort_col in df_filtered.columns:
        df_filtered = df_filtered.copy()
        df_filtered["_sort"] = pd.to_numeric(df_filtered[sort_col], errors="coerce")
        df_filtered = df_filtered.sort_values("_sort", ascending=ascending).drop(columns=["_sort"])

    st.write(f"**{len(df_filtered):,} 銘柄**（全 {len(df):,} 銘柄中）")

    # ── 表示列選択 ────────────────────────────────────────────
    default_cols = [
        # 基本情報
        "銘柄コード", "銘柄名", "市場", "セクター17", "セクター33",
        "終値", "時価総額", "決算発表予定日", "会計年度",
        # バリュエーション
        "PER_実績ベース", "PBR_実績ベース", "ROE_実績ベース", "自己資本比率",
        # 信用・需給
        "発行株式総数",
        "信用買い-発行済比率", "信用買い-出来高倍率",
        "機関空売り_時価総額比",
        "信用買い残_週次01_最古", "信用買い残_週次02", "信用買い残_週次03",
        "信用買い残_週次04", "信用買い残_週次05", "信用買い残_週次06",
        "信用買い残_週次07", "信用買い残_週次08_直近",
        # 成長性（CAGR + 単年Y1/Y2）
        "売上高_CAGR2年", "売上高_成長率Y1", "売上高_成長率Y2",
        "営業利益_CAGR2年", "営業利益_成長率Y1", "営業利益_成長率Y2",
        "最終益_CAGR2年", "最終益_成長率Y1", "最終益_成長率Y2",
        # 成長性（予想）
        "売上高_予想対実績伸び率", "営業利益_予想対実績伸び率", "最終益_予想対実績伸び率",
        # 財務実績
        "売上高_一昨年通期実績", "売上高_昨年通期実績", "売上高_今年通期実績", "売上高_来年通期予想",
        "営業利益_一昨年通期実績", "営業利益_昨年通期実績", "営業利益_今年通期実績", "営業利益_来年通期予想",
        "最終益_一昨年通期実績", "最終益_昨年通期実績", "最終益_今年通期実績", "最終益_来年通期予想",
        # 流動性・現金
        "現金及び現金同等物_直近期末", "現金同等物_時価総額比",
        "純資産額_直近期末",
        "出来高_5日平均", "売買代金_5日平均",
    ]
    default_cols = [c for c in default_cols if c in df_filtered.columns]

    with st.expander("表示列の選択"):
        all_cols = list(df_filtered.columns)
        selected_cols = st.multiselect("表示する列", all_cols, default=default_cols)
    if not selected_cols:
        selected_cols = default_cols

    # ── データ表示 ────────────────────────────────────────────
    display_limit = st.slider("表示件数", 50, 5000, 500, 50)
    df_show = df_filtered[selected_cols].head(display_limit)
    df_base = df_filtered.head(display_limit).reset_index(drop=True)

    # フォーマット済みDFをキャッシュ（フィルタ/ソート変更時だけ再計算）
    _fmt_key = (id(df_filtered), len(df_show), tuple(selected_cols), display_limit)
    if st.session_state.get("_fmt_key") != _fmt_key:
        st.session_state._fmt_key = _fmt_key
        st.session_state._fmt_df = _format_df(df_show)
    df_formatted = st.session_state._fmt_df

    # 現在TVで表示中の行に「▶」マーカーを付ける
    _cur = min(st.session_state.get("tv_idx", 0), len(df_base) - 1)
    df_display = df_formatted.copy()
    df_display.insert(0, "TV", ["▶" if i == _cur else "" for i in range(len(df_display))])

    event = st.dataframe(
        df_display,
        use_container_width=True,
        height=600,
        on_select="rerun",
        selection_mode="single-row",
        key="screening_table",
        hide_index=True,
        column_config={
            "TV":     st.column_config.Column("TV",    width="small",  pinned=True),
            "銘柄コード": st.column_config.Column("コード",  width="small",  pinned=True),
            "銘柄名":   st.column_config.Column("銘柄名",  width="medium", pinned=True),
        },
    )

    # ── TradingView 連携 ───────────────────────────────────────
    import requests as _req

    def _send_to_tv(symbol: str) -> str | None:
        """tv_bridge にPOSTして銘柄を切り替える。エラー文字列 or None を返す。"""
        try:
            r = _req.post(
                "http://localhost:9876/set-symbol",
                json={"symbol": symbol},
                timeout=10,
            )
            d = r.json()
            return None if d.get("success") else d.get("error", "不明なエラー")
        except Exception as e:
            return f"{type(e).__name__}: {e}"

    def _row_to_symbol(row) -> tuple[str, str, str]:
        """DataFrameの行から (code, name, symbol) を返す。"""
        raw_code = row.get("銘柄コード", None)
        try:
            code = str(int(float(raw_code))) if pd.notna(raw_code) else ""
        except Exception:
            code = str(raw_code) if pd.notna(raw_code) else ""
        name = str(row.get("銘柄名", ""))
        symbol = f"TSE:{code}" if code else ""
        return code, name, symbol

    # session_state で現在の選択インデックスを管理
    if "tv_idx" not in st.session_state:
        st.session_state.tv_idx = 0
    if "tv_nav_active" not in st.session_state:
        st.session_state.tv_nav_active = False

    total = len(df_base)

    # 行クリックで自動送信（ナビボタン直後は無視）
    selected_rows = (event.selection.rows if event and hasattr(event, "selection") else [])
    if st.session_state.tv_nav_active:
        # ナビボタン押下後の rerun → フラグを戻すだけ、行選択は無視
        st.session_state.tv_nav_active = False
    elif selected_rows:
        new_idx = selected_rows[0]
        # 前回のクリックと異なる行なら送信（rerun連打による重複送信を防ぐ）
        if st.session_state.get("_last_click_idx") != new_idx:
            st.session_state._last_click_idx = new_idx
            st.session_state.tv_idx = new_idx
            row = df_base.iloc[new_idx]
            code, name, symbol = _row_to_symbol(row)
            if symbol:
                err = _send_to_tv(symbol)
                if err:
                    st.error(f"TV送信エラー: {err}")

    # 前/次ボタン
    cur_idx = min(st.session_state.tv_idx, total - 1)
    row = df_base.iloc[cur_idx]
    code, name, symbol = _row_to_symbol(row)

    nav_prev, nav_next, info_col = st.columns([1, 1, 8])
    with nav_prev:
        if st.button("◀ 前", disabled=cur_idx <= 0):
            st.session_state.tv_idx = cur_idx - 1
            st.session_state.tv_nav_active = True
            r2 = df_base.iloc[st.session_state.tv_idx]
            _, _, s2 = _row_to_symbol(r2)
            if s2:
                err = _send_to_tv(s2)
                if err:
                    st.error(f"TV送信エラー: {err}")
    with nav_next:
        if st.button("次 ▶", disabled=cur_idx >= total - 1):
            st.session_state.tv_idx = cur_idx + 1
            st.session_state.tv_nav_active = True
            r2 = df_base.iloc[st.session_state.tv_idx]
            _, _, s2 = _row_to_symbol(r2)
            if s2:
                err = _send_to_tv(s2)
                if err:
                    st.error(f"TV送信エラー: {err}")
    with info_col:
        if symbol:
            st.info(f"📈 表示中: **{code} {name}** (`{symbol}`)　{cur_idx + 1} / {total}")

    st.divider()

    # ── テクニカルスキャン ─────────────────────────────────────
    st.subheader("📊 テクニカルスキャン（TradingView連携）")
    st.caption("チャートに表示中のインジケーター値で追加スクリーニング。tv_bridge.py 起動・TradingViewCDPモード起動が前提。")

    with st.expander("スキャン設定"):
        tc1, tc2 = st.columns(2)
        with tc1:
            st.markdown("**インジケーター条件**")
            st.caption("チャートに表示中のインジケーター名を指定。`/study-values`で確認可。")
            rsi_min = st.number_input("RSI 下限", value=0.0, min_value=0.0, max_value=100.0)
            rsi_max = st.number_input("RSI 上限", value=70.0, min_value=0.0, max_value=100.0)
        with tc2:
            st.markdown("**スキャン設定**")
            wait_secs = st.number_input("銘柄切替後の待機秒数", value=2.5, min_value=1.0, max_value=10.0)
            max_symbols = st.number_input("スキャン上限銘柄数", value=30, min_value=1, max_value=200)

    if st.button("📊 テクニカルスキャン実行", type="primary"):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from tv_screener import TvScreener

        scanner = TvScreener()
        health = scanner.health_check()
        if not health.get("success"):
            st.error(f"tv_bridge.py に接続できません。別ターミナルで `python tv_bridge.py` を起動してください。\n{health.get('error','')}")
        else:
            # スキャン対象銘柄リスト作成
            scan_df = df_filtered.head(int(max_symbols)).reset_index(drop=True)
            symbols = []
            for _, row in scan_df.iterrows():
                raw = row.get("銘柄コード", None)
                try:
                    code = str(int(float(raw))) if pd.notna(raw) else ""
                except Exception:
                    code = str(raw) if pd.notna(raw) else ""
                if code:
                    symbols.append(f"TSE:{code}")

            conditions = {}
            if rsi_min > 0 or rsi_max < 100:
                conditions["RSI_RSI"] = {}
                if rsi_min > 0:
                    conditions["RSI_RSI"]["min"] = rsi_min
                if rsi_max < 100:
                    conditions["RSI_RSI"]["max"] = rsi_max

            st.info(f"{len(symbols)}銘柄をスキャン中… 約{len(symbols) * wait_secs:.0f}秒かかります")
            progress_bar = st.progress(0)
            status_text = st.empty()

            results = []
            for i, symbol in enumerate(symbols):
                status_text.text(f"[{i+1}/{len(symbols)}] {symbol} を確認中...")
                progress_bar.progress((i + 1) / len(symbols))
                result = scanner.scan_symbol(symbol, wait_secs=float(wait_secs))
                if result.success and conditions:
                    flat = scanner._flatten_studies(result.studies)
                    result.passed, result.reason = scanner._evaluate(flat, result.ohlcv, conditions)
                elif result.success:
                    result.passed = True
                    result.reason = "条件なし（全銘柄）"
                results.append(result)

            status_text.empty()
            progress_bar.empty()

            passed = [r for r in results if r.passed]
            st.success(f"スキャン完了: **{len(passed)} / {len(results)} 銘柄**がパス")

            if passed:
                rows = []
                for r in passed:
                    flat = scanner._flatten_studies(r.studies)
                    row_data = {"銘柄": r.symbol, "判定": r.reason}
                    row_data.update({k: f"{v:.2f}" if v is not None else "—" for k, v in flat.items()})
                    rows.append(row_data)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.divider()

    # ── Excel エクスポート ─────────────────────────────────────
    st.subheader("📥 Excelエクスポート")
    col_ex1, col_ex2 = st.columns(2)

    with col_ex1:
        st.write("**全件（書式付き・スクリーニングシート付き）**")
        if st.button("Excelを生成してダウンロード", type="primary"):
            with st.spinner("Excel生成中…"):
                tmp = Path(EXCEL_PATH)
                parquet_to_excel(DATA_PATH, tmp)
                with open(tmp, "rb") as f:
                    st.download_button(
                        label="⬇️ screening_master.xlsx をダウンロード",
                        data=f.read(),
                        file_name="screening_master.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

    with col_ex2:
        st.write("**フィルタ済み結果のみ（簡易書式）**")
        if st.button("フィルタ結果をExcelに出力"):
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                df_filtered.to_excel(w, index=False, sheet_name="スクリーニング結果")
            st.download_button(
                label="⬇️ filtered_result.xlsx をダウンロード",
                data=buf.getvalue(),
                file_name="filtered_result.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


if __name__ == "__main__":
    main()
