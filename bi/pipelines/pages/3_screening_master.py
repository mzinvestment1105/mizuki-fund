"""スクリーニングマスター閲覧・Excelエクスポートページ"""
from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import streamlit as st

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

# 数値フォーマット用
_PERCENT_COLS = {
    "売上高_CAGR2年", "営業利益_CAGR2年", "最終益_CAGR2年",
    "売上高_予想対実績伸び率", "営業利益_予想対実績伸び率", "最終益_予想対実績伸び率",
    "自己資本比率", "ROE_今期実績",
}
_RATIO_COLS = {
    "PER_実績ベース", "PBR_実績ベース",
    "信用買残_発行済株数比", "信用買残_出来高5日比",
    "機関空売り_時価総額比", "現金同等物_時価総額比",
}
_INT_COLS = {
    "時価総額", "終値",
    "売上高_一昨年通期実績", "売上高_昨年通期実績", "売上高_今年通期実績", "売上高_来年通期予想",
    "営業利益_一昨年通期実績", "営業利益_昨年通期実績", "営業利益_今年通期実績", "営業利益_来年通期予想",
    "最終益_一昨年通期実績", "最終益_昨年通期実績", "最終益_今年通期実績", "最終益_来年通期予想",
    "現金及び現金同等物_直近期末", "純資産額_直近期末",
    "出来高_5日平均", "売買代金_5日平均",
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
    st.title("📊 スクリーニングマスター")

    if not DATA_PATH.exists():
        st.error(f"データが見つかりません: {DATA_PATH}")
        return

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

    with st.expander("🔍 スクリーニング条件", expanded=True):
        conditions: dict[str, tuple[float | None, float | None]] = {}
        cols_left, cols_right = st.columns(2)
        for i, (jp_col, memo) in enumerate(SCREENING_TABLE_COLUMNS):
            if jp_col not in df.columns:
                continue
            target = cols_left if i % 2 == 0 else cols_right

            is_pct = jp_col in _PERCENT_COLS
            is_int = jp_col in _INT_COLS
            unit_mult, unit_label = _SCREENING_UNIT.get(jp_col, (None, ""))

            if is_pct:
                ph_lo, ph_hi = "下限 (%)", "上限 (%)"
            elif unit_label:
                ph_lo, ph_hi = f"下限 ({unit_label})", f"上限 ({unit_label})"
            else:
                ph_lo, ph_hi = "下限", "上限"

            with target:
                st.caption(f"**{jp_col}** — {memo}")
                cc1, cc2 = st.columns(2)
                if is_int:
                    lo_raw = cc1.text_input(f"下限##{jp_col}", value="",
                                            key=f"lo_{jp_col}", label_visibility="collapsed",
                                            placeholder=ph_lo)
                    hi_raw = cc2.text_input(f"上限##{jp_col}", value="",
                                            key=f"hi_{jp_col}", label_visibility="collapsed",
                                            placeholder=ph_hi)
                    lo = _parse_text(lo_raw)
                    hi = _parse_text(hi_raw)
                    mult = unit_mult or 1.0
                    if lo is not None:
                        cc1.caption(f"= {lo * mult:,.0f} 円")
                    if hi is not None:
                        cc2.caption(f"= {hi * mult:,.0f} 円")
                elif is_pct:
                    lo = cc1.number_input(f"下限##{jp_col}", value=None, format="%.2f",
                                          key=f"lo_{jp_col}", label_visibility="collapsed",
                                          placeholder=ph_lo)
                    hi = cc2.number_input(f"上限##{jp_col}", value=None, format="%.2f",
                                          key=f"hi_{jp_col}", label_visibility="collapsed",
                                          placeholder=ph_hi)
                else:
                    lo = cc1.number_input(f"下限##{jp_col}", value=None, format="%g",
                                          key=f"lo_{jp_col}", label_visibility="collapsed",
                                          placeholder=ph_lo)
                    hi = cc2.number_input(f"上限##{jp_col}", value=None, format="%g",
                                          key=f"hi_{jp_col}", label_visibility="collapsed",
                                          placeholder=ph_hi)

                if lo is not None or hi is not None:
                    def _adj(v: float | None, _pct=is_pct, _mult=unit_mult) -> float | None:
                        if v is None:
                            return None
                        if _pct:
                            return v / 100
                        if _mult is not None:
                            return v * _mult
                        return v
                    conditions[jp_col] = (_adj(lo), _adj(hi))

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
        "PER_実績ベース", "PBR_実績ベース", "ROE_今期実績",
        "信用買残_発行済株数比", "信用買残_出来高5日比",
        "機関空売り_時価総額比",
        "売上高_CAGR2年", "営業利益_CAGR2年", "最終益_CAGR2年",
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
        "PER_実績ベース", "PBR_実績ベース", "ROE_今期実績", "自己資本比率",
        # 信用・需給
        "信用買残_発行済株数比", "信用買残_出来高5日比",
        "機関空売り_時価総額比",
        "信用買い残_週次01_最古", "信用買い残_週次02", "信用買い残_週次03",
        "信用買い残_週次04", "信用買い残_週次05", "信用買い残_週次06",
        "信用買い残_週次07", "信用買い残_週次08_直近",
        # 成長性（CAGR）
        "売上高_CAGR2年", "営業利益_CAGR2年", "最終益_CAGR2年",
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
    st.dataframe(
        _format_df(df_filtered[selected_cols].head(display_limit)),
        use_container_width=True,
        height=600,
    )

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
