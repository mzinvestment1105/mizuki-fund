from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


DATA_PATH = Path("data") / "processed" / "screening_master.parquet"


def _load_df() -> pd.DataFrame:
    if not DATA_PATH.exists():
        return pd.DataFrame()
    return pd.read_parquet(DATA_PATH)


def main() -> None:
    st.title("Screening Master（最新データ）")

    if "reload_token" not in st.session_state:
        st.session_state["reload_token"] = 0

    if st.button("最新データを読み直す", type="primary"):
        st.session_state["reload_token"] += 1

    # reload_token を引数に渡して cache を無効化する
    @st.cache_data(show_spinner=True)
    def _cached_load_df(_token: int) -> pd.DataFrame:
        return _load_df()

    df = _cached_load_df(st.session_state["reload_token"])
    if df.empty:
        st.warning(f"{DATA_PATH} が見つからないか、空です。先にデータ生成を実行してください。")
        return

    mtime = None
    if DATA_PATH.exists():
        try:
            mtime = DATA_PATH.stat().st_mtime
        except OSError:
            mtime = None

    col1, col2 = st.columns(2)
    with col1:
        st.metric("行数", df.shape[0])
    with col2:
        if mtime is None:
            st.metric("最終更新日時", "—")
        else:
            st.metric(
                "最終更新日時",
                pd.to_datetime(mtime, unit="s").isoformat(timespec="seconds"),
            )

    # 軽量表示（必要なら後で拡張）
    st.write(f"列数: {len(df.columns)} / データ: {len(df)} 行")

    top_n = st.number_input("表示上限（行）", min_value=10, max_value=5000, value=1000, step=50)

    # 可能なら MarketCap で並べる
    sort_col = None
    if "MarketCap" in df.columns:
        sort_col = "MarketCap"

    if sort_col:
        df2 = df.copy()
        df2["MarketCap_num"] = pd.to_numeric(df2["MarketCap"], errors="coerce")
        df2 = df2.sort_values("MarketCap_num", ascending=False).drop(columns=["MarketCap_num"], errors="ignore")
    else:
        df2 = df

    st.dataframe(df2.head(int(top_n)), use_container_width=True)


if __name__ == "__main__":
    main()

