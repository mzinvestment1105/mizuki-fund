import streamlit as st


st.set_page_config(page_title="Screening Dashboard", layout="wide")

st.title("J-Quants Screening Dashboard")

st.markdown(
    """
このダッシュボードは、`data/processed/screening_master.parquet` を読み取り、
スクリーニング用のデータを表形式で閲覧するための画面です。

左のメニューから `screening_master` ページを開いてください。
"""
)

st.info(
    "データ生成（自動更新）は Windows タスクスケジューラで平日19:00に行います。"
)

