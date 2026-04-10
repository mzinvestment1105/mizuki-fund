import streamlit as st

st.set_page_config(page_title="Mizuki Fund Dashboard", layout="wide")

st.title("Mizuki Fund Dashboard")

st.markdown(
    """
左のメニューから各ページを開いてください。

| ページ | 内容 |
|--------|------|
| 📊 スクリーニングマスター | 全銘柄スクリーニング・フィルタ・Excelエクスポート |
"""
)

st.info(
    "データは GitHub Actions により平日 19:00 JST に自動更新されます。"
    "最新データは右上の更新時刻で確認できます。"
)
