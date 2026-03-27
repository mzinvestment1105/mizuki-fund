"""
ダッシュボードのサブページ: スクリーンショットから銘柄を読み取り、材料レポート（Markdown）を表示する。

起動: リポジトリ直下で `streamlit run dashboard_app.py`（サイドバーに本ページが表示されます）
"""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

import streamlit as st

from mover_report_core import _maybe_load_dotenv
from report_from_screenshot import run_from_image_file

_maybe_load_dotenv()

st.set_page_config(page_title="値幅レポート（画像）", layout="wide")
st.title("値幅レポート（画像）")

st.info(
    """
**「レポート生成」を押すと、裏で次の順に動きます（ネットにデータが送られます）。**

1. **画像**を一時ファイルに保存し、**OpenAI**のサーバーへ送ります（銘柄コード・銘柄名の読み取り）。
2. 読み取った各銘柄について **Tavily** が **Web 検索**します（ニュース・記事の要約と URL を取得）。
3. その内容を材料に **OpenAI** が **日本語のレポート（Markdown）** を書きます。

**かかるもの**: API の利用料（OpenAI・Tavily）が発生することがあります。  
**時間**: 銘柄数によりますが、数十秒〜数分かかることがあります。  
**必要な設定**: `.env` に `OPENAI_API_KEY` と `TAVILY_API_KEY`（J-Quants はこのページでは使いません）。
"""
)

st.caption(
    "Cursor などで「Run」を押したときは、ターミナルで Streamlit が起動し、ブラウザが開きます。そのウィンドウがこのアプリです。"
)
st.caption("ターミナルで使うコマンドのコピペ一覧は、プロジェクト直下の「ターミナル_コピペ手順.md」を開いてください。")

if not st.session_state.get("_mover_deps_ok"):
    try:
        import openai  # noqa: F401
    except ImportError:
        st.error("依存パッケージが不足しています。`pip install -r requirements-movers.txt` を実行してください。")
        st.stop()
    st.session_state["_mover_deps_ok"] = True

uploaded = st.file_uploader("スクリーンショット（PNG / JPEG / WebP）", type=["png", "jpg", "jpeg", "webp"])

c1, c2 = st.columns(2)
with c1:
    as_of_str = st.text_input("基準日（任意、YYYY-MM-DD）", placeholder="空欄なら画像の日付推定または今日")
with c2:
    vision_model = st.text_input("Vision モデル", value="gpt-4o-mini")
chat_model = st.text_input("レポート生成モデル", value="gpt-4o-mini")

run = st.button("レポート生成", type="primary")

if run and uploaded is not None:
    suffix = Path(uploaded.name).suffix or ".png"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(uploaded.getbuffer())
        tmp_path = Path(tmp.name)
    try:
        as_of_override = None
        if as_of_str.strip():
            try:
                as_of_override = date.fromisoformat(as_of_str.strip()[:10])
            except ValueError:
                st.error("基準日の形式が不正です（YYYY-MM-DD）。")
                st.stop()
        with st.status("レポート作成の進行状況（開いたままお待ちください）", expanded=True) as status:
            status.write("① 画像を一時保存しました。これから OpenAI で銘柄を読み取ります。")
            status.write("② 続けて Tavily で検索し、OpenAI で文章化します（1本の処理のため待ち時間はまとまって表示されます）。")
            md, _img, as_of_used = run_from_image_file(
                tmp_path,
                as_of_override=as_of_override,
                model_vision=vision_model.strip() or "gpt-4o-mini",
                model_chat=chat_model.strip() or "gpt-4o-mini",
            )
            status.update(label="完了: 下にレポートを表示しました", state="complete", expanded=False)
        st.success(f"基準日（使用値）: **{as_of_used}**")
        st.markdown(md)
        st.download_button(
            "Markdown をダウンロード",
            data=md.encode("utf-8"),
            file_name=f"screenshot_{as_of_used.isoformat()}.md",
            mime="text/markdown",
        )
    except SystemExit as e:
        st.error(str(e) or "処理を中止しました。")
    except Exception as e:
        st.exception(e)
    finally:
        tmp_path.unlink(missing_ok=True)
elif run and uploaded is None:
    st.warning("画像をアップロードしてください。")
