"""
全ページ共通の Wide / Narrow レイアウトトグル。

使い方:
    from utils.layout_toggle import render_layout_toggle
    # set_page_config(layout="wide") の後に呼ぶ
    render_layout_toggle()

デフォルトは narrow（max-width: 860px）。
サイドバーの「Wide 表示」チェックで wide に切り替わる。
CSS 注入方式のため st.rerun() 不要。
"""
import streamlit as st

_SESSION_KEY = "_wide_layout"

_NARROW_CSS = """<style>
[data-testid="stMainBlockContainer"],
.block-container {
    max-width: 860px !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
    margin-left: auto !important;
    margin-right: auto !important;
}
</style>"""


def render_layout_toggle() -> None:
    """サイドバーに Wide 表示チェックボックスを描画する。"""
    st.session_state.setdefault(_SESSION_KEY, False)
    st.sidebar.checkbox("Wide 表示", key=_SESSION_KEY)
    if not st.session_state[_SESSION_KEY]:
        st.markdown(_NARROW_CSS, unsafe_allow_html=True)
