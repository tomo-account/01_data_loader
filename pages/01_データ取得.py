"""
データ取得 — 株価・マクロ・財務・ニュース・TDnet 取得
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import streamlit as st

from config.paths import (
    PRICES_STOCKS_DAILY, PRICES_MACRO_DAILY,
    FINANCIALS, NEWS_RSS, NEWS_TDNET, NEWS_KESSAN, NEWS_KESSAN_SCHEDULE,
)
from utils.date_utils import latest_business_day
from utils.layout_toggle import render_layout_toggle

st.set_page_config(
    page_title="データ取得",
    page_icon="📥",
    layout="wide",
    initial_sidebar_state="expanded",
)

render_layout_toggle()

st.title("📥 データ取得")
st.html('<div style="height: 64px;"></div>')

# ── subprocess ヘルパー ──────────────────────────────────────────
_ENV  = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
_ROOT = Path(__file__).resolve().parent.parent


def _run(cmd: list[str]) -> tuple[int, str]:
    """サブプロセスを実行し (returncode, 結合出力) を返す。"""
    result = subprocess.run(
        cmd, capture_output=True, cwd=str(_ROOT), env=_ENV,
        text=True, encoding="utf-8", errors="replace",
    )
    parts = []
    if result.stdout:
        parts.append(result.stdout)
    if result.stderr:
        parts.append(result.stderr)
    return result.returncode, "\n".join(parts).strip()


def _tail(text: str, n: int = 20) -> str:
    lines = text.strip().splitlines()
    return "\n".join(lines[-n:]) if lines else ""


def _run_parallel(tasks: dict[str, list[str]]) -> dict[str, tuple[int, str]]:
    """tasks = {label: cmd} を並列実行し {label: (rc, out)} を返す。"""
    if not tasks:
        return {}
    results: dict[str, tuple[int, str]] = {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        future_to_label = {executor.submit(_run, cmd): label for label, cmd in tasks.items()}
        for future in as_completed(future_to_label):
            results[future_to_label[future]] = future.result()
    return results


_JOB_SAVE_PATHS: dict[str, str] = {
    "①個別株":          "data/prices/stocks/",
    "②マクロ":          "data/prices/macro/",
    "③財務":            "data/financials/",
    "④TDnet":           "data/news/tdnet/",
    "⑤RSS":             "data/news/rss/",
    "⑥決算短信XBRL":    "data/statements/",
    "TDnet一括":         "data/news/tdnet/",
    "XBRL一括":          "data/statements/",
    "①個別株(OHLCV)":   "data/prices/stocks/",
    "決算短信カレンダー": "data/news/kessan/",
    "決算発表予定":       "data/news/kessan_schedule/",
}


def _show_run_result(label: str, results: dict[str, tuple[int, str]]) -> None:
    """並列実行の結果を Streamlit にステータス＋ログ表示する。"""
    if not results:
        st.warning("実行対象がありません。")
        return
    n_fail = sum(1 for rc, _ in results.values() if rc != 0)
    status = "✅ 完了" if n_fail == 0 else f"⚠️ {n_fail}/{len(results)} 失敗"
    with st.expander(f"{status} — {label}", expanded=(n_fail > 0)):
        for lbl, (rc, out) in results.items():
            icon = "✅" if rc == 0 else "❌"
            save_path = _JOB_SAVE_PATHS.get(lbl, "")
            path_note = f"　→ `{save_path}`" if save_path else ""
            st.markdown(f"**{icon} {lbl}**（exit={rc}）{path_note}")
            st.code(_tail(out, 30) or "(出力なし)", language="text")



# ── 取得済み期間ヘルパー（60秒キャッシュ） ─────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def _stocks_coverage() -> str:
    files = list(PRICES_STOCKS_DAILY.glob("*.parquet"))
    if not files:
        return "—（未取得）"
    try:
        df = pd.read_parquet(files[0])
        oldest = pd.Timestamp(df.index[0]).date()
        newest = pd.Timestamp(df.index[-1]).date()
        return f"{oldest} 〜 {newest}（{len(files)} 銘柄）"
    except Exception:
        return f"確認中…（{len(files)} 銘柄）"


@st.cache_data(ttl=60, show_spinner=False)
def _macro_coverage() -> str:
    files = list(PRICES_MACRO_DAILY.glob("*.parquet"))
    if not files:
        return "—（未取得）"
    try:
        df = pd.read_parquet(files[0])
        oldest = pd.Timestamp(df.index[0]).date()
        newest = pd.Timestamp(df.index[-1]).date()
        return f"{oldest} 〜 {newest}（{len(files)} 指標）"
    except Exception:
        return f"確認中…（{len(files)} 指標）"


@st.cache_data(ttl=60, show_spinner=False)
def _financials_coverage() -> str:
    files = list(FINANCIALS.glob("*.csv"))
    return f"{len(files)} 銘柄分" if files else "—（未取得）"


@st.cache_data(ttl=60, show_spinner=False)
def _rss_coverage() -> str:
    files = sorted(NEWS_RSS.glob("*.csv"))
    if not files:
        return "—（未取得）"
    dates = [f.stem for f in files]
    return f"{dates[0]} 〜 {dates[-1]}（{len(dates)} 日分）"


@st.cache_data(ttl=60, show_spinner=False)
def _tdnet_coverage() -> str:
    files = sorted(NEWS_TDNET.glob("*.csv"))
    if not files:
        return "—（未取得）"
    dates = [f.stem for f in files]
    return f"{dates[0]} 〜 {dates[-1]}（{len(dates)} 日分）"


@st.cache_data(ttl=60, show_spinner=False)
def _kessan_coverage() -> str:
    files = sorted(NEWS_KESSAN.glob("*.csv"))
    if not files:
        return "—（未取得）"
    dates = [f.stem for f in files]
    return f"{dates[0]} 〜 {dates[-1]}（{len(dates)} 日分）"


@st.cache_data(ttl=60, show_spinner=False)
def _kessan_schedule_coverage() -> str:
    f = NEWS_KESSAN_SCHEDULE / "latest.csv"
    if not f.exists():
        return "—（未取得）"
    try:
        df = pd.read_csv(f)
        fetched_at = df["fetched_at"].iloc[0] if "fetched_at" in df.columns and not df.empty else "不明"
        n = len(df)
        return f"{n} 件（取得: {fetched_at}）"
    except Exception:
        return "確認中…"


# ── データ取得検証 ──────────────────────────────────────────────
# 連続日で終値比がこの値以下、または逆数以上なら株式分割・統合の疑い。
# 0.55 = 約 45% 以上の急変動。2:1 分割（≒0.5）と 1:2 併合（≒2.0）を捕捉できる。
# yfinance auto_adjust=True で通常は補正されるため、検出時は取り込み漏れが疑われる。
_SPLIT_RATIO_THRESHOLD = 0.55
_STALE_DAYS            = 5


def _build_verify_report() -> dict:
    """株価・マクロデータの取得状況・欠損・株式分割疑いを集計して dict で返す。"""
    today = latest_business_day()
    stock_files = sorted(PRICES_STOCKS_DAILY.glob("*.parquet"))
    macro_files = sorted(PRICES_MACRO_DAILY.glob("*.parquet"))

    split_warnings: list[str] = []
    nan_warnings:   list[str] = []
    stale_warnings: list[str] = []
    latest_stock_date = None
    oldest_stock_date = None

    for f in stock_files:
        try:
            df = pd.read_parquet(f)
            if df.empty:
                nan_warnings.append(f"{f.stem}: データ空")
                continue
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            sym         = f.stem
            file_latest = df.index[-1].date()
            file_oldest = df.index[0].date()
            if latest_stock_date is None or file_latest > latest_stock_date:
                latest_stock_date = file_latest
            if oldest_stock_date is None or file_oldest < oldest_stock_date:
                oldest_stock_date = file_oldest

            days_lag = (today - file_latest).days
            if days_lag > _STALE_DAYS:
                stale_warnings.append(f"{sym}: 最新 {file_latest}（{days_lag}日遅れ）")

            close_col = "Close" if "Close" in df.columns else ("close" if "close" in df.columns else None)
            if close_col:
                nan_count = int(df[close_col].isna().sum())
                if nan_count > 0:
                    nan_warnings.append(f"{sym}: 終値 NaN {nan_count}行")

                closes = df[close_col].dropna()
                if len(closes) > 1:
                    ratio = closes / closes.shift(1)
                    suspicious = ratio[(ratio < _SPLIT_RATIO_THRESHOLD) | (ratio > 1.0 / _SPLIT_RATIO_THRESHOLD)]
                    for dt, r in suspicious.items():
                        split_warnings.append(f"{sym}: {pd.Timestamp(dt).date()} 終値比 {r:.2f}x")
        except Exception as e:
            nan_warnings.append(f"{f.stem}: 読み込みエラー ({e})")

    latest_macro_date = None
    for f in macro_files:
        try:
            df = pd.read_parquet(f)
            if df.empty:
                continue
            df.index = pd.to_datetime(df.index)
            file_latest = df.index[-1].date()
            if latest_macro_date is None or file_latest > latest_macro_date:
                latest_macro_date = file_latest
        except Exception:
            pass

    return {
        "today":              today,
        "total_stocks":       len(stock_files),
        "total_macros":       len(macro_files),
        "oldest_stock_date":  oldest_stock_date,
        "latest_stock_date":  latest_stock_date,
        "latest_macro_date":  latest_macro_date,
        "split_warnings":     split_warnings,
        "nan_warnings":       nan_warnings,
        "stale_warnings":     stale_warnings,
    }


def _render_verify_report(rep: dict) -> None:
    st.markdown("##### 📊 データ取得検証レポート")
    if rep["total_stocks"] == 0 and rep["total_macros"] == 0:
        st.error("株価・マクロデータが見つかりません。先にデータ取得を実行してください。")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("個別株ファイル数", rep["total_stocks"])
        if rep["oldest_stock_date"] and rep["latest_stock_date"]:
            st.caption(f"{rep['oldest_stock_date']} 〜 {rep['latest_stock_date']}")
    with c2:
        st.metric("マクロ指標ファイル数", rep["total_macros"])
        if rep["latest_macro_date"]:
            st.caption(f"最新: {rep['latest_macro_date']}")
    with c3:
        st.metric("基準営業日", str(rep["today"]))

    has_issues = bool(rep["split_warnings"] or rep["nan_warnings"] or rep["stale_warnings"])

    if rep["stale_warnings"]:
        with st.expander(f"⚠️ データ遅れ（{len(rep['stale_warnings'])} 件）", expanded=True):
            for w in rep["stale_warnings"]:
                st.warning(w, icon="⚠️")

    if rep["nan_warnings"]:
        with st.expander(f"⚠️ 欠損・読み込みエラー（{len(rep['nan_warnings'])} 件）", expanded=True):
            for w in rep["nan_warnings"]:
                st.warning(w, icon="⚠️")

    if rep["split_warnings"]:
        pct = (1 - _SPLIT_RATIO_THRESHOLD) * 100
        with st.expander(f"🔀 株式分割・統合疑い（{len(rep['split_warnings'])} 件）", expanded=True):
            st.caption(
                f"連続日間で終値が {pct:.0f}% 以上変動した銘柄です。"
                "yfinance の auto_adjust で本来は補正されるため、検出時は分割・統合の取り込み漏れが疑われます。"
            )
            for w in rep["split_warnings"]:
                st.info(w, icon="🔀")

    if not has_issues:
        st.success("問題なし — 欠損・データ遅れ・株式分割疑いは検出されませんでした。", icon="✅")


# ── 最新営業日（取得ボタン共通） ───────────────────────────────
_LATEST_BD_STR = latest_business_day().isoformat()

# ── 一括取得（メイン） ──────────────────────────────────────
_btn_c1, _btn_c2, _btn_c3 = st.columns(3)
with _btn_c1:
    daily_clicked = st.button(
        "📅 日次データ", width="stretch",
        help="株価(日足・5分足 直近3日) + マクロ + 財務(EDINET DB) + TDnet + RSS + 決算短信XBRL を並列取得。",
    )
with _btn_c2:
    news_clicked = st.button(
        "🗞 ニュース", width="stretch",
        help="マクロ指標・TDnet 適時開示・RSS ニュースを並列取得。株価・財務は取得しない。",
    )
with _btn_c3:
    schedule_clicked = st.button(
        "📋 決算発表予定", width="stretch",
        help=f"J-Quants API で将来の決算発表予定を取得して latest.csv に保存。現在: {_kessan_schedule_coverage()}",
    )

if schedule_clicked:
    with st.spinner("決算発表予定を取得中…"):
        results = _run_parallel({
            "決算発表予定": [
                "python", "collectors/fetch_kessan_calendar.py",
                "--mode", "future",
            ],
        })
    st.cache_data.clear()
    st.session_state["_fetch_result"] = ("決算発表予定取得", results)

if news_clicked:
    with st.spinner("ニュース取得を並列実行中…"):
        results = _run_parallel({
            "①マクロ": ["python", "collectors/fetch_prices_macro.py"],
            "②TDnet":  ["python", "collectors/fetch_news.py", "--date", _LATEST_BD_STR, "--mode", "tdnet"],
            "③RSS":    ["python", "collectors/fetch_news.py", "--date", _LATEST_BD_STR, "--mode", "rss"],
        })
    st.cache_data.clear()
    st.session_state["_fetch_result"] = ("ニュース取得", results)

if daily_clicked:
    with st.spinner("日次データを並列実行中…"):
        results = _run_parallel({
            "①個別株":       ["python", "collectors/fetch_prices_stocks.py", "--fivemin-days", "3"],
            "②マクロ":       ["python", "collectors/fetch_prices_macro.py"],
            "③財務":         ["python", "collectors/fetch_financials.py"],
            "④TDnet":        ["python", "collectors/fetch_news.py", "--date", _LATEST_BD_STR, "--mode", "tdnet"],
            "⑤RSS":          ["python", "collectors/fetch_news.py", "--date", _LATEST_BD_STR, "--mode", "rss"],
            "⑥決算短信XBRL": ["python", "collectors/fetch_statements.py", "--date", _LATEST_BD_STR],
        })
    st.cache_data.clear()
    st.session_state["_fetch_result"] = ("日次データ取得", results)

if _result := st.session_state.get("_fetch_result"):
    _show_run_result(*_result)

with st.expander("🗂 過去分一括取得"):
    st.caption("TDnet 適時開示（CSV）・決算短信 XBRL・OHLCV・決算短信カレンダー を指定期間まとめて取得します。既に CSV が存在する日はスキップします。")

    _hist_c1, _hist_c2, _hist_c3 = st.columns(3)
    with _hist_c1:
        _hist_start = st.date_input("開始日", value=None, key="hist_start", help="取得開始日")
        _hist_end   = st.date_input("終了日", value=None, key="hist_end",  help="取得終了日")
    with _hist_c2:
        _hist_tdnet_clicked = st.button(
            "📥 TDnet 一括取得", width="stretch",
            help="指定期間の TDnet 適時開示 CSV を順番に取得。既存日はスキップ。",
        )
        _hist_xbrl_clicked = st.button(
            "📋 XBRL 一括変換", width="stretch",
            help="指定期間の TDnet CSV から決算短信 XBRL を取得・変換。",
        )
    with _hist_c3:
        _hist_ohlcv_clicked = st.button(
            "📊 OHLCV 一括取得", width="stretch",
            help="株価の5分足を最大59日分フルで再取得。マクロ・財務・ニュースは取得しない。",
        )
        _hist_kessan_clicked = st.button(
            "📅 決算短信カレンダー取得", width="stretch",
            help=f"J-Quants API で決算短信の開示日時を取得（.env に JQUANTS_MAIL / JQUANTS_PASSWORD 要設定）。取得済み: {_kessan_coverage()}",
        )

    if _hist_tdnet_clicked or _hist_xbrl_clicked:
        if not _hist_start or not _hist_end:
            st.warning("開始日と終了日を両方指定してください。")
        elif _hist_end < _hist_start:
            st.warning("終了日は開始日以降を指定してください。")
        else:
            _s = _hist_start.isoformat()
            _e = _hist_end.isoformat()
            if _hist_tdnet_clicked:
                with st.spinner(f"TDnet 一括取得中（{_s} 〜 {_e}）…"):
                    results = _run_parallel({
                        "TDnet一括": [
                            "python", "collectors/fetch_news.py",
                            "--mode", "tdnet",
                            "--start-date", _s, "--end-date", _e,
                        ],
                    })
                st.session_state["_fetch_result"] = ("TDnet 一括取得", results)
            if _hist_xbrl_clicked:
                with st.spinner(f"XBRL 一括変換中（{_s} 〜 {_e}）…"):
                    results = _run_parallel({
                        "XBRL一括": [
                            "python", "collectors/fetch_statements.py",
                            "--start-date", _s, "--end-date", _e,
                        ],
                    })
                st.session_state["_fetch_result"] = ("XBRL 一括変換", results)
            st.rerun()

    if _hist_ohlcv_clicked:
        with st.spinner("OHLCV（5分足フル）を実行中…"):
            results = _run_parallel({
                "①個別株": ["python", "collectors/fetch_prices_stocks.py"],
            })
        st.cache_data.clear()
        st.session_state["_fetch_result"] = ("OHLCV 一括取得", results)
        st.rerun()

    if _hist_kessan_clicked:
        if not _hist_start or not _hist_end:
            st.warning("開始日と終了日を両方指定してください。")
        elif _hist_end < _hist_start:
            st.warning("終了日は開始日以降を指定してください。")
        else:
            _s = _hist_start.isoformat()
            _e = _hist_end.isoformat()
            with st.spinner(f"決算短信カレンダー取得中（{_s} 〜 {_e}）…"):
                results = _run_parallel({
                    "決算短信カレンダー": [
                        "python", "collectors/fetch_kessan_calendar.py",
                        "--start-date", _s, "--end-date", _e,
                    ],
                })
            st.cache_data.clear()
            st.session_state["_fetch_result"] = ("決算短信カレンダー取得", results)
            st.rerun()

st.html('<div style="height: 64px;"></div>')

# ── 説明 ─────────────────────────────────────────────────────
st.markdown("#### 📖 Note")


with st.expander("📋 取得するデータ"):
    st.markdown("""
**OHLCVデータ（yfinance）**

保存先: `data/prices`（銘柄ごと1ファイル）                

- 日足（3年）と5分足（最大59日）の OHLCV を Parquet 形式で銘柄ごとに保存
- 5分足は定期実行して継ぎ足す運用（60日超の過去データは yfinance では取得不可）
- 株式分割時は `fix_split_5min.py` で手動補正（分割日以前の価格・出来高を遡及修正）
- MultiIndex 列を Open/High/Low/Close/Volume にフラット化して保存
- タイムゾーンは処理中 UTC に正規化、保存時は JST インデックス + `datetime_utc` 列の両方を付与

---

**財務データ（EDINET DB API）**

保存先: `data/financials/{code}.csv`（銘柄ごと1ファイル）

| フィールド | 内容 | 用途 |
|:--|:--|:--|
| `eps` | 1株当たり当期純利益（分割調整済み優先） | 予想PERのフォールバック |
| `bps` | 1株当たり純資産（同上） | PBR = 株価 ÷ BPS |
| `dps` | 1株当たり配当金（同上） | 配当利回りのフォールバック |
| `PER` | 株価収益率（API提供値） | バリュエーション参照 |
| `PBR` | ※ None（アプリ側で 株価 ÷ BPS 算出） | — |
| `ROE` | 自己資本利益率（`roe_official` → 計算値） | スクリーニング |
| `net_income` | 当期純利益（金額） | ROE 計算の補完 |
| `net_assets` | 純資産（金額） | ROE・BPS 計算の補完 |
| `shares_issued` | 発行済株式数 | BPS 計算の補完 |
| `fiscal_year` | 決算期 | データ鮮度確認 |
| `submit_date` | 提出日 | データ鮮度確認 |
| `industry` | 業種 | セクター分類補完 |
| `q_eps` | 直近四半期 EPS（速報） | 四半期進捗確認 |
| `q_net_income` | 直近四半期純利益（速報） | 同上 |
| `q_quarter` | 四半期番号 | 同上 |
| `q_disclosure` | 四半期開示日 | 同上 |

※ 財務データは、.env ファイルに保存されているAPIキーと data/financials/_edinet_map.json に保存されているEDINETコードを使って取得。
※ EPS・DPS は **有報ベースの確定実績値**。予想PER・予想配当利回りの算出には決算短信の予想値（`data/statements/`）を優先し、取得できない場合に EDINET DB 値をフォールバックとして使用。
※ レート制限: 100 calls/day（無料プラン）。デフォルトは7日以内取得済みの銘柄をスキップ。

---

**財務指標の優先順位**

PER は EDINET DB の値。PBR は `株価 ÷ BPS` で算出。
配当利回り = `予想DPS ÷ 当日終値 × 100`（配当性向 100% 超は除外）。
`manual_overrides.csv` に登録した値が最優先。

---

**決算短信（XBRL・JSON）**

保存先: `data/statements/{code}_{提出日}_{期種}.json`（銘柄・期種ごと）

TDnet から XBRL ZIP をダウンロードし、`mapping.csv` の定義に従って項目を抽出して JSON に変換。
同一銘柄・同一期種で複数バージョンが存在する場合（監査前→監査後差し替えなど）は、最新版を正本とし旧版で不足項目を補完してマージする。

| フィールド | 内容 | 用途 |
|:--|:--|:--|
| `performance.forecast.eps` | 今期・来期の予想EPS | 予想PER = 株価 ÷ 予想EPS |
| `performance.current.eps` | 今期実績EPS | 予想EPSが取得できない場合のフォールバック |
| `dividend.forecast_next.annual` | 来期予想配当（FY決算時） | 予想配当利回り |
| `dividend.forecast_current.annual` | 今期予想配当（Q1〜Q3時） | 同上 |
| `dividend.actual_current.annual` | 今期実績配当 | 予想が取得できない場合のフォールバック |
| `balance_sheet.current.owners_equity` | 純資産（自己資本） | BPS 計算用 |
| `shares.issued_at_period_end` | 期末発行済株式数 | BPS 計算用 |
| `shares.treasury_at_period_end` | 期末自己株式数 | BPS 計算用（発行済から控除） |
| `metadata.period_type` | 期種（FY / Q1 / Q2 / Q3） | EPS・DPS の優先順位切替 |
| `metadata.fiscal_year_end` | 決算期末日 | バージョン照合・マージ対象の特定 |

※ EPS・DPS は `period_type` に応じて優先するフィールドを切り替える（FY は来期予想優先、Q1〜Q3 は今期予想優先）。
※ BPS は XBRL に直接タグがないため、`owners_equity ÷ (issued - treasury)` で計算。

---

**適時開示（TDnet）**

保存先: `data/news/tdnet/{日付}.csv`（日付ごと1ファイル）

TDnet から開示一覧を日次で取得。既にCSVが存在する日はスキップするため、過去分の一括取得にも対応。
TDnet サーバーの保持期間は数ヶ月程度のため、それ以上の過去データは「決算短信カレンダー」で補完する。

| フィールド | 内容 |
|:--|:--|
| 開示日時 | 開示された日時 |
| 証券コード | 開示企業の証券コード |
| 会社名 | 開示企業名 |
| 開示タイトル | 開示内容のタイトル |
| PDF URL | 開示資料の PDF リンク |

---

**決算短信カレンダー（J-Quants API）**

保存先: `data/news/kessan/{日付}.csv`（日付ごと1ファイル）

JPX の J-Quants API（無料登録）から決算短信の開示日時を取得。TDnet と同一ソースのため日時・銘柄の対応が正確。
TDnet データが存在しない日付は、`03_適時開示・ニュース.py` が自動的にこちらを参照する。

設定: `.env` に `JQUANTS_MAIL` と `JQUANTS_PASSWORD` を記載（`jpx-jquants.com` で無料登録）。

| フィールド | 内容 |
|:--|:--|
| `date` | 開示日 |
| `time` | 開示時刻（実際の TDnet 開示時刻） |
| `code` | 銘柄コード（4桁） |
| `company` | 会社名 |
| `title` | 決算種別（例：決算短信（通期・連結・日本基準）） |
| `pdf_url` | — （J-Quants では取得不可、空文字） |

---

**決算発表予定（J-Quants API）**

保存先: `data/news/kessan_schedule/latest.csv`（取得のたびに上書き）

JPX の J-Quants API `/v1/fins/announcement` エンドポイントで将来の決算発表予定日時を取得。
企業が TDnet に届け出た予定日時のため、発表時刻が確定していない場合は時刻が空になる場合がある。
「📋 決算発表予定」ボタンで随時更新。`03_適時開示・ニュース.py` に一覧表示される。

| フィールド | 内容 |
|:--|:--|
| `date` | 発表予定日 |
| `time` | 発表予定時刻（未確定の場合は空） |
| `code` | 銘柄コード（4桁） |
| `company` | 会社名 |
| `title` | 決算種別（例：決算短信（通期）） |
| `fetched_at` | データ取得日時 |

---

**ニュース（RSS）**

保存先: `data/news/rss/{日付}.csv`（日付ごと1ファイル）

`config/rss_sources.py` に定義したメディアの公式 RSS フィードから記事を取得。既にCSVが存在する日はスキップするため、過去分の一括取得にも対応。

| フィールド | 内容 |
|:--|:--|
| タイトル | 記事タイトル |
| URL | 記事リンク |
| 公開日時 | 記事の掲載日時 |
| メディア名 | 取得元メディア |
""")


with st.expander("📋 ボタン早見表"):
    _btn_df = pd.DataFrame([
        {"ボタン": "📅 日次データ",       "個別株 OHLCV": "3日",   "マクロ": "✓", "財務": "✓", "TDnet": "✓", "RSS": "✓", "決算短信XBRL": "✓"},
        {"ボタン": "🗞 ニュース",         "個別株 OHLCV": "",      "マクロ": "✓", "財務": "",  "TDnet": "✓", "RSS": "✓", "決算短信XBRL": ""},
        {"ボタン": "📊 OHLCV 一括取得",  "個別株 OHLCV": "全期間", "マクロ": "",  "財務": "",  "TDnet": "",  "RSS": "",  "決算短信XBRL": ""},
        {"ボタン": "📥 TDnet 一括取得",  "個別株 OHLCV": "",      "マクロ": "",  "財務": "",  "TDnet": "✓", "RSS": "",  "決算短信XBRL": ""},
        {"ボタン": "📋 XBRL 一括変換",   "個別株 OHLCV": "",      "マクロ": "",  "財務": "",  "TDnet": "",  "RSS": "",  "決算短信XBRL": "✓"},
    ])
    st.dataframe(_btn_df, use_container_width=True, hide_index=True)
    st.caption(
        "※「日次データ」の個別株 OHLCV は日足（3年）+ 5分足（直近3日）。"
        "「一括取得/変換」系ボタンは「過去分一括取得」セクション内に配置されています。"
    )


# ── 検証・補助 ──────────────────────────────────────────────

st.html('<div style="height: 64px;"></div>')

st.markdown("#### 🛠 検証・補助・データ補正")
_sub_c1, _sub_c2 = st.columns(2)
with _sub_c1:
    if st.button(
        "🔎 データ取得検証", width="stretch", type="secondary",
        help="全 parquet を走査して最新日・欠損・株式分割疑いをチェック",
    ):
        with st.spinner("検証中…"):
            st.session_state["_verify_report"] = _build_verify_report()
with _sub_c2:
    if st.button(
        "🏷 セクター CSV 再生成", width="stretch", type="secondary",
        help="data/master/sectors/ を topix_all.csv から再生成",
    ):
        with st.spinner("セクター CSV を再生成中…"):
            rc, out = _run(["python", "collectors/build_sectors.py"])
        st.session_state["_fetch_result"] = ("セクター CSV 再生成", {"build_sectors": (rc, out)})
        st.rerun()

if _rep := st.session_state.get("_verify_report"):
    st.html('<div style="height: 16px;"></div>')
    _render_verify_report(_rep)



with st.expander("🔀 株式分割 5分足補正"):
    st.caption(
        "株式分割が発生した銘柄の 5 分足 Parquet に遡及補正を適用します。"
        "補正前に必ず「ドライラン」で内容を確認してください。"
        "補正ログは `data/master/split_corrections.csv` に記録されます。"
    )
    _sp_c1, _sp_c2, _sp_c3 = st.columns(3)
    with _sp_c1:
        _sp_code  = st.text_input("証券コード", placeholder="例: 8001", key="sp_code")
        _sp_date  = st.text_input("分割日（YYYY-MM-DD）", placeholder="例: 2026-01-15", key="sp_date")
    with _sp_c2:
        _sp_ratio = st.text_input("分割比率 A:B", placeholder="例: 1:5（1株→5株）", key="sp_ratio")
        _sp_dry   = st.checkbox("ドライラン（書き込みなし）", value=True, key="sp_dry")
    with _sp_c3:
        _sp_nobak = st.checkbox("バックアップなし", value=False, key="sp_nobak")
        _sp_run   = st.button("▶ 実行", type="primary", key="sp_run")

    if _sp_run:
        if not _sp_code or not _sp_date or not _sp_ratio:
            st.warning("証券コード・分割日・分割比率をすべて入力してください。")
        else:
            _sp_cmd = [
                "python", "collectors/fix_split_5min.py",
                "--code", _sp_code.strip(),
                "--split-date", _sp_date.strip(),
                "--split", _sp_ratio.strip(),
            ]
            if _sp_dry:
                _sp_cmd.append("--dry-run")
            if _sp_nobak:
                _sp_cmd.append("--no-backup")
            _label = f"{'[DRY-RUN] ' if _sp_dry else ''}株式分割補正 {_sp_code} ({_sp_ratio}, {_sp_date})"
            with st.spinner(_label + "…"):
                _sp_rc, _sp_out = _run(_sp_cmd)
            _icon   = "✅" if _sp_rc == 0 else "❌"
            _status = "完了" if _sp_rc == 0 else f"失敗（exit={_sp_rc}）"
            with st.expander(f"{_icon} {_label} — {_status}", expanded=True):
                st.code(_sp_out or "(出力なし)", language="text")

with st.expander("財務指標を手動で上書きする方法（株式分割・配当予想修正など）"):
    st.markdown("""
###  財務指標の手動上書き

自動取得された財務データが実態と乖離している場合（株式分割・配当予想修正など）は、`data/master/manual_overrides.csv` のCSVに行を追加することで最優先で上書きできます。

#### ファイルの場所
```
data/master/manual_overrides.csv
```

#### 列の説明

| 列 | 説明 | 例 |
|---|---|---|
| `code` | 証券コード（4桁） | `8001` |
| `fiscal_year_end` | 対象の決算期末日 | `2025-03-31` |
| `key` | 上書きするフィールド名 | `forecast_dps_annual` |
| `value` | 上書きする値 | `40.0` |
| `note` | 理由・メモ | `1:5 株式分割による補正` |
| `source` | 情報源（URL等） | `https://...` |
| `updated_at` | 登録日 | `2026-04-30` |

#### 対応している `key` の種類

| key | 効果 |
|---|---|
| `forecast_dps_annual` | 配当利回りを `DPS ÷ 現在株価` で再計算 |
| `bps` | PBR を `現在株価 ÷ BPS` で再計算 |
| `eps` | PER を `現在株価 ÷ EPS` で再計算 |
| `per` | PER を直接上書き |
| `pbr` | PBR を直接上書き |
| `roe` | ROE を直接上書き |

#### よくある使用例

**① 株式分割で BPS・DPS がずれている場合**

分割後の株価に対して分割前の BPS・DPS が使われると、PBR が過小・配当利回りが過大になります。
分割比率で割った値を `bps` と `forecast_dps_annual` に登録してください。

```
# 例: 8001 伊藤忠（2026-01-01付 1:5 分割）
8001,2025-03-31,bps,811.84,1:5分割補正（4059.19÷5）,,2026-04-30
8001,2025-03-31,forecast_dps_annual,40.0,1:5分割補正（200÷5）,,2026-04-30
```

**② 配当予想が修正された場合**

有価証券報告書の実績 DPS より新しい予想配当が発表されている場合は `forecast_dps_annual` で上書きします。

```
# 例: 5020 ENEOS（FY2026予想配当34円）
5020,2025-03-31,forecast_dps_annual,34.0,FY2026予想配当（有報実績26円から更新）,,2026-04-30
```

#### 注意事項
- `fiscal_year_end` が一致する行が優先されます。一致する行がない場合は最新の行が使われます。
- PER は株式分割の前後で変わらないため、分割補正では通常 `bps` と `forecast_dps_annual` の登録のみで十分です。
- キャッシュが残っている場合はアプリを再起動するか、ブラウザをリロードしてください。
""")

st.html('<div style="height: 64px;"></div>')
