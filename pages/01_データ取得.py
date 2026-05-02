"""
データ取得 UI
"""
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from config.paths import PRICES_STOCKS_DAILY, PRICES_MACRO_DAILY
from utils.date_utils import latest_business_day

st.set_page_config(
    page_title="データ取得ツール",
    page_icon="🔄",
    initial_sidebar_state="expanded",
)

st.title("🔄 データ取得ツール")
st.html('<div style="height: 32px;"></div>')

# ── subprocess ヘルパー ──────────────────────────────────────────
_ENV  = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
_ROOT = Path(__file__).resolve().parent.parent


def _run(cmd: list[str]) -> tuple[int, str]:
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


def _tail(text: str, n: int = 30) -> str:
    lines = text.strip().splitlines()
    return "\n".join(lines[-n:]) if lines else ""


def _show_result(label: str, rc: int, out: str) -> None:
    icon   = "✅" if rc == 0 else "❌"
    status = "完了" if rc == 0 else f"失敗（exit={rc}）"
    with st.expander(f"{icon} {label} — {status}", expanded=(rc != 0)):
        st.code(_tail(out) or "(出力なし)", language="text")


_TODAY = latest_business_day().isoformat()

# ── データ取得検証 ──────────────────────────────────────────────
# 連続日で終値比がこの値以下、または逆数以上なら株式分割・統合の疑い。
# 0.55 = 約 45% 以上の急変動。2:1 分割（≒0.5）と 1:2 併合（≒2.0）を捕捉できる。
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
                    suspicious = ratio[
                        (ratio < _SPLIT_RATIO_THRESHOLD) | (ratio > 1.0 / _SPLIT_RATIO_THRESHOLD)
                    ]
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
        "today":             today,
        "total_stocks":      len(stock_files),
        "total_macros":      len(macro_files),
        "oldest_stock_date": oldest_stock_date,
        "latest_stock_date": latest_stock_date,
        "latest_macro_date": latest_macro_date,
        "split_warnings":    split_warnings,
        "nan_warnings":      nan_warnings,
        "stale_warnings":    stale_warnings,
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

# ── ボタン ───────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.markdown("**株価・マクロ**")

    if st.button("📈 OHLCVデータ（銘柄）", width="stretch",
                 help="yfinance で東証銘柄の日足（3年）・5分足（直近59日）を取得。\n保存先: data/prices/stocks/"):
        with st.spinner("OHLCVデータ（銘柄）を取得中…"):
            rc, out = _run(["python", "collectors/fetch_prices_stocks.py"])
        st.cache_data.clear()
        _show_result("OHLCVデータ（銘柄）", rc, out)

    if st.button("📊 OHLCVデータ（マクロ指標）", width="stretch",
                 help="yfinance で日経225・S&P500・VIX・ドル円など主要指標の日足・5分足を取得。\n保存先: data/prices/macro/"):
        with st.spinner("OHLCVデータ（マクロ指標）を取得中…"):
            rc, out = _run(["python", "collectors/fetch_prices_macro.py"])
        st.cache_data.clear()
        _show_result("OHLCVデータ（マクロ指標）", rc, out)

    st.html('<div style="height: 16px;"></div>')
    st.markdown("**財務・決算**")

    if st.button("💹 財務データ（EDINET DB API）", width="stretch",
                 help="EDINET DB API で EPS・BPS・DPS・ROE 等を取得。APIキー（EDINETDB_API_KEY）が必要。\n保存先: data/financials/"):
        with st.spinner("財務データを取得中…"):
            rc, out = _run(["python", "collectors/fetch_financials.py"])
        _show_result("財務データ（EDINET DB API）", rc, out)

    if st.button("📋 決算短信（XBRL取得）", width="stretch",
                 help=f"TDnet から決算短信の XBRL ZIP をダウンロード（対象日: {_TODAY}）。\n保存先: data/statements_zip/"):
        with st.spinner("決算短信 XBRL を取得中…"):
            rc, out = _run(["python", "collectors/fetch_statements.py", "--date", _TODAY])
        _show_result("決算短信（XBRL取得）", rc, out)

    if st.button("🔄 決算短信（JSON変換）", width="stretch",
                 help="ダウンロード済みの XBRL ZIP を JSON に一括変換（mapping.csv に基づく）。\n保存先: data/statements/"):
        with st.spinner("XBRL → JSON 変換中…"):
            rc, out = _run(["python", "collectors/xbrl_to_json.py", "--all"])
        _show_result("決算短信（JSON変換）", rc, out)

with col2:
    st.markdown("**ニュース・開示**")

    if st.button("📰 ニュース（RSS）", width="stretch",
                 help=f"各メディアの RSS フィードから記事を取得（対象日: {_TODAY}）。\n保存先: data/news/rss/"):
        with st.spinner("RSS ニュースを取得中…"):
            rc, out = _run(["python", "collectors/fetch_news.py",
                            "--date", _TODAY, "--mode", "rss"])
        _show_result("ニュース（RSS）", rc, out)

    if st.button("📢 適時開示（TDnet）", width="stretch",
                 help=f"TDnet から適時開示一覧を取得（対象日: {_TODAY}）。\n保存先: data/news/tdnet/"):
        with st.spinner("TDnet 適時開示を取得中…"):
            rc, out = _run(["python", "collectors/fetch_news.py",
                            "--date", _TODAY, "--mode", "tdnet"])
        _show_result("適時開示（TDnet）", rc, out)

st.html('<div style="height: 32px;"></div>')

# ── 検証・補助・データ補正 ───────────────────────────────────────
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
            _sec_rc, _sec_out = _run(["python", "collectors/build_sectors.py"])
        _show_result("セクター CSV 再生成", _sec_rc, _sec_out)

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


st.html('<div style="height: 32px;"></div>')

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

| フィールド | 内容 |
|:--|:--|
| 開示日時 | 開示された日時 |
| 証券コード | 開示企業の証券コード |
| 会社名 | 開示企業名 |
| 開示タイトル | 開示内容のタイトル |
| PDF URL | 開示資料の PDF リンク |

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