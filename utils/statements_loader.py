"""
決算短信 JSON / Markdown ローダー

xbrl-converter が出力した `data/statements/` 配下の JSON。
ファイル名は `<code>_<fiscal_year_end>_<period>.json` 形式。
訂正再開示は同名ファイルに上書き保存（最新版のみ保持）。

公開 API:
    list_statements(code, period=None)               -> list[Path]
    latest_statement_path(code, period=None)         -> Path | None
    load_latest_statement(code, period=None)         -> dict | None
    load_latest_statement_merged(code, period=None)  -> dict | None  # ★推奨
    list_revisions(code, period=None)                -> list[dict]
    latest_statement_md_path(code, period=None)      -> Path | None

`load_latest_statement_merged` は最新版を base として、旧版にあって
最新版に「無い情報」（キー欠落・null・空文字・空 dict）を補完したものを返す。
監査前版にしか存在しない `audit.reviewed_by_audit_firm` 等の情報が
失われない設計。
"""
from __future__ import annotations
import json
import re
from pathlib import Path

from config.paths import STATEMENTS, STATEMENTS_MD


# ファイル名パターン: <code>_<fiscal_year_end>_<period>.json
# 例: 8001_2026-03-31_FY.json
_NAME_RE = re.compile(r"^(?P<code>\w+)_(?P<fiscal_year_end>\d{4}-\d{2}-\d{2})_(?P<period>FY|Q1|Q2|Q3)\b")


def _parse_filename(path: Path) -> dict | None:
    m = _NAME_RE.match(path.stem)
    if not m:
        return None
    return {
        "code":            m.group("code"),
        "fiscal_year_end": m.group("fiscal_year_end"),
        "period":          m.group("period"),
        "path":            path,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 一覧
# ──────────────────────────────────────────────────────────────────────────────

def _read_fiscal_year_end(path: Path) -> str | None:
    """JSON から metadata.fiscal_year_end を読み取る（軽量読み込み）。"""
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d.get("metadata", {}).get("fiscal_year_end")
    except Exception:
        return None


def list_statements(code: str, period: str | None = None,
                    fiscal_year_end: str | None = None) -> list[Path]:
    """
    指定銘柄（・期種・会計年度末）の JSON ファイル一覧を fiscal_year_end 昇順で返す。
    """
    if not STATEMENTS.exists():
        return []
    matches: list[dict] = []
    for p in STATEMENTS.glob(f"{code}_*.json"):
        info = _parse_filename(p)
        if info is None:
            continue
        if period and info["period"] != period:
            continue
        if fiscal_year_end and info["fiscal_year_end"] != fiscal_year_end:
            continue
        matches.append(info)
    matches.sort(key=lambda x: x["fiscal_year_end"])
    return [m["path"] for m in matches]


def list_revisions(code: str, period: str | None = None,
                   fiscal_year_end: str | None = None) -> list[dict]:
    """
    一覧を返す。各エントリ: {code, fiscal_year_end, period, path}。
    fiscal_year_end 昇順。
    """
    if not STATEMENTS.exists():
        return []
    matches: list[dict] = []
    for p in STATEMENTS.glob(f"{code}_*.json"):
        info = _parse_filename(p)
        if info is None:
            continue
        if period and info["period"] != period:
            continue
        if fiscal_year_end and info["fiscal_year_end"] != fiscal_year_end:
            continue
        matches.append(info)
    matches.sort(key=lambda x: x["fiscal_year_end"])
    return matches


# ──────────────────────────────────────────────────────────────────────────────
# 最新版（filing_date が最大のもの）
# ──────────────────────────────────────────────────────────────────────────────

def latest_statement_path(code: str, period: str | None = None) -> Path | None:
    """同一(銘柄, 期種) で最新 fiscal_year_end の JSON パスを返す。"""
    files = list_statements(code, period)
    return files[-1] if files else None


def load_latest_statement(code: str, period: str | None = None) -> dict | None:
    """最新の JSON を辞書として読み込んで返す（マージなし）。無ければ None。"""
    path = latest_statement_path(code, period)
    if path is None:
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_latest_statement_merged(code: str, period: str | None = None,
                                 fiscal_year_end: str | None = None) -> dict | None:
    """
    指定銘柄・期種・会計年度末の最新 JSON を返す。

    ファイル名が {code}_{fiscal_year_end}_{period}.json 形式のため、
    同一年度・期種のファイルは常に1つ（訂正は上書き）。
    fiscal_year_end 省略時は最新年度のファイルを返す。
    """
    path = latest_statement_path(code, period) if fiscal_year_end is None \
        else (list_statements(code, period, fiscal_year_end) or [None])[-1]
    if path is None:
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ──────────────────────────────────────────────────────────────────────────────
# Markdown 版
# ──────────────────────────────────────────────────────────────────────────────

def latest_statement_md_path(code: str, period: str | None = None) -> Path | None:
    """対応する Markdown ファイルの最新版パスを返す。"""
    if not STATEMENTS_MD.exists():
        return None
    matches: list[dict] = []
    for p in STATEMENTS_MD.glob(f"{code}_*.md"):
        info = _parse_filename(p)
        if info is None:
            continue
        if period and info["period"] != period:
            continue
        matches.append(info)
    if not matches:
        return None
    matches.sort(key=lambda x: x["date"])
    return matches[-1]["path"]


def load_latest_statement_md(code: str, period: str | None = None) -> str | None:
    """最新版の Markdown 本文を文字列として返す。"""
    path = latest_statement_md_path(code, period)
    if path is None:
        return None
    return path.read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────────
# 1株あたり指標（EPS / BPS / DPS）抽出
# ──────────────────────────────────────────────────────────────────────────────

def extract_per_share_data(code: str) -> dict:
    """
    最新の決算短信JSONからEPS/BPS/DPSを抽出して返す。
    取得できなかった項目はNone。

    EPS優先順位:
      FY    : performance.forecast.eps（来期予想）→ performance.current.eps（今期実績）
      Q1-Q3 : performance.forecast.eps（今期予想）→ lower/upper平均 → performance.current.eps
    DPS優先順位:
      FY    : dividend.forecast_next.annual（来期予想）→ dividend.actual_current.annual（今期実績）
      Q1-Q3 : dividend.forecast_current.annual（今期予想）→ lower/upper平均 → actual_current.annual

    返り値: {"eps": float|None, "bps": float|None, "dps": float|None}
    """
    stmt = load_latest_statement_merged(code)
    if stmt is None:
        return {"eps": None, "bps": None, "dps": None}

    def _f(v) -> float | None:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    period_type = (stmt.get("metadata") or {}).get("period_type", "")
    perf_fc  = (stmt.get("performance") or {}).get("forecast") or {}
    perf_cur = (stmt.get("performance") or {}).get("current")  or {}

    # EPS: 予想を優先（予想PERに使用）、フォールバックは実績
    if period_type == "FY":
        # 来期予想EPS → 今期実績EPS
        eps = _f(perf_fc.get("eps"))
        if eps is None:
            eps = _f(perf_cur.get("eps"))
    else:
        # 今期予想EPS → レンジ平均 → 今期実績EPS
        eps = _f(perf_fc.get("eps"))
        if eps is None:
            fc_eps_lower = _f((perf_fc.get("lower") or {}).get("eps"))
            fc_eps_upper = _f((perf_fc.get("upper") or {}).get("eps"))
            if fc_eps_lower is not None and fc_eps_upper is not None:
                eps = (fc_eps_lower + fc_eps_upper) / 2
            else:
                eps = fc_eps_lower if fc_eps_lower is not None else fc_eps_upper
        if eps is None:
            eps = _f(perf_cur.get("eps"))

    # BPS = owners_equity / (発行済み株式数 - 自己株式数)
    bps = None
    bs = (stmt.get("balance_sheet") or {}).get("current") or {}
    sh = stmt.get("shares") or {}
    owners_equity = _f(bs.get("owners_equity"))
    issued   = _f(sh.get("issued_at_period_end"))
    treasury = _f(sh.get("treasury_at_period_end")) or 0.0
    if owners_equity and issued and (issued - treasury) > 0:
        bps = owners_equity / (issued - treasury)

    # DPS: period_type によって優先順位を変える
    #   FY  : actual_current.annual（確定実績）優先
    #         ← Q3サプリメントの forecast が merge されても使わない
    #   Q1-Q3: forecast_current.annual（今期予想）優先 → 実績にフォールバック
    div = stmt.get("dividend") or {}
    fc  = div.get("forecast_current") or {}
    ac  = div.get("actual_current")   or {}

    def _fc_val(key: str) -> float | None:
        """lower/upper はフラット値またはネスト dict {"annual": v} の両形式に対応"""
        v = fc.get(key)
        if isinstance(v, dict):
            return _f(v.get("annual"))
        return _f(v)

    if period_type == "FY":
        # 来期予想 → 今期実績（来期予想を発表していない企業はフォールバック）
        fc_next = (stmt.get("dividend") or {}).get("forecast_next") or {}
        dps = _f(fc_next.get("annual"))
        if dps is None:
            dps = _f(ac.get("annual"))
    else:
        # Q1-Q3: 今期予想 → lower/upper 平均 → 実績の順
        dps = _f(fc.get("annual"))
        if dps is None:
            fc_lower = _fc_val("lower")
            fc_upper = _fc_val("upper")
            if fc_lower is not None and fc_upper is not None:
                dps = (fc_lower + fc_upper) / 2
            else:
                dps = fc_lower if fc_lower is not None else fc_upper
        if dps is None:
            dps = _f(ac.get("annual"))

    return {"eps": eps, "bps": bps, "dps": dps}
