"""
utils.financial_metrics — 有報 JSON → 財務指標計算

使い方:
    from utils.financial_metrics import build_financials_df, build_valuation, get_current_price

    df   = build_financials_df("E00041")      # 5期分指標 DataFrame
    val  = build_valuation("E00041", "1662")  # PER・PBR・配当利回り・時価総額
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from config.paths import YUHO, PRICES_STOCKS_DAILY
from utils.statements_loader import extract_per_share_data

# 期間キーを古い順に並べる
_PERIOD_ORDER = ["prior4", "prior3", "prior2", "prior1", "current"]


# ══════════════════════════════════════════════════════════════════
# JSON ローダー
# ══════════════════════════════════════════════════════════════════

def load_yuho_all(edinet_code: str) -> list[dict]:
    """edinet_code に対応する全 JSON を filing_date 昇順で返す。"""
    dir_path = YUHO / edinet_code
    if not dir_path.exists():
        return []
    jsons = []
    for p in sorted(dir_path.glob("*.json")):
        try:
            jsons.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            continue
    jsons.sort(key=lambda x: x.get("metadata", {}).get("filing_date", ""))
    return jsons


def load_yuho_latest(edinet_code: str) -> dict | None:
    """最新（filing_date が最大）の有報 JSON を返す。"""
    jsons = load_yuho_all(edinet_code)
    return jsons[-1] if jsons else None


# ══════════════════════════════════════════════════════════════════
# 指標計算ヘルパー
# ══════════════════════════════════════════════════════════════════

def _safe_div(numerator: Any, denominator: Any) -> float | None:
    """ゼロ除算・None を安全に処理した除算。"""
    try:
        if numerator is None or denominator is None or denominator == 0:
            return None
        return numerator / denominator
    except (TypeError, ZeroDivisionError):
        return None


def _pct(value: Any) -> float | None:
    """小数 → % 変換。既に % 値（>1）の場合はそのまま返す。"""
    if value is None:
        return None
    # equity_ratio・roe は 0.749 形式で格納されている
    return round(value * 100, 2) if abs(value) <= 2 else round(float(value), 2)


def _growth(current: Any, prior: Any) -> float | None:
    """前期比成長率（%）。"""
    if current is None or prior is None or prior == 0:
        return None
    return round((current - prior) / abs(prior) * 100, 1)


def _fiscal_year_end_approx(base_end: str, offset_years: int) -> str:
    """current の期末日から offset_years 年前の期末日を概算する。"""
    if not base_end:
        return ""
    try:
        d = date.fromisoformat(base_end)
        year = d.year - offset_years
        # うるう年2/29対策
        try:
            return date(year, d.month, d.day).isoformat()
        except ValueError:
            return date(year, d.month, 28).isoformat()
    except (ValueError, TypeError):
        return ""


# ══════════════════════════════════════════════════════════════════
# メイン API
# ══════════════════════════════════════════════════════════════════

def build_financials_df(edinet_code: str) -> pd.DataFrame:
    """
    5期分の財務データを整形した DataFrame を返す（古い順）。

    主要列:
      fiscal_year_end, accounting_standard
      net_sales, operating_income, ordinary_income, net_income  (円)
      eps, bps, dps  (円/株)
      equity_ratio, roe, roa  (%)
      operating_margin, net_margin  (%)
      de_ratio  (倍)
      operating_cf, investing_cf, financing_cf, fcf  (円)
      net_sales_growth, net_income_growth  (%, 前期比)
    """
    data = load_yuho_latest(edinet_code)
    if data is None:
        return pd.DataFrame()

    meta    = data.get("metadata", {})
    summary = data.get("summary_5yr", {})
    std     = meta.get("accounting_standard", "JP")
    base_end = meta.get("fiscal_year_end", "")

    # 期間キーごとのオフセット（current=0, prior1=1, ...）
    offsets = {"prior4": 4, "prior3": 3, "prior2": 2, "prior1": 1, "current": 0}

    rows = []
    for key in _PERIOD_ORDER:
        period = summary.get(key)
        if period is None:
            continue

        ns  = period.get("net_sales")
        oi  = period.get("operating_income")   # IFRS 営業利益 / JP は None
        ordi = period.get("ordinary_income")   # JP GAAP 経常利益
        pbt = period.get("profit_before_tax")  # IFRS 税引前利益
        ni  = period.get("net_income")
        na  = period.get("net_assets")
        ta  = period.get("total_assets")
        cash = period.get("cash_end")

        eps = period.get("eps")
        bps = period.get("bps")
        dps = period.get("dps")

        gp  = period.get("gross_profit")
        sga = period.get("sga_expense")
        ibd_cur = period.get("interest_bearing_debt_current")
        ibd_ncl = period.get("interest_bearing_debt_noncurrent")
        bonds   = period.get("bonds_payable")
        tax_exp = period.get("income_tax_expense")

        ocf = period.get("operating_cf")
        icf = period.get("investing_cf")
        fcf = (_safe_div(ocf, 1) or 0) + (icf or 0) if (ocf is not None and icf is not None) else None

        # ROA: 純利益 / 総資産
        roa = _pct(_safe_div(ni, ta))

        # 粗利率
        gross_margin = _pct(_safe_div(gp, ns))

        # 利益率: JP GAAP は ordinary_income を営業利益代替として使用
        income_for_margin = oi if oi is not None else ordi
        op_margin  = _pct(_safe_div(income_for_margin, ns))
        net_margin = _pct(_safe_div(ni, ns))

        # 有利子負債合計（流動 + 固定 + 社債）
        ibd_total: float | None = None
        ibd_parts = [v for v in [ibd_cur, ibd_ncl, bonds] if v is not None]
        if ibd_parts:
            ibd_total = sum(ibd_parts)

        # D/E レシオ（有利子負債 / 純資産）— 有利子負債が取れた場合はそちらを優先
        if ibd_total is not None and na:
            de_ratio = round(ibd_total / na, 2)
        elif ta and na:
            de_ratio = round(((ta or 0) - (na or 0)) / na, 2)  # 負債合計での概算
        else:
            de_ratio = None

        # 総合商社等で oi が None の場合: 粗利 - 販管費 で近似
        if oi is None and gp is not None and sga is not None:
            oi = gp - sga

        # ROIC = NOPAT / 投下資本
        # 実効税率: 法人税費用 / 税引前利益。取れない場合は 30% を使用
        roic = None
        if oi is not None and na is not None:
            if pbt and pbt != 0 and tax_exp is not None:
                tax_rate = min(max(tax_exp / pbt, 0.0), 0.60)
            else:
                tax_rate = 0.30
            nopat = oi * (1 - tax_rate)
            # 投下資本 = 純資産 + 有利子負債 - 現金
            ibd_for_ic = ibd_total if ibd_total is not None else 0
            invested_capital = na + ibd_for_ic - (cash or 0)
            roic = _pct(_safe_div(nopat, invested_capital))

        fy_end = _fiscal_year_end_approx(base_end, offsets[key])

        rows.append({
            "period_key":         key,
            "fiscal_year_end":    fy_end,
            "accounting_standard": std,
            # PL
            "net_sales":          ns,
            "gross_profit":       gp,
            "operating_income":   oi,    # IFRS: 営業利益 / JP GAAP: None
            "ordinary_income":    ordi,  # JP GAAP: 経常利益 / IFRS: None
            "profit_before_tax":  pbt,   # IFRS: 税引前利益
            "net_income":         ni,
            # 1株指標
            "eps":                eps,
            "bps":                bps,
            "dps":                dps,
            # BS
            "net_assets":         na,
            "total_assets":       ta,
            "cash_end":           cash,
            "interest_bearing_debt": ibd_total,
            # 指標（% 換算済み）
            "equity_ratio":       _pct(period.get("equity_ratio")) or _pct(_safe_div(na, ta)),
            "roe":                _pct(period.get("roe")),
            "roa":                roa,
            "roic":               roic,
            "gross_margin":       gross_margin,
            "operating_margin":   op_margin,
            "net_margin":         net_margin,
            "de_ratio":           de_ratio,
            # CF
            "operating_cf":       ocf,
            "investing_cf":       icf,
            "financing_cf":       period.get("financing_cf"),
            "fcf":                fcf,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # 成長率（前行との比較）
    df["net_sales_growth"]   = [None] + [
        _growth(df["net_sales"].iloc[i], df["net_sales"].iloc[i - 1])
        for i in range(1, len(df))
    ]
    df["net_income_growth"]  = [None] + [
        _growth(df["net_income"].iloc[i], df["net_income"].iloc[i - 1])
        for i in range(1, len(df))
    ]
    income_col = "operating_income" if std == "IFRS" else "ordinary_income"
    df["operating_income_growth"] = [None] + [
        _growth(df[income_col].iloc[i], df[income_col].iloc[i - 1])
        for i in range(1, len(df))
    ]

    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════
# 株価取得
# ══════════════════════════════════════════════════════════════════

def get_current_price(sec_code: str) -> float | None:
    """日足 Parquet から最新終値を返す。ファイルがなければ None。"""
    path = PRICES_STOCKS_DAILY / f"{sec_code}.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=["Close"])
        closes = df["Close"].dropna()
        return float(closes.iloc[-1]) if not closes.empty else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════
# バリュエーション
# ══════════════════════════════════════════════════════════════════

def build_valuation(edinet_code: str, sec_code: str, price: float | None = None) -> dict:
    """
    決算短信（予想優先）+ 有報 BPS と株価から市場評価指標を返す。

    EPS・DPS は extract_per_share_data() が「予想 → 実績」の優先順位で取得。
    BPS は有報の実績値を使用（BSに予想なし）。
    price を省略すると get_current_price(sec_code) で自動取得する。

    返却キー:
      price, per, pbr, dividend_yield (%), market_cap (円), shares_approx (株)
      eps_source: "forecast" | "actual"  (EPSの出所)
      dps_source: "forecast" | "actual"  (DPSの出所)
    """
    # 決算短信から予想 EPS・DPS（BPS も取得されるが有報 BPS を優先）
    ps = extract_per_share_data(sec_code)
    eps = ps.get("eps")
    dps = ps.get("dps")

    # BPS は有報の最新実績を使用
    data = load_yuho_latest(edinet_code)
    summary = data.get("summary_5yr", {}) if data else {}
    current = summary.get("current", {})
    bps = current.get("bps") or ps.get("bps")
    na  = current.get("net_assets")

    if price is None:
        price = get_current_price(sec_code)

    result: dict[str, Any] = {"price": price}

    if price:
        result["per"] = round(price / eps, 2) if eps and eps > 0 else None
        result["pbr"] = round(price / bps, 2) if bps and bps > 0 else None
        result["dividend_yield"] = round(dps / price * 100, 2) if dps and price else None

        # 発行済株式数の概算: 純資産(円) ÷ BPS(円/株)
        if na and bps and bps > 0:
            shares_approx = int(na / bps)
            result["shares_approx"] = shares_approx
            result["market_cap"]    = int(price * shares_approx)
        else:
            result["shares_approx"] = None
            result["market_cap"]    = None
    else:
        result.update({"per": None, "pbr": None, "dividend_yield": None,
                       "shares_approx": None, "market_cap": None})

    return result
