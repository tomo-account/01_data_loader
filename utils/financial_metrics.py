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

from config.paths import YUHO, PRICES_STOCKS_DAILY, STOCK_SPLITS
from utils.statements_loader import extract_per_share_data


def _load_split_ratios() -> dict[str, float]:
    """STOCK_SPLITS CSVから {edinet_code: 累積分割比率} を返す。"""
    if not STOCK_SPLITS.exists():
        return {}
    ratios: dict[str, float] = {}
    with STOCK_SPLITS.open(encoding="utf-8") as f:
        import csv
        for row in csv.DictReader(f):
            code  = row.get("edinet_code", "").strip()
            ratio = row.get("ratio", "").strip()
            if code and ratio:
                ratios[code] = ratios.get(code, 1.0) * float(ratio)
    return ratios

# 期間キーを古い順に並べる
_PERIOD_ORDER = ["prior4", "prior3", "prior2", "prior1", "current"]


# ══════════════════════════════════════════════════════════════════
# JSON ローダー
# ══════════════════════════════════════════════════════════════════

def load_yuho_all(edinet_code: str) -> list[dict]:
    """edinet_code に対応する全 JSON を fiscal_year_end 昇順で返す。"""
    dir_path = YUHO / edinet_code
    if not dir_path.exists():
        return []
    jsons = []
    for p in sorted(dir_path.glob("*.json")):
        try:
            jsons.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            continue
    jsons.sort(key=lambda x: x.get("metadata", {}).get("fiscal_year_end", ""))
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

def _iter_periods_from_file(data: dict) -> list[tuple[str, str, dict]]:
    """JSON 1件から (filing_date, fiscal_year_end, 財務データ) のリストを返す。

    新フォーマット（financials キー）: 1年分のみ
    旧フォーマット（summary_5yr キー）: 5年分を period_key から fiscal_year_end を算出して展開
    """
    meta = data.get("metadata", {})
    fd   = meta.get("filing_date", "")

    if "financials" in data:
        fy = meta.get("fiscal_year_end", "")
        return [(fd, fy, data["financials"])]

    # 旧フォーマット
    base_end = meta.get("fiscal_year_end", "")
    offsets  = {"prior4": 4, "prior3": 3, "prior2": 2, "prior1": 1, "current": 0}
    result: list[tuple[str, str, dict]] = []
    for key, period in data.get("summary_5yr", {}).items():
        offset = offsets.get(key, 0)
        fy = _fiscal_year_end_approx(base_end, offset)
        result.append((fd, fy, period))
    return result


def build_financials_df(edinet_code: str) -> pd.DataFrame:
    """
    全年度 JSON から財務 DataFrame を返す（fiscal_year_end 昇順）。

    新フォーマット（{edinet_code}_{fy_end}.json / financials キー）と
    旧フォーマット（{doc_id}.json / summary_5yr キー）の両方に対応する。
    同一 fiscal_year_end が複数ファイルに存在する場合は filing_date が
    最新のものを採用する。

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
    jsons = load_yuho_all(edinet_code)
    if not jsons:
        return pd.DataFrame()

    # fiscal_year_end → (filing_date, period_data, accounting_standard)
    # filing_date が最新のエントリで上書き
    per_year: dict[str, tuple[str, dict, str]] = {}
    for data in jsons:
        meta = data.get("metadata", {})
        std  = meta.get("accounting_standard", "JP")
        for fd, fy, period in _iter_periods_from_file(data):
            if not fy:
                continue
            if fy not in per_year or fd > per_year[fy][0]:
                per_year[fy] = (fd, period, std)

    rows = []
    for fy_end in sorted(per_year):
        _, period, std = per_year[fy_end]
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
        ibd_cur  = period.get("interest_bearing_debt_current")
        ibd_ncl  = period.get("interest_bearing_debt_noncurrent")
        bonds    = period.get("bonds_payable")
        cp       = period.get("commercial_papers")
        cur_bond = period.get("current_portion_of_bonds")
        lease_cl = period.get("lease_liabilities_current")
        lease_nc = period.get("lease_liabilities_noncurrent")
        tax_exp  = period.get("income_tax_expense")

        # BS 詳細
        cur_assets  = period.get("current_assets")
        cur_liab    = period.get("current_liabilities")
        ncur_assets = period.get("noncurrent_assets")
        ncur_liab   = period.get("noncurrent_liabilities")
        trade_rec   = period.get("trade_receivables")
        inventories = period.get("inventories")
        trade_pay   = period.get("trade_payables")
        ppe         = period.get("ppe")
        goodwill    = period.get("goodwill")
        intangibles = period.get("intangible_assets")
        inv_sec     = period.get("investment_securities")
        eq_inv      = period.get("equity_method_investments")
        treasury    = period.get("treasury_stock")
        nci         = period.get("noncontrolling_interests")

        ocf = period.get("operating_cf")
        icf = period.get("investing_cf")

        da        = period.get("depreciation")
        capex_ppe = period.get("capex_ppe")
        capex_int = period.get("capex_intangible")
        capex: int | float | None = None
        if capex_ppe is not None:
            capex = capex_ppe + (capex_int or 0)

        # FCF = 営業CF − CAPEX。CAPEX未取得時は投資CF全体で代替
        if ocf is not None and capex is not None:
            fcf = ocf - abs(capex)
        elif ocf is not None and icf is not None:
            fcf = ocf + icf
        else:
            fcf = None

        # ROA: 純利益 / 総資産
        roa = _pct(_safe_div(ni, ta))

        # 粗利率
        gross_margin = _pct(_safe_div(gp, ns))

        # 利益率: JP GAAP は ordinary_income を営業利益代替として使用
        income_for_margin = oi if oi is not None else ordi
        op_margin  = _pct(_safe_div(income_for_margin, ns))
        net_margin = _pct(_safe_div(ni, ns))

        # 有利子負債合計（借入金 + 社債 + CP + 1年内社債 + リース負債）
        ibd_total: float | None = None
        ibd_parts = [v for v in [ibd_cur, ibd_ncl, bonds, cp, cur_bond, lease_cl, lease_nc]
                     if v is not None]
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

        # 運転資本 = 売上債権 + 棚卸資産 − 買掛金
        working_capital: int | float | None = None
        if trade_rec is not None or inventories is not None or trade_pay is not None:
            working_capital = (trade_rec or 0) + (inventories or 0) - (trade_pay or 0)

        # 純現金 (NC) = 現金 − 有利子負債
        net_cash: int | float | None = None
        if cash is not None or ibd_total is not None:
            net_cash = (cash or 0) - (ibd_total or 0)

        # ROIC = NOPAT / 投下資本
        # 実効税率: 法人税費用 / 税引前利益。取れない場合は 30% を使用
        roic = None
        if oi is not None:
            if pbt and pbt != 0 and tax_exp is not None:
                tax_rate = min(max(tax_exp / pbt, 0.0), 0.60)
            else:
                tax_rate = 0.30
            nopat = oi * (1 - tax_rate)
            # 投下資本: BS詳細があれば「運転資本 + 固定資産」で計算（より正確）
            #           なければ「純資産 + 有利子負債 − 現金」で代替
            if working_capital is not None and ppe is not None:
                invested_capital = working_capital + ppe + (goodwill or 0) + (intangibles or 0)
            elif na is not None:
                ibd_for_ic = ibd_total if ibd_total is not None else 0
                invested_capital = na + ibd_for_ic - (cash or 0)
            else:
                invested_capital = None
            roic = _pct(_safe_div(nopat, invested_capital))

        # EBITDA = 営業利益 + D&A
        ebitda: int | float | None = None
        if oi is not None and da is not None:
            ebitda = oi + da
        ebitda_margin = _pct(_safe_div(ebitda, ns))

        # FCFマージン
        fcf_margin = _pct(_safe_div(fcf, ns))

        rows.append({
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
            # BS 集計
            "net_assets":         na,
            "total_assets":       ta,
            "cash_end":           cash,
            "interest_bearing_debt": ibd_total,
            "net_cash":           net_cash,
            # BS 詳細
            "current_assets":     cur_assets,
            "current_liabilities": cur_liab,
            "noncurrent_assets":  ncur_assets,
            "noncurrent_liabilities": ncur_liab,
            "trade_receivables":  trade_rec,
            "inventories":        inventories,
            "trade_payables":     trade_pay,
            "working_capital":    working_capital,
            "ppe":                ppe,
            "goodwill":           goodwill,
            "intangible_assets":  intangibles,
            "investment_securities": inv_sec,
            "equity_method_investments": eq_inv,
            "treasury_stock":     treasury,
            "noncontrolling_interests": nci,
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
            "depreciation":       da,
            "capex":              capex,
            "fcf":                fcf,
            "fcf_margin":         fcf_margin,
            # EBITDA
            "ebitda":             ebitda,
            "ebitda_margin":      ebitda_margin,
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

    # 株式分割補正（EPS / BPS / DPS を分割比率で除算）
    split_ratios = _load_split_ratios()
    ratio = split_ratios.get(edinet_code)
    if ratio and ratio != 1.0:
        for col in ("eps", "eps_diluted", "bps", "dps"):
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda v: round(v / ratio, 2) if pd.notna(v) else v
                )

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
    決算短信（予想優先）+ 有報データ + 株価から市場評価指標を返す。

    EPS・DPS は extract_per_share_data() が「予想 → 実績」の優先順位で取得。
    BPS は有報の実績値を使用（BSに予想なし）。
    price を省略すると get_current_price(sec_code) で自動取得する。

    返却キー:
      price, per, pbr, dividend_yield (%)
      market_cap (円), shares_approx (株)
      ev (円), ev_ebitda (倍)
      net_cash (円), net_cash_per_share (円/株)
      eps_source: "forecast" | "actual"
      dps_source: "forecast" | "actual"
    """
    ps = extract_per_share_data(sec_code)
    eps = ps.get("eps")
    dps = ps.get("dps")

    data = load_yuho_latest(edinet_code)
    if data:
        current = data.get("financials") or data.get("summary_5yr", {}).get("current", {})
    else:
        current = {}
    bps = current.get("bps") or ps.get("bps")
    na  = current.get("net_assets")

    if price is None:
        price = get_current_price(sec_code)

    # 発行済株式数の概算: 自己資本 ÷ BPS（価格不要）
    shares_approx = int(na / bps) if (na and bps and bps > 0) else None
    market_cap    = int(price * shares_approx) if (price and shares_approx) else None

    result: dict[str, Any] = {
        "price":         price,
        "market_cap":    market_cap,
        "shares_approx": shares_approx,
    }

    if price:
        result["per"]            = round(price / eps, 2) if eps and eps > 0 else None
        result["pbr"]            = round(price / bps, 2) if bps and bps > 0 else None
        result["dividend_yield"] = round(dps / price * 100, 2) if dps else None
    else:
        result.update({"per": None, "pbr": None, "dividend_yield": None})

    # EV・EV/EBITDA・純現金 — build_financials_df の最新期から取得
    fin_df = build_financials_df(edinet_code)
    if not fin_df.empty:
        lat    = fin_df.iloc[-1]
        ibd    = lat.get("interest_bearing_debt")
        cash   = lat.get("cash_end")
        nci    = lat.get("noncontrolling_interests")
        ebitda = lat.get("ebitda")
        nc     = lat.get("net_cash")

        # EV = 時価総額 + 有利子負債 − 現金 + 非支配株主持分（簿価）
        # ibd が取得できない場合は 0 として計算（総合商社等 IFRS の一部）
        ev: int | None = None
        if market_cap is not None and cash is not None:
            ev = int(market_cap + (ibd or 0) - (cash or 0) + (nci or 0))

        result["ev"]       = ev
        result["ev_ebitda"] = (
            round(ev / ebitda, 2) if ev is not None and ebitda and ebitda > 0 else None
        )
        result["net_cash"] = int(nc) if nc is not None else None
        result["net_cash_per_share"] = (
            round(nc / shares_approx, 2) if nc is not None and shares_approx else None
        )
    else:
        result.update({"ev": None, "ev_ebitda": None,
                       "net_cash": None, "net_cash_per_share": None})

    return result
