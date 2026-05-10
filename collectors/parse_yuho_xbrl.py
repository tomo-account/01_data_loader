"""
collectors.parse_yuho_xbrl — EDINET 有報 XBRL-to-CSV → JSON 変換

EDINET API type=5 でダウンロードした ZIP 内の
  XBRL_TO_CSV/jpcrp030000-asr-001_*.csv
を読み込み、統一 JSON スキーマに変換する。

使い方（CLI）:
    python collectors/parse_yuho_xbrl.py                   # data/yuho_zip 以下を全件処理
    python collectors/parse_yuho_xbrl.py --zip S100R3GX.zip  # 単一 ZIP を変換

使い方（API）:
    from collectors.parse_yuho_xbrl import parse_zip, parse_all
    json_obj = parse_zip(Path("data/yuho_zip/E00041/S100R3GX.zip"))
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import zipfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.paths import YUHO_ZIP, YUHO

PARSER_VERSION = "0.1.2"

# ── コンテキストID → 集計期間キー ────────────────────────────────────────
_PERIOD_MAP: dict[str, str] = {
    "Prior4YearDuration":  "prior4",
    "Prior3YearDuration":  "prior3",
    "Prior2YearDuration":  "prior2",
    "Prior1YearDuration":  "prior1",
    "CurrentYearDuration": "current",
    "Prior4YearInstant":   "prior4",
    "Prior3YearInstant":   "prior3",
    "Prior2YearInstant":   "prior2",
    "Prior1YearInstant":   "prior1",
    "CurrentYearInstant":  "current",
}

# ── 主要財務指標マッピング（SummaryOfBusinessResults 5期分） ────────────
_SUMMARY_FIELDS: dict[str, str] = {
    # 損益（Duration）
    "jpcrp_cor:NetSalesSummaryOfBusinessResults":                                       "net_sales",
    "jpcrp_cor:OrdinaryIncomeLossSummaryOfBusinessResults":                             "ordinary_income",
    "jpcrp_cor:ProfitLossAttributableToOwnersOfParentSummaryOfBusinessResults":         "net_income",
    "jpcrp_cor:ComprehensiveIncomeSummaryOfBusinessResults":                            "comprehensive_income",
    "jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults":                      "eps",
    "jpcrp_cor:DilutedEarningsPerShareSummaryOfBusinessResults":                        "eps_diluted",
    "jpcrp_cor:DividendsPerShareSummaryOfBusinessResults":                              "dps",
    "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults":                           "dps",
    # CF（Duration）
    "jpcrp_cor:NetCashProvidedByUsedInOperatingActivitiesSummaryOfBusinessResults":     "operating_cf",
    "jpcrp_cor:NetCashProvidedByUsedInInvestingActivitiesSummaryOfBusinessResults":     "investing_cf",
    "jpcrp_cor:NetCashProvidedByUsedInFinancingActivitiesSummaryOfBusinessResults":     "financing_cf",
    # 貸借（Instant）
    "jpcrp_cor:NetAssetsSummaryOfBusinessResults":                                      "net_assets",
    "jpcrp_cor:TotalAssetsSummaryOfBusinessResults":                                    "total_assets",
    "jpcrp_cor:NetAssetsPerShareSummaryOfBusinessResults":                              "bps",
    "jpcrp_cor:CashAndCashEquivalentsSummaryOfBusinessResults":                         "cash_end",
    # 指標（Instant / Duration）
    "jpcrp_cor:EquityToAssetRatioSummaryOfBusinessResults":                             "equity_ratio",
    "jpcrp_cor:RateOfReturnOnEquitySummaryOfBusinessResults":                           "roe",
    "jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults":                             "per",
    # IFRS SummaryOfBusinessResults
    "jpcrp_cor:RevenueIFRSSummaryOfBusinessResults":                                    "net_sales",
    "jpcrp_cor:RevenuesSummaryOfBusinessResults":                                       "net_sales",
    "jpcrp_cor:ProfitLossAttributableToOwnersOfParentIFRSSummaryOfBusinessResults":    "net_income",
    "jpcrp_cor:ComprehensiveIncomeAttributableToOwnersOfParentIFRSSummaryOfBusinessResults": "comprehensive_income",
    "jpcrp_cor:EquityAttributableToOwnersOfParentIFRSSummaryOfBusinessResults":        "net_assets",
    "jpcrp_cor:TotalAssetsIFRSSummaryOfBusinessResults":                                "total_assets",
    "jpcrp_cor:RatioOfOwnersEquityToGrossAssetsIFRSSummaryOfBusinessResults":          "equity_ratio",
    "jpcrp_cor:EquityToAssetRatioIFRSSummaryOfBusinessResults":                        "bps",
    "jpcrp_cor:BasicEarningsLossPerShareIFRSSummaryOfBusinessResults":                 "eps",
    "jpcrp_cor:DilutedEarningsLossPerShareIFRSSummaryOfBusinessResults":               "eps_diluted",
    "jpcrp_cor:RateOfReturnOnEquityIFRSSummaryOfBusinessResults":                      "roe",
    "jpcrp_cor:PriceEarningsRatioIFRSSummaryOfBusinessResults":                        "per",
    "jpcrp_cor:CashFlowsFromUsedInOperatingActivitiesIFRSSummaryOfBusinessResults":    "operating_cf",
    "jpcrp_cor:CashFlowsFromUsedInInvestingActivitiesIFRSSummaryOfBusinessResults":    "investing_cf",
    "jpcrp_cor:CashFlowsFromUsedInFinancingActivitiesIFRSSummaryOfBusinessResults":    "financing_cf",
    "jpcrp_cor:CashAndCashEquivalentsIFRSSummaryOfBusinessResults":                    "cash_end",
    "jpcrp_cor:NetAssetsPerShareIFRSSummaryOfBusinessResults":                         "bps",
    "jpcrp_cor:OperatingProfitLossSummaryOfBusinessResults":                           "operating_income",
    "jpcrp_cor:ProfitBeforeTaxSummaryOfBusinessResults":                               "profit_before_tax",
    "jpcrp_cor:ProfitLossSummaryOfBusinessResults":                                    "net_income",
    # ── 主要財務諸表（Current/Prior1 のみ） ─────────────────────────────────
    # 石油・ガス業界タクソノミー (jpigp_cor) — ENEOS・出光・コスモ等
    "jpigp_cor:OperatingProfitLossIFRS":              "operating_income",
    "jpigp_cor:GrossProfitIFRS":                      "gross_profit",
    "jpigp_cor:IncomeTaxExpenseIFRS":                 "income_tax_expense",
    "jpigp_cor:BondsAndBorrowingsCLIFRS":             "interest_bearing_debt_current",
    "jpigp_cor:BondsAndBorrowingsNCLIFRS":            "interest_bearing_debt_noncurrent",
    "jpigp_cor:LongTermDebtNCLIFRS":                  "interest_bearing_debt_noncurrent",
    "jpigp_cor:LeaseLiabilitiesCLIFRS":               "lease_liabilities_current",
    "jpigp_cor:LeaseLiabilitiesNCLIFRS":              "lease_liabilities_noncurrent",
    # 汎用 IFRS（総合商社・資源会社等）— 順次拡張予定
    "ifrs-full:OperatingProfit":                      "operating_income",
    "ifrs-full:GrossProfit":                          "gross_profit",
    "ifrs-full:IncomeTaxExpense":                     "income_tax_expense",
    "ifrs-full:CurrentBorrowings":                    "interest_bearing_debt_current",
    "ifrs-full:NoncurrentBorrowings":                 "interest_bearing_debt_noncurrent",
    "ifrs-full:CurrentLeaseLiabilities":              "lease_liabilities_current",
    "ifrs-full:NoncurrentLeaseLiabilities":           "lease_liabilities_noncurrent",
    # 総合商社 IFRS — 営業利益要素なし。粗利・販管費から計算するために抽出
    "jpigp_cor:SellingGeneralAndAdministrativeExpensesIFRS": "sga_expense",
    "jpigp_cor:ProfitLossBeforeTaxIFRS":              "profit_before_tax",
    # JP GAAP — 粗利（売上総利益）
    "jpcrp_cor:GrossProfitLossSummaryOfBusinessResults": "gross_profit",
    "jppfs_cor:GrossProfit":                          "gross_profit",
    "jppfs_cor:OperatingIncome":                      "operating_income",
    "jppfs_cor:IncomeTaxesCurrent":                   "income_tax_expense",
    "jppfs_cor:ShortTermLoansPayable":                "interest_bearing_debt_current",
    "jppfs_cor:LongTermLoansPayable":                 "interest_bearing_debt_noncurrent",
    "jppfs_cor:BondsPayable":                         "bonds_payable",
    "jppfs_cor:CommercialPapersLiabilities":          "commercial_papers",
    "jppfs_cor:CurrentPortionOfBonds":                "current_portion_of_bonds",
    # ── 減価償却費・償却費 (D&A) ─────────────────────────────────────────────
    # JP GAAP — CF計算書の減価償却費加算項目（最も安定して存在する）
    "jppfs_cor:DepreciationAndAmortizationOpeCF":                                 "depreciation",
    # IFRS jpigp — CF計算書（ENEOS・コスモ等）
    "jpigp_cor:DepreciationAndAmortizationOpeCFIFRS":                             "depreciation",
    # IFRS jpigp — CF計算書（三菱商事等）
    "jpigp_cor:DepreciationExpenseOpeCFIFRS":                                     "depreciation",
    # IFRS jpigp — P&L営業費用内の減価償却費（三井物産等 CF非掲載の場合のフォールバック）
    "jpigp_cor:DepreciationAndAmortizationOperatingExpensesIFRS":                 "depreciation",
    # ── 設備投資 CAPEX（投資CF内訳、通常マイナス値） ────────────────────────
    # JP GAAP — 有形固定資産取得
    "jppfs_cor:PurchaseOfPropertyPlantAndEquipmentInvCF":                         "capex_ppe",
    # JP GAAP — 無形固定資産取得
    "jppfs_cor:PurchaseOfIntangibleAssetsInvCF":                                  "capex_intangible",
    # IFRS jpigp — 有形固定資産取得
    "jpigp_cor:PurchaseOfPropertyPlantAndEquipmentInvCFIFRS":                     "capex_ppe",
    # IFRS jpigp — 無形固定資産取得
    "jpigp_cor:PurchaseOfIntangibleAssetsInvCFIFRS":                              "capex_intangible",
    # ══ BS 詳細（Instant） ═══════════════════════════════════════════════════
    # ── 流動資産・流動負債 合計 ───────────────────────────────────────────────
    "jppfs_cor:CurrentAssets":                                                    "current_assets",
    "jpigp_cor:CurrentAssetsIFRS":                                                "current_assets",
    "jppfs_cor:CurrentLiabilities":                                               "current_liabilities",
    "jpigp_cor:TotalCurrentLiabilitiesIFRS":                                      "current_liabilities",
    "jppfs_cor:NoncurrentAssets":                                                 "noncurrent_assets",
    "jpigp_cor:NonCurrentAssetsIFRS":                                             "noncurrent_assets",
    "jppfs_cor:NoncurrentLiabilities":                                            "noncurrent_liabilities",
    # ── 売上債権（営業資産） ──────────────────────────────────────────────────
    "jppfs_cor:NotesAndAccountsReceivableTrade":                                  "trade_receivables",
    "jpigp_cor:TradeAndOtherReceivablesCAIFRS":                                   "trade_receivables",
    # ── 棚卸資産 ─────────────────────────────────────────────────────────────
    "jppfs_cor:Inventories":                                                      "inventories",
    "jpigp_cor:InventoriesCAIFRS":                                                "inventories",
    # ── 買掛金（営業負債） ────────────────────────────────────────────────────
    "jppfs_cor:NotesAndAccountsPayableTrade":                                     "trade_payables",
    "jpigp_cor:TradeAndOtherPayablesCLIFRS":                                      "trade_payables",
    # ── 有形固定資産（純額） ──────────────────────────────────────────────────
    "jppfs_cor:PropertyPlantAndEquipment":                                        "ppe",
    "jpigp_cor:PropertyPlantAndEquipmentIFRS":                                    "ppe",
    # ── のれん ───────────────────────────────────────────────────────────────
    "jppfs_cor:Goodwill":                                                         "goodwill",
    "jpigp_cor:GoodwillIFRS":                                                     "goodwill",
    # ── 無形固定資産（のれん除く） ────────────────────────────────────────────
    "jppfs_cor:IntangibleAssets":                                                 "intangible_assets",
    "jpigp_cor:IntangibleAssetsIFRS":                                             "intangible_assets",
    # ── 投資（持分法 / その他投資有価証券） ──────────────────────────────────
    "jppfs_cor:InvestmentSecurities":                                             "investment_securities",
    "jpigp_cor:OtherInvestmentsIFRS":                                             "investment_securities",
    "jpigp_cor:InvestmentsAccountedForUsingEquityMethodIFRS":                     "equity_method_investments",
    # ── 自己株式 ─────────────────────────────────────────────────────────────
    "jppfs_cor:TreasuryStock":                                                    "treasury_stock",
    "jpigp_cor:TreasurySharesIFRS":                                               "treasury_stock",
    # ── 非支配株主持分 ────────────────────────────────────────────────────────
    "jppfs_cor:NonControllingInterests":                                          "noncontrolling_interests",
    "jpigp_cor:NonControllingInterestsIFRS":                                      "noncontrolling_interests",
}

# ── DEI 要素マッピング ────────────────────────────────────────────────────
_DEI_FIELDS: dict[str, str] = {
    "jpdei_cor:EDINETCodeDEI":                                 "edinet_code",
    "jpdei_cor:SecurityCodeDEI":                               "sec_code_raw",
    "jpdei_cor:FilerNameInJapaneseDEI":                        "company_name",
    "jpdei_cor:AccountingStandardsDEI":                        "accounting_standard_raw",
    "jpdei_cor:WhetherConsolidatedFinancialStatementsArePreparedDEI": "is_consolidated_raw",
    "jpdei_cor:CurrentFiscalYearStartDateDEI":                 "fiscal_year_start",
    "jpdei_cor:CurrentFiscalYearEndDateDEI":                   "fiscal_year_end",
    "jpdei_cor:CurrentPeriodEndDateDEI":                       "period_end",
    "jpdei_cor:TypeOfCurrentPeriodDEI":                        "period_type",
    "jpdei_cor:AmendmentFlagDEI":                              "is_amendment_raw",
}

_NULL_VALUES = {"－", "—", "-", "", "N/A", "n/a"}


# ══════════════════════════════════════════════════════════════════════════════
# CSV ロード
# ══════════════════════════════════════════════════════════════════════════════

def _load_csv_from_zip(zip_path: Path) -> tuple[list[dict], str]:
    """ZIP から jpcrp030000-asr-*.csv を読み込み (rows, csv_filename) を返す。"""
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        targets = [n for n in names if re.search(r"jpcrp030000-asr-\d+_.*\.csv$", n)]
        if not targets:
            raise FileNotFoundError(f"有報 CSV が見つかりません: {zip_path}")
        csv_name = targets[0]
        raw = zf.read(csv_name)

    # EDINET の XBRL-to-CSV は UTF-16 LE (BOM付き) で出力される
    for enc in ("utf-16", "utf-8-sig", "utf-8", "cp932"):
        try:
            text = raw.decode(enc)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        raise UnicodeDecodeError(f"CSV のエンコーディングを特定できません: {csv_name}")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    rows = list(reader)
    return rows, Path(csv_name).name


def _parse_filename(csv_name: str) -> dict[str, str]:
    """CSV ファイル名から fiscal_year_end / filing_date を抽出。
    例: jpcrp030000-asr-001_E00041-000_2023-03-31_01_2023-06-27.csv
    """
    parts = csv_name.replace(".csv", "").split("_")
    dates = [p for p in parts if re.match(r"\d{4}-\d{2}-\d{2}$", p)]
    return {
        "fiscal_year_end": dates[0] if len(dates) >= 1 else "",
        "filing_date":     dates[1] if len(dates) >= 2 else "",
    }


# ══════════════════════════════════════════════════════════════════════════════
# 値の正規化
# ══════════════════════════════════════════════════════════════════════════════

def _to_number(raw: str) -> int | float | None:
    """文字列を数値に変換。変換不能なら None。"""
    s = raw.strip()
    if s in _NULL_VALUES:
        return None
    s = s.replace(",", "").replace("△", "-").replace("▲", "-")
    try:
        n = float(s)
        return int(n) if n == int(n) else n
    except ValueError:
        return None


def _accounting_std(raw: str) -> str:
    """会計基準文字列を正規化。"""
    r = raw.lower()
    if "ifrs" in r:
        return "IFRS"
    if "us" in r:
        return "US"
    return "JP"


# ══════════════════════════════════════════════════════════════════════════════
# パーサー本体
# ══════════════════════════════════════════════════════════════════════════════

def _build_lookup(rows: list[dict]) -> dict[tuple[str, str], str]:
    """(要素ID, コンテキストID) → 値 のルックアップテーブルを構築。"""
    lut: dict[tuple[str, str], str] = {}
    col_elem = "要素ID"
    col_ctx  = "コンテキストID"
    col_val  = "値"
    for row in rows:
        elem = row.get(col_elem, "").strip('"').strip()
        ctx  = row.get(col_ctx,  "").strip('"').strip()
        val  = row.get(col_val,  "").strip('"').strip()
        if elem and ctx:
            lut[(elem, ctx)] = val
    return lut


def _extract_dei(lut: dict[tuple[str, str], str]) -> dict[str, Any]:
    """DEI 要素からメタデータを抽出。"""
    raw: dict[str, str] = {}
    ctx = "FilingDateInstant"
    for elem, field in _DEI_FIELDS.items():
        val = lut.get((elem, ctx), "")
        raw[field] = val

    sec = raw.get("sec_code_raw", "")
    sec4 = sec[:4] if len(sec) == 5 and sec.endswith("0") else sec[:4] if len(sec) >= 4 else sec

    return {
        "edinet_code":        raw.get("edinet_code", ""),
        "sec_code":           sec4,
        "company_name":       raw.get("company_name", ""),
        "accounting_standard": _accounting_std(raw.get("accounting_standard_raw", "")),
        "is_consolidated":    raw.get("is_consolidated_raw", "").lower() == "true",
        "fiscal_year_start":  raw.get("fiscal_year_start", ""),
        "fiscal_year_end":    raw.get("fiscal_year_end", "") or raw.get("period_end", ""),
        "period_type":        raw.get("period_type", "FY"),
        "is_amendment":       raw.get("is_amendment_raw", "false").lower() == "true",
    }


_DPS_ELEMENTS = {
    "jpcrp_cor:DividendPaidPerShareSummaryOfBusinessResults",
    "jpcrp_cor:DividendsPerShareSummaryOfBusinessResults",
}

# DPS は非連結コンテキストで報告されることが多いため専用マップを持つ
_DPS_PERIOD_MAP: dict[str, str] = {
    f"Prior4YearDuration_NonConsolidatedMember":  "prior4",
    f"Prior3YearDuration_NonConsolidatedMember":  "prior3",
    f"Prior2YearDuration_NonConsolidatedMember":  "prior2",
    f"Prior1YearDuration_NonConsolidatedMember":  "prior1",
    f"CurrentYearDuration_NonConsolidatedMember": "current",
}


def _extract_summary(lut: dict[tuple[str, str], str]) -> dict[str, dict]:
    """Summary of Business Results（5期分）を抽出。"""
    periods = list(_PERIOD_MAP.keys())
    result: dict[str, dict] = {p: {} for p in set(_PERIOD_MAP.values())}

    for elem, field in _SUMMARY_FIELDS.items():
        # DPS は通常コンテキスト → 非連結コンテキストの順で探す
        ctx_list = list(periods)
        if elem in _DPS_ELEMENTS:
            ctx_list = ctx_list + list(_DPS_PERIOD_MAP.keys())
        combined_map = {**_PERIOD_MAP, **_DPS_PERIOD_MAP}

        for ctx_id in ctx_list:
            val_raw = lut.get((elem, ctx_id))
            if val_raw is None:
                continue
            val = _to_number(val_raw)
            if val is None:
                continue
            period_key = combined_map[ctx_id]
            # 先着優先（同じ period_key・field に複数マッチした場合）
            if field not in result[period_key]:
                result[period_key][field] = val

    # 空の期間を除去
    return {k: v for k, v in result.items() if v}


# ══════════════════════════════════════════════════════════════════════════════
# 公開 API
# ══════════════════════════════════════════════════════════════════════════════

def parse_zip(zip_path: Path) -> dict:
    """ZIP ファイル 1 件を JSON オブジェクトに変換して返す。"""
    rows, csv_name = _load_csv_from_zip(zip_path)
    lut = _build_lookup(rows)

    fn_meta = _parse_filename(csv_name)
    dei     = _extract_dei(lut)
    summary = _extract_summary(lut)

    # filing_date は CSV ファイル名から取得（DEI に含まれないため）
    metadata = {**dei}
    if not metadata.get("fiscal_year_end"):
        metadata["fiscal_year_end"] = fn_meta["fiscal_year_end"]
    metadata["filing_date"]  = fn_meta["filing_date"]
    metadata["doc_id"]       = zip_path.stem

    return {
        "metadata":   metadata,
        "summary_5yr": summary,
        "_source": {
            "format":         "edinet-xbrl-csv",
            "csv_file":       csv_name,
            "parser_version": PARSER_VERSION,
        },
    }


parse = parse_zip  # 決算短信パーサーと統一したシグネチャ


import datetime as _dt

_PERIOD_OFFSETS: dict[str, int] = {
    "prior4": 4, "prior3": 3, "prior2": 2, "prior1": 1, "current": 0,
}


def _fy_end_approx(base_end: str, offset: int) -> str:
    """base_end から offset 年前の期末日を概算する（うるう年対応）。"""
    try:
        d = _dt.date.fromisoformat(base_end)
        y = d.year - offset
        try:
            return _dt.date(y, d.month, d.day).isoformat()
        except ValueError:
            return _dt.date(y, d.month, 28).isoformat()
    except (ValueError, TypeError):
        return ""


def parse_all(
    zip_root: Path = YUHO_ZIP,
    out_root: Path = YUHO,
    force:    bool = False,
) -> tuple[int, int, int]:
    """zip_root 以下の全 ZIP を処理し、年度別 JSON を out_root に書き出す。

    出力ファイル名: {edinet_code}_{fiscal_year_end}.json
    1 ZIP から最大 5 ファイル（prior4〜current）を生成する。
    同名ファイルが存在する場合は force=True のときのみ上書き。

    返り値: (ok, skip, err) のカウント。
    """
    ok = skip = err = 0
    zips = sorted(zip_root.rglob("*.zip"))
    print(f"[parse_yuho] 対象 {len(zips)} 件")

    for zp in zips:
        edinet_code = zp.parent.name
        try:
            obj = parse_zip(zp)
        except Exception as e:
            print(f"  [err] {zp.name}: {e}", file=sys.stderr)
            err += 1
            continue

        base_meta = obj["metadata"]
        base_end  = base_meta.get("fiscal_year_end", "")
        company   = base_meta.get("company_name", "")

        for period_key, period_data in obj.get("summary_5yr", {}).items():
            offset   = _PERIOD_OFFSETS.get(period_key, 0)
            fy_end   = _fy_end_approx(base_end, offset)
            if not fy_end:
                continue

            fname    = f"{edinet_code}_{fy_end}.json"
            out_path = out_root / edinet_code / fname

            if out_path.exists() and not force:
                skip += 1
                continue

            yearly_obj = {
                "metadata":   {**base_meta, "fiscal_year_end": fy_end},
                "financials": period_data,
                "_source":    obj.get("_source", {}),
            }
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                json.dumps(yearly_obj, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"  [ok] {edinet_code}/{fname}  {company} ({period_key})")
            ok += 1

    print(f"\n[done] ok={ok}  skip={skip}  err={err}")
    if ok:
        print(f"  JSON → {out_root}")
    return ok, skip, err


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    p = argparse.ArgumentParser(description="EDINET 有報 XBRL-to-CSV → JSON 変換")
    p.add_argument("--zip", metavar="PATH", help="単一 ZIP ファイルを変換して標準出力に出力")
    p.add_argument("--out", metavar="DIR",  help=f"出力ディレクトリ (default: {YUHO})")
    p.add_argument("--force", action="store_true", help="既存 JSON も上書き")
    args = p.parse_args()

    out_root = Path(args.out) if args.out else YUHO

    if args.zip:
        obj = parse_zip(Path(args.zip))
        print(json.dumps(obj, ensure_ascii=False, indent=2))
    else:
        parse_all(out_root=out_root, force=args.force)


if __name__ == "__main__":
    main()
