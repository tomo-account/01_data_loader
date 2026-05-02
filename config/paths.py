"""
プロジェクト全体のパス定数
全モジュールはここから Path を import してハードコードを避ける
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DATA    = ROOT / "data"
MASTER  = DATA / "master"
SECTORS = MASTER / "sectors"

PRICES_STOCKS_DAILY = DATA / "prices" / "stocks" / "daily"
PRICES_STOCKS_5MIN  = DATA / "prices" / "stocks" / "5min"
PRICES_MACRO_DAILY  = DATA / "prices" / "macro" / "daily"
PRICES_MACRO_5MIN   = DATA / "prices" / "macro" / "5min"

NEWS       = DATA / "news"
NEWS_RSS   = NEWS / "rss"
NEWS_TDNET = NEWS / "tdnet"
FINANCIALS = DATA / "financials"

# 決算短信 XBRL ZIP / JSON / Markdown（xbrl-converter 出力）
STATEMENTS_ZIP = DATA / "statements_zip"
STATEMENTS    = DATA / "statements"
STATEMENTS_MD = DATA / "statements_md"

OUTPUTS         = DATA / "outputs"
OUTPUTS_EXCEL   = OUTPUTS / "excel"
OUTPUTS_FIGURES = OUTPUTS / "figures"
OUTPUTS_REPORTS = OUTPUTS / "reports"

# 銘柄マスタ CSV
TOPIX_ALL      = MASTER / "topix_all.csv"
PRICE_TARGETS  = MASTER / "price_targets.csv"
WATCH_PERSONAL      = MASTER / "watch_personal.csv"
WATCH_MARKET        = MASTER / "watch_market.csv"
WATCH_MARKET_SELECT = MASTER / "watch_market_select.csv"

# 手動マスタ（EDINET DB 等の不正確な値を上書きする用）
MANUAL_OVERRIDES = MASTER / "manual_overrides.csv"
