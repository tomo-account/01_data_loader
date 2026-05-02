"""
Parquet / CSV 読み込み共通ユーティリティ

全アプリ・全スクリプトはここ経由でデータを読む。
パスの管理は config/paths.py に一元化されているため、
データ形式を変えても修正箇所はこのファイルのみ。
"""
import datetime
import pandas as pd
from pathlib import Path

from config.paths import (
    PRICES_STOCKS_DAILY, PRICES_STOCKS_5MIN,
    PRICES_MACRO_DAILY,  PRICES_MACRO_5MIN,
    NEWS_RSS, NEWS_TDNET, FINANCIALS,
    TOPIX_ALL, PRICE_TARGETS, WATCH_PERSONAL, WATCH_MARKET, WATCH_MARKET_SELECT,
)


# ── 株価 ──────────────────────────────────────────────

def _drop_incomplete(df: pd.DataFrame) -> pd.DataFrame:
    """当日分の未確定行（最終行のみ）を除去する。過去の NaN 行は保持する。"""
    if not df.empty and pd.isna(df["Close"].iloc[-1]):
        return df.iloc[:-1]
    return df


def load_stock_daily(code: str) -> pd.DataFrame:
    """個別銘柄の日足 Parquet を読み込む"""
    return _drop_incomplete(pd.read_parquet(PRICES_STOCKS_DAILY / f"{code}.parquet"))


def load_stock_5min(code: str) -> pd.DataFrame:
    """個別銘柄の5分足 Parquet を読み込む"""
    return _drop_incomplete(pd.read_parquet(PRICES_STOCKS_5MIN / f"{code}.parquet"))


# ── マクロ ────────────────────────────────────────────

def load_macro_daily(key: str) -> pd.DataFrame:
    """マクロ指標の日足 Parquet を読み込む（key = config/tickers_macro のキー）"""
    return pd.read_parquet(PRICES_MACRO_DAILY / f"{key}.parquet")


def load_macro_5min(key: str) -> pd.DataFrame:
    """マクロ指標の5分足 Parquet を読み込む"""
    return pd.read_parquet(PRICES_MACRO_5MIN / f"{key}.parquet")


def load_all_macro_daily() -> dict[str, pd.DataFrame]:
    """全マクロ指標の日足を {key: DataFrame} で返す"""
    from config.tickers_macro import MACRO_TICKERS
    result = {}
    for key in MACRO_TICKERS:
        path = PRICES_MACRO_DAILY / f"{key}.parquet"
        if path.exists():
            result[key] = pd.read_parquet(path)
    return result


# ── ニュース ──────────────────────────────────────────

def load_news(date: str | None = None) -> pd.DataFrame:
    """
    ニュース CSV を読み込む。
    date="YYYY-MM-DD"（省略時は今日）
    ファイルが存在しない（未取得）場合は FileNotFoundError を送出する。
    ファイルが存在するが空の場合は空 DataFrame を返す（その日ニュースなし）。
    """
    if date is None:
        date = datetime.date.today().isoformat()
    path = NEWS_RSS / f"{date}.csv"
    if not path.exists():
        raise FileNotFoundError(f"ニュースデータ未取得: {path.name}")
    return pd.read_csv(path)


def load_tdnet(date: str | None = None) -> pd.DataFrame:
    """
    TDnet 適時開示 CSV を読み込む。
    date="YYYY-MM-DD"（省略時は今日）
    ファイルが存在しない（未取得）場合は FileNotFoundError を送出する。
    ファイルが存在するが空の場合は空 DataFrame を返す（その日開示なし）。
    """
    if date is None:
        date = datetime.date.today().isoformat()
    path = NEWS_TDNET / f"{date}.csv"
    if not path.exists():
        raise FileNotFoundError(f"TDnet データ未取得: {path.name}")
    return pd.read_csv(path, dtype={"code": str})


# ── 財務 ──────────────────────────────────────────────

def load_financials(code: str) -> pd.DataFrame:
    """銘柄コードの財務 CSV を読み込む"""
    path = FINANCIALS / f"{code}.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


# ── 銘柄マスタ ────────────────────────────────────────

def load_topix_all() -> pd.DataFrame:
    return pd.read_csv(TOPIX_ALL, dtype={"コード": str})


def load_price_targets() -> pd.DataFrame:
    return pd.read_csv(PRICE_TARGETS, dtype={"コード": str})


def load_watch_personal() -> pd.DataFrame:
    return pd.read_csv(WATCH_PERSONAL, dtype={"コード": str})


def load_watch_market() -> pd.DataFrame:
    return pd.read_csv(WATCH_MARKET, dtype={"コード": str})


def load_watch_market_select() -> pd.DataFrame:
    return pd.read_csv(WATCH_MARKET_SELECT, dtype={"コード": str})


def load_sector(sector_en: str) -> pd.DataFrame:
    """sectors/<sector_en>.csv を読み込む"""
    from config.paths import SECTORS
    path = SECTORS / f"{sector_en}.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype={"コード": str})
