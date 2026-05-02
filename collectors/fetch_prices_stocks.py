"""
個別銘柄の株価取得（yfinance）

対象 : data/master/price_targets.csv
日足 : 3年分取得・上書き  → data/prices/stocks/daily/<code>.parquet
5分足: 継ぎ足し保存       → data/prices/stocks/5min/<code>.parquet
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import datetime
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

from config.paths import PRICE_TARGETS, PRICES_STOCKS_DAILY, PRICES_STOCKS_5MIN
from utils.data_loader import load_price_targets

load_dotenv()

FIVEMIN_INTERVAL = "5m"
FIVEMIN_MAX_DAYS = 59  # yfinance の 5分足取得上限


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance が返す MultiIndex 列をフラット化する（Close/High/Low/Open/Volume に統一）"""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def fetch_daily(ticker: str, code: str) -> None:
    """3年分の日足を取得して上書き保存（OHLCV）"""
    # 市場終了後（16時以降）に実行する前提。end は exclusive なので +1日で当日確定データを含める
    end   = datetime.date.today() + datetime.timedelta(days=1)
    start = datetime.date.today() - datetime.timedelta(days=365 * 3)
    df = yf.download(ticker, start=start.isoformat(), end=end.isoformat(),
                     interval="1d", auto_adjust=True, progress=False)
    if df.empty:
        print(f"  [SKIP] {code} ({ticker}) - 日足データなし")
        return
    df = _flatten_columns(df)
    # Close=NaN の行を除去（未確定行など）
    df = df.dropna(subset=["Close"])
    if df.empty:
        print(f"  [SKIP] {code} ({ticker}) - 有効な日足データなし")
        return
    df.index.name = "date"
    out = PRICES_STOCKS_DAILY / f"{code}.parquet"
    df.to_parquet(out)
    print(f"  [OK] {code} 日足 {len(df)}行 → {out.name}")


def _to_utc(df: pd.DataFrame) -> pd.DataFrame:
    """インデックスを UTC に正規化（naive → UTC localize、他tz → UTC convert）"""
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df


def fetch_5min(ticker: str, code: str, fivemin_days: int = FIVEMIN_MAX_DAYS) -> None:
    """5分足を取得して既存データに継ぎ足し（重複除去）。
    インデックス = JST、datetime_utc 列 = UTC の両方を保存。
    fivemin_days: 取得日数（最大 59）
    """
    days = min(fivemin_days, FIVEMIN_MAX_DAYS)
    df_new = yf.download(ticker, period=f"{days}d", interval=FIVEMIN_INTERVAL,
                         auto_adjust=True, progress=False)
    if df_new.empty:
        print(f"  [SKIP] {code} ({ticker}) - 5分足データなし")
        return
    df_new = _flatten_columns(df_new)
    df_new = _to_utc(df_new)
    df_new = df_new.drop(columns=["datetime_utc"], errors="ignore")  # 再生成するため除去
    # Volume=0 の行を除去（寄り付き前の気配値・未取引バーなど）
    df_new = df_new[df_new["Volume"] > 0]

    out = PRICES_STOCKS_5MIN / f"{code}.parquet"
    if out.exists():
        df_old = pd.read_parquet(out)
        if isinstance(df_old.columns, pd.MultiIndex):
            df_old = _flatten_columns(df_old)
        df_old = _to_utc(df_old)  # UTC/JST 混在に対応
        df_old = df_old.drop(columns=["datetime_utc"], errors="ignore")  # 再生成するため除去
        # Volume=0 の古いデータも除去（過去ファイルのゴーストデータをクリーニング）
        df_old = df_old[df_old["Volume"] > 0]
        df = pd.concat([df_old, df_new]).sort_index()
        df = df[~df.index.duplicated(keep="last")]
    else:
        df = df_new

    if df.empty:
        print(f"  [SKIP] {code} ({ticker}) - 有効な5分足データなし（Volume=0のみ）")
        return

    # インデックスを JST に変換
    df.index = df.index.tz_convert("Asia/Tokyo")
    df.index.name = "datetime"
    # UTC 列を先頭に追加（旧プロジェクトとの互換性・タイムゾーン確認用）
    df.insert(0, "datetime_utc", df.index.tz_convert("UTC"))
    df.to_parquet(out)
    print(f"  [OK] {code} 5分足 {len(df)}行 → {out.name}")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="個別銘柄株価取得")
    parser.add_argument("--fivemin-days", type=int, default=FIVEMIN_MAX_DAYS,
                        help=f"5分足の取得日数（最大 {FIVEMIN_MAX_DAYS}、デフォルト {FIVEMIN_MAX_DAYS}）")
    args = parser.parse_args()

    targets = load_price_targets()
    total = len(targets)
    for i, row in targets.iterrows():
        code   = str(row["コード"])
        ticker = str(row["ティッカーコード"])
        print(f"[{i+1}/{total}] {code} {row['銘柄']} ({ticker})")
        fetch_daily(ticker, code)
        fetch_5min(ticker, code, fivemin_days=args.fivemin_days)
    print("個別銘柄 完了")


if __name__ == "__main__":
    main()
