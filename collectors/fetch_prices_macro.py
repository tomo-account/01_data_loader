"""
マクロ指標・先物・指数・為替の株価取得（yfinance）

対象 : config/tickers_macro.MACRO_TICKERS（14本）
日足 : 3年分取得・上書き + chg_pct（前日比%）列付加
       → data/prices/macro/daily/<key>.parquet
5分足: 継ぎ足し保存
       → data/prices/macro/5min/<key>.parquet
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import datetime
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

from config.tickers_macro import MACRO_TICKERS
from config.paths import PRICES_MACRO_DAILY, PRICES_MACRO_5MIN

load_dotenv()

FIVEMIN_MAX_DAYS = 59

# Volume=0 を除去しないティッカー
# 株式インデックス（SOX/DJI/VIX等）・為替・DX-Y.NYB は Volume が常に 0 のため除去しない
_NO_VOL_FILTER = {"N225", "DJI", "GSPC", "IXIC", "SOX", "VIX", "TNX", "HSI", "TWII", "KS11", "SSEC", "EURUSD", "JPY_X", "DX_F"}
# ETF は Volume>0 が正常なのでフィルタ対象（_NO_VOL_FILTER に含めない）


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """yfinance が返す MultiIndex 列をフラット化する（Close/High/Low/Open/Volume に統一）"""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def fetch_daily(key: str, ticker: str) -> None:
    """3年分の日足を取得・chg_pct 付加して上書き保存"""
    # 市場終了後（16時以降）に実行する前提。end は exclusive なので +1日で当日確定データを含める
    end   = datetime.date.today() + datetime.timedelta(days=1)
    start = datetime.date.today() - datetime.timedelta(days=365 * 3)
    df = yf.download(ticker, start=start.isoformat(), end=end.isoformat(),
                     interval="1d", auto_adjust=True, progress=False)
    if df.empty:
        print(f"  [SKIP] {key} ({ticker}) - 日足データなし")
        return
    df = _flatten_columns(df)
    # Close=NaN の行を除去（未確定行など）
    df = df.dropna(subset=["Close"])
    if df.empty:
        print(f"  [SKIP] {key} ({ticker}) - 有効な日足データなし")
        return
    df.index.name = "date"
    df["chg_pct"] = df["Close"].pct_change() * 100
    out = PRICES_MACRO_DAILY / f"{key}.parquet"
    df.to_parquet(out)
    print(f"  [OK] {key} 日足 {len(df)}行 chg_pct付 → {out.name}")


def _to_utc(df: pd.DataFrame) -> pd.DataFrame:
    """インデックスを UTC に正規化（naive → UTC localize、他tz → UTC convert）"""
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df


def _detect_split(df_old: pd.DataFrame, df_new: pd.DataFrame) -> bool:
    """
    新旧データの重複期間の Close 価格比の中央値で大幅乖離を検出する。

    通常の価格変動: 比率 0.85〜1.15 → False（分割なし）
    大幅乖離あり  : 比率範囲外      → True（旧データを破棄して再構築）
    """
    common = df_old.index.intersection(df_new.index)
    if len(common) < 3:
        return False
    ratio = (df_old.loc[common, "Close"] / df_new.loc[common, "Close"]).median()
    return not (0.85 < ratio < 1.15)


def fetch_5min(key: str, ticker: str) -> None:
    """5分足を取得して既存データに継ぎ足し（重複除去）。
    インデックス = JST、datetime_utc 列 = UTC の両方を保存。
    """
    df_new = yf.download(ticker, period=f"{FIVEMIN_MAX_DAYS}d", interval="5m",
                         auto_adjust=True, progress=False)
    if df_new.empty:
        print(f"  [SKIP] {key} ({ticker}) - 5分足データなし")
        return
    df_new = _flatten_columns(df_new)
    df_new = _to_utc(df_new)
    df_new = df_new.drop(columns=["datetime_utc"], errors="ignore")
    # Volume=0 の行を除去（先物のみ）
    # 株式インデックス（SOX/DJI等）・為替は Volume=0 が正常のため除去しない
    if key not in _NO_VOL_FILTER:
        df_new = df_new[df_new["Volume"] > 0]

    out = PRICES_MACRO_5MIN / f"{key}.parquet"
    if out.exists():
        df_old = pd.read_parquet(out)
        if isinstance(df_old.columns, pd.MultiIndex):
            df_old = _flatten_columns(df_old)
        df_old = _to_utc(df_old)
        df_old = df_old.drop(columns=["datetime_utc"], errors="ignore")
        if key not in _NO_VOL_FILTER:
            df_old = df_old[df_old["Volume"] > 0]
        if _detect_split(df_old, df_new):
            print(f"  [SPLIT] {key} 大幅乖離検出 → 5分足を再構築")
            df = df_new
        else:
            df = pd.concat([df_old, df_new]).sort_index()
            df = df[~df.index.duplicated(keep="last")]
    else:
        df = df_new

    if df.empty:
        print(f"  [SKIP] {key} ({ticker}) - 有効な5分足データなし")
        return

    # インデックスを JST に変換
    df.index = df.index.tz_convert("Asia/Tokyo")
    df.index.name = "datetime"
    # UTC 列を先頭に追加（旧プロジェクトとの互換性・タイムゾーン確認用）
    df.insert(0, "datetime_utc", df.index.tz_convert("UTC"))
    df.to_parquet(out)
    print(f"  [OK] {key} 5分足 {len(df)}行 → {out.name}")


def main() -> None:
    total = len(MACRO_TICKERS)
    for i, (key, ticker) in enumerate(MACRO_TICKERS.items()):
        print(f"[{i+1}/{total}] {key} ({ticker})")
        fetch_daily(key, ticker)
        fetch_5min(key, ticker)
    print("マクロ 完了")


if __name__ == "__main__":
    main()
