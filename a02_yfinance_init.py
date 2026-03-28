"""
a02_yfinance_init.py
============================
【初回実行用】yfinanceから株価データを取得してCSVに保存する

■ 概要
  初回のみ実行するスクリプトです。
  過去データをまとめて取得し、以下のCSVファイルを新規作成します。

    _5min.csv    : 5分足（直近60日分）
    _daily.csv   : 日足（直近3年分）

  2回目以降のデータ更新は a03_yfinance_update.py を使用してください。

■ 取得対象の銘柄
  - 5分足: _stock_list.xlsx の「ティッカーコード」列
  - 日足 : _topix_list.xlsx の「ティッカーコード」列
  - 日経平均（^N225）はすべての時間足に追加されます

■ 注意事項
  - yfinanceは非公式APIです。大量の銘柄を連続取得するとサーバー負荷になります。
    SLEEP_TIME を調整して、適切な間隔を空けて実行してください。
  - 5分足は yfinance の仕様上、直近60日分しか取得できません。
  - 取得データのタイムゾーンはUTCです。日本時間は Datetime_JST 列に変換済みです。

■ 実行方法
  python a02_yfinance_init.py
"""

import pandas as pd
import yfinance as yf
import os
import time
from datetime import datetime

# ================================================
# 設定
# ================================================
LIST_FILE_FILTERING = "_stock_list.xlsx"   # 5分足データ取得用銘柄リスト
LIST_FILE_TOPIX     = "_topix_list.xlsx"        # 日足用銘柄リスト（TOPIX全銘柄）
EXCEL_TICKER_COL    = 'ティッカーコード'           # stock_list / topix_list 共通の列名
TOPIX_CODE_COL      = 'ティッカーコード'            # topix_list の列名
TIMEZONE_JST        = 'Asia/Tokyo'
SLEEP_TIME          = 0.5                       # 銘柄間の待機秒数（サーバー負荷軽減）

# 日経平均（全時間足・全CSVに追加）
NIKKEI225_TICKER = "^N225"

# 時間足ごとの設定
#   period : yfinanceに渡す取得期間
#   ※ 5分足は仕様上 "60d" が上限
INTERVAL_CONFIGS = {
    "5m": {"save_file": "_5min.csv",   "period": "60d"},
    "1d": {"save_file": "_daily.csv",  "period": "3y"},
}


# ================================================
# ヘルパー関数
# ================================================

def load_tickers(interval: str) -> list:
    """
    時間足に応じてExcelから銘柄リストを読み込む。
    いずれのファイルも「ティッカーコード」列をそのまま使用する。
    日経平均（^N225）をリスト末尾に追加して返す。
    """
    try:
        if interval == "1d":
            df = pd.read_excel(LIST_FILE_TOPIX, engine='openpyxl')
            tickers = df[TOPIX_CODE_COL].dropna().unique().tolist()
            list_name = LIST_FILE_TOPIX
        else:
            df = pd.read_excel(LIST_FILE_FILTERING, engine='openpyxl')
            tickers = df[EXCEL_TICKER_COL].dropna().unique().tolist()
            list_name = LIST_FILE_FILTERING
    except Exception as e:
        print(f"  ❌ Excel読み込みエラー ({interval}): {e}")
        return []

    if NIKKEI225_TICKER not in tickers:
        tickers = list(tickers) + [NIKKEI225_TICKER]

    print(f"  📋 銘柄リスト: {list_name}（{len(tickers)}銘柄、日経225含む）")
    return tickers


def fetch_and_format(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """
    1銘柄のデータをyfinanceで取得し、列名を整形して返す。

    Returns:
        整形済みDataFrame。取得失敗または空の場合は空のDataFrameを返す。
    """
    try:
        df = yf.download(ticker, period=period, interval=interval,
                         progress=False, auto_adjust=True)
        if df.empty:
            return pd.DataFrame()

        # MultiIndex解除（yfinanceのバージョン差異に対応）
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()

        if interval == "1d":
            # 日足：Date列をdatetime化
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            elif 'Datetime' in df.columns:
                df = df.rename(columns={'Datetime': 'Date'})
                df['Date'] = pd.to_datetime(df['Date'])
        else:
            # 5分足：Datetime列をUTCとして保持し、JSTも付加
            df = df.rename(columns={'Date': 'Datetime'})
            df['Datetime']     = pd.to_datetime(df['Datetime'], utc=True)
            df['Datetime_JST'] = df['Datetime'].dt.tz_convert(TIMEZONE_JST)

        df['Ticker'] = ticker
        return df

    except Exception:
        return pd.DataFrame()


# ================================================
# メイン処理
# ================================================

def init_market_data():
    """
    全時間足の株価データを初回取得してCSVに保存する。
    既存のCSVがある場合は上書きされます。
    """
    print("=" * 70)
    print(f"  yfinance 初回データ取得スクリプト")
    print(f"  実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    execution_summary = []

    for interval, conf in INTERVAL_CONFIGS.items():
        save_file = conf["save_file"]
        period    = conf["period"]

        print(f"\n🚀 【{interval}】取得開始（期間: {period}）")

        # 既存ファイルの確認
        if os.path.exists(save_file):
            print(f"  ⚠️  {save_file} が既に存在します。上書きします。")

        tickers = load_tickers(interval)
        if not tickers:
            continue

        new_frames = []
        success_count = 0

        for i, ticker in enumerate(tickers):
            print(f"  [{i+1:>4}/{len(tickers)}] {ticker:<15}", end="\r")
            df = fetch_and_format(ticker, period, interval)
            if not df.empty:
                new_frames.append(df)
                success_count += 1
            time.sleep(SLEEP_TIME)

        if not new_frames:
            print(f"  ❌ 取得できたデータがありません。")
            continue

        # 保存処理
        final_df = pd.concat(new_frames, ignore_index=True)

        if interval == "1d":
            final_df['Date'] = pd.to_datetime(final_df['Date']).dt.strftime('%Y-%m-%d')
            key_col = 'Date'
            fixed   = ['Date', 'Ticker']
        else:
            key_col = 'Datetime'
            fixed   = ['Datetime', 'Datetime_JST', 'Ticker']

        final_df = final_df.sort_values(['Ticker', key_col]).reset_index(drop=True)
        cols = fixed + [c for c in final_df.columns if c not in fixed]
        final_df[cols].to_csv(save_file, index=False, encoding='utf_8_sig')

        execution_summary.append({
            "interval"      : interval,
            "file"          : save_file,
            "tickers_total" : len(tickers),
            "success"       : success_count,
            "total_rows"    : len(final_df),
            "latest"        : final_df[key_col].max(),
        })
        print(f"  ✅ 保存完了: {save_file}（{success_count}/{len(tickers)}銘柄、{len(final_df):,}行）")

    # 最終レポート
    print("\n" + "=" * 70)
    print(f"📊 実行レポート ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print("=" * 70)
    print(f"{'足':<5} | {'銘柄数':>8} | {'総行数':>10} | {'最新日時':<22} | ファイル")
    print("-" * 70)
    for s in execution_summary:
        print(f"{s['interval']:<5} | "
              f"{s['success']:>3}/{s['tickers_total']:<4} | "
              f"{s['total_rows']:>10,} | "
              f"{str(s['latest']):<22} | "
              f"{s['file']}")
    print("=" * 70)
    print("\n✅ 初回取得完了。次回以降は a03_yfinance_update.py を実行してください。")


if __name__ == "__main__":
    init_market_data()