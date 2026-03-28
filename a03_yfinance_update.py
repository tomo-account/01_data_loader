"""
a03_yfinance_update.py
============================
【定期実行用】yfinanceから株価データを取得してCSVに継ぎ足す

■ 概要
  _5min.csv / _daily.csv に最新データを継ぎ足します。
  初回実行は a02_yfinance_init.py を使用してください。

■ 取得対象の銘柄
  - 5分足: _stock_list.xlsx の「ティッカーコード」列
  - 日足 : _topix_list.xlsx の「ティッカーコード」列
  - 日経平均（^N225）はすべての時間足に追加されます

■ 実行方法
  python a03_yfinance_update.py
"""

import pandas as pd
import yfinance as yf
import os
import time
import shutil
from datetime import datetime
from pathlib import Path

# --- 設定 ---
LIST_FILE_FILTERING = "_stock_list.xlsx"
LIST_FILE_TOPIX = "_topix_list.xlsx"
EXCEL_TICKER_COL = 'ティッカーコード'     # stock_list / topix_list 共通の列名
TOPIX_CODE_COL = 'ティッカーコード'       # topix_list の列名
TIMEZONE_JST = 'Asia/Tokyo'
BACKUP_DIR = "backups"
SLEEP_TIME = 0.5

# 日経平均ティッカー（全時間軸・全CSVに追記）
NIKKEI225_TICKER = "^N225"


INTERVAL_CONFIGS = {
    "5m": {"save_file": "_5min.csv", "period": "3d", "update_mode": "append"},
    "1d": {"save_file": "_daily.csv", "period": "3d", "update_mode": "overwrite"}
}

TIMEZONE_JST_STR = "Asia/Tokyo"

# ================================================
# Parquet変換ヘルパー
# ================================================

def _csv_to_parquet(csv_path: Path, parquet_path: Path) -> bool:
    """
    CSVをParquetに変換して保存する。
    アプリ側の _build_parquet_if_needed と同じ変換ロジックを使用。
    """
    if not csv_path.exists():
        return False
    if "5min" in csv_path.name:
        df = pd.read_csv(csv_path)
        dt_col = "Datetime_JST" if "Datetime_JST" in df.columns else "Datetime" if "Datetime" in df.columns else None
        if dt_col is None:
            return False
        df["Datetime"] = pd.to_datetime(df[dt_col])
        if df["Datetime"].dt.tz is None:
            df["Datetime"] = df["Datetime"].dt.tz_localize(TIMEZONE_JST_STR)
        else:
            df["Datetime"] = df["Datetime"].dt.tz_convert(TIMEZONE_JST_STR)
        df["_date"] = df["Datetime"].dt.date.astype(str)
        df["Ticker"] = df["Ticker"].astype(str).str.strip()
        drop_cols = [c for c in ["Datetime_JST"] if c in df.columns]
        df.drop(columns=drop_cols, inplace=True)
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
    else:
        df = pd.read_csv(csv_path)
        if "Date" not in df.columns or "Ticker" not in df.columns:
            return False
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df["Ticker"] = df["Ticker"].astype(str).str.strip()
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
    return True

def update_market_data():
    # 1. バックアップ（5mのみ対象）
    if not os.path.exists(BACKUP_DIR): os.makedirs(BACKUP_DIR)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for interval in ["5m"]:
        f = INTERVAL_CONFIGS[interval]["save_file"]
        if os.path.exists(f): shutil.copy2(f, os.path.join(BACKUP_DIR, f"{f}_bak_{timestamp}.csv"))
    print(f"✅ 5分足のバックアップ完了")

    execution_summary = []

    # 2. 取得ループ
    for interval, conf in INTERVAL_CONFIGS.items():
        save_file = conf["save_file"]
        period = conf["period"]
        mode = conf["update_mode"]
        
        # --- 銘柄リストの切り替え ---
        try:
            if interval == "1d":
                # 日足は _topix_list.xlsx の「ティッカーコード」列をそのまま使用
                df_list = pd.read_excel(LIST_FILE_TOPIX, engine='openpyxl')
                tickers = df_list[TOPIX_CODE_COL].dropna().unique().tolist()
                list_name = LIST_FILE_TOPIX
            else:
                # 5m は _stock_list.xlsx の「ティッカーコード」列をそのまま使用
                df_list = pd.read_excel(LIST_FILE_FILTERING, engine='openpyxl')
                tickers = df_list[EXCEL_TICKER_COL].dropna().unique().tolist()
                list_name = LIST_FILE_FILTERING
        except Exception as e:
            print(f"Excel読み込みエラー ({interval}): {e}")
            continue

        # 日経225を全時間軸のリスト末尾に追加
        if NIKKEI225_TICKER not in tickers:
            tickers = list(tickers) + [NIKKEI225_TICKER]

        print(f"\n🚀 【{interval}】 取得中 ({mode}) / リスト: {list_name}（日経225含む）...")
        
        # 継ぎ足しモードの場合のみ既存読み込み
        old_df = pd.read_csv(save_file) if mode == "append" and os.path.exists(save_file) else pd.DataFrame()
        
        new_data_frames = []
        success_count = 0
        
        for i, ticker in enumerate(tickers):
            try:
                print(f"  [{i+1}/{len(tickers)}] {ticker}...", end="\r")
                df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
                if not df.empty:
                    if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
                    df = df.reset_index()
                    
                    if interval == "1d":
                        if 'Date' in df.columns:
                            df['Date'] = pd.to_datetime(df['Date'])
                        elif 'Datetime' in df.columns: # yfinanceの仕様により稀にDatetimeで返る場合があるため
                            df = df.rename(columns={'Datetime': 'Date'})
                            df['Date'] = pd.to_datetime(df['Date'])
                    else:
                        df = df.rename(columns={'Date': 'Datetime'})
                        df['Datetime'] = pd.to_datetime(df['Datetime'], utc=True)
                        df['Datetime_JST'] = df['Datetime'].dt.tz_convert(TIMEZONE_JST)
                    
                    df['Ticker'] = ticker
                    new_data_frames.append(df)
                    success_count += 1
                time.sleep(SLEEP_TIME)
            except Exception as e:
                pass

        # 3. 保存処理
        if new_data_frames:
            new_df_all = pd.concat(new_data_frames, ignore_index=True)
            
            if mode == "append" and not old_df.empty:
                # 継ぎ足し処理
                dt_col = 'Datetime'
                old_df[dt_col] = pd.to_datetime(old_df[dt_col], utc=True)
                final_df = pd.concat([old_df, new_df_all], ignore_index=True)
                final_df = final_df.drop_duplicates(subset=[dt_col, 'Ticker'], keep='last')
                fixed = ['Datetime', 'Datetime_JST', 'Ticker']
            else:
                # 新規保存（日足など）
                final_df = new_df_all
                if interval == "1d":
                    final_df['Date'] = pd.to_datetime(final_df['Date']).dt.strftime('%Y-%m-%d')
                    fixed = ['Date', 'Ticker']
                else:
                    fixed = ['Datetime', 'Datetime_JST', 'Ticker']

            # ソートと列整理
            key_col = fixed[0]
            final_df = final_df.sort_values(['Ticker', key_col]).reset_index(drop=True)
            cols = fixed + [c for c in final_df.columns if c not in fixed]
            final_df[cols].to_csv(save_file, index=False, encoding='utf_8_sig')
            
            # レポート用データの蓄積
            execution_summary.append({
                "interval": interval,
                "mode": "継ぎ足し" if mode == "append" else "新規取得",
                "file": save_file,
                "total_rows": len(final_df),
                "success": success_count,
                "tickers_count": len(tickers),
                "latest": final_df[key_col].max()
            })

    # 4. 最終レポート表示
    print("\n" + "="*80)
    print(f"📊 最終実行レポート ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print("="*80)
    print(f"{'足':<5} | {'方式':<8} | {'銘柄数':<6} | {'総行数':<10} | {'最新日時':<20}")
    print("-" * 80)
    for s in execution_summary:
        print(f"{s['interval']:<5} | {s['mode']:<8} | {s['success']:>3}/{s['tickers_count']:<2} | {s['total_rows']:>10,} | {str(s['latest']):<20}")
    print("="*80)

    # 5. Parquet変換（継ぎ足し完了後に実行）
    print("\n📦 Parquet変換中...")
    parquet_targets = {
        "_5min.csv":   "_5min.parquet",
        "_daily.csv":  "_daily.parquet",
    }
    for csv_name, parquet_name in parquet_targets.items():
        csv_p     = Path(csv_name)
        parquet_p = Path(parquet_name)
        if _csv_to_parquet(csv_p, parquet_p):
            print(f"  ✅ {parquet_name} 変換完了")
        elif not csv_p.exists():
            print(f"  ⚠️  {csv_name} が存在しないためスキップ")
        else:
            print(f"  ℹ️  {parquet_name} 変換失敗（列構成を確認してください）")


if __name__ == "__main__":
    update_market_data()