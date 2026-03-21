import pandas as pd
import yfinance as yf
import os
import time
import shutil
from datetime import datetime

# --- 設定 ---
LIST_FILE_FILTERING = "_filtering_list.xlsx"
LIST_FILE_TOPIX = "_topix_list.xlsx"  # 日足用
EXCEL_TICKER_COL = '銘柄コード_yf'     # filtering_list用
TOPIX_CODE_COL = 'コード'             # topix_list用
TIMEZONE_JST = 'Asia/Tokyo'
BACKUP_DIR = "backups"
SLEEP_TIME = 0.5

# 日経平均ティッカー（全時間軸・全CSVに追記）
NIKKEI225_TICKER = "^N225"

# --- 先物設定 ---
# CME日経先物（円建て）。ドル建てにしたい場合は "NKD=F" に変更
FUTURES_TICKER   = "NIY=F"
FUTURES_CONFIGS  = {
    # 5分足：直近3日分を継ぎ足し保存
    "5m": {"save_file": "_futures_5min.csv", "period": "3d",  "update_mode": "append"},
    # 日足：直近2年分を毎回上書き（先物は限月ロールがあるため長期は参考値）
    "1d": {"save_file": "_futures_daily.csv","period": "2y",  "update_mode": "overwrite"},
}

INTERVAL_CONFIGS = {
    "5m": {"save_file": "_5min.csv", "period": "3d", "update_mode": "append"},
    "1h": {"save_file": "_1h.csv", "period": "3d", "update_mode": "append"},
    "1d": {"save_file": "_daily.csv", "period": "1y", "update_mode": "overwrite"}
}

def update_market_data():
    # 1. バックアップ（5mと1hのみ対象）
    if not os.path.exists(BACKUP_DIR): os.makedirs(BACKUP_DIR)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for interval in ["5m", "1h"]:
        f = INTERVAL_CONFIGS[interval]["save_file"]
        if os.path.exists(f): shutil.copy2(f, os.path.join(BACKUP_DIR, f"{f}_bak_{timestamp}.csv"))
    print(f"✅ 5分足・1時間足のバックアップ完了")

    execution_summary = []

    # 2. 取得ループ
    for interval, conf in INTERVAL_CONFIGS.items():
        save_file = conf["save_file"]
        period = conf["period"]
        mode = conf["update_mode"]
        
        # --- 銘柄リストの切り替え ---
        try:
            if interval == "1d":
                # 日足の場合は _topix_list.xlsx を使用
                df_list = pd.read_excel(LIST_FILE_TOPIX, engine='openpyxl')
                # 「コード」列に ".T" を付与して文字列化
                # ※ str(c) だと "1234.0.T" になるため、末尾の ".0" を除去してから付与
                def to_ticker(c):
                    s = str(c).strip()
                    if s.endswith(".0") and s[:-2].isdigit():
                        s = s[:-2]
                    return f"{s}.T"
                tickers = [to_ticker(c) for c in df_list[TOPIX_CODE_COL].dropna().unique()]
                list_name = LIST_FILE_TOPIX
            else:
                # 5m, 1h の場合は _filtering_list.xlsx を使用
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

def update_futures_data():
    """
    CME日経先物（NIY=F）の5分足・日足を取得して CSV に保存する。

    5分足 (_futures_5min.csv)
      - 継ぎ足しモード：既存CSVに新しいデータを追記し重複を除去
      - Datetime / Datetime_JST / Ticker / OHLCV の列構成
      - 夜間セッション（17:00〜翌6:00）のデータも含まれる

    日足 (_futures_daily.csv)
      - 上書きモード：直近2年分を毎回取り直す
      - 先物は限月ロールがあるため長期データは参考値として扱う
      - Date / Ticker / OHLCV の列構成
      - 前日比(%) を futures_chg 列として付加して保存

    呼び出し元の update_market_data() の末尾から実行される。
    """
    print(f"\n🚀 【先物】{FUTURES_TICKER} の取得開始...")

    # バックアップ（5分足のみ）
    f5 = FUTURES_CONFIGS["5m"]["save_file"]
    if os.path.exists(f5):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(f5, os.path.join(BACKUP_DIR, f"{f5}_bak_{timestamp}.csv"))

    futures_summary = []

    for interval, conf in FUTURES_CONFIGS.items():
        save_file = conf["save_file"]
        period    = conf["period"]
        mode      = conf["update_mode"]

        try:
            print(f"  [{interval}] {FUTURES_TICKER} 取得中...", end="\r")
            df = yf.download(
                FUTURES_TICKER,
                period=period,
                interval=interval,
                progress=False,
                auto_adjust=True
            )
        except Exception as e:
            print(f"  ❌ 取得失敗 ({interval}): {e}")
            continue

        if df.empty:
            print(f"  ⚠️  データなし ({interval})")
            continue

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.reset_index()
        df['Ticker'] = FUTURES_TICKER

        # ── 5分足の整形 ──────────────────────────────────────────
        if interval == "5m":
            df = df.rename(columns={'Date': 'Datetime'})
            df['Datetime']     = pd.to_datetime(df['Datetime'], utc=True)
            df['Datetime_JST'] = df['Datetime'].dt.tz_convert(TIMEZONE_JST)

            if mode == "append" and os.path.exists(save_file):
                old_df = pd.read_csv(save_file)
                old_df['Datetime'] = pd.to_datetime(old_df['Datetime'], utc=True)
                final_df = pd.concat([old_df, df], ignore_index=True)
                final_df = final_df.drop_duplicates(subset=['Datetime', 'Ticker'], keep='last')
            else:
                final_df = df

            final_df = final_df.sort_values('Datetime').reset_index(drop=True)
            fixed = ['Datetime', 'Datetime_JST', 'Ticker']

        # ── 日足の整形 ────────────────────────────────────────────
        else:
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            elif 'Datetime' in df.columns:
                df = df.rename(columns={'Datetime': 'Date'})
                df['Date'] = pd.to_datetime(df['Date'])

            df = df.sort_values('Date')

            # 前日比(%) を付加（勝率分析アプリで直接使える形にする）
            df['futures_chg'] = (
                (df['Close'] - df['Close'].shift(1)) / df['Close'].shift(1) * 100
            ).round(3)

            df['Date']  = df['Date'].dt.strftime('%Y-%m-%d')
            final_df    = df
            fixed       = ['Date', 'Ticker']

        # ── 保存 ─────────────────────────────────────────────────
        key_col  = fixed[0]
        cols     = fixed + [c for c in final_df.columns if c not in fixed]
        final_df[cols].to_csv(save_file, index=False, encoding='utf_8_sig')

        futures_summary.append({
            "interval"    : interval,
            "mode"        : "継ぎ足し" if mode == "append" else "上書き",
            "file"        : save_file,
            "total_rows"  : len(final_df),
            "latest"      : final_df[key_col].max()
        })

    # ── 先物レポート ─────────────────────────────────────────────
    if futures_summary:
        print(f"\n{'─'*80}")
        print(f"📡 先物取得レポート ({FUTURES_TICKER})")
        print(f"{'─'*80}")
        print(f"{'足':<5} | {'方式':<8} | {'総行数':<10} | {'最新日時':<25} | {'ファイル'}")
        print(f"{'─'*80}")
        for s in futures_summary:
            print(f"{s['interval']:<5} | {s['mode']:<8} | {s['total_rows']:>10,} | "
                  f"{str(s['latest']):<25} | {s['file']}")
        print(f"{'─'*80}")
    else:
        print("  ⚠️  先物データの取得・保存に失敗しました。")


if __name__ == "__main__":
    update_market_data()
    update_futures_data()