import pandas as pd
import yfinance as yf
import os
import time
import shutil
from datetime import datetime
from pathlib import Path

# --- 設定 ---
TIMEZONE_JST = 'Asia/Tokyo'
BACKUP_DIR = "backups"
SLEEP_TIME = 0.5

# --- 管理対象銘柄 ---
# 追加・削除したい場合はここを編集する
FUTURES_TICKERS = {
    "NIY=F": "日経先物（円建て）",
    "CL=F":  "原油先物（WTI）",
    "GC=F":  "金先物",
    "JPY=X": "ドル円",
}

# ================================================
# Parquet変換ヘルパー
# ================================================

def _csv_to_parquet_futures(csv_path: Path, parquet_path: Path) -> bool:
    """
    先物・為替CSVをParquetに変換して保存する。
    5分足: Datetime / Datetime_JST / Ticker / Name / OHLCV
    日足 : Date / Ticker / Name / OHLCV / chg_pct
    """
    if not csv_path.exists():
        return False
    df = pd.read_csv(csv_path, encoding="utf_8_sig")
    if "5min" in csv_path.name:
        if "Datetime" not in df.columns:
            return False
        df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True)
        df["_date"] = df["Datetime"].dt.tz_convert(TIMEZONE_JST).dt.date.astype(str)
        df["Ticker"] = df["Ticker"].astype(str).str.strip()
    else:
        if "Date" not in df.columns:
            return False
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df["Ticker"] = df["Ticker"].astype(str).str.strip()
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    return True


# --- 時間軸設定 ---
# CSVが存在しない場合は first_run_period で一括取得（初回自動判定）
# CSVが存在する場合は period で継ぎ足し取得（2回目以降）
INTERVAL_CONFIGS = {
    "5m": {
        "save_file":        "_5min_futures.csv",
        "period":           "3d",    # 2回目以降（継ぎ足し）
        "first_run_period": "60d",   # 初回のみ：yfinance 5分足の最大取得期間
        "update_mode":      "append",
    },
    "1d": {
        "save_file":        "_daily_futures.csv",
        "period":           "3y",    # 毎回3年分を上書き取得（a03と同方式）
        "first_run_period": "3y",    # 初回のみ
        "update_mode":      "overwrite",
    },
}


def update_multi_futures():
    """
    複数先物・為替（NIY=F / CL=F / GC=F / JPY=X）の
    5分足・日足を取得して CSV に保存する。

    【初回実行】CSVが存在しない場合に自動判定
      5分足：過去60日分（yfinance の上限）を一括取得
      日足  ：過去2年分を一括取得

    【2回目以降】CSVが存在する場合
      5分足・日足ともに直近3日分を継ぎ足し保存（重複除去）

    列構成:
      5分足 → Datetime / Datetime_JST / Ticker / Name / OHLCV
      日足  → Date / Ticker / Name / OHLCV / chg_pct（前日比%）
    """
    # バックアップ（5分足のみ）
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    f5 = INTERVAL_CONFIGS["5m"]["save_file"]
    if os.path.exists(f5):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(f5, os.path.join(BACKUP_DIR, f"{f5}_bak_{timestamp}.csv"))

    print("=" * 70)
    print(f"🚀 先物・為替マルチ取得スクリプト")
    print(f"   実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   対象銘柄: {', '.join(FUTURES_TICKERS.keys())}")
    print("=" * 70)

    summary = []

    for interval, conf in INTERVAL_CONFIGS.items():
        save_file   = conf["save_file"]
        mode        = conf["update_mode"]
        is_intraday = (interval != "1d")

        # ── 初回 / 2回目以降を CSVの有無で自動判定 ──────────────
        is_first_run = not os.path.exists(save_file)
        period       = conf["first_run_period"] if is_first_run else conf["period"]
        run_label    = f"初回・一括取得（{period}）" if is_first_run else f"継ぎ足し（{period}）"

        print(f"\n📥 【{interval}】{run_label}")

        new_frames = []

        for ticker, name in FUTURES_TICKERS.items():
            try:
                print(f"  {ticker} ({name})...", end="\r")
                df = yf.download(
                    ticker,
                    period=period,
                    interval=interval,
                    progress=False,
                    auto_adjust=True,
                )
            except Exception as e:
                print(f"  ❌ {ticker} 取得失敗: {e}")
                time.sleep(SLEEP_TIME)
                continue

            if df.empty:
                print(f"  ⚠️  {ticker} データなし")
                time.sleep(SLEEP_TIME)
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.reset_index()
            df['Ticker'] = ticker
            df['Name']   = name

            # ── 5分足の整形 ────────────────────────────────────
            if is_intraday:
                df = df.rename(columns={'Date': 'Datetime'})
                df['Datetime']     = pd.to_datetime(df['Datetime'], utc=True)
                df['Datetime_JST'] = df['Datetime'].dt.tz_convert(TIMEZONE_JST)

            # ── 日足の整形 ──────────────────────────────────────
            else:
                if 'Datetime' in df.columns:
                    df = df.rename(columns={'Datetime': 'Date'})
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.sort_values('Date')
                df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

            new_frames.append(df)
            print(f"  ✅ {ticker} ({name}) 取得完了")
            time.sleep(SLEEP_TIME)

        if not new_frames:
            print(f"  ⚠️  [{interval}] 取得データなし。スキップします。")
            continue

        new_df = pd.concat(new_frames, ignore_index=True)

        # ── 保存処理 ────────────────────────────────────────────
        if is_intraday:
            key_col = 'Datetime'
            fixed   = ['Datetime', 'Datetime_JST', 'Ticker', 'Name']

            if mode == "append" and not is_first_run:
                old_df = pd.read_csv(save_file, encoding='utf_8_sig')
                old_df[key_col] = pd.to_datetime(old_df[key_col], utc=True)
                final_df = pd.concat([old_df, new_df], ignore_index=True)
                final_df = final_df.drop_duplicates(subset=[key_col, 'Ticker'], keep='last')
            else:
                final_df = new_df

            final_df = final_df.sort_values(['Ticker', key_col]).reset_index(drop=True)

        else:
            key_col = 'Date'
            fixed   = ['Date', 'Ticker', 'Name']

            # overwrite: 取得データで全件上書き（a03の日足と同方式）
            final_df = new_df
            final_df = final_df.sort_values(['Ticker', key_col]).reset_index(drop=True)

            # chg_pct を銘柄ごとに再計算（継ぎ足し後も正確な値にする）
            final_df['chg_pct'] = (
                final_df.groupby('Ticker')['Close']
                .transform(lambda x: x.pct_change() * 100)
                .round(3)
            )

        cols = fixed + [c for c in final_df.columns if c not in fixed]
        final_df[cols].to_csv(save_file, index=False, encoding='utf_8_sig')

        summary.append({
            "interval":   interval,
            "run_label":  run_label,
            "file":       save_file,
            "total_rows": len(final_df),
            "tickers_ok": len(new_frames),
            "latest":     final_df[key_col].max(),
        })

    # ── 最終レポート ─────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"📊 実行レポート ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print("=" * 70)
    if summary:
        print(f"{'足':<5} | {'銘柄数':<5} | {'総行数':<10} | {'最新日時':<25} | ファイル")
        print("-" * 70)
        for s in summary:
            print(
                f"{s['interval']:<5} | "
                f"{s['tickers_ok']:>2}/{len(FUTURES_TICKERS):<2} | "
                f"{s['total_rows']:>10,} | "
                f"{str(s['latest']):<25} | {s['file']}"
            )
            print(f"        └─ {s['run_label']}")
    else:
        print("  ⚠️  保存データなし")
    print("=" * 70)

    # Parquet変換（継ぎ足し完了後に実行）
    print("\n📦 Parquet変換中...")
    parquet_targets = {
        "_5min_futures.csv":   "_5min_futures.parquet",
        "_daily_futures.csv":  "_daily_futures.parquet",
    }
    for csv_name, parquet_name in parquet_targets.items():
        csv_p     = Path(csv_name)
        parquet_p = Path(parquet_name)
        if _csv_to_parquet_futures(csv_p, parquet_p):
            print(f"  ✅ {parquet_name} 変換完了")
        elif not csv_p.exists():
            print(f"  ⚠️  {csv_name} が存在しないためスキップ")
        else:
            print(f"  ℹ️  {parquet_name} 変換失敗（列構成を確認してください）")


if __name__ == "__main__":
    update_multi_futures()