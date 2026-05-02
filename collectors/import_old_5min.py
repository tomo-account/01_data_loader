"""
旧5分足データを現在のParquetに prepend する

入力 : C:\_python\stock_analysis\_ohlcv\_5min.csv
出力 : data/prices/stocks/5min/{code}.parquet（上書き）

処理フロー:
  1. 旧CSVをロード・銘柄別にグループ化
  2. 各銘柄について現在のParquetと重複期間の価格を比較
  3. 乖離が2%超 → 旧データを比率補正（株式分割対応）
  4. 旧データ（新データ開始前）を prepend して保存
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from config.paths import PRICES_STOCKS_5MIN

OLD_CSV = Path(r"C:\_python\stock_analysis\_ohlcv\_5min.csv")
SPLIT_THRESHOLD = 0.02   # 2%以上の乖離で補正あり
MIN_OVERLAP_ROWS = 10    # 比率計算に必要な最低重複行数


def run() -> None:
    print("旧5分足CSV ロード中（しばらくかかります）...")
    old_all = pd.read_csv(
        OLD_CSV,
        parse_dates=["Datetime"],
        usecols=["Datetime", "Ticker", "Close", "High", "Low", "Open", "Volume"],
    )
    old_all["Datetime"] = pd.to_datetime(old_all["Datetime"], utc=True)
    old_all = old_all.set_index("Datetime").sort_index()
    print(f"ロード完了: {len(old_all):,} 行 / {old_all['Ticker'].nunique()} 銘柄")

    updated  = 0
    adjusted = 0
    skipped  = 0
    no_file  = 0

    grouped = old_all.groupby("Ticker")

    for ticker, old_df in grouped:
        code = ticker.replace(".T", "")
        parquet_path = PRICES_STOCKS_5MIN / f"{code}.parquet"

        if not parquet_path.exists():
            no_file += 1
            continue

        old_df = old_df[["Close", "High", "Low", "Open", "Volume"]].copy()

        # 現在のParquet
        new_df = pd.read_parquet(parquet_path)
        new_df.index = pd.to_datetime(new_df.index, utc=True)
        new_start = new_df.index.min()

        # 旧データのうち新データより前の期間だけ追加対象
        old_before = old_df[old_df.index < new_start]
        if old_before.empty:
            skipped += 1
            continue

        # 重複期間（新データ開始以降）で価格比率を確認
        ratio = 1.0
        overlap_old = old_df[old_df.index >= new_start]
        common_idx  = overlap_old.index.intersection(new_df.index)

        if len(common_idx) >= MIN_OVERLAP_ROWS:
            old_prices = overlap_old.loc[common_idx, "Close"]
            new_prices = new_df.loc[common_idx, "Close"]
            valid = old_prices > 0
            if valid.sum() >= MIN_OVERLAP_ROWS:
                ratio = float((new_prices[valid] / old_prices[valid]).median())

        # 乖離が閾値超なら旧データを補正（株式分割調整）
        if abs(ratio - 1.0) > SPLIT_THRESHOLD:
            for col in ["Close", "High", "Low", "Open"]:
                old_before[col] = old_before[col] * ratio
            adjusted += 1
            print(f"  [ADJUST] {code}  ratio={ratio:.4f}")

        # prepend → 重複除去 → 保存
        combined = pd.concat([old_before, new_df]).sort_index()
        combined = combined[~combined.index.duplicated(keep="last")]
        combined.index.name = "datetime"
        combined.to_parquet(parquet_path)
        updated += 1

        if updated % 100 == 0:
            print(f"  進捗: {updated} 銘柄更新済み...")

    print()
    print(f"=== 完了 ===")
    print(f"  更新: {updated} 銘柄")
    print(f"  比率補正（分割対応）: {adjusted} 銘柄")
    print(f"  スキップ（追加データなし）: {skipped} 銘柄")
    print(f"  Parquetなし: {no_file} 銘柄")


if __name__ == "__main__":
    run()
