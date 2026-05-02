"""
既存 Parquet ファイルの MultiIndex 列を一括フラット化

yfinance の仕様変更により列が MultiIndex になって保存されたファイルを修正する。
fetch_prices_*.py を修正済みの今後は不要だが、既存データの一時修正用として保持。

実行方法: python collectors/fix_parquet_columns.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from config.paths import (
    PRICES_STOCKS_DAILY, PRICES_STOCKS_5MIN,
    PRICES_MACRO_DAILY,  PRICES_MACRO_5MIN,
)


def fix_file(path: Path) -> bool:
    """MultiIndex 列なら修正して上書き保存。変更なしなら False を返す。"""
    df = pd.read_parquet(path)
    if not isinstance(df.columns, pd.MultiIndex):
        return False
    df.columns = df.columns.get_level_values(0)
    df.to_parquet(path)
    return True


def fix_directory(directory: Path, label: str) -> None:
    files = sorted(directory.glob("*.parquet"))
    fixed = 0
    for f in files:
        if fix_file(f):
            fixed += 1
    print(f"  {label}: {len(files)}ファイル中 {fixed}件修正")


def main() -> None:
    print("Parquet MultiIndex 修正開始...")
    fix_directory(PRICES_STOCKS_DAILY, "個別株 日足")
    fix_directory(PRICES_STOCKS_5MIN,  "個別株 5分足")
    fix_directory(PRICES_MACRO_DAILY,  "マクロ 日足")
    fix_directory(PRICES_MACRO_5MIN,   "マクロ 5分足")
    print("完了")


if __name__ == "__main__":
    main()
