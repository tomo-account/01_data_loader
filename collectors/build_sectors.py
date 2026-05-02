"""
セクター別 CSV 自動生成ユーティリティ

data/master/watch_market.csv を読み込み、セクターＡ列でグループ化して
data/master/sectors/<english_name>.csv に分割保存する。

watch_market.csv を更新したあとに実行する。
実行方法: python collectors/build_sectors.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from config.paths import WATCH_MARKET, SECTORS
from config.sector_map import SECTOR_MAP


def sector_to_filename(sector_jp: str) -> str:
    """
    セクターＡ名 → ファイル名（英語スネークケース）
    sector_map に登録済みなら定義値を使用。
    未登録の場合は日本語をそのままスネークケース化して使用。
    """
    if sector_jp in SECTOR_MAP:
        return SECTOR_MAP[sector_jp]
    # フォールバック: 記号・空白を _ に置換して小文字化
    cleaned = sector_jp.replace("・", "_").replace("　", "_").replace(" ", "_")
    cleaned = "".join(c if c.isalnum() or c == "_" else "" for c in cleaned)
    return cleaned.lower().strip("_")


def build_sectors() -> None:
    if not WATCH_MARKET.exists():
        print(f"[ERROR] {WATCH_MARKET} が見つかりません。先に watch_market.csv を配置してください。")
        return

    SECTORS.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(WATCH_MARKET, dtype={"コード": str})
    if "セクターＡ" not in df.columns:
        print("[ERROR] watch_market.csv に 'セクターＡ' 列がありません。")
        return

    groups = df.groupby("セクターＡ")
    print(f"セクター数: {len(groups)}")

    for sector_jp, group in groups:
        fname = sector_to_filename(str(sector_jp))
        out = SECTORS / f"{fname}.csv"
        group.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"  [{len(group):3d}銘柄] {sector_jp:20s} → sectors/{fname}.csv")

    print(f"\n完了: {len(groups)} ファイルを {SECTORS} に生成しました。")


if __name__ == "__main__":
    build_sectors()
