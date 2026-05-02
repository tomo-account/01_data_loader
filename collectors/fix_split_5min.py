"""
株式分割補正スクリプト（5分足 Parquet）

分割比率・対象銘柄・分割日を指定して、分割日より前の 5 分足データを補正する。
補正前に自動バックアップを作成し、補正ログ（data/master/split_corrections.csv）に記録する。
同じ（銘柄・分割日・比率）の組み合わせが既にログに存在する場合は処理を中断する。

使い方:
  python collectors/fix_split_5min.py --code 8001 --split-date 2026-01-15 --split 1:5
  python collectors/fix_split_5min.py --code 8001 --split-date 2026-01-15 --split 1:5 --dry-run
  python collectors/fix_split_5min.py --code 8001 --split-date 2026-01-15 --split 1:5 --no-backup

補正式（"A:B" 分割 = 1株が B/A 株になる）:
  price_factor  = A / B  （価格は分割後に 1/N になる）
  volume_factor = B / A  （出来高は分割後に N 倍になる）
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.paths import MASTER, PRICES_STOCKS_5MIN

CORRECTION_LOG = MASTER / "split_corrections.csv"
LOG_COLUMNS = [
    "code", "split_date", "split_ratio",
    "price_factor", "volume_factor",
    "rows_affected", "backup_path", "applied_at",
]

PRICE_COLS  = ["Open", "High", "Low", "Close"]
VOLUME_COLS = ["Volume"]


# ──────────────────────────────────────────────────────────────────────────────

def _parse_split(split_str: str) -> tuple[float, float]:
    """
    "A:B" → (price_factor=A/B, volume_factor=B/A)
    例: "1:5" → 0.2, 5.0  （1株→5株の5分割）
    """
    try:
        a_str, b_str = split_str.split(":")
        a, b = float(a_str), float(b_str)
        if a <= 0 or b <= 0:
            raise ValueError
    except (ValueError, AttributeError):
        print(f"[ERROR] --split の形式が不正です（例: '1:5'）: {split_str!r}")
        sys.exit(1)
    return a / b, b / a


def _already_applied(code: str, split_date: str, split_ratio: str) -> bool:
    """補正ログに同じエントリが存在するか確認する。"""
    if not CORRECTION_LOG.exists():
        return False
    with open(CORRECTION_LOG, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("code") == code
                    and row.get("split_date") == split_date
                    and row.get("split_ratio") == split_ratio):
                return True
    return False


def _append_log(
    code: str, split_date: str, split_ratio: str,
    price_factor: float, volume_factor: float,
    rows_affected: int, backup_path: str,
) -> None:
    """補正ログに1行追記する（ファイルがなければヘッダ付きで新規作成）。"""
    MASTER.mkdir(parents=True, exist_ok=True)
    write_header = not CORRECTION_LOG.exists()
    with open(CORRECTION_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow({
            "code":          code,
            "split_date":    split_date,
            "split_ratio":   split_ratio,
            "price_factor":  round(price_factor, 8),
            "volume_factor": round(volume_factor, 8),
            "rows_affected": rows_affected,
            "backup_path":   backup_path,
            "applied_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })


# ──────────────────────────────────────────────────────────────────────────────

def fix_split(
    code: str,
    split_date: str,
    split_str: str,
    dry_run: bool = False,
    no_backup: bool = False,
) -> None:
    parquet_path = PRICES_STOCKS_5MIN / f"{code}.parquet"

    # ── ファイル存在確認 ──────────────────────────────────────────
    if not parquet_path.exists():
        print(f"[ERROR] ファイルが見つかりません: {parquet_path}")
        sys.exit(1)

    # ── 二重適用防止 ──────────────────────────────────────────────
    if _already_applied(code, split_date, split_str):
        print(
            f"[SKIP] コード {code} の分割 {split_str}（{split_date}）は"
            f" 既にログに記録されています。処理をスキップします。"
        )
        return

    # ── 比率計算 ──────────────────────────────────────────────────
    price_factor, volume_factor = _parse_split(split_str)

    # ── データ読み込み ────────────────────────────────────────────
    df = pd.read_parquet(parquet_path)
    if df.index.tz is not None:
        cutoff = pd.Timestamp(split_date, tz=df.index.tz)
    else:
        cutoff = pd.Timestamp(split_date)

    mask = df.index < cutoff
    rows_affected = int(mask.sum())

    # ── プレビュー ────────────────────────────────────────────────
    print(f"コード       : {code}")
    print(f"Parquet      : {parquet_path}")
    print(f"分割比率     : {split_str}  (price ×{price_factor:.6g}, volume ×{volume_factor:.6g})")
    print(f"分割日       : {split_date}")
    print(f"総行数       : {len(df):,}")
    print(f"補正対象行数 : {rows_affected:,}  （{split_date} より前）")

    if rows_affected == 0:
        print("[INFO] 補正対象の行がありません。処理を終了します。")
        return

    # 補正前後サンプル（最後の補正対象行を表示）
    sample_idx = df.index[mask][-1]
    print(f"\n--- 補正対象の最終行（{sample_idx}）---")
    print(f"  補正前: {df.loc[sample_idx, PRICE_COLS + VOLUME_COLS].to_dict()}")

    sample_row = df.loc[sample_idx].copy()
    for col in PRICE_COLS:
        if col in sample_row:
            sample_row[col] = round(sample_row[col] * price_factor, 4)
    for col in VOLUME_COLS:
        if col in sample_row:
            sample_row[col] = round(sample_row[col] * volume_factor, 0)
    print(f"  補正後: {sample_row[PRICE_COLS + VOLUME_COLS].to_dict()}")

    if dry_run:
        print("\n[DRY-RUN] ファイルへの書き込みはスキップしました。")
        return

    # ── バックアップ ──────────────────────────────────────────────
    backup_path_str = ""
    if not no_backup:
        bak = Path(str(parquet_path) + ".bak")
        shutil.copy2(parquet_path, bak)
        backup_path_str = str(bak)
        print(f"\nバックアップ : {bak}")

    # ── 補正適用 ──────────────────────────────────────────────────
    for col in PRICE_COLS:
        if col in df.columns:
            df.loc[mask, col] = (df.loc[mask, col] * price_factor).round(4)

    for col in VOLUME_COLS:
        if col in df.columns:
            df.loc[mask, col] = (
                (df.loc[mask, col] * volume_factor).round(0).astype("int64")
            )

    df.to_parquet(parquet_path)
    print(f"[OK] {parquet_path} を上書き保存しました。")

    # ── ログ記録 ──────────────────────────────────────────────────
    _append_log(
        code=code,
        split_date=split_date,
        split_ratio=split_str,
        price_factor=price_factor,
        volume_factor=volume_factor,
        rows_affected=rows_affected,
        backup_path=backup_path_str,
    )
    print(f"[LOG] {CORRECTION_LOG} に記録しました。")


# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="5分足 Parquet に株式分割補正を適用する"
    )
    parser.add_argument("--code",       required=True, help="証券コード（例: 8001）")
    parser.add_argument("--split-date", required=True, help="分割日 YYYY-MM-DD（この日より前のデータを補正）")
    parser.add_argument("--split",      required=True, help="分割比率 A:B（例: 1:5 = 1株→5株）")
    parser.add_argument("--dry-run",    action="store_true", help="補正内容をプレビューし、ファイルへの書き込みはしない")
    parser.add_argument("--no-backup",  action="store_true", help="バックアップを作成しない")
    args = parser.parse_args()

    fix_split(
        code=args.code,
        split_date=args.split_date,
        split_str=args.split,
        dry_run=args.dry_run,
        no_backup=args.no_backup,
    )


if __name__ == "__main__":
    main()
