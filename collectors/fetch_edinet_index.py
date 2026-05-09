"""
金融庁 EDINET API v2 — 書類インデックス取得

1日1リクエストで当日提出書類の一覧を取得し、
data/edinet_index.parquet に追記保存する。

対象 docTypeCode:
  120 … 有価証券報告書
  130 … 訂正有価証券報告書

実行方法:
    python collectors/fetch_edinet_index.py               # 当日分
    python collectors/fetch_edinet_index.py --date 2024-06-30
    python collectors/fetch_edinet_index.py --start 2024-01-01 --end 2024-12-31
    python collectors/fetch_edinet_index.py --backfill-years 3   # 直近 N 年の営業日を一括取得
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
from dotenv import load_dotenv

from config.paths import EDINET_IDX

load_dotenv()

API_BASE   = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"
TARGET_DOC = {"120", "130"}   # 有価証券報告書 / 訂正有報
SLEEP_SEC  = 0.5              # レート制限対策


def _api_key() -> str:
    key = os.getenv("EDINET_API_KEY", "")
    if not key:
        sys.exit("[error] EDINET_API_KEY が .env に設定されていません。")
    return key


def fetch_day(dt: date, key: str) -> pd.DataFrame:
    """指定日の書類一覧を取得し DataFrame を返す（有報のみフィルタ済み）。"""
    params = {
        "date": dt.strftime("%Y-%m-%d"),
        "type": 2,            # 1=件数のみ / 2=メタデータ付き
        "Subscription-Key": key,
    }
    resp = requests.get(API_BASE, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", [])
    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df[df["docTypeCode"].isin(TARGET_DOC)].copy()
    if df.empty:
        return df

    # 必要カラムのみ残す
    keep = [
        "docID", "edinetCode", "secCode", "JCN",
        "filerName", "docTypeCode", "periodStart", "periodEnd",
        "submitDateTime", "docDescription",
    ]
    df = df[[c for c in keep if c in df.columns]]
    df["fetch_date"] = dt.strftime("%Y-%m-%d")
    return df


def load_existing() -> pd.DataFrame:
    if EDINET_IDX.exists():
        return pd.read_parquet(EDINET_IDX)
    return pd.DataFrame()


def save(df: pd.DataFrame) -> None:
    EDINET_IDX.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(EDINET_IDX, index=False)


def _business_days(start: date, end: date) -> list[date]:
    days = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:  # 月〜金
            days.append(cur)
        cur += timedelta(days=1)
    return days


def run(dates: list[date], force: bool = False) -> None:
    key = _api_key()
    existing = load_existing()

    already: set[str] = set()
    if not existing.empty and "fetch_date" in existing.columns:
        already = set(existing["fetch_date"].unique())

    new_frames: list[pd.DataFrame] = []
    skipped = 0

    for dt in dates:
        ds = dt.strftime("%Y-%m-%d")
        if ds in already and not force:
            skipped += 1
            continue
        print(f"  fetch {ds} ...", end="", flush=True)
        try:
            df = fetch_day(dt, key)
            if df.empty:
                print(" (no yuho)")
            else:
                print(f" {len(df)} docs")
                new_frames.append(df)
        except requests.HTTPError as e:
            print(f" [HTTP {e.response.status_code}] skip")
        except Exception as e:
            print(f" [error] {e}")
        time.sleep(SLEEP_SEC)

    if skipped:
        print(f"  skip {skipped} dates (already fetched)")

    if not new_frames:
        print("新規データなし。")
        return

    combined = pd.concat([existing] + new_frames, ignore_index=True)
    # docID 単位で重複除去（再取得時のため）
    combined = combined.drop_duplicates(subset=["docID"], keep="last")
    combined = combined.sort_values("submitDateTime", na_position="last")
    save(combined)
    print(f"[done] {sum(len(f) for f in new_frames)} 件追加 → {EDINET_IDX}")


def main() -> None:
    p = argparse.ArgumentParser(description="EDINET 有価証券報告書インデックス取得")
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("--date",           metavar="YYYY-MM-DD", help="指定日のみ取得")
    grp.add_argument("--start",          metavar="YYYY-MM-DD", help="--end と組み合わせて範囲取得")
    grp.add_argument("--backfill-years", type=int, metavar="N", help="直近 N 年分の営業日を一括取得")
    p.add_argument("--end",   metavar="YYYY-MM-DD", help="範囲取得の終了日（省略時=今日）")
    p.add_argument("--force", action="store_true",  help="取得済み日付も再取得")
    args = p.parse_args()

    today = date.today()

    if args.date:
        dates = [date.fromisoformat(args.date)]
    elif args.start:
        start = date.fromisoformat(args.start)
        end   = date.fromisoformat(args.end) if args.end else today
        dates = _business_days(start, end)
    elif args.backfill_years:
        start = today.replace(year=today.year - args.backfill_years)
        dates = _business_days(start, today)
    else:
        dates = [today]

    print(f"[fetch_edinet_index] {len(dates)} 日分を処理")
    run(dates, force=args.force)


if __name__ == "__main__":
    main()
