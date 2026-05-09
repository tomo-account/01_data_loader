"""
TDnet 適時開示取得

ソース : https://www.release.tdnet.info/inbs/
保存先 : data/news/tdnet/{date}.csv（日付ごと上書き）
列     : date, time, code, company, title, pdf_url

※ 開示 0 件（土日・祝日）でもヘッダのみの CSV を保存する（「未取得」と区別するため）

使い方:
    python collectors/fetch_tdnet.py                             # 直近営業日
    python collectors/fetch_tdnet.py --date 2026-04-30
    python collectors/fetch_tdnet.py --start-date 2026-03-01 --end-date 2026-04-30
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import datetime
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup

from config.paths import NEWS_TDNET
from utils.date_utils import latest_business_day

TDNET_BASE   = "https://www.release.tdnet.info/inbs/"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}
SLEEP_SEC    = 0.5


def fetch_tdnet_page(page_url: str) -> list[dict]:
    """TDnet 1ページ分の開示一覧を取得。"""
    resp = requests.get(TDNET_BASE + page_url, headers=HTTP_HEADERS, timeout=10)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    records = []
    for row in soup.select("table tr"):
        cols = row.find_all("td")
        if len(cols) < 4:
            continue
        t    = cols[0].get_text(strip=True)
        code = cols[1].get_text(strip=True)
        comp = cols[2].get_text(strip=True)
        titl = cols[3].get_text(strip=True)
        if not (len(t) == 5 and t[2] == ":"):
            continue
        a   = cols[3].find("a") or row.find("a")
        pdf = (TDNET_BASE + a["href"]) if a and a.get("href") else ""
        records.append({
            "time":    t,
            "code":    code,
            "company": comp,
            "title":   titl,
            "pdf_url": pdf,
        })
    return records


def fetch_tdnet_date(date_str: str) -> list[dict]:
    """
    指定日の TDnet 開示を全ページ取得。
    URL を直接構築するため、ドロップダウンに依存しない。
    """
    raw_date    = date_str.replace("-", "")
    all_records = []
    page = 1
    while True:
        page_url = f"I_list_{page:03d}_{raw_date}.html"
        print(f"  TDnet page {page}...", end="", flush=True)
        try:
            records = fetch_tdnet_page(page_url)
        except Exception as e:
            print(f" ERR: {e}")
            break
        if not records:
            print(" (終了)")
            break
        print(f" {len(records)}件")
        all_records.extend(records)
        page += 1
        time.sleep(SLEEP_SEC)
    return all_records


def save_tdnet(records: list[dict], date_str: str) -> None:
    """
    TDnet 開示を CSV 保存。
    records が空（土日・祝日）でもヘッダのみの CSV を保存し「未取得」と区別する。
    """
    NEWS_TDNET.mkdir(parents=True, exist_ok=True)
    out = NEWS_TDNET / f"{date_str}.csv"

    if records:
        df = pd.DataFrame(records)
        df.insert(0, "date", date_str)
    else:
        df = pd.DataFrame(columns=["date", "time", "code", "company", "title", "pdf_url"])

    df.to_csv(out, index=False, encoding="utf-8-sig")

    if records:
        print(f"  保存: {out.name} ({len(df)}件)")
    else:
        print(f"  保存: {out.name} (0件 ※土日・祝日のため開示なし)")


def _business_days_in_range(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    days, current = [], start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += datetime.timedelta(days=1)
    return days


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="TDnet 適時開示取得")
    parser.add_argument("--date",       default=None, help="取得日 YYYY-MM-DD（省略時=直近営業日）")
    parser.add_argument("--start-date", default=None, help="範囲取得の開始日 YYYY-MM-DD")
    parser.add_argument("--end-date",   default=None, help="範囲取得の終了日 YYYY-MM-DD")
    args = parser.parse_args()

    if args.start_date and args.end_date:
        sd   = datetime.date.fromisoformat(args.start_date)
        ed   = datetime.date.fromisoformat(args.end_date)
        days = _business_days_in_range(sd, ed)
        print(f"=== TDnet 一括取得（{args.start_date} 〜 {args.end_date}、{len(days)} 営業日）===")
        for d in days:
            d_str = d.isoformat()
            out   = NEWS_TDNET / f"{d_str}.csv"
            if out.exists():
                print(f"  [SKIP] {d_str} — CSV 既存")
                continue
            print(f"\n--- {d_str} ---")
            save_tdnet(fetch_tdnet_date(d_str), d_str)
    else:
        target = args.date or latest_business_day().isoformat()
        print(f"=== TDnet 適時開示取得（{target}）===")
        save_tdnet(fetch_tdnet_date(target), target)

    print("\nTDnet 取得 完了")


if __name__ == "__main__":
    main()
