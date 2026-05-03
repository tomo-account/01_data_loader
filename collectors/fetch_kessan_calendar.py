"""
決算短信カレンダー取得 — J-Quants API v2

[history モード]  /v2/fins/summary を日付ごとに取得
  保存先: data/news/kessan/{date}.csv（日別・TDnet CSV 互換形式）
  列: date, time, code, company, title, pdf_url

[future モード]   /v2/equities/earnings-calendar で翌営業日の発表予定を取得
  保存先: data/news/kessan_schedule/latest.csv（毎回上書き）
  列: date, time, code, company, title, fetched_at
  ※ 3月・9月決算企業のみ対象
"""
import os
import sys
import datetime
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
import pandas as pd
from dotenv import load_dotenv

from config.paths import NEWS_KESSAN, NEWS_KESSAN_SCHEDULE

load_dotenv()

API_BASE  = "https://api.jquants.com/v2"
SLEEP_SEC = 1.0

_PERIOD_JP = {"FY": "通期", "1Q": "第1四半期", "2Q": "第2四半期", "3Q": "第3四半期", "4Q": "第4四半期"}


def _cur_per_to_title(cur_per_type: str) -> str:
    period = _PERIOD_JP.get(cur_per_type, cur_per_type)
    return f"決算短信（{period}）" if period else "決算短信"


# ── 認証 ──────────────────────────────────────────────────────────

def _auth() -> str:
    """API キーを返す（v2 は x-api-key ヘッダー認証）。"""
    print("=== J-Quants 認証 ===")
    api_key = os.environ.get("JQUANTS_API_KEY", "").strip()
    if not api_key:
        api_key = os.environ.get("JQUANTS_REFRESH_TOKEN", "").strip()
    if not api_key:
        print("ERROR: .env に JQUANTS_API_KEY を設定してください。")
        sys.exit(1)
    print("  認証OK（API キー使用）")
    return api_key


def _headers(api_key: str) -> dict:
    return {"x-api-key": api_key}


# ── history モード ────────────────────────────────────────────────

def _fetch_summary_date(api_key: str, date_str: str) -> list[dict]:
    headers, records, params = _headers(api_key), [], {"date": date_str}
    while True:
        resp = requests.get(f"{API_BASE}/fins/summary", headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("data", []))
        pk = data.get("pagination_key")
        if not pk:
            break
        params = {"date": date_str, "pagination_key": pk}
        time.sleep(SLEEP_SEC)
    return records


def _to_tdnet_rows(records: list[dict], date_str: str) -> list[dict]:
    rows = []
    for r in records:
        code = str(r.get("Code", "")).strip()
        if len(code) == 5 and code.endswith("0"):
            code = code[:4]
        rows.append({
            "date":    r.get("DiscDate", date_str),
            "time":    r.get("DiscTime", ""),
            "code":    code,
            "company": "",
            "title":   _cur_per_to_title(r.get("CurPerType", "")),
            "pdf_url": "",
        })
    return rows


def _save_history(rows: list[dict], date_str: str) -> None:
    NEWS_KESSAN.mkdir(parents=True, exist_ok=True)
    out = NEWS_KESSAN / f"{date_str}.csv"
    df = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["date", "time", "code", "company", "title", "pdf_url"])
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"  保存: {out.name} ({len(df)} 件)")


def _business_days(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += datetime.timedelta(days=1)
    return days


def run_history(api_key: str, start_date: str, end_date: str, force: bool) -> None:
    sd, ed = datetime.date.fromisoformat(start_date), datetime.date.fromisoformat(end_date)
    days   = _business_days(sd, ed)
    print(f"\n=== 決算短信カレンダー取得（{start_date} 〜 {end_date}、{len(days)} 営業日）===")
    for i, d in enumerate(days, 1):
        d_str = d.isoformat()
        if (NEWS_KESSAN / f"{d_str}.csv").exists() and not force:
            print(f"  [SKIP] {d_str}")
            continue
        print(f"  [{i}/{len(days)}] {d_str} ...", end="", flush=True)
        for attempt in range(4):
            try:
                records = _fetch_summary_date(api_key, d_str)
                print(f" {len(records)} 件取得")
                _save_history(_to_tdnet_rows(records, d_str), d_str)
                break
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    wait = 10 * (attempt + 1)
                    print(f" 429 (待機 {wait}s)", end="", flush=True)
                    time.sleep(wait)
                else:
                    print(f" ERR: {e}")
                    break
            except Exception as e:
                print(f" ERR: {e}")
                break
        time.sleep(SLEEP_SEC)
    print("\n決算短信カレンダー取得 完了")


# ── future モード ─────────────────────────────────────────────────

def _fetch_earnings_calendar(api_key: str) -> list[dict]:
    """翌営業日の決算発表予定を取得（3月・9月決算企業のみ）。"""
    headers, records, params = _headers(api_key), [], {}
    while True:
        resp = requests.get(
            f"{API_BASE}/equities/earnings-calendar", headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("data", []))
        pk = data.get("pagination_key")
        if not pk:
            break
        params = {"pagination_key": pk}
        time.sleep(SLEEP_SEC)
    return records


def _announcement_to_rows(records: list[dict]) -> list[dict]:
    rows = []
    for r in records:
        code = str(r.get("Code", "")).strip()
        if len(code) == 5 and code.endswith("0"):
            code = code[:4]
        fq    = r.get("FQ", "")
        title = f"決算短信（{_PERIOD_JP.get(fq, fq)}）" if fq else "決算短信"
        rows.append({
            "date":       r.get("Date", ""),
            "time":       "",
            "code":       code,
            "company":    r.get("CoName", ""),
            "title":      title,
            "fetched_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        })
    return rows


def _save_schedule(rows: list[dict]) -> None:
    NEWS_KESSAN_SCHEDULE.mkdir(parents=True, exist_ok=True)
    out = NEWS_KESSAN_SCHEDULE / "latest.csv"
    df  = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["date", "time", "code", "company", "title", "fetched_at"])
    df  = df.sort_values(["date", "time"]).reset_index(drop=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"  保存: {out} ({len(df)} 件)")


def run_future(api_key: str) -> None:
    print("\n=== 決算発表予定取得（翌営業日スケジュール・3月9月決算のみ）===")
    records = _fetch_earnings_calendar(api_key)
    print(f"  {len(records)} 件取得")
    rows = _announcement_to_rows(records)
    _save_schedule(rows)
    print("決算発表予定取得 完了")


# ── エントリーポイント ─────────────────────────────────────────────

def main(mode: str = "history", start_date: str | None = None,
         end_date: str | None = None, force: bool = False) -> None:
    api_key = _auth()
    if mode == "future":
        run_future(api_key)
    else:
        if not start_date or not end_date:
            print("ERROR: history モードは --start-date と --end-date が必要です。")
            sys.exit(1)
        run_history(api_key, start_date, end_date, force)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="J-Quants 決算短信カレンダー取得 (API v2)")
    parser.add_argument("--mode", default="history", choices=["history", "future"],
                        help="history: 過去実績（日別CSV）/ future: 翌営業日予定（latest.csv）")
    parser.add_argument("--start-date", default=None, help="[history] 取得開始日 YYYY-MM-DD")
    parser.add_argument("--end-date",   default=None, help="[history] 取得終了日 YYYY-MM-DD")
    parser.add_argument("--force", action="store_true", help="[history] 既存 CSV も上書き")
    args = parser.parse_args()
    main(args.mode, args.start_date, args.end_date, args.force)
