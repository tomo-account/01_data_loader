"""
決算短信 XBRL ZIP ダウンロード & JSON 変換

TDnet 適時開示 CSV から「決算短信」を検出し、対象銘柄の XBRL ZIP を
ダウンロードして xbrl_to_json.convert() で JSON に変換する。

使い方:
    python collectors/fetch_statements.py                # 直近営業日
    python collectors/fetch_statements.py --date 2026-04-30

保存先:
    ZIP : data/statements_zip/{code}/{doc_id}.zip  (存在すれば再ダウンロードしない)
    JSON: data/statements/                          (常に上書き)
"""
import argparse
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.paths import STATEMENTS, STATEMENTS_ZIP
from utils.data_loader import load_price_targets, load_tdnet
from utils.date_utils import latest_business_day

TDNET_BASE   = "https://www.release.tdnet.info/inbs/"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}
SLEEP_SEC    = 1.0
XBRL_IN_DIR  = STATEMENTS_ZIP

from collectors.xbrl_to_json import convert as xbrl_convert  # noqa: E402


def pdf_url_to_xbrl_url(pdf_url: str) -> str | None:
    """
    TDnet PDF URL → XBRL ZIP URL に変換する。
    PDF:  https://www.release.tdnet.info/inbs/140120260430NNNNNN.pdf
    XBRL: https://www.release.tdnet.info/inbs/081220260430NNNNNN.zip
    先頭4桁が "1401" でない場合は None を返す（添付書類のみ等）。
    """
    filename = pdf_url.rsplit("/", 1)[-1]  # "140120260430NNNNNN.pdf"
    if not re.fullmatch(r"1[0-9]{3}\d+\.pdf", filename):
        return None
    if not filename.startswith("1401"):
        return None
    doc_id = filename[4:-4]  # "YYYYMMDDNNNNNN"
    return TDNET_BASE + "0812" + doc_id + ".zip"


def download_zip(url: str, dest: Path) -> bool:
    """ZIP をダウンロードして dest に保存。成功したら True。"""
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=30, stream=True)
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"    [ERR] ダウンロード失敗: {e}")
        if dest.exists():
            dest.unlink()
        return False


def process_row(code: str, title: str, pdf_url: str) -> None:
    """1件の開示を処理（ZIP DL → JSON 変換）。"""
    xbrl_url = pdf_url_to_xbrl_url(pdf_url)
    if xbrl_url is None:
        print(f"  [SKIP] {code} {title[:30]} — URL パターン不一致")
        return

    doc_id   = xbrl_url.rsplit("/", 1)[-1][4:-4]  # "YYYYMMDDNNNNNN"
    zip_path = XBRL_IN_DIR / code / f"{doc_id}.zip"

    if zip_path.exists():
        print(f"  [SKIP] {code} ZIP 既存: {zip_path.name}")
    else:
        print(f"  [DL]   {code} {title[:40]}")
        print(f"         {xbrl_url}")
        ok = download_zip(xbrl_url, zip_path)
        if not ok:
            return
        time.sleep(SLEEP_SEC)

    # JSON 変換（常に上書き）
    try:
        STATEMENTS.mkdir(parents=True, exist_ok=True)
        out_path = xbrl_convert(zip_path, out_dir=STATEMENTS)
        print(f"  [OK]   JSON: {out_path.name}")
    except Exception as e:
        print(f"  [ERR] 変換失敗 {zip_path.name}: {e}")


def _process_date(date_str: str, target_codes: set[str]) -> None:
    """1日分の TDnet CSV を読んで決算短信 XBRL を処理する。"""
    print(f"\n--- {date_str} ---")
    try:
        tdnet = load_tdnet(date_str)
    except FileNotFoundError:
        print(f"  [SKIP] TDnet CSV なし: tdnet_{date_str}.csv")
        return

    if tdnet.empty:
        print("  開示データなし（休場日またはデータ 0 件）")
        return

    tdnet["code"] = tdnet["code"].astype(str).str[:4]
    mask = (
        tdnet["title"].str.contains("決算短信", na=False)
        & tdnet["code"].isin(target_codes)
    )
    targets = tdnet[mask].copy()
    print(f"  決算短信ヒット: {len(targets)} 件（全開示 {len(tdnet)} 件中）")

    if targets.empty:
        return

    for _, row in targets.iterrows():
        process_row(
            code    = str(row["code"]),
            title   = str(row["title"]),
            pdf_url = str(row["pdf_url"]),
        )


def _business_days_in_range(start_str: str, end_str: str) -> list[str]:
    import datetime
    sd = datetime.date.fromisoformat(start_str)
    ed = datetime.date.fromisoformat(end_str)
    days = []
    cur = sd
    while cur <= ed:
        if cur.weekday() < 5:
            days.append(cur.isoformat())
        cur += datetime.timedelta(days=1)
    return days


def main() -> None:
    parser = argparse.ArgumentParser(description="決算短信 XBRL ZIP 取得 & 変換")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD（省略時: 直近営業日）")
    parser.add_argument("--start-date", default=None, help="範囲取得の開始日 YYYY-MM-DD")
    parser.add_argument("--end-date",   default=None, help="範囲取得の終了日 YYYY-MM-DD")
    args = parser.parse_args()

    target_codes: set[str] = set(load_price_targets()["コード"].astype(str))
    print(f"対象銘柄: {len(target_codes)} 件")

    if args.start_date and args.end_date:
        days = _business_days_in_range(args.start_date, args.end_date)
        print(f"範囲取得: {args.start_date} 〜 {args.end_date}（{len(days)} 営業日）")
        for d in days:
            _process_date(d, target_codes)
    else:
        date_str = args.date or latest_business_day().isoformat()
        _process_date(date_str, target_codes)

    print("\n完了。")


if __name__ == "__main__":
    main()
