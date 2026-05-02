"""
1 銘柄だけ EDINET DB から再取得して結果を表示する。
本番更新の前に修正したパースロジックの動作確認用。

使い方:
    python collectors/test_refetch_one.py 1939
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from collectors.fetch_financials import (
    get_api_key, search_edinet_code, fetch_company, parse_company,
    load_edinet_map, save_financials,
)

load_dotenv()


def main(code: str, save: bool = False) -> None:
    api_key = get_api_key()
    sec_code = code.zfill(4)

    cache = load_edinet_map()
    edinet_code = cache.get(sec_code)
    if not edinet_code:
        print(f"[search] {sec_code}...", end="", flush=True)
        edinet_code = search_edinet_code(sec_code)
        if not edinet_code:
            print(" not found")
            sys.exit(1)
        print(f" -> {edinet_code}")

    print(f"[fetch]  {sec_code} [{edinet_code}]")
    company = fetch_company(edinet_code, api_key)
    if not company:
        print("ERROR")
        sys.exit(1)

    fin  = company.get("latest_financials") or {}
    earn = company.get("latest_earnings")   or {}

    print("\n=== latest_financials 全フィールド ===")
    for k, v in fin.items():
        print(f"  {k:>40}: {v}")

    print("\n=== latest_earnings 全フィールド ===")
    for k, v in earn.items():
        print(f"  {k:>40}: {v}")

    print("\n=== その他トップレベルキー ===")
    for k in company:
        if k not in ("latest_financials", "latest_earnings"):
            print(f"  {k:>40}: {company[k]}")

    print("\n=== parse_company の結果 ===")
    record = parse_company(company, code, company.get("name", ""), f"{code}.T")
    for k, v in record.items():
        print(f"  {k:>16}: {v}")

    if save:
        save_financials(code, record)
        print(f"\n[saved] data/financials/{code}.csv に保存しました")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使い方: python collectors/test_refetch_one.py <コード> [--save]")
        sys.exit(1)
    code = sys.argv[1]
    save_flag = "--save" in sys.argv[2:]
    main(code, save=save_flag)
