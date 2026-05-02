"""
EDINET DB 財務データ取得

データソース: EDINET DB (https://edinetdb.jp/v1)
              XBRL 解析済み JSON で EPS/BPS/ROE/PER/PBR 等を取得できる第三者サービス

取得対象  : watch_personal + watch_market のユニオン（デフォルト）
保存先   : data/financials/<code>.csv  ← 銘柄ごと1ファイル
キャッシュ: data/financials/_edinet_map.json（証券コード→EDINETコード）
レート制限: 100 calls/day（無料プラン）

実行方法:
    python collectors/fetch_financials.py
    python collectors/fetch_financials.py --target watch_market
    python collectors/fetch_financials.py --target price_targets --force
    python collectors/fetch_financials.py --rebuild-map
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
from dotenv import load_dotenv

from config.paths import FINANCIALS, WATCH_PERSONAL, WATCH_MARKET, WATCH_MARKET_SELECT, PRICE_TARGETS
from utils.data_loader import (
    load_watch_personal,
    load_watch_market,
    load_watch_market_select,
    load_price_targets,
)

load_dotenv()

# ── API 設定 ──────────────────────────────────────────────────────
API_BASE        = "https://edinetdb.jp/v1"
SEARCH_URL      = f"{API_BASE}/search"
COMPANY_URL     = f"{API_BASE}/companies"
HTTP_TIMEOUT    = 15
SLEEP_SEC       = 0.6   # API への礼節（1秒未満）
EDINET_MAP_FILE = FINANCIALS / "_edinet_map.json"
SKIP_DAYS       = 7     # この日数以内に取得済みならスキップ


# ── ユーティリティ ────────────────────────────────────────────────

def get_api_key() -> str:
    key = os.environ.get("EDINETDB_API_KEY", "").strip()
    if not key:
        print("[ERROR] 環境変数 EDINETDB_API_KEY が設定されていません。")
        print("        .env ファイルに EDINETDB_API_KEY=edb_xxx を追加してください。")
        sys.exit(1)
    return key


def load_target(target: str) -> pd.DataFrame:
    """--target に応じた銘柄リストを返す"""
    if target == "watch_market":
        return load_watch_market()
    if target == "watch_market_select":
        return load_watch_market_select()
    if target == "watch_personal":
        return load_watch_personal()
    if target == "price_targets":
        return load_price_targets()
    # デフォルト: watch_personal + watch_market のユニオン
    df_a = load_watch_personal()
    df_b = load_watch_market()
    df = pd.concat([df_a, df_b]).drop_duplicates(subset=["コード"]).reset_index(drop=True)
    return df


def load_edinet_map() -> dict[str, str]:
    """キャッシュ（証券コード → EDINETコード）を読み込む"""
    if EDINET_MAP_FILE.exists():
        try:
            return json.loads(EDINET_MAP_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_edinet_map(mapping: dict[str, str]) -> None:
    FINANCIALS.mkdir(parents=True, exist_ok=True)
    EDINET_MAP_FILE.write_text(
        json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def is_recent(code: str) -> bool:
    """SKIP_DAYS 以内に取得済みなら True"""
    path = FINANCIALS / f"{code}.csv"
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=SKIP_DAYS)


# ── EDINET DB API ─────────────────────────────────────────────────

def search_edinet_code(sec_code: str) -> str | None:
    """証券コード4桁 → EDINETコード（認証不要）"""
    try:
        resp = requests.get(SEARCH_URL, params={"q": sec_code}, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [ERR] search({sec_code}): {e}")
        return None

    candidates = data if isinstance(data, list) else data.get("results", data.get("data", []))
    if not isinstance(candidates, list):
        return None

    for item in candidates:
        if not isinstance(item, dict):
            continue
        item_sec = str(item.get("sec_code", "")).strip()
        if item_sec == sec_code or item_sec == sec_code + "0":
            return item.get("edinet_code")

    return candidates[0].get("edinet_code") if candidates and isinstance(candidates[0], dict) else None


def fetch_company(edinet_code: str, api_key: str) -> dict | None:
    """EDINETコード → 財務データ JSON（認証必要）"""
    try:
        resp = requests.get(
            f"{COMPANY_URL}/{edinet_code}",
            headers={"X-API-Key": api_key},
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code == 401:
            print("  [ERR] 401 Unauthorized: EDINETDB_API_KEY が無効です。")
            return None
        if resp.status_code == 429:
            print("  [ERR] 429 Too Many Requests: 100 calls/day の制限に到達しました。")
            return None
        resp.raise_for_status()
        body = resp.json()
        return body.get("data") if isinstance(body, dict) else None
    except Exception as e:
        print(f"  [ERR] company({edinet_code}): {e}")
        return None


# ── パース ────────────────────────────────────────────────────────

def parse_company(company: dict, code: str, name: str, ticker: str) -> dict:
    """
    API レスポンスから保存用 dict を組み立てる。
    分割調整済みフィールド (adjusted_*) を優先することで、
    株式分割があった銘柄でも EPS/BPS/DPS の単位を一貫させる
    （例: 1939 四電工 2024-10 1:3 分割で DPS が分割前ベースになる問題への対処）。
    """
    fin  = company.get("latest_financials") or {}
    earn = company.get("latest_earnings")   or {}

    # 分割調整済を優先
    eps = fin.get("adjusted_eps") or fin.get("eps")
    bps = fin.get("adjusted_bps") or fin.get("bps")
    dps = fin.get("adjusted_dividend_per_share") or fin.get("dividend_per_share")

    roe_raw = fin.get("roe_official")
    roe = round(float(roe_raw) * 100, 2) if isinstance(roe_raw, (int, float)) else None

    per_api = fin.get("per")
    per = round(float(per_api), 2) if per_api is not None else None

    net_income = fin.get("net_income")
    net_assets = fin.get("net_assets")
    if roe is None and net_income and net_assets and net_assets > 0:
        roe = round(net_income / net_assets * 100, 2)

    return {
        "code":          code,
        "name":          name,
        "ticker":        ticker,
        "edinet_code":   company.get("edinet_code", ""),
        "industry":      company.get("industry", ""),
        "fiscal_year":   fin.get("fiscal_year", ""),
        "submit_date":   fin.get("submit_date", ""),
        "fetched_at":    datetime.now().strftime("%Y-%m-%d"),
        "eps":           eps,
        "bps":           bps,
        "PER":           per,
        "PBR":           None,   # 株価が必要なため analysis/notable.py で 株価÷BPS 算出
        "ROE":           roe,
        "net_income":    net_income,
        "net_assets":    net_assets,
        "shares_issued": fin.get("shares_issued"),
        "dps":           dps,
        # 直近四半期速報
        "q_eps":         earn.get("eps"),
        "q_net_income":  earn.get("net_income"),
        "q_quarter":     earn.get("quarter"),
        "q_disclosure":  earn.get("disclosure_date"),
    }


def save_financials(code: str, row: dict) -> None:
    FINANCIALS.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([row])
    df.to_csv(FINANCIALS / f"{code}.csv", index=False, encoding="utf-8-sig")


# ── メイン ────────────────────────────────────────────────────────

def main(target: str = "default", force: bool = False, rebuild_map: bool = False) -> None:
    print("=" * 60)
    print("EDINET DB 財務データ取得")
    print(f"対象: {target}  force={force}  rebuild_map={rebuild_map}")
    print(f"実行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    api_key    = get_api_key()
    targets    = load_target(target)
    edinet_map = {} if rebuild_map else load_edinet_map()

    print(f"対象銘柄: {len(targets)}銘柄  キャッシュ: {len(edinet_map)}件\n")

    ok_count   = 0
    skip_count = 0
    err_count  = 0
    api_calls  = 0

    for _, row in targets.iterrows():
        code   = str(row["コード"])
        name   = str(row["銘柄"])
        ticker = str(row["ティッカーコード"])

        # .T 以外（米国株等）はスキップ
        if not ticker.upper().endswith(".T"):
            print(f"  [SKIP] {code} {name} -.T 以外")
            skip_count += 1
            continue

        # 取得済みスキップ
        if not force and is_recent(code):
            print(f"  [SKIP] {code} {name} -{SKIP_DAYS}日以内に取得済み")
            skip_count += 1
            continue

        sec_code = code.zfill(4)

        # ── EDINET コード解決 ──────────────────────────────
        edinet_code = edinet_map.get(sec_code)
        if not edinet_code or rebuild_map:
            print(f"  [SEARCH] {code} {name}...", end="", flush=True)
            edinet_code = search_edinet_code(sec_code)
            api_calls += 1
            time.sleep(SLEEP_SEC)
            if not edinet_code:
                print(" not found")
                err_count += 1
                continue
            print(f" -> {edinet_code}")
            edinet_map[sec_code] = edinet_code

        # ── 財務取得 ──────────────────────────────────────
        print(f"  [FETCH]  {code} {name} [{edinet_code}]...", end="", flush=True)
        company = fetch_company(edinet_code, api_key)
        api_calls += 1
        time.sleep(SLEEP_SEC)

        if not company:
            print(" ERROR")
            err_count += 1
            # 429 なら即終了
            if api_calls >= 98:
                print("\n[WARN] API 呼出数が上限に近いため終了します。")
                break
            continue

        record = parse_company(company, code, name, ticker)
        save_financials(code, record)
        print(f" OK  PER={record['PER']}  ROE={record['ROE']}%  EPS={record['eps']}")
        ok_count += 1

    # キャッシュ保存
    save_edinet_map(edinet_map)

    print("\n" + "=" * 60)
    print(f"完了: OK={ok_count}  SKIP={skip_count}  ERR={err_count}  API呼出={api_calls}/100")
    print(f"キャッシュ保存: {EDINET_MAP_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EDINET DB 財務データ取得")
    parser.add_argument(
        "--target",
        default="default",
        choices=["default", "watch_market", "watch_market_select", "watch_personal", "price_targets"],
        help="取得対象リスト（default=watch_personal+watch_market のユニオン）",
    )
    parser.add_argument("--force",       action="store_true", help="取得済みファイルも再取得")
    parser.add_argument("--rebuild-map", action="store_true", help="EDINETコードキャッシュを再構築")
    args = parser.parse_args()
    main(args.target, args.force, args.rebuild_map)
