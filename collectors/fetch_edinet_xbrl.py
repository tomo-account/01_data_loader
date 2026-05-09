"""
金融庁 EDINET API v2 — 有報 XBRL ZIP ダウンロード & JSON 変換

事前条件:
    fetch_edinet_index.py で edinet_index.parquet が作成済みであること。

対象銘柄:
    デフォルトは Phase 1 対象 3 セクター（石油元売・総合商社・資源）の CSV から証券コードを取得。
    --codes オプションで個別指定も可能。

保存先:
    data/yuho_zip/{edinet_code}/{doc_id}.zip  … 生 ZIP
    data/yuho/{edinet_code}/{doc_id}.json     … JSON 変換済み（XBRL → JSON）

実行方法:
    python collectors/fetch_edinet_xbrl.py                    # Phase 1 対象 3 セクター全銘柄
    python collectors/fetch_edinet_xbrl.py --codes 5020 1605  # 指定銘柄のみ
    python collectors/fetch_edinet_xbrl.py --force            # 既存 ZIP も再ダウンロード
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import time
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
from dotenv import load_dotenv

from config.paths import EDINET_IDX, YUHO_ZIP, YUHO, SECTORS

load_dotenv()

API_BASE  = "https://disclosure.edinet-fsa.go.jp/api/v2/documents"
SLEEP_SEC = 1.0   # ZIP ダウンロードはサイズが大きいためやや長め


def _api_key() -> str:
    key = os.getenv("EDINET_API_KEY", "")
    if not key:
        sys.exit("[error] EDINET_API_KEY が .env に設定されていません。")
    return key


def _load_index(codes: set[str]) -> pd.DataFrame:
    if not EDINET_IDX.exists():
        sys.exit(
            "[error] edinet_index.parquet が見つかりません。\n"
            "        先に fetch_edinet_index.py を実行してください。"
        )
    df = pd.read_parquet(EDINET_IDX)
    # secCode は "5020" 形式（4桁）— インデックス側は "50200" の場合があるので両対応
    df["sec4"] = df["secCode"].astype(str).str[:4]
    return df[df["sec4"].isin(codes)].copy()


def download_zip(doc_id: str, key: str) -> bytes:
    url = f"{API_BASE}/{doc_id}"
    params = {
        "type": 5,                  # 5 = XBRL 書類一式 ZIP
        "Subscription-Key": key,
    }
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    return resp.content


def convert_to_json(zip_bytes: bytes, doc_id: str) -> dict | None:
    """ZIP 内の primary XBRL を読み取り、xbrl_to_json.py の converter で変換。"""
    try:
        from collectors.xbrl_to_json import convert_xbrl_bytes  # type: ignore
    except ImportError:
        # xbrl_to_json が未配置の場合は ZIP 保存のみ
        return None

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        # Primary instance document: PublicDoc 直下の *_InstanceDocument.xbrl を優先
        xbrl_files = [n for n in names if n.endswith(".xbrl") and "PublicDoc" in n]
        if not xbrl_files:
            xbrl_files = [n for n in names if n.endswith(".xbrl")]
        if not xbrl_files:
            return None
        xbrl_bytes = zf.read(xbrl_files[0])

    return convert_xbrl_bytes(xbrl_bytes, doc_id)


def run(codes: set[str], force: bool = False) -> None:
    key = _api_key()
    df = _load_index(codes)

    if df.empty:
        print("インデックスに対象銘柄の有報が見つかりません。")
        print("  fetch_edinet_index.py --backfill-years 3 を先に実行してください。")
        return

    print(f"[fetch_edinet_xbrl] 対象 {len(df)} 件（{df['sec4'].nunique()} 銘柄）")

    ok = err = skip = 0

    for _, row in df.iterrows():
        doc_id     = row["docID"]
        edinet_cd  = row["edinetCode"]
        period_end = str(row.get("periodEnd", ""))[:7]  # YYYY-MM
        label      = f"{row.get('filerName','')} [{edinet_cd}] {period_end}"

        zip_path  = YUHO_ZIP / edinet_cd / f"{doc_id}.zip"
        json_path = YUHO      / edinet_cd / f"{doc_id}.json"

        if zip_path.exists() and not force:
            skip += 1
            continue

        print(f"  DL {label} ... ", end="", flush=True)
        try:
            data = download_zip(doc_id, key)
        except requests.HTTPError as e:
            print(f"[HTTP {e.response.status_code}]")
            err += 1
            time.sleep(SLEEP_SEC)
            continue
        except Exception as e:
            print(f"[error] {e}")
            err += 1
            time.sleep(SLEEP_SEC)
            continue

        # ZIP 保存
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path.write_bytes(data)

        # JSON 変換（xbrl_to_json.py が存在する場合のみ）
        converted = convert_to_json(data, doc_id)
        if converted is not None:
            import json as _json
            json_path.parent.mkdir(parents=True, exist_ok=True)
            json_path.write_text(_json.dumps(converted, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"OK (zip+json, {len(data)//1024}KB)")
        else:
            print(f"OK (zip only, {len(data)//1024}KB)")

        ok += 1
        time.sleep(SLEEP_SEC)

    print(f"\n[done] ok={ok}  skip={skip}  err={err}")
    if ok:
        print(f"  ZIP → {YUHO_ZIP}")
        print(f"  JSON → {YUHO}")


def main() -> None:
    p = argparse.ArgumentParser(description="EDINET 有価証券報告書 XBRL ZIP ダウンロード")
    p.add_argument("--codes", nargs="+", metavar="CODE", help="対象証券コード（4桁）。省略時は sector_map 全銘柄")
    p.add_argument("--force", action="store_true", help="取得済み ZIP も再ダウンロード")
    args = p.parse_args()

    if args.codes:
        target_codes = set(args.codes)
    else:
        # デフォルト: Phase 1 対象 3 セクターの CSV から証券コードを収集
        phase1_sectors = ["trading_companies", "oil_refining", "resources"]
        target_codes: set[str] = set()
        for sector in phase1_sectors:
            csv_path = SECTORS / f"{sector}.csv"
            if csv_path.exists():
                df_s = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
                code_col = next(
                    (c for c in df_s.columns if c in ("コード", "code", "証券コード")),
                    None,
                )
                if code_col:
                    target_codes.update(df_s[code_col].str[:4].dropna().tolist())
        if not target_codes:
            sys.exit("[error] セクター CSV から証券コードを読み取れませんでした。--codes で直接指定してください。")

    run(target_codes, force=args.force)


if __name__ == "__main__":
    main()
