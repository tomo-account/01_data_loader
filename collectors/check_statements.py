"""
check_statements.py — 取得済み決算短信 JSON の主要指標欠損チェック

使い方:
    python collectors/check_statements.py
    python collectors/check_statements.py --latest   # 銘柄ごとに最新ファイルのみ対象
"""
import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.paths import STATEMENTS

# チェックする主要指標（json_path → 表示ラベル）
CHECK_FIELDS = {
    "metadata.code":                                        "code",
    "metadata.filing_date":                                 "filing_date",
    "metadata.period_type":                                 "period_type",
    "performance.current.revenue":                          "revenue(cur)",
    "performance.current.operating_profit":                 "op_profit(cur)",
    "performance.current.profit_attributable_to_owners":    "net_income(cur)",
    "performance.forecast.revenue":                         "revenue(fcst)",
    "performance.forecast.operating_profit":                "op_profit(fcst)",
    "performance.forecast.profit_attributable_to_owners":   "net_income(fcst)",
    "performance.forecast.eps":                             "eps(fcst)",
    "dividend.forecast_current.annual":                     "dps(fcst)",
}


def _get(obj: dict, path: str):
    """ドットパスで nested dict から値を取得。"""
    cur = obj
    for key in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def check_file(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    missing = [label for field, label in CHECK_FIELDS.items()
               if _get(data, field) is None]
    return {
        "file":    path.name,
        "code":    _get(data, "metadata.code") or path.stem.split("_")[0],
        "date":    _get(data, "metadata.filing_date") or "",
        "period":  _get(data, "metadata.period_type") or "",
        "std":     _get(data, "metadata.accounting_standard") or "",
        "missing": missing,
        "total":   len(CHECK_FIELDS),
    }


def main():
    parser = argparse.ArgumentParser(description="決算短信 JSON 欠損チェック")
    parser.add_argument("--latest", action="store_true",
                        help="銘柄ごとに最新ファイルのみチェック")
    args = parser.parse_args()

    files = sorted(STATEMENTS.glob("*.json"))
    if not files:
        print("JSON ファイルが見つかりません。")
        return

    if args.latest:
        # 銘柄ごとに最新ファイルのみ
        latest: dict[str, Path] = {}
        for f in files:
            code = f.stem.split("_")[0]
            if code not in latest or f.stem > latest[code].stem:
                latest[code] = f
        files = sorted(latest.values())

    results = [check_file(f) for f in files]

    # 欠損なし / あり で分類
    ok      = [r for r in results if not r["missing"]]
    missing = [r for r in results if r["missing"]]

    print(f"\n=== 決算短信 JSON 欠損チェック ({len(results)} ファイル) ===\n")
    print(f"[OK]  全項目あり: {len(ok)} 件")
    print(f"[NG]  欠損あり  : {len(missing)} 件\n")

    if missing:
        print("─" * 72)
        print(f"{'ファイル':<35} {'基準':<6} {'欠損項目'}")
        print("─" * 72)
        for r in sorted(missing, key=lambda x: len(x["missing"]), reverse=True):
            labels = ", ".join(r["missing"])
            print(f"{r['file']:<35} {r['std']:<6} {labels}")

    # 指標別の欠損率サマリー
    print("\n─── 指標別 欠損率 ───")
    for field, label in CHECK_FIELDS.items():
        n_missing = sum(1 for r in results if label in r["missing"])
        pct = n_missing / len(results) * 100
        bar = "#" * int(pct / 5)
        print(f"  {label:<18} {n_missing:>3}/{len(results)}  {pct:5.1f}%  {bar}")


if __name__ == "__main__":
    main()
