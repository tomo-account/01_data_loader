import pandas as pd
import os

# --- 設定 ---
CSV_FILES = {
    "5分足":   "_5min.csv",
    "日足":    "_daily.csv",
}

PRICE_COLS  = ["Open", "High", "Low", "Close"]
VOLUME_COL  = "Volume"

def check_market_data():
    all_ok = True

    for label, filepath in CSV_FILES.items():
        if not os.path.exists(filepath):
            print(f"\n⚠️  [{label}] ファイルが見つかりません: {filepath}")
            continue

        df = pd.read_csv(filepath)
        total_rows  = len(df)
        total_cells = df.size

        print(f"\n{'='*70}")
        print(f"📂 [{label}] {filepath}  （総行数: {total_rows:,} 行 / 総セル数: {total_cells:,}）")
        print(f"{'='*70}")

        file_ok = True

        # ────────────────────────────────────────────
        # 1. 欠損値チェック
        # ────────────────────────────────────────────
        missing_total = df.isnull().sum().sum()
        print(f"\n  【欠損値チェック】")
        if missing_total == 0:
            print(f"  ✅ 欠損なし")
        else:
            file_ok = False
            print(f"  ❌ 欠損セル総数: {missing_total:,}")
            missing_by_col = df.isnull().sum()
            missing_by_col = missing_by_col[missing_by_col > 0]
            print(f"  列ごとの欠損数:\n{missing_by_col.to_string()}")

        # ────────────────────────────────────────────
        # 2. 重複チェック（Ticker × 時刻）
        # ────────────────────────────────────────────
        time_col = "Date" if "Date" in df.columns else "Datetime"
        print(f"\n  【重複チェック】（キー: Ticker × {time_col}）")

        if "Ticker" in df.columns and time_col in df.columns:
            dup_mask  = df.duplicated(subset=["Ticker", time_col], keep=False)
            dup_count = dup_mask.sum()

            if dup_count == 0:
                print(f"  ✅ 重複なし")
            else:
                file_ok = False
                print(f"  ❌ 重複行: {dup_count:,} 行")
                dup_by_ticker = (
                    df[dup_mask]
                    .groupby("Ticker")
                    .size()
                    .sort_values(ascending=False)
                )
                print(f"\n  【重複の多い銘柄（上位10件）】")
                print(dup_by_ticker.head(10).to_string())
                print(f"\n  【重複サンプル（先頭6行）】")
                print(
                    df[dup_mask]
                    .sort_values(["Ticker", time_col])
                    .head(6)
                    .to_string(index=False)
                )
        else:
            print(f"  ⚠️  スキップ（Ticker または {time_col} 列なし）")

        # ────────────────────────────────────────────
        # 3. 価格異常値チェック（0以下・NaN除く）
        # ────────────────────────────────────────────
        print(f"\n  【価格異常値チェック（<=0）】")
        price_issues = False
        for col in PRICE_COLS:
            if col in df.columns:
                bad = (df[col] <= 0).sum()
                if bad > 0:
                    file_ok = False
                    price_issues = True
                    print(f"  ❌ {col}: {bad:,} 件")
        if not price_issues:
            print(f"  ✅ 異常なし")

        # ────────────────────────────────────────────
        # 4. 出来高ゼロチェック
        # ────────────────────────────────────────────
        print(f"\n  【出来高ゼロチェック】")
        if VOLUME_COL in df.columns:
            zero_vol = (df[VOLUME_COL] == 0).sum()
            if zero_vol == 0:
                print(f"  ✅ 出来高ゼロなし")
            else:
                # 出来高ゼロは警告止まり（祝日・板薄銘柄で発生しうる）
                print(f"  ⚠️  出来高ゼロ: {zero_vol:,} 件（祝日・薄商い銘柄の可能性あり）")
        else:
            print(f"  ⚠️  {VOLUME_COL} 列なし")

        # ────────────────────────────────────────────
        # 5. High/Low 逆転チェック
        # ────────────────────────────────────────────
        print(f"\n  【High/Low 逆転チェック】")
        if "High" in df.columns and "Low" in df.columns:
            invert = (df["High"] < df["Low"]).sum()
            if invert == 0:
                print(f"  ✅ 逆転なし")
            else:
                file_ok = False
                print(f"  ❌ High < Low: {invert:,} 件")
        else:
            print(f"  ⚠️  High/Low 列なし")

        # ────────────────────────────────────────────
        # 6. 日時の連続性チェック（銘柄ごとのデータ範囲）
        # ────────────────────────────────────────────
        print(f"\n  【日時範囲サマリ（全銘柄）】")
        if time_col in df.columns and "Ticker" in df.columns:
            df[time_col] = pd.to_datetime(df[time_col], utc=False, errors='coerce')
            summary = (
                df.groupby("Ticker")[time_col]
                .agg(["min", "max", "count"])
                .rename(columns={"min": "最古", "max": "最新", "count": "行数"})
            )
            print(f"  銘柄数: {len(summary):,}")
            print(f"  全体の最古: {summary['最古'].min()}")
            print(f"  全体の最新: {summary['最新'].max()}")
            print(f"  銘柄あたり行数: min={summary['行数'].min():,}  max={summary['行数'].max():,}  mean={summary['行数'].mean():.0f}")

        # ファイル総合判定
        if file_ok:
            print(f"\n  ✅ [{label}] 問題なし")
        else:
            all_ok = False
            print(f"\n  ❌ [{label}] 上記の問題を確認してください")

    print(f"\n{'='*70}")
    if all_ok:
        print("✅ 全ファイル チェック完了 — 問題なし")
    else:
        print("❌ チェック完了 — 一部ファイルに問題あり（上記を確認）")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    check_market_data()
