"""
a01_stock_filtering.py
============================
【銘柄フィルタリング】TOPIXリストからデイトレ適性銘柄を抽出する

■ 概要
  _topix_list.xlsx（東証上場銘柄一覧）を読み込み、
  売買代金・ボラティリティ・株価でフィルタリングして
  デイトレ向き銘柄リストを _stock_list.xlsx に保存します。

  このファイルは a03_yfinance_init.py / a03_yfinance_update.py で
  5分足・1時間足の取得対象銘柄として使用されます。

■ 実行順序
  1. a01_stock_filtering.py  ← このファイル（初回のみ）
  2. a03_yfinance_init.py    （初回のみ）
  3. a03_yfinance_update.py  （定期実行）

■ フィルタリング条件（必要に応じて変更してください）
  - 直近株価 500円以上
  - ボラティリティ（日中値幅率の20日平均）1.0%以上
  - 上記を満たした銘柄を売買代金の多い順に並べ、上位720銘柄を抽出

■ 入力ファイル
  _topix_list.xlsx : 東証上場銘柄一覧（JPXサイトからダウンロード）
  https://www.jpx.co.jp/markets/statistics-equities/misc/01.html

■ 出力ファイル
  _stock_list.xlsx : フィルタリング後の銘柄リスト

■ 注意事項
  - yfinanceは非公式APIです。CHUNK_SIZE と SLEEP_TIME を調整して
    サーバーへの負荷を抑えてください。
  - 実行には数分〜十数分かかります（銘柄数による）。

■ 実行方法
  python a01_stock_filtering.py
"""

import pandas as pd
import yfinance as yf
import time
from datetime import datetime

# ================================================
# 設定
# ================================================
INPUT_FILE   = "_topix_list.xlsx"    # JPXからダウンロードした東証上場銘柄一覧、「コード」に銘柄コード、「銘柄」に銘柄名を格納
OUTPUT_FILE  = "_stock_list.xlsx"
TARGET_COUNT = 720                   # 抽出する銘柄数
CHUNK_SIZE   = 50                    # yfinanceで一度に取得する銘柄数
SLEEP_TIME   = 1.0                   # チャンク間の待機秒数（サーバー負荷軽減）

# フィルタリング条件
MIN_PRICE      = 500    # 最低株価（円）：低位株を除外
MIN_VOLATILITY = 1.0    # 最低ボラティリティ（%）：値動きの小さい銘柄を除外


# ================================================
# ヘルパー関数
# ================================================

def get_ticker_symbol(code) -> str:
    """
    銘柄コードをyfinance形式（末尾に.T）に変換する。
    数字のみ（1234）、英字混じり（123A）、Excel誤差（1234.0）のすべてに対応。

    Examples:
        7203   → "7203.T"
        7203.0 → "7203.T"  （Excelが付けた余分な.0を除去）
        7203.T → "7203.T"  （すでに.T付きの場合はそのまま）
    """
    s = str(code).strip()
    if s.endswith('.0') and s[:-2].isdigit():
        s = s[:-2]
    if s.endswith('.T'):
        return s
    return f"{s}.T"


def extract_ticker_df(data: pd.DataFrame, ticker: str, chunk: list) -> pd.DataFrame:
    """
    yf.downloadの結果から特定銘柄のデータを切り出す。

    yfinanceのバージョンによってMultiIndexの構造（level0とlevel1の順序）が
    異なる場合があるため、get_level_values()で動的に判定する。

    Args:
        data   : yf.downloadの戻り値（複数銘柄のMultiIndex DataFrame）
        ticker : 切り出したい銘柄のTickerシンボル
        chunk  : 今回のチャンクの銘柄リスト（単一銘柄判定に使用）

    Returns:
        銘柄単体のDataFrame。取得できない場合は空のDataFrameを返す。
    """
    if data is None or data.empty:
        return pd.DataFrame()

    # 単一銘柄の場合はMultiIndexにならない
    if len(chunk) == 1:
        return data.copy()

    if not isinstance(data.columns, pd.MultiIndex):
        return pd.DataFrame()

    lv0 = data.columns.get_level_values(0).unique().tolist()
    lv1 = data.columns.get_level_values(1).unique().tolist()

    # level0 = Ticker, level1 = フィールド名（Close, High...）の形式
    if ticker in lv0:
        df = data[ticker].copy()
    # level0 = フィールド名, level1 = Ticker の形式（バージョンによって逆転する）
    elif ticker in lv1:
        df = data.xs(ticker, axis=1, level=1).copy()
    else:
        return pd.DataFrame()

    return df.dropna(how='all')


# ================================================
# メイン処理
# ================================================

def main():
    print("=" * 70)
    print(f"  銘柄フィルタリング スクリプト")
    print(f"  実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # 1. Excelから全銘柄を読み込む
    try:
        df_input   = pd.read_excel(INPUT_FILE, engine='openpyxl')
        raw_codes  = df_input['コード'].dropna().unique()
    except Exception as e:
        print(f"❌ ファイル読み込みエラー: {e}")
        return

    all_tickers = [get_ticker_symbol(c) for c in raw_codes]
    print(f"📋 取得対象: {len(all_tickers)} 銘柄")
    print(f"🔍 フィルタ条件: 株価 {MIN_PRICE}円以上 / "
          f"ボラティリティ {MIN_VOLATILITY}%以上 / 上位{TARGET_COUNT}銘柄")

    results = []

    # 2. チャンク単位でデータ取得・指標計算
    total_chunks = (len(all_tickers) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for chunk_idx, i in enumerate(range(0, len(all_tickers), CHUNK_SIZE)):
        chunk = all_tickers[i : i + CHUNK_SIZE]
        print(f"\n  チャンク {chunk_idx + 1}/{total_chunks}（{i+1}〜{min(i+CHUNK_SIZE, len(all_tickers))}銘柄目）取得中...")

        try:
            # 直近20営業日の日足データを一括取得
            data = yf.download(
                chunk,
                period="20d",
                interval="1d",
                group_by='ticker',
                progress=False,
                auto_adjust=True,
            )

            for ticker in chunk:
                df_stock = extract_ticker_df(data, ticker, chunk)
                if df_stock.empty or len(df_stock) < 5:
                    continue

                # 必要列の存在確認
                if not all(c in df_stock.columns for c in ['Close', 'High', 'Low', 'Volume']):
                    continue

                df_stock = df_stock.dropna(subset=['Close', 'High', 'Low', 'Volume'])
                if df_stock.empty:
                    continue

                # 売買代金（億円）= 終値 × 出来高 ÷ 1億
                avg_value = ((df_stock['Close'] * df_stock['Volume']) / 1_0000_0000).mean()

                # ボラティリティ（日中値幅率の平均）
                avg_vol = ((df_stock['High'] - df_stock['Low']) / df_stock['Close']).mean() * 100

                # 最新終値（低位株除外用）
                last_price = df_stock['Close'].iloc[-1]

                results.append({
                    "コード"              : ticker.replace(".T", ""),
                    "銘柄コード_yf"       : ticker,
                    "直近株価"            : round(float(last_price), 1),
                    "売買代金_20日平均(億円)": round(float(avg_value), 2),
                    "ボラティリティ_avg(%)": round(float(avg_vol), 2),
                })

            time.sleep(SLEEP_TIME)

        except Exception as e:
            print(f"  ⚠️  チャンクエラー（スキップ）: {e}")
            continue

    if not results:
        print("❌ データを取得できませんでした。")
        return

    # 3. フィルタリングと並び替え
    df_res = pd.DataFrame(results)

    df_filtered = df_res[
        (df_res['直近株価']             >= MIN_PRICE) &
        (df_res['ボラティリティ_avg(%)'] >= MIN_VOLATILITY)
    ].copy()

    df_top = (
        df_filtered
        .sort_values(by="売買代金_20日平均(億円)", ascending=False)
        .head(TARGET_COUNT)
        .reset_index(drop=True)
    )

    # 4. 保存
    df_top.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')

    # 5. 結果レポート
    print("\n" + "=" * 70)
    print(f"📊 実行レポート ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print("=" * 70)
    print(f"  取得試行  : {len(all_tickers)} 銘柄")
    print(f"  取得成功  : {len(df_res)} 銘柄")
    print(f"  フィルタ後: {len(df_filtered)} 銘柄")
    print(f"  出力銘柄数: {len(df_top)} 銘柄 → {OUTPUT_FILE}")
    print("=" * 70)
    print(f"\n✅ 完了。次のステップ: a03_yfinance_init.py を実行してください。")


if __name__ == "__main__":
    main()