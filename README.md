# 📈 yfinance 株価データ収集ツール

yfinance を使って日本株・先物・為替データを取得・管理するPythonスクリプト群です。  
TOPIX構成銘柄の財務指標収集から、5分足〜日足の時系列データの初回取得・定期更新・品質チェックまでをカバーします。

<br><br>

## 📁 ファイル構成

| ファイル | 役割 |
|---|---|
| `a01_yfinance_info_update.py` | TOPIX銘柄の財務指標（PER・PBR・配当など）をExcelに出力 |
| `a02_yfinance_init.py` | 5分足・日足データの**初回一括取得** |
| `a03_yfinance_update.py` | 5分足・日足データの**定期更新（継ぎ足し）＋Parquet変換** |
| `a04_yfinance_futures_multi.py` | 先物・為替データの取得（初回自動判定・定期更新・Parquet変換対応） |
| `a05_check_missing.py` | 取得済みCSVの品質チェック（欠損・重複・異常値） |

<br><br>

## 🗂️ 必要な入力ファイル

実行前に以下のExcelファイルを同じディレクトリに配置してください。

| ファイル名 | 用途 | 必須列 |
|---|---|---|
| `_stock_list.xlsx` | 5分足の取得対象銘柄 | `ティッカーコード` |
| `_topix_list.xlsx` | 日足・財務指標の取得対象銘柄 | `ティッカーコード` |

> ティッカーコードは yfinance 形式で記載してください（例: `7203.T`）。  
> 日経平均（`^N225`）はすべての時間足に自動で追加されます。

<br><br>

## 📦 セットアップ

- Python 3.9 以上

```bash
# 依存ライブラリ
pandas==2.3.3
yfinance==1.0
openpyxl==3.1.5
pyarrow          # Parquet変換に必要

# リポジトリをクローン
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>

# 依存ライブラリをインストール
pip install -r requirements.txt
```

<br><br>

## 🚀 使い方

### 1. 財務指標の取得（`a01`）

TOPIX銘柄のPER・PBR・配当利回りなどをまとめてExcelに出力します。

```bash
python a01_yfinance_info_update.py
```

**出力:** `stock_data_results.xlsx`

取得される項目: 年月日 / インダストリー / 時価総額（億円）/ 売買代金20日平均（億円）/ 1株配当 / 配当落ち日 / PER / PER（予想）/ PBR / 株価 / 配当利回り

<br><br>

### 2. 時系列データの初回取得（`a02`）

**初回のみ実行**してください。過去データをまとめて取得してCSVを新規作成します。

```bash
python a02_yfinance_init.py
```

**出力ファイルと取得期間:**

| ファイル | 時間足 | 取得期間 | 対象銘柄リスト |
|---|---|---|---|
| `_5min.csv` | 5分足 | 直近60日 | `_stock_list.xlsx` |
| `_daily.csv` | 日足 | 直近3年 | `_topix_list.xlsx` |

> ⚠️ yfinance の仕様上、5分足は直近60日分が上限です。

<br><br>

### 3. 時系列データの定期更新（`a03`）

**2回目以降**はこちらを実行してください。既存CSVに最新データを継ぎ足し、Parquetファイルも自動生成します。

```bash
python a03_yfinance_update.py
```

- 5分足：`_5min.csv` に最新データを継ぎ足し（重複除去あり）
- 日足：`_daily.csv` を直近3年分で上書き更新
- 実行前に5分足の自動バックアップを `backups/` ディレクトリに保存
- 更新完了後、`_5min.parquet` / `_daily.parquet` を自動生成

<br><br>

### 4. 先物・為替データの取得（`a04`）

日経先物・原油・金・ドル円の5分足・日足を取得します。CSVの有無により初回/更新を自動判定し、Parquetファイルも自動生成します。

```bash
python a04_yfinance_futures_multi.py
```

**取得銘柄:**

| ティッカー | 銘柄名 |
|---|---|
| `NIY=F` | 日経先物（円建て） |
| `CL=F` | 原油先物（WTI） |
| `GC=F` | 金先物 |
| `JPY=X` | ドル円 |

**出力ファイル:**

| ファイル | 時間足 | 初回取得期間 | 更新期間 |
|---|---|---|---|
| `_5min_futures.csv` | 5分足 | 直近60日 | 直近3日（継ぎ足し） |
| `_daily_futures.csv` | 日足 | 直近3年 | 直近3日（継ぎ足し） |
| `_5min_futures.parquet` | 5分足 | — | 自動生成 |
| `_daily_futures.parquet` | 日足 | — | 自動生成 |

銘柄の追加・削除は `FUTURES_TICKERS` 辞書を編集してください。

<br><br>

### 5. データ品質チェック（`a05`）

`_5min.csv` / `_daily.csv` の品質を確認します。

```bash
python a05_check_missing.py
```

**チェック項目:**

- 欠損値の有無・列ごとの件数
- 重複レコード（Ticker × 時刻）
- 価格異常値（0以下）
- 出来高ゼロ（警告のみ）
- High/Low 逆転
- 銘柄ごとのデータ範囲サマリ

<br><br>

## 📋 推奨実行フロー

```
初回セットアップ
    ↓
python a02_yfinance_init.py              # 時系列データの一括取得
python a04_yfinance_futures_multi.py     # 先物・為替の初回取得

定期実行（毎日・毎週など）
    ↓
python a03_yfinance_update.py            # 時系列データの継ぎ足し＋Parquet変換
python a04_yfinance_futures_multi.py     # 先物・為替の継ぎ足し＋Parquet変換
python a01_yfinance_info_update.py       # 財務指標の更新（任意）
python a05_check_missing.py              # データ品質の確認
```

<br><br>

## ⚙️ 設定のカスタマイズ

各スクリプト冒頭の設定セクションで以下を変更できます。

| 設定項目 | デフォルト値 | 説明 |
|---|---|---|
| `SLEEP_TIME` | 0.5秒 | 銘柄間の待機時間（サーバー負荷軽減） |
| `BACKUP_DIR` | `backups/` | バックアップ保存先 |
| `HIST_PERIOD` | `1mo`（a01のみ） | 売買代金計算に使う期間 |
| `AVG_DAYS` | 20日（a01のみ） | 売買代金の平均計算日数 |

<br><br>

## ⚠️ 注意事項

- yfinance は非公式APIです。利用規約の変更により予告なく動作しなくなる場合があります。
- 大量銘柄の連続取得はサーバー負荷になります。`SLEEP_TIME` を適切に設定してください。
- 取得データのタイムゾーンはUTCです。日本時間は `Datetime_JST` 列に変換済みです。
- 本ツールは情報収集を目的としており、投資判断の根拠として使用する場合は自己責任でお願いします。

<br><br>

## 📄 ライセンス

MIT License
