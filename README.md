# yfinanceを使って株価データをCSVでローカルに保存する

<br><br>

## 📂 ファイル構成

```
.
├── 01_stock_filtering.py     # 銘柄リストから分析対象を抽出
├── 02_yfinance_init.py       # 初回に実施、過去データの一括取得・保存
├── 03_yfinance_update.py     # 定期的に実施、最新データの継ぎ足し保存
├── _topix_list.xlsx          # JPXのHPからダウンロード ※ここでは列名を少し変えています
├── _filtering_list.xlsx      # 01_stock_filtering.py で生成されるファイル
.
```

<br><br>

## 🛠 事前準備

### 1. ライブラリのインストール

```bash
pip install pandas==2.3.3 yfinance==1.0 openpyxl==3.1.5 streamlit==1.52.2 altair==6.0.0 pyarrow==22.0.0
```

### 2. 東証上場銘柄一覧のダウンロード

以下のJPXサイトから「東証上場銘柄一覧」をExcelでダウンロードし、

- ファイル名：`_topix_list.xlsx`
- 「コード」列：４桁の銘柄コード
- 「銘柄」列：銘柄名

として、スクリプトと同じフォルダに保存してください。

- [東証上場銘柄一覧（JPX）](https://www.jpx.co.jp/markets/statistics-equities/misc/01.html)

<br><br>

## 実行手順

### Step 1：銘柄フィルタリング（初回のみ）

TOPIXの全銘柄から、売買代金・ボラティリティ・株価でフィルタリングして、デイトレ向き銘柄リストを作成します。

```bash
python 01_stock_filtering.py
```

実行すると `_filtering_list.xlsx` が生成されます。

**フィルタリング条件（`01_stock_filtering.py` 内で変更可能）**

| 条件 | デフォルト値 |
|:---|:---|
| 最低株価 | 500円以上 |
| 最低ボラティリティ | 1.0%以上（日中値幅率の20日平均） |
| 抽出銘柄数 | 上位720銘柄（売買代金順） |

<br><br>

### Step 2：株価データの初回取得（初回のみ）

`_filtering_list.xlsx`（5分足・1時間足）と `_topix_list.xlsx`（日足）をもとに、過去データを一括取得してCSVに保存します。

```bash
python 02_yfinance_init.py
```

以下のCSVファイルが生成されます。

| ファイル | 内容 | 取得期間 |
|:---|:---|:---|
| `_5min.csv` | 5分足データ | 直近60日 |
| `_1h.csv` | 1時間足データ | 直近2年 | 
| `_daily.csv` | 日足データ | 直近3年 |

> **注意**：銘柄数が多いため、完了まで数十分かかる場合があります。

<br><br>

### Step 3：データの定期更新

最新データをCSVに継ぎ足し保存します。毎営業日の市場終了後に実行することを想定しています。

```bash
python 03_yfinance_update.py
```

<br><br>

## yfinanceを使う上での注意事項

- yfinanceは**非公式API**です。大量の銘柄を頻繁に取得するとサーバーに負荷がかかります。各スクリプト内の `SLEEP_TIME` を調整して、適切な間隔を空けて実行してください。
- 5分足は yfinance の仕様上、**直近60日分**しか取得できません。継続的にデータを蓄積するために、`03_yfinance_update.py` を定期実行することを推奨します。
- 取得データのタイムゾーンはUTCです。日本時間は `Datetime_JST` 列に変換済みです。
- yfinanceのバージョンによってMultiIndex構造が変わる場合があります。本スクリプトでは動的に判定する処理を入れています。

<br><br>

## データの取り扱いについて

- 本リポジトリは個人利用および学習を目的としたツールであり、投資勧誘を目的としたものではありません。
- `yfinance` ライブラリを使用しています。利用にあたっては、Yahoo! の規約を遵守してください。
- 短時間での大量取得はサーバーに負担がかかります。APIのレート制限を守り、過度なリクエストは避けてください。

### Yahoo! 規約類

- [Yahoo! Finance Terms of Service](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html)
- [Yahoo! Developer API Terms of Use](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm)
- [Yahoo! 権利関係ページ](https://legal.yahoo.com/us/en/yahoo/permissions/requests/index.html)

<br><br>

## ⚠️ 免責事項

- **データの正確性**：取得データは正確性や即時性を保証しません。
- **損害への責任**：本ツールの利用により生じたいかなる損害についても、制作者は一切の責任を負いません。

<br><br>

## 関連記事

- [Qiita：yfinanceとStreamlitで株価分析アプリを作ってみよう（準備編）](https://qiita.com/minnano-python/items/946761b0855ed75c381a)

<br><br>

## License

MIT License
