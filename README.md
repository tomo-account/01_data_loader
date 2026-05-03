# 01_data_loader

株価分析アプリ向けのデータ取得ツールです。株価・財務・決算短信・ニュースを無料APIで取得し、ローカルに保存します。Streamlit でUIを提供しています。

Qiita 解説記事：[Streamlitで株価分析アプリを作ってみよう ②（データ取得編）](#)

---

## 取得できるデータ

| データ | 取得元 | 保存先 |
|:--|:--|:--|
| 株価 OHLCV（日足・5分足） | yfinance | `data/prices/stocks/` |
| マクロ指標 OHLCV（日足・5分足） | yfinance | `data/prices/macro/` |
| 財務データ（EPS・BPS・DPS・ROE 等） | EDINET DB API | `data/financials/` |
| 決算短信（XBRL → JSON） | TDnet | `data/statements/` |
| 適時開示 | TDnet | `data/news/tdnet/` |
| ニュース | 各メディア RSS | `data/news/rss/` |
| 決算短信カレンダー（発表日時・過去実績） | J-Quants API | `data/news/kessan/` |
| 決算発表予定（翌営業日スケジュール） | J-Quants API | `data/news/kessan_schedule/` |

---

## セットアップ

### 1. 依存ライブラリのインストール

```bash
pip install yfinance pandas pyarrow requests feedparser python-dotenv streamlit
```

### 2. 環境変数の設定

財務データ・決算発表スケジュールの取得には API キーが必要です。

```bash
cp .env.example .env
```

`.env` を開いて API キーを設定してください。

```env
EDINETDB_API_KEY=your_api_key_here
JQUANTS_API_KEY=your_jquants_api_key_here
```

- EDINET DB API キーの取得：https://edinet-db.com/
- J-Quants API キーの取得：https://jpx-jquants.com/（ダッシュボード → API キー管理）

### 3. 銘柄リストの作成

`data/master/price_targets.csv` に取得対象の銘柄を登録します。

```csv
コード,銘柄,ティッカーコード
5020,ENEOSホールディングス,5020.T
8001,伊藤忠商事,8001.T
```

---

## 使い方

### Streamlit アプリとして起動

```bash
streamlit run pages/01_データ取得.py
```

ブラウザでUIが開きます。各ボタンからデータを取得できます。

### コマンドラインから実行

```bash
# 株価（日足・5分足）
python collectors/fetch_prices_stocks.py

# マクロ指標
python collectors/fetch_prices_macro.py

# 財務データ
python collectors/fetch_financials.py

# 決算短信 XBRL 取得
python collectors/fetch_statements.py --date 2026-05-01

# 決算短信 JSON 変換
python collectors/xbrl_to_json.py --all

# 適時開示・ニュース
python collectors/fetch_news.py --date 2026-05-01 --mode tdnet
python collectors/fetch_news.py --date 2026-05-01 --mode rss

# 決算短信カレンダー（過去実績）
python collectors/fetch_kessan_calendar.py --mode history --start-date 2024-03-01 --end-date 2025-05-02

# 決算発表予定（翌営業日）
python collectors/fetch_kessan_calendar.py --mode future
```

### 株式分割の補正

株式分割が発生した場合、5分足の過去データを遡及補正します。

```bash
# ドライラン（プレビューのみ、書き込みなし）
python collectors/fix_split_5min.py --code 8001 --split-date 2026-01-15 --split 1:5 --dry-run

# 実行
python collectors/fix_split_5min.py --code 8001 --split-date 2026-01-15 --split 1:5
```

補正ログは `data/master/split_corrections.csv` に記録されます。

---

## フォルダ構成

```
01_data_loader/
├── collectors/                   取得・変換スクリプト
│   ├── fetch_prices_stocks.py      株価（日足・5分足）
│   ├── fetch_prices_macro.py       マクロ指標
│   ├── fetch_financials.py         財務データ（EDINET DB API）
│   ├── fetch_news.py               ニュース・適時開示
│   ├── fetch_kessan_calendar.py    決算短信カレンダー・発表予定（J-Quants API）
│   ├── fetch_statements.py         決算短信 XBRL ZIP 取得
│   ├── xbrl_to_json.py             XBRL → JSON 変換
│   ├── mapping.csv                 XBRL タグマッピング定義
│   ├── fix_split_5min.py           株式分割 5分足遡及補正
│   └── build_sectors.py            セクター別銘柄 CSV 生成
│
├── config/
│   ├── paths.py                    全データパス定義
│   ├── rss_sources.py              RSS フィード URL 一覧
│   ├── tickers_macro.py            マクロ指標ティッカー定義
│   └── sector_map.py               セクターマッピング定義
│
├── utils/
│   ├── data_loader.py              CSV / Parquet 読み込み関数
│   ├── date_utils.py               直近営業日計算
│   └── layout_toggle.py            Streamlit UI 部品
│
├── data/                           保存先（実データは .gitignore で除外）
│   ├── master/                     銘柄マスタ・補正ログ
│   ├── prices/stocks/              株価 Parquet（日足・5分足）
│   ├── prices/macro/               マクロ指標 Parquet
│   ├── financials/                 財務データ CSV
│   ├── news/tdnet/                 適時開示 CSV
│   ├── news/rss/                   ニュース CSV
│   ├── news/kessan/                決算短信カレンダー CSV（日別）
│   ├── news/kessan_schedule/       決算発表予定 CSV（latest.csv）
│   ├── statements/                 決算短信 JSON
│   └── statements_zip/             決算短信 XBRL ZIP
│
└── pages/
    └── 01_データ取得.py            Streamlit データ取得 UI
```

---

## 注意事項

- `yfinance` は非公式ライブラリです。Yahoo Finance の仕様変更により取得できなくなる場合があります。
- EDINET DB API の無料プランは **100 calls/day** の上限があります。
- J-Quants API の無料プランは過去データ約15ヶ月分、決算発表予定は翌営業日分・3月9月決算企業のみが対象です。
- 取得したデータは個人利用の範囲でご使用ください。
