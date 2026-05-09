"""
RSS ニュース取得

ソース : config/rss_sources.RSS_SOURCES
保存先 : data/news/rss/{date}.csv（当日分に追記・重複除去）
列     : date, label, category, title, url, published, is_relevant

is_relevant フラグ:
  True  … タイトルに市場関連キーワードを含む記事
  False … 関係薄い記事（保存はするが素材生成では原則スキップ）
  ※ category="geopolitics" の記事は無条件で True
  ※ concat 後に全行で再計算するため、既存 CSV のフラグも最新キーワードで上書きされる

使い方:
    python collectors/fetch_rss.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import datetime
import pandas as pd
import feedparser

from config.rss_sources import RSS_SOURCES
from config.paths import NEWS_RSS

NEVER_RELEVANT_LABELS: set[str] = {
    "NHK経済",
}

MARKET_KEYWORDS: list[str] = [
    # ── 市場・相場全般 ──
    "株", "相場", "株式", "株価", "日経", "TOPIX", "東証",
    "市場", "騰落", "上昇", "下落", "反発", "反落", "急騰", "急落",
    # ── 為替・金利・マクロ ──
    "為替", "円安", "円高", "ドル", "ユーロ", "ポンド", "円相場",
    "金利", "利上げ", "利下げ", "利回り", "債券", "政策金利",
    "インフレ", "デフレ", "景気", "GDP", "CPI", "PMI", "物価",
    "賃上げ", "賃金", "雇用統計", "雇用", "失業",
    "日銀", "植田", "Fed", "FRB", "FOMC", "ECB", "BOJ",
    "財政", "補正予算", "貿易収支", "経常収支",
    # ── 商品・エネルギー ──
    "原油", "WTI", "ブレント", "天然ガス", "金先物", "金価格",
    "コモディティ", "資源",
    # ── 企業・決算 ──
    "決算", "業績", "増益", "減益", "黒字", "赤字",
    "上方修正", "下方修正", "配当", "自社株買い",
    "M&A", "買収", "合併", "上場", "IPO", "TOB",
    "売上", "営業利益", "純利益", "EPS", "受注",
    # ── セクター・テーマ ──
    "半導体", "AI", "人工知能", "データセンター",
    "電気自動車", "EV", "バッテリー", "再生可能エネルギー",
    "防衛", "医薬品", "バイオ", "不動産", "銀行",
    # ── 地政学・政策（英語ソース含む） ──
    "関税", "貿易", "輸出", "輸入", "制裁", "イラン", "トランプ",
    "日米", "米中", "中国経済",
    "tariff", "trade", "sanction",
    # ── 英語ソース（Reuters / MarketWatch / CNBC 等） ──
    "stock", "market", "rate", "Fed", "earnings", "revenue",
    "GDP", "inflation", "oil", "dollar", "yen", "bond",
    "nasdaq", "s&p", "dow", "nikkei", "semiconductor",
    "interest rate", "central bank",
    "jobs", "employment", "unemployment",
    "retail", "housing", "manufacturing", "yield", "treasury",
    "china", "japan", "economy", "recession", "growth",
]


def _is_relevant(title: str, category: str, label: str = "") -> bool:
    if label in NEVER_RELEVANT_LABELS:
        return False
    if category == "geopolitics":
        return True
    t = title.lower()
    return any(kw.lower() in t for kw in MARKET_KEYWORDS)


def fetch_source(source: dict) -> list[dict]:
    """1 RSS ソースから記事リストを取得（is_relevant フラグ付き）。"""
    feed     = feedparser.parse(source["url"])
    today    = datetime.date.today().isoformat()
    category = source["category"]
    rows = []
    for entry in feed.entries:
        title = entry.get("title", "")
        rows.append({
            "date":        today,
            "label":       source["label"],
            "category":    category,
            "title":       title,
            "url":         entry.get("link", ""),
            "published":   entry.get("published", ""),
            "is_relevant": _is_relevant(title, category, source["label"]),
        })
    return rows


def fetch_all_rss() -> list[dict]:
    all_articles = []
    for source in RSS_SOURCES:
        try:
            articles = fetch_source(source)
            relevant = sum(1 for a in articles if a["is_relevant"])
            print(f"  [OK] {source['label']} {len(articles)}件 (関連:{relevant}件)")
            all_articles.extend(articles)
        except Exception as e:
            print(f"  [ERR] {source['label']}: {e}")
    return all_articles


def save_rss(articles: list[dict]) -> None:
    today = datetime.date.today().isoformat()
    out   = NEWS_RSS / f"{today}.csv"
    NEWS_RSS.mkdir(parents=True, exist_ok=True)

    df_new = pd.DataFrame(articles)

    if out.exists():
        df_old = pd.read_csv(out)
        df = pd.concat([df_old, df_new]).drop_duplicates(subset=["url"], keep="last")
    else:
        df = df_new

    # concat 後に全行で再計算（旧フォーマット CSV 補完 & キーワード更新に対応）
    df["is_relevant"] = [
        _is_relevant(str(r.get("title", "")), str(r.get("category", "")), str(r.get("label", "")))
        for r in df.to_dict("records")
    ]

    df.to_csv(out, index=False, encoding="utf-8-sig")
    total    = len(df)
    relevant = int(df["is_relevant"].sum())
    print(f"  保存: {out.name} ({total}件 / 関連:{relevant}件 / 非関連:{total - relevant}件)")


def main() -> None:
    print("=== RSS ニュース取得 ===")
    articles = fetch_all_rss()
    save_rss(articles)
    print("\nRSS 取得 完了")


if __name__ == "__main__":
    main()
