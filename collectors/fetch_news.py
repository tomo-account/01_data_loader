"""
ニュース取得

[1] RSS ニュース
    ソース : config/rss_sources.RSS_SOURCES
    保存先 : data/news/YYYY-MM-DD.csv（当日分に追記・重複除去）
    列     : date, label, category, title, url, published, is_relevant

    is_relevant フラグ:
      True  … タイトルに市場関連キーワードを含む記事（Claude 素材生成で優先使用）
      False … 関係薄い記事（保存はするが素材生成では原則スキップ）
      ※ category="geopolitics" の記事は無条件で True
      ※ concat 後に全行で再計算するため、既存 CSV のフラグも最新キーワードで上書きされる

[2] TDnet 適時開示
    ソース : https://www.release.tdnet.info/inbs/
    保存先 : data/news/tdnet_YYYY-MM-DD.csv（日付ごと上書き）
    列     : date, time, code, company, title, pdf_url
    ※ TDnet は全件 is_relevant=True 扱い（適時開示は常に市場関連）
    ※ 開示 0 件（土日・祝日）でもヘッダのみのCSVを保存する（「未取得」と区別するため）
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import datetime
import time
import pandas as pd
import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from config.rss_sources import RSS_SOURCES
from config.paths import NEWS_RSS, NEWS_TDNET

load_dotenv()

TDNET_BASE   = "https://www.release.tdnet.info/inbs/"
TDNET_MAIN   = TDNET_BASE + "I_main_00.html"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}
SLEEP_SEC    = 0.5

# ── 市場関連キーワード ────────────────────────────────────────────
# タイトルにこれらのいずれかが含まれれば is_relevant=True
# 追加・削除は自由（大文字小文字は問わない）
# ── ラベル別除外リスト ─────────────────────────────────────────────
# このリストに含まれるラベルの記事は、タイトルに市場キーワードが
# あっても is_relevant=False にする。
# 理由: NHK経済 は生活・行政ニュースが多く、市場分析には不向き。
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
    """
    タイトルと category から市場関連度を判定。
    NEVER_RELEVANT_LABELS に含まれるラベルは無条件で False。
    geopolitics カテゴリは無条件で True。
    それ以外はキーワードマッチ。
    """
    if label in NEVER_RELEVANT_LABELS:
        return False
    if category == "geopolitics":
        return True
    t = title.lower()
    return any(kw.lower() in t for kw in MARKET_KEYWORDS)


# ── RSS ──────────────────────────────────────────────────────────

def fetch_source(source: dict) -> list[dict]:
    """1 RSS ソースから記事リストを取得（is_relevant フラグ付き）"""
    feed = feedparser.parse(source["url"])
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

    # ── is_relevant を全行で再計算 ───────────────────────────────
    # concat 後に再計算することで:
    # ① 旧フォーマットCSV（is_relevant列なし）を正しく補完できる
    # ② dedup で古い行が残った場合も最新キーワードで正しい値になる
    df["is_relevant"] = [
        _is_relevant(str(row.get("title", "")), str(row.get("category", "")), str(row.get("label", "")))
        for row in df.to_dict("records")
    ]

    df.to_csv(out, index=False, encoding="utf-8-sig")
    total    = len(df)
    relevant = int(df["is_relevant"].sum())
    print(f"  保存: {out.name} ({total}件 / 関連:{relevant}件 / 非関連:{total - relevant}件)")


# ── TDnet ─────────────────────────────────────────────────────────

def get_tdnet_date_urls() -> dict[str, str]:
    """
    TDnet メインページから {日付文字列: URL} のマップを取得
    例: {"2026-04-10": "I_list_001_20260410.html", ...}
    """
    resp = requests.get(TDNET_MAIN, headers=HTTP_HEADERS, timeout=10)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")
    sel  = soup.find("select", id="day-selector")
    if not sel:
        return {}
    result = {}
    for opt in sel.find_all("option"):
        val = opt.get("value", "").strip()
        if not val:
            continue
        import re
        m = re.search(r"(\d{8})", val)
        if m:
            raw      = m.group(1)
            date_str = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
            result[date_str] = val
    return result


def fetch_tdnet_page(page_url: str) -> list[dict]:
    """TDnet 1ページ分の開示一覧を取得"""
    resp = requests.get(TDNET_BASE + page_url, headers=HTTP_HEADERS, timeout=10)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    rows = soup.select("table tr")
    records = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue
        t    = cols[0].get_text(strip=True)
        code = cols[1].get_text(strip=True)
        comp = cols[2].get_text(strip=True)
        titl = cols[3].get_text(strip=True)

        if not (len(t) == 5 and t[2] == ":"):
            continue

        a   = cols[3].find("a") or row.find("a")
        pdf = (TDNET_BASE + a["href"]) if a and a.get("href") else ""

        records.append({
            "time":    t,
            "code":    code,
            "company": comp,
            "title":   titl,
            "pdf_url": pdf,
        })
    return records


def fetch_tdnet_date(date_str: str) -> list[dict]:
    """
    指定日の TDnet 開示を全ページ取得。
    date_str: "YYYY-MM-DD"
    ドロップダウンに依存せず URL を直接構築するため、2ヶ月超の過去日も取得可能。
    """
    raw_date = date_str.replace("-", "")  # "20260430"
    all_records = []
    page = 1
    while True:
        page_url = f"I_list_{page:03d}_{raw_date}.html"
        print(f"  TDnet page {page}...", end="", flush=True)
        try:
            records = fetch_tdnet_page(page_url)
        except Exception as e:
            print(f" ERR: {e}")
            break

        if not records:
            print(" (終了)")
            break

        print(f" {len(records)}件")
        all_records.extend(records)
        page += 1
        time.sleep(SLEEP_SEC)

    return all_records


def save_tdnet(records: list[dict], date_str: str) -> None:
    """
    TDnet 開示を CSV 保存。
    records が空（土日・祝日）でもヘッダのみのCSVを保存し「未取得」と区別できるようにする。
    """
    NEWS_TDNET.mkdir(parents=True, exist_ok=True)
    out = NEWS_TDNET / f"{date_str}.csv"

    if records:
        df = pd.DataFrame(records)
        df.insert(0, "date", date_str)
    else:
        # 開示なし（土日・祝日）: ヘッダのみ保存
        df = pd.DataFrame(columns=["date", "time", "code", "company", "title", "pdf_url"])

    df.to_csv(out, index=False, encoding="utf-8-sig")

    if records:
        print(f"  保存: {out.name} ({len(df)}件)")
    else:
        print(f"  保存: {out.name} (0件 ※土日・祝日のため開示なし)")


# ── メイン ────────────────────────────────────────────────────────

def _business_days_in_range(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    """start〜end の平日（月〜金）リストを返す。"""
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # 0=月 … 4=金
            days.append(current)
        current += datetime.timedelta(days=1)
    return days


def main(date: str | None = None, mode: str = "all",
         start_date: str | None = None, end_date: str | None = None) -> None:
    """
    date       : "YYYY-MM-DD" を指定すると TDnet はその日付で取得。省略時は今日。
    mode       : "all" / "rss" / "tdnet"
    start_date : 範囲取得の開始日（tdnet モード専用）
    end_date   : 範囲取得の終了日（tdnet モード専用）
    start_date/end_date を指定すると、その範囲の TDnet を一括取得する。
    既に CSV が存在する日はスキップする（--force で上書き可能）。
    """
    today_str   = datetime.date.today().isoformat()
    target_date = date if date else today_str
    is_today    = (target_date == today_str)

    # ── RSS ──
    if mode in ("all", "rss"):
        if is_today:
            print("=== RSS ニュース取得（本日分） ===")
            articles = fetch_all_rss()
            save_rss(articles)
        else:
            print("=== RSS ニュース取得 スキップ ===")
            print(f"  RSS は過去日の取得に対応していません（指定日: {target_date}）")
            if mode == "rss":
                print("  ※ mode=rss が指定されましたが、過去日のため何も取得しません。")
    else:
        print("=== RSS ニュース取得 スキップ（mode=tdnet）===")

    # ── TDnet ──
    if mode in ("all", "tdnet"):
        if start_date and end_date:
            # 範囲一括取得
            sd = datetime.date.fromisoformat(start_date)
            ed = datetime.date.fromisoformat(end_date)
            days = _business_days_in_range(sd, ed)
            print(f"\n=== TDnet 適時開示 一括取得（{start_date} 〜 {end_date}、{len(days)} 営業日）===")
            for d in days:
                d_str = d.isoformat()
                out   = NEWS_TDNET / f"{d_str}.csv"
                if out.exists():
                    print(f"  [SKIP] {d_str} — CSV 既存")
                    continue
                print(f"\n--- {d_str} ---")
                records = fetch_tdnet_date(d_str)
                save_tdnet(records, d_str)
        else:
            print(f"\n=== TDnet 適時開示取得（{target_date}） ===")
            records = fetch_tdnet_date(target_date)
            save_tdnet(records, target_date)
    else:
        print("\n=== TDnet 取得 スキップ（mode=rss）===")

    print("\nニュース取得 完了")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ニュース取得")
    parser.add_argument(
        "--date", default=None,
        help="取得日 YYYY-MM-DD（省略時=本日）。TDnet はこの日付で取得。RSS は本日のみ対応。"
    )
    parser.add_argument(
        "--mode", default="all", choices=["all", "rss", "tdnet"],
        help="取得モード: all=RSS+TDnet（デフォルト）/ rss=RSSのみ / tdnet=TDnetのみ"
    )
    parser.add_argument(
        "--start-date", default=None,
        help="範囲取得の開始日 YYYY-MM-DD（--end-date と併用、tdnet モード専用）"
    )
    parser.add_argument(
        "--end-date", default=None,
        help="範囲取得の終了日 YYYY-MM-DD（--start-date と併用）"
    )
    args = parser.parse_args()
    main(
        date=args.date,
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
    )
