"""
RSS ニュースソース定義

category:
  japan_disclosure  … 東証適時開示
  japan_news        … 日本経済ニュース
  global_news       … 米国・国際ニュース
  geopolitics       … 地政学（イラン・トランプ関税・原油 等）
"""

RSS_SOURCES: list[dict] = [
    # ── 日本 ────────────────────────────────────────
    # TDnet（東証適時開示）は RSS 非対応のためスクレイピング対応が必要
    # fetch_tdnet.py 内で個別実装
    {
        "label":    "NHK経済",
        "url":      "https://www3.nhk.or.jp/rss/news/cat3.xml",
        "category": "japan_news",
    },
    {
        "label":    "東洋経済",
        "url":      "https://toyokeizai.net/list/feed/rss",
        "category": "japan_news",
    },
    {
        "label":    "ダイヤモンド",
        "url":      "https://diamond.jp/feed/top",
        "category": "japan_news",
    },

    # ── 日本主要経済紙（Google News 経由） ───────────────
    {
        "label":    "GoogleNews_nikkei",
        "url":      "https://news.google.com/rss/search?q=site:nikkei.com&hl=ja&gl=JP&ceid=JP:ja",
        "category": "japan_news",
    },
    {
        "label":    "GoogleNews_bloomberg_jp",
        "url":      "https://news.google.com/rss/search?q=site:bloomberg.co.jp&hl=ja&gl=JP&ceid=JP:ja",
        "category": "global_news",
    },

    # ── 米国・国際 ──────────────────────────────────
    {
        "label":    "Reuters",
        "url":      "https://feeds.reuters.com/reuters/businessNews",
        "category": "global_news",
    },
    {
        "label":    "Yahoo Finance",
        "url":      "https://finance.yahoo.com/news/rssindex",
        "category": "global_news",
    },
    {
        "label":    "MarketWatch",
        "url":      "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
        "category": "global_news",
    },
    {
        "label":    "CNBC",
        "url":      "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "category": "global_news",
    },

    # ── 地政学 ──────────────────────────────────────
    {
        "label":    "GoogleNews_geopolitics",
        "url":      "https://news.google.com/rss/search?q=%E3%82%A4%E3%83%A9%E3%83%B3+%E3%83%88%E3%83%A9%E3%83%B3%E3%83%97+%E9%96%A2%E7%A8%8E+%E5%8E%9F%E6%B2%B9&hl=ja&gl=JP&ceid=JP:ja",
        "category": "geopolitics",
    },
]
