"""
マクロ・先物・指数・為替のティッカー定義

キー   = Parquet ファイル名（= / ^ を _ に置換した識別子）
値     = yfinance ティッカー文字列
"""

MACRO_TICKERS: dict[str, str] = {
    # 日本
    "N225":   "^N225",     # 日経225（現物指数）
    "NIY_F":  "NIY=F",     # 日経225先物

    # 米先物
    "ES_F":   "ES=F",      # S&P500 先物
    "NQ_F":   "NQ=F",      # Nasdaq100 先物
    "YM_F":   "YM=F",      # ダウ先物

    # 米指数現物
    "GSPC":   "^GSPC",     # S&P500
    "IXIC":   "^IXIC",     # Nasdaq Composite
    "DJI":    "^DJI",      # ダウ平均

    # ボラティリティ・セクター指数
    "VIX":    "^VIX",      # VIX 恐怖指数
    "SOX":    "^SOX",      # フィラデルフィア半導体指数
    "NYFANG": "^NYFANG",   # NYSE FANG+ Index（大型テック10銘柄）

    # 金利
    "TNX":    "^TNX",      # 米10年債利回り

    # コモディティ
    "CL_F":   "CL=F",      # 原油先物（WTI）
    "GC_F":   "GC=F",      # 金先物

    # コモディティ追加
    "HG_F":   "HG=F",      # 銅先物（景気先行指標）

    # アジア株
    "HSI":    "^HSI",       # 香港ハンセン指数
    "TWII":   "^TWII",      # 台湾加権指数（TSMC中心・東京時間と重なる）
    "KS11":   "^KS11",      # 韓国KOSPI（サムスン中心・東京時間と重なる）
    "SSEC":   "000001.SS",  # 上海総合指数（中国関連株の説明変数）

    # グローバル株式（オルカン）
    "ACWI":       "ACWI",    # iShares MSCI ACWI ETF（米国上場・USD）
    "MAXIS_ACWI": "2559.T",  # MAXIS全世界株式（オール・カントリー）上場投信（東証・JPY）

    # 為替
    "JPY_X":  "JPY=X",     # ドル円
    "EURUSD": "EURUSD=X",  # ユーロドル

    # ドルインデックス
    "DX_F":   "DX-Y.NYB",  # ドルインデックス（ICE、DX=F は yfinance 非対応）
}

# 逆引き: yfinance ティッカー → ファイルキー
KEY_BY_TICKER: dict[str, str] = {v: k for k, v in MACRO_TICKERS.items()}
