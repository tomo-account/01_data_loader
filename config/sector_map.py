"""
セクター名（日本語）↔ ファイル名（英語スネークケース）の対応表

data/master/sectors/<filename>.csv のファイル名として使用
watch_market.csv の「セクターＡ」列とこのマップを突き合わせて
sectors/ を動的に生成する
"""

SECTOR_MAP: dict[str, str] = {
    # 半導体・電子（43セクターすべて watch_market.csv の実際のセクターＡ名を使用）
    "半導体材料":           "semiconductor_materials",
    "半導体製造装置":       "semiconductor_equipment",
    "半導体メーカー・商社": "semiconductor_makers",
    "電子部品":             "electronic_components",
    "FA・産業ロボット":     "fa_robots",
    "ITベンダー":           "it_vendors",
    "重電・総合電機":       "heavy_electronics",
    "家電・AV・OA":         "consumer_electronics",
    "精密・医療機器":       "precision_medical",
    # 自動車・輸送
    "自動車":               "auto",
    "航空会社":             "airlines",
    "海運業":               "shipping",
    "鉄道":                 "railways",
    # 重工・建設
    "重工":                 "heavy_industry",
    "ゼネコン":             "construction",
    "建機・産業機械":       "construction_machinery",
    "空調・住設・工具":     "hvac_tools",
    "総合エンジニアリング": "engineering",
    # 素材
    "鉄鋼":                 "steel",
    "非鉄金属":             "nonferrous_metals",
    "高機能材料":           "advanced_materials",
    "ガラス":               "glass",
    "ゴム製品":             "rubber",
    "繊維":                 "textile",
    "総合化学":             "chemicals",
    "資源":                 "resources",
    "電線":                 "electric_wire",
    # エネルギー
    "電力会社":             "electric_power",
    "電力系電設会社":       "power_construction",
    "ガス会社":             "gas",
    "石油元売":             "oil_refining",
    # 商社・金融
    "総合商社":             "trading_companies",
    "銀行":                 "banks",
    "証券":                 "securities",
    "保険会社":             "insurance",
    "リース会社":           "leasing",
    "デベロッパー":         "real_estate_dev",
    # 通信
    "通信キャリア":         "telecom",
    "ソフトバンク":         "softbank",
    # 消費・生活
    "食品":                 "food",
    "小売":                 "retail",
    "サービス・その他":     "services",
    # ヘルスケア
    "医薬品":               "pharma",
}

# 逆引き: 英語スネークケース → 日本語
SECTOR_JP: dict[str, str] = {v: k for k, v in SECTOR_MAP.items()}
