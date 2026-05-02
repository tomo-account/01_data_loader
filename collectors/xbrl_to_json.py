"""
xbrl_to_json.py — 決算短信 IXBRL を統一スキーマ JSON に変換する。

使い方:
    python xbrl_to_json.py <input>           # 入力: ZIP / ディレクトリ / IXBRL HTML
    python xbrl_to_json.py <input> --output <dir>
"""
from __future__ import annotations
import argparse
import csv
import json
import re
import sys
import unicodedata
import warnings
import zipfile
from pathlib import Path
from collections import defaultdict
from datetime import date, datetime

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


HERE             = Path(__file__).resolve().parent
MAPPING_CSV      = HERE / "mapping.csv"
DEFAULT_OUT_DIR  = Path(r"C:\stock_analysis\data\statements")
PARSER_VERSION   = "0.1.0"


# ══════════════════════════════════════════════════════════════════════════════
# Mapping CSV ローダー
# ══════════════════════════════════════════════════════════════════════════════

def load_mapping_rules(csv_path: Path = MAPPING_CSV) -> list[dict]:
    """mapping.csv を読み込み、各行を辞書化したリストを返す。"""
    if not csv_path.exists():
        raise FileNotFoundError(f"mapping.csv が見つかりません: {csv_path}")
    with open(csv_path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# ══════════════════════════════════════════════════════════════════════════════
# 入力ファイル検出
# ══════════════════════════════════════════════════════════════════════════════

def find_ixbrl_path(input_path: Path) -> Path:
    """入力（ZIP / ディレクトリ / .htm）から Summary の IXBRL ファイルを取得。"""
    if input_path.is_file():
        if input_path.suffix.lower() == ".htm" or input_path.suffix.lower() == ".html":
            return input_path
        if input_path.suffix.lower() == ".zip":
            # ZIP は一時展開 → Summary/ の IXBRL を返す
            extract_dir = input_path.with_suffix("")
            extract_dir.mkdir(exist_ok=True)
            with zipfile.ZipFile(input_path) as zf:
                zf.extractall(extract_dir)
            return _find_in_dir(extract_dir)

    if input_path.is_dir():
        return _find_in_dir(input_path)

    raise FileNotFoundError(f"入力ファイルが見つかりません: {input_path}")


def _find_in_dir(directory: Path) -> Path:
    """
    ディレクトリ内の Summary/*-ixbrl.htm を探す。
    Summary が無い場合は ZIP を自動展開して再検索する。
    Summary が無い ZIP（修正再開示・Attachment のみ）は明示的にエラーにする。
    """
    candidates = list(directory.glob("**/Summary/*-ixbrl.htm"))
    if candidates:
        return candidates[0]

    # ZIP を展開して再検索
    zips = list(directory.glob("**/*.zip"))
    if zips:
        for zp in zips:
            extract_dir = zp.parent / zp.stem
            extract_dir.mkdir(exist_ok=True)
            with zipfile.ZipFile(zp) as zf:
                zf.extractall(extract_dir)
        candidates = list(directory.glob("**/Summary/*-ixbrl.htm"))
        if candidates:
            return candidates[0]

    # Summary 無し: Attachment のみが含まれる修正再開示などの可能性
    has_attachment = bool(list(directory.glob("**/Attachment/*-ixbrl.htm")))
    if has_attachment:
        raise FileNotFoundError(
            f"Summary が無く Attachment のみが含まれています（修正再開示の可能性）: {directory}"
        )
    raise FileNotFoundError(f"IXBRL ファイルが見つかりません: {directory}")


# ══════════════════════════════════════════════════════════════════════════════
# 会計基準・期種別の検出
# ══════════════════════════════════════════════════════════════════════════════

def detect_accounting_standard(ixbrl_path: Path) -> str:
    """ファイル名から会計基準を判定。"""
    name = ixbrl_path.name
    if "qcedjpsm" in name or "acedjpsm" in name:
        return "JP"
    if "qcedifsm" in name or "acedifsm" in name:
        return "IFRS"
    if "qcedussm" in name or "acedussm" in name:
        return "US"
    return "JP"  # フォールバック


def detect_period_type(facts: list[dict]) -> str:
    """QuarterlyPeriod (1/2/3) から Q1/Q2/Q3 を判定。無ければ FY。"""
    for f in facts:
        if f.get("name") == "tse-ed-t:QuarterlyPeriod":
            v = f.get("value", "").strip()
            if v in ("1", "2", "3"):
                return f"Q{v}"
    return "FY"


def period_substitution(period_type: str) -> str:
    """{period} プレースホルダの置換値を返す。"""
    return {"Q1": "AccumulatedQ1", "Q2": "AccumulatedQ2", "Q3": "AccumulatedQ3", "FY": "Year"}[period_type]


# ══════════════════════════════════════════════════════════════════════════════
# IXBRL パーサー
# ══════════════════════════════════════════════════════════════════════════════

def find_attachment_dir(ixbrl_path: Path) -> Path | None:
    """Summary IXBRL のパスから Attachment ディレクトリを探す。"""
    for ancestor in ixbrl_path.parents:
        cand = ancestor / "Attachment"
        if cand.is_dir():
            return cand
    return None


def parse_label_file(lab_xml: Path) -> dict[str, str]:
    """lab.xml から concept ID → 日本語ラベル の辞書を構築する。"""
    labels: dict[str, str] = {}
    if not lab_xml.exists():
        return labels
    with open(lab_xml, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "lxml-xml")

    # link:loc は href で xsd 内の concept を指し、xlink:label でローカル名を持つ
    # link:label は xlink:label で「label_*」のような ID を持ち、xml:lang="ja" のテキストが日本語ラベル
    # link:labelArc が loc <-> label をつなぐ

    loc_to_concept: dict[str, str] = {}
    for loc in soup.find_all("link:loc"):
        href  = loc.get("xlink:href", "")
        label = loc.get("xlink:label", "")
        # href の "#" 以降が concept ID
        if "#" in href:
            concept_id = href.split("#", 1)[1]
            # concept_id は "tse-qcedjpfr-50190_PetroleumReportableSegmentsMember" のような形
            loc_to_concept[label] = concept_id

    # label 要素の xlink:label → text マッピング（preferredLabel = "label" のみ採用、verboseLabel は無視）
    label_id_to_text: dict[str, str] = {}
    for lab in soup.find_all("link:label"):
        if lab.get("xml:lang") != "ja":
            continue
        role = lab.get("xlink:role", "")
        if not role.endswith("/role/label"):
            continue  # 標準ラベルだけ採用
        label_id = lab.get("xlink:label", "")
        label_id_to_text[label_id] = lab.text.strip()

    # arc で loc.label と label.label を結ぶ
    for arc in soup.find_all("link:labelArc"):
        from_label = arc.get("xlink:from", "")
        to_label   = arc.get("xlink:to", "")
        concept_id = loc_to_concept.get(from_label)
        text       = label_id_to_text.get(to_label)
        if concept_id and text and concept_id not in labels:
            labels[concept_id] = text

    return labels


def parse_ixbrl(path: Path) -> tuple[dict, list[dict]]:
    """IXBRL から (contexts, facts) を抽出。"""
    with open(path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "lxml-xml")

    contexts: dict[str, dict] = {}
    for ctx in soup.find_all("xbrli:context"):
        cid = ctx.get("id")
        period = ctx.find("xbrli:period")
        period_info: dict = {}
        if period:
            instant = period.find("xbrli:instant")
            start   = period.find("xbrli:startDate")
            end     = period.find("xbrli:endDate")
            if instant:
                period_info = {"instant": instant.text}
            elif start and end:
                period_info = {"start": start.text, "end": end.text}
        contexts[cid] = {
            "period":  period_info,
            "members": [em.text.strip() for em in ctx.find_all("xbrldi:explicitMember")],
        }

    facts: list[dict] = []
    for tag in soup.find_all("ix:nonFraction"):
        facts.append({
            "name":     tag.get("name", ""),
            "context":  tag.get("contextRef", ""),
            "unit":     tag.get("unitRef", ""),
            "decimals": tag.get("decimals", ""),
            "scale":    tag.get("scale", ""),
            "sign":     tag.get("sign", ""),
            "value":    tag.text.strip(),
            "kind":     "numeric",
        })
    for tag in soup.find_all("ix:nonNumeric"):
        facts.append({
            "name":    tag.get("name", ""),
            "context": tag.get("contextRef", ""),
            "value":   tag.text.strip(),
            "kind":    "text",
        })

    return contexts, facts


# ══════════════════════════════════════════════════════════════════════════════
# Attachment 統合パース
# ══════════════════════════════════════════════════════════════════════════════

def parse_attachment_ixbrls(attachment_dir: Path) -> tuple[dict, list[dict]]:
    """Attachment 内の全 IXBRL を集約して (contexts, facts) を返す。
    Attachment では context が qcbs (BS) ファイルに集約されているため、複数ファイルを統合する。"""
    all_contexts: dict = {}
    all_facts: list[dict] = []
    for path in sorted(attachment_dir.glob("*-ixbrl.htm")):
        ctxs, facts = parse_ixbrl(path)
        all_contexts.update(ctxs)
        for f in facts:
            f["_source_file"] = path.name
        all_facts.extend(facts)
    return all_contexts, all_facts


# ── セグメント情報の抽出 ─────────────────────────────────────────────────

# 集計合計を表す member（個別セグメントではない）
_SEGMENT_AGGREGATE_MEMBERS = {
    "ReportableSegmentsMember",
    "OperatingSegmentsNotIncludedInReportableSegmentsAndOtherRevenueGeneratingBusinessActivitiesMember",
    "TotalOfReportableSegmentsAndOthersMember",
    "ReconcilingItemsMember",
    "EntityTotalMember",
    "OtherAndAdjustmentsAndEliminationsMember",
}

# 個別セグメントの member 名末尾パターン（複数形・単数形両方ある）
_SEGMENT_INDIVIDUAL_SUFFIXES = (
    "ReportableSegmentsMember",  # 例: 5019 出光興産
    "ReportableSegmentMember",   # 例: 8058 三菱商事
)

# 抽出する数値タグ（segment）— 日本基準と IFRS の両方に対応
_SEGMENT_NUMERIC_TAGS = {
    # 日本基準（jpcrp / jppfs）
    "jpcrp_cor:RevenuesFromExternalCustomers": "external_revenue",
    "jpcrp_cor:TransactionsWithOtherSegments": "intersegment_revenue",
    "jppfs_cor:NetSales":                       "total_revenue",
    "jppfs_cor:OperatingIncome":                "operating_income",
    "jpcrp_cor:EquityInEarningsLossesOfAffiliates": "equity_method_income",
    # IFRS（jpigp）
    "jpigp_cor:SalesToExternalCustomersIFRS":   "external_revenue",
    "jpigp_cor:IntersegmentSalesIFRS":          "intersegment_revenue",
    "jpigp_cor:NetSalesIFRS":                    "total_revenue",
    "jpigp_cor:Revenue2IFRS":                    "total_revenue",          # 8058 三菱商事系
    "jpigp_cor:OperatingProfitLossIFRS":        "operating_income",
    "jpigp_cor:ShareOfProfitLossOfInvestmentsAccountedForUsingEquityMethodIFRS": "equity_method_income",
    "jpigp_cor:GrossProfitIFRS":                "gross_profit",
    "jpigp_cor:ProfitLossAttributableToOwnersOfParentIFRS": "profit_attributable_to_owners",
    "jpigp_cor:ProfitLossBeforeTaxIFRS":        "profit_before_tax",
    "jpigp_cor:FinanceIncomeIFRS":              "finance_income",
    "jpigp_cor:FinanceCostsIFRS":               "finance_costs",
    "jpigp_cor:AssetsIFRS":                     "assets",
}


def _segment_member_key(member: str) -> tuple[str, str] | None:
    """member 文字列から (segment_key, role) を取得。
    segment_key: 'Petroleum' 等の英語識別子
    role: 'individual' / 'aggregate_reportable' / 'aggregate_others' / 'aggregate_total' / 'reconciling'
    """
    # namespace prefix 除去
    short = member.split(":", 1)[-1]
    # 会社固有の prefix を除去（例: "tse-qcedjpfr-50190PetroleumReportableSegmentsMember" → "PetroleumReportableSegmentsMember"）
    # prefix を含むパターンと含まないパターンを処理
    if short in _SEGMENT_AGGREGATE_MEMBERS:
        roles = {
            "ReportableSegmentsMember": "aggregate_reportable",
            "OperatingSegmentsNotIncludedInReportableSegmentsAndOtherRevenueGeneratingBusinessActivitiesMember": "aggregate_others",
            "TotalOfReportableSegmentsAndOthersMember": "aggregate_total",
            "ReconcilingItemsMember": "reconciling",
            "EntityTotalMember": "aggregate_entity_total",
            "OtherAndAdjustmentsAndEliminationsMember": "aggregate_others_adj",
        }
        return (short, roles[short])
    # 個別セグメント: 末尾が ReportableSegmentsMember (複数形) または ReportableSegmentMember (単数形)
    for suffix in _SEGMENT_INDIVIDUAL_SUFFIXES:
        if short.endswith(suffix):
            body = short[:-len(suffix)]
            # 先頭が小文字 prefix（会社固有）の場合は大文字始まりまでスキップ
            for i, ch in enumerate(body):
                if ch.isupper():
                    seg_key = body[i:]
                    return (seg_key, "individual")
            return (body, "individual")
    return None


def extract_segments(contexts: dict, facts: list[dict], labels: dict[str, str]) -> dict:
    """セグメント情報を抽出して JSON ブロックを返す。"""
    # 期間種別判定: Prior1YTDDuration / CurrentYTDDuration を採用
    PERIOD_PREFIXES = {
        "Prior1YTDDuration_":   "prior",
        "CurrentYTDDuration_":  "current",
    }
    BARE_CONTEXTS = {"Prior1YTDDuration", "CurrentYTDDuration"}

    # データ収集: segments[period][seg_key][field] = value
    segments_data: dict[str, dict[str, dict]] = {"current": {}, "prior": {}}
    aggregates: dict[str, dict[str, dict]] = {"current": {}, "prior": {}}

    for f in facts:
        if f.get("kind") != "numeric":
            continue
        tag_name = f.get("name", "")
        field    = _SEGMENT_NUMERIC_TAGS.get(tag_name)
        if not field:
            # TotalSegmentProfit は会社固有 prefix を持つので別ハンドリング
            if tag_name.endswith(":TotalSegmentProfit"):
                field = "segment_profit"
            else:
                continue

        ctx_id = f.get("context", "")
        # 期間判定
        period = None
        for prefix, p in PERIOD_PREFIXES.items():
            if ctx_id.startswith(prefix):
                period = p
                member_part = ctx_id[len(prefix):]
                break
        if period is None:
            continue

        ctx = contexts.get(ctx_id)
        if not ctx:
            continue
        members = ctx.get("members", [])
        if not members:
            continue
        member = members[0]
        key_role = _segment_member_key(member)
        if not key_role:
            continue
        seg_key, role = key_role

        # scale 適用
        scale = f.get("scale", "")
        unit  = f.get("unit", "")
        sign  = f.get("sign", "")
        val = normalize_value(f.get("value", ""), "int", scale, unit, sign)
        if val is None:
            continue

        target = aggregates if role != "individual" else segments_data
        target[period].setdefault(seg_key, {})
        target[period][seg_key][field] = val

    # ラベル付与（labels 辞書は concept_id ベース）
    def _attach_label(seg_key: str) -> str | None:
        # concept_id の典型形: "<prefix>_<seg_key>ReportableSegment(s)Member"
        for suffix_form in (f"{seg_key}ReportableSegmentsMember", f"{seg_key}ReportableSegmentMember"):
            for cid, text in labels.items():
                if cid.endswith(suffix_form):
                    return text
        return labels.get(seg_key)

    def _build_list(period: str) -> list[dict]:
        out = []
        for seg_key, data in segments_data[period].items():
            out.append({
                "key":   seg_key,
                "label": _attach_label(seg_key),
                **data,
            })
        return out

    def _build_aggregates(period: str) -> dict:
        return {role_key: data for role_key, data in aggregates[period].items()}

    has_individual = any(segments_data["current"]) or any(segments_data["prior"])
    has_aggregate  = any(aggregates["current"]) or any(aggregates["prior"])
    if not has_individual and not has_aggregate:
        return {}

    return {
        "current":      _build_list("current"),
        "prior":        _build_list("prior"),
        "aggregates":   {
            "current": _build_aggregates("current"),
            "prior":   _build_aggregates("prior"),
        },
    }


# ── qualitative.htm の Markdown 化 ───────────────────────────────────────

def parse_qualitative_html(html_path: Path) -> str:
    """qualitative.htm を Markdown 形式のテキストに変換する。"""
    if not html_path.exists():
        return ""
    with open(html_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "lxml")

    # body 要素のみを対象
    body = soup.find("body") or soup
    parts: list[str] = []

    def _emit_text(s: str) -> None:
        if s:
            parts.append(s)

    def _walk(node) -> None:
        if isinstance(node, str):
            return
        name = getattr(node, "name", None)
        if name is None:
            return

        if name in ("script", "style", "head", "meta"):
            return

        if name in ("h1", "h2", "h3", "h4", "h5"):
            level = int(name[1])
            text  = node.get_text(separator=" ", strip=True)
            if text:
                _emit_text("")
                _emit_text("#" * level + " " + text)
                _emit_text("")
            return

        if name == "table":
            rows = node.find_all("tr")
            if rows:
                _emit_text("")
                # 各行のセルテキストを抽出
                table_rows = []
                for tr in rows:
                    cells = [td.get_text(separator=" ", strip=True).replace("|", "\\|") for td in tr.find_all(["td", "th"])]
                    if any(cells):
                        table_rows.append(cells)
                if table_rows:
                    n_cols = max(len(r) for r in table_rows)
                    for r in table_rows:
                        r += [""] * (n_cols - len(r))
                    # ヘッダ行を区切り線で表現
                    parts.append("| " + " | ".join(table_rows[0]) + " |")
                    parts.append("|" + "|".join(["---"] * n_cols) + "|")
                    for r in table_rows[1:]:
                        parts.append("| " + " | ".join(r) + " |")
                _emit_text("")
            return

        if name == "p":
            text = node.get_text(separator=" ", strip=True)
            if text:
                _emit_text(text)
                _emit_text("")
            return

        # それ以外は子要素を再帰的に処理
        for child in node.children:
            _walk(child)

    _walk(body)

    md = "\n".join(parts)
    # 連続空行を整理
    while "\n\n\n" in md:
        md = md.replace("\n\n\n", "\n\n")
    return md.strip()


# ══════════════════════════════════════════════════════════════════════════════
# 値の正規化
# ══════════════════════════════════════════════════════════════════════════════

_DATE_RE = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")


def _to_halfwidth(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def normalize_value(raw: str, value_type: str, scale: str, unit: str, sign: str = "") -> any:
    """文字列値を Python の型に変換。scale 適用も行う。"""
    if raw is None or raw == "":
        return None

    if value_type == "bool":
        v = _to_halfwidth(raw).strip().lower()
        if v in ("有", "true"):
            return True
        if v in ("無", "false"):
            return False
        return None

    if value_type == "str":
        return raw

    if value_type == "date":
        v = _to_halfwidth(raw).strip()
        m = _DATE_RE.match(v)
        if m:
            return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        if re.match(r"^\d{4}-\d{2}-\d{2}", v):
            return v[:10]
        return v  # フォールバック

    # 数値
    cleaned = _to_halfwidth(raw).replace(",", "").replace("△", "-").strip()
    try:
        n = float(cleaned)
    except ValueError:
        return None

    if sign == "-":
        n = -abs(n)

    # scale 適用方針:
    #   JPY / Shares / JPYPerShares / NumberOfCompanies → 10^scale を乗算
    #   Pure → そのまま (パーセント等、表示値を保持)
    s = int(scale) if scale and scale.lstrip("-").isdigit() else 0
    if unit != "Pure" and s != 0:
        n = n * (10 ** s)

    if value_type == "int":
        return int(round(n))
    return n


# ══════════════════════════════════════════════════════════════════════════════
# JSON 構築
# ══════════════════════════════════════════════════════════════════════════════

def _set_nested(d: dict, path: str, value: any) -> None:
    """'a.b.c' のようなドットパスで nested dict にセット。既存 None は上書き。"""
    parts = path.split(".")
    cur = d
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    last = parts[-1]
    # 既に値が入っているとき: 既存が None なら上書き、非 None なら priority 1 が既に当たっていると判断してスキップ
    if last in cur and cur[last] is not None:
        return
    cur[last] = value


def _empty_template() -> dict:
    """JSON スキーマの空テンプレートを生成。"""
    return {
        "metadata":              {},
        "performance":           {"current": {}, "prior": {}, "change_pct": {},
                                  "forecast": {"lower": {}, "upper": {}},
                                  "forecast_change_pct": {}},
        "balance_sheet":         {"current": {}, "prior_year_end": {}},
        "dividend":              {"actual_prior":   {}, "actual_current": {},
                                  "forecast_current": {"lower": {}, "upper": {}}},
        "shares":                {},
        "consolidation_changes": {},
        "corrections":           {},
        "accounting_changes":    {},
        "audit":                 {},
        "notes":                 {},
        "_source":               {},
        "_warnings":             [],
    }


def build_json(
    rules: list[dict],
    contexts: dict,
    facts: list[dict],
    *,
    period_type: str,
    accounting_standard: str,
    ixbrl_path: Path,
) -> dict:
    """マッピングルールに従って JSON 出力を組み立てる。"""
    period_repl = period_substitution(period_type)

    # ファクトを name → list[fact] でルックアップしやすい辞書に
    facts_by_name: dict[str, list[dict]] = defaultdict(list)
    for f in facts:
        facts_by_name[f["name"]].append(f)
    # 完全一致用インデックスも保持（高速パス）
    facts_index: dict[tuple[str, str], dict] = {(f["name"], f["context"]): f for f in facts}

    out = _empty_template()
    unmatched_tags: set[str] = set()

    for rule in rules:
        std = rule["accounting_standard"]
        # 会計基準フィルタ
        if std != "any" and std != accounting_standard:
            continue

        tag = rule["xbrl_tag"]
        if not tag:
            continue
        ctx_pattern = rule["context_pattern"].replace("{period}", period_repl)
        json_path   = rule["json_path"]
        vtype       = rule["value_type"]

        # ① 完全一致（高速パス）
        fact = facts_index.get((tag, ctx_pattern))

        # ② 前方一致フォールバック:
        #    コンテキストIDが ctx_pattern で始まるファクトを候補とし、
        #    IDが短い（余分なメンバー修飾が少ない = 連結合計に近い）順に採用
        if fact is None:
            candidates = [
                f for f in facts_by_name.get(tag, [])
                if f["context"].startswith(ctx_pattern)
            ]
            if candidates:
                fact = min(candidates, key=lambda f: len(f["context"]))

        if fact is None:
            continue

        raw   = fact.get("value", "")
        scale = fact.get("scale", "")
        unit  = fact.get("unit", "")
        sign  = fact.get("sign", "")

        normalized = normalize_value(raw, vtype, scale, unit, sign)
        if normalized is None:
            continue

        # period_type の特殊処理
        if json_path == "metadata.period_type":
            v = str(raw).strip()
            normalized = {"1": "Q1", "2": "Q2", "3": "Q3"}.get(v, "FY")

        # SecuritiesCode は末尾 0 を除去して 4 桁化
        if json_path == "metadata.code":
            s = str(raw).strip()
            normalized = s[:-1] if len(s) == 5 and s.endswith("0") else s

        _set_nested(out, json_path, normalized)

    # ── metadata 補強 ──
    out["metadata"]["accounting_standard"] = accounting_standard
    out["metadata"]["consolidated"]        = True  # Summary は連結固定（v0.1）
    if "period_type" not in out["metadata"]:
        out["metadata"]["period_type"] = period_type

    # 期間情報を context から（member 付き context にもフォールバック）
    out["metadata"]["current_period"]      = _resolve_period(contexts, f"Current{period_repl}Duration")
    out["metadata"]["current_fiscal_year"] = _resolve_period(contexts, "CurrentYearDuration")
    out["metadata"]["prior_period"]        = _resolve_period(contexts, f"Prior{period_repl}Duration")

    # _source
    out["_source"] = {
        "format":         "ixbrl",
        "file":           ixbrl_path.name,
        "parser_version": PARSER_VERSION,
    }

    # 空 dict のクリーンアップ
    out = _prune_empty(out)
    return out


def _ctx_period(ctx: dict | None) -> dict | None:
    if not ctx:
        return None
    p = ctx.get("period", {})
    if "start" in p and "end" in p:
        return {"start": p["start"], "end": p["end"]}
    if "instant" in p:
        return {"instant": p["instant"]}
    return None


def _resolve_period(contexts: dict, base_id: str) -> dict | None:
    """base_id 自体の context が無ければ、`{base_id}_*` で始まる member 付き context にフォールバック。"""
    if base_id in contexts:
        return _ctx_period(contexts[base_id])
    # member 付きを探す
    prefix = base_id + "_"
    for cid, ctx in contexts.items():
        if cid.startswith(prefix):
            return _ctx_period(ctx)
    return None


def _prune_empty(obj):
    """空の dict ({}) を再帰的に null へ置換、不要キーを除去。"""
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            v2 = _prune_empty(v)
            if v2 == {} or v2 is None:
                cleaned[k] = None
            else:
                cleaned[k] = v2
        return cleaned
    if isinstance(obj, list):
        return [_prune_empty(x) for x in obj]
    return obj


# ══════════════════════════════════════════════════════════════════════════════
# 出力ファイル名生成
# ══════════════════════════════════════════════════════════════════════════════

def make_output_path(out_dir: Path, json_obj: dict) -> Path:
    code        = json_obj["metadata"].get("code", "unknown")
    filing_date = json_obj["metadata"].get("filing_date", "0000-00-00") or "0000-00-00"
    period      = json_obj["metadata"].get("period_type", "FY")
    return out_dir / f"{code}_{filing_date}_{period}.json"


# ══════════════════════════════════════════════════════════════════════════════
# メイン
# ══════════════════════════════════════════════════════════════════════════════

def _find_attachment_dir_anywhere(input_path: Path) -> Path | None:
    """ZIP / ディレクトリから Attachment ディレクトリを探す（Summary 無し対応）。"""
    if input_path.is_file() and input_path.suffix.lower() == ".zip":
        extract_dir = input_path.with_suffix("")
        extract_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(input_path) as zf:
            zf.extractall(extract_dir)
        search_root = extract_dir
    else:
        search_root = input_path
    for d in search_root.glob("**/Attachment"):
        if d.is_dir() and any(d.glob("*-ixbrl.htm")):
            return d
    return None


def _derive_correction_metadata(input_path: Path, attachment_dir: Path,
                                contexts: dict) -> dict:
    """Summary 無し ZIP からメタデータを推測する。"""
    # CurrentYTDDuration から会計期間を取得
    cur = contexts.get("CurrentYTDDuration") or {}
    period_info = cur.get("period", {})
    start_str = period_info.get("start", "")
    end_str   = period_info.get("end", "")

    period_type     = "FY"
    fiscal_year_end: str | None = None
    current_period: dict | None = None
    current_fiscal_year: dict | None = None
    if start_str and end_str:
        try:
            sd = datetime.fromisoformat(start_str).date()
            ed = datetime.fromisoformat(end_str).date()
            months = (ed.year - sd.year) * 12 + (ed.month - sd.month) + 1
            period_type = {3: "Q1", 6: "Q2", 9: "Q3", 12: "FY"}.get(months, "FY")
            # 会計年度末 = 期首から12ヶ月後の前日
            fy_end = sd.replace(year=sd.year + 1) - __import__("datetime").timedelta(days=1)
            fiscal_year_end = fy_end.isoformat()
            current_period = {"start": start_str, "end": end_str}
            current_fiscal_year = {"start": sd.isoformat(), "end": fy_end.isoformat()}
        except Exception:
            pass

    # 銘柄コード: Attachment ファイル名から 5 桁コード抽出 → 4 桁化
    code = "unknown"
    original_filing_date = ""
    sample = next(attachment_dir.glob("*-ixbrl.htm"), None)
    if sample is not None:
        # 例: tse-qcediffr-80310-2024-12-31-01-2025-02-04-ixbrl.htm
        parts = sample.stem.split("-")
        for p in parts:
            if p.isdigit() and len(p) == 5:
                code = p[:4] if p.endswith("0") else p
                break
        # ファイル名末尾付近の日付（元開示日）を抽出
        date_matches = re.findall(r"\d{4}-\d{2}-\d{2}", sample.stem)
        if len(date_matches) >= 2:
            original_filing_date = date_matches[-1]

    # 訂正開示日: ZIP ファイル名のシリアルから推定（08{4}{YYYYMMDD}{連番}）
    correction_date = ""
    zip_name = input_path.stem if input_path.suffix.lower() == ".zip" else input_path.name
    m = re.search(r"08\d{2}(\d{4})(\d{2})(\d{2})", zip_name)
    if m:
        correction_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # 会計基準: ファイル名 prefix から
    accounting_standard = "JP"
    if sample is not None:
        if "ifsm" in sample.name or "iffr" in sample.name:
            accounting_standard = "IFRS"
        elif "ussm" in sample.name or "usfr" in sample.name:
            accounting_standard = "US"

    return {
        "code":                  code,
        "company_name":          None,
        "document_name":         "（訂正再開示・Attachmentのみ）",
        "accounting_standard":   accounting_standard,
        "consolidated":          True,
        "period_type":           period_type,
        "filing_date":           correction_date or original_filing_date or "0000-00-00",
        "fiscal_year_end":       fiscal_year_end,
        "current_period":        current_period,
        "current_fiscal_year":   current_fiscal_year,
        "prior_period":          None,
        "is_correction":         True,
        "corrects_original_filing_date": original_filing_date or None,
        "correction_date":       correction_date or None,
    }


def convert_correction_only(input_path: Path, out_dir: Path) -> Path:
    """Summary 無し（Attachment のみ）の訂正再開示を JSON 化する。"""
    attachment_dir = _find_attachment_dir_anywhere(input_path)
    if attachment_dir is None:
        raise FileNotFoundError(f"Attachment が見つかりません: {input_path}")

    print(f"[correct] {attachment_dir}", file=sys.stderr)

    # Attachment IXBRL 群を統合パース
    att_contexts, att_facts = parse_attachment_ixbrls(attachment_dir)
    print(f"[attach]  contexts={len(att_contexts)} facts={len(att_facts)}", file=sys.stderr)

    # ラベル
    lab_files = list(attachment_dir.glob("*-lab.xml"))
    labels: dict[str, str] = {}
    if lab_files:
        labels = parse_label_file(lab_files[0])

    # メタデータ推測
    metadata = _derive_correction_metadata(input_path, attachment_dir, att_contexts)
    print(f"[derived] code={metadata['code']} period={metadata['period_type']} fy_end={metadata['fiscal_year_end']} filing={metadata['filing_date']}", file=sys.stderr)

    # 訂正版 JSON を構築（Summary 由来の項目は null）
    json_obj = _empty_template()
    json_obj["metadata"] = metadata

    # セグメント情報
    segments = extract_segments(att_contexts, att_facts, labels)
    if segments:
        json_obj["segments"] = segments
        print(f"[seg]     current={len(segments.get('current', []))} prior={len(segments.get('prior', []))}", file=sys.stderr)

    # qualitative.htm
    qual_path = attachment_dir / "qualitative.htm"
    if qual_path.exists():
        md = parse_qualitative_html(qual_path)
        if md:
            json_obj["qualitative"] = md
            print(f"[qual]    {len(md)} chars", file=sys.stderr)

    # ソース情報
    json_obj["_source"] = {
        "format":         "ixbrl-attachment-only",
        "file":           attachment_dir.name,
        "parser_version": PARSER_VERSION,
    }

    # 空 dict クリーンアップ
    json_obj = _prune_empty(json_obj)

    out_path = make_output_path(out_dir, json_obj)
    out_path.write_text(json.dumps(json_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write]   {out_path}", file=sys.stderr)
    return out_path


def convert(input_path: Path, out_dir: Path | None = None) -> Path:
    out_dir = out_dir or DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # Summary を試す。無ければ訂正再開示として処理。
    try:
        ixbrl_path = find_ixbrl_path(input_path)
    except FileNotFoundError as e:
        if "Attachment のみ" in str(e):
            return convert_correction_only(input_path, out_dir)
        raise

    print(f"[parse]   {ixbrl_path}", file=sys.stderr)

    rules = load_mapping_rules()
    contexts, facts = parse_ixbrl(ixbrl_path)

    accounting_standard = detect_accounting_standard(ixbrl_path)
    period_type         = detect_period_type(facts)
    print(f"[detect]  standard={accounting_standard} period={period_type}", file=sys.stderr)

    json_obj = build_json(
        rules, contexts, facts,
        period_type=period_type,
        accounting_standard=accounting_standard,
        ixbrl_path=ixbrl_path,
    )

    # ── Attachment 処理（セグメント情報・qualitative） ──
    attachment_dir = find_attachment_dir(ixbrl_path)
    if attachment_dir is not None:
        print(f"[attach]  {attachment_dir}", file=sys.stderr)

        # ラベルファイル（lab.xml）
        lab_files = list(attachment_dir.glob("*-lab.xml"))
        labels: dict[str, str] = {}
        if lab_files:
            labels = parse_label_file(lab_files[0])
            print(f"[labels]  {len(labels)} entries from {lab_files[0].name}", file=sys.stderr)

        # Attachment IXBRL 群を統合パース
        att_contexts, att_facts = parse_attachment_ixbrls(attachment_dir)
        print(f"[attach]  contexts={len(att_contexts)} facts={len(att_facts)}", file=sys.stderr)

        # セグメント情報抽出
        segments = extract_segments(att_contexts, att_facts, labels)
        if segments:
            json_obj["segments"] = segments
            print(f"[seg]     current={len(segments.get('current', []))} prior={len(segments.get('prior', []))}", file=sys.stderr)

        # qualitative.htm
        qual_path = attachment_dir / "qualitative.htm"
        if qual_path.exists():
            md = parse_qualitative_html(qual_path)
            if md:
                json_obj["qualitative"] = md
                print(f"[qual]    {len(md)} chars", file=sys.stderr)

    # 空 dict クリーンアップを再実行
    json_obj = _prune_empty(json_obj)

    out_path = make_output_path(out_dir, json_obj)
    out_path.write_text(json.dumps(json_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write]   {out_path}", file=sys.stderr)
    return out_path


def main() -> None:
    p = argparse.ArgumentParser(description="決算短信 IXBRL → JSON 変換")
    p.add_argument("input", help="ZIP / ディレクトリ / IXBRL HTML ファイル")
    p.add_argument("--output", "-o", default=None, help=f"出力ディレクトリ (default: {DEFAULT_OUT_DIR})")
    args = p.parse_args()

    out_dir = Path(args.output) if args.output else None
    convert(Path(args.input), out_dir)


if __name__ == "__main__":
    main()
