
# elite_proc_reporter.py
# Drop-in Snowflake Streamlit app with:
# - searchable stored procedure picker
# - DESCRIBE PROCEDURE parameter harvesting
# - optional Export metadata label overlay
# - authoritative lookup config support (if present)
# - hardened auto-discovery fallback for messy warehouses
# - single-value KEY/ID/CODE friendly dropdowns
# - multi-value KEYLIST/IDLIST/CODELIST selection builder
# - server-side lookup search + paging
# - run history + result export

from __future__ import annotations

from snowflake.snowpark.context import get_active_session
import streamlit as st
import pandas as pd
import re
import time as _time
from datetime import date, time, datetime
from typing import Optional, Dict, Any, List, Tuple
import json

from elite_proc_runner_core import (
    ProcUIMeta, ParamUIMeta, LookupUIMeta, ParamSubmission,
    humanize_identifier_advanced, is_numeric, is_bool, is_date, is_time, is_timestamp,
    is_variantish, is_stringish, sql_literal, match_export_metadata, resolve_proc_ui_meta,
    resolve_param_ui_meta, resolve_lookup_ui_meta, disambiguate_lookup_labels, proc_instance_key,
    build_param_submission, build_call_sql, validate_variant_json, normalize_name,
    analyze_execution_error,
)

# =========================================================
# App Config
# =========================================================
st.set_page_config(page_title="📊 Elite Proc Reports", layout="wide", initial_sidebar_state="expanded")
session = get_active_session()

APP_TITLE = "📊 Elite Proc Reports"
APP_SUBTITLE = "Stored procedures → elite reporting experience (fast, friendly, fully searchable)."

DEFAULT_LOOKUP_CONFIG_FQN = "EXPORT.PARAM_LOOKUP_CONFIG"

LOOKUP_SCHEMA_PRIORITY = {
    "READER": 120,
    "PARAMETER": 118,
    "SYSTEM": 115,
    "SECURITY": 112,
    "AGENT": 108,
    "TERMSET": 104,
    "CATALOG": 100,
    "AP": 96,
    "BUSINESSPARTNER": 94,
    "COMPLIANCE": 92,
    "MESSAGE": 90,
    "TRACKING": 88,
    "ACCOUNTING": 82,
    "CUSTOMEREXCLUSION": 80,
    "ANALYSIS": 58,
    "DATASET": 46,
    "DATALAKE": 34,
    "SPEND": 26,
    "STAGING": 18,
    "IMPORT": 8,
    "ZOOKEEPER": 0,
}

LOOKUP_TABLE_PENALTIES = {
    "HISTORY": -120,
    "HISTORICAL": -110,
    "MPOWER_VIEW": -80,
    "VIEW": -55,
    "STAGING": -65,
    "SUMMARY": -28,
    "REPORT": -24,
    "TRANS": -28,
    "INVOICE": -16,
    "COMMENTS": -48,
    "COMMENT": -48,
    "ANCESTOR": -22,
    "SHARE": -34,
    "VASHARE": -34,
    "FINANCIAL": -18,
    "EXCLUSION": -18,
    "OVERRIDE": -14,
    "ATTRIBUTES": -8,
    "METRICS": -18,
    "TICKETS": -18,
    "MESSAGE": -18,
}

LOOKUP_TRAILING_NOISE = (
    "WITHALL",
    "EXCLUDEALL",
    "NOALL",
    "ALLGROUPS",
    "EXCLUDINGCURRENT",
    "CURRENT",
    "FROM",
    "TO",
    "ALL",
    "EAS",
    "AIB",
    "24PERIODS",
    "DISTORGAS",
)

LOOKUP_PREFIX_RELAX = (
    "SESSION",
    "FTP",
    "USA",
    "US",
)

LOOKUP_INLINE_FAMILY_RELAX = (
    ("YEARFISCALPERIOD", "FISCALPERIOD"),
    ("YEARCONTRACTPERIOD", "CONTRACTPERIOD"),
    ("COMPASSSECTOR", "SECTOR"),
    ("COMPASSSECTORDIVISION", "SECTORDIVISION"),
    ("COMPASSDIVISIONREGION", "DIVISIONREGION"),
    ("COMPASSREGIONCUSTOMER", "REGIONCUSTOMER"),
    ("CUSTOMERGROUPWITHALL", "CUSTOMERGROUP"),
    ("CUSTOMERGROUPSECTORWITHALL", "CUSTOMERGROUPSECTOR"),
    ("CUSTOMERGROUPSECTORALL", "CUSTOMERGROUPSECTOR"),
    ("CUSTOMERGROUPCUSTOMERLEVELALLGROUPS", "CUSTOMERLEVEL"),
)

# =========================================================
# UI Polish
# =========================================================
st.markdown(
    """
<style>
.block-container { padding-top: 1.15rem; padding-bottom: 2.5rem; max-width: 1600px; }
h1 { margin-bottom: 0.2rem; }
.small-muted { opacity: 0.72; font-size: 0.92rem; }
.hr-soft { height: 1px; background: rgba(255,255,255,0.08); border: 0; margin: 0.8rem 0 1.0rem 0; }
.kbd {
  display:inline-block; padding: 0.08rem 0.40rem; border-radius: 0.35rem;
  border:1px solid rgba(255,255,255,0.18); background: rgba(255,255,255,0.06);
  font-size: 0.85rem; opacity: 0.92;
}
.lookup-source {
  font-size: 0.84rem;
  opacity: 0.80;
}
</style>
""",
    unsafe_allow_html=True,
)

st.title(APP_TITLE)
st.caption(APP_SUBTITLE)

# =========================================================
# Sidebar Settings
# =========================================================
with st.sidebar:
    st.header("Report settings")

    CFG_ALLOW_NULLS = True
    CFG_EMPTY_TEXT_AS_NULL = True
    CFG_USE_NAMED_ARGS = True
    CFG_HUMANIZE_NAMES = True
    CFG_ENABLE_METADATA_OVERLAY = True
    CFG_SMART_LOOKUPS = True
    CFG_ENABLE_LIST_LOOKUPS = True
    CFG_SMART_LOOKUPS_FOR_STRING_CODE = False
    CFG_USE_LOOKUP_CONFIG = True
    CFG_LOOKUP_CONFIG_FQN = DEFAULT_LOOKUP_CONFIG_FQN
    CFG_LOOKUP_PAGE_SIZE = 250
    CFG_LOOKUP_DISCOVERY_TABLE_LIMIT = 400
    CFG_SHOW_DEBUG = False
    CFG_SHOW_IDS_IN_LOOKUPS = False

    with st.expander("Admin / diagnostics", expanded=False):
        CFG_SMART_LOOKUPS = st.checkbox("Smart lookups for KEY/ID params", value=CFG_SMART_LOOKUPS)
        CFG_ENABLE_LIST_LOOKUPS = st.checkbox("Smart lookups for *KEYLIST/*IDLIST params", value=CFG_ENABLE_LIST_LOOKUPS)
        CFG_SMART_LOOKUPS_FOR_STRING_CODE = st.checkbox("Also try lookups for *CODE params", value=CFG_SMART_LOOKUPS_FOR_STRING_CODE)
        CFG_ALLOW_NULLS = st.checkbox("Allow NULL for any parameter", value=CFG_ALLOW_NULLS)
        CFG_EMPTY_TEXT_AS_NULL = st.checkbox("Treat empty text/JSON as NULL", value=CFG_EMPTY_TEXT_AS_NULL)
        CFG_SHOW_IDS_IN_LOOKUPS = st.checkbox("Show technical IDs in lookup labels", value=CFG_SHOW_IDS_IN_LOOKUPS)
        CFG_USE_LOOKUP_CONFIG = st.checkbox("Prefer lookup config table", value=CFG_USE_LOOKUP_CONFIG)
        CFG_LOOKUP_CONFIG_FQN = st.text_input("Lookup config table", value=CFG_LOOKUP_CONFIG_FQN)
        CFG_LOOKUP_PAGE_SIZE = st.number_input("Lookup page size", min_value=25, max_value=5000, value=CFG_LOOKUP_PAGE_SIZE, step=25)
        CFG_LOOKUP_DISCOVERY_TABLE_LIMIT = st.number_input("Lookup discovery candidate tables", min_value=50, max_value=1200, value=CFG_LOOKUP_DISCOVERY_TABLE_LIMIT, step=25)
        CFG_SHOW_DEBUG = st.checkbox("Show debug tools", value=CFG_SHOW_DEBUG)


# =========================================================
# SQL / Parsing Helpers
# =========================================================
def qident(name: str) -> str:
    """Double-quote an identifier safely."""
    name = (name or "").replace('"', '""')
    return f'"{name}"'


def esc_sql_str(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return (s or "").replace("'", "''")


def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).replace('"', "").strip().upper() for c in df.columns]
    return df


def run_show(sql_command: str) -> pd.DataFrame:
    """
    Execute SHOW ... then return the result via RESULT_SCAN(LAST_QUERY_ID()) as pandas.
    """
    session.sql(sql_command).collect()
    return session.sql("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))").to_pandas()


def first_paren_group(s: str) -> str:
    """
    Return the first balanced "(...)" group found in s, including parentheses.
    Intentionally ignores later "(...)" such as RETURN TABLE(...).
    """
    s = (s or "").strip()
    depth = 0
    start = None
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
            if depth == 1:
                start = i
        elif ch == ")":
            depth -= 1
            if depth == 0 and start is not None:
                return s[start : i + 1].strip()
    return "()"


def split_top_level_commas(s: str) -> List[str]:
    """
    Split a string by commas not inside parentheses.
    Example: "NUMBER(38,0), VARCHAR(10)" -> ["NUMBER(38,0)", "VARCHAR(10)"]
    """
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    for ch in (s or ""):
        if ch == "," and depth == 0:
            part = "".join(buf).strip()
            if part:
                parts.append(part)
            buf = []
            continue
        if ch == "(":
            depth += 1
        elif ch == ")" and depth > 0:
            depth -= 1
        buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


TYPE_FIRST_WORDS = {
    "NUMBER", "NUMERIC", "DECIMAL",
    "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
    "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL",
    "BOOLEAN", "BOOL",
    "VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT",
    "BINARY", "VARBINARY",
    "DATE", "TIME",
    "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "DATETIME",
    "VARIANT", "OBJECT", "ARRAY",
    "GEOGRAPHY", "GEOMETRY",
    "VECTOR",
}


def looks_like_type_first_word(token: str) -> bool:
    token = (token or "").upper()
    m = re.match(r"^([A-Z_]+)", token)
    base = m.group(1) if m else token
    return base in TYPE_FIRST_WORDS


def signature_types_only(paren_group: str) -> str:
    """
    Convert "(A NUMBER, B VARCHAR)" or "(NUMBER, VARCHAR)" into "(NUMBER, VARCHAR)"
    for DESCRIBE PROCEDURE overload disambiguation.
    """
    pg = (paren_group or "").strip()
    if not (pg.startswith("(") and pg.endswith(")")):
        return "()"

    inner = pg[1:-1].strip()
    if not inner:
        return "()"

    segs = split_top_level_commas(inner)
    out_types: List[str] = []

    for seg in segs:
        seg = " ".join((seg or "").strip().split())
        if not seg:
            continue

        tokens = seg.split(" ")

        if tokens and tokens[0].upper() in ("IN", "OUT", "INOUT", "INPUT", "OUTPUT"):
            tokens = tokens[1:]
        if not tokens:
            continue

        up = [t.upper() for t in tokens]
        if "DEFAULT" in up:
            tokens = tokens[:up.index("DEFAULT")]
        if not tokens:
            continue

        if looks_like_type_first_word(tokens[0]):
            dtype = " ".join(tokens).upper()
        else:
            dtype = " ".join(tokens[1:]).upper() if len(tokens) > 1 else tokens[0].upper()

        dtype = dtype.strip()
        if dtype:
            out_types.append(dtype)

    return "(" + ", ".join(out_types) + ")"


def parse_describe_signature_value(sig: str) -> List[Dict[str, Any]]:
    """
    Parse DESCRIBE PROCEDURE property VALUE for 'signature', e.g.:
      "(BUSINESSKEY NUMBER, YEARCONTRACTPERIODKEY NUMBER DEFAULT 2025)"
    Returns list of dicts: {"name":..., "type":..., "default": Optional[str]}
    """
    s = (sig or "").strip()
    if not s:
        return []

    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1].strip()
    if not s:
        return []

    segs = split_top_level_commas(s)
    params: List[Dict[str, Any]] = []

    for i, seg in enumerate(segs, start=1):
        seg = " ".join((seg or "").strip().split())
        if not seg:
            continue

        tokens = seg.split(" ")

        if tokens and tokens[0].upper() in ("IN", "OUT", "INOUT", "INPUT", "OUTPUT"):
            tokens = tokens[1:]
        if not tokens:
            continue

        default_expr: Optional[str] = None
        up = [t.upper() for t in tokens]
        if "DEFAULT" in up:
            di = up.index("DEFAULT")
            default_expr = " ".join(tokens[di + 1 :]).strip() or None
            tokens = tokens[:di]
        if not tokens:
            continue

        if len(tokens) == 1:
            params.append({"name": f"ARG{i}", "type": tokens[0].upper(), "default": default_expr})
            continue

        if looks_like_type_first_word(tokens[0]):
            dtype = " ".join(tokens).upper()
            params.append({"name": f"ARG{i}", "type": dtype, "default": default_expr})
            continue

        name = tokens[0].strip('"')
        dtype = " ".join(tokens[1:]).strip().upper()
        params.append({"name": name, "type": dtype, "default": default_expr})

    return params


def is_generic_arg_name(name: str) -> bool:
    n = (name or "").strip().upper()
    return bool(re.match(r"^(ARG|P)\d+$", n))


def uniq_keep_order(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def parse_fqn(default_db: str, fqn: str) -> Optional[Tuple[str, str, str]]:
    raw = (fqn or "").strip()
    if not raw:
        return None
    parts = [p.strip().strip('"') for p in raw.split(".") if p.strip()]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return default_db, parts[0], parts[1]
    return None


# =========================================================
# Smart Lookup Helpers
# =========================================================
def norm_name(s: str) -> str:
    return re.sub(r"[^A-Z0-9_]", "", (s or "").upper())


def norm_no_us(s: str) -> str:
    return norm_name(s).replace("_", "")


def is_keyish_name(s: str) -> bool:
    n = norm_no_us(s)
    return n.endswith("KEY") or n.endswith("ID")


def is_codeish_name(s: str) -> bool:
    n = norm_no_us(s)
    return n.endswith("CODE")


def is_lookup_list_name(s: str) -> bool:
    n = norm_no_us(s)
    return n.endswith("KEYLIST") or n.endswith("IDLIST") or n.endswith("CODELIST")


def split_suffixish(s: str) -> Tuple[str, str]:
    n = norm_no_us(s)
    for suf in ("KEYLIST", "IDLIST", "CODELIST", "KEY", "ID", "CODE"):
        if n.endswith(suf) and len(n) > len(suf):
            return n[: -len(suf)], suf
    return n, ""


def base_from_suffixish(s: str) -> str:
    return split_suffixish(s)[0]


def remove_trailing_noise(base: str) -> List[str]:
    vals: List[str] = [base]
    cur = base
    changed = True
    while changed and cur:
        changed = False
        for token in LOOKUP_TRAILING_NOISE:
            if cur.endswith(token) and len(cur) > len(token):
                cur = cur[: -len(token)]
                vals.append(cur)
                changed = True
                break
    return uniq_keep_order(vals)


def pluralize_entity(base: str) -> str:
    b = (base or "").upper()
    if not b:
        return b
    if b.endswith("Y") and len(b) > 1 and b[-2] not in "AEIOU":
        return b[:-1] + "IES"
    if b.endswith(("S", "X", "Z", "CH", "SH")):
        return b + "ES"
    return b + "S"


def entity_table_name_variants(base: str) -> List[str]:
    b = (base or "").upper()
    if not b:
        return []
    out = [b, pluralize_entity(b)]
    if b.endswith("CATEGORY"):
        out.append(b[:-8] + "CATEGORIES")
    if b.endswith("BUSINESS"):
        out.append("BUSINESSES")
    return uniq_keep_order(out)


def generate_lookup_hints(param_name: str) -> List[str]:
    """
    Produce ordered lookup-family candidates from a parameter name.
    Examples:
      SESSIONBUSINESSKEY -> [SESSIONBUSINESSKEY, BUSINESSKEY]
      SESSIONYEARCONTRACTPERIODKEYFROM -> [..., YEARCONTRACTPERIODKEY, CONTRACTPERIODKEY]
      SESSIONCUSTOMERGROUPWITHALLKEY -> [..., CUSTOMERGROUPKEY]
    """
    raw = norm_no_us(param_name)
    if not raw:
        return []

    hints: List[str] = [raw]
    cur = raw

    for pref in LOOKUP_PREFIX_RELAX:
        if cur.startswith(pref) and len(cur) > len(pref):
            cur = cur[len(pref):]
            hints.append(cur)

    base, suf = split_suffixish(cur)
    if base and suf:
        base_variants = remove_trailing_noise(base)

        for old, new in LOOKUP_INLINE_FAMILY_RELAX:
            for b in list(base_variants):
                if old in b:
                    base_variants.append(b.replace(old, new))

        # period/family relaxations
        for b in list(base_variants):
            if b.startswith("YEAR") and ("PERIOD" in b or "CONTRACT" in b):
                base_variants.append(b[4:])
            if b.startswith("USA") and "FISCALPERIOD" in b:
                base_variants.append(b[3:])
            if b.startswith("US") and "FISCALPERIOD" in b:
                base_variants.append(b[2:])
            if b.startswith("FTP") and ("FISCALPERIOD" in b or "CUSTOMERLEVEL" in b):
                base_variants.append(b[3:])
            if b.startswith("FOODBUY") and "SECTOR" in b:
                base_variants.append(b.replace("FOODBUY", "", 1))
            if b.startswith("COMPASS") and ("SECTOR" in b or "DIVISION" in b or "REGION" in b or "CUSTOMER" in b):
                base_variants.append(b.replace("COMPASS", "", 1))

        base_variants = uniq_keep_order(base_variants)
        hints.extend([f"{b}{suf}" for b in base_variants])

        # family fallback: if KEYLIST/IDLIST/CODELIST, also search singular family
        if suf in ("KEYLIST", "IDLIST", "CODELIST"):
            singular_suf = suf.replace("LIST", "")
            hints.extend([f"{b}{singular_suf}" for b in base_variants])

    # exact relaxed forms for common suffix wrappers
    n2 = raw
    for old, new in (
        ("KEYLIST", "KEY"),
        ("IDLIST", "ID"),
        ("CODELIST", "CODE"),
        ("LIST", ""),
    ):
        if n2.endswith(old) and len(n2) > len(old):
            hints.append(n2[: -len(old)] + new)

    return uniq_keep_order(hints)


def schema_score(schema_name: str) -> int:
    return LOOKUP_SCHEMA_PRIORITY.get((schema_name or "").upper(), 40)


def table_penalty(table_name: str) -> int:
    t = norm_no_us(table_name)
    score = 0
    for token, penalty in LOOKUP_TABLE_PENALTIES.items():
        if token in t:
            score += penalty
    return score


def table_entity_bonus(table_name: str, hint_families: List[str]) -> int:
    t = norm_no_us(table_name)
    best = 0
    for i, family in enumerate(hint_families):
        fam = base_from_suffixish(family)
        variants = entity_table_name_variants(fam)
        for v in variants:
            if t == v:
                best = max(best, 170 - i * 8)
            elif t.startswith(v):
                best = max(best, 120 - i * 6)
            elif t.endswith(v):
                best = max(best, 105 - i * 6)
            elif v in t:
                best = max(best, 70 - i * 4)
    return best


def score_label_col(col: str, dt: str, hint_families: List[str]) -> int:
    c = norm_no_us(col)
    score = 0

    for i, fam in enumerate(hint_families):
        base = base_from_suffixish(fam)
        weight = max(0, 60 - i * 5)
        if c == f"{base}NAME":
            score = max(score, 220 + weight)
        if c == f"{base}DESCRIPTION":
            score = max(score, 180 + weight)
        if c == f"{base}DESC":
            score = max(score, 165 + weight)
        if c == f"{base}CODE":
            score = max(score, 150 + weight)
        if c == f"{base}DISPLAYNAME":
            score = max(score, 210 + weight)
        if base and base in c and c.endswith("NAME"):
            score = max(score, 185 + weight)
        if base and base in c and c.endswith("CODE"):
            score = max(score, 135 + weight)

    generic_hits = {
        "DISPLAYNAME": 210,
        "FULLNAME": 205,
        "LONGNAME": 200,
        "SHORTNAME": 198,
        "NAME": 190,
        "DESCRIPTION": 165,
        "DESC": 150,
        "LABEL": 145,
        "TITLE": 142,
        "CODE": 125,
    }
    for token, pts in generic_hits.items():
        if c == token:
            score = max(score, pts)
        elif c.endswith(token):
            score = max(score, pts - 8)

    if "STATUS" in c or "FLAG" in c or "ACTIVE" in c:
        score -= 35

    if is_stringish(dt):
        score += 25
    else:
        score -= 30

    return score


def score_detail_col(col: str, dt: str, label_col: str, hint_families: List[str]) -> int:
    c = norm_no_us(col)
    lbl = norm_no_us(label_col)
    if c == lbl:
        return -10**9

    score = 0
    for i, fam in enumerate(hint_families):
        base = base_from_suffixish(fam)
        weight = max(0, 50 - i * 5)
        if c == f"{base}DESCRIPTION":
            score = max(score, 220 + weight)
        if c == f"{base}DESC":
            score = max(score, 200 + weight)
        if c == f"{base}CODE":
            score = max(score, 170 + weight)
        if c == f"{base}TYPE":
            score = max(score, 120 + weight)

    generic_hits = {
        "DESCRIPTION": 200,
        "DESC": 175,
        "DETAIL": 165,
        "TYPE": 120,
        "CATEGORY": 110,
        "STATUS": 85,
        "CODE": 135,
    }
    for token, pts in generic_hits.items():
        if c == token:
            score = max(score, pts)
        elif c.endswith(token):
            score = max(score, pts - 8)

    if is_stringish(dt):
        score += 18
    else:
        score -= 15

    return score


@st.cache_data(ttl=600, show_spinner=False)
def table_exists(db: str, schema: str, table: str) -> bool:
    sql = f"""
    SELECT 1
    FROM {qident(db)}.INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_SCHEMA) = '{esc_sql_str((schema or '').upper())}'
      AND UPPER(TABLE_NAME) = '{esc_sql_str((table or '').upper())}'
    LIMIT 1
    """
    try:
        df = session.sql(sql).to_pandas()
        return not df.empty
    except Exception:
        return False


@st.cache_data(ttl=600, show_spinner=False)
def get_column_dtype(db: str, schema: str, table: str, column: str) -> Optional[str]:
    sql = f"""
    SELECT DATA_TYPE
    FROM {qident(db)}.INFORMATION_SCHEMA.COLUMNS
    WHERE UPPER(TABLE_SCHEMA) = '{esc_sql_str((schema or '').upper())}'
      AND UPPER(TABLE_NAME) = '{esc_sql_str((table or '').upper())}'
      AND UPPER(COLUMN_NAME) = '{esc_sql_str((column or '').upper())}'
    LIMIT 1
    """
    try:
        df = norm_cols(session.sql(sql).to_pandas())
        if df.empty:
            return None
        return str(df.iloc[0].get("DATA_TYPE") or "").upper() or None
    except Exception:
        return None


@st.cache_data(ttl=600, show_spinner=False)
def lookup_config_status(default_db: str, config_fqn: str) -> Dict[str, Any]:
    parsed = parse_fqn(default_db, config_fqn)
    if parsed is None:
        return {"exists": False, "db": None, "schema": None, "table": None, "reason": "invalid_fqn"}

    cdb, cschema, ctable = parsed
    exists = table_exists(cdb, cschema, ctable)
    return {"exists": exists, "db": cdb, "schema": cschema, "table": ctable, "reason": None if exists else "not_found"}


@st.cache_data(ttl=300, show_spinner=False)
def get_lookup_config_df(default_db: str, config_fqn: str) -> pd.DataFrame:
    status = lookup_config_status(default_db, config_fqn)
    if not status.get("exists"):
        return pd.DataFrame()

    fq = f"{qident(status['db'])}.{qident(status['schema'])}.{qident(status['table'])}"
    try:
        df = norm_cols(session.sql(f"SELECT * FROM {fq}").to_pandas())
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    if "IS_ACTIVE" in df.columns:
        active_mask = df["IS_ACTIVE"].astype(str).str.upper().isin(["1", "TRUE", "T", "Y", "YES"])
        df = df[active_mask].copy()

    for col in ["PARAM_NAME", "LOOKUP_DATABASE", "LOOKUP_SCHEMA", "LOOKUP_TABLE", "ID_COLUMN", "LABEL_COLUMN", "DETAIL_COLUMN", "WHERE_CLAUSE", "ID_DATA_TYPE"]:
        if col not in df.columns:
            df[col] = None

    if "MATCH_PRIORITY" not in df.columns:
        df["MATCH_PRIORITY"] = 0

    df["PARAM_NAME_NORM"] = df["PARAM_NAME"].fillna("").astype(str).map(norm_no_us)
    df["LOOKUP_DATABASE"] = df["LOOKUP_DATABASE"].fillna(status["db"]).astype(str)
    df["LOOKUP_SCHEMA"] = df["LOOKUP_SCHEMA"].fillna("").astype(str)
    df["LOOKUP_TABLE"] = df["LOOKUP_TABLE"].fillna("").astype(str)
    df["ID_COLUMN"] = df["ID_COLUMN"].fillna("").astype(str)
    df["LABEL_COLUMN"] = df["LABEL_COLUMN"].fillna("").astype(str)
    df["DETAIL_COLUMN"] = df["DETAIL_COLUMN"].where(df["DETAIL_COLUMN"].notna(), None)
    df["WHERE_CLAUSE"] = df["WHERE_CLAUSE"].where(df["WHERE_CLAUSE"].notna(), None)
    df["ID_DATA_TYPE"] = df["ID_DATA_TYPE"].where(df["ID_DATA_TYPE"].notna(), None)
    return df


def config_row_to_source(default_db: str, row: pd.Series) -> Optional[Dict[str, Any]]:
    lookup_db = str(row.get("LOOKUP_DATABASE") or default_db).strip()
    lookup_schema = str(row.get("LOOKUP_SCHEMA") or "").strip()
    lookup_table = str(row.get("LOOKUP_TABLE") or "").strip()
    id_col = str(row.get("ID_COLUMN") or "").strip()
    label_col = str(row.get("LABEL_COLUMN") or "").strip()
    detail_col_raw = row.get("DETAIL_COLUMN")
    detail_col = str(detail_col_raw).strip() if detail_col_raw is not None and str(detail_col_raw).strip() else None
    where_clause_raw = row.get("WHERE_CLAUSE")
    where_clause = str(where_clause_raw).strip() if where_clause_raw is not None and str(where_clause_raw).strip() else None
    id_dtype = str(row.get("ID_DATA_TYPE") or "").strip().upper() or None

    if not (lookup_db and lookup_schema and lookup_table and id_col and label_col):
        return None

    if id_dtype is None:
        id_dtype = get_column_dtype(lookup_db, lookup_schema, lookup_table, id_col)

    return {
        "database": lookup_db,
        "schema": lookup_schema,
        "table": lookup_table,
        "id_col": id_col,
        "label_col": label_col,
        "detail_col": detail_col,
        "where_clause": where_clause,
        "id_dtype": id_dtype,
        "score": int(row.get("MATCH_PRIORITY") or 0) + 10000,
        "resolver": "config",
        "matched_hint": str(row.get("PARAM_NAME") or ""),
    }


def resolve_lookup_source_from_config(default_db: str, param_name: str, config_fqn: str) -> Optional[Dict[str, Any]]:
    cfg = get_lookup_config_df(default_db, config_fqn)
    if cfg.empty:
        return None

    hints = generate_lookup_hints(param_name)
    hint_norms = [norm_no_us(h) for h in hints]
    raw_norm = norm_no_us(param_name)

    rows = cfg[cfg["PARAM_NAME_NORM"].isin(hint_norms)].copy()
    if rows.empty:
        return None

    def _rank(r: pd.Series) -> Tuple[int, int]:
        pn = str(r.get("PARAM_NAME_NORM") or "")
        exact_actual = 1 if pn == raw_norm else 0
        hint_pos = hint_norms.index(pn) if pn in hint_norms else 999
        prio = int(r.get("MATCH_PRIORITY") or 0)
        return exact_actual, -hint_pos + prio

    rows["_RANK_EXACT"] = rows.apply(lambda r: _rank(r)[0], axis=1)
    rows["_RANK_HINT"] = rows.apply(lambda r: _rank(r)[1], axis=1)
    rows = rows.sort_values(["_RANK_EXACT", "_RANK_HINT"], ascending=[False, False]).reset_index(drop=True)

    for _, row in rows.iterrows():
        src = config_row_to_source(default_db, row)
        if src is None:
            continue
        if table_exists(src["database"], src["schema"], src["table"]):
            return src

    return None


@st.cache_data(ttl=600, show_spinner=False)
def discover_best_lookup_source(
    db: str,
    param_name: str,
    cand_limit: int,
    use_lookup_config: bool,
    config_fqn: str,
) -> Optional[Dict[str, Any]]:
    """
    Discover a lookup source (schema/table + id/label/detail cols) for a parameter.
    Resolution order:
      1) authoritative lookup config row (if enabled and available)
      2) scored auto-discovery from INFORMATION_SCHEMA
    """
    if use_lookup_config:
        cfg_src = resolve_lookup_source_from_config(db, param_name, config_fqn)
        if cfg_src is not None:
            return cfg_src

    hints = generate_lookup_hints(param_name)
    if not hints:
        return None

    hint_norms = uniq_keep_order([norm_no_us(h) for h in hints])
    cand_limit = int(cand_limit) if int(cand_limit) > 0 else 400

    in_exact = ", ".join([f"'{esc_sql_str(h)}'" for h in hints])
    in_norm = ", ".join([f"'{esc_sql_str(h)}'" for h in hint_norms])

    sql = f"""
    WITH cand AS (
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME
        FROM {qident(db)}.INFORMATION_SCHEMA.COLUMNS
        WHERE (
            UPPER(COLUMN_NAME) IN ({in_exact})
            OR REPLACE(UPPER(COLUMN_NAME), '_', '') IN ({in_norm})
        )
          AND UPPER(TABLE_SCHEMA) <> 'INFORMATION_SCHEMA'
        GROUP BY TABLE_SCHEMA, TABLE_NAME
        LIMIT {cand_limit}
    )
    SELECT
        c.TABLE_SCHEMA,
        c.TABLE_NAME,
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.ORDINAL_POSITION
    FROM {qident(db)}.INFORMATION_SCHEMA.COLUMNS c
    JOIN cand
      ON c.TABLE_SCHEMA = cand.TABLE_SCHEMA
     AND c.TABLE_NAME  = cand.TABLE_NAME
    ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
    """

    try:
        cols = norm_cols(session.sql(sql).to_pandas())
    except Exception:
        return None

    if cols.empty:
        return None

    best: Optional[Dict[str, Any]] = None
    best_score = -10**9

    hint_families = hints

    for (sch, tbl), g in cols.groupby(["TABLE_SCHEMA", "TABLE_NAME"], sort=False):
        g = g.copy()
        col_names = g["COLUMN_NAME"].astype(str).tolist()

        id_col_actual = None
        id_dtype = None
        hint_rank = 999

        for rank, hint in enumerate(hints):
            for _, r in g.iterrows():
                col = str(r["COLUMN_NAME"])
                if norm_name(col) == norm_name(hint):
                    id_col_actual = col
                    id_dtype = str(r["DATA_TYPE"])
                    hint_rank = rank
                    break
            if id_col_actual is not None:
                break

        if id_col_actual is None:
            for rank, hint in enumerate(hint_norms):
                for _, r in g.iterrows():
                    col = str(r["COLUMN_NAME"])
                    if norm_no_us(col) == hint:
                        id_col_actual = col
                        id_dtype = str(r["DATA_TYPE"])
                        hint_rank = rank
                        break
                if id_col_actual is not None:
                    break

        if id_col_actual is None:
            continue

        label_best = None
        label_best_score = -10**9

        for _, r in g.iterrows():
            col = str(r["COLUMN_NAME"])
            dt = str(r["DATA_TYPE"])
            if norm_no_us(col) == norm_no_us(id_col_actual):
                continue
            s = score_label_col(col, dt, hint_families)
            if s > label_best_score:
                label_best_score = s
                label_best = col

        if label_best is None or label_best_score < 110:
            continue

        detail_best = None
        detail_best_score = -10**9
        for _, r in g.iterrows():
            col = str(r["COLUMN_NAME"])
            dt = str(r["DATA_TYPE"])
            if norm_no_us(col) in (norm_no_us(id_col_actual), norm_no_us(label_best)):
                continue
            s = score_detail_col(col, dt, label_best, hint_families)
            if s > detail_best_score:
                detail_best_score = s
                detail_best = col

        if detail_best_score < 120:
            detail_best = None

        total = 0
        total += label_best_score
        total += schema_score(str(sch))
        total += table_entity_bonus(str(tbl), hint_families)
        total += table_penalty(str(tbl))
        total += max(0, 40 - hint_rank * 4)

        if total > best_score:
            best_score = total
            best = {
                "database": db,
                "schema": str(sch),
                "table": str(tbl),
                "id_col": str(id_col_actual),
                "label_col": str(label_best),
                "detail_col": str(detail_best) if detail_best else None,
                "where_clause": None,
                "id_dtype": str(id_dtype).upper() if id_dtype else None,
                "score": int(total),
                "resolver": "auto",
                "matched_hint": hints[hint_rank] if hint_rank < len(hints) else param_name,
            }

    return best


def apply_where_clause(base_select: str, id_col_expr: str, label_col_expr: str, detail_col_expr: Optional[str], where_clause: Optional[str], search: str) -> str:
    clauses = [f"{id_col_expr} IS NOT NULL"]
    wc = str(where_clause or "").strip()
    if wc:
        clauses.append(f"({wc})")

    s = (search or "").strip()
    if s:
        s_esc = esc_sql_str(s)
        search_parts = [
            f"TO_VARCHAR({label_col_expr}) ILIKE '%{s_esc}%'",
            f"TO_VARCHAR({id_col_expr}) ILIKE '%{s_esc}%'",
        ]
        if detail_col_expr:
            search_parts.insert(1, f"TO_VARCHAR({detail_col_expr}) ILIKE '%{s_esc}%'")
        clauses.append("(" + " OR ".join(search_parts) + ")")

    return base_select + "\nWHERE " + "\n  AND ".join(clauses) + "\n"


@st.cache_data(ttl=300, show_spinner=False)
def lookup_search_options(
    db: str,
    schema: str,
    table: str,
    id_col: str,
    label_col: str,
    detail_col: Optional[str],
    where_clause: Optional[str],
    search: str,
    limit: int,
    offset: int,
) -> pd.DataFrame:
    """
    Server-side lookup search with paging.
    Returns columns: ID, LABEL, DETAIL
    """
    lim = int(limit) if int(limit) > 0 else 250
    off = int(offset) if int(offset) >= 0 else 0

    idc = qident(id_col)
    lbl = qident(label_col)
    det = qident(detail_col) if detail_col else None
    fq = f"{qident(db)}.{qident(schema)}.{qident(table)}"

    if det:
        base_select = f"""
        SELECT
          {idc} AS ID,
          TO_VARCHAR({lbl}) AS LABEL,
          TO_VARCHAR({det}) AS DETAIL
        FROM {fq}
        """
        sql = apply_where_clause(base_select, idc, lbl, det, where_clause, search) + f"""
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {idc} ORDER BY TO_VARCHAR({lbl}), TO_VARCHAR({idc})) = 1
        ORDER BY LABEL, TO_VARCHAR(ID)
        LIMIT {lim + 1} OFFSET {off}
        """
    else:
        base_select = f"""
        SELECT
          {idc} AS ID,
          TO_VARCHAR({lbl}) AS LABEL,
          NULL AS DETAIL
        FROM {fq}
        """
        sql = apply_where_clause(base_select, idc, lbl, None, where_clause, search) + f"""
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {idc} ORDER BY TO_VARCHAR({lbl}), TO_VARCHAR({idc})) = 1
        ORDER BY LABEL, TO_VARCHAR(ID)
        LIMIT {lim + 1} OFFSET {off}
        """

    df = norm_cols(session.sql(sql).to_pandas())
    if df.empty:
        return pd.DataFrame(columns=["ID", "LABEL", "DETAIL"])

    for c in ["ID", "LABEL", "DETAIL"]:
        if c not in df.columns:
            df[c] = None

    df["LABEL"] = df["LABEL"].fillna(df["ID"].astype(str))
    has_next = len(df) > lim
    if has_next:
        df = df.iloc[:lim].copy()
    out = df[["ID", "LABEL", "DETAIL"]].reset_index(drop=True)
    out.attrs["has_next"] = has_next
    return out


@st.cache_data(ttl=600, show_spinner=False)
def lookup_fetch_one_by_id(
    db: str,
    schema: str,
    table: str,
    id_col: str,
    label_col: str,
    detail_col: Optional[str],
    where_clause: Optional[str],
    id_dtype: Optional[str],
    id_value: Any,
) -> Optional[Tuple[Any, str, Optional[str]]]:
    """
    Fetch label/detail for a single ID value so the selectbox can keep a stable selection
    even when paging/searching.
    """
    if id_value is None:
        return None

    idc = qident(id_col)
    lbl = qident(label_col)
    det = qident(detail_col) if detail_col else None
    fq = f"{qident(db)}.{qident(schema)}.{qident(table)}"
    id_lit = sql_literal(id_value, id_dtype or "VARCHAR")

    where_parts = [f"{idc} = {id_lit}"]
    wc = str(where_clause or "").strip()
    if wc:
        where_parts.append(f"({wc})")

    if det:
        sql = f"""
        SELECT
          {idc} AS ID,
          TO_VARCHAR({lbl}) AS LABEL,
          TO_VARCHAR({det}) AS DETAIL
        FROM {fq}
        WHERE {' AND '.join(where_parts)}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {idc} ORDER BY TO_VARCHAR({lbl})) = 1
        LIMIT 1
        """
    else:
        sql = f"""
        SELECT
          {idc} AS ID,
          TO_VARCHAR({lbl}) AS LABEL,
          NULL AS DETAIL
        FROM {fq}
        WHERE {' AND '.join(where_parts)}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {idc} ORDER BY TO_VARCHAR({lbl})) = 1
        LIMIT 1
        """

    try:
        df = norm_cols(session.sql(sql).to_pandas())
    except Exception:
        return None

    if df.empty:
        return None

    rid = df.iloc[0].get("ID")
    rlabel = str(df.iloc[0].get("LABEL") or "")
    rdetail = df.iloc[0].get("DETAIL")
    rdetail_s = str(rdetail) if rdetail is not None else None
    return (rid, rlabel, rdetail_s)


@st.cache_data(ttl=300, show_spinner=False)
def lookup_fetch_many_by_ids(
    db: str,
    schema: str,
    table: str,
    id_col: str,
    label_col: str,
    detail_col: Optional[str],
    where_clause: Optional[str],
    id_dtype: Optional[str],
    id_values: Tuple[Any, ...],
) -> pd.DataFrame:
    if not id_values:
        return pd.DataFrame(columns=["ID", "LABEL", "DETAIL"])

    idc = qident(id_col)
    lbl = qident(label_col)
    det = qident(detail_col) if detail_col else None
    fq = f"{qident(db)}.{qident(schema)}.{qident(table)}"

    lits = ", ".join([sql_literal(v, id_dtype or "VARCHAR") for v in id_values])

    where_parts = [f"{idc} IN ({lits})"]
    wc = str(where_clause or "").strip()
    if wc:
        where_parts.append(f"({wc})")

    if det:
        sql = f"""
        SELECT
          {idc} AS ID,
          TO_VARCHAR({lbl}) AS LABEL,
          TO_VARCHAR({det}) AS DETAIL
        FROM {fq}
        WHERE {' AND '.join(where_parts)}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {idc} ORDER BY TO_VARCHAR({lbl})) = 1
        ORDER BY TO_VARCHAR({lbl}), TO_VARCHAR({idc})
        """
    else:
        sql = f"""
        SELECT
          {idc} AS ID,
          TO_VARCHAR({lbl}) AS LABEL,
          NULL AS DETAIL
        FROM {fq}
        WHERE {' AND '.join(where_parts)}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {idc} ORDER BY TO_VARCHAR({lbl})) = 1
        ORDER BY TO_VARCHAR({lbl}), TO_VARCHAR({idc})
        """

    try:
        df = norm_cols(session.sql(sql).to_pandas())
    except Exception:
        return pd.DataFrame(columns=["ID", "LABEL", "DETAIL"])

    if df.empty:
        return pd.DataFrame(columns=["ID", "LABEL", "DETAIL"])

    for c in ["ID", "LABEL", "DETAIL"]:
        if c not in df.columns:
            df[c] = None

    df["LABEL"] = df["LABEL"].fillna(df["ID"].astype(str))
    return df[["ID", "LABEL", "DETAIL"]].reset_index(drop=True)


def format_lookup_label(id_val: Any, label: str, detail: Optional[str], show_ids: bool) -> str:
    base = (label or "").strip() or str(id_val)
    if show_ids and id_val is not None:
        base = f"{base} ({id_val})"
    if detail is None or str(detail).strip() == "" or str(detail).strip().upper() == "NONE":
        return base
    return f"{base} — {detail}"


def is_lookup_scalar_param(param_name: str, dtype: str, allow_code: bool) -> bool:
    if is_bool(dtype) or is_date(dtype) or is_time(dtype) or is_timestamp(dtype) or is_variantish(dtype):
        return False
    n = norm_no_us(param_name)
    if n.endswith("KEY") or n.endswith("ID"):
        return is_numeric(dtype) or is_stringish(dtype)
    if allow_code and n.endswith("CODE"):
        return is_stringish(dtype)
    return False


def is_lookup_list_param(param_name: str, dtype: str, allow_code: bool) -> bool:
    if not is_stringish(dtype):
        return False
    n = norm_no_us(param_name)
    if n.endswith("KEYLIST") or n.endswith("IDLIST"):
        return True
    if allow_code and n.endswith("CODELIST"):
        return True
    return False


# =========================================================
# Discovery: Databases / Schemas / Procedures
# =========================================================
@st.cache_data(ttl=600, show_spinner=False)
def get_databases_df() -> pd.DataFrame:
    df = norm_cols(run_show("SHOW DATABASES"))
    if df.empty:
        return pd.DataFrame(columns=["NAME", "COMMENT"])
    if "NAME" not in df.columns:
        df["NAME"] = df.iloc[:, 0].astype(str)
    if "COMMENT" not in df.columns:
        df["COMMENT"] = ""
    out = df[["NAME", "COMMENT"]].copy()
    out["NAME"] = out["NAME"].astype(str)
    out["COMMENT"] = out["COMMENT"].fillna("").astype(str)
    return out.sort_values("NAME").reset_index(drop=True)


@st.cache_data(ttl=600, show_spinner=False)
def get_schemas_df(db: str) -> pd.DataFrame:
    df = norm_cols(run_show(f"SHOW SCHEMAS IN DATABASE {qident(db)}"))
    if df.empty:
        return pd.DataFrame(columns=["NAME", "COMMENT"])
    if "NAME" not in df.columns:
        df["NAME"] = df.iloc[:, 0].astype(str)
    if "COMMENT" not in df.columns:
        df["COMMENT"] = ""
    out = df[["NAME", "COMMENT"]].copy()
    out["NAME"] = out["NAME"].astype(str)
    out["COMMENT"] = out["COMMENT"].fillna("").astype(str)
    return out.sort_values("NAME").reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False)
def get_procedures_df(db: str, schema: str) -> pd.DataFrame:
    df = norm_cols(run_show(f"SHOW PROCEDURES IN SCHEMA {qident(db)}.{qident(schema)}"))
    if df.empty:
        return df

    if "IS_BUILTIN" in df.columns:
        df = df[df["IS_BUILTIN"].astype(str).str.upper() != "Y"].copy()

    if "NAME" not in df.columns:
        df["NAME"] = df.iloc[:, 0].astype(str)
    if "ARGUMENTS" not in df.columns:
        df["ARGUMENTS"] = "()"
    if "COMMENT" not in df.columns:
        df["COMMENT"] = ""
    if "CREATED_ON" not in df.columns:
        df["CREATED_ON"] = ""

    df["NAME"] = df["NAME"].astype(str)
    df["ARGUMENTS"] = df["ARGUMENTS"].fillna("()").astype(str)
    df["COMMENT"] = df["COMMENT"].fillna("").astype(str)

    df["PROC_ID"] = df["NAME"].astype(str) + " " + df["ARGUMENTS"].astype(str)

    def _disp(r: pd.Series) -> str:
        nm = str(r.get("NAME") or "").strip()
        args = str(r.get("ARGUMENTS") or "").strip()
        cmt = str(r.get("COMMENT") or "").strip()
        base = f"{nm} {args}".strip()
        return f"{base} — {cmt}" if cmt else base

    df["DISPLAY"] = df.apply(_disp, axis=1)
    return df.sort_values(["NAME", "ARGUMENTS"]).reset_index(drop=True)


# =========================================================
# Export Metadata Overlay (optional friendly labels)
# =========================================================
@st.cache_data(ttl=600, show_spinner=False)
def find_export_metadata_tables(db: str) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT
      TABLE_SCHEMA,
      TABLE_NAME,
      COUNT(DISTINCT UPPER(COLUMN_NAME)) AS HIT_COUNT
    FROM {qident(db)}.INFORMATION_SCHEMA.COLUMNS
    WHERE UPPER(COLUMN_NAME) IN (
      'EXPORTREPORTCODE',
      'EXPORTREPORTNAME',
      'EXPORTREPORTDESCRIPTION',
      'EXPORTPARAMNAME',
      'EXPORTPARAMTOKEN',
      'EXPORTPARAMPROMPT'
    )
    GROUP BY TABLE_SCHEMA, TABLE_NAME
    HAVING COUNT(DISTINCT UPPER(COLUMN_NAME)) >= 3
    ORDER BY HIT_COUNT DESC, TABLE_SCHEMA, TABLE_NAME
    """
    try:
        df = norm_cols(session.sql(sql).to_pandas())
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        out.append(
            {
                "schema": str(r.get("TABLE_SCHEMA", "")),
                "table": str(r.get("TABLE_NAME", "")),
                "score": int(r.get("HIT_COUNT", 0)),
            }
        )
    return out


@st.cache_data(ttl=300, show_spinner=False)
def get_export_reports(db: str, meta_schema: str, meta_table: str) -> pd.DataFrame:
    fq = f"{qident(db)}.{qident(meta_schema)}.{qident(meta_table)}"
    sql = f"""
    SELECT DISTINCT
      ExportReportCode,
      ExportReportName
    FROM {fq}
    WHERE ExportReportCode IS NOT NULL
    ORDER BY ExportReportCode
    """
    try:
        return norm_cols(session.sql(sql).to_pandas())
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def get_export_params_for_report(db: str, meta_schema: str, meta_table: str, report_code: str) -> pd.DataFrame:
    fq = f"{qident(db)}.{qident(meta_schema)}.{qident(meta_table)}"
    sql = f"""
    SELECT
      ExportParamName,
      ExportParamToken,
      ExportParamPrompt
    FROM {fq}
    WHERE ExportReportCode = '{esc_sql_str(report_code)}'
    ORDER BY ExportParamToken, ExportParamName
    """
    try:
        return norm_cols(session.sql(sql).to_pandas())
    except Exception:
        return pd.DataFrame()


# =========================================================
# Harvest Params (DESCRIBE PROCEDURE)
# =========================================================
@st.cache_data(ttl=300, show_spinner=False)
def describe_procedure_df(db: str, schema: str, proc: str, type_signature: str) -> pd.DataFrame:
    fq = f"{qident(db)}.{qident(schema)}.{qident(proc)}"
    sql = f"DESCRIBE PROCEDURE {fq}{type_signature}"
    return norm_cols(session.sql(sql).to_pandas())


@st.cache_data(ttl=300, show_spinner=False)
def harvest_params(db: str, schema: str, proc: str, type_signature: str) -> List[Dict[str, Any]]:
    """
    Returns list of dicts: {"name":..., "type":..., "default": Optional[str]}
    Priority:
      1) DESCRIBE PROCEDURE property signature
      2) DESCRIBE PROCEDURE property arguments
      3) DESCRIBE rows NAME/TYPE
      4) fallback from type_signature => ARG1/ARG2...
    """
    try:
        df = describe_procedure_df(db, schema, proc, type_signature)
    except Exception:
        df = pd.DataFrame()

    if not df.empty and "PROPERTY" in df.columns and "VALUE" in df.columns:
        sig_rows = df[df["PROPERTY"].astype(str).str.lower() == "signature"]
        if not sig_rows.empty:
            parsed = parse_describe_signature_value(str(sig_rows.iloc[0]["VALUE"]))
            if parsed:
                return parsed

        arg_rows = df[df["PROPERTY"].astype(str).str.lower() == "arguments"]
        if not arg_rows.empty:
            parsed = parse_describe_signature_value(str(arg_rows.iloc[0]["VALUE"]))
            if parsed:
                return parsed

    if not df.empty and {"NAME", "TYPE"}.issubset(df.columns):
        params: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            n = str(r.get("NAME") or "").strip()
            t = str(r.get("TYPE") or "").strip()
            if n and t and n.upper() not in ("NULL",):
                params.append({"name": n.strip('"'), "type": t.upper(), "default": None})
        if params:
            return params

    inner = type_signature[1:-1].strip() if (type_signature or "").startswith("(") else ""
    types = split_top_level_commas(inner) if inner else []
    return [{"name": f"ARG{i+1}", "type": types[i].upper(), "default": None} for i in range(len(types))]


# =========================================================
# Input Helpers
# =========================================================
def id_hint_for_param(real_param_name: str, overlay_active: bool, friendly_overlay: Dict[str, Dict[str, str]]) -> str:
    """
    What identifier name should we resolve for lookups?
    Priority:
      1) overlay token (e.g. @BusinessKey)
      2) overlay label
      3) actual parameter name
    """
    if overlay_active and real_param_name in friendly_overlay:
        tok = str(friendly_overlay[real_param_name].get("token") or "").strip()
        lab = str(friendly_overlay[real_param_name].get("label") or "").strip()
        if tok.startswith("@") and len(tok) > 1:
            return tok[1:]
        if tok:
            return tok
        if lab:
            return lab
    return real_param_name


def safe_param_label(real_name: str, overlay_active: bool, friendly_overlay: Dict[str, Dict[str, str]]) -> Tuple[str, str]:
    """
    Return (label, help_text) for a parameter.
    """
    label = real_name
    help_txt = ""

    if overlay_active and real_name in friendly_overlay:
        label = friendly_overlay[real_name].get("label") or real_name
        help_txt = friendly_overlay[real_name].get("prompt") or ""

    if CFG_HUMANIZE_NAMES and (not overlay_active or real_name not in friendly_overlay):
        label = humanize_identifier(label) or real_name

    return label, help_txt


def bool_tri_state_widget(label: str, key: str, help_txt: str) -> Optional[bool]:
    options = [None, True, False]

    def _fmt(v):
        if v is None:
            return "NULL"
        return "TRUE" if v else "FALSE"

    return st.selectbox(label, options=options, format_func=_fmt, key=key, help=help_txt)


# =========================================================
# Lookup Widget Rendering Helpers
# =========================================================
def bool_tri_state_widget(label: str, key: str, help_txt: str) -> Optional[bool]:
    options = [None, True, False]

    def _fmt(v):
        if v is None:
            return "No value selected"
        return "True" if v else "False"

    return st.selectbox(label, options=options, format_func=_fmt, key=key, help=help_txt)


def _lookup_option_records(opt_df: pd.DataFrame) -> List[Dict[str, Any]]:
    records = []
    if opt_df is None or opt_df.empty:
        return records
    for _, r in opt_df.iterrows():
        records.append({
            'id': r.get('ID'),
            'label': r.get('LABEL'),
            'detail': r.get('DETAIL'),
            'code': r.get('CODE') if 'CODE' in opt_df.columns else None,
        })
    return records


def render_lookup_single(src: Dict[str, Any], widget_base_key: str, label: str, help_txt: str) -> Tuple[Any, Dict[str, Any], str]:
    search_state_key = f"{widget_base_key}::lkp_search"
    page_state_key = f"{widget_base_key}::lkp_page"
    page_size_state_key = f"{widget_base_key}::lkp_page_size"
    selected_id_key = f"{widget_base_key}::selected_id"
    st.session_state.setdefault(search_state_key, "")
    st.session_state.setdefault(page_state_key, 0)
    st.session_state.setdefault(page_size_state_key, int(CFG_LOOKUP_PAGE_SIZE))

    cur_search = str(st.session_state[search_state_key] or "").strip()
    cur_page = int(st.session_state[page_state_key] or 0)
    cur_limit = int(st.session_state[page_size_state_key] or CFG_LOOKUP_PAGE_SIZE)
    cur_offset = cur_page * cur_limit

    try:
        opt_df = lookup_search_options(db=src['database'], schema=src['schema'], table=src['table'], id_col=src['id_col'], label_col=src['label_col'], detail_col=src.get('detail_col'), where_clause=src.get('where_clause'), search=cur_search, limit=cur_limit, offset=cur_offset)
    except Exception:
        opt_df = pd.DataFrame(columns=['ID','LABEL','DETAIL'])

    has_next = bool(getattr(opt_df, 'attrs', {}).get('has_next', False))
    option_records = _lookup_option_records(opt_df)
    label_map = disambiguate_lookup_labels(option_records, show_technical_ids=CFG_SHOW_IDS_IN_LOOKUPS)
    display_map = {o['id']: label_map[o['id']] for o in option_records}

    prior_selected = st.session_state.get(selected_id_key, None)
    if prior_selected is not None and prior_selected not in display_map:
        fetched = lookup_fetch_one_by_id(db=src['database'], schema=src['schema'], table=src['table'], id_col=src['id_col'], label_col=src['label_col'], detail_col=src.get('detail_col'), where_clause=src.get('where_clause'), id_dtype=src.get('id_dtype'), id_value=prior_selected)
        if fetched:
            fid, flabel, fdetail = fetched
            display_map[fid] = disambiguate_lookup_labels([{'id': fid, 'label': flabel, 'detail': fdetail, 'code': None}], show_technical_ids=CFG_SHOW_IDS_IN_LOOKUPS)[fid]

    with st.expander(f"Browse {label}", expanded=False):
        search_val = st.text_input(f"Search {label}", value=cur_search, key=f"{widget_base_key}::search_input", placeholder="Search name, description, code, or ID", help=help_txt)
        nav = st.columns([1,1,3])
        if nav[0].button('⬅️ Prev', key=f"{widget_base_key}::prev", disabled=cur_page == 0):
            st.session_state[page_state_key] = max(0, cur_page - 1)
            st.rerun()
        if nav[1].button('➡️ Next', key=f"{widget_base_key}::next", disabled=not has_next):
            st.session_state[page_state_key] = cur_page + 1
            st.rerun()
        if search_val != cur_search:
            st.session_state[search_state_key] = search_val.strip()
            st.session_state[page_state_key] = 0
            st.rerun()
        start_row = 0 if not option_records else cur_offset + 1
        end_row = cur_offset + len(option_records)
        nav[2].caption(f"Showing {start_row}–{end_row}" if option_records else 'No results on this page')
        st.caption(f"Source: {src['database']}.{src['schema']}.{src['table']} · resolver={src.get('resolver','auto')}")

    option_ids = [None] + [o['id'] for o in option_records]
    chosen_id = st.selectbox(label, options=option_ids, index=0 if prior_selected is None or prior_selected not in option_ids else option_ids.index(prior_selected), format_func=lambda v: '— Select —' if v is None else display_map.get(v, str(v)), key=f"{widget_base_key}::lkp_select", help=help_txt)
    st.session_state[selected_id_key] = chosen_id
    selected_display = '' if chosen_id is None else display_map.get(chosen_id, str(chosen_id))
    if selected_display:
        st.caption(f"Selected {label}: {selected_display}")
    return chosen_id, {'resolver': src.get('resolver'), 'matched_hint': src.get('matched_hint'), 'source': f"{src['database']}.{src['schema']}.{src['table']}", 'search': cur_search, 'page': cur_page, 'returned': len(option_records), 'has_next': has_next}, selected_display


def render_lookup_multi(src: Dict[str, Any], widget_base_key: str, label: str, help_txt: str) -> Tuple[str, str]:
    plural = label if label.endswith('s') else f"{label}s"
    search_state_key = f"{widget_base_key}::lkp_search"
    page_state_key = f"{widget_base_key}::lkp_page"
    selected_ids_key = f"{widget_base_key}::selected_ids"
    st.session_state.setdefault(search_state_key, '')
    st.session_state.setdefault(page_state_key, 0)
    st.session_state.setdefault(selected_ids_key, [])
    cur_search = str(st.session_state[search_state_key] or '').strip()
    cur_page = int(st.session_state[page_state_key] or 0)
    cur_limit = int(CFG_LOOKUP_PAGE_SIZE)
    cur_offset = cur_page * cur_limit
    try:
        opt_df = lookup_search_options(db=src['database'], schema=src['schema'], table=src['table'], id_col=src['id_col'], label_col=src['label_col'], detail_col=src.get('detail_col'), where_clause=src.get('where_clause'), search=cur_search, limit=cur_limit, offset=cur_offset)
    except Exception:
        opt_df = pd.DataFrame(columns=['ID','LABEL','DETAIL'])
    has_next = bool(getattr(opt_df, 'attrs', {}).get('has_next', False))
    option_records = _lookup_option_records(opt_df)
    label_map = disambiguate_lookup_labels(option_records, show_technical_ids=CFG_SHOW_IDS_IN_LOOKUPS)
    display_map = {o['id']: label_map[o['id']] for o in option_records}
    selected_ids = list(st.session_state.get(selected_ids_key, []))
    with st.expander(f"Browse {plural}", expanded=False):
        search_val = st.text_input(f"Search {plural}", value=cur_search, key=f"{widget_base_key}::search_input", placeholder="Search name, description, code, or ID")
        nav = st.columns([1,1,3])
        if nav[0].button('⬅️ Prev', key=f"{widget_base_key}::prev", disabled=cur_page == 0):
            st.session_state[page_state_key] = max(0, cur_page - 1)
            st.rerun()
        if nav[1].button('➡️ Next', key=f"{widget_base_key}::next", disabled=not has_next):
            st.session_state[page_state_key] = cur_page + 1
            st.rerun()
        if search_val != cur_search:
            st.session_state[search_state_key] = search_val.strip()
            st.session_state[page_state_key] = 0
            st.rerun()
        nav[2].caption(f"Showing {cur_offset + 1 if option_records else 0}–{cur_offset + len(option_records)}")

    add_choice = st.selectbox(f"Add {plural}", options=[None] + [o['id'] for o in option_records], format_func=lambda v: '— Select —' if v is None else display_map.get(v, str(v)), key=f"{widget_base_key}::lkp_picker", help=help_txt)
    action_cols = st.columns([1,1,2])
    if action_cols[0].button('➕ Add', key=f"{widget_base_key}::add_one") and add_choice is not None and add_choice not in selected_ids:
        selected_ids.append(add_choice)
        st.session_state[selected_ids_key] = selected_ids
        st.rerun()
    if action_cols[1].button('🧹 Clear', key=f"{widget_base_key}::clear_all"):
        st.session_state[selected_ids_key] = []
        st.rerun()
    summary = ', '.join(display_map.get(v, str(v)) for v in selected_ids)
    st.caption(f"Selected {plural}: {summary}" if selected_ids else f"Selected {plural}: none")
    if selected_ids:
        remove_choice = st.selectbox(f"Remove {label}", options=[None] + selected_ids, format_func=lambda v: '— Select —' if v is None else display_map.get(v, str(v)), key=f"{widget_base_key}::lkp_remove")
        if st.button('➖ Remove', key=f"{widget_base_key}::remove_one") and remove_choice is not None:
            st.session_state[selected_ids_key] = [x for x in selected_ids if x != remove_choice]
            st.rerun()
    return ','.join(str(v) for v in st.session_state.get(selected_ids_key, [])), summary


# =========================================================
# Session State: History
# =========================================================
if "run_history" not in st.session_state:
    st.session_state["run_history"] = []


def push_history(entry: Dict[str, Any]) -> None:
    st.session_state["run_history"] = [entry] + st.session_state["run_history"]
    st.session_state["run_history"] = st.session_state["run_history"][:25]


# =========================================================
# UI: Tabs
# =========================================================
tab_report, tab_history, tab_debug = st.tabs(["Run Report", "Recent Runs", "Diagnostics"])


# =========================================================
# Report Builder Tab
# =========================================================
with tab_report:
    db_df = get_databases_df()
    if db_df.empty:
        st.error("No databases visible to this session.")
        st.stop()

    db_map_comment = {r["NAME"]: r.get("COMMENT", "") for _, r in db_df.iterrows()}
    db = st.selectbox("Database", options=db_df["NAME"].astype(str).tolist(), format_func=lambda n: f"{n} — {db_map_comment.get(n,'').strip()}" if (db_map_comment.get(n) or '').strip() else n, key='db_select')
    schema_df = get_schemas_df(db)
    if schema_df.empty:
        st.error("No schemas visible in this database (or insufficient privileges).")
        st.stop()
    sch_map_comment = {r["NAME"]: r.get("COMMENT", "") for _, r in schema_df.iterrows()}
    schema = st.selectbox("Schema", options=schema_df["NAME"].astype(str).tolist(), format_func=lambda n: f"{n} — {sch_map_comment.get(n,'').strip()}" if (sch_map_comment.get(n) or '').strip() else n, key='schema_select')

    procs_df = get_procedures_df(db, schema)
    if procs_df.empty:
        st.warning("No procedures found in this schema.")
        st.stop()

    proc_search = st.text_input('Search reports', value='', placeholder='Search by report name, description, comment, procedure, or schema', key='proc_search')
    meta_candidates = find_export_metadata_tables(db) if CFG_ENABLE_METADATA_OVERLAY else []
    export_row_by_proc = {}
    export_params_rows: List[Dict[str, Any]] = []
    chosen_report_code = ''
    if meta_candidates:
        picked = meta_candidates[0]
        reports_df = get_export_reports(db, picked['schema'], picked['table'])
        if not reports_df.empty and 'EXPORTREPORTCODE' in reports_df.columns:
            report_options = [f"{str(r.get('EXPORTREPORTNAME') or r.get('EXPORTREPORTCODE') or '').strip()} — {str(r.get('EXPORTREPORTCODE') or '').strip()}" for _, r in reports_df.iterrows()]
            chosen_report_label = st.selectbox('Report metadata', options=['(None)'] + report_options, key='report_pick')
            if chosen_report_label != '(None)':
                chosen_report_row = reports_df.iloc[report_options.index(chosen_report_label)]
                chosen_report_code = str(chosen_report_row.get('EXPORTREPORTCODE') or '').strip()
                export_row_by_proc = {str(r.get('NAME') or '').upper(): chosen_report_row.to_dict() for _, r in procs_df.iterrows()}
                export_params_df = get_export_params_for_report(db, picked['schema'], picked['table'], chosen_report_code)
                export_params_rows = export_params_df.to_dict('records') if not export_params_df.empty else []

    proc_options = []
    for _, row in procs_df.iterrows():
        proc_name = str(row.get('NAME') or '').strip()
        type_sig = signature_types_only(first_paren_group(str(row.get('ARGUMENTS') or '()')))
        proc_meta = resolve_proc_ui_meta(db, schema, proc_name, type_sig, proc_comment=str(row.get('COMMENT') or ''), export_row=export_row_by_proc.get(proc_name.upper()))
        display = proc_meta.display_name
        if str(row.get('COMMENT') or '').strip() and display != str(row.get('COMMENT') or '').strip():
            display = f"{display} — {str(row.get('COMMENT') or '').strip()}"
        proc_options.append((str(row['PROC_ID']), display, proc_meta))
    if proc_search.strip():
        search = proc_search.strip().lower()
        proc_options = [p for p in proc_options if search in p[1].lower() or search in p[2].proc_name.lower() or search in p[2].schema.lower()]
    if not proc_options:
        st.info('No reports match your search.')
        st.stop()
    selected_proc_id = st.selectbox('Report', options=[p[0] for p in proc_options], format_func=lambda pid: next(x[1] for x in proc_options if x[0] == pid), key='proc_select')
    proc_row = procs_df[procs_df['PROC_ID'] == selected_proc_id].iloc[0]
    proc_name = str(proc_row.get('NAME') or '').strip()
    raw_args = str(proc_row.get('ARGUMENTS') or '').strip()
    proc_comment = str(proc_row.get('COMMENT') or '').strip()
    proc_created_on = str(proc_row.get('CREATED_ON') or '').strip()
    type_sig = signature_types_only(first_paren_group(raw_args))
    proc_meta = next(x[2] for x in proc_options if x[0] == selected_proc_id)
    proc_key = proc_instance_key(db, schema, proc_name, type_sig)

    st.header(proc_meta.display_name)
    st.caption(proc_meta.description or proc_comment or 'Run the selected report with business-friendly parameters.')
    with st.expander('Technical details', expanded=False):
        st.code(f'{db}.{schema}.{proc_name}{type_sig}')
        st.caption(f'Created: {proc_created_on or "—"}')

    params = harvest_params(db, schema, proc_name, type_sig) or []
    meta_mapping = match_export_metadata(params, export_params_rows) if export_params_rows else {}
    ui_params = [resolve_param_ui_meta(proc_meta, p, idx, overlay_row=meta_mapping.get(str(p.get('name') or f'ARG{idx}')), allow_null=CFG_ALLOW_NULLS) for idx, p in enumerate(params, start=1)]
    use_named_args_for_call = bool(params) and all(not is_generic_arg_name(str(p.get('name') or '')) for p in params)
    validation_errors: Dict[str, str] = {}
    submissions: List[Tuple[str, ParamSubmission]] = []
    submission_display: List[str] = []
    lookup_debug_rows = []

    for p, ui_meta in zip(params, ui_params):
        real_name = ui_meta.param_name
        ptype = (p.get('type') or '').upper().strip()
        widget_base_key = f'param::{proc_key}::{real_name}'
        st.markdown(f"**{ui_meta.display_label}**  \n<span class='small-muted'>{real_name} · {ptype}</span>", unsafe_allow_html=True)
        badges = []
        if ui_meta.default_mode == 'proc_default':
            badges.append('Uses procedure default')
        if ui_meta.allow_null:
            badges.append('NULL allowed')
        if ui_meta.required:
            badges.append('Required')
        if badges:
            st.caption(' · '.join(badges))
        if ui_meta.help_text:
            st.caption(ui_meta.help_text)

        mode_options = ['Set value']
        if ui_meta.default_mode == 'proc_default':
            mode_options = ['Use procedure default', 'Set value']
        if ui_meta.allow_null:
            mode_options.append('Pass NULL')
        mode_label = st.radio(f"{ui_meta.display_label} mode", options=mode_options, horizontal=True, key=f"{widget_base_key}::mode", label_visibility='collapsed')
        mode = 'DEFAULT' if mode_label == 'Use procedure default' else ('NULL' if mode_label == 'Pass NULL' else 'VALUE')
        raw_value = None
        display_value = ''
        lookup_name = ui_meta.lookup_key or normalize_name(real_name)
        use_scalar_lookup = CFG_SMART_LOOKUPS and is_lookup_scalar_param(lookup_name, ptype, CFG_SMART_LOOKUPS_FOR_STRING_CODE)
        use_list_lookup = CFG_ENABLE_LIST_LOOKUPS and is_lookup_list_param(lookup_name, ptype, CFG_SMART_LOOKUPS_FOR_STRING_CODE)

        if mode == 'VALUE':
            src = None
            if use_scalar_lookup or use_list_lookup:
                discovered = discover_best_lookup_source(db=db, param_name=lookup_name, cand_limit=int(CFG_LOOKUP_DISCOVERY_TABLE_LIMIT), use_lookup_config=bool(CFG_USE_LOOKUP_CONFIG), config_fqn=CFG_LOOKUP_CONFIG_FQN)
                lkp_meta = resolve_lookup_ui_meta(proc_meta, ui_meta, real_name, config_match=discovered if discovered and discovered.get('resolver') == 'config' else None, discovered_match=discovered)
                src = discovered if lkp_meta else None
            if src and use_list_lookup:
                raw_value, display_value = render_lookup_multi(src, widget_base_key, ui_meta.display_label, ui_meta.help_text)
            elif src:
                raw_value, dbg, display_value = render_lookup_single(src, widget_base_key, ui_meta.display_label, ui_meta.help_text)
                lookup_debug_rows.append({'Param': real_name, 'Label': ui_meta.display_label, **dbg})
            elif is_bool(ptype):
                raw_value = bool_tri_state_widget(ui_meta.display_label, key=f"{widget_base_key}::bool", help_txt=ui_meta.help_text)
                display_value = '' if raw_value is None else str(raw_value)
            elif is_numeric(ptype):
                raw_value = st.text_input(ui_meta.display_label, value=st.session_state.get(f"{widget_base_key}::num", ''), key=f"{widget_base_key}::num", placeholder='Enter a number')
                display_value = str(raw_value).strip()
                if display_value == '':
                    mode = 'UNSET'
                else:
                    raw_value = float(display_value) if '.' in display_value else int(display_value)
            elif is_date(ptype):
                raw_value = st.text_input(ui_meta.display_label, value=st.session_state.get(f"{widget_base_key}::date", ''), key=f"{widget_base_key}::date", placeholder='YYYY-MM-DD')
                display_value = str(raw_value).strip()
                if display_value == '':
                    mode = 'UNSET'
            elif is_time(ptype):
                raw_value = st.text_input(ui_meta.display_label, value=st.session_state.get(f"{widget_base_key}::time", ''), key=f"{widget_base_key}::time", placeholder='HH:MM:SS')
                display_value = str(raw_value).strip()
                if display_value == '':
                    mode = 'UNSET'
            elif is_timestamp(ptype):
                raw_value = st.text_input(ui_meta.display_label, value=st.session_state.get(f"{widget_base_key}::ts", ''), key=f"{widget_base_key}::ts", placeholder='YYYY-MM-DD HH:MM:SS')
                display_value = str(raw_value).strip()
                if display_value == '':
                    mode = 'UNSET'
            elif is_variantish(ptype):
                raw_value = st.text_area(ui_meta.display_label, value=st.session_state.get(f"{widget_base_key}::json", ''), key=f"{widget_base_key}::json", placeholder='{"key":"value"}')
                display_value = str(raw_value).strip()
                if display_value == '':
                    mode = 'UNSET'
            else:
                raw_value = st.text_input(ui_meta.display_label, value=st.session_state.get(f"{widget_base_key}::txt", ''), key=f"{widget_base_key}::txt", placeholder=ui_meta.placeholder or f'Enter {ui_meta.display_label.lower()}')
                display_value = str(raw_value).strip()
                if display_value == '' and ui_meta.required:
                    mode = 'UNSET'

        submission = build_param_submission(mode, raw_value, ptype, display_value=display_value)
        if submission.mode == 'VALUE' and is_variantish(ptype) and raw_value not in (None, ''):
            json_err = validate_variant_json(raw_value)
            if json_err:
                submission = ParamSubmission(submission.mode, raw_value, None, display_value, False, json_err)
        if submission.mode == 'VALUE' and is_timestamp(ptype) and raw_value not in (None, '') and submission.sql_literal is None:
            submission = ParamSubmission(submission.mode, raw_value, None, display_value, False, 'Invalid timestamp value.')
        if submission.mode == 'UNSET' and not ui_meta.required and ui_meta.default_mode == 'proc_default':
            submission = build_param_submission('DEFAULT', None, ptype, display_value='Use procedure default')
        if not submission.is_valid:
            validation_errors[real_name] = submission.validation_error
            st.error(f"{ui_meta.display_label}: {submission.validation_error}")
        elif submission.display_value:
            submission_display.append(f"{ui_meta.display_label}: {submission.display_value}")
        submissions.append((real_name, submission))
        st.markdown('<hr class="hr-soft"/>', unsafe_allow_html=True)

    try:
        call_sql = build_call_sql(db, schema, proc_name, submissions, named_args=use_named_args_for_call)
    except Exception as exc:
        call_sql = f'-- invalid call: {exc}'
        validation_errors['call_sql'] = str(exc)

    st.markdown('### Run summary')
    if submission_display:
        for line in submission_display:
            st.write(f'• {line}')
    else:
        st.caption('No parameter values selected yet.')
    with st.expander('Generated CALL SQL', expanded=False):
        st.code(call_sql)
        if use_named_args_for_call:
            st.caption('Using named arguments because the procedure signature exposed real parameter names.')
        else:
            st.caption('Using positional arguments because Snowflake only exposed generic names such as ARG1/ARG2 for this procedure signature.')

    run_disabled = bool(validation_errors) or any(s.mode == 'UNSET' for _, s in submissions)
    if validation_errors:
        st.warning('Fix the parameter errors below before running the report.')
    run_row = st.columns([1.2,1.2,4])
    run_clicked = run_row[0].button('🚀 Run report', type='primary', disabled=run_disabled)
    clear_clicked = run_row[1].button('🧹 Clear inputs')
    run_row[2].caption('The report runs only after you click Run report.')

    if clear_clicked:
        keys_to_clear = [k for k in list(st.session_state.keys()) if k.startswith(f'param::{proc_key}::')]
        for k in keys_to_clear:
            del st.session_state[k]
        st.success('Cleared inputs for this report.')
        st.stop()

    if run_clicked:
        try:
            with st.spinner('Running report…'):
                t0 = _time.perf_counter()
                out_df = session.sql(call_sql).to_pandas()
                duration_s = _time.perf_counter() - t0
            out_df = norm_cols(out_df) if isinstance(out_df, pd.DataFrame) else pd.DataFrame()
            qid = None
            try:
                qid = session.sql('SELECT LAST_QUERY_ID() AS QID').to_pandas().iloc[0]['QID']
            except Exception:
                pass
            st.session_state['last_run'] = {'when': datetime.utcnow().isoformat(timespec='seconds') + 'Z', 'db': db, 'schema': schema, 'proc': proc_name, 'proc_instance_key': proc_key, 'display_name': proc_meta.display_name, 'type_sig': type_sig, 'sql': call_sql, 'summary': submission_display, 'duration_s': duration_s, 'rows': int(len(out_df)), 'cols': int(len(out_df.columns)), 'query_id': qid}
            st.session_state['last_result_df'] = out_df
            push_history(st.session_state['last_run'])
            st.success(f'Execution successful · {len(out_df):,} rows · {duration_s:.2f}s')
        except Exception as e:
            err_info = analyze_execution_error(e)
            qid = None
            try:
                qid = session.sql('SELECT LAST_QUERY_ID() AS QID').to_pandas().iloc[0]['QID']
            except Exception:
                pass
            failure_run = {
                'when': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
                'db': db,
                'schema': schema,
                'proc': proc_name,
                'proc_instance_key': proc_key,
                'display_name': proc_meta.display_name,
                'type_sig': type_sig,
                'sql': call_sql,
                'summary': submission_display,
                'duration_s': None,
                'rows': 0,
                'cols': 0,
                'query_id': qid,
                'status': 'failed',
                'error_summary': err_info.get('summary') or 'Execution failed',
            }
            st.session_state['last_run'] = failure_run
            push_history(failure_run)
            st.error(err_info.get('summary') or 'Execution failed')
            for detail in err_info.get('details') or []:
                st.caption(detail)
            if err_info.get('hint'):
                st.info(err_info['hint'])
            if qid:
                st.caption(f'Last query ID: {qid}')
            with st.expander('Raw Snowflake error', expanded=False):
                st.code(err_info.get('raw_message') or str(e))

    if 'last_run' in st.session_state and 'last_result_df' in st.session_state:
        lr = st.session_state['last_run']
        df_res: pd.DataFrame = st.session_state['last_result_df']
        st.markdown('### Results')
        metrics = st.columns([1,1,1,2])
        metrics[0].metric('Rows', f"{lr.get('rows',0):,}")
        metrics[1].metric('Columns', f"{lr.get('cols',0):,}")
        metrics[2].metric('Runtime', f"{lr.get('duration_s',0.0):.2f}s")
        metrics[3].write(f"**Query ID:** {lr.get('query_id') or '—'}")
        if isinstance(df_res, pd.DataFrame) and not df_res.empty:
            st.dataframe(df_res, use_container_width=True, height=520)
            safe_name = re.sub(r'[^A-Za-z0-9]+', '_', proc_meta.display_name).strip('_') or proc_name
            st.download_button('⬇️ Download CSV', data=df_res.to_csv(index=False).encode('utf-8'), file_name=f"{safe_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}Z.csv", mime='text/csv')
        else:
            st.info('Procedure executed, but returned no rows (or no tabular output).')

# =========================================================
# History Tab
# =========================================================
with tab_history:
    st.subheader("Recent Runs")
    hist = st.session_state.get("run_history", [])
    if not hist:
        st.info("No runs yet.")
    else:
        hist_df = pd.DataFrame(hist)
        show_cols = [c for c in ["when", "display_name", "db", "schema", "proc", "rows", "cols", "duration_s", "query_id"] if c in hist_df.columns]
        st.dataframe(hist_df[show_cols], use_container_width=True, height=320)

        with st.expander("Details (SQL for recent runs)", expanded=False):
            for i, h in enumerate(hist[:10], start=1):
                st.markdown(f"**{i}. {h.get('when','')} · {h.get('display_name', h.get('proc',''))}**")
                st.caption(
                    f"Rows={h.get('rows','')} · Runtime={h.get('duration_s',''):.2f}s"
                    if isinstance(h.get("duration_s"), (int, float))
                    else ""
                )
                st.code(h.get("sql", ""))
                st.markdown("---")


# =========================================================
# Debug Tab
# =========================================================
with tab_debug:
    st.subheader("Diagnostics")
    if not CFG_SHOW_DEBUG:
        st.info("Enable **Show debug tools** in the sidebar to see debug panels.")
    else:
        st.markdown("#### Procedure metadata + DESCRIBE output")
        st.write("Procedure selected:")
        st.code(f"{db}.{schema}.{proc_name}")
        st.write("SHOW PROCEDURES raw ARGUMENTS:")
        st.code(raw_args or "(empty)")
        st.write("DESCRIBE signature used (types-only):")
        st.code(type_sig)

        cfg_status = lookup_config_status(db, CFG_LOOKUP_CONFIG_FQN)
        st.markdown("#### Lookup config status")
        st.json(cfg_status)

        if cfg_status.get("exists"):
            cfg_df = get_lookup_config_df(db, CFG_LOOKUP_CONFIG_FQN)
            st.write("Lookup config preview:")
            st.dataframe(cfg_df.head(50), use_container_width=True, height=260)

        try:
            ddf = describe_procedure_df(db, schema, proc_name, type_sig)
            st.write("DESCRIBE PROCEDURE output:")
            st.dataframe(ddf, use_container_width=True, height=360)
        except Exception as e:
            st.warning("Could not run DESCRIBE PROCEDURE (privileges or signature issue).")
            st.code(str(e))

        st.markdown("#### Lookup debug")
        if lookup_debug_rows:
            st.dataframe(pd.DataFrame(lookup_debug_rows), use_container_width=True, height=320)
        else:
            st.caption("No lookup attempts yet (or no KEY/ID params).")
