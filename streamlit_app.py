
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
    st.header("⚙️ Settings")

    CFG_SMART_LOOKUPS = st.checkbox("Smart lookups for KEY/ID params", value=True)
    CFG_ENABLE_LIST_LOOKUPS = st.checkbox("Smart lookups for *KEYLIST/*IDLIST params", value=True)
    CFG_SMART_LOOKUPS_FOR_STRING_CODE = st.checkbox("Also try lookups for *CODE params", value=False)

    CFG_ALLOW_NULLS = st.checkbox("Allow NULL for any parameter", value=True)
    CFG_EMPTY_TEXT_AS_NULL = st.checkbox("Treat empty text/JSON as NULL", value=True)

    CFG_SHOW_IDS_IN_LOOKUPS = st.checkbox("Show IDs in lookup labels", value=True)

    st.divider()
    st.subheader("Lookup Authority")
    CFG_USE_LOOKUP_CONFIG = st.checkbox(
        "Prefer lookup config table",
        value=True,
        help="If the config table exists and contains a matching row, it overrides auto-discovery.",
    )
    CFG_LOOKUP_CONFIG_FQN = st.text_input(
        "Lookup config table",
        value=DEFAULT_LOOKUP_CONFIG_FQN,
        help="2-part or 3-part name. Example: EXPORT.PARAM_LOOKUP_CONFIG or MYDB.EXPORT.PARAM_LOOKUP_CONFIG",
    )

    st.divider()
    st.subheader("Lookup Performance")
    CFG_LOOKUP_PAGE_SIZE = st.number_input("Lookup page size", min_value=25, max_value=5000, value=250, step=25)
    CFG_LOOKUP_DISCOVERY_TABLE_LIMIT = st.number_input(
        "Lookup discovery candidate tables",
        min_value=50,
        max_value=1200,
        value=400,
        step=25,
        help="How many candidate tables/views we consider when auto-discovering lookup sources.",
    )

    st.divider()
    st.subheader("CALL Behavior")
    CFG_USE_NAMED_ARGS = st.checkbox("Use named arguments in CALL", value=True)

    st.divider()
    st.subheader("UI")
    CFG_HUMANIZE_NAMES = st.checkbox("Humanize identifiers in UI", value=True)
    CFG_ENABLE_METADATA_OVERLAY = st.checkbox("Enable Export metadata overlay", value=True)

    st.divider()
    CFG_SHOW_DEBUG = st.checkbox("Show debug tools", value=False)


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


def is_numeric(dtype: str) -> bool:
    d = (dtype or "").upper()
    return any(
        x in d
        for x in [
            "NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT",
            "TINYINT", "BYTEINT", "FLOAT", "DOUBLE", "REAL"
        ]
    )


def is_bool(dtype: str) -> bool:
    d = (dtype or "").upper()
    return "BOOLEAN" in d or d == "BOOL"


def is_date(dtype: str) -> bool:
    return (dtype or "").upper().startswith("DATE")


def is_time(dtype: str) -> bool:
    return (dtype or "").upper().startswith("TIME")


def is_timestamp(dtype: str) -> bool:
    d = (dtype or "").upper()
    return d.startswith("TIMESTAMP") or d.startswith("DATETIME")


def is_variantish(dtype: str) -> bool:
    d = (dtype or "").upper()
    return any(x in d for x in ["VARIANT", "OBJECT", "ARRAY"])


def is_stringish(dtype: str) -> bool:
    d = (dtype or "").upper()
    return any(x in d for x in ["VARCHAR", "CHAR", "TEXT", "STRING"])


def sql_literal(value: Any, dtype: str) -> str:
    """
    Convert widget values into a Snowflake SQL literal/expression.
    """
    dtype_u = (dtype or "").upper()

    if value is None:
        return "NULL"

    if is_bool(dtype_u):
        return "TRUE" if bool(value) else "FALSE"

    if is_numeric(dtype_u):
        return str(value)

    if is_date(dtype_u):
        if isinstance(value, date):
            return f"DATE '{value.isoformat()}'"
        return f"DATE '{esc_sql_str(str(value))}'"

    if is_time(dtype_u):
        if isinstance(value, time):
            return f"TIME '{value.strftime('%H:%M:%S')}'"
        return f"TIME '{esc_sql_str(str(value))}'"

    if is_timestamp(dtype_u):
        s = str(value)
        if "LTZ" in dtype_u:
            return f"TO_TIMESTAMP_LTZ('{esc_sql_str(s)}')"
        if "TZ" in dtype_u:
            return f"TO_TIMESTAMP_TZ('{esc_sql_str(s)}')"
        return f"TO_TIMESTAMP_NTZ('{esc_sql_str(s)}')"

    if is_variantish(dtype_u):
        s = str(value).strip()
        if not s:
            return "NULL"
        return f"PARSE_JSON('{esc_sql_str(s)}')"

    return f"'{esc_sql_str(str(value))}'"


def is_generic_arg_name(name: str) -> bool:
    n = (name or "").strip().upper()
    return bool(re.match(r"^(ARG|P)\d+$", n))


def humanize_identifier(s: str) -> str:
    """
    Best-effort humanization for UI labels.
    - underscores => spaces + Title Case
    - suffix KEY/ID/CODE => split
    """
    raw = (s or "").strip().strip('"')
    if not raw:
        return ""

    up = raw.upper()

    if "_" in raw:
        parts = [p for p in re.split(r"_+", raw) if p]
        return " ".join([p[:1].upper() + p[1:].lower() if p.isalpha() else p for p in parts])

    for suf in ("KEY", "ID", "CODE"):
        if up.endswith(suf) and len(raw) > len(suf):
            base = raw[: -len(suf)]
            base_h = base[:1].upper() + base[1:].lower()
            return f"{base_h} {suf.title()}"

    return raw[:1].upper() + raw[1:].lower()


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
        LIMIT {lim} OFFSET {off}
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
        LIMIT {lim} OFFSET {off}
        """

    df = norm_cols(session.sql(sql).to_pandas())
    if df.empty:
        return pd.DataFrame(columns=["ID", "LABEL", "DETAIL"])

    for c in ["ID", "LABEL", "DETAIL"]:
        if c not in df.columns:
            df[c] = None

    df["LABEL"] = df["LABEL"].fillna(df["ID"].astype(str))
    return df[["ID", "LABEL", "DETAIL"]].reset_index(drop=True)


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
def render_lookup_single(
    src: Dict[str, Any],
    widget_base_key: str,
    help_txt: str,
) -> Any:
    search_state_key = f"{widget_base_key}::lkp_search"
    page_state_key = f"{widget_base_key}::lkp_page"
    page_size_state_key = f"{widget_base_key}::lkp_page_size"
    selected_id_key = f"{widget_base_key}::selected_id"

    if search_state_key not in st.session_state:
        st.session_state[search_state_key] = ""
    if page_state_key not in st.session_state:
        st.session_state[page_state_key] = 0
    if page_size_state_key not in st.session_state:
        st.session_state[page_size_state_key] = int(CFG_LOOKUP_PAGE_SIZE)

    with st.expander("🔎 Search / Browse options", expanded=False):
        with st.form(f"{widget_base_key}::search_form", clear_on_submit=False):
            new_search = st.text_input(
                "Search (label / detail / id)",
                value=st.session_state[search_state_key],
                placeholder="Type and hit Search…",
            )
            new_page_size = st.number_input(
                "Page size",
                min_value=25,
                max_value=5000,
                value=int(st.session_state[page_size_state_key]),
                step=25,
            )
            btns = st.columns([1, 1, 1.6])
            do_search = btns[0].form_submit_button("Search")
            do_clear = btns[1].form_submit_button("Clear")
            _ = btns[2].form_submit_button("Keep")

        if do_search:
            st.session_state[search_state_key] = new_search.strip()
            st.session_state[page_state_key] = 0
            st.session_state[page_size_state_key] = int(new_page_size)

        if do_clear:
            st.session_state[search_state_key] = ""
            st.session_state[page_state_key] = 0
            st.session_state[page_size_state_key] = int(new_page_size)

        nav = st.columns([1, 1, 2.3])
        if nav[0].button("⬅️ Prev", key=f"{widget_base_key}::prev"):
            st.session_state[page_state_key] = max(0, int(st.session_state[page_state_key]) - 1)
        if nav[1].button("➡️ Next", key=f"{widget_base_key}::next"):
            st.session_state[page_state_key] = int(st.session_state[page_state_key]) + 1
        nav[2].markdown(
            (
                f"<div class='lookup-source'>"
                f"Source: <b>{src['database']}.{src['schema']}.{src['table']}</b> "
                f"(ID={src['id_col']}, Label={src['label_col']}"
                + (f", Detail={src['detail_col']}" if src.get("detail_col") else "")
                + (f", Filter={src['where_clause']}" if src.get("where_clause") else "")
                + f") · resolver={src.get('resolver','auto')} · hint={src.get('matched_hint','')}"
                + f"</div>"
            ),
            unsafe_allow_html=True,
        )

    cur_search = str(st.session_state[search_state_key] or "").strip()
    cur_page = int(st.session_state[page_state_key] or 0)
    cur_limit = int(st.session_state[page_size_state_key] or CFG_LOOKUP_PAGE_SIZE)
    cur_offset = cur_page * cur_limit

    try:
        opt_df = lookup_search_options(
            db=src["database"],
            schema=src["schema"],
            table=src["table"],
            id_col=src["id_col"],
            label_col=src["label_col"],
            detail_col=src.get("detail_col"),
            where_clause=src.get("where_clause"),
            search=cur_search,
            limit=cur_limit,
            offset=cur_offset,
        )
    except Exception:
        opt_df = pd.DataFrame(columns=["ID", "LABEL", "DETAIL"])

    display_map: Dict[Any, Tuple[str, Optional[str]]] = {}
    ids: List[Any] = []

    if not opt_df.empty:
        for _, r in opt_df.iterrows():
            rid = r.get("ID")
            rlabel = str(r.get("LABEL") or "")
            rdetail = r.get("DETAIL")
            rdetail_s = str(rdetail) if rdetail is not None else None
            ids.append(rid)
            display_map[rid] = (rlabel, rdetail_s)

    prior_selected = st.session_state.get(selected_id_key, None)
    if prior_selected is not None and prior_selected not in display_map:
        fetched = lookup_fetch_one_by_id(
            db=src["database"],
            schema=src["schema"],
            table=src["table"],
            id_col=src["id_col"],
            label_col=src["label_col"],
            detail_col=src.get("detail_col"),
            where_clause=src.get("where_clause"),
            id_dtype=src.get("id_dtype"),
            id_value=prior_selected,
        )
        if fetched:
            fid, flabel, fdetail = fetched
            display_map[fid] = (flabel, fdetail)
            ids = [fid] + ids

    seen = set()
    dedup_ids: List[Any] = []
    for v in ids:
        if v not in seen:
            seen.add(v)
            dedup_ids.append(v)

    options = [None] + dedup_ids

    def _fmt_id(v):
        if v is None:
            return "— Select —"
        lbl, det = display_map.get(v, (str(v), None))
        return format_lookup_label(v, lbl, det, CFG_SHOW_IDS_IN_LOOKUPS)

    chosen_id = st.selectbox(
        "Pick a value",
        options=options,
        index=0 if prior_selected is None else (options.index(prior_selected) if prior_selected in options else 0),
        format_func=_fmt_id,
        key=f"{widget_base_key}::lkp_select",
        help=help_txt or "Server-side search + paging (all values accessible).",
    )

    st.session_state[selected_id_key] = chosen_id

    if chosen_id is None and cur_search and opt_df.empty:
        st.caption("No matches on this page for the current search.")
    elif cur_search:
        st.caption(f"Showing page {cur_page + 1} for search: {cur_search}")

    return chosen_id, {
        "resolver": src.get("resolver"),
        "matched_hint": src.get("matched_hint"),
        "source": f"{src['database']}.{src['schema']}.{src['table']}",
        "search": cur_search,
        "page": cur_page,
        "returned": 0 if opt_df.empty else int(len(opt_df)),
    }


def render_lookup_multi(
    src: Dict[str, Any],
    widget_base_key: str,
    help_txt: str,
) -> str:
    search_state_key = f"{widget_base_key}::lkp_search"
    page_state_key = f"{widget_base_key}::lkp_page"
    page_size_state_key = f"{widget_base_key}::lkp_page_size"
    selected_ids_key = f"{widget_base_key}::selected_ids"
    picker_key = f"{widget_base_key}::lkp_picker"
    remove_key = f"{widget_base_key}::lkp_remove"

    if search_state_key not in st.session_state:
        st.session_state[search_state_key] = ""
    if page_state_key not in st.session_state:
        st.session_state[page_state_key] = 0
    if page_size_state_key not in st.session_state:
        st.session_state[page_size_state_key] = int(CFG_LOOKUP_PAGE_SIZE)
    if selected_ids_key not in st.session_state:
        st.session_state[selected_ids_key] = []

    with st.expander("🔎 Search / Build list", expanded=False):
        with st.form(f"{widget_base_key}::search_form", clear_on_submit=False):
            new_search = st.text_input(
                "Search (label / detail / id)",
                value=st.session_state[search_state_key],
                placeholder="Type and hit Search…",
            )
            new_page_size = st.number_input(
                "Page size",
                min_value=25,
                max_value=5000,
                value=int(st.session_state[page_size_state_key]),
                step=25,
            )
            btns = st.columns([1, 1, 1.6])
            do_search = btns[0].form_submit_button("Search")
            do_clear = btns[1].form_submit_button("Clear")
            _ = btns[2].form_submit_button("Keep")

        if do_search:
            st.session_state[search_state_key] = new_search.strip()
            st.session_state[page_state_key] = 0
            st.session_state[page_size_state_key] = int(new_page_size)

        if do_clear:
            st.session_state[search_state_key] = ""
            st.session_state[page_state_key] = 0
            st.session_state[page_size_state_key] = int(new_page_size)

        nav = st.columns([1, 1, 2.3])
        if nav[0].button("⬅️ Prev", key=f"{widget_base_key}::prev"):
            st.session_state[page_state_key] = max(0, int(st.session_state[page_state_key]) - 1)
        if nav[1].button("➡️ Next", key=f"{widget_base_key}::next"):
            st.session_state[page_state_key] = int(st.session_state[page_state_key]) + 1
        nav[2].markdown(
            (
                f"<div class='lookup-source'>"
                f"Source: <b>{src['database']}.{src['schema']}.{src['table']}</b> "
                f"(ID={src['id_col']}, Label={src['label_col']}"
                + (f", Detail={src['detail_col']}" if src.get("detail_col") else "")
                + (f", Filter={src['where_clause']}" if src.get("where_clause") else "")
                + f") · resolver={src.get('resolver','auto')} · hint={src.get('matched_hint','')}"
                + f"</div>"
            ),
            unsafe_allow_html=True,
        )

        cur_search = str(st.session_state[search_state_key] or "").strip()
        cur_page = int(st.session_state[page_state_key] or 0)
        cur_limit = int(st.session_state[page_size_state_key] or CFG_LOOKUP_PAGE_SIZE)
        cur_offset = cur_page * cur_limit

        try:
            opt_df = lookup_search_options(
                db=src["database"],
                schema=src["schema"],
                table=src["table"],
                id_col=src["id_col"],
                label_col=src["label_col"],
                detail_col=src.get("detail_col"),
                where_clause=src.get("where_clause"),
                search=cur_search,
                limit=cur_limit,
                offset=cur_offset,
            )
        except Exception:
            opt_df = pd.DataFrame(columns=["ID", "LABEL", "DETAIL"])

        display_map: Dict[Any, Tuple[str, Optional[str]]] = {}
        page_ids: List[Any] = []

        if not opt_df.empty:
            for _, r in opt_df.iterrows():
                rid = r.get("ID")
                rlabel = str(r.get("LABEL") or "")
                rdetail = r.get("DETAIL")
                rdetail_s = str(rdetail) if rdetail is not None else None
                page_ids.append(rid)
                display_map[rid] = (rlabel, rdetail_s)

        selected_ids = list(st.session_state.get(selected_ids_key, []))
        missing = tuple(v for v in selected_ids if v not in display_map)
        if missing:
            fetched_df = lookup_fetch_many_by_ids(
                db=src["database"],
                schema=src["schema"],
                table=src["table"],
                id_col=src["id_col"],
                label_col=src["label_col"],
                detail_col=src.get("detail_col"),
                where_clause=src.get("where_clause"),
                id_dtype=src.get("id_dtype"),
                id_values=missing,
            )
            for _, r in fetched_df.iterrows():
                rid = r.get("ID")
                display_map[rid] = (
                    str(r.get("LABEL") or rid),
                    str(r.get("DETAIL")) if r.get("DETAIL") is not None else None,
                )

        options = [None] + uniq_keep_order([str(v) for v in page_ids])
        page_value_map = {str(v): v for v in page_ids}
        selected_value = st.selectbox(
            "Pick a value to add",
            options=options,
            format_func=lambda v: "— Select —" if v is None else format_lookup_label(
                page_value_map[v],
                display_map.get(page_value_map[v], (v, None))[0],
                display_map.get(page_value_map[v], (v, None))[1],
                CFG_SHOW_IDS_IN_LOOKUPS,
            ),
            key=picker_key,
            help=help_txt or "Search, page, and add values into the list.",
        )

        action_cols = st.columns([1, 1, 1, 2])
        if action_cols[0].button("➕ Add", key=f"{widget_base_key}::add_one") and selected_value is not None:
            actual = page_value_map[selected_value]
            if actual not in selected_ids:
                selected_ids.append(actual)
                st.session_state[selected_ids_key] = selected_ids

        if action_cols[1].button("➕ Add page", key=f"{widget_base_key}::add_page"):
            for actual in page_ids:
                if actual not in selected_ids:
                    selected_ids.append(actual)
            st.session_state[selected_ids_key] = selected_ids

        if action_cols[2].button("🧹 Clear all", key=f"{widget_base_key}::clear_all"):
            selected_ids = []
            st.session_state[selected_ids_key] = selected_ids

        if selected_ids:
            remove_options = [None] + [str(v) for v in selected_ids]
            remove_map = {str(v): v for v in selected_ids}
            remove_value = st.selectbox(
                "Remove one",
                options=remove_options,
                format_func=lambda v: "— Select —" if v is None else format_lookup_label(
                    remove_map[v],
                    display_map.get(remove_map[v], (v, None))[0],
                    display_map.get(remove_map[v], (v, None))[1],
                    CFG_SHOW_IDS_IN_LOOKUPS,
                ),
                key=remove_key,
            )
            if st.button("➖ Remove", key=f"{widget_base_key}::remove_one") and remove_value is not None:
                actual = remove_map[remove_value]
                selected_ids = [x for x in selected_ids if x != actual]
                st.session_state[selected_ids_key] = selected_ids

            preview_df = lookup_fetch_many_by_ids(
                db=src["database"],
                schema=src["schema"],
                table=src["table"],
                id_col=src["id_col"],
                label_col=src["label_col"],
                detail_col=src.get("detail_col"),
                where_clause=src.get("where_clause"),
                id_dtype=src.get("id_dtype"),
                id_values=tuple(selected_ids),
            )
            st.caption(f"Selected {len(selected_ids)} value(s)")
            if not preview_df.empty:
                st.dataframe(preview_df, use_container_width=True, height=min(260, 38 * (len(preview_df) + 1)))
        else:
            st.caption("No values selected yet.")

    selected_ids = list(st.session_state.get(selected_ids_key, []))
    if selected_ids:
        csv_value = ",".join([str(v) for v in selected_ids])
        st.caption(f"Selected {len(selected_ids)} value(s) for this parameter.")
        return csv_value

    return ""


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
tab_report, tab_history, tab_debug = st.tabs(["📋 Report Builder", "🕒 Run History", "🛠️ Debug"])


# =========================================================
# Report Builder Tab
# =========================================================
with tab_report:
    db_df = get_databases_df()
    if db_df.empty:
        st.error("No databases visible to this session.")
        st.stop()

    db_map_comment = {r["NAME"]: r.get("COMMENT", "") for _, r in db_df.iterrows()}
    db_options = db_df["NAME"].astype(str).tolist()

    def _fmt_db(n: str) -> str:
        c = (db_map_comment.get(n) or "").strip()
        return f"{n} — {c}" if c else n

    sel_row_1 = st.columns([1.2, 1.2, 2.2], gap="large")

    with sel_row_1[0]:
        db = st.selectbox("Database", options=db_options, format_func=_fmt_db, key="db_select")

    schema_df = get_schemas_df(db)
    if schema_df.empty:
        st.error("No schemas visible in this database (or insufficient privileges).")
        st.stop()

    sch_map_comment = {r["NAME"]: r.get("COMMENT", "") for _, r in schema_df.iterrows()}
    schema_options = schema_df["NAME"].astype(str).tolist()

    def _fmt_schema(n: str) -> str:
        c = (sch_map_comment.get(n) or "").strip()
        return f"{n} — {c}" if c else n

    with sel_row_1[1]:
        schema = st.selectbox("Schema", options=schema_options, format_func=_fmt_schema, key="schema_select")

    procs_df = get_procedures_df(db, schema)
    if procs_df.empty:
        st.warning("No procedures found in this schema.")
        st.stop()

    with sel_row_1[2]:
        proc_search = st.text_input(
            "Search procedures",
            value="",
            placeholder="Type to filter by name / comment…",
            key="proc_search",
        )

        fdf = procs_df.copy()
        if proc_search.strip():
            s = proc_search.strip().lower()
            fdf = fdf[
                fdf["DISPLAY"].astype(str).str.lower().str.contains(re.escape(s), na=False)
                | fdf["NAME"].astype(str).str.lower().str.contains(re.escape(s), na=False)
            ].copy()

        if fdf.empty:
            st.info("No procedures match your search.")
            st.stop()

        proc_id_options = fdf["PROC_ID"].astype(str).tolist()
        disp_map = {r["PROC_ID"]: r["DISPLAY"] for _, r in fdf.iterrows()}

        def _fmt_proc(pid: str) -> str:
            return disp_map.get(pid, pid)

        selected_proc_id = st.selectbox("Procedure", options=proc_id_options, format_func=_fmt_proc, key="proc_select")

    proc_row = procs_df[procs_df["PROC_ID"] == selected_proc_id].iloc[0]
    proc_name = str(proc_row.get("NAME") or "").strip()
    raw_args = str(proc_row.get("ARGUMENTS") or "").strip()
    proc_comment = str(proc_row.get("COMMENT") or "").strip()
    proc_created_on = str(proc_row.get("CREATED_ON") or "").strip()

    paren = first_paren_group(raw_args)
    type_sig = signature_types_only(paren)

    st.markdown('<hr class="hr-soft"/>', unsafe_allow_html=True)

    meta_cols = st.columns([2.0, 1.2, 1.2, 1.8], gap="large")
    with meta_cols[0]:
        st.subheader("Selected report")
        st.write(f"**{db}.{schema}.{proc_name}**")
        if proc_comment:
            st.caption(proc_comment)
        else:
            st.caption("No comment on this procedure.")

    with meta_cols[1]:
        st.subheader("Signature")
        st.code(type_sig)

    with meta_cols[2]:
        st.subheader("Created")
        st.write(proc_created_on if proc_created_on else "—")

    with meta_cols[3]:
        st.subheader("Lookup mode")
        lookup_cfg_status = lookup_config_status(db, CFG_LOOKUP_CONFIG_FQN)
        if CFG_USE_LOOKUP_CONFIG and lookup_cfg_status.get("exists"):
            st.caption(
                f"Config active: {lookup_cfg_status['db']}.{lookup_cfg_status['schema']}.{lookup_cfg_status['table']}"
            )
        elif CFG_USE_LOOKUP_CONFIG:
            st.caption("Config not found. Auto-discovery fallback is active.")
        else:
            st.caption("Auto-discovery only.")

    st.markdown("### Parameters")

    try:
        params = harvest_params(db, schema, proc_name, type_sig)
    except Exception as e:
        st.error("Could not harvest parameters for this procedure.")
        st.code(str(e))
        st.stop()

    if params is None:
        params = []

    friendly_overlay: Dict[str, Dict[str, str]] = {}
    overlay_active = False
    generic_names = bool(params) and all(is_generic_arg_name(p.get("name", "")) for p in params)

    if CFG_ENABLE_METADATA_OVERLAY:
        with st.expander("✨ Optional: Friendly labels (Export metadata overlay)", expanded=generic_names):
            st.caption(
                "If Snowflake exposes ARG1/ARG2, overlay friendly labels/prompts from an Export metadata table. "
                "This does NOT change the real CALL — it only upgrades the UI."
            )

            meta_candidates = find_export_metadata_tables(db)
            if not meta_candidates:
                st.info("No candidate metadata tables detected (or insufficient privileges).")
            else:
                options = [f'{c["schema"]}.{c["table"]}  (score={c["score"]})' for c in meta_candidates]
                pick = st.selectbox("Metadata table", options, index=0, key="meta_table_pick")

                picked = meta_candidates[options.index(pick)]
                meta_schema = picked["schema"]
                meta_table = picked["table"]

                reports_df = get_export_reports(db, meta_schema, meta_table)
                if reports_df.empty or "EXPORTREPORTCODE" not in reports_df.columns:
                    st.warning("Could not read ExportReportCode/ExportReportName from that table.")
                else:
                    def _rlabel(r):
                        code = str(r.get("EXPORTREPORTCODE") or "").strip()
                        name = str(r.get("EXPORTREPORTNAME") or "").strip()
                        return f"{code} — {name}" if name else code

                    report_labels = [_rlabel(r) for _, r in reports_df.iterrows()]
                    chosen_label = st.selectbox("Report", report_labels, key="report_pick")
                    chosen_row = reports_df.iloc[report_labels.index(chosen_label)]
                    report_code = str(chosen_row.get("EXPORTREPORTCODE") or "").strip()

                    meta_params_df = get_export_params_for_report(db, meta_schema, meta_table, report_code)

                    if meta_params_df.empty:
                        st.warning("No parameters found for that report code in the metadata table.")
                    else:
                        overlay_active = st.checkbox("Apply overlay to input labels", value=True, key="apply_overlay")
                        if overlay_active:
                            mp = meta_params_df.copy()

                            def _friendly_from_row(r):
                                tok = str(r.get("EXPORTPARAMTOKEN") or "").strip()
                                nm = str(r.get("EXPORTPARAMNAME") or "").strip()
                                if tok.startswith("@"):
                                    return tok[1:].strip()
                                if tok:
                                    return tok
                                return nm or ""

                            mp["FRIENDLY"] = mp.apply(_friendly_from_row, axis=1)
                            mp["PROMPT"] = mp.get("EXPORTPARAMPROMPT", "")

                            overlay_list = mp.to_dict("records")
                            friendly_overlay = {}

                            for i, p in enumerate(params):
                                if i < len(overlay_list):
                                    real = p["name"]
                                    friendly_overlay[real] = {
                                        "label": overlay_list[i].get("FRIENDLY") or real,
                                        "prompt": overlay_list[i].get("PROMPT") or "",
                                        "token": overlay_list[i].get("EXPORTPARAMTOKEN") or "",
                                        "meta_name": overlay_list[i].get("EXPORTPARAMNAME") or "",
                                    }

                            if friendly_overlay:
                                preview = []
                                for p in params:
                                    real = p["name"]
                                    ov = friendly_overlay.get(real, {})
                                    preview.append(
                                        {
                                            "Real Parameter": real,
                                            "UI Label": ov.get("label", real),
                                            "Prompt": ov.get("prompt", ""),
                                            "Token": ov.get("token", ""),
                                        }
                                    )
                                st.dataframe(pd.DataFrame(preview), use_container_width=True)

    user_inputs: Dict[str, Any] = {}
    lookup_debug_rows: List[Dict[str, Any]] = []

    if not params:
        st.info("This procedure has no parameters.")
    else:
        cols = st.columns(2, gap="large")

        for idx, p in enumerate(params, start=1):
            real_name = p.get("name", f"ARG{idx}")
            ptype = (p.get("type") or "").upper().strip()
            pdefault = (p.get("default") or "").strip() if p.get("default") else ""

            label, help_txt = safe_param_label(real_name, overlay_active, friendly_overlay)
            if pdefault:
                help_txt = (help_txt + "\n\n" if help_txt else "") + f"Default (from signature): {pdefault}"

            widget_base_key = f"param::{db}::{schema}::{proc_name}::{real_name}"
            target_col = cols[(idx - 1) % 2]

            with target_col:
                st.markdown(f"**{label}**  \n<span class='small-muted'>{ptype}</span>", unsafe_allow_html=True)

                is_null = False
                if CFG_ALLOW_NULLS:
                    is_null = st.checkbox("NULL", key=f"{widget_base_key}::null", help="Pass NULL to the procedure")

                if is_null:
                    user_inputs[real_name] = None
                    st.caption(help_txt or "Passing NULL.")
                    st.markdown('<hr class="hr-soft"/>', unsafe_allow_html=True)
                    continue

                lookup_name = id_hint_for_param(real_name, overlay_active, friendly_overlay)
                use_scalar_lookup = CFG_SMART_LOOKUPS and is_lookup_scalar_param(lookup_name, ptype, CFG_SMART_LOOKUPS_FOR_STRING_CODE)
                use_list_lookup = CFG_ENABLE_LIST_LOOKUPS and is_lookup_list_param(lookup_name, ptype, CFG_SMART_LOOKUPS_FOR_STRING_CODE)

                if use_scalar_lookup or use_list_lookup:
                    src = discover_best_lookup_source(
                        db=db,
                        param_name=lookup_name,
                        cand_limit=int(CFG_LOOKUP_DISCOVERY_TABLE_LIMIT),
                        use_lookup_config=bool(CFG_USE_LOOKUP_CONFIG),
                        config_fqn=CFG_LOOKUP_CONFIG_FQN,
                    )

                    if src:
                        if use_list_lookup:
                            chosen_csv = render_lookup_multi(src=src, widget_base_key=widget_base_key, help_txt=help_txt)
                            user_inputs[real_name] = chosen_csv
                            lookup_debug_rows.append(
                                {
                                    "Param": real_name,
                                    "Lookup Name": lookup_name,
                                    "Mode": "multi",
                                    "Source": f"{src['database']}.{src['schema']}.{src['table']}",
                                    "Resolver": src.get("resolver", ""),
                                    "Hint": src.get("matched_hint", ""),
                                    "Score": src.get("score", ""),
                                    "Where": src.get("where_clause", "") or "",
                                }
                            )
                            if help_txt:
                                st.caption(help_txt)
                            st.markdown('<hr class="hr-soft"/>', unsafe_allow_html=True)
                            continue

                        chosen_value, dbg = render_lookup_single(src=src, widget_base_key=widget_base_key, help_txt=help_txt)
                        user_inputs[real_name] = chosen_value
                        lookup_debug_rows.append(
                            {
                                "Param": real_name,
                                "Lookup Name": lookup_name,
                                "Mode": "single",
                                "Source": dbg.get("source", ""),
                                "Resolver": dbg.get("resolver", ""),
                                "Hint": dbg.get("matched_hint", ""),
                                "Search": dbg.get("search", ""),
                                "Page": dbg.get("page", ""),
                                "Returned": dbg.get("returned", ""),
                                "Score": src.get("score", ""),
                                "Where": src.get("where_clause", "") or "",
                            }
                        )
                        if help_txt:
                            st.caption(help_txt)
                        st.markdown('<hr class="hr-soft"/>', unsafe_allow_html=True)
                        continue

                    lookup_debug_rows.append(
                        {
                            "Param": real_name,
                            "Lookup Name": lookup_name,
                            "Mode": "single" if use_scalar_lookup else "multi",
                            "Source": "(none found)",
                            "Resolver": "",
                            "Hint": ", ".join(generate_lookup_hints(lookup_name)[:5]),
                            "Score": "",
                            "Where": "",
                        }
                    )

                # ---- Fallback widgets
                if is_bool(ptype) and CFG_ALLOW_NULLS:
                    user_inputs[real_name] = bool_tri_state_widget("Value", key=f"{widget_base_key}::bool", help_txt=help_txt)

                elif is_bool(ptype):
                    user_inputs[real_name] = st.checkbox("Value", value=False, help=help_txt, key=f"{widget_base_key}::bool2")

                elif is_numeric(ptype):
                    user_inputs[real_name] = st.number_input(
                        "Value",
                        value=0,
                        step=1,
                        help=help_txt,
                        key=f"{widget_base_key}::num",
                    )

                elif is_date(ptype):
                    user_inputs[real_name] = st.date_input(
                        "Value",
                        value=date.today(),
                        help=help_txt,
                        key=f"{widget_base_key}::date",
                    )

                elif is_time(ptype):
                    user_inputs[real_name] = st.time_input(
                        "Value",
                        value=time(0, 0, 0),
                        help=help_txt,
                        key=f"{widget_base_key}::time",
                    )

                elif is_timestamp(ptype):
                    user_inputs[real_name] = st.text_input(
                        "Value",
                        value="",
                        help=help_txt or "Enter timestamp string (e.g., 2025-01-31 13:45:00).",
                        key=f"{widget_base_key}::ts",
                        placeholder="YYYY-MM-DD HH:MM:SS",
                    )

                elif is_variantish(ptype):
                    user_inputs[real_name] = st.text_area(
                        "Value (JSON)",
                        value="",
                        help=help_txt or "Enter JSON (passed via PARSE_JSON('...')).",
                        key=f"{widget_base_key}::json",
                        height=110,
                        placeholder='{"key":"value"}',
                    )

                else:
                    user_inputs[real_name] = st.text_input(
                        "Value",
                        value="",
                        help=help_txt,
                        key=f"{widget_base_key}::txt",
                    )

                st.markdown('<hr class="hr-soft"/>', unsafe_allow_html=True)

    fq = f"{qident(db)}.{qident(schema)}.{qident(proc_name)}"
    arg_exprs: List[str] = []
    arg_values_positional: List[str] = []

    for i, p in enumerate(params, start=1):
        real_name = p.get("name", f"ARG{i}")
        ptype = (p.get("type") or "").upper().strip()
        val = user_inputs.get(real_name)

        if CFG_EMPTY_TEXT_AS_NULL and val is not None and isinstance(val, str) and val.strip() == "" and (
            is_stringish(ptype) or is_variantish(ptype) or is_timestamp(ptype)
        ):
            val = None

        lit = sql_literal(val, ptype)
        arg_values_positional.append(lit)

        if CFG_USE_NAMED_ARGS:
            if re.match(r"^[A-Z_][A-Z0-9_\$]*$", real_name.upper()):
                arg_exprs.append(f"{real_name} => {lit}")
            else:
                arg_exprs.append(f"{qident(real_name)} => {lit}")

    if CFG_USE_NAMED_ARGS:
        call_sql = f"CALL {fq}({', '.join(arg_exprs)});"
    else:
        call_sql = f"CALL {fq}({', '.join(arg_values_positional)});"

    with st.expander("🧾 Generated CALL SQL", expanded=False):
        if overlay_active and friendly_overlay:
            mapping_lines = []
            for p in params:
                real = p["name"]
                ui = friendly_overlay.get(real, {}).get("label", "")
                if ui and ui != real:
                    mapping_lines.append(f"-- {ui} maps to {real}")
            if mapping_lines:
                st.code("\n".join(mapping_lines) + "\n" + call_sql)
            else:
                st.code(call_sql)
        else:
            st.code(call_sql)

    run_row = st.columns([1.2, 1.0, 6.0], gap="large")
    with run_row[0]:
        run_clicked = st.button("🚀 Run report", type="primary")
    with run_row[1]:
        clear_clicked = st.button("🧹 Clear inputs")
    with run_row[2]:
        st.caption("Runs only when you click **Run report** — changing inputs won’t execute anything.")

    if clear_clicked:
        keys_to_clear = [k for k in st.session_state.keys() if k.startswith(f"param::{db}::{schema}::{proc_name}::")]
        for k in keys_to_clear:
            try:
                del st.session_state[k]
            except Exception:
                pass
        st.success("Cleared inputs for this procedure. Scroll up; the UI will refresh.")
        st.stop()

    if run_clicked:
        try:
            with st.spinner("Running procedure…"):
                t0 = _time.perf_counter()
                out_df = session.sql(call_sql).to_pandas()
                t1 = _time.perf_counter()

            out_df = norm_cols(out_df) if isinstance(out_df, pd.DataFrame) else pd.DataFrame()
            duration_s = t1 - t0

            try:
                qid = session.sql("SELECT LAST_QUERY_ID() AS QID").to_pandas().iloc[0]["QID"]
            except Exception:
                qid = None

            st.session_state["last_run"] = {
                "when": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "db": db,
                "schema": schema,
                "proc": proc_name,
                "type_sig": type_sig,
                "sql": call_sql,
                "duration_s": duration_s,
                "rows": int(len(out_df)) if isinstance(out_df, pd.DataFrame) else 0,
                "cols": int(len(out_df.columns)) if isinstance(out_df, pd.DataFrame) else 0,
                "query_id": qid,
            }
            st.session_state["last_result_df"] = out_df

            push_history(st.session_state["last_run"])
            st.success(f"Execution successful · {len(out_df):,} rows · {duration_s:.2f}s")

        except Exception as e:
            st.error("Execution failed")
            st.code(str(e))

    if "last_run" in st.session_state and "last_result_df" in st.session_state:
        lr = st.session_state["last_run"]
        df_res: pd.DataFrame = st.session_state["last_result_df"]

        st.markdown("### Results")

        mcols = st.columns([1.2, 1.2, 1.2, 2.4], gap="large")
        mcols[0].metric("Rows", f"{lr.get('rows', 0):,}")
        mcols[1].metric("Columns", f"{lr.get('cols', 0):,}")
        mcols[2].metric("Runtime", f"{lr.get('duration_s', 0.0):.2f}s")
        qid = lr.get("query_id")
        mcols[3].write(f"**Query ID:** {qid}" if qid else "**Query ID:** —")

        if isinstance(df_res, pd.DataFrame) and not df_res.empty:
            st.dataframe(df_res, use_container_width=True, height=520)

            csv_bytes = df_res.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Download CSV",
                data=csv_bytes,
                file_name=f"{db}_{schema}_{proc_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}Z.csv",
                mime="text/csv",
            )
        else:
            st.info("Procedure executed, but returned no rows (or no tabular output).")


# =========================================================
# History Tab
# =========================================================
with tab_history:
    st.subheader("Run History (this session)")
    hist = st.session_state.get("run_history", [])
    if not hist:
        st.info("No runs yet.")
    else:
        hist_df = pd.DataFrame(hist)
        show_cols = [c for c in ["when", "db", "schema", "proc", "rows", "cols", "duration_s", "query_id"] if c in hist_df.columns]
        st.dataframe(hist_df[show_cols], use_container_width=True, height=320)

        with st.expander("Details (SQL for recent runs)", expanded=False):
            for i, h in enumerate(hist[:10], start=1):
                st.markdown(f"**{i}. {h.get('when','')} · {h.get('db','')}.{h.get('schema','')}.{h.get('proc','')}**")
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
    st.subheader("Debug")
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
