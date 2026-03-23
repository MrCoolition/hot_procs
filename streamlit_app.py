
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
from dataclasses import dataclass
from datetime import date, time, datetime
import json
import math
from typing import Optional, Dict, Any, List, Sequence, Tuple

# === CORE_HELPERS_START ===
from dataclasses import dataclass
from datetime import date, datetime, time
import json
import math
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

DOMAIN_LEXICON = {
    'BUSINESS', 'CUSTOMER', 'SUPPLIER', 'REGION', 'DIVISION', 'SECTOR', 'GROUP', 'LEVEL',
    'PERIOD', 'CONTRACT', 'FISCAL', 'YEAR', 'MONTH', 'REPORT', 'ACCOUNT', 'LOCATION',
    'STATUS', 'TYPE', 'FORMAT', 'EXPORT', 'COMPANY', 'ENTITY', 'SESSION', 'FOODBUY', 'COMPASS'
}
KNOWN_ACRONYMS = {'ID', 'SQL', 'FTP', 'USA', 'AP', 'JSON'}
TECHNICAL_SUFFIXES = ('KEYLIST', 'IDLIST', 'CODELIST', 'KEY', 'ID', 'CODE', 'LIST')
TIME_RE = re.compile(r'^TIME(?:\s*\([^)]*\))?$')
TIMESTAMP_RE = re.compile(r'^(?:TIMESTAMP(?:_(?:NTZ|LTZ|TZ))?|DATETIME)(?:\s*\([^)]*\))?$')
DATE_RE = re.compile(r'^DATE(?:\s*\([^)]*\))?$')
BOOL_RE = re.compile(r'^(?:BOOLEAN|BOOL)$')
NUMERIC_RE = re.compile(r'^(?:NUMBER|DECIMAL|NUMERIC|INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT|FLOAT|DOUBLE|REAL)(?:\s*\([^)]*\))?$')
VARIANT_RE = re.compile(r'^(?:VARIANT|OBJECT|ARRAY)')
STRING_RE = re.compile(r'^(?:VARCHAR|CHAR|CHARACTER|TEXT|STRING)(?:\s*\([^)]*\))?$')


@dataclass(frozen=True)
class ProcUIMeta:
    database: str
    schema: str
    proc_name: str
    type_signature: str
    display_name: str
    description: str = ''
    category: str = ''
    report_code: str = ''
    source: str = 'heuristic'


@dataclass(frozen=True)
class ParamUIMeta:
    param_name: str
    ordinal: int
    display_label: str
    short_label: str
    help_text: str = ''
    placeholder: str = ''
    group_name: str = ''
    display_order: int = 0
    control_type: str = 'auto'
    default_mode: str = 'none'
    default_value: Optional[str] = None
    allow_null: bool = False
    lookup_key: str = ''
    list_encoding: str = 'csv'
    show_technical_id: bool = False
    required: bool = True
    source: str = 'heuristic'


@dataclass(frozen=True)
class LookupUIMeta:
    database: str
    schema: str
    table: str
    id_col: str
    label_col: str
    detail_col: Optional[str] = None
    code_col: Optional[str] = None
    where_clause: Optional[str] = None
    id_dtype: Optional[str] = None
    display_mode: str = 'friendly'
    search_columns: Tuple[str, ...] = ()
    sort_expression: str = ''
    confidence_score: int = 0
    resolver: str = 'heuristic'


@dataclass(frozen=True)
class ParamSubmission:
    mode: str
    raw_value: Any
    sql_literal: Optional[str]
    display_value: str
    is_valid: bool
    validation_error: str = ''


def esc_sql_str(s: str) -> str:
    return (s or '').replace("'", "''")


def normalize_name(s: str) -> str:
    return re.sub(r'[^A-Z0-9]', '', str(s or '').upper())


def _split_identifier_tokens(raw: str) -> List[str]:
    s = str(raw or '').strip().strip('"')
    if not s:
        return []
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', s)
    s = re.sub(r'[_\-]+', ' ', s)
    seed: List[str] = []
    for token in s.split():
        if token.isupper() and len(token) > 4:
            rest = token
            while rest:
                match = None
                for word in sorted(DOMAIN_LEXICON | KNOWN_ACRONYMS | {'FROM', 'TO'}, key=len, reverse=True):
                    if rest.startswith(word):
                        match = word
                        break
                if match:
                    seed.append(match)
                    rest = rest[len(match):]
                else:
                    m = re.match(r'^[A-Z]+?(?=[A-Z][a-z]|\d|$)', rest)
                    if m:
                        seed.append(m.group(0))
                        rest = rest[len(m.group(0)):]
                    else:
                        seed.append(rest)
                        rest = ''
        else:
            seed.append(token)
    return [t for t in seed if t]


def humanize_identifier_advanced(identifier: str, aliases: Optional[Dict[str, str]] = None) -> str:
    aliases = aliases or {}
    if identifier in aliases:
        return aliases[identifier]
    upper = str(identifier or '').strip().strip('"').upper()
    if upper in aliases:
        return aliases[upper]
    tokens = _split_identifier_tokens(identifier)
    if not tokens:
        return ''

    is_list = False
    while tokens and tokens[-1].upper() in {'LIST', 'KEYLIST', 'IDLIST', 'CODELIST'}:
        is_list = True
        tokens.pop()
    while tokens and tokens[-1].upper() in {'KEY', 'ID', 'CODE'}:
        tokens.pop()
    tokens = [t.upper() for t in tokens if t]
    tokens = [t for t in tokens if t not in {'SESSION'}]
    if tokens and tokens[-1] == 'KEYFROM':
        tokens = tokens[:-1] + ['FROM']
    if tokens and tokens[-1] == 'KEYTO':
        tokens = tokens[:-1] + ['TO']
    if not tokens:
        tokens = [upper]
    words = []
    for tok in tokens:
        if tok in {'FROM', 'TO'}:
            words.append(tok.title())
        elif tok in KNOWN_ACRONYMS:
            words.append(tok)
        else:
            words.append(tok.title())
    if is_list and words:
        if words[-1].endswith('s'):
            pass
        elif words[-1].endswith('y'):
            words[-1] = words[-1][:-1] + 'ies'
        else:
            words[-1] = words[-1] + 's'
    return ' '.join(words)


def is_numeric(dtype: str) -> bool:
    return bool(NUMERIC_RE.match(str(dtype or '').upper().strip()))


def is_bool(dtype: str) -> bool:
    return bool(BOOL_RE.match(str(dtype or '').upper().strip()))


def is_date(dtype: str) -> bool:
    return bool(DATE_RE.match(str(dtype or '').upper().strip()))


def is_time(dtype: str) -> bool:
    return bool(TIME_RE.match(str(dtype or '').upper().strip()))


def is_timestamp(dtype: str) -> bool:
    return bool(TIMESTAMP_RE.match(str(dtype or '').upper().strip()))


def is_variantish(dtype: str) -> bool:
    return bool(VARIANT_RE.match(str(dtype or '').upper().strip()))


def is_stringish(dtype: str) -> bool:
    return bool(STRING_RE.match(str(dtype or '').upper().strip()))


def parse_timestamp_value(value: Any) -> Optional[str]:
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=' ', timespec='seconds')
    text = str(value).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M'):
        try:
            return datetime.strptime(text, fmt).strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def sql_literal(value: Any, dtype: str) -> str:
    dtype_u = str(dtype or '').upper().strip()
    if value is None:
        return 'NULL'
    if is_timestamp(dtype_u):
        parsed = parse_timestamp_value(value)
        s = parsed or str(value)
        if 'NTZ' in dtype_u or 'DATETIME' in dtype_u or ('TZ' not in dtype_u and 'LTZ' not in dtype_u):
            return f"TO_TIMESTAMP_NTZ('{esc_sql_str(s)}')"
        if 'LTZ' in dtype_u:
            return f"TO_TIMESTAMP_LTZ('{esc_sql_str(s)}')"
        return f"TO_TIMESTAMP_TZ('{esc_sql_str(s)}')"
    if is_time(dtype_u):
        if isinstance(value, time):
            rendered = value.strftime('%H:%M:%S')
        else:
            rendered = str(value).strip()
        return f"TIME '{esc_sql_str(rendered)}'"
    if is_date(dtype_u):
        rendered = value.isoformat() if isinstance(value, date) and not isinstance(value, datetime) else str(value).strip()
        return f"DATE '{esc_sql_str(rendered)}'"
    if is_bool(dtype_u):
        return 'TRUE' if bool(value) else 'FALSE'
    if is_numeric(dtype_u):
        if isinstance(value, float) and math.isnan(value):
            raise ValueError('NaN is not a valid numeric literal')
        return str(value)
    if is_variantish(dtype_u):
        if isinstance(value, (dict, list)):
            payload = json.dumps(value)
        else:
            payload = str(value).strip()
            json.loads(payload)
        return f"PARSE_JSON('{esc_sql_str(payload)}')"
    return f"'{esc_sql_str(str(value))}'"


def extract_proc_name_from_export_sql(export_sql: str) -> str:
    sql = str(export_sql or '').strip()
    if not sql:
        return ''
    normalized = re.sub(r'\s+', ' ', sql)
    patterns = [
        r'(?i)\bCALL\s+((?:"[^"]+"|[A-Z0-9_]+)(?:\.(?:"[^"]+"|[A-Z0-9_]+)){0,2})\s*\(',
        r'(?i)\bFROM\s+TABLE\s*\(\s*((?:"[^"]+"|[A-Z0-9_]+)(?:\.(?:"[^"]+"|[A-Z0-9_]+)){0,2})\s*\(',
    ]
    for pattern in patterns:
        m = re.search(pattern, normalized)
        if not m:
            continue
        ident = m.group(1)
        parts = [part.strip().strip('"') for part in ident.split('.') if part.strip()]
        if parts:
            return parts[-1].upper()
    return ''


def match_export_metadata(params: Sequence[Dict[str, Any]], meta_rows: Sequence[Dict[str, Any]], proc_key: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    by_token = {}
    by_name = {}
    for row in meta_rows:
        token = normalize_name(str(row.get('EXPORTPARAMTOKEN') or '').lstrip('@'))
        name = normalize_name(row.get('EXPORTPARAMNAME') or '')
        if token:
            by_token.setdefault(token, []).append(row)
        if name:
            by_name.setdefault(name, []).append(row)
    mapping: Dict[str, Dict[str, Any]] = {}
    used_ids = set()
    for ordinal, param in enumerate(params, start=1):
        name_norm = normalize_name(param.get('name'))
        for pool in (by_token.get(name_norm, []), by_name.get(name_norm, [])):
            if len(pool) == 1 and id(pool[0]) not in used_ids:
                mapping[param['name']] = pool[0]
                used_ids.add(id(pool[0]))
                break
    remaining_params = [p for p in params if p['name'] not in mapping]
    remaining_rows = [r for r in meta_rows if id(r) not in used_ids]
    has_sequence = all(any(k in r for k in ('ORDINAL', 'SEQUENCE', 'EXPORTPARAMSEQUENCE')) for r in remaining_rows) if remaining_rows else False
    if remaining_params and remaining_rows and len(remaining_params) == len(remaining_rows) and has_sequence:
        seq_key = 'ORDINAL' if 'ORDINAL' in remaining_rows[0] else ('SEQUENCE' if 'SEQUENCE' in remaining_rows[0] else 'EXPORTPARAMSEQUENCE')
        ordered_rows = sorted(remaining_rows, key=lambda r: int(r.get(seq_key) or 0))
        if len({int(r.get(seq_key) or 0) for r in ordered_rows}) == len(ordered_rows):
            for param, row in zip(remaining_params, ordered_rows):
                mapping[param['name']] = row
    return mapping


def resolve_proc_ui_meta(database: str, schema: str, proc_name: str, type_sig: str, proc_comment: str = '', config_row: Optional[Dict[str, Any]] = None, export_row: Optional[Dict[str, Any]] = None) -> ProcUIMeta:
    if config_row:
        return ProcUIMeta(database, schema, proc_name, type_sig, config_row.get('display_name') or proc_name, config_row.get('description') or '', config_row.get('category') or '', config_row.get('report_code') or '', 'config')
    if export_row:
        return ProcUIMeta(database, schema, proc_name, type_sig, export_row.get('EXPORTREPORTNAME') or proc_name, export_row.get('EXPORTREPORTDESCRIPTION') or '', export_row.get('CATEGORY') or '', export_row.get('EXPORTREPORTCODE') or '', 'export_metadata')
    if proc_comment.strip():
        return ProcUIMeta(database, schema, proc_name, type_sig, proc_comment.strip().split(' - ')[0], proc_comment.strip(), '', '', 'comment')
    return ProcUIMeta(database, schema, proc_name, type_sig, humanize_identifier_advanced(proc_name), '', '', '', 'heuristic')


def resolve_param_ui_meta(proc_ui_meta: ProcUIMeta, param: Dict[str, Any], ordinal: int, overlay_row: Optional[Dict[str, Any]] = None, aliases: Optional[Dict[str, str]] = None, allow_null: bool = False) -> ParamUIMeta:
    param_name = str(param.get('name') or f'ARG{ordinal}')
    default_value = param.get('default')
    has_default = default_value not in (None, '')
    required = not has_default
    if overlay_row:
        label_seed = overlay_row.get('DISPLAY_LABEL') or overlay_row.get('EXPORTPARAMTOKEN') or overlay_row.get('EXPORTPARAMNAME') or param_name
        label_seed = str(label_seed).lstrip('@')
        source = 'export_metadata'
        help_text = str(overlay_row.get('EXPORTPARAMPROMPT') or overlay_row.get('HELP_TEXT') or '')
    else:
        label_seed = param_name
        source = 'heuristic'
        help_text = ''
    display_label = humanize_identifier_advanced(label_seed, aliases=aliases)
    short_label = display_label
    lookup_key = normalize_name(label_seed) or normalize_name(param_name)
    allow_null_selection = allow_null and not required
    return ParamUIMeta(
        param_name=param_name,
        ordinal=ordinal,
        display_label=display_label,
        short_label=short_label,
        help_text=help_text,
        placeholder=f'Enter {display_label.lower()}' if not help_text else '',
        display_order=ordinal,
        default_mode='proc_default' if has_default else 'none',
        default_value=default_value,
        allow_null=allow_null_selection,
        lookup_key=lookup_key,
        list_encoding='csv',
        required=required,
        source=source,
    )


def resolve_lookup_ui_meta(proc_ui_meta: ProcUIMeta, param_ui_meta: ParamUIMeta, raw_param_name: str, config_match: Optional[Dict[str, Any]] = None, discovered_match: Optional[Dict[str, Any]] = None, min_confidence: int = 180) -> Optional[LookupUIMeta]:
    row = config_match or discovered_match
    if not row:
        return None
    score = int(row.get('score') or row.get('confidence_score') or 0)
    resolver = row.get('resolver') or ('config' if config_match else 'auto')
    if config_match is None and score < min_confidence:
        return None
    return LookupUIMeta(
        database=row['database'], schema=row['schema'], table=row['table'], id_col=row['id_col'], label_col=row['label_col'],
        detail_col=row.get('detail_col'), code_col=row.get('code_col'), where_clause=row.get('where_clause'), id_dtype=row.get('id_dtype'),
        display_mode='friendly', search_columns=tuple(c for c in [row.get('label_col'), row.get('detail_col'), row.get('code_col'), row.get('id_col')] if c),
        sort_expression=row.get('sort_expression') or '', confidence_score=score, resolver=resolver,
    )


def disambiguate_lookup_labels(options: Sequence[Dict[str, Any]], show_technical_ids: bool = False) -> Dict[Any, str]:
    labels = {o['id']: (str(o.get('label') or '').strip() or str(o['id'])) for o in options}
    grouped = {}
    for o in options:
        grouped.setdefault(labels[o['id']], []).append(o)
    for group in grouped.values():
        if len(group) <= 1:
            continue
        temp = {o['id']: labels[o['id']] for o in group}
        for attr in ('detail', 'code', 'id'):
            inv = {}
            for o in group:
                extra = o.get(attr)
                if attr == 'id':
                    extra = o['id']
                if extra not in (None, ''):
                    temp[o['id']] = f"{labels[o['id']]} — {extra}"
                inv.setdefault(temp[o['id']], []).append(o)
            if all(len(v) == 1 for v in inv.values()):
                labels.update(temp)
                break
            labels.update(temp)
    if show_technical_ids:
        for o in options:
            if str(o['id']) not in labels[o['id']]:
                labels[o['id']] = f"{labels[o['id']]} [{o['id']}]"
    return labels


def proc_instance_key(database: str, schema: str, proc_name: str, type_signature: str) -> str:
    return f'{database}::{schema}::{proc_name}::{type_signature}'


def build_param_submission(mode: str, raw_value: Any, dtype: str, display_value: str = '') -> ParamSubmission:
    mode = mode.upper()
    if mode == 'DEFAULT':
        return ParamSubmission(mode, raw_value, None, display_value or 'Use procedure default', True)
    if mode == 'NULL':
        return ParamSubmission(mode, raw_value, 'NULL', display_value or 'NULL', True)
    if mode == 'UNSET':
        return ParamSubmission(mode, raw_value, None, display_value, False, 'A value is required.')
    try:
        lit = sql_literal(raw_value, dtype)
        return ParamSubmission(mode, raw_value, lit, display_value or str(raw_value), True)
    except Exception as exc:
        return ParamSubmission(mode, raw_value, None, display_value, False, str(exc))


def build_call_sql(database: str, schema: str, proc_name: str, params: Sequence[Tuple[str, ParamSubmission]], named_args: bool = True) -> str:
    fq = f'"{database}"."{schema}"."{proc_name}"'
    arg_exprs = []
    positional = []
    for name, submission in params:
        if submission.mode == 'DEFAULT':
            continue
        if not submission.is_valid:
            raise ValueError(submission.validation_error or f'Invalid value for {name}')
        lit = submission.sql_literal or 'NULL'
        positional.append(lit)
        arg_exprs.append(f'{name} => {lit}')
    rendered = ', '.join(arg_exprs if named_args else positional)
    return f'CALL {fq}({rendered});'




def extract_unsupported_use_statements(proc_ddl: str) -> List[str]:
    text = str(proc_ddl or '')
    if not text:
        return []
    matches = re.findall(r'(?im)\bUSE\s+(DATABASE|SCHEMA|ROLE|WAREHOUSE|SECONDARY\s+ROLES?)\b', text)
    for quoted in re.findall(r"'([^']*)'", text):
        matches.extend(re.findall(r'(?im)\bUSE\s+(DATABASE|SCHEMA|ROLE|WAREHOUSE|SECONDARY\s+ROLES?)\b', quoted))
    normalized = []
    for match in matches:
        stmt = re.sub(r'\s+', ' ', str(match).upper()).strip()
        normalized.append(f'USE {stmt}')
    seen = []
    for stmt in normalized:
        if stmt not in seen:
            seen.append(stmt)
    return seen


def build_streamlit_proc_hardening_guide(database: str, schema: str, proc_name: str) -> str:
    fq_table = f'{database}.{schema}.OPENSTOCKREPORT'
    fq_proc = f'{database}.{schema}.{proc_name}'
    return '\n'.join([
        '# Proc hardening rules for Streamlit/Snowpark compatibility',
        '',
        '1. Remove all USE statements from procedure code:',
        '   - USE DATABASE',
        '   - USE SCHEMA',
        '   - USE WAREHOUSE',
        '   - USE ROLE',
        '',
        '2. Fully qualify every object reference:',
        '   - db.schema.table',
        '   - db.schema.view',
        '   - db.schema.stage',
        '   - db.schema.function',
        '   - db.schema.procedure',
        '',
        '3. Fully qualify the procedure call site:',
        '   - CALL db.schema.proc_name(...)',
        '',
        '4. Fully qualify all SQL inside EXECUTE IMMEDIATE strings.',
        '',
        '5. Do not rely on session context for object resolution.',
        '',
        '6. Do not rely on worksheet-selected database/schema/warehouse.',
        '',
        '7. Preserve EXECUTE AS behavior explicitly:',
        '   - EXECUTE AS OWNER or EXECUTE AS CALLER',
        '   - do not assume session role/context will fill gaps',
        '',
        'Example translation:',
        '```sql',
        f'USE DATABASE {database};',
        f'USE SCHEMA {schema};',
        '',
        'SELECT * FROM OPENSTOCKREPORT;',
        f"CALL {proc_name}('20260316','20260323');",
        '',
        '-- becomes --',
        '',
        f'SELECT * FROM {fq_table};',
        f"CALL {fq_proc}('20260316','20260323');",
        '',
        f"EXECUTE IMMEDIATE 'UPDATE OPENSTOCKREPORT SET ...';",
        '',
        '-- becomes --',
        '',
        f"EXECUTE IMMEDIATE 'UPDATE {fq_table} SET ...';",
        '```',
        '',
        'If the procedure still fails after qualification, check for:',
        '- USE ROLE or role switching requirements.',
        '- Dynamically built object names that also need qualification.',
        '- Missing USAGE/privileges for the Streamlit caller role.',
    ])


def build_use_statement_remediation(database: str, schema: str, proc_name: str, statements: Sequence[str]) -> Dict[str, Any]:
    fq_proc = f'{database}.{schema}.{proc_name}'
    quoted_fq_proc = f'{qident(database)}.{qident(schema)}.{qident(proc_name)}'
    unique_statements = list(dict.fromkeys(str(stmt).strip() for stmt in statements if str(stmt).strip()))
    hardening_guide = build_streamlit_proc_hardening_guide(database, schema, proc_name)
    return {
        'summary': 'This procedure cannot run from Streamlit because its body changes Snowflake session context.',
        'details': [
            'Detected session-changing statements in the procedure definition: ' + ', '.join(unique_statements) + '.',
            'Snowflake blocks these statements when the procedure is invoked from Streamlit or other restricted runtimes.',
            f'Convert {fq_proc} from session-context-driven SQL to fully qualified SQL so object resolution does not depend on USE statements.',
        ],
        'hint': 'Yes: rewrite the procedure to remove USE statements, fully qualify every referenced object, and qualify SQL inside EXECUTE IMMEDIATE strings.',
        'developer_note': '\n'.join([
            f'Procedure: {quoted_fq_proc}',
            'Blocked statements: ' + ', '.join(unique_statements),
            '',
            'Answer:',
            'Yes. Convert the procedure from session-context-driven SQL to fully qualified SQL, and it should run from Streamlit if context dependence is the blocker.',
            '',
            'Recommended fix:',
            '  1. Delete USE DATABASE / USE SCHEMA / USE ROLE / USE WAREHOUSE statements from the procedure body.',
            '  2. Replace unqualified object references with fully qualified names such as "DB"."SCHEMA"."OBJECT".',
            '  3. Fully qualify SQL inside EXECUTE IMMEDIATE strings too.',
            '  4. Preserve EXECUTE AS OWNER/CALLER explicitly and verify privileges.',
            '  5. Recreate the procedure and retry the report from Streamlit.',
            '',
            hardening_guide,
        ]),
    }


@st.cache_data(ttl=300, show_spinner=False)
def get_procedure_ddl(db: str, schema: str, proc: str, type_signature: str) -> str:
    fq = f"{qident(db)}.{qident(schema)}.{qident(proc)}{type_signature}"
    sql = f"SELECT GET_DDL('PROCEDURE', '{esc_sql_str(fq)}') AS PROCEDURE_DDL"
    df = norm_cols(session.sql(sql).to_pandas())
    if df.empty or 'PROCEDURE_DDL' not in df.columns:
        return ''
    return str(df.iloc[0]['PROCEDURE_DDL'] or '')


def analyze_execution_error(message: Any) -> Dict[str, Any]:
    text = str(message or '').strip()
    summary = text.splitlines()[-1].strip() if text else 'Unknown execution error.'
    details: List[str] = []
    hint = ''
    category = 'generic'

    unknown_func = re.search(r"Unknown user-defined function\s+([A-Z0-9_$.]+)", text, re.IGNORECASE)
    if unknown_func:
        object_name = unknown_func.group(1).rstrip('.')
        category = 'missing_function'
        summary = f'Missing function dependency: {object_name}'
        details.append('The stored procedure references a user-defined function that is not available in the current database context.')
        hint = 'Verify that the function exists, that the procedure is pointing at the right database/schema, or update the procedure dependency before retrying.'
        return {
            'category': category,
            'summary': summary,
            'details': details,
            'hint': hint,
            'object_name': object_name,
            'raw_message': text,
        }

    missing_schema = re.search(r"Schema\s+'([^']+)'\s+does not exist or not authorized", text, re.IGNORECASE)
    if missing_schema:
        object_name = missing_schema.group(1).rstrip('.')
        category = 'missing_schema'
        summary = f'Missing or unauthorized schema: {object_name}'
        details.append('Snowflake could not resolve a schema referenced by the stored procedure or one of its dependent queries.')
        hint = 'Confirm the caller role can use that schema and that the session database/schema context is set to the report\'s database/schema before execution.'
        return {
            'category': category,
            'summary': summary,
            'details': details,
            'hint': hint,
            'object_name': object_name,
            'raw_message': text,
        }

    unsupported_stmt = re.search(r"Unsupported statement type '([^']+)'", text, re.IGNORECASE)
    if unsupported_stmt:
        object_name = unsupported_stmt.group(1).upper()
        category = 'unsupported_statement'
        summary = f'Unsupported statement inside the stored procedure: {object_name}'
        details.append('The stored procedure tried to execute a SQL statement type that is not allowed in this runtime.')
        if object_name == 'USE':
            details.append('This usually means the procedure body contains USE DATABASE, USE SCHEMA, USE ROLE, or USE WAREHOUSE statements.')
            hint = 'Remove those USE statements from the procedure body and fully qualify referenced objects before retrying.'
        else:
            hint = 'Remove session-changing statements such as USE DATABASE/SCHEMA from the execution path, or fully qualify object references before retrying.'
        return {
            'category': category,
            'summary': summary,
            'details': details,
            'hint': hint,
            'object_name': object_name,
            'raw_message': text,
        }

    if 'SQL compilation error' in text.upper():
        category = 'sql_compilation'
        summary = 'SQL compilation error inside the stored procedure'
        details.append('Snowflake could not compile the SQL generated by the procedure body.')
        hint = 'Review the procedure definition and referenced database objects, then rerun the report.'
    elif text:
        details.append(summary)

    return {
        'category': category,
        'summary': summary,
        'details': details,
        'hint': hint,
        'object_name': None,
        'raw_message': text,
    }

def validate_variant_json(raw_value: Any) -> Optional[str]:
    if raw_value in (None, ''):
        return None
    try:
        json.loads(str(raw_value))
        return None
    except Exception as exc:
        return str(exc)
# === CORE_HELPERS_END ===

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


def set_session_context(database: str, schema: Optional[str] = None) -> None:
    """Pin the Snowflake session to the selected database/schema before dependent queries run."""
    if database:
        session.sql(f"USE DATABASE {qident(database)}").collect()
    if database and schema:
        session.sql(f"USE SCHEMA {qident(database)}.{qident(schema)}").collect()


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
      ARRAY_AGG(DISTINCT UPPER(COLUMN_NAME)) WITHIN GROUP (ORDER BY UPPER(COLUMN_NAME)) AS COLS
    FROM {qident(db)}.INFORMATION_SCHEMA.COLUMNS
    WHERE UPPER(COLUMN_NAME) LIKE 'EXPORT%'
    GROUP BY TABLE_SCHEMA, TABLE_NAME
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    try:
        df = norm_cols(session.sql(sql).to_pandas())
    except Exception:
        return []

    buckets: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for _, r in df.iterrows():
        schema = str(r.get("TABLE_SCHEMA") or "").strip()
        table = str(r.get("TABLE_NAME") or "").strip()
        cols_raw = r.get("COLS")
        if isinstance(cols_raw, list):
            col_values = cols_raw
        elif isinstance(cols_raw, str):
            col_values = [c.strip().strip('[]" ') for c in cols_raw.split(',') if c.strip()]
        else:
            col_values = []
        cols = {str(c).upper() for c in col_values if str(c).strip()}
        row = {"schema": schema, "table": table, "cols": cols}
        bucket = buckets.setdefault(schema, {"reports": [], "bridges": [], "params": []})
        if {'EXPORTREPORTKEY', 'EXPORTREPORTCODE', 'EXPORTREPORTNAME'}.issubset(cols):
            bucket['reports'].append(row)
        if {'EXPORTREPORTKEY', 'EXPORTPARAMKEY'}.issubset(cols):
            bucket['bridges'].append(row)
        if {'EXPORTPARAMKEY', 'EXPORTPARAMNAME', 'EXPORTPARAMTOKEN'}.issubset(cols):
            bucket['params'].append(row)

    out: List[Dict[str, Any]] = []
    for schema, groups in buckets.items():
        if groups['reports'] and groups['params']:
            if groups['bridges']:
                for reports in groups['reports']:
                    for bridge in groups['bridges']:
                        for params in groups['params']:
                            out.append({
                                'schema': schema,
                                'reports_table': reports['table'],
                                'bridge_table': bridge['table'],
                                'params_table': params['table'],
                                'mode': 'joined',
                                'score': 100,
                            })
            else:
                for reports in groups['reports']:
                    if {'EXPORTPARAMNAME', 'EXPORTPARAMTOKEN'}.issubset(reports['cols']):
                        out.append({
                            'schema': schema,
                            'reports_table': reports['table'],
                            'bridge_table': '',
                            'params_table': reports['table'],
                            'mode': 'single',
                            'score': 50,
                        })
    out.sort(key=lambda r: (-int(r.get('score') or 0), r.get('schema') or '', r.get('reports_table') or '', r.get('params_table') or ''))
    return out


def report_metadata_query(db: str, source: Dict[str, Any], report_code: Optional[str] = None, params_only: bool = False) -> str:
    schema = source['schema']
    reports_fq = f"{qident(db)}.{qident(schema)}.{qident(source['reports_table'])}"
    mode = str(source.get('mode') or 'single').lower()
    filters = ["a.ExportReportCode IS NOT NULL"]
    if report_code:
        filters.append(f"a.ExportReportCode = '{esc_sql_str(report_code)}'")
    where_clause = ' AND '.join(filters)
    if mode == 'joined' and source.get('bridge_table') and source.get('params_table'):
        bridge_fq = f"{qident(db)}.{qident(schema)}.{qident(source['bridge_table'])}"
        params_fq = f"{qident(db)}.{qident(schema)}.{qident(source['params_table'])}"
        select_list = """
      a.ExportReportCode,
      a.ExportReportName,
      a.ExportReportDescription,
      a.ExportReportSQL,
      c.ExportParamName,
      c.ExportParamToken,
      c.ExportParamPrompt
        """.strip()
        if params_only:
            select_list = """
      c.ExportParamName,
      c.ExportParamToken,
      c.ExportParamPrompt
            """.strip()
        return f"""
    SELECT {select_list}
    FROM {reports_fq} a
    LEFT JOIN {bridge_fq} b ON a.ExportReportKey = b.ExportReportKey
    LEFT JOIN {params_fq} c ON b.ExportParamKey = c.ExportParamKey
    WHERE {where_clause}
    ORDER BY a.ExportReportCode, c.ExportParamToken, c.ExportParamName
    """
    select_list = """
      ExportReportCode,
      ExportReportName,
      ExportReportDescription,
      ExportReportSQL,
      ExportParamName,
      ExportParamToken,
      ExportParamPrompt
    """.strip()
    if params_only:
        select_list = """
      ExportParamName,
      ExportParamToken,
      ExportParamPrompt
        """.strip()
    return f"""
    SELECT {select_list}
    FROM {reports_fq}
    WHERE {where_clause}
    ORDER BY ExportReportCode, ExportParamToken, ExportParamName
    """


@st.cache_data(ttl=300, show_spinner=False)
def get_export_reports(db: str, source: Dict[str, Any]) -> pd.DataFrame:
    sql = report_metadata_query(db, source)
    try:
        df = norm_cols(session.sql(sql).to_pandas())
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    subset = [c for c in ['EXPORTREPORTCODE', 'EXPORTREPORTNAME', 'EXPORTREPORTDESCRIPTION', 'EXPORTREPORTSQL'] if c in df.columns]
    return df[subset].drop_duplicates().sort_values(['EXPORTREPORTCODE', 'EXPORTREPORTNAME']).reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False)
def get_export_params_for_report(db: str, source: Dict[str, Any], report_code: str) -> pd.DataFrame:
    sql = report_metadata_query(db, source, report_code=report_code, params_only=True)
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
    export_row_by_proc: Dict[str, Dict[str, Any]] = {}
    export_params_by_proc: Dict[str, List[Dict[str, Any]]] = {}
    chosen_report_code = ''
    chosen_report_proc = ''
    if meta_candidates:
        picked = meta_candidates[0]
        reports_df = get_export_reports(db, picked)
        if not reports_df.empty and 'EXPORTREPORTCODE' in reports_df.columns:
            report_options = []
            report_records: List[Dict[str, Any]] = []
            for _, report_row in reports_df.iterrows():
                report_dict = report_row.to_dict()
                report_code = str(report_dict.get('EXPORTREPORTCODE') or '').strip()
                report_name = str(report_dict.get('EXPORTREPORTNAME') or report_code or '').strip()
                target_proc = extract_proc_name_from_export_sql(report_dict.get('EXPORTREPORTSQL'))
                if target_proc:
                    export_row_by_proc[target_proc] = report_dict
                report_options.append(f"{report_name} — {report_code}" if report_code else report_name)
                report_records.append(report_dict)
            chosen_report_label = st.selectbox('Report metadata', options=['(None)'] + report_options, key='report_pick')
            if chosen_report_label != '(None)':
                chosen_report_row = report_records[report_options.index(chosen_report_label)]
                chosen_report_code = str(chosen_report_row.get('EXPORTREPORTCODE') or '').strip()
                chosen_report_proc = extract_proc_name_from_export_sql(chosen_report_row.get('EXPORTREPORTSQL'))
                if chosen_report_proc:
                    export_row_by_proc[chosen_report_proc] = chosen_report_row
                export_params_df = get_export_params_for_report(db, picked, chosen_report_code)
                export_params_rows = export_params_df.to_dict('records') if not export_params_df.empty else []
                if chosen_report_proc:
                    export_params_by_proc[chosen_report_proc] = export_params_rows
                    st.caption(f'Silver-platter metadata linked report code {chosen_report_code or "(unknown)"} to procedure {chosen_report_proc}.')
                elif export_params_rows:
                    st.caption('Loaded report metadata, but could not infer a target procedure from ExportReportSQL.')

    proc_options = []
    for _, row in procs_df.iterrows():
        proc_name = str(row.get('NAME') or '').strip()
        type_sig = signature_types_only(first_paren_group(str(row.get('ARGUMENTS') or '()')))
        proc_meta = resolve_proc_ui_meta(db, schema, proc_name, type_sig, proc_comment=str(row.get('COMMENT') or ''), export_row=export_row_by_proc.get(proc_name.upper()))
        display = proc_meta.display_name
        if str(row.get('COMMENT') or '').strip() and display != str(row.get('COMMENT') or '').strip():
            display = f"{display} — {str(row.get('COMMENT') or '').strip()}"
        proc_options.append((str(row['PROC_ID']), display, proc_meta))
    if chosen_report_proc:
        proc_options = [p for p in proc_options if p[2].proc_name.upper() == chosen_report_proc]
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
    proc_export_params = export_params_by_proc.get(proc_name.upper(), [])
    meta_mapping = match_export_metadata(params, proc_export_params) if proc_export_params else {}
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

    preflight_issue: Optional[Dict[str, Any]] = None
    try:
        proc_ddl = get_procedure_ddl(db, schema, proc_name, type_sig)
        unsupported_use_statements = extract_unsupported_use_statements(proc_ddl)
        if unsupported_use_statements:
            preflight_issue = build_use_statement_remediation(db, schema, proc_name, unsupported_use_statements)
    except Exception:
        preflight_issue = None

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

    run_disabled = bool(validation_errors) or any(s.mode == 'UNSET' for _, s in submissions) or bool(preflight_issue)
    if validation_errors:
        st.warning('Fix the parameter errors below before running the report.')
    if preflight_issue:
        st.error(preflight_issue['summary'])
        for detail in preflight_issue.get('details') or []:
            st.caption(detail)
        if preflight_issue.get('hint'):
            st.info(preflight_issue['hint'])
        if preflight_issue.get('developer_note'):
            with st.expander('Developer handoff', expanded=False):
                st.code(preflight_issue['developer_note'])
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
                set_session_context(db, schema)
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
            if err_info.get('category') == 'unsupported_statement' and err_info.get('object_name') == 'USE':
                runtime_use_issue = build_use_statement_remediation(db, schema, proc_name, ['USE'])
                with st.expander('Developer handoff', expanded=False):
                    st.code(runtime_use_issue['developer_note'])
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
