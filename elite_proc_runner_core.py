from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
import json
import math
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
        allow_null=allow_null,
        lookup_key=lookup_key,
        list_encoding='csv',
        required=not has_default,
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


def validate_variant_json(raw_value: Any) -> Optional[str]:
    if raw_value in (None, ''):
        return None
    try:
        json.loads(str(raw_value))
        return None
    except Exception as exc:
        return str(exc)
