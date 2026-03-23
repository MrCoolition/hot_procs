from elite_proc_runner_core import (
    ProcUIMeta,
    humanize_identifier_advanced,
    is_time,
    is_timestamp,
    sql_literal,
    match_export_metadata,
    disambiguate_lookup_labels,
    build_param_submission,
    build_call_sql,
    proc_instance_key,
    analyze_execution_error,
    resolve_param_ui_meta,
)


def test_humanize_identifier_advanced_examples():
    assert humanize_identifier_advanced('BUSINESSKEY') == 'Business'
    assert humanize_identifier_advanced('EXPORTFORMATCODE') == 'Export Format'
    assert humanize_identifier_advanced('CUSTOMERGROUPKEYLIST') == 'Customer Groups'
    assert humanize_identifier_advanced('SESSIONYEARCONTRACTPERIODKEYFROM') == 'Year Contract Period From'


def test_time_vs_timestamp_detection():
    assert is_time('TIME')
    assert not is_time('TIMESTAMP_NTZ')
    assert is_timestamp('TIMESTAMP_NTZ')
    assert is_timestamp('TIMESTAMP_LTZ')
    assert is_timestamp('TIMESTAMP_TZ')
    assert is_timestamp('DATETIME')


def test_sql_literal_generation():
    assert sql_literal('2025-01-01 10:00:00', 'TIMESTAMP_NTZ').startswith('TO_TIMESTAMP_NTZ')
    assert sql_literal('2025-01-01', 'DATE') == "DATE '2025-01-01'"
    assert sql_literal('10:00:00', 'TIME') == "TIME '10:00:00'"
    assert sql_literal(42, 'NUMBER') == '42'
    assert sql_literal(True, 'BOOLEAN') == 'TRUE'
    assert sql_literal('{"a":1}', 'VARIANT').startswith('PARSE_JSON')
    assert sql_literal('abc', 'VARCHAR') == "'abc'"


def test_export_metadata_matching_token_and_name_then_safe_ordinal():
    params = [
        {'name': 'BUSINESSKEY'},
        {'name': 'ARG2'},
    ]
    rows = [
        {'EXPORTPARAMTOKEN': '@BusinessKey', 'EXPORTPARAMNAME': 'Business', 'EXPORTPARAMPROMPT': 'Pick a business'},
        {'EXPORTPARAMNAME': 'ARG2', 'ORDINAL': 2, 'EXPORTPARAMPROMPT': 'Second value'},
    ]
    mapping = match_export_metadata(params, rows)
    assert mapping['BUSINESSKEY']['EXPORTPARAMTOKEN'] == '@BusinessKey'
    assert mapping['ARG2']['EXPORTPARAMPROMPT'] == 'Second value'


def test_lookup_label_disambiguation_algorithm():
    labels = disambiguate_lookup_labels([
        {'id': 1, 'label': 'Springfield', 'detail': 'Illinois', 'code': 'IL'},
        {'id': 2, 'label': 'Springfield', 'detail': 'Missouri', 'code': 'MO'},
    ])
    assert labels[1] == 'Springfield — Illinois'
    assert labels[2] == 'Springfield — Missouri'


def test_call_builder_omits_defaulted_params():
    submissions = [
        ('BUSINESSKEY', build_param_submission('DEFAULT', None, 'NUMBER', 'Use procedure default')),
        ('EXPORTFORMATCODE', build_param_submission('VALUE', 'CSV', 'VARCHAR', 'CSV')),
    ]
    sql = build_call_sql('DB', 'SCHEMA', 'PROC', submissions)
    assert 'BUSINESSKEY' not in sql
    assert 'EXPORTFORMATCODE => ' in sql


def test_overload_safe_session_key_generation():
    assert proc_instance_key('DB', 'SCH', 'PROC', '(NUMBER)') != proc_instance_key('DB', 'SCH', 'PROC', '(VARCHAR)')


def test_call_builder_supports_positional_arguments_for_generic_param_names():
    submissions = [
        ('ARG1', build_param_submission('NULL', None, 'NUMBER', 'NULL')),
        ('ARG2', build_param_submission('VALUE', 'CSV', 'VARCHAR', 'CSV')),
    ]
    sql = build_call_sql('DB', 'SCHEMA', 'PROC', submissions, named_args=False)
    assert sql == "CALL \"DB\".\"SCHEMA\".\"PROC\"(NULL, 'CSV');"


def test_analyze_execution_error_for_missing_function_dependency():
    err = analyze_execution_error("(1304): x: SQL compilation error: Unknown user-defined function SYSTEM.GETCONTRACTPERIODKEYFORDATE.")
    assert err['category'] == 'missing_function'
    assert err['object_name'] == 'SYSTEM.GETCONTRACTPERIODKEYFORDATE'
    assert 'Verify that the function exists' in err['hint']


def test_required_params_do_not_offer_null_mode_even_when_global_nulls_enabled():
    proc_meta = ProcUIMeta('DB', 'SCHEMA', 'PROC', '(NUMBER)', 'Proc')

    required_param = resolve_param_ui_meta(proc_meta, {'name': 'TABKEY', 'type': 'NUMBER'}, 1, allow_null=True)
    optional_param = resolve_param_ui_meta(proc_meta, {'name': 'TABKEY', 'type': 'NUMBER', 'default': '42'}, 1, allow_null=True)

    assert required_param.required is True
    assert required_param.allow_null is False
    assert optional_param.required is False
    assert optional_param.allow_null is True
