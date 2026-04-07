from __future__ import annotations

import ast
import os
import sys

from cosecha.engine.pytest import collector as pytest_collector_module


def _first_decorator(source: str) -> ast.expr:
    module = ast.parse(source)
    function = module.body[0]
    assert isinstance(function, ast.FunctionDef)
    return function.decorator_list[0]


def test_static_skip_decision_and_skipif_paths() -> None:
    skip_decorator = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.skip(reason='by mark')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    skip_decision = pytest_collector_module._build_static_skip_decision(
        (skip_decorator,),
        ('pytest',),
    )
    assert skip_decision.skip_reason == 'by mark'

    skipif_true = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.skipif(True, reason='skipif true')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    skipif_decision = pytest_collector_module._parse_skipif_decorator(
        skipif_true,
        literal_bindings={},
        expression_bindings={},
    )
    assert skipif_decision.skip_reason == 'skipif true'

    skipif_false = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.skipif(False)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert pytest_collector_module._parse_skipif_decorator(
        skipif_false,
        literal_bindings={},
        expression_bindings={},
    ) == pytest_collector_module.PytestStaticSkipDecision()

    skipif_runtime = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.skipif('os.name == \\'nt\\'')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    runtime_decision = pytest_collector_module._parse_skipif_decorator(
        skipif_runtime,
        literal_bindings={},
        expression_bindings={},
    )
    assert runtime_decision.requires_pytest_runtime is True


def test_static_xfail_and_condition_paths() -> None:
    non_call_xfail = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.xfail',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    non_call_decision = pytest_collector_module._parse_xfail_decorator(
        non_call_xfail,
        literal_bindings={},
        expression_bindings={},
    )
    assert non_call_decision.xfail_reason == 'Expected failure by pytest xfail mark'

    xfail_true = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.xfail(True, reason='known bug', strict=True, run=False)",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    xfail_decision = pytest_collector_module._parse_xfail_decorator(
        xfail_true,
        literal_bindings={},
        expression_bindings={},
    )
    assert xfail_decision.xfail_reason == 'known bug'
    assert xfail_decision.strict is True
    assert xfail_decision.run is False

    xfail_false = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.xfail(False)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert pytest_collector_module._parse_xfail_decorator(
        xfail_false,
        literal_bindings={},
        expression_bindings={},
    ) == pytest_collector_module.PytestStaticXfailDecision()

    dynamic_run = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.xfail(True, run=runtime_flag)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    dynamic_run_decision = pytest_collector_module._parse_xfail_decorator(
        dynamic_run,
        literal_bindings={},
        expression_bindings={},
    )
    assert dynamic_run_decision.requires_pytest_runtime is True


def test_usefixtures_and_filterwarnings_decisions() -> None:
    valid_usefixtures = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.usefixtures('db', 'cache')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    valid_decision = pytest_collector_module._parse_usefixtures_decorator(
        valid_usefixtures,
    )
    assert valid_decision.fixture_names == ('db', 'cache')

    keyword_usefixtures = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.usefixtures(name='db')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    keyword_decision = pytest_collector_module._parse_usefixtures_decorator(
        keyword_usefixtures,
    )
    assert keyword_decision.issue_code == 'pytest_runtime_usefixtures'

    non_literal_usefixtures = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.usefixtures(dynamic_fixture)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    non_literal_decision = pytest_collector_module._parse_usefixtures_decorator(
        non_literal_usefixtures,
    )
    assert non_literal_decision.requires_pytest_runtime is True

    merged_decision = pytest_collector_module._build_usefixtures_decision(
        (
            valid_usefixtures,
            _first_decorator(
                '\n'.join(
                    (
                        "@pytest.mark.usefixtures('cache', 'queue')",
                        'def test_case():',
                        '    pass',
                    ),
                ),
            ),
        ),
        ('pytest',),
    )
    assert merged_decision.fixture_names == ('db', 'cache', 'queue')

    filterwarnings_decorator = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.filterwarnings('ignore:demo')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    filterwarnings_decision = pytest_collector_module._build_filterwarnings_decision(
        (filterwarnings_decorator,),
        ('pytest',),
    )
    assert filterwarnings_decision.requires_pytest_runtime is True


def test_merge_skip_and_xfail_decisions() -> None:
    local_skip = pytest_collector_module.PytestStaticSkipDecision(
        skip_reason='local',
    )
    assert pytest_collector_module._merge_skip_decisions(
        inherited_skip_reason='inherited',
        inherited_skip_issue=None,
        local_decision=local_skip,
    ).skip_reason == 'inherited'

    inherited_issue = pytest_collector_module._merge_skip_decisions(
        inherited_skip_reason=None,
        inherited_skip_issue='skip issue',
        local_decision=pytest_collector_module.PytestStaticSkipDecision(),
    )
    assert inherited_issue.issue_code == 'pytest_unsupported_skip_condition'

    inherited_runtime = pytest_collector_module._merge_skip_decisions(
        inherited_skip_reason=None,
        inherited_skip_issue=None,
        inherited_runtime_reason='runtime skip',
        local_decision=pytest_collector_module.PytestStaticSkipDecision(),
    )
    assert inherited_runtime.requires_pytest_runtime is True

    inherited_xfail = pytest_collector_module.PytestStaticXfailDecision(
        xfail_reason='xfail inherited',
    )
    local_xfail = pytest_collector_module.PytestStaticXfailDecision(
        xfail_reason='xfail local',
    )
    assert (
        pytest_collector_module._merge_xfail_decisions(
            inherited_xfail=inherited_xfail,
            local_decision=local_xfail,
        ).xfail_reason
        == 'xfail inherited'
    )


def test_static_condition_evaluation_and_comparators() -> None:
    assert pytest_collector_module._evaluate_static_skip_condition(
        ast.parse('True and not False').body[0].value,  # type: ignore[attr-defined]
        literal_bindings={},
        expression_bindings={},
    )

    compare_node = ast.parse('sys.platform == sys.platform').body[0].value  # type: ignore[attr-defined]
    assert pytest_collector_module._evaluate_static_skip_compare(
        compare_node,
        literal_bindings={},
        expression_bindings={},
    )

    membership_node = ast.parse("'x' in ('x', 'y')").body[0].value  # type: ignore[attr-defined]
    assert pytest_collector_module._evaluate_static_skip_compare(
        membership_node,
        literal_bindings={},
        expression_bindings={},
    )
    assert pytest_collector_module._skip_non_membership_compare('z', ('x', 'y'))
    assert pytest_collector_module._build_skip_comparator(ast.Eq()) is not None
    assert pytest_collector_module._build_skip_comparator(ast.NotIn()) is not None
    assert pytest_collector_module._build_skip_comparator(ast.MatMult()) is None
    assert pytest_collector_module._is_supported_static_compare(compare_node)
    assert not pytest_collector_module._is_supported_static_compare(
        ast.parse('value').body[0].value,  # type: ignore[attr-defined]
    )


def test_marker_extraction_helpers() -> None:
    decorator = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.skipif(condition=True, reason='by keyword')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert isinstance(decorator, ast.Call)
    assert (
        pytest_collector_module._extract_marker_reason(
            decorator,
            default='default',
            literal_bindings={},
        )
        == 'by keyword'
    )
    condition_node = pytest_collector_module._extract_marker_condition_node(decorator)
    assert condition_node is not None

    bool_keyword = pytest_collector_module._extract_bool_marker_keyword(
        _first_decorator(
            '\n'.join(
                (
                    '@pytest.mark.xfail(True, strict=True)',
                    'def test_case():',
                    '    pass',
                ),
            ),
        ),
        keyword_name='strict',
        default=False,
        literal_bindings={},
        expression_bindings={},
    )
    assert bool_keyword is True


def test_xfail_raises_extraction_and_symbol_paths() -> None:
    decorator = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.xfail(True, raises=(ValueError, custom.errors.DemoError))',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert isinstance(decorator, ast.Call)
    raises_paths = pytest_collector_module._extract_xfail_raises_paths(
        decorator,
        literal_bindings={},
        expression_bindings={},
    )
    assert raises_paths == ('ValueError', 'custom.errors.DemoError')

    single_name = ast.parse('ValueError').body[0].value  # type: ignore[attr-defined]
    assert pytest_collector_module._extract_exception_symbol_paths(single_name) == (
        'ValueError',
    )
    assert pytest_collector_module._extract_exception_symbol_path(
        ast.parse('pkg.error.CustomError').body[0].value,  # type: ignore[attr-defined]
    ) == 'pkg.error.CustomError'


def test_runtime_string_condition_and_skip_operands() -> None:
    literal_bindings = {}
    expression_bindings = {}
    string_condition = ast.parse("'sys.platform == \"darwin\"'").body[0].value  # type: ignore[attr-defined]
    assert pytest_collector_module._is_runtime_string_condition(
        string_condition,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )

    assert pytest_collector_module._evaluate_skip_operand(
        ast.parse('os.name').body[0].value,  # type: ignore[attr-defined]
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    ) == os.name
    assert pytest_collector_module._evaluate_skip_operand(
        ast.parse('sys.platform').body[0].value,  # type: ignore[attr-defined]
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    ) == sys.platform
    assert pytest_collector_module._evaluate_skip_operand(
        ast.parse('sys.version_info').body[0].value,  # type: ignore[attr-defined]
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    ) == sys.version_info
    assert pytest_collector_module._evaluate_skip_operand(
        ast.parse('sys.implementation.name').body[0].value,  # type: ignore[attr-defined]
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    ) == sys.implementation.name


def test_skipif_and_marker_reason_fallback_paths() -> None:
    non_call_skipif = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.skipif',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    non_call_decision = pytest_collector_module._parse_skipif_decorator(
        non_call_skipif,
        literal_bindings={},
        expression_bindings={},
    )
    assert non_call_decision.issue_code == 'pytest_unsupported_skip_condition'

    missing_condition = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.skipif(reason='reason only')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    missing_condition_decision = pytest_collector_module._parse_skipif_decorator(
        missing_condition,
        literal_bindings={},
        expression_bindings={},
    )
    assert missing_condition_decision.issue_code == (
        'pytest_unsupported_skip_condition'
    )

    dynamic_condition = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.skipif(runtime_flag)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    dynamic_condition_decision = pytest_collector_module._parse_skipif_decorator(
        dynamic_condition,
        literal_bindings={},
        expression_bindings={},
    )
    assert dynamic_condition_decision.requires_pytest_runtime is True

    positional_reason = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.skipif(True, 'positional reason')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert isinstance(positional_reason, ast.Call)
    assert (
        pytest_collector_module._extract_marker_reason(
            positional_reason,
            default='default',
            literal_bindings={},
        )
        == 'positional reason'
    )

    non_string_positional_reason = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.skipif(True, runtime_reason)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert isinstance(non_string_positional_reason, ast.Call)
    assert (
        pytest_collector_module._extract_marker_reason(
            non_string_positional_reason,
            default='default',
            literal_bindings={},
        )
        == 'default'
    )


def test_xfail_runtime_and_exception_path_guards() -> None:
    xfail_without_condition = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.xfail(reason='known')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert isinstance(xfail_without_condition, ast.Call)
    assert (
        pytest_collector_module._validate_xfail_condition(
            xfail_without_condition,
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )

    string_condition = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.xfail('sys.platform == \"darwin\"')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert isinstance(string_condition, ast.Call)
    string_condition_decision = pytest_collector_module._validate_xfail_condition(
        string_condition,
        literal_bindings={},
        expression_bindings={},
    )
    assert string_condition_decision is not None
    assert string_condition_decision.requires_pytest_runtime is True

    dynamic_condition = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.xfail(runtime_flag)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert isinstance(dynamic_condition, ast.Call)
    dynamic_condition_decision = pytest_collector_module._validate_xfail_condition(
        dynamic_condition,
        literal_bindings={},
        expression_bindings={},
    )
    assert dynamic_condition_decision is not None
    assert dynamic_condition_decision.requires_pytest_runtime is True

    strict_dynamic = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.xfail(True, strict=runtime_flag)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    strict_dynamic_decision = pytest_collector_module._parse_xfail_decorator(
        strict_dynamic,
        literal_bindings={},
        expression_bindings={},
    )
    assert strict_dynamic_decision.requires_pytest_runtime is True

    raises_dynamic = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.xfail(True, raises=(ValueError, runtime_error()))',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    raises_dynamic_decision = pytest_collector_module._parse_xfail_decorator(
        raises_dynamic,
        literal_bindings={},
        expression_bindings={},
    )
    assert raises_dynamic_decision.requires_pytest_runtime is True

    raises_type = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.xfail(True, raises=RuntimeError)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert isinstance(raises_type, ast.Call)
    assert pytest_collector_module._extract_xfail_raises_paths(
        raises_type,
        literal_bindings={'RuntimeError': RuntimeError},
        expression_bindings={},
    ) == ('RuntimeError',)

    assert (
        pytest_collector_module._extract_exception_symbol_path(
            ast.parse('1').body[0].value,  # type: ignore[attr-defined]
        )
        is None
    )
    assert (
        pytest_collector_module._extract_exception_symbol_path(
            ast.parse('runtime_error().value').body[0].value,  # type: ignore[attr-defined]
        )
        is None
    )
    assert (
        pytest_collector_module._extract_exception_symbol_paths(
            ast.parse('(ValueError, runtime_error())').body[0].value,  # type: ignore[attr-defined]
        )
        is None
    )


def test_static_skip_condition_compare_failure_paths() -> None:
    assert (
        pytest_collector_module._evaluate_static_skip_condition(
            ast.parse('True and runtime_flag').body[0].value,  # type: ignore[attr-defined]
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )
    assert (
        pytest_collector_module._evaluate_static_skip_condition(
            ast.parse('not runtime_flag').body[0].value,  # type: ignore[attr-defined]
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )

    invalid_boolop_node = ast.BoolOp(
        op=ast.BitAnd(),  # type: ignore[arg-type]
        values=[ast.Constant(value=True), ast.Constant(value=False)],
    )
    assert (
        pytest_collector_module._evaluate_static_skip_condition(
            invalid_boolop_node,
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )
    assert pytest_collector_module._evaluate_static_skip_condition(
        ast.parse('False or True').body[0].value,  # type: ignore[attr-defined]
        literal_bindings={},
        expression_bindings={},
    )

    assert (
        pytest_collector_module._evaluate_static_skip_compare(
            ast.parse('runtime_flag == 1').body[0].value,  # type: ignore[attr-defined]
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )
    assert (
        pytest_collector_module._evaluate_static_skip_compare(
            ast.parse('1 == runtime_flag').body[0].value,  # type: ignore[attr-defined]
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )
    assert (
        pytest_collector_module._evaluate_static_skip_compare(
            ast.parse('1 is 1').body[0].value,  # type: ignore[attr-defined]
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )
    assert (
        pytest_collector_module._evaluate_static_skip_compare(
            ast.parse('True == runtime_flag').body[0].value,  # type: ignore[attr-defined]
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )
    invalid_comparator_node = ast.Compare(
        left=ast.Constant(value=True),
        ops=[ast.Add()],  # type: ignore[list-item]
        comparators=[ast.Constant(value=True)],
    )
    assert (
        pytest_collector_module._evaluate_static_skip_compare(
            invalid_comparator_node,
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )
    assert (
        pytest_collector_module._evaluate_static_skip_compare(
            ast.parse("True < 'a'").body[0].value,  # type: ignore[attr-defined]
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )
    assert (
        pytest_collector_module._evaluate_static_skip_compare(
            ast.parse('True == False == True').body[0].value,  # type: ignore[attr-defined]
            literal_bindings={},
            expression_bindings={},
        )
        is False
    )
    assert (
        pytest_collector_module._skip_membership_compare('value', 'scalar')
        is None
    )
    assert (
        pytest_collector_module._extract_exception_symbol_paths(
            ast.parse('runtime_error()').body[0].value,  # type: ignore[attr-defined]
        )
        is None
    )


def test_static_decision_and_issue_helper_branches() -> None:
    skipif_decorator = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.skipif(True)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    skip_decision = pytest_collector_module._build_static_skip_decision(
        (skipif_decorator,),
        ('pytest',),
        literal_bindings={},
        expression_bindings={},
    )
    assert skip_decision.skip_reason is not None

    xfail_decorator = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.xfail(True)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    xfail_decision = pytest_collector_module._build_static_xfail_decision(
        (xfail_decorator,),
        ('pytest',),
        literal_bindings={},
        expression_bindings={},
    )
    assert xfail_decision.xfail_reason is not None

    keyword_usefixtures = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.usefixtures(name='db')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    usefixtures_decision = pytest_collector_module._build_usefixtures_decision(
        (keyword_usefixtures,),
        ('pytest',),
    )
    assert usefixtures_decision.issue_code == 'pytest_runtime_usefixtures'

    local_skip = pytest_collector_module._merge_skip_decisions(
        inherited_skip_reason=None,
        inherited_skip_issue=None,
        local_decision=pytest_collector_module.PytestStaticSkipDecision(
            skip_reason='local skip',
        ),
    )
    assert local_skip.skip_reason == 'local skip'

    non_call_skip = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.skip',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert (
        pytest_collector_module._parse_skip_decorator(
            non_call_skip,
            literal_bindings={},
        ).skip_reason
        == 'Skipped by pytest mark'
    )

    assert isinstance(xfail_decorator, ast.Call)
    issue = pytest_collector_module._build_xfail_issue(
        xfail_decorator,
        'unsupported',
    )
    assert issue.issue_code == 'pytest_unsupported_xfail_condition'

    non_call_usefixtures = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.usefixtures',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    non_call_usefixtures_decision = pytest_collector_module._parse_usefixtures_decorator(
        non_call_usefixtures,
    )
    assert non_call_usefixtures_decision.requires_pytest_runtime is True
