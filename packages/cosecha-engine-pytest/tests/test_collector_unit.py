from __future__ import annotations

import asyncio
import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.engine.pytest import collector as pytest_collector_module


def _first_function(source: str) -> ast.FunctionDef:
    module = ast.parse(source)
    function = module.body[0]
    assert isinstance(function, ast.FunctionDef)
    return function


def _first_decorator(source: str) -> ast.expr:
    return _first_function(source).decorator_list[0]


def _parse_expression(source: str) -> ast.expr:
    return ast.parse(source, mode='eval').body


def test_extract_runtime_requirements_and_merge_helpers() -> None:
    function = _first_function(
        '\n'.join(
            (
                "@pytest.mark.requires('db')",
                "@pytest.mark.requires('db')",
                '@pytest.mark.requires(1)',
                "@pytest.mark.requires_capability('db', 'rw')",
                "@pytest.mark.requires_capability('db', 'rw')",
                "@pytest.mark.requires_capability('db')",
                "@pytest.mark.requires_mode('db', 'sync')",
                '@pytest.mark.requires_mode("db", 1)',
                "@pytest.mark.disallow_mode('db', 'legacy')",
                "@pytest.mark.disallow_mode('db', 'legacy')",
                '@pytest.mark.disallow_mode("db", 1)',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    interfaces, capabilities, required_modes, disallowed_modes = (
        pytest_collector_module._extract_runtime_requirements(
            list(function.decorator_list),
            ('pytest',),
            inherited_interfaces=('api',),
            inherited_capabilities=(('api', 'read'),),
            inherited_required_modes=(('api', 'sync'),),
            inherited_disallowed_modes=(('api', 'legacy'),),
        )
    )
    assert interfaces == ('api', 'db')
    assert capabilities == (('api', 'read'), ('db', 'rw'))
    assert required_modes == (('api', 'sync'), ('db', 'sync'))
    assert disallowed_modes == (('api', 'legacy'), ('db', 'legacy'))

    assert pytest_collector_module._merge_selection_labels(
        ('api',),
        ('api', 'slow'),
    ) == ('api', 'slow')
    assert pytest_collector_module._merge_usefixture_names(
        ('db',),
        ('db', 'cache'),
    ) == ('db', 'cache')


def test_special_marker_and_literal_argument_guards() -> None:
    assert (
        pytest_collector_module._extract_special_marker_name(
            _parse_expression('value'),
            {'pytest'},
            marker_names={'requires'},
        )
        is None
    )
    assert (
        pytest_collector_module._extract_special_marker_name(
            _parse_expression('pytest.other.requires'),
            {'pytest'},
            marker_names={'requires'},
        )
        is None
    )
    assert (
        pytest_collector_module._extract_special_marker_name(
            _parse_expression('pytest.requires'),
            {'pytest'},
            marker_names={'requires'},
        )
        is None
    )
    assert (
        pytest_collector_module._extract_special_marker_name(
            _parse_expression('pytest.mark.other'),
            {'pytest'},
            marker_names={'requires'},
        )
        is None
    )
    assert (
        pytest_collector_module._extract_special_marker_name(
            _parse_expression('pkg.mark.requires'),
            {'pytest'},
            marker_names={'requires'},
        )
        is None
    )

    decorator = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.requires('db')",
                'def test_case():',
                '    pass',
            ),
        ),
    )
    assert isinstance(decorator, ast.Call)
    assert (
        pytest_collector_module._extract_string_literal_argument(
            decorator,
            index=2,
        )
        is None
    )
    assert (
        pytest_collector_module._extract_string_literal_argument(
            _first_decorator(
                '\n'.join(
                    (
                        '@pytest.mark.requires(value)',
                        'def test_case():',
                        '    pass',
                    ),
                ),
            ),
            index=0,
        )
        is None
    )


def test_parametrize_ids_indirect_arg_names_and_cases() -> None:
    positional_ids = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1], ['id-1'])",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(positional_ids, ast.Call)
    assert pytest_collector_module._parse_parametrize_ids(
        positional_ids,
        literal_bindings={},
    ) == ('id-1',)

    keyword_ids = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1], ids=['id-2'])",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(keyword_ids, ast.Call)
    assert pytest_collector_module._parse_parametrize_ids(
        keyword_ids,
        literal_bindings={},
    ) == ('id-2',)

    invalid_ids = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1], ids=runtime_ids)",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(invalid_ids, ast.Call)
    assert (
        pytest_collector_module._parse_parametrize_ids(
            invalid_ids,
            literal_bindings={},
        )
        is False
    )

    non_string_ids = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1], ids=[1])",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(non_string_ids, ast.Call)
    assert (
        pytest_collector_module._parse_parametrize_ids(
            non_string_ids,
            literal_bindings={},
        )
        is False
    )

    assert pytest_collector_module._parse_parametrize_indirect(
        keyword_ids,
        arg_names=('value',),
        literal_bindings={},
    ) == ()
    indirect_true = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1], indirect=True)",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(indirect_true, ast.Call)
    assert pytest_collector_module._parse_parametrize_indirect(
        indirect_true,
        arg_names=('value',),
        literal_bindings={},
    ) == ('value',)

    indirect_list = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize(('a','b'), [(1,2)], indirect=['b', 'b'])",
                'def test_case(a, b):',
                '    pass',
            ),
        ),
    )
    assert isinstance(indirect_list, ast.Call)
    assert pytest_collector_module._parse_parametrize_indirect(
        indirect_list,
        arg_names=('a', 'b'),
        literal_bindings={},
    ) == ('b',)

    invalid_indirect = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1], indirect=['missing'])",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(invalid_indirect, ast.Call)
    assert (
        pytest_collector_module._parse_parametrize_indirect(
            invalid_indirect,
            arg_names=('value',),
            literal_bindings={},
        )
        is False
    )

    assert pytest_collector_module._parse_parametrize_arg_names(
        _parse_expression('runtime_name'),
        literal_bindings={},
    ) is None
    assert pytest_collector_module._parse_parametrize_arg_names(
        _parse_expression("['a', 1]"),
        literal_bindings={},
    ) is None

    context = pytest_collector_module._PytestParametrizeContext(
        marker_aliases={'pytest'},
        literal_bindings={},
        expression_bindings={},
    )
    assert (
        pytest_collector_module._parse_parametrize_cases(
            _parse_expression('[(1,), (2,)]'),
            arg_names=('value',),
            case_ids=('one',),
            parametrize_context=context,
        )
        is None
    )
    assert (
        pytest_collector_module._parse_parametrize_cases(
            _parse_expression('[(1,), (2,)]'),
            arg_names=('value',),
            case_ids=('one', 'two', 'three'),
            parametrize_context=context,
        )
        is None
    )


def test_parametrize_row_helpers_and_literal_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert pytest_collector_module._normalize_parametrize_row_values(
        1,
        width=1,
    ) == (1,)
    assert (
        pytest_collector_module._normalize_parametrize_row_values(
            (1, 2),
            width=1,
        )
        is None
    )
    assert (
        pytest_collector_module._normalize_parametrize_row_values(
            1,
            width=2,
        )
        is None
    )
    assert pytest_collector_module._normalize_parametrize_row_values(
        (1, 2),
        width=2,
    ) == (1, 2)

    assert (
        pytest_collector_module._resolve_literal_mapping(
            _parse_expression('value'),
            literal_bindings={},
        )
        is pytest_collector_module.PARSE_FAILURE
    )
    assert (
        pytest_collector_module._resolve_literal_mapping(
            _parse_expression('{missing: 1}'),
            literal_bindings={},
        )
        is pytest_collector_module.PARSE_FAILURE
    )
    assert pytest_collector_module._resolve_literal_mapping(
        _parse_expression("{'key': 1}"),
        literal_bindings={},
    ) == {'key': 1}

    context = pytest_collector_module._PytestParametrizeContext(
        marker_aliases={'pytest'},
        literal_bindings={},
        expression_bindings={},
    )
    assert (
        pytest_collector_module._parse_pytest_param_call(
            _parse_expression('pytest.param(runtime_value)'),
            width=1,
            marker_aliases={'pytest'},
            parametrize_context=context,
        )
        is pytest_collector_module.PARSE_FAILURE
    )
    assert (
        pytest_collector_module._parse_pytest_param_call(
            _parse_expression('pytest.param(1, id=1)'),
            width=1,
            marker_aliases={'pytest'},
            parametrize_context=context,
        )
        is pytest_collector_module.PARSE_FAILURE
    )
    assert (
        pytest_collector_module._parse_pytest_param_call(
            _parse_expression('pytest.param(1, 2)'),
            width=1,
            marker_aliases={'pytest'},
            parametrize_context=context,
        )
        is pytest_collector_module.PARSE_FAILURE
    )

    monkeypatch.setattr(
        pytest_collector_module,
        '_is_pytest_param_call',
        lambda *_args, **_kwargs: True,
    )
    assert (
        pytest_collector_module._parse_pytest_param_call(
            _parse_expression('value'),
            width=1,
            marker_aliases={'pytest'},
            parametrize_context=context,
        )
        is pytest_collector_module.PARSE_FAILURE
    )

    nested_marks = pytest_collector_module._extract_pytest_param_mark_nodes(
        _parse_expression('[pytest.mark.api, [pytest.mark.slow]]'),
        expression_bindings={},
    )
    assert nested_marks is not None
    assert len(nested_marks) == 2
    additive_marks = pytest_collector_module._extract_pytest_param_mark_nodes(
        _parse_expression('pytest.mark.api + pytest.mark.slow'),
        expression_bindings={},
    )
    assert additive_marks is not None
    assert len(additive_marks) == 2


def test_discover_imported_static_bindings_and_source_bindings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / 'source.py'
    source_path.write_text(
        '\n'.join(
            (
                'from alpha import *',
                'from beta import value as renamed, expression, missing',
                'from gamma import *',
            ),
        ),
        encoding='utf-8',
    )
    alpha_path = tmp_path / 'alpha.py'
    beta_path = tmp_path / 'beta.py'
    gamma_path = tmp_path / 'gamma.py'
    original_discover_source_static_bindings = (
        pytest_collector_module._discover_source_static_bindings
    )

    assert pytest_collector_module._discover_imported_static_bindings(
        source_path,
        root_path=None,
        configured_definition_paths=(),
        active_sources=(source_path.resolve(),),
    ) == ({}, {})
    assert pytest_collector_module._discover_imported_static_bindings(
        tmp_path / 'missing.py',
        root_path=None,
        configured_definition_paths=(),
    ) == ({}, {})

    monkeypatch.setattr(
        pytest_collector_module,
        '_resolve_imported_source_path',
        lambda statement, **_kwargs: {
            'alpha': alpha_path,
            'beta': beta_path,
            'gamma': gamma_path,
        }.get(statement.module),
    )
    expression_node = _parse_expression('bound_expression')
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_source_static_bindings',
        lambda path, **_kwargs: (
            ({'star_literal': 1}, {'star_expr': expression_node})
            if Path(path).resolve() == alpha_path.resolve()
            else (
                ({'value': 2}, {'expression': _parse_expression('42')})
                if Path(path).resolve() == beta_path.resolve()
                else ({}, {})
            )
        ),
    )

    literals, expressions = pytest_collector_module._discover_imported_static_bindings(
        source_path,
        root_path=tmp_path,
        configured_definition_paths=(),
    )
    assert literals == {'star_literal': 1, 'renamed': 2}
    assert set(expressions) == {'star_expr', 'expression'}
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_source_static_bindings',
        original_discover_source_static_bindings,
    )

    existing_source = tmp_path / 'existing.py'
    existing_source.write_text('value = 1\n', encoding='utf-8')
    assert pytest_collector_module._discover_source_static_bindings(
        existing_source,
        root_path=None,
        configured_definition_paths=(),
        active_sources=(existing_source.resolve(),),
    ) == ({}, {})

    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_static_binding_context',
        lambda *_args, **_kwargs: ({'literal': 1}, {'expr': _parse_expression('x')}),
    )
    source_literals, source_expressions = (
        pytest_collector_module._discover_source_static_bindings(
            existing_source,
            root_path=None,
            configured_definition_paths=(),
        )
    )
    assert source_literals == {'literal': 1}
    assert set(source_expressions) == {'expr'}


def test_discover_imported_fixture_bindings_and_exports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / 'source.py'
    source_path.write_text(
        '\n'.join(
            (
                'from alpha import *',
                'from beta import fixture_a as alias_a, fixture_b, missing',
                'from gamma import *',
            ),
        ),
        encoding='utf-8',
    )
    alpha_path = tmp_path / 'alpha.py'
    beta_path = tmp_path / 'beta.py'
    gamma_path = tmp_path / 'gamma.py'
    original_discover_fixture_export_bindings = (
        pytest_collector_module._discover_fixture_export_bindings
    )
    fixture_a = pytest_collector_module.PytestFixtureDefinition(
        function_name='fixture_a',
        line=1,
        source_path=str(alpha_path),
    )
    fixture_b = pytest_collector_module.PytestFixtureDefinition(
        function_name='fixture_b',
        line=2,
        source_path=str(beta_path),
    )

    assert pytest_collector_module._discover_imported_fixture_bindings(
        source_path,
        root_path=None,
        active_sources=(source_path.resolve(),),
    ) == {}
    assert pytest_collector_module._discover_imported_fixture_bindings(
        tmp_path / 'missing.py',
        root_path=None,
    ) == {}

    monkeypatch.setattr(
        pytest_collector_module,
        '_resolve_imported_source_path',
        lambda statement, **_kwargs: {
            'alpha': alpha_path,
            'beta': beta_path,
            'gamma': gamma_path,
        }.get(statement.module),
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_export_bindings',
        lambda path, **_kwargs: (
            {'fixture_a': fixture_a}
            if Path(path).resolve() == alpha_path.resolve()
            else (
                {'fixture_a': fixture_a, 'fixture_b': fixture_b}
                if Path(path).resolve() == beta_path.resolve()
                else {}
            )
        ),
    )
    bindings = pytest_collector_module._discover_imported_fixture_bindings(
        source_path,
        root_path=tmp_path,
    )
    assert bindings['fixture_a'] is fixture_a
    assert bindings['fixture_b'] is fixture_b
    assert bindings['alias_a'] is fixture_a
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_export_bindings',
        original_discover_fixture_export_bindings,
    )

    export_source = tmp_path / 'export.py'
    export_source.write_text('def fixture_local():\n    return 1\n', encoding='utf-8')
    fixture_local = pytest_collector_module.PytestFixtureDefinition(
        function_name='fixture_local',
        line=1,
        source_path=str(export_source),
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_aliases',
        lambda _module: {'pytest'},
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_imported_fixture_bindings',
        lambda *_args, **_kwargs: {'fixture_a': fixture_a},
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_definitions',
        lambda *_args, **_kwargs: {'fixture_local': fixture_local},
    )
    exported = pytest_collector_module._discover_fixture_export_bindings(
        export_source,
        root_path=tmp_path,
    )
    assert set(exported) == {'fixture_a', 'fixture_local'}


def test_resolve_import_and_module_spec_paths(tmp_path: Path) -> None:
    root_path = tmp_path / 'project'
    tests_path = root_path / 'tests'
    pkg_path = root_path / 'pkg'
    tests_path.mkdir(parents=True)
    pkg_path.mkdir(parents=True)
    source_path = tests_path / 'test_demo.py'
    source_path.write_text('pass\n', encoding='utf-8')
    absolute_module = pkg_path / 'mod.py'
    absolute_module.write_text('', encoding='utf-8')
    relative_module = tests_path / 'helpers.py'
    relative_module.write_text('', encoding='utf-8')

    statement_pytest = ast.parse('from pytest import mark').body[0]
    assert isinstance(statement_pytest, ast.ImportFrom)
    assert (
        pytest_collector_module._resolve_imported_source_path(
            statement_pytest,
            source_path=source_path,
            root_path=root_path,
            configured_definition_paths=(),
        )
        is None
    )

    relative_statement = ast.parse('from .helpers import value').body[0]
    assert isinstance(relative_statement, ast.ImportFrom)
    assert pytest_collector_module._resolve_imported_source_path(
        relative_statement,
        source_path=source_path,
        root_path=root_path,
        configured_definition_paths=(),
    ) == relative_module.resolve()

    configured_path = root_path / 'definitions' / 'defs.py'
    configured_path.parent.mkdir(parents=True)
    configured_path.write_text('', encoding='utf-8')
    configured_target = root_path / 'definitions' / 'pkg2.py'
    configured_target.write_text('', encoding='utf-8')
    configured_statement = ast.parse('from pkg2 import value').body[0]
    assert isinstance(configured_statement, ast.ImportFrom)
    assert pytest_collector_module._resolve_imported_source_path(
        configured_statement,
        source_path=source_path,
        root_path=None,
        configured_definition_paths=(configured_path,),
    ) == configured_target.resolve()

    assert (
        pytest_collector_module._resolve_module_spec_source_path(
            '',
            source_path=source_path,
            root_path=root_path,
            configured_definition_paths=(configured_path,),
        )
        is None
    )
    assert pytest_collector_module._resolve_module_spec_source_path(
        'pkg.mod',
        source_path=source_path,
        root_path=root_path,
        configured_definition_paths=(configured_path,),
    ) == absolute_module.resolve()
    assert (
        pytest_collector_module._resolve_module_spec_source_path(
            'missing.module',
            source_path=source_path,
            root_path=root_path,
            configured_definition_paths=(),
        )
        is None
    )


def test_plugin_resolution_and_runtime_reasoning(tmp_path: Path) -> None:
    root_path = tmp_path / 'project'
    tests_path = root_path / 'tests'
    plugins_path = root_path / 'plugins'
    tests_path.mkdir(parents=True)
    plugins_path.mkdir(parents=True)

    test_file = tests_path / 'test_demo.py'
    test_file.write_text(
        "pytest_plugins = ('plugins.a', 'external.plugin')\n",
        encoding='utf-8',
    )
    dynamic_file = tests_path / 'test_dynamic.py'
    dynamic_file.write_text(
        '\n'.join(
            (
                'plugins_ref = runtime_plugins',
                'pytest_plugins = plugins_ref',
            ),
        ),
        encoding='utf-8',
    )
    plugin_a = plugins_path / 'a.py'
    plugin_a.write_text("pytest_plugins = ('plugins.b',)\n", encoding='utf-8')
    plugin_b = plugins_path / 'b.py'
    plugin_b.write_text('', encoding='utf-8')

    assert pytest_collector_module._discover_pytest_plugin_sources(
        test_file,
        root_path=root_path,
        active_sources=(test_file.resolve(),),
    ) == ()
    assert pytest_collector_module._discover_pytest_plugin_sources(
        root_path / 'missing.py',
        root_path=root_path,
    ) == ()

    plugin_sources = pytest_collector_module._discover_pytest_plugin_sources(
        test_file,
        root_path=root_path,
    )
    plugin_specs = {plugin_source.module_spec for plugin_source in plugin_sources}
    assert plugin_specs == {'plugins.a', 'plugins.b'}

    module = ast.parse(test_file.read_text(encoding='utf-8'))
    literal_bindings, expression_bindings = (
        pytest_collector_module._discover_static_binding_context(
            module,
            source_path=test_file,
            root_path=root_path,
            configured_definition_paths=(),
        )
    )
    assert (
        pytest_collector_module._discover_pytest_plugins_runtime_reason(
            module,
            source_path=test_file,
            root_path=root_path,
            configured_definition_paths=(),
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )
        == 'external pytest plugin requires pytest runtime adapter: external.plugin'
    )

    dynamic_module = ast.parse(dynamic_file.read_text(encoding='utf-8'))
    dynamic_literal_bindings, dynamic_expression_bindings = (
        pytest_collector_module._discover_static_binding_context(
            dynamic_module,
            source_path=dynamic_file,
            root_path=root_path,
            configured_definition_paths=(),
        )
    )
    assert (
        pytest_collector_module._discover_pytest_plugins_runtime_reason(
            dynamic_module,
            source_path=dynamic_file,
            root_path=root_path,
            configured_definition_paths=(),
            literal_bindings=dynamic_literal_bindings,
            expression_bindings=dynamic_expression_bindings,
        )
        == 'dynamic pytest_plugins require pytest runtime adapter'
    )

    references = pytest_collector_module._discover_external_pytest_plugin_references(
        (test_file, dynamic_file),
        root_path=root_path,
        configured_definition_paths=(),
    )
    assert [reference.module_spec for reference in references] == [
        'external.plugin',
    ]


def test_configured_definition_source_paths_and_plugin_sequence_resolution(
    tmp_path: Path,
) -> None:
    definitions_dir = tmp_path / 'defs'
    definitions_dir.mkdir(parents=True)
    nested_path = definitions_dir / 'nested'
    nested_path.mkdir()
    file_path = definitions_dir / 'alpha.py'
    file_path.write_text('', encoding='utf-8')
    nested_file = nested_path / 'beta.py'
    nested_file.write_text('', encoding='utf-8')
    not_python = definitions_dir / 'notes.txt'
    not_python.write_text('', encoding='utf-8')

    discovered_paths = pytest_collector_module._discover_configured_definition_source_paths(
        (file_path, definitions_dir, tmp_path / 'missing'),
    )
    assert file_path.resolve() in discovered_paths
    assert nested_file.resolve() in discovered_paths
    assert all(path.suffix == '.py' for path in discovered_paths)

    expression_bindings = {'plugins_ref': _parse_expression("('a', 'b')")}
    assert pytest_collector_module._resolve_pytest_plugin_spec_sequence(
        _parse_expression('plugins_ref + ("c",)'),
        literal_bindings={},
        expression_bindings=expression_bindings,
    ) == ('a', 'b', 'c')
    assert (
        pytest_collector_module._resolve_pytest_plugin_spec_sequence(
            _parse_expression('plugins_ref + runtime_plugins'),
            literal_bindings={},
            expression_bindings=expression_bindings,
        )
        is None
    )
    assert pytest_collector_module._resolve_pytest_plugin_spec_sequence(
        _parse_expression("{'a', 'b'}"),
        literal_bindings={},
        expression_bindings={},
    ) in {('a', 'b'), ('b', 'a')}
    assert (
        pytest_collector_module._resolve_pytest_plugin_spec_sequence(
            _parse_expression("('a', 1)"),
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )


def test_parametrize_spec_failure_paths() -> None:
    non_call_decorator = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.parametrize',
                'def test_case():',
                '    pass',
            ),
        ),
    )
    result = pytest_collector_module._parse_parametrize_specs(
        (non_call_decorator,),
        ('pytest',),
        literal_bindings={},
        expression_bindings={},
    )
    assert result.issue_code == 'pytest_unsupported_parametrize'

    repeated_parameters = _first_function(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1])",
                "@pytest.mark.parametrize('value', [2])",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    repeated_result = pytest_collector_module._parse_parametrize_specs(
        tuple(repeated_parameters.decorator_list),
        ('pytest',),
        literal_bindings={},
        expression_bindings={},
    )
    assert repeated_result.issue_code == 'pytest_unsupported_parametrize'
    parse_spec_issue = pytest_collector_module._parse_parametrize_specs(
        (
            _first_decorator(
                '\n'.join(
                    (
                        "@pytest.mark.parametrize('value')",
                        'def test_case(value):',
                        '    pass',
                    ),
                ),
            ),
        ),
        ('pytest',),
        literal_bindings={},
        expression_bindings={},
    )
    assert parse_spec_issue.issue_code == 'pytest_unsupported_parametrize'

    too_few_args = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value')",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(too_few_args, ast.Call)
    context = pytest_collector_module._PytestParametrizeContext(
        marker_aliases={'pytest'},
        literal_bindings={},
        expression_bindings={},
    )
    assert (
        pytest_collector_module._parse_parametrize_spec(
            too_few_args,
            parametrize_context=context,
        ).issue_code
        == 'pytest_unsupported_parametrize'
    )

    invalid_names = _first_decorator(
        '\n'.join(
            (
                '@pytest.mark.parametrize(runtime_names, [1])',
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(invalid_names, ast.Call)
    assert (
        pytest_collector_module._parse_parametrize_spec(
            invalid_names,
            parametrize_context=context,
        ).issue_code
        == 'pytest_unsupported_parametrize'
    )

    invalid_keyword = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1], scope='module')",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(invalid_keyword, ast.Call)
    assert (
        pytest_collector_module._parse_parametrize_spec(
            invalid_keyword,
            parametrize_context=context,
        ).issue_code
        == 'pytest_unsupported_parametrize'
    )

    invalid_metadata = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1], ids=runtime_ids)",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(invalid_metadata, ast.Call)
    assert (
        pytest_collector_module._parse_parametrize_spec(
            invalid_metadata,
            parametrize_context=context,
        ).issue_code
        == 'pytest_unsupported_parametrize'
    )

    invalid_rows = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', runtime_rows)",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(invalid_rows, ast.Call)
    assert (
        pytest_collector_module._parse_parametrize_spec(
            invalid_rows,
            parametrize_context=context,
        ).issue_code
        == 'pytest_unsupported_parametrize'
    )
    assert (
        pytest_collector_module._validate_parametrize_keywords(invalid_keyword)
        is not None
    )


def test_literal_binding_and_row_resolution_helpers() -> None:
    assert pytest_collector_module._safe_literal_eval(_parse_expression('[1]')) == [1]
    assert (
        pytest_collector_module._safe_literal_eval(_parse_expression('runtime_value'))
        is None
    )

    module = ast.parse(
        '\n'.join(
            (
                'a = 1',
                'b, c = 2, 3',
                'd: int = 4',
                'e: int',
            ),
        ),
    )
    literal_bindings = pytest_collector_module._discover_literal_bindings(module)
    assert literal_bindings['a'] == 1
    assert literal_bindings['d'] == 4
    assert 'e' not in literal_bindings
    expression_bindings = pytest_collector_module._discover_expression_bindings(module)
    assert set(expression_bindings) == {'a', 'd'}

    assign_multi = ast.parse('a = b = 1').body[0]
    assert isinstance(assign_multi, ast.Assign)
    assert pytest_collector_module._extract_literal_binding_target(assign_multi) is None
    assign_attr = ast.parse('obj.value = 1').body[0]
    assert isinstance(assign_attr, ast.Assign)
    assert pytest_collector_module._extract_literal_binding_target(assign_attr) is None
    expr_stmt = ast.parse('print(1)').body[0]
    assert (
        pytest_collector_module._extract_literal_binding_value(expr_stmt) is None
    )

    resolved_rows = pytest_collector_module._resolve_parametrize_rows_node(
        _parse_expression('rows'),
        literal_bindings={'rows': [(1,), (2,)]},
        expression_bindings={},
    )
    assert isinstance(resolved_rows, ast.Tuple | ast.List)
    assert (
        pytest_collector_module._resolve_parametrize_rows_node(
            _parse_expression('rows'),
            literal_bindings={'rows': 'not-rows'},
            expression_bindings={},
        )
        is pytest_collector_module.PARSE_FAILURE
    )


def test_alias_fixture_and_support_reason_helpers() -> None:
    module = ast.parse(
        '\n'.join(
            (
                'import pytest as pt',
                'from pytest import mark as mk',
                'from pytest import fixture as fx',
            ),
        ),
    )
    assert {'pytest', 'pt', 'mk'}.issubset(
        pytest_collector_module._discover_marker_aliases(module),
    )
    assert {'fixture', 'pytest', 'pt', 'fx'}.issubset(
        pytest_collector_module._discover_fixture_aliases(module),
    )

    fixture_function = _first_function(
        '\n'.join(
            (
                '@fixture',
                'def fixture_a():',
                '    pass',
            ),
        ),
    )
    assert pytest_collector_module._is_fixture_definition(
        fixture_function,
        {'fixture', 'pytest'},
    )
    assert not pytest_collector_module._is_fixture_definition(
        _first_function('def fixture_b():\n    pass\n'),
        {'fixture'},
    )
    assert not pytest_collector_module._is_fixture_definition(
        _first_function(
            '\n'.join(
                (
                    '@runtime_fixture()',
                    'def fixture_c():',
                    '    pass',
                ),
            ),
        ),
        {'fixture'},
    )
    assert not pytest_collector_module._is_special_marker_decorator(
        _parse_expression('value'),
        {'pytest'},
        marker_name='requires',
    )
    assert not pytest_collector_module._is_special_marker_decorator(
        _parse_expression('pytest.mark.other'),
        {'pytest'},
        marker_name='requires',
    )
    assert not pytest_collector_module._is_special_marker_decorator(
        _parse_expression('pytest.requires'),
        {'pytest'},
        marker_name='requires',
    )
    assert not pytest_collector_module._is_special_marker_decorator(
        _parse_expression('pytest.other.requires'),
        {'pytest'},
        marker_name='requires',
    )
    assert pytest_collector_module._is_special_marker_decorator(
        _parse_expression('pytest.mark.requires'),
        {'pytest'},
        marker_name='requires',
    )

    fixtures = {
        'fixture_a': pytest_collector_module.PytestFixtureDefinition(
            function_name='fixture_a',
            line=1,
            fixture_names=('fixture_b',),
        ),
        'fixture_b': pytest_collector_module.PytestFixtureDefinition(
            function_name='fixture_b',
            line=2,
            fixture_names=('fixture_a',),
        ),
    }
    support_cache: dict[str, str | None] = {}
    assert (
        pytest_collector_module._get_fixture_support_reason(
            'request',
            fixtures,
            support_cache=support_cache,
        )
        is None
    )
    assert (
        pytest_collector_module._get_fixture_support_reason(
            'resource_fixture',
            fixtures,
            support_cache=support_cache,
            resource_fixture_names=('resource_fixture',),
        )
        is None
    )
    assert (
        pytest_collector_module._get_fixture_support_reason(
            'missing_fixture',
            fixtures,
            support_cache=support_cache,
        )
        == 'missing_dependency'
    )
    assert (
        pytest_collector_module._get_fixture_support_reason(
            'fixture_a',
            fixtures,
            support_cache=support_cache,
        )
        == 'cyclic_dependency'
    )
    assert support_cache['fixture_a'] == 'cyclic_dependency'
    assert not pytest_collector_module._is_supported_fixture_definition(
        'fixture_a',
        fixtures['fixture_a'],
        fixtures,
    )
    assert pytest_collector_module._supports_pytest_callable_signature(
        ast.parse('def test_case(arg):\n    pass\n').body[0],
        class_name=None,
        fixtures={},
    )
    invalid_signature = ast.parse('def test_case(*, arg):\n    pass\n').body[0]
    assert isinstance(invalid_signature, ast.FunctionDef)
    assert not pytest_collector_module._supports_pytest_callable_signature(
        invalid_signature,
        class_name=None,
        fixtures={},
    )


def test_fixture_discovery_metadata_and_visibility_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / 'source.py'
    source_path.write_text('def fixture_local():\n    return 1\n', encoding='utf-8')
    original_discover_fixture_definitions_from_imported_sources = (
        pytest_collector_module._discover_fixture_definitions_from_imported_sources
    )
    imported_fixture = pytest_collector_module.PytestFixtureDefinition(
        function_name='fixture_imported',
        line=1,
        source_path=str(source_path),
    )
    discovered_fixture = pytest_collector_module.PytestFixtureDefinition(
        function_name='fixture_local',
        line=2,
        source_path=str(source_path),
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_imported_fixture_bindings',
        lambda *_args, **_kwargs: {'fixture_imported': imported_fixture},
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_definitions_from_imported_sources',
        lambda *_args, **_kwargs: {'fixture_from_import': imported_fixture},
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_aliases',
        lambda _module: {'fixture'},
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_definitions',
        lambda *_args, **_kwargs: {'fixture_local': discovered_fixture},
    )
    definitions = pytest_collector_module._discover_fixture_definitions_for_source_path(
        source_path,
        root_path=tmp_path,
    )
    assert set(definitions) == {
        'fixture_imported',
        'fixture_from_import',
        'fixture_local',
    }
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_definitions_from_imported_sources',
        original_discover_fixture_definitions_from_imported_sources,
    )

    imported_source = tmp_path / 'imported.py'
    imported_source.write_text('def fixture_imported():\n    return 1\n', encoding='utf-8')
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_aliases',
        lambda _module: {'fixture'},
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_definitions',
        lambda *_args, **_kwargs: {'fixture_imported': imported_fixture},
    )
    imported_definitions = (
        pytest_collector_module._discover_fixture_definitions_from_imported_sources(
            (
                pytest_collector_module.PytestFixtureDefinition(
                    function_name='fixture_imported',
                    line=1,
                    source_path=str(imported_source),
                ),
            ),
        )
    )
    assert 'fixture_imported' in imported_definitions

    configured_definition = tmp_path / 'configured.py'
    configured_definition.write_text('', encoding='utf-8')
    plugin_path = tmp_path / 'plugin.py'
    plugin_path.write_text('', encoding='utf-8')
    metadata = pytest_collector_module._build_fixture_source_metadata(
        configured_definition,
        root_path=tmp_path,
        configured_definition_paths=(configured_definition,),
        visible_test_paths=(),
    )
    assert metadata.category == 'configured_definition_fixture'

    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_pytest_plugin_sources',
        lambda *_args, **_kwargs: (
            pytest_collector_module.PytestPluginSource(
                module_spec='plugins.demo',
                source_path=plugin_path,
            ),
        ),
    )
    plugin_metadata = pytest_collector_module._build_fixture_source_metadata(
        plugin_path,
        root_path=tmp_path,
        configured_definition_paths=(),
        visible_test_paths=(tmp_path / 'tests' / 'test_demo.py',),
    )
    assert plugin_metadata.category == 'pytest_plugin_fixture'

    test_file = tmp_path / 'tests' / 'test_demo.py'
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text('def test_case():\n    pass\n', encoding='utf-8')
    imported_visible = tmp_path / 'imported_visible.py'
    imported_visible.write_text('', encoding='utf-8')
    plugin_visible = tmp_path / 'plugin_visible.py'
    plugin_visible.write_text('', encoding='utf-8')
    missing_visible = tmp_path / 'missing_visible.py'
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_imported_fixture_source_paths',
        lambda *_args, **_kwargs: (imported_visible, missing_visible),
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_pytest_plugin_sources',
        lambda *_args, **_kwargs: (
            pytest_collector_module.PytestPluginSource(
                module_spec='plugins.visible',
                source_path=plugin_visible,
            ),
        ),
    )
    visible_sources = pytest_collector_module._discover_visible_fixture_source_paths(
        test_file,
        root_path=tmp_path,
    )
    assert test_file.resolve() in visible_sources
    assert imported_visible.resolve() in visible_sources
    assert plugin_visible.resolve() in visible_sources


def test_fixture_knowledge_discovery_and_collector_event_emission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_definition = tmp_path / 'configured.py'
    configured_definition.write_text('', encoding='utf-8')
    visible_source = tmp_path / 'visible.py'
    visible_source.write_text('', encoding='utf-8')
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_visible_fixture_source_paths',
        lambda *_args, **_kwargs: (visible_source,),
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_build_fixture_source_metadata',
        lambda *_args, **_kwargs: pytest_collector_module.PytestFixtureSourceMetadata(),
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_knowledge_records',
        lambda *_args, **_kwargs: (),
    )
    records = pytest_collector_module._discover_fixture_knowledge_by_source(
        (),
        root_path=tmp_path,
        configured_definition_paths=(configured_definition,),
    )
    assert records == {}

    test_file = tmp_path / 'tests' / 'test_case.py'
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text('def test_case():\n    pass\n', encoding='utf-8')
    failed_file = tmp_path / 'failed_test.py'
    definition = pytest_collector_module.PytestTestDefinition(
        function_name='test_case',
        line=1,
    )
    test_item = pytest_collector_module.PytestTestItem(
        test_file,
        definition,
        tmp_path,
    )

    class _EventStream:
        def __init__(self) -> None:
            self.events: list[object] = []

        async def emit(self, event: object) -> None:
            self.events.append(event)

    event_stream = _EventStream()
    fixture_record = pytest_collector_module.PytestFixtureKnowledgeRecord(
        source_path=str(test_file),
        function_name='fixture_a',
        line=1,
    )
    external_reference = pytest_collector_module.PytestExternalPluginReference(
        module_spec='external.plugin',
        runtime_reason='external plugin',
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_knowledge_by_source',
        lambda *_args, **_kwargs: {test_file.resolve(): (fixture_record,)},
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_external_pytest_plugin_references',
        lambda *_args, **_kwargs: (external_reference,),
    )

    collector_like = SimpleNamespace(
        _domain_event_stream=event_stream,
        collected_tests=[SimpleNamespace(path=None), test_item],
        config=SimpleNamespace(root_path=tmp_path),
        _engine_name='pytest',
        _configured_definition_paths=(),
        failed_files={failed_file},
    )
    asyncio.run(pytest_collector_module.PytestCollector._emit_knowledge_events(collector_like))
    assert len(event_stream.events) >= 5

    loader_like = SimpleNamespace(
        config=SimpleNamespace(root_path=tmp_path),
        _configured_definition_paths=(),
        resource_fixture_names=(),
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_pytest_tests',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError('boom')),
    )
    assert (
        asyncio.run(
            pytest_collector_module.PytestCollector.load_tests_from_file(
                loader_like,
                test_file,
            ),
        )
        is None
    )


def test_statement_definition_and_test_definition_guard_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_build_test_definitions = pytest_collector_module._build_test_definitions
    helper_class = ast.parse('class Helper:\n    pass\n').body[0]
    assert isinstance(helper_class, ast.ClassDef)
    assert pytest_collector_module._discover_statement_definitions(
        helper_class,
        ('pytest',),
        {},
        build_context=pytest_collector_module.PytestDefinitionBuildContext(),
    ) == ()

    test_class = ast.parse(
        '\n'.join(
            (
                'class TestSuite:',
                '    value = 1',
                '    def test_case(self):',
                '        pass',
            ),
        ),
    ).body[0]
    assert isinstance(test_class, ast.ClassDef)
    monkeypatch.setattr(
        pytest_collector_module,
        '_build_test_definitions',
        lambda node, *_args, **_kwargs: (
            pytest_collector_module.PytestTestDefinition(
                function_name=node.name,
                line=node.lineno,
            ),
        ),
    )
    definitions = pytest_collector_module._discover_statement_definitions(
        test_class,
        ('pytest',),
        {},
        build_context=pytest_collector_module.PytestDefinitionBuildContext(),
    )
    assert len(definitions) == 1
    monkeypatch.setattr(
        pytest_collector_module,
        '_build_test_definitions',
        original_build_test_definitions,
    )

    test_node = ast.parse('def test_case():\n    pass\n').body[0]
    assert isinstance(test_node, ast.FunctionDef)
    monkeypatch.setattr(
        pytest_collector_module,
        '_parse_parametrize_specs',
        lambda *_args, **_kwargs: pytest_collector_module.PytestParametrizeParseResult(
            issue_code='pytest_unsupported_parametrize',
        ),
    )
    assert (
        pytest_collector_module._build_test_definitions(
            test_node,
            ('pytest',),
            {},
            build_context=pytest_collector_module.PytestDefinitionBuildContext(),
        )
        == ()
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_parse_parametrize_specs',
        lambda *_args, **_kwargs: pytest_collector_module.PytestParametrizeParseResult(
            specs=(),
        ),
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_supports_pytest_callable_signature',
        lambda *_args, **_kwargs: False,
    )
    assert (
        pytest_collector_module._build_test_definitions(
            test_node,
            ('pytest',),
            {},
            build_context=pytest_collector_module.PytestDefinitionBuildContext(),
        )
        == ()
    )


def test_conftest_and_nonlocal_fixture_discovery_branch_coverage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_discover_pytest_plugin_fixture_definitions = (
        pytest_collector_module._discover_pytest_plugin_fixture_definitions
    )
    source_path = tmp_path / 'tests' / 'test_demo.py'
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text('def test_case():\n    pass\n', encoding='utf-8')
    configured_path = tmp_path / 'defs.py'
    configured_path.write_text('', encoding='utf-8')
    conftest_path = tmp_path / 'conftest.py'
    conftest_path.write_text('', encoding='utf-8')

    assert pytest_collector_module._resolve_conftest_stop_path(
        source_path,
        root_path=None,
    ) == source_path.parent
    assert pytest_collector_module._resolve_conftest_stop_path(
        source_path,
        root_path=tmp_path / 'elsewhere',
    ) == source_path.parent

    fixture_definition = pytest_collector_module.PytestFixtureDefinition(
        function_name='fixture_a',
        line=1,
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_pytest_plugin_fixture_definitions',
        lambda *_args, **_kwargs: {'plugin_fixture': fixture_definition},
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_definitions_for_source_path',
        lambda path, **_kwargs: {
            f'fixture_from_{Path(path).stem}': fixture_definition,
        },
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_conftest_paths',
        lambda *_args, **_kwargs: (conftest_path,),
    )
    fixtures = pytest_collector_module._discover_nonlocal_fixture_definitions(
        source_path,
        root_path=tmp_path,
        configured_definition_paths=(configured_path,),
    )
    assert set(fixtures) == {
        'plugin_fixture',
        'fixture_from_defs',
        'fixture_from_conftest',
    }

    source_fixture_map = pytest_collector_module._discover_fixture_definitions_for_source_paths(
        (configured_path, conftest_path),
        root_path=tmp_path,
    )
    assert 'fixture_from_defs' in source_fixture_map
    assert 'fixture_from_conftest' in source_fixture_map
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_pytest_plugin_fixture_definitions',
        original_discover_pytest_plugin_fixture_definitions,
    )

    plugin_source = pytest_collector_module.PytestPluginSource(
        module_spec='plugins.demo',
        source_path=configured_path,
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_pytest_plugin_sources',
        lambda *_args, **_kwargs: (plugin_source,),
    )
    plugin_fixtures = pytest_collector_module._discover_pytest_plugin_fixture_definitions(
        source_path,
        root_path=tmp_path,
    )
    assert 'fixture_from_defs' in plugin_fixtures


def test_parametrize_row_and_metadata_failure_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    decorator = _first_decorator(
        '\n'.join(
            (
                "@pytest.mark.parametrize('value', [1], indirect=runtime_indirect)",
                'def test_case(value):',
                '    pass',
            ),
        ),
    )
    assert isinstance(decorator, ast.Call)
    case_ids, indirect_names, issue = pytest_collector_module._parse_parametrize_metadata(
        decorator,
        arg_names=('value',),
        literal_bindings={},
    )
    assert case_ids is None
    assert indirect_names == ()
    assert issue is not None

    assert pytest_collector_module._parse_parametrize_ids(
        _first_decorator(
            '\n'.join(
                (
                    "@pytest.mark.parametrize('value', [1], ids=[None, 'ok'])",
                    'def test_case(value):',
                    '    pass',
                ),
            ),
        ),
        literal_bindings={},
    ) == (None, 'ok')
    assert (
        pytest_collector_module._parse_parametrize_indirect(
            _first_decorator(
                '\n'.join(
                    (
                        "@pytest.mark.parametrize('value', [1], indirect='bad')",
                        'def test_case(value):',
                        '    pass',
                    ),
                ),
            ),
            arg_names=('value',),
            literal_bindings={},
        )
        is False
    )

    context = pytest_collector_module._PytestParametrizeContext(
        marker_aliases={'pytest'},
        literal_bindings={},
        expression_bindings={},
    )
    assert (
        pytest_collector_module._parse_parametrize_cases(
            _parse_expression('runtime_rows'),
            arg_names=('value',),
            case_ids=None,
            parametrize_context=context,
        )
        is None
    )
    assert (
        pytest_collector_module._parse_parametrize_cases(
            _parse_expression('[(1, 2)]'),
            arg_names=('value',),
            case_ids=None,
            parametrize_context=context,
        )
        is None
    )

    monkeypatch.setattr(
        pytest_collector_module,
        '_parse_pytest_param_call',
        lambda *_args, **_kwargs: pytest_collector_module.PARSE_FAILURE,
    )
    assert (
        pytest_collector_module._parse_parametrize_row(
            _parse_expression('pytest.param(1)'),
            width=1,
            marker_aliases={'pytest'},
            default_case_id=None,
            parametrize_context=context,
        )
        is None
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_parse_pytest_param_call',
        lambda *_args, **_kwargs: None,
    )
    assert (
        pytest_collector_module._parse_parametrize_row(
            _parse_expression('runtime_value'),
            width=1,
            marker_aliases={'pytest'},
            default_case_id=None,
            parametrize_context=context,
        )
        is None
    )
    assert (
        pytest_collector_module._parse_parametrize_row(
            _parse_expression('(1, 2)'),
            width=1,
            marker_aliases={'pytest'},
            default_case_id=None,
            parametrize_context=context,
        )
        is None
    )


def test_pytest_param_metadata_and_mark_failure_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_extract_marks = pytest_collector_module._extract_pytest_param_mark_nodes
    assert (
        pytest_collector_module._parse_pytest_param_metadata(
            _parse_expression('pytest.param(1, unknown=2)'),
            marker_aliases={'pytest'},
            literal_bindings={},
            expression_bindings={},
        )
        is pytest_collector_module.PARSE_FAILURE
    )
    assert (
        pytest_collector_module._parse_pytest_param_metadata(
            _parse_expression('pytest.param(1, marks=runtime_marks)'),
            marker_aliases={'pytest'},
            literal_bindings={},
            expression_bindings={},
        )
        is pytest_collector_module.PARSE_FAILURE
    )

    monkeypatch.setattr(
        pytest_collector_module,
        '_extract_pytest_param_mark_nodes',
        lambda *_args, **_kwargs: None,
    )
    assert (
        pytest_collector_module._parse_pytest_param_marks(
            _parse_expression('runtime_marks'),
            marker_aliases={'pytest'},
            literal_bindings={},
            expression_bindings={},
        )
        is pytest_collector_module.PARSE_FAILURE
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_extract_pytest_param_mark_nodes',
        lambda *_args, **_kwargs: (_parse_expression('other.mark.label'),),
    )
    assert (
        pytest_collector_module._parse_pytest_param_marks(
            _parse_expression('runtime_marks'),
            marker_aliases={'pytest'},
            literal_bindings={},
            expression_bindings={},
        )
        is pytest_collector_module.PARSE_FAILURE
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_extract_pytest_param_mark_nodes',
        lambda *_args, **_kwargs: (_parse_expression('pytest.mark.usefixtures(name="db")'),),
    )
    assert (
        pytest_collector_module._parse_pytest_param_marks(
            _parse_expression('runtime_marks'),
            marker_aliases={'pytest'},
            literal_bindings={},
            expression_bindings={},
        )
        is pytest_collector_module.PARSE_FAILURE
    )

    def _patched_extract_marks(marks_node, **kwargs):
        if isinstance(marks_node, ast.Constant):
            return None
        return original_extract_marks(marks_node, **kwargs)

    monkeypatch.setattr(
        pytest_collector_module,
        '_extract_pytest_param_mark_nodes',
        original_extract_marks,
    )
    monkeypatch.setattr(
        pytest_collector_module,
        '_extract_pytest_param_mark_nodes',
        _patched_extract_marks,
    )
    assert (
        pytest_collector_module._extract_pytest_param_mark_nodes(
            _parse_expression('[pytest.mark.api, 1]'),
            expression_bindings={},
        )
        is None
    )
    assert (
        pytest_collector_module._extract_pytest_param_mark_nodes(
            _parse_expression('1 + pytest.mark.api'),
            expression_bindings={},
        )
        is None
    )
    assert (
        pytest_collector_module._extract_pytest_param_mark_nodes(
            _parse_expression('pytest.mark.api + 1'),
            expression_bindings={},
        )
        is None
    )


def test_misc_runtime_and_render_helpers(tmp_path: Path) -> None:
    assert (
        pytest_collector_module._normalize_parametrize_row_values((1,), width=2)
        is None
    )
    assert not pytest_collector_module._is_pytest_param_call(
        ast.Call(func=ast.Name(id='param', ctx=ast.Load()), args=[], keywords=[]),
        {'pytest'},
    )

    assert pytest_collector_module._build_parametrized_case_id(
        pytest_collector_module.PytestParametrizeCase(values=(), case_id=None),
        7,
    ) == '7'
    assert pytest_collector_module._render_parameter_value('value') == 'value'
    assert (
        pytest_collector_module._resolve_literal_reference_with_sentinel(
            _parse_expression("{'k': 1}"),
            literal_bindings={},
        )
        == {'k': 1}
    )

    module = ast.parse('def other():\n    pass\n')
    assert (
        pytest_collector_module._extract_marker_name(
            _parse_expression('value'),
            {'pytest'},
        )
        is None
    )
    assert (
        pytest_collector_module._extract_marker_name(
            _parse_expression('pytest.mark'),
            {'pytest'},
        )
        is None
    )
    assert (
        pytest_collector_module._discover_function_documentation(
            module,
            'missing',
        )
        is None
    )
    assert (
        pytest_collector_module._is_fixture_definition(
            _first_function(
                '\n'.join(
                    (
                        '@pkg.pytest.fixture',
                        'def fixture_a():',
                        '    pass',
                    ),
                ),
            ),
            {'pytest'},
        )
        is False
    )
    assert (
        pytest_collector_module._is_fixture_definition(
            _first_function(
                '\n'.join(
                    (
                        '@other.fixture',
                        'def fixture_b():',
                        '    pass',
                    ),
                ),
            ),
            {'pytest'},
        )
        is False
    )
    assert not pytest_collector_module._is_supported_fixture_reference(
        'missing_fixture',
        {},
    )
    assert (
        pytest_collector_module._get_fixture_support_reason(
            'cached_fixture',
            {},
            support_cache={'cached_fixture': None},
        )
        is None
    )
    assert (
        pytest_collector_module._get_pytest_runtime_reason(
            ('missing_fixture',),
            {},
        )
        == 'fixture requires pytest runtime: missing_fixture'
    )
    assert (
        pytest_collector_module._merge_pytest_runtime_reasons(
            None,
            'runtime reason',
            'other',
        )
        == 'runtime reason'
    )

    root = tmp_path / 'root'
    root.mkdir(parents=True)
    source_file = root / 'tests' / 'test_demo.py'
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_text('pass\n', encoding='utf-8')
    package_dir = root / 'pkgdir'
    package_dir.mkdir()
    init_file = package_dir / '__init__.py'
    init_file.write_text('', encoding='utf-8')
    relative_pkg = source_file.parent.parent / 'pkg'
    relative_pkg.mkdir(exist_ok=True)
    relative_pkg_file = relative_pkg / '__init__.py'
    relative_pkg_file.write_text('', encoding='utf-8')
    assert pytest_collector_module._resolve_import_module_candidate(
        root,
        ('pkgdir',),
    ) == init_file.resolve()
    relative_statement = ast.parse('from ..pkg import value').body[0]
    assert isinstance(relative_statement, ast.ImportFrom)
    assert pytest_collector_module._resolve_imported_source_path(
        relative_statement,
        source_path=source_file,
        root_path=root,
        configured_definition_paths=(),
    ) == relative_pkg_file.resolve()
    assert pytest_collector_module._discover_pytest_plugin_source_paths(
        source_file,
        root_path=root,
    ) == ()
    assert (
        pytest_collector_module._resolve_pytest_plugin_spec_sequence(
            _parse_expression("'plugins.local'"),
            literal_bindings={},
            expression_bindings={},
        )
        == ('plugins.local',)
    )

    local_plugin = root / 'plugins' / 'local.py'
    local_plugin.parent.mkdir(parents=True)
    local_plugin.write_text('', encoding='utf-8')
    module_with_local_plugin = ast.parse("pytest_plugins = ('plugins.local',)\n")
    literal_bindings, expression_bindings = (
        pytest_collector_module._discover_static_binding_context(
            module_with_local_plugin,
            source_path=source_file,
            root_path=root,
            configured_definition_paths=(),
        )
    )
    assert (
        pytest_collector_module._discover_pytest_plugins_runtime_reason(
            module_with_local_plugin,
            source_path=source_file,
            root_path=root,
            configured_definition_paths=(),
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )
        is None
    )
