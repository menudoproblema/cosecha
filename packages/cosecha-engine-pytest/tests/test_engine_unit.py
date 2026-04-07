from __future__ import annotations

import ast
import asyncio

from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.capabilities import DraftValidationIssue
from cosecha.core.items import TestResultStatus
from cosecha.core.manifest_types import ResourceBindingSpec
from cosecha.engine.pytest import PytestEngine, PytestTestDefinition, PytestTestItem
from cosecha.engine.pytest import engine as engine_module
from cosecha_internal.testkit import DummyReporter, build_config


def _build_validation_context():
    return engine_module._PytestDraftValidationContext(
        marker_aliases={'pytest'},
        literal_bindings={},
        expression_bindings={},
    )


def test_engine_initialize_applies_definition_overrides_and_fixture_names(
    tmp_path: Path,
) -> None:
    config = build_config(tmp_path)
    config.definition_paths = (tmp_path / 'existing_defs.py',)
    engine = PytestEngine(
        'pytest',
        reporter=DummyReporter(),
        definition_paths=(tmp_path / 'extra_defs.py',),
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='workspace',
                fixture_name='cosecha_workspace',
            ),
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='db',
                fixture_name=None,
            ),
        ),
    )

    engine.initialize(config, '')

    assert set(config.definition_paths) == {
        tmp_path / 'existing_defs.py',
        tmp_path / 'extra_defs.py',
    }
    assert engine.collector.resource_fixture_names == ('cosecha_workspace',)


def test_engine_generate_new_context_keeps_resource_bindings(tmp_path: Path) -> None:
    binding = ResourceBindingSpec(
        engine_type='pytest',
        resource_name='workspace',
        fixture_name='cosecha_workspace',
    )
    engine = PytestEngine(
        'pytest',
        reporter=DummyReporter(),
        resource_bindings=(binding,),
    )
    engine.initialize(build_config(tmp_path), '')

    context = asyncio.run(engine.generate_new_context(test=None))

    assert context.resource_bindings == (binding,)


def test_engine_start_test_handles_issue_paths(tmp_path: Path) -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    test_path = tmp_path / 'test_demo.py'
    test_path.write_text('def test_case():\n    pass\n', encoding='utf-8')

    xfail_item = PytestTestItem(
        test_path,
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            xfail_issue='unsupported xfail condition',
        ),
        tmp_path,
    )
    with pytest.raises(RuntimeError, match='unsupported xfail condition'):
        asyncio.run(engine.start_test(xfail_item))
    assert xfail_item.status == TestResultStatus.ERROR
    assert xfail_item.failure_kind == 'collection'
    assert xfail_item.error_code == 'pytest_case_xfail_issue'

    skip_item = PytestTestItem(
        test_path,
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            skip_reason='skip this case',
        ),
        tmp_path,
    )
    asyncio.run(engine.start_test(skip_item))
    assert skip_item.status == TestResultStatus.SKIPPED
    assert skip_item.message == 'skip this case'

    xfail_no_run_item = PytestTestItem(
        test_path,
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            xfail_reason='known bug',
            xfail_run=False,
        ),
        tmp_path,
    )
    asyncio.run(engine.start_test(xfail_no_run_item))
    assert xfail_no_run_item.status == TestResultStatus.SKIPPED
    assert xfail_no_run_item.message == 'Expected failure (not run): known bug'


def test_engine_start_test_ignores_non_pytest_items() -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    asyncio.run(engine.start_test(SimpleNamespace(name='non-pytest')))


def test_engine_session_methods_reset_runtime_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def _track_reset(**kwargs) -> None:
        calls.append(kwargs)

    async def _raise_finish(_self) -> None:
        msg = 'finish boom'
        raise RuntimeError(msg)

    monkeypatch.setattr(
        engine_module,
        'reset_pytest_runtime_batch_cache',
        _track_reset,
    )
    monkeypatch.setattr(engine_module.Engine, 'finish_session', _raise_finish)

    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')

    asyncio.run(engine.start_session())
    with pytest.raises(RuntimeError, match='finish boom'):
        asyncio.run(engine.finish_session())

    assert calls[0] == {
        'root_path': tmp_path,
        'clear_registrations': False,
    }
    assert calls[1] == {'root_path': tmp_path}


def test_engine_resolve_definition_delegates_to_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def _fake_to_thread(fn, *args, **kwargs):
        captured['fn'] = fn
        captured['args'] = args
        captured['kwargs'] = kwargs
        return fn(*args, **kwargs)

    def _fake_resolver(context, *, step_type: str, step_text: str):
        return (f'{context.engine_name}:{step_type}:{step_text}',)

    monkeypatch.setattr(engine_module.asyncio, 'to_thread', _fake_to_thread)
    monkeypatch.setattr(
        engine_module,
        '_resolve_pytest_definitions',
        _fake_resolver,
    )

    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')

    result = asyncio.run(
        engine.resolve_definition(
            test_path=tmp_path / 'tests' / 'test_demo.py',
            step_type='fixture',
            step_text='workspace',
        ),
    )

    assert result == ('pytest:fixture:workspace',)
    assert captured['fn'] is _fake_resolver
    context = captured['args'][0]
    assert context.engine_name == 'pytest'
    assert context.root_path == tmp_path


def test_engine_validate_draft_reports_syntax_errors(tmp_path: Path) -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')

    result = asyncio.run(
        engine.validate_draft(
            source_content='def test_case(:\n    pass\n',
            test_path=tmp_path / 'tests' / 'test_bad.py',
        ),
    )

    assert result.test_count == 0
    assert len(result.issues) == 1
    assert result.issues[0].code == 'pytest_syntax_error'


def test_engine_validate_draft_success_path_uses_discovery_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = PytestEngine(
        'pytest',
        reporter=DummyReporter(),
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='workspace',
                fixture_name='cosecha_workspace',
            ),
        ),
    )
    engine.initialize(build_config(tmp_path), '')
    source_path = tmp_path / 'tests' / 'test_demo.py'
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text('def test_case():\n    pass\n', encoding='utf-8')

    monkeypatch.setattr(engine_module, '_discover_fixture_aliases', lambda _module: {'pytest'})
    monkeypatch.setattr(
        engine_module,
        '_discover_configured_definition_source_paths',
        lambda _definition_paths: (tmp_path / 'defs.py',),
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_static_binding_context',
        lambda *_args, **_kwargs: ({}, {}),
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_nonlocal_fixture_definitions',
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_fixture_definitions',
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        engine_module,
        'discover_pytest_tests_from_module',
        lambda *_args, **_kwargs: (SimpleNamespace(), SimpleNamespace()),
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_pytest_plugins_runtime_reason',
        lambda *_args, **_kwargs: 'pytest plugins require runtime',
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_draft_validation_issues',
        lambda *_args, **_kwargs: (
            DraftValidationIssue(
                code='demo_issue',
                message='demo message',
                severity='warning',
            ),
        ),
    )

    result = asyncio.run(
        engine.validate_draft(
            source_content='def test_case():\n    pass\n',
            test_path=source_path,
        ),
    )

    assert result.test_count == 2
    assert len(result.issues) == 1
    assert result.issues[0].code == 'demo_issue'


def test_resolve_pytest_definitions_routes_by_step_type(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = engine_module._PytestDefinitionResolutionContext(
        engine_name='pytest',
        test_path=tmp_path / 'tests' / 'test_demo.py',
        root_path=tmp_path,
    )
    monkeypatch.setattr(
        engine_module,
        '_resolve_fixture_definitions',
        lambda **_kwargs: ('fixture',),
    )
    monkeypatch.setattr(
        engine_module,
        '_resolve_external_pytest_plugin_definitions',
        lambda **_kwargs: ('plugin',),
    )
    monkeypatch.setattr(
        engine_module,
        '_resolve_test_definitions',
        lambda **_kwargs: ('test',),
    )

    assert engine_module._resolve_pytest_definitions(
        context,
        step_type='fixture',
        step_text='workspace',
    ) == ('fixture',)
    assert engine_module._resolve_pytest_definitions(
        context,
        step_type='plugin',
        step_text='pkg.plugin',
    ) == ('plugin',)
    assert engine_module._resolve_pytest_definitions(
        context,
        step_type='test',
        step_text='test_case',
    ) == ('test',)
    assert engine_module._resolve_pytest_definitions(
        context,
        step_type='unknown',
        step_text='x',
    ) == ()


def test_resolve_fixture_definitions_prefers_imported_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = engine_module._PytestDefinitionResolutionContext(
        engine_name='pytest',
        test_path=tmp_path / 'tests' / 'test_demo.py',
        root_path=tmp_path,
    )
    imported = SimpleNamespace(marker='imported')
    monkeypatch.setattr(
        engine_module,
        '_resolve_imported_fixture_binding_definition',
        lambda **_kwargs: imported,
    )

    result = engine_module._resolve_fixture_definitions(
        context=context,
        test_path=context.test_path,
        fixture_name='workspace',
    )
    assert result == (imported,)


def test_resolve_fixture_definitions_falls_back_to_manifest_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = engine_module._PytestDefinitionResolutionContext(
        engine_name='pytest',
        test_path=tmp_path / 'tests' / 'test_demo.py',
        root_path=tmp_path,
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='workspace',
                fixture_name='cosecha_workspace',
            ),
        ),
    )
    monkeypatch.setattr(
        engine_module,
        '_resolve_imported_fixture_binding_definition',
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_visible_fixture_source_paths',
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(engine_module, 'discover_cosecha_manifest', lambda: None)

    result = engine_module._resolve_fixture_definitions(
        context=context,
        test_path=context.test_path,
        fixture_name='cosecha_workspace',
    )

    assert len(result) == 1
    assert result[0].resolution_source == 'manifest_resource_binding'
    assert result[0].provider_name == 'workspace'


def test_resolve_imported_fixture_binding_definition_reads_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition_path = tmp_path / 'defs.py'
    definition_path.write_text('', encoding='utf-8')
    test_path = tmp_path / 'tests' / 'test_demo.py'
    test_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.write_text('', encoding='utf-8')
    context = engine_module._PytestDefinitionResolutionContext(
        engine_name='pytest',
        test_path=test_path,
        root_path=tmp_path,
        configured_definition_paths=(definition_path,),
    )

    monkeypatch.setattr(
        engine_module,
        '_discover_conftest_paths',
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_imported_fixture_bindings',
        lambda *_args, **_kwargs: {
            'workspace_fixture': SimpleNamespace(
                source_path=str(definition_path),
                function_name='workspace_provider',
            ),
        },
    )
    monkeypatch.setattr(
        engine_module,
        '_build_fixture_source_metadata',
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_fixture_knowledge_records',
        lambda *_args, **_kwargs: (
            SimpleNamespace(
                function_name='workspace_provider',
                line=7,
                source_category='configured_definition_fixture',
                provider_kind='definition_path',
                provider_name='defs.py',
                documentation='fixture docs',
            ),
        ),
    )

    result = engine_module._resolve_imported_fixture_binding_definition(
        test_path=test_path,
        fixture_name='workspace_fixture',
        context=context,
    )

    assert result is not None
    assert result.file_path == str(definition_path.resolve())
    assert result.line == 7
    assert result.function_name == 'workspace_fixture'


def test_resolve_external_plugin_definitions_matches_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = engine_module._PytestDefinitionResolutionContext(
        engine_name='pytest',
        test_path=tmp_path / 'tests' / 'test_demo.py',
        root_path=tmp_path,
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_external_pytest_plugin_references',
        lambda *_args, **_kwargs: (
            SimpleNamespace(
                module_spec='pkg.demo_plugin',
                runtime_reason='plugin loaded at runtime',
            ),
        ),
    )
    monkeypatch.setattr(
        engine_module,
        '_build_external_pytest_plugin_file_path',
        lambda module_spec: f'<external:{module_spec}>',
    )

    result = engine_module._resolve_external_pytest_plugin_definitions(
        context=context,
        plugin_spec='pkg.demo_plugin',
    )
    assert len(result) == 1
    assert result[0].provider_name == 'pkg.demo_plugin'
    assert result[0].runtime_required is True
    assert result[0].file_path == '<external:pkg.demo_plugin>'
    assert (
        engine_module._resolve_external_pytest_plugin_definitions(
            context=context,
            plugin_spec='pkg.other',
        )
        == ()
    )


def test_resolve_test_definitions_builds_patterns_and_docs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_path = tmp_path / 'tests' / 'test_demo.py'
    monkeypatch.setattr(
        engine_module,
        '_discover_pytest_tests',
        lambda *_args, **_kwargs: (
            PytestTestDefinition(
                function_name='test_case',
                class_name=None,
                line=10,
            ),
            PytestTestDefinition(
                function_name='test_case',
                class_name='TestSuite',
                line=20,
            ),
        ),
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_test_documentation',
        lambda *_args, **_kwargs: {
            (None, 'test_case'): 'module-level doc',
            ('TestSuite', 'test_case'): 'class doc',
        },
    )

    result = engine_module._resolve_test_definitions(
        engine_name='pytest',
        test_path=test_path,
        test_name='test_case',
        root_path=tmp_path,
    )
    assert len(result) == 2
    assert result[0].documentation == 'module-level doc'
    assert result[1].documentation == 'class doc'


def test_discover_test_documentation_and_pattern_builder(tmp_path: Path) -> None:
    test_path = tmp_path / 'tests' / 'test_docs.py'
    test_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.write_text(
        '\n'.join(
            (
                'def test_module():',
                '    """module doc"""',
                '    pass',
                '',
                'class TestSuite:',
                '    def test_method(self):',
                '        """method doc"""',
                '        pass',
            ),
        ),
        encoding='utf-8',
    )

    docs = engine_module._discover_test_documentation(test_path)
    assert docs[(None, 'test_module')] == 'module doc'
    assert docs[('TestSuite', 'test_method')] == 'method doc'

    module_definition = PytestTestDefinition(
        function_name='test_module',
        line=1,
    )
    class_definition = PytestTestDefinition(
        function_name='test_method',
        line=1,
        class_name='TestSuite',
    )
    assert engine_module._build_test_resolution_patterns(module_definition) == (
        'test_module',
    )
    assert engine_module._build_test_resolution_patterns(class_definition) == (
        'TestSuite.test_method',
        'test_method',
    )


def test_engine_issue_builders_cover_inherited_and_runtime_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = PytestEngine(
        'pytest',
        reporter=DummyReporter(),
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='workspace',
                fixture_name='cosecha_workspace',
            ),
        ),
    )
    engine.initialize(build_config(tmp_path), '')

    node = ast.parse('def test_case():\n    pass\n').body[0]
    assert isinstance(node, ast.FunctionDef)
    validation_context = _build_validation_context()

    assert (
        engine._build_pytest_plugins_runtime_issue(node, runtime_reason=None)
        is None
    )
    runtime_issue = engine._build_pytest_plugins_runtime_issue(
        node,
        runtime_reason='runtime only',
    )
    assert runtime_issue is not None
    assert runtime_issue.code == 'pytest_runtime_pytest_plugins'

    inherited_usefixtures_issue = engine._build_usefixtures_issue(
        node,
        marker_aliases={'pytest'},
        inherited_usefixtures_issue='class level issue',
    )
    assert inherited_usefixtures_issue is not None
    monkeypatch.setattr(
        engine_module,
        '_build_usefixtures_decision',
        lambda *_args, **_kwargs: SimpleNamespace(
            issue_code='pytest_runtime_usefixtures',
            issue_message='usefixtures runtime',
            issue_line=5,
        ),
    )
    decision_usefixtures_issue = engine._build_usefixtures_issue(
        node,
        marker_aliases={'pytest'},
    )
    assert decision_usefixtures_issue is not None
    assert decision_usefixtures_issue.code == 'pytest_runtime_usefixtures'

    inherited_xfail = engine._build_xfail_issue(
        node,
        marker_aliases={'pytest'},
        validation_context=validation_context,
        inherited_xfail_issue='xfail inherited',
    )
    assert inherited_xfail is not None
    runtime_xfail = engine._build_xfail_issue(
        node,
        marker_aliases={'pytest'},
        validation_context=validation_context,
        inherited_runtime_reason='xfail runtime',
    )
    assert runtime_xfail is not None
    monkeypatch.setattr(
        engine_module,
        '_build_static_xfail_decision',
        lambda *_args, **_kwargs: SimpleNamespace(
            issue_code='pytest_unsupported_xfail_condition',
            issue_message='unsupported xfail',
            issue_line=7,
            requires_pytest_runtime=False,
            runtime_reason=None,
        ),
    )
    xfail_issue = engine._build_xfail_issue(
        node,
        marker_aliases={'pytest'},
        validation_context=validation_context,
    )
    assert xfail_issue is not None
    assert xfail_issue.code == 'pytest_unsupported_xfail_condition'

    inherited_skip = engine._build_skip_issue(
        node,
        marker_aliases={'pytest'},
        validation_context=validation_context,
        inherited_skip_issue='skip inherited',
    )
    assert inherited_skip is not None
    runtime_skip = engine._build_skip_issue(
        node,
        marker_aliases={'pytest'},
        validation_context=validation_context,
        inherited_runtime_reason='skip runtime',
    )
    assert runtime_skip is not None
    monkeypatch.setattr(
        engine_module,
        '_build_static_skip_decision',
        lambda *_args, **_kwargs: SimpleNamespace(
            issue_code='pytest_unsupported_skip_condition',
            issue_message='unsupported skip',
            issue_line=8,
            requires_pytest_runtime=False,
            runtime_reason=None,
        ),
    )
    skip_issue = engine._build_skip_issue(
        node,
        marker_aliases={'pytest'},
        validation_context=validation_context,
    )
    assert skip_issue is not None
    assert skip_issue.code == 'pytest_unsupported_skip_condition'

    monkeypatch.setattr(
        engine_module,
        '_parse_parametrize_specs',
        lambda *_args, **_kwargs: SimpleNamespace(
            issue_code='pytest_bad_parametrize',
            issue_message='bad parametrize',
            issue_line=9,
            specs=(),
        ),
    )
    parametrize_issue = engine._build_parametrize_issue(
        node,
        marker_aliases={'pytest'},
        literal_bindings={},
        expression_bindings={},
    )
    assert parametrize_issue is not None
    assert parametrize_issue.code == 'pytest_bad_parametrize'

    monkeypatch.setattr(
        engine_module,
        '_supports_pytest_callable_signature',
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        engine_module,
        '_parse_parametrize_specs',
        lambda *_args, **_kwargs: SimpleNamespace(
            specs=(SimpleNamespace(arg_names=('arg',)),),
        ),
    )
    signature_issue = engine._build_signature_issue(
        node,
        fixtures={},
        validation_context=validation_context,
    )
    assert signature_issue is not None
    assert signature_issue.code == 'pytest_unsupported_test_signature'

    fixture = SimpleNamespace(function_name='demo_fixture', line=11)
    monkeypatch.setattr(
        engine_module,
        '_is_supported_fixture_definition',
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        engine_module,
        '_get_fixture_support_reason',
        lambda *_args, **_kwargs: 'cyclic_dependency',
    )
    fixture_issue = engine._build_fixture_issue(fixture, fixtures={})
    assert fixture_issue is not None
    assert fixture_issue.code == 'pytest_unsupported_fixture_definition'


def test_build_draft_validation_issues_traverses_functions_and_test_classes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')
    module = ast.parse(
        '\n'.join(
            (
                'value = 1',
                '',
                'def helper():',
                '    pass',
                '',
                'def test_top_level():',
                '    pass',
                '',
                'class TestSuite:',
                '    marker = True',
                '    def test_method(self):',
                '        pass',
                '',
                'class HelperClass:',
                '    def test_ignored(self):',
                '        pass',
            ),
        ),
    )
    fixtures = {
        'fixture_a': SimpleNamespace(
            function_name='fixture_a',
            line=3,
        ),
    }
    recorded_nodes: list[str] = []
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_fixture_issue',
        lambda *_args, **_kwargs: DraftValidationIssue(
            code='fixture_issue',
            message='fixture issue',
            severity='error',
            line=3,
        ),
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_test_node_issues',
        lambda _self, node, **_kwargs: (
            recorded_nodes.append(node.name),
            DraftValidationIssue(
                code=f'issue_{node.name}',
                message=f'issue {node.name}',
                severity='warning',
                line=node.lineno,
            ),
        )[1:],
    )

    issues = engine._build_draft_validation_issues(
        module,
        fixtures,
        literal_bindings={},
        expression_bindings={},
        pytest_plugins_runtime_reason='runtime reason',
    )

    assert 'test_top_level' in recorded_nodes
    assert 'test_method' in recorded_nodes
    assert 'test_ignored' not in recorded_nodes
    issue_codes = {issue.code for issue in issues}
    assert 'fixture_issue' in issue_codes
    assert 'issue_test_top_level' in issue_codes
    assert 'issue_test_method' in issue_codes


def test_build_test_node_issues_appends_xfail_and_skip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')
    node = ast.parse('def test_case():\n    pass\n').body[0]
    assert isinstance(node, ast.FunctionDef)
    validation_context = _build_validation_context()

    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_pytest_plugins_runtime_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_usefixtures_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_xfail_issue',
        lambda *_args, **_kwargs: DraftValidationIssue(
            code='xfail',
            message='xfail issue',
            severity='warning',
            line=node.lineno,
        ),
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_skip_issue',
        lambda *_args, **_kwargs: DraftValidationIssue(
            code='skip',
            message='skip issue',
            severity='warning',
            line=node.lineno,
        ),
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_parametrize_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_signature_issue',
        lambda *_args, **_kwargs: None,
    )

    issues = engine._build_test_node_issues(
        node,
        fixtures={},
        validation_context=validation_context,
    )
    assert [issue.code for issue in issues] == ['xfail', 'skip']


def test_build_test_node_issues_short_circuits_on_parametrize_issue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')
    node = ast.parse('def test_case():\n    pass\n').body[0]
    assert isinstance(node, ast.FunctionDef)
    validation_context = _build_validation_context()

    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_pytest_plugins_runtime_issue',
        lambda *_args, **_kwargs: DraftValidationIssue(
            code='plugins',
            message='plugins',
            severity='warning',
            line=node.lineno,
        ),
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_usefixtures_issue',
        lambda *_args, **_kwargs: DraftValidationIssue(
            code='usefixtures',
            message='usefixtures',
            severity='warning',
            line=node.lineno,
        ),
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_xfail_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_skip_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_parametrize_issue',
        lambda *_args, **_kwargs: DraftValidationIssue(
            code='parametrize',
            message='parametrize',
            severity='error',
            line=node.lineno,
        ),
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_signature_issue',
        lambda *_args, **_kwargs: DraftValidationIssue(
            code='signature',
            message='signature',
            severity='error',
            line=node.lineno,
        ),
    )

    issues = engine._build_test_node_issues(
        node,
        fixtures={},
        validation_context=validation_context,
    )
    assert [issue.code for issue in issues] == [
        'plugins',
        'usefixtures',
        'parametrize',
    ]


def test_build_test_node_issues_includes_signature_when_no_parametrize_issue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')
    node = ast.parse('def test_case():\n    pass\n').body[0]
    assert isinstance(node, ast.FunctionDef)
    validation_context = _build_validation_context()

    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_pytest_plugins_runtime_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_usefixtures_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_xfail_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_skip_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_parametrize_issue',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module.PytestEngine,
        '_build_signature_issue',
        lambda *_args, **_kwargs: DraftValidationIssue(
            code='signature',
            message='signature issue',
            severity='error',
            line=node.lineno,
        ),
    )

    issues = engine._build_test_node_issues(
        node,
        fixtures={},
        validation_context=validation_context,
    )
    assert len(issues) == 1
    assert issues[0].code == 'signature'


def test_resolve_fixture_definitions_from_visible_source_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / 'definitions.py'
    source_path.write_text('', encoding='utf-8')
    context = engine_module._PytestDefinitionResolutionContext(
        engine_name='pytest',
        test_path=tmp_path / 'tests' / 'test_demo.py',
        root_path=tmp_path,
        configured_definition_paths=(tmp_path / 'configured.py',),
    )
    monkeypatch.setattr(
        engine_module,
        '_resolve_imported_fixture_binding_definition',
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_visible_fixture_source_paths',
        lambda *_args, **_kwargs: (source_path,),
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_conftest_paths',
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        engine_module,
        '_build_fixture_source_metadata',
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_fixture_knowledge_records',
        lambda *_args, **_kwargs: (
            SimpleNamespace(
                function_name='workspace_fixture',
                line=12,
                source_category='configured_definition_fixture',
                provider_kind='definition_path',
                provider_name='definitions.py',
                documentation='fixture docs',
            ),
        ),
    )

    result = engine_module._resolve_fixture_definitions(
        context=context,
        test_path=context.test_path,
        fixture_name='workspace_fixture',
    )

    assert len(result) == 1
    assert result[0].function_name == 'workspace_fixture'
    assert result[0].line == 12


def test_engine_dependency_descriptor_exposed() -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    dependencies = engine.describe_engine_dependencies()
    assert len(dependencies) == 2
    assert dependencies[0].target_engine_name == 'gherkin'


def test_build_signature_parametrize_and_issue_helpers_non_test_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')
    helper_node = ast.parse('def helper():\n    pass\n').body[0]
    assert isinstance(helper_node, ast.FunctionDef)
    validation_context = _build_validation_context()

    assert (
        engine._build_signature_issue(
            helper_node,
            fixtures={},
            validation_context=validation_context,
        )
        is None
    )
    assert (
        engine._build_parametrize_issue(
            helper_node,
            marker_aliases={'pytest'},
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )
    assert (
        engine._build_usefixtures_issue(
            helper_node,
            marker_aliases={'pytest'},
        )
        is None
    )
    assert (
        engine._build_xfail_issue(
            helper_node,
            marker_aliases={'pytest'},
            validation_context=validation_context,
        )
        is None
    )
    assert (
        engine._build_skip_issue(
            helper_node,
            marker_aliases={'pytest'},
            validation_context=validation_context,
        )
        is None
    )

    monkeypatch.setattr(
        engine_module,
        '_supports_pytest_callable_signature',
        lambda *_args, **_kwargs: True,
    )
    test_node = ast.parse('def test_case():\n    pass\n').body[0]
    assert isinstance(test_node, ast.FunctionDef)
    assert (
        engine._build_signature_issue(
            test_node,
            fixtures={},
            validation_context=validation_context,
        )
        is None
    )
    method_node = ast.parse(
        'class TestSuite:\n    def test_case(self, fixture_a):\n        pass\n',
    ).body[0].body[0]
    assert isinstance(method_node, ast.FunctionDef)
    monkeypatch.setattr(
        engine_module,
        '_supports_pytest_callable_signature',
        lambda *_args, **_kwargs: False,
    )
    method_issue = engine._build_signature_issue(
        method_node,
        class_name='TestSuite',
        fixtures={},
        validation_context=validation_context,
    )
    assert method_issue is not None
    assert 'single `self` parameter plus fixture parameters' in (
        method_issue.message
    )

    monkeypatch.setattr(
        engine_module,
        '_build_usefixtures_decision',
        lambda *_args, **_kwargs: SimpleNamespace(
            issue_code=None,
            issue_message=None,
            issue_line=None,
        ),
    )
    assert (
        engine._build_usefixtures_issue(
            test_node,
            marker_aliases={'pytest'},
        )
        is None
    )
    monkeypatch.setattr(
        engine_module,
        '_build_static_xfail_decision',
        lambda *_args, **_kwargs: SimpleNamespace(
            issue_code=None,
            requires_pytest_runtime=False,
            issue_message=None,
            runtime_reason=None,
            issue_line=None,
        ),
    )
    assert (
        engine._build_xfail_issue(
            test_node,
            marker_aliases={'pytest'},
            validation_context=validation_context,
        )
        is None
    )
    monkeypatch.setattr(
        engine_module,
        '_build_static_skip_decision',
        lambda *_args, **_kwargs: SimpleNamespace(
            issue_code=None,
            requires_pytest_runtime=False,
            issue_message=None,
            runtime_reason=None,
            issue_line=None,
        ),
    )
    assert (
        engine._build_skip_issue(
            test_node,
            marker_aliases={'pytest'},
            validation_context=validation_context,
        )
        is None
    )
    monkeypatch.setattr(
        engine_module,
        '_parse_parametrize_specs',
        lambda *_args, **_kwargs: SimpleNamespace(
            issue_code=None,
            issue_message=None,
            issue_line=None,
            specs=(),
        ),
    )
    assert (
        engine._build_parametrize_issue(
            test_node,
            marker_aliases={'pytest'},
            literal_bindings={},
            expression_bindings={},
        )
        is None
    )


def test_build_fixture_issue_supported_and_fallback_messages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')
    fixture = SimpleNamespace(function_name='fixture_a', line=5)

    monkeypatch.setattr(
        engine_module,
        '_is_supported_fixture_definition',
        lambda *_args, **_kwargs: True,
    )
    assert engine._build_fixture_issue(fixture, fixtures={}) is None

    monkeypatch.setattr(
        engine_module,
        '_is_supported_fixture_definition',
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        engine_module,
        '_get_fixture_support_reason',
        lambda *_args, **_kwargs: 'non_cyclic',
    )
    issue = engine._build_fixture_issue(fixture, fixtures={})
    assert issue is not None
    assert 'supports fixtures whose unresolved dependencies' in issue.message


def test_resolve_fixture_and_imported_definition_continue_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_path = tmp_path / 'tests' / 'test_demo.py'
    test_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.write_text('', encoding='utf-8')
    source_path = tmp_path / 'source_defs.py'
    source_path.write_text('', encoding='utf-8')
    context = engine_module._PytestDefinitionResolutionContext(
        engine_name='pytest',
        test_path=test_path,
        root_path=tmp_path,
    )

    monkeypatch.setattr(
        engine_module,
        '_resolve_imported_fixture_binding_definition',
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_visible_fixture_source_paths',
        lambda *_args, **_kwargs: (source_path,),
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_conftest_paths',
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        engine_module,
        '_build_fixture_source_metadata',
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_fixture_knowledge_records',
        lambda *_args, **_kwargs: (
            SimpleNamespace(
                function_name='other_fixture',
                line=1,
                source_category='fixture',
                provider_kind='k',
                provider_name='p',
                documentation=None,
            ),
        ),
    )
    assert (
        engine_module._resolve_fixture_definitions(
            context=context,
            test_path=test_path,
            fixture_name='workspace_fixture',
        )
        == ()
    )

    configured_path = tmp_path / 'configured.py'
    configured_path.write_text('', encoding='utf-8')
    context_with_config = engine_module._PytestDefinitionResolutionContext(
        engine_name='pytest',
        test_path=test_path,
        root_path=tmp_path,
        configured_definition_paths=(configured_path,),
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_imported_fixture_bindings',
        lambda *_args, **_kwargs: {
            'fixture_name': SimpleNamespace(
                source_path=None,
                function_name='provider',
            ),
        },
    )
    assert (
        engine_module._resolve_imported_fixture_binding_definition(
            test_path=test_path,
            fixture_name='fixture_name',
            context=context_with_config,
        )
        is None
    )

    monkeypatch.setattr(
        engine_module,
        '_discover_imported_fixture_bindings',
        lambda *_args, **_kwargs: {
            'fixture_name': SimpleNamespace(
                source_path=str(configured_path),
                function_name='provider',
            ),
        },
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_fixture_knowledge_records',
        lambda *_args, **_kwargs: (
            SimpleNamespace(
                function_name='other_provider',
                line=3,
                source_category='fixture',
                provider_kind='k',
                provider_name='p',
                documentation=None,
            ),
        ),
    )
    assert (
        engine_module._resolve_imported_fixture_binding_definition(
            test_path=test_path,
            fixture_name='fixture_name',
            context=context_with_config,
        )
        is None
    )


def test_resolve_imported_fixture_binding_definition_continue_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_path = tmp_path / 'tests' / 'test_demo.py'
    test_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.write_text('', encoding='utf-8')
    configured_path = tmp_path / 'configured.py'
    configured_path.write_text('', encoding='utf-8')
    conftest_path = tmp_path / 'tests' / 'conftest.py'
    context = engine_module._PytestDefinitionResolutionContext(
        engine_name='pytest',
        test_path=test_path,
        root_path=tmp_path,
        configured_definition_paths=(configured_path,),
    )
    binding_source = tmp_path / 'bindings.py'
    binding_source.write_text('', encoding='utf-8')

    monkeypatch.setattr(
        engine_module,
        '_discover_conftest_paths',
        lambda *_args, **_kwargs: (conftest_path,),
    )

    def _discover_imported_bindings(source_path, **_kwargs):
        if Path(source_path).resolve() == test_path.resolve():
            return {}
        return {
            'target_fixture': SimpleNamespace(
                source_path=str(binding_source),
                function_name='target_fixture',
            ),
        }

    monkeypatch.setattr(
        engine_module,
        '_discover_imported_fixture_bindings',
        _discover_imported_bindings,
    )
    monkeypatch.setattr(
        engine_module,
        '_build_fixture_source_metadata',
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_fixture_knowledge_records',
        lambda *_args, **_kwargs: (
            SimpleNamespace(
                function_name='other_fixture',
                line=7,
                source_category='fixture',
                provider_kind='definition_path',
                provider_name='definitions.py',
                documentation=None,
            ),
        ),
    )

    result = engine_module._resolve_imported_fixture_binding_definition(
        test_path=test_path,
        fixture_name='target_fixture',
        context=context,
    )
    assert result is None


def test_resolve_test_definitions_and_docs_continue_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_path = tmp_path / 'tests' / 'test_docs.py'
    test_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.write_text(
        '\n'.join(
            (
                'value = 1',
                '',
                'class TestSuite:',
                '    value = 2',
                '    def test_case(self):',
                '        """case doc"""',
                '        pass',
            ),
        ),
        encoding='utf-8',
    )
    real_discover_docs = engine_module._discover_test_documentation
    monkeypatch.setattr(
        engine_module,
        '_discover_pytest_tests',
        lambda *_args, **_kwargs: (
            PytestTestDefinition(function_name='test_case', line=1),
        ),
    )
    monkeypatch.setattr(
        engine_module,
        '_discover_test_documentation',
        lambda *_args, **_kwargs: {(None, 'test_case'): 'doc'},
    )
    assert (
        engine_module._resolve_test_definitions(
            engine_name='pytest',
            test_path=test_path,
            test_name='other_name',
            root_path=tmp_path,
        )
        == ()
    )

    docs = real_discover_docs(test_path)
    assert docs[('TestSuite', 'test_case')] == 'case doc'
