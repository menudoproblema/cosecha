from __future__ import annotations

import asyncio
import json
import logging
import sys

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.config import ConfigSnapshot
from cosecha.core.cosecha_manifest import ManifestValidationError
from cosecha.core.operations import KnowledgeQueryContext, RunOperationResult
from cosecha.core.runtime import LocalRuntimeProvider
from cosecha.shell import runner_cli
from cosecha_internal.testkit import build_config


def _dummy_snapshot() -> ConfigSnapshot:
    return ConfigSnapshot(
        root_path='/workspace/demo',
        output_mode='summary',
        output_detail='standard',
        capture_log=True,
        stop_on_error=False,
        concurrency=1,
        strict_step_ambiguity=False,
    )


def _runtime_context(tmp_path: Path) -> runner_cli.RuntimeCliContext:
    return runner_cli.RuntimeCliContext(
        args=Namespace(),
        config=build_config(tmp_path),
        plugins=(),
        runtime_provider=LocalRuntimeProvider(),
        selection=runner_cli.CliSelection(),
    )


def test_build_instrumentation_metadata_payload_and_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = SimpleNamespace(
        root_path=str(tmp_path),
        session_id='session-1',
        config_snapshot=_dummy_snapshot(),
    )
    payload = runner_cli._build_instrumentation_metadata_payload(
        artifact,
        tmp_path / 'kb.db',
        extra_fields={'trace_id': 'trace-1'},
    )
    assert payload['knowledge_base_path'] == str(tmp_path / 'kb.db')
    assert payload['trace_id'] == 'trace-1'

    metadata_path = tmp_path / 'metadata.json'
    monkeypatch.setenv(
        runner_cli.COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
        str(metadata_path),
    )
    runner_cli._write_instrumentation_metadata_from_environment(
        artifact,
        tmp_path / 'kb.db',
    )
    written = json.loads(metadata_path.read_text(encoding='utf-8'))
    assert written['session_id'] == 'session-1'

    monkeypatch.delenv(
        runner_cli.COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
        raising=False,
    )
    runner_cli._write_instrumentation_metadata_from_environment(
        artifact,
        tmp_path / 'kb.db',
    )


def test_load_parser_manifest_swallows_validation_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_cli,
        'load_cosecha_manifest',
        lambda *args, **kwargs: (_ for _ in ()).throw(
            ManifestValidationError('invalid manifest'),
        ),
    )
    assert runner_cli._load_parser_manifest() is None


def test_create_parser_registers_manifest_hooks_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, object]] = []
    manifest = SimpleNamespace(path='cosecha.toml')
    monkeypatch.setattr(runner_cli, '_load_parser_manifest', lambda: manifest)
    monkeypatch.setattr(
        runner_cli,
        'register_manifest_hook_arguments',
        lambda parser, loaded_manifest: calls.append((parser, loaded_manifest)),
    )

    parser, _available_plugins = runner_cli._create_parser(
        include_manifest_hook_arguments=True,
    )

    assert parser is not None
    assert len(calls) == 3
    assert all(call[1] is manifest for call in calls)


def test_parse_args_covers_runtime_and_maintenance_variants(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    parsed = (
        runner_cli.parse_args(['plan', 'analyze']),
        runner_cli.parse_args(['plan', 'explain']),
        runner_cli.parse_args(['plan', 'simulate']),
        runner_cli.parse_args(['manifest', 'explain']),
        runner_cli.parse_args(['knowledge', 'query', 'definitions']),
        runner_cli.parse_args(['knowledge', 'query', 'registry']),
        runner_cli.parse_args(['knowledge', 'query', 'resources']),
        runner_cli.parse_args(['knowledge', 'query', 'artifacts']),
        runner_cli.parse_args(['knowledge', 'query', 'events']),
        runner_cli.parse_args(['knowledge', 'reset']),
        runner_cli.parse_args(['knowledge', 'rebuild']),
        runner_cli.parse_args(['session', 'artifacts']),
        runner_cli.parse_args(['session', 'events']),
        runner_cli.parse_args(['doctor']),
        runner_cli.parse_args(['gherkin', 'validate', 'tests/a.feature']),
        runner_cli.parse_args(['gherkin', 'pre-commit', 'tests/a.feature']),
        runner_cli.parse_args(['pytest', 'validate', 'tests/test_demo.py']),
    )

    assert isinstance(parsed[0], runner_cli.AnalyzeCliRequest)
    assert isinstance(parsed[1], runner_cli.ExplainCliRequest)
    assert isinstance(parsed[2], runner_cli.SimulateCliRequest)
    assert isinstance(parsed[3], runner_cli.ManifestExplainCliRequest)
    assert isinstance(parsed[4], runner_cli.KnowledgeQueryCliRequest)
    assert isinstance(parsed[9], runner_cli.KnowledgeResetCliRequest)
    assert isinstance(parsed[10], runner_cli.KnowledgeRebuildCliRequest)
    assert isinstance(parsed[11], runner_cli.SessionQueryCliRequest)
    assert isinstance(parsed[12], runner_cli.SessionQueryCliRequest)
    assert isinstance(parsed[13], runner_cli.DoctorCliRequest)
    assert isinstance(parsed[14], runner_cli.GherkinValidateCliRequest)
    assert isinstance(parsed[15], runner_cli.GherkinPreCommitCliRequest)
    assert isinstance(parsed[16], runner_cli.PytestValidateCliRequest)


def test_parse_args_help_branch_raises_when_parser_does_not_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeParser:
        def parse_args(self, _argv):
            return Namespace()

    monkeypatch.setattr(
        runner_cli,
        '_create_parser',
        lambda **kwargs: (_FakeParser(), []),
    )

    with pytest.raises(RuntimeError, match='returned without exiting'):
        runner_cli.parse_args(['--help'])


@pytest.mark.parametrize(
    ('builder', 'payload'),
    (
        (
            runner_cli._build_runtime_request,
            Namespace(command_group='plan', plan_command='bad', mode='strict'),
        ),
        (
            runner_cli._build_manifest_request,
            Namespace(manifest_command='bad', manifest_file=None),
        ),
        (
            runner_cli._build_session_query_request,
            Namespace(session_command='bad'),
        ),
        (
            runner_cli._build_pytest_request,
            Namespace(pytest_command='bad'),
        ),
        (
            runner_cli._build_gherkin_request,
            Namespace(gherkin_command='bad'),
        ),
        (
            runner_cli._build_knowledge_request,
            Namespace(knowledge_command='bad'),
        ),
        (
            runner_cli._build_maintenance_request,
            Namespace(command_group='bad'),
        ),
    ),
)
def test_builders_unreachable_branches_raise_assertion(
    builder,
    payload: Namespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_cli,
        '_build_runtime_context',
        lambda args, *, available_plugins: object(),
    )
    monkeypatch.setattr(
        runner_cli,
        '_build_gherkin_runtime_context',
        lambda args, *, available_plugins: object(),
    )
    monkeypatch.setattr(
        runner_cli,
        '_build_selection',
        lambda args: runner_cli.CliSelection(),
    )
    monkeypatch.setattr(
        runner_cli,
        '_resolve_root_path',
        lambda: Path('/tmp'),
    )

    kwargs = {}
    if builder in {
        runner_cli._build_runtime_request,
        runner_cli._build_pytest_request,
        runner_cli._build_gherkin_request,
        runner_cli._build_knowledge_request,
        runner_cli._build_maintenance_request,
    }:
        kwargs['available_plugins'] = []

    with pytest.raises(AssertionError):
        builder(payload, **kwargs)


def test_normalize_cli_path_selector_absolute_and_invalid_cases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_path = (tmp_path / 'tests').resolve()
    root_path.mkdir(parents=True)
    inside = root_path / 'suite' / 'a.feature'
    inside.parent.mkdir(parents=True)
    inside.write_text('Feature: A\n', encoding='utf-8')

    normalized = runner_cli._normalize_cli_path_selector(
        str(inside),
        root_path=root_path,
    )
    assert normalized == 'suite/a.feature'

    with pytest.raises(ValueError, match='Path selector must point inside'):
        runner_cli._normalize_cli_path_selector(
            '/outside/path.feature',
            root_path=root_path,
        )

    monkeypatch.chdir(tmp_path)
    unresolved = runner_cli._normalize_cli_path_selector(
        'unknown/feature.feature',
        root_path=root_path,
    )
    assert unresolved == 'unknown/feature.feature'


def test_collect_feature_and_python_paths_error_branches(
    tmp_path: Path,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir(parents=True)
    invalid_file = root_path / 'README.md'
    invalid_file.write_text('# docs\n', encoding='utf-8')

    assert runner_cli._collect_gherkin_feature_paths(
        ('missing.feature',),
        root_path=root_path,
        allow_missing=True,
    ) == ()
    with pytest.raises(ValueError, match='Expected a .feature file'):
        runner_cli._collect_gherkin_feature_paths(
            ('README.md',),
            root_path=root_path,
        )

    with pytest.raises(ValueError, match='Pytest path does not exist'):
        runner_cli._collect_python_validation_paths(
            ('missing.py',),
            root_path=root_path,
        )
    with pytest.raises(ValueError, match='Expected a .py file'):
        runner_cli._collect_python_validation_paths(
            ('README.md',),
            root_path=root_path,
        )


def test_apply_gherkin_format_edits_import_error_branch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    original_import = __import__

    def _broken_import(name, *args, **kwargs):
        if name == 'cosecha.engine.gherkin.formatter':
            raise ModuleNotFoundError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr('builtins.__import__', _broken_import)
    with pytest.raises(RuntimeError, match='optional package'):
        runner_cli._apply_gherkin_format_edits(
            'Feature: Demo\n',
            file_path=tmp_path / 'demo.feature',
        )


def test_execute_gherkin_format_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path_a = tmp_path / 'a.feature'
    path_b = tmp_path / 'b.feature'
    path_a.write_text('Feature: A\n', encoding='utf-8')
    path_b.write_text('Feature: B\n', encoding='utf-8')

    monkeypatch.setattr(
        runner_cli,
        '_collect_gherkin_feature_paths',
        lambda *args, **kwargs: (),
    )
    runner_cli._execute_gherkin_format(
        runner_cli.GherkinFormatCliRequest(
            root_path=tmp_path,
            paths=('x',),
            check=False,
        ),
    )

    monkeypatch.setattr(
        runner_cli,
        '_collect_gherkin_feature_paths',
        lambda *args, **kwargs: (path_a, path_b),
    )
    monkeypatch.setattr(
        runner_cli,
        '_apply_gherkin_format_edits',
        lambda content, *, file_path: content + '# formatted\n'
        if file_path == path_a
        else content,
    )
    with pytest.raises(SystemExit, match='1'):
        runner_cli._execute_gherkin_format(
            runner_cli.GherkinFormatCliRequest(
                root_path=tmp_path,
                paths=('x',),
                check=True,
            ),
        )


def test_format_validation_issue_with_location() -> None:
    issue = SimpleNamespace(
        severity='error',
        code='gherkin.syntax',
        line=4,
        column=2,
        message='unexpected token',
    )
    rendered = runner_cli._format_validation_issue(issue)
    assert '[ERROR] gherkin.syntax: line=4 column=2 unexpected token' == rendered


def test_validate_gherkin_features_and_pytest_modules_core_flow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir(parents=True)
    feature_ok = root_path / 'ok.feature'
    feature_missing_engine = root_path / 'missing_engine.feature'
    module_ok = root_path / 'test_ok.py'
    module_missing_engine = root_path / 'test_missing.py'
    for path in (feature_ok, feature_missing_engine):
        path.write_text('Feature: Demo\n', encoding='utf-8')
    for path in (module_ok, module_missing_engine):
        path.write_text('def test_demo():\n    pass\n', encoding='utf-8')

    issues = (
        SimpleNamespace(
            severity='warning',
            code='draft.warn',
            line=None,
            column=None,
            message='warning',
        ),
    )

    class _FakeRunner:
        def __init__(self, *_args, **_kwargs):
            return None

        async def start_session(self, *_args, **_kwargs):
            return None

        def find_engine(self, path):
            name = Path(path).name
            if name in {'missing_engine.feature', 'test_missing.py'}:
                return None
            if name.endswith('.py'):
                return SimpleNamespace(name='pytest')
            return SimpleNamespace(name='gherkin')

        async def execute_operation(self, operation):
            del operation
            return SimpleNamespace(validation=SimpleNamespace(issues=issues))

        async def finish_session(self):
            return None

        def _stop_log_capture(self):
            return None

    monkeypatch.setattr(runner_cli, 'Runner', _FakeRunner)
    monkeypatch.setattr(
        runner_cli,
        'setup_engines',
        lambda *args, **kwargs: ((), {}),
    )
    monkeypatch.setattr(
        runner_cli,
        '_collect_gherkin_feature_paths',
        lambda *args, **kwargs: (feature_ok, feature_missing_engine),
    )
    monkeypatch.setattr(
        runner_cli,
        '_collect_python_validation_paths',
        lambda *args, **kwargs: (module_ok, module_missing_engine),
    )
    if runner_cli.capture_handler not in runner_cli.root_logger.handlers:
        runner_cli.root_logger.addHandler(runner_cli.capture_handler)

    context = _runtime_context(root_path)
    gherkin_result = asyncio.run(
        runner_cli._validate_gherkin_features(
            runner_cli.GherkinValidateCliRequest(
                context=context,
                paths=('ok.feature',),
            ),
        ),
    )
    pytest_result = asyncio.run(
        runner_cli._validate_pytest_modules(
            runner_cli.PytestValidateCliRequest(
                context=context,
                paths=('test_ok.py',),
            ),
        ),
    )

    assert len(gherkin_result) == 2
    assert len(pytest_result) == 2
    assert any('No active Gherkin engine matched this file' in item[0] for _, item in gherkin_result)
    assert any('No active Pytest engine matched this file' in item[0] for _, item in pytest_result)


def test_execute_validate_wrappers_branches(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def _validate_gherkin_with_issues(_request):
        return (
            (tmp_path / 'ok.feature', ()),
            (tmp_path / 'bad.feature', ('issue',)),
        )

    monkeypatch.setattr(
        runner_cli,
        '_validate_gherkin_features',
        _validate_gherkin_with_issues,
    )
    with pytest.raises(SystemExit, match='1'):
        runner_cli._execute_gherkin_validate(
            runner_cli.GherkinValidateCliRequest(
                context=_runtime_context(tmp_path),
                paths=('x.feature',),
            ),
        )

    async def _validate_gherkin_empty(_request):
        return ()

    monkeypatch.setattr(
        runner_cli,
        '_validate_gherkin_features',
        _validate_gherkin_empty,
    )
    runner_cli._execute_gherkin_validate(
        runner_cli.GherkinValidateCliRequest(
            context=_runtime_context(tmp_path),
            paths=('x.feature',),
        ),
    )

    async def _validate_pytest_with_issues(_request):
        return (
            (tmp_path / 'ok.py', ()),
            (tmp_path / 'bad.py', ('issue',)),
        )

    monkeypatch.setattr(
        runner_cli,
        '_validate_pytest_modules',
        _validate_pytest_with_issues,
    )
    with pytest.raises(SystemExit, match='1'):
        runner_cli._execute_pytest_validate(
            runner_cli.PytestValidateCliRequest(
                context=_runtime_context(tmp_path),
                paths=('x.py',),
            ),
        )
    async def _validate_pytest_empty(_request):
        return ()

    monkeypatch.setattr(
        runner_cli,
        '_validate_pytest_modules',
        _validate_pytest_empty,
    )
    runner_cli._execute_pytest_validate(
        runner_cli.PytestValidateCliRequest(
            context=_runtime_context(tmp_path),
            paths=('x.py',),
        ),
    )


def test_execute_gherkin_pre_commit_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    feature = tmp_path / 'a.feature'
    feature.write_text('Feature: A\n', encoding='utf-8')

    monkeypatch.setattr(
        runner_cli,
        '_collect_gherkin_feature_paths',
        lambda *args, **kwargs: (),
    )
    runner_cli._execute_gherkin_pre_commit(
        runner_cli.GherkinPreCommitCliRequest(
            context=_runtime_context(tmp_path),
            paths=('a.feature',),
            write=True,
        ),
    )

    monkeypatch.setattr(
        runner_cli,
        '_collect_gherkin_feature_paths',
        lambda *args, **kwargs: (feature,),
    )
    monkeypatch.setattr(
        runner_cli,
        '_apply_gherkin_format_edits',
        lambda content, *, file_path: content + '# changed\n',
    )
    async def _validate_gherkin_pre_commit(_request):
        return ((feature, ('issue',)),)

    monkeypatch.setattr(
        runner_cli,
        '_validate_gherkin_features',
        _validate_gherkin_pre_commit,
    )
    with pytest.raises(SystemExit, match='1'):
        runner_cli._execute_gherkin_pre_commit(
            runner_cli.GherkinPreCommitCliRequest(
                context=_runtime_context(tmp_path),
                paths=('a.feature',),
                write=False,
            ),
        )


def test_manifest_execute_flows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(runner_cli, 'load_cosecha_manifest', lambda *args, **kwargs: None)
    with pytest.raises(SystemExit, match='2'):
        runner_cli._execute_manifest_validate(
            runner_cli.ManifestValidateCliRequest(manifest_file=None),
        )
    with pytest.raises(SystemExit, match='2'):
        runner_cli._execute_manifest_show(
            runner_cli.ManifestShowCliRequest(manifest_file=None),
        )

    manifest = SimpleNamespace(path='cosecha.toml', to_dict=lambda: {'a': 1})
    monkeypatch.setattr(runner_cli, 'load_cosecha_manifest', lambda *args, **kwargs: manifest)
    monkeypatch.setattr(runner_cli, 'validate_cosecha_manifest', lambda loaded: ['invalid'])
    with pytest.raises(SystemExit, match='2'):
        runner_cli._execute_manifest_validate(
            runner_cli.ManifestValidateCliRequest(manifest_file=None),
        )
    monkeypatch.setattr(runner_cli, 'validate_cosecha_manifest', lambda loaded: [])
    runner_cli._execute_manifest_validate(
        runner_cli.ManifestValidateCliRequest(manifest_file=None),
    )
    runner_cli._execute_manifest_show(
        runner_cli.ManifestShowCliRequest(manifest_file=None),
    )
    output = capsys.readouterr().out
    assert 'Valid manifest' in output
    assert '"a": 1' in output


def test_manifest_explain_and_serializer_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    explanation = SimpleNamespace(
        manifest_path='cosecha.toml',
        schema_version=1,
        root_path=str(tmp_path),
        selected_engine_names=('pytest',),
        requested_paths=('tests',),
        normalized_paths=('tests',),
        active_engines=(),
        inactive_engine_ids=(),
        evaluated_engines=(),
        active_runtime_profile_ids=(),
        inactive_runtime_profile_ids=(),
        evaluated_runtime_profiles=(),
        active_resource_names=(),
        inactive_resource_names=(),
        evaluated_resources=(),
        workspace={'fingerprint': 'fp-1'},
        execution_context={'execution_root': str(tmp_path)},
    )
    payload = runner_cli._serialize_manifest_explanation_payload(explanation)
    assert payload['workspace_fingerprint'] == 'fp-1'
    assert payload['execution_context']['execution_root'] == str(tmp_path)

    manifest = SimpleNamespace(path='cosecha.toml', manifest_dir=str(tmp_path))
    monkeypatch.setattr(runner_cli, 'load_cosecha_manifest', lambda *_: manifest)
    monkeypatch.setattr(
        runner_cli,
        'apply_manifest_cli_overrides',
        lambda loaded, args: loaded,
    )
    workspace = SimpleNamespace(
        knowledge_anchor=tmp_path,
        workspace_root=tmp_path,
        fingerprint='fp',
    )
    monkeypatch.setattr(runner_cli, 'resolve_workspace', lambda start_path: workspace)
    monkeypatch.setattr(runner_cli, 'build_execution_context', lambda ws: SimpleNamespace())
    monkeypatch.setattr(runner_cli, '_bind_shadow_context_from_environment', lambda ctx: ctx)
    monkeypatch.setattr(runner_cli, 'explain_cosecha_manifest', lambda *args, **kwargs: explanation)
    printed: dict[str, object] = {}
    monkeypatch.setattr(runner_cli, '_print_json_payload', lambda data: printed.update(data))

    runner_cli._execute_manifest_explain(
        runner_cli.ManifestExplainCliRequest(
            args=Namespace(),
            manifest_file=None,
            selection=runner_cli.CliSelection(),
            root_path=tmp_path,
        ),
    )
    assert printed['manifest']['path'] == 'cosecha.toml'


def test_knowledge_reset_and_rebuild_and_file_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    kb_root = tmp_path / '.cosecha'
    kb_root.mkdir(parents=True)
    db_path = kb_root / 'kb.db'
    db_path.write_text('x', encoding='utf-8')
    (kb_root / 'kb.db-wal').write_text('x', encoding='utf-8')

    base_paths = runner_cli._iter_knowledge_base_file_paths(
        tmp_path,
        storage_root=kb_root,
    )
    assert any(path.name == 'kb.db' for path in base_paths)
    removed = runner_cli._delete_knowledge_base_files(
        tmp_path,
        storage_root=kb_root,
    )
    assert removed

    workspace = SimpleNamespace(workspace_root=tmp_path)
    execution_context = SimpleNamespace(knowledge_storage_root=kb_root)
    monkeypatch.setattr(
        runner_cli,
        '_resolve_workspace_context_for_path',
        lambda root_path: (workspace, execution_context),
    )
    monkeypatch.setattr(
        runner_cli,
        'resolve_knowledge_base_path',
        lambda *args, **kwargs: kb_root / 'kb.db',
    )
    runner_cli._execute_knowledge_reset(
        runner_cli.KnowledgeResetCliRequest(root_path=tmp_path),
    )

    class _FakeRunner:
        def __init__(self, *_args, **_kwargs):
            self.knowledge_base = SimpleNamespace(
                snapshot=lambda: SimpleNamespace(
                    tests=(1,),
                    definitions=(1, 2),
                    registry_snapshots=(),
                    resources=(),
                ),
            )

        async def start_session(self, *_args, **_kwargs):
            return None

        async def finish_session(self):
            return None

    monkeypatch.setattr(runner_cli, 'setup_engines', lambda *args, **kwargs: ((), {}))
    monkeypatch.setattr(runner_cli, 'Runner', _FakeRunner)
    context = _runtime_context(tmp_path)
    runner_cli._execute_knowledge_rebuild(
        runner_cli.KnowledgeRebuildCliRequest(context=context),
    )


def test_query_helpers_and_execute_request_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match='latest'):
        runner_cli._latest_preset_sort([{'value': 1}], items_key='unsupported')
    with pytest.raises(ValueError, match='failures'):
        runner_cli._failures_preset_items([{'value': 1}], items_key='unknown')

    events = runner_cli._event_failure_items(
        [
            {'event_type': 'node.requeued'},
            {'event_type': 'session.finished', 'has_failures': True},
            {'event_type': 'test.finished', 'status': 'error'},
            {'event_type': 'x', 'error_code': 'E'},
        ],
    )
    assert len(events) == 4
    assert runner_cli._extract_nested_value({'a': {'b': 1}}, 'a.b') == 1
    assert runner_cli._extract_nested_value({'a': 1}, 'a.b') is None
    assert runner_cli._apply_query_preset([], items_key='tests', preset=None) == ([], None, 'asc')
    assert runner_cli._apply_query_preset([], items_key='tests', preset='x') == ([], None, 'asc')
    assert runner_cli._sort_query_items([{'a': 1}], sort_by=None, sort_order='asc') == [{'a': 1}]
    assert runner_cli._project_query_items([{'a': 1}], fields=('a',)) == [{'a': 1}]
    assert runner_cli._coerce_query_items({'a': 1}) is None
    assert runner_cli._find_query_items_key({'unknown': []}) is None
    assert runner_cli._format_query_payload({'x': 1}, render_options=runner_cli.QueryRenderOptions()) == {'x': 1}

    class _FakeResult:
        def to_dict(self):
            return {'tests': [{'status': 'passed'}], 'result_type': 'knowledge.tests'}

    class _FakeRunner:
        def __init__(self, *_args, **_kwargs):
            return None

        async def execute_operation(self, operation):
            del operation
            return _FakeResult()

    monkeypatch.setattr(runner_cli, 'Runner', _FakeRunner)
    printed: dict[str, object] = {}
    monkeypatch.setattr(runner_cli, '_print_json_payload', lambda data: printed.update(data))
    runner_cli._execute_query_request(
        runner_cli.KnowledgeQueryCliRequest(
            config=build_config(tmp_path),
            operation=object(),
            render_options=runner_cli.QueryRenderOptions(),
        ),
    )
    assert printed['result_type'] == 'knowledge.tests'


def test_execute_session_summary_request_and_doctor_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact = SimpleNamespace(
        report_summary=None,
        has_failures=False,
        plan_id=None,
        recorded_at=1.0,
        root_path=str(tmp_path),
        session_id='session-1',
        trace_id='trace-1',
    )

    class _FakeResult:
        def to_dict(self):
            return {
                'context': KnowledgeQueryContext(
                    source='persistent_knowledge_base',
                    freshness='fresh',
                ).to_dict(),
                'artifacts': [artifact],
            }

    class _FakeRunner:
        def __init__(self, *_args, **_kwargs):
            return None

        async def execute_operation(self, operation):
            del operation
            return _FakeResult()

    monkeypatch.setattr(runner_cli, 'Runner', _FakeRunner)
    monkeypatch.setattr(
        runner_cli.QuerySessionArtifactsOperationResult,
        'from_dict',
        classmethod(
            lambda cls, data: SimpleNamespace(
                context=SimpleNamespace(to_dict=lambda: data['context']),
                artifacts=(artifact,),
            ),
        ),
    )
    monkeypatch.setattr(runner_cli, '_format_query_payload', lambda payload, render_options: payload)
    printed: dict[str, object] = {}
    monkeypatch.setattr(runner_cli, '_print_json_payload', lambda payload: printed.update(payload))

    runner_cli._execute_session_summary_request(
        runner_cli.SessionSummaryCliRequest(
            config=build_config(tmp_path),
            operation=SimpleNamespace(),
            render_options=runner_cli.QueryRenderOptions(),
        ),
    )
    assert printed['result_type'] == 'session.summary'

    runner_cli._print_doctor_selection(
        runner_cli.CliSelection(include_paths=('a',), exclude_paths=('b',)),
    )
    runner_cli._print_inactive_decisions('Inactive', [('engine', ())])

    request = runner_cli.DoctorCliRequest(
        args=Namespace(),
        manifest_file=None,
        selection=runner_cli.CliSelection(),
        root_path=tmp_path,
    )
    issues: list[str] = []
    monkeypatch.setattr(runner_cli, 'load_cosecha_manifest', lambda *_: None)
    runner_cli._check_doctor_manifest(request, issues=issues)
    assert issues

    issues.clear()
    monkeypatch.setattr(runner_cli, 'load_cosecha_manifest', lambda *_: SimpleNamespace(path='cosecha.toml', manifest_dir=str(tmp_path)))
    monkeypatch.setattr(runner_cli, 'validate_cosecha_manifest', lambda *_: ['invalid'])
    runner_cli._check_doctor_manifest(request, issues=issues)
    assert issues == ['invalid']

    issues.clear()
    explanation = SimpleNamespace(
        active_engines=(SimpleNamespace(name='pytest'),),
        evaluated_engines=(SimpleNamespace(name='gherkin', active=False, reasons=('filtered',)),),
        active_runtime_profile_ids=('local',),
        evaluated_runtime_profiles=(SimpleNamespace(id='ci', active=False, reasons=('manual',)),),
        active_resource_names=('db',),
        evaluated_resources=(SimpleNamespace(name='cache', active=False, reasons=('off',)),),
    )
    workspace = SimpleNamespace(knowledge_anchor=tmp_path, workspace_root=tmp_path, fingerprint='fp')
    monkeypatch.setattr(runner_cli, 'validate_cosecha_manifest', lambda *_: [])
    monkeypatch.setattr(runner_cli, 'apply_manifest_cli_overrides', lambda manifest, args: manifest)
    monkeypatch.setattr(runner_cli, 'resolve_workspace', lambda start_path: workspace)
    monkeypatch.setattr(runner_cli, 'build_execution_context', lambda ws: SimpleNamespace())
    monkeypatch.setattr(runner_cli, '_bind_shadow_context_from_environment', lambda ctx: ctx)
    monkeypatch.setattr(runner_cli, 'explain_cosecha_manifest', lambda *args, **kwargs: explanation)
    monkeypatch.setattr(runner_cli, 'setup_engines', lambda *args, **kwargs: ((), {}))
    runner_cli._check_doctor_manifest(request, issues=issues)
    assert issues == []

    monkeypatch.setattr(
        runner_cli,
        '_resolve_workspace_context_for_path',
        lambda root_path: (
            SimpleNamespace(workspace_root=tmp_path),
            SimpleNamespace(knowledge_storage_root=tmp_path / '.cosecha'),
        ),
    )
    monkeypatch.setattr(
        runner_cli,
        'resolve_knowledge_base_path',
        lambda *args, **kwargs: tmp_path / '.cosecha' / 'kb.db',
    )
    runner_cli._check_doctor_knowledge_base(request, issues=issues)

    monkeypatch.setattr(
        runner_cli,
        '_check_doctor_manifest',
        lambda request, *, issues: issues.extend(['boom']),
    )
    monkeypatch.setattr(
        runner_cli,
        '_check_doctor_knowledge_base',
        lambda request, *, issues: None,
    )
    with pytest.raises(SystemExit, match='2'):
        runner_cli._execute_doctor(request)

    monkeypatch.setattr(
        runner_cli,
        '_check_doctor_manifest',
        lambda request, *, issues: None,
    )
    runner_cli._execute_doctor(request)
    output = capsys.readouterr().out
    assert 'Doctor status: OK' in output


def test_execute_runtime_request_and_non_runtime_dispatch_and_main(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    non_runtime = runner_cli.ManifestShowCliRequest(manifest_file=None)
    with pytest.raises(RuntimeError, match='Non-runtime requests'):
        runner_cli._execute_runtime_request(non_runtime)

    context = _runtime_context(tmp_path)
    run_request = runner_cli.RunCliRequest(context=context)
    analyze_request = runner_cli.AnalyzeCliRequest(context=context)

    class _FakeRunner:
        def __init__(self, *_args, **_kwargs):
            return None

        async def execute_operation(self, operation):
            if isinstance(operation, runner_cli.RunOperation):
                return RunOperationResult(has_failures=False, total_tests=1)
            return SimpleNamespace(to_dict=lambda: {'plan': 'ok'})

    monkeypatch.setattr(runner_cli, 'Runner', _FakeRunner)
    runner_cli._execute_runtime_request(run_request)

    printed: dict[str, object] = {}
    monkeypatch.setattr(runner_cli, '_print_json_payload', lambda payload: printed.update(payload))
    runner_cli._execute_runtime_request(analyze_request)
    assert printed == {'plan': 'ok'}

    handled = runner_cli._execute_non_runtime_request(run_request)
    assert handled is False

    monkeypatch.setattr(runner_cli, 'parse_args', lambda argv=None: non_runtime)
    monkeypatch.setattr(runner_cli, '_execute_non_runtime_request', lambda request: True)
    monkeypatch.setattr(
        runner_cli,
        'create_loaded_discovery_registry',
        lambda: object(),
    )

    class _Ctx:
        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(runner_cli, 'using_discovery_registry', lambda registry: _Ctx())
    runner_cli.main([])


def test_remaining_runtime_builders_and_context_helpers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context = _runtime_context(tmp_path)
    explain = runner_cli.ExplainCliRequest(context=context, mode='relaxed')
    simulate = runner_cli.SimulateCliRequest(context=context, mode='relaxed')
    assert explain.build_operation().operation_type == 'plan.explain'
    assert simulate.build_operation().operation_type == 'plan.simulate'

    root_path = (tmp_path / 'tests').resolve()
    root_path.mkdir(parents=True)
    normalized = runner_cli._normalize_cli_path_selector(
        f'{root_path.name}/unit/demo.feature',
        root_path=root_path,
    )
    assert normalized == 'unit/demo.feature'

    workspace = SimpleNamespace(
        knowledge_anchor=root_path,
        workspace_root=root_path,
        fingerprint='fp',
    )
    execution_context = SimpleNamespace(
        execution_root=root_path / 'exec',
        knowledge_storage_root=root_path / '.cosecha',
    )
    monkeypatch.setattr(
        runner_cli,
        'resolve_workspace',
        lambda start_path: workspace,
    )
    monkeypatch.setattr(
        runner_cli,
        'build_execution_context',
        lambda ws: execution_context,
    )
    bound = runner_cli._resolve_workspace_context_for_path(root_path)
    assert bound[0] is workspace

    monkeypatch.setenv(runner_cli.COSECHA_SHADOW_ROOT_ENV, str(root_path / 'shadow'))
    monkeypatch.setattr(
        runner_cli,
        'bind_shadow_execution_context',
        lambda execution_context, shadow_context: ('bound', shadow_context.root_path),
    )
    assert runner_cli._bind_shadow_context_from_environment(execution_context)[0] == 'bound'


def test_runtime_context_plugin_activation_and_manifest_hook_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    args = Namespace(
        output='summary',
        detail='standard',
        stop_on_error=False,
        no_capture_log=False,
        concurrency=1,
        strict_step_ambiguity=False,
        persist_live_engine_snapshots=False,
        extra_reports=[],
        definition_paths=[],
        runtime='local',
        selected_engines=[],
        include_paths=[],
        exclude_paths=[],
        include_labels=[],
        exclude_labels=[],
        test_limit=None,
    )

    monkeypatch.setattr(runner_cli, '_resolve_root_path', lambda: tmp_path)
    monkeypatch.setattr(runner_cli, '_resolve_workspace', lambda start_path=None: SimpleNamespace(knowledge_anchor=tmp_path))
    monkeypatch.setattr(runner_cli, '_build_reports', lambda _args: {})

    class _Plugin:
        @classmethod
        def parse_args(cls, parsed_args):
            del parsed_args
            return cls()

    class _PluginNone:
        @classmethod
        def parse_args(cls, parsed_args):
            del parsed_args
            return None

    runtime_context = runner_cli._build_runtime_context(
        args,
        available_plugins=[_Plugin, _PluginNone],
    )
    gherkin_context = runner_cli._build_gherkin_runtime_context(
        args,
        available_plugins=[_Plugin, _PluginNone],
    )
    assert len(runtime_context.plugins) == 1
    assert len(gherkin_context.plugins) == 1

    assert runner_cli._should_include_manifest_hook_arguments(
        Namespace(command_group='gherkin', gherkin_command='fmt'),
    ) is False


def test_apply_gherkin_format_edits_with_fake_formatter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class _Point:
        def __init__(self, line: int):
            self.line = line

    class _Range:
        def __init__(self, start: int, end: int):
            self.start = _Point(start)
            self.end = _Point(end)

    class _Edit:
        def __init__(self, new_text: str):
            self.range = _Range(0, 0)
            self.new_text = new_text

    class _Formatter:
        def provide_document_formatting_edits(self, document):
            del document
            return (_Edit('Feature: Updated'),)

    class _Document:
        def __init__(self, *, uri: str, source: str, version: int):
            self.uri = uri
            self.source = source
            self.version = version

    fake_module = SimpleNamespace(
        GherkinDocumentFormattingEditProvider=_Formatter,
        PlainTextDocument=_Document,
    )
    monkeypatch.setitem(sys.modules, 'cosecha.engine.gherkin.formatter', fake_module)
    updated = runner_cli._apply_gherkin_format_edits(
        'Feature: Old\n',
        file_path=tmp_path / 'demo.feature',
    )
    assert updated.startswith('Feature: Updated')


def test_execute_gherkin_format_write_and_check_ok_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path_a = tmp_path / 'a.feature'
    path_a.write_text('Feature: A\n', encoding='utf-8')
    monkeypatch.setattr(
        runner_cli,
        '_collect_gherkin_feature_paths',
        lambda *args, **kwargs: (path_a,),
    )
    monkeypatch.setattr(
        runner_cli,
        '_apply_gherkin_format_edits',
        lambda content, *, file_path: content + '# updated\n',
    )
    runner_cli._execute_gherkin_format(
        runner_cli.GherkinFormatCliRequest(
            root_path=tmp_path,
            paths=('a.feature',),
            check=False,
        ),
    )

    monkeypatch.setattr(
        runner_cli,
        '_apply_gherkin_format_edits',
        lambda content, *, file_path: content,
    )
    runner_cli._execute_gherkin_format(
        runner_cli.GherkinFormatCliRequest(
            root_path=tmp_path,
            paths=('a.feature',),
            check=True,
        ),
    )


def test_validate_feature_and_python_modules_extra_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    original_validate_gherkin = runner_cli._validate_gherkin_features
    root_path = tmp_path / 'tests'
    root_path.mkdir(parents=True)
    feature_file = (root_path / 'inside.feature').resolve()
    feature_file.write_text('Feature: X\n', encoding='utf-8')
    py_file = (root_path / 'inside.py').resolve()
    py_file.write_text('def test_x():\n    pass\n', encoding='utf-8')

    class _SwitchingPath:
        def __init__(self, path: Path, relative_text: str) -> None:
            self._path = path
            self._relative_text = relative_text
            self._relative_calls = 0

        def relative_to(self, _root_path: Path):
            self._relative_calls += 1
            if self._relative_calls == 1:
                return SimpleNamespace(as_posix=lambda: self._relative_text)
            raise ValueError('forced')

        def read_text(self, *, encoding: str = 'utf-8') -> str:
            return self._path.read_text(encoding=encoding)

        def __str__(self) -> str:
            return str(self._path)

    async def _empty_validate(_request):
        return ()

    monkeypatch.setattr(runner_cli, '_validate_gherkin_features', _empty_validate)
    runner_cli._execute_gherkin_validate(
        runner_cli.GherkinValidateCliRequest(
            context=_runtime_context(root_path),
            paths=('x.feature',),
        ),
    )
    monkeypatch.setattr(
        runner_cli,
        '_validate_gherkin_features',
        original_validate_gherkin,
    )

    monkeypatch.setattr(
        runner_cli,
        '_collect_gherkin_feature_paths',
        lambda *args, **kwargs: (_SwitchingPath(feature_file, 'inside.feature'),),
    )
    monkeypatch.setattr(
        runner_cli,
        'setup_engines',
        lambda *args, **kwargs: ((), {}),
    )

    class _Runner:
        def __init__(self, *_args, **_kwargs):
            return None

        async def start_session(self, *_args, **_kwargs):
            return None

        def find_engine(self, _path):
            if str(_path).endswith('.py'):
                return SimpleNamespace(name='pytest')
            return SimpleNamespace(name='gherkin')

        async def execute_operation(self, operation):
            del operation
            return SimpleNamespace(validation=SimpleNamespace(issues=()))

        async def finish_session(self):
            return None

        def _stop_log_capture(self):
            return None

    monkeypatch.setattr(runner_cli, 'Runner', _Runner)
    result = asyncio.run(
        runner_cli._validate_gherkin_features(
            runner_cli.GherkinValidateCliRequest(
                context=_runtime_context(root_path),
                paths=('inside.feature',),
            ),
        ),
    )
    assert len(result) == 1
    assert str(result[0][0]).endswith('inside.feature')
    assert result[0][1] == ()

    monkeypatch.setattr(
        runner_cli,
        '_collect_python_validation_paths',
        lambda *args, **kwargs: (),
    )
    result_py = asyncio.run(
        runner_cli._validate_pytest_modules(
            runner_cli.PytestValidateCliRequest(
                context=_runtime_context(root_path),
                paths=('outside.py',),
            ),
        ),
    )
    assert result_py == ()

    monkeypatch.setattr(
        runner_cli,
        '_collect_python_validation_paths',
        lambda *args, **kwargs: (_SwitchingPath(py_file, 'inside.py'),),
    )
    result_py_2 = asyncio.run(
        runner_cli._validate_pytest_modules(
            runner_cli.PytestValidateCliRequest(
                context=_runtime_context(root_path),
                paths=('inside.py',),
            ),
        ),
    )
    assert len(result_py_2) == 1
    assert str(result_py_2[0][0]).endswith('inside.py')
    assert result_py_2[0][1] == ()


def test_execute_pytest_validate_validated_message_and_manifest_explain_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def _validated(_request):
        return ((tmp_path / 'ok.py', ()),)

    monkeypatch.setattr(runner_cli, '_validate_pytest_modules', _validated)
    runner_cli._execute_pytest_validate(
        runner_cli.PytestValidateCliRequest(
            context=_runtime_context(tmp_path),
            paths=('ok.py',),
        ),
    )

    monkeypatch.setattr(runner_cli, 'load_cosecha_manifest', lambda *_: None)
    with pytest.raises(SystemExit, match='2'):
        runner_cli._execute_manifest_explain(
            runner_cli.ManifestExplainCliRequest(
                args=Namespace(),
                manifest_file=None,
                selection=runner_cli.CliSelection(),
                root_path=tmp_path,
            ),
        )


def test_knowledge_reset_already_absent_and_query_preset_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace = SimpleNamespace(workspace_root=tmp_path)
    execution_context = SimpleNamespace(knowledge_storage_root=tmp_path / '.cosecha')
    monkeypatch.setattr(
        runner_cli,
        '_resolve_workspace_context_for_path',
        lambda root_path: (workspace, execution_context),
    )
    monkeypatch.setattr(
        runner_cli,
        'resolve_knowledge_base_path',
        lambda *args, **kwargs: tmp_path / '.cosecha' / 'kb.db',
    )
    monkeypatch.setattr(
        runner_cli,
        '_delete_knowledge_base_files',
        lambda *args, **kwargs: (),
    )
    runner_cli._execute_knowledge_reset(
        runner_cli.KnowledgeResetCliRequest(root_path=tmp_path),
    )

    artifacts = runner_cli._failures_preset_items(
        [{'has_failures': False}, {'has_failures': True}],
        items_key='artifacts',
    )
    events = runner_cli._failures_preset_items(
        [{'event_type': 'node.retrying'}],
        items_key='events',
    )
    assert artifacts == [{'has_failures': True}]
    assert events == [{'event_type': 'node.retrying'}]
    runner_cli._print_inactive_decisions('Inactive', [])


def test_format_query_payload_branches_with_custom_coercion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {'tests': [{'status': 'passed'}], 'result_type': 'knowledge.tests'}

    calls = {'count': 0}

    def _coerce(value):
        calls['count'] += 1
        if calls['count'] == 1:
            return [{'status': 'passed'}]
        return None

    monkeypatch.setattr(runner_cli, '_coerce_query_items', _coerce)
    assert runner_cli._format_query_payload(
        payload,
        render_options=runner_cli.QueryRenderOptions(),
    ) == payload

    monkeypatch.setattr(runner_cli, '_coerce_query_items', lambda value: value if isinstance(value, list) else None)
    formatted = runner_cli._format_query_payload(
        payload,
        render_options=runner_cli.QueryRenderOptions(page_size=1),
    )
    assert formatted['page']['limit'] == 1


def test_doctor_knowledge_base_and_execute_doctor_context_lines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = runner_cli.DoctorCliRequest(
        args=Namespace(),
        manifest_file=None,
        selection=runner_cli.CliSelection(),
        root_path=tmp_path,
    )

    workspace = SimpleNamespace(workspace_root=tmp_path)
    execution_context = SimpleNamespace(
        knowledge_storage_root=tmp_path / '.cosecha',
        execution_root=tmp_path / 'exec',
    )
    monkeypatch.setattr(
        runner_cli,
        '_resolve_workspace_context_for_path',
        lambda root_path: (workspace, execution_context),
    )
    monkeypatch.setattr(
        runner_cli,
        'resolve_knowledge_base_path',
        lambda *args, **kwargs: tmp_path / '.cosecha' / 'kb.db',
    )

    class _KB:
        def snapshot(self):
            return SimpleNamespace(tests=(1,), definitions=(1,), registry_snapshots=(1,), resources=(1,))

        def query_session_artifacts(self, _query):
            return (
                SimpleNamespace(
                    session_id='s1',
                    trace_id=None,
                    plan_id=None,
                    has_failures=False,
                ),
            )

        def close(self):
            return None

    db_path = tmp_path / '.cosecha' / 'kb.db'
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_text('x', encoding='utf-8')
    monkeypatch.setattr(runner_cli, 'ReadOnlyPersistentKnowledgeBase', lambda _db_path: _KB())
    runner_cli._check_doctor_knowledge_base(request, issues=[])

    monkeypatch.setattr(runner_cli, '_check_doctor_manifest', lambda request, *, issues: None)
    monkeypatch.setattr(runner_cli, '_check_doctor_knowledge_base', lambda request, *, issues: None)
    monkeypatch.setattr(
        runner_cli,
        '_resolve_workspace_context_for_path',
        lambda root_path: (
            SimpleNamespace(workspace_root=tmp_path, fingerprint='fp'),
            SimpleNamespace(
                execution_root=tmp_path / 'exec',
                knowledge_storage_root=tmp_path / '.cosecha',
            ),
        ),
    )
    runner_cli._execute_doctor(request)


def test_execute_non_runtime_request_true_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {'value': False}
    monkeypatch.setattr(
        runner_cli,
        '_execute_manifest_show',
        lambda request: called.update(value=True),
    )
    handled = runner_cli._execute_non_runtime_request(
        runner_cli.ManifestShowCliRequest(manifest_file=None),
    )
    assert handled is True
    assert called['value'] is True


def test_normalize_cli_path_selector_root_prefixed_escape_branch(
    tmp_path: Path,
) -> None:
    root_path = (tmp_path / 'tests').resolve()
    root_path.mkdir(parents=True)
    normalized = runner_cli._normalize_cli_path_selector(
        'tests/../outside',
        root_path=root_path,
    )
    assert normalized == '../outside'


def test_normalize_cli_path_selector_returns_raw_relative_when_outside_root(
    tmp_path: Path,
) -> None:
    root_path = (tmp_path / 'tests').resolve()
    root_path.mkdir(parents=True)
    normalized = runner_cli._normalize_cli_path_selector(
        '../outside',
        root_path=root_path,
    )
    assert normalized == '../outside'


def test_collect_gherkin_missing_path_raises_explicit_error(tmp_path: Path) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir(parents=True)
    with pytest.raises(ValueError, match='Gherkin path does not exist'):
        runner_cli._collect_gherkin_feature_paths(
            ('missing.feature',),
            root_path=root_path,
        )


def test_apply_gherkin_format_edits_no_edit_branch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class _Formatter:
        def provide_document_formatting_edits(self, document):
            del document
            return ()

    class _Document:
        def __init__(self, *, uri: str, source: str, version: int):
            self.uri = uri
            self.source = source
            self.version = version

    monkeypatch.setitem(
        sys.modules,
        'cosecha.engine.gherkin.formatter',
        SimpleNamespace(
            GherkinDocumentFormattingEditProvider=_Formatter,
            PlainTextDocument=_Document,
        ),
    )
    content = 'Feature: Demo\n'
    assert (
        runner_cli._apply_gherkin_format_edits(
            content,
            file_path=tmp_path / 'demo.feature',
        )
        == content
    )


def test_validate_gherkin_empty_result_and_validated_message(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def _empty_collect(_request):
        return ()

    monkeypatch.setattr(
        runner_cli,
        '_collect_gherkin_feature_paths',
        lambda *args, **kwargs: (),
    )
    empty = asyncio.run(
        runner_cli._validate_gherkin_features(
            runner_cli.GherkinValidateCliRequest(
                context=_runtime_context(tmp_path),
                paths=('x.feature',),
            ),
        ),
    )
    assert empty == ()

    monkeypatch.setattr(runner_cli, '_validate_gherkin_features', _empty_collect)
    runner_cli._execute_gherkin_validate(
        runner_cli.GherkinValidateCliRequest(
            context=_runtime_context(tmp_path),
            paths=('x.feature',),
        ),
    )

    async def _single_ok(_request):
        return ((tmp_path / 'ok.feature', ()),)

    monkeypatch.setattr(runner_cli, '_validate_gherkin_features', _single_ok)
    runner_cli._execute_gherkin_validate(
        runner_cli.GherkinValidateCliRequest(
            context=_runtime_context(tmp_path),
            paths=('ok.feature',),
        ),
    )


def test_gherkin_pre_commit_remaining_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    feature = tmp_path / 'a.feature'
    feature.write_text('Feature: A\n', encoding='utf-8')

    monkeypatch.setattr(
        runner_cli,
        '_collect_gherkin_feature_paths',
        lambda *args, **kwargs: (feature,),
    )
    monkeypatch.setattr(
        runner_cli,
        '_apply_gherkin_format_edits',
        lambda content, *, file_path: content + '# changed\n',
    )

    async def _issues_empty(_request):
        return ((feature, ()),)

    monkeypatch.setattr(runner_cli, '_validate_gherkin_features', _issues_empty)
    with pytest.raises(SystemExit, match='1'):
        runner_cli._execute_gherkin_pre_commit(
            runner_cli.GherkinPreCommitCliRequest(
                context=_runtime_context(tmp_path),
                paths=('a.feature',),
                write=True,
            ),
        )

    monkeypatch.setattr(
        runner_cli,
        '_apply_gherkin_format_edits',
        lambda content, *, file_path: content,
    )
    runner_cli._execute_gherkin_pre_commit(
        runner_cli.GherkinPreCommitCliRequest(
            context=_runtime_context(tmp_path),
            paths=('a.feature',),
            write=True,
        ),
    )


def test_validate_pytest_removes_capture_handler_and_prints_validated(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir(parents=True)
    py_file = root_path / 'test_ok.py'
    py_file.write_text('def test_ok():\n    pass\n', encoding='utf-8')
    monkeypatch.setattr(
        runner_cli,
        '_collect_python_validation_paths',
        lambda *args, **kwargs: (py_file,),
    )
    monkeypatch.setattr(
        runner_cli,
        'setup_engines',
        lambda *args, **kwargs: ((), {}),
    )

    class _Runner:
        def __init__(self, *_args, **_kwargs):
            return None

        async def start_session(self, *_args, **_kwargs):
            return None

        def find_engine(self, _path):
            return SimpleNamespace(name='pytest')

        async def execute_operation(self, operation):
            del operation
            return SimpleNamespace(validation=SimpleNamespace(issues=()))

        async def finish_session(self):
            return None

        def _stop_log_capture(self):
            return None

    monkeypatch.setattr(runner_cli, 'Runner', _Runner)
    if runner_cli.capture_handler not in runner_cli.root_logger.handlers:
        runner_cli.root_logger.addHandler(runner_cli.capture_handler)
    result = asyncio.run(
        runner_cli._validate_pytest_modules(
            runner_cli.PytestValidateCliRequest(
                context=_runtime_context(root_path),
                paths=('test_ok.py',),
            ),
        ),
    )
    assert result == ((py_file, ()),)

    async def _single_ok(_request):
        return ((py_file, ()),)

    monkeypatch.setattr(runner_cli, '_validate_pytest_modules', _single_ok)
    runner_cli._execute_pytest_validate(
        runner_cli.PytestValidateCliRequest(
            context=_runtime_context(root_path),
            paths=('test_ok.py',),
        ),
    )


def test_execute_knowledge_reset_prints_reset_when_files_removed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace = SimpleNamespace(workspace_root=tmp_path)
    execution_context = SimpleNamespace(knowledge_storage_root=tmp_path / '.cosecha')
    monkeypatch.setattr(
        runner_cli,
        '_resolve_workspace_context_for_path',
        lambda root_path: (workspace, execution_context),
    )
    db_path = tmp_path / '.cosecha' / 'kb.db'
    monkeypatch.setattr(
        runner_cli,
        'resolve_knowledge_base_path',
        lambda *args, **kwargs: db_path,
    )
    monkeypatch.setattr(
        runner_cli,
        '_delete_knowledge_base_files',
        lambda *args, **kwargs: (db_path,),
    )
    runner_cli._execute_knowledge_reset(
        runner_cli.KnowledgeResetCliRequest(root_path=tmp_path),
    )
