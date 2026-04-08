from __future__ import annotations

import asyncio
import sqlite3
import stat
import sys

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.knowledge_base import (
    DefinitionKnowledge,
    TestKnowledge as CoreTestKnowledge,
)
from cosecha.core.operations import QueryCapabilitiesOperation
from cosecha.engine.gherkin.definition_knowledge import (
    build_gherkin_definition_record,
)
from cosecha_mcp import service as service_module
from cosecha_mcp.service import CosechaMcpService
from cosecha_mcp.workspace import CosechaWorkspacePaths


def _run_async(coro):
    return asyncio.run(coro)


def _build_workspace(tmp_path: Path) -> CosechaWorkspacePaths:
    project_path = (tmp_path / 'project').resolve()
    tests_root = (project_path / 'tests').resolve()
    knowledge_base_path = (project_path / '.cosecha' / 'kb.db').resolve()
    tests_root.mkdir(parents=True, exist_ok=True)
    knowledge_base_path.parent.mkdir(parents=True, exist_ok=True)
    return CosechaWorkspacePaths(
        project_path=project_path,
        root_path=tests_root,
        manifest_path=None,
        knowledge_base_path=knowledge_base_path,
    )


def test_init_uses_env_default_start_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('COSECHA_MCP_ROOT', '/tmp/from-env')

    service = CosechaMcpService()

    assert service._default_start_path == '/tmp/from-env'


def test_describe_knowledge_base_returns_payload_when_database_is_missing(
    tmp_path: Path,
) -> None:
    workspace = _build_workspace(tmp_path)

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

        def _read_knowledge_base_file_metadata(self, db_path: Path):
            assert db_path == workspace.knowledge_base_path
            return {'schema_version': None}

    payload = TestableService().describe_knowledge_base()

    assert payload['exists'] is False
    assert payload['schema_version'] is None
    assert 'latest_session_artifact' not in payload


def test_describe_path_freshness_and_basic_query_wrappers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _build_workspace(tmp_path)
    feature_path = workspace.root_path / 'features' / 'demo.feature'
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    feature_path.write_text('Feature: Demo\n', encoding='utf-8')

    test_entry = CoreTestKnowledge(
        node_id='node-1',
        node_stable_id='stable-1',
        engine_name='gherkin',
        test_name='Demo',
        test_path='features/demo.feature',
        status='passed',
        indexed_at=feature_path.stat().st_mtime + 10,
        content_hash='different-hash',
    )
    definition_entry = DefinitionKnowledge(
        engine_name='gherkin',
        file_path='features/demo.feature',
        definition_count=1,
        discovery_mode='ast',
        indexed_at=feature_path.stat().st_mtime + 10,
        descriptors=(
            build_gherkin_definition_record(
                source_line=3,
                function_name='step_demo',
                step_type='given',
                patterns=('a demo',),
            ),
        ),
        content_hash='different-hash',
    )

    captured: dict[str, object] = {}

    @contextmanager
    def _using_discovery_registry(_registry):
        yield

    class _FakeKnowledgeBase:
        @staticmethod
        def query_tests(query):
            captured['tests_query'] = query
            return (test_entry,)

        @staticmethod
        def query_definitions(query):
            captured['definitions_query'] = query
            return (definition_entry,)

        @staticmethod
        def query_registry_items(query):
            captured['registry_query'] = query
            return ()

        @staticmethod
        def query_resources(query):
            captured['resources_query'] = query
            return ()

        @staticmethod
        def query_session_artifacts(query):
            captured['artifacts_query'] = query
            return ()

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

        @contextmanager
        def _open_readonly_knowledge_base(self, current_workspace):
            assert current_workspace == workspace
            yield _FakeKnowledgeBase()

        def _resolve_session_id(
            self,
            current_workspace,
            *,
            requested_session_id,
        ):
            assert current_workspace == workspace
            return 'session-1' if requested_session_id == 'last' else None

    monkeypatch.setattr(
        service_module,
        'create_loaded_discovery_registry',
        lambda: object(),
    )
    monkeypatch.setattr(
        service_module,
        'using_discovery_registry',
        _using_discovery_registry,
    )

    service = TestableService()
    freshness = service.describe_path_freshness(path='features', limit=10)
    definitions = _run_async(service.query_definitions(step_text='demo'))
    registry = _run_async(service.query_registry_items(engine_name='gherkin'))
    resources = _run_async(service.query_resources(scope='session'))
    artifacts = _run_async(service.read_session_artifacts(session_id='last'))

    assert freshness['scope_path'] == 'features'
    assert freshness['tests'][0]['freshness'] == 'stale'
    assert 'content_hash_mismatch' in freshness['tests'][0]['stale_reasons']
    assert definitions['result_type'] == 'knowledge.definitions'
    assert registry['result_type'] == 'knowledge.registry_items'
    assert resources['result_type'] == 'knowledge.resources'
    assert artifacts['result_type'] == 'knowledge.session_artifacts'
    assert captured['tests_query'].engine_name is None
    assert captured['artifacts_query'].session_id == 'session-1'


def test_list_recent_sessions_delegates_to_read_session_artifacts() -> None:
    captured: dict[str, object] = {}

    class TestableService(CosechaMcpService):
        async def read_session_artifacts(self, **kwargs):
            captured['kwargs'] = kwargs
            return {'ok': True}

    result = _run_async(TestableService().list_recent_sessions(limit=3))

    assert result == {'ok': True}
    assert captured['kwargs']['session_id'] is None
    assert captured['kwargs']['limit'] == 3


def test_describe_session_coverage_handles_missing_artifact(
    tmp_path: Path,
) -> None:
    workspace = _build_workspace(tmp_path)

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

        def _resolve_session_id(
            self,
            current_workspace,
            *,
            requested_session_id,
        ):
            assert current_workspace == workspace
            assert requested_session_id == 'last'
            return 'session-missing'

        @contextmanager
        def _open_readonly_knowledge_base(self, current_workspace):
            assert current_workspace == workspace
            yield SimpleNamespace(query_session_artifacts=lambda _query: ())

    payload = _run_async(TestableService().describe_session_coverage())

    assert payload['has_coverage'] is False
    assert payload['reason'] == 'session artifact not found for session-missing'


def test_extract_coverage_summary_returns_none_without_report_summary() -> None:
    assert CosechaMcpService._extract_coverage_summary(SimpleNamespace()) is None
    assert (
        CosechaMcpService._extract_coverage_summary(
            SimpleNamespace(report_summary=SimpleNamespace()),
        )
        is None
    )


def test_inspect_test_plan_and_execution_timeline_wrappers(
    tmp_path: Path,
) -> None:
    workspace = _build_workspace(tmp_path)
    captured: dict[str, object] = {}

    class _FakeKnowledgeBase:
        @staticmethod
        def query_domain_events(query):
            captured['timeline_query'] = query
            return ()

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

        async def _execute_runner_operation_in_subprocess(
            self,
            current_workspace,
            *,
            operation,
            selected_engine_names=None,
        ):
            captured['operation'] = operation
            captured['selected_engine_names'] = selected_engine_names
            assert current_workspace == workspace
            return {'result': {'result_type': 'plan.analysis'}}

        def _resolve_session_id(
            self,
            current_workspace,
            *,
            requested_session_id,
        ):
            assert current_workspace == workspace
            assert requested_session_id == 'last'
            return 'session-1'

        @contextmanager
        def _open_readonly_knowledge_base(self, current_workspace):
            assert current_workspace == workspace
            yield _FakeKnowledgeBase()

        def _serialize_operation_result(self, current_workspace, result):
            assert current_workspace == workspace
            if hasattr(result, 'to_dict'):
                return {'result_type': result.to_dict()['result_type']}
            return {'result_type': result['result_type']}

    service = TestableService()
    plan = _run_async(
        service.inspect_test_plan(
            test_path='tests/features/a.feature',
            paths=['tests/features/b.feature'],
            selection_labels=['slow'],
            test_limit=4,
            mode='strict',
        ),
    )
    timeline = _run_async(
        service.get_execution_timeline(
            session_id='last',
            plan_id='plan-1',
            node_stable_id='node-1',
            event_type='test.finished',
            limit=7,
        ),
    )

    assert plan['result_type'] == 'plan.analysis'
    assert captured['operation'].paths == (
        'features/b.feature',
        'features/a.feature',
    )
    assert timeline['result_type'] == 'knowledge.events'
    assert captured['timeline_query'].session_id == 'session-1'
    assert captured['timeline_query'].event_type == 'test.finished'


def test_list_test_execution_history_filters_and_limits_results(
) -> None:
    workspace = CosechaWorkspacePaths(
        project_path=Path('/tmp/project'),
        root_path=Path('/tmp/project/tests'),
        manifest_path=None,
        knowledge_base_path=Path('/tmp/project/.cosecha/kb.db'),
    )
    events = (
        SimpleNamespace(
            duration=1.0,
            engine_name='gherkin',
            error_code=None,
            failure_kind=None,
            node_id='node-1',
            node_stable_id='stable-1',
            status='passed',
            test_name='One',
            test_path='features/one.feature',
            timestamp=11.0,
            metadata=SimpleNamespace(
                plan_id='plan-1',
                session_id='session-1',
                trace_id='trace-1',
            ),
        ),
        SimpleNamespace(
            duration=2.0,
            engine_name='pytest',
            error_code='E',
            failure_kind='assertion',
            node_id='node-2',
            node_stable_id='stable-2',
            status='failed',
            test_name='Two',
            test_path='features/two.feature',
            timestamp=12.0,
            metadata=SimpleNamespace(
                plan_id='plan-2',
                session_id='session-1',
                trace_id='trace-2',
            ),
        ),
    )

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

        def _resolve_session_id(
            self,
            current_workspace,
            *,
            requested_session_id,
        ):
            assert current_workspace == workspace
            assert requested_session_id == 'last'
            return 'session-1'

        def _load_recent_test_finished_events(self, **_kwargs):
            return events

    payload = TestableService().list_test_execution_history(
        session_id='last',
        engine_name='gherkin',
        status='passed',
        limit=1,
    )

    assert payload['history_total_count'] == 1
    assert payload['history_returned_count'] == 1
    assert payload['history'][0]['test_name'] == 'One'


def test_list_engines_and_refresh_knowledge_base_wrappers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _build_workspace(tmp_path)
    stale_file = workspace.knowledge_base_path.with_suffix('.db-shm')
    stale_file.write_text('', encoding='utf-8')

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

        async def _execute_runner_operation_in_subprocess(
            self,
            current_workspace,
            *,
            operation,
            selected_engine_names=None,
        ):
            assert current_workspace == workspace
            if isinstance(operation, QueryCapabilitiesOperation):
                assert selected_engine_names == {'gherkin'}
                return {
                    'result': {'result_type': 'query.capabilities'},
                    'engine_names': ['pytest', 7, 'gherkin'],
                }
            return {'result': {'result_type': 'plan.analysis'}}

        def _serialize_operation_result(self, current_workspace, result):
            assert current_workspace == workspace
            return dict(result)

        def describe_knowledge_base(self, *, start_path: str | None = None):
            del start_path
            return {'exists': True}

    monkeypatch.setattr(
        service_module,
        'iter_knowledge_base_file_paths',
        lambda _path: (
            stale_file,
            workspace.knowledge_base_path.with_suffix('.db-wal'),
        ),
    )

    service = TestableService()
    capabilities = _run_async(
        service.list_engines_and_capabilities(selected_engines=['gherkin']),
    )
    refresh = _run_async(
        service.refresh_knowledge_base(
            rebuild=True,
            paths=['tests/features/demo.feature'],
            selection_labels=['smoke'],
            test_limit=2,
        ),
    )

    assert capabilities['engine_names'] == ['7', 'gherkin', 'pytest']
    assert refresh['result_type'] == 'plan.analysis'
    assert refresh['knowledge_base'] == {'exists': True}
    assert refresh['rebuild'] is True
    assert stale_file.exists() is False


def test_internal_builder_and_subprocess_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _build_workspace(tmp_path)
    captured: dict[str, object] = {}

    class _FakeRunnerResult:
        def __init__(
            self,
            config,
            engines,
            hooks,
            *,
            runtime_provider,
        ) -> None:
            captured['runner_args'] = (config, engines, hooks, runtime_provider)

    @contextmanager
    def _using_discovery_registry(_registry):
        yield

    monkeypatch.setattr(
        service_module,
        'create_loaded_discovery_registry',
        lambda: object(),
    )
    monkeypatch.setattr(
        service_module,
        'using_discovery_registry',
        _using_discovery_registry,
    )
    monkeypatch.setattr(
        service_module,
        'setup_engines',
        lambda _config, **_kwargs: (['hook'], {'gherkin': object()}),
    )
    monkeypatch.setattr(service_module, 'Runner', _FakeRunnerResult)

    service = CosechaMcpService(default_start_path='/tmp/default')
    runner = service._build_runner_for_selection(
        workspace,
        selected_engine_names={'gherkin'},
    )
    assert isinstance(runner, _FakeRunnerResult)

    class _BuildRunnerService(CosechaMcpService):
        def _build_runner_for_selection(
            self,
            current_workspace,
            *,
            selected_engine_names,
        ):
            captured['build_runner'] = (
                current_workspace,
                selected_engine_names,
            )
            return object()

    _BuildRunnerService()._build_runner(workspace)
    assert captured['build_runner'][1] is None

    monkeypatch.setattr(
        service_module,
        'resolve_cosecha_workspace',
        lambda start_path: captured.setdefault('start_path', start_path) or workspace,
    )
    service._resolve_workspace(start_path='/tmp/start')
    assert captured['start_path'] == '/tmp/start'

    closed: list[bool] = []

    class _FakeReadOnlyKnowledgeBase:
        def __init__(self, db_path: Path) -> None:
            assert db_path == workspace.knowledge_base_path

        def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr(
        service_module,
        'ReadOnlyPersistentKnowledgeBase',
        _FakeReadOnlyKnowledgeBase,
    )
    with service._open_readonly_knowledge_base(workspace):
        pass
    assert closed == [True]


def test_resolve_session_id_execute_operation_and_parse_success(
    tmp_path: Path,
) -> None:
    workspace = _build_workspace(tmp_path)
    service = CosechaMcpService()

    class _FakeKnowledgeBase:
        @staticmethod
        def snapshot():
            return SimpleNamespace(session=SimpleNamespace(session_id='latest'))

    class TestableService(CosechaMcpService):
        @contextmanager
        def _open_readonly_knowledge_base(self, current_workspace):
            assert current_workspace == workspace
            yield _FakeKnowledgeBase()

    testable = TestableService()
    assert (
        testable._resolve_session_id(workspace, requested_session_id='manual')
        == 'manual'
    )
    assert (
        testable._resolve_session_id(workspace, requested_session_id=None)
        is None
    )
    assert (
        testable._resolve_session_id(workspace, requested_session_id='last')
        == 'latest'
    )

    class _FakeOperation:
        @staticmethod
        def to_dict():
            return {'operation_type': 'query.capabilities'}

    captured: dict[str, object] = {}

    class AsyncTestableService(CosechaMcpService):
        def _resolve_workspace_python_executable(self, current_workspace):
            assert current_workspace == workspace
            return '/usr/bin/python3'

        async def _run_runner_subprocess_request(
            self,
            current_workspace,
            *,
            python_executable,
            request_payload,
        ):
            assert current_workspace == workspace
            captured['python_executable'] = python_executable
            captured['request_payload'] = request_payload
            return {'result': {'ok': True}}

    async_service = AsyncTestableService()
    result = _run_async(
        async_service._execute_runner_operation_in_subprocess(
            workspace,
            operation=_FakeOperation(),
            selected_engine_names={'pytest', 'gherkin'},
        ),
    )

    assert result == {'result': {'ok': True}}
    assert captured['python_executable'] == '/usr/bin/python3'
    assert captured['request_payload']['selected_engine_names'] == [
        'gherkin',
        'pytest',
    ]
    assert captured['request_payload']['start_path'] == str(workspace.project_path)

    parsed = service._parse_runner_subprocess_response(
        workspace,
        python_executable='/usr/bin/python3',
        returncode=0,
        stdout=b'{"ok": true}',
        stderr=b'',
    )
    assert parsed == {'ok': True}


def test_run_runner_subprocess_request_builds_child_process_invocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _build_workspace(tmp_path)
    captured: dict[str, object] = {}

    class _FakeProcess:
        returncode = 0

        async def communicate(self, payload: bytes):
            captured['stdin_payload'] = payload
            return b'{"ok": true}', b''

    async def _create_subprocess_exec(*args, **kwargs):
        captured['args'] = args
        captured['kwargs'] = kwargs
        return _FakeProcess()

    class TestableService(CosechaMcpService):
        def _build_workspace_subprocess_env(
            self,
            current_workspace,
            *,
            python_executable: str,
        ) -> dict[str, str]:
            assert current_workspace == workspace
            assert python_executable == '/usr/bin/python3'
            return {'PYTHONPATH': '/tmp/path'}

        def _parse_runner_subprocess_response(self, *_args, **_kwargs):
            return {'ok': True}

    monkeypatch.setattr(
        service_module.asyncio,
        'create_subprocess_exec',
        _create_subprocess_exec,
    )

    result = _run_async(
        TestableService()._run_runner_subprocess_request(
            workspace,
            python_executable='/usr/bin/python3',
            request_payload={'hello': 'world'},
        ),
    )

    assert result == {'ok': True}
    assert captured['args'][:3] == (
        '/usr/bin/python3',
        '-m',
        'cosecha_mcp.worker',
    )
    assert captured['kwargs']['cwd'] == str(workspace.project_path)
    assert captured['stdin_payload'] == b'{"hello": "world"}'


def test_parse_runner_subprocess_response_appends_stderr_details_on_errors(
) -> None:
    service = CosechaMcpService()
    workspace = CosechaWorkspacePaths(
        project_path=Path('/tmp/project'),
        root_path=Path('/tmp/project/tests'),
        manifest_path=None,
        knowledge_base_path=Path('/tmp/project/.cosecha/kb.db'),
    )

    with pytest.raises(RuntimeError, match='stderr'):
        service._parse_runner_subprocess_response(
            workspace,
            python_executable='/usr/bin/python3',
            returncode=0,
            stdout=b'',
            stderr=b'boom',
        )
    with pytest.raises(RuntimeError, match='stderr'):
        service._parse_runner_subprocess_response(
            workspace,
            python_executable='/usr/bin/python3',
            returncode=0,
            stdout=b'{',
            stderr=b'broken-json',
        )


def test_compaction_helpers_cover_analysis_and_event_metadata_shapes() -> None:
    service = CosechaMcpService()

    payload = {
        'analysis': {
            'issues': [{'id': 1}],
            'node_semantics': [{'id': 2}],
            'plan': [{'id': 3}],
        },
    }
    service._compact_known_response_fields(payload)
    assert payload['analysis']['issues_total_count'] == 1
    assert payload['analysis']['plan_total_count'] == 1

    events_payload = {'events': ['event']}
    service._compact_list_response_field(
        events_payload,
        'events',
        total_count=2,
    )
    assert events_payload['events_omitted_count'] == 1
    assert 'next_after_sequence_number' not in events_payload

    events_payload = {'events': [{'metadata': 'invalid'}]}
    service._compact_list_response_field(
        events_payload,
        'events',
        total_count=2,
    )
    assert events_payload['events_omitted_count'] == 1
    assert 'next_after_sequence_number' not in events_payload

    compacted = service._compact_json_value(list(range(30)))
    assert compacted[-1] == {'_truncated_item_count': 18}

    history_payload = {'history': ['record']}
    service._compact_list_response_field(
        history_payload,
        'history',
        total_count=2,
    )
    assert history_payload['history_omitted_count'] == 1


def test_metadata_filtering_scope_and_freshness_builders(
    tmp_path: Path,
) -> None:
    service = CosechaMcpService()
    workspace = _build_workspace(tmp_path)
    existing_path = workspace.root_path / 'features' / 'a.feature'
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_text('Feature: A\n', encoding='utf-8')

    metadata_for_missing = service._read_knowledge_base_file_metadata(
        workspace.knowledge_base_path,
    )
    assert metadata_for_missing['domain_event_count'] == 0

    matches = service._filter_search_matches(
        kind='test',
        entries=(
            {'name': 'alpha'},
            {'name': 'beta'},
        ),
        query='alp',
        label_builder=lambda payload: payload['name'],
    )
    assert [match['label'] for match in matches] == ['alpha']

    assert service._normalize_workspace_reference(
        workspace=workspace,
        raw_path=str(existing_path),
    ) == 'features/a.feature'
    assert service._normalize_workspace_reference(
        workspace=workspace,
        raw_path='features/a.feature',
    ) == 'features/a.feature'

    assert service._matches_scope(
        candidate_path='features/a.feature',
        scope_path='features',
        include_children=True,
    ) is True
    assert service._matches_scope(
        candidate_path='features',
        scope_path='features',
        include_children=False,
    ) is True
    assert service._matches_scope(
        candidate_path='other/a.feature',
        scope_path='features',
        include_children=False,
    ) is False

    entries = (
        SimpleNamespace(
            path='features/a.feature',
            indexed_at=existing_path.stat().st_mtime - 1,
            invalidated_at=existing_path.stat().st_mtime + 1,
            content_hash='bad',
        ),
        SimpleNamespace(
            path='features/missing.feature',
            indexed_at=None,
            invalidated_at=None,
            content_hash=None,
        ),
    )
    reports = service._build_freshness_reports(
        workspace=workspace,
        entries=entries,
        path_getter=lambda entry: entry.path,
        indexed_at_getter=lambda entry: entry.indexed_at,
        invalidated_at_getter=lambda entry: entry.invalidated_at,
        content_hash_getter=lambda entry: entry.content_hash,
        metadata_builder=lambda grouped: {'group_size': len(grouped)},
        scope_path='features',
        include_children=True,
    )

    assert len(reports) == 2
    assert reports[0]['freshness'] == 'stale'
    assert 'invalidated' in reports[0]['stale_reasons']
    assert 'content_hash_mismatch' in reports[0]['stale_reasons']
    assert 'missing_on_disk' in reports[1]['stale_reasons']

    assert service._build_freshness_reports(
        workspace=workspace,
        entries=(SimpleNamespace(path='outside/x.feature'),),
        path_getter=lambda entry: entry.path,
        indexed_at_getter=lambda _entry: None,
        invalidated_at_getter=lambda _entry: None,
        content_hash_getter=lambda _entry: None,
        metadata_builder=lambda grouped: {'group_size': len(grouped)},
        scope_path='features',
        include_children=False,
    ) == []

    test_metadata = service._build_test_freshness_metadata(
        [
            CoreTestKnowledge(
                node_id='n-1',
                node_stable_id='s-1',
                engine_name='gherkin',
                test_name='A',
                test_path='features/a.feature',
                status=None,
            ),
            CoreTestKnowledge(
                node_id='n-2',
                node_stable_id='s-2',
                engine_name='gherkin',
                test_name='B',
                test_path='features/b.feature',
                status='passed',
            ),
        ],
    )
    assert test_metadata['status_counts'] == {'passed': 1, 'unknown': 1}

    definition_metadata = service._build_definition_freshness_metadata(
        [
            DefinitionKnowledge(
                engine_name='gherkin',
                file_path='steps/demo.py',
                definition_count=1,
                discovery_mode='ast',
                descriptors=(
                    build_gherkin_definition_record(
                        source_line=5,
                        function_name='step_demo',
                        step_type='given',
                        patterns=('a demo',),
                    ),
                ),
            ),
        ],
    )
    assert definition_metadata['definition_count'] == 1
    assert definition_metadata['payload_kinds'] == ['gherkin.step']
    assert len(service._build_file_hash(existing_path)) == 64


def test_read_knowledge_base_file_metadata_reads_existing_database(
    tmp_path: Path,
) -> None:
    service = CosechaMcpService()
    db_path = (tmp_path / 'kb.db').resolve()
    connection = sqlite3.connect(db_path)
    try:
        connection.execute('CREATE TABLE meta (key TEXT, value TEXT)')
        connection.execute(
            'CREATE TABLE domain_event_log (sequence_number INTEGER, timestamp REAL)',
        )
        connection.execute(
            'INSERT INTO meta (key, value) VALUES (?, ?)',
            ('schema_version', '42'),
        )
        connection.executemany(
            'INSERT INTO domain_event_log (sequence_number, timestamp) VALUES (?, ?)',
            [(1, 10.0), (3, 20.0)],
        )
        connection.commit()
    finally:
        connection.close()

    metadata = service._read_knowledge_base_file_metadata(db_path)

    assert metadata['schema_version'] == '42'
    assert metadata['domain_event_count'] == 2
    assert metadata['latest_event_sequence_number'] == 3
    assert metadata['latest_event_timestamp'] == 20.0


def test_event_loading_runner_closing_and_environment_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _build_workspace(tmp_path)
    captured: dict[str, object] = {}

    class _FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _FakeConnection:
        def __init__(self) -> None:
            self.closed = False

        def execute(self, sql: str, params: tuple[object, ...]):
            captured['sql'] = sql
            captured['params'] = params
            return _FakeCursor(rows=[('{"id": 1}',)])

        def close(self) -> None:
            self.closed = True
            captured['closed'] = True

    monkeypatch.setattr(
        service_module.sqlite3,
        'connect',
        lambda *_args, **_kwargs: _FakeConnection(),
    )
    monkeypatch.setattr(
        service_module,
        'decode_json_dict',
        lambda payload: {'decoded': payload},
    )
    monkeypatch.setattr(
        service_module,
        'deserialize_domain_event',
        lambda payload: SimpleNamespace(payload=payload),
    )

    service = CosechaMcpService()
    events = service._load_recent_test_finished_events(
        workspace=workspace,
        session_id='session-1',
        limit=2,
    )

    assert len(events) == 1
    assert events[0].payload['decoded'] == '{"id": 1}'
    assert 'session_id = ?' in captured['sql']
    assert captured['params'][-1] == 2
    assert captured['closed'] is True

    closed: list[bool] = []
    service._close_runner(
        SimpleNamespace(_knowledge_base=SimpleNamespace(close=lambda: closed.append(True))),
    )
    service._close_runner(SimpleNamespace())
    assert closed == [True]

    missing_workspace = CosechaWorkspacePaths(
        project_path=tmp_path / 'missing-project',
        root_path=tmp_path / 'missing-project' / 'tests',
        manifest_path=None,
        knowledge_base_path=tmp_path / 'missing-project' / '.cosecha' / 'kb.db',
    )
    assert service._discover_workspace_site_packages(missing_workspace) == ()
    assert service._discover_workspace_python_executables(missing_workspace) == ()


def test_list_test_execution_history_skips_non_matching_events() -> None:
    workspace = CosechaWorkspacePaths(
        project_path=Path('/tmp/project'),
        root_path=Path('/tmp/project/tests'),
        manifest_path=None,
        knowledge_base_path=Path('/tmp/project/.cosecha/kb.db'),
    )
    matching = SimpleNamespace(
        duration=1.0,
        engine_name='gherkin',
        error_code=None,
        failure_kind=None,
        node_id='node-1',
        node_stable_id='stable-1',
        status='passed',
        test_name='One',
        test_path='features/one.feature',
        timestamp=11.0,
        metadata=SimpleNamespace(
            plan_id='plan-1',
            session_id='session-1',
            trace_id='trace-1',
        ),
    )
    by_engine = SimpleNamespace(**(matching.__dict__ | {'engine_name': 'pytest'}))
    by_path = SimpleNamespace(**(matching.__dict__ | {'test_path': 'other.feature'}))
    by_status = SimpleNamespace(**(matching.__dict__ | {'status': 'failed'}))

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

        def _resolve_session_id(self, *_args, **_kwargs):
            return None

        def _load_recent_test_finished_events(self, **_kwargs):
            return (by_engine, by_path, by_status, matching)

    payload = TestableService().list_test_execution_history(
        engine_name='gherkin',
        test_path='features/one.feature',
        status='passed',
        limit=5,
    )

    assert payload['history_returned_count'] == 1
    assert payload['history'][0]['engine_name'] == 'gherkin'


def test_python_discovery_read_version_and_repo_path_utilities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _build_workspace(tmp_path)
    executable = workspace.project_path / '.venv' / 'bin' / 'python'
    executable.parent.mkdir(parents=True, exist_ok=True)
    executable.write_text('#!/bin/sh\n', encoding='utf-8')
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)

    service = CosechaMcpService()
    discovered = service._discover_workspace_python_executables(workspace)
    assert executable.resolve() in discovered

    class _NoCandidatesService(CosechaMcpService):
        def _discover_workspace_python_executables(self, _workspace):
            return ()

    assert (
        _NoCandidatesService()._resolve_workspace_python_executable(workspace)
        == sys.executable
    )

    monkeypatch.setattr(
        service_module.subprocess,
        'run',
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout='3.12\n',
        ),
    )
    assert service._read_python_version(executable) == '3.12'

    monkeypatch.setattr(
        service_module.subprocess,
        'run',
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout='',
        ),
    )
    assert service._read_python_version(executable) is None

    class _EnvService(CosechaMcpService):
        def _discover_workspace_source_paths(self, _workspace):
            return (Path('/workspace/src'),)

        def _is_monorepo_checkout(self) -> bool:
            return False

        def _discover_workspace_site_packages(self, _workspace):
            return (Path('/workspace/site-packages'),)

    env = _EnvService()._build_workspace_subprocess_env(
        workspace,
        python_executable=sys.executable,
    )
    assert '/workspace/src' in env['PYTHONPATH']
    assert '/workspace/site-packages' in env['PYTHONPATH']

    fake_repo = tmp_path / 'fake-repo'
    fake_service_path = fake_repo / 'a' / 'b' / 'c' / 'd' / 'service.py'
    fake_service_path.parent.mkdir(parents=True, exist_ok=True)
    fake_service_path.write_text('', encoding='utf-8')
    monkeypatch.setattr(service_module, '__file__', str(fake_service_path))

    assert service._discover_repo_source_paths() == ()

    packages_root = fake_repo / 'packages'
    (packages_root / 'pkg-a' / 'src').mkdir(parents=True, exist_ok=True)
    (packages_root / 'not-a-dir.txt').write_text('', encoding='utf-8')
    repo_paths = service._discover_repo_source_paths()
    assert (packages_root / 'pkg-a' / 'src').resolve() in repo_paths

    deduped = service._dedupe_paths(
        [Path('/tmp/a'), Path('/tmp/a'), Path('/tmp/b')],
    )
    assert deduped == (Path('/tmp/a').resolve(), Path('/tmp/b').resolve())
