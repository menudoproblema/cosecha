from __future__ import annotations

import asyncio
import os
import sys
import threading

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.config import ConfigSnapshot
from cosecha.core.instrumentation import (
    COSECHA_COVERAGE_ACTIVE_ENV,
    COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
    COSECHA_RUNTIME_STATE_DIR_ENV,
    COSECHA_SHADOW_ROOT_ENV,
)
from cosecha.core.knowledge_base import (
    DefinitionKnowledge,
    PlanKnowledge,
    PersistentKnowledgeBase,
    ResourceKnowledge,
    SessionKnowledge,
    TestKnowledge as CoreTestKnowledge,
)
from cosecha.core.registry_knowledge import RegistryKnowledgeSnapshot
from cosecha.core.session_artifacts import (
    InstrumentationSummary,
    SessionArtifact,
    SessionArtifactPersistencePolicy,
    SessionReportSummary,
    SessionTelemetrySummary,
    SessionTimingSnapshot,
)
from cosecha_mcp.service import CosechaMcpService
from cosecha_mcp.workspace import CosechaWorkspacePaths


OTHER_PYTHON = '/tmp/other-python'
PROJECT_ROOT = Path('/tmp/project')
PROJECT_TESTS_ROOT = PROJECT_ROOT / 'tests'
PROJECT_KB_PATH = PROJECT_TESTS_ROOT / '.cosecha' / 'kb.db'
RUNNER_TEST_LIMIT = 5
NO_SUBPROCESS_MESSAGE = 'query_tests should not use subprocess'


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    payload: dict[str, object] = {}
    error: dict[str, BaseException] = {}

    def _run_in_thread() -> None:
        try:
            payload['result'] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover
            error['exception'] = exc

    thread = threading.Thread(target=_run_in_thread)
    thread.start()
    thread.join()

    if 'exception' in error:
        raise error['exception']

    return payload['result']


def _build_workspace(tmp_path: Path):
    project_path = tmp_path / 'project'
    tests_root = project_path / 'tests'
    knowledge_base_path = tests_root / '.cosecha' / 'kb.db'
    knowledge_base_path.parent.mkdir(parents=True, exist_ok=True)
    knowledge_base_path.write_text('', encoding='utf-8')
    workspace_paths = CosechaWorkspacePaths(
        project_path=project_path,
        root_path=tests_root,
        manifest_path=None,
        knowledge_base_path=knowledge_base_path,
    )
    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    return workspace_paths, SimpleNamespace(
        project_path=project_path,
        site_packages_path=(
            project_path / '.venv' / 'lib' / version_name / 'site-packages'
        ),
    )


def test_service_discovers_workspace_import_paths(tmp_path: Path) -> None:
    workspace, handle = _build_workspace(tmp_path)
    project_site_packages = handle.site_packages_path
    sibling_src = workspace.project_path.parent / 'shared-lib' / 'src'
    project_site_packages.mkdir(parents=True)
    sibling_src.mkdir(parents=True)

    service = CosechaMcpService()
    discovered_paths = service._discover_workspace_import_paths(workspace)

    assert workspace.project_path.resolve() in discovered_paths
    assert (
        workspace.project_path.joinpath('tests').resolve()
        in discovered_paths
    )
    assert project_site_packages.resolve() in discovered_paths
    assert sibling_src.resolve() in discovered_paths


def test_service_prefers_matching_workspace_python(tmp_path: Path) -> None:
    workspace, _handle = _build_workspace(tmp_path)
    candidate = workspace.project_path / '.venv' / 'bin' / 'python'
    candidate.parent.mkdir(parents=True)
    candidate.write_text('', encoding='utf-8')

    class TestableService(CosechaMcpService):
        def _discover_workspace_python_executables(self, current_workspace):
            assert current_workspace == workspace
            return (candidate,)

        def _read_python_version(self, executable):
            assert executable == candidate
            return f'{sys.version_info.major}.{sys.version_info.minor}'

    service = TestableService()

    assert (
        service._resolve_workspace_python_executable(workspace)
        == str(candidate)
    )


def test_service_rejects_workspace_python_version_mismatch(
    tmp_path: Path,
) -> None:
    workspace, _handle = _build_workspace(tmp_path)
    candidate = workspace.project_path / '.venv' / 'bin' / 'python'
    candidate.parent.mkdir(parents=True)
    candidate.write_text('', encoding='utf-8')

    class TestableService(CosechaMcpService):
        def _discover_workspace_python_executables(self, current_workspace):
            assert current_workspace == workspace
            return (candidate,)

        def _read_python_version(self, executable):
            assert executable == candidate
            return '3.11'

    service = TestableService()

    with pytest.raises(
        RuntimeError,
        match='none match the MCP interpreter ABI',
    ):
        service._resolve_workspace_python_executable(workspace)


def test_build_workspace_subprocess_env_is_minimal_outside_monorepo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace, _handle = _build_workspace(tmp_path)
    monkeypatch.setenv('PYTHONPATH', '/existing/path')

    class TestableService(CosechaMcpService):
        def _is_monorepo_checkout(self) -> bool:
            return False

    service = TestableService()
    env = service._build_workspace_subprocess_env(
        workspace,
        python_executable=OTHER_PYTHON,
    )

    pythonpath_entries = env['PYTHONPATH'].split(os.pathsep)
    assert '/existing/path' in pythonpath_entries
    assert str(workspace.project_path.resolve()) in pythonpath_entries
    assert str(workspace.root_path.resolve()) in pythonpath_entries
    assert not any(
        '/Users/uve/Proyectos/cosecha/packages/' in entry
        for entry in pythonpath_entries
    )


def test_build_workspace_subprocess_env_includes_repo_sources_in_monorepo_checkout(
    tmp_path: Path,
) -> None:
    workspace, _handle = _build_workspace(tmp_path)
    service = CosechaMcpService()

    env = service._build_workspace_subprocess_env(
        workspace,
        python_executable=OTHER_PYTHON,
    )

    pythonpath_entries = env['PYTHONPATH'].split(os.pathsep)
    assert any(
        entry.endswith('/packages/cosecha-mcp/src')
        for entry in pythonpath_entries
    )


def test_build_config_uses_workspace_and_execution_context_paths() -> None:
    workspace = CosechaWorkspacePaths(
        project_path=PROJECT_ROOT,
        root_path=PROJECT_TESTS_ROOT,
        manifest_path=None,
        knowledge_base_path=PROJECT_KB_PATH,
        workspace_root=PROJECT_ROOT,
        knowledge_anchor=PROJECT_TESTS_ROOT,
        execution_root=PROJECT_ROOT,
        workspace_fingerprint='workspace-fingerprint',
    )
    service = CosechaMcpService()

    config = service._build_config(workspace)

    assert config.workspace is not None
    assert config.execution_context is not None
    assert config.root_path == PROJECT_TESTS_ROOT.resolve()
    assert config.workspace_root_path == PROJECT_ROOT.resolve()
    assert config.execution_root_path == PROJECT_ROOT.resolve()
    assert config.knowledge_storage_root_path == PROJECT_KB_PATH.parent.resolve()
    assert (
        config.execution_context.workspace_fingerprint
        == 'workspace-fingerprint'
    )


def test_build_workspace_subprocess_env_strips_inherited_shadow_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = CosechaWorkspacePaths(
        project_path=PROJECT_ROOT,
        root_path=PROJECT_TESTS_ROOT,
        manifest_path=None,
        knowledge_base_path=PROJECT_KB_PATH,
    )
    for key, value in (
        (COSECHA_SHADOW_ROOT_ENV, '/tmp/shadow'),
        (COSECHA_RUNTIME_STATE_DIR_ENV, '/tmp/runtime-state'),
        (COSECHA_INSTRUMENTATION_METADATA_FILE_ENV, '/tmp/meta.json'),
        (COSECHA_COVERAGE_ACTIVE_ENV, '1'),
    ):
        monkeypatch.setenv(key, value)

    class TestableService(CosechaMcpService):
        def _is_monorepo_checkout(self) -> bool:
            return False

    service = TestableService()
    env = service._build_workspace_subprocess_env(
        workspace,
        python_executable=OTHER_PYTHON,
    )

    assert COSECHA_SHADOW_ROOT_ENV not in env
    assert COSECHA_RUNTIME_STATE_DIR_ENV not in env
    assert COSECHA_INSTRUMENTATION_METADATA_FILE_ENV not in env
    assert COSECHA_COVERAGE_ACTIVE_ENV not in env


def test_service_run_tests_requires_explicit_opt_in(
) -> None:
    service = CosechaMcpService()

    with pytest.raises(
        PermissionError,
        match='run_tests is disabled by default',
    ):
        _run_async(service.run_tests())


def test_service_run_tests_uses_subprocess_response_and_serializes_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = CosechaWorkspacePaths(
        project_path=PROJECT_ROOT,
        root_path=PROJECT_TESTS_ROOT,
        manifest_path=None,
        knowledge_base_path=PROJECT_KB_PATH,
    )
    monkeypatch.setenv('COSECHA_MCP_ENABLE_RUN_TESTS', '1')

    captured: dict[str, object] = {}

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            return workspace

        async def _execute_runner_operation_in_subprocess(
            self,
            workspace,
            *,
            operation,
            selected_engine_names=None,
        ):
            captured['paths'] = operation.paths
            captured['selection_labels'] = operation.selection_labels
            captured['selected_engine_names'] = selected_engine_names
            captured['test_limit'] = operation.test_limit
            return {
                'result': {
                    'has_failures': False,
                    'result_type': 'run.result',
                },
            }

        def describe_knowledge_base(self, *, start_path: str | None = None):
            return {'exists': True}

    service = TestableService()
    payload = _run_async(
        service.run_tests(
            paths=['tests/unit/example.feature'],
            selection_labels=['slow'],
            test_limit=RUNNER_TEST_LIMIT,
            selected_engines=['gherkin'],
            start_path=str(PROJECT_ROOT),
        ),
    )

    assert captured['paths'] == ('unit/example.feature',)
    assert captured['selection_labels'] == ('slow',)
    assert captured['selected_engine_names'] == {'gherkin'}
    assert captured['test_limit'] == RUNNER_TEST_LIMIT
    assert payload['has_failures'] is False
    assert payload['selected_engines'] == ['gherkin']
    assert payload['knowledge_base'] == {'exists': True}


def test_service_compacts_large_event_payloads() -> None:
    service = CosechaMcpService()
    payload = {
        'events': [
            {
                'event_type': 'log.chunk',
                'message': 'x' * 10_000,
                'metadata': {'sequence_number': index},
            }
            for index in range(200)
        ],
    }

    service._compact_list_response_field(payload, 'events')

    assert payload['truncated'] is True
    assert payload['events_returned_count'] < payload['events_total_count']
    assert payload['events_omitted_count'] > 0
    assert payload['next_after_sequence_number'] >= 0


def test_query_tests_reads_persistent_knowledge_without_subprocess(
    tmp_path: Path,
) -> None:
    workspace, _handle = _build_workspace(tmp_path)
    fake_tests = (
        CoreTestKnowledge(
            node_id='node-1',
            node_stable_id='stable-1',
            engine_name='gherkin',
            test_name='Example',
            test_path='unit/example.feature',
            session_id='session-1',
            plan_id='plan-1',
            trace_id='trace-1',
            status='passed',
            indexed_at=1.0,
        ),
    )

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            return workspace

        @contextmanager
        def _open_readonly_knowledge_base(self, current_workspace):
            assert current_workspace == workspace

            class FakeKnowledgeBase:
                def query_tests(self, query):
                    assert query.engine_name == 'gherkin'
                    return fake_tests

            yield FakeKnowledgeBase()

        async def _execute_runner_operation_in_subprocess(
            self,
            *args,
            **kwargs,
        ):
            raise AssertionError(NO_SUBPROCESS_MESSAGE)

    service = TestableService()
    payload = _run_async(
        service.query_tests(
            engine_name='gherkin',
            start_path=str(workspace.project_path),
        ),
    )

    assert payload['result_type'] == 'knowledge.tests'
    assert payload['context']['source'] == 'persistent_knowledge_base'
    assert len(payload['tests']) == 1


def test_describe_workspace_returns_resolved_workspace_payload() -> None:
    workspace = CosechaWorkspacePaths(
        project_path=PROJECT_ROOT,
        root_path=PROJECT_TESTS_ROOT,
        manifest_path=None,
        knowledge_base_path=PROJECT_KB_PATH,
    )

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

    assert TestableService().describe_workspace() == workspace.to_dict()


def test_describe_knowledge_base_reads_snapshot_and_latest_artifact(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / 'project'
    tests_root = project_path / 'tests'
    knowledge_base_path = project_path / '.cosecha' / 'kb.db'
    tests_root.mkdir(parents=True)
    knowledge_base_path.parent.mkdir(parents=True)
    knowledge_base_path.write_text('sqlite', encoding='utf-8')
    workspace = CosechaWorkspacePaths(
        project_path=project_path,
        root_path=tests_root,
        manifest_path=tests_root / 'cosecha.toml',
        knowledge_base_path=knowledge_base_path,
    )

    class _Artifact:
        def to_dict(self) -> dict[str, object]:
            return {'session_id': 'session-1'}

    class _Snapshot:
        definitions = ('def-1',)
        registry_snapshots = ('reg-1', 'reg-2')
        resources = ('resource-1',)
        tests = ('test-1', 'test-2')
        session = SessionKnowledge(
            root_path='/workspace/demo',
            workspace_fingerprint='workspace-1',
            concurrency=1,
            session_id='session-1',
            trace_id='trace-1',
            started_at=10.0,
        )
        latest_plan = PlanKnowledge(
            mode='strict',
            executable=True,
            node_count=2,
            issue_count=0,
            plan_id='plan-1',
            correlation_id='corr-1',
            session_id='session-1',
            trace_id='trace-1',
            analyzed_at=11.0,
        )

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

        def _read_knowledge_base_file_metadata(self, db_path: Path):
            assert db_path == knowledge_base_path
            return {'schema_version': 15}

        @contextmanager
        def _open_readonly_knowledge_base(self, current_workspace):
            assert current_workspace == workspace

            class _FakeKnowledgeBase:
                @staticmethod
                def snapshot():
                    return _Snapshot()

                @staticmethod
                def query_session_artifacts(_query):
                    return (_Artifact(),)

            yield _FakeKnowledgeBase()

    payload = TestableService().describe_knowledge_base()

    assert payload['exists'] is True
    assert payload['schema_version'] == 15
    assert payload['current_snapshot_counts'] == {
        'definitions': 1,
        'registry_snapshots': 2,
        'resources': 1,
        'tests': 2,
    }
    assert payload['latest_session']['session_id'] == 'session-1'
    assert payload['latest_plan']['plan_id'] == 'plan-1'
    assert payload['latest_session_artifact'] == {'session_id': 'session-1'}


def test_search_catalog_filters_entries_and_rejects_empty_query(
    tmp_path: Path,
) -> None:
    workspace = CosechaWorkspacePaths(
        project_path=tmp_path / 'project',
        root_path=tmp_path / 'project' / 'tests',
        manifest_path=None,
        knowledge_base_path=tmp_path / 'project' / '.cosecha' / 'kb.db',
    )
    definition = DefinitionKnowledge(
        engine_name='gherkin',
        file_path='steps/demo.py',
        definition_count=1,
        discovery_mode='ast',
    )
    resource = ResourceKnowledge(
        name='database connection',
        scope='session',
    )
    registry_snapshot = RegistryKnowledgeSnapshot(
        engine_name='gherkin',
        module_spec='tests.steps.demo',
        package_hash='abc',
        layout_key='gherkin',
        loader_schema_version='v1',
    )

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            del start_path
            return workspace

        @contextmanager
        def _open_readonly_knowledge_base(self, current_workspace):
            assert current_workspace == workspace

            class _FakeKnowledgeBase:
                @staticmethod
                def query_tests(_query):
                    return (
                        CoreTestKnowledge(
                            node_id='node-1',
                            node_stable_id='stable-1',
                            engine_name='gherkin',
                            test_name='Demo scenario',
                            test_path='features/demo.feature',
                            indexed_at=1.0,
                        ),
                    )

                @staticmethod
                def query_definitions(_query):
                    return (definition,)

                @staticmethod
                def query_resources(_query):
                    return (resource,)

                @staticmethod
                def query_registry_items(_query):
                    return (registry_snapshot,)

            yield _FakeKnowledgeBase()

    service = TestableService()
    payload = service.search_catalog('demo', limit=10)
    kinds = {match['kind'] for match in payload['matches']}
    assert kinds == {'definition', 'registry', 'test'}
    assert payload['matches_total_count'] >= 3

    with pytest.raises(ValueError, match='requires a non-empty query'):
        service.search_catalog('   ')


def test_parse_runner_subprocess_response_handles_error_cases() -> None:
    service = CosechaMcpService()
    workspace = CosechaWorkspacePaths(
        project_path=PROJECT_ROOT,
        root_path=PROJECT_TESTS_ROOT,
        manifest_path=None,
        knowledge_base_path=PROJECT_KB_PATH,
    )

    with pytest.raises(RuntimeError, match='worker process failed'):
        service._parse_runner_subprocess_response(
            workspace,
            python_executable='/usr/bin/python3',
            returncode=1,
            stdout=b'{}',
            stderr=b'boom',
        )

    with pytest.raises(RuntimeError, match='returned an empty payload'):
        service._parse_runner_subprocess_response(
            workspace,
            python_executable='/usr/bin/python3',
            returncode=0,
            stdout=b'',
            stderr=b'',
        )

    with pytest.raises(RuntimeError, match='returned invalid JSON'):
        service._parse_runner_subprocess_response(
            workspace,
            python_executable='/usr/bin/python3',
            returncode=0,
            stdout=b'{',
            stderr=b'',
        )

    with pytest.raises(RuntimeError, match='must be a JSON object'):
        service._parse_runner_subprocess_response(
            workspace,
            python_executable='/usr/bin/python3',
            returncode=0,
            stdout=b'[]',
            stderr=b'',
        )

def _build_session_artifact(
    *,
    session_id: str,
    recorded_at: float,
    coverage_payload: dict[str, object] | None,
) -> SessionArtifact:
    instrumentation_summaries: dict[str, InstrumentationSummary] = {}
    if coverage_payload is not None:
        instrumentation_summaries['coverage'] = InstrumentationSummary(
            instrumentation_name='coverage',
            summary_kind='coverage.py',
            payload=coverage_payload,
        )
    return SessionArtifact(
        session_id=session_id,
        trace_id=f'trace-{session_id}',
        root_path='/workspace/demo',
        plan_id=None,
        config_snapshot=ConfigSnapshot(
            root_path='/workspace/demo',
            output_mode='summary',
            output_detail='standard',
            capture_log=True,
            stop_on_error=False,
            concurrency=1,
            strict_step_ambiguity=False,
        ),
        capability_snapshots=(),
        plan_explanation=None,
        timing=SessionTimingSnapshot(),
        has_failures=False,
        report_summary=SessionReportSummary(
            total_tests=1,
            status_counts=(('passed', 1),),
            failure_kind_counts=(),
            engine_summaries=(),
            live_engine_snapshots=(),
            failed_examples=(),
            failed_files=(),
            instrumentation_summaries=instrumentation_summaries,
        ),
        telemetry_summary=SessionTelemetrySummary(
            span_count=0,
            distinct_span_names=0,
        ),
        persistence_policy=SessionArtifactPersistencePolicy(),
        recorded_at=recorded_at,
    )


def _coverage_workspace() -> CosechaWorkspacePaths:
    return CosechaWorkspacePaths(
        project_path=PROJECT_ROOT,
        root_path=PROJECT_TESTS_ROOT,
        manifest_path=None,
        knowledge_base_path=PROJECT_KB_PATH,
    )


def _describe_coverage_service(
    workspace: CosechaWorkspacePaths,
    *,
    artifacts: tuple[SessionArtifact, ...],
    session_from_snapshot: str | None = None,
) -> CosechaMcpService:
    class _FakeSession:
        def __init__(self, session_id: str) -> None:
            self.session_id = session_id

    class _FakeSnapshot:
        def __init__(self, session_id: str | None) -> None:
            self.session = (
                _FakeSession(session_id) if session_id is not None else None
            )

    class _FakeKnowledgeBase:
        def __init__(self, snapshot_session_id: str | None) -> None:
            self._snapshot_session_id = snapshot_session_id

        def snapshot(self) -> _FakeSnapshot:
            return _FakeSnapshot(self._snapshot_session_id)

        def query_session_artifacts(self, query):
            if query.session_id is None:
                return tuple(artifacts)
            return tuple(
                artifact
                for artifact in artifacts
                if artifact.session_id == query.session_id
            )

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            return workspace

        @contextmanager
        def _open_readonly_knowledge_base(self, current_workspace):
            assert current_workspace == workspace
            yield _FakeKnowledgeBase(session_from_snapshot)

    return TestableService()


def test_describe_session_coverage_returns_coverage_payload_for_latest_session(
) -> None:
    workspace = _coverage_workspace()
    artifact = _build_session_artifact(
        session_id='session-1',
        recorded_at=123.0,
        coverage_payload={
            'total_coverage': 87.5,
            'report_type': 'term',
            'measurement_scope': 'controller_process',
            'branch': False,
            'engine_names': ['pytest'],
            'source_targets': ['src/demo'],
            'includes_python_subprocesses': True,
            'includes_worker_processes': False,
        },
    )

    service = _describe_coverage_service(
        workspace,
        artifacts=(artifact,),
        session_from_snapshot='session-1',
    )

    payload = _run_async(
        service.describe_session_coverage(
            session_id='last',
            start_path=str(workspace.project_path),
        ),
    )

    assert payload['has_coverage'] is True
    assert payload['session_id'] == 'session-1'
    assert payload['total_coverage'] == 87.5
    assert payload['recorded_at'] == 123.0
    assert payload['coverage_summary']['instrumentation_name'] == 'coverage'
    assert (
        payload['coverage_summary']['payload']['source_targets']
        == ['src/demo']
    )


def test_describe_session_coverage_reports_missing_coverage() -> None:
    workspace = _coverage_workspace()
    artifact = _build_session_artifact(
        session_id='session-2',
        recorded_at=456.0,
        coverage_payload=None,
    )

    service = _describe_coverage_service(
        workspace,
        artifacts=(artifact,),
        session_from_snapshot='session-2',
    )

    payload = _run_async(
        service.describe_session_coverage(
            session_id='last',
            start_path=str(workspace.project_path),
        ),
    )

    assert payload['has_coverage'] is False
    assert payload['session_id'] == 'session-2'
    assert payload['total_coverage'] is None
    assert payload['reason'] == 'session has no coverage instrumentation'


def test_describe_session_coverage_handles_empty_knowledge_base() -> None:
    workspace = _coverage_workspace()

    service = _describe_coverage_service(
        workspace,
        artifacts=(),
        session_from_snapshot=None,
    )

    payload = _run_async(
        service.describe_session_coverage(
            session_id='last',
            start_path=str(workspace.project_path),
        ),
    )

    assert payload['has_coverage'] is False
    assert payload['session_id'] is None
    assert payload['reason'] == 'no session recorded yet'


def test_list_coverage_history_filters_sessions_without_coverage() -> None:
    workspace = _coverage_workspace()
    artifact_with_coverage = _build_session_artifact(
        session_id='session-1',
        recorded_at=100.0,
        coverage_payload={
            'total_coverage': 72.0,
            'report_type': 'term',
            'measurement_scope': 'controller_process',
            'branch': True,
            'engine_names': ['pytest'],
            'source_targets': ['src/demo'],
            'includes_python_subprocesses': True,
            'includes_worker_processes': False,
        },
    )
    artifact_without_coverage = _build_session_artifact(
        session_id='session-2',
        recorded_at=200.0,
        coverage_payload=None,
    )
    artifact_with_newer_coverage = _build_session_artifact(
        session_id='session-3',
        recorded_at=300.0,
        coverage_payload={
            'total_coverage': 88.4,
            'report_type': 'term-missing',
            'measurement_scope': 'controller_process',
            'branch': False,
            'engine_names': ['pytest'],
            'source_targets': ['src/demo', 'src/extra'],
            'includes_python_subprocesses': True,
            'includes_worker_processes': False,
        },
    )

    service = _describe_coverage_service(
        workspace,
        artifacts=(
            artifact_with_newer_coverage,
            artifact_without_coverage,
            artifact_with_coverage,
        ),
    )

    payload = _run_async(
        service.list_coverage_history(
            limit=5,
            start_path=str(workspace.project_path),
        ),
    )

    assert payload['entries_returned_count'] == 2
    session_ids = [entry['session_id'] for entry in payload['entries']]
    assert session_ids == ['session-3', 'session-1']
    assert payload['entries'][0]['total_coverage'] == 88.4
    assert payload['entries'][0]['branch'] is False
    assert payload['entries'][1]['total_coverage'] == 72.0
    assert payload['entries'][1]['branch'] is True


def test_describe_session_coverage_rejects_none_session_id() -> None:
    workspace = _coverage_workspace()

    service = _describe_coverage_service(
        workspace,
        artifacts=(),
        session_from_snapshot=None,
    )

    payload = _run_async(
        service.describe_session_coverage(
            session_id=None,
            start_path=str(workspace.project_path),
        ),
    )

    assert payload['has_coverage'] is False
    assert payload['session_id'] is None
    assert payload['coverage_summary'] is None
    assert payload['total_coverage'] is None
    assert 'list_coverage_history' in payload['reason']


def test_list_coverage_history_returns_entries_sorted_by_recorded_at_desc(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / 'project'
    tests_root = project_path / 'tests'
    kb_path = tests_root / '.cosecha' / 'kb.db'
    kb_path.parent.mkdir(parents=True)

    knowledge_base = PersistentKnowledgeBase(kb_path)
    try:
        def _store(session_id: str, recorded_at: float, total: float) -> None:
            knowledge_base.store_session_artifact(
                _build_session_artifact(
                    session_id=session_id,
                    recorded_at=recorded_at,
                    coverage_payload={
                        'total_coverage': total,
                        'report_type': 'term',
                        'measurement_scope': 'controller_process',
                        'branch': False,
                        'engine_names': ['pytest'],
                        'source_targets': ['src/demo'],
                        'includes_python_subprocesses': True,
                        'includes_worker_processes': False,
                    },
                ),
            )

        # Insertion order deliberately NOT matching recorded_at order.
        _store('session-middle', recorded_at=200.0, total=80.0)
        _store('session-newest', recorded_at=300.0, total=90.0)
        _store('session-oldest', recorded_at=100.0, total=70.0)
    finally:
        knowledge_base.close()

    workspace = CosechaWorkspacePaths(
        project_path=project_path,
        root_path=tests_root,
        manifest_path=None,
        knowledge_base_path=kb_path,
    )

    class TestableService(CosechaMcpService):
        def _resolve_workspace(self, *, start_path: str | None = None):
            return workspace

    service = TestableService()
    payload = _run_async(
        service.list_coverage_history(
            limit=10,
            start_path=str(project_path),
        ),
    )

    session_ids = [entry['session_id'] for entry in payload['entries']]
    totals = [entry['total_coverage'] for entry in payload['entries']]
    assert session_ids == [
        'session-newest',
        'session-middle',
        'session-oldest',
    ]
    assert totals == [90.0, 80.0, 70.0]
