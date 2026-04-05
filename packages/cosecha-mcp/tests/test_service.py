from __future__ import annotations

import os
import sys

from contextlib import contextmanager
from pathlib import Path

import pytest

from cosecha.core.knowledge_base import TestKnowledge as CoreTestKnowledge
from cosecha_mcp.service import CosechaMcpService
from cosecha_mcp.workspace import CosechaWorkspacePaths

OTHER_PYTHON = '/tmp/other-python'
PROJECT_ROOT = Path('/tmp/project')
PROJECT_TESTS_ROOT = PROJECT_ROOT / 'tests'
PROJECT_KB_PATH = PROJECT_TESTS_ROOT / '.cosecha' / 'kb.db'
RUNNER_TEST_LIMIT = 5
NO_SUBPROCESS_MESSAGE = 'query_tests should not use subprocess'


def _build_workspace(tmp_path: Path) -> CosechaWorkspacePaths:
    project_path = tmp_path / 'project'
    root_path = project_path / 'tests'
    kb_path = root_path / '.cosecha' / 'kb.db'
    root_path.mkdir(parents=True)
    kb_path.parent.mkdir(parents=True)
    kb_path.write_text('', encoding='utf-8')
    return CosechaWorkspacePaths(
        project_path=project_path,
        root_path=root_path,
        manifest_path=None,
        knowledge_base_path=kb_path,
    )


def test_service_discovers_workspace_import_paths(
    tmp_path: Path,
) -> None:
    workspace = _build_workspace(tmp_path)
    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    project_site_packages = (
        workspace.project_path
        / '.venv'
        / 'lib'
        / version_name
        / 'site-packages'
    )
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


def test_service_prefers_matching_workspace_python(
    tmp_path: Path,
) -> None:
    workspace = _build_workspace(tmp_path)
    candidate = tmp_path / '.venv' / 'bin' / 'python'
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
    workspace = _build_workspace(tmp_path)
    candidate = tmp_path / '.venv' / 'bin' / 'python'
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
    workspace = _build_workspace(tmp_path)
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
    workspace = _build_workspace(tmp_path)
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


@pytest.mark.asyncio
async def test_service_run_tests_requires_explicit_opt_in(
) -> None:
    service = CosechaMcpService()

    with pytest.raises(
        PermissionError,
        match='run_tests is disabled by default',
    ):
        await service.run_tests()


@pytest.mark.asyncio
async def test_service_run_tests_uses_subprocess_response_and_serializes_result(
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
    payload = await service.run_tests(
        paths=['tests/unit/example.feature'],
        selection_labels=['slow'],
        test_limit=RUNNER_TEST_LIMIT,
        selected_engines=['gherkin'],
        start_path=str(PROJECT_ROOT),
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


@pytest.mark.asyncio
async def test_query_tests_reads_persistent_knowledge_without_subprocess(
    tmp_path: Path,
) -> None:
    workspace = _build_workspace(tmp_path)
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

        async def _execute_runner_operation_in_subprocess(self, *args, **kwargs):
            raise AssertionError('query_tests should not use subprocess')

    service = TestableService()
    payload = await service.query_tests(
        engine_name='gherkin',
        start_path=str(workspace.project_path),
    )

    assert payload['result_type'] == 'knowledge.tests'
    assert payload['context']['source'] == 'persistent_knowledge_base'
    assert len(payload['tests']) == 1
