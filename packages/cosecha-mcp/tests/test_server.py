from __future__ import annotations

import asyncio

from types import SimpleNamespace
from unittest.mock import AsyncMock

import cosecha_mcp.server as mcp_server


def _run_async(coro):
    return asyncio.run(coro)


def test_sync_tools_delegate_to_service(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def _record(name: str, **kwargs):
        calls.append((name, kwargs))
        return {'tool': name}

    monkeypatch.setattr(
        mcp_server,
        'SERVICE',
        SimpleNamespace(
            describe_workspace=lambda **kwargs: _record(
                'describe_workspace',
                **kwargs,
            ),
            describe_knowledge_base=lambda **kwargs: _record(
                'describe_knowledge_base',
                **kwargs,
            ),
            describe_path_freshness=lambda **kwargs: _record(
                'describe_path_freshness',
                **kwargs,
            ),
            search_catalog=lambda query, **kwargs: _record(
                'search_catalog',
                query=query,
                **kwargs,
            ),
            list_test_execution_history=lambda **kwargs: _record(
                'list_test_execution_history',
                **kwargs,
            ),
        ),
    )

    assert mcp_server.describe_workspace(start_path='/tmp')['tool'] == (
        'describe_workspace'
    )
    assert mcp_server.describe_knowledge_base(start_path='/tmp')['tool'] == (
        'describe_knowledge_base'
    )
    assert mcp_server.describe_path_freshness(
        path='tests/demo.feature',
        engine_name='gherkin',
        include_children=False,
        limit=5,
        start_path='/tmp',
    )['tool'] == 'describe_path_freshness'
    assert mcp_server.search_catalog(
        query='demo',
        kinds=['tests'],
        engine_name='gherkin',
        limit=7,
        start_path='/tmp',
    )['tool'] == 'search_catalog'
    assert mcp_server.list_test_execution_history(
        test_path='tests/demo.feature',
        engine_name='gherkin',
        status='failed',
        session_id='session-1',
        limit=3,
        start_path='/tmp',
    )['tool'] == 'list_test_execution_history'

    assert calls[0][0] == 'describe_workspace'
    assert calls[-1][0] == 'list_test_execution_history'


def test_async_tools_delegate_to_service(monkeypatch) -> None:
    service = SimpleNamespace(
        query_tests=AsyncMock(return_value={'tool': 'query_tests'}),
        query_definitions=AsyncMock(
            return_value={'tool': 'query_definitions'},
        ),
        query_registry_items=AsyncMock(
            return_value={'tool': 'query_registry_items'},
        ),
        query_resources=AsyncMock(return_value={'tool': 'query_resources'}),
        read_session_artifacts=AsyncMock(
            return_value={'tool': 'read_session_artifacts'},
        ),
        list_recent_sessions=AsyncMock(
            return_value={'tool': 'list_recent_sessions'},
        ),
        describe_session_coverage=AsyncMock(
            return_value={'tool': 'describe_session_coverage'},
        ),
        list_coverage_history=AsyncMock(
            return_value={'tool': 'list_coverage_history'},
        ),
        inspect_test_plan=AsyncMock(
            return_value={'tool': 'inspect_test_plan'},
        ),
        get_execution_timeline=AsyncMock(
            return_value={'tool': 'get_execution_timeline'},
        ),
        list_engines_and_capabilities=AsyncMock(
            return_value={'tool': 'list_engines_and_capabilities'},
        ),
        refresh_knowledge_base=AsyncMock(
            return_value={'tool': 'refresh_knowledge_base'},
        ),
        run_tests=AsyncMock(return_value={'tool': 'run_tests'}),
    )
    monkeypatch.setattr(mcp_server, 'SERVICE', service)

    assert _run_async(mcp_server.query_tests())['tool'] == 'query_tests'
    assert _run_async(mcp_server.query_definitions())['tool'] == (
        'query_definitions'
    )
    assert _run_async(mcp_server.query_registry_items())['tool'] == (
        'query_registry_items'
    )
    assert _run_async(mcp_server.query_resources())['tool'] == (
        'query_resources'
    )
    assert _run_async(mcp_server.read_session_artifacts())['tool'] == (
        'read_session_artifacts'
    )
    assert _run_async(mcp_server.list_recent_sessions())['tool'] == (
        'list_recent_sessions'
    )
    assert _run_async(mcp_server.describe_session_coverage())['tool'] == (
        'describe_session_coverage'
    )
    assert _run_async(mcp_server.list_coverage_history())['tool'] == (
        'list_coverage_history'
    )
    assert _run_async(mcp_server.inspect_test_plan())['tool'] == (
        'inspect_test_plan'
    )
    assert _run_async(mcp_server.get_execution_timeline())['tool'] == (
        'get_execution_timeline'
    )
    assert _run_async(mcp_server.list_engines_and_capabilities())['tool'] == (
        'list_engines_and_capabilities'
    )
    assert _run_async(mcp_server.refresh_knowledge_base())['tool'] == (
        'refresh_knowledge_base'
    )
    assert _run_async(mcp_server.run_tests())['tool'] == 'run_tests'

    assert service.query_tests.await_count == 1
    assert service.run_tests.await_count == 1


def test_main_runs_mcp_server_stdio(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        mcp_server,
        'MCP_SERVER',
        SimpleNamespace(run=lambda **kwargs: calls.append(kwargs['transport'])),
    )

    mcp_server.main()

    assert calls == ['stdio']
