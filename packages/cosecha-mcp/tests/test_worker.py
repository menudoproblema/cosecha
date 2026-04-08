from __future__ import annotations

import asyncio
import json

from io import StringIO
from types import SimpleNamespace

import pytest

from cosecha.core.operations import QueryCapabilitiesOperation
from cosecha_mcp import worker


def test_deserialize_operation_rejects_unknown_type() -> None:
    with pytest.raises(ValueError, match='Unsupported runner operation'):
        worker._deserialize_operation({'operation_type': 'unknown'})


def test_execute_request_requires_operation_object(monkeypatch) -> None:
    monkeypatch.setattr(
        worker,
        'resolve_cosecha_workspace',
        lambda _start_path=None: SimpleNamespace(to_dict=lambda: {}),
    )

    with pytest.raises(
        ValueError,
        match='Worker request requires an operation object',
    ):
        asyncio.run(worker._execute_request({'operation': 'invalid'}))


def test_execute_request_runs_operation_and_closes_runner(monkeypatch) -> None:
    class FakeResult:
        def to_dict(self):
            return {'result_type': 'query.capabilities'}

    class FakeRunner:
        def __init__(self) -> None:
            self.engines = (
                SimpleNamespace(name='pytest'),
                SimpleNamespace(name='gherkin'),
            )
            self.operation = None

        async def execute_operation(self, operation):
            self.operation = operation
            return FakeResult()

    fake_runner = FakeRunner()
    closed: list[bool] = []
    selected_engine_name_sets: list[set[str] | None] = []

    class FakeService:
        def _build_runner_for_selection(
            self,
            workspace,
            *,
            selected_engine_names,
        ):
            del workspace
            selected_engine_name_sets.append(selected_engine_names)
            return fake_runner

        def _close_runner(self, runner):
            assert runner is fake_runner
            closed.append(True)

    workspace = SimpleNamespace(to_dict=lambda: {'project_path': '/workspace'})
    monkeypatch.setattr(
        worker,
        'resolve_cosecha_workspace',
        lambda _start_path=None: workspace,
    )
    monkeypatch.setattr(worker, 'CosechaMcpService', FakeService)

    response = asyncio.run(
        worker._execute_request(
            {
                'operation': QueryCapabilitiesOperation().to_dict(),
                'selected_engine_names': ['gherkin', 7],
                'start_path': '/workspace',
            },
        ),
    )

    assert response['result']['result_type'] == 'query.capabilities'
    assert response['workspace'] == {'project_path': '/workspace'}
    assert response['engine_names'] == ['gherkin', 'pytest']
    assert isinstance(fake_runner.operation, QueryCapabilitiesOperation)
    assert closed == [True]
    assert selected_engine_name_sets == [{'7', 'gherkin'}]


def test_main_writes_response_as_json(monkeypatch) -> None:
    monkeypatch.setattr(worker.sys, 'stdin', StringIO('{"operation": {}}'))
    stdout = StringIO()
    monkeypatch.setattr(worker.sys, 'stdout', stdout)

    async def _fake_execute(_request):
        return {'ok': True}

    monkeypatch.setattr(worker, '_execute_request', _fake_execute)

    worker.main()

    assert json.loads(stdout.getvalue()) == {'ok': True}


def test_main_exits_on_invalid_request_payload(monkeypatch) -> None:
    monkeypatch.setattr(worker.sys, 'stdin', StringIO('[]'))
    monkeypatch.setattr(worker.sys, 'stdout', StringIO())
    stderr = StringIO()
    monkeypatch.setattr(worker.sys, 'stderr', stderr)

    with pytest.raises(SystemExit) as error:
        worker.main()

    assert error.value.code == 1
    assert 'Worker request must be a JSON object' in stderr.getvalue()
