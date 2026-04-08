from __future__ import annotations

import asyncio

from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.config import Config
from cosecha.core.execution_ir import TestExecutionNodeSnapshot
from cosecha.core.items import TestResultStatus
from cosecha.core.reporting_ir import TestReport, serialize_test_report
from cosecha.core.runtime import (
    ExecutionBodyResult,
    ProcessRuntimeProvider,
    RuntimeProvider,
    _OrphanedResourceManager,
    _PersistentWorker,
    _collect_worker_ephemeral_capabilities,
    _group_plan_nodes_for_workers,
    _resolve_group_slots,
)
from cosecha.core.runtime_protocol import RuntimeExecuteResponse
from cosecha.core.scheduler import (
    RoundRobinWorkerSelectionPolicy,
    SchedulingDecision,
    SchedulingPlan,
)
from cosecha.core.serialization import encode_json_bytes


class _BaseRuntime(RuntimeProvider):
    async def execute(self, node, executor):
        return await executor(node)


class _Reader:
    def __init__(self, lines: tuple[bytes, ...]) -> None:
        self._lines = list(lines)

    async def readline(self) -> bytes:
        await asyncio.sleep(0)
        if self._lines:
            return self._lines.pop(0)
        return b''


class _WaitWorker:
    def __init__(self, *, delay: float, result: bool) -> None:
        self._delay = delay
        self._result = result
        self.cancelled = False

    def has_live_activity(self) -> bool:
        return False

    async def wait_for_live_activity(self, timeout_seconds: float | None) -> bool:
        del timeout_seconds
        try:
            await asyncio.sleep(self._delay)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return self._result


def _make_runtime_execute_response() -> RuntimeExecuteResponse:
    return RuntimeExecuteResponse(
        report=serialize_test_report(
            TestReport(
                path='tests/demo.feature',
                status=TestResultStatus.PASSED,
                message=None,
                duration=0.01,
            ),
        ),
        phase_durations={'body': 0.01},
    )


def _make_node(*, node_id: str, stable_id: str, path: str):
    return SimpleNamespace(
        id=node_id,
        stable_id=stable_id,
        engine_name='gherkin',
        test_path=path,
        source_content=None,
        engine=SimpleNamespace(describe_capabilities=lambda: ()),
    )


def test_runtime_provider_base_defaults_cover_unhit_methods(tmp_path: Path) -> None:
    provider = _BaseRuntime()
    config = Config(root_path=tmp_path, concurrency=0)

    assert _BaseRuntime.runtime_name() == '_baseruntime'
    assert provider.describe_capabilities() == ()
    assert provider.legacy_session_scope() == 'run'
    assert provider.scheduler_worker_count(config) == 1
    assert provider.live_execution_granularity() == 'streaming'
    assert asyncio.run(provider.wait_for_live_observability(None)) is False


@pytest.mark.asyncio
async def test_runtime_internal_worker_helpers_and_stream_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _OrphanedResourceManager()
    manager._active_resources_by_worker[3] = {
        ('mongo', 'worker', 'h1'): SimpleNamespace(
            name='mongo',
            scope='worker',
            external_handle='h1',
        ),
    }
    reaped = await manager.reap_worker(3)
    assert reaped == ()

    state_path = tmp_path / 'worker-1.json'
    state_path.write_text(
        (
            '{"active_resources":['
            '{"name":1,"scope":"worker","external_handle":"h"},'
            '{"name":"a","scope":1,"external_handle":"h"},'
            '{"name":"a","scope":"worker","external_handle":1}'
            '],'
            '"pending_resources":['
            '1,'
            '{"name":"a","scope":1,"external_handle":"h"},'
            '{"name":"a","scope":"worker","external_handle":1},'
            '{"name":"ok","scope":"worker","external_handle":"h"}'
            ']}'
        ),
        encoding='utf-8',
    )
    records = manager._load_worker_state_resources(state_path)
    assert list(records) == [('ok', 'worker', 'h')]

    worker = _PersistentWorker.__new__(_PersistentWorker)
    worker.worker_id = 1
    worker.session_id = 'session-1'
    worker.stdout = _Reader(
        (
            encode_json_bytes(_make_runtime_execute_response().to_dict()) + b'\n',
            b'',
        ),
    )
    worker._stderr_text = []
    worker._ready_future = asyncio.get_running_loop().create_future()
    worker._command_futures = {}
    worker._live_activity_event = asyncio.Event()
    worker._live_domain_events = []
    worker._live_log_events = []
    assert worker.has_live_activity() is False

    worker._live_activity_event.set()
    worker._live_domain_events = [object()]
    worker._reset_live_activity_if_idle()
    assert worker._live_activity_event.is_set() is True
    await worker._read_stdout()

    monkeypatch.setattr(
        'cosecha.core.runtime.build_ephemeral_artifact_capability',
        lambda descriptors, declared_component_id: None,
    )
    monkeypatch.setattr(
        'cosecha.core.runtime.component_id_from_component_type',
        lambda component_type: 'component::engine',
    )
    assert _collect_worker_ephemeral_capabilities((_make_node(node_id='n1', stable_id='s1', path='a.feature'),)) == ()


@pytest.mark.asyncio
async def test_process_runtime_internal_branches_cover_slot_binding_finish_and_state(
    tmp_path: Path,
) -> None:
    provider = ProcessRuntimeProvider(worker_count=2)
    provider.initialize(Config(root_path=tmp_path))
    provider._session_id = 'session-1'

    assert ProcessRuntimeProvider.runtime_worker_model() == 'persistent_workers'
    assert provider.legacy_session_scope() == 'worker'

    node = _make_node(node_id='n1', stable_id='s1', path='a.feature')
    provider.bind_execution_slot(node, 0)
    assert provider._assigned_worker_by_node == {}

    provider._shadow_managed_externally = True
    provider._shadow_context = SimpleNamespace(cleanup=lambda preserve: (_ for _ in ()).throw(AssertionError(preserve)))
    provider._shadow_binding = None
    provider._workers = []
    provider._run_resource_manager = SimpleNamespace(
        close=lambda: asyncio.sleep(0),
        build_resource_timing_snapshot=lambda: (),
    )
    await provider.finish()
    assert provider._shadow_context is None

    provider._runtime_state_dir = tmp_path / 'runtime'
    provider._runtime_state_dir.mkdir(parents=True, exist_ok=True)
    provider._session_id = 'session-2'
    assert provider._build_worker_state_error(1) is None
    provider._delete_worker_state(1)

    state_path = provider._runtime_state_dir / provider._session_id / 'worker-2.json'
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        '{"status":"degraded","unhealthy_resources":["mongo"]}',
        encoding='utf-8',
    )
    error = provider._build_worker_state_error(2)
    assert error is not None
    assert error.args[0] == 'worker_local_unhealthy:mongo'


@pytest.mark.asyncio
async def test_process_runtime_wait_cancels_pending_tasks_and_group_slot_fast_path() -> (
    None
):
    provider = ProcessRuntimeProvider(worker_count=2)
    slow = _WaitWorker(delay=10.0, result=True)
    provider._workers = [_WaitWorker(delay=0.0, result=False), slow]
    assert await provider.wait_for_live_observability(0.1) is False
    assert slow.cancelled is True

    node_a = _make_node(node_id='n1', stable_id='s1', path='a.feature')
    node_b = _make_node(node_id='n2', stable_id='s2', path='b.feature')
    grouped = _group_plan_nodes_for_workers((node_a, node_b))
    scheduling = SchedulingPlan(
        worker_count=2,
        decisions=(
            SchedulingDecision(
                node_id='n1',
                node_stable_id='s1',
                worker_slot=0,
                max_attempts=1,
            ),
            SchedulingDecision(
                node_id='n2',
                node_stable_id='s2',
                worker_slot=1,
                max_attempts=1,
            ),
        ),
    )
    slots = _resolve_group_slots(
        grouped,
        scheduling_plan=scheduling,
        worker_count=2,
        worker_selection_policy=RoundRobinWorkerSelectionPolicy(),
    )
    assert set(slots) == set(grouped)
