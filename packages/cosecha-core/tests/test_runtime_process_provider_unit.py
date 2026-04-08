from __future__ import annotations

import asyncio

from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from cosecha.core.config import Config
from cosecha.core.domain_events import (
    DomainEventMetadata,
    LogChunkEvent,
    ResourceLifecycleEvent,
    WorkerDegradedEvent,
    WorkerRecoveredEvent,
)
from cosecha.core.execution_ir import (
    ExecutionRequest,
    TestExecutionNodeSnapshot,
)
from cosecha.core.items import TestResultStatus
from cosecha.core.reporting_ir import TestReport, serialize_test_report
from cosecha.core.resources import ResourceRequirement, ResourceTiming
from cosecha.core.runtime import (
    ExecutionBodyResult,
    ProcessRuntimeProvider,
    RuntimeInfrastructureError,
    _OrphanedResourceManager,
    _PersistentWorker,
)
from cosecha.core.runtime_protocol import (
    RuntimeBootstrapResponse,
    RuntimeEnvelopeMetadata,
    RuntimeErrorResponse,
    RuntimeEventResponse,
    RuntimeExecuteCommand,
    RuntimeExecuteResponse,
    RuntimeProtocolError,
    RuntimeReadyResponse,
    RuntimeShutdownResponse,
    RuntimeSnapshotResourceTimingsCommand,
    RuntimeSnapshotResourceTimingsResponse,
)
from cosecha.core.serialization import encode_json_bytes


if TYPE_CHECKING:
    from collections.abc import Iterable


def _make_test_report() -> TestReport:
    return TestReport(
        path='tests/process.feature',
        status=TestResultStatus.PASSED,
        message=None,
        duration=0.05,
    )


def _make_execution_request(root_path: Path) -> ExecutionRequest:
    config = Config(root_path=root_path)
    return ExecutionRequest(
        cwd=str(root_path),
        root_path=str(root_path),
        config_snapshot=config.snapshot(),
        node=TestExecutionNodeSnapshot(
            id='dummy:test.feature:0',
            stable_id='dummy:test.feature:stable-0',
            engine_name='dummy',
            test_name='test',
            test_path='test.feature',
        ),
    )


def _make_bootstrap_node(root_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        stable_id='dummy:test.feature:stable-0',
        engine=SimpleNamespace(config=Config(root_path=root_path)),
        snapshot=TestExecutionNodeSnapshot(
            id='dummy:test.feature:0',
            stable_id='dummy:test.feature:stable-0',
            engine_name='dummy',
            test_name='test',
            test_path='test.feature',
        ),
    )


def _done_future(*, value: object | None = None) -> asyncio.Future[object]:
    future: asyncio.Future[object] = (
        asyncio.get_running_loop().create_future()
    )
    future.set_result(value)
    return future


class _FakeReader:
    def __init__(self, lines: Iterable[bytes]) -> None:
        self._lines = list(lines)

    async def readline(self) -> bytes:
        await asyncio.sleep(0)
        if self._lines:
            return self._lines.pop(0)
        return b''


class _FakeWriter:
    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.drain_calls = 0

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        self.drain_calls += 1


class _FakeProcess:
    def __init__(
        self,
        *,
        stdin: _FakeWriter | None = None,
        stdout: _FakeReader | None = None,
        stderr: _FakeReader | None = None,
        returncode: int | None = None,
    ) -> None:
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.wait_calls = 0

    async def wait(self) -> int:
        self.wait_calls += 1
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


class _ProviderWorker:
    def __init__(
        self,
        worker_id: int,
        *,
        execute_result: ExecutionBodyResult | None = None,
        execute_error: Exception | None = None,
        shutdown_result: tuple[
            tuple[ResourceTiming, ...],
            tuple[object, ...],
        ] = ((), ()),
        shutdown_error: Exception | None = None,
        live_domain_events: tuple[object, ...] = (),
        live_log_events: tuple[object, ...] = (),
        has_live_activity: bool = False,
        wait_result: bool = False,
        alive: bool = True,
    ) -> None:
        self.worker_id = worker_id
        self.execute_result = execute_result
        self.execute_error = execute_error
        self.shutdown_result = shutdown_result
        self.shutdown_error = shutdown_error
        self._live_domain_events = list(live_domain_events)
        self._live_log_events = list(live_log_events)
        self._has_live_activity = has_live_activity
        self._wait_result = wait_result
        self._alive = alive
        self.last_request = None
        self.bootstrap_calls: list[tuple[object, tuple[object, ...]]] = []

    async def execute(self, request) -> ExecutionBodyResult:
        self.last_request = request
        if self.execute_error is not None:
            raise self.execute_error
        assert self.execute_result is not None
        return self.execute_result

    async def bootstrap(
        self,
        plan,
        *,
        ephemeral_capabilities=(),
        resource_materialization_snapshots=(),
    ) -> None:
        del resource_materialization_snapshots
        self.bootstrap_calls.append((plan, tuple(ephemeral_capabilities)))

    async def shutdown(
        self,
    ) -> tuple[tuple[ResourceTiming, ...], tuple[object, ...]]:
        if self.shutdown_error is not None:
            raise self.shutdown_error
        return self.shutdown_result

    def is_alive(self) -> bool:
        return self._alive

    def take_live_domain_events(self) -> tuple[object, ...]:
        events = tuple(self._live_domain_events)
        self._live_domain_events.clear()
        return events

    def take_live_log_events(self) -> tuple[object, ...]:
        events = tuple(self._live_log_events)
        self._live_log_events.clear()
        return events

    def has_live_activity(self) -> bool:
        return self._has_live_activity

    async def wait_for_live_activity(self, timeout_seconds: float | None) -> bool:
        del timeout_seconds
        return self._wait_result


def _build_persistent_worker(
    *,
    worker_id: int = 2,
    stdin: _FakeWriter | None = None,
    stdout: _FakeReader | None = None,
    stderr: _FakeReader | None = None,
    returncode: int | None = None,
) -> tuple[_PersistentWorker, _FakeProcess]:
    worker = _PersistentWorker.__new__(_PersistentWorker)
    process = _FakeProcess(
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        returncode=returncode,
    )
    worker.worker_id = worker_id
    worker.session_id = 'session-1'
    worker.process = process
    worker.stdin = stdin
    worker.stdout = stdout
    worker._stdout_lock = asyncio.Lock()
    worker._ready_future = asyncio.get_running_loop().create_future()
    worker._command_futures = {}
    worker._live_activity_event = asyncio.Event()
    worker._live_domain_events = []
    worker._live_log_events = []
    worker._stderr_text = []
    worker._stderr_task = _done_future()
    worker._stdout_reader_task = _done_future()
    return worker, process


def _build_runtime_event(
    *,
    stream_kind: str,
    worker_id: int = 1,
) -> RuntimeEventResponse:
    return RuntimeEventResponse(
        event=LogChunkEvent(
            message='line',
            logger_name='tests.runtime',
            level='info',
            metadata=DomainEventMetadata(worker_id=worker_id),
        ),
        stream_kind=stream_kind,  # type: ignore[arg-type]
    )


def _build_runtime_execute_response(
    *,
    in_reply_to: str | None = None,
) -> RuntimeExecuteResponse:
    return RuntimeExecuteResponse(
        metadata=RuntimeEnvelopeMetadata(in_reply_to=in_reply_to),
        report=serialize_test_report(_make_test_report()),
        phase_durations={'body': 0.01},
    )


def test_orphaned_resource_manager_tracks_acquired_and_released_events() -> None:
    manager = _OrphanedResourceManager()
    acquired = ResourceLifecycleEvent(
        action='acquired',
        name='mongo',
        scope='worker',
        external_handle='handle-1',
        metadata=DomainEventMetadata(worker_id=3),
    )
    released = ResourceLifecycleEvent(
        action='released',
        name='mongo',
        scope='worker',
        external_handle='handle-1',
        metadata=DomainEventMetadata(worker_id=3),
    )

    manager.observe_resource_event(acquired)
    manager.observe_resource_event(released)

    assert manager._active_resources_by_worker == {}


def test_orphaned_resource_manager_sorts_dependency_depth_for_reaping() -> None:
    manager = _OrphanedResourceManager()
    manager.register_plan(
        (
            SimpleNamespace(
                resource_requirements=(
                    ResourceRequirement(
                        name='mongo',
                        scope='worker',
                        depends_on=('network',),
                        setup=lambda: object(),
                    ),
                    ResourceRequirement(
                        name='network',
                        scope='worker',
                        setup=lambda: object(),
                    ),
                ),
            ),
        ),
    )

    ordered = manager._order_resources_for_reap(
        (
            SimpleNamespace(
                name='network',
                scope='worker',
                external_handle='network-1',
            ),
            SimpleNamespace(
                name='mongo',
                scope='worker',
                external_handle='mongo-1',
            ),
        ),
    )

    assert [resource.name for resource in ordered] == ['mongo', 'network']


def test_orphaned_resource_manager_loads_worker_state_records(tmp_path: Path) -> None:
    manager = _OrphanedResourceManager()
    state_path = tmp_path / 'worker-1.json'
    state_path.write_text(
        (
            '{"active_resources":[{"name":"mongo","scope":"worker",'
            '"external_handle":"mongo-1"},42],'
            '"pending_resources":[{"name":"mongo","scope":"worker",'
            '"external_handle":"mongo-1"},{"name":"cache","scope":"worker",'
            '"external_handle":"cache-1"},{"scope":"worker"}]}'
        ),
        encoding='utf-8',
    )

    records = manager._load_worker_state_resources(state_path)
    missing = manager._load_worker_state_resources(tmp_path / 'missing.json')

    assert sorted(records) == [
        ('cache', 'worker', 'cache-1'),
        ('mongo', 'worker', 'mongo-1'),
    ]
    assert missing == {}


@pytest.mark.asyncio
async def test_persistent_worker_send_command_requires_open_pipes() -> None:
    worker, _ = _build_persistent_worker(stdin=None, stdout=None)

    with pytest.raises(RuntimeError, match='is not available'):
        await worker._send_command(RuntimeSnapshotResourceTimingsCommand())


@pytest.mark.asyncio
async def test_persistent_worker_send_command_handles_error_and_invalid_responses(
) -> None:
    writer = _FakeWriter()
    worker, _ = _build_persistent_worker(stdin=writer, stdout=_FakeReader(()))
    worker._stderr_text.append('stderr line\n')

    command = RuntimeSnapshotResourceTimingsCommand(
        metadata=RuntimeEnvelopeMetadata(message_id='msg-1'),
    )

    async def _resolve_with_error() -> None:
        while 'msg-1' not in worker._command_futures:
            await asyncio.sleep(0)
        worker._command_futures['msg-1'].set_result(
            RuntimeErrorResponse(
                metadata=RuntimeEnvelopeMetadata(in_reply_to='msg-1'),
                error=RuntimeProtocolError(
                    code='resource_unhealthy',
                    message='resource unhealthy',
                    recoverable=True,
                    fatal=False,
                ),
            ),
        )

    task = asyncio.create_task(_resolve_with_error())
    with pytest.raises(RuntimeInfrastructureError, match='resource_unhealthy'):
        await worker._send_command(command)
    await task

    command = RuntimeSnapshotResourceTimingsCommand(
        metadata=RuntimeEnvelopeMetadata(message_id='msg-2'),
    )

    async def _resolve_with_invalid() -> None:
        while 'msg-2' not in worker._command_futures:
            await asyncio.sleep(0)
        worker._command_futures['msg-2'].set_result(RuntimeReadyResponse())

    task = asyncio.create_task(_resolve_with_invalid())
    with pytest.raises(RuntimeError, match='Persistent runtime worker failed'):
        await worker._send_command(command)
    await task
    assert writer.drain_calls == 2


@pytest.mark.asyncio
async def test_persistent_worker_execute_bootstrap_collect_and_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, process = _build_persistent_worker(
        stdin=_FakeWriter(),
        stdout=_FakeReader(()),
        returncode=None,
    )
    worker._stderr_task = _done_future()
    worker._stdout_reader_task = _done_future()
    request = _make_execution_request(Path.cwd())

    async def _send_execute(self, command):
        del self, command
        return _build_runtime_execute_response()

    monkeypatch.setattr(_PersistentWorker, '_send_command', _send_execute)
    executed = await worker.execute(request)
    assert executed.report.status is TestResultStatus.PASSED

    async def _send_bootstrap(self, command):
        del self, command
        return RuntimeBootstrapResponse()

    monkeypatch.setattr(_PersistentWorker, '_send_command', _send_bootstrap)
    await worker.bootstrap((_make_bootstrap_node(Path.cwd()),))

    async def _send_timings(self, command):
        del self, command
        return RuntimeSnapshotResourceTimingsResponse(
            resource_timings=(
                ResourceTiming(name='mongo', scope='worker'),
            ),
        )

    monkeypatch.setattr(_PersistentWorker, '_send_command', _send_timings)
    timings = await worker.collect_resource_timings()
    assert timings[0].name == 'mongo'

    async def _send_shutdown(self, command):
        del self, command
        return RuntimeShutdownResponse(
            domain_events=(
                WorkerRecoveredEvent(worker_id=2),
            ),
        )

    monkeypatch.setattr(_PersistentWorker, '_send_command', _send_shutdown)
    _, events = await worker.shutdown()
    assert process.wait_calls == 1
    assert events[0].event_type == 'worker.recovered'
    assert worker.is_alive() is False


@pytest.mark.asyncio
async def test_persistent_worker_validates_command_response_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker, _ = _build_persistent_worker(
        stdin=_FakeWriter(),
        stdout=_FakeReader(()),
    )
    request = _make_execution_request(Path.cwd())

    async def _ready_response(self, command):
        del self, command
        return RuntimeReadyResponse()

    monkeypatch.setattr(_PersistentWorker, '_send_command', _ready_response)
    with pytest.raises(RuntimeError, match='invalid execute response'):
        await worker.execute(request)
    with pytest.raises(RuntimeError, match='invalid bootstrap response'):
        await worker.bootstrap((_make_bootstrap_node(Path.cwd()),))
    with pytest.raises(RuntimeError, match='invalid resource snapshot'):
        await worker.collect_resource_timings()
    with pytest.raises(RuntimeError, match='invalid shutdown response'):
        await worker.shutdown()


@pytest.mark.asyncio
async def test_persistent_worker_waits_for_live_activity() -> None:
    worker, _ = _build_persistent_worker(
        stdin=_FakeWriter(),
        stdout=_FakeReader(()),
    )
    worker._live_domain_events.append(
        LogChunkEvent(message='x', logger_name='t', level='info'),
    )
    assert await worker.wait_for_live_activity(timeout_seconds=None) is True

    worker._live_domain_events.clear()
    worker._live_log_events.clear()
    assert await worker.wait_for_live_activity(timeout_seconds=0.01) is False

    async def _set_activity() -> None:
        await asyncio.sleep(0)
        worker._live_log_events.append(
            LogChunkEvent(message='y', logger_name='t', level='info'),
        )
        worker._live_activity_event.set()

    task = asyncio.create_task(_set_activity())
    assert await worker.wait_for_live_activity(timeout_seconds=None) is True
    await task


@pytest.mark.asyncio
async def test_persistent_worker_reads_stdout_and_stderr_streams() -> None:
    reply_id = 'reply-1'
    ready = RuntimeReadyResponse()
    log_event = _build_runtime_event(stream_kind='log')
    domain_event = _build_runtime_event(stream_kind='domain')
    execute_response = _build_runtime_execute_response(in_reply_to=reply_id)

    stdout = _FakeReader(
        (
            encode_json_bytes(ready.to_dict()) + b'\n',
            encode_json_bytes(log_event.to_dict()) + b'\n',
            encode_json_bytes(domain_event.to_dict()) + b'\n',
            encode_json_bytes(execute_response.to_dict()) + b'\n',
            b'',
        ),
    )
    stderr = _FakeReader((b'worker warning\n', b''))
    worker, _ = _build_persistent_worker(
        stdin=_FakeWriter(),
        stdout=stdout,
        stderr=stderr,
    )
    response_future = asyncio.get_running_loop().create_future()
    worker._command_futures[reply_id] = response_future

    await worker._read_stdout()
    await worker._read_stderr()

    assert worker._ready_future.done() is True
    assert worker._live_activity_event.is_set() is True
    assert len(worker._live_log_events) == 1
    assert len(worker._live_domain_events) == 1
    assert response_future.done() is True
    assert 'worker warning' in ''.join(worker._stderr_text)

    worker.stdout = None
    with pytest.raises(RuntimeError, match='has no stdout pipe'):
        await worker._read_stdout()
    worker.process.stderr = None
    await worker._read_stderr()


@pytest.mark.asyncio
async def test_persistent_worker_fails_pending_futures() -> None:
    worker, _ = _build_persistent_worker(
        stdin=_FakeWriter(),
        stdout=_FakeReader(()),
    )
    pending = asyncio.get_running_loop().create_future()
    worker._command_futures['msg'] = pending
    error = RuntimeError('worker failed')

    worker._fail_pending_responses(error)

    assert worker._ready_future.done() is True
    assert pending.done() is True
    assert isinstance(pending.exception(), RuntimeError)


@pytest.mark.asyncio
async def test_process_runtime_start_validates_initialization_and_worker_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = ProcessRuntimeProvider()
    with pytest.raises(RuntimeError, match='initialize\\(\\) must run'):
        await provider.start()

    provider.initialize(Config(root_path=tmp_path))
    provider._execution_context = None
    with pytest.raises(RuntimeError, match='initialize\\(\\) must run'):
        await provider.start()

    provider = ProcessRuntimeProvider(worker_count=1)
    provider.initialize(Config(root_path=tmp_path))
    events: list[str] = []

    class _Binding:
        def __enter__(self):
            events.append('enter')
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            events.append('exit')
            return False

    async def _boom_start(*args, **kwargs):
        del args, kwargs
        raise RuntimeError('boom')

    monkeypatch.setattr('cosecha.core.runtime.binding_shadow', lambda *args, **kwargs: _Binding())
    monkeypatch.setattr('cosecha.core.runtime._PersistentWorker.start', _boom_start)

    with pytest.raises(RuntimeError, match='boom'):
        await provider.start()
    assert events == ['enter', 'exit']
    assert provider._shadow_binding is None


@pytest.mark.asyncio
async def test_process_runtime_prepare_handles_empty_workers_and_run_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = ProcessRuntimeProvider(worker_count=1)
    provider.initialize(Config(root_path=tmp_path))

    async def _no_start() -> None:
        return None

    monkeypatch.setattr(provider, 'start', _no_start)
    await provider.prepare(())

    worker = _ProviderWorker(worker_id=0)
    provider._workers = [worker]
    run_requirement = ResourceRequirement(
        name='mongo',
        scope='run',
        setup=lambda: {'dsn': 'memory://mongo'},
    )
    node = SimpleNamespace(resource_requirements=(run_requirement,))
    monkeypatch.setattr(
        'cosecha.core.runtime._collect_worker_ephemeral_capabilities',
        lambda nodes: (),
    )

    await provider.prepare((node,))

    assert worker.bootstrap_calls
    assert provider._run_materialization_snapshots


@pytest.mark.asyncio
async def test_process_runtime_execute_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = ProcessRuntimeProvider(worker_count=1)
    provider.initialize(Config(root_path=tmp_path))
    provider._session_id = 'session-1'
    node = SimpleNamespace(
        id='node-1',
        stable_id='stable-1',
        engine=SimpleNamespace(config=Config(root_path=tmp_path)),
    )
    monkeypatch.setattr(
        'cosecha.core.runtime.ExecutionRequest.from_node',
        lambda *args, **kwargs: _make_execution_request(tmp_path),
    )

    with pytest.raises(RuntimeError, match='must assign a worker'):
        await provider.execute(node, lambda _: None)

    error_worker = _ProviderWorker(
        0,
        execute_error=RuntimeInfrastructureError(
            code='resource_health',
            message='resource failed',
            recoverable=True,
            fatal=False,
        ),
        alive=True,
    )
    provider._workers = [error_worker]
    provider._assigned_worker_by_node[node.id] = error_worker
    handled: list[str] = []

    async def _record_failure(worker, error):
        del worker, error
        handled.append('handled')

    monkeypatch.setattr(provider, '_handle_worker_failure', _record_failure)
    with pytest.raises(RuntimeInfrastructureError):
        await provider.execute(node, lambda _: None)
    assert handled == ['handled']
    assert provider._degraded_worker_ids == {0}


@pytest.mark.asyncio
async def test_process_runtime_execute_marks_worker_recovery_and_state_degrade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = ProcessRuntimeProvider(worker_count=1)
    provider.initialize(Config(root_path=tmp_path))
    provider._session_id = 'session-1'
    result = ExecutionBodyResult(
        report=_make_test_report(),
        domain_events=(
            ResourceLifecycleEvent(
                action='acquired',
                name='mongo',
                scope='worker',
                external_handle='mongo-1',
            ),
        ),
    )
    worker = _ProviderWorker(0, execute_result=result, alive=True)
    node = SimpleNamespace(
        id='node-1',
        stable_id='stable-1',
        engine=SimpleNamespace(config=Config(root_path=tmp_path)),
    )
    provider._workers = [worker]
    provider._assigned_worker_by_node[node.id] = worker
    monkeypatch.setattr(
        'cosecha.core.runtime.ExecutionRequest.from_node',
        lambda *args, **kwargs: _make_execution_request(tmp_path),
    )

    provider._degraded_worker_ids.add(worker.worker_id)
    monkeypatch.setattr(provider, '_build_worker_state_error', lambda _wid: None)
    await provider.execute(node, lambda _: None)
    assert worker.worker_id not in provider._degraded_worker_ids
    assert any(
        isinstance(event, WorkerRecoveredEvent)
        for event in provider._domain_events
    )

    state_error = RuntimeInfrastructureError(
        code='worker_local_unhealthy',
        message='degraded state',
        recoverable=True,
        fatal=False,
    )
    monkeypatch.setattr(provider, '_build_worker_state_error', lambda _wid: state_error)
    previous_event_count = len(provider._domain_events)
    await provider.execute(node, lambda _: None)
    assert len(provider._domain_events) > previous_event_count
    assert any(
        isinstance(event, WorkerDegradedEvent)
        for event in provider._domain_events
    )


@pytest.mark.asyncio
async def test_process_runtime_finish_without_workers_resets_state(
    tmp_path: Path,
) -> None:
    provider = ProcessRuntimeProvider(worker_count=1)
    provider.initialize(Config(root_path=tmp_path))
    provider._session_id = 'session-1'
    cleanup_calls: list[bool] = []
    provider._shadow_context = SimpleNamespace(
        cleanup=lambda preserve: cleanup_calls.append(preserve),
    )
    provider._shadow_binding = SimpleNamespace(
        __exit__=lambda *args: False,
    )
    provider._run_resource_manager = SimpleNamespace(
        close=lambda: asyncio.sleep(0),
        build_resource_timing_snapshot=lambda: (
            ResourceTiming(name='mongo', scope='run'),
        ),
    )

    await provider.finish()

    assert provider._session_id is None
    assert cleanup_calls == [False]


@pytest.mark.asyncio
async def test_process_runtime_finish_with_workers_collects_and_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = ProcessRuntimeProvider(worker_count=2)
    provider.initialize(Config(root_path=tmp_path))
    provider._session_id = 'session-1'
    provider._shadow_context = SimpleNamespace(cleanup=lambda preserve: None)
    provider._shadow_binding = SimpleNamespace(__exit__=lambda *args: False)
    provider._run_resource_manager = SimpleNamespace(
        close=lambda: asyncio.sleep(0),
        build_resource_timing_snapshot=lambda: (),
    )
    ok_worker = _ProviderWorker(
        0,
        shutdown_result=(
            (ResourceTiming(name='mongo', scope='worker'),),
            (
                WorkerRecoveredEvent(worker_id=0),
            ),
        ),
        live_log_events=(
            LogChunkEvent(message='live', logger_name='tests', level='info'),
        ),
    )
    failing_worker = _ProviderWorker(
        1,
        shutdown_error=RuntimeError('shutdown failed'),
        alive=False,
    )
    provider._workers = [ok_worker, failing_worker]
    handled: list[int] = []

    async def _record_failure(worker, error):
        del error
        handled.append(worker.worker_id)

    monkeypatch.setattr(provider, '_handle_worker_failure', _record_failure)
    deleted: list[int] = []
    monkeypatch.setattr(provider, '_delete_worker_state', lambda worker_id: deleted.append(worker_id))

    await provider.finish()

    assert handled == [1]
    assert deleted == [0]
    assert provider._workers == []
    assert provider._session_id is None


@pytest.mark.asyncio
async def test_process_runtime_handle_worker_failure_recovery_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = ProcessRuntimeProvider(worker_count=1)
    provider.initialize(Config(root_path=tmp_path))
    provider._session_id = 'session-1'
    provider._cwd = tmp_path
    provider._root_path = tmp_path
    provider._workers = [_ProviderWorker(0, alive=False)]
    provider._bootstrap_plan = (
        SimpleNamespace(id='node-1'),
    )
    provider._run_materialization_snapshots = (
        SimpleNamespace(name='snapshot'),
    )
    provider._runtime_state_dir = tmp_path
    reaped: list[int] = []

    async def _reap(self, worker_id: int, *, state_path):
        del self
        del state_path
        reaped.append(worker_id)
        return ()

    monkeypatch.setattr(_OrphanedResourceManager, 'reap_worker', _reap)
    monkeypatch.setattr(
        'cosecha.core.runtime._collect_worker_ephemeral_capabilities',
        lambda nodes: (),
    )
    deleted: list[int] = []
    monkeypatch.setattr(provider, '_delete_worker_state', lambda worker_id: deleted.append(worker_id))

    recovered_worker = _ProviderWorker(0, alive=True)

    async def _start_recovered(*args, **kwargs):
        del args, kwargs
        return recovered_worker

    monkeypatch.setattr('cosecha.core.runtime._PersistentWorker.start', _start_recovered)
    await provider._handle_worker_failure(provider._workers[0], RuntimeError('boom'))
    assert reaped == [0]
    assert deleted == [0]
    assert provider._workers[0] is recovered_worker
    assert recovered_worker.bootstrap_calls

    alive_worker = _ProviderWorker(0, alive=True)
    await provider._handle_worker_failure(alive_worker, RuntimeError('ignore'))
    assert reaped == [0]

    provider._cwd = None
    provider._root_path = None
    provider._session_id = None
    dead_worker = _ProviderWorker(0, alive=False)
    await provider._handle_worker_failure(dead_worker, RuntimeError('missing-context'))


def test_process_runtime_worker_degraded_predicates_and_marking() -> None:
    provider = ProcessRuntimeProvider(worker_count=1)
    provider._session_id = 'session-1'
    alive_worker = _ProviderWorker(4, alive=True)
    dead_worker = _ProviderWorker(5, alive=False)
    resource_error = RuntimeInfrastructureError(
        code='resource_ping_failed',
        message='failed',
        recoverable=True,
        fatal=False,
    )

    assert provider._should_mark_worker_as_degraded(dead_worker, resource_error) is False
    assert provider._should_mark_worker_as_degraded(alive_worker, RuntimeError('boom')) is False
    assert provider._should_mark_worker_as_degraded(alive_worker, resource_error) is True

    provider._mark_worker_degraded(alive_worker, resource_error)
    provider._mark_worker_degraded(alive_worker, resource_error)

    degraded_events = [
        event
        for event in provider._domain_events
        if isinstance(event, WorkerDegradedEvent)
    ]
    assert len(degraded_events) == 1


@pytest.mark.asyncio
async def test_process_runtime_wait_for_live_observability_branches() -> None:
    provider = ProcessRuntimeProvider(worker_count=2)
    provider._workers = [_ProviderWorker(0, has_live_activity=True)]
    assert await provider.wait_for_live_observability(0.01) is True

    provider._workers = []
    assert await provider.wait_for_live_observability(0.0) is False

    provider._workers = [
        _ProviderWorker(0, wait_result=False),
        _ProviderWorker(1, wait_result=True),
    ]
    assert await provider.wait_for_live_observability(0.1) is True
