from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import sys
import time

from collections import defaultdict
from dataclasses import replace
from pathlib import Path

from cosecha.core.capture import CapturedLogContext, CaptureLogHandler
from cosecha.core.config import Config
from cosecha.core.domain_event_stream import (
    DomainEventSink,
    DomainEventStream,
    InMemoryDomainEventSink,
)
from cosecha.core.domain_events import (
    DomainEvent,
    DomainEventMetadata,
    LogChunkEvent,
    ResourceLifecycleEvent,
    WorkerHeartbeatEvent,
)
from cosecha.core.execution_ir import (
    ExecutionBootstrap,
    ExecutionRequest,
    TestExecutionNode,
    TestExecutionNodeSnapshot,
    build_execution_node_id,
    build_execution_node_stable_id,
    build_test_path_label,
)
from cosecha.core.execution_runtime import (
    ExecutionBodyOptions,
    execute_test_body,
)
from cosecha.core.reporting_ir import serialize_test_report
from cosecha.core.resources import ResourceManager
from cosecha.core.runtime_protocol import (
    RuntimeBootstrapCommand,
    RuntimeBootstrapResponse,
    RuntimeCommand,
    RuntimeEnvelopeMetadata,
    RuntimeEventResponse,
    RuntimeExecuteCommand,
    RuntimeExecuteResponse,
    RuntimeReadyResponse,
    RuntimeResponse,
    RuntimeShutdownCommand,
    RuntimeShutdownResponse,
    RuntimeSnapshotResourceTimingsCommand,
    RuntimeSnapshotResourceTimingsResponse,
    build_runtime_protocol_error,
    deserialize_runtime_command,
)
from cosecha.core.serialization import (
    decode_json_dict,
    encode_json_bytes,
    encode_json_text,
)
from cosecha.core.utils import setup_engines


_CONTROL_STDOUT = sys.stdout
_RUNTIME_STATE_DIR = Path('.cosecha/runtime')
_ROOT_LOGGER = logging.getLogger()
WORKER_HEARTBEAT_INTERVAL_SECONDS = 5.0


class _WorkerStateRegistrySink(DomainEventSink):
    __slots__ = (
        '_active_resources',
        '_path',
        '_pending_resources',
        '_status',
        '_worker_id',
    )

    def __init__(
        self,
        path: Path,
        *,
        worker_id: int,
    ) -> None:
        self._path = path
        self._worker_id = worker_id
        self._status = 'ready'
        self._active_resources: dict[
            tuple[str, str, str],
            dict[str, object],
        ] = {}
        self._pending_resources: dict[
            tuple[str, str],
            dict[str, object],
        ] = {}

    async def emit(self, event: DomainEvent) -> None:
        if not isinstance(event, ResourceLifecycleEvent):
            return

        external_handle = event.external_handle
        if external_handle is None:
            self._persist()
            return

        key = (event.name, event.scope, external_handle)
        if event.action == 'acquired':
            self._active_resources[key] = {
                'external_handle': external_handle,
                'name': event.name,
                'scope': event.scope,
            }
        else:
            self._active_resources.pop(key, None)

        self._persist()

    async def close(self) -> None:
        self._persist()

    def touch(self, status: str) -> None:
        self._status = status
        self._persist()

    def record_resource_state(
        self,
        action: str,
        name: str,
        scope: str,
        external_handle: str | None,
    ) -> None:
        pending_key = (name, scope)
        if action == 'pending':
            payload = {
                'name': name,
                'scope': scope,
            }
            if external_handle is not None:
                payload['external_handle'] = external_handle
            self._pending_resources[pending_key] = payload
            self._persist()
            return

        self._pending_resources.pop(pending_key, None)
        if action == 'pending_cleared':
            self._persist()
            return

        if external_handle is None:
            self._persist()
            return

        key = (name, scope, external_handle)
        if action == 'acquired':
            self._active_resources[key] = {
                'external_handle': external_handle,
                'name': name,
                'scope': scope,
            }
        elif action == 'released':
            self._active_resources.pop(key, None)
        self._persist()

    def sync_runtime_state(self, resource_manager: ResourceManager) -> None:
        self._persist(
            resource_timings=tuple(
                resource_manager.build_resource_timing_snapshot(),
            ),
            readiness_states=tuple(
                resource_manager.build_readiness_snapshot(),
            ),
            unhealthy_resources=tuple(
                resource_manager.build_unhealthy_resource_snapshot(),
            ),
        )

    def _persist(
        self,
        *,
        resource_timings=(),
        readiness_states=(),
        unhealthy_resources=(),
    ) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            encode_json_text(
                {
                    'active_resources': list(
                        self._active_resources.values(),
                    ),
                    'heartbeat_at': time.time(),
                    'pending_resources': list(
                        self._pending_resources.values(),
                    ),
                    'readiness_states': [
                        readiness_state.to_dict()
                        for readiness_state in readiness_states
                    ],
                    'resource_timings': [
                        resource_timing.to_dict()
                        for resource_timing in resource_timings
                    ],
                    'status': self._status,
                    'unhealthy_resources': list(unhealthy_resources),
                    'worker_id': self._worker_id,
                },
            ),
            encoding='utf-8',
        )


class _RuntimeResponseEventSink(DomainEventSink):
    __slots__ = ('_metadata_provider', '_streamed_event_ids')

    def __init__(
        self,
        metadata_provider,
        streamed_event_ids: set[str],
    ) -> None:
        self._metadata_provider = metadata_provider
        self._streamed_event_ids = streamed_event_ids

    async def emit(self, event: DomainEvent) -> None:
        metadata = self._metadata_provider()
        if metadata is None:
            return

        _emit_response(
            RuntimeEventResponse(
                event=event,
                stream_kind=(
                    'log' if isinstance(event, LogChunkEvent) else 'domain'
                ),
                metadata=metadata,
            ),
        )
        self._streamed_event_ids.add(event.metadata.event_id)

    async def close(self) -> None:
        return None


def _emit_response(response: RuntimeResponse) -> None:
    _CONTROL_STDOUT.write(f'{encode_json_text(response.to_dict())}\n')
    _CONTROL_STDOUT.flush()


async def _run_worker(
    request_path: Path,
    response_path: Path,
) -> None:
    request = ExecutionRequest.from_dict(
        decode_json_dict(await asyncio.to_thread(request_path.read_bytes)),
    )
    config = Config.from_snapshot(request.config_snapshot)
    hooks, engines = setup_engines(config)
    del hooks
    engine_path, engine = _find_engine(engines, request.node.engine_name)
    engine.initialize(config, engine_path)

    test_path = config.root_path / request.node.test_path
    await engine.collect(test_path)
    await engine.start_session()
    try:
        node = _find_execution_node(engine, config.root_path, request)
        resource_manager = ResourceManager(
            legacy_session_scope='worker',
            unsupported_scopes=('run',),
        )
        result = await execute_test_body(
            node,
            resource_manager,
            ExecutionBodyOptions(root_path=config.root_path),
        )
    finally:
        await engine.finish_session()

    response_payload = {
        'phase_durations': result.phase_durations,
        'report': serialize_test_report(result.report),
        'resource_timings': [
            resource_timing.to_dict()
            for resource_timing in result.resource_timings
        ],
    }
    await asyncio.to_thread(
        response_path.write_bytes,
        encode_json_bytes(response_payload),
    )


class _PersistentWorkerSession:
    __slots__ = (
        '_active_response_metadata',
        '_capture_handler',
        '_capture_log_active',
        '_domain_event_sink',
        '_domain_event_stream',
        '_heartbeat_interval_seconds',
        '_heartbeat_task',
        '_local_unhealthy_error',
        '_pending_log_event_tasks',
        '_prepared_nodes_by_id',
        '_prepared_nodes_by_stable_id',
        '_root_logger_handlers',
        '_started_engine_names',
        '_stream_response_sink',
        '_streamed_event_ids',
        '_worker_state_sink',
        'config',
        'engines',
        'resource_manager',
        'root_path',
        'session_id',
        'worker_id',
    )

    def __init__(
        self,
        root_path: Path,
        engines: dict[str, object],
        resource_manager: ResourceManager,
        *,
        session_id: str,
        worker_id: int,
    ) -> None:
        self.root_path = root_path
        self.session_id = session_id
        self.config: Config | None = None
        self.engines = engines
        self.resource_manager = resource_manager
        self.worker_id = worker_id
        self._started_engine_names: set[str] = set()
        self._prepared_nodes_by_id: dict[str, TestExecutionNode] = {}
        self._prepared_nodes_by_stable_id: dict[str, TestExecutionNode] = {}
        self._capture_handler = CaptureLogHandler()
        self._capture_log_active = False
        self._pending_log_event_tasks: set[asyncio.Task[None]] = set()
        self._heartbeat_interval_seconds = WORKER_HEARTBEAT_INTERVAL_SECONDS
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._local_unhealthy_error: Exception | None = None
        self._root_logger_handlers: tuple[logging.Handler, ...] = ()
        self._domain_event_stream = DomainEventStream()
        self._domain_event_sink = InMemoryDomainEventSink()
        self._active_response_metadata: RuntimeEnvelopeMetadata | None = None
        self._streamed_event_ids: set[str] = set()
        self._stream_response_sink = _RuntimeResponseEventSink(
            self._current_response_metadata,
            self._streamed_event_ids,
        )
        self._worker_state_sink = _WorkerStateRegistrySink(
            root_path
            / _RUNTIME_STATE_DIR
            / session_id
            / f'worker-{worker_id}.json',
            worker_id=worker_id,
        )
        self._domain_event_stream.add_sink(self._domain_event_sink)
        self._domain_event_stream.add_sink(self._stream_response_sink)
        self._domain_event_stream.add_sink(self._worker_state_sink)
        self.resource_manager.bind_domain_event_stream(
            self._domain_event_stream,
        )
        self.resource_manager.bind_resource_state_recorder(
            self._worker_state_sink.record_resource_state,
        )
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    @classmethod
    async def start(
        cls,
        *,
        cwd: Path,
        root_path: Path,
        session_id: str,
        worker_id: int,
    ) -> _PersistentWorkerSession:
        os.chdir(cwd)
        hooks, engines = setup_engines(
            Config(root_path=root_path, capture_log=False),
        )
        del hooks

        return cls(
            root_path,
            engines,
            ResourceManager(
                legacy_session_scope='worker',
                mark_local_failures=True,
            ),
            session_id=session_id,
            worker_id=worker_id,
        )

    async def bootstrap(
        self,
        bootstrap: ExecutionBootstrap,
        *,
        metadata: RuntimeEnvelopeMetadata,
    ) -> RuntimeBootstrapResponse:
        self._clear_buffered_domain_events()
        self._bind_resource_event_metadata(metadata=metadata)
        self._worker_state_sink.touch('alive')
        self._worker_state_sink.sync_runtime_state(self.resource_manager)
        self._local_unhealthy_error = None
        await self._reset_started_engines()
        await self.resource_manager.close()
        self.resource_manager.bind_materialization_snapshots(
            bootstrap.resource_materialization_snapshots,
        )
        self._prepared_nodes_by_id.clear()
        self._prepared_nodes_by_stable_id.clear()
        self._apply_config_snapshot(bootstrap.config_snapshot)

        nodes_by_engine: dict[str, list[TestExecutionNodeSnapshot]] = (
            defaultdict(list)
        )
        for node in bootstrap.nodes:
            nodes_by_engine[node.engine_name].append(node)

        for engine_name, snapshots in nodes_by_engine.items():
            _engine_path, engine = _find_engine(self.engines, engine_name)
            assigned_tests = await self._prepare_engine_nodes(
                engine,
                snapshots,
            )
            if not assigned_tests:
                continue

            await engine.start_session()
            self._prime_engine_execution_nodes(engine, snapshots)
            self._started_engine_names.add(engine_name)
            self._register_prepared_nodes(
                engine,
                snapshots,
                assigned_tests,
            )

        return RuntimeBootstrapResponse(
            metadata=_build_reply_metadata(metadata),
        )

    def _apply_config_snapshot(self, config_snapshot) -> None:
        if self.config is not None:
            current_fingerprint = self.config.snapshot().fingerprint
            if current_fingerprint == config_snapshot.fingerprint:
                return

        config = Config.from_snapshot(config_snapshot)
        for engine_path, engine in self.engines.items():
            engine.initialize(config, engine_path)
            engine.bind_domain_event_stream(self._domain_event_stream)
        self.config = config

    def _clear_buffered_domain_events(self) -> None:
        self._domain_event_sink.events.clear()
        self._streamed_event_ids.clear()

    def _take_buffered_domain_events(
        self,
    ) -> tuple[DomainEvent, ...]:
        events = tuple(
            event
            for event in self._domain_event_sink.events
            if event.metadata.event_id not in self._streamed_event_ids
        )
        self._domain_event_sink.events.clear()
        self._streamed_event_ids.clear()
        for event in events:
            object.__setattr__(event.metadata, 'sequence_number', None)
        return events

    def _current_response_metadata(
        self,
    ) -> RuntimeEnvelopeMetadata | None:
        return self._active_response_metadata

    def _bind_resource_event_metadata(
        self,
        *,
        metadata: RuntimeEnvelopeMetadata,
        node_id: str | None = None,
        node_stable_id: str | None = None,
        test_id: str | None = None,
    ) -> None:
        def _provider(_requirement, _scope: str, current_test_id: str | None):
            effective_test_id = (
                current_test_id if current_test_id is not None else test_id
            )
            return DomainEventMetadata(
                correlation_id=node_stable_id or metadata.correlation_id,
                idempotency_key=None,
                session_id=metadata.session_id,
                plan_id=None,
                node_id=node_id if effective_test_id is not None else None,
                node_stable_id=(
                    node_stable_id if effective_test_id is not None else None
                ),
                trace_id=metadata.trace_id,
                worker_id=self.worker_id,
            )

        self.resource_manager.bind_domain_event_metadata_provider(_provider)

    async def execute(
        self,
        request: ExecutionRequest,
        *,
        metadata: RuntimeEnvelopeMetadata,
    ) -> RuntimeExecuteResponse:
        self._clear_buffered_domain_events()
        self._bind_resource_event_metadata(
            metadata=metadata,
            node_id=request.node.id,
            node_stable_id=request.node.stable_id,
            test_id=request.node.id,
        )
        self._worker_state_sink.touch('alive')
        self._worker_state_sink.sync_runtime_state(self.resource_manager)
        if self._local_unhealthy_error is not None:
            raise self._local_unhealthy_error
        live_log_context = CapturedLogContext(
            session_id=metadata.session_id,
            trace_id=metadata.trace_id,
            node_id=request.node.id,
            node_stable_id=request.node.stable_id,
            worker_id=self.worker_id,
        )
        self._start_log_capture()
        _engine_path, engine = _find_engine(
            self.engines,
            request.node.engine_name,
        )
        node = self._find_prepared_execution_node(engine, request)
        self._active_response_metadata = _build_reply_metadata(metadata)
        try:
            with self._capture_handler.bind_live_context(live_log_context):
                result = await execute_test_body(
                    node,
                    self.resource_manager,
                    ExecutionBodyOptions(
                        root_path=self.root_path,
                        session_id=metadata.session_id,
                        trace_id=metadata.trace_id,
                        worker_id=self.worker_id,
                    ),
                )
            await self._flush_pending_log_events()
        finally:
            self._active_response_metadata = None
            self._stop_log_capture()
        self._worker_state_sink.touch('alive')
        self._worker_state_sink.sync_runtime_state(self.resource_manager)
        domain_events = self._take_buffered_domain_events()
        return RuntimeExecuteResponse(
            metadata=_build_reply_metadata(metadata),
            report=serialize_test_report(result.report),
            phase_durations=result.phase_durations,
            resource_timings=result.resource_timings,
            domain_events=domain_events,
            resource_events=tuple(
                event
                for event in domain_events
                if isinstance(event, ResourceLifecycleEvent)
            ),
        )

    def snapshot_resource_timings(
        self,
        *,
        metadata: RuntimeEnvelopeMetadata,
    ) -> RuntimeSnapshotResourceTimingsResponse:
        return RuntimeSnapshotResourceTimingsResponse(
            metadata=_build_reply_metadata(metadata),
            resource_timings=tuple(
                self.resource_manager.build_resource_timing_snapshot(),
            ),
        )

    async def close(
        self,
        *,
        metadata: RuntimeEnvelopeMetadata,
    ) -> RuntimeShutdownResponse:
        self._clear_buffered_domain_events()
        self._bind_resource_event_metadata(metadata=metadata)
        self._worker_state_sink.touch('closing')
        self._worker_state_sink.sync_runtime_state(self.resource_manager)
        self._active_response_metadata = _build_reply_metadata(metadata)
        try:
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._heartbeat_task
                self._heartbeat_task = None
            await self._reset_started_engines()
            await self.resource_manager.close()
            await self._flush_pending_log_events()
        finally:
            self._active_response_metadata = None
            resource_timings = tuple(
                self.resource_manager.build_resource_timing_snapshot(),
            )

        self._worker_state_sink.touch('closed')
        self._worker_state_sink.sync_runtime_state(self.resource_manager)
        domain_events = self._take_buffered_domain_events()
        return RuntimeShutdownResponse(
            resource_timings=resource_timings,
            domain_events=domain_events,
            resource_events=tuple(
                event
                for event in domain_events
                if isinstance(event, ResourceLifecycleEvent)
            ),
            metadata=_build_reply_metadata(metadata),
        )

    async def _reset_started_engines(self) -> None:
        for engine_name in tuple(self._started_engine_names):
            _engine_path, engine = _find_engine(self.engines, engine_name)
            await engine.finish_session()
        self._started_engine_names.clear()

    def _start_log_capture(self) -> None:
        if self._capture_log_active:
            return

        self._root_logger_handlers = tuple(_ROOT_LOGGER.handlers)
        for handler in self._root_logger_handlers:
            _ROOT_LOGGER.removeHandler(handler)
        self._capture_handler.set_emit_callback(
            self._schedule_live_log_chunk_event,
        )
        _ROOT_LOGGER.addHandler(self._capture_handler)
        self._capture_log_active = True

    def _stop_log_capture(self) -> None:
        if not self._capture_log_active:
            return

        if self._capture_handler in _ROOT_LOGGER.handlers:
            _ROOT_LOGGER.removeHandler(self._capture_handler)
        self._capture_handler.set_emit_callback(None)
        for handler in self._root_logger_handlers:
            if handler not in _ROOT_LOGGER.handlers:
                _ROOT_LOGGER.addHandler(handler)
        self._root_logger_handlers = ()
        self._capture_log_active = False

    def _schedule_live_log_chunk_event(
        self,
        record: logging.LogRecord,
        message: str,
        context: CapturedLogContext,
    ) -> None:
        task = asyncio.create_task(
            self._domain_event_stream.emit(
                LogChunkEvent(
                    message=message,
                    level=record.levelname.lower(),
                    logger_name=record.name,
                    metadata=DomainEventMetadata(
                        session_id=context.session_id,
                        trace_id=context.trace_id,
                        node_id=context.node_id,
                        node_stable_id=context.node_stable_id,
                        worker_id=context.worker_id,
                    ),
                ),
            ),
        )
        self._pending_log_event_tasks.add(task)
        task.add_done_callback(self._pending_log_event_tasks.discard)

    async def _flush_pending_log_events(self) -> None:
        if not self._pending_log_event_tasks:
            return

        await asyncio.gather(
            *tuple(self._pending_log_event_tasks),
            return_exceptions=True,
        )
        self._pending_log_event_tasks.clear()

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval_seconds)
                local_health_failures = (
                    await self.resource_manager.probe_local_health()
                )
                if local_health_failures:
                    self._local_unhealthy_error = local_health_failures[0]
                    self._worker_state_sink.touch('degraded')
                else:
                    self._worker_state_sink.touch('alive')
                self._worker_state_sink.sync_runtime_state(
                    self.resource_manager,
                )
                await self._domain_event_stream.emit(
                    WorkerHeartbeatEvent(
                        worker_id=self.worker_id,
                        status=(
                            'degraded' if local_health_failures else 'alive'
                        ),
                        metadata=DomainEventMetadata(
                            session_id=self.session_id,
                            worker_id=self.worker_id,
                        ),
                    ),
                )
        except asyncio.CancelledError:
            raise

    async def _prepare_engine_nodes(
        self,
        engine,
        snapshots: list[TestExecutionNodeSnapshot],
    ) -> tuple[object, ...]:
        tests: list[object] = []
        content_snapshots = [
            snapshot
            for snapshot in snapshots
            if snapshot.source_content is not None
        ]
        if content_snapshots:
            if not hasattr(engine, 'load_tests_from_content'):
                msg = (
                    f'Engine {engine.name!r} does not support injected '
                    'execution content'
                )
                raise ValueError(msg)

            self._seed_content_step_directories(engine, content_snapshots)
            for snapshot in content_snapshots:
                loaded_tests = await engine.load_tests_from_content(
                    snapshot.source_content or '',
                    self.root_path / snapshot.test_path,
                )
                tests.extend(loaded_tests)

        file_paths = tuple(
            sorted(
                {
                    self.root_path / snapshot.test_path
                    for snapshot in snapshots
                    if snapshot.source_content is None
                },
            ),
        )
        if file_paths:
            collect_path = (
                file_paths[0] if len(file_paths) == 1 else file_paths
            )
            collector = getattr(engine, 'collector', None)
            if collector is not None:
                collector.skip_step_catalog_discovery = True
            try:
                await engine.collect(collect_path)
            finally:
                if collector is not None:
                    collector.skip_step_catalog_discovery = False
            tests.extend(engine.get_collected_tests())

        return tuple(tests)

    def _seed_content_step_directories(
        self,
        engine,
        snapshots: list[TestExecutionNodeSnapshot],
    ) -> None:
        collector = getattr(engine, 'collector', None)
        step_directories = getattr(collector, 'steps_directories', None)
        if step_directories is None:
            return

        resolved_step_directories = {
            _resolve_snapshot_step_directory(self.root_path, step_directory)
            for snapshot in snapshots
            for step_directory in snapshot.step_directories
        }
        step_directories.update(resolved_step_directories)

    def _register_prepared_nodes(
        self,
        engine,
        snapshots: list[TestExecutionNodeSnapshot],
        assigned_tests: tuple[object, ...],
    ) -> None:
        nodes_by_stable_id: dict[str, TestExecutionNode] = {}
        path_indexes: dict[str, int] = defaultdict(int)

        for test in assigned_tests:
            test_path = build_test_path_label(self.root_path, test.path)
            index = path_indexes[test_path]
            path_indexes[test_path] += 1
            node = TestExecutionNode(
                id=build_execution_node_id(engine.name, test_path, index),
                stable_id=build_execution_node_stable_id(
                    self.root_path,
                    engine.name,
                    test,
                ),
                engine=engine,
                test=test,
                engine_name=engine.name,
                test_name=repr(test),
                test_path=test_path,
                resource_requirements=test.get_resource_requirements(),
            )
            nodes_by_stable_id[node.stable_id] = node
            self._prepared_nodes_by_id[node.id] = node
            self._prepared_nodes_by_stable_id[node.stable_id] = node

        for snapshot in snapshots:
            node = nodes_by_stable_id.get(snapshot.stable_id)
            if node is None:
                msg = (
                    'Could not bootstrap execution node '
                    f'{snapshot.id!r} ({snapshot.stable_id!r})'
                )
                raise ValueError(msg)

            node = replace(
                node,
                required_step_texts=snapshot.required_step_texts,
                step_candidate_files=snapshot.step_candidate_files,
            )

            nodes_by_stable_id[snapshot.stable_id] = node
            self._prepared_nodes_by_id[node.id] = node
            self._prepared_nodes_by_stable_id[node.stable_id] = node
            self._prepared_nodes_by_id[snapshot.id] = node
            self._prepared_nodes_by_stable_id[snapshot.stable_id] = node

    def _find_prepared_execution_node(
        self,
        engine,
        request: ExecutionRequest,
    ) -> TestExecutionNode:
        self._prime_engine_execution_node(engine, request.node)
        node = self._prepared_nodes_by_id.get(
            request.node.id,
        ) or self._prepared_nodes_by_stable_id.get(request.node.stable_id)
        if node is not None:
            return node

        msg = f'Could not find execution node {request.node.id!r}'
        raise ValueError(msg)

    def _prime_engine_execution_nodes(
        self,
        engine,
        snapshots: list[TestExecutionNodeSnapshot],
    ) -> None:
        for snapshot in snapshots:
            self._prime_engine_execution_node(engine, snapshot)

    def _prime_engine_execution_node(
        self,
        engine,
        snapshot: TestExecutionNodeSnapshot,
    ) -> None:
        prime_execution_node = getattr(engine, 'prime_execution_node', None)
        if prime_execution_node is None:
            return

        prime_execution_node(snapshot)


async def _run_persistent_worker(
    *,
    cwd: Path,
    root_path: Path,
    session_id: str,
    worker_id: int,
) -> None:
    session = await _PersistentWorkerSession.start(
        cwd=cwd,
        root_path=root_path,
        session_id=session_id,
        worker_id=worker_id,
    )
    _emit_response(RuntimeReadyResponse())
    if sys.stderr is not None:
        sys.stdout = sys.stderr

    while True:
        raw_line = await asyncio.to_thread(sys.stdin.readline)
        if not raw_line:
            response = await session.close(metadata=RuntimeEnvelopeMetadata())
            _emit_response(response)
            return

        command: RuntimeCommand | None = None
        try:
            command = deserialize_runtime_command(
                decode_json_dict(raw_line),
            )
            response = await _dispatch_runtime_command(session, command)
        except Exception as error:
            response = _build_worker_error_response(
                error,
                metadata=(
                    _build_reply_metadata(command.metadata)
                    if command is not None
                    else RuntimeEnvelopeMetadata()
                ),
            )

        _emit_response(response)
        if isinstance(command, RuntimeShutdownCommand):
            return


async def _dispatch_runtime_command(
    session: _PersistentWorkerSession,
    command: RuntimeCommand,
) -> RuntimeResponse:
    if isinstance(command, RuntimeShutdownCommand):
        return await session.close(metadata=command.metadata)
    if isinstance(command, RuntimeSnapshotResourceTimingsCommand):
        return session.snapshot_resource_timings(metadata=command.metadata)
    if isinstance(command, RuntimeBootstrapCommand):
        return await session.bootstrap(
            command.bootstrap,
            metadata=command.metadata,
        )
    if isinstance(command, RuntimeExecuteCommand):
        return await session.execute(
            command.request,
            metadata=command.metadata,
        )

    msg = f'Unsupported runtime command: {type(command).__name__}'
    raise ValueError(msg)


def _build_worker_error_response(
    error: Exception,
    *,
    metadata: RuntimeEnvelopeMetadata,
) -> RuntimeResponse:
    code = getattr(error, 'code', None)
    if not isinstance(code, str) or not code:
        code = (
            'worker_local_unhealthy'
            if bool(getattr(error, 'unhealthy', False))
            else 'worker_command_failed'
        )

    return build_runtime_protocol_error(
        code=code,
        message=str(error),
        recoverable=bool(getattr(error, 'recoverable', False)),
        fatal=bool(getattr(error, 'fatal', True)),
        metadata=metadata,
    )


def _build_reply_metadata(
    request_metadata: RuntimeEnvelopeMetadata,
) -> RuntimeEnvelopeMetadata:
    return RuntimeEnvelopeMetadata(
        correlation_id=request_metadata.correlation_id,
        in_reply_to=request_metadata.message_id,
        session_id=request_metadata.session_id,
        trace_id=request_metadata.trace_id,
    )


def _find_engine(
    engines: dict[str, object],
    engine_name: str,
) -> tuple[str, object]:
    for engine_path, engine in engines.items():
        if engine.name == engine_name:
            return engine_path, engine

    msg = f'Could not find engine {engine_name!r}'
    raise ValueError(msg)


def _resolve_snapshot_step_directory(
    root_path: Path,
    step_directory: str,
) -> Path:
    path = Path(step_directory)
    return path if path.is_absolute() else root_path / path


def _find_execution_node(
    engine,
    root_path: Path,
    request: ExecutionRequest,
) -> TestExecutionNode:
    for index, test in enumerate(engine.get_collected_tests()):
        test_path = build_test_path_label(root_path, test.path)
        node_id = build_execution_node_id(engine.name, test_path, index)
        stable_id = build_execution_node_stable_id(
            root_path,
            engine.name,
            test,
        )
        if node_id != request.node.id and stable_id != request.node.stable_id:
            continue

        return TestExecutionNode(
            id=node_id,
            stable_id=stable_id,
            engine=engine,
            test=test,
            engine_name=engine.name,
            test_name=repr(test),
            test_path=test_path,
            resource_requirements=test.get_resource_requirements(),
        )

    msg = f'Could not find execution node {request.node.id!r}'
    raise ValueError(msg)


def main() -> None:
    parser = argparse.ArgumentParser(description='Cosecha process worker')
    parser.add_argument('request_path', type=Path, nargs='?')
    parser.add_argument('response_path', type=Path, nargs='?')
    parser.add_argument('--persistent', action='store_true', default=False)
    parser.add_argument('--cwd', type=Path)
    parser.add_argument('--root-path', type=Path)
    parser.add_argument('--session-id', type=str)
    parser.add_argument('--worker-id', type=int, default=0)
    args = parser.parse_args()

    if args.persistent:
        if (
            args.cwd is None
            or args.root_path is None
            or args.session_id is None
        ):
            msg = '--persistent requires --cwd, --root-path and --session-id'
            raise ValueError(msg)

        asyncio.run(
            _run_persistent_worker(
                cwd=args.cwd,
                root_path=args.root_path,
                session_id=args.session_id,
                worker_id=args.worker_id,
            ),
        )
        return

    if args.request_path is None or args.response_path is None:
        msg = (
            'request_path and response_path are required without --persistent'
        )
        raise ValueError(msg)

    asyncio.run(_run_worker(args.request_path, args.response_path))


if __name__ == '__main__':
    main()
