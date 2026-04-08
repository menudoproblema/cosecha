from __future__ import annotations

import asyncio
import io
import logging
import pytest

from dataclasses import replace
from typing import TYPE_CHECKING

from cosecha.core.config import Config
from cosecha.core.capabilities import (
    CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
    CapabilityAttribute,
    CapabilityDescriptor,
)
from cosecha.core.domain_events import (
    DomainEventMetadata,
    LogChunkEvent,
    ResourceLifecycleEvent,
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
from cosecha.core.items import TestResultStatus
from cosecha.core.reporting_ir import TestReport
from cosecha.core.resources import (
    ResourceError,
    ResourceManager,
    ResourceRequirement,
)
from cosecha.core.runtime import ExecutionBodyResult
from cosecha.core.runtime_protocol import RuntimeEnvelopeMetadata
from cosecha.core.runtime_worker import (
    _build_worker_ephemeral_capabilities,
    _dispatch_runtime_command,
    _emit_response,
    _find_engine,
    _find_execution_node,
    _resolve_snapshot_step_directory,
    _run_persistent_worker,
    _run_worker,
    _PersistentWorkerSession,
    _binding_worker_shadow,
    _build_worker_error_response,
    _resolve_worker_state_root,
    _RuntimeResponseEventSink,
    _WorkerStateRegistrySink,
    main,
)
from cosecha.core.serialization import (
    decode_json_dict,
    encode_json_bytes,
    encode_json_text,
)
from cosecha.core.shadow import (
    EphemeralArtifactCapability,
    acquire_shadow_handle,
    get_active_shadow,
)


if TYPE_CHECKING:
    from pathlib import Path


WORKER_ID = 3


class _ShadowAwareWorkerComponent:
    COSECHA_COMPONENT_ID = 'cosecha.engine.shadow-aware-worker'

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name=CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='component_id',
                        value=self.COSECHA_COMPONENT_ID,
                    ),
                    CapabilityAttribute(
                        name='ephemeral_domain',
                        value='runtime',
                    ),
                ),
            ),
        )


def test_worker_state_registry_sink_persists_runtime_snapshots(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / 'worker-state.json'
    sink = _WorkerStateRegistrySink(state_path, worker_id=WORKER_ID)
    manager = ResourceManager()

    async def _run() -> None:
        await manager.acquire_for_test(
            'node-1',
            (
                ResourceRequirement(
                    name='session_db',
                    scope='run',
                    setup=lambda: {'dsn': 'mongo://localhost/test'},
                ),
            ),
        )

    asyncio.run(_run())
    sink.sync_runtime_state(manager)

    payload = decode_json_dict(state_path.read_bytes())

    assert payload['worker_id'] == WORKER_ID
    assert payload['status'] == 'ready'
    assert payload['unhealthy_resources'] == []
    assert payload['readiness_states'] == [
        {
            'name': 'session_db',
            'reason': None,
            'scope': 'run',
            'status': 'ready',
        },
    ]
    assert payload['resource_timings'][0]['name'] == 'session_db'
    assert payload['resource_timings'][0]['scope'] == 'run'


def test_worker_state_registry_sink_tracks_pending_and_active_resources(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / 'worker-state.json'
    sink = _WorkerStateRegistrySink(state_path, worker_id=WORKER_ID)

    sink.record_resource_state(
        action='pending',
        name='browser',
        scope='worker',
        external_handle='browser-1',
    )
    pending_payload = decode_json_dict(state_path.read_bytes())

    assert pending_payload['pending_resources'] == [
        {
            'external_handle': 'browser-1',
            'name': 'browser',
            'scope': 'worker',
        },
    ]

    sink.record_resource_state(
        action='acquired',
        name='browser',
        scope='worker',
        external_handle='browser-1',
    )
    active_payload = decode_json_dict(state_path.read_bytes())

    assert active_payload['pending_resources'] == []
    assert active_payload['active_resources'] == [
        {
            'external_handle': 'browser-1',
            'name': 'browser',
            'scope': 'worker',
        },
    ]


def test_build_worker_error_response_preserves_typed_error_code() -> None:
    response = _build_worker_error_response(
        ResourceError(
            'session_db',
            'health check failed',
            code='resource_health_check_failed',
        ),
        metadata=RuntimeEnvelopeMetadata(in_reply_to='request-1'),
    )

    assert response.error.code == 'resource_health_check_failed'
    assert response.error.fatal is False
    assert response.error.recoverable is False
    assert response.metadata.in_reply_to == 'request-1'


def test_build_worker_error_response_uses_local_unhealthy_fallback() -> None:
    class LocalUnhealthyError(RuntimeError):
        unhealthy = True

    response = _build_worker_error_response(
        LocalUnhealthyError('disk full'),
        metadata=RuntimeEnvelopeMetadata(),
    )

    assert response.error.code == 'worker_local_unhealthy'


def test_resolve_worker_state_root_honors_environment_override(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / 'shadow' / 'runtime'
    monkeypatch.setenv('COSECHA_RUNTIME_STATE_DIR', str(state_root))

    assert _resolve_worker_state_root(tmp_path) == state_root.resolve()


def test_binding_worker_shadow_binds_shadow_and_component_grants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    shadow_root = tmp_path / 'shadow' / 'session-1'
    knowledge_storage_root = tmp_path / '.cosecha'
    monkeypatch.setenv('COSECHA_SHADOW_ROOT', str(shadow_root))
    monkeypatch.setenv(
        'COSECHA_KNOWLEDGE_STORAGE_ROOT',
        str(knowledge_storage_root),
    )

    component = _ShadowAwareWorkerComponent()

    with _binding_worker_shadow(
        granted_capabilities=tuple(),
    ):
        with pytest.raises(PermissionError):
            acquire_shadow_handle(component.COSECHA_COMPONENT_ID)

    with _binding_worker_shadow(
        granted_capabilities=(
            EphemeralArtifactCapability(
                component_id=component.COSECHA_COMPONENT_ID,
                ephemeral_domain='runtime',
            ),
        ),
    ):
        handle = acquire_shadow_handle(component.COSECHA_COMPONENT_ID)

        assert handle.ephemeral_root == (
            shadow_root.resolve()
            / 'runtime'
            / component.COSECHA_COMPONENT_ID
        )

    with _binding_worker_shadow((component,)):
        shadow = get_active_shadow()
        handle = acquire_shadow_handle(component.COSECHA_COMPONENT_ID)

        assert shadow.root_path == shadow_root.resolve()
        assert shadow.knowledge_storage_root == knowledge_storage_root.resolve()
        assert handle.ephemeral_root == (
            shadow.runtime_component_dir(component.COSECHA_COMPONENT_ID)
        )


class _DummyTest:
    def __init__(self, path: Path, name: str = 'dummy-test') -> None:
        self.path = path
        self._name = name

    def __repr__(self) -> str:
        return self._name

    def get_resource_requirements(self) -> tuple[ResourceRequirement, ...]:
        return ()


class _DummyCollector:
    def __init__(self) -> None:
        self.skip_step_catalog_discovery = False
        self.steps_directories: set[Path] = set()


class _DummyEngine:
    def __init__(
        self,
        name: str,
        tests: tuple[_DummyTest, ...] = (),
    ) -> None:
        self.name = name
        self._tests = list(tests)
        self.collector = _DummyCollector()
        self.initialized: list[tuple[Config, str]] = []
        self.bound_streams: list[object] = []
        self.collect_calls: list[object] = []
        self.start_session_calls = 0
        self.finish_session_calls = 0
        self.prime_calls: list[TestExecutionNodeSnapshot] = []
        self.loaded_from_content_calls: list[tuple[str, Path]] = []

    def initialize(self, config: Config, engine_path: str) -> None:
        self.initialized.append((config, engine_path))
        self.config = config

    def bind_domain_event_stream(self, stream) -> None:
        self.bound_streams.append(stream)

    async def collect(self, test_path) -> None:
        self.collect_calls.append(test_path)

    async def start_session(self) -> None:
        self.start_session_calls += 1

    async def finish_session(self) -> None:
        self.finish_session_calls += 1

    def get_collected_tests(self) -> tuple[_DummyTest, ...]:
        return tuple(self._tests)

    async def load_tests_from_content(
        self,
        source_content: str,
        test_path: Path,
    ) -> tuple[_DummyTest, ...]:
        self.loaded_from_content_calls.append((source_content, test_path))
        return (_DummyTest(test_path, name='injected'),)

    def prime_execution_node(self, snapshot: TestExecutionNodeSnapshot) -> None:
        self.prime_calls.append(snapshot)


def _build_snapshot(
    root_path: Path,
    test: _DummyTest,
    *,
    engine_name: str = 'dummy',
    index: int = 0,
    source_content: str | None = None,
    step_directories: tuple[str, ...] = (),
) -> TestExecutionNodeSnapshot:
    test_path_label = build_test_path_label(root_path, test.path)
    return TestExecutionNodeSnapshot(
        id=build_execution_node_id(engine_name, test_path_label, index),
        stable_id=build_execution_node_stable_id(root_path, engine_name, test),
        engine_name=engine_name,
        test_name=repr(test),
        test_path=test_path_label,
        source_content=source_content,
        step_directories=step_directories,
    )


def _build_request(
    root_path: Path,
    snapshot: TestExecutionNodeSnapshot,
) -> ExecutionRequest:
    config = Config(root_path=root_path)
    return ExecutionRequest(
        cwd=str(root_path),
        root_path=str(root_path),
        config_snapshot=config.snapshot(),
        node=snapshot,
    )


def _build_execution_result(path: str) -> ExecutionBodyResult:
    return ExecutionBodyResult(
        report=TestReport(
            path=path,
            status=TestResultStatus.PASSED,
            message=None,
            duration=0.01,
        ),
        phase_durations={'run': 0.01},
    )


def test_runtime_response_event_sink_streams_domain_and_log_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata: RuntimeEnvelopeMetadata | None = None
    streamed_event_ids: set[str] = set()
    emitted = []
    sink = _RuntimeResponseEventSink(
        lambda: metadata,
        streamed_event_ids,
    )

    monkeypatch.setattr(
        'cosecha.core.runtime_worker._emit_response',
        emitted.append,
    )

    async def _run() -> None:
        await sink.emit(
            LogChunkEvent(
                message='hello',
                level='info',
                logger_name='worker',
                metadata=DomainEventMetadata(),
            ),
        )
        assert emitted == []

        nonlocal metadata
        metadata = RuntimeEnvelopeMetadata(
            correlation_id='corr-1',
            session_id='session-1',
        )
        await sink.emit(
            LogChunkEvent(
                message='stream now',
                level='warning',
                logger_name='worker',
                metadata=DomainEventMetadata(),
            ),
        )
        await sink.close()

    asyncio.run(_run())

    assert len(emitted) == 1
    assert emitted[0].stream_kind == 'log'
    assert len(streamed_event_ids) == 1


def test_run_worker_executes_request_and_writes_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_path = tmp_path / 'sample.feature'
    test_path.write_text('Feature: runtime worker\n', encoding='utf-8')
    test = _DummyTest(test_path)
    snapshot = _build_snapshot(tmp_path, test)
    request = _build_request(tmp_path, snapshot)

    request_path = tmp_path / 'request.json'
    response_path = tmp_path / 'response.json'
    request_path.write_bytes(encode_json_bytes(request.to_dict()))

    engine = _DummyEngine('dummy', (test,))

    async def _fake_execute_test_body(node, resource_manager, options):
        del node, resource_manager, options
        return _build_execution_result(path='sample.feature')

    monkeypatch.setattr(
        'cosecha.core.runtime_worker.setup_engines',
        lambda _config: ((), {'dummy.py': engine}),
    )
    monkeypatch.setattr(
        'cosecha.core.runtime_worker.execute_test_body',
        _fake_execute_test_body,
    )

    asyncio.run(_run_worker(request_path, response_path))
    payload = decode_json_dict(response_path.read_bytes())

    assert payload['report']['path'] == 'sample.feature'
    assert payload['report']['status'] == TestResultStatus.PASSED.value
    assert payload['phase_durations'] == {'run': 0.01}
    assert engine.start_session_calls == 1
    assert engine.finish_session_calls == 1
    assert engine.collect_calls == [test_path]


def test_persistent_worker_session_bootstrap_execute_and_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_path = tmp_path / 'session.feature'
    test_path.write_text('Feature: persistent session\n', encoding='utf-8')
    test = _DummyTest(test_path)
    snapshot = _build_snapshot(tmp_path, test)
    bootstrap = ExecutionBootstrap(
        config_snapshot=Config(root_path=tmp_path).snapshot(),
        nodes=(snapshot,),
    )
    request = _build_request(tmp_path, snapshot)
    engine = _DummyEngine('dummy', (test,))

    async def _fake_execute_test_body(node, resource_manager, options):
        del node, resource_manager, options
        return _build_execution_result(path='session.feature')

    monkeypatch.setattr(
        'cosecha.core.runtime_worker.execute_test_body',
        _fake_execute_test_body,
    )

    metadata = RuntimeEnvelopeMetadata(
        message_id='m-1',
        correlation_id='c-1',
        session_id='session-1',
        trace_id='trace-1',
    )

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(
                legacy_session_scope='worker',
                mark_local_failures=True,
            ),
            session_id='session-1',
            worker_id=1,
        )
        bootstrap_response = await session.bootstrap(
            bootstrap,
            metadata=metadata,
        )
        execute_response = await session.execute(
            request,
            metadata=metadata,
        )
        shutdown_response = await session.close(metadata=metadata)

        assert bootstrap_response.response_type == 'bootstrap'
        assert execute_response.response_type == 'execute'
        assert shutdown_response.response_type == 'shutdown'

    asyncio.run(_run())

    assert engine.start_session_calls == 1
    assert engine.finish_session_calls == 1
    assert engine.collect_calls == [test_path]
    assert engine.prime_calls == [snapshot, snapshot]


def test_persistent_worker_session_execute_propagates_local_unhealthy_error(
    tmp_path: Path,
) -> None:
    test = _DummyTest(tmp_path / 'unhealthy.feature')
    snapshot = _build_snapshot(tmp_path, test)
    request = _build_request(tmp_path, snapshot)
    engine = _DummyEngine('dummy', (test,))

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(
                legacy_session_scope='worker',
                mark_local_failures=True,
            ),
            session_id='session-2',
            worker_id=2,
        )
        session._local_unhealthy_error = RuntimeError('disk unavailable')
        with pytest.raises(RuntimeError, match='disk unavailable'):
            await session.execute(
                request,
                metadata=RuntimeEnvelopeMetadata(session_id='session-2'),
            )
        await session.close(metadata=RuntimeEnvelopeMetadata(session_id='session-2'))

    asyncio.run(_run())


def test_persistent_worker_prepare_nodes_supports_content_and_collect_paths(
    tmp_path: Path,
) -> None:
    file_path = tmp_path / 'file.feature'
    file_path.write_text('Feature: from file\n', encoding='utf-8')
    injected_path = tmp_path / 'injected.feature'
    injected_test = _DummyTest(injected_path, name='injected')
    file_test = _DummyTest(file_path, name='file')

    engine = _DummyEngine('dummy', (file_test,))
    snapshots = [
        _build_snapshot(
            tmp_path,
            injected_test,
            source_content='Feature: injected',
            step_directories=('steps',),
        ),
        _build_snapshot(tmp_path, file_test),
    ]

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(
                legacy_session_scope='worker',
                mark_local_failures=True,
            ),
            session_id='session-3',
            worker_id=3,
        )
        tests = await session._prepare_engine_nodes(engine, snapshots)
        assert {repr(test) for test in tests} == {'injected', 'file'}
        assert engine.collector.skip_step_catalog_discovery is False
        assert engine.collect_calls == [file_path]
        assert engine.loaded_from_content_calls == [
            ('Feature: injected', injected_path),
        ]
        await session.close(metadata=RuntimeEnvelopeMetadata(session_id='session-3'))

    asyncio.run(_run())
    assert tmp_path / 'steps' in engine.collector.steps_directories


def test_prepare_engine_nodes_requires_content_loader(
    tmp_path: Path,
) -> None:
    class _NoContentEngine:
        def __init__(self, name: str, tests: tuple[_DummyTest, ...]) -> None:
            self.name = name
            self._tests = tests
            self.collector = _DummyCollector()

        async def collect(self, _test_path) -> None:
            return None

        def get_collected_tests(self) -> tuple[_DummyTest, ...]:
            return self._tests

    test = _DummyTest(tmp_path / 'content.feature')
    engine = _NoContentEngine('dummy', (test,))
    snapshots = [
        _build_snapshot(
            tmp_path,
            test,
            source_content='Feature: dynamic',
        ),
    ]

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(
                legacy_session_scope='worker',
                mark_local_failures=True,
            ),
            session_id='session-4',
            worker_id=4,
        )
        with pytest.raises(ValueError, match='does not support injected'):
            await session._prepare_engine_nodes(
                engine,
                snapshots,
            )
        await session.close(metadata=RuntimeEnvelopeMetadata(session_id='session-4'))

    asyncio.run(_run())


def test_register_and_resolve_prepared_execution_nodes(
    tmp_path: Path,
) -> None:
    test = _DummyTest(tmp_path / 'prepared.feature')
    engine = _DummyEngine('dummy', (test,))
    snapshot = _build_snapshot(tmp_path, test)
    request = _build_request(
        tmp_path,
        replace(snapshot, id='dummy:prepared.feature:999'),
    )

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(
                legacy_session_scope='worker',
                mark_local_failures=True,
            ),
            session_id='session-5',
            worker_id=5,
        )
        session._register_prepared_nodes(engine, [snapshot], (test,))
        node = session._find_prepared_execution_node(engine, request)
        assert node.stable_id == snapshot.stable_id
        with pytest.raises(ValueError, match='Could not find execution node'):
            session._find_prepared_execution_node(
                engine,
                _build_request(
                    tmp_path,
                    replace(
                        snapshot,
                        id='missing-id',
                        stable_id='missing-stable-id',
                    ),
                ),
            )
        await session.close(metadata=RuntimeEnvelopeMetadata(session_id='session-5'))

    asyncio.run(_run())


def test_register_prepared_nodes_raises_when_snapshot_is_missing(
    tmp_path: Path,
) -> None:
    test = _DummyTest(tmp_path / 'first.feature')
    engine = _DummyEngine('dummy', (test,))
    snapshot = _build_snapshot(
        tmp_path,
        _DummyTest(tmp_path / 'other.feature'),
    )

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(
                legacy_session_scope='worker',
                mark_local_failures=True,
            ),
            session_id='session-6',
            worker_id=6,
        )
        with pytest.raises(ValueError, match='Could not bootstrap execution node'):
            session._register_prepared_nodes(engine, [snapshot], (test,))
        await session.close(metadata=RuntimeEnvelopeMetadata(session_id='session-6'))

    asyncio.run(_run())


def test_runtime_worker_helpers_find_engine_and_execution_nodes(
    tmp_path: Path,
) -> None:
    test = _DummyTest(tmp_path / 'helper.feature')
    engine = _DummyEngine('dummy', (test,))
    snapshot = _build_snapshot(tmp_path, test)
    request = _build_request(tmp_path, snapshot)

    engine_path, found_engine = _find_engine({'dummy.py': engine}, 'dummy')
    node = _find_execution_node(engine, tmp_path, request)

    assert engine_path == 'dummy.py'
    assert found_engine is engine
    assert node.id == snapshot.id
    assert _resolve_snapshot_step_directory(tmp_path, 'steps') == (
        tmp_path / 'steps'
    )

    absolute_steps = (tmp_path / 'abs').resolve()
    assert _resolve_snapshot_step_directory(tmp_path, str(absolute_steps)) == (
        absolute_steps
    )

    with pytest.raises(ValueError, match='Could not find engine'):
        _find_engine({'dummy.py': engine}, 'other')
    with pytest.raises(ValueError, match='Could not find execution node'):
        _find_execution_node(
            engine,
            tmp_path,
            _build_request(
                tmp_path,
                replace(
                    snapshot,
                    id='missing-id',
                    stable_id='missing-stable-id',
                ),
            ),
        )


def test_dispatch_runtime_command_routes_to_session_methods(
    tmp_path: Path,
) -> None:
    test = _DummyTest(tmp_path / 'dispatch.feature')
    snapshot = _build_snapshot(tmp_path, test)
    request = _build_request(tmp_path, snapshot)
    bootstrap = ExecutionBootstrap(
        config_snapshot=Config(root_path=tmp_path).snapshot(),
        nodes=(snapshot,),
    )
    calls: list[str] = []

    class _Session:
        async def close(self, *, metadata):
            del metadata
            calls.append('close')
            return _build_worker_error_response(
                RuntimeError('closed'),
                metadata=RuntimeEnvelopeMetadata(),
            )

        def snapshot_resource_timings(self, *, metadata):
            del metadata
            calls.append('snapshot')
            return _build_worker_error_response(
                RuntimeError('snapshot'),
                metadata=RuntimeEnvelopeMetadata(),
            )

        async def bootstrap(self, bootstrap_value, *, metadata):
            del bootstrap_value, metadata
            calls.append('bootstrap')
            return _build_worker_error_response(
                RuntimeError('bootstrap'),
                metadata=RuntimeEnvelopeMetadata(),
            )

        async def execute(self, request_value, *, metadata):
            del request_value, metadata
            calls.append('execute')
            return _build_worker_error_response(
                RuntimeError('execute'),
                metadata=RuntimeEnvelopeMetadata(),
            )

    from cosecha.core.runtime_protocol import (
        RuntimeBootstrapCommand,
        RuntimeExecuteCommand,
        RuntimeShutdownCommand,
        RuntimeSnapshotResourceTimingsCommand,
    )

    async def _run() -> None:
        session = _Session()
        await _dispatch_runtime_command(session, RuntimeShutdownCommand())
        await _dispatch_runtime_command(
            session,
            RuntimeSnapshotResourceTimingsCommand(),
        )
        await _dispatch_runtime_command(
            session,
            RuntimeBootstrapCommand(bootstrap),
        )
        await _dispatch_runtime_command(
            session,
            RuntimeExecuteCommand(request),
        )
        with pytest.raises(ValueError, match='Unsupported runtime command'):
            await _dispatch_runtime_command(
                session,
                object(),  # type: ignore[arg-type]
            )

    asyncio.run(_run())
    assert calls == ['close', 'snapshot', 'bootstrap', 'execute']


def test_run_persistent_worker_emits_ready_and_shutdown_responses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emitted = []

    class _Session:
        async def close(self, *, metadata):
            del metadata
            from cosecha.core.runtime_protocol import RuntimeShutdownResponse

            return RuntimeShutdownResponse()

    async def _fake_start(**_kwargs):
        return _Session()

    from cosecha.core.runtime_protocol import RuntimeShutdownCommand

    monkeypatch.setattr(
        'cosecha.core.runtime_worker._PersistentWorkerSession.start',
        _fake_start,
    )
    monkeypatch.setattr(
        'cosecha.core.runtime_worker._emit_response',
        emitted.append,
    )
    monkeypatch.setattr(
        'sys.stdin',
        io.StringIO(
            f'{encode_json_text(RuntimeShutdownCommand().to_dict())}\n',
        ),
    )
    monkeypatch.setattr('sys.stderr', io.StringIO())

    asyncio.run(
        _run_persistent_worker(
            cwd=tmp_path,
            root_path=tmp_path,
            session_id='session-main',
            worker_id=7,
        ),
    )

    assert emitted[0].response_type == 'ready'
    assert emitted[1].response_type == 'shutdown'


def test_main_routes_worker_modes_and_validates_required_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    async def _fake_run_worker(request_path: Path, response_path: Path) -> None:
        calls.append(('single', (request_path, response_path)))

    async def _fake_run_persistent_worker(  # noqa: PLR0913
        *,
        cwd: Path,
        root_path: Path,
        session_id: str,
        worker_id: int,
    ) -> None:
        calls.append(
            (
                'persistent',
                (cwd, root_path, session_id, worker_id),
            ),
        )

    monkeypatch.setattr(
        'cosecha.core.runtime_worker._run_worker',
        _fake_run_worker,
    )
    monkeypatch.setattr(
        'cosecha.core.runtime_worker._run_persistent_worker',
        _fake_run_persistent_worker,
    )

    request_path = tmp_path / 'request.json'
    response_path = tmp_path / 'response.json'
    request_path.write_text('{}', encoding='utf-8')
    response_path.write_text('{}', encoding='utf-8')

    monkeypatch.setattr(
        'sys.argv',
        [
            'runtime_worker.py',
            str(request_path),
            str(response_path),
        ],
    )
    main()

    monkeypatch.setattr(
        'sys.argv',
        ['runtime_worker.py', '--persistent', '--cwd', str(tmp_path)],
    )
    with pytest.raises(ValueError, match='--persistent requires'):
        main()

    monkeypatch.setattr(
        'sys.argv',
        [
            'runtime_worker.py',
            '--persistent',
            '--cwd',
            str(tmp_path),
            '--root-path',
            str(tmp_path),
            '--session-id',
            'session-8',
            '--worker-id',
            '8',
        ],
    )
    main()

    assert calls == [
        ('single', (request_path, response_path)),
        ('persistent', (tmp_path, tmp_path, 'session-8', 8)),
    ]


def test_main_requires_paths_without_persistent_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr('sys.argv', ['runtime_worker.py'])
    with pytest.raises(
        ValueError,
        match='request_path and response_path are required without --persistent',
    ):
        main()


def test_build_worker_ephemeral_capabilities_handles_invalid_components(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _InvalidComponent:
        def describe_capabilities(self):
            return ()

    class _NoCapabilityComponent:
        COSECHA_COMPONENT_ID = 'cosecha.engine.no-capability'

        def describe_capabilities(self):
            return ()

    monkeypatch.setattr(
        'cosecha.core.runtime_worker.component_id_from_component_type',
        lambda component_type: (
            (_ for _ in ()).throw(RuntimeError('bad id'))
            if component_type is _InvalidComponent
            else _NoCapabilityComponent.COSECHA_COMPONENT_ID
        ),
    )
    monkeypatch.setattr(
        'cosecha.core.runtime_worker.build_ephemeral_artifact_capability',
        lambda *_args, **_kwargs: None,
    )

    capabilities = _build_worker_ephemeral_capabilities(
        (_InvalidComponent(), _NoCapabilityComponent(), object()),
    )
    assert capabilities == {}


def test_worker_state_sink_resource_event_branches(
    tmp_path: Path,
) -> None:
    sink = _WorkerStateRegistrySink(tmp_path / 'worker-state.json', worker_id=9)

    async def _run() -> None:
        await sink.emit(
            LogChunkEvent(
                message='not-a-resource-event',
                level='info',
                logger_name='worker',
                metadata=DomainEventMetadata(),
            ),
        )
        await sink.emit(
            ResourceLifecycleEvent(
                action='acquired',
                name='db',
                scope='worker',
                external_handle=None,
                metadata=DomainEventMetadata(),
            ),
        )

    asyncio.run(_run())
    sink.record_resource_state('released', 'db', 'worker', 'h-1')
    sink.record_resource_state('pending', 'cache', 'worker', None)
    sink.record_resource_state('pending_cleared', 'cache', 'worker', None)
    sink.record_resource_state('unknown', 'cache', 'worker', None)
    asyncio.run(sink.close())

    payload = decode_json_dict((tmp_path / 'worker-state.json').read_bytes())
    assert payload['active_resources'] == []
    assert payload['pending_resources'] == []


def test_emit_response_writes_single_json_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = io.StringIO()
    monkeypatch.setattr('cosecha.core.runtime_worker._CONTROL_STDOUT', stream)
    from cosecha.core.runtime_protocol import RuntimeReadyResponse

    _emit_response(RuntimeReadyResponse())
    lines = stream.getvalue().splitlines()

    assert len(lines) == 1
    payload = decode_json_dict(lines[0])
    assert payload['response_type'] == 'ready'


def test_persistent_worker_session_start_classmethod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _DummyEngine('dummy')
    changed_cwd = {'value': None}

    monkeypatch.setattr(
        'cosecha.core.runtime_worker.setup_engines',
        lambda _config: ((), {'dummy.py': engine}),
    )
    monkeypatch.setattr(
        'cosecha.core.runtime_worker.os.chdir',
        lambda cwd: changed_cwd.update(value=cwd),
    )

    async def _run() -> None:
        session = await _PersistentWorkerSession.start(
            cwd=tmp_path,
            root_path=tmp_path,
            session_id='session-start',
            worker_id=10,
        )
        assert session.worker_id == 10
        assert changed_cwd['value'] == tmp_path
        await session.close(metadata=RuntimeEnvelopeMetadata(session_id='session-start'))

    asyncio.run(_run())


def test_persistent_worker_session_internal_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_path = tmp_path / 'helpers.feature'
    test_path.write_text('Feature: helpers\n', encoding='utf-8')
    test = _DummyTest(test_path)
    snapshot = _build_snapshot(tmp_path, test)
    bootstrap = ExecutionBootstrap(
        config_snapshot=Config(root_path=tmp_path).snapshot(),
        nodes=(snapshot,),
    )
    engine = _DummyEngine('dummy', (test,))
    emitted = []
    monkeypatch.setattr('cosecha.core.runtime_worker._emit_response', emitted.append)

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(legacy_session_scope='worker', mark_local_failures=True),
            session_id='session-helper',
            worker_id=11,
        )
        session._apply_config_snapshot(bootstrap.config_snapshot)
        session._apply_config_snapshot(bootstrap.config_snapshot)
        assert len(engine.initialized) == 1

        assert session._current_response_metadata() is None
        metadata = RuntimeEnvelopeMetadata(
            session_id='session-helper',
            correlation_id='corr',
            trace_id='trace',
        )
        session._active_response_metadata = metadata
        assert session._current_response_metadata() == metadata
        session._bind_resource_event_metadata(
            metadata=metadata,
            node_id='node-id',
            node_stable_id='stable-id',
            test_id='test-id',
        )
        provider = session.resource_manager._event_metadata_provider
        assert provider is not None
        event_metadata = provider(None, 'test', None)
        assert event_metadata.node_id == 'node-id'
        assert event_metadata.node_stable_id == 'stable-id'
        session._active_response_metadata = None

        session._start_log_capture()
        session._start_log_capture()
        session._active_response_metadata = metadata
        session._schedule_live_log_chunk_event(
            logging.LogRecord(
                name='cosecha.worker',
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg='hello worker',
                args=(),
                exc_info=None,
            ),
            'hello worker',
            context=SimpleNamespace(
                session_id='session-helper',
                trace_id='trace',
                node_id='node-id',
                node_stable_id='stable-id',
                worker_id=11,
            ),
        )
        await session._flush_pending_log_events()
        assert emitted
        session._active_response_metadata = None
        session._stop_log_capture()
        session._stop_log_capture()

        session._heartbeat_interval_seconds = 0

        async def _fake_probe_local_health(self):
            del self
            return (
                ResourceError(
                    'db',
                    'degraded',
                    code='resource_health_check_failed',
                ),
            )

        monkeypatch.setattr(
            ResourceManager,
            'probe_local_health',
            _fake_probe_local_health,
        )
        heartbeat_task = asyncio.create_task(session._heartbeat_loop())
        await asyncio.sleep(0)
        heartbeat_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await heartbeat_task

        empty_engine = _DummyEngine('dummy', ())
        empty_session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': empty_engine},
            ResourceManager(legacy_session_scope='worker', mark_local_failures=True),
            session_id='session-helper-empty',
            worker_id=12,
        )
        await empty_session.bootstrap(
            bootstrap,
            metadata=RuntimeEnvelopeMetadata(session_id='session-helper-empty'),
        )
        assert empty_engine.start_session_calls == 0
        await empty_session.close(
            metadata=RuntimeEnvelopeMetadata(session_id='session-helper-empty'),
        )

        assert session.snapshot_resource_timings(
            metadata=RuntimeEnvelopeMetadata(),
        ).response_type == 'snapshot_resource_timings'

        class _NoPrimeEngine:
            name = 'no-prime'

        session._prime_engine_execution_node(_NoPrimeEngine(), snapshot)
        await session.close(metadata=RuntimeEnvelopeMetadata(session_id='session-helper'))

    from types import SimpleNamespace

    asyncio.run(_run())


def test_run_persistent_worker_eof_and_shutdown_error_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emitted = []

    class _ClosingSession:
        async def close(self, *, metadata):
            del metadata
            from cosecha.core.runtime_protocol import RuntimeShutdownResponse

            return RuntimeShutdownResponse()

    class _FailingCloseSession:
        async def close(self, *, metadata):
            del metadata
            raise RuntimeError('shutdown failed')

    async def _start_closing(**_kwargs):
        return _ClosingSession()

    async def _start_failing(**_kwargs):
        return _FailingCloseSession()

    from cosecha.core.runtime_protocol import RuntimeShutdownCommand

    monkeypatch.setattr(
        'cosecha.core.runtime_worker._emit_response',
        emitted.append,
    )
    monkeypatch.setattr('sys.stderr', io.StringIO())

    monkeypatch.setattr(
        'cosecha.core.runtime_worker._PersistentWorkerSession.start',
        _start_closing,
    )
    monkeypatch.setattr('sys.stdin', io.StringIO(''))
    asyncio.run(
        _run_persistent_worker(
            cwd=tmp_path,
            root_path=tmp_path,
            session_id='session-eof',
            worker_id=12,
        ),
    )

    monkeypatch.setattr(
        'cosecha.core.runtime_worker._PersistentWorkerSession.start',
        _start_failing,
    )
    monkeypatch.setattr(
        'sys.stdin',
        io.StringIO(
            f'{encode_json_text(RuntimeShutdownCommand().to_dict())}\n',
        ),
    )
    asyncio.run(
        _run_persistent_worker(
            cwd=tmp_path,
            root_path=tmp_path,
            session_id='session-error',
            worker_id=13,
        ),
    )

    response_types = [response.response_type for response in emitted]
    assert response_types.count('ready') == 2
    assert 'shutdown' in response_types
    assert 'error' in response_types


def test_worker_state_sink_emit_tracks_acquired_and_released_resources(
    tmp_path: Path,
) -> None:
    sink = _WorkerStateRegistrySink(tmp_path / 'worker-state.json', worker_id=14)

    async def _run() -> None:
        await sink.emit(
            ResourceLifecycleEvent(
                action='acquired',
                name='db',
                scope='worker',
                external_handle='db-handle',
                metadata=DomainEventMetadata(),
            ),
        )
        await sink.emit(
            ResourceLifecycleEvent(
                action='released',
                name='db',
                scope='worker',
                external_handle='db-handle',
                metadata=DomainEventMetadata(),
            ),
        )

    asyncio.run(_run())
    payload = decode_json_dict((tmp_path / 'worker-state.json').read_bytes())
    assert payload['active_resources'] == []


def test_persistent_worker_take_buffered_domain_events_clears_streamed_ids(
    tmp_path: Path,
) -> None:
    engine = _DummyEngine('dummy')

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(legacy_session_scope='worker', mark_local_failures=True),
            session_id='session-stream',
            worker_id=15,
        )
        event = LogChunkEvent(
            message='chunk',
            level='info',
            logger_name='worker',
            metadata=DomainEventMetadata(),
        )
        session._domain_event_sink.events.append(event)
        events = session._take_buffered_domain_events()
        assert len(events) == 1
        assert session._streamed_event_ids == set()
        await session.close(metadata=RuntimeEnvelopeMetadata(session_id='session-stream'))

    asyncio.run(_run())


def test_persistent_worker_heartbeat_loop_marks_alive_without_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _DummyEngine('dummy')
    emitted_events = []

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(legacy_session_scope='worker', mark_local_failures=True),
            session_id='session-heartbeat',
            worker_id=16,
        )
        session._heartbeat_interval_seconds = 0

        async def _healthy_probe(self):
            del self
            return ()

        monkeypatch.setattr(ResourceManager, 'probe_local_health', _healthy_probe)

        async def _fake_emit(self, event):
            del self
            emitted_events.append(event)

        monkeypatch.setattr(
            'cosecha.core.domain_event_stream.DomainEventStream.emit',
            _fake_emit,
        )

        task = asyncio.create_task(session._heartbeat_loop())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert emitted_events
        await session.close(
            metadata=RuntimeEnvelopeMetadata(session_id='session-heartbeat'),
        )

    asyncio.run(_run())


def test_seed_content_step_directories_returns_when_collector_lacks_field(
    tmp_path: Path,
) -> None:
    engine = _DummyEngine('dummy')

    class _CollectorWithoutStepDirectories:
        skip_step_catalog_discovery = False

    engine.collector = _CollectorWithoutStepDirectories()
    snapshot = _build_snapshot(
        tmp_path,
        _DummyTest(tmp_path / 'content.feature'),
        source_content='Feature: inline',
        step_directories=('steps',),
    )

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(legacy_session_scope='worker', mark_local_failures=True),
            session_id='session-seed',
            worker_id=17,
        )
        session._seed_content_step_directories(engine, [snapshot])
        await session.close(metadata=RuntimeEnvelopeMetadata(session_id='session-seed'))

    asyncio.run(_run())


def test_persistent_worker_heartbeat_loop_marks_degraded_on_local_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _DummyEngine('dummy')

    async def _run() -> None:
        session = _PersistentWorkerSession(
            tmp_path,
            {'dummy.py': engine},
            ResourceManager(legacy_session_scope='worker', mark_local_failures=True),
            session_id='session-heartbeat-degraded',
            worker_id=18,
        )
        session._heartbeat_interval_seconds = 0

        async def _degraded_probe(self):
            del self
            return (
                ResourceError(
                    'db',
                    'health failed',
                    code='resource_health_check_failed',
                ),
            )

        async def _fake_emit(self, _event):
            del self, _event
            return None

        monkeypatch.setattr(ResourceManager, 'probe_local_health', _degraded_probe)
        monkeypatch.setattr(
            'cosecha.core.domain_event_stream.DomainEventStream.emit',
            _fake_emit,
        )

        task = asyncio.create_task(session._heartbeat_loop())
        await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert session._local_unhealthy_error is not None
        await session.close(
            metadata=RuntimeEnvelopeMetadata(
                session_id='session-heartbeat-degraded',
            ),
        )

    asyncio.run(_run())
