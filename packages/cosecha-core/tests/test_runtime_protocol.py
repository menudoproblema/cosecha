from __future__ import annotations

import pytest

from cosecha.core.config import ConfigSnapshot
from cosecha.core.domain_events import (
    DomainEventMetadata,
    LogChunkEvent,
    ResourceLifecycleEvent,
    serialize_domain_event,
)
from cosecha.core.execution_ir import (
    ExecutionBootstrap,
    ExecutionRequest,
    TestExecutionNodeSnapshot,
)
from cosecha.core.resources import ResourceTiming
from cosecha.core.runtime_protocol import (
    RUNTIME_PROTOCOL_VERSION,
    RuntimeBootstrapCommand,
    RuntimeEnvelopeMetadata,
    RuntimeErrorResponse,
    RuntimeEventResponse,
    RuntimeExecuteCommand,
    RuntimeExecuteResponse,
    RuntimeSnapshotResourceTimingsCommand,
    RuntimeSnapshotResourceTimingsResponse,
    RuntimeShutdownCommand,
    RuntimeShutdownResponse,
    RuntimeReadyResponse,
    build_runtime_protocol_error,
    cast_resource_lifecycle_action,
    cast_runtime_event_stream_kind,
    cast_optional_int,
    cast_optional_str,
    deserialize_resource_lifecycle_event,
    deserialize_runtime_command,
    deserialize_runtime_response,
)


def _build_snapshot() -> TestExecutionNodeSnapshot:
    return TestExecutionNodeSnapshot(
        id='dummy:test.feature:0',
        stable_id='dummy:test.feature:abc123',
        engine_name='dummy',
        test_name='test feature',
        test_path='test.feature',
    )


def _build_config_snapshot() -> ConfigSnapshot:
    return ConfigSnapshot(
        root_path='/workspace/project/tests',
        output_mode='summary',
        output_detail='standard',
        capture_log=False,
        stop_on_error=False,
        concurrency=1,
        strict_step_ambiguity=False,
    )


def test_runtime_protocol_roundtrips_bootstrap_and_execute_command() -> None:
    bootstrap = RuntimeBootstrapCommand(
        bootstrap=ExecutionBootstrap(
            config_snapshot=_build_config_snapshot(),
            nodes=(_build_snapshot(),),
        ),
        metadata=RuntimeEnvelopeMetadata(
            correlation_id='plan-1',
            session_id='session-1',
        ),
    )
    execute = RuntimeExecuteCommand(
        request=ExecutionRequest(
            cwd='/workspace/project',
            root_path='/workspace/project/tests',
            config_snapshot=_build_config_snapshot(),
            node=_build_snapshot(),
        ),
        metadata=RuntimeEnvelopeMetadata(
            correlation_id='dummy:test.feature:abc123',
            idempotency_key='dummy:test.feature:abc123',
            session_id='session-1',
        ),
    )

    restored_bootstrap = deserialize_runtime_command(bootstrap.to_dict())
    restored_execute = deserialize_runtime_command(execute.to_dict())

    assert isinstance(restored_bootstrap, RuntimeBootstrapCommand)
    assert restored_bootstrap.protocol_version == RUNTIME_PROTOCOL_VERSION
    assert restored_bootstrap.bootstrap.nodes[0].stable_id == (
        'dummy:test.feature:abc123'
    )
    assert isinstance(restored_execute, RuntimeExecuteCommand)
    assert restored_execute.request.node.id == 'dummy:test.feature:0'
    assert restored_execute.metadata.idempotency_key == (
        'dummy:test.feature:abc123'
    )


def test_runtime_protocol_roundtrips_execute_and_event_responses() -> None:
    execute_response = RuntimeExecuteResponse(
        report={
            'duration': 0.1,
            'exception_text': None,
            'message': 'ok',
            'path': 'test.feature',
            'status': 'passed',
        },
        phase_durations={'run': 0.05},
        resource_timings=(
            ResourceTiming(
                name='db',
                scope='worker',
                acquire_count=1,
                acquire_duration=0.2,
            ),
        ),
        metadata=RuntimeEnvelopeMetadata(
            correlation_id='dummy:test.feature:abc123',
            in_reply_to='request-1',
            session_id='session-1',
            trace_id='trace-1',
        ),
        domain_events=(
            LogChunkEvent(
                message='hello',
                level='info',
                logger_name='tests.runtime',
                metadata=DomainEventMetadata(
                    node_stable_id='dummy:test.feature:abc123',
                    session_id='session-1',
                    trace_id='trace-1',
                    worker_id=1,
                ),
            ),
        ),
        resource_events=(
            ResourceLifecycleEvent(
                action='acquired',
                external_handle='db:worker-1',
                name='db',
                scope='worker',
                test_id='dummy:test.feature:0',
                metadata=DomainEventMetadata(
                    node_id='dummy:test.feature:0',
                    node_stable_id='dummy:test.feature:abc123',
                    session_id='session-1',
                    trace_id='trace-1',
                    worker_id=1,
                ),
            ),
        ),
    )
    event_response = RuntimeEventResponse(
        event=LogChunkEvent(
            message='streamed',
            level='info',
            logger_name='tests.runtime',
            metadata=DomainEventMetadata(
                node_stable_id='dummy:test.feature:abc123',
                session_id='session-1',
                trace_id='trace-1',
                worker_id=1,
            ),
        ),
        stream_kind='log',
        metadata=RuntimeEnvelopeMetadata(
            correlation_id='dummy:test.feature:abc123',
            in_reply_to='request-1',
            session_id='session-1',
            trace_id='trace-1',
        ),
    )

    restored_execute = deserialize_runtime_response(execute_response.to_dict())
    restored_event = deserialize_runtime_response(event_response.to_dict())

    assert isinstance(restored_execute, RuntimeExecuteResponse)
    assert restored_execute.domain_events[0].event_type == 'log.chunk'
    assert restored_execute.phase_durations == {'run': 0.05}
    assert restored_execute.resource_events[0].external_handle == 'db:worker-1'
    assert restored_execute.resource_timings[0].name == 'db'
    assert isinstance(restored_event, RuntimeEventResponse)
    assert restored_event.stream_kind == 'log'
    assert restored_event.metadata.in_reply_to == 'request-1'


def test_runtime_protocol_roundtrips_shutdown_and_typed_error_response(
) -> None:
    shutdown = RuntimeShutdownResponse(
        resource_timings=(
            ResourceTiming(
                name='db',
                scope='worker',
                release_count=1,
                release_duration=0.1,
            ),
        ),
        domain_events=(
            LogChunkEvent(
                message='shutdown',
                level='info',
                logger_name='tests.runtime',
                metadata=DomainEventMetadata(worker_id=0),
            ),
        ),
        resource_events=(
            ResourceLifecycleEvent(
                action='released',
                external_handle='db:worker-0',
                name='db',
                scope='worker',
                metadata=DomainEventMetadata(worker_id=0),
            ),
        ),
    )
    error = build_runtime_protocol_error(
        code='worker_command_failed',
        message='boom',
        recoverable=False,
        fatal=True,
        metadata=RuntimeEnvelopeMetadata(
            correlation_id='command-1',
            in_reply_to='request-1',
            session_id='session-1',
        ),
    )

    restored_shutdown = deserialize_runtime_response(shutdown.to_dict())
    restored_error = deserialize_runtime_response(error.to_dict())

    assert isinstance(restored_shutdown, RuntimeShutdownResponse)
    assert restored_shutdown.resource_events[0].action == 'released'
    assert restored_shutdown.resource_timings[0].release_duration == (
        pytest.approx(0.1)
    )
    assert isinstance(restored_error, RuntimeErrorResponse)
    assert restored_error.error.code == 'worker_command_failed'
    assert restored_error.metadata.correlation_id == 'command-1'


def test_runtime_protocol_roundtrips_shutdown_and_snapshot_commands() -> None:
    snapshot = RuntimeSnapshotResourceTimingsCommand(
        metadata=RuntimeEnvelopeMetadata(in_reply_to='request-1'),
    )
    shutdown = RuntimeShutdownCommand(
        metadata=RuntimeEnvelopeMetadata(in_reply_to='request-2'),
    )

    restored_snapshot = deserialize_runtime_command(snapshot.to_dict())
    restored_shutdown = deserialize_runtime_command(shutdown.to_dict())

    assert isinstance(restored_snapshot, RuntimeSnapshotResourceTimingsCommand)
    assert restored_snapshot.metadata.in_reply_to == 'request-1'
    assert isinstance(restored_shutdown, RuntimeShutdownCommand)
    assert restored_shutdown.metadata.in_reply_to == 'request-2'


def test_runtime_protocol_roundtrips_snapshot_resource_timings_response() -> None:
    response = RuntimeSnapshotResourceTimingsResponse(
        resource_timings=(
            ResourceTiming(
                name='mongo',
                scope='worker',
                acquire_count=1,
                acquire_duration=0.1,
            ),
        ),
        metadata=RuntimeEnvelopeMetadata(session_id='session-1'),
    )

    restored = deserialize_runtime_response(response.to_dict())

    assert isinstance(restored, RuntimeSnapshotResourceTimingsResponse)
    assert restored.resource_timings[0].name == 'mongo'


def test_runtime_protocol_rejects_unknown_command_and_response_type() -> None:
    with pytest.raises(ValueError, match='Unknown worker command'):
        deserialize_runtime_command(
            {
                'command': 'unknown',
                'protocol_version': RUNTIME_PROTOCOL_VERSION,
            },
        )

    with pytest.raises(ValueError, match='Unknown worker response type'):
        deserialize_runtime_response(
            {
                'response_type': 'unknown',
                'status': 'ok',
                'protocol_version': RUNTIME_PROTOCOL_VERSION,
            },
        )


def test_runtime_protocol_rejects_invalid_protocol_versions() -> None:
    with pytest.raises(ValueError, match='Unsupported runtime protocol version'):
        RuntimeBootstrapCommand(
            bootstrap=ExecutionBootstrap(
                config_snapshot=_build_config_snapshot(),
                nodes=(_build_snapshot(),),
            ),
            protocol_version=RUNTIME_PROTOCOL_VERSION + 1,
        )

    with pytest.raises(ValueError, match='Unsupported runtime protocol version'):
        deserialize_runtime_response(
            {
                'response_type': 'ready',
                'status': 'ready',
                'protocol_version': RUNTIME_PROTOCOL_VERSION + 1,
                'metadata': {'event_id': 'meta-1'},
            },
        )


def test_runtime_protocol_cast_and_resource_deserialize_guards() -> None:
    assert cast_optional_str(None) is None
    assert cast_optional_str(12) == '12'
    assert cast_optional_int(None) is None
    assert cast_optional_int('12') == 12
    assert cast_resource_lifecycle_action('acquired') == 'acquired'
    assert cast_runtime_event_stream_kind('domain') == 'domain'

    with pytest.raises(ValueError, match='Invalid resource lifecycle action'):
        cast_resource_lifecycle_action('broken')
    with pytest.raises(ValueError, match='Invalid runtime event stream kind'):
        cast_runtime_event_stream_kind('broken')
    with pytest.raises(ValueError, match='Expected resource.lifecycle payload'):
        deserialize_resource_lifecycle_event(
            serialize_domain_event(
                LogChunkEvent(
                    message='x',
                    level='info',
                    logger_name='tests.runtime',
                ),
            ),
        )


def test_runtime_protocol_normalize_dict_guard() -> None:
    with pytest.raises(ValueError, match='Expected dict payload'):
        deserialize_runtime_response(
            {
                'response_type': 'error',
                'status': 'error',
                'protocol_version': RUNTIME_PROTOCOL_VERSION,
                'error': 'not-a-dict',
            },
        )


def test_runtime_protocol_deserialize_defaults_metadata_when_missing() -> None:
    restored = deserialize_runtime_response(
        {
            'response_type': 'ready',
            'status': 'ready',
            'protocol_version': RUNTIME_PROTOCOL_VERSION,
        },
    )

    assert isinstance(restored, RuntimeReadyResponse)
    assert restored.metadata.correlation_id == restored.metadata.message_id
