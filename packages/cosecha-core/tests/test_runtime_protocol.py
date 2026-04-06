from __future__ import annotations

import pytest

from cosecha.core.config import ConfigSnapshot
from cosecha.core.domain_events import (
    DomainEventMetadata,
    LogChunkEvent,
    ResourceLifecycleEvent,
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
    RuntimeShutdownResponse,
    build_runtime_protocol_error,
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
