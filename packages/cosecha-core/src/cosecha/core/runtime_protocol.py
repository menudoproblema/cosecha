from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal
from uuid import uuid4

from cosecha.core.domain_events import (
    DomainEvent,
    ResourceLifecycleEvent,
    deserialize_domain_event,
    serialize_domain_event,
)
from cosecha.core.execution_ir import ExecutionBootstrap, ExecutionRequest
from cosecha.core.resources import ResourceTiming
from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


type RuntimeCommandType = Literal[
    'bootstrap',
    'execute',
    'shutdown',
    'snapshot_resource_timings',
]
type RuntimeResponseType = Literal[
    'bootstrap',
    'error',
    'event',
    'execute',
    'ready',
    'shutdown',
    'snapshot_resource_timings',
]
type RuntimeResponseStatus = Literal['error', 'ok', 'ready']
type RuntimeEventStreamKind = Literal['domain', 'log']

RUNTIME_PROTOCOL_VERSION = 5


def build_runtime_message_id() -> str:
    return uuid4().hex


@dataclass(slots=True, frozen=True)
class RuntimeEnvelopeMetadata:
    message_id: str = field(default_factory=build_runtime_message_id)
    correlation_id: str | None = None
    session_id: str | None = None
    trace_id: str | None = None
    idempotency_key: str | None = None
    in_reply_to: str | None = None

    def __post_init__(self) -> None:
        if self.correlation_id is None:
            object.__setattr__(self, 'correlation_id', self.message_id)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RuntimeEnvelopeMetadata:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeProtocolError:
    code: str
    message: str
    recoverable: bool = False
    fatal: bool = True

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RuntimeProtocolError:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeBootstrapCommand:
    bootstrap: ExecutionBootstrap
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    command: RuntimeCommandType = 'bootstrap'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class RuntimeExecuteCommand:
    request: ExecutionRequest
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    command: RuntimeCommandType = 'execute'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class RuntimeSnapshotResourceTimingsCommand:
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    command: RuntimeCommandType = 'snapshot_resource_timings'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class RuntimeShutdownCommand:
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    command: RuntimeCommandType = 'shutdown'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


type RuntimeCommand = (
    RuntimeBootstrapCommand
    | RuntimeExecuteCommand
    | RuntimeShutdownCommand
    | RuntimeSnapshotResourceTimingsCommand
)


@dataclass(slots=True, frozen=True)
class RuntimeReadyResponse:
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    response_type: RuntimeResponseType = 'ready'
    status: RuntimeResponseStatus = 'ready'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class RuntimeBootstrapResponse:
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    response_type: RuntimeResponseType = 'bootstrap'
    status: RuntimeResponseStatus = 'ok'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class RuntimeEventResponse:
    event: DomainEvent
    stream_kind: RuntimeEventStreamKind
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    response_type: RuntimeResponseType = 'event'
    status: RuntimeResponseStatus = 'ok'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return {
            'event': serialize_domain_event(self.event),
            'metadata': self.metadata.to_dict(),
            'protocol_version': self.protocol_version,
            'response_type': self.response_type,
            'status': self.status,
            'stream_kind': self.stream_kind,
        }


@dataclass(slots=True, frozen=True)
class RuntimeExecuteResponse:
    report: dict[str, object]
    phase_durations: dict[str, float] = field(default_factory=dict)
    resource_timings: tuple[ResourceTiming, ...] = field(
        default_factory=tuple,
    )
    domain_events: tuple[DomainEvent, ...] = field(default_factory=tuple)
    resource_events: tuple[ResourceLifecycleEvent, ...] = field(
        default_factory=tuple,
    )
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    response_type: RuntimeResponseType = 'execute'
    status: RuntimeResponseStatus = 'ok'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return {
            'metadata': self.metadata.to_dict(),
            'domain_events': [
                serialize_domain_event(domain_event)
                for domain_event in self.domain_events
            ],
            'phase_durations': self.phase_durations,
            'protocol_version': self.protocol_version,
            'report': self.report,
            'resource_events': [
                serialize_domain_event(resource_event)
                for resource_event in self.resource_events
            ],
            'resource_timings': [
                resource_timing.to_dict()
                for resource_timing in self.resource_timings
            ],
            'response_type': self.response_type,
            'status': self.status,
        }


@dataclass(slots=True, frozen=True)
class RuntimeSnapshotResourceTimingsResponse:
    resource_timings: tuple[ResourceTiming, ...] = field(
        default_factory=tuple,
    )
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    response_type: RuntimeResponseType = 'snapshot_resource_timings'
    status: RuntimeResponseStatus = 'ok'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class RuntimeShutdownResponse:
    resource_timings: tuple[ResourceTiming, ...] = field(
        default_factory=tuple,
    )
    domain_events: tuple[DomainEvent, ...] = field(default_factory=tuple)
    resource_events: tuple[ResourceLifecycleEvent, ...] = field(
        default_factory=tuple,
    )
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    response_type: RuntimeResponseType = 'shutdown'
    status: RuntimeResponseStatus = 'ok'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return {
            'metadata': self.metadata.to_dict(),
            'domain_events': [
                serialize_domain_event(domain_event)
                for domain_event in self.domain_events
            ],
            'protocol_version': self.protocol_version,
            'resource_events': [
                serialize_domain_event(resource_event)
                for resource_event in self.resource_events
            ],
            'resource_timings': [
                resource_timing.to_dict()
                for resource_timing in self.resource_timings
            ],
            'response_type': self.response_type,
            'status': self.status,
        }


@dataclass(slots=True, frozen=True)
class RuntimeErrorResponse:
    error: RuntimeProtocolError
    metadata: RuntimeEnvelopeMetadata = field(
        default_factory=RuntimeEnvelopeMetadata,
    )
    response_type: RuntimeResponseType = 'error'
    status: RuntimeResponseStatus = 'error'
    protocol_version: int = RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        _validate_protocol_version(self.protocol_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


type RuntimeResponse = (
    RuntimeBootstrapResponse
    | RuntimeErrorResponse
    | RuntimeEventResponse
    | RuntimeExecuteResponse
    | RuntimeReadyResponse
    | RuntimeShutdownResponse
    | RuntimeSnapshotResourceTimingsResponse
)


def deserialize_runtime_command(
    data: dict[str, object],
) -> RuntimeCommand:
    _validate_protocol_version(data.get('protocol_version'))
    command = str(data['command'])
    metadata = _deserialize_metadata(data)
    if command == 'bootstrap':
        return RuntimeBootstrapCommand(
            bootstrap=ExecutionBootstrap.from_dict(data['bootstrap']),
            metadata=metadata,
        )
    if command == 'execute':
        return RuntimeExecuteCommand(
            request=ExecutionRequest.from_dict(data['request']),
            metadata=metadata,
        )
    if command == 'snapshot_resource_timings':
        return RuntimeSnapshotResourceTimingsCommand(metadata=metadata)
    if command == 'shutdown':
        return RuntimeShutdownCommand(metadata=metadata)

    msg = f'Unknown worker command: {command}'
    raise ValueError(msg)


def deserialize_runtime_response(
    data: dict[str, object],
) -> RuntimeResponse:
    _validate_protocol_version(data.get('protocol_version'))
    response_type = str(data['response_type'])
    metadata = _deserialize_metadata(data)
    response: RuntimeResponse
    if response_type == 'ready':
        response = RuntimeReadyResponse(metadata=metadata)
    elif response_type == 'bootstrap':
        response = RuntimeBootstrapResponse(metadata=metadata)
    elif response_type == 'event':
        response = RuntimeEventResponse(
            event=deserialize_domain_event(
                _normalize_object_dict(data['event']),
            ),
            stream_kind=cast_runtime_event_stream_kind(
                data.get('stream_kind'),
            ),
            metadata=metadata,
        )
    elif response_type == 'execute':
        response = RuntimeExecuteResponse(
            metadata=metadata,
            domain_events=tuple(
                deserialize_domain_event(domain_event)
                for domain_event in data.get('domain_events', [])
            ),
            report=_normalize_object_dict(data['report']),
            phase_durations={
                str(name): float(duration)
                for name, duration in _normalize_object_dict(
                    data.get('phase_durations', {}),
                ).items()
            },
            resource_events=tuple(
                deserialize_resource_lifecycle_event(resource_event)
                for resource_event in data.get('resource_events', [])
            ),
            resource_timings=tuple(
                ResourceTiming.from_dict(resource_timing)
                for resource_timing in data.get(
                    'resource_timings',
                    [],
                )
            ),
        )
    elif response_type == 'snapshot_resource_timings':
        response = RuntimeSnapshotResourceTimingsResponse(
            metadata=metadata,
            resource_timings=tuple(
                ResourceTiming.from_dict(resource_timing)
                for resource_timing in data.get(
                    'resource_timings',
                    [],
                )
            ),
        )
    elif response_type == 'shutdown':
        response = RuntimeShutdownResponse(
            metadata=metadata,
            domain_events=tuple(
                deserialize_domain_event(domain_event)
                for domain_event in data.get('domain_events', [])
            ),
            resource_events=tuple(
                deserialize_resource_lifecycle_event(resource_event)
                for resource_event in data.get('resource_events', [])
            ),
            resource_timings=tuple(
                ResourceTiming.from_dict(resource_timing)
                for resource_timing in data.get(
                    'resource_timings',
                    [],
                )
            ),
        )
    elif response_type == 'error':
        response = RuntimeErrorResponse(
            error=RuntimeProtocolError.from_dict(
                _normalize_object_dict(data['error']),
            ),
            metadata=metadata,
        )
    else:
        msg = f'Unknown worker response type: {response_type}'
        raise ValueError(msg)

    return response


def build_runtime_protocol_error(
    *,
    code: str,
    message: str,
    recoverable: bool = False,
    fatal: bool = True,
    metadata: RuntimeEnvelopeMetadata | None = None,
) -> RuntimeErrorResponse:
    return RuntimeErrorResponse(
        error=RuntimeProtocolError(
            code=code,
            message=message,
            recoverable=recoverable,
            fatal=fatal,
        ),
        metadata=metadata or RuntimeEnvelopeMetadata(),
    )


def _validate_protocol_version(value: object) -> None:
    if int(value) != RUNTIME_PROTOCOL_VERSION:
        msg = (
            'Unsupported runtime protocol version: '
            f'{value!r} != {RUNTIME_PROTOCOL_VERSION!r}'
        )
        raise ValueError(msg)


def _normalize_object_dict(data: object) -> dict[str, object]:
    if isinstance(data, dict):
        return {str(key): value for key, value in data.items()}

    msg = f'Expected dict payload, got {type(data).__name__}'
    raise ValueError(msg)


def _deserialize_metadata(
    data: dict[str, object],
) -> RuntimeEnvelopeMetadata:
    raw_metadata = data.get('metadata')
    if raw_metadata is None:
        return RuntimeEnvelopeMetadata()

    return RuntimeEnvelopeMetadata.from_dict(
        _normalize_object_dict(raw_metadata),
    )


def cast_optional_str(value: object) -> str | None:
    if value is None:
        return None

    return str(value)


def cast_optional_int(value: object) -> int | None:
    if value is None:
        return None

    return int(value)


def cast_resource_lifecycle_action(
    value: object | None,
) -> Literal['acquired', 'released']:
    if value in {'acquired', 'released'}:
        return value

    msg = f'Invalid resource lifecycle action: {value!r}'
    raise ValueError(msg)


def cast_runtime_event_stream_kind(
    value: object | None,
) -> RuntimeEventStreamKind:
    if value in {'domain', 'log'}:
        return value

    msg = f'Invalid runtime event stream kind: {value!r}'
    raise ValueError(msg)


def serialize_resource_lifecycle_event(
    event: ResourceLifecycleEvent,
) -> dict[str, object]:
    return serialize_domain_event(event)


def deserialize_resource_lifecycle_event(
    data: object,
) -> ResourceLifecycleEvent:
    event = deserialize_domain_event(_normalize_object_dict(data))
    if isinstance(event, ResourceLifecycleEvent):
        return event

    msg = f'Expected resource.lifecycle payload, got {event.event_type!r}'
    raise ValueError(msg)
