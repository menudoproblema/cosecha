from __future__ import annotations

import time

from dataclasses import dataclass, field
from functools import singledispatch
from typing import Literal
from uuid import uuid4

from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord
from cosecha.core.knowledge_test_descriptor import TestDescriptorKnowledge
from cosecha.core.registry_knowledge import RegistryKnowledgeEntry


type DomainEventType = Literal[
    'definition.materialized',
    'engine.snapshot_updated',
    'knowledge.indexed',
    'knowledge.invalidated',
    'log.chunk',
    'registry.knowledge_indexed',
    'node.assigned',
    'node.enqueued',
    'node.requeued',
    'node.retrying',
    'node.scheduled',
    'plan.analyzed',
    'resource.lifecycle',
    'resource.readiness_transition',
    'session.finished',
    'session.started',
    'step.finished',
    'step.started',
    'test.finished',
    'test.knowledge_indexed',
    'test.knowledge_invalidated',
    'test.started',
    'worker.degraded',
    'worker.heartbeat',
    'worker.recovered',
]


def build_domain_event_id() -> str:
    return uuid4().hex


def build_domain_event_timestamp() -> float:
    return time.time()


@dataclass(slots=True, frozen=True)
class DomainEventMetadata:
    event_id: str = field(default_factory=build_domain_event_id)
    sequence_number: int | None = None
    correlation_id: str | None = None
    idempotency_key: str | None = None
    session_id: str | None = None
    plan_id: str | None = None
    node_id: str | None = None
    node_stable_id: str | None = None
    trace_id: str | None = None
    worker_id: int | None = None

    def __post_init__(self) -> None:
        if self.correlation_id is None:
            object.__setattr__(self, 'correlation_id', self.event_id)


@dataclass(slots=True, frozen=True)
class SessionStartedEvent:
    root_path: str
    concurrency: int
    workspace_fingerprint: str | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'session.started'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class SessionFinishedEvent:
    has_failures: bool
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'session.finished'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class PlanAnalyzedEvent:
    mode: str
    executable: bool
    node_count: int
    issue_count: int
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'plan.analyzed'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class NodeScheduledEvent:
    node_id: str
    node_stable_id: str
    worker_slot: int
    max_attempts: int
    timeout_seconds: float | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'node.scheduled'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class NodeEnqueuedEvent:
    node_id: str
    node_stable_id: str
    preferred_worker_slot: int
    max_attempts: int
    timeout_seconds: float | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'node.enqueued'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class NodeAssignedEvent:
    node_id: str
    node_stable_id: str
    worker_slot: int
    attempt: int = 1
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'node.assigned'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class NodeRequeuedEvent:
    node_id: str
    node_stable_id: str
    previous_worker_slot: int
    attempt: int
    failure_kind: str | None = None
    error_code: str | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'node.requeued'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class NodeRetryingEvent:
    node_id: str
    node_stable_id: str
    attempt: int
    failure_kind: str | None = None
    error_code: str | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'node.retrying'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class KnowledgeIndexedEvent:
    engine_name: str
    file_path: str
    definition_count: int
    discovery_mode: str
    knowledge_version: str
    content_hash: str | None = None
    descriptors: tuple[DefinitionKnowledgeRecord, ...] = ()
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'knowledge.indexed'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class KnowledgeInvalidatedEvent:
    engine_name: str
    file_path: str
    reason: str
    knowledge_version: str
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'knowledge.invalidated'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class DefinitionMaterializedEvent:
    engine_name: str
    file_path: str
    definition_count: int
    discovery_mode: str
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'definition.materialized'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class RegistryKnowledgeIndexedEvent:
    engine_name: str
    module_spec: str
    package_hash: str
    layout_key: str
    loader_schema_version: str
    entries: tuple[RegistryKnowledgeEntry, ...] = ()
    source_count: int = 0
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'registry.knowledge_indexed'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class TestStartedEvent:
    node_id: str
    node_stable_id: str
    engine_name: str
    test_name: str
    test_path: str
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'test.started'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class StepStartedEvent:
    node_id: str
    node_stable_id: str
    engine_name: str
    test_name: str
    test_path: str
    step_type: str
    step_keyword: str
    step_text: str
    source_line: int | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'step.started'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class StepFinishedEvent:
    node_id: str
    node_stable_id: str
    engine_name: str
    test_name: str
    test_path: str
    step_type: str
    step_keyword: str
    step_text: str
    status: str
    source_line: int | None = None
    message: str | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'step.finished'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class LogChunkEvent:
    message: str
    level: str
    logger_name: str
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'log.chunk'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class EngineSnapshotUpdatedEvent:
    engine_name: str
    snapshot_kind: str
    payload: dict[str, object]
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'engine.snapshot_updated'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class TestKnowledgeIndexedEvent:
    engine_name: str
    file_path: str
    tests: tuple[TestDescriptorKnowledge, ...]
    discovery_mode: str
    knowledge_version: str
    content_hash: str | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'test.knowledge_indexed'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class TestKnowledgeInvalidatedEvent:
    engine_name: str
    file_path: str
    reason: str
    knowledge_version: str
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'test.knowledge_invalidated'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class TestFinishedEvent:
    node_id: str
    node_stable_id: str
    engine_name: str
    test_name: str
    test_path: str
    status: str
    duration: float
    failure_kind: str | None = None
    error_code: str | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'test.finished'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class ResourceLifecycleEvent:
    action: Literal['acquired', 'released']
    name: str
    scope: str
    test_id: str | None = None
    external_handle: str | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'resource.lifecycle'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class ResourceReadinessTransitionEvent:
    name: str
    scope: str
    status: Literal[
        'starting',
        'ready',
        'degraded',
        'unhealthy',
        'unhealthy_local',
    ]
    reason: str | None = None
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'resource.readiness_transition'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class WorkerHeartbeatEvent:
    worker_id: int
    status: Literal[
        'ready',
        'alive',
        'degraded',
        'closing',
        'closed',
        'lost',
        'recovered',
    ]
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'worker.heartbeat'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class WorkerDegradedEvent:
    worker_id: int
    reason: str
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'worker.degraded'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


@dataclass(slots=True, frozen=True)
class WorkerRecoveredEvent:
    worker_id: int
    metadata: DomainEventMetadata = field(
        default_factory=DomainEventMetadata,
    )
    event_type: DomainEventType = 'worker.recovered'
    timestamp: float = field(default_factory=build_domain_event_timestamp)


type DomainEvent = (
    SessionStartedEvent
    | SessionFinishedEvent
    | NodeEnqueuedEvent
    | NodeAssignedEvent
    | NodeRequeuedEvent
    | NodeScheduledEvent
    | NodeRetryingEvent
    | KnowledgeIndexedEvent
    | KnowledgeInvalidatedEvent
    | DefinitionMaterializedEvent
    | RegistryKnowledgeIndexedEvent
    | PlanAnalyzedEvent
    | TestStartedEvent
    | StepStartedEvent
    | StepFinishedEvent
    | LogChunkEvent
    | EngineSnapshotUpdatedEvent
    | TestKnowledgeIndexedEvent
    | TestKnowledgeInvalidatedEvent
    | TestFinishedEvent
    | ResourceLifecycleEvent
    | ResourceReadinessTransitionEvent
    | WorkerHeartbeatEvent
    | WorkerDegradedEvent
    | WorkerRecoveredEvent
)


def serialize_domain_event(event: DomainEvent) -> dict[str, object]:
    return {
        'event_type': event.event_type,
        'metadata': _serialize_domain_event_metadata(event.metadata),
        'timestamp': event.timestamp,
        **_serialize_domain_event_payload(event),
    }


def deserialize_domain_event(data: dict[str, object]) -> DomainEvent:
    event_type = str(data['event_type'])
    metadata = _deserialize_domain_event_metadata(data.get('metadata'))
    timestamp = float(data['timestamp'])
    deserializer = _DOMAIN_EVENT_DESERIALIZERS.get(event_type)
    if deserializer is None:
        msg = f'Unsupported domain event type: {event_type!r}'
        raise ValueError(msg)

    return deserializer(data, metadata, timestamp)


@singledispatch
def _serialize_domain_event_payload(event: object) -> dict[str, object]:
    msg = f'Unsupported domain event type: {type(event)!r}'
    raise TypeError(msg)


@_serialize_domain_event_payload.register
def _(event: SessionStartedEvent) -> dict[str, object]:
    return {
        'concurrency': event.concurrency,
        'root_path': event.root_path,
        'workspace_fingerprint': event.workspace_fingerprint,
    }


@_serialize_domain_event_payload.register
def _(event: SessionFinishedEvent) -> dict[str, object]:
    return {'has_failures': event.has_failures}


@_serialize_domain_event_payload.register
def _(event: PlanAnalyzedEvent) -> dict[str, object]:
    return {
        'executable': event.executable,
        'issue_count': event.issue_count,
        'mode': event.mode,
        'node_count': event.node_count,
    }


@_serialize_domain_event_payload.register
def _(event: NodeScheduledEvent) -> dict[str, object]:
    return {
        'max_attempts': event.max_attempts,
        'node_id': event.node_id,
        'node_stable_id': event.node_stable_id,
        'timeout_seconds': event.timeout_seconds,
        'worker_slot': event.worker_slot,
    }


@_serialize_domain_event_payload.register
def _(event: NodeEnqueuedEvent) -> dict[str, object]:
    return {
        'max_attempts': event.max_attempts,
        'node_id': event.node_id,
        'node_stable_id': event.node_stable_id,
        'preferred_worker_slot': event.preferred_worker_slot,
        'timeout_seconds': event.timeout_seconds,
    }


@_serialize_domain_event_payload.register
def _(event: NodeAssignedEvent) -> dict[str, object]:
    return {
        'attempt': event.attempt,
        'node_id': event.node_id,
        'node_stable_id': event.node_stable_id,
        'worker_slot': event.worker_slot,
    }


@_serialize_domain_event_payload.register
def _(event: NodeRequeuedEvent) -> dict[str, object]:
    return {
        'attempt': event.attempt,
        'error_code': event.error_code,
        'failure_kind': event.failure_kind,
        'node_id': event.node_id,
        'node_stable_id': event.node_stable_id,
        'previous_worker_slot': event.previous_worker_slot,
    }


@_serialize_domain_event_payload.register
def _(event: NodeRetryingEvent) -> dict[str, object]:
    return {
        'attempt': event.attempt,
        'error_code': event.error_code,
        'failure_kind': event.failure_kind,
        'node_id': event.node_id,
        'node_stable_id': event.node_stable_id,
    }


@_serialize_domain_event_payload.register
def _(event: KnowledgeIndexedEvent) -> dict[str, object]:
    return {
        'content_hash': event.content_hash,
        'definition_count': event.definition_count,
        'descriptors': [
            descriptor.to_dict() for descriptor in event.descriptors
        ],
        'discovery_mode': event.discovery_mode,
        'engine_name': event.engine_name,
        'file_path': event.file_path,
        'knowledge_version': event.knowledge_version,
    }


@_serialize_domain_event_payload.register
def _(event: KnowledgeInvalidatedEvent) -> dict[str, object]:
    return {
        'engine_name': event.engine_name,
        'file_path': event.file_path,
        'knowledge_version': event.knowledge_version,
        'reason': event.reason,
    }


@_serialize_domain_event_payload.register
def _(event: DefinitionMaterializedEvent) -> dict[str, object]:
    return {
        'definition_count': event.definition_count,
        'discovery_mode': event.discovery_mode,
        'engine_name': event.engine_name,
        'file_path': event.file_path,
    }


@_serialize_domain_event_payload.register
def _(event: RegistryKnowledgeIndexedEvent) -> dict[str, object]:
    return {
        'engine_name': event.engine_name,
        'entries': [entry.to_dict() for entry in event.entries],
        'layout_key': event.layout_key,
        'loader_schema_version': event.loader_schema_version,
        'module_spec': event.module_spec,
        'package_hash': event.package_hash,
        'source_count': event.source_count,
    }


@_serialize_domain_event_payload.register
def _(event: TestStartedEvent) -> dict[str, object]:
    return {
        'engine_name': event.engine_name,
        'node_id': event.node_id,
        'node_stable_id': event.node_stable_id,
        'test_name': event.test_name,
        'test_path': event.test_path,
    }


@_serialize_domain_event_payload.register
def _(event: StepStartedEvent) -> dict[str, object]:
    return {
        'engine_name': event.engine_name,
        'node_id': event.node_id,
        'node_stable_id': event.node_stable_id,
        'source_line': event.source_line,
        'step_keyword': event.step_keyword,
        'step_text': event.step_text,
        'step_type': event.step_type,
        'test_name': event.test_name,
        'test_path': event.test_path,
    }


@_serialize_domain_event_payload.register
def _(event: StepFinishedEvent) -> dict[str, object]:
    return {
        'engine_name': event.engine_name,
        'message': event.message,
        'node_id': event.node_id,
        'node_stable_id': event.node_stable_id,
        'source_line': event.source_line,
        'status': event.status,
        'step_keyword': event.step_keyword,
        'step_text': event.step_text,
        'step_type': event.step_type,
        'test_name': event.test_name,
        'test_path': event.test_path,
    }


@_serialize_domain_event_payload.register
def _(event: LogChunkEvent) -> dict[str, object]:
    return {
        'level': event.level,
        'logger_name': event.logger_name,
        'message': event.message,
    }


@_serialize_domain_event_payload.register
def _(event: EngineSnapshotUpdatedEvent) -> dict[str, object]:
    return {
        'engine_name': event.engine_name,
        'payload': event.payload,
        'snapshot_kind': event.snapshot_kind,
    }


@_serialize_domain_event_payload.register
def _(event: TestKnowledgeIndexedEvent) -> dict[str, object]:
    return {
        'content_hash': event.content_hash,
        'discovery_mode': event.discovery_mode,
        'engine_name': event.engine_name,
        'file_path': event.file_path,
        'knowledge_version': event.knowledge_version,
        'tests': [descriptor.to_dict() for descriptor in event.tests],
    }


@_serialize_domain_event_payload.register
def _(event: TestKnowledgeInvalidatedEvent) -> dict[str, object]:
    return {
        'engine_name': event.engine_name,
        'file_path': event.file_path,
        'knowledge_version': event.knowledge_version,
        'reason': event.reason,
    }


@_serialize_domain_event_payload.register
def _(event: TestFinishedEvent) -> dict[str, object]:
    return {
        'duration': event.duration,
        'engine_name': event.engine_name,
        'error_code': event.error_code,
        'failure_kind': event.failure_kind,
        'node_id': event.node_id,
        'node_stable_id': event.node_stable_id,
        'status': event.status,
        'test_name': event.test_name,
        'test_path': event.test_path,
    }


@_serialize_domain_event_payload.register
def _(event: ResourceLifecycleEvent) -> dict[str, object]:
    return {
        'action': event.action,
        'external_handle': event.external_handle,
        'name': event.name,
        'scope': event.scope,
        'test_id': event.test_id,
    }


@_serialize_domain_event_payload.register
def _(event: ResourceReadinessTransitionEvent) -> dict[str, object]:
    return {
        'name': event.name,
        'reason': event.reason,
        'scope': event.scope,
        'status': event.status,
    }


@_serialize_domain_event_payload.register
def _(event: WorkerHeartbeatEvent) -> dict[str, object]:
    return {
        'status': event.status,
        'worker_id': event.worker_id,
    }


@_serialize_domain_event_payload.register
def _(event: WorkerDegradedEvent) -> dict[str, object]:
    return {
        'reason': event.reason,
        'worker_id': event.worker_id,
    }


@_serialize_domain_event_payload.register
def _(event: WorkerRecoveredEvent) -> dict[str, object]:
    return {
        'worker_id': event.worker_id,
    }


def _deserialize_session_started_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return SessionStartedEvent(
        root_path=str(data['root_path']),
        workspace_fingerprint=(
            None
            if data.get('workspace_fingerprint') is None
            else str(data.get('workspace_fingerprint'))
        ),
        concurrency=int(data['concurrency']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_session_finished_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return SessionFinishedEvent(
        has_failures=bool(data['has_failures']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_plan_analyzed_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return PlanAnalyzedEvent(
        mode=str(data['mode']),
        executable=bool(data['executable']),
        node_count=int(data['node_count']),
        issue_count=int(data['issue_count']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_node_scheduled_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return NodeScheduledEvent(
        node_id=str(data['node_id']),
        node_stable_id=str(data['node_stable_id']),
        worker_slot=int(data['worker_slot']),
        max_attempts=int(data['max_attempts']),
        timeout_seconds=_cast_optional_float(data.get('timeout_seconds')),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_node_retrying_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return NodeRetryingEvent(
        node_id=str(data['node_id']),
        node_stable_id=str(data['node_stable_id']),
        attempt=int(data['attempt']),
        failure_kind=_cast_optional_str(data.get('failure_kind')),
        error_code=_cast_optional_str(data.get('error_code')),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_node_enqueued_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return NodeEnqueuedEvent(
        node_id=str(data['node_id']),
        node_stable_id=str(data['node_stable_id']),
        preferred_worker_slot=int(data['preferred_worker_slot']),
        max_attempts=int(data['max_attempts']),
        timeout_seconds=_cast_optional_float(data.get('timeout_seconds')),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_node_assigned_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return NodeAssignedEvent(
        node_id=str(data['node_id']),
        node_stable_id=str(data['node_stable_id']),
        worker_slot=int(data['worker_slot']),
        attempt=int(data.get('attempt', 1)),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_node_requeued_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return NodeRequeuedEvent(
        node_id=str(data['node_id']),
        node_stable_id=str(data['node_stable_id']),
        attempt=int(data['attempt']),
        failure_kind=_cast_optional_str(data.get('failure_kind')),
        error_code=_cast_optional_str(data.get('error_code')),
        previous_worker_slot=int(data['previous_worker_slot']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_knowledge_indexed_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return KnowledgeIndexedEvent(
        engine_name=str(data['engine_name']),
        file_path=str(data['file_path']),
        definition_count=int(data['definition_count']),
        discovery_mode=str(data['discovery_mode']),
        knowledge_version=str(data['knowledge_version']),
        content_hash=_cast_optional_str(data.get('content_hash')),
        descriptors=tuple(
            DefinitionKnowledgeRecord.from_dict(record)
            for record in data.get('descriptors', ())
            if isinstance(record, dict)
        ),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_knowledge_invalidated_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return KnowledgeInvalidatedEvent(
        engine_name=str(data['engine_name']),
        file_path=str(data['file_path']),
        reason=str(data['reason']),
        knowledge_version=str(data['knowledge_version']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_definition_materialized_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return DefinitionMaterializedEvent(
        engine_name=str(data['engine_name']),
        file_path=str(data['file_path']),
        definition_count=int(data['definition_count']),
        discovery_mode=str(data['discovery_mode']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_registry_knowledge_indexed_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return RegistryKnowledgeIndexedEvent(
        engine_name=str(data['engine_name']),
        module_spec=str(data['module_spec']),
        package_hash=str(data['package_hash']),
        layout_key=str(data['layout_key']),
        loader_schema_version=str(data['loader_schema_version']),
        entries=tuple(
            RegistryKnowledgeEntry.from_dict(record)
            for record in data.get('entries', ())
            if isinstance(record, dict)
        ),
        source_count=int(data.get('source_count', 0)),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_test_started_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return TestStartedEvent(
        node_id=str(data['node_id']),
        node_stable_id=str(data['node_stable_id']),
        engine_name=str(data['engine_name']),
        test_name=str(data['test_name']),
        test_path=str(data['test_path']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_engine_snapshot_updated_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    payload = data.get('payload')
    return EngineSnapshotUpdatedEvent(
        engine_name=str(data['engine_name']),
        snapshot_kind=str(data['snapshot_kind']),
        payload=payload if isinstance(payload, dict) else {},
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_step_started_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return StepStartedEvent(
        node_id=str(data['node_id']),
        node_stable_id=str(data['node_stable_id']),
        engine_name=str(data['engine_name']),
        test_name=str(data['test_name']),
        test_path=str(data['test_path']),
        step_type=str(data['step_type']),
        step_keyword=str(data['step_keyword']),
        step_text=str(data['step_text']),
        source_line=_cast_optional_int(data.get('source_line')),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_step_finished_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return StepFinishedEvent(
        node_id=str(data['node_id']),
        node_stable_id=str(data['node_stable_id']),
        engine_name=str(data['engine_name']),
        test_name=str(data['test_name']),
        test_path=str(data['test_path']),
        step_type=str(data['step_type']),
        step_keyword=str(data['step_keyword']),
        step_text=str(data['step_text']),
        status=str(data['status']),
        source_line=_cast_optional_int(data.get('source_line')),
        message=_cast_optional_str(data.get('message')),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_log_chunk_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return LogChunkEvent(
        message=str(data['message']),
        level=str(data['level']),
        logger_name=str(data['logger_name']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_test_knowledge_indexed_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return TestKnowledgeIndexedEvent(
        engine_name=str(data['engine_name']),
        file_path=str(data['file_path']),
        tests=tuple(
            TestDescriptorKnowledge.from_dict(record)
            for record in data.get('tests', ())
            if isinstance(record, dict)
        ),
        discovery_mode=str(data['discovery_mode']),
        knowledge_version=str(data['knowledge_version']),
        content_hash=_cast_optional_str(data.get('content_hash')),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_test_knowledge_invalidated_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return TestKnowledgeInvalidatedEvent(
        engine_name=str(data['engine_name']),
        file_path=str(data['file_path']),
        reason=str(data['reason']),
        knowledge_version=str(data['knowledge_version']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_test_finished_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return TestFinishedEvent(
        node_id=str(data['node_id']),
        node_stable_id=str(data['node_stable_id']),
        engine_name=str(data['engine_name']),
        test_name=str(data['test_name']),
        test_path=str(data['test_path']),
        status=str(data['status']),
        duration=float(data['duration']),
        failure_kind=_cast_optional_str(data.get('failure_kind')),
        error_code=_cast_optional_str(data.get('error_code')),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_resource_lifecycle_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return ResourceLifecycleEvent(
        action=str(data['action']),
        name=str(data['name']),
        scope=str(data['scope']),
        test_id=_cast_optional_str(data.get('test_id')),
        external_handle=_cast_optional_str(data.get('external_handle')),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_resource_readiness_transition_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return ResourceReadinessTransitionEvent(
        name=str(data['name']),
        scope=str(data['scope']),
        status=str(data['status']),
        reason=_cast_optional_str(data.get('reason')),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_worker_heartbeat_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return WorkerHeartbeatEvent(
        worker_id=int(data['worker_id']),
        status=str(data['status']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_worker_degraded_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return WorkerDegradedEvent(
        worker_id=int(data['worker_id']),
        reason=str(data['reason']),
        metadata=metadata,
        timestamp=timestamp,
    )


def _deserialize_worker_recovered_event(
    data: dict[str, object],
    metadata: DomainEventMetadata,
    timestamp: float,
) -> DomainEvent:
    return WorkerRecoveredEvent(
        worker_id=int(data['worker_id']),
        metadata=metadata,
        timestamp=timestamp,
    )


_DOMAIN_EVENT_DESERIALIZERS = {
    'definition.materialized': _deserialize_definition_materialized_event,
    'engine.snapshot_updated': _deserialize_engine_snapshot_updated_event,
    'knowledge.indexed': _deserialize_knowledge_indexed_event,
    'knowledge.invalidated': _deserialize_knowledge_invalidated_event,
    'log.chunk': _deserialize_log_chunk_event,
    'registry.knowledge_indexed': (
        _deserialize_registry_knowledge_indexed_event
    ),
    'node.assigned': _deserialize_node_assigned_event,
    'node.enqueued': _deserialize_node_enqueued_event,
    'node.requeued': _deserialize_node_requeued_event,
    'node.retrying': _deserialize_node_retrying_event,
    'node.scheduled': _deserialize_node_scheduled_event,
    'plan.analyzed': _deserialize_plan_analyzed_event,
    'resource.lifecycle': _deserialize_resource_lifecycle_event,
    'resource.readiness_transition': (
        _deserialize_resource_readiness_transition_event
    ),
    'session.finished': _deserialize_session_finished_event,
    'session.started': _deserialize_session_started_event,
    'step.finished': _deserialize_step_finished_event,
    'step.started': _deserialize_step_started_event,
    'test.finished': _deserialize_test_finished_event,
    'test.knowledge_indexed': _deserialize_test_knowledge_indexed_event,
    'test.knowledge_invalidated': (
        _deserialize_test_knowledge_invalidated_event
    ),
    'test.started': _deserialize_test_started_event,
    'worker.degraded': _deserialize_worker_degraded_event,
    'worker.heartbeat': _deserialize_worker_heartbeat_event,
    'worker.recovered': _deserialize_worker_recovered_event,
}


def _serialize_domain_event_metadata(
    metadata: DomainEventMetadata,
) -> dict[str, object]:
    return {
        'correlation_id': metadata.correlation_id,
        'event_id': metadata.event_id,
        'idempotency_key': metadata.idempotency_key,
        'node_id': metadata.node_id,
        'node_stable_id': metadata.node_stable_id,
        'plan_id': metadata.plan_id,
        'sequence_number': metadata.sequence_number,
        'session_id': metadata.session_id,
        'trace_id': metadata.trace_id,
        'worker_id': metadata.worker_id,
    }


def _deserialize_domain_event_metadata(
    value: object,
) -> DomainEventMetadata:
    if not isinstance(value, dict):
        msg = 'Domain event metadata payload must be a mapping'
        raise TypeError(msg)

    return DomainEventMetadata(
        event_id=str(value['event_id']),
        sequence_number=_cast_optional_int(value.get('sequence_number')),
        correlation_id=_cast_optional_str(value.get('correlation_id')),
        idempotency_key=_cast_optional_str(value.get('idempotency_key')),
        session_id=_cast_optional_str(value.get('session_id')),
        plan_id=_cast_optional_str(value.get('plan_id')),
        node_id=_cast_optional_str(value.get('node_id')),
        node_stable_id=_cast_optional_str(value.get('node_stable_id')),
        trace_id=_cast_optional_str(value.get('trace_id')),
        worker_id=_cast_optional_int(value.get('worker_id')),
    )


def _cast_optional_str(value: object) -> str | None:
    if value is None:
        return None

    return str(value)


def _cast_optional_int(value: object) -> int | None:
    if value is None:
        return None

    return int(value)


def _cast_optional_float(value: object) -> float | None:
    if value is None:
        return None

    return float(value)
