from __future__ import annotations

import asyncio
import queue
import shutil
import sqlite3
import threading

from collections import OrderedDict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord
from cosecha.core.discovery import get_definition_query_provider
from cosecha.core.domain_events import (
    DefinitionMaterializedEvent,
    DomainEvent,
    EngineSnapshotUpdatedEvent,
    KnowledgeIndexedEvent,
    KnowledgeInvalidatedEvent,
    LogChunkEvent,
    NodeRetryingEvent,
    NodeScheduledEvent,
    PlanAnalyzedEvent,
    RegistryKnowledgeIndexedEvent,
    ResourceLifecycleEvent,
    ResourceReadinessTransitionEvent,
    SessionFinishedEvent,
    SessionStartedEvent,
    StepFinishedEvent,
    StepStartedEvent,
    TestFinishedEvent,
    TestKnowledgeIndexedEvent,
    TestKnowledgeInvalidatedEvent,
    TestStartedEvent,
    WorkerDegradedEvent,
    WorkerHeartbeatEvent,
    WorkerRecoveredEvent,
    deserialize_domain_event,
    serialize_domain_event,
)
from cosecha.core.items import FailureKind  # noqa: TC001
from cosecha.core.registry_knowledge import (
    RegistryKnowledgeEntry,
    RegistryKnowledgeQuery,
    RegistryKnowledgeSnapshot,
)
from cosecha.core.serialization import (
    decode_json,
    decode_json_dict,
    decode_json_list,
    encode_json_text,
)
from cosecha.core.session_artifacts import (
    SessionArtifact,
    default_session_artifact_persistence_policy,
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable


KNOWLEDGE_BASE_PATH = Path('.cosecha/kb.db')
LEGACY_KNOWLEDGE_BASE_PATH = Path('.cosecha/knowledge_base.sqlite3')
KNOWLEDGE_BASE_SCHEMA_VERSION = 15
MAX_IDEMPOTENCY_KEYS = 4096
MAX_IDEMPOTENCY_KEY_AGE_SECONDS = 1800.0
KNOWLEDGE_BASE_PERSIST_BATCH_SIZE = 128
KNOWLEDGE_BASE_PERSIST_QUEUE_MAXSIZE = 1024
PERSISTENCE_WORKER_JOIN_TIMEOUT_SECONDS = 5.0
PERSISTENT_KNOWLEDGE_BASE_CLOSE_TIMEOUT_SECONDS = 5.0
LIVE_EXECUTION_EVENT_TAIL_LIMIT = 256
LIVE_EXECUTION_RUNNING_TEST_LIMIT = 128
LIVE_EXECUTION_WORKER_LIMIT = 64
LIVE_EXECUTION_RESOURCE_LIMIT = 128
LIVE_EXECUTION_LOG_CHUNK_LIMIT = 256


def _iter_sqlite_sidecar_suffixes() -> tuple[str, ...]:
    return ('', '-wal', '-shm', '-journal')


def iter_knowledge_base_file_paths(db_path: Path) -> tuple[Path, ...]:
    return tuple(
        Path(f'{db_path}{suffix}')
        for suffix in _iter_sqlite_sidecar_suffixes()
    )


def resolve_knowledge_base_path(
    root_path: Path,
    *,
    knowledge_storage_root: Path | None = None,
    migrate_legacy: bool = True,
) -> Path:
    if knowledge_storage_root is None:
        db_path = root_path / KNOWLEDGE_BASE_PATH
        legacy_db_path = root_path / LEGACY_KNOWLEDGE_BASE_PATH
    else:
        db_path = knowledge_storage_root.resolve() / 'kb.db'
        legacy_db_path = root_path / LEGACY_KNOWLEDGE_BASE_PATH
        legacy_current_db_path = root_path / KNOWLEDGE_BASE_PATH
        if db_path.exists():
            return db_path
        if legacy_current_db_path.exists():
            legacy_db_path = legacy_current_db_path

    if db_path.exists():
        return db_path

    if not legacy_db_path.exists():
        return db_path

    if not migrate_legacy:
        return legacy_db_path

    db_path.parent.mkdir(parents=True, exist_ok=True)
    for source_path, target_path in zip(
        iter_knowledge_base_file_paths(legacy_db_path),
        iter_knowledge_base_file_paths(db_path),
        strict=True,
    ):
        if not source_path.exists() or target_path.exists():
            continue
        shutil.move(str(source_path), str(target_path))

    return db_path if db_path.exists() else legacy_db_path


def _workspace_scope_key(
    *,
    root_path: str,
    workspace_fingerprint: str | None,
) -> str:
    return workspace_fingerprint or root_path


@dataclass(slots=True, frozen=True)
class IdempotencyWindowPolicy:
    max_entries: int = MAX_IDEMPOTENCY_KEYS
    max_age_seconds: float | None = MAX_IDEMPOTENCY_KEY_AGE_SECONDS


@dataclass(slots=True, frozen=True)
class SessionKnowledge:
    root_path: str
    workspace_fingerprint: str | None
    concurrency: int
    session_id: str | None
    trace_id: str | None
    started_at: float
    finished_at: float | None = None
    has_failures: bool | None = None


@dataclass(slots=True, frozen=True)
class PlanKnowledge:
    mode: str
    executable: bool
    node_count: int
    issue_count: int
    plan_id: str | None
    correlation_id: str | None
    session_id: str | None
    trace_id: str | None
    analyzed_at: float


@dataclass(slots=True, frozen=True)
class TestKnowledge:
    node_id: str
    node_stable_id: str
    engine_name: str
    test_name: str
    test_path: str
    correlation_id: str | None = None
    session_id: str | None = None
    plan_id: str | None = None
    trace_id: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    status: str | None = None
    duration: float | None = None
    selection_labels: tuple[str, ...] = ()
    source_line: int | None = None
    discovery_mode: str | None = None
    knowledge_version: str | None = None
    content_hash: str | None = None
    indexed_at: float | None = None
    invalidated_at: float | None = None
    invalidation_reason: str | None = None
    worker_slot: int | None = None
    scheduled_at: float | None = None
    max_attempts: int | None = None
    timeout_seconds: float | None = None
    retry_count: int = 0
    failure_kind: FailureKind | None = None
    last_error_code: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'correlation_id': self.correlation_id,
            'duration': self.duration,
            'engine_name': self.engine_name,
            'failure_kind': self.failure_kind,
            'finished_at': self.finished_at,
            'discovery_mode': self.discovery_mode,
            'content_hash': self.content_hash,
            'indexed_at': self.indexed_at,
            'invalidated_at': self.invalidated_at,
            'invalidation_reason': self.invalidation_reason,
            'knowledge_version': self.knowledge_version,
            'last_error_code': self.last_error_code,
            'max_attempts': self.max_attempts,
            'node_id': self.node_id,
            'node_stable_id': self.node_stable_id,
            'plan_id': self.plan_id,
            'retry_count': self.retry_count,
            'scheduled_at': self.scheduled_at,
            'selection_labels': list(self.selection_labels),
            'session_id': self.session_id,
            'source_line': self.source_line,
            'started_at': self.started_at,
            'status': self.status,
            'test_name': self.test_name,
            'test_path': self.test_path,
            'timeout_seconds': self.timeout_seconds,
            'trace_id': self.trace_id,
            'worker_slot': self.worker_slot,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> TestKnowledge:
        return cls(
            node_id=str(data['node_id']),
            node_stable_id=str(data['node_stable_id']),
            engine_name=str(data['engine_name']),
            test_name=str(data['test_name']),
            test_path=str(data['test_path']),
            correlation_id=_cast_optional_str(data.get('correlation_id')),
            session_id=_cast_optional_str(data.get('session_id')),
            plan_id=_cast_optional_str(data.get('plan_id')),
            trace_id=_cast_optional_str(data.get('trace_id')),
            started_at=_cast_optional_float(data.get('started_at')),
            finished_at=_cast_optional_float(data.get('finished_at')),
            status=_cast_optional_str(data.get('status')),
            duration=_cast_optional_float(data.get('duration')),
            selection_labels=tuple(
                str(label) for label in data.get('selection_labels', ())
            ),
            source_line=_cast_optional_int(data.get('source_line')),
            discovery_mode=_cast_optional_str(data.get('discovery_mode')),
            content_hash=_cast_optional_str(data.get('content_hash')),
            knowledge_version=_cast_optional_str(
                data.get('knowledge_version'),
            ),
            indexed_at=_cast_optional_float(data.get('indexed_at')),
            invalidated_at=_cast_optional_float(
                data.get('invalidated_at'),
            ),
            invalidation_reason=_cast_optional_str(
                data.get('invalidation_reason'),
            ),
            worker_slot=_cast_optional_int(data.get('worker_slot')),
            scheduled_at=_cast_optional_float(data.get('scheduled_at')),
            max_attempts=_cast_optional_int(data.get('max_attempts')),
            timeout_seconds=_cast_optional_float(
                data.get('timeout_seconds'),
            ),
            retry_count=int(data.get('retry_count', 0)),
            failure_kind=_cast_optional_str(data.get('failure_kind')),
            last_error_code=_cast_optional_str(data.get('last_error_code')),
        )


@dataclass(slots=True, frozen=True)
class ResourceKnowledge:
    name: str
    scope: str
    readiness_status: str | None = None
    readiness_reason: str | None = None
    correlation_id: str | None = None
    session_id: str | None = None
    plan_id: str | None = None
    trace_id: str | None = None
    last_worker_id: int | None = None
    owner_worker_id: int | None = None
    last_node_id: str | None = None
    last_node_stable_id: str | None = None
    owner_node_id: str | None = None
    owner_node_stable_id: str | None = None
    acquire_count: int = 0
    release_count: int = 0
    last_test_id: str | None = None
    owner_test_id: str | None = None
    external_handle: str | None = None
    last_heartbeat_at: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'acquire_count': self.acquire_count,
            'correlation_id': self.correlation_id,
            'external_handle': self.external_handle,
            'last_node_id': self.last_node_id,
            'last_node_stable_id': self.last_node_stable_id,
            'last_heartbeat_at': self.last_heartbeat_at,
            'last_test_id': self.last_test_id,
            'last_worker_id': self.last_worker_id,
            'name': self.name,
            'owner_node_id': self.owner_node_id,
            'owner_node_stable_id': self.owner_node_stable_id,
            'owner_test_id': self.owner_test_id,
            'owner_worker_id': self.owner_worker_id,
            'plan_id': self.plan_id,
            'readiness_reason': self.readiness_reason,
            'readiness_status': self.readiness_status,
            'release_count': self.release_count,
            'scope': self.scope,
            'session_id': self.session_id,
            'trace_id': self.trace_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ResourceKnowledge:
        return cls(
            name=str(data['name']),
            scope=str(data['scope']),
            readiness_status=_cast_optional_str(
                data.get('readiness_status'),
            ),
            readiness_reason=_cast_optional_str(
                data.get('readiness_reason'),
            ),
            correlation_id=_cast_optional_str(data.get('correlation_id')),
            session_id=_cast_optional_str(data.get('session_id')),
            plan_id=_cast_optional_str(data.get('plan_id')),
            trace_id=_cast_optional_str(data.get('trace_id')),
            last_worker_id=_cast_optional_int(data.get('last_worker_id')),
            owner_worker_id=_cast_optional_int(
                data.get('owner_worker_id'),
            ),
            last_node_id=_cast_optional_str(data.get('last_node_id')),
            last_node_stable_id=_cast_optional_str(
                data.get('last_node_stable_id'),
            ),
            owner_node_id=_cast_optional_str(data.get('owner_node_id')),
            owner_node_stable_id=_cast_optional_str(
                data.get('owner_node_stable_id'),
            ),
            acquire_count=int(data.get('acquire_count', 0)),
            release_count=int(data.get('release_count', 0)),
            last_test_id=_cast_optional_str(data.get('last_test_id')),
            owner_test_id=_cast_optional_str(data.get('owner_test_id')),
            external_handle=_cast_optional_str(data.get('external_handle')),
            last_heartbeat_at=_cast_optional_float(
                data.get('last_heartbeat_at'),
            ),
        )


@dataclass(slots=True, frozen=True)
class WorkerKnowledge:
    worker_id: int
    status: str
    session_id: str | None = None
    plan_id: str | None = None
    trace_id: str | None = None
    last_heartbeat_at: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'last_heartbeat_at': self.last_heartbeat_at,
            'plan_id': self.plan_id,
            'session_id': self.session_id,
            'status': self.status,
            'trace_id': self.trace_id,
            'worker_id': self.worker_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> WorkerKnowledge:
        return cls(
            worker_id=int(data['worker_id']),
            status=str(data['status']),
            session_id=_cast_optional_str(data.get('session_id')),
            plan_id=_cast_optional_str(data.get('plan_id')),
            trace_id=_cast_optional_str(data.get('trace_id')),
            last_heartbeat_at=_cast_optional_float(
                data.get('last_heartbeat_at'),
            ),
        )


@dataclass(slots=True, frozen=True)
class DefinitionKnowledge:
    engine_name: str
    file_path: str
    definition_count: int
    discovery_mode: str
    descriptors: tuple[DefinitionKnowledgeRecord, ...] = ()
    knowledge_version: str | None = None
    content_hash: str | None = None
    correlation_id: str | None = None
    session_id: str | None = None
    trace_id: str | None = None
    indexed_at: float | None = None
    invalidated_at: float | None = None
    invalidation_reason: str | None = None
    materialized_count: int = 0
    last_materialized_at: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'correlation_id': self.correlation_id,
            'definition_count': self.definition_count,
            'descriptors': [
                descriptor.to_dict() for descriptor in self.descriptors
            ],
            'discovery_mode': self.discovery_mode,
            'engine_name': self.engine_name,
            'file_path': self.file_path,
            'content_hash': self.content_hash,
            'indexed_at': self.indexed_at,
            'invalidated_at': self.invalidated_at,
            'invalidation_reason': self.invalidation_reason,
            'knowledge_version': self.knowledge_version,
            'last_materialized_at': self.last_materialized_at,
            'materialized_count': self.materialized_count,
            'session_id': self.session_id,
            'trace_id': self.trace_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> DefinitionKnowledge:
        return cls(
            engine_name=str(data['engine_name']),
            file_path=str(data['file_path']),
            definition_count=int(data['definition_count']),
            discovery_mode=str(data['discovery_mode']),
            descriptors=tuple(
                DefinitionKnowledgeRecord.from_dict(record)
                for record in data.get('descriptors', ())
                if isinstance(record, dict)
            ),
            content_hash=_cast_optional_str(data.get('content_hash')),
            knowledge_version=_cast_optional_str(
                data.get('knowledge_version'),
            ),
            correlation_id=_cast_optional_str(data.get('correlation_id')),
            session_id=_cast_optional_str(data.get('session_id')),
            trace_id=_cast_optional_str(data.get('trace_id')),
            indexed_at=_cast_optional_float(data.get('indexed_at')),
            invalidated_at=_cast_optional_float(data.get('invalidated_at')),
            invalidation_reason=_cast_optional_str(
                data.get('invalidation_reason'),
            ),
            materialized_count=int(data.get('materialized_count', 0)),
            last_materialized_at=_cast_optional_float(
                data.get('last_materialized_at'),
            ),
        )

    def matching_descriptors(
        self,
        *,
        step_type: str | None = None,
        step_text: str | None = None,
    ) -> tuple[DefinitionKnowledgeRecord, ...]:
        if step_type is None and step_text is None:
            return self.descriptors

        provider = get_definition_query_provider(self.engine_name)
        if provider is not None:
            return tuple(
                provider.matching_descriptors(
                    self.descriptors,
                    step_type=step_type,
                    step_text=step_text,
                ),
            )

        return tuple(
            descriptor
            for descriptor in self.descriptors
            if step_type is None and step_text is None
        )


@dataclass(slots=True, frozen=True)
class KnowledgeSnapshot:
    session: SessionKnowledge | None = None
    latest_plan: PlanKnowledge | None = None
    tests: tuple[TestKnowledge, ...] = field(default_factory=tuple)
    resources: tuple[ResourceKnowledge, ...] = field(default_factory=tuple)
    workers: tuple[WorkerKnowledge, ...] = field(default_factory=tuple)
    definitions: tuple[DefinitionKnowledge, ...] = field(default_factory=tuple)
    registry_snapshots: tuple[RegistryKnowledgeSnapshot, ...] = field(
        default_factory=tuple,
    )


@dataclass(slots=True, frozen=True)
class LiveTestKnowledge:
    node_id: str
    node_stable_id: str
    engine_name: str
    test_name: str
    status: str
    started_at: float | None = None
    worker_slot: int | None = None
    current_step: LiveStepKnowledge | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'engine_name': self.engine_name,
            'node_id': self.node_id,
            'node_stable_id': self.node_stable_id,
            'started_at': self.started_at,
            'status': self.status,
            'test_name': self.test_name,
            'worker_slot': self.worker_slot,
            'current_step': (
                None
                if self.current_step is None
                else self.current_step.to_dict()
            ),
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LiveTestKnowledge:
        return cls(
            node_id=str(data['node_id']),
            node_stable_id=str(data['node_stable_id']),
            engine_name=str(data['engine_name']),
            test_name=str(data['test_name']),
            status=str(data['status']),
            started_at=_cast_optional_float(data.get('started_at')),
            worker_slot=_cast_optional_int(data.get('worker_slot')),
            current_step=(
                LiveStepKnowledge.from_dict(current_step)
                if isinstance(
                    current_step := data.get('current_step'),
                    dict,
                )
                else None
            ),
        )


@dataclass(slots=True, frozen=True)
class LiveStepKnowledge:
    step_type: str
    step_keyword: str
    step_text: str
    status: str
    source_line: int | None = None
    message: str | None = None
    updated_at: float | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'message': self.message,
            'source_line': self.source_line,
            'status': self.status,
            'step_keyword': self.step_keyword,
            'step_text': self.step_text,
            'step_type': self.step_type,
            'updated_at': self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LiveStepKnowledge:
        return cls(
            step_type=str(data['step_type']),
            step_keyword=str(data['step_keyword']),
            step_text=str(data['step_text']),
            status=str(data['status']),
            source_line=_cast_optional_int(data.get('source_line')),
            message=_cast_optional_str(data.get('message')),
            updated_at=_cast_optional_float(data.get('updated_at')),
        )


@dataclass(slots=True, frozen=True)
class LiveLogChunk:
    message: str
    level: str
    logger_name: str
    emitted_at: float
    node_stable_id: str | None = None
    worker_id: int | None = None
    sequence_number: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'emitted_at': self.emitted_at,
            'level': self.level,
            'logger_name': self.logger_name,
            'message': self.message,
            'node_stable_id': self.node_stable_id,
            'sequence_number': self.sequence_number,
            'worker_id': self.worker_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LiveLogChunk:
        return cls(
            message=str(data['message']),
            level=str(data['level']),
            logger_name=str(data['logger_name']),
            emitted_at=float(data['emitted_at']),
            node_stable_id=_cast_optional_str(data.get('node_stable_id')),
            worker_id=_cast_optional_int(data.get('worker_id')),
            sequence_number=_cast_optional_int(data.get('sequence_number')),
        )


@dataclass(slots=True, frozen=True)
class LiveEngineSnapshot:
    engine_name: str
    snapshot_kind: str
    node_stable_id: str
    payload: dict[str, object]
    worker_id: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'engine_name': self.engine_name,
            'snapshot_kind': self.snapshot_kind,
            'node_stable_id': self.node_stable_id,
            'payload': self.payload,
            'worker_id': self.worker_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LiveEngineSnapshot:
        payload = data.get('payload')
        return cls(
            engine_name=str(data['engine_name']),
            snapshot_kind=str(data['snapshot_kind']),
            node_stable_id=str(data['node_stable_id']),
            payload=payload if isinstance(payload, dict) else {},
            worker_id=_cast_optional_int(data.get('worker_id')),
        )


@dataclass(slots=True, frozen=True)
class LiveExecutionSnapshot:
    session_id: str | None = None
    trace_id: str | None = None
    plan_id: str | None = None
    last_sequence_number: int = 0
    running_tests: tuple[LiveTestKnowledge, ...] = field(
        default_factory=tuple,
    )
    truncated_running_test_count: int = 0
    workers: tuple[WorkerKnowledge, ...] = field(default_factory=tuple)
    truncated_worker_count: int = 0
    resources: tuple[ResourceKnowledge, ...] = field(default_factory=tuple)
    truncated_resource_count: int = 0
    recent_log_chunks: tuple[LiveLogChunk, ...] = field(
        default_factory=tuple,
    )
    truncated_log_chunk_count: int = 0
    engine_snapshots: tuple[LiveEngineSnapshot, ...] = field(
        default_factory=tuple,
    )
    recent_events: tuple[DomainEvent, ...] = field(default_factory=tuple)
    truncated_event_count: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            'engine_snapshots': [
                snapshot.to_dict() for snapshot in self.engine_snapshots
            ],
            'last_sequence_number': self.last_sequence_number,
            'plan_id': self.plan_id,
            'recent_events': [
                serialize_domain_event(event) for event in self.recent_events
            ],
            'resources': [resource.to_dict() for resource in self.resources],
            'running_tests': [test.to_dict() for test in self.running_tests],
            'session_id': self.session_id,
            'trace_id': self.trace_id,
            'truncated_event_count': self.truncated_event_count,
            'truncated_log_chunk_count': self.truncated_log_chunk_count,
            'truncated_resource_count': self.truncated_resource_count,
            'truncated_running_test_count': (
                self.truncated_running_test_count
            ),
            'truncated_worker_count': self.truncated_worker_count,
            'workers': [worker.to_dict() for worker in self.workers],
            'recent_log_chunks': [
                log_chunk.to_dict() for log_chunk in self.recent_log_chunks
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LiveExecutionSnapshot:
        return cls(
            session_id=_cast_optional_str(data.get('session_id')),
            trace_id=_cast_optional_str(data.get('trace_id')),
            plan_id=_cast_optional_str(data.get('plan_id')),
            last_sequence_number=int(data.get('last_sequence_number', 0)),
            engine_snapshots=tuple(
                LiveEngineSnapshot.from_dict(record)
                for record in data.get('engine_snapshots', ())
                if isinstance(record, dict)
            ),
            running_tests=tuple(
                LiveTestKnowledge.from_dict(record)
                for record in data.get('running_tests', ())
                if isinstance(record, dict)
            ),
            truncated_running_test_count=int(
                data.get('truncated_running_test_count', 0),
            ),
            workers=tuple(
                WorkerKnowledge.from_dict(record)
                for record in data.get('workers', ())
                if isinstance(record, dict)
            ),
            truncated_worker_count=int(
                data.get('truncated_worker_count', 0),
            ),
            resources=tuple(
                ResourceKnowledge.from_dict(record)
                for record in data.get('resources', ())
                if isinstance(record, dict)
            ),
            truncated_resource_count=int(
                data.get('truncated_resource_count', 0),
            ),
            recent_log_chunks=tuple(
                LiveLogChunk.from_dict(record)
                for record in data.get('recent_log_chunks', ())
                if isinstance(record, dict)
            ),
            truncated_log_chunk_count=int(
                data.get('truncated_log_chunk_count', 0),
            ),
            recent_events=tuple(
                deserialize_domain_event(record)
                for record in data.get('recent_events', ())
                if isinstance(record, dict)
            ),
            truncated_event_count=int(
                data.get('truncated_event_count', 0),
            ),
        )


@dataclass(slots=True, frozen=True)
class SessionArtifactQuery:
    session_id: str | None = None
    trace_id: str | None = None
    limit: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'limit': self.limit,
            'session_id': self.session_id,
            'trace_id': self.trace_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SessionArtifactQuery:
        return cls(
            session_id=_cast_optional_str(data.get('session_id')),
            trace_id=_cast_optional_str(data.get('trace_id')),
            limit=_cast_optional_int(data.get('limit')),
        )


@dataclass(slots=True, frozen=True)
class DomainEventQuery:
    event_type: str | None = None
    session_id: str | None = None
    plan_id: str | None = None
    node_stable_id: str | None = None
    after_sequence_number: int | None = None
    limit: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'after_sequence_number': self.after_sequence_number,
            'event_type': self.event_type,
            'limit': self.limit,
            'node_stable_id': self.node_stable_id,
            'plan_id': self.plan_id,
            'session_id': self.session_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> DomainEventQuery:
        return cls(
            event_type=_cast_optional_str(data.get('event_type')),
            session_id=_cast_optional_str(data.get('session_id')),
            plan_id=_cast_optional_str(data.get('plan_id')),
            node_stable_id=_cast_optional_str(data.get('node_stable_id')),
            after_sequence_number=_cast_optional_int(
                data.get('after_sequence_number'),
            ),
            limit=_cast_optional_int(data.get('limit')),
        )


@dataclass(slots=True, frozen=True)
class TestKnowledgeQuery:
    __test__ = False
    engine_name: str | None = None
    test_path: str | None = None
    status: str | None = None
    failure_kind: FailureKind | None = None
    node_stable_id: str | None = None
    plan_id: str | None = None
    limit: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'engine_name': self.engine_name,
            'failure_kind': self.failure_kind,
            'limit': self.limit,
            'node_stable_id': self.node_stable_id,
            'plan_id': self.plan_id,
            'status': self.status,
            'test_path': self.test_path,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> TestKnowledgeQuery:
        return cls(
            engine_name=_cast_optional_str(data.get('engine_name')),
            test_path=_cast_optional_str(data.get('test_path')),
            status=_cast_optional_str(data.get('status')),
            failure_kind=_cast_optional_str(data.get('failure_kind')),
            node_stable_id=_cast_optional_str(data.get('node_stable_id')),
            plan_id=_cast_optional_str(data.get('plan_id')),
            limit=_cast_optional_int(data.get('limit')),
        )


@dataclass(slots=True, frozen=True)
class DefinitionKnowledgeQuery:
    __test__ = False
    engine_name: str | None = None
    file_path: str | None = None
    step_type: str | None = None
    step_text: str | None = None
    discovery_mode: str | None = None
    include_invalidated: bool = True
    limit: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'discovery_mode': self.discovery_mode,
            'engine_name': self.engine_name,
            'file_path': self.file_path,
            'include_invalidated': self.include_invalidated,
            'limit': self.limit,
            'step_text': self.step_text,
            'step_type': self.step_type,
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> DefinitionKnowledgeQuery:
        return cls(
            engine_name=_cast_optional_str(data.get('engine_name')),
            file_path=_cast_optional_str(data.get('file_path')),
            step_type=_cast_optional_str(data.get('step_type')),
            step_text=_cast_optional_str(data.get('step_text')),
            discovery_mode=_cast_optional_str(data.get('discovery_mode')),
            include_invalidated=bool(
                data.get('include_invalidated', True),
            ),
            limit=_cast_optional_int(data.get('limit')),
        )


@dataclass(slots=True, frozen=True)
class ResourceKnowledgeQuery:
    __test__ = False
    name: str | None = None
    scope: str | None = None
    last_test_id: str | None = None
    limit: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'last_test_id': self.last_test_id,
            'limit': self.limit,
            'name': self.name,
            'scope': self.scope,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ResourceKnowledgeQuery:
        return cls(
            name=_cast_optional_str(data.get('name')),
            scope=_cast_optional_str(data.get('scope')),
            last_test_id=_cast_optional_str(data.get('last_test_id')),
            limit=_cast_optional_int(data.get('limit')),
        )

class KnowledgeReader(Protocol):
    def snapshot(self) -> KnowledgeSnapshot: ...

    def query_domain_events(
        self,
        query: DomainEventQuery,
    ) -> tuple[DomainEvent, ...]: ...

    def query_tests(
        self,
        query: TestKnowledgeQuery,
    ) -> tuple[TestKnowledge, ...]: ...

    def query_definitions(
        self,
        query: DefinitionKnowledgeQuery,
    ) -> tuple[DefinitionKnowledge, ...]: ...

    def query_registry_items(
        self,
        query: RegistryKnowledgeQuery,
    ) -> tuple[RegistryKnowledgeSnapshot, ...]: ...

    def query_resources(
        self,
        query: ResourceKnowledgeQuery,
    ) -> tuple[ResourceKnowledge, ...]: ...

    def query_session_artifacts(
        self,
        query: SessionArtifactQuery,
    ) -> tuple[SessionArtifact, ...]: ...

    def close(self) -> None: ...


class LiveExecutionReader(Protocol):
    def live_snapshot(self) -> LiveExecutionSnapshot: ...


class KnowledgeBase(KnowledgeReader, LiveExecutionReader, Protocol):
    def apply(self, event: DomainEvent) -> None: ...

    def store_session_artifact(self, artifact: SessionArtifact) -> None: ...


class InMemoryKnowledgeBase:
    __slots__ = (
        '_active_tests',
        '_definitions',
        '_domain_events',
        '_event_handlers',
        '_idempotency_policy',
        '_last_sequence_number',
        '_latest_plan',
        '_live_current_steps',
        '_live_engine_snapshots',
        '_live_event_tail',
        '_live_event_truncated_count',
        '_live_log_chunk_tail',
        '_live_log_chunk_truncated_count',
        '_processed_idempotency_keys',
        '_registry_snapshots',
        '_resources',
        '_session',
        '_session_artifacts',
        '_tests',
        '_workers',
    )

    def __init__(self) -> None:
        self._session: SessionKnowledge | None = None
        self._latest_plan: PlanKnowledge | None = None
        self._last_sequence_number = 0
        self._domain_events: dict[int, DomainEvent] = {}
        self._active_tests: dict[str, LiveTestKnowledge] = {}
        self._live_event_tail: deque[DomainEvent] = deque(
            maxlen=LIVE_EXECUTION_EVENT_TAIL_LIMIT,
        )
        self._live_event_truncated_count = 0
        self._live_log_chunk_tail: deque[LiveLogChunk] = deque(
            maxlen=LIVE_EXECUTION_LOG_CHUNK_LIMIT,
        )
        self._live_log_chunk_truncated_count = 0
        self._live_current_steps: dict[str, LiveStepKnowledge] = {}
        self._live_engine_snapshots: dict[str, LiveEngineSnapshot] = {}
        self._tests: dict[str, TestKnowledge] = {}
        self._resources: dict[tuple[str, str], ResourceKnowledge] = {}
        self._workers: dict[int, WorkerKnowledge] = {}
        self._definitions: dict[tuple[str, str], DefinitionKnowledge] = {}
        self._registry_snapshots: dict[
            tuple[str, str, str, str, str],
            RegistryKnowledgeSnapshot,
        ] = {}
        self._session_artifacts: dict[str, SessionArtifact] = {}
        self._idempotency_policy = IdempotencyWindowPolicy(
            max_entries=MAX_IDEMPOTENCY_KEYS,
            max_age_seconds=MAX_IDEMPOTENCY_KEY_AGE_SECONDS,
        )
        self._processed_idempotency_keys: OrderedDict[str, float] = (
            OrderedDict()
        )
        self._event_handlers: dict[
            type[DomainEvent],
            Callable[[DomainEvent], None],
        ] = {
            SessionStartedEvent: self._apply_session_event,
            SessionFinishedEvent: self._apply_session_event,
            PlanAnalyzedEvent: self._apply_plan_event,
            NodeScheduledEvent: self._apply_test_event,
            NodeRetryingEvent: self._apply_test_event,
            TestStartedEvent: self._apply_test_event,
            TestFinishedEvent: self._apply_test_event,
            StepStartedEvent: self._apply_live_step_event,
            StepFinishedEvent: self._apply_live_step_event,
            EngineSnapshotUpdatedEvent: self._apply_live_engine_snapshot_event,
            LogChunkEvent: self._apply_live_log_chunk_event,
            TestKnowledgeIndexedEvent: self._apply_test_knowledge_event,
            TestKnowledgeInvalidatedEvent: self._apply_test_knowledge_event,
            KnowledgeIndexedEvent: self._apply_definition_event,
            KnowledgeInvalidatedEvent: self._apply_definition_event,
            DefinitionMaterializedEvent: self._apply_definition_event,
            RegistryKnowledgeIndexedEvent: (
                self._apply_registry_knowledge_event
            ),
            ResourceLifecycleEvent: self._apply_resource_event,
            ResourceReadinessTransitionEvent: (
                self._apply_resource_readiness_transition_event
            ),
            WorkerDegradedEvent: self._apply_worker_degraded_event,
            WorkerHeartbeatEvent: self._apply_worker_heartbeat_event,
            WorkerRecoveredEvent: self._apply_worker_recovered_event,
        }

    @property
    def session(self) -> SessionKnowledge | None:
        return self._session

    @property
    def latest_plan(self) -> PlanKnowledge | None:
        return self._latest_plan

    def get_test(
        self,
        node_stable_id: str,
    ) -> TestKnowledge | None:
        return self._tests.get(node_stable_id)

    def get_resource(
        self,
        name: str,
        scope: str,
    ) -> ResourceKnowledge | None:
        return self._resources.get((name, scope))

    def get_worker(
        self,
        worker_id: int,
    ) -> WorkerKnowledge | None:
        return self._workers.get(worker_id)

    def get_definition(
        self,
        engine_name: str,
        file_path: str,
    ) -> DefinitionKnowledge | None:
        return self._definitions.get((engine_name, file_path))

    def get_registry_snapshot(
        self,
        engine_name: str,
        module_spec: str,
        package_hash: str,
        layout_key: str,
        loader_schema_version: str,
    ) -> RegistryKnowledgeSnapshot | None:
        return self._registry_snapshots.get(
            (
                engine_name,
                module_spec,
                package_hash,
                layout_key,
                loader_schema_version,
            ),
        )

    def clear_runtime_projection(self) -> None:
        self._session = None
        self._latest_plan = None
        self._active_tests = {}
        self._resources = {}
        self._workers = {}
        self._live_current_steps = {}
        self._live_engine_snapshots = {}
        self._live_log_chunk_tail.clear()
        self._live_log_chunk_truncated_count = 0
        self._live_event_tail.clear()
        self._live_event_truncated_count = 0
        self._processed_idempotency_keys = OrderedDict(
            (
                key,
                timestamp,
            )
            for key, timestamp in self._processed_idempotency_keys.items()
            if not key.startswith('runtime:')
        )

    def load_snapshot(self, snapshot: KnowledgeSnapshot) -> None:
        self._session = snapshot.session
        self._latest_plan = snapshot.latest_plan
        self._active_tests = {}
        self._live_current_steps = {}
        self._live_engine_snapshots = {}
        self._live_log_chunk_tail.clear()
        self._live_log_chunk_truncated_count = 0
        self._live_event_tail.clear()
        self._live_event_truncated_count = 0
        self._tests = {test.node_stable_id: test for test in snapshot.tests}
        self._resources = {
            (resource.name, resource.scope): resource
            for resource in snapshot.resources
        }
        self._definitions = {
            (definition.engine_name, definition.file_path): definition
            for definition in snapshot.definitions
        }
        self._registry_snapshots = {
            (
                snapshot.engine_name,
                snapshot.module_spec,
                snapshot.package_hash,
                snapshot.layout_key,
                snapshot.loader_schema_version,
            ): snapshot
            for snapshot in snapshot.registry_snapshots
        }

    def apply(self, event: DomainEvent) -> None:
        self.apply_event(event)

    def live_snapshot(self) -> LiveExecutionSnapshot:
        session_id = (
            None if self._session is None else self._session.session_id
        )
        trace_id = None if self._session is None else self._session.trace_id
        plan_id = (
            None if self._latest_plan is None else self._latest_plan.plan_id
        )
        running_tests = tuple(
            LiveTestKnowledge(
                node_id=test.node_id,
                node_stable_id=test.node_stable_id,
                engine_name=test.engine_name,
                test_name=test.test_name,
                status=test.status,
                started_at=test.started_at,
                worker_slot=test.worker_slot,
                current_step=self._live_current_steps.get(
                    test.node_stable_id,
                ),
            )
            for test in sorted(
                self._active_tests.values(),
                key=lambda current_test: (
                    current_test.started_at or 0.0,
                    current_test.node_stable_id,
                ),
                reverse=True,
            )
        )
        workers = tuple(
            sorted(
                self._workers.values(),
                key=lambda worker: worker.worker_id,
            ),
        )
        resources = tuple(
            sorted(
                (
                    resource
                    for resource in self._resources.values()
                    if resource.owner_node_id is not None
                    or resource.owner_worker_id is not None
                ),
                key=lambda resource: (resource.name, resource.scope),
            ),
        )
        limited_running_tests = running_tests[
            :LIVE_EXECUTION_RUNNING_TEST_LIMIT
        ]
        limited_workers = workers[:LIVE_EXECUTION_WORKER_LIMIT]
        limited_resources = resources[:LIVE_EXECUTION_RESOURCE_LIMIT]
        limited_log_chunks = tuple(self._live_log_chunk_tail)
        engine_snapshots = tuple(
            sorted(
                self._live_engine_snapshots.values(),
                key=lambda snapshot: (
                    snapshot.engine_name,
                    snapshot.node_stable_id,
                    snapshot.snapshot_kind,
                ),
            ),
        )
        return LiveExecutionSnapshot(
            session_id=session_id,
            trace_id=trace_id,
            plan_id=plan_id,
            last_sequence_number=self._last_sequence_number,
            running_tests=limited_running_tests,
            truncated_running_test_count=(
                len(running_tests) - len(limited_running_tests)
            ),
            workers=limited_workers,
            truncated_worker_count=len(workers) - len(limited_workers),
            resources=limited_resources,
            truncated_resource_count=(len(resources) - len(limited_resources)),
            recent_log_chunks=limited_log_chunks,
            truncated_log_chunk_count=(self._live_log_chunk_truncated_count),
            engine_snapshots=engine_snapshots,
            recent_events=tuple(self._live_event_tail),
            truncated_event_count=self._live_event_truncated_count,
        )

    def query_domain_events(
        self,
        query: DomainEventQuery,
    ) -> tuple[DomainEvent, ...]:
        events = tuple(
            event for _, event in sorted(self._domain_events.items())
        )
        return _filter_domain_events(events, query)

    def store_session_artifact(self, artifact: SessionArtifact) -> None:
        persisted_artifact = artifact.apply_persistence_policy()
        self._session_artifacts[persisted_artifact.session_id] = (
            persisted_artifact
        )
        self._prune_session_artifacts(
            _workspace_scope_key(
                root_path=persisted_artifact.root_path,
                workspace_fingerprint=persisted_artifact.workspace_fingerprint,
            ),
            persisted_artifact.persistence_policy.max_artifacts_per_scope,
            persisted_artifact.persistence_policy.max_artifact_age_seconds,
            persisted_artifact.recorded_at,
        )

    def apply_event(self, event: DomainEvent) -> bool:
        if self._should_skip_duplicate_event(event):
            return False

        handler = self._event_handlers.get(type(event))
        if handler is None:
            return False

        handler(event)
        self._record_domain_event(event)
        return True

    def _prune_session_artifacts(
        self,
        retention_scope_value: str,
        max_artifacts: int,
        max_age_seconds: float | None,
        recorded_at: float,
    ) -> None:
        reference_recorded_at = max(
            (
                artifact.recorded_at
                for artifact in self._session_artifacts.values()
                if _workspace_scope_key(
                    root_path=artifact.root_path,
                    workspace_fingerprint=artifact.workspace_fingerprint,
                )
                == retention_scope_value
            ),
            default=recorded_at,
        )
        retained = sorted(
            (
                artifact
                for artifact in self._session_artifacts.values()
                if _workspace_scope_key(
                    root_path=artifact.root_path,
                    workspace_fingerprint=artifact.workspace_fingerprint,
                )
                == retention_scope_value
                and (
                    max_age_seconds is None
                    or artifact.recorded_at
                    >= (reference_recorded_at - max_age_seconds)
                )
            ),
            key=lambda artifact: (
                artifact.recorded_at,
                artifact.session_id,
            ),
            reverse=True,
        )
        expired = tuple(
            artifact.session_id
            for artifact in self._session_artifacts.values()
            if _workspace_scope_key(
                root_path=artifact.root_path,
                workspace_fingerprint=artifact.workspace_fingerprint,
            )
            == retention_scope_value
            and max_age_seconds is not None
            and artifact.recorded_at
            < (reference_recorded_at - max_age_seconds)
        )
        for session_id in expired:
            self._session_artifacts.pop(session_id, None)
        for artifact in retained[max_artifacts:]:
            self._session_artifacts.pop(artifact.session_id, None)

    def _record_domain_event(
        self,
        event: DomainEvent,
    ) -> None:
        sequence_number = event.metadata.sequence_number
        if sequence_number is None:
            return

        if len(self._live_event_tail) == self._live_event_tail.maxlen:
            self._live_event_truncated_count += 1
        self._domain_events[sequence_number] = event
        self._live_event_tail.append(event)
        if isinstance(event, LogChunkEvent):
            if (
                len(self._live_log_chunk_tail)
                == self._live_log_chunk_tail.maxlen
            ):
                self._live_log_chunk_truncated_count += 1
            self._live_log_chunk_tail.append(
                LiveLogChunk(
                    message=event.message,
                    level=event.level,
                    logger_name=event.logger_name,
                    emitted_at=event.timestamp,
                    node_stable_id=event.metadata.node_stable_id,
                    worker_id=event.metadata.worker_id,
                    sequence_number=sequence_number,
                ),
            )

    def _apply_session_event(
        self,
        event: SessionStartedEvent | SessionFinishedEvent,
    ) -> None:
        if isinstance(event, SessionStartedEvent):
            self._session = SessionKnowledge(
                root_path=event.root_path,
                workspace_fingerprint=event.workspace_fingerprint,
                concurrency=event.concurrency,
                session_id=event.metadata.session_id,
                trace_id=event.metadata.trace_id,
                started_at=event.timestamp,
            )
            return

        if self._session is None:
            return

        self._reconcile_finished_session(
            session_id=event.metadata.session_id or self._session.session_id,
            finished_at=event.timestamp,
        )
        self._session = SessionKnowledge(
            root_path=self._session.root_path,
            workspace_fingerprint=self._session.workspace_fingerprint,
            concurrency=self._session.concurrency,
            session_id=event.metadata.session_id or self._session.session_id,
            trace_id=event.metadata.trace_id or self._session.trace_id,
            started_at=self._session.started_at,
            finished_at=event.timestamp,
            has_failures=event.has_failures,
        )

    def _reconcile_finished_session(
        self,
        *,
        session_id: str | None,
        finished_at: float,
    ) -> None:
        for node_stable_id, active_test in tuple(self._active_tests.items()):
            previous = self._tests.get(node_stable_id)
            if (
                previous is not None
                and session_id is not None
                and previous.session_id not in {None, session_id}
            ):
                continue
            self._active_tests.pop(node_stable_id, None)
            self._live_current_steps.pop(node_stable_id, None)
            self._live_engine_snapshots.pop(node_stable_id, None)
            if previous is None:
                continue
            self._tests[node_stable_id] = TestKnowledge(
                node_id=previous.node_id or active_test.node_id,
                node_stable_id=node_stable_id,
                engine_name=previous.engine_name or active_test.engine_name,
                test_name=previous.test_name or active_test.test_name,
                test_path=previous.test_path,
                correlation_id=previous.correlation_id,
                session_id=previous.session_id or session_id,
                plan_id=previous.plan_id,
                trace_id=previous.trace_id,
                started_at=(
                    active_test.started_at
                    if active_test.started_at is not None
                    else previous.started_at
                ),
                finished_at=finished_at,
                status='error',
                duration=previous.duration,
                selection_labels=previous.selection_labels,
                source_line=previous.source_line,
                discovery_mode=previous.discovery_mode,
                knowledge_version=previous.knowledge_version,
                content_hash=previous.content_hash,
                indexed_at=previous.indexed_at,
                invalidated_at=previous.invalidated_at,
                invalidation_reason=previous.invalidation_reason,
                worker_slot=(
                    active_test.worker_slot
                    if active_test.worker_slot is not None
                    else previous.worker_slot
                ),
                scheduled_at=previous.scheduled_at,
                max_attempts=previous.max_attempts,
                timeout_seconds=previous.timeout_seconds,
                retry_count=previous.retry_count,
                failure_kind=previous.failure_kind or 'runtime',
                last_error_code=previous.last_error_code or 'session_aborted',
            )

        for node_stable_id, previous in tuple(self._tests.items()):
            if previous.finished_at is not None:
                continue
            if session_id is not None and previous.session_id not in {
                None,
                session_id,
            }:
                continue
            if previous.scheduled_at is None and previous.started_at is None:
                continue
            self._live_current_steps.pop(node_stable_id, None)
            self._live_engine_snapshots.pop(node_stable_id, None)
            self._tests[node_stable_id] = TestKnowledge(
                node_id=previous.node_id,
                node_stable_id=node_stable_id,
                engine_name=previous.engine_name,
                test_name=previous.test_name,
                test_path=previous.test_path,
                correlation_id=previous.correlation_id,
                session_id=previous.session_id or session_id,
                plan_id=previous.plan_id,
                trace_id=previous.trace_id,
                started_at=previous.started_at,
                finished_at=finished_at,
                status='error',
                duration=previous.duration,
                selection_labels=previous.selection_labels,
                source_line=previous.source_line,
                discovery_mode=previous.discovery_mode,
                knowledge_version=previous.knowledge_version,
                content_hash=previous.content_hash,
                indexed_at=previous.indexed_at,
                invalidated_at=previous.invalidated_at,
                invalidation_reason=previous.invalidation_reason,
                worker_slot=previous.worker_slot,
                scheduled_at=previous.scheduled_at,
                max_attempts=previous.max_attempts,
                timeout_seconds=previous.timeout_seconds,
                retry_count=previous.retry_count,
                failure_kind=previous.failure_kind or 'runtime',
                last_error_code=previous.last_error_code or 'session_aborted',
            )

    def _apply_plan_event(
        self,
        event: PlanAnalyzedEvent,
    ) -> None:
        self._latest_plan = PlanKnowledge(
            mode=event.mode,
            executable=event.executable,
            node_count=event.node_count,
            issue_count=event.issue_count,
            plan_id=event.metadata.plan_id,
            correlation_id=event.metadata.correlation_id,
            session_id=event.metadata.session_id,
            trace_id=event.metadata.trace_id,
            analyzed_at=event.timestamp,
        )

    def _apply_test_event(
        self,
        event: (
            NodeScheduledEvent
            | NodeRetryingEvent
            | TestStartedEvent
            | TestFinishedEvent
        ),
    ) -> None:
        previous = self._tests.get(event.node_stable_id)
        active_previous = self._active_tests.get(event.node_stable_id)
        if isinstance(event, NodeScheduledEvent):
            self._tests[event.node_stable_id] = TestKnowledge(
                node_id=event.node_id,
                node_stable_id=event.node_stable_id,
                engine_name=(
                    previous.engine_name if previous is not None else ''
                ),
                test_name=previous.test_name if previous is not None else '',
                test_path=previous.test_path if previous is not None else '',
                correlation_id=event.metadata.correlation_id,
                session_id=event.metadata.session_id,
                plan_id=event.metadata.plan_id,
                trace_id=event.metadata.trace_id,
                started_at=(
                    previous.started_at if previous is not None else None
                ),
                finished_at=(
                    previous.finished_at if previous is not None else None
                ),
                status=previous.status if previous is not None else None,
                duration=previous.duration if previous is not None else None,
                selection_labels=(
                    previous.selection_labels if previous is not None else ()
                ),
                source_line=(
                    previous.source_line if previous is not None else None
                ),
                discovery_mode=(
                    previous.discovery_mode if previous is not None else None
                ),
                knowledge_version=(
                    previous.knowledge_version
                    if previous is not None
                    else None
                ),
                indexed_at=(
                    previous.indexed_at if previous is not None else None
                ),
                invalidated_at=(
                    previous.invalidated_at if previous is not None else None
                ),
                invalidation_reason=(
                    previous.invalidation_reason
                    if previous is not None
                    else None
                ),
                worker_slot=event.worker_slot,
                scheduled_at=event.timestamp,
                max_attempts=event.max_attempts,
                timeout_seconds=event.timeout_seconds,
                retry_count=(
                    previous.retry_count if previous is not None else 0
                ),
                failure_kind=(
                    previous.failure_kind if previous is not None else None
                ),
                last_error_code=(
                    previous.last_error_code if previous is not None else None
                ),
            )
            return

        if isinstance(event, NodeRetryingEvent):
            self._tests[event.node_stable_id] = TestKnowledge(
                node_id=event.node_id,
                node_stable_id=event.node_stable_id,
                engine_name=(
                    previous.engine_name if previous is not None else ''
                ),
                test_name=previous.test_name if previous is not None else '',
                test_path=previous.test_path if previous is not None else '',
                correlation_id=event.metadata.correlation_id,
                session_id=event.metadata.session_id,
                plan_id=event.metadata.plan_id,
                trace_id=event.metadata.trace_id,
                started_at=(
                    previous.started_at if previous is not None else None
                ),
                finished_at=(
                    previous.finished_at if previous is not None else None
                ),
                status=previous.status if previous is not None else None,
                duration=previous.duration if previous is not None else None,
                selection_labels=(
                    previous.selection_labels if previous is not None else ()
                ),
                source_line=(
                    previous.source_line if previous is not None else None
                ),
                discovery_mode=(
                    previous.discovery_mode if previous is not None else None
                ),
                knowledge_version=(
                    previous.knowledge_version
                    if previous is not None
                    else None
                ),
                indexed_at=(
                    previous.indexed_at if previous is not None else None
                ),
                invalidated_at=(
                    previous.invalidated_at if previous is not None else None
                ),
                invalidation_reason=(
                    previous.invalidation_reason
                    if previous is not None
                    else None
                ),
                worker_slot=(
                    previous.worker_slot if previous is not None else None
                ),
                scheduled_at=(
                    previous.scheduled_at if previous is not None else None
                ),
                max_attempts=(
                    previous.max_attempts if previous is not None else None
                ),
                timeout_seconds=(
                    previous.timeout_seconds if previous is not None else None
                ),
                retry_count=(
                    (previous.retry_count if previous is not None else 0) + 1
                ),
                failure_kind=event.failure_kind,
                last_error_code=event.error_code,
            )
            return

        if isinstance(event, TestStartedEvent):
            self._active_tests[event.node_stable_id] = LiveTestKnowledge(
                node_id=event.node_id,
                node_stable_id=event.node_stable_id,
                engine_name=event.engine_name,
                test_name=event.test_name,
                status='running',
                started_at=event.timestamp,
                worker_slot=(
                    previous.worker_slot if previous is not None else None
                ),
            )
            return

        self._active_tests.pop(event.node_stable_id, None)
        self._tests[event.node_stable_id] = TestKnowledge(
            node_id=event.node_id,
            node_stable_id=event.node_stable_id,
            engine_name=event.engine_name,
            test_name=event.test_name,
            test_path=event.test_path,
            correlation_id=event.metadata.correlation_id,
            session_id=event.metadata.session_id,
            plan_id=event.metadata.plan_id,
            trace_id=event.metadata.trace_id,
            started_at=(
                active_previous.started_at
                if active_previous is not None
                else (previous.started_at if previous is not None else None)
            ),
            finished_at=event.timestamp,
            status=event.status,
            duration=event.duration,
            selection_labels=(
                previous.selection_labels if previous is not None else ()
            ),
            source_line=(
                previous.source_line if previous is not None else None
            ),
            discovery_mode=(
                previous.discovery_mode if previous is not None else None
            ),
            knowledge_version=(
                previous.knowledge_version if previous is not None else None
            ),
            indexed_at=(previous.indexed_at if previous is not None else None),
            invalidated_at=(
                previous.invalidated_at if previous is not None else None
            ),
            invalidation_reason=(
                previous.invalidation_reason if previous is not None else None
            ),
            worker_slot=(
                active_previous.worker_slot
                if active_previous is not None
                else (previous.worker_slot if previous is not None else None)
            ),
            scheduled_at=(
                previous.scheduled_at if previous is not None else None
            ),
            max_attempts=(
                previous.max_attempts if previous is not None else None
            ),
            timeout_seconds=(
                previous.timeout_seconds if previous is not None else None
            ),
            retry_count=(previous.retry_count if previous is not None else 0),
            failure_kind=event.failure_kind,
            last_error_code=(
                event.error_code
                if event.error_code is not None
                else (
                    previous.last_error_code if previous is not None else None
                )
            ),
        )
        self._live_current_steps.pop(event.node_stable_id, None)
        self._live_engine_snapshots.pop(event.node_stable_id, None)

    def _apply_test_knowledge_event(
        self,
        event: TestKnowledgeIndexedEvent | TestKnowledgeInvalidatedEvent,
    ) -> None:
        if isinstance(event, TestKnowledgeInvalidatedEvent):
            stale_tests = tuple(
                test.node_stable_id
                for test in self._tests.values()
                if test.engine_name == event.engine_name
                and test.test_path == event.file_path
            )
            for node_stable_id in stale_tests:
                self._tests.pop(node_stable_id, None)
                self._active_tests.pop(node_stable_id, None)
                self._live_engine_snapshots.pop(node_stable_id, None)
            return

        indexed_stable_ids: set[str] = set()
        for descriptor in event.tests:
            indexed_stable_ids.add(descriptor.stable_id)
            previous = self._tests.get(descriptor.stable_id)
            self._tests[descriptor.stable_id] = TestKnowledge(
                node_id=descriptor.stable_id,
                node_stable_id=descriptor.stable_id,
                engine_name=event.engine_name,
                test_name=descriptor.test_name,
                test_path=descriptor.file_path,
                correlation_id=event.metadata.correlation_id,
                session_id=event.metadata.session_id,
                plan_id=event.metadata.plan_id,
                trace_id=event.metadata.trace_id,
                started_at=(
                    previous.started_at if previous is not None else None
                ),
                finished_at=(
                    previous.finished_at if previous is not None else None
                ),
                status=previous.status if previous is not None else None,
                duration=previous.duration if previous is not None else None,
                selection_labels=descriptor.selection_labels,
                source_line=descriptor.source_line,
                discovery_mode=event.discovery_mode,
                content_hash=event.content_hash,
                knowledge_version=event.knowledge_version,
                indexed_at=event.timestamp,
                invalidated_at=None,
                invalidation_reason=None,
                worker_slot=(
                    previous.worker_slot if previous is not None else None
                ),
                scheduled_at=(
                    previous.scheduled_at if previous is not None else None
                ),
                max_attempts=(
                    previous.max_attempts if previous is not None else None
                ),
                timeout_seconds=(
                    previous.timeout_seconds if previous is not None else None
                ),
                retry_count=(
                    previous.retry_count if previous is not None else 0
                ),
                failure_kind=(
                    previous.failure_kind if previous is not None else None
                ),
                last_error_code=(
                    previous.last_error_code if previous is not None else None
                ),
            )

        stale_tests = tuple(
            test.node_stable_id
            for test in self._tests.values()
            if test.engine_name == event.engine_name
            and test.test_path == event.file_path
            and test.node_stable_id not in indexed_stable_ids
        )
        for node_stable_id in stale_tests:
            self._tests.pop(node_stable_id, None)
            self._live_engine_snapshots.pop(node_stable_id, None)

    def _apply_definition_event(
        self,
        event: (
            KnowledgeIndexedEvent
            | KnowledgeInvalidatedEvent
            | DefinitionMaterializedEvent
        ),
    ) -> None:
        key = (event.engine_name, event.file_path)
        if isinstance(event, KnowledgeIndexedEvent):
            previous = self._definitions.get(key)
            self._definitions[key] = DefinitionKnowledge(
                engine_name=event.engine_name,
                file_path=event.file_path,
                definition_count=event.definition_count,
                discovery_mode=event.discovery_mode,
                descriptors=event.descriptors,
                content_hash=event.content_hash,
                knowledge_version=event.knowledge_version,
                correlation_id=event.metadata.correlation_id,
                session_id=event.metadata.session_id,
                trace_id=event.metadata.trace_id,
                indexed_at=event.timestamp,
                invalidated_at=(
                    previous.invalidated_at if previous is not None else None
                ),
                invalidation_reason=(
                    previous.invalidation_reason
                    if previous is not None
                    else None
                ),
                materialized_count=(
                    previous.materialized_count if previous is not None else 0
                ),
                last_materialized_at=(
                    previous.last_materialized_at
                    if previous is not None
                    else None
                ),
            )
            return

        if isinstance(event, KnowledgeInvalidatedEvent):
            previous = self._definitions.get(key) or DefinitionKnowledge(
                engine_name=event.engine_name,
                file_path=event.file_path,
                definition_count=0,
                discovery_mode='unknown',
            )
            self._definitions[key] = DefinitionKnowledge(
                engine_name=previous.engine_name,
                file_path=previous.file_path,
                definition_count=previous.definition_count,
                discovery_mode=previous.discovery_mode,
                descriptors=previous.descriptors,
                content_hash=previous.content_hash,
                knowledge_version=event.knowledge_version,
                correlation_id=event.metadata.correlation_id,
                session_id=event.metadata.session_id,
                trace_id=event.metadata.trace_id,
                indexed_at=previous.indexed_at,
                invalidated_at=event.timestamp,
                invalidation_reason=event.reason,
                materialized_count=previous.materialized_count,
                last_materialized_at=previous.last_materialized_at,
            )
            return

        previous = self._definitions.get(key) or DefinitionKnowledge(
            engine_name=event.engine_name,
            file_path=event.file_path,
            definition_count=event.definition_count,
            discovery_mode=event.discovery_mode,
        )
        self._definitions[key] = DefinitionKnowledge(
            engine_name=previous.engine_name,
            file_path=previous.file_path,
            definition_count=event.definition_count,
            discovery_mode=event.discovery_mode,
            descriptors=previous.descriptors,
            content_hash=previous.content_hash,
            knowledge_version=previous.knowledge_version,
            correlation_id=event.metadata.correlation_id,
            session_id=event.metadata.session_id,
            trace_id=event.metadata.trace_id,
            indexed_at=previous.indexed_at,
            invalidated_at=previous.invalidated_at,
            invalidation_reason=previous.invalidation_reason,
            materialized_count=previous.materialized_count + 1,
            last_materialized_at=event.timestamp,
        )

    def _apply_registry_knowledge_event(
        self,
        event: RegistryKnowledgeIndexedEvent,
    ) -> None:
        key = (
            event.engine_name,
            event.module_spec,
            event.package_hash,
            event.layout_key,
            event.loader_schema_version,
        )
        self._registry_snapshots[key] = RegistryKnowledgeSnapshot(
            engine_name=event.engine_name,
            module_spec=event.module_spec,
            package_hash=event.package_hash,
            layout_key=event.layout_key,
            loader_schema_version=event.loader_schema_version,
            entries=event.entries,
            source_count=event.source_count,
            created_at=event.timestamp,
        )

    def _apply_resource_event(
        self,
        event: ResourceLifecycleEvent,
    ) -> None:
        key = (event.name, event.scope)
        previous = self._resources.get(key) or ResourceKnowledge(
            name=event.name,
            scope=event.scope,
        )
        self._resources[key] = ResourceKnowledge(
            name=previous.name,
            scope=previous.scope,
            readiness_status=previous.readiness_status,
            readiness_reason=previous.readiness_reason,
            correlation_id=event.metadata.correlation_id,
            external_handle=event.external_handle,
            session_id=event.metadata.session_id,
            plan_id=event.metadata.plan_id,
            trace_id=event.metadata.trace_id,
            last_heartbeat_at=(
                event.timestamp
                if event.metadata.worker_id is not None
                else previous.last_heartbeat_at
            ),
            last_worker_id=event.metadata.worker_id,
            owner_worker_id=(
                event.metadata.worker_id
                if event.action == 'acquired'
                else None
            ),
            last_node_id=event.metadata.node_id,
            last_node_stable_id=event.metadata.node_stable_id,
            owner_node_id=(
                event.metadata.node_id if event.action == 'acquired' else None
            ),
            owner_node_stable_id=(
                event.metadata.node_stable_id
                if event.action == 'acquired'
                else None
            ),
            acquire_count=(
                previous.acquire_count
                + (1 if event.action == 'acquired' else 0)
            ),
            release_count=(
                previous.release_count
                + (1 if event.action == 'released' else 0)
            ),
            last_test_id=event.test_id,
            owner_test_id=(
                event.test_id if event.action == 'acquired' else None
            ),
        )

    def _apply_resource_readiness_transition_event(
        self,
        event: ResourceReadinessTransitionEvent,
    ) -> None:
        key = (event.name, event.scope)
        previous = self._resources.get(key) or ResourceKnowledge(
            name=event.name,
            scope=event.scope,
        )
        self._resources[key] = ResourceKnowledge(
            name=previous.name,
            scope=previous.scope,
            readiness_status=event.status,
            readiness_reason=event.reason,
            correlation_id=event.metadata.correlation_id,
            session_id=event.metadata.session_id,
            plan_id=event.metadata.plan_id,
            trace_id=event.metadata.trace_id,
            last_worker_id=event.metadata.worker_id,
            owner_worker_id=previous.owner_worker_id,
            last_node_id=event.metadata.node_id,
            last_node_stable_id=event.metadata.node_stable_id,
            owner_node_id=previous.owner_node_id,
            owner_node_stable_id=previous.owner_node_stable_id,
            acquire_count=previous.acquire_count,
            release_count=previous.release_count,
            last_test_id=previous.last_test_id,
            owner_test_id=previous.owner_test_id,
            external_handle=previous.external_handle,
            last_heartbeat_at=(
                event.timestamp
                if event.metadata.worker_id is not None
                else previous.last_heartbeat_at
            ),
        )

    def _apply_live_step_event(
        self,
        event: StepStartedEvent | StepFinishedEvent,
    ) -> None:
        if isinstance(event, StepStartedEvent):
            self._live_current_steps[event.node_stable_id] = LiveStepKnowledge(
                step_type=event.step_type,
                step_keyword=event.step_keyword,
                step_text=event.step_text,
                status='running',
                source_line=event.source_line,
                updated_at=event.timestamp,
            )
            return

        self._live_current_steps[event.node_stable_id] = LiveStepKnowledge(
            step_type=event.step_type,
            step_keyword=event.step_keyword,
            step_text=event.step_text,
            status=event.status,
            source_line=event.source_line,
            message=event.message,
            updated_at=event.timestamp,
        )

    def _apply_live_engine_snapshot_event(
        self,
        event: EngineSnapshotUpdatedEvent,
    ) -> None:
        node_stable_id = event.metadata.node_stable_id
        if node_stable_id is None:
            return

        self._live_engine_snapshots[node_stable_id] = LiveEngineSnapshot(
            engine_name=event.engine_name,
            snapshot_kind=event.snapshot_kind,
            node_stable_id=node_stable_id,
            payload=event.payload,
            worker_id=event.metadata.worker_id,
        )

    def _apply_live_log_chunk_event(
        self,
        event: LogChunkEvent,
    ) -> None:
        del event

    def _apply_worker_heartbeat_event(
        self,
        event: WorkerHeartbeatEvent,
    ) -> None:
        self._workers[event.worker_id] = WorkerKnowledge(
            worker_id=event.worker_id,
            status=event.status,
            session_id=event.metadata.session_id,
            plan_id=event.metadata.plan_id,
            trace_id=event.metadata.trace_id,
            last_heartbeat_at=event.timestamp,
        )
        for key, resource in tuple(self._resources.items()):
            if resource.owner_worker_id != event.worker_id:
                continue

            self._resources[key] = ResourceKnowledge(
                name=resource.name,
                scope=resource.scope,
                readiness_status=resource.readiness_status,
                readiness_reason=resource.readiness_reason,
                correlation_id=resource.correlation_id,
                session_id=resource.session_id,
                plan_id=resource.plan_id,
                trace_id=resource.trace_id,
                last_worker_id=resource.last_worker_id,
                owner_worker_id=resource.owner_worker_id,
                last_node_id=resource.last_node_id,
                last_node_stable_id=resource.last_node_stable_id,
                owner_node_id=resource.owner_node_id,
                owner_node_stable_id=resource.owner_node_stable_id,
                acquire_count=resource.acquire_count,
                release_count=resource.release_count,
                last_test_id=resource.last_test_id,
                owner_test_id=resource.owner_test_id,
                external_handle=resource.external_handle,
                last_heartbeat_at=event.timestamp,
            )

    def _apply_worker_degraded_event(
        self,
        event: WorkerDegradedEvent,
    ) -> None:
        self._workers[event.worker_id] = WorkerKnowledge(
            worker_id=event.worker_id,
            status='degraded',
            session_id=event.metadata.session_id,
            plan_id=event.metadata.plan_id,
            trace_id=event.metadata.trace_id,
            last_heartbeat_at=event.timestamp,
        )

    def _apply_worker_recovered_event(
        self,
        event: WorkerRecoveredEvent,
    ) -> None:
        self._workers[event.worker_id] = WorkerKnowledge(
            worker_id=event.worker_id,
            status='recovered',
            session_id=event.metadata.session_id,
            plan_id=event.metadata.plan_id,
            trace_id=event.metadata.trace_id,
            last_heartbeat_at=event.timestamp,
        )

    def _should_skip_duplicate_event(
        self,
        event: DomainEvent,
    ) -> bool:
        self._prune_idempotency_keys(event.timestamp)
        if event.metadata.sequence_number is None:
            object.__setattr__(
                event.metadata,
                'sequence_number',
                self._last_sequence_number + 1,
            )

        idempotency_key = event.metadata.idempotency_key
        if idempotency_key is None:
            sequence_number = event.metadata.sequence_number
            if sequence_number is None:
                return False

            if sequence_number <= self._last_sequence_number:
                return True

            self._last_sequence_number = sequence_number
            return False

        if idempotency_key in self._processed_idempotency_keys:
            self._processed_idempotency_keys.move_to_end(idempotency_key)
            return True

        self._processed_idempotency_keys[idempotency_key] = event.timestamp
        self._processed_idempotency_keys.move_to_end(idempotency_key)
        self._prune_idempotency_keys(event.timestamp)
        sequence_number = event.metadata.sequence_number
        if sequence_number is not None:
            self._last_sequence_number = max(
                self._last_sequence_number,
                sequence_number,
            )
        return False

    def _prune_idempotency_keys(
        self,
        reference_timestamp: float,
    ) -> None:
        max_age_seconds = self._idempotency_policy.max_age_seconds
        if max_age_seconds is not None:
            expiration_threshold = reference_timestamp - max_age_seconds
            while self._processed_idempotency_keys:
                oldest_key = next(iter(self._processed_idempotency_keys))
                oldest_timestamp = self._processed_idempotency_keys[oldest_key]
                if oldest_timestamp >= expiration_threshold:
                    break
                self._processed_idempotency_keys.popitem(last=False)

        while (
            len(self._processed_idempotency_keys)
            > self._idempotency_policy.max_entries
        ):
            self._processed_idempotency_keys.popitem(last=False)

    def snapshot(self) -> KnowledgeSnapshot:
        return KnowledgeSnapshot(
            session=self._session,
            latest_plan=self._latest_plan,
            tests=tuple(
                sorted(
                    self._tests.values(),
                    key=lambda test: test.node_stable_id,
                ),
            ),
            resources=tuple(
                sorted(
                    self._resources.values(),
                    key=lambda resource: (resource.name, resource.scope),
                ),
            ),
            workers=tuple(
                sorted(
                    self._workers.values(),
                    key=lambda worker: worker.worker_id,
                ),
            ),
            definitions=tuple(
                sorted(
                    self._definitions.values(),
                    key=lambda definition: (
                        definition.engine_name,
                        definition.file_path,
                    ),
                ),
            ),
            registry_snapshots=tuple(
                sorted(
                    self._registry_snapshots.values(),
                    key=lambda snapshot: (
                        snapshot.engine_name,
                        snapshot.module_spec,
                        snapshot.layout_key,
                        snapshot.package_hash,
                        snapshot.loader_schema_version,
                    ),
                ),
            ),
        )

    def query_tests(
        self,
        query: TestKnowledgeQuery,
    ) -> tuple[TestKnowledge, ...]:
        tests = tuple(
            sorted(
                self._tests.values(),
                key=lambda test: (
                    test.test_path,
                    test.engine_name,
                    test.source_line or 0,
                    test.test_name,
                    test.node_stable_id,
                ),
            ),
        )
        return _filter_test_knowledge(tests, query)

    def query_definitions(
        self,
        query: DefinitionKnowledgeQuery,
    ) -> tuple[DefinitionKnowledge, ...]:
        definitions = tuple(
            sorted(
                self._definitions.values(),
                key=lambda definition: (
                    definition.engine_name,
                    definition.file_path,
                ),
            ),
        )
        return _filter_definition_knowledge(definitions, query)

    def query_registry_items(
        self,
        query: RegistryKnowledgeQuery,
    ) -> tuple[RegistryKnowledgeSnapshot, ...]:
        registry_snapshots = tuple(
            sorted(
                self._registry_snapshots.values(),
                key=lambda snapshot: (
                    snapshot.engine_name,
                    snapshot.module_spec,
                    snapshot.layout_key,
                    snapshot.package_hash,
                    snapshot.loader_schema_version,
                ),
            ),
        )
        return _filter_registry_knowledge(registry_snapshots, query)

    def query_resources(
        self,
        query: ResourceKnowledgeQuery,
    ) -> tuple[ResourceKnowledge, ...]:
        resources = tuple(
            sorted(
                self._resources.values(),
                key=lambda resource: (resource.name, resource.scope),
            ),
        )
        return _filter_resource_knowledge(resources, query)

    def query_session_artifacts(
        self,
        query: SessionArtifactQuery,
    ) -> tuple[SessionArtifact, ...]:
        artifacts = tuple(
            sorted(
                self._session_artifacts.values(),
                key=lambda artifact: artifact.recorded_at,
                reverse=True,
            ),
        )
        return _filter_session_artifacts(artifacts, query)

    def close(self) -> None:
        return None


class PersistentKnowledgeBase:
    __slots__ = (
        '_closed',
        '_connection',
        '_connection_lock',
        '_db_path',
        '_mirror',
    )

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
        )
        self._connection.execute('PRAGMA journal_mode=WAL;')
        self._connection.row_factory = sqlite3.Row
        self._connection_lock = threading.RLock()
        self._closed = False
        self._mirror = InMemoryKnowledgeBase()
        with self._connection_lock:
            self._initialize_schema()
            self._mirror.load_snapshot(self._load_snapshot_from_db())

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def schema_version(self) -> int:
        return KNOWLEDGE_BASE_SCHEMA_VERSION

    def apply(self, event: DomainEvent) -> None:
        applied = self.apply_in_memory(event)
        if not applied:
            return

        self.persist_event(event)

    def apply_in_memory(self, event: DomainEvent) -> bool:
        if isinstance(event, SessionStartedEvent):
            self._mirror.clear_runtime_projection()

        return self._mirror.apply_event(event)

    def persist_event(self, event: DomainEvent) -> None:
        self.persist_events((event,))

    def persist_events(
        self,
        events: tuple[DomainEvent, ...],
    ) -> None:
        if not events:
            return

        with self._connection_lock:
            for event in events:
                self._persist_domain_event_record(event)
                if isinstance(event, SessionStartedEvent):
                    self._clear_runtime_projection()

                self._persist_event(event)

            self._connection.commit()

    def query_tests(
        self,
        query: TestKnowledgeQuery,
    ) -> tuple[TestKnowledge, ...]:
        with self._connection_lock:
            return _load_test_knowledge(self._connection, query)

    def live_snapshot(self) -> LiveExecutionSnapshot:
        return self._mirror.live_snapshot()

    def query_domain_events(
        self,
        query: DomainEventQuery,
    ) -> tuple[DomainEvent, ...]:
        with self._connection_lock:
            return _load_domain_events(self._connection, query)

    def query_definitions(
        self,
        query: DefinitionKnowledgeQuery,
    ) -> tuple[DefinitionKnowledge, ...]:
        with self._connection_lock:
            return _load_definition_knowledge(self._connection, query)

    def query_registry_items(
        self,
        query: RegistryKnowledgeQuery,
    ) -> tuple[RegistryKnowledgeSnapshot, ...]:
        with self._connection_lock:
            return _load_registry_knowledge(self._connection, query)

    def query_resources(
        self,
        query: ResourceKnowledgeQuery,
    ) -> tuple[ResourceKnowledge, ...]:
        with self._connection_lock:
            resources = _load_resource_knowledge(self._connection, query)
            return _filter_resource_knowledge(resources, query)

    def query_session_artifacts(
        self,
        query: SessionArtifactQuery,
    ) -> tuple[SessionArtifact, ...]:
        with self._connection_lock:
            artifacts = _load_session_artifacts(self._connection, query)
            return _filter_session_artifacts(artifacts, query)

    def store_session_artifact(self, artifact: SessionArtifact) -> None:
        persisted_artifact = artifact.apply_persistence_policy()
        self._mirror.store_session_artifact(persisted_artifact)
        with self._connection_lock:
            self._persist_session_artifact(persisted_artifact.session_id)
            self._prune_persisted_session_artifacts(
                _workspace_scope_key(
                    root_path=persisted_artifact.root_path,
                    workspace_fingerprint=(
                        persisted_artifact.workspace_fingerprint
                    ),
                ),
                persisted_artifact.persistence_policy.max_artifacts_per_scope,
                persisted_artifact.persistence_policy.max_artifact_age_seconds,
                persisted_artifact.recorded_at,
            )
            self._connection.commit()

    def snapshot(self) -> KnowledgeSnapshot:
        return self._mirror.snapshot()

    def close(self) -> None:
        if self._closed:
            return

        with self._connection_lock:
            self._connection.commit()
            self._connection.close()
            self._closed = True

    def interrupt(self) -> None:
        if self._closed:
            return

        with self._connection_lock:
            self._connection.interrupt()

    def force_close(self) -> None:
        if self._closed:
            return

        with self._connection_lock:
            try:
                self._connection.close()
            finally:
                self._closed = True

    def _initialize_schema(self) -> None:
        self._connection.execute(
            'CREATE TABLE IF NOT EXISTS meta '
            '(key TEXT PRIMARY KEY, value TEXT NOT NULL)',
        )
        current_version = self._read_meta('schema_version')
        if current_version is None:
            self._create_tables()
            self._write_meta(
                'schema_version',
                str(KNOWLEDGE_BASE_SCHEMA_VERSION),
            )
            self._connection.commit()
            return

        if current_version != str(KNOWLEDGE_BASE_SCHEMA_VERSION):
            self._migrate_schema(current_version)

        self._create_tables()
        self._write_meta(
            'schema_version',
            str(KNOWLEDGE_BASE_SCHEMA_VERSION),
        )
        self._connection.commit()

    def _migrate_schema(self, current_version: str) -> None:
        try:
            version = int(current_version)
        except (TypeError, ValueError):
            self._reset_schema()
            return

        while version < KNOWLEDGE_BASE_SCHEMA_VERSION:
            migration = _SCHEMA_MIGRATIONS.get(version)
            if migration is None:
                self._reset_schema()
                return

            migration(self._connection)
            version += 1
            self._write_meta('schema_version', str(version))
            self._connection.commit()

    def _reset_schema(self) -> None:
        self._connection.executescript(
            """
            DROP TABLE IF EXISTS session_state;
            DROP TABLE IF EXISTS latest_plan;
            DROP TABLE IF EXISTS tests;
            DROP TABLE IF EXISTS resources;
            DROP TABLE IF EXISTS workers;
            DROP TABLE IF EXISTS definitions;
            DROP TABLE IF EXISTS registry_snapshots;
            DROP TABLE IF EXISTS session_artifacts;
            DROP TABLE IF EXISTS domain_event_log;
            """,
        )

    def _create_tables(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS session_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                root_path TEXT NOT NULL,
                workspace_fingerprint TEXT,
                concurrency INTEGER NOT NULL,
                session_id TEXT,
                trace_id TEXT,
                started_at REAL NOT NULL,
                finished_at REAL,
                has_failures INTEGER
            );
            CREATE TABLE IF NOT EXISTS latest_plan (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                mode TEXT NOT NULL,
                executable INTEGER NOT NULL,
                node_count INTEGER NOT NULL,
                issue_count INTEGER NOT NULL,
                plan_id TEXT,
                correlation_id TEXT,
                session_id TEXT,
                trace_id TEXT,
                analyzed_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tests (
                node_stable_id TEXT PRIMARY KEY,
                node_id TEXT NOT NULL,
                engine_name TEXT NOT NULL,
                test_name TEXT NOT NULL,
                test_path TEXT NOT NULL,
                correlation_id TEXT,
                session_id TEXT,
                plan_id TEXT,
                trace_id TEXT,
                started_at REAL,
                finished_at REAL,
                status TEXT,
                duration REAL,
                selection_labels_json TEXT NOT NULL,
                source_line INTEGER,
                discovery_mode TEXT,
                knowledge_version TEXT,
                content_hash TEXT,
                indexed_at REAL,
                invalidated_at REAL,
                invalidation_reason TEXT,
                worker_slot INTEGER,
                scheduled_at REAL,
                max_attempts INTEGER,
                timeout_seconds REAL,
                retry_count INTEGER NOT NULL,
                failure_kind TEXT,
                last_error_code TEXT
            );
            CREATE TABLE IF NOT EXISTS resources (
                name TEXT NOT NULL,
                scope TEXT NOT NULL,
                correlation_id TEXT,
                session_id TEXT,
                plan_id TEXT,
                trace_id TEXT,
                last_worker_id INTEGER,
                owner_worker_id INTEGER,
                readiness_status TEXT,
                readiness_reason TEXT,
                last_node_id TEXT,
                last_node_stable_id TEXT,
                owner_node_id TEXT,
                owner_node_stable_id TEXT,
                acquire_count INTEGER NOT NULL,
                release_count INTEGER NOT NULL,
                last_test_id TEXT,
                owner_test_id TEXT,
                external_handle TEXT,
                last_heartbeat_at REAL,
                PRIMARY KEY (name, scope)
            );
            CREATE TABLE IF NOT EXISTS workers (
                worker_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                session_id TEXT,
                plan_id TEXT,
                trace_id TEXT,
                last_heartbeat_at REAL
            );
            CREATE TABLE IF NOT EXISTS definitions (
                engine_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                definition_count INTEGER NOT NULL,
                discovery_mode TEXT NOT NULL,
                descriptors_json TEXT NOT NULL,
                knowledge_version TEXT,
                content_hash TEXT,
                correlation_id TEXT,
                session_id TEXT,
                trace_id TEXT,
                indexed_at REAL,
                invalidated_at REAL,
                invalidation_reason TEXT,
                materialized_count INTEGER NOT NULL,
                last_materialized_at REAL,
                PRIMARY KEY (engine_name, file_path)
            );
            CREATE TABLE IF NOT EXISTS registry_snapshots (
                engine_name TEXT NOT NULL,
                module_spec TEXT NOT NULL,
                package_hash TEXT NOT NULL,
                layout_key TEXT NOT NULL,
                loader_schema_version TEXT NOT NULL,
                entries_json TEXT NOT NULL,
                source_count INTEGER NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (
                    engine_name,
                    module_spec,
                    package_hash,
                    layout_key,
                    loader_schema_version
                )
            );
            CREATE TABLE IF NOT EXISTS session_artifacts (
                session_id TEXT PRIMARY KEY,
                trace_id TEXT,
                root_path TEXT NOT NULL,
                workspace_fingerprint TEXT,
                plan_id TEXT,
                config_snapshot_json TEXT NOT NULL,
                capability_snapshots_json TEXT NOT NULL,
                plan_explanation_json TEXT,
                timing_json TEXT,
                has_failures INTEGER,
                report_summary_json TEXT,
                telemetry_summary_json TEXT,
                persistence_policy_json TEXT NOT NULL,
                recorded_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS domain_event_log (
                sequence_number INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                session_id TEXT,
                plan_id TEXT,
                node_stable_id TEXT,
                payload_json TEXT NOT NULL,
                timestamp REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tests_engine_path
                ON tests (engine_name, test_path);
            CREATE INDEX IF NOT EXISTS idx_tests_plan_status
                ON tests (plan_id, status);
            CREATE INDEX IF NOT EXISTS idx_tests_failure_kind
                ON tests (failure_kind);
            CREATE INDEX IF NOT EXISTS idx_tests_path
                ON tests (test_path);
            CREATE INDEX IF NOT EXISTS idx_definitions_engine_path
                ON definitions (engine_name, file_path);
            CREATE INDEX IF NOT EXISTS idx_definitions_discovery
                ON definitions (discovery_mode, invalidated_at);
            CREATE INDEX IF NOT EXISTS idx_registry_snapshots_lookup
                ON registry_snapshots (
                    engine_name,
                    module_spec,
                    layout_key,
                    package_hash,
                    loader_schema_version
                );
            CREATE INDEX IF NOT EXISTS idx_domain_event_log_session_sequence
                ON domain_event_log (session_id, sequence_number);
            CREATE INDEX IF NOT EXISTS idx_domain_event_log_plan_sequence
                ON domain_event_log (plan_id, sequence_number);
            """,
        )

    def _persist_event(self, event: DomainEvent) -> None:
        if isinstance(event, SessionStartedEvent | SessionFinishedEvent):
            self._persist_session()
        elif isinstance(event, PlanAnalyzedEvent):
            self._persist_latest_plan()
        elif isinstance(
            event,
            (
                NodeScheduledEvent,
                NodeRetryingEvent,
                TestStartedEvent,
                TestFinishedEvent,
            ),
        ):
            self._persist_test(event.node_stable_id)
        elif isinstance(
            event,
            (
                TestKnowledgeIndexedEvent,
                TestKnowledgeInvalidatedEvent,
            ),
        ):
            self._persist_tests_for_file(
                event.engine_name,
                event.file_path,
            )
        elif isinstance(
            event,
            (
                ResourceLifecycleEvent,
                ResourceReadinessTransitionEvent,
            ),
        ):
            self._persist_resource(event.name, event.scope)
        elif isinstance(event, WorkerHeartbeatEvent):
            self._persist_worker(event.worker_id)
        elif isinstance(
            event,
            (
                KnowledgeIndexedEvent,
                KnowledgeInvalidatedEvent,
                DefinitionMaterializedEvent,
            ),
        ):
            self._persist_definition(
                event.engine_name,
                event.file_path,
            )
        elif isinstance(event, RegistryKnowledgeIndexedEvent):
            self._persist_registry_snapshot(
                event.engine_name,
                event.module_spec,
                event.package_hash,
                event.layout_key,
                event.loader_schema_version,
            )

    def _persist_session(self) -> None:
        session = self._mirror.session
        if session is None:
            self._connection.execute('DELETE FROM session_state')
            return

        self._connection.execute(
            """
            INSERT INTO session_state (
                id,
                root_path,
                workspace_fingerprint,
                concurrency,
                session_id,
                trace_id,
                started_at,
                finished_at,
                has_failures
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                root_path = excluded.root_path,
                workspace_fingerprint = excluded.workspace_fingerprint,
                concurrency = excluded.concurrency,
                session_id = excluded.session_id,
                trace_id = excluded.trace_id,
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                has_failures = excluded.has_failures
            """,
            (
                session.root_path,
                session.workspace_fingerprint,
                session.concurrency,
                session.session_id,
                session.trace_id,
                session.started_at,
                session.finished_at,
                _bool_to_int(session.has_failures),
            ),
        )

    def _persist_latest_plan(self) -> None:
        plan = self._mirror.latest_plan
        if plan is None:
            self._connection.execute('DELETE FROM latest_plan')
            return

        self._connection.execute(
            """
            INSERT INTO latest_plan (
                id,
                mode,
                executable,
                node_count,
                issue_count,
                plan_id,
                correlation_id,
                session_id,
                trace_id,
                analyzed_at
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                mode = excluded.mode,
                executable = excluded.executable,
                node_count = excluded.node_count,
                issue_count = excluded.issue_count,
                plan_id = excluded.plan_id,
                correlation_id = excluded.correlation_id,
                session_id = excluded.session_id,
                trace_id = excluded.trace_id,
                analyzed_at = excluded.analyzed_at
            """,
            (
                plan.mode,
                _bool_to_int(plan.executable),
                plan.node_count,
                plan.issue_count,
                plan.plan_id,
                plan.correlation_id,
                plan.session_id,
                plan.trace_id,
                plan.analyzed_at,
            ),
        )

    def _persist_domain_event_record(
        self,
        event: DomainEvent,
    ) -> None:
        sequence_number = event.metadata.sequence_number
        if sequence_number is None:
            msg = 'Cannot persist domain event without sequence_number'
            raise ValueError(msg)

        self._connection.execute(
            """
            INSERT OR REPLACE INTO domain_event_log (
                sequence_number,
                event_id,
                event_type,
                session_id,
                plan_id,
                node_stable_id,
                payload_json,
                timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sequence_number,
                event.metadata.event_id,
                event.event_type,
                event.metadata.session_id,
                event.metadata.plan_id,
                event.metadata.node_stable_id,
                encode_json_text(serialize_domain_event(event)),
                event.timestamp,
            ),
        )

    def _persist_test(self, node_stable_id: str) -> None:
        test = self._mirror.get_test(node_stable_id)
        if test is None:
            self._connection.execute(
                'DELETE FROM tests WHERE node_stable_id = ?',
                (node_stable_id,),
            )
            return

        self._connection.execute(
            """
            INSERT INTO tests (
                node_stable_id,
                node_id,
                engine_name,
                test_name,
                test_path,
                correlation_id,
                session_id,
                plan_id,
                trace_id,
                started_at,
                finished_at,
                status,
                duration,
                selection_labels_json,
                source_line,
                discovery_mode,
                knowledge_version,
                content_hash,
                indexed_at,
                invalidated_at,
                invalidation_reason,
                worker_slot,
                scheduled_at,
                max_attempts,
                timeout_seconds,
                retry_count,
                failure_kind,
                last_error_code
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(node_stable_id) DO UPDATE SET
                node_id = excluded.node_id,
                engine_name = excluded.engine_name,
                test_name = excluded.test_name,
                test_path = excluded.test_path,
                correlation_id = excluded.correlation_id,
                session_id = excluded.session_id,
                plan_id = excluded.plan_id,
                trace_id = excluded.trace_id,
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                status = excluded.status,
                duration = excluded.duration,
                selection_labels_json = excluded.selection_labels_json,
                source_line = excluded.source_line,
                discovery_mode = excluded.discovery_mode,
                knowledge_version = excluded.knowledge_version,
                content_hash = excluded.content_hash,
                indexed_at = excluded.indexed_at,
                invalidated_at = excluded.invalidated_at,
                invalidation_reason = excluded.invalidation_reason,
                worker_slot = excluded.worker_slot,
                scheduled_at = excluded.scheduled_at,
                max_attempts = excluded.max_attempts,
                timeout_seconds = excluded.timeout_seconds,
                retry_count = excluded.retry_count,
                failure_kind = excluded.failure_kind,
                last_error_code = excluded.last_error_code
            """,
            (
                test.node_stable_id,
                test.node_id,
                test.engine_name,
                test.test_name,
                test.test_path,
                test.correlation_id,
                test.session_id,
                test.plan_id,
                test.trace_id,
                test.started_at,
                test.finished_at,
                test.status,
                test.duration,
                encode_json_text(list(test.selection_labels)),
                test.source_line,
                test.discovery_mode,
                test.knowledge_version,
                test.content_hash,
                test.indexed_at,
                test.invalidated_at,
                test.invalidation_reason,
                test.worker_slot,
                test.scheduled_at,
                test.max_attempts,
                test.timeout_seconds,
                test.retry_count,
                test.failure_kind,
                test.last_error_code,
            ),
        )

    def _persist_tests_for_file(
        self,
        engine_name: str,
        file_path: str,
    ) -> None:
        self._connection.execute(
            'DELETE FROM tests WHERE engine_name = ? AND test_path = ?',
            (engine_name, file_path),
        )
        tests = tuple(
            test
            for test in self._mirror.query_tests(
                TestKnowledgeQuery(
                    engine_name=engine_name,
                    test_path=file_path,
                ),
            )
            if test.test_path == file_path
        )
        for test in tests:
            self._persist_test(test.node_stable_id)

    def _persist_resource(self, name: str, scope: str) -> None:
        resource = self._mirror.get_resource(name, scope)
        if resource is None:
            self._connection.execute(
                'DELETE FROM resources WHERE name = ? AND scope = ?',
                (name, scope),
            )
            return

        self._connection.execute(
            """
            INSERT INTO resources (
                name,
                scope,
                correlation_id,
                session_id,
                plan_id,
                trace_id,
                last_worker_id,
                owner_worker_id,
                readiness_status,
                readiness_reason,
                last_node_id,
                last_node_stable_id,
                owner_node_id,
                owner_node_stable_id,
                acquire_count,
                release_count,
                last_test_id,
                owner_test_id,
                external_handle,
                last_heartbeat_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(name, scope) DO UPDATE SET
                correlation_id = excluded.correlation_id,
                session_id = excluded.session_id,
                plan_id = excluded.plan_id,
                trace_id = excluded.trace_id,
                last_worker_id = excluded.last_worker_id,
                owner_worker_id = excluded.owner_worker_id,
                readiness_status = excluded.readiness_status,
                readiness_reason = excluded.readiness_reason,
                last_node_id = excluded.last_node_id,
                last_node_stable_id = excluded.last_node_stable_id,
                owner_node_id = excluded.owner_node_id,
                owner_node_stable_id = excluded.owner_node_stable_id,
                acquire_count = excluded.acquire_count,
                release_count = excluded.release_count,
                last_test_id = excluded.last_test_id,
                owner_test_id = excluded.owner_test_id,
                external_handle = excluded.external_handle,
                last_heartbeat_at = excluded.last_heartbeat_at
            """,
            (
                resource.name,
                resource.scope,
                resource.correlation_id,
                resource.session_id,
                resource.plan_id,
                resource.trace_id,
                resource.last_worker_id,
                resource.owner_worker_id,
                resource.readiness_status,
                resource.readiness_reason,
                resource.last_node_id,
                resource.last_node_stable_id,
                resource.owner_node_id,
                resource.owner_node_stable_id,
                resource.acquire_count,
                resource.release_count,
                resource.last_test_id,
                resource.owner_test_id,
                resource.external_handle,
                resource.last_heartbeat_at,
            ),
        )

    def _persist_worker(self, worker_id: int) -> None:
        worker = self._mirror.get_worker(worker_id)
        if worker is None:
            self._connection.execute(
                'DELETE FROM workers WHERE worker_id = ?',
                (worker_id,),
            )
            return

        self._connection.execute(
            """
            INSERT INTO workers (
                worker_id,
                status,
                session_id,
                plan_id,
                trace_id,
                last_heartbeat_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                status = excluded.status,
                session_id = excluded.session_id,
                plan_id = excluded.plan_id,
                trace_id = excluded.trace_id,
                last_heartbeat_at = excluded.last_heartbeat_at
            """,
            (
                worker.worker_id,
                worker.status,
                worker.session_id,
                worker.plan_id,
                worker.trace_id,
                worker.last_heartbeat_at,
            ),
        )

    def _persist_definition(
        self,
        engine_name: str,
        file_path: str,
    ) -> None:
        definition = self._mirror.get_definition(engine_name, file_path)
        if definition is None:
            self._connection.execute(
                'DELETE FROM definitions '
                'WHERE engine_name = ? AND file_path = ?',
                (engine_name, file_path),
            )
            return

        self._connection.execute(
            """
            INSERT INTO definitions (
                engine_name,
                file_path,
                definition_count,
                discovery_mode,
                descriptors_json,
                knowledge_version,
                content_hash,
                correlation_id,
                session_id,
                trace_id,
                indexed_at,
                invalidated_at,
                invalidation_reason,
                materialized_count,
                last_materialized_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(engine_name, file_path) DO UPDATE SET
                definition_count = excluded.definition_count,
                discovery_mode = excluded.discovery_mode,
                descriptors_json = excluded.descriptors_json,
                knowledge_version = excluded.knowledge_version,
                content_hash = excluded.content_hash,
                correlation_id = excluded.correlation_id,
                session_id = excluded.session_id,
                trace_id = excluded.trace_id,
                indexed_at = excluded.indexed_at,
                invalidated_at = excluded.invalidated_at,
                invalidation_reason = excluded.invalidation_reason,
                materialized_count = excluded.materialized_count,
                last_materialized_at = excluded.last_materialized_at
            """,
            (
                definition.engine_name,
                definition.file_path,
                definition.definition_count,
                definition.discovery_mode,
                _serialize_definition_descriptors(definition.descriptors),
                definition.knowledge_version,
                definition.content_hash,
                definition.correlation_id,
                definition.session_id,
                definition.trace_id,
                definition.indexed_at,
                definition.invalidated_at,
                definition.invalidation_reason,
                definition.materialized_count,
                definition.last_materialized_at,
            ),
        )

    def _persist_registry_snapshot(
        self,
        engine_name: str,
        module_spec: str,
        package_hash: str,
        layout_key: str,
        loader_schema_version: str,
    ) -> None:
        snapshot = self._mirror.get_registry_snapshot(
            engine_name,
            module_spec,
            package_hash,
            layout_key,
            loader_schema_version,
        )
        if snapshot is None:
            self._connection.execute(
                """
                DELETE FROM registry_snapshots
                WHERE engine_name = ?
                  AND module_spec = ?
                  AND package_hash = ?
                  AND layout_key = ?
                  AND loader_schema_version = ?
                """,
                (
                    engine_name,
                    module_spec,
                    package_hash,
                    layout_key,
                    loader_schema_version,
                ),
            )
            return

        self._connection.execute(
            """
            INSERT INTO registry_snapshots (
                engine_name,
                module_spec,
                package_hash,
                layout_key,
                loader_schema_version,
                entries_json,
                source_count,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(
                engine_name,
                module_spec,
                package_hash,
                layout_key,
                loader_schema_version
            ) DO UPDATE SET
                entries_json = excluded.entries_json,
                source_count = excluded.source_count,
                created_at = excluded.created_at
            """,
            (
                snapshot.engine_name,
                snapshot.module_spec,
                snapshot.package_hash,
                snapshot.layout_key,
                snapshot.loader_schema_version,
                _serialize_registry_entries(snapshot.entries),
                snapshot.source_count,
                snapshot.created_at,
            ),
        )

    def _persist_session_artifact(self, session_id: str) -> None:
        artifacts = self._mirror.query_session_artifacts(
            SessionArtifactQuery(session_id=session_id, limit=1),
        )
        if not artifacts:
            self._connection.execute(
                'DELETE FROM session_artifacts WHERE session_id = ?',
                (session_id,),
            )
            return

        artifact = artifacts[0]
        self._connection.execute(
            """
            INSERT INTO session_artifacts (
                session_id,
                trace_id,
                root_path,
                workspace_fingerprint,
                plan_id,
                config_snapshot_json,
                capability_snapshots_json,
                plan_explanation_json,
                timing_json,
                has_failures,
                report_summary_json,
                telemetry_summary_json,
                persistence_policy_json,
                recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                trace_id = excluded.trace_id,
                root_path = excluded.root_path,
                workspace_fingerprint = excluded.workspace_fingerprint,
                plan_id = excluded.plan_id,
                config_snapshot_json = excluded.config_snapshot_json,
                capability_snapshots_json = excluded.capability_snapshots_json,
                plan_explanation_json = excluded.plan_explanation_json,
                timing_json = excluded.timing_json,
                has_failures = excluded.has_failures,
                report_summary_json = excluded.report_summary_json,
                telemetry_summary_json = excluded.telemetry_summary_json,
                persistence_policy_json = excluded.persistence_policy_json,
                recorded_at = excluded.recorded_at
            """,
            (
                artifact.session_id,
                artifact.trace_id,
                artifact.root_path,
                artifact.workspace_fingerprint,
                artifact.plan_id,
                encode_json_text(artifact.config_snapshot.to_dict()),
                encode_json_text(
                    [
                        snapshot.to_dict()
                        for snapshot in artifact.capability_snapshots
                    ],
                ),
                (
                    None
                    if artifact.plan_explanation is None
                    else encode_json_text(
                        artifact.plan_explanation.to_dict(),
                    )
                ),
                (
                    None
                    if artifact.timing is None
                    else encode_json_text(artifact.timing.to_dict())
                ),
                _bool_to_int(artifact.has_failures),
                (
                    None
                    if artifact.report_summary is None
                    else encode_json_text(
                        artifact.report_summary.to_dict(),
                    )
                ),
                (
                    None
                    if artifact.telemetry_summary is None
                    else encode_json_text(
                        artifact.telemetry_summary.to_dict(),
                    )
                ),
                encode_json_text(artifact.persistence_policy.to_dict()),
                artifact.recorded_at,
            ),
        )

    def _prune_persisted_session_artifacts(
        self,
        retention_scope_value: str,
        max_artifacts: int,
        max_age_seconds: float | None,
        recorded_at: float,
    ) -> None:
        if max_age_seconds is not None:
            reference_recorded_at = self._connection.execute(
                """
                SELECT COALESCE(MAX(recorded_at), ?)
                FROM session_artifacts
                WHERE COALESCE(workspace_fingerprint, root_path) = ?
                """,
                (recorded_at, retention_scope_value),
            ).fetchone()[0]
            self._connection.execute(
                """
                DELETE FROM session_artifacts
                WHERE COALESCE(workspace_fingerprint, root_path) = ?
                  AND recorded_at < ?
                """,
                (
                    retention_scope_value,
                    float(reference_recorded_at) - max_age_seconds,
                ),
            )
        self._connection.execute(
            """
            DELETE FROM session_artifacts
            WHERE COALESCE(workspace_fingerprint, root_path) = ?
              AND session_id NOT IN (
                  SELECT session_id
                  FROM session_artifacts
                  WHERE COALESCE(workspace_fingerprint, root_path) = ?
                  ORDER BY recorded_at DESC, session_id DESC
                  LIMIT ?
              )
            """,
            (retention_scope_value, retention_scope_value, max_artifacts),
        )

    def _clear_runtime_projection(self) -> None:
        self._connection.executescript(
            """
            DELETE FROM session_state;
            DELETE FROM latest_plan;
            DELETE FROM resources;
            """,
        )

    def _load_snapshot_from_db(self) -> KnowledgeSnapshot:
        return KnowledgeSnapshot(
            session=_load_session_knowledge(self._connection),
            latest_plan=_load_plan_knowledge(self._connection),
            tests=_load_test_knowledge(self._connection),
            resources=_load_resource_knowledge(self._connection),
            workers=_load_worker_knowledge(self._connection),
            definitions=_load_definition_knowledge(self._connection),
            registry_snapshots=_load_registry_knowledge(self._connection),
        )

    def _read_meta(self, key: str) -> str | None:
        return _read_meta(self._connection, key)

    def _write_meta(self, key: str, value: str) -> None:
        self._connection.execute(
            """
            INSERT INTO meta (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


class KnowledgeBaseDomainEventSink:
    __slots__ = (
        '_persistence_queue',
        '_worker_error',
        '_worker_thread',
        'knowledge_base',
    )

    def __init__(self, knowledge_base: KnowledgeBase) -> None:
        self.knowledge_base = knowledge_base
        self._persistence_queue: queue.Queue[DomainEvent | None] | None = None
        self._worker_error: BaseException | None = None
        self._worker_thread: threading.Thread | None = None

    async def start(self) -> None:
        return None

    async def emit(self, event: DomainEvent) -> None:
        if not isinstance(self.knowledge_base, PersistentKnowledgeBase):
            self.knowledge_base.apply(event)
            return

        self._raise_worker_error()
        if not self.knowledge_base.apply_in_memory(event):
            return

        self._ensure_worker_started()
        await asyncio.to_thread(self._persistence_queue.put, event)

    async def close(self) -> None:
        errors: list[BaseException] = []
        if (
            self._worker_thread is not None
            and self._persistence_queue is not None
        ):
            await asyncio.to_thread(self._persistence_queue.put, None)
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(
                        self._worker_thread.join,
                        PERSISTENCE_WORKER_JOIN_TIMEOUT_SECONDS,
                    ),
                    timeout=(PERSISTENCE_WORKER_JOIN_TIMEOUT_SECONDS + 0.1),
                )
            except TimeoutError as error:
                msg = (
                    'Knowledge base persistence worker did not stop '
                    f'within {PERSISTENCE_WORKER_JOIN_TIMEOUT_SECONDS:.1f}s'
                )
                errors.append(RuntimeError(msg))
                errors[-1].__cause__ = error

            if self._worker_thread.is_alive():
                errors.append(
                    RuntimeError(
                        'Knowledge base persistence worker remained alive '
                        'after graceful shutdown timeout',
                    ),
                )
            else:
                try:
                    self._raise_worker_error()
                except RuntimeError as error:
                    errors.append(error)

        if isinstance(self.knowledge_base, PersistentKnowledgeBase):
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self.knowledge_base.close),
                    timeout=(PERSISTENT_KNOWLEDGE_BASE_CLOSE_TIMEOUT_SECONDS),
                )
            except TimeoutError as error:
                self.knowledge_base.interrupt()
                try:
                    self.knowledge_base.force_close()
                except BaseException as close_error:
                    errors.append(close_error)

                msg = (
                    'Persistent knowledge base close exceeded '
                    f'{PERSISTENT_KNOWLEDGE_BASE_CLOSE_TIMEOUT_SECONDS:.1f}s'
                )
                timeout_error = RuntimeError(msg)
                timeout_error.__cause__ = error
                errors.append(timeout_error)
        else:
            self.knowledge_base.close()

        if not errors:
            return

        if len(errors) == 1:
            raise errors[0]

        msg = 'Knowledge base sink shutdown failed'
        raise ExceptionGroup(msg, errors)

    def _ensure_worker_started(self) -> None:
        if self._worker_thread is not None:
            return

        self._persistence_queue = queue.Queue(
            maxsize=KNOWLEDGE_BASE_PERSIST_QUEUE_MAXSIZE,
        )
        self._worker_thread = threading.Thread(
            target=self._run_persistence_worker,
            name='cosecha-kb-sink',
            daemon=True,
        )
        self._worker_thread.start()

    def _run_persistence_worker(self) -> None:
        if self._persistence_queue is None:
            return

        while True:
            event = self._persistence_queue.get()
            if event is None:
                self._persistence_queue.task_done()
                return

            batch: list[DomainEvent] = [event]
            should_stop = False
            while len(batch) < KNOWLEDGE_BASE_PERSIST_BATCH_SIZE:
                try:
                    queued_event = self._persistence_queue.get_nowait()
                except queue.Empty:
                    break

                if queued_event is None:
                    should_stop = True
                    self._persistence_queue.task_done()
                    break

                batch.append(queued_event)

            try:
                self.knowledge_base.persist_events(tuple(batch))
            except BaseException as error:
                self._worker_error = error
                return
            finally:
                for _ in batch:
                    self._persistence_queue.task_done()

            if should_stop:
                return

    def _raise_worker_error(self) -> None:
        if self._worker_error is not None:
            msg = 'Knowledge base persistence worker failed'
            raise RuntimeError(msg) from self._worker_error


class ReadOnlyPersistentKnowledgeBase:
    __slots__ = ('_closed', '_connection', '_db_path')

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._connection = sqlite3.connect(
            f'file:{self._db_path}?mode=ro',
            uri=True,
        )
        self._connection.row_factory = sqlite3.Row
        self._closed = False
        self._validate_schema()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def snapshot(self) -> KnowledgeSnapshot:
        return KnowledgeSnapshot(
            session=_load_session_knowledge(self._connection),
            latest_plan=_load_plan_knowledge(self._connection),
            tests=_load_test_knowledge(self._connection),
            resources=_load_resource_knowledge(self._connection),
            workers=_load_worker_knowledge(self._connection),
            definitions=_load_definition_knowledge(self._connection),
            registry_snapshots=_load_registry_knowledge(self._connection),
        )

    def live_snapshot(self) -> LiveExecutionSnapshot:
        return LiveExecutionSnapshot()

    def has_test_path(
        self,
        test_path: str,
        *,
        engine_name: str | None = None,
    ) -> bool:
        if engine_name is None:
            row = self._connection.execute(
                'SELECT 1 FROM tests WHERE test_path = ? LIMIT 1',
                (test_path,),
            ).fetchone()
        else:
            row = self._connection.execute(
                'SELECT 1 FROM tests WHERE test_path = ? '
                'AND engine_name = ? LIMIT 1',
                (test_path, engine_name),
            ).fetchone()

        return row is not None

    def query_tests(
        self,
        query: TestKnowledgeQuery,
    ) -> tuple[TestKnowledge, ...]:
        return _load_test_knowledge(self._connection, query)

    def query_domain_events(
        self,
        query: DomainEventQuery,
    ) -> tuple[DomainEvent, ...]:
        return _load_domain_events(self._connection, query)

    def query_definitions(
        self,
        query: DefinitionKnowledgeQuery,
    ) -> tuple[DefinitionKnowledge, ...]:
        return _load_definition_knowledge(self._connection, query)

    def query_registry_items(
        self,
        query: RegistryKnowledgeQuery,
    ) -> tuple[RegistryKnowledgeSnapshot, ...]:
        return _load_registry_knowledge(self._connection, query)

    def query_resources(
        self,
        query: ResourceKnowledgeQuery,
    ) -> tuple[ResourceKnowledge, ...]:
        resources = _load_resource_knowledge(self._connection, query)
        return _filter_resource_knowledge(resources, query)

    def query_session_artifacts(
        self,
        query: SessionArtifactQuery,
    ) -> tuple[SessionArtifact, ...]:
        artifacts = _load_session_artifacts(self._connection, query)
        return _filter_session_artifacts(artifacts, query)

    def close(self) -> None:
        if self._closed:
            return

        self._connection.close()
        self._closed = True

    def _validate_schema(self) -> None:
        current_version = _read_meta(self._connection, 'schema_version')
        if current_version != str(KNOWLEDGE_BASE_SCHEMA_VERSION):
            msg = (
                'Read-only knowledge base schema mismatch: '
                f'expected {KNOWLEDGE_BASE_SCHEMA_VERSION}, '
                f'got {current_version!r}'
            )
            raise ValueError(msg)


def _load_session_knowledge(
    connection: sqlite3.Connection,
) -> SessionKnowledge | None:
    row = connection.execute(
        'SELECT * FROM session_state WHERE id = 1',
    ).fetchone()
    if row is None:
        return None

    return SessionKnowledge(
        root_path=str(row['root_path']),
        workspace_fingerprint=_cast_optional_str(
            row['workspace_fingerprint'],
        ),
        concurrency=int(row['concurrency']),
        session_id=_cast_optional_str(row['session_id']),
        trace_id=_cast_optional_str(row['trace_id']),
        started_at=float(row['started_at']),
        finished_at=_cast_optional_float(row['finished_at']),
        has_failures=_cast_optional_bool(row['has_failures']),
    )


def _load_plan_knowledge(
    connection: sqlite3.Connection,
) -> PlanKnowledge | None:
    row = connection.execute(
        'SELECT * FROM latest_plan WHERE id = 1',
    ).fetchone()
    if row is None:
        return None

    return PlanKnowledge(
        mode=str(row['mode']),
        executable=bool(row['executable']),
        node_count=int(row['node_count']),
        issue_count=int(row['issue_count']),
        plan_id=_cast_optional_str(row['plan_id']),
        correlation_id=_cast_optional_str(row['correlation_id']),
        session_id=_cast_optional_str(row['session_id']),
        trace_id=_cast_optional_str(row['trace_id']),
        analyzed_at=float(row['analyzed_at']),
    )


def _load_test_knowledge(
    connection: sqlite3.Connection,
    query: TestKnowledgeQuery | None = None,
) -> tuple[TestKnowledge, ...]:
    sql = [
        'SELECT * FROM tests',
    ]
    parameters: list[object] = []
    predicates: list[str] = []
    if query is not None:
        if query.engine_name is not None:
            predicates.append('engine_name = ?')
            parameters.append(query.engine_name)
        if query.test_path is not None:
            predicates.append('test_path = ?')
            parameters.append(query.test_path)
        if query.status is not None:
            predicates.append('status = ?')
            parameters.append(query.status)
        if query.failure_kind is not None:
            predicates.append('failure_kind = ?')
            parameters.append(query.failure_kind)
        if query.node_stable_id is not None:
            predicates.append('node_stable_id = ?')
            parameters.append(query.node_stable_id)
        if query.plan_id is not None:
            predicates.append('plan_id = ?')
            parameters.append(query.plan_id)

    if predicates:
        sql.append('WHERE ' + ' AND '.join(predicates))

    sql.append(
        'ORDER BY test_path, engine_name, COALESCE(source_line, 0), '
        'test_name, node_stable_id',
    )
    if query is not None and query.limit is not None:
        sql.append('LIMIT ?')
        parameters.append(query.limit)

    rows = connection.execute(' '.join(sql), tuple(parameters)).fetchall()
    return tuple(
        TestKnowledge(
            node_id=str(row['node_id']),
            node_stable_id=str(row['node_stable_id']),
            engine_name=str(row['engine_name']),
            test_name=str(row['test_name']),
            test_path=str(row['test_path']),
            correlation_id=_cast_optional_str(row['correlation_id']),
            session_id=_cast_optional_str(row['session_id']),
            plan_id=_cast_optional_str(row['plan_id']),
            trace_id=_cast_optional_str(row['trace_id']),
            started_at=_cast_optional_float(row['started_at']),
            finished_at=_cast_optional_float(row['finished_at']),
            status=_cast_optional_str(row['status']),
            duration=_cast_optional_float(row['duration']),
            selection_labels=tuple(
                str(label)
                for label in decode_json_list(
                    row['selection_labels_json'] or '[]',
                )
            ),
            source_line=_cast_optional_int(row['source_line']),
            discovery_mode=_cast_optional_str(row['discovery_mode']),
            knowledge_version=_cast_optional_str(row['knowledge_version']),
            content_hash=_cast_optional_str(row['content_hash']),
            indexed_at=_cast_optional_float(row['indexed_at']),
            invalidated_at=_cast_optional_float(row['invalidated_at']),
            invalidation_reason=_cast_optional_str(
                row['invalidation_reason'],
            ),
            worker_slot=_cast_optional_int(row['worker_slot']),
            scheduled_at=_cast_optional_float(row['scheduled_at']),
            max_attempts=_cast_optional_int(row['max_attempts']),
            timeout_seconds=_cast_optional_float(row['timeout_seconds']),
            retry_count=int(row['retry_count']),
            failure_kind=_cast_optional_str(row['failure_kind']),
            last_error_code=_cast_optional_str(row['last_error_code']),
        )
        for row in rows
    )


def _load_resource_knowledge(
    connection: sqlite3.Connection,
    query: ResourceKnowledgeQuery | None = None,
) -> tuple[ResourceKnowledge, ...]:
    sql = ['SELECT * FROM resources']
    parameters: list[object] = []
    predicates: list[str] = []
    if query is not None:
        if query.name is not None:
            predicates.append('name = ?')
            parameters.append(query.name)
        if query.scope is not None:
            predicates.append('scope = ?')
            parameters.append(query.scope)
        if query.last_test_id is not None:
            predicates.append('last_test_id = ?')
            parameters.append(query.last_test_id)

    if predicates:
        sql.append('WHERE ' + ' AND '.join(predicates))

    sql.append('ORDER BY name, scope')
    if query is not None and query.limit is not None:
        sql.append('LIMIT ?')
        parameters.append(query.limit)

    rows = connection.execute(
        ' '.join(sql),
        tuple(parameters),
    ).fetchall()
    return tuple(
        ResourceKnowledge(
            name=str(row['name']),
            scope=str(row['scope']),
            readiness_status=_cast_optional_str(
                row['readiness_status'],
            ),
            readiness_reason=_cast_optional_str(
                row['readiness_reason'],
            ),
            correlation_id=_cast_optional_str(row['correlation_id']),
            session_id=_cast_optional_str(row['session_id']),
            plan_id=_cast_optional_str(row['plan_id']),
            trace_id=_cast_optional_str(row['trace_id']),
            last_worker_id=_cast_optional_int(row['last_worker_id']),
            owner_worker_id=_cast_optional_int(row['owner_worker_id']),
            last_node_id=_cast_optional_str(row['last_node_id']),
            last_node_stable_id=_cast_optional_str(
                row['last_node_stable_id'],
            ),
            owner_node_id=_cast_optional_str(row['owner_node_id']),
            owner_node_stable_id=_cast_optional_str(
                row['owner_node_stable_id'],
            ),
            acquire_count=int(row['acquire_count']),
            release_count=int(row['release_count']),
            last_test_id=_cast_optional_str(row['last_test_id']),
            owner_test_id=_cast_optional_str(row['owner_test_id']),
            external_handle=_cast_optional_str(row['external_handle']),
            last_heartbeat_at=_cast_optional_float(
                row['last_heartbeat_at'],
            ),
        )
        for row in rows
    )


def _load_worker_knowledge(
    connection: sqlite3.Connection,
) -> tuple[WorkerKnowledge, ...]:
    rows = connection.execute(
        'SELECT * FROM workers ORDER BY worker_id',
    ).fetchall()
    return tuple(
        WorkerKnowledge(
            worker_id=int(row['worker_id']),
            status=str(row['status']),
            session_id=_cast_optional_str(row['session_id']),
            plan_id=_cast_optional_str(row['plan_id']),
            trace_id=_cast_optional_str(row['trace_id']),
            last_heartbeat_at=_cast_optional_float(
                row['last_heartbeat_at'],
            ),
        )
        for row in rows
    )


def _load_definition_knowledge(
    connection: sqlite3.Connection,
    query: DefinitionKnowledgeQuery | None = None,
) -> tuple[DefinitionKnowledge, ...]:
    sql = ['SELECT * FROM definitions']
    parameters: list[object] = []
    predicates: list[str] = []
    if query is not None:
        if query.engine_name is not None:
            predicates.append('engine_name = ?')
            parameters.append(query.engine_name)
        if query.file_path is not None:
            predicates.append('file_path = ?')
            parameters.append(query.file_path)
        if query.discovery_mode is not None:
            predicates.append('discovery_mode = ?')
            parameters.append(query.discovery_mode)
        if not query.include_invalidated:
            predicates.append('invalidated_at IS NULL')

    if predicates:
        sql.append('WHERE ' + ' AND '.join(predicates))

    sql.append('ORDER BY engine_name, file_path')
    if (
        query is not None
        and query.limit is not None
        and query.step_type is None
        and query.step_text is None
    ):
        sql.append('LIMIT ?')
        parameters.append(query.limit)

    rows = connection.execute(' '.join(sql), tuple(parameters)).fetchall()
    definitions = tuple(
        DefinitionKnowledge(
            engine_name=str(row['engine_name']),
            file_path=str(row['file_path']),
            definition_count=int(row['definition_count']),
            discovery_mode=str(row['discovery_mode']),
            descriptors=_deserialize_definition_descriptors(
                row['descriptors_json'],
            ),
            knowledge_version=_cast_optional_str(row['knowledge_version']),
            content_hash=_cast_optional_str(row['content_hash']),
            correlation_id=_cast_optional_str(row['correlation_id']),
            session_id=_cast_optional_str(row['session_id']),
            trace_id=_cast_optional_str(row['trace_id']),
            indexed_at=_cast_optional_float(row['indexed_at']),
            invalidated_at=_cast_optional_float(row['invalidated_at']),
            invalidation_reason=_cast_optional_str(
                row['invalidation_reason'],
            ),
            materialized_count=int(row['materialized_count']),
            last_materialized_at=_cast_optional_float(
                row['last_materialized_at'],
            ),
        )
        for row in rows
    )
    if query is None:
        return definitions

    return _filter_definition_knowledge(definitions, query)


def _load_registry_knowledge(
    connection: sqlite3.Connection,
    query: RegistryKnowledgeQuery | None = None,
) -> tuple[RegistryKnowledgeSnapshot, ...]:
    sql = ['SELECT * FROM registry_snapshots']
    parameters: list[object] = []
    predicates: list[str] = []
    if query is not None:
        if query.engine_name is not None:
            predicates.append('engine_name = ?')
            parameters.append(query.engine_name)
        if query.module_spec is not None:
            predicates.append('module_spec = ?')
            parameters.append(query.module_spec)
        if query.package_hash is not None:
            predicates.append('package_hash = ?')
            parameters.append(query.package_hash)
        if query.layout_key is not None:
            predicates.append('layout_key = ?')
            parameters.append(query.layout_key)
        if query.loader_schema_version is not None:
            predicates.append('loader_schema_version = ?')
            parameters.append(query.loader_schema_version)

    if predicates:
        sql.append('WHERE ' + ' AND '.join(predicates))

    sql.append(
        'ORDER BY engine_name, module_spec, layout_key, package_hash, '
        'loader_schema_version',
    )
    if query is not None and query.limit is not None:
        sql.append('LIMIT ?')
        parameters.append(query.limit)

    rows = connection.execute(' '.join(sql), tuple(parameters)).fetchall()
    snapshots = tuple(
        RegistryKnowledgeSnapshot(
            engine_name=str(row['engine_name']),
            module_spec=str(row['module_spec']),
            package_hash=str(row['package_hash']),
            layout_key=str(row['layout_key']),
            loader_schema_version=str(row['loader_schema_version']),
            entries=_deserialize_registry_entries(row['entries_json']),
            source_count=int(row['source_count']),
            created_at=float(row['created_at']),
        )
        for row in rows
    )
    if query is None:
        return snapshots

    return _filter_registry_knowledge(snapshots, query)


def _load_session_artifacts(
    connection: sqlite3.Connection,
    query: SessionArtifactQuery | None = None,
) -> tuple[SessionArtifact, ...]:
    sql = ['SELECT * FROM session_artifacts']
    parameters: list[object] = []
    predicates: list[str] = []
    if query is not None:
        if query.session_id is not None:
            predicates.append('session_id = ?')
            parameters.append(query.session_id)
        if query.trace_id is not None:
            predicates.append('trace_id = ?')
            parameters.append(query.trace_id)

    if predicates:
        sql.append('WHERE ' + ' AND '.join(predicates))

    sql.append('ORDER BY recorded_at DESC, session_id')
    if query is not None and query.limit is not None:
        sql.append('LIMIT ?')
        parameters.append(query.limit)

    rows = connection.execute(
        ' '.join(sql),
        tuple(parameters),
    ).fetchall()
    return tuple(
        SessionArtifact.from_dict(
            {
                'capability_snapshots': decode_json(
                    row['capability_snapshots_json'] or '[]',
                ),
                'config_snapshot': decode_json(row['config_snapshot_json']),
                'has_failures': _cast_optional_bool(row['has_failures']),
                'plan_explanation': (
                    None
                    if row['plan_explanation_json'] is None
                    else decode_json(str(row['plan_explanation_json']))
                ),
                'plan_id': _cast_optional_str(row['plan_id']),
                'persistence_policy': decode_json(
                    row['persistence_policy_json'],
                ),
                'recorded_at': float(row['recorded_at']),
                'report_summary': (
                    None
                    if row['report_summary_json'] is None
                    else decode_json(str(row['report_summary_json']))
                ),
                'root_path': str(row['root_path']),
                'workspace_fingerprint': _cast_optional_str(
                    row['workspace_fingerprint'],
                ),
                'session_id': str(row['session_id']),
                'telemetry_summary': (
                    None
                    if row['telemetry_summary_json'] is None
                    else decode_json(str(row['telemetry_summary_json']))
                ),
                'timing': (
                    None
                    if row['timing_json'] is None
                    else decode_json(str(row['timing_json']))
                ),
                'trace_id': _cast_optional_str(row['trace_id']),
            },
        )
        for row in rows
    )


def _load_domain_events(
    connection: sqlite3.Connection,
    query: DomainEventQuery | None = None,
) -> tuple[DomainEvent, ...]:
    sql = [
        'SELECT payload_json FROM domain_event_log',
    ]
    parameters: list[object] = []
    where_clauses: list[str] = []

    if query is not None:
        if query.event_type is not None:
            where_clauses.append('event_type = ?')
            parameters.append(query.event_type)
        if query.session_id is not None:
            where_clauses.append('session_id = ?')
            parameters.append(query.session_id)
        if query.plan_id is not None:
            where_clauses.append('plan_id = ?')
            parameters.append(query.plan_id)
        if query.node_stable_id is not None:
            where_clauses.append('node_stable_id = ?')
            parameters.append(query.node_stable_id)
        if query.after_sequence_number is not None:
            where_clauses.append('sequence_number > ?')
            parameters.append(query.after_sequence_number)

    if where_clauses:
        sql.append('WHERE ' + ' AND '.join(where_clauses))

    sql.append('ORDER BY sequence_number')

    if query is not None and query.limit is not None:
        sql.append('LIMIT ?')
        parameters.append(query.limit)

    rows = connection.execute(
        ' '.join(sql),
        tuple(parameters),
    ).fetchall()

    return tuple(
        deserialize_domain_event(
            decode_json_dict(str(row['payload_json'])),
        )
        for row in rows
    )


def _bool_to_int(value: object) -> int | None:
    if value is None:
        return None

    return int(bool(value))


def _serialize_definition_descriptors(
    descriptors: tuple[DefinitionKnowledgeRecord, ...],
) -> str:
    return encode_json_text(
        [descriptor.to_dict() for descriptor in descriptors],
    )


def _deserialize_definition_descriptors(
    value: object,
) -> tuple[DefinitionKnowledgeRecord, ...]:
    if value is None:
        return ()

    try:
        records = decode_json_list(str(value))
    except ValueError:
        return ()

    return tuple(
        DefinitionKnowledgeRecord.from_dict(record)
        for record in records
        if isinstance(record, dict)
    )


def _serialize_registry_entries(
    entries: tuple[RegistryKnowledgeEntry, ...],
) -> str:
    return encode_json_text([entry.to_dict() for entry in entries])


def _deserialize_registry_entries(
    value: object,
) -> tuple[RegistryKnowledgeEntry, ...]:
    if value is None:
        return ()

    try:
        records = decode_json_list(str(value))
    except ValueError:
        return ()

    return tuple(
        RegistryKnowledgeEntry.from_dict(record)
        for record in records
        if isinstance(record, dict)
    )


def _filter_test_knowledge(
    tests: tuple[TestKnowledge, ...],
    query: TestKnowledgeQuery,
) -> tuple[TestKnowledge, ...]:
    filtered = tests
    if query.engine_name is not None:
        filtered = tuple(
            test for test in filtered if test.engine_name == query.engine_name
        )
    if query.test_path is not None:
        filtered = tuple(
            test for test in filtered if test.test_path == query.test_path
        )
    if query.status is not None:
        filtered = tuple(
            test for test in filtered if test.status == query.status
        )
    if query.failure_kind is not None:
        filtered = tuple(
            test
            for test in filtered
            if test.failure_kind == query.failure_kind
        )
    if query.node_stable_id is not None:
        filtered = tuple(
            test
            for test in filtered
            if test.node_stable_id == query.node_stable_id
        )
    if query.plan_id is not None:
        filtered = tuple(
            test for test in filtered if test.plan_id == query.plan_id
        )
    if query.limit is not None:
        filtered = filtered[: query.limit]

    return filtered


def _filter_definition_knowledge(
    definitions: tuple[DefinitionKnowledge, ...],
    query: DefinitionKnowledgeQuery,
) -> tuple[DefinitionKnowledge, ...]:
    filtered = definitions
    if query.engine_name is not None:
        filtered = tuple(
            definition
            for definition in filtered
            if definition.engine_name == query.engine_name
        )
    if query.file_path is not None:
        filtered = tuple(
            definition
            for definition in filtered
            if definition.file_path == query.file_path
        )
    if query.step_type is not None:
        filtered = tuple(
            definition
            for definition in filtered
            if definition.matching_descriptors(step_type=query.step_type)
        )
    if query.step_text is not None:
        filtered = tuple(
            definition
            for definition in filtered
            if definition.matching_descriptors(
                step_type=query.step_type,
                step_text=query.step_text,
            )
        )
    if query.discovery_mode is not None:
        filtered = tuple(
            definition
            for definition in filtered
            if definition.discovery_mode == query.discovery_mode
        )
    if not query.include_invalidated:
        filtered = tuple(
            definition
            for definition in filtered
            if definition.invalidated_at is None
        )
    if query.limit is not None:
        filtered = filtered[: query.limit]

    return filtered


def _filter_registry_knowledge(
    snapshots: tuple[RegistryKnowledgeSnapshot, ...],
    query: RegistryKnowledgeQuery,
) -> tuple[RegistryKnowledgeSnapshot, ...]:
    filtered = snapshots
    if query.engine_name is not None:
        filtered = tuple(
            snapshot
            for snapshot in filtered
            if snapshot.engine_name == query.engine_name
        )
    if query.module_spec is not None:
        filtered = tuple(
            snapshot
            for snapshot in filtered
            if snapshot.module_spec == query.module_spec
        )
    if query.package_hash is not None:
        filtered = tuple(
            snapshot
            for snapshot in filtered
            if snapshot.package_hash == query.package_hash
        )
    if query.layout_key is not None:
        filtered = tuple(
            snapshot
            for snapshot in filtered
            if snapshot.layout_key == query.layout_key
        )
    if query.loader_schema_version is not None:
        filtered = tuple(
            snapshot
            for snapshot in filtered
            if snapshot.loader_schema_version == query.loader_schema_version
        )
    if query.limit is not None:
        filtered = filtered[: query.limit]

    return filtered


def _filter_resource_knowledge(
    resources: tuple[ResourceKnowledge, ...],
    query: ResourceKnowledgeQuery,
) -> tuple[ResourceKnowledge, ...]:
    filtered = resources
    if query.name is not None:
        filtered = tuple(
            resource for resource in filtered if resource.name == query.name
        )
    if query.scope is not None:
        filtered = tuple(
            resource for resource in filtered if resource.scope == query.scope
        )
    if query.last_test_id is not None:
        filtered = tuple(
            resource
            for resource in filtered
            if resource.last_test_id == query.last_test_id
        )
    if query.limit is not None:
        filtered = filtered[: query.limit]

    return filtered


def _filter_session_artifacts(
    artifacts: tuple[SessionArtifact, ...],
    query: SessionArtifactQuery,
) -> tuple[SessionArtifact, ...]:
    filtered = artifacts
    if query.session_id is not None:
        filtered = tuple(
            artifact
            for artifact in filtered
            if artifact.session_id == query.session_id
        )
    if query.trace_id is not None:
        filtered = tuple(
            artifact
            for artifact in filtered
            if artifact.trace_id == query.trace_id
        )
    if query.limit is not None:
        filtered = filtered[: query.limit]

    return filtered


def _filter_domain_events(
    events: tuple[DomainEvent, ...],
    query: DomainEventQuery,
) -> tuple[DomainEvent, ...]:
    filtered = events
    if query.event_type is not None:
        filtered = tuple(
            event for event in filtered if event.event_type == query.event_type
        )
    if query.session_id is not None:
        filtered = tuple(
            event
            for event in filtered
            if event.metadata.session_id == query.session_id
        )
    if query.plan_id is not None:
        filtered = tuple(
            event
            for event in filtered
            if event.metadata.plan_id == query.plan_id
        )
    if query.node_stable_id is not None:
        filtered = tuple(
            event
            for event in filtered
            if event.metadata.node_stable_id == query.node_stable_id
        )
    if query.after_sequence_number is not None:
        filtered = tuple(
            event
            for event in filtered
            if (
                event.metadata.sequence_number is not None
                and event.metadata.sequence_number
                > query.after_sequence_number
            )
        )
    if query.limit is not None:
        filtered = filtered[: query.limit]

    return filtered


def _read_meta(
    connection: sqlite3.Connection,
    key: str,
) -> str | None:
    row = connection.execute(
        'SELECT value FROM meta WHERE key = ?',
        (key,),
    ).fetchone()
    if row is None:
        return None

    return str(row['value'])


def _migrate_schema_v8_to_v9(connection: sqlite3.Connection) -> None:
    default_policy_json = encode_json_text(
        default_session_artifact_persistence_policy().to_dict(),
    )
    connection.execute(
        """
        ALTER TABLE session_artifacts
        ADD COLUMN persistence_policy_json TEXT NOT NULL DEFAULT
        """
        f" '{default_policy_json}'",
    )


def _migrate_schema_v9_to_v10(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS domain_event_log (
            sequence_number INTEGER PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            session_id TEXT,
            plan_id TEXT,
            node_stable_id TEXT,
            payload_json TEXT NOT NULL,
            timestamp REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_domain_event_log_session_sequence
            ON domain_event_log (session_id, sequence_number);
        CREATE INDEX IF NOT EXISTS idx_domain_event_log_plan_sequence
            ON domain_event_log (plan_id, sequence_number);
        """,
    )


def _migrate_schema_v10_to_v11(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        ALTER TABLE session_artifacts
        ADD COLUMN report_summary_json TEXT
        """,
    )
    connection.execute(
        """
        ALTER TABLE session_artifacts
        ADD COLUMN telemetry_summary_json TEXT
        """,
    )


def _migrate_schema_v11_to_v12(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS registry_snapshots (
            engine_name TEXT NOT NULL,
            module_spec TEXT NOT NULL,
            package_hash TEXT NOT NULL,
            layout_key TEXT NOT NULL,
            loader_schema_version TEXT NOT NULL,
            entries_json TEXT NOT NULL,
            source_count INTEGER NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY (
                engine_name,
                module_spec,
                package_hash,
                layout_key,
                loader_schema_version
            )
        );
        CREATE INDEX IF NOT EXISTS idx_registry_snapshots_lookup
            ON registry_snapshots (
                engine_name,
                module_spec,
                layout_key,
                package_hash,
                loader_schema_version
            );
        """,
    )


def _migrate_schema_v12_to_v13(connection: sqlite3.Connection) -> None:
    tests_table = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'tests'
        """,
    ).fetchone()
    if tests_table is None:
        return
    connection.execute(
        """
        ALTER TABLE tests
        ADD COLUMN failure_kind TEXT
        """,
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tests_failure_kind
            ON tests (failure_kind)
        """,
    )


def _migrate_schema_v13_to_v14(connection: sqlite3.Connection) -> None:
    resources_table = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'resources'
        """,
    ).fetchone()
    if resources_table is None:
        return

    connection.execute(
        """
        ALTER TABLE resources
        ADD COLUMN readiness_status TEXT
        """,
    )
    connection.execute(
        """
        ALTER TABLE resources
        ADD COLUMN readiness_reason TEXT
        """,
    )


def _migrate_schema_v14_to_v15(connection: sqlite3.Connection) -> None:
    session_state_table = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'session_state'
        """,
    ).fetchone()
    if session_state_table is not None:
        connection.execute(
            """
            ALTER TABLE session_state
            ADD COLUMN workspace_fingerprint TEXT
            """,
        )

    session_artifacts_table = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'session_artifacts'
        """,
    ).fetchone()
    if session_artifacts_table is not None:
        connection.execute(
            """
            ALTER TABLE session_artifacts
            ADD COLUMN workspace_fingerprint TEXT
            """,
        )


_SCHEMA_MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    8: _migrate_schema_v8_to_v9,
    9: _migrate_schema_v9_to_v10,
    10: _migrate_schema_v10_to_v11,
    11: _migrate_schema_v11_to_v12,
    12: _migrate_schema_v12_to_v13,
    13: _migrate_schema_v13_to_v14,
    14: _migrate_schema_v14_to_v15,
}


def _cast_optional_str(value: object) -> str | None:
    if value is None:
        return None

    return str(value)


def _cast_optional_float(value: object) -> float | None:
    if value is None:
        return None

    return float(value)


def _cast_optional_int(value: object) -> int | None:
    if value is None:
        return None

    return int(value)


def _cast_optional_bool(value: object) -> bool | None:
    if value is None:
        return None

    return bool(value)
