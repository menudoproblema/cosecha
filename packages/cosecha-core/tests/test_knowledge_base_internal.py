from __future__ import annotations

import asyncio
import queue
import sqlite3

from collections import OrderedDict
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from cosecha.core.config import Config
from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord
from cosecha.core.domain_events import (
    DefinitionMaterializedEvent,
    DomainEventMetadata,
    EngineSnapshotUpdatedEvent,
    KnowledgeIndexedEvent,
    KnowledgeInvalidatedEvent,
    LogChunkEvent,
    NodeRetryingEvent,
    RegistryKnowledgeIndexedEvent,
    ResourceLifecycleEvent,
    ResourceReadinessTransitionEvent,
    SessionFinishedEvent,
    SessionStartedEvent,
    TestFinishedEvent as FinishedEvent,
    WorkerDegradedEvent,
    WorkerRecoveredEvent,
    TestKnowledgeIndexedEvent as IndexedEvent,
    TestKnowledgeInvalidatedEvent as InvalidatedEvent,
)
from cosecha.core.knowledge_base import (
    KNOWLEDGE_BASE_PATH,
    LEGACY_KNOWLEDGE_BASE_PATH,
    DefinitionKnowledge,
    DefinitionKnowledgeQuery,
    DomainEventQuery,
    IdempotencyWindowPolicy,
    InMemoryKnowledgeBase,
    KnowledgeBaseDomainEventSink,
    LiveStepKnowledge,
    LiveTestKnowledge,
    PersistentKnowledgeBase,
    ReadOnlyPersistentKnowledgeBase,
    ResourceKnowledge,
    ResourceKnowledgeQuery,
    SessionArtifactQuery,
    WorkerKnowledge,
    TestKnowledge as InternalTestKnowledge,
    TestKnowledge as KnowledgeTestRecord,
    TestKnowledgeQuery as KnowledgeTestQuery,
    _cast_optional_float,
    _cast_optional_int,
    _cast_optional_str,
    _deserialize_definition_descriptors,
    _deserialize_registry_entries,
    _filter_definition_knowledge,
    _filter_domain_events,
    _filter_registry_knowledge,
    _filter_resource_knowledge,
    _filter_session_artifacts,
    _filter_test_knowledge,
    _load_definition_knowledge,
    _load_domain_events,
    _load_registry_knowledge,
    _load_resource_knowledge,
    _load_session_artifacts,
    _load_test_knowledge,
    _migrate_schema_v10_to_v11,
    _migrate_schema_v11_to_v12,
    _migrate_schema_v12_to_v13,
    _migrate_schema_v13_to_v14,
    _migrate_schema_v14_to_v15,
    _migrate_schema_v9_to_v10,
    _migrate_schema_v8_to_v9,
    _serialize_registry_entries,
    iter_knowledge_base_file_paths,
    resolve_knowledge_base_path,
)
from cosecha.core.knowledge_test_descriptor import (
    TestDescriptorKnowledge as DescriptorKnowledge,
)
from cosecha.core.registry_knowledge import (
    RegistryKnowledgeEntry,
    RegistryKnowledgeQuery,
    RegistryKnowledgeSnapshot,
)
from cosecha.core.session_artifacts import SessionArtifact


if TYPE_CHECKING:
    pass


def _metadata(
    *,
    sequence_number: int,
    session_id: str = 'session-1',
    plan_id: str = 'plan-1',
    node_stable_id: str = 'stable-1',
    node_id: str = 'node-1',
    worker_id: int | None = 1,
) -> DomainEventMetadata:
    return DomainEventMetadata(
        sequence_number=sequence_number,
        session_id=session_id,
        plan_id=plan_id,
        node_stable_id=node_stable_id,
        node_id=node_id,
        worker_id=worker_id,
    )


def test_filter_helpers_and_registry_entry_serialization() -> None:
    tests = (
        KnowledgeTestRecord(
            node_id='node-1',
            node_stable_id='stable-1',
            engine_name='gherkin',
            test_name='A',
            test_path='tests/a.feature',
            status='passed',
            failure_kind='assertion_error',
            plan_id='plan-1',
        ),
        KnowledgeTestRecord(
            node_id='node-2',
            node_stable_id='stable-2',
            engine_name='pytest',
            test_name='B',
            test_path='tests/b.py',
            status='failed',
            plan_id='plan-2',
        ),
    )
    filtered_tests = _filter_test_knowledge(
        tests,
        KnowledgeTestQuery(
            engine_name='gherkin',
            test_path='tests/a.feature',
            status='passed',
            failure_kind='assertion_error',
            node_stable_id='stable-1',
            plan_id='plan-1',
            limit=1,
        ),
    )
    assert len(filtered_tests) == 1

    definitions = (
        DefinitionKnowledge(
            engine_name='gherkin',
            file_path='steps.py',
            definition_count=1,
            discovery_mode='ast',
            descriptors=(
                DefinitionKnowledgeRecord(
                    source_line=10,
                    function_name='step_impl',
                ),
            ),
        ),
    )
    filtered_definitions = _filter_definition_knowledge(
        definitions,
        DefinitionKnowledgeQuery(
            engine_name='gherkin',
            file_path='steps.py',
            step_type='given',
            step_text='some step',
            discovery_mode='ast',
            include_invalidated=False,
            limit=1,
        ),
    )
    assert filtered_definitions == ()

    resources = (
        ResourceKnowledge(name='mongo', scope='worker', last_test_id='test-1'),
        ResourceKnowledge(name='redis', scope='run', last_test_id='test-2'),
    )
    filtered_resources = _filter_resource_knowledge(
        resources,
        ResourceKnowledgeQuery(
            name='mongo',
            scope='worker',
            last_test_id='test-1',
            limit=1,
        ),
    )
    assert len(filtered_resources) == 1

    snapshots = (
        RegistryKnowledgeSnapshot(
            engine_name='gherkin',
            module_spec='module',
            package_hash='hash',
            layout_key='layout',
            loader_schema_version='schema-v1',
            entries=(),
            source_count=1,
            created_at=1.0,
        ),
    )
    filtered_snapshots = _filter_registry_knowledge(
        snapshots,
        RegistryKnowledgeQuery(
            engine_name='gherkin',
            module_spec='module',
            package_hash='hash',
            layout_key='layout',
            loader_schema_version='schema-v1',
            limit=1,
        ),
    )
    assert len(filtered_snapshots) == 1

    events = (
        SessionStartedEvent(
            root_path='.',
            concurrency=1,
            metadata=_metadata(sequence_number=1),
        ),
    )
    filtered_events = _filter_domain_events(
        events,
        DomainEventQuery(
            event_type='session.started',
            session_id='session-1',
            plan_id='plan-1',
            node_stable_id='stable-1',
            after_sequence_number=0,
            limit=1,
        ),
    )
    assert len(filtered_events) == 1

    entry = RegistryKnowledgeEntry(
        layout_name='layout',
        module_import_path='module.path',
        qualname='Qual.name',
        class_name='StepClass',
    )
    encoded = _serialize_registry_entries((entry,))
    assert _deserialize_registry_entries(None) == ()
    assert _deserialize_registry_entries('invalid json') == ()
    decoded = _deserialize_registry_entries(
        f'[{entry.to_dict()}, 42]'.replace("'", '"'),
    )
    assert len(decoded) == 1
    assert isinstance(encoded, str)


def test_in_memory_live_snapshot_truncation_and_artifact_pruning() -> None:
    knowledge = InMemoryKnowledgeBase()
    knowledge._record_domain_event(
        SessionStartedEvent(
            root_path='.',
            concurrency=1,
            metadata=DomainEventMetadata(),
        ),
    )
    for sequence in range(1, 280):
        knowledge._record_domain_event(
            LogChunkEvent(
                message=f'line-{sequence}',
                level='info',
                logger_name='tests',
                metadata=_metadata(sequence_number=sequence),
            ),
        )

    snapshot = knowledge.live_snapshot()
    assert snapshot.truncated_event_count > 0
    assert snapshot.truncated_log_chunk_count > 0

    artifact_policy = SessionArtifact(
        session_id='session-a',
        root_path='.',
        workspace_fingerprint='ws-1',
        config_snapshot=Config(root_path=Path('.')).snapshot(),
        capability_snapshots=(),
        recorded_at=1.0,
    ).persistence_policy

    artifact_a = SessionArtifact(
        session_id='session-a',
        root_path='.',
        workspace_fingerprint='ws-1',
        config_snapshot=Config(root_path=Path('.')).snapshot(),
        capability_snapshots=(),
        recorded_at=10.0,
        persistence_policy=artifact_policy,
    )
    artifact_b = SessionArtifact(
        session_id='session-b',
        root_path='.',
        workspace_fingerprint='ws-1',
        config_snapshot=Config(root_path=Path('.')).snapshot(),
        capability_snapshots=(),
        recorded_at=20.0,
        persistence_policy=artifact_policy,
    )
    knowledge.store_session_artifact(artifact_a)
    knowledge.store_session_artifact(artifact_b)
    assert knowledge.query_session_artifacts(SessionArtifactQuery(limit=5))


def test_persistent_knowledge_base_low_level_loaders_and_close_paths(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / '.cosecha' / 'kb.db'
    knowledge = PersistentKnowledgeBase(db_path)
    metadata = _metadata(sequence_number=1)

    knowledge.apply(
        ResourceLifecycleEvent(
            action='acquired',
            name='mongo',
            scope='worker',
            test_id='test-1',
            external_handle='mongo-1',
            metadata=metadata,
        ),
    )
    knowledge.apply(
        ResourceReadinessTransitionEvent(
            name='mongo',
            scope='worker',
            status='ready',
            reason='ok',
            metadata=_metadata(sequence_number=2),
        ),
    )
    knowledge.apply(
        RegistryKnowledgeIndexedEvent(
            engine_name='gherkin',
            module_spec='module.spec',
            package_hash='package-hash',
            layout_key='layout-key',
            loader_schema_version='schema-v1',
            entries=(
                RegistryKnowledgeEntry(
                    layout_name='layout',
                    module_import_path='module.path',
                    qualname='Step.qualname',
                    class_name='StepClass',
                ),
            ),
            source_count=1,
            metadata=_metadata(sequence_number=3),
        ),
    )
    knowledge.store_session_artifact(
        SessionArtifact(
            session_id='session-1',
            root_path=str(tmp_path),
            workspace_fingerprint='ws-1',
            trace_id='trace-1',
            plan_id='plan-1',
            config_snapshot=Config(root_path=tmp_path).snapshot(),
            capability_snapshots=(),
            recorded_at=42.0,
        ),
    )

    resources = _load_resource_knowledge(
        knowledge._connection,
        ResourceKnowledgeQuery(
            name='mongo',
            scope='worker',
            last_test_id='test-1',
            limit=1,
        ),
    )
    assert len(resources) == 1

    registry_items = _load_registry_knowledge(
        knowledge._connection,
        RegistryKnowledgeQuery(
            engine_name='gherkin',
            module_spec='module.spec',
            package_hash='package-hash',
            layout_key='layout-key',
            loader_schema_version='schema-v1',
            limit=1,
        ),
    )
    assert len(registry_items) == 1

    artifacts = _load_session_artifacts(
        knowledge._connection,
        SessionArtifactQuery(
            session_id='session-1',
            trace_id='trace-1',
            limit=1,
        ),
    )
    assert len(artifacts) == 1

    events = _load_domain_events(
        knowledge._connection,
        DomainEventQuery(
            event_type='resource.lifecycle',
            session_id='session-1',
            plan_id='plan-1',
            node_stable_id='stable-1',
            after_sequence_number=0,
            limit=1,
        ),
    )
    assert len(events) == 1

    knowledge.interrupt()
    knowledge.force_close()
    knowledge.force_close()


def test_schema_migration_v14_to_v15_updates_optional_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    connection.execute(
        'CREATE TABLE session_state (id INTEGER PRIMARY KEY, root_path TEXT)',
    )
    connection.execute(
        'CREATE TABLE session_artifacts (session_id TEXT PRIMARY KEY)',
    )

    _migrate_schema_v14_to_v15(connection)

    session_columns = {
        row['name']
        for row in connection.execute('PRAGMA table_info(session_state)')
    }
    artifacts_columns = {
        row['name']
        for row in connection.execute('PRAGMA table_info(session_artifacts)')
    }
    assert 'workspace_fingerprint' in session_columns
    assert 'workspace_fingerprint' in artifacts_columns

    knowledge = PersistentKnowledgeBase.__new__(PersistentKnowledgeBase)
    knowledge._connection = connection  # type: ignore[attr-defined]
    reset_calls: list[str] = []
    monkeypatch.setattr(
        PersistentKnowledgeBase,
        '_reset_schema',
        lambda self: reset_calls.append('reset'),
    )
    knowledge._migrate_schema('invalid')
    knowledge._migrate_schema('1')
    assert reset_calls


def test_cast_helpers_and_resolve_knowledge_base_path_variants(
    tmp_path: Path,
) -> None:
    assert _cast_optional_str(None) is None
    assert _cast_optional_str(12) == '12'
    assert _cast_optional_int(None) is None
    assert _cast_optional_int('7') == 7
    assert _cast_optional_float(None) is None
    assert _cast_optional_float('2.5') == 2.5

    root_path = tmp_path / 'root'
    root_path.mkdir(parents=True, exist_ok=True)
    storage_root = tmp_path / 'storage'
    storage_root.mkdir(parents=True, exist_ok=True)

    storage_db_path = storage_root / 'kb.db'
    storage_db_path.write_text('db', encoding='utf-8')
    assert (
        resolve_knowledge_base_path(
            root_path,
            knowledge_storage_root=storage_root,
        )
        == storage_db_path
    )

    storage_db_path.unlink()
    legacy_current_path = root_path / KNOWLEDGE_BASE_PATH
    legacy_current_path.parent.mkdir(parents=True, exist_ok=True)
    for file_path in iter_knowledge_base_file_paths(legacy_current_path):
        file_path.write_text('legacy', encoding='utf-8')

    assert (
        resolve_knowledge_base_path(
            root_path,
            knowledge_storage_root=storage_root,
            migrate_legacy=False,
        )
        == legacy_current_path
    )

    # Si el archivo destino ya existe no se sobreescribe durante la migracion.
    storage_db_path.write_text('already-there', encoding='utf-8')
    moved_path = resolve_knowledge_base_path(
        root_path,
        knowledge_storage_root=storage_root,
    )
    assert moved_path == storage_db_path

    legacy_default_path = root_path / LEGACY_KNOWLEDGE_BASE_PATH
    legacy_default_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_default_path.write_text('legacy-default', encoding='utf-8')
    migrated_default = resolve_knowledge_base_path(root_path)
    assert migrated_default == root_path / KNOWLEDGE_BASE_PATH


def test_in_memory_reconcile_and_apply_knowledge_events() -> None:
    knowledge = InMemoryKnowledgeBase()
    active_test = InternalTestKnowledge(
        node_id='node-a',
        node_stable_id='stable-a',
        engine_name='engine',
        test_name='A',
        test_path='tests/a.feature',
        session_id='session-a',
        started_at=1.0,
    )
    stale_session_test = InternalTestKnowledge(
        node_id='node-b',
        node_stable_id='stable-b',
        engine_name='engine',
        test_name='B',
        test_path='tests/b.feature',
        session_id='session-b',
        scheduled_at=1.0,
    )
    knowledge._tests = {
        'stable-a': active_test,
        'stable-b': stale_session_test,
    }
    knowledge._active_tests = {
        'stable-a': knowledge.live_snapshot().running_tests[0]
        if knowledge.live_snapshot().running_tests
        else KnowledgeTestRecord(
            node_id='node-a',
            node_stable_id='stable-a',
            engine_name='engine',
            test_name='A',
            test_path='tests/a.feature',
            session_id='session-a',
            started_at=1.0,
        ),  # type: ignore[assignment]
    }
    knowledge._live_current_steps = {'stable-a': object()}  # type: ignore[assignment]
    knowledge._live_engine_snapshots = {'stable-a': object()}  # type: ignore[assignment]

    knowledge._reconcile_finished_session(
        session_id='session-a',
        finished_at=5.0,
    )
    assert knowledge._tests['stable-a'].status == 'error'
    assert knowledge._tests['stable-b'].finished_at is None

    descriptor = DescriptorKnowledge(
        stable_id='stable-a',
        test_name='A',
        file_path='tests/a.feature',
        source_line=3,
    )
    knowledge._tests['stale-in-file'] = InternalTestKnowledge(
        node_id='node-c',
        node_stable_id='stale-in-file',
        engine_name='engine',
        test_name='C',
        test_path='tests/a.feature',
    )
    knowledge._apply_test_knowledge_event(
        IndexedEvent(
            engine_name='engine',
            file_path='tests/a.feature',
            tests=(descriptor,),
            discovery_mode='ast',
            knowledge_version='1',
            metadata=DomainEventMetadata(),
        ),
    )
    assert 'stable-a' in knowledge._tests
    assert 'stale-in-file' not in knowledge._tests

    knowledge._live_engine_snapshots['stable-a'] = object()  # type: ignore[assignment]
    knowledge._apply_test_knowledge_event(
        InvalidatedEvent(
            engine_name='engine',
            file_path='tests/a.feature',
            reason='changed',
            knowledge_version='2',
            metadata=DomainEventMetadata(),
        ),
    )
    assert 'stable-a' not in knowledge._tests

    definition_event = KnowledgeIndexedEvent(
        engine_name='engine',
        file_path='steps.py',
        definition_count=1,
        discovery_mode='ast',
        knowledge_version='1',
        descriptors=(
            DefinitionKnowledgeRecord(
                source_line=8,
                function_name='step_impl',
            ),
        ),
        metadata=DomainEventMetadata(),
    )
    knowledge._apply_definition_event(definition_event)
    knowledge._apply_definition_event(
        KnowledgeInvalidatedEvent(
            engine_name='engine',
            file_path='steps.py',
            reason='changed',
            knowledge_version='2',
            metadata=DomainEventMetadata(),
        ),
    )
    knowledge._apply_definition_event(
        DefinitionMaterializedEvent(
            engine_name='engine',
            file_path='steps.py',
            definition_count=2,
            discovery_mode='runtime',
            metadata=DomainEventMetadata(),
        ),
    )
    assert knowledge._definitions[('engine', 'steps.py')].materialized_count >= 1


def test_in_memory_duplicate_event_window_and_schema_migrations() -> None:
    knowledge = InMemoryKnowledgeBase()
    knowledge._idempotency_policy = IdempotencyWindowPolicy(
        max_entries=1,
        max_age_seconds=0.0,
    )
    knowledge._processed_idempotency_keys = OrderedDict()

    event = SessionStartedEvent(
        root_path='.',
        concurrency=1,
        metadata=DomainEventMetadata(idempotency_key='k1'),
    )
    assert knowledge._should_skip_duplicate_event(event) is False
    assert knowledge._should_skip_duplicate_event(event) is True

    event_no_key = SessionStartedEvent(
        root_path='.',
        concurrency=1,
        metadata=DomainEventMetadata(sequence_number=2),
    )
    assert knowledge._should_skip_duplicate_event(event_no_key) is False
    assert knowledge._should_skip_duplicate_event(event_no_key) is True

    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    connection.execute('CREATE TABLE session_artifacts (session_id TEXT PRIMARY KEY)')
    _migrate_schema_v8_to_v9(connection)
    _migrate_schema_v10_to_v11(connection)
    _migrate_schema_v12_to_v13(connection)
    _migrate_schema_v13_to_v14(connection)

    connection.execute(
        'CREATE TABLE tests (node_stable_id TEXT PRIMARY KEY)',
    )
    connection.execute(
        'CREATE TABLE resources (name TEXT, scope TEXT)',
    )
    _migrate_schema_v12_to_v13(connection)
    _migrate_schema_v13_to_v14(connection)

    test_columns = {
        row['name'] for row in connection.execute('PRAGMA table_info(tests)')
    }
    resource_columns = {
        row['name']
        for row in connection.execute('PRAGMA table_info(resources)')
    }
    assert 'failure_kind' in test_columns
    assert 'readiness_status' in resource_columns
    assert 'readiness_reason' in resource_columns


def test_persistent_loaders_readonly_queries_and_sink_paths(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / '.cosecha' / 'kb.db'
    knowledge = PersistentKnowledgeBase(db_path)

    metadata = DomainEventMetadata(
        sequence_number=1,
        session_id='session-1',
        plan_id='plan-1',
        node_stable_id='stable-1',
    )
    knowledge.apply(
        FinishedEvent(
            node_id='node-1',
            node_stable_id='stable-1',
            engine_name='engine',
            test_name='test_one',
            test_path='tests/one.feature',
            status='failed',
            duration=0.1,
            failure_kind='runtime',
            error_code='E1',
            metadata=metadata,
        ),
    )
    knowledge.apply(
        KnowledgeIndexedEvent(
            engine_name='engine',
            file_path='steps.py',
            definition_count=1,
            discovery_mode='ast',
            knowledge_version='1',
            descriptors=(
                DefinitionKnowledgeRecord(
                    source_line=4,
                    function_name='step_one',
                ),
            ),
            metadata=DomainEventMetadata(),
        ),
    )
    knowledge.store_session_artifact(
        SessionArtifact(
            session_id='session-1',
            root_path=str(tmp_path),
            workspace_fingerprint='ws',
            config_snapshot=Config(root_path=tmp_path).snapshot(),
            capability_snapshots=(),
            recorded_at=1.0,
        ),
    )

    loaded_tests = _load_test_knowledge(
        knowledge._connection,
        KnowledgeTestQuery(
            engine_name='engine',
            test_path='tests/one.feature',
            status='failed',
            failure_kind='runtime',
            node_stable_id='stable-1',
            plan_id='plan-1',
            limit=1,
        ),
    )
    assert len(loaded_tests) == 1

    loaded_definitions = _load_definition_knowledge(
        knowledge._connection,
        DefinitionKnowledgeQuery(
            engine_name='engine',
            file_path='steps.py',
            discovery_mode='ast',
            include_invalidated=False,
            step_type='given',
            step_text='x',
            limit=1,
        ),
    )
    assert isinstance(loaded_definitions, tuple)
    assert _deserialize_definition_descriptors(None) == ()
    assert _deserialize_definition_descriptors('invalid') == ()

    assert knowledge.query_resources(ResourceKnowledgeQuery()) == ()
    assert knowledge.query_session_artifacts(SessionArtifactQuery(limit=1))
    knowledge.close()

    read_only = ReadOnlyPersistentKnowledgeBase(db_path)
    assert read_only.has_test_path('tests/one.feature')
    assert read_only.has_test_path('tests/one.feature', engine_name='engine')
    assert read_only.query_resources(ResourceKnowledgeQuery()) == ()
    read_only.close()

    connection = sqlite3.connect(tmp_path / 'invalid.db')
    connection.execute(
        'CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)',
    )
    connection.execute(
        'INSERT INTO meta (key, value) VALUES (?, ?)',
        ('schema_version', '0'),
    )
    connection.commit()
    connection.close()
    with pytest.raises(ValueError, match='schema mismatch'):
        ReadOnlyPersistentKnowledgeBase(tmp_path / 'invalid.db')

    sink = KnowledgeBaseDomainEventSink(InMemoryKnowledgeBase())

    async def _close_sink() -> None:
        await sink.emit(
            SessionStartedEvent(
                root_path='.',
                concurrency=1,
                metadata=DomainEventMetadata(),
            ),
        )
        await sink.close()

    import asyncio

    asyncio.run(_close_sink())


def test_knowledge_models_roundtrip_additional_paths() -> None:
    test_record = KnowledgeTestRecord(
        node_id='node-1',
        node_stable_id='stable-1',
        engine_name='engine',
        test_name='test',
        test_path='tests/test.feature',
        selection_labels=('fast',),
        source_line=10,
        discovery_mode='ast',
        knowledge_version='1',
        content_hash='hash',
        indexed_at=1.0,
        invalidated_at=2.0,
        invalidation_reason='stale',
        worker_slot=1,
        scheduled_at=3.0,
        max_attempts=2,
        timeout_seconds=4.0,
        retry_count=1,
        failure_kind='runtime',
        last_error_code='E1',
    )
    assert KnowledgeTestRecord.from_dict(test_record.to_dict()) == test_record

    resource_record = ResourceKnowledge(
        name='mongo',
        scope='worker',
        readiness_status='ready',
        readiness_reason='ok',
        last_test_id='test-1',
    )
    assert ResourceKnowledge.from_dict(resource_record.to_dict()) == resource_record

    worker = WorkerKnowledge(worker_id=1, status='running', session_id='session-1')
    assert WorkerKnowledge.from_dict(worker.to_dict()) == worker

    definition = DefinitionKnowledge(
        engine_name='engine',
        file_path='steps.py',
        definition_count=1,
        discovery_mode='ast',
        descriptors=(
            DefinitionKnowledgeRecord(
                source_line=10,
                function_name='step_impl',
            ),
        ),
    )
    assert DefinitionKnowledge.from_dict(definition.to_dict()) == definition
    assert definition.matching_descriptors() == definition.descriptors

    live_step = LiveStepKnowledge(
        step_type='given',
        step_keyword='Given',
        step_text='a step',
        status='passed',
        source_line=11,
        message='ok',
        updated_at=2.0,
    )
    live_test = LiveTestKnowledge(
        node_id='node-1',
        node_stable_id='stable-1',
        engine_name='engine',
        test_name='test',
        status='running',
        started_at=1.0,
        worker_slot=1,
        current_step=live_step,
    )
    assert LiveStepKnowledge.from_dict(live_step.to_dict()) == live_step
    assert LiveTestKnowledge.from_dict(live_test.to_dict()) == live_test


def test_in_memory_additional_reconcile_and_query_paths(tmp_path: Path) -> None:
    kb = InMemoryKnowledgeBase()
    kb._idempotency_policy = IdempotencyWindowPolicy(max_entries=1)

    kb._apply_session_event(
        SessionFinishedEvent(
            has_failures=False,
            metadata=DomainEventMetadata(session_id='session-1'),
        ),
    )

    definition = DefinitionKnowledge(
        engine_name='engine',
        file_path='steps.py',
        definition_count=1,
        discovery_mode='ast',
    )
    snapshot = RegistryKnowledgeSnapshot(
        engine_name='engine',
        module_spec='module',
        package_hash='hash',
        layout_key='layout',
        loader_schema_version='v1',
        entries=(),
        source_count=1,
        created_at=1.0,
    )
    resource = ResourceKnowledge(name='mongo', scope='worker')
    kb._definitions[(definition.engine_name, definition.file_path)] = definition
    kb._registry_snapshots[('engine', 'module', 'hash', 'layout', 'v1')] = snapshot
    kb._resources[(resource.name, resource.scope)] = resource

    kb.apply(
        SessionStartedEvent(
            root_path='.',
            concurrency=1,
            metadata=DomainEventMetadata(
                sequence_number=1,
                idempotency_key='one',
            ),
        ),
    )
    kb.apply(
        SessionStartedEvent(
            root_path='.',
            concurrency=1,
            metadata=DomainEventMetadata(
                sequence_number=2,
                idempotency_key='two',
            ),
        ),
    )
    assert isinstance(kb.query_domain_events(DomainEventQuery(limit=1)), tuple)
    assert len(kb._processed_idempotency_keys) <= 1

    assert kb.query_definitions(DefinitionKnowledgeQuery()) == (definition,)
    assert kb.query_registry_items(RegistryKnowledgeQuery()) == (snapshot,)
    assert kb.query_resources(ResourceKnowledgeQuery()) == (resource,)

    artifact_old = SessionArtifact(
        session_id='old',
        root_path=str(tmp_path),
        workspace_fingerprint='ws',
        config_snapshot=Config(root_path=tmp_path).snapshot(),
        capability_snapshots=(),
        recorded_at=1.0,
    )
    artifact_new = SessionArtifact(
        session_id='new',
        root_path=str(tmp_path),
        workspace_fingerprint='ws',
        config_snapshot=Config(root_path=tmp_path).snapshot(),
        capability_snapshots=(),
        recorded_at=100.0,
    )
    kb._session_artifacts = OrderedDict(
        ((artifact_old.session_id, artifact_old), (artifact_new.session_id, artifact_new)),
    )
    kb._prune_session_artifacts(
        'ws',
        max_artifacts=1,
        max_age_seconds=10.0,
        recorded_at=100.0,
    )
    assert tuple(kb._session_artifacts.keys()) == ('new',)

    kb._active_tests['mismatch'] = LiveTestKnowledge(
        node_id='node-1',
        node_stable_id='mismatch',
        engine_name='engine',
        test_name='mismatch',
        status='running',
    )
    kb._tests['mismatch'] = KnowledgeTestRecord(
        node_id='node-1',
        node_stable_id='mismatch',
        engine_name='engine',
        test_name='mismatch',
        test_path='tests/mismatch.feature',
        session_id='other-session',
        started_at=1.0,
    )
    kb._active_tests['orphan'] = LiveTestKnowledge(
        node_id='node-2',
        node_stable_id='orphan',
        engine_name='engine',
        test_name='orphan',
        status='running',
    )
    kb._tests['skip-unstarted'] = KnowledgeTestRecord(
        node_id='node-3',
        node_stable_id='skip-unstarted',
        engine_name='engine',
        test_name='skip',
        test_path='tests/skip.feature',
        session_id='session-1',
    )
    kb._tests['finish-me'] = KnowledgeTestRecord(
        node_id='node-4',
        node_stable_id='finish-me',
        engine_name='engine',
        test_name='finish',
        test_path='tests/finish.feature',
        session_id='session-1',
        scheduled_at=1.0,
    )
    kb._reconcile_finished_session(session_id='session-1', finished_at=5.0)
    assert kb._tests['finish-me'].status == 'error'

    kb.apply(
        NodeRetryingEvent(
            node_id='node-4',
            node_stable_id='finish-me',
            attempt=2,
            failure_kind='runtime',
            error_code='E2',
            metadata=DomainEventMetadata(session_id='session-1', plan_id='plan-1'),
        ),
    )
    assert kb._tests['finish-me'].retry_count >= 1

    kb.apply(
        EngineSnapshotUpdatedEvent(
            engine_name='engine',
            snapshot_kind='runtime',
            payload={'phase': 'call'},
            metadata=DomainEventMetadata(node_stable_id=None),
        ),
    )
    kb.apply(
        LogChunkEvent(
            message='hello',
            level='info',
            logger_name='runner',
            metadata=DomainEventMetadata(sequence_number=3),
        ),
    )
    kb.apply(
        WorkerDegradedEvent(
            worker_id=1,
            reason='slow',
            metadata=DomainEventMetadata(session_id='session-1', plan_id='plan-1'),
        ),
    )
    kb.apply(
        WorkerRecoveredEvent(
            worker_id=1,
            metadata=DomainEventMetadata(session_id='session-1', plan_id='plan-1'),
        ),
    )
    assert kb._workers[1].status == 'recovered'


def test_persistent_private_delete_and_interrupt_paths(tmp_path: Path) -> None:
    db_path = tmp_path / '.cosecha' / 'kb.db'
    kb = PersistentKnowledgeBase(db_path)
    assert kb.schema_version >= 1

    duplicate = SessionStartedEvent(
        root_path='.',
        concurrency=1,
        metadata=DomainEventMetadata(idempotency_key='dup'),
    )
    kb.apply(duplicate)
    kb.apply(duplicate)
    kb.persist_events(())
    kb._persist_session()
    kb._persist_latest_plan()
    kb._persist_test('missing')
    kb._persist_resource('missing', 'worker')
    kb._persist_worker(999)
    kb._persist_definition('engine', 'missing.py')
    kb._persist_registry_snapshot('engine', 'module', 'hash', 'layout', 'v1')
    kb._persist_session_artifact('missing-session')

    with pytest.raises(ValueError, match='without sequence_number'):
        kb._persist_domain_event_record(
            SessionStartedEvent(
                root_path='.',
                concurrency=1,
                metadata=DomainEventMetadata(),
            ),
        )

    assert isinstance(kb.query_domain_events(DomainEventQuery()), tuple)
    assert kb.query_registry_items(RegistryKnowledgeQuery()) == ()
    kb.close()
    kb.interrupt()


def test_sink_close_and_worker_failure_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persistent = PersistentKnowledgeBase(tmp_path / '.cosecha' / 'kb.db')
    sink = KnowledgeBaseDomainEventSink(persistent)

    sink._run_persistence_worker()

    sink._persistence_queue = queue.Queue()
    sink._persistence_queue.put(
        SessionStartedEvent(
            root_path='.',
            concurrency=1,
            metadata=DomainEventMetadata(sequence_number=1),
        ),
    )
    sink._persistence_queue.put(None)
    monkeypatch.setattr(
        PersistentKnowledgeBase,
        'persist_events',
        lambda self, _events: (_ for _ in ()).throw(RuntimeError('persist failed')),
    )
    sink._run_persistence_worker()
    with pytest.raises(RuntimeError, match='worker failed'):
        sink._raise_worker_error()

    class _ThreadDone:
        def join(self, _timeout: float) -> None:
            return None

        def is_alive(self) -> bool:
            return False

    sink = KnowledgeBaseDomainEventSink(persistent)
    sink._worker_thread = _ThreadDone()  # type: ignore[assignment]
    sink._persistence_queue = queue.Queue()
    sink._worker_error = RuntimeError('background boom')
    with pytest.raises(RuntimeError, match='worker failed'):
        asyncio.run(sink.close())

    class _ThreadAlive:
        def join(self, _timeout: float) -> None:
            return None

        def is_alive(self) -> bool:
            return True

    sink = KnowledgeBaseDomainEventSink(persistent)
    sink._worker_thread = _ThreadAlive()  # type: ignore[assignment]
    sink._persistence_queue = queue.Queue()
    original_wait_for = asyncio.wait_for
    wait_calls = {'count': 0}

    async def _patched_wait_for(awaitable, timeout):
        wait_calls['count'] += 1
        if wait_calls['count'] in {1, 2}:
            closer = getattr(awaitable, 'close', None)
            if callable(closer):
                closer()
            raise TimeoutError
        return await original_wait_for(awaitable, timeout)

    monkeypatch.setattr(asyncio, 'wait_for', _patched_wait_for)
    monkeypatch.setattr(
        PersistentKnowledgeBase,
        'force_close',
        lambda self: (_ for _ in ()).throw(RuntimeError('force close failed')),
    )
    with pytest.raises(ExceptionGroup, match='shutdown failed'):
        asyncio.run(sink.close())
    persistent.close()


def test_read_only_helpers_filter_and_migration_paths(tmp_path: Path) -> None:
    db_path = tmp_path / '.cosecha' / 'kb.db'
    persistent = PersistentKnowledgeBase(db_path)
    persistent.close()

    read_only = ReadOnlyPersistentKnowledgeBase(db_path)
    assert read_only.db_path == db_path
    assert read_only.snapshot().tests == ()
    assert read_only.live_snapshot().recent_events == ()
    assert read_only.query_domain_events(DomainEventQuery()) == ()
    assert read_only.query_registry_items(RegistryKnowledgeQuery()) == ()
    read_only.close()
    read_only.close()

    artifact = SessionArtifact(
        session_id='s1',
        trace_id='t1',
        root_path='.',
        workspace_fingerprint='ws',
        config_snapshot=Config(root_path=tmp_path).snapshot(),
        capability_snapshots=(),
        recorded_at=1.0,
    )
    assert _filter_session_artifacts(
        (artifact,),
        SessionArtifactQuery(trace_id='missing'),
    ) == ()

    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    connection.execute(
        'CREATE TABLE session_artifacts ('
        'session_id TEXT PRIMARY KEY, '
        'trace_id TEXT, '
        'root_path TEXT NOT NULL, '
        'workspace_fingerprint TEXT, '
        'plan_id TEXT, '
        'config_snapshot_json TEXT NOT NULL, '
        'capability_snapshots_json TEXT NOT NULL, '
        'plan_explanation_json TEXT, '
        'timing_json TEXT, '
        'has_failures INTEGER'
        ')',
    )
    _migrate_schema_v8_to_v9(connection)
    _migrate_schema_v9_to_v10(connection)
    _migrate_schema_v10_to_v11(connection)
    _migrate_schema_v11_to_v12(connection)


def test_knowledge_base_gap_helpers_and_pruning_paths(tmp_path: Path) -> None:
    assert _cast_optional_str(None) is None
    assert _cast_optional_str(0) == '0'
    assert _cast_optional_int(None) is None
    assert _cast_optional_int('9') == 9
    assert _cast_optional_float(None) is None
    assert _cast_optional_float('1.25') == 1.25

    root_path = tmp_path / 'root'
    root_path.mkdir(parents=True, exist_ok=True)
    storage_root = tmp_path / 'storage'
    storage_root.mkdir(parents=True, exist_ok=True)
    legacy_path = root_path / LEGACY_KNOWLEDGE_BASE_PATH
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_path.write_text('legacy', encoding='utf-8')
    target_path = storage_root / 'kb.db'
    Path(f'{target_path}-wal').write_text('already-open', encoding='utf-8')

    resolved = resolve_knowledge_base_path(
        root_path,
        knowledge_storage_root=storage_root,
    )
    assert resolved == target_path
    assert target_path.exists()

    knowledge = InMemoryKnowledgeBase()
    base_artifact = SessionArtifact(
        session_id='session-1',
        root_path=str(root_path),
        workspace_fingerprint='ws',
        config_snapshot=Config(root_path=root_path).snapshot(),
        capability_snapshots=(),
        recorded_at=1.0,
    )
    newer_artifact = SessionArtifact(
        session_id='session-2',
        root_path=str(root_path),
        workspace_fingerprint='ws',
        config_snapshot=Config(root_path=root_path).snapshot(),
        capability_snapshots=(),
        recorded_at=2.0,
    )
    knowledge._session_artifacts = {
        base_artifact.session_id: base_artifact,
        newer_artifact.session_id: newer_artifact,
    }
    knowledge._prune_session_artifacts(
        'ws',
        max_artifacts=1,
        max_age_seconds=None,
        recorded_at=2.0,
    )
    assert tuple(knowledge._session_artifacts.keys()) == ('session-2',)

    knowledge._apply_live_log_chunk_event(
        LogChunkEvent(
            message='noop',
            level='info',
            logger_name='runner',
            metadata=DomainEventMetadata(sequence_number=1),
        ),
    )

    class _FlakyMetadata:
        def __init__(self) -> None:
            self.idempotency_key = None
            self._reads = 0

        @property
        def sequence_number(self):  # type: ignore[override]
            self._reads += 1
            if self._reads == 1:
                return 1
            return None

        @sequence_number.setter
        def sequence_number(self, value):  # type: ignore[override]
            del value

    class _FlakyEvent:
        def __init__(self) -> None:
            self.metadata = _FlakyMetadata()
            self.timestamp = 0.0

    assert knowledge._should_skip_duplicate_event(_FlakyEvent()) is False


def test_persistent_schema_migration_and_sink_start_gap_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cosecha.core import knowledge_base as knowledge_base_module

    original_migrate_schema = PersistentKnowledgeBase._migrate_schema
    original_reset_schema = PersistentKnowledgeBase._reset_schema

    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    connection.execute(
        'CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)',
    )
    connection.execute(
        "INSERT INTO meta (key, value) VALUES ('schema_version', '0')",
    )
    connection.commit()

    knowledge = PersistentKnowledgeBase.__new__(PersistentKnowledgeBase)
    knowledge._connection = connection  # type: ignore[attr-defined]
    migrate_calls: list[str] = []
    monkeypatch.setattr(
        PersistentKnowledgeBase,
        '_migrate_schema',
        lambda self, version: migrate_calls.append(version),
    )
    monkeypatch.setattr(PersistentKnowledgeBase, '_create_tables', lambda self: None)
    monkeypatch.setattr(
        PersistentKnowledgeBase,
        '_write_meta',
        lambda self, key, value: None,
    )
    knowledge._initialize_schema()
    assert migrate_calls == ['0']
    monkeypatch.setattr(
        PersistentKnowledgeBase,
        '_migrate_schema',
        original_migrate_schema,
    )

    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    knowledge = PersistentKnowledgeBase.__new__(PersistentKnowledgeBase)
    knowledge._connection = connection  # type: ignore[attr-defined]
    written_versions: list[str] = []
    monkeypatch.setattr(
        knowledge_base_module,
        '_SCHEMA_MIGRATIONS',
        {1: lambda _connection: _connection.execute('CREATE TABLE IF NOT EXISTS x (id INTEGER)')},
    )
    monkeypatch.setattr(
        PersistentKnowledgeBase,
        '_write_meta',
        lambda self, key, value: written_versions.append(f'{key}:{value}'),
    )
    monkeypatch.setattr(PersistentKnowledgeBase, '_reset_schema', lambda self: None)
    knowledge._migrate_schema('1')
    assert 'schema_version:2' in written_versions
    monkeypatch.setattr(
        PersistentKnowledgeBase,
        '_reset_schema',
        original_reset_schema,
    )

    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    connection.execute('CREATE TABLE tests (id INTEGER)')
    knowledge = PersistentKnowledgeBase.__new__(PersistentKnowledgeBase)
    knowledge._connection = connection  # type: ignore[attr-defined]
    knowledge._reset_schema()
    assert connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='tests'",
    ).fetchone() is None

    sink = KnowledgeBaseDomainEventSink(InMemoryKnowledgeBase())
    asyncio.run(sink.start())
