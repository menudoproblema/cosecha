from __future__ import annotations

import time

import pytest

from cosecha.core import domain_events
from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord
from cosecha.core.knowledge_test_descriptor import (
    TestDescriptorKnowledge as DescriptorKnowledge,
)
from cosecha.core.registry_knowledge import RegistryKnowledgeEntry


def test_domain_events_use_wall_clock_timestamps() -> None:
    before = time.time()
    event = domain_events.TestStartedEvent(
        node_id='node-1',
        node_stable_id='stable-node-1',
        engine_name='gherkin',
        test_name='Scenario: auth',
        test_path='features/auth.feature',
    )
    after = time.time()

    assert before <= event.timestamp <= after


def test_domain_event_metadata_defaults_correlation_to_event_id() -> None:
    metadata = domain_events.DomainEventMetadata()

    assert metadata.correlation_id == metadata.event_id


def test_definition_and_knowledge_events_roundtrip() -> None:
    event = domain_events.KnowledgeIndexedEvent(
        engine_name='gherkin',
        file_path='tests/steps/auth.py',
        definition_count=1,
        discovery_mode='ast',
        knowledge_version='v1',
        descriptors=(
            DefinitionKnowledgeRecord(
                source_line=7,
                function_name='given_user_logs_in',
                category='step',
                payload={'pattern': 'the user logs in'},
            ),
        ),
        metadata=domain_events.DomainEventMetadata(
            sequence_number=7,
            session_id='session-1',
            plan_id='plan-1',
            node_stable_id='node-stable-1',
            trace_id='trace-1',
        ),
    )

    restored = domain_events.deserialize_domain_event(
        domain_events.serialize_domain_event(event),
    )

    assert restored == event


def test_registry_and_test_knowledge_events_roundtrip() -> None:
    registry_event = domain_events.RegistryKnowledgeIndexedEvent(
        engine_name='gherkin',
        module_spec='tests.steps.auth',
        package_hash='pkg-1',
        layout_key='tests-root',
        loader_schema_version='gherkin_registry_loader_snapshot:v3',
        entries=(
            RegistryKnowledgeEntry(
                layout_name='tests-root',
                module_import_path='tests.steps.auth',
                qualname='AuthSteps',
                class_name='AuthSteps',
            ),
        ),
        source_count=1,
    )
    test_event = domain_events.TestKnowledgeIndexedEvent(
        engine_name='gherkin',
        file_path='features/auth.feature',
        tests=(
            DescriptorKnowledge(
                stable_id='stable-1',
                test_name='Scenario: auth',
                file_path='features/auth.feature',
                source_line=3,
                selection_labels=('api',),
            ),
        ),
        discovery_mode='ast',
        knowledge_version='v2',
    )

    restored_registry = domain_events.deserialize_domain_event(
        domain_events.serialize_domain_event(registry_event),
    )
    restored_test = domain_events.deserialize_domain_event(
        domain_events.serialize_domain_event(test_event),
    )

    assert restored_registry == registry_event
    assert restored_test == test_event


def test_live_and_runtime_assignment_events_roundtrip() -> None:
    events = (
        domain_events.StepFinishedEvent(
            node_id='node-1',
            node_stable_id='stable-node-1',
            engine_name='gherkin',
            test_name='Scenario: auth',
            test_path='features/auth.feature',
            step_type='given',
            step_keyword='Given ',
            step_text='the user logs in',
            status='passed',
            message='step finished',
            metadata=domain_events.DomainEventMetadata(
                sequence_number=8,
                session_id='session-1',
                plan_id='plan-1',
                node_stable_id='stable-node-1',
                trace_id='trace-1',
                worker_id=2,
            ),
        ),
        domain_events.NodeEnqueuedEvent(
            node_id='node-1',
            node_stable_id='stable-node-1',
            preferred_worker_slot=1,
            max_attempts=2,
        ),
        domain_events.NodeAssignedEvent(
            node_id='node-1',
            node_stable_id='stable-node-1',
            worker_slot=0,
            attempt=1,
        ),
        domain_events.NodeRequeuedEvent(
            node_id='node-1',
            node_stable_id='stable-node-1',
            previous_worker_slot=0,
            attempt=2,
            failure_kind='infrastructure',
            error_code='transient_worker_error',
        ),
        domain_events.WorkerDegradedEvent(
            worker_id=0,
            reason='transient_worker_error',
        ),
        domain_events.WorkerRecoveredEvent(worker_id=0),
    )

    for event in events:
        restored = domain_events.deserialize_domain_event(
            domain_events.serialize_domain_event(event),
        )
        assert restored == event


def _metadata(event_suffix: str) -> domain_events.DomainEventMetadata:
    return domain_events.DomainEventMetadata(
        event_id=f'event-{event_suffix}',
        sequence_number=7,
        correlation_id=f'correlation-{event_suffix}',
        idempotency_key=f'idempotency-{event_suffix}',
        session_id='session-1',
        plan_id='plan-1',
        node_id='node-1',
        node_stable_id='stable-node-1',
        trace_id='trace-1',
        worker_id=2,
    )


@pytest.mark.parametrize(
    ('event',),
    (
        (
            domain_events.SessionStartedEvent(
                root_path='/workspace/tests',
                concurrency=3,
                workspace_fingerprint='workspace-fp',
                metadata=_metadata('session-started'),
                timestamp=11.0,
            ),
        ),
        (
            domain_events.SessionFinishedEvent(
                has_failures=False,
                metadata=_metadata('session-finished'),
                timestamp=12.0,
            ),
        ),
        (
            domain_events.PlanAnalyzedEvent(
                mode='strict',
                executable=True,
                node_count=4,
                issue_count=1,
                metadata=_metadata('plan-analyzed'),
                timestamp=13.0,
            ),
        ),
        (
            domain_events.NodeScheduledEvent(
                node_id='node-1',
                node_stable_id='stable-node-1',
                worker_slot=1,
                max_attempts=4,
                timeout_seconds=2.5,
                metadata=_metadata('node-scheduled'),
                timestamp=14.0,
            ),
        ),
        (
            domain_events.NodeRetryingEvent(
                node_id='node-1',
                node_stable_id='stable-node-1',
                attempt=3,
                failure_kind='infrastructure',
                error_code='network_timeout',
                metadata=_metadata('node-retrying'),
                timestamp=15.0,
            ),
        ),
        (
            domain_events.KnowledgeInvalidatedEvent(
                engine_name='gherkin',
                file_path='features/auth.feature',
                reason='file_changed',
                knowledge_version='v2',
                metadata=_metadata('knowledge-invalidated'),
                timestamp=16.0,
            ),
        ),
        (
            domain_events.DefinitionMaterializedEvent(
                engine_name='gherkin',
                file_path='steps/auth.py',
                definition_count=2,
                discovery_mode='runtime',
                metadata=_metadata('definition-materialized'),
                timestamp=17.0,
            ),
        ),
        (
            domain_events.EngineSnapshotUpdatedEvent(
                engine_name='gherkin',
                snapshot_kind='runtime',
                payload={'tests': 2},
                metadata=_metadata('engine-snapshot'),
                timestamp=18.0,
            ),
        ),
        (
            domain_events.TestKnowledgeInvalidatedEvent(
                engine_name='gherkin',
                file_path='features/auth.feature',
                reason='manifest_changed',
                knowledge_version='v3',
                metadata=_metadata('test-knowledge-invalidated'),
                timestamp=19.0,
            ),
        ),
        (
            domain_events.TestFinishedEvent(
                node_id='node-1',
                node_stable_id='stable-node-1',
                engine_name='gherkin',
                test_name='Scenario: auth',
                test_path='features/auth.feature',
                status='failed',
                duration=2.75,
                failure_kind='assertion',
                error_code='expectation_failed',
                metadata=_metadata('test-finished'),
                timestamp=20.0,
            ),
        ),
        (
            domain_events.ResourceLifecycleEvent(
                action='acquired',
                name='mongo',
                scope='worker',
                test_id='test-1',
                external_handle='resource-1',
                metadata=_metadata('resource-lifecycle'),
                timestamp=21.0,
            ),
        ),
        (
            domain_events.ResourceReadinessTransitionEvent(
                name='mongo',
                scope='worker',
                status='degraded',
                reason='healthcheck failed',
                metadata=_metadata('resource-readiness'),
                timestamp=22.0,
            ),
        ),
        (
            domain_events.WorkerHeartbeatEvent(
                worker_id=2,
                status='alive',
                metadata=_metadata('worker-heartbeat'),
                timestamp=23.0,
            ),
        ),
    ),
)
def test_domain_event_roundtrip_for_remaining_event_types(event) -> None:
    restored = domain_events.deserialize_domain_event(
        domain_events.serialize_domain_event(event),
    )

    assert restored == event


def test_deserialize_domain_event_rejects_unsupported_type() -> None:
    with pytest.raises(ValueError, match='Unsupported domain event type'):
        domain_events.deserialize_domain_event(
            {
                'event_type': 'unknown.event',
                'metadata': {'event_id': 'event-1'},
                'timestamp': 1.0,
            },
        )


def test_deserialize_domain_event_rejects_non_mapping_metadata() -> None:
    with pytest.raises(
        TypeError,
        match='Domain event metadata payload must be a mapping',
    ):
        domain_events.deserialize_domain_event(
            {
                'event_type': 'session.finished',
                'metadata': 'not-a-dict',
                'timestamp': 1.0,
                'has_failures': False,
            },
        )


def test_domain_event_payload_serializer_rejects_unknown_types() -> None:
    with pytest.raises(TypeError, match='Unsupported domain event type'):
        domain_events._serialize_domain_event_payload(object())


def test_deserializers_apply_optional_defaults_and_payload_guards() -> None:
    node_payload = domain_events.serialize_domain_event(
        domain_events.NodeAssignedEvent(
            node_id='node-1',
            node_stable_id='stable-node-1',
            worker_slot=2,
            attempt=5,
            metadata=_metadata('node-assigned-default'),
            timestamp=30.0,
        ),
    )
    node_payload.pop('attempt', None)
    restored_node = domain_events.deserialize_domain_event(node_payload)

    registry_payload = domain_events.serialize_domain_event(
        domain_events.RegistryKnowledgeIndexedEvent(
            engine_name='gherkin',
            module_spec='tests.steps.auth',
            package_hash='pkg-1',
            layout_key='tests',
            loader_schema_version='schema-v3',
            entries=(),
            source_count=2,
            metadata=_metadata('registry-default'),
            timestamp=31.0,
        ),
    )
    registry_payload.pop('source_count', None)
    restored_registry = domain_events.deserialize_domain_event(registry_payload)

    snapshot_payload = domain_events.serialize_domain_event(
        domain_events.EngineSnapshotUpdatedEvent(
            engine_name='gherkin',
            snapshot_kind='runtime',
            payload={'kept': True},
            metadata=_metadata('snapshot-payload'),
            timestamp=32.0,
        ),
    )
    snapshot_payload['payload'] = ['not', 'a', 'dict']
    restored_snapshot = domain_events.deserialize_domain_event(snapshot_payload)

    assert isinstance(restored_node, domain_events.NodeAssignedEvent)
    assert restored_node.attempt == 1
    assert isinstance(
        restored_registry,
        domain_events.RegistryKnowledgeIndexedEvent,
    )
    assert restored_registry.source_count == 0
    assert isinstance(
        restored_snapshot,
        domain_events.EngineSnapshotUpdatedEvent,
    )
    assert restored_snapshot.payload == {}
