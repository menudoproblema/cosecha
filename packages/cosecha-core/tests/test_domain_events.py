from __future__ import annotations

import time

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
