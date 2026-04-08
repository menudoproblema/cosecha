from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cosecha.core.capabilities import (
    CapabilityComponentSnapshot,
    CapabilityDescriptor,
    DraftValidationResult,
)
from cosecha.core.domain_events import (
    DomainEventMetadata,
    NodeAssignedEvent,
    TestFinishedEvent as FinishedEvent,
)
from cosecha.core.engine_dependencies import EngineDependencyQuery
from cosecha.core.execution_ir import (
    NodePlanningSemantics,
    PlanExplanation,
    PlanningIssue,
)
from cosecha.core.extensions import (
    ExtensionComponentSnapshot,
    ExtensionDescriptor,
    ExtensionQuery,
)
from cosecha.core.knowledge_base import (
    DefinitionKnowledgeQuery,
    DomainEventQuery,
    LiveEngineSnapshot,
    LiveExecutionSnapshot,
    LiveLogChunk,
    ResourceKnowledgeQuery,
    SessionArtifactQuery,
    TestKnowledgeQuery as StoredTestKnowledgeQuery,
)
from cosecha.core.operations import (
    AnalyzePlanOperation,
    DraftValidationOperation,
    ExplainPlanOperation,
    ExplainPlanOperationResult,
    KnowledgeQueryContext,
    LiveExecutionContext,
    LiveStatusQuery,
    LiveSubscriptionQuery,
    QueryCapabilitiesOperation,
    QueryCapabilitiesOperationResult,
    QueryEventsOperationResult,
    QueryExtensionsOperation,
    QueryExtensionsOperationResult,
    QueryLiveStatusOperation,
    QueryLiveStatusOperationResult,
    QueryLiveSubscriptionOperation,
    QueryLiveSubscriptionOperationResult,
    QueryLiveTailOperation,
    QueryLiveTailOperationResult,
    QueryRegistryItemsOperation,
    QueryResourcesOperation,
    QuerySessionArtifactsOperation,
    QueryTestsOperation,
    ResolveDefinitionOperation,
    RunOperation,
    SimulatePlanOperation,
    deserialize_operation,
    operation_intent,
)
from cosecha.core.registry_knowledge import RegistryKnowledgeQuery
from cosecha_internal.testkit import build_config


if TYPE_CHECKING:
    from pathlib import Path


def _build_plan_explanation() -> PlanExplanation:
    return PlanExplanation(
        mode='relaxed',
        executable=False,
        estimated_total_duration=3.5,
        issues=(
            PlanningIssue(
                code='duplicated_node_id',
                message='Duplicated execution node id: dummy:first.feature:0',
                node_id='dummy:first.feature:0',
            ),
        ),
        node_semantics=(
            NodePlanningSemantics(
                node_id='dummy:first.feature:0',
                node_stable_id='dummy:first.feature:stable',
                engine_name='dummy',
                estimated_duration=3.5,
                resource_names=('session_db',),
            ),
        ),
        fallback_reasons=('planning_issues_present',),
    )


def _build_live_snapshot() -> LiveExecutionSnapshot:
    return LiveExecutionSnapshot(
        session_id='session-1',
        trace_id='trace-1',
        plan_id='plan-1',
        last_sequence_number=3,
        recent_log_chunks=(
            LiveLogChunk(
                message='worker ready',
                level='INFO',
                logger_name='cosecha.worker',
                emitted_at=1.5,
                sequence_number=2,
            ),
        ),
        engine_snapshots=(
            LiveEngineSnapshot(
                engine_name='gherkin',
                snapshot_kind='catalog',
                node_stable_id='stable-1',
                payload={'steps': 3},
                worker_id=1,
            ),
        ),
        recent_events=(
            NodeAssignedEvent(
                node_id='node-1',
                node_stable_id='stable-1',
                worker_slot=1,
                metadata=DomainEventMetadata(sequence_number=3),
            ),
        ),
    )


def test_operation_roundtrips_cover_run_planning_and_queries(
) -> None:
    operations = (
        RunOperation(
            paths=('tests/payments',),
            selection_labels=('slow', '~legacy'),
            node_stable_ids=('stable-1',),
            test_limit=5,
        ),
        AnalyzePlanOperation(
            paths=('tests/payments',),
            selection_labels=('fast',),
            test_limit=3,
            mode='relaxed',
        ),
        ExplainPlanOperation(
            paths=('tests/payments',),
            selection_labels=('fast',),
            test_limit=2,
        ),
        SimulatePlanOperation(
            paths=('tests/payments',),
            selection_labels=('fast', '~legacy'),
            test_limit=3,
            mode='relaxed',
        ),
        QueryCapabilitiesOperation(
            component_kind='engine',
            component_name='gherkin',
        ),
        QueryExtensionsOperation(
            query=ExtensionQuery(
                extension_kind='plugin',
                canonical_name='TimingPlugin',
            ),
        ),
        QueryLiveStatusOperation(
            query=LiveStatusQuery(
                session_id='session-1',
                node_stable_id='stable-1',
                worker_id=2,
                include_engine_snapshots=True,
            ),
        ),
    )

    for operation in operations:
        restored = deserialize_operation(operation.to_dict())
        assert restored == operation


def test_operation_roundtrips_cover_subscription_resolution_and_knowledge(
) -> None:
    operations = (
        QueryLiveSubscriptionOperation(
            query=LiveSubscriptionQuery(
                session_id='session-1',
                node_stable_id='stable-1',
                worker_id=2,
                after_sequence_number=10,
                limit=5,
                include_engine_snapshots=True,
            ),
        ),
        QueryLiveTailOperation(
            query=DomainEventQuery(
                session_id='session-1',
                node_stable_id='stable-1',
                limit=5,
            ),
        ),
        QueryRegistryItemsOperation(
            query=RegistryKnowledgeQuery(
                engine_name='gherkin',
                module_spec='demo_pkg',
                package_hash='pkg-hash',
                layout_key='helper:demo_pkg.base.BaseItem',
                loader_schema_version='gherkin_registry_loader_snapshot:v3',
                limit=1,
            ),
        ),
        DraftValidationOperation(
            engine_name='gherkin',
            test_path='tests/payments/draft.feature',
            source_content='Feature: Draft',
        ),
        ResolveDefinitionOperation(
            engine_name='gherkin',
            test_path='tests/payments/draft.feature',
            step_type='given',
            step_text='the user logs in',
        ),
        QueryTestsOperation(
            query=StoredTestKnowledgeQuery(
                engine_name='gherkin',
                node_stable_id='node-1',
                limit=5,
            ),
        ),
        QueryResourcesOperation(
            query=ResourceKnowledgeQuery(name='session_db', scope='test'),
        ),
        QuerySessionArtifactsOperation(
            query=SessionArtifactQuery(session_id='session-1', limit=2),
        ),
    )

    for operation in operations:
        restored = deserialize_operation(operation.to_dict())
        assert restored == operation


def test_operation_results_roundtrip_for_capabilities_extensions_and_plan(
) -> None:
    capability_result = QueryCapabilitiesOperationResult(
        snapshots=(
            CapabilityComponentSnapshot(
                component_name='gherkin',
                component_kind='engine',
                capabilities=(
                    CapabilityDescriptor(
                        name='selection_labels',
                        level='supported',
                    ),
                ),
            ),
        ),
    )
    extension_result = QueryExtensionsOperationResult(
        snapshots=(
            ExtensionComponentSnapshot(
                component_name='timing',
                descriptor=ExtensionDescriptor(
                    canonical_name='TimingPlugin',
                    extension_kind='plugin',
                    api_version=1,
                    implementation='demo:TimingPlugin',
                ),
            ),
        ),
    )
    explanation_result = ExplainPlanOperationResult(
        explanation=_build_plan_explanation(),
    )

    assert (
        QueryCapabilitiesOperationResult.from_dict(
            capability_result.to_dict(),
        )
        == capability_result
    )
    assert (
        QueryExtensionsOperationResult.from_dict(
            extension_result.to_dict(),
        )
        == extension_result
    )
    assert (
        ExplainPlanOperationResult.from_dict(explanation_result.to_dict())
        == explanation_result
    )


def test_operation_results_roundtrip_for_live_status_tail_and_subscription(
) -> None:
    context = LiveExecutionContext()
    snapshot = _build_live_snapshot()
    status_result = QueryLiveStatusOperationResult(
        snapshot=snapshot,
        context=context,
    )
    tail_result = QueryLiveTailOperationResult(
        events=snapshot.recent_events,
        context=context,
        log_chunks=snapshot.recent_log_chunks,
    )
    subscription_result = QueryLiveSubscriptionOperationResult(
        snapshot=snapshot,
        events=snapshot.recent_events,
        log_chunks=snapshot.recent_log_chunks,
        next_sequence_number=4,
        context=context,
    )
    events_result = QueryEventsOperationResult(
        events=(
            FinishedEvent(
                node_id='node-1',
                node_stable_id='stable-1',
                engine_name='gherkin',
                test_name='Scenario: auth',
                test_path='features/auth.feature',
                status='passed',
                duration=0.25,
                metadata=DomainEventMetadata(sequence_number=4),
            ),
        ),
        context=KnowledgeQueryContext(
            source='persistent_knowledge_base',
            freshness='fresh',
        ),
    )

    assert (
        QueryLiveStatusOperationResult.from_dict(status_result.to_dict())
        == status_result
    )
    assert (
        QueryLiveTailOperationResult.from_dict(tail_result.to_dict())
        == tail_result
    )
    assert (
        QueryLiveSubscriptionOperationResult.from_dict(
            subscription_result.to_dict(),
        )
        == subscription_result
    )
    assert (
        QueryEventsOperationResult.from_dict(events_result.to_dict())
        == events_result
    )


def test_operation_intent_distinguishes_read_only_dry_run_and_mutation(
) -> None:
    assert operation_intent(QueryCapabilitiesOperation()) == 'read_only'
    assert operation_intent(SimulatePlanOperation()) == 'dry_run'
    assert operation_intent(RunOperation()) == 'mutation'


def test_deserialize_operation_rejects_unknown_operation_type() -> None:
    with pytest.raises(ValueError, match='Unknown operation type'):
        deserialize_operation({'operation_type': 'demo.unknown'})


def test_draft_validation_result_roundtrip_supports_nested_payload() -> None:
    result = DraftValidationResult(
        test_count=1,
        required_step_texts=(('given', 'the user logs in'),),
        step_candidate_files=('tests/steps/auth.py',),
    )

    assert DraftValidationResult.from_dict(result.to_dict()) == result


def test_build_config_snapshot_stays_serializable_for_operation_payloads(
    tmp_path: Path,
) -> None:
    snapshot = build_config(tmp_path).snapshot()

    assert snapshot == snapshot.from_dict(snapshot.to_dict())


def test_query_types_roundtrip_via_to_dict_helpers() -> None:
    queries = (
        DomainEventQuery(session_id='session-1', limit=5),
        DefinitionKnowledgeQuery(
            engine_name='gherkin',
            step_type='given',
            step_text='the user logs in',
        ),
        ResourceKnowledgeQuery(name='session_db', scope='worker'),
        SessionArtifactQuery(trace_id='trace-1', limit=2),
        StoredTestKnowledgeQuery(
            engine_name='gherkin',
            status='passed',
            limit=3,
        ),
        EngineDependencyQuery(
            source_engine_name='gherkin',
            target_engine_name='pytest',
        ),
    )

    for query in queries:
        assert type(query).from_dict(query.to_dict()) == query
