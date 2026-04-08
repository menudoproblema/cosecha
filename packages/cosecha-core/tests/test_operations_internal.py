from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.capabilities import DraftValidationResult
from cosecha.core.domain_events import DomainEventMetadata, NodeAssignedEvent
from cosecha.core.engine_dependencies import EngineDependencyQuery
from cosecha.core.execution_ir import (
    NodePlanningSemantics,
    PlanExplanation,
    PlanningAnalysis,
    PlanningIssue,
    TestExecutionNodeSnapshot,
)
from cosecha.core.knowledge_base import (
    DefinitionKnowledgeQuery,
    DomainEventQuery,
    LiveExecutionSnapshot,
    ResourceKnowledgeQuery,
    SessionArtifactQuery,
    TestKnowledgeQuery,
)
from cosecha.core.operations import (
    AnalyzePlanOperationResult,
    DraftValidationOperationResult,
    ExplainPlanOperationResult,
    HypotheticalSchedulingDecision,
    KnowledgeQueryContext,
    LiveExecutionContext,
    LiveStatusQuery,
    LiveSubscriptionQuery,
    QueryDefinitionsOperation,
    QueryEngineDependenciesOperation,
    QueryEventsOperation,
    QueryEventsOperationResult,
    QueryRegistryItemsOperationResult,
    QueryResourcesOperationResult,
    QuerySessionArtifactsOperationResult,
    QueryTestsOperationResult,
    ResolveDefinitionOperationResult,
    ResolvedDefinition,
    RunOperationResult,
    SimulatePlanOperationResult,
    cast_definition_resolution_source,
    cast_knowledge_freshness,
    cast_knowledge_source,
    cast_live_execution_delivery_mode,
    cast_live_execution_granularity,
    cast_live_execution_source,
    cast_live_execution_volatility,
    cast_optional_float,
    cast_optional_int,
    cast_optional_str,
    cast_planning_mode,
    cast_required_mapping,
    cast_simulation_resource_policy,
    deserialize_operation,
    normalize_operation_paths,
)
from cosecha.core.registry_knowledge import RegistryKnowledgeQuery
from cosecha.core.scheduler import SchedulingDecision


def _build_plan_analysis() -> PlanningAnalysis:
    snapshot = TestExecutionNodeSnapshot(
        id='node-1',
        stable_id='stable-1',
        engine_name='gherkin',
        test_name='Scenario',
        test_path='features/demo.feature',
    )
    return PlanningAnalysis(
        mode='strict',
        plan=(SimpleNamespace(snapshot=snapshot),),
        issues=(PlanningIssue(code='warning', message='warn'),),
        node_semantics=(
            NodePlanningSemantics(
                node_id='node-1',
                node_stable_id='stable-1',
                engine_name='gherkin',
            ),
        ),
    )


def test_operations_internal_roundtrips_cover_unhit_serializers() -> None:
    assert LiveExecutionContext.from_dict(LiveExecutionContext().to_dict()) == (
        LiveExecutionContext()
    )
    assert LiveStatusQuery.from_dict(LiveStatusQuery().to_dict()) == LiveStatusQuery()
    assert QueryEventsOperation.from_dict(
        QueryEventsOperation(query=DomainEventQuery(limit=2)).to_dict(),
    ) == QueryEventsOperation(query=DomainEventQuery(limit=2))
    assert LiveSubscriptionQuery.from_dict(
        LiveSubscriptionQuery(limit=3).to_dict(),
    ) == LiveSubscriptionQuery(limit=3)
    assert QueryEngineDependenciesOperation.from_dict(
        QueryEngineDependenciesOperation(
            query=EngineDependencyQuery(source_engine_name='gherkin'),
        ).to_dict(),
    ) == QueryEngineDependenciesOperation(
        query=EngineDependencyQuery(source_engine_name='gherkin'),
    )

    resolved_definition = ResolvedDefinition(
        engine_name='gherkin',
        file_path='steps.py',
        line=10,
        step_type='given',
        patterns=('user logs in',),
        resolution_source='runtime_registry',
    )
    assert ResolvedDefinition.from_dict(
        resolved_definition.to_dict(),
    ) == resolved_definition

    operation = QueryDefinitionsOperation(
        query=DefinitionKnowledgeQuery(engine_name='gherkin', limit=1),
    )
    assert QueryDefinitionsOperation.from_dict(operation.to_dict()) == operation

    run_result = RunOperationResult(has_failures=False, total_tests=2)
    assert RunOperationResult.from_dict(run_result.to_dict()) == run_result

    analyze_result = AnalyzePlanOperationResult(analysis=_build_plan_analysis())
    payload = analyze_result.to_dict()
    assert payload['analysis']['issues']
    assert payload['analysis']['node_semantics']
    assert payload['analysis']['plan']

    explanation_result = ExplainPlanOperationResult(
        explanation=PlanExplanation(
            mode='relaxed',
            executable=True,
        ),
    )
    assert (
        ExplainPlanOperationResult.from_dict(explanation_result.to_dict())
        == explanation_result
    )

    decision = SchedulingDecision(
        node_id='node-1',
        node_stable_id='stable-1',
        worker_slot=1,
        max_attempts=2,
    )
    hypothetical = HypotheticalSchedulingDecision.from_scheduling_decision(
        decision,
    )
    assert (
        HypotheticalSchedulingDecision.from_dict(hypothetical.to_dict())
        == hypothetical
    )

    simulation_result = SimulatePlanOperationResult(
        explanation=PlanExplanation(mode='relaxed', executable=True),
        plan=(),
        hypothetical_scheduling=(hypothetical,),
    )
    assert (
        SimulatePlanOperationResult.from_dict(simulation_result.to_dict())
        == simulation_result
    )

    resolution_result = ResolveDefinitionOperationResult(
        definitions=(resolved_definition,),
    )
    assert (
        ResolveDefinitionOperationResult.from_dict(resolution_result.to_dict())
        == resolution_result
    )

    draft_result = DraftValidationOperationResult(
        engine_name='gherkin',
        test_path='demo.feature',
        validation=DraftValidationResult(
            test_count=1,
            required_step_texts=(),
            step_candidate_files=(),
        ),
    )
    assert draft_result.to_dict()['engine_name'] == 'gherkin'
    assert DraftValidationOperationResult.from_dict(draft_result.to_dict()) == (
        draft_result
    )

    query_context = KnowledgeQueryContext(
        source='persistent_knowledge_base',
        freshness='fresh',
    )
    tests_result = QueryTestsOperationResult(tests=(), context=query_context)
    assert QueryTestsOperationResult.from_dict(tests_result.to_dict()) == tests_result
    registry_result = QueryRegistryItemsOperationResult(
        registry_snapshots=(),
        context=query_context,
    )
    assert (
        QueryRegistryItemsOperationResult.from_dict(registry_result.to_dict())
        == registry_result
    )
    resources_result = QueryResourcesOperationResult(resources=(), context=query_context)
    assert (
        QueryResourcesOperationResult.from_dict(resources_result.to_dict())
        == resources_result
    )
    artifacts_result = QuerySessionArtifactsOperationResult(
        artifacts=(),
        context=query_context,
    )
    assert (
        QuerySessionArtifactsOperationResult.from_dict(artifacts_result.to_dict())
        == artifacts_result
    )

    events_result = QueryEventsOperationResult(
        events=(
            NodeAssignedEvent(
                node_id='node-1',
                node_stable_id='stable-1',
                worker_slot=1,
                metadata=DomainEventMetadata(sequence_number=1),
            ),
        ),
        context=query_context,
    )
    payload = events_result.to_dict()
    payload['events'].append('invalid')
    assert QueryEventsOperationResult.from_dict(payload).events


def test_operations_internal_cast_helpers_and_deserializer_errors() -> None:
    assert deserialize_operation({'operation_type': 'run'}) == deserialize_operation(
        {'operation_type': 'run'},
    )
    assert normalize_operation_paths(None) == ()
    assert normalize_operation_paths(('tests',)) == ('tests',)

    assert cast_optional_int(None) is None
    assert cast_optional_int('2') == 2
    assert cast_optional_float(None) is None
    assert cast_optional_float('3.5') == 3.5
    assert cast_optional_str(None) is None
    assert cast_optional_str(7) == '7'

    assert cast_planning_mode('strict') == 'strict'
    with pytest.raises(ValueError, match='Invalid planning mode'):
        cast_planning_mode('broken')

    assert cast_knowledge_freshness('fresh') == 'fresh'
    with pytest.raises(ValueError, match='Invalid knowledge freshness'):
        cast_knowledge_freshness('broken')

    assert cast_knowledge_source('live_session') == 'live_session'
    with pytest.raises(ValueError, match='Invalid knowledge source'):
        cast_knowledge_source('broken')

    assert cast_live_execution_source('live_projection') == 'live_projection'
    with pytest.raises(ValueError, match='Invalid live execution source'):
        cast_live_execution_source('broken')

    assert cast_live_execution_volatility('volatile') == 'volatile'
    with pytest.raises(ValueError, match='Invalid live execution volatility'):
        cast_live_execution_volatility('broken')

    assert cast_live_execution_delivery_mode(None) == 'poll_by_cursor'
    assert cast_live_execution_delivery_mode('poll_by_cursor') == 'poll_by_cursor'
    with pytest.raises(ValueError, match='Invalid live execution delivery mode'):
        cast_live_execution_delivery_mode('broken')

    assert cast_live_execution_granularity(None) == 'streaming'
    assert cast_live_execution_granularity('consolidated_response') == (
        'consolidated_response'
    )
    with pytest.raises(ValueError, match='Invalid live execution granularity'):
        cast_live_execution_granularity('broken')

    assert cast_definition_resolution_source('runtime_registry') == (
        'runtime_registry'
    )
    with pytest.raises(ValueError, match='Invalid definition resolution source'):
        cast_definition_resolution_source('broken')

    assert cast_simulation_resource_policy('non_live_only') == 'non_live_only'
    assert cast_simulation_resource_policy('anything') == 'non_live_only'

    assert cast_required_mapping({'context': {}}, 'context') == {}
    with pytest.raises(TypeError, match='Expected a mapping'):
        cast_required_mapping({'context': []}, 'context')


def test_operations_internal_query_roundtrips_cover_registry_paths() -> None:
    registry_query = RegistryKnowledgeQuery(
        engine_name='gherkin',
        module_spec='pkg.module',
        package_hash='pkg-hash',
        layout_key='helper:pkg.Base',
        loader_schema_version='loader:v1',
        limit=1,
    )
    assert RegistryKnowledgeQuery.from_dict(registry_query.to_dict()) == registry_query

    snapshot = LiveExecutionSnapshot(
        session_id='session-1',
        trace_id='trace-1',
        plan_id='plan-1',
    )
    assert LiveExecutionSnapshot.from_dict(snapshot.to_dict()) == snapshot

    assert SessionArtifactQuery.from_dict(
        SessionArtifactQuery(limit=1).to_dict(),
    ) == SessionArtifactQuery(limit=1)
    assert ResourceKnowledgeQuery.from_dict(
        ResourceKnowledgeQuery(name='db').to_dict(),
    ) == ResourceKnowledgeQuery(name='db')
    assert TestKnowledgeQuery.from_dict(TestKnowledgeQuery(limit=1).to_dict()) == (
        TestKnowledgeQuery(limit=1)
    )
