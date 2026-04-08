from __future__ import annotations

from cosecha.core.engine_dependencies import (
    EngineDependencyDescribingComponent,
    EngineDependencyQuery,
    EngineDependencyRule,
    ProjectedEngineDependencyIssue,
    build_engine_dependency_rule_key,
)


def test_engine_dependency_contract_roundtrips_and_matching() -> None:
    rule = EngineDependencyRule(
        source_engine_name='gherkin',
        target_engine_name='python',
        dependency_kind='execution',
        projection_policy='block_execution',
        summary='cross-engine dependency',
        required_capabilities=('run',),
        operation_types=('execution.live_status',),
        shared_trace_required=True,
    )
    issue = ProjectedEngineDependencyIssue(
        source_engine_name='gherkin',
        target_engine_name='python',
        dependency_kind='execution',
        projection_policy='block_execution',
        source_node_stable_id='stable-1',
        source_test_name='Scenario',
        source_test_path='features/auth.feature',
        source_status='failed',
        severity='error',
        message='blocked',
        plan_id='plan-1',
        trace_id='trace-1',
    )
    query = EngineDependencyQuery(
        source_engine_name='gherkin',
        target_engine_name='python',
        dependency_kind='execution',
        projection_policy='block_execution',
        plan_id='plan-1',
    )

    assert EngineDependencyRule.from_dict(rule.to_dict()) == rule
    assert ProjectedEngineDependencyIssue.from_dict(issue.to_dict()) == issue
    assert EngineDependencyQuery.from_dict(query.to_dict()) == query
    assert query.matches(rule) is True
    assert build_engine_dependency_rule_key(rule) == (
        'gherkin:python:execution:block_execution:execution.live_status'
    )


def test_engine_dependency_query_matches_false_on_each_filter() -> None:
    rule = EngineDependencyRule(
        source_engine_name='engine-a',
        target_engine_name='engine-b',
        dependency_kind='planning',
        projection_policy='diagnostic_only',
    )

    assert EngineDependencyQuery(source_engine_name='x').matches(rule) is False
    assert EngineDependencyQuery(target_engine_name='x').matches(rule) is False
    assert EngineDependencyQuery(dependency_kind='knowledge').matches(rule) is False
    assert (
        EngineDependencyQuery(projection_policy='block_execution').matches(rule)
        is False
    )


def test_engine_dependency_describing_protocol_runtime_checkable() -> None:
    class _Provider:
        def describe_engine_dependencies(
            self,
        ) -> tuple[EngineDependencyRule, ...]:
            return ()

    assert isinstance(_Provider(), EngineDependencyDescribingComponent)
