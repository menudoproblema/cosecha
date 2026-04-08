from __future__ import annotations

import asyncio

from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from cosecha.core.capabilities import DraftValidationResult
from cosecha.core.engine_dependencies import (
    EngineDependencyQuery,
    EngineDependencyRule,
)
from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.execution_ir import PlanningAnalysis
from cosecha.core.extensions import ExtensionQuery
from cosecha.core.knowledge_base import (
    DefinitionKnowledgeQuery,
    DomainEventQuery,
    LiveExecutionSnapshot,
    ResourceKnowledgeQuery,
    SessionArtifactQuery,
    TestKnowledgeQuery,
)
from cosecha.core.operations import (
    AnalyzePlanOperation,
    DraftValidationOperation,
    ExplainPlanOperation,
    QueryCapabilitiesOperation,
    QueryDefinitionsOperation,
    QueryEngineDependenciesOperation,
    QueryEventsOperation,
    QueryExtensionsOperation,
    QueryLiveStatusOperation,
    QueryLiveSubscriptionOperation,
    QueryLiveTailOperation,
    QueryRegistryItemsOperation,
    QueryResourcesOperation,
    QuerySessionArtifactsOperation,
    QueryTestsOperation,
    ResolveDefinitionOperation,
    RunOperation,
    SimulatePlanOperation,
)
from cosecha.core.registry_knowledge import RegistryKnowledgeQuery
from cosecha.core.runner import Runner, RunnerRuntimeError
from cosecha_internal.testkit import DummyReporter, ListCollector, build_config


if TYPE_CHECKING:
    from pathlib import Path


class _DummyContext(BaseContext):
    async def cleanup(self) -> None:
        return None


class _DummyEngine(Engine):
    def __init__(
        self,
        name: str,
        *,
        dependency_rules: tuple[EngineDependencyRule, ...] = (),
    ) -> None:
        super().__init__(
            name,
            collector=ListCollector(()),
            reporter=DummyReporter(),
        )
        self._dependency_rules = dependency_rules

    async def generate_new_context(self, test):
        del test
        return _DummyContext()

    def describe_engine_dependencies(self) -> tuple[EngineDependencyRule, ...]:
        return self._dependency_rules


class _DraftAndDefinitionEngine(_DummyEngine):
    async def validate_draft(
        self,
        source_content: str,
        test_path,
    ) -> DraftValidationResult:
        del source_content, test_path
        return DraftValidationResult(test_count=1)

    async def resolve_definition(
        self,
        *,
        test_path,
        step_type: str,
        step_text: str,
    ) -> tuple[object, ...]:
        del test_path, step_type, step_text
        return ()


def _build_runner(
    tmp_path: Path,
    *,
    engines: dict[str, Engine] | None = None,
) -> Runner:
    engine_map = engines or {'': _DummyEngine('dummy')}
    return Runner(build_config(tmp_path), engine_map)


def test_execute_operation_dispatches_run_and_planning_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    recorded_paths: list[tuple[str, ...]] = []
    run_tests_calls: list[tuple[tuple[str, ...], int | None]] = []
    planning_sentinel = object()

    async def _fake_run_with_session(self, paths, callback):
        del self
        recorded_paths.append(tuple(paths))
        return await callback()

    async def _fake_run_tests(
        self,
        selection_labels,
        test_limit,
        execution_plan=None,
    ):
        del self
        del execution_plan
        run_tests_calls.append((tuple(selection_labels or ()), test_limit))

    async def _fake_execute_without_session(self, _operation):
        del self
        return planning_sentinel

    monkeypatch.setattr(Runner, '_run_with_session', _fake_run_with_session)
    monkeypatch.setattr(Runner, 'run_tests', _fake_run_tests)
    monkeypatch.setattr(Runner, 'has_failures', lambda self: True)
    monkeypatch.setattr(Runner, '_count_collected_tests', lambda self: 7)
    monkeypatch.setattr(
        Runner,
        '_execute_operation_without_session_management',
        _fake_execute_without_session,
    )
    monkeypatch.setattr(
        Runner,
        '_authorize_operation',
        lambda self, _operation: None,
    )

    async def _run() -> None:
        run_result = await runner.execute_operation(
            RunOperation(
                paths=('tests/demo',),
                selection_labels=('fast',),
                test_limit=3,
            ),
        )
        assert run_result.has_failures is True
        assert run_result.total_tests == 7

        for operation in (
            AnalyzePlanOperation(paths=('tests/a',)),
            ExplainPlanOperation(paths=('tests/b',)),
            SimulatePlanOperation(paths=('tests/c',)),
        ):
            assert await runner.execute_operation(operation) is planning_sentinel

    asyncio.run(_run())

    assert run_tests_calls == [(('fast',), 3)]
    assert recorded_paths == [
        ('tests/demo',),
        ('tests/a',),
        ('tests/b',),
        ('tests/c',),
    ]


def test_execute_operation_dispatches_query_and_document_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    query_sentinel = object()
    document_paths: list[tuple[str, ...]] = []

    async def _fake_run_with_session(self, paths, callback):
        del self
        document_paths.append(tuple(paths))
        return await callback()

    async def _fake_execute_without_session(self, _operation):
        del self
        return query_sentinel

    monkeypatch.setattr(Runner, '_run_with_session', _fake_run_with_session)
    monkeypatch.setattr(
        Runner,
        '_execute_operation_without_session_management',
        _fake_execute_without_session,
    )
    monkeypatch.setattr(
        Runner,
        '_build_document_collect_paths',
        lambda self, test_path: (f'doc::{test_path}',),
    )
    monkeypatch.setattr(
        Runner,
        '_authorize_operation',
        lambda self, _operation: None,
    )

    async def _run() -> None:
        assert (
            await runner.execute_operation(
                QueryCapabilitiesOperation(component_name='dummy'),
            )
            is query_sentinel
        )
        assert (
            await runner.execute_operation(
                DraftValidationOperation(
                    engine_name='dummy',
                    test_path='features/draft.feature',
                    source_content='Feature: draft',
                ),
            )
            is query_sentinel
        )
        assert (
            await runner.execute_operation(
                ResolveDefinitionOperation(
                    engine_name='dummy',
                    test_path='features/draft.feature',
                    step_type='given',
                    step_text='a prepared context',
                ),
            )
            is query_sentinel
        )
        with pytest.raises(TypeError, match='Unsupported operation'):
            await runner.execute_operation(object())  # type: ignore[arg-type]

    asyncio.run(_run())
    assert document_paths == [
        ('doc::features/draft.feature',),
        ('doc::features/draft.feature',),
    ]


def test_active_session_operation_guards_and_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)

    async def _run() -> None:
        with pytest.raises(RunnerRuntimeError, match='no active session'):
            await runner.execute_operation_in_active_session(
                QueryCapabilitiesOperation(),
            )

    asyncio.run(_run())

    runner._before_session_hooks_ran = True

    async def _run_with_active_session() -> None:
        with pytest.raises(TypeError, match='managed session lifecycle'):
            await runner.execute_operation_in_active_session(RunOperation())

    asyncio.run(_run_with_active_session())

    sentinel = object()
    authorized: list[str] = []

    async def _fake_execute_without_session(self, _operation):
        del self
        return sentinel

    monkeypatch.setattr(
        Runner,
        '_execute_operation_without_session_management',
        _fake_execute_without_session,
    )
    monkeypatch.setattr(
        Runner,
        '_authorize_operation',
        lambda self, operation: authorized.append(type(operation).__name__),
    )

    async def _run_query() -> None:
        assert (
            await runner.execute_operation_in_active_session(
                QueryCapabilitiesOperation(),
            )
            is sentinel
        )

    asyncio.run(_run_query())
    assert authorized == ['QueryCapabilitiesOperation']


def test_execute_operation_without_session_management_dispatches_all_types(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    analysis = PlanningAnalysis(mode='strict', plan=())
    simulate_sentinel = object()
    draft_sentinel = object()
    definition_sentinel = object()
    query_sentinel = object()

    async def _fake_simulate(self, _operation):
        del self
        return simulate_sentinel

    async def _fake_draft_validation(self, _operation):
        del self
        return draft_sentinel

    async def _fake_definition(self, _operation):
        del self
        return definition_sentinel

    async def _fake_query(self, _operation):
        del self
        return query_sentinel

    async def _fake_selected_plan_analysis(self, **_kwargs):
        del self
        return analysis

    monkeypatch.setattr(
        Runner,
        '_build_selected_plan_analysis',
        _fake_selected_plan_analysis,
    )
    monkeypatch.setattr(Runner, '_execute_simulate_plan_operation', _fake_simulate)
    monkeypatch.setattr(
        Runner,
        '_execute_draft_validation_operation',
        _fake_draft_validation,
    )
    monkeypatch.setattr(
        Runner,
        '_execute_definition_resolution_operation',
        _fake_definition,
    )
    monkeypatch.setattr(Runner, '_execute_query_operation', _fake_query)

    async def _run() -> None:
        analyze_result = await runner._execute_operation_without_session_management(
            AnalyzePlanOperation(),
        )
        explain_result = await runner._execute_operation_without_session_management(
            ExplainPlanOperation(),
        )
        assert analyze_result.analysis == analysis
        assert explain_result.explanation == analysis.explanation
        assert (
            await runner._execute_operation_without_session_management(
                SimulatePlanOperation(),
            )
            is simulate_sentinel
        )
        assert (
            await runner._execute_operation_without_session_management(
                DraftValidationOperation(
                    engine_name='dummy',
                    test_path='draft.feature',
                    source_content='Feature: draft',
                ),
            )
            is draft_sentinel
        )
        assert (
            await runner._execute_operation_without_session_management(
                ResolveDefinitionOperation(
                    engine_name='dummy',
                    test_path='draft.feature',
                    step_type='given',
                    step_text='a step',
                ),
            )
            is definition_sentinel
        )
        assert (
            await runner._execute_operation_without_session_management(
                QueryCapabilitiesOperation(),
            )
            is query_sentinel
        )
        with pytest.raises(TypeError, match='not supported without session'):
            await runner._execute_operation_without_session_management(
                RunOperation(),
            )

    asyncio.run(_run())


def test_execute_query_and_persisted_query_operations_cover_all_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    static_sentinel = object()
    subscription_sentinel = object()
    persisted_sentinel = object()
    dependency_rule = EngineDependencyRule(
        source_engine_name='dummy',
        target_engine_name='dummy',
        dependency_kind='execution',
        projection_policy='block_execution',
    )
    dependency_issue = SimpleNamespace(kind='issue')
    snapshot = LiveExecutionSnapshot(
        recent_log_chunks=(),
        recent_events=(),
    )

    async def _fake_drain_observability(self) -> None:
        del self
        return None

    async def _fake_subscription(self, _operation):
        del self
        return subscription_sentinel

    monkeypatch.setattr(
        Runner,
        '_execute_static_metadata_query_operation',
        lambda self, _operation: static_sentinel,
    )
    monkeypatch.setattr(
        Runner,
        '_drain_runtime_provider_observability',
        _fake_drain_observability,
    )
    monkeypatch.setattr(
        Runner,
        '_filter_live_snapshot',
        lambda self, _query: snapshot,
    )
    monkeypatch.setattr(
        Runner,
        '_execute_live_subscription_operation',
        _fake_subscription,
    )
    runner._knowledge_base = SimpleNamespace(live_snapshot=lambda: snapshot)
    monkeypatch.setattr(
        Runner,
        '_query_live_tail',
        lambda self, _op, _snap: (),
    )
    monkeypatch.setattr(
        Runner,
        '_query_live_tail_log_chunks',
        lambda self, _op, _snap: (),
    )
    monkeypatch.setattr(
        Runner,
        '_query_engine_dependencies',
        lambda self, _operation: (dependency_rule,),
    )
    monkeypatch.setattr(
        Runner,
        '_project_engine_dependency_issues',
        lambda self, _rules, _operation: (dependency_issue,),
    )
    monkeypatch.setattr(
        Runner,
        '_execute_persisted_knowledge_query_operation',
        lambda self, _operation: persisted_sentinel,
    )

    async def _run() -> None:
        assert (
            await runner._execute_query_operation(QueryCapabilitiesOperation())
            is static_sentinel
        )
        live_status_result = await runner._execute_query_operation(
            QueryLiveStatusOperation(),
        )
        assert live_status_result.snapshot == snapshot
        assert (
            await runner._execute_query_operation(
                QueryLiveSubscriptionOperation(),
            )
            is subscription_sentinel
        )
        live_tail_result = await runner._execute_query_operation(
            QueryLiveTailOperation(query=DomainEventQuery()),
        )
        assert live_tail_result.events == ()
        assert live_tail_result.log_chunks == ()
        dependency_result = await runner._execute_query_operation(
            QueryEngineDependenciesOperation(
                query=EngineDependencyQuery(source_engine_name='dummy'),
            ),
        )
        assert dependency_result.rules == (dependency_rule,)
        assert dependency_result.projected_issues == (dependency_issue,)
        assert (
            await runner._execute_query_operation(
                QueryTestsOperation(query=TestKnowledgeQuery()),
            )
            is persisted_sentinel
        )
        with pytest.raises(TypeError, match='Unsupported query operation'):
            await runner._execute_query_operation(object())  # type: ignore[arg-type]

    asyncio.run(_run())


def test_persisted_queries_draft_definition_and_dependency_projection(
    tmp_path: Path,
) -> None:
    dependency_rule = EngineDependencyRule(
        source_engine_name='dummy',
        target_engine_name='aux',
        dependency_kind='execution',
        projection_policy='degrade_to_explain',
    )
    duplicate_rule = EngineDependencyRule(
        source_engine_name='dummy',
        target_engine_name='aux',
        dependency_kind='execution',
        projection_policy='degrade_to_explain',
    )
    runner = _build_runner(
        tmp_path,
        engines={
            'dummy': _DraftAndDefinitionEngine(
                'dummy',
                dependency_rules=(dependency_rule, duplicate_rule),
            ),
            'aux': _DummyEngine('aux'),
        },
    )
    runner._knowledge_base = SimpleNamespace(
        query_tests=lambda _query: (
            SimpleNamespace(
                engine_name='dummy',
                node_stable_id='stable-1',
                test_name='Scenario: projected',
                test_path='tests/projected.feature',
                status='failed',
                plan_id='plan-1',
                trace_id='trace-1',
            ),
        ),
        query_definitions=lambda _query: (),
        query_domain_events=lambda _query: (),
        query_registry_items=lambda _query: (),
        query_resources=lambda _query: (),
        query_session_artifacts=lambda _query: (),
        snapshot=lambda: SimpleNamespace(latest_plan=SimpleNamespace(plan_id='plan-1')),
    )

    persisted_tests = runner._execute_persisted_knowledge_query_operation(
        QueryTestsOperation(query=TestKnowledgeQuery()),
    )
    persisted_definitions = runner._execute_persisted_knowledge_query_operation(
        QueryDefinitionsOperation(query=DefinitionKnowledgeQuery()),
    )
    persisted_events = runner._execute_persisted_knowledge_query_operation(
        QueryEventsOperation(query=DomainEventQuery()),
    )
    persisted_registry = runner._execute_persisted_knowledge_query_operation(
        QueryRegistryItemsOperation(query=RegistryKnowledgeQuery()),
    )
    persisted_resources = runner._execute_persisted_knowledge_query_operation(
        QueryResourcesOperation(query=ResourceKnowledgeQuery()),
    )
    persisted_artifacts = runner._execute_persisted_knowledge_query_operation(
        QuerySessionArtifactsOperation(query=SessionArtifactQuery()),
    )

    assert persisted_tests.context.source == 'persistent_knowledge_base'
    assert persisted_definitions.context.source == 'persistent_knowledge_base'
    assert persisted_events.context.source == 'persistent_knowledge_base'
    assert persisted_registry.context.source == 'persistent_knowledge_base'
    assert persisted_resources.context.source == 'persistent_knowledge_base'
    assert persisted_artifacts.context.source == 'persistent_knowledge_base'

    dependency_rules = runner.describe_engine_dependencies()
    projected_issues = runner._project_engine_dependency_issues(
        dependency_rules,
        QueryEngineDependenciesOperation(
            query=EngineDependencyQuery(plan_id='plan-1'),
        ),
    )
    assert len(dependency_rules) == 1
    assert projected_issues[0].severity == 'warning'

    async def _run() -> None:
        draft_result = await runner._execute_draft_validation_operation(
            DraftValidationOperation(
                engine_name='dummy',
                test_path='draft.feature',
                source_content='Feature: draft',
            ),
        )
        definition_result = await runner._execute_definition_resolution_operation(
            ResolveDefinitionOperation(
                engine_name='dummy',
                test_path='draft.feature',
                step_type='given',
                step_text='a step',
            ),
        )
        manifest_result = await runner._execute_draft_validation_operation(
            DraftValidationOperation(
                engine_name='dummy',
                test_path='cosecha.toml',
                source_content='[manifest]\nschema_version = 1\n',
            ),
        )

        assert draft_result.validation.test_count == 1
        assert definition_result.definitions == ()
        assert manifest_result.validation.issues != ()

    asyncio.run(_run())
