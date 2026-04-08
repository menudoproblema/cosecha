from __future__ import annotations

import asyncio
import logging

from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from cosecha.core.capabilities import (
    CAPABILITY_LIVE_EXECUTION_OBSERVABILITY,
    CapabilityDescriptor,
    CapabilityOperationBinding,
)
from cosecha.core.domain_events import (
    DomainEventMetadata,
    EngineSnapshotUpdatedEvent,
    LogChunkEvent,
    NodeRequeuedEvent,
    NodeRetryingEvent,
    SessionStartedEvent,
)
from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.exceptions import Skipped
from cosecha.core.execution_ir import (
    NodePlanningSemantics,
    PlanningAnalysis,
    TestExecutionNode,
)
from cosecha.core.items import (
    ExecutionPredicateEvaluation,
    TestItem,
    TestPreflightDecision,
    TestResultStatus,
)
from cosecha.core.knowledge_base import (
    DefinitionKnowledgeQuery,
    DomainEventQuery,
    InMemoryKnowledgeBase,
    LiveEngineSnapshot,
    LiveExecutionSnapshot,
    LiveLogChunk,
    LiveTestKnowledge,
    PersistentKnowledgeBase,
    ResourceKnowledgeQuery,
    ResourceKnowledge,
    SessionArtifactQuery,
    TestKnowledgeQuery,
    WorkerKnowledge,
)
from cosecha.core.operations import (
    AnalyzePlanOperation,
    DraftValidationOperation,
    ExplainPlanOperation,
    LiveStatusQuery,
    LiveSubscriptionQuery,
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
    SimulatePlanOperation,
)
from cosecha.core.reporter import NullReporter, QueuedReporter, Reporter
from cosecha.core.reporting_ir import TestReport
from cosecha.core.resources import ResourceRequirement
from cosecha.core.runner import (
    OperationCapabilityError,
    Runner,
)
from cosecha.core.runtime import ExecutionBodyResult, RuntimeInfrastructureError
from cosecha.core.scheduler import (
    NodeExecutionTimeoutError,
    SchedulingDecision,
    SchedulingPlan,
)
from cosecha_internal.testkit import DummyReporter, ListCollector, build_config
from cosecha.core.registry_knowledge import RegistryKnowledgeQuery
from cosecha.core.plugins.base import PlanMiddleware


if TYPE_CHECKING:
    from argparse import ArgumentParser, Namespace


class _DummyContext(BaseContext):
    async def cleanup(self) -> None:
        return None


class _SimpleTest(TestItem):
    def __init__(
        self,
        path: Path,
        *,
        labels: tuple[str, ...] = (),
    ) -> None:
        super().__init__(path)
        self._labels = set(labels)

    async def run(self, context) -> None:
        del context
        self.status = TestResultStatus.PASSED

    def has_selection_label(self, name: str) -> bool:
        return name in self._labels


class _EngineWithControls(Engine):
    def __init__(
        self,
        name: str,
        *,
        capabilities: tuple[CapabilityDescriptor, ...] = (),
    ) -> None:
        super().__init__(name, collector=ListCollector(()), reporter=DummyReporter())
        self._capabilities = capabilities
        self.collect_calls: list[tuple[object, tuple[Path, ...]]] = []
        self.preflight_behavior: dict[str, object] = {}

    async def generate_new_context(self, test):
        del test
        return _DummyContext()

    async def collect(self, path=None, excluded_paths=()):
        self.collect_calls.append((path, excluded_paths))
        if path == 'raise-collect':
            msg = 'collect boom'
            raise RuntimeError(msg)

    def preflight_test(self, test: TestItem):
        behavior = self.preflight_behavior.get(str(test.path))
        if isinstance(behavior, Exception):
            raise behavior
        return behavior

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return self._capabilities

    def build_live_snapshot_payload(self, node, phase: str):
        return {'node': node.id, 'phase': phase}


class _NoopPlugin:
    @classmethod
    def plugin_name(cls) -> str:
        return 'noop'

    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args: Namespace):
        del args

    @classmethod
    def required_capabilities(cls) -> tuple[str, ...]:
        return ()

    @classmethod
    def finish_priority(cls) -> int:
        return 0

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return ()

    async def initialize(self, _context) -> None:
        return None

    async def start(self) -> None:
        return None

    async def finish(self) -> None:
        return None

    async def after_session_closed(self) -> None:
        return None


class _StructuredReporter(Reporter):
    def __init__(self, path: Path | None = None) -> None:
        del path

    @classmethod
    def reporter_name(cls) -> str:
        return 'structured-reporter'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'structured'

    async def add_test(self, test) -> None:
        del test

    async def add_test_result(self, test) -> None:
        del test

    async def print_report(self) -> None:
        return None


class _ConsoleReporter(Reporter):
    @classmethod
    def reporter_name(cls) -> str:
        return 'console-reporter'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'console'

    async def add_test(self, test) -> None:
        del test

    async def add_test_result(self, test) -> None:
        del test

    async def print_report(self) -> None:
        return None


class _NotAReporter:
    pass


class _SimpleTelemetry:
    @asynccontextmanager
    async def span(self, *_args, **_kwargs):
        yield 'span-id'

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None

    @property
    def trace_id(self) -> str | None:
        return 'trace-1'


def _build_runner(
    tmp_path: Path,
    *,
    engines: dict[str, Engine] | None = None,
    stop_on_error: bool = False,
) -> Runner:
    config = build_config(tmp_path)
    config.stop_on_error = stop_on_error
    return Runner(config, engines or {'': _EngineWithControls('engine')})


def _build_node(
    engine: Engine,
    test: _SimpleTest,
    *,
    index: int,
    resource_requirements: tuple[ResourceRequirement, ...] = (),
) -> TestExecutionNode:
    return TestExecutionNode(
        id=f'node-{index}',
        stable_id=f'stable-{index}',
        engine=engine,
        test=test,
        engine_name=engine.name,
        test_name=f'test-{index}',
        test_path=str(test.path),
        resource_requirements=resource_requirements,
    )


def _operation_descriptor(operation_type: str) -> CapabilityDescriptor:
    return CapabilityDescriptor(
        name='cap',
        level='supported',
        operations=(
            CapabilityOperationBinding(operation_type=operation_type),
        ),
    )


def test_runner_reporter_discovery_and_attach_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cosecha.core import runner as runner_module

    monkeypatch.setattr(
        runner_module,
        'iter_shell_reporting_contributions',
        lambda: (_NotAReporter, _StructuredReporter, _ConsoleReporter),
    )

    reporter_types = Runner.available_reporter_types()
    assert reporter_types == {'structured-reporter': _StructuredReporter}
    assert Runner.default_console_reporter_type() is _ConsoleReporter

    config = build_config(tmp_path)
    config.reports = {'structured-reporter': tmp_path / 'report.json'}
    engine = _EngineWithControls('engine')
    runner = Runner(config, {'': engine})

    assert len(runner._extra_reporters) == 1
    assert not isinstance(engine.reporter.descriptor_target(), NullReporter)


def test_runner_operation_authorization_and_context_helpers(
    tmp_path: Path,
) -> None:
    engine_supported = _EngineWithControls(
        'supported',
        capabilities=(_operation_descriptor('draft.validate'),),
    )
    engine_unsupported = _EngineWithControls('unsupported')
    runner = _build_runner(
        tmp_path,
        engines={'a': engine_supported, 'b': engine_unsupported},
    )

    runtime_caps = (
        CapabilityDescriptor(
            name=CAPABILITY_LIVE_EXECUTION_OBSERVABILITY,
            level='supported',
            operations=(
                CapabilityOperationBinding(
                    operation_type='execution.live_status',
                ),
            ),
        ),
    )
    runner._runtime_provider = SimpleNamespace(
        describe_capabilities=lambda: runtime_caps,
        scheduler_worker_count=lambda _config: 1,
    )

    assert runner._component_supports_operation(runtime_caps, 'execution.live_status')
    runner._require_runtime_operation('execution.live_status', 'live_execution_observability')

    with pytest.raises(OperationCapabilityError):
        runner._require_runtime_operation('execution.live_tail', 'live_execution_observability')

    with pytest.raises(ValueError, match='Unknown engine'):
        runner._require_engine_operation(
            engine_name='missing',
            operation_type='draft.validate',
            capability_name='draft_validation',
        )

    with pytest.raises(OperationCapabilityError):
        runner._require_engine_operation(
            engine_name='unsupported',
            operation_type='draft.validate',
            capability_name='draft_validation',
        )

    with pytest.raises(OperationCapabilityError):
        runner._require_all_engines_operation(
            operation_type='draft.validate',
            capability_name='draft_validation',
        )

    runner._authorize_operation(QueryCapabilitiesOperation())
    runner._authorize_operation(QueryExtensionsOperation())
    runner._authorize_operation(QueryEngineDependenciesOperation())
    runner._authorize_operation(QueryEventsOperation())
    runner._authorize_operation(
        QueryTestsOperation(query=TestKnowledgeQuery()),
    )
    runner._authorize_operation(
        QueryDefinitionsOperation(query=DefinitionKnowledgeQuery()),
    )
    runner._authorize_operation(
        QueryRegistryItemsOperation(query=RegistryKnowledgeQuery()),
    )
    runner._authorize_operation(
        QueryResourcesOperation(query=ResourceKnowledgeQuery()),
    )
    runner._authorize_operation(
        QuerySessionArtifactsOperation(query=SessionArtifactQuery()),
    )
    runner._authorize_operation(QueryLiveStatusOperation())

    with pytest.raises(OperationCapabilityError):
        runner._authorize_operation(
            QueryLiveTailOperation(query=DomainEventQuery()),
        )

    with pytest.raises(OperationCapabilityError):
        runner._authorize_operation(AnalyzePlanOperation())

    with pytest.raises(OperationCapabilityError):
        runner._authorize_operation(SimulatePlanOperation())

    runner._authorize_operation(
        DraftValidationOperation(
            engine_name='supported',
            test_path='cosecha.toml',
            source_content='[manifest]',
        ),
    )
    runner._authorize_operation(
        DraftValidationOperation(
            engine_name='supported',
            test_path='draft.feature',
            source_content='Feature: draft',
        ),
    )

    with pytest.raises(OperationCapabilityError):
        runner._authorize_operation(
            ResolveDefinitionOperation(
                engine_name='unsupported',
                test_path='draft.feature',
                step_type='given',
                step_text='a step',
            ),
        )

    assert runner._knowledge_query_context().source == 'persistent_knowledge_base'
    runner._before_session_hooks_ran = True
    assert runner._knowledge_query_context().source == 'live_session'

    assert runner._find_engine_by_name('missing') is None
    assert runner._build_document_collect_paths('feature.feature') == ()
    assert runner._build_document_collect_paths('tests/demo.feature') == ('tests',)


def test_runner_filter_live_snapshot_and_tail_helpers(tmp_path: Path) -> None:
    runner = _build_runner(tmp_path)
    event_a = SessionStartedEvent(
        root_path='.',
        concurrency=1,
        metadata=DomainEventMetadata(
            sequence_number=3,
            session_id='session-1',
            plan_id='plan-1',
            node_stable_id='stable-1',
            worker_id=1,
        ),
    )
    event_b = SessionStartedEvent(
        root_path='.',
        concurrency=1,
        metadata=DomainEventMetadata(
            sequence_number=4,
            session_id='session-1',
            plan_id='plan-1',
            node_stable_id='stable-2',
            worker_id=2,
        ),
    )
    snapshot = LiveExecutionSnapshot(
        session_id='session-1',
        last_sequence_number=9,
        running_tests=(
            LiveTestKnowledge(
                node_id='node-1',
                node_stable_id='stable-1',
                engine_name='engine',
                test_name='A',
                status='running',
                worker_slot=1,
            ),
            LiveTestKnowledge(
                node_id='node-2',
                node_stable_id='stable-2',
                engine_name='engine',
                test_name='B',
                status='running',
                worker_slot=2,
            ),
        ),
        workers=(
            WorkerKnowledge(worker_id=1, status='ok'),
            WorkerKnowledge(worker_id=2, status='ok'),
        ),
        resources=(
            ResourceKnowledge(
                name='mongo',
                scope='worker',
                owner_node_stable_id='stable-1',
                owner_worker_id=1,
            ),
            ResourceKnowledge(
                name='redis',
                scope='worker',
                owner_node_stable_id='stable-2',
                owner_worker_id=2,
            ),
        ),
        recent_log_chunks=(
            LiveLogChunk(
                message='one',
                level='info',
                logger_name='runner',
                emitted_at=1.0,
                node_stable_id='stable-1',
                worker_id=1,
                sequence_number=3,
            ),
            LiveLogChunk(
                message='two',
                level='info',
                logger_name='runner',
                emitted_at=2.0,
                node_stable_id='stable-2',
                worker_id=2,
                sequence_number=4,
            ),
        ),
        engine_snapshots=(
            LiveEngineSnapshot(
                engine_name='engine',
                snapshot_kind='runtime',
                node_stable_id='stable-1',
                payload={'phase': 'call'},
                worker_id=1,
            ),
            LiveEngineSnapshot(
                engine_name='engine',
                snapshot_kind='runtime',
                node_stable_id='stable-2',
                payload={'phase': 'call'},
                worker_id=2,
            ),
        ),
        recent_events=(event_a, event_b),
    )
    runner._knowledge_base = SimpleNamespace(live_snapshot=lambda: snapshot)

    mismatched = runner._filter_live_snapshot(
        LiveStatusQuery(session_id='other-session'),
    )
    assert mismatched == LiveExecutionSnapshot()

    filtered = runner._filter_live_snapshot(
        LiveStatusQuery(
            session_id='session-1',
            node_stable_id='stable-1',
            worker_id=1,
            include_engine_snapshots=True,
        ),
    )
    assert len(filtered.running_tests) == 1
    assert len(filtered.workers) == 1
    assert len(filtered.resources) == 1
    assert len(filtered.recent_events) == 1
    assert len(filtered.recent_log_chunks) == 1
    assert len(filtered.engine_snapshots) == 1

    tail = runner._query_live_tail(
        QueryLiveTailOperation(
            query=DomainEventQuery(
                plan_id='plan-1',
                node_stable_id='stable-1',
                after_sequence_number=2,
                limit=1,
            ),
        ),
        snapshot,
    )
    assert len(tail) == 1

    log_tail = runner._query_live_tail_log_chunks(
        QueryLiveTailOperation(
            query=DomainEventQuery(after_sequence_number=2, limit=1),
        ),
        snapshot,
    )
    assert len(log_tail) == 1

    sub_tail = runner._query_live_subscription_log_chunks(
        LiveSubscriptionQuery(after_sequence_number=2, limit=1),
        snapshot,
    )
    assert len(sub_tail) == 1

    assert runner._live_subscription_has_updates(
        LiveSubscriptionQuery(after_sequence_number=None),
        snapshot,
    )
    assert runner._live_subscription_has_updates(
        LiveSubscriptionQuery(after_sequence_number=1),
        snapshot,
    )
    assert not runner._live_subscription_has_updates(
        LiveSubscriptionQuery(after_sequence_number=99),
        snapshot,
    )


def test_runner_live_wait_and_subscription_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)

    async def _runtime_wait(_timeout: float) -> bool:
        return False

    runner._runtime_provider = SimpleNamespace(
        wait_for_live_observability=_runtime_wait,
        live_execution_delivery_mode=lambda: 'poll_by_cursor',
        live_execution_granularity=lambda: 'streaming',
    )

    snapshots = (
        LiveExecutionSnapshot(last_sequence_number=0),
        LiveExecutionSnapshot(last_sequence_number=3),
    )
    calls = {'count': 0}

    async def _drain(self) -> None:
        del self

    def _filter(self, _query):
        del self
        calls['count'] += 1
        return snapshots[min(calls['count'] - 1, 1)]

    monkeypatch.setattr(Runner, '_drain_runtime_provider_observability', _drain)
    monkeypatch.setattr(Runner, '_filter_live_snapshot', _filter)

    async def _exercise() -> None:
        runner._live_update_version = 1
        assert await runner._wait_for_local_live_update(0, 0.01) is True
        assert await runner._wait_for_local_live_update(1, 0.0001) is False
        assert await runner._wait_for_live_activity(1, 0.0001) is False

        operation = QueryLiveSubscriptionOperation(
            query=LiveSubscriptionQuery(
                after_sequence_number=2,
                timeout_seconds=0.1,
                include_engine_snapshots=False,
            ),
        )
        result = await runner._execute_live_subscription_operation(operation)
        assert result.next_sequence_number == 0

    asyncio.run(_exercise())


@pytest.mark.parametrize(
    ('decision', 'execute_results', 'retry_flags', 'expected_exception'),
    [
        (
            SchedulingDecision(
                node_id='node-1',
                node_stable_id='stable-1',
                worker_slot=0,
                max_attempts=1,
                timeout_seconds=None,
            ),
            ['ok'],
            [False],
            None,
        ),
        (
            SchedulingDecision(
                node_id='node-1',
                node_stable_id='stable-1',
                worker_slot=0,
                max_attempts=2,
                timeout_seconds=0.01,
            ),
            [RuntimeInfrastructureError(code='boom', message='x'), 'ok'],
            [True, False],
            None,
        ),
        (
            SchedulingDecision(
                node_id='node-1',
                node_stable_id='stable-1',
                worker_slot=0,
                max_attempts=1,
                timeout_seconds=0.01,
            ),
            [TimeoutError()],
            [False],
            NodeExecutionTimeoutError,
        ),
    ],
)
def test_runner_execute_with_scheduler_policy_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    decision: SchedulingDecision,
    execute_results: list[object],
    retry_flags: list[bool],
    expected_exception: type[Exception] | None,
) -> None:
    runner = _build_runner(tmp_path)

    node = _build_node(
        runner.engines[0],
        _SimpleTest(tmp_path / 'demo.feature'),
        index=1,
    )
    report = TestReport(
        path=str(node.test.path),
        status=TestResultStatus.PASSED,
        message=None,
        duration=0.1,
    )

    async def _execute(_node, _executor):
        outcome = execute_results.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return ExecutionBodyResult(report=report)

    runner._runtime_provider = SimpleNamespace(execute=_execute)

    drain_calls: list[str] = []

    async def _drain(self) -> None:
        del self
        drain_calls.append('drain')

    monkeypatch.setattr(Runner, '_drain_runtime_provider_observability', _drain)

    retry_values = iter(retry_flags)
    backoff_calls: list[int] = []
    events: list[object] = []

    runner._scheduler = SimpleNamespace(
        should_retry=lambda *_args: next(retry_values),
        backoff_for_attempt=lambda attempt: backoff_calls.append(attempt) or 0.0,
    )

    async def _emit(event) -> None:
        events.append(event)

    runner._domain_event_stream = SimpleNamespace(emit=_emit)

    async def _run() -> None:
        if expected_exception is None:
            result = await runner._execute_with_scheduler_policy(
                node,
                decision,
                test_span_id='span',
                node_attributes={},
            )
            assert result.report.status is TestResultStatus.PASSED
        else:
            with pytest.raises(expected_exception):
                await runner._execute_with_scheduler_policy(
                    node,
                    decision,
                    test_span_id='span',
                    node_attributes={},
                )

    asyncio.run(_run())

    assert drain_calls
    if expected_exception is None and backoff_calls:
        assert any(isinstance(event, NodeRequeuedEvent) for event in events)
        assert any(isinstance(event, NodeRetryingEvent) for event in events)


def test_runner_apply_runtime_and_simulation_compatibility(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Provider:
        def supports_mode(self, mode: str) -> bool:
            return mode in {'dry_run', 'ephemeral'}

    class _LiveOnlyProvider:
        def supports_mode(self, mode: str) -> bool:
            return mode == 'live'

    runner = _build_runner(tmp_path)
    engine = runner.engines[0]

    test_a = _SimpleTest(tmp_path / 'a.feature')
    test_b = _SimpleTest(tmp_path / 'b.feature')

    node_a = _build_node(
        engine,
        test_a,
        index=1,
        resource_requirements=(
            ResourceRequirement(
                name='db',
                provider=_Provider(),
                scope='run',
            ),
        ),
    )
    node_b = _build_node(
        engine,
        test_b,
        index=2,
        resource_requirements=(
            ResourceRequirement(
                name='queue',
                provider=_LiveOnlyProvider(),
                scope='run',
            ),
        ),
    )

    analysis = PlanningAnalysis(
        mode='relaxed',
        plan=(node_a, node_b),
        node_semantics=(
            NodePlanningSemantics(
                node_id=node_a.id,
                node_stable_id=node_a.stable_id,
                engine_name=node_a.engine_name,
            ),
            NodePlanningSemantics(
                node_id=node_b.id,
                node_stable_id=node_b.stable_id,
                engine_name=node_b.engine_name,
            ),
        ),
    )

    simulated = runner._apply_simulation_compatibility(analysis)
    assert any(issue.code == 'simulation_requires_live_resource_mode' for issue in simulated.issues)
    assert any('simulation.resource.db=' in hint for hint in simulated.node_semantics[0].runtime_hints)

    runner._runtime_provider = SimpleNamespace(
        describe_capabilities=lambda: (
            CapabilityDescriptor(name='run_scoped_resources', level='unsupported'),
        ),
        legacy_session_scope=lambda: 'run',
    )

    monkeypatch.setattr(
        'cosecha.core.runner.resolve_runtime_requirement_issues',
        lambda *_args, **_kwargs: ['missing service'],
    )

    runtime_adjusted = runner._apply_runtime_compatibility(analysis)
    assert any(issue.code == 'runtime_requirements_not_executable' for issue in runtime_adjusted.issues)
    assert any(issue.code == 'unsupported_runtime_resource_scope' for issue in runtime_adjusted.issues)


def test_runner_run_tests_stop_on_error_and_skip_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path, stop_on_error=True)
    runner.telemetry_stream = _SimpleTelemetry()

    engine = runner.engines[0]
    test_1 = _SimpleTest(tmp_path / 'one.feature', labels=('fast',))
    test_2 = _SimpleTest(tmp_path / 'two.feature', labels=('fast',))
    node_1 = _build_node(engine, test_1, index=1)
    node_2 = _build_node(engine, test_2, index=2)

    async def _prepare(*_args, **_kwargs) -> None:
        return None

    runner._runtime_provider = SimpleNamespace(
        scheduler_worker_count=lambda _config: 1,
        prepare=_prepare,
        bind_execution_slot=lambda *_args, **_kwargs: None,
        take_resource_timings=lambda: (),
        take_domain_events=lambda: (),
        take_log_events=lambda: (),
        describe_capabilities=lambda: (),
    )

    async def _drain(self) -> None:
        del self

    monkeypatch.setattr(Runner, '_drain_runtime_provider_observability', _drain)

    planner = PlanningAnalysis(
        mode='strict',
        plan=(node_1, node_2),
        node_semantics=(
            NodePlanningSemantics(
                node_id=node_1.id,
                node_stable_id=node_1.stable_id,
                engine_name=node_1.engine_name,
            ),
            NodePlanningSemantics(
                node_id=node_2.id,
                node_stable_id=node_2.stable_id,
                engine_name=node_2.engine_name,
                execution_predicate=ExecutionPredicateEvaluation(
                    state='not_executable',
                    reason='profile mismatch',
                ),
            ),
        ),
    )

    monkeypatch.setattr(
        Runner,
        '_apply_runtime_compatibility',
        lambda self, analysis: planner,
    )
    monkeypatch.setattr(
        Runner,
        '_apply_knowledge_estimates',
        lambda self, analysis, _kb: analysis,
    )

    runner._scheduler = SimpleNamespace(
        build_plan=lambda *_args, **_kwargs: SchedulingPlan(
            worker_count=1,
            decisions=(
                SchedulingDecision(
                    node_id=node_1.id,
                    node_stable_id=node_1.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
            ),
        ),
    )

    calls = {'execute': 0}

    async def _execute_with_policy(*_args, **_kwargs):
        calls['execute'] += 1
        if calls['execute'] == 1:
            msg = 'boom'
            raise RuntimeInfrastructureError(code='infra', message=msg)
        return ExecutionBodyResult(
            report=TestReport(
                path=str(test_1.path),
                status=TestResultStatus.PASSED,
                message=None,
                duration=0.1,
            ),
        )

    monkeypatch.setattr(
        Runner,
        '_execute_with_scheduler_policy',
        _execute_with_policy,
    )

    async def _run() -> None:
        await runner.run_tests(
            selection_labels=['fast'],
            execution_plan=(node_1, node_2),
        )

    asyncio.run(_run())

    assert test_1.status is TestResultStatus.ERROR
    assert test_2.status is TestResultStatus.SKIPPED


def test_runner_run_tests_preflight_and_hook_error_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _EngineWithControls('engine')
    runner = _build_runner(tmp_path, engines={'': engine})
    runner.telemetry_stream = _SimpleTelemetry()

    test_preflight_error = _SimpleTest(tmp_path / 'preflight-error.feature')
    engine.preflight_behavior[str(test_preflight_error.path)] = RuntimeError('pref')

    test_preflight_skip = _SimpleTest(tmp_path / 'preflight-skip.feature')
    engine.preflight_behavior[str(test_preflight_skip.path)] = TestPreflightDecision(
        status=TestResultStatus.SKIPPED,
        message='skipped in preflight',
    )

    node_a = _build_node(engine, test_preflight_error, index=1)
    node_b = _build_node(engine, test_preflight_skip, index=2)

    runner._scheduler = SimpleNamespace(
        build_plan=lambda *_args, **_kwargs: SchedulingPlan(
            worker_count=1,
            decisions=(
                SchedulingDecision(
                    node_id=node_a.id,
                    node_stable_id=node_a.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
                SchedulingDecision(
                    node_id=node_b.id,
                    node_stable_id=node_b.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
            ),
        ),
    )

    async def _prepare(*_args, **_kwargs) -> None:
        return None

    runner._runtime_provider = SimpleNamespace(
        scheduler_worker_count=lambda _config: 1,
        prepare=_prepare,
        bind_execution_slot=lambda *_args, **_kwargs: None,
        take_resource_timings=lambda: (),
        take_domain_events=lambda: (),
        take_log_events=lambda: (),
        describe_capabilities=lambda: (),
    )

    async def _drain(self) -> None:
        del self

    monkeypatch.setattr(Runner, '_drain_runtime_provider_observability', _drain)

    async def _execute(*_args, **_kwargs):
        return ExecutionBodyResult(
            report=TestReport(
                path='x',
                status=TestResultStatus.PASSED,
                message=None,
                duration=0.1,
            ),
        )

    monkeypatch.setattr(Runner, '_execute_with_scheduler_policy', _execute)

    async def _run() -> None:
        await runner.run_tests(execution_plan=(node_a, node_b))

    asyncio.run(_run())

    assert test_preflight_error.status is TestResultStatus.ERROR
    assert test_preflight_skip.status is TestResultStatus.SKIPPED


def test_runner_start_and_finish_session_error_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine_a = _EngineWithControls('a')
    engine_b = _EngineWithControls('b')
    runner = _build_runner(tmp_path, engines={'a': engine_a, 'b': engine_b})

    runner.telemetry_stream = _SimpleTelemetry()

    async def _start_plugins(self) -> None:
        self._started_plugins = [_NoopPlugin()]

    monkeypatch.setattr(Runner, '_start_plugins', _start_plugins)
    monkeypatch.setattr(Runner, '_bind_controller_shadow', lambda self: None)
    monkeypatch.setattr(Runner, '_reset_session_observability', lambda self: None)

    original_collect = _EngineWithControls.collect

    async def _collect(self, path=None, excluded_paths=()):
        if self is engine_b:
            msg = 'collect failed'
            raise RuntimeError(msg)
        await original_collect(self, path, excluded_paths)

    monkeypatch.setattr(_EngineWithControls, 'collect', _collect)

    with pytest.raises(RuntimeError, match='collect failed'):
        asyncio.run(runner.start_session(paths=('raise-collect',)))

    async def _finish_engine_session(self, _engine):
        del self
        msg = 'finish engine fail'
        raise RuntimeError(msg)

    monkeypatch.setattr(Runner, '_finish_engine_session', _finish_engine_session)
    monkeypatch.setattr(
        Runner,
        '_emit_runtime_domain_events',
        lambda self, _events: asyncio.sleep(0),
    )
    monkeypatch.setattr(
        Runner,
        '_flush_pending_live_log_events',
        lambda self: asyncio.sleep(0),
    )
    monkeypatch.setattr(
        Runner,
        '_print_session_report_summary',
        lambda self, _summary: None,
    )
    monkeypatch.setattr(Runner, '_persist_session_artifact', lambda self: None)
    monkeypatch.setattr(
        Runner,
        '_build_session_report_summary',
        lambda self, _max_failure_examples: SimpleNamespace(),
    )
    monkeypatch.setattr(
        Runner,
        '_unbind_controller_shadow',
        lambda self, preserve=False: None,
    )
    monkeypatch.setattr(
        Runner,
        '_reconcile_unfinished_collected_tests',
        lambda self, **_kwargs: None,
    )
    monkeypatch.setattr(Runner, 'has_failures', lambda self: True)

    runner._runtime_provider = SimpleNamespace(
        finish=lambda: asyncio.sleep(0),
        take_resource_timings=lambda: (),
        take_domain_events=lambda: (),
        should_preserve_shadow_context=lambda: False,
    )
    runner._resource_manager = SimpleNamespace(
        close=lambda: asyncio.sleep(0),
        merge_observed_timings=lambda *_args, **_kwargs: None,
    )
    runner._reporting_coordinator = SimpleNamespace(
        finish_extra_reporters=lambda: asyncio.sleep(0),
    )
    runner._domain_event_stream = SimpleNamespace(
        emit=lambda *_args, **_kwargs: asyncio.sleep(0),
        close=lambda: asyncio.sleep(0),
    )

    runner._started_engines = [engine_a]
    runner._before_session_hooks_ran = True
    runner._session_run_aborted = True

    with pytest.raises(RuntimeError, match='finish engine fail'):
        asyncio.run(runner.finish_session())


def test_runner_constructor_and_helper_error_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_root = tmp_path / 'missing-root'
    config = build_config(missing_root)
    with pytest.raises(Exception, match='does not exists'):
        Runner(config, {'': _EngineWithControls('engine')})

    from cosecha.core import runner as runner_module

    monkeypatch.setattr(
        runner_module,
        'iter_shell_reporting_contributions',
        lambda: (_StructuredReporter,),
    )
    assert Runner.default_console_reporter_type() is None

    engine = _EngineWithControls(
        'engine',
        capabilities=(_operation_descriptor('plan.analyze'),),
    )
    runner = _build_runner(tmp_path, engines={'': engine})
    runner.bind_scheduler(SimpleNamespace())
    assert runner.knowledge_base is runner._knowledge_base

    runner._require_all_engines_operation(
        operation_type='plan.analyze',
        capability_name='selection_labels',
    )
    runner._authorize_operation(AnalyzePlanOperation())

    skip_labels, execute_labels = runner._split_selection_labels(
        ('~slow', 'fast'),
    )
    assert skip_labels == ['slow']
    assert execute_labels == ['fast']

    with pytest.raises(ValueError, match='Invalid path selector'):
        runner._normalize_collect_paths(('~',))

    included, excluded = runner._normalize_collect_paths((tmp_path / 'a.feature',))
    assert included is not None
    assert excluded == ()

    with pytest.raises(ValueError, match='Invalid path'):
        runner._normalize_collect_paths((tmp_path.parent,))


def test_runner_live_snapshot_and_tail_branches(tmp_path: Path) -> None:
    runner = _build_runner(tmp_path)
    runner.config.persist_live_engine_snapshots = True

    ignored_event = EngineSnapshotUpdatedEvent(
        engine_name='engine',
        snapshot_kind='runtime',
        payload={'phase': 'call'},
        metadata=DomainEventMetadata(node_stable_id=None),
    )
    runner._record_live_engine_snapshot(ignored_event)
    assert runner._session_live_engine_snapshots == {}

    first_event = EngineSnapshotUpdatedEvent(
        engine_name='engine',
        snapshot_kind='runtime',
        payload={'phase': 'setup'},
        metadata=DomainEventMetadata(node_stable_id='stable-1', worker_id=1),
    )
    second_event = EngineSnapshotUpdatedEvent(
        engine_name='engine',
        snapshot_kind='runtime',
        payload={'phase': 'call'},
        metadata=DomainEventMetadata(node_stable_id='stable-1', worker_id=2),
    )
    runner._record_live_engine_snapshot(first_event)
    runner._record_live_engine_snapshot(second_event)

    summaries = runner._build_live_engine_snapshot_summaries()
    assert len(summaries) == 1
    assert summaries[0].update_count == 2

    event_a = SessionStartedEvent(
        root_path='.',
        concurrency=1,
        metadata=DomainEventMetadata(sequence_number=1, node_stable_id='stable-1'),
    )
    event_b = SessionStartedEvent(
        root_path='.',
        concurrency=1,
        metadata=DomainEventMetadata(sequence_number=2, node_stable_id='stable-2'),
    )
    live_snapshot = LiveExecutionSnapshot(
        last_sequence_number=2,
        engine_snapshots=(
            LiveEngineSnapshot(
                engine_name='engine',
                snapshot_kind='runtime',
                node_stable_id='stable-1',
                payload={'phase': 'call'},
                worker_id=1,
            ),
        ),
        recent_events=(event_a, event_b),
        recent_log_chunks=(
            LiveLogChunk(
                message='one',
                level='info',
                logger_name='runner',
                emitted_at=1.0,
                node_stable_id='stable-1',
                worker_id=1,
                sequence_number=1,
            ),
            LiveLogChunk(
                message='two',
                level='info',
                logger_name='runner',
                emitted_at=2.0,
                node_stable_id='stable-2',
                worker_id=2,
                sequence_number=2,
            ),
        ),
    )
    runner._knowledge_base = SimpleNamespace(live_snapshot=lambda: live_snapshot)
    filtered = runner._filter_live_snapshot(
        LiveStatusQuery(include_engine_snapshots=False),
    )
    assert filtered.engine_snapshots == ()

    events = runner._query_live_tail(
        QueryLiveTailOperation(query=DomainEventQuery(limit=1)),
        live_snapshot,
    )
    assert len(events) == 1
    assert events[0].metadata.sequence_number == 2

    chunks = runner._query_live_tail_log_chunks(
        QueryLiveTailOperation(query=DomainEventQuery(limit=1)),
        live_snapshot,
    )
    assert len(chunks) == 1
    assert chunks[0].sequence_number == 2


def test_runner_wait_and_live_log_event_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    runner._live_update_version = 1

    async def _slow_runtime_wait(_timeout: float) -> bool:
        await asyncio.sleep(5)
        return False

    runner._runtime_provider = SimpleNamespace(
        wait_for_live_observability=_slow_runtime_wait,
        live_execution_delivery_mode=lambda: 'poll_by_cursor',
        live_execution_granularity=lambda: 'streaming',
    )

    async def _exercise_wait() -> None:
        assert await runner._wait_for_live_activity(0, 0.05) is True

    asyncio.run(_exercise_wait())

    snapshots = [
        LiveExecutionSnapshot(last_sequence_number=0),
        LiveExecutionSnapshot(last_sequence_number=4),
    ]
    snapshot_calls = {'index': 0}

    async def _drain(self) -> None:
        del self

    async def _wait_for_activity(self, _version: int, _timeout: float) -> bool:
        del self
        return True

    def _filter(self, _query):
        del self
        index = min(snapshot_calls['index'], len(snapshots) - 1)
        snapshot_calls['index'] += 1
        return snapshots[index]

    monkeypatch.setattr(Runner, '_drain_runtime_provider_observability', _drain)
    monkeypatch.setattr(Runner, '_wait_for_live_activity', _wait_for_activity)
    monkeypatch.setattr(Runner, '_filter_live_snapshot', _filter)

    async def _exercise_subscription() -> None:
        result = await runner._execute_live_subscription_operation(
            QueryLiveSubscriptionOperation(
                query=LiveSubscriptionQuery(
                    after_sequence_number=3,
                    timeout_seconds=0.1,
                ),
            ),
        )
        assert result.next_sequence_number == 4

    asyncio.run(_exercise_subscription())

    emitted: list[LogChunkEvent] = []
    runner._domain_event_stream = SimpleNamespace(
        emit=lambda event: emitted.append(event) or asyncio.sleep(0),
    )

    async def _exercise_logs() -> None:
        context = runner._build_live_log_context(
            node_id='node-1',
            node_stable_id='stable-1',
            worker_id=1,
        )
        record = logging.LogRecord(
            name='runner',
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg='hello',
            args=(),
            exc_info=None,
        )
        runner._schedule_live_log_chunk_event(record, 'hello', context)
        await runner._flush_pending_live_log_events()
        assert emitted

    asyncio.run(_exercise_logs())


def test_runner_direct_phase_and_lifecycle_error_paths(
    tmp_path: Path,
) -> None:
    runner = _build_runner(tmp_path)
    runner.telemetry_stream = _SimpleTelemetry()

    async def _exercise() -> None:
        with pytest.raises(RuntimeError, match='phase boom'):
            await runner._run_test_phase(
                'failing-phase',
                lambda: asyncio.sleep(0, result=None)
                if False
                else (_ for _ in ()).throw(RuntimeError('phase boom')),
                parent_span_id='span',
                attributes={},
            )

    asyncio.run(_exercise())

    class _LegacyFinishEngine(_EngineWithControls):
        async def finish_test(self, test) -> None:  # type: ignore[override]
            del test
            msg = 'legacy finish boom'
            raise RuntimeError(msg)

    class _FailingStartEngine(_EngineWithControls):
        async def start_test(self, test) -> None:
            del test
            msg = 'start boom'
            raise RuntimeError(msg)

    legacy_engine = _LegacyFinishEngine('legacy')
    failing_start_engine = _FailingStartEngine('starter')
    sample_test = _SimpleTest(tmp_path / 'sample.feature')

    async def _exercise_engine_paths() -> None:
        with pytest.raises(RuntimeError, match='legacy finish boom'):
            await runner._finish_engine_test(legacy_engine, sample_test, report=None)
        with pytest.raises(RuntimeError, match='start boom'):
            await runner._start_engine_test(failing_start_engine, sample_test)

    asyncio.run(_exercise_engine_paths())

    async def _fail_finish_reporter(_engine) -> None:
        msg = 'finish reporter boom'
        raise RuntimeError(msg)

    runner._reporting_coordinator = SimpleNamespace(
        finish_engine_reporter=_fail_finish_reporter,
    )
    with pytest.raises(RuntimeError, match='finish reporter boom'):
        asyncio.run(runner._finish_engine_session(_EngineWithControls('x')))


def test_runner_run_tests_hook_and_phase_failure_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _EngineWithControls('engine')
    runner = _build_runner(tmp_path, engines={'': engine})
    runner.telemetry_stream = _SimpleTelemetry()

    test_skip = _SimpleTest(tmp_path / 'skip.feature')
    test_error = _SimpleTest(tmp_path / 'error.feature')
    node_skip = _build_node(engine, test_skip, index=1)
    node_error = _build_node(engine, test_error, index=2)

    class _HookWithBeforeErrors:
        async def before_test_run(self, test, _engine) -> None:
            if str(test.path).endswith('skip.feature'):
                raise Skipped('hook skipped')
            msg = 'hook boom'
            raise RuntimeError(msg)

        async def after_test_run(self, _test, _engine) -> None:
            return None

    runner.hooks = (_HookWithBeforeErrors(),)
    runner._scheduler = SimpleNamespace(
        build_plan=lambda *_args, **_kwargs: SchedulingPlan(
            worker_count=1,
            decisions=(
                SchedulingDecision(
                    node_id=node_skip.id,
                    node_stable_id=node_skip.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
                SchedulingDecision(
                    node_id=node_error.id,
                    node_stable_id=node_error.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
            ),
        ),
    )
    runner._runtime_provider = SimpleNamespace(
        scheduler_worker_count=lambda _config: 1,
        prepare=lambda *_args, **_kwargs: asyncio.sleep(0),
        bind_execution_slot=lambda *_args, **_kwargs: None,
        take_resource_timings=lambda: (),
        take_domain_events=lambda: (),
        take_log_events=lambda: (),
        describe_capabilities=lambda: (),
    )
    monkeypatch.setattr(
        Runner,
        '_drain_runtime_provider_observability',
        lambda self: asyncio.sleep(0),
    )
    monkeypatch.setattr(
        Runner,
        '_execute_with_scheduler_policy',
        lambda *_args, **_kwargs: asyncio.sleep(
            0,
            result=ExecutionBodyResult(
                report=TestReport(
                    path='x',
                    status=TestResultStatus.PASSED,
                    message=None,
                    duration=0.1,
                ),
            ),
        ),
    )

    asyncio.run(runner.run_tests(execution_plan=(node_skip, node_error)))
    assert test_skip.status is TestResultStatus.SKIPPED
    assert test_error.status is TestResultStatus.ERROR

    test_phase = _SimpleTest(tmp_path / 'phase.feature')
    node_phase = _build_node(engine, test_phase, index=3)
    runner.hooks = (_NoopPlugin(),)
    runner._scheduler = SimpleNamespace(
        build_plan=lambda *_args, **_kwargs: SchedulingPlan(
            worker_count=1,
            decisions=(
                SchedulingDecision(
                    node_id=node_phase.id,
                    node_stable_id=node_phase.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
            ),
        ),
    )

    async def _run_phase(self, name, callback, **_kwargs):
        del self
        if name == 'finish_test':
            msg = 'finish phase boom'
            raise RuntimeError(msg)
        if name == 'after_test_run_hooks':
            msg = 'after hook boom'
            raise RuntimeError(msg)
        await callback()
        return 0.01

    monkeypatch.setattr(Runner, '_run_test_phase', _run_phase)
    monkeypatch.setattr(
        Runner,
        '_execute_with_scheduler_policy',
        lambda *_args, **_kwargs: asyncio.sleep(
            0,
            result=ExecutionBodyResult(
                report=TestReport(
                    path='x',
                    status=TestResultStatus.PASSED,
                    message=None,
                    duration=0.1,
                ),
            ),
        ),
    )
    asyncio.run(runner.run_tests(execution_plan=(node_phase,)))
    assert test_phase.status is TestResultStatus.ERROR


def test_runner_control_plane_edge_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)

    class _DependencyEngine(_EngineWithControls):
        def describe_engine_dependencies(self):
            from cosecha.core.engine_dependencies import EngineDependencyRule

            return (
                EngineDependencyRule(
                    source_engine_name='unknown',
                    target_engine_name='engine',
                    dependency_kind='execution',
                    projection_policy='block_execution',
                ),
                EngineDependencyRule(
                    source_engine_name='engine',
                    target_engine_name='unknown',
                    dependency_kind='execution',
                    projection_policy='block_execution',
                ),
            )

    runner = _build_runner(tmp_path, engines={'': _DependencyEngine('engine')})
    assert runner.describe_engine_dependencies() == ()

    runner._knowledge_base = SimpleNamespace(
        snapshot=lambda: SimpleNamespace(latest_plan=None),
        query_tests=lambda _query: (
            SimpleNamespace(
                engine_name='engine',
                node_stable_id='stable',
                test_name='test',
                test_path='tests/x.feature',
                status='failed',
                plan_id='plan-1',
                trace_id='trace-1',
            ),
        ),
    )
    from cosecha.core.engine_dependencies import EngineDependencyRule, EngineDependencyQuery

    operation = QueryEngineDependenciesOperation(query=EngineDependencyQuery())
    assert runner._project_engine_dependency_issues((), operation) == ()

    rule = EngineDependencyRule(
        source_engine_name='engine',
        target_engine_name='aux',
        dependency_kind='execution',
        projection_policy='block_execution',
    )
    operation_with_plan = QueryEngineDependenciesOperation(
        query=EngineDependencyQuery(plan_id='plan-1'),
    )
    issues = runner._project_engine_dependency_issues((rule,), operation_with_plan)
    assert issues and issues[0].severity == 'error'

    with pytest.raises(ValueError, match='Unknown engine for draft validation'):
        asyncio.run(
            runner._execute_draft_validation_operation(
                DraftValidationOperation(
                    engine_name='missing',
                    test_path='draft.feature',
                    source_content='Feature: draft',
                ),
            ),
        )
    with pytest.raises(TypeError, match='does not support draft validation'):
        asyncio.run(
            runner._execute_draft_validation_operation(
                DraftValidationOperation(
                    engine_name='engine',
                    test_path='draft.feature',
                    source_content='Feature: draft',
                ),
            ),
        )
    with pytest.raises(ValueError, match='Unknown engine for definition resolution'):
        asyncio.run(
            runner._execute_definition_resolution_operation(
                ResolveDefinitionOperation(
                    engine_name='missing',
                    test_path='draft.feature',
                    step_type='given',
                    step_text='a step',
                ),
            ),
        )
    with pytest.raises(TypeError, match='does not support definition resolution'):
        asyncio.run(
            runner._execute_definition_resolution_operation(
                ResolveDefinitionOperation(
                    engine_name='engine',
                    test_path='draft.feature',
                    step_type='given',
                    step_text='a step',
                ),
            ),
        )

    monkeypatch.setattr(
        Runner,
        'describe_system_capabilities',
        lambda self: (
            SimpleNamespace(component_kind='engine', component_name='engine'),
            SimpleNamespace(component_kind='runtime', component_name='runtime'),
        ),
    )
    capability_result = runner._execute_query_capabilities_operation(
        QueryCapabilitiesOperation(component_name='engine'),
    )
    assert len(capability_result.snapshots) == 1

    monkeypatch.setattr(
        Runner,
        'describe_system_extensions',
        lambda self: (
            SimpleNamespace(
                component_name='engine',
                descriptor=SimpleNamespace(
                    extension_kind='resource',
                    canonical_name='x.y',
                    stability='stable',
                ),
            ),
            SimpleNamespace(
                component_name='runtime',
                descriptor=SimpleNamespace(
                    extension_kind='resource',
                    canonical_name='x.z',
                    stability='experimental',
                ),
            ),
        ),
    )
    extension_result = runner._execute_query_extensions_operation(
        QueryExtensionsOperation(
            query=SimpleNamespace(
                extension_kind='resource',
                component_name='engine',
                canonical_name='x.y',
                stability='stable',
            ),
        ),
    )
    assert len(extension_result.snapshots) == 1

    called: list[tuple[tuple[str, ...], int | None]] = []
    monkeypatch.setattr(
        Runner,
        'run_tests',
        lambda self, labels, limit, execution_plan=None: called.append(
            (tuple(labels or ()), limit),
        )
        or asyncio.sleep(0),
    )
    test = _SimpleTest(tmp_path / 'inject.feature')
    injected_plan = runner.build_injected_execution_plan(runner.engines[0], (test,))
    asyncio.run(
        runner.run_execution_plan(
            injected_plan,
            selection_labels=['fast'],
            test_limit=2,
        ),
    )
    assert called == [(('fast',), 2)]
    monkeypatch.setattr(runner.engines[0], 'is_file_collected', lambda _path: True)
    monkeypatch.setattr(runner.engines[0], 'is_file_failed', lambda _path: False)
    assert runner.find_engine(test.path) is not None

    analysis = PlanningAnalysis(mode='strict', plan=())
    monkeypatch.setattr(
        Runner,
        'build_execution_plan_analysis',
        lambda self, mode='strict': asyncio.sleep(0, result=analysis),
    )
    assert asyncio.run(runner.build_execution_plan(mode='strict')) == ()
    assert asyncio.run(runner.explain_execution_plan(mode='strict')) == analysis.explanation

    not_executable = PlanningAnalysis(
        mode='strict',
        plan=(),
        issues=(
            SimpleNamespace(
                severity='error',
                message='not executable',
            ),
        ),
    )
    monkeypatch.setattr(
        Runner,
        'build_execution_plan_analysis',
        lambda self, mode='strict': asyncio.sleep(0, result=not_executable),
    )
    with pytest.raises(ValueError, match='not executable'):
        asyncio.run(runner.build_execution_plan(mode='strict'))


def test_runner_additional_helper_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    runner.telemetry_stream = _SimpleTelemetry()

    queued_engine = _EngineWithControls('queued')
    queued_engine.reporter = QueuedReporter(NullReporter())
    runner._attach_default_console_reporter(queued_engine, _ConsoleReporter)
    assert isinstance(queued_engine.reporter, QueuedReporter)
    assert isinstance(queued_engine.reporter.descriptor_target(), _ConsoleReporter)

    runner._domain_event_node_stable_ids['node-1'] = 'stable-1'
    runner._last_plan_id = 'plan-1'
    metadata = runner._build_resource_event_metadata(object(), 'run', 'node-1')
    assert metadata.node_stable_id == 'stable-1'
    assert metadata.correlation_id == 'stable-1'
    fallback_metadata = runner._build_resource_event_metadata(object(), 'run', None)
    assert fallback_metadata.correlation_id == 'plan-1'

    outside_path = tmp_path.parent / 'outside'
    normalized = runner._normalize_snapshot_paths((outside_path,))
    assert normalized == (str(outside_path),)

    runner.engines[0].collector = SimpleNamespace(steps_directories={outside_path})
    assert runner._build_engine_step_directories(runner.engines[0]) == (
        str(outside_path),
    )

    class _BrokenConsole:
        def status(self, _message, **_kwargs):
            msg = 'unexpected'
            raise TypeError(msg)

    runner.console = _BrokenConsole()
    with pytest.raises(TypeError, match='unexpected'):
        runner._console_status('x', spinner='circle')

    async def _exercise_none_stream() -> None:
        runner._domain_event_stream = None
        context = runner._build_live_log_context(
            node_id='node-1',
            node_stable_id='stable-1',
            worker_id=1,
        )
        record = logging.LogRecord(
            name='runner',
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg='hello',
            args=(),
            exc_info=None,
        )
        runner._schedule_live_log_chunk_event(record, 'hello', context)
        await runner._flush_pending_live_log_events()

    asyncio.run(_exercise_none_stream())

    class _FailingPlugin(PlanMiddleware):
        @classmethod
        def register_arguments(cls, parser) -> None:
            del parser

        @classmethod
        def parse_args(cls, args):
            del args

        async def initialize(self, _context) -> None:
            msg = 'plugin init boom'
            raise RuntimeError(msg)

        async def start(self) -> None:
            return None

        async def finish(self) -> None:
            return None

    runner.plugins = (_FailingPlugin(),)
    runner._runtime_provider = SimpleNamespace(
        describe_capabilities=lambda: (),
        runtime_worker_model=lambda: 'single_process',
    )
    with pytest.raises(RuntimeError, match='plugin init boom'):
        asyncio.run(runner._start_plugins())


def test_runner_session_management_additional_error_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)

    async def _start_ok(self, _paths=None):
        del self, _paths
        return None

    async def _finish_fail(self):
        del self
        msg = 'finish failed'
        raise RuntimeError(msg)

    monkeypatch.setattr(Runner, 'start_session', _start_ok)
    monkeypatch.setattr(Runner, 'finish_session', _finish_fail)

    async def _ok_callback():
        return SimpleNamespace()

    runner._before_session_hooks_ran = True
    with pytest.raises(RuntimeError, match='finish failed'):
        asyncio.run(runner._run_with_session((), _ok_callback))


def test_runner_plan_and_scheduler_additional_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    engine = runner.engines[0]
    test = _SimpleTest(tmp_path / 'plan.feature')
    node = _build_node(engine, test, index=1)
    engine.get_collected_tests = lambda: (test,)  # type: ignore[method-assign]

    class _Middleware(_NoopPlugin, PlanMiddleware):
        async def transform_planning_analysis(self, analysis):
            return replace(analysis, issues=analysis.issues + (SimpleNamespace(code='x', message='x', severity='warning'),))

    runner._started_plugins = (_Middleware(),)

    async def _emit(_event):
        return None

    runner._domain_event_stream = SimpleNamespace(emit=_emit)
    analysis = asyncio.run(runner.build_execution_plan_analysis(mode='strict'))
    assert analysis.issues

    simulated_input = PlanningAnalysis(
        mode='relaxed',
        plan=(node,),
        node_semantics=(
            NodePlanningSemantics(
                node_id=node.id,
                node_stable_id=node.stable_id,
                engine_name=node.engine_name,
                runtime_hints=('hint',),
            ),
        ),
    )
    runner._runtime_provider = SimpleNamespace(
        scheduler_worker_count=lambda _config: 1,
    )
    runner._scheduler = SimpleNamespace(
        build_plan=lambda *_args, **_kwargs: SchedulingPlan(
            worker_count=1,
            decisions=(
                SchedulingDecision(
                    node_id=node.id,
                    node_stable_id=node.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
            ),
        ),
        should_retry=lambda *_args: False,
        backoff_for_attempt=lambda _attempt: 0.01,
    )
    monkeypatch.setattr(
        Runner,
        '_build_selected_plan_analysis',
        lambda self, **_kwargs: asyncio.sleep(0, result=simulated_input),
    )
    result = asyncio.run(runner._execute_simulate_plan_operation(SimulatePlanOperation()))
    assert result.explanation.mode == 'relaxed'

    async def _execute_timeout(*_args, **_kwargs):
        raise TimeoutError

    runner._runtime_provider = SimpleNamespace(
        execute=_execute_timeout,
    )
    monkeypatch.setattr(
        Runner,
        '_drain_runtime_provider_observability',
        lambda self: asyncio.sleep(0),
    )
    with pytest.raises(TimeoutError):
        asyncio.run(
            runner._execute_with_scheduler_policy(
                node,
                SchedulingDecision(
                    node_id=node.id,
                    node_stable_id=node.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                    timeout_seconds=None,
                ),
                test_span_id='span',
                node_attributes={},
            ),
        )


def test_runner_persist_artifact_and_summary_branches(
    tmp_path: Path,
) -> None:
    runner = _build_runner(tmp_path)
    runner.config.persist_live_engine_snapshots = True
    runner._session_live_engine_snapshots = {
        ('engine', 'runtime', 'stable-1'): SimpleNamespace(
            engine_name='engine',
            snapshot_kind='runtime',
            node_stable_id='stable-1',
            last_updated_at=1.0,
        ),
    }

    lines: list[str] = []
    summary = SimpleNamespace(
        total_tests=1,
        status_counts=(('passed', 1), ('failed', 0), ('error', 0), ('skipped', 0), ('pending', 0)),
        failed_files=(),
        failed_examples=(),
        failure_kind_counts=(('runtime', 1),),
        live_engine_snapshots=(),
        engine_summaries=(
            SimpleNamespace(
                engine_name='engine',
                total_tests=1,
                status_counts=(('passed', 1), ('failed', 0), ('error', 0), ('skipped', 0), ('pending', 0)),
                detail_counts=(('detail', 1),),
                failure_kind_counts=(('runtime', 1),),
                failed_examples=(),
                failed_files=(),
            ),
        ),
        instrumentation_summaries={},
    )
    runner.console = SimpleNamespace(is_debug_mode=lambda: True)
    runner._append_engine_session_summary(lines, summary.engine_summaries[0])
    assert lines
    assert runner._should_render_verbose_session_summary() is True

    runner._domain_event_session_id = None
    runner._persist_session_artifact()
    runner._domain_event_session_id = 'session'
    runner.config = SimpleNamespace(root_path=tmp_path)
    runner._persist_session_artifact()


def test_runner_remaining_simple_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)

    plain_engine = _EngineWithControls('plain')
    plain_engine.reporter = NullReporter()
    runner._attach_default_console_reporter(plain_engine, _ConsoleReporter)
    assert isinstance(plain_engine.reporter, _ConsoleReporter)

    live_snapshot = LiveExecutionSnapshot(
        recent_log_chunks=(
            LiveLogChunk(
                message='one',
                level='info',
                logger_name='runner',
                emitted_at=1.0,
                sequence_number=1,
            ),
        ),
    )
    unsliced = runner._query_live_tail_log_chunks(
        QueryLiveTailOperation(query=DomainEventQuery(limit=5)),
        live_snapshot,
    )
    assert len(unsliced) == 1

    async def _wait_local_update() -> None:
        async def _tick():
            await asyncio.sleep(0)
            async with runner._live_update_condition:
                runner._live_update_version = 2
                runner._live_update_condition.notify_all()

        task = asyncio.create_task(_tick())
        assert await runner._wait_for_local_live_update(1, 0.1) is True
        await task

    asyncio.run(_wait_local_update())

    monkeypatch.setattr(
        Runner,
        'build_execution_plan_analysis',
        lambda self: asyncio.sleep(
            0,
            result=PlanningAnalysis(
                mode='strict',
                plan=(),
                issues=(SimpleNamespace(message='not executable', severity='error'),),
            ),
        ),
    )
    with pytest.raises(ValueError, match='not executable'):
        asyncio.run(runner.run_tests())

    pending = _SimpleTest(tmp_path / 'pending.feature')
    passed = _SimpleTest(tmp_path / 'passed.feature')
    passed.status = TestResultStatus.PASSED
    runner.engines[0].get_collected_tests = lambda: (passed, pending)  # type: ignore[method-assign]
    runner._reconcile_unfinished_collected_tests(
        status=TestResultStatus.SKIPPED,
        message='done',
    )
    assert passed.status is TestResultStatus.PASSED
    assert pending.status is TestResultStatus.SKIPPED
    runner._reconcile_unfinished_collected_tests(
        status=TestResultStatus.ERROR,
        message='err',
        failure_kind='runtime',
        error_code='E',
    )
    assert pending.failure_kind is None

    runner._controller_shadow_binding = object()
    runner._bind_controller_shadow()
    assert runner._controller_shadow_binding is not None

    monkeypatch.setattr(
        Runner,
        '_ephemeral_capability_from_component',
        lambda self, _type, descriptors: descriptors[0] if descriptors else None,
    )
    runner.engines = (
        _EngineWithControls('engine', capabilities=(_operation_descriptor('x'),)),
    )
    runner.plugins = (
        SimpleNamespace(describe_capabilities=lambda: (_operation_descriptor('p'),)),
    )
    runner._runtime_provider = SimpleNamespace(
        describe_capabilities=lambda: (_operation_descriptor('r'),),
    )
    runner.engines[0].reporter = SimpleNamespace(
        descriptor_target=lambda: SimpleNamespace(
            describe_capabilities=lambda: (_operation_descriptor('rep'),),
        ),
    )
    assert tuple(runner._iter_active_ephemeral_capabilities())

    analysis = PlanningAnalysis(
        mode='strict',
        plan=(),
        node_semantics=(
            NodePlanningSemantics(
                node_id='n',
                node_stable_id='s',
                engine_name='e',
                runtime_hints=('base',),
            ),
        ),
    )
    enriched = runner._apply_knowledge_estimates(
        analysis,
        SimpleNamespace(
            query_tests=lambda _query: (SimpleNamespace(duration=1.25),),
        ),
    )
    assert any('estimated_duration=' in hint for hint in enriched.node_semantics[0].runtime_hints)

    event = SessionStartedEvent(root_path='.', concurrency=1, metadata=DomainEventMetadata())
    runner._domain_event_session_id = 'session-1'
    runner._last_plan_id = 'plan-1'
    runner.telemetry_stream = _SimpleTelemetry()
    runner._normalize_runtime_domain_event(event)
    assert event.metadata.session_id == 'session-1'

    writer_calls: list[tuple[object, object]] = []
    runner._knowledge_base = InMemoryKnowledgeBase()
    runner._session_artifact_metadata_writer = (
        lambda artifact, db_path: writer_calls.append((artifact, db_path))
    )
    runner._domain_event_session_id = 'session-1'
    runner._last_plan_id = None
    runner.session_timing = None
    runner.config = build_config(tmp_path)
    monkeypatch.setattr(Runner, 'has_failures', lambda self: False)
    monkeypatch.setattr(Runner, 'describe_system_capabilities', lambda self: ())
    monkeypatch.setattr(Runner, '_build_session_report_summary', lambda self, _max: SimpleNamespace(
        total_tests=0,
        status_counts=(),
        failure_kind_counts=(),
        engine_summaries=(),
        live_engine_snapshots=(),
        failed_examples=(),
        failed_files=(),
        instrumentation_summaries={},
    ))
    monkeypatch.setattr(Runner, '_build_session_telemetry_summary', lambda self: SimpleNamespace(
        span_count=0,
        distinct_span_names=0,
        top_span_names=(),
    ))
    runner._persist_session_artifact()
    assert writer_calls and writer_calls[0][1] is None

    summary = SimpleNamespace(
        total_tests=1,
        status_counts=(('passed', 1), ('failed', 0), ('error', 0), ('skipped', 0), ('pending', 0)),
        failed_files=(),
        failed_examples=(),
        failure_kind_counts=(),
        live_engine_snapshots=(),
        engine_summaries=(
            SimpleNamespace(
                engine_name='engine',
                total_tests=1,
                status_counts=(('passed', 1), ('failed', 0), ('error', 0), ('skipped', 0), ('pending', 0)),
                detail_counts=(),
                failure_kind_counts=(),
                failed_examples=(),
                failed_files=(),
            ),
        ),
        instrumentation_summaries={},
    )
    runner.console = SimpleNamespace(
        is_debug_mode=lambda: True,
        print_summary=lambda *_args, **_kwargs: None,
    )
    runner._print_session_report_summary(summary)

    monkeypatch.setattr(
        Runner,
        'build_execution_plan_analysis',
        lambda self, mode='strict': asyncio.sleep(0, result=analysis),
    )
    monkeypatch.setattr(
        Runner,
        '_select_execution_nodes',
        lambda self, _plan, **_kwargs: (),
    )
    selected = asyncio.run(
        runner._build_selected_plan_analysis(
            mode='strict',
            selection_labels=('fast',),
            test_limit=None,
        ),
    )
    assert isinstance(selected, PlanningAnalysis)

    monkeypatch.setattr(
        'cosecha.core.runner.parse_cosecha_manifest_text',
        lambda *_args, **_kwargs: None,
    )
    valid_result = asyncio.run(
        runner._execute_draft_validation_operation(
            DraftValidationOperation(
                engine_name='engine',
                test_path='cosecha.toml',
                source_content='schema_version = 1',
            ),
        ),
    )
    assert valid_result.validation.test_count == 0
    monkeypatch.setattr(runner.engines[0], 'is_file_collected', lambda _path: False)
    monkeypatch.setattr(runner.engines[0], 'is_file_failed', lambda _path: False)
    assert runner.find_engine(tmp_path / 'unknown.feature') is None


def test_runner_start_session_collect_excluded_and_cancel_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled = {'slow': False}
    collect_calls: list[tuple[object, tuple[Path, ...]]] = []

    class _SlowEngine(_EngineWithControls):
        async def collect(self, path=None, excluded_paths=()):
            collect_calls.append((path, excluded_paths))
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                cancelled['slow'] = True
                raise

    class _FailingEngine(_EngineWithControls):
        async def collect(self, path=None, excluded_paths=()):
            collect_calls.append((path, excluded_paths))
            msg = 'collect boom'
            raise RuntimeError(msg)

    runner = _build_runner(
        tmp_path,
        engines={'slow': _SlowEngine('slow'), 'fail': _FailingEngine('fail')},
    )
    monkeypatch.setattr(Runner, '_reset_session_observability', lambda self: None)
    monkeypatch.setattr(Runner, '_bind_controller_shadow', lambda self: None)
    monkeypatch.setattr(Runner, '_start_log_capture', lambda self: None)
    monkeypatch.setattr(Runner, '_start_plugins', lambda self: asyncio.sleep(0))
    runner._reporting_coordinator = SimpleNamespace(
        start_extra_reporters=lambda: asyncio.sleep(0),
    )
    runner._runtime_provider = SimpleNamespace(start=lambda: asyncio.sleep(0))

    with pytest.raises(RuntimeError, match='collect boom'):
        asyncio.run(runner.start_session(paths=('tests', '~tests/excluded')))

    assert cancelled['slow'] is True
    assert collect_calls


def test_runner_finish_session_and_persistent_artifact_writer_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_persist_session_artifact = Runner._persist_session_artifact

    monkeypatch.setattr(
        Runner,
        '_flush_pending_live_log_events',
        lambda self: asyncio.sleep(0),
    )
    monkeypatch.setattr(Runner, '_print_session_report_summary', lambda self, _summary: None)
    monkeypatch.setattr(Runner, '_build_session_report_summary', lambda self, _max: SimpleNamespace())
    monkeypatch.setattr(Runner, '_persist_session_artifact', lambda self: None)
    monkeypatch.setattr(Runner, '_unbind_controller_shadow', lambda self, preserve=False: None)
    monkeypatch.setattr(Runner, '_reconcile_unfinished_collected_tests', lambda self, **kwargs: None)
    monkeypatch.setattr(Runner, 'has_failures', lambda self: False)

    def _build_finish_runner() -> Runner:
        local_runner = _build_runner(tmp_path)
        local_runner._started_engines = [local_runner.engines[0]]
        local_runner._before_session_hooks_ran = True
        local_runner._runtime_provider = SimpleNamespace(
            finish=lambda: asyncio.sleep(0),
            take_resource_timings=lambda: (),
            take_domain_events=lambda: (),
            should_preserve_shadow_context=lambda: False,
        )
        local_runner._resource_manager = SimpleNamespace(
            close=lambda: asyncio.sleep(0),
            merge_observed_timings=lambda *_args, **_kwargs: None,
        )
        local_runner._domain_event_stream = SimpleNamespace(
            emit=lambda *_args, **_kwargs: asyncio.sleep(0),
            close=lambda: asyncio.sleep(0),
        )
        local_runner.telemetry_stream = _SimpleTelemetry()
        return local_runner

    runner = _build_finish_runner()
    runner._reporting_coordinator = SimpleNamespace(
        finish_engine_reporter=lambda *_args, **_kwargs: asyncio.sleep(0),
        finish_extra_reporters=lambda: (_ for _ in ()).throw(RuntimeError('extra boom')),
    )
    with pytest.raises(RuntimeError, match='extra boom'):
        asyncio.run(runner.finish_session())

    runner = _build_finish_runner()
    runner.hooks = (
        SimpleNamespace(after_session_finish=lambda: asyncio.sleep(0, result=RuntimeError('hook boom'))),
    )
    runner._reporting_coordinator = SimpleNamespace(
        finish_engine_reporter=lambda *_args, **_kwargs: asyncio.sleep(0),
        finish_extra_reporters=lambda: asyncio.sleep(0),
    )
    with pytest.raises(RuntimeError, match='hook boom'):
        asyncio.run(runner.finish_session())

    runner = _build_finish_runner()
    runner._reporting_coordinator = SimpleNamespace(
        finish_engine_reporter=lambda *_args, **_kwargs: asyncio.sleep(0),
        finish_extra_reporters=lambda: asyncio.sleep(0),
    )
    runner._started_plugins = [
        SimpleNamespace(
            __class__=SimpleNamespace(__name__='Plugin'),
            plugin_name=lambda: 'plugin',
            finish_priority=lambda: 0,
            finish=lambda: (_ for _ in ()).throw(RuntimeError('plugin boom')),
            after_session_closed=lambda: asyncio.sleep(0),
        ),
    ]
    with pytest.raises(RuntimeError, match='plugin boom'):
        asyncio.run(runner.finish_session())

    runner = _build_finish_runner()
    runner._reporting_coordinator = SimpleNamespace(
        finish_engine_reporter=lambda *_args, **_kwargs: asyncio.sleep(0),
        finish_extra_reporters=lambda: asyncio.sleep(0),
    )
    runner._stop_on_error_triggered = True
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        Runner,
        '_reconcile_unfinished_collected_tests',
        lambda self, **kwargs: calls.append(kwargs),
    )
    asyncio.run(runner.finish_session())
    assert calls and calls[0]['status'] is TestResultStatus.SKIPPED

    runner = _build_finish_runner()
    runner._reporting_coordinator = SimpleNamespace(
        finish_engine_reporter=lambda *_args, **_kwargs: asyncio.sleep(0),
        finish_extra_reporters=lambda: asyncio.sleep(0),
    )

    @asynccontextmanager
    async def _span(*_args, **_kwargs):
        yield 'span'

    runner.telemetry_stream = SimpleNamespace(
        span=_span,
        flush=lambda: asyncio.sleep(0),
        close=lambda: (_ for _ in ()).throw(RuntimeError('telemetry boom')),
        trace_id='trace-1',
    )
    with pytest.raises(RuntimeError, match='telemetry boom'):
        asyncio.run(runner.finish_session())

    writer_calls: list[tuple[object, object]] = []
    runner = _build_runner(tmp_path)
    monkeypatch.setattr(Runner, '_persist_session_artifact', original_persist_session_artifact)
    runner._knowledge_base = PersistentKnowledgeBase(tmp_path / '.cosecha' / 'kb.db')
    runner._session_artifact_metadata_writer = (
        lambda artifact, db_path: writer_calls.append((artifact, db_path))
    )
    monkeypatch.setattr(Runner, 'has_failures', lambda self: False)
    monkeypatch.setattr(Runner, 'describe_system_capabilities', lambda self: ())
    monkeypatch.setattr(
        Runner,
        '_build_session_report_summary',
        lambda self, _max: SimpleNamespace(
            total_tests=0,
            status_counts=(),
            failure_kind_counts=(),
            engine_summaries=(),
            live_engine_snapshots=(),
            failed_examples=(),
            failed_files=(),
            instrumentation_summaries={},
            to_dict=lambda: {
                'total_tests': 0,
                'status_counts': [],
                'failure_kind_counts': [],
                'engine_summaries': [],
                'live_engine_snapshots': [],
                'failed_examples': [],
                'failed_files': [],
                'instrumentation_summaries': {},
            },
        ),
    )
    monkeypatch.setattr(
        Runner,
        '_build_session_telemetry_summary',
        lambda self: SimpleNamespace(
            span_count=0,
            distinct_span_names=0,
            top_span_names=(),
            to_dict=lambda: {
                'span_count': 0,
                'distinct_span_names': 0,
                'top_span_names': [],
            },
        ),
    )
    runner._persist_session_artifact()
    assert writer_calls and writer_calls[0][1] is not None


def test_runner_run_tests_failure_and_scheduler_retry_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    runner.telemetry_stream = _SimpleTelemetry()
    engine = runner.engines[0]
    test = _SimpleTest(tmp_path / 'test.feature')
    node = _build_node(engine, test, index=1)
    runner._runtime_provider = SimpleNamespace(
        scheduler_worker_count=lambda _config: 1,
        prepare=lambda *_args, **_kwargs: asyncio.sleep(0),
        bind_execution_slot=lambda *_args, **_kwargs: None,
        take_resource_timings=lambda: (),
        take_domain_events=lambda: (),
        take_log_events=lambda: (),
        describe_capabilities=lambda: (),
        legacy_session_scope=lambda: 'run',
    )
    runner._scheduler = SimpleNamespace(
        build_plan=lambda *_args, **_kwargs: SchedulingPlan(
            worker_count=1,
            decisions=(
                SchedulingDecision(
                    node_id=node.id,
                    node_stable_id=node.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
            ),
        ),
    )
    monkeypatch.setattr(
        Runner,
        '_drain_runtime_provider_observability',
        lambda self: asyncio.sleep(0),
    )

    async def _phase_start_error(self, name, callback, **_kwargs):
        del self
        if name == 'start_test':
            msg = 'start phase boom'
            raise RuntimeError(msg)
        await callback()
        return 0.01

    monkeypatch.setattr(Runner, '_run_test_phase', _phase_start_error)
    asyncio.run(runner.run_tests(execution_plan=(node,)))
    assert test.status is TestResultStatus.ERROR

    async def _phase_finish_error(self, name, callback, **_kwargs):
        del self
        if name == 'finish_test':
            msg = 'finish phase boom'
            raise RuntimeError(msg)
        await callback()
        return 0.01

    monkeypatch.setattr(Runner, '_run_test_phase', _phase_finish_error)
    original_policy = Runner._execute_with_scheduler_policy

    async def _body_pass(*_args, **_kwargs):
        return ExecutionBodyResult(
            report=TestReport(
                path='x',
                status=TestResultStatus.PASSED,
                message=None,
                duration=0.1,
            ),
        )

    monkeypatch.setattr(
        Runner,
        '_execute_with_scheduler_policy',
        _body_pass,
    )
    test.status = TestResultStatus.PENDING
    asyncio.run(runner.run_tests(execution_plan=(node,)))
    assert test.status is TestResultStatus.ERROR
    monkeypatch.setattr(Runner, '_execute_with_scheduler_policy', original_policy)

    retry_runner = _build_runner(tmp_path)
    retry_node = _build_node(retry_runner.engines[0], _SimpleTest(tmp_path / 'retry.feature'), index=2)
    retry_runner._domain_event_stream = SimpleNamespace(emit=lambda *_a, **_k: asyncio.sleep(0))

    async def _raise_retry(*_args, **_kwargs):
        msg = 'retry boom'
        raise RuntimeError(msg)

    retry_runner._runtime_provider = SimpleNamespace(
        execute=_raise_retry,
    )
    retry_flags = iter((True, False))
    retry_runner._scheduler = SimpleNamespace(
        should_retry=lambda *_args: next(retry_flags),
        backoff_for_attempt=lambda _attempt: 0.01,
    )
    monkeypatch.setattr(
        Runner,
        '_drain_runtime_provider_observability',
        lambda self: asyncio.sleep(0),
    )
    original_sleep = asyncio.sleep
    monkeypatch.setattr(
        asyncio,
        'sleep',
        lambda _seconds, *args, **kwargs: original_sleep(0, *args, **kwargs),
    )
    with pytest.raises(RuntimeError, match='retry boom'):
        asyncio.run(
            retry_runner._execute_with_scheduler_policy(
                retry_node,
                SchedulingDecision(
                    node_id=retry_node.id,
                    node_stable_id=retry_node.stable_id,
                    worker_slot=0,
                    max_attempts=2,
                ),
                test_span_id='span',
                node_attributes={},
            ),
        )


def test_runner_start_session_excluded_collect_success_branch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    runner.telemetry_stream = _SimpleTelemetry()
    collect_calls: list[tuple[object, tuple[Path, ...]]] = []

    async def _collect(path=None, excluded_paths=()):
        collect_calls.append((path, excluded_paths))

    monkeypatch.setattr(runner.engines[0], 'collect', _collect)
    monkeypatch.setattr(Runner, '_reset_session_observability', lambda self: None)
    monkeypatch.setattr(Runner, '_bind_controller_shadow', lambda self: None)
    monkeypatch.setattr(Runner, '_start_log_capture', lambda self: None)
    monkeypatch.setattr(Runner, '_start_plugins', lambda self: asyncio.sleep(0))
    runner._reporting_coordinator = SimpleNamespace(
        start_extra_reporters=lambda: asyncio.sleep(0),
        start_engine_reporter=lambda *_args, **_kwargs: asyncio.sleep(0),
    )
    runner._runtime_provider = SimpleNamespace(start=lambda: asyncio.sleep(0))
    runner._domain_event_stream = SimpleNamespace(emit=lambda *_args, **_kwargs: asyncio.sleep(0))

    asyncio.run(runner.start_session(paths=('tests', '~tests/excluded')))
    assert collect_calls
    assert collect_calls[0][1]


def test_runner_run_tests_stop_execution_short_circuit_and_worker_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import cosecha.core.runner as runner_module

    runner = _build_runner(tmp_path, stop_on_error=True)
    runner.telemetry_stream = _SimpleTelemetry()
    engine = runner.engines[0]
    first_test = _SimpleTest(tmp_path / 'first.feature')
    second_test = _SimpleTest(tmp_path / 'second.feature')
    first_node = _build_node(engine, first_test, index=1)
    second_node = _build_node(engine, second_test, index=2)
    execute_calls: list[str] = []

    async def _execute(node, _body):
        execute_calls.append(node.id)
        if node.id != first_node.id:
            msg = 'second node must be skipped'
            raise AssertionError(msg)
        return ExecutionBodyResult(
            report=TestReport(
                path='x',
                status=TestResultStatus.FAILED,
                message='failed',
                duration=0.1,
            ),
        )

    runner._runtime_provider = SimpleNamespace(
        scheduler_worker_count=lambda _config: 1,
        prepare=lambda *_args, **_kwargs: asyncio.sleep(0),
        bind_execution_slot=lambda *_args, **_kwargs: None,
        execute=_execute,
        take_resource_timings=lambda: (),
        take_domain_events=lambda: (),
        take_log_events=lambda: (),
        describe_capabilities=lambda: (),
    )
    runner._scheduler = SimpleNamespace(
        build_plan=lambda *_args, **_kwargs: SchedulingPlan(
            worker_count=1,
            decisions=(
                SchedulingDecision(
                    node_id=first_node.id,
                    node_stable_id=first_node.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
                SchedulingDecision(
                    node_id=second_node.id,
                    node_stable_id=second_node.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
            ),
        ),
        should_retry=lambda *_args, **_kwargs: False,
    )
    runner._domain_event_stream = SimpleNamespace(emit=lambda *_args, **_kwargs: asyncio.sleep(0))
    monkeypatch.setattr(
        Runner,
        '_drain_runtime_provider_observability',
        lambda self: asyncio.sleep(0),
    )

    class _StopAwareAssignmentState:
        def __init__(self, decisions, *, pinned_node_ids=()):
            del pinned_node_ids
            self._decisions = tuple(decisions)
            self._index = 0

        def claim_next(self, worker_slot: int):
            del worker_slot
            if self._index < len(self._decisions):
                decision = self._decisions[self._index]
                self._index += 1
                return decision
            return None

        def complete(self, _job) -> None:
            return None

        def is_complete(self) -> bool:
            return False

        def has_pending(self) -> bool:
            return False

    monkeypatch.setattr(
        runner_module,
        'RuntimeAssignmentState',
        _StopAwareAssignmentState,
    )

    asyncio.run(runner.run_tests(execution_plan=(first_node, second_node)))
    assert execute_calls == [first_node.id]
    assert second_test.status is TestResultStatus.PENDING


def test_runner_run_tests_worker_pending_and_idle_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import cosecha.core.runner as runner_module

    runner = _build_runner(tmp_path)
    runner.telemetry_stream = _SimpleTelemetry()
    node = _build_node(runner.engines[0], _SimpleTest(tmp_path / 'idle.feature'), index=1)

    runner._runtime_provider = SimpleNamespace(
        scheduler_worker_count=lambda _config: 1,
        prepare=lambda *_args, **_kwargs: asyncio.sleep(0),
        bind_execution_slot=lambda *_args, **_kwargs: None,
        execute=lambda *_args, **_kwargs: asyncio.sleep(0),
        take_resource_timings=lambda: (),
        take_domain_events=lambda: (),
        take_log_events=lambda: (),
        describe_capabilities=lambda: (),
    )
    runner._scheduler = SimpleNamespace(
        build_plan=lambda *_args, **_kwargs: SchedulingPlan(
            worker_count=1,
            decisions=(
                SchedulingDecision(
                    node_id=node.id,
                    node_stable_id=node.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
            ),
        ),
        should_retry=lambda *_args, **_kwargs: False,
    )
    runner._domain_event_stream = SimpleNamespace(emit=lambda *_args, **_kwargs: asyncio.sleep(0))
    monkeypatch.setattr(
        Runner,
        '_drain_runtime_provider_observability',
        lambda self: asyncio.sleep(0),
    )

    class _PendingThenIdleAssignmentState:
        def __init__(self, _decisions, *, pinned_node_ids=()):
            del pinned_node_ids
            self._claim_calls = 0

        def claim_next(self, worker_slot: int):
            del worker_slot
            self._claim_calls += 1
            return None

        def complete(self, _job) -> None:
            msg = 'no job should complete'
            raise AssertionError(msg)

        def is_complete(self) -> bool:
            return False

        def has_pending(self) -> bool:
            return self._claim_calls == 1

    monkeypatch.setattr(
        runner_module,
        'RuntimeAssignmentState',
        _PendingThenIdleAssignmentState,
    )

    asyncio.run(runner.run_tests(execution_plan=(node,)))


def test_runner_after_test_hook_failure_sets_error_for_passed_test(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _build_runner(tmp_path)
    runner.telemetry_stream = _SimpleTelemetry()
    node = _build_node(
        runner.engines[0],
        _SimpleTest(tmp_path / 'after-hook.feature'),
        index=1,
    )

    class _AfterHookFailure:
        async def before_test_run(self, _test, _engine) -> None:
            return None

        async def after_test_run(self, _test, _engine) -> None:
            msg = 'after hook boom'
            raise RuntimeError(msg)

    runner.hooks = (_AfterHookFailure(),)
    runner._runtime_provider = SimpleNamespace(
        scheduler_worker_count=lambda _config: 1,
        prepare=lambda *_args, **_kwargs: asyncio.sleep(0),
        bind_execution_slot=lambda *_args, **_kwargs: None,
        take_resource_timings=lambda: (),
        take_domain_events=lambda: (),
        take_log_events=lambda: (),
        describe_capabilities=lambda: (),
    )
    runner._scheduler = SimpleNamespace(
        build_plan=lambda *_args, **_kwargs: SchedulingPlan(
            worker_count=1,
            decisions=(
                SchedulingDecision(
                    node_id=node.id,
                    node_stable_id=node.stable_id,
                    worker_slot=0,
                    max_attempts=1,
                ),
            ),
        ),
        should_retry=lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        Runner,
        '_drain_runtime_provider_observability',
        lambda self: asyncio.sleep(0),
    )
    monkeypatch.setattr(
        Runner,
        '_execute_with_scheduler_policy',
        lambda *_args, **_kwargs: asyncio.sleep(
            0,
            result=ExecutionBodyResult(
                report=TestReport(
                    path='x',
                    status=TestResultStatus.PASSED,
                    message=None,
                    duration=0.1,
                ),
            ),
        ),
    )

    asyncio.run(runner.run_tests(execution_plan=(node,)))
    assert node.test.status is TestResultStatus.ERROR
    assert node.test.message == 'Error in after_test_run hook'
    assert node.test.failure_kind == 'hook'
