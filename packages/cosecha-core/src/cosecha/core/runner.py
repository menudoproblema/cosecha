from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import sys
import time

from dataclasses import replace
from math import inf
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from cosecha.core.capabilities import (
    CAPABILITY_DRAFT_VALIDATION,
    CAPABILITY_LIVE_EXECUTION_OBSERVABILITY,
    CAPABILITY_PLAN_EXPLANATION,
    CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
    CAPABILITY_SELECTION_LABELS,
    CapabilityComponentSnapshot,
    DefinitionResolvingEngine,
    DraftValidatingEngine,
    DraftValidationIssue,
    DraftValidationResult,
    build_capability_map,
    build_component_capability_snapshot,
)
from cosecha.core.capture import CapturedLogContext, CaptureLogHandler
from cosecha.core.cosecha_manifest import (
    ManifestValidationError,
    parse_cosecha_manifest_text,
)
from cosecha.core.discovery import (
    iter_console_presenter_contributions,
    iter_shell_reporting_contributions,
)
from cosecha.core.domain_event_stream import DomainEventStream
from cosecha.core.domain_events import (
    DomainEvent,
    DomainEventMetadata,
    EngineSnapshotUpdatedEvent,
    LogChunkEvent,
    NodeAssignedEvent,
    NodeEnqueuedEvent,
    NodeRequeuedEvent,
    NodeRetryingEvent,
    NodeScheduledEvent,
    PlanAnalyzedEvent,
    SessionFinishedEvent,
    SessionStartedEvent,
    TestFinishedEvent,
    TestStartedEvent,
    build_domain_event_id,
)
from cosecha.core.engine_dependencies import (
    EngineDependencyRule,
    ProjectedEngineDependencyIssue,
    build_engine_dependency_rule_key,
)
from cosecha.core.exceptions import Skipped
from cosecha.core.execution_ir import (
    PlanningAnalysis,
    PlanningIssue,
    PlanningMode,
    TestExecutionNode,
    analyze_execution_plan,
    build_execution_node_id,
    build_execution_node_stable_id,
    build_test_path_label,
    filter_execution_nodes,
    validate_execution_plan,
)
from cosecha.core.execution_runtime import (
    ExecutionBodyOptions,
    execute_test_body,
)
from cosecha.core.extensions import (
    ExtensionComponentSnapshot,
    build_engine_extension_snapshot,
    build_plugin_extension_snapshot,
    build_reporter_extension_snapshot,
    build_runtime_extension_snapshot,
)
from cosecha.core.items import (
    ExecutionPredicateEvaluation,
    TestResultStatus,
    resolve_failure_kind,
)
from cosecha.core.knowledge_base import (
    DomainEventQuery,
    KnowledgeBase,
    KnowledgeBaseDomainEventSink,
    LiveExecutionSnapshot,
    PersistentKnowledgeBase,
    TestKnowledgeQuery,
    resolve_knowledge_base_path,
)
from cosecha.core.operations import (
    AnalyzePlanOperation,
    AnalyzePlanOperationResult,
    DraftValidationOperation,
    DraftValidationOperationResult,
    ExplainPlanOperation,
    ExplainPlanOperationResult,
    HypotheticalSchedulingDecision,
    KnowledgeQueryContext,
    LiveExecutionContext,
    Operation,
    OperationResult,
    QueryCapabilitiesOperation,
    QueryCapabilitiesOperationResult,
    QueryDefinitionsOperation,
    QueryDefinitionsOperationResult,
    QueryEngineDependenciesOperation,
    QueryEngineDependenciesOperationResult,
    QueryEventsOperation,
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
    QueryRegistryItemsOperationResult,
    QueryResourcesOperation,
    QueryResourcesOperationResult,
    QuerySessionArtifactsOperation,
    QuerySessionArtifactsOperationResult,
    QueryTestsOperation,
    QueryTestsOperationResult,
    ResolveDefinitionOperation,
    ResolveDefinitionOperationResult,
    RunOperation,
    RunOperationResult,
    SimulatePlanOperation,
    SimulatePlanOperationResult,
)
from cosecha.core.plugins.base import PlanMiddleware, PluginContext
from cosecha.core.reporter import NullReporter, QueuedReporter, Reporter
from cosecha.core.reporting_coordinator import ReportingCoordinator
from cosecha.core.reporting_ir import ensure_test_report, reconcile_test_report
from cosecha.core.resources import ResourceManager, normalize_resource_scope
from cosecha.core.runtime import (
    LocalRuntimeProvider,
    RuntimeInfrastructureError,
    RuntimeProvider,
)
from cosecha.core.runtime_profiles import resolve_runtime_requirement_issues
from cosecha.core.scheduler import (
    ExecutionScheduler,
    NodeExecutionTimeoutError,
    RuntimeAssignmentState,
    SchedulingDecision,
)
from cosecha.core.session_artifacts import (
    EngineReportSummary,
    LiveEngineSnapshotSummary,
    SessionArtifact,
    SessionReportState,
    SessionReportSummary,
    SessionTelemetrySummary,
    SessionTimingSnapshot,
    default_session_artifact_persistence_policy,
)
from cosecha.core.session_timing import SessionTiming, TestTiming
from cosecha.core.telemetry import TelemetryStream
from cosecha.core.utils import is_subpath, validate_plugin_class


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Awaitable, Callable, Iterable

    from cosecha.core.config import Config
    from cosecha.core.engines import Engine
    from cosecha.core.hooks import Hook
    from cosecha.core.items import TestItem
    from cosecha.core.plugins.base import Plugin
root_logger = logging.getLogger()
capture_handler = CaptureLogHandler()
ENGINE_FINISH_TEST_WITH_REPORT_PARAMS = 3


class _StepQueryCandidate:
    __slots__ = ('step_text', 'step_type')

    def __init__(self, step_type: str, step_text: str) -> None:
        self.step_type = step_type
        self.step_text = step_text


class RunnerRuntimeError(Exception): ...


class OperationCapabilityError(PermissionError): ...


class _LiveUpdateSignalSink:
    __slots__ = ('_runner',)

    def __init__(self, runner: Runner) -> None:
        self._runner = runner

    async def emit(self, event: DomainEvent) -> None:
        await self._runner._notify_live_update(event)

    async def close(self) -> None:
        return None


class _SessionArtifactEngineSnapshotSink:
    __slots__ = ('_runner',)

    def __init__(self, runner: Runner) -> None:
        self._runner = runner

    async def emit(self, event: DomainEvent) -> None:
        if isinstance(event, EngineSnapshotUpdatedEvent):
            self._runner._record_live_engine_snapshot(event)

    async def close(self) -> None:
        return None


class Runner:
    __slots__ = (
        '_before_session_hooks_ran',
        '_capture_log_active',
        '_domain_event_node_stable_ids',
        '_domain_event_session_id',
        '_domain_event_stream',
        '_engine_finish_test_supports_report',
        '_extra_reporters',
        '_knowledge_base',
        '_last_plan_id',
        '_latest_plan_explanation',
        '_live_update_condition',
        '_live_update_version',
        '_pending_live_log_event_tasks',
        '_reporting_coordinator',
        '_resource_manager',
        '_root_logger_handlers',
        '_runtime_provider',
        '_scheduler',
        '_session_artifact_metadata_writer',
        '_session_live_engine_snapshots',
        '_session_report_state',
        '_started_engines',
        '_started_plugins',
        'config',
        'console',
        'engines',
        'hooks',
        'plugins',
        'results',
        'session_timing',
        'telemetry_stream',
    )

    def __init__(  # noqa: PLR0913, PLR0915
        self,
        config: Config,
        engines: dict[str, Engine],
        hooks: Iterable[Hook] = (),
        plugins: Iterable[Plugin] = (),
        runtime_provider: RuntimeProvider | None = None,
        session_artifact_metadata_writer: (
            Callable[[SessionArtifact, Path | None], None] | None
        ) = None,
    ) -> None:
        self.config = config
        self.console = self.config.console

        reporter_types = self.available_reporter_types()
        default_console_reporter_type = self.default_console_reporter_type()
        self._extra_reporters: list[Reporter] = []
        for name, path in self.config.reports.items():
            reporter_cls = reporter_types.get(name)
            if reporter_cls:
                reporter = QueuedReporter(reporter_cls(path))
                reporter.initialize(self.config)
                self._extra_reporters.append(reporter)
        self._reporting_coordinator = ReportingCoordinator(
            tuple(self._extra_reporters),
        )

        if not self.config.root_path.exists():
            msg = f'Path "{self.config.root_path}" does not exists'
            raise RunnerRuntimeError(msg)

        for engine_path, engine in engines.items():
            if default_console_reporter_type is not None:
                self._attach_default_console_reporter(
                    engine,
                    default_console_reporter_type,
                )
            engine.initialize(self.config, engine_path)
            self._reporting_coordinator.initialize_engine_reporter(
                self.config,
                engine,
            )

        self.engines = tuple(engines.values())
        self.hooks = tuple(hooks)
        self.plugins = tuple(plugins)
        # Guardamos si hemos activado la captura para restaurarla solo cuando
        # esta sesion haya llegado a modificar el logger global.
        self._capture_log_active = False
        # Conservamos los handlers originales del root logger para dejarlos tal
        # y como estaban al terminar la sesion.
        self._root_logger_handlers: tuple[logging.Handler, ...] = ()
        # Registramos los plugins arrancados para poder cerrarlos aunque el
        # flujo falle a mitad del proceso.
        self._started_plugins: list[Plugin] = []
        # Registramos los engines que si llegaron a abrir sesion.
        self._started_engines: list[Engine] = []
        # Recordamos si ejecutamos los hooks de apertura para no llamar al
        # cierre cuando nunca llegamos a abrir la sesion completa.
        self._before_session_hooks_ran = False
        self.session_timing = SessionTiming(session_start=time.perf_counter())
        self.telemetry_stream = TelemetryStream()
        self._domain_event_stream = DomainEventStream()
        self._domain_event_node_stable_ids: dict[str, str] = {}
        self._domain_event_session_id = build_domain_event_id()
        self._knowledge_base = self._create_knowledge_base()
        self._last_plan_id: str | None = None
        self._live_update_condition = asyncio.Condition()
        self._live_update_version = 0
        self._latest_plan_explanation = None
        self._pending_live_log_event_tasks: set[asyncio.Task[None]] = set()
        self._session_live_engine_snapshots: dict[
            tuple[str, str, str],
            LiveEngineSnapshotSummary,
        ] = {}
        self._domain_event_stream.add_sink(
            KnowledgeBaseDomainEventSink(self._knowledge_base),
        )
        self._domain_event_stream.add_sink(_LiveUpdateSignalSink(self))
        self._domain_event_stream.add_sink(
            _SessionArtifactEngineSnapshotSink(self),
        )
        self._resource_manager = ResourceManager(
            legacy_session_scope='run',
        )
        self._session_report_state = SessionReportState()
        self._resource_manager.bind_domain_event_stream(
            self._domain_event_stream,
        )
        self._resource_manager.bind_domain_event_metadata_provider(
            self._build_resource_event_metadata,
        )
        self._resource_manager.bind_telemetry_stream(self.telemetry_stream)
        self._reporting_coordinator.bind_telemetry_stream(self.telemetry_stream)
        self._runtime_provider = runtime_provider or LocalRuntimeProvider()
        self._session_artifact_metadata_writer = (
            session_artifact_metadata_writer
        )
        self._runtime_provider.initialize(config)
        self._scheduler = ExecutionScheduler(
            worker_selection_policy=(
                self._runtime_provider.scheduler_worker_selection_policy()
            ),
        )
        self._engine_finish_test_supports_report: dict[type[Engine], bool] = {}

        for hook in self.hooks:
            hook.set_config(config)

    @classmethod
    def available_reporter_types(cls) -> dict[str, type[Reporter]]:
        reporter_types: dict[str, type[Reporter]] = {}
        for contribution in iter_shell_reporting_contributions():
            if not issubclass(contribution, Reporter):
                continue
            if contribution.reporter_output_kind() != 'structured':
                continue
            reporter_types[contribution.reporter_name()] = contribution
        return reporter_types

    @classmethod
    def default_console_reporter_type(cls) -> type[Reporter] | None:
        del cls
        for contribution in iter_shell_reporting_contributions():
            if not issubclass(contribution, Reporter):
                continue
            if contribution.reporter_output_kind() != 'console':
                continue
            return contribution
        return None

    def _attach_default_console_reporter(
        self,
        engine: Engine,
        reporter_type: type[Reporter],
    ) -> None:
        descriptor_reporter = engine.reporter.descriptor_target()
        if not isinstance(descriptor_reporter, NullReporter):
            return
        replacement = reporter_type()
        if isinstance(engine.reporter, QueuedReporter):
            engine.reporter = engine.reporter.with_wrapped(replacement)
            return
        engine.reporter = replacement

    def bind_scheduler(
        self,
        scheduler: ExecutionScheduler,
    ) -> None:
        self._scheduler = scheduler

    def _start_log_capture(self) -> None:
        # Guardamos los handlers previos antes de tocar el logging global.
        self._root_logger_handlers = tuple(root_logger.handlers)

        # Quitamos temporalmente los handlers previos para dejar solo la
        # captura de Cosecha durante la sesion.
        for handler in self._root_logger_handlers:
            root_logger.removeHandler(handler)

        # Conectamos el handler de captura usado por el runner.
        capture_handler.set_emit_callback(self._schedule_live_log_chunk_event)
        root_logger.addHandler(capture_handler)

        # Marcamos que esta sesion ya ha modificado el logger global.
        self._capture_log_active = True

    def describe_system_capabilities(
        self,
    ) -> tuple[CapabilityComponentSnapshot, ...]:
        return (
            *(
                build_component_capability_snapshot(
                    component_name=engine.name,
                    component_kind='engine',
                    descriptors=engine.describe_capabilities(),
                )
                for engine in self.engines
            ),
            *(
                build_component_capability_snapshot(
                    component_name=plugin.__class__.plugin_name(),
                    component_kind='plugin',
                    descriptors=plugin.describe_capabilities(),
                )
                for plugin in self.plugins
            ),
            build_component_capability_snapshot(
                component_name=type(self._runtime_provider).__name__,
                component_kind='runtime',
                descriptors=self._runtime_provider.describe_capabilities(),
            ),
        )

    def describe_system_extensions(
        self,
    ) -> tuple[ExtensionComponentSnapshot, ...]:
        reporter_snapshots: dict[
            tuple[str, str],
            ExtensionComponentSnapshot,
        ] = {}
        for reporter in (
            *(engine.reporter for engine in self.engines),
            *self._extra_reporters,
        ):
            snapshot = build_reporter_extension_snapshot(reporter)
            reporter_snapshots[
                (
                    snapshot.component_name,
                    snapshot.descriptor.implementation,
                )
            ] = snapshot

        return (
            *(
                build_engine_extension_snapshot(
                    engine,
                    descriptors=engine.describe_capabilities(),
                )
                for engine in self.engines
            ),
            *(
                build_plugin_extension_snapshot(
                    plugin,
                    descriptors=plugin.describe_capabilities(),
                )
                for plugin in self.plugins
            ),
            build_runtime_extension_snapshot(
                self._runtime_provider,
                descriptors=self._runtime_provider.describe_capabilities(),
            ),
            *(reporter_snapshots[key] for key in sorted(reporter_snapshots)),
        )

    def _component_supports_operation(
        self,
        descriptors,
        operation_type: str,
    ) -> bool:
        return any(
            descriptor.level == 'supported'
            and any(
                binding.operation_type == operation_type
                for binding in descriptor.operations
            )
            for descriptor in descriptors
        )

    def _require_runtime_operation(
        self,
        operation_type: str,
        capability_name: str,
    ) -> None:
        descriptors = self._runtime_provider.describe_capabilities()
        if self._component_supports_operation(descriptors, operation_type):
            return

        msg = (
            'Runtime does not authorize operation '
            f'{operation_type!r} via capability {capability_name!r}'
        )
        raise OperationCapabilityError(msg)

    def _require_engine_operation(
        self,
        *,
        engine_name: str,
        operation_type: str,
        capability_name: str,
    ) -> None:
        engine = self._find_engine_by_name(engine_name)
        if engine is None:
            msg = f'Unknown engine: {engine_name}'
            raise ValueError(msg)

        if self._component_supports_operation(
            engine.describe_capabilities(),
            operation_type,
        ):
            return

        msg = (
            'Engine does not authorize operation '
            f'{operation_type!r} via capability {capability_name!r}: '
            f'{engine_name}'
        )
        raise OperationCapabilityError(msg)

    def _require_all_engines_operation(
        self,
        *,
        operation_type: str,
        capability_name: str,
    ) -> None:
        unsupported_engines = tuple(
            engine.name
            for engine in self.engines
            if not self._component_supports_operation(
                engine.describe_capabilities(),
                operation_type,
            )
        )
        if not unsupported_engines:
            return

        msg = (
            'Not every active engine authorizes operation '
            f'{operation_type!r} via capability {capability_name!r}: '
            f'{", ".join(sorted(unsupported_engines))}'
        )
        raise OperationCapabilityError(msg)

    def _authorize_operation(
        self,
        operation: Operation,
    ) -> None:
        if isinstance(
            operation,
            (
                QueryCapabilitiesOperation,
                QueryExtensionsOperation,
                QueryEngineDependenciesOperation,
                QueryEventsOperation,
                QueryTestsOperation,
                QueryDefinitionsOperation,
                QueryRegistryItemsOperation,
                QueryResourcesOperation,
                QuerySessionArtifactsOperation,
            ),
        ):
            return

        if isinstance(
            operation,
            (
                QueryLiveStatusOperation,
                QueryLiveTailOperation,
                QueryLiveSubscriptionOperation,
            ),
        ):
            self._require_runtime_operation(
                operation.operation_type,
                CAPABILITY_LIVE_EXECUTION_OBSERVABILITY,
            )
            return

        if isinstance(
            operation,
            (
                AnalyzePlanOperation,
                ExplainPlanOperation,
                SimulatePlanOperation,
            ),
        ):
            self._require_all_engines_operation(
                operation_type=(
                    'plan.simulate'
                    if isinstance(operation, SimulatePlanOperation)
                    else operation.operation_type
                ),
                capability_name=(
                    CAPABILITY_SELECTION_LABELS
                    if isinstance(operation, AnalyzePlanOperation)
                    else CAPABILITY_PLAN_EXPLANATION
                ),
            )
            return

        if isinstance(operation, DraftValidationOperation):
            if Path(operation.test_path).name == 'cosecha.toml':
                return
            self._require_engine_operation(
                engine_name=operation.engine_name,
                operation_type=operation.operation_type,
                capability_name=CAPABILITY_DRAFT_VALIDATION,
            )
            return

        if isinstance(operation, ResolveDefinitionOperation):
            self._require_engine_operation(
                engine_name=operation.engine_name,
                operation_type=operation.operation_type,
                capability_name=CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
            )

    @property
    def knowledge_base(self) -> KnowledgeBase:
        return self._knowledge_base

    def _knowledge_query_context(self) -> KnowledgeQueryContext:
        if self._started_engines or self._before_session_hooks_ran:
            return KnowledgeQueryContext(
                source='live_session',
                freshness='fresh',
            )

        return KnowledgeQueryContext(
            source='persistent_knowledge_base',
            freshness='unknown',
        )

    async def _emit_engine_snapshot_update(
        self,
        *,
        node: TestExecutionNode,
        decision: SchedulingDecision,
        payload: dict[str, object] | None,
        snapshot_kind: str,
    ) -> None:
        if self._domain_event_stream is None or payload is None:
            return

        await self._domain_event_stream.emit(
            EngineSnapshotUpdatedEvent(
                engine_name=node.engine_name,
                snapshot_kind=snapshot_kind,
                payload=payload,
                metadata=self._build_domain_event_metadata(
                    correlation_id=node.stable_id,
                    node_id=node.id,
                    node_stable_id=node.stable_id,
                    worker_id=decision.worker_slot,
                ),
            ),
        )

    def _record_live_engine_snapshot(
        self,
        event: EngineSnapshotUpdatedEvent,
    ) -> None:
        if not getattr(
            self.config,
            'persist_live_engine_snapshots',
            False,
        ):
            return

        node_stable_id = event.metadata.node_stable_id
        if node_stable_id is None:
            return

        key = (event.engine_name, event.snapshot_kind, node_stable_id)
        previous = self._session_live_engine_snapshots.get(key)
        if previous is None:
            self._session_live_engine_snapshots[key] = (
                LiveEngineSnapshotSummary(
                    engine_name=event.engine_name,
                    snapshot_kind=event.snapshot_kind,
                    node_stable_id=node_stable_id,
                    payload=dict(event.payload),
                    payload_keys=tuple(sorted(event.payload)),
                    payload_size=len(event.payload),
                    worker_id=event.metadata.worker_id,
                    last_updated_at=event.timestamp,
                )
            )
            return

        self._session_live_engine_snapshots[key] = replace(
            previous,
            payload=dict(event.payload),
            payload_keys=tuple(sorted(event.payload)),
            payload_size=len(event.payload),
            update_count=previous.update_count + 1,
            worker_id=event.metadata.worker_id,
            last_updated_at=event.timestamp,
        )

    def _build_engine_snapshot_payload(
        self,
        node: TestExecutionNode,
        *,
        current_phase: str,
    ) -> dict[str, object] | None:
        return node.engine.build_live_snapshot_payload(node, current_phase)

    async def _notify_live_update(
        self,
        event: DomainEvent | None = None,
    ) -> None:
        del event
        async with self._live_update_condition:
            self._live_update_version += 1
            self._live_update_condition.notify_all()

    def _live_execution_context(
        self,
        snapshot: LiveExecutionSnapshot,
    ) -> LiveExecutionContext:
        return LiveExecutionContext(
            delivery_mode=self._runtime_provider.live_execution_delivery_mode(),
            granularity=self._runtime_provider.live_execution_granularity(),
            truncated=(
                snapshot.truncated_running_test_count > 0
                or snapshot.truncated_worker_count > 0
                or snapshot.truncated_resource_count > 0
                or snapshot.truncated_log_chunk_count > 0
                or snapshot.truncated_event_count > 0
            ),
        )

    def _filter_live_snapshot(
        self,
        query,
    ) -> LiveExecutionSnapshot:
        snapshot = self._knowledge_base.live_snapshot()
        if (
            query.session_id is not None
            and snapshot.session_id != query.session_id
        ):
            return LiveExecutionSnapshot()

        running_tests = snapshot.running_tests
        workers = snapshot.workers
        resources = snapshot.resources
        recent_events = snapshot.recent_events
        recent_log_chunks = snapshot.recent_log_chunks

        if query.node_stable_id is not None:
            running_tests = tuple(
                test
                for test in running_tests
                if test.node_stable_id == query.node_stable_id
            )
            resources = tuple(
                resource
                for resource in resources
                if resource.owner_node_stable_id == query.node_stable_id
            )
            recent_events = tuple(
                event
                for event in recent_events
                if event.metadata.node_stable_id == query.node_stable_id
            )
            recent_log_chunks = tuple(
                log_chunk
                for log_chunk in recent_log_chunks
                if log_chunk.node_stable_id == query.node_stable_id
            )

        if query.worker_id is not None:
            running_tests = tuple(
                test
                for test in running_tests
                if test.worker_slot == query.worker_id
            )
            workers = tuple(
                worker
                for worker in workers
                if worker.worker_id == query.worker_id
            )
            resources = tuple(
                resource
                for resource in resources
                if resource.owner_worker_id == query.worker_id
            )
            recent_events = tuple(
                event
                for event in recent_events
                if event.metadata.worker_id == query.worker_id
            )
            recent_log_chunks = tuple(
                log_chunk
                for log_chunk in recent_log_chunks
                if log_chunk.worker_id == query.worker_id
            )

        engine_snapshots = snapshot.engine_snapshots
        if query.node_stable_id is not None:
            engine_snapshots = tuple(
                current_snapshot
                for current_snapshot in engine_snapshots
                if current_snapshot.node_stable_id == query.node_stable_id
            )
        if query.worker_id is not None:
            engine_snapshots = tuple(
                current_snapshot
                for current_snapshot in engine_snapshots
                if current_snapshot.worker_id == query.worker_id
            )
        if not getattr(query, 'include_engine_snapshots', False):
            engine_snapshots = ()

        return replace(
            snapshot,
            engine_snapshots=engine_snapshots,
            running_tests=running_tests,
            workers=workers,
            resources=resources,
            recent_events=recent_events,
            recent_log_chunks=recent_log_chunks,
        )

    def _query_live_tail(
        self,
        operation: QueryLiveTailOperation,
        snapshot: LiveExecutionSnapshot,
    ) -> tuple[DomainEvent, ...]:
        query = operation.query
        events = tuple(
            event
            for event in snapshot.recent_events
            if (
                query.event_type is None
                or event.event_type == query.event_type
            )
            and (
                query.session_id is None
                or event.metadata.session_id == query.session_id
            )
            and (
                query.plan_id is None
                or event.metadata.plan_id == query.plan_id
            )
            and (
                query.node_stable_id is None
                or event.metadata.node_stable_id == query.node_stable_id
                or getattr(event, 'node_stable_id', None)
                == query.node_stable_id
            )
            and (
                query.after_sequence_number is None
                or (
                    event.metadata.sequence_number is not None
                    and event.metadata.sequence_number
                    > query.after_sequence_number
                )
            )
        )
        if query.limit is None or len(events) <= query.limit:
            return events

        return events[-query.limit :]

    def _query_live_tail_log_chunks(
        self,
        operation: QueryLiveTailOperation,
        snapshot: LiveExecutionSnapshot,
    ):
        query = operation.query
        log_chunks = tuple(
            log_chunk
            for log_chunk in snapshot.recent_log_chunks
            if (
                query.node_stable_id is None
                or log_chunk.node_stable_id == query.node_stable_id
            )
            and (
                query.after_sequence_number is None
                or (
                    log_chunk.sequence_number is not None
                    and log_chunk.sequence_number > query.after_sequence_number
                )
            )
        )
        if query.limit is None or len(log_chunks) <= query.limit:
            return log_chunks

        return log_chunks[-query.limit :]

    def _query_live_subscription_log_chunks(
        self,
        query,
        snapshot: LiveExecutionSnapshot,
    ):
        log_chunks = tuple(
            log_chunk
            for log_chunk in snapshot.recent_log_chunks
            if (
                query.after_sequence_number is None
                or (
                    log_chunk.sequence_number is not None
                    and log_chunk.sequence_number > query.after_sequence_number
                )
            )
        )
        if query.limit is None or len(log_chunks) <= query.limit:
            return log_chunks

        return log_chunks[-query.limit :]

    def _live_subscription_has_updates(
        self,
        query,
        snapshot: LiveExecutionSnapshot,
    ) -> bool:
        if query.after_sequence_number is None:
            return True

        if any(
            event.metadata.sequence_number is not None
            and event.metadata.sequence_number > query.after_sequence_number
            for event in snapshot.recent_events
        ):
            return True

        return any(
            log_chunk.sequence_number is not None
            and log_chunk.sequence_number > query.after_sequence_number
            for log_chunk in snapshot.recent_log_chunks
        )

    async def _wait_for_local_live_update(
        self,
        version: int,
        timeout_seconds: float,
    ) -> bool:
        try:
            async with self._live_update_condition:
                if self._live_update_version > version:
                    return True

                await asyncio.wait_for(
                    self._live_update_condition.wait_for(
                        lambda: self._live_update_version > version,
                    ),
                    timeout_seconds,
                )
                return True
        except TimeoutError:
            return False

    async def _wait_for_live_activity(
        self,
        version: int,
        timeout_seconds: float,
    ) -> bool:
        tasks = {
            asyncio.create_task(
                self._wait_for_local_live_update(
                    version,
                    timeout_seconds,
                ),
            ),
            asyncio.create_task(
                self._runtime_provider.wait_for_live_observability(
                    timeout_seconds,
                ),
            ),
        }
        try:
            while tasks:
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if any(task.result() for task in done):
                    for task in pending:
                        task.cancel()
                    for task in pending:
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
                    return True
                tasks = pending
            return False
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _execute_live_subscription_operation(
        self,
        operation: QueryLiveSubscriptionOperation,
    ) -> QueryLiveSubscriptionOperationResult:
        await self._drain_runtime_provider_observability()
        snapshot = self._filter_live_snapshot(operation.query)
        timeout_seconds = operation.query.timeout_seconds
        deadline = (
            None
            if timeout_seconds is None or timeout_seconds <= 0
            else (time.monotonic() + timeout_seconds)
        )
        while (
            not self._live_subscription_has_updates(
                operation.query,
                snapshot,
            )
            and deadline is not None
        ):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            start_version = self._live_update_version
            if not await self._wait_for_live_activity(
                start_version,
                remaining,
            ):
                break

            await asyncio.sleep(0)
            await self._drain_runtime_provider_observability()
            snapshot = self._filter_live_snapshot(operation.query)

        return QueryLiveSubscriptionOperationResult(
            snapshot=snapshot,
            events=self._query_live_tail(
                QueryLiveTailOperation(
                    query=DomainEventQuery(
                        session_id=operation.query.session_id,
                        node_stable_id=operation.query.node_stable_id,
                        after_sequence_number=(
                            operation.query.after_sequence_number
                        ),
                        limit=operation.query.limit,
                    ),
                ),
                snapshot,
            ),
            log_chunks=self._query_live_subscription_log_chunks(
                operation.query,
                snapshot,
            ),
            next_sequence_number=snapshot.last_sequence_number,
            context=self._live_execution_context(snapshot),
        )

    def _build_live_log_context(
        self,
        *,
        node_id: str,
        node_stable_id: str,
        worker_id: int | None,
    ) -> CapturedLogContext:
        return CapturedLogContext(
            session_id=self._domain_event_session_id,
            plan_id=self._last_plan_id,
            trace_id=self.telemetry_stream.trace_id,
            node_id=node_id,
            node_stable_id=node_stable_id,
            worker_id=worker_id,
        )

    def _schedule_live_log_chunk_event(
        self,
        record: logging.LogRecord,
        message: str,
        context: CapturedLogContext,
    ) -> None:
        if self._domain_event_stream is None:
            return

        task = asyncio.create_task(
            self._domain_event_stream.emit(
                LogChunkEvent(
                    message=message,
                    level=record.levelname.lower(),
                    logger_name=record.name,
                    metadata=DomainEventMetadata(
                        session_id=context.session_id,
                        plan_id=context.plan_id,
                        trace_id=context.trace_id,
                        node_id=context.node_id,
                        node_stable_id=context.node_stable_id,
                        worker_id=context.worker_id,
                    ),
                ),
            ),
        )
        self._pending_live_log_event_tasks.add(task)
        task.add_done_callback(self._pending_live_log_event_tasks.discard)

    async def _flush_pending_live_log_events(self) -> None:
        if not self._pending_live_log_event_tasks:
            return

        await asyncio.gather(
            *tuple(self._pending_live_log_event_tasks),
            return_exceptions=True,
        )
        self._pending_live_log_event_tasks.clear()

    def _find_engine_by_name(
        self,
        engine_name: str,
    ) -> Engine | None:
        for engine in self.engines:
            if engine.name == engine_name:
                return engine

        return None

    def _build_document_collect_paths(
        self,
        test_path: str,
    ) -> tuple[str, ...]:
        collect_path = Path(test_path).parent
        if str(collect_path) in ('', '.'):
            return ()

        return (str(collect_path),)

    def _split_selection_labels(
        self,
        selection_labels: list[str] | tuple[str, ...] | None,
    ) -> tuple[list[str], list[str]]:
        skip_labels: list[str] = []
        execute_labels: list[str] = []

        for label in selection_labels or ():
            if label.startswith('~'):
                skip_labels.append(label.removeprefix('~'))
            else:
                execute_labels.append(label)

        return (skip_labels, execute_labels)

    def _create_knowledge_base(self) -> KnowledgeBase:
        return PersistentKnowledgeBase(
            resolve_knowledge_base_path(
                self.config.workspace_root_path,
                knowledge_storage_root=self.config.knowledge_storage_root_path,
            ),
        )

    def _stop_log_capture(self) -> None:
        # No restauramos nada si esta sesion nunca activo la captura.
        if not self._capture_log_active:
            return

        # Quitamos el handler de captura solo si sigue presente.
        if capture_handler in root_logger.handlers:
            root_logger.removeHandler(capture_handler)
        capture_handler.set_emit_callback(None)

        # Reponemos los handlers originales en el mismo orden.
        for handler in self._root_logger_handlers:
            if handler not in root_logger.handlers:
                root_logger.addHandler(handler)

        # Limpiamos el estado interno para dejar el runner reutilizable.
        self._root_logger_handlers = ()
        self._capture_log_active = False

    def _normalize_collect_paths(
        self,
        paths: str | Path | Iterable[str | Path] | None,
    ) -> tuple[tuple[Path, ...] | None, tuple[Path, ...]]:
        if paths is None:
            return (None, ())

        raw_paths = (paths,) if isinstance(paths, str | Path) else tuple(paths)

        included_paths_by_resolved: dict[Path, Path] = {}
        excluded_paths_by_resolved: dict[Path, Path] = {}
        for raw_path in raw_paths:
            is_excluded = False
            if isinstance(raw_path, str):
                is_excluded = raw_path.startswith('~')
                normalized_raw_path = (
                    raw_path.removeprefix('~') if is_excluded else raw_path
                )
                if not normalized_raw_path:
                    msg = f'Invalid path selector: {raw_path}'
                    raise ValueError(msg)
                path = Path(normalized_raw_path)
            else:
                path = raw_path

            if not path.is_absolute():
                path = self.config.root_path / path

            if not is_subpath(self.config.root_path, path):
                msg = f'Invalid path: {path.resolve()}'
                raise ValueError(msg)

            resolved_path = path.resolve()
            target_paths = (
                excluded_paths_by_resolved
                if is_excluded
                else included_paths_by_resolved
            )
            target_paths.setdefault(resolved_path, resolved_path)

        included_paths = (
            tuple(included_paths_by_resolved.values())
            if included_paths_by_resolved
            else None
        )
        return (included_paths, tuple(excluded_paths_by_resolved.values()))

    async def _run_with_session(
        self,
        paths: tuple[str, ...],
        callback: Callable[[], Awaitable[OperationResult]],
    ) -> OperationResult:
        run_error: Exception | None = None

        try:
            await self.start_session(paths or None)
            result = await callback()
            if (
                self.session_timing is not None
                and self.session_timing.run_end is None
            ):
                self.session_timing.run_end = time.perf_counter()
        except BaseException as error:
            run_error = error
            raise
        else:
            return result
        finally:
            if (
                self._capture_log_active
                or self._started_plugins
                or self._started_engines
                or self._before_session_hooks_ran
            ):
                try:
                    await asyncio.shield(self.finish_session())
                except BaseException:
                    if run_error is None:
                        raise

    def _build_node_telemetry_attributes(
        self,
        node: TestExecutionNode,
    ) -> dict[str, object]:
        snapshot = node.snapshot
        return {
            'engine': snapshot.engine_name,
            'node_id': snapshot.id,
            'node_stable_id': snapshot.stable_id,
            'test_class': node.test.__class__.__name__,
            'test_name': snapshot.test_name,
            'test_path': snapshot.test_path,
            'cosecha.engine.name': snapshot.engine_name,
            'cosecha.node.id': snapshot.id,
            'cosecha.node.stable_id': snapshot.stable_id,
        }

    def _build_cxp_engine_attributes(
        self,
        engine_name: str,
        operation_name: str,
        *,
        outcome: str = 'success',
    ) -> dict[str, object]:
        return {
            'cosecha.engine.name': engine_name,
            'cosecha.operation.name': operation_name,
            'cosecha.outcome': outcome,
        }

    def _build_cxp_plugin_attributes(
        self,
        plugin,
    ) -> dict[str, object]:
        return {
            'cosecha.plugin.name': plugin.plugin_name(),
        }

    def _build_domain_event_metadata(
        self,
        *,
        correlation_id: str | None = None,
        idempotency_key: str | None = None,
        node_id: str | None = None,
        node_stable_id: str | None = None,
        worker_id: int | None = None,
    ) -> DomainEventMetadata:
        return DomainEventMetadata(
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
            session_id=self._domain_event_session_id,
            plan_id=self._last_plan_id,
            node_id=node_id,
            node_stable_id=node_stable_id,
            trace_id=self.telemetry_stream.trace_id,
            worker_id=worker_id,
        )

    def _build_resource_event_metadata(
        self,
        requirement,
        scope: str,
        test_id: str | None,
    ) -> DomainEventMetadata:
        del requirement, scope
        node_stable_id = (
            self._domain_event_node_stable_ids.get(test_id)
            if test_id is not None
            else None
        )
        correlation_id = node_stable_id or self._last_plan_id
        return self._build_domain_event_metadata(
            correlation_id=correlation_id,
            node_id=test_id,
            node_stable_id=node_stable_id,
        )

    def _build_engine_step_directories(
        self,
        engine: Engine,
    ) -> tuple[str, ...]:
        collector = getattr(engine, 'collector', None)
        step_directories = getattr(collector, 'steps_directories', ())
        normalized_step_directories: list[str] = []

        for step_directory in sorted(step_directories):
            try:
                normalized_step_directories.append(
                    str(
                        step_directory.resolve().relative_to(
                            self.config.root_path.resolve(),
                        ),
                    ),
                )
            except Exception:
                normalized_step_directories.append(str(step_directory))

        return tuple(normalized_step_directories)

    def _normalize_snapshot_paths(
        self,
        paths: Iterable[Path],
    ) -> tuple[str, ...]:
        normalized_paths: list[str] = []
        for current_path in sorted(paths):
            try:
                normalized_paths.append(
                    str(
                        current_path.resolve().relative_to(
                            self.config.root_path.resolve(),
                        ),
                    ),
                )
            except Exception:
                normalized_paths.append(str(current_path))

        return tuple(normalized_paths)

    def _build_required_step_texts(
        self,
        test: TestItem,
    ) -> tuple[tuple[str, str], ...]:
        return tuple(test.get_required_step_texts())

    def _build_test_name(
        self,
        test: TestItem,
    ) -> str:
        for attribute in ('test_name', 'name', 'title', 'id'):
            value = getattr(test, attribute, None)
            if value:
                return str(value)

        return repr(test)

    def _build_engine_step_candidate_files(
        self,
        engine: Engine,
        test: TestItem,
        required_step_texts: tuple[tuple[str, str], ...],
    ) -> tuple[str, ...]:
        collector = getattr(engine, 'collector', None)
        step_index = getattr(collector, 'step_catalog', None)
        if step_index is None or not required_step_texts:
            return ()

        candidate_files = step_index.find_candidate_files_for_steps(
            tuple(
                _StepQueryCandidate(step_type, step_text)
                for step_type, step_text in required_step_texts
            ),
        )
        return self._normalize_snapshot_paths(candidate_files)

    async def _run_test_phase(
        self,
        name: str,
        callback: Callable[[], Awaitable[None]],
        *,
        parent_span_id: str,
        attributes: dict[str, object],
    ) -> float:
        phase_start = time.perf_counter()
        phase_attributes = attributes | {
            'cosecha.operation.name': 'test.phase',
            'cosecha.phase': name,
            'cosecha.outcome': 'success',
        }
        async with self.telemetry_stream.span(
            'engine.test.phase',
            parent_span_id=parent_span_id,
            attributes=phase_attributes,
        ):
            try:
                await callback()
            except Exception:
                phase_attributes['cosecha.outcome'] = 'failure'
                raise
        return time.perf_counter() - phase_start

    async def _finish_engine_test(
        self,
        engine: Engine,
        test: TestItem,
        report,
    ) -> None:
        attributes = self._build_cxp_engine_attributes(
            engine.name,
            'test.finish',
        ) | {
            'cosecha.node.id': getattr(test, 'id', None) or '',
            'cosecha.node.stable_id': getattr(test, 'stable_id', None) or '',
        }
        async with self.telemetry_stream.span(
            'engine.test.finish',
            attributes=attributes,
        ):
            try:
                supports_report = self._supports_finish_test_report(engine)
                if supports_report:
                    await engine.finish_test(test, report)
                    return

                await engine.finish_test(test)
            except Exception:
                attributes['cosecha.outcome'] = 'failure'
                raise

    async def _start_engine_test(
        self,
        engine: Engine,
        test: TestItem,
    ) -> None:
        attributes = self._build_cxp_engine_attributes(
            engine.name,
            'test.start',
        ) | {
            'cosecha.node.id': getattr(test, 'id', None) or '',
            'cosecha.node.stable_id': getattr(test, 'stable_id', None) or '',
        }
        async with self.telemetry_stream.span(
            'engine.test.start',
            attributes=attributes,
        ):
            try:
                await self._reporting_coordinator.record_engine_test_start(
                    engine,
                    test,
                )
                await engine.start_test(test)
            except Exception:
                attributes['cosecha.outcome'] = 'failure'
                raise

    async def _start_engine_session(
        self,
        engine: Engine,
    ) -> None:
        attributes = self._build_cxp_engine_attributes(
            engine.name,
            'session.start',
        )
        async with self.telemetry_stream.span(
            'engine.session.start',
            attributes=attributes,
        ):
            try:
                await self._reporting_coordinator.start_engine_reporter(engine)
                await engine.start_session()
            except Exception:
                attributes['cosecha.outcome'] = 'failure'
                raise

    async def _finish_engine_session(
        self,
        engine: Engine,
    ) -> None:
        attributes = self._build_cxp_engine_attributes(
            engine.name,
            'session.finish',
        )
        async with self.telemetry_stream.span(
            'engine.session.finish',
            attributes=attributes,
        ):
            try:
                await self._reporting_coordinator.finish_engine_reporter(
                    engine,
                )
                await engine.finish_session()
            except Exception:
                attributes['cosecha.outcome'] = 'failure'
                raise

    def _supports_finish_test_report(self, engine: Engine) -> bool:
        engine_type = type(engine)
        supports_report = self._engine_finish_test_supports_report.get(
            engine_type,
        )
        if supports_report is not None:
            return supports_report

        finish_signature = inspect.signature(engine_type.finish_test)
        parameters = tuple(finish_signature.parameters.values())
        supports_report = len(
            parameters,
        ) >= ENGINE_FINISH_TEST_WITH_REPORT_PARAMS or any(
            parameter.kind
            in (
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.VAR_POSITIONAL,
            )
            for parameter in parameters
        )
        self._engine_finish_test_supports_report[engine_type] = supports_report
        return supports_report

    def _console_status(
        self,
        message: str,
        *,
        spinner: str,
    ):
        try:
            return self.console.status(
                message,
                spinner=spinner,
                transient=True,
            )
        except TypeError as error:
            if "unexpected keyword argument 'transient'" not in str(error):
                raise
            return self.console.status(message, spinner=spinner)

    def _should_render_running_status(self) -> bool:
        should_render_run_status = getattr(
            self.console,
            'should_render_run_status',
            None,
        )
        if callable(should_render_run_status):
            return bool(should_render_run_status())

        return True

    def _should_render_collection_status(self) -> bool:
        should_render_collection_status = getattr(
            self.console,
            'should_render_collection_status',
            None,
        )
        if callable(should_render_collection_status):
            return bool(should_render_collection_status())

        return True

    def _reset_session_observability(self) -> None:
        self.session_timing = SessionTiming(session_start=time.perf_counter())
        self.telemetry_stream = TelemetryStream()
        self._domain_event_stream = DomainEventStream()
        self._domain_event_node_stable_ids = {}
        self._domain_event_session_id = build_domain_event_id()
        self._session_live_engine_snapshots = {}
        self._knowledge_base.close()
        self._knowledge_base = self._create_knowledge_base()
        self._last_plan_id = None
        self._latest_plan_explanation = None
        self._pending_live_log_event_tasks.clear()
        self._domain_event_stream.add_sink(
            KnowledgeBaseDomainEventSink(self._knowledge_base),
        )
        self._domain_event_stream.add_sink(_LiveUpdateSignalSink(self))
        self._domain_event_stream.add_sink(
            _SessionArtifactEngineSnapshotSink(self),
        )
        self._resource_manager = ResourceManager(
            legacy_session_scope='run',
        )
        self._resource_manager.bind_domain_event_stream(
            self._domain_event_stream,
        )
        self._resource_manager.bind_domain_event_metadata_provider(
            self._build_resource_event_metadata,
        )
        self._resource_manager.bind_telemetry_stream(self.telemetry_stream)
        self._reporting_coordinator.bind_telemetry_stream(self.telemetry_stream)

    async def _start_plugins(self) -> None:
        available_capability_names = self._available_plugin_capability_names()
        for plugin in sorted(
            self.plugins,
            key=lambda current_plugin: current_plugin.start_priority(),
        ):
            validate_plugin_class(plugin.__class__)
            self._validate_plugin_required_capabilities(
                plugin,
                available_capability_names,
            )
            plugin_context = PluginContext(
                config=self.config,
                session_timing=self.session_timing,
                telemetry_stream=self.telemetry_stream,
                domain_event_stream=self._domain_event_stream,
                knowledge_base=self._knowledge_base,
                resource_manager=self._resource_manager,
                engine_names=tuple(engine.name for engine in self.engines),
                runtime_worker_model=(
                    self._runtime_provider.runtime_worker_model()
                ),
                session_report_state=self._session_report_state,
            )
            init_attributes = self._build_cxp_plugin_attributes(plugin)
            async with self.telemetry_stream.span(
                'plugin.initialize',
                attributes=init_attributes,
            ):
                try:
                    # Inicializamos cada plugin con la configuracion efectiva.
                    await plugin.initialize(plugin_context)
                except Exception:
                    raise

            # Registramos el plugin antes de arrancarlo para poder intentar su
            # cierre incluso si `start()` falla a mitad del proceso.
            self._started_plugins.append(plugin)

            # Arrancamos el plugin antes de comenzar la recoleccion.
            async with self.telemetry_stream.span(
                'plugin.start',
                attributes=self._build_cxp_plugin_attributes(plugin),
            ):
                await plugin.start()

    def _available_plugin_capability_names(self) -> set[str]:
        capability_names: set[str] = set()
        for engine in self.engines:
            capability_names.update(
                build_capability_map(engine.describe_capabilities()),
            )

        capability_names.update(
            build_capability_map(
                self._runtime_provider.describe_capabilities(),
            ),
        )
        return capability_names

    def _validate_plugin_required_capabilities(
        self,
        plugin,
        available_capability_names: set[str],
    ) -> None:
        missing_capabilities = tuple(
            sorted(
                capability_name
                for capability_name in plugin.required_capabilities()
                if capability_name not in available_capability_names
            ),
        )
        if not missing_capabilities:
            return

        msg = (
            'Plugin requires unsupported capabilities: '
            f'{plugin.__class__.plugin_name()} -> '
            f'{", ".join(missing_capabilities)}'
        )
        raise OperationCapabilityError(msg)

    async def start_session(
        self,
        paths: str | Path | Iterable[str | Path] | None = None,
    ):
        collect_paths, excluded_paths = self._normalize_collect_paths(paths)
        hook_collect_path: Path | None = None
        if collect_paths is not None and len(collect_paths) == 1:
            hook_collect_path = collect_paths[0]

        if self.config.capture_log:
            # Activamos la captura solo cuando ya vamos a arrancar la sesion.
            self._start_log_capture()

        await self._reporting_coordinator.start_extra_reporters()
        self._reset_session_observability()
        await self._runtime_provider.start()
        await self._start_plugins()

        await asyncio.gather(
            *(hook.before_collect(hook_collect_path) for hook in self.hooks),
        )

        self.session_timing.collect_start = time.perf_counter()

        async def _collect_engine(engine: Engine) -> None:
            engine.bind_session_timing(self.session_timing)
            engine.bind_domain_event_stream(self._domain_event_stream)
            attributes = self._build_cxp_engine_attributes(
                engine.name,
                'collect',
            )
            async with self.telemetry_stream.span(
                'engine.collect',
                attributes=attributes,
            ):
                try:
                    if excluded_paths:
                        await engine.collect(collect_paths, excluded_paths)
                        return

                    if collect_paths is None:
                        await engine.collect()
                        return

                    if len(collect_paths) == 1:
                        await engine.collect(collect_paths[0])
                        return

                    await engine.collect(collect_paths)
                except Exception:
                    attributes['cosecha.outcome'] = 'failure'
                    raise

        try:
            # Iniciamos la recoleccion de tests mediante los motores.
            collection_status = (
                self._console_status(
                    'Collecting tests...',
                    spinner='monkey',
                )
                if self._should_render_collection_status()
                else contextlib.nullcontext()
            )
            with collection_status:
                collect_tasks = [
                    asyncio.create_task(_collect_engine(engine))
                    for engine in self.engines
                ]
                try:
                    await asyncio.gather(*collect_tasks)
                except BaseException:
                    for task in collect_tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(
                        *collect_tasks,
                        return_exceptions=True,
                    )
                    raise
        finally:
            self.session_timing.collect_end = time.perf_counter()

        # Disparamos los hooks de apertura de sesion en paralelo para no
        # bloquear el arranque del motor innecesariamente.
        await asyncio.gather(
            *(hook.before_session_start() for hook in self.hooks),
        )

        # Anotamos que la sesion ya paso por los hooks de apertura.
        self._before_session_hooks_ran = True
        await self._domain_event_stream.emit(
            SessionStartedEvent(
                root_path=str(self.config.root_path),
                workspace_fingerprint=(
                    None
                    if self.config.workspace is None
                    else self.config.workspace.fingerprint
                ),
                concurrency=self.config.concurrency,
                metadata=self._build_domain_event_metadata(
                    correlation_id=self._domain_event_session_id,
                    idempotency_key=(
                        f'runtime:session-start:{self._domain_event_session_id}'
                    ),
                ),
            ),
        )

        for engine in self.engines:
            # Registramos el engine antes de arrancarlo para poder intentar su
            # cierre incluso si `start_session()` falla parcialmente.
            self._started_engines.append(engine)

            # Abrimos la sesion del engine antes de ejecutar los tests.
            await self._start_engine_session(engine)

    async def finish_session(self):  # noqa: PLR0912,PLR0915
        # Conservamos el primer error de cierre pero seguimos limpiando el
        # resto de recursos para no dejar basura global detras.
        first_error: Exception | None = None

        st = self.session_timing
        if st is not None and st.shutdown_start is None:
            st.shutdown_start = time.perf_counter()
        if st is not None and st.run_end is None:
            st.run_end = st.shutdown_start

        phase_start = time.perf_counter()
        async with self.telemetry_stream.span('shutdown.engines'):
            for engine in reversed(self._started_engines):
                try:
                    # Cerramos primero los engines porque pueden imprimir el
                    # reporte final o vaciar buffers pendientes.
                    await self._finish_engine_session(engine)
                except Exception as error:
                    if first_error is None:
                        first_error = error
        if st is not None:
            st.record_shutdown_phase(
                'engines_finish_session',
                time.perf_counter() - phase_start,
            )

        phase_start = time.perf_counter()
        async with self.telemetry_stream.span('shutdown.extra_reporters'):
            try:
                await self._reporting_coordinator.finish_extra_reporters()
            except Exception as error:
                if first_error is None:
                    first_error = error
        if st is not None:
            st.record_shutdown_phase(
                'extra_reporters_print_report',
                time.perf_counter() - phase_start,
            )

        if self._before_session_hooks_ran:
            # Ejecutamos los hooks de cierre en paralelo solo si llegamos a
            # abrir la sesion completa.
            phase_start = time.perf_counter()
            async with self.telemetry_stream.span('shutdown.hooks'):
                results = await asyncio.gather(
                    *(hook.after_session_finish() for hook in self.hooks),
                    return_exceptions=True,
                )
            if st is not None:
                st.record_shutdown_phase(
                    'after_session_finish_hooks',
                    time.perf_counter() - phase_start,
                )

            for result in results:
                if isinstance(result, Exception) and first_error is None:
                    first_error = result

        phase_start = time.perf_counter()
        async with self.telemetry_stream.span('shutdown.runtime_provider'):
            await self._runtime_provider.finish()
            self._resource_manager.merge_observed_timings(
                self._runtime_provider.take_resource_timings(),
            )
            await self._emit_runtime_domain_events(
                self._runtime_provider.take_domain_events(),
            )
        if st is not None:
            st.record_shutdown_phase(
                'runtime_provider_finish',
                time.perf_counter() - phase_start,
            )

        phase_start = time.perf_counter()
        async with self.telemetry_stream.span('shutdown.resources'):
            await self._resource_manager.close()
        if st is not None:
            st.record_shutdown_phase(
                'resource_manager_close',
                time.perf_counter() - phase_start,
            )

        for plugin in sorted(
            self._started_plugins,
            key=lambda current_plugin: current_plugin.finish_priority(),
        ):
            phase_start = time.perf_counter()
            try:
                async with self.telemetry_stream.span(
                    'plugin.finish',
                    attributes=self._build_cxp_plugin_attributes(plugin),
                ):
                    await plugin.finish()
                # Conservamos tambien el span legacy de shutdown para no
                # cambiar de golpe la telemetria interna ya existente.
                async with self.telemetry_stream.span(
                    f'shutdown.plugin.{plugin.__class__.__name__}',
                ):
                    pass
            except Exception as error:
                if first_error is None:
                    first_error = error
            if st is not None:
                st.record_shutdown_phase(
                    f'plugin_finish:{plugin.__class__.__name__}',
                    time.perf_counter() - phase_start,
                )

        plugins_after_close = tuple(
            sorted(
                self._started_plugins,
                key=lambda current_plugin: current_plugin.finish_priority(),
            ),
        )

        phase_start = time.perf_counter()
        # Restauramos siempre el logging global al terminar la sesion.
        await self._flush_pending_live_log_events()
        self._stop_log_capture()
        if st is not None:
            st.record_shutdown_phase(
                'log_capture_restore',
                time.perf_counter() - phase_start,
            )
            st.shutdown_end = time.perf_counter()
            st.session_end = st.shutdown_end

        await self.telemetry_stream.flush()
        await self._domain_event_stream.emit(
            SessionFinishedEvent(
                has_failures=self.has_failures(),
                metadata=self._build_domain_event_metadata(
                    correlation_id=self._domain_event_session_id,
                    idempotency_key=(
                        f'runtime:session-finish:{self._domain_event_session_id}'
                    ),
                ),
            ),
        )
        await self._domain_event_stream.close()
        self._print_session_report_summary(
            self._build_session_report_summary(
                default_session_artifact_persistence_policy().max_failure_examples,
            ),
        )
        self._persist_session_artifact()

        for plugin in plugins_after_close:
            async with self.telemetry_stream.span(
                'plugin.after_session_closed',
                attributes=self._build_cxp_plugin_attributes(plugin),
            ):
                await plugin.after_session_closed()

        phase_start = time.perf_counter()
        try:
            await self.telemetry_stream.close()
        except Exception as error:
            if first_error is None:
                first_error = error
        if st is not None:
            st.record_shutdown_phase(
                'telemetry_stream_close',
                time.perf_counter() - phase_start,
            )

        # Limpiamos el estado interno aunque algun cierre haya fallado.
        self._domain_event_node_stable_ids.clear()
        self._last_plan_id = None
        self._latest_plan_explanation = None
        self._started_engines.clear()
        self._started_plugins.clear()
        self._before_session_hooks_ran = False

        # Propagamos el primer error de limpieza una vez hecho el best effort.
        if first_error is not None:
            raise first_error

    async def build_execution_plan_analysis(
        self,
        *,
        mode: PlanningMode = 'strict',
    ) -> PlanningAnalysis:
        plan_items: list[TestExecutionNode] = []
        for engine in self.engines:
            step_directories = self._build_engine_step_directories(engine)
            for index, test in enumerate(engine.get_collected_tests()):
                required_step_texts = self._build_required_step_texts(test)
                plan_items.append(
                    TestExecutionNode(
                        id=build_execution_node_id(
                            engine.name,
                            build_test_path_label(
                                self.config.root_path,
                                test.path,
                            ),
                            index,
                        ),
                        stable_id=build_execution_node_stable_id(
                            self.config.root_path,
                            engine.name,
                            test,
                        ),
                        engine=engine,
                        test=test,
                        engine_name=engine.name,
                        test_name=self._build_test_name(test),
                        test_path=build_test_path_label(
                            self.config.root_path,
                            test.path,
                        ),
                        required_step_texts=required_step_texts,
                        step_candidate_files=(
                            self._build_engine_step_candidate_files(
                                engine,
                                test,
                                required_step_texts,
                            )
                        ),
                        step_directories=step_directories,
                        resource_requirements=test.get_resource_requirements(),
                    ),
                )

        plan = tuple(plan_items)
        analysis = analyze_execution_plan(plan, mode=mode)
        for plugin in self._started_plugins:
            if not isinstance(plugin, PlanMiddleware):
                continue

            analysis = await plugin.transform_planning_analysis(analysis)

        analysis = self._apply_runtime_compatibility(analysis)
        analysis = self._apply_knowledge_estimates(
            analysis,
            self._knowledge_base,
        )
        plan_id = build_domain_event_id()
        self._last_plan_id = plan_id
        self._latest_plan_explanation = analysis.explanation

        await self._domain_event_stream.emit(
            PlanAnalyzedEvent(
                mode=mode,
                executable=analysis.executable,
                node_count=len(analysis.plan),
                issue_count=len(analysis.issues),
                metadata=self._build_domain_event_metadata(
                    correlation_id=plan_id,
                    idempotency_key=f'runtime:plan:{plan_id}',
                ),
            ),
        )

        return analysis

    async def build_execution_plan(
        self,
        *,
        mode: PlanningMode = 'strict',
    ) -> tuple[TestExecutionNode, ...]:
        analysis = await self.build_execution_plan_analysis(mode=mode)
        if analysis.executable or mode == 'relaxed':
            return analysis.plan

        first_issue = analysis.issues[0]
        raise ValueError(first_issue.message)

    async def explain_execution_plan(
        self,
        *,
        mode: PlanningMode = 'relaxed',
    ):
        return (
            await self.build_execution_plan_analysis(mode=mode)
        ).explanation

    async def run_tests(  # noqa: PLR0915
        self,
        selection_labels: list[str] | None = None,
        test_limit: int | None = None,
        execution_plan: tuple[TestExecutionNode, ...] | None = None,
    ):
        skip_labels, execute_labels = self._split_selection_labels(
            selection_labels,
        )

        test_limit = test_limit or cast('int', inf)
        worker_count = self._runtime_provider.scheduler_worker_count(
            self.config,
        )
        stop_execution = False

        if execution_plan is None:
            analysis = await self.build_execution_plan_analysis()
            if not analysis.executable:
                first_issue = analysis.issues[0]
                raise ValueError(first_issue.message)
            resolved_execution_plan = analysis.plan
        else:
            resolved_execution_plan = validate_execution_plan(execution_plan)
            analysis = self._apply_runtime_compatibility(
                analyze_execution_plan(
                    resolved_execution_plan,
                    mode='relaxed',
                ),
            )
            analysis = self._apply_knowledge_estimates(
                analysis,
                self._knowledge_base,
            )
        selected_execution_plan = self._select_execution_nodes(
            resolved_execution_plan,
            skip_labels=skip_labels,
            execute_labels=execute_labels,
            test_limit=test_limit,
        )
        selected_analysis = self._filter_planning_analysis(
            analysis,
            selected_execution_plan,
        )
        selected_semantics_by_id = {
            semantics.node_id: semantics
            for semantics in selected_analysis.node_semantics
        }

        async def _report_skipped_test(
            test: TestItem,
            message: str,
        ) -> None:
            test.status = TestResultStatus.SKIPPED
            test.message = message
            await self._reporting_coordinator.record_extra_test_result(test)

        not_executable_node_ids = {
            semantics.node_id
            for semantics in selected_analysis.node_semantics
            if semantics.execution_predicate.state == 'not_executable'
        }
        for node in selected_execution_plan:
            if node.id not in not_executable_node_ids:
                continue
            semantics = selected_semantics_by_id[node.id]
            await _report_skipped_test(
                node.test,
                semantics.execution_predicate.reason
                or 'Runtime profile does not satisfy test requirements',
            )

        selected_execution_plan = tuple(
            node
            for node in selected_execution_plan
            if node.id not in not_executable_node_ids
        )
        if not selected_execution_plan:
            return

        scheduling_plan = self._scheduler.build_plan(
            selected_execution_plan,
            worker_count=worker_count,
        )
        selected_nodes_by_id = {
            node.id: node for node in selected_execution_plan
        }
        pinned_node_ids = tuple(
            node.id
            for node in selected_execution_plan
            if any(
                normalize_resource_scope(requirement.scope)
                in {'run', 'worker'}
                for requirement in node.resource_requirements
            )
        )
        assignment_state = RuntimeAssignmentState(
            scheduling_plan.decisions,
            pinned_node_ids=pinned_node_ids,
        )
        self._domain_event_node_stable_ids = {
            node.id: node.stable_id for node in selected_execution_plan
        }
        for decision in scheduling_plan.decisions:
            await self._domain_event_stream.emit(
                NodeScheduledEvent(
                    node_id=decision.node_id,
                    node_stable_id=decision.node_stable_id,
                    worker_slot=decision.worker_slot,
                    max_attempts=decision.max_attempts,
                    timeout_seconds=decision.timeout_seconds,
                    metadata=self._build_domain_event_metadata(
                        correlation_id=decision.node_stable_id,
                        idempotency_key=(
                            f'runtime:node-scheduled:{decision.node_stable_id}'
                        ),
                        node_id=decision.node_id,
                        node_stable_id=decision.node_stable_id,
                    ),
                ),
            )
            await self._domain_event_stream.emit(
                NodeEnqueuedEvent(
                    node_id=decision.node_id,
                    node_stable_id=decision.node_stable_id,
                    preferred_worker_slot=decision.worker_slot,
                    max_attempts=decision.max_attempts,
                    timeout_seconds=decision.timeout_seconds,
                    metadata=self._build_domain_event_metadata(
                        correlation_id=decision.node_stable_id,
                        idempotency_key=(
                            f'runtime:node-enqueued:{decision.node_stable_id}'
                        ),
                        node_id=decision.node_id,
                        node_stable_id=decision.node_stable_id,
                    ),
                ),
            )
        await self._runtime_provider.prepare(
            selected_execution_plan,
            scheduling_plan=scheduling_plan,
        )
        await self._drain_runtime_provider_observability()

        async def _run_single_test(  # noqa: PLR0912,PLR0915
            node: TestExecutionNode,
            decision: SchedulingDecision,
        ):
            nonlocal stop_execution
            test = node.test
            engine = node.engine
            prime_execution_node = getattr(
                engine,
                'prime_execution_node',
                None,
            )
            if prime_execution_node is not None:
                prime_execution_node(node)
            if stop_execution:
                return

            test.failure_kind = None
            test.error_code = None
            test_phase_durations: dict[str, float] = {}
            node_attributes = self._build_node_telemetry_attributes(node)
            live_log_context = self._build_live_log_context(
                node_id=node.id,
                node_stable_id=node.stable_id,
                worker_id=decision.worker_slot,
            )
            report = None

            with capture_handler.bind_live_context(live_log_context):
                hook_lifecycle_started = False
                engine_test_started = False
                engine_result_recorded = False
                preflight_test = getattr(engine, 'preflight_test', None)
                if callable(preflight_test):
                    try:
                        preflight_decision = preflight_test(test)
                    except Exception as error:
                        test.status = TestResultStatus.ERROR
                        test.message = 'Error evaluating test preflight'
                        test.failure_kind = resolve_failure_kind(
                            error,
                            default='bootstrap',
                        )
                        test.error_code = getattr(error, 'code', None)
                        test.exc_info = sys.exc_info()
                    else:
                        if preflight_decision is not None:
                            test.status = preflight_decision.status
                            test.message = preflight_decision.message
                            test.failure_kind = preflight_decision.failure_kind
                            test.error_code = preflight_decision.error_code

                async def _record_nonstarted_engine_result() -> None:
                    nonlocal engine_result_recorded
                    if engine_result_recorded or engine_test_started:
                        return

                    await self._reporting_coordinator.record_engine_test_start(
                        engine,
                        test,
                    )
                    await (
                        self._reporting_coordinator.record_engine_test_result(
                            engine,
                            report or test,
                        )
                    )
                    engine_result_recorded = True

                try:
                    if test.status not in {
                        TestResultStatus.SKIPPED,
                        TestResultStatus.ERROR,
                    }:
                        hook_lifecycle_started = True
                        for hook in self.hooks:
                            await hook.before_test_run(test, engine)
                except Skipped as e:
                    test.status = TestResultStatus.SKIPPED
                    test.message = e.reason
                except Exception as error:
                    test.status = TestResultStatus.ERROR
                    test.message = 'Error in before_test_run hook'
                    test.failure_kind = resolve_failure_kind(
                        error,
                        default='hook',
                    )
                    test.error_code = getattr(error, 'code', None)
                    test.exc_info = sys.exc_info()

                _t = time.perf_counter()
                await self._domain_event_stream.emit(
                    TestStartedEvent(
                        node_id=node.id,
                        node_stable_id=node.stable_id,
                        engine_name=node.engine_name,
                        test_name=node.test_name,
                        test_path=node.test_path,
                        metadata=self._build_domain_event_metadata(
                            correlation_id=node.stable_id,
                            idempotency_key=(
                                f'runtime:test-started:{node.stable_id}'
                            ),
                            node_id=node.id,
                            node_stable_id=node.stable_id,
                            worker_id=decision.worker_slot,
                        ),
                    ),
                )
                await self._emit_engine_snapshot_update(
                    node=node,
                    decision=decision,
                    payload=self._build_engine_snapshot_payload(
                        node,
                        current_phase='setup',
                    ),
                    snapshot_kind='engine_runtime',
                )
                execute_attributes = node_attributes | {
                    'cosecha.operation.name': 'test.execute',
                    'cosecha.outcome': 'success',
                }
                async with self.telemetry_stream.span(
                    'engine.test.execute',
                    attributes=execute_attributes,
                ) as test_span_id:
                    try:
                        if test.status not in {
                            TestResultStatus.ERROR,
                            TestResultStatus.SKIPPED,
                        }:
                            try:
                                engine_test_started = True
                                test_phase_durations[
                                    'start_test'
                                ] = await self._run_test_phase(
                                    'start_test',
                                    lambda: self._start_engine_test(
                                        engine,
                                        test,
                                    ),
                                    parent_span_id=test_span_id,
                                    attributes=node_attributes,
                                )
                            except Exception as error:
                                if test.status in (
                                    TestResultStatus.PENDING,
                                    TestResultStatus.PASSED,
                                ):
                                    test.status = TestResultStatus.ERROR
                                    test.message = 'Error starting test'
                                execute_attributes[
                                    'cosecha.outcome'
                                ] = 'failure'
                                if test.failure_kind is None:
                                    test.failure_kind = resolve_failure_kind(
                                        error,
                                        default='bootstrap',
                                    )
                                if test.error_code is None:
                                    test.error_code = getattr(
                                        error,
                                        'code',
                                        None,
                                    )
                                test.exc_info = sys.exc_info()

                        if test.status not in {
                            TestResultStatus.SKIPPED,
                            TestResultStatus.ERROR,
                        }:
                            try:
                                await self._emit_engine_snapshot_update(
                                    node=node,
                                    decision=decision,
                                    payload=self._build_engine_snapshot_payload(
                                        node,
                                        current_phase='call',
                                    ),
                                    snapshot_kind='engine_runtime',
                                )
                                self._runtime_provider.bind_execution_slot(
                                    node,
                                    decision.worker_slot,
                                )
                                body_result = (
                                    await self._execute_with_scheduler_policy(
                                        node,
                                        decision,
                                        test_span_id=test_span_id,
                                        node_attributes=node_attributes,
                                    )
                                )
                                report = body_result.report
                                test_phase_durations.update(
                                    body_result.phase_durations,
                                )
                                self._resource_manager.merge_observed_timings(
                                    body_result.resource_timings,
                                )
                                await self._emit_runtime_domain_events(
                                    body_result.domain_events,
                                )
                                test.status = report.status
                                test.message = report.message
                                test.duration = report.duration
                                test.failure_kind = report.failure_kind
                                test.error_code = report.error_code
                            except Exception as error:
                                if test.status in (
                                    TestResultStatus.PENDING,
                                    TestResultStatus.PASSED,
                                ):
                                    test.status = TestResultStatus.ERROR
                                    test.message = 'Error running test'
                                if test.failure_kind is None:
                                    test.failure_kind = resolve_failure_kind(
                                        error,
                                        default=(
                                            'infrastructure'
                                            if isinstance(
                                                error,
                                                RuntimeInfrastructureError,
                                            )
                                            else 'runtime'
                                        ),
                                    )
                                if test.error_code is None:
                                    test.error_code = getattr(
                                        error,
                                        'code',
                                        None,
                                    )
                                test.exc_info = sys.exc_info()
                    finally:
                        try:
                            if test.status in {
                                TestResultStatus.ERROR,
                                TestResultStatus.SKIPPED,
                            }:
                                await _record_nonstarted_engine_result()

                            if engine_test_started:
                                await self._emit_engine_snapshot_update(
                                    node=node,
                                    decision=decision,
                                    payload=self._build_engine_snapshot_payload(
                                        node,
                                        current_phase='teardown',
                                    ),
                                    snapshot_kind='engine_runtime',
                                )
                                test_phase_durations[
                                    'finish_test'
                                ] = await self._run_test_phase(
                                    'finish_test',
                                    lambda: self._finish_engine_test(
                                        engine,
                                        test,
                                        report,
                                    ),
                                    parent_span_id=test_span_id,
                                    attributes=node_attributes,
                                )
                        except Exception:
                            if test.status in (
                                TestResultStatus.PENDING,
                                TestResultStatus.PASSED,
                            ):
                                test.status = TestResultStatus.ERROR
                                test.message = 'Error finishing test'
                            error = sys.exc_info()[1]
                            if test.failure_kind is None:
                                test.failure_kind = resolve_failure_kind(
                                    error,
                                    default='runtime',
                                )
                            if test.error_code is None:
                                test.error_code = getattr(error, 'code', None)
                            test.exc_info = sys.exc_info()

                        async def _run_after_test_hooks() -> None:
                            for hook in self.hooks:
                                await hook.after_test_run(test, engine)

                        if hook_lifecycle_started:
                            try:
                                test_phase_durations[
                                    'after_test_run_hooks'
                                ] = await self._run_test_phase(
                                    'after_test_run_hooks',
                                    _run_after_test_hooks,
                                    parent_span_id=test_span_id,
                                    attributes=node_attributes,
                                )
                            except Exception:
                                if test.status in (
                                    TestResultStatus.PENDING,
                                    TestResultStatus.PASSED,
                                ):
                                    test.status = TestResultStatus.ERROR
                                    test.message = (
                                        'Error in after_test_run hook'
                                    )
                                error = sys.exc_info()[1]
                                if test.failure_kind is None:
                                    test.failure_kind = resolve_failure_kind(
                                        error,
                                        default='hook',
                                    )
                                if test.error_code is None:
                                    test.error_code = getattr(
                                        error,
                                        'code',
                                        None,
                                    )
                                test.exc_info = sys.exc_info()

                        async def _run_extra_reporters() -> None:
                            nonlocal engine_result_recorded
                            coordinator = self._reporting_coordinator
                            report_subject = reconcile_test_report(
                                report or test,
                                test,
                                self.config.root_path,
                            )
                            if not engine_result_recorded:
                                await coordinator.record_engine_test_result(
                                    engine,
                                    report_subject,
                                )
                                engine_result_recorded = True
                            await coordinator.record_extra_test_result(
                                report_subject,
                            )

                        test_phase_durations[
                            'extra_reporters'
                        ] = await self._run_test_phase(
                            'extra_reporters',
                            _run_extra_reporters,
                            parent_span_id=test_span_id,
                            attributes=node_attributes,
                        )

                        await self._flush_pending_live_log_events()
                        test.duration = time.perf_counter() - _t
                        if (
                            self.session_timing is not None
                            and test.status != TestResultStatus.SKIPPED
                        ):
                            self.session_timing.tests.append(
                                TestTiming(
                                    repr(test),
                                    test.duration,
                                    phases=test_phase_durations,
                                ),
                            )

                await self.telemetry_stream.flush()
                await self._domain_event_stream.emit(
                    TestFinishedEvent(
                        node_id=node.id,
                        node_stable_id=node.stable_id,
                        engine_name=node.engine_name,
                        test_name=node.test_name,
                        test_path=node.test_path,
                        status=test.status.value,
                        duration=test.duration,
                        failure_kind=test.failure_kind,
                        error_code=test.error_code,
                        metadata=self._build_domain_event_metadata(
                            correlation_id=node.stable_id,
                            idempotency_key=(
                                f'runtime:test-finished:{node.stable_id}'
                            ),
                            node_id=node.id,
                            node_stable_id=node.stable_id,
                            worker_id=decision.worker_slot,
                        ),
                    ),
                )

            if test.has_failed and self.config.stop_on_error:
                stop_execution = True

        async def _worker(worker_slot: int) -> None:
            while True:
                job = assignment_state.claim_next(worker_slot)
                if job is None:
                    if assignment_state.is_complete():
                        return
                    if stop_execution:
                        return

                    if assignment_state.has_pending():
                        await asyncio.sleep(0)
                        continue

                    return

                node = selected_nodes_by_id[job.node_id]
                try:
                    await self._domain_event_stream.emit(
                        NodeAssignedEvent(
                            node_id=job.node_id,
                            node_stable_id=job.node_stable_id,
                            worker_slot=job.worker_slot,
                            metadata=self._build_domain_event_metadata(
                                correlation_id=job.node_stable_id,
                                idempotency_key=(
                                    'runtime:node-assigned:'
                                    f'{job.node_stable_id}:{job.worker_slot}'
                                ),
                                node_id=job.node_id,
                                node_stable_id=job.node_stable_id,
                                worker_id=job.worker_slot,
                            ),
                        ),
                    )
                    await _run_single_test(node, job)
                finally:
                    assignment_state.complete(job)

        running_status_context = (
            self._console_status(
                'Running tests...',
                spinner='circle',
            )
            if self._should_render_running_status()
            else contextlib.nullcontext()
        )
        with running_status_context:
            await asyncio.gather(
                *(
                    _worker(worker_slot)
                    for worker_slot in range(scheduling_plan.worker_count)
                ),
            )

    async def _execute_with_scheduler_policy(
        self,
        node: TestExecutionNode,
        decision: SchedulingDecision,
        *,
        test_span_id: str,
        node_attributes: dict[str, object],
    ):
        attempt = 1
        while True:
            try:
                execute_coro = self._runtime_provider.execute(
                    node,
                    lambda current_node: execute_test_body(
                        current_node,
                        self._resource_manager,
                        ExecutionBodyOptions(
                            root_path=self.config.root_path,
                            telemetry_stream=self.telemetry_stream,
                            parent_span_id=test_span_id,
                            telemetry_attributes=node_attributes,
                            session_id=self._domain_event_session_id,
                            plan_id=self._last_plan_id,
                            trace_id=self.telemetry_stream.trace_id,
                            worker_id=decision.worker_slot,
                        ),
                    ),
                )
                timeout_seconds = decision.timeout_seconds
                if timeout_seconds is None:
                    result = await execute_coro
                    await self._drain_runtime_provider_observability()
                    return result

                result = await asyncio.wait_for(
                    execute_coro,
                    timeout=timeout_seconds,
                )
                await self._drain_runtime_provider_observability()
                return result
            except TimeoutError as error:
                await self._drain_runtime_provider_observability()
                timeout_seconds = decision.timeout_seconds
                if timeout_seconds is None:
                    raise

                scheduler_error = NodeExecutionTimeoutError(
                    node.id,
                    node.stable_id,
                    timeout_seconds,
                )
                if not self._scheduler.should_retry(attempt, scheduler_error):
                    raise scheduler_error from error
            except Exception as error:
                await self._drain_runtime_provider_observability()
                if not self._scheduler.should_retry(attempt, error):
                    raise
                retry_failure_kind = resolve_failure_kind(
                    error,
                    default=(
                        'infrastructure'
                        if isinstance(error, RuntimeInfrastructureError)
                        else 'runtime'
                    ),
                )

                await self._domain_event_stream.emit(
                    NodeRequeuedEvent(
                        node_id=node.id,
                        node_stable_id=node.stable_id,
                        previous_worker_slot=decision.worker_slot,
                        attempt=attempt + 1,
                        failure_kind=retry_failure_kind,
                        error_code=getattr(error, 'code', None),
                        metadata=self._build_domain_event_metadata(
                            correlation_id=node.stable_id,
                            idempotency_key=(
                                'runtime:node-requeued:'
                                f'{node.stable_id}:{attempt + 1}'
                            ),
                            node_id=node.id,
                            node_stable_id=node.stable_id,
                            worker_id=decision.worker_slot,
                        ),
                    ),
                )

                await self._domain_event_stream.emit(
                    NodeRetryingEvent(
                        node_id=node.id,
                        node_stable_id=node.stable_id,
                        attempt=attempt + 1,
                        failure_kind=retry_failure_kind,
                        error_code=getattr(error, 'code', None),
                        metadata=self._build_domain_event_metadata(
                            correlation_id=node.stable_id,
                            idempotency_key=(
                                'runtime:node-retrying:'
                                f'{node.stable_id}:{attempt + 1}'
                            ),
                            node_id=node.id,
                            node_stable_id=node.stable_id,
                        ),
                    ),
                )

            backoff_seconds = self._scheduler.backoff_for_attempt(attempt)
            attempt += 1
            if backoff_seconds > 0:
                await asyncio.sleep(backoff_seconds)

    def _select_execution_nodes(
        self,
        execution_plan: tuple[TestExecutionNode, ...],
        *,
        skip_labels: list[str],
        execute_labels: list[str],
        test_limit: int,
    ) -> tuple[TestExecutionNode, ...]:
        return filter_execution_nodes(
            execution_plan,
            skip_labels=skip_labels,
            execute_labels=execute_labels,
            test_limit=test_limit,
        )

    def _filter_planning_analysis(
        self,
        analysis: PlanningAnalysis,
        selected_execution_plan: tuple[TestExecutionNode, ...],
    ) -> PlanningAnalysis:
        selected_node_ids = {node.id for node in selected_execution_plan}
        selected_semantics = tuple(
            semantics
            for semantics in analysis.node_semantics
            if semantics.node_id in selected_node_ids
        )
        selected_issues = tuple(
            issue
            for issue in analysis.issues
            if issue.node_id is None or issue.node_id in selected_node_ids
        )
        return PlanningAnalysis(
            mode=analysis.mode,
            plan=selected_execution_plan,
            issues=selected_issues,
            node_semantics=selected_semantics,
        )

    def _apply_simulation_compatibility(
        self,
        analysis: PlanningAnalysis,
    ) -> PlanningAnalysis:
        issues = list(analysis.issues)
        node_semantics = []

        for node, semantics in zip(
            analysis.plan,
            analysis.node_semantics,
            strict=True,
        ):
            node_issues = list(semantics.issues)
            runtime_hints = list(semantics.runtime_hints)

            for requirement in node.resource_requirements:
                provider = requirement.resolve_provider()
                compatible_non_live_modes = tuple(
                    mode
                    for mode in ('dry_run', 'ephemeral')
                    if provider.supports_mode(mode)
                )
                if compatible_non_live_modes:
                    runtime_hints.append(
                        'simulation.'
                        f'resource.{requirement.name}='
                        f'{compatible_non_live_modes[0]}',
                    )
                    continue

                issue = PlanningIssue(
                    code='simulation_requires_live_resource_mode',
                    message=(
                        'Simulation cannot validate resource '
                        f'{requirement.name!r} for {node.id} without '
                        'materializing live infrastructure'
                    ),
                    node_id=node.id,
                    node_stable_id=node.stable_id,
                )
                issues.append(issue)
                node_issues.append(issue)

            node_semantics.append(
                replace(
                    semantics,
                    runtime_hints=tuple(runtime_hints),
                    issues=tuple(node_issues),
                ),
            )

        return PlanningAnalysis(
            mode=analysis.mode,
            plan=analysis.plan,
            issues=tuple(issues),
            node_semantics=tuple(node_semantics),
        )

    def _apply_runtime_compatibility(
        self,
        analysis: PlanningAnalysis,
    ) -> PlanningAnalysis:
        runtime_capabilities = build_capability_map(
            self._runtime_provider.describe_capabilities(),
        )
        run_scope_support = runtime_capabilities.get('run_scoped_resources')

        issues = list(analysis.issues)
        node_semantics = list(analysis.node_semantics)
        for index, (node, semantics) in enumerate(
            zip(analysis.plan, node_semantics, strict=True),
        ):
            runtime_hints = list(semantics.runtime_hints)
            node_issues = list(semantics.issues)
            execution_predicate = semantics.execution_predicate
            runtime_requirement_issues = resolve_runtime_requirement_issues(
                node.test.get_runtime_requirement_set(),
                getattr(node.engine, 'runtime_service_offerings', ()),
            )
            if runtime_requirement_issues:
                issue = PlanningIssue(
                    code='runtime_requirements_not_executable',
                    message=(
                        'Runtime profile does not satisfy test requirements '
                        f'for {node.id}: '
                        f'{"; ".join(runtime_requirement_issues)}'
                    ),
                    severity='warning',
                    node_id=node.id,
                    node_stable_id=node.stable_id,
                )
                issues.append(issue)
                node_issues.append(issue)
                execution_predicate = ExecutionPredicateEvaluation(
                    state='not_executable',
                    reason='; '.join(runtime_requirement_issues),
                )
                runtime_hints.append('runtime.compatibility=not_executable')

            if (
                run_scope_support is not None
                and run_scope_support.level == 'unsupported'
            ):
                unsupported_resource_names = tuple(
                    sorted(
                        requirement.name
                        for requirement in node.resource_requirements
                        if normalize_resource_scope(
                            requirement.scope,
                            legacy_session_scope=(
                                self._runtime_provider.legacy_session_scope()
                            ),
                        )
                        == 'run'
                    ),
                )
                if unsupported_resource_names:
                    issue = PlanningIssue(
                        code='unsupported_runtime_resource_scope',
                        message=(
                            'Runtime '
                            f'{type(self._runtime_provider).__name__} '
                            'does not '
                            'support run-scoped resources yet for '
                            f'{node.id}: '
                            f'{", ".join(unsupported_resource_names)}'
                        ),
                        node_id=node.id,
                        node_stable_id=node.stable_id,
                    )
                    issues.append(issue)
                    node_issues.append(issue)
                    runtime_hints.append(
                        'runtime.run_scoped_resources=unsupported',
                    )

            node_semantics[index] = replace(
                semantics,
                execution_predicate=execution_predicate,
                runtime_hints=tuple(runtime_hints),
                issues=tuple(node_issues),
            )

        return PlanningAnalysis(
            mode=analysis.mode,
            plan=analysis.plan,
            issues=tuple(issues),
            node_semantics=tuple(node_semantics),
        )

    def _apply_knowledge_estimates(
        self,
        analysis: PlanningAnalysis,
        knowledge_base: KnowledgeBase,
    ) -> PlanningAnalysis:
        node_semantics = []
        for semantics in analysis.node_semantics:
            historical_tests = knowledge_base.query_tests(
                TestKnowledgeQuery(
                    node_stable_id=semantics.node_stable_id,
                    limit=1,
                ),
            )
            has_historical_duration = (
                historical_tests and historical_tests[0].duration is not None
            )
            estimated_duration = (
                historical_tests[0].duration
                if has_historical_duration
                else None
            )
            runtime_hints = semantics.runtime_hints
            if estimated_duration is not None:
                runtime_hints = (
                    *runtime_hints,
                    f'estimated_duration={estimated_duration:.6f}',
                )
            node_semantics.append(
                replace(
                    semantics,
                    estimated_duration=estimated_duration,
                    runtime_hints=runtime_hints,
                ),
            )

        return PlanningAnalysis(
            mode=analysis.mode,
            plan=analysis.plan,
            issues=analysis.issues,
            node_semantics=tuple(node_semantics),
        )

    async def _emit_runtime_domain_events(
        self,
        events,
    ) -> None:
        for event in events:
            self._normalize_runtime_domain_event(event)
            await self._domain_event_stream.emit(event)

    async def _drain_runtime_provider_observability(self) -> None:
        self._resource_manager.merge_observed_timings(
            self._runtime_provider.take_resource_timings(),
        )
        await self._emit_runtime_domain_events(
            self._runtime_provider.take_domain_events(),
        )
        await self._emit_runtime_domain_events(
            self._runtime_provider.take_log_events(),
        )

    def _normalize_runtime_domain_event(
        self,
        event: DomainEvent,
    ) -> None:
        metadata = event.metadata
        if metadata.sequence_number is not None:
            object.__setattr__(metadata, 'sequence_number', None)
        if metadata.session_id is None:
            object.__setattr__(
                metadata,
                'session_id',
                self._domain_event_session_id,
            )
        if metadata.plan_id is None:
            object.__setattr__(metadata, 'plan_id', self._last_plan_id)
        if metadata.trace_id is None:
            object.__setattr__(
                metadata,
                'trace_id',
                self.telemetry_stream.trace_id,
            )

    def _persist_session_artifact(self) -> None:
        if self._domain_event_session_id is None:
            return
        if not hasattr(self.config, 'snapshot'):
            return

        persistence_policy = default_session_artifact_persistence_policy()

        artifact = SessionArtifact(
            session_id=self._domain_event_session_id,
            trace_id=self.telemetry_stream.trace_id,
            root_path=str(self.config.root_path),
            workspace_fingerprint=(
                None
                if self.config.workspace is None
                else self.config.workspace.fingerprint
            ),
            plan_id=self._last_plan_id,
            config_snapshot=self.config.snapshot(),
            capability_snapshots=self.describe_system_capabilities(),
            plan_explanation=self._latest_plan_explanation,
            timing=SessionTimingSnapshot.from_session_timing(
                self.session_timing,
            ),
            has_failures=self.has_failures(),
            report_summary=self._build_session_report_summary(
                persistence_policy.max_failure_examples,
            ),
            telemetry_summary=self._build_session_telemetry_summary(),
            persistence_policy=persistence_policy,
            recorded_at=time.time(),
        )
        if isinstance(self._knowledge_base, PersistentKnowledgeBase):
            writer = PersistentKnowledgeBase(self._knowledge_base.db_path)
            try:
                writer.store_session_artifact(artifact)
            finally:
                writer.close()
            if self._session_artifact_metadata_writer is not None:
                self._session_artifact_metadata_writer(
                    artifact,
                    self._knowledge_base.db_path,
                )
            return

        self._knowledge_base.store_session_artifact(artifact)
        if self._session_artifact_metadata_writer is not None:
            self._session_artifact_metadata_writer(artifact, None)

    def _build_session_report_summary(
        self,
        max_failure_examples: int,
    ) -> SessionReportSummary:
        status_counts: dict[str, int] = {
            status.value: 0 for status in TestResultStatus
        }
        failure_kind_counts: dict[str, int] = {}
        failed_examples: list[str] = []
        failed_files: list[str] = []
        total_tests = 0
        engine_summaries: list[EngineReportSummary] = []

        for engine in self.engines:
            engine_summary = self._build_engine_report_summary(
                engine,
                max_failure_examples=max_failure_examples,
            )
            engine_summaries.append(engine_summary)
            total_tests += engine_summary.total_tests
            failed_files.extend(engine_summary.failed_files)
            for status, count in engine_summary.status_counts:
                status_counts[status] = status_counts.get(status, 0) + count
            for failure_kind, count in engine_summary.failure_kind_counts:
                failure_kind_counts[failure_kind] = (
                    failure_kind_counts.get(failure_kind, 0) + count
                )
            remaining_failure_examples = max_failure_examples - len(
                failed_examples,
            )
            if remaining_failure_examples > 0:
                failed_examples.extend(
                    engine_summary.failed_examples[
                        :remaining_failure_examples
                    ],
                )

        return SessionReportSummary(
            total_tests=total_tests,
            status_counts=tuple(sorted(status_counts.items())),
            failure_kind_counts=tuple(sorted(failure_kind_counts.items())),
            engine_summaries=tuple(engine_summaries),
            live_engine_snapshots=self._build_live_engine_snapshot_summaries(),
            failed_examples=tuple(failed_examples),
            failed_files=tuple(failed_files),
            instrumentation_summaries=dict(
                self._session_report_state.instrumentation_summaries,
            ),
        )

    def _build_live_engine_snapshot_summaries(
        self,
    ) -> tuple[LiveEngineSnapshotSummary, ...]:
        if not getattr(
            self.config,
            'persist_live_engine_snapshots',
            False,
        ):
            return ()

        return tuple(
            sorted(
                self._session_live_engine_snapshots.values(),
                key=lambda snapshot: (
                    snapshot.engine_name,
                    snapshot.snapshot_kind,
                    snapshot.node_stable_id,
                    snapshot.last_updated_at or 0.0,
                ),
            ),
        )

    def _build_engine_report_summary(
        self,
        engine: Engine,
        *,
        max_failure_examples: int,
    ) -> EngineReportSummary:
        status_counts: dict[str, int] = {
            status.value: 0 for status in TestResultStatus
        }
        failure_kind_counts: dict[str, int] = {}
        detail_counts: dict[str, int] = {}
        failed_examples: list[str] = []
        total_tests = 0
        feature_keys: set[str] = set()
        failed_feature_keys: set[str] = set()
        presenter_types = {
            contribution.contribution_name: contribution
            for contribution in iter_console_presenter_contributions()
        }

        failed_files = tuple(
            str(failed_file)
            for failed_file in sorted(engine.collector.failed_files, key=str)
        )

        for test in engine.get_collected_tests():
            total_tests += 1
            status_counts[test.status.value] = (
                status_counts.get(test.status.value, 0) + 1
            )
            if test.failure_kind is not None:
                failure_kind_counts[test.failure_kind] = (
                    failure_kind_counts.get(test.failure_kind, 0) + 1
                )
            if test.has_failed and len(failed_examples) < max_failure_examples:
                failed_examples.append(
                    f'{engine.name}:{self._build_test_name(test)}',
                )

            report = ensure_test_report(test, self.config.root_path)
            presenter = presenter_types.get(report.engine_name or '')
            accumulate_summary = getattr(
                presenter,
                'accumulate_engine_summary',
                None,
            )
            if callable(accumulate_summary):
                accumulate_summary(
                    report,
                    detail_counts=detail_counts,
                    feature_keys=feature_keys,
                    failed_feature_keys=failed_feature_keys,
                )

        if feature_keys:
            detail_counts['features.total'] = len(feature_keys)
            detail_counts['features.failed'] = len(failed_feature_keys)

        return EngineReportSummary(
            engine_name=engine.name,
            total_tests=total_tests,
            status_counts=tuple(sorted(status_counts.items())),
            failure_kind_counts=tuple(sorted(failure_kind_counts.items())),
            detail_counts=tuple(sorted(detail_counts.items())),
            failed_examples=tuple(failed_examples),
            failed_files=failed_files,
        )

    def _print_session_report_summary(
        self,
        summary: SessionReportSummary,
    ) -> None:
        print_summary = getattr(self.console, 'print_summary', None)
        if not callable(print_summary):
            return

        status_counts = dict(summary.status_counts)
        passed_count = status_counts.get(
            TestResultStatus.PASSED.value,
            0,
        )
        failed_count = status_counts.get(
            TestResultStatus.FAILED.value,
            0,
        )
        lines = [
            ', '.join(
                (
                    f'Tests ({summary.total_tests}):',
                    f'{passed_count} passed',
                    f'{failed_count} failed',
                ),
            ),
        ]
        for count, label, style in self._iter_optional_status_segments(
            status_counts,
        ):
            del style
            lines[0] += f', {count} {label}'

        if summary.failure_kind_counts:
            lines.append(
                'Failure kinds: '
                + ', '.join(
                    f'{failure_kind}={count}'
                    for failure_kind, count in summary.failure_kind_counts
                ),
            )

        if (
            summary.engine_summaries
            and self._should_render_verbose_session_summary()
        ):
            lines.append('')
            lines.append('By engine:')
            for engine_summary in summary.engine_summaries:
                self._append_engine_session_summary(lines, engine_summary)
        elif len(summary.engine_summaries) > 1:
            self._append_compact_engine_session_summary(lines, summary)

        print_summary('Session', '\n'.join(lines))

    def _iter_optional_status_segments(
        self,
        status_counts: dict[str, int],
    ) -> tuple[tuple[int, str, str], ...]:
        segments: list[tuple[int, str, str]] = []
        for status, label, style in (
            (TestResultStatus.ERROR, 'errors', 'red'),
            (TestResultStatus.SKIPPED, 'skipped', 'yellow'),
            (TestResultStatus.PENDING, 'pending', 'grey42'),
        ):
            count = status_counts.get(status.value, 0)
            if count:
                segments.append((count, label, style))
        return tuple(segments)

    def _append_engine_session_summary(
        self,
        lines: list[str],
        engine_summary: EngineReportSummary,
    ) -> None:
        engine_status_counts = dict(engine_summary.status_counts)
        lines.append(
            (
                f'- {engine_summary.engine_name}: '
                f'{engine_summary.total_tests} tests'
            ),
        )
        lines.append(
            (
                f' passed={engine_status_counts.get("passed", 0)},'
                f' failed={engine_status_counts.get("failed", 0)},'
                f' errors={engine_status_counts.get("error", 0)},'
                f' skipped={engine_status_counts.get("skipped", 0)},'
                f' pending={engine_status_counts.get("pending", 0)}'
            ),
        )
        if engine_summary.detail_counts:
            lines.append(
                '  details: '
                + ', '.join(
                    f'{name}={count}'
                    for name, count in engine_summary.detail_counts
                ),
            )
        if engine_summary.failure_kind_counts:
            lines.append(
                '  failure_kinds: '
                + ', '.join(
                    f'{name}={count}'
                    for name, count in engine_summary.failure_kind_counts
                ),
            )

    def _append_compact_engine_session_summary(
        self,
        lines: list[str],
        summary: SessionReportSummary,
    ) -> None:
        lines.append(
            'Engines: '
            + ', '.join(
                (f'{engine_summary.engine_name}={engine_summary.total_tests}')
                for engine_summary in summary.engine_summaries
            ),
        )

    def _should_render_verbose_session_summary(self) -> bool:
        is_debug_mode = getattr(self.console, 'is_debug_mode', None)
        if callable(is_debug_mode) and is_debug_mode():
            return True

        is_trace_mode = getattr(self.console, 'is_trace_mode', None)
        return callable(is_trace_mode) and is_trace_mode()

    def _build_session_telemetry_summary(self) -> SessionTelemetrySummary:
        summary = self.telemetry_stream.summary()
        return SessionTelemetrySummary(
            span_count=int(summary['span_count']),
            distinct_span_names=int(summary['distinct_span_names']),
            top_span_names=tuple(
                (str(name), int(count))
                for name, count in summary['top_span_names']
            ),
        )

    async def run(
        self,
        paths: str | Path | Iterable[str | Path] | None = None,
        selection_labels: list[str] | None = None,
        test_limit: int | None = None,
    ) -> bool:
        operation = RunOperation(
            paths=(
                ()
                if paths is None
                else tuple(str(current_path) for current_path in (paths,))
                if isinstance(paths, str | Path)
                else tuple(str(current_path) for current_path in paths)
            ),
            selection_labels=tuple(selection_labels or ()),
            test_limit=test_limit,
        )
        result = await self.execute_operation(operation)
        return cast('RunOperationResult', result).has_failures

    def has_active_session(self) -> bool:
        return bool(self._started_engines or self._before_session_hooks_ran)

    async def execute_operation_in_active_session(
        self,
        operation: Operation,
    ) -> OperationResult:
        if not self.has_active_session():
            msg = 'Runner has no active session'
            raise RunnerRuntimeError(msg)

        if isinstance(operation, RunOperation):
            msg = (
                'RunOperation requires managed session lifecycle when '
                'executed through the runner control plane'
            )
            raise TypeError(msg)

        self._authorize_operation(operation)
        return await self._execute_operation_without_session_management(
            operation,
        )

    async def execute_operation(
        self,
        operation: Operation,
    ) -> OperationResult:
        result: OperationResult | None = None
        if isinstance(operation, RunOperation):

            async def _run_operation() -> OperationResult:
                await self.run_tests(
                    list(operation.selection_labels) or None,
                    operation.test_limit,
                )
                return RunOperationResult(
                    has_failures=self.has_failures(),
                    total_tests=self._count_collected_tests(),
                )

            result = await self._run_with_session(
                operation.paths,
                _run_operation,
            )
        elif isinstance(
            operation,
            (
                AnalyzePlanOperation,
                ExplainPlanOperation,
                SimulatePlanOperation,
            ),
        ):
            self._authorize_operation(operation)

            async def _in_session_operation() -> OperationResult:
                return (
                    await self._execute_operation_without_session_management(
                        operation,
                    )
                )

            result = await self._run_with_session(
                operation.paths
                if isinstance(
                    operation,
                    (
                        AnalyzePlanOperation,
                        ExplainPlanOperation,
                        SimulatePlanOperation,
                    ),
                )
                else (),
                _in_session_operation,
            )
        elif isinstance(
            operation,
            (
                QueryCapabilitiesOperation,
                QueryExtensionsOperation,
                QueryLiveSubscriptionOperation,
                QueryLiveStatusOperation,
                QueryLiveTailOperation,
                QueryEngineDependenciesOperation,
                QueryEventsOperation,
                QueryTestsOperation,
                QueryDefinitionsOperation,
                QueryRegistryItemsOperation,
                QueryResourcesOperation,
                QuerySessionArtifactsOperation,
            ),
        ):
            self._authorize_operation(operation)
            result = await self._execute_operation_without_session_management(
                operation,
            )
        elif isinstance(operation, DraftValidationOperation):
            self._authorize_operation(operation)

            async def _validate_draft_operation() -> OperationResult:
                return (
                    await self._execute_operation_without_session_management(
                        operation,
                    )
                )

            result = await self._run_with_session(
                self._build_document_collect_paths(
                    operation.test_path,
                ),
                _validate_draft_operation,
            )
        elif isinstance(operation, ResolveDefinitionOperation):
            self._authorize_operation(operation)

            async def _resolve_definition_operation() -> OperationResult:
                return (
                    await self._execute_operation_without_session_management(
                        operation,
                    )
                )

            result = await self._run_with_session(
                self._build_document_collect_paths(
                    operation.test_path,
                ),
                _resolve_definition_operation,
            )
        else:
            msg = f'Unsupported operation: {operation!r}'
            raise TypeError(msg)

        return result

    async def _execute_operation_without_session_management(
        self,
        operation: Operation,
    ) -> OperationResult:
        result: OperationResult | None = None
        if isinstance(operation, AnalyzePlanOperation):
            analysis = await self._build_selected_plan_analysis(
                mode=operation.mode,
                selection_labels=operation.selection_labels,
                test_limit=operation.test_limit,
            )
            result = AnalyzePlanOperationResult(analysis=analysis)
        elif isinstance(operation, ExplainPlanOperation):
            analysis = await self._build_selected_plan_analysis(
                mode=operation.mode,
                selection_labels=operation.selection_labels,
                test_limit=operation.test_limit,
            )
            result = ExplainPlanOperationResult(
                explanation=analysis.explanation,
            )
        elif isinstance(operation, SimulatePlanOperation):
            result = await self._execute_simulate_plan_operation(operation)
        elif isinstance(operation, DraftValidationOperation):
            result = await self._execute_draft_validation_operation(
                operation,
            )
        elif isinstance(operation, ResolveDefinitionOperation):
            result = await self._execute_definition_resolution_operation(
                operation,
            )
        elif isinstance(
            operation,
            (
                QueryCapabilitiesOperation,
                QueryExtensionsOperation,
                QueryLiveSubscriptionOperation,
                QueryLiveStatusOperation,
                QueryLiveTailOperation,
                QueryEngineDependenciesOperation,
                QueryEventsOperation,
                QueryTestsOperation,
                QueryDefinitionsOperation,
                QueryRegistryItemsOperation,
                QueryResourcesOperation,
                QuerySessionArtifactsOperation,
            ),
        ):
            result = await self._execute_query_operation(operation)
        else:
            msg = (
                'Operation is not supported without session lifecycle '
                f'management: {operation!r}'
            )
            raise TypeError(msg)

        return result

    async def _execute_simulate_plan_operation(
        self,
        operation: SimulatePlanOperation,
    ) -> SimulatePlanOperationResult:
        analysis = await self._build_selected_plan_analysis(
            mode=operation.mode,
            selection_labels=operation.selection_labels,
            test_limit=operation.test_limit,
        )
        simulation_analysis = self._apply_simulation_compatibility(analysis)
        worker_count = self._runtime_provider.scheduler_worker_count(
            self.config,
        )
        scheduling_plan = self._scheduler.build_plan(
            simulation_analysis.plan,
            worker_count=worker_count,
        )
        explanation = simulation_analysis.explanation
        self._latest_plan_explanation = explanation
        return SimulatePlanOperationResult(
            explanation=explanation,
            plan=tuple(node.snapshot for node in simulation_analysis.plan),
            hypothetical_scheduling=tuple(
                HypotheticalSchedulingDecision.from_scheduling_decision(
                    decision,
                )
                for decision in scheduling_plan.decisions
            ),
        )

    async def _build_selected_plan_analysis(
        self,
        *,
        mode: PlanningMode,
        selection_labels: tuple[str, ...] = (),
        test_limit: int | None = None,
    ) -> PlanningAnalysis:
        skip_labels, execute_labels = self._split_selection_labels(
            selection_labels,
        )
        selected_limit = test_limit or cast('int', inf)
        analysis = await self.build_execution_plan_analysis(mode=mode)
        selected_execution_plan = self._select_execution_nodes(
            analysis.plan,
            skip_labels=skip_labels,
            execute_labels=execute_labels,
            test_limit=selected_limit,
        )
        return self._filter_planning_analysis(
            analysis,
            selected_execution_plan,
        )

    async def _execute_query_operation(
        self,
        operation: Operation,
    ) -> OperationResult:
        if isinstance(
            operation,
            (
                QueryCapabilitiesOperation,
                QueryExtensionsOperation,
            ),
        ):
            return self._execute_static_metadata_query_operation(operation)
        if isinstance(operation, QueryLiveStatusOperation):
            await self._drain_runtime_provider_observability()
            snapshot = self._filter_live_snapshot(operation.query)
            return QueryLiveStatusOperationResult(
                snapshot=snapshot,
                context=self._live_execution_context(snapshot),
            )
        if isinstance(operation, QueryLiveSubscriptionOperation):
            return await self._execute_live_subscription_operation(
                operation,
            )
        if isinstance(operation, QueryLiveTailOperation):
            await self._drain_runtime_provider_observability()
            snapshot = self._knowledge_base.live_snapshot()
            return QueryLiveTailOperationResult(
                events=self._query_live_tail(operation, snapshot),
                log_chunks=self._query_live_tail_log_chunks(
                    operation,
                    snapshot,
                ),
                context=self._live_execution_context(snapshot),
            )
        if isinstance(operation, QueryEngineDependenciesOperation):
            dependency_rules = self._query_engine_dependencies(operation)
            return QueryEngineDependenciesOperationResult(
                rules=dependency_rules,
                projected_issues=self._project_engine_dependency_issues(
                    dependency_rules,
                    operation,
                ),
            )
        if isinstance(
            operation,
            (
                QueryTestsOperation,
                QueryDefinitionsOperation,
                QueryEventsOperation,
                QueryRegistryItemsOperation,
                QueryResourcesOperation,
                QuerySessionArtifactsOperation,
            ),
        ):
            return self._execute_persisted_knowledge_query_operation(
                operation,
            )

        msg = f'Unsupported query operation: {operation!r}'
        raise TypeError(msg)

    def _execute_static_metadata_query_operation(
        self,
        operation: QueryCapabilitiesOperation | QueryExtensionsOperation,
    ) -> OperationResult:
        if isinstance(operation, QueryCapabilitiesOperation):
            return self._execute_query_capabilities_operation(operation)
        return self._execute_query_extensions_operation(operation)

    def _execute_persisted_knowledge_query_operation(
        self,
        operation: (
            QueryTestsOperation
            | QueryDefinitionsOperation
            | QueryEventsOperation
            | QueryRegistryItemsOperation
            | QueryResourcesOperation
            | QuerySessionArtifactsOperation
        ),
    ) -> OperationResult:
        if isinstance(operation, QueryTestsOperation):
            return QueryTestsOperationResult(
                tests=self._knowledge_base.query_tests(operation.query),
                context=self._knowledge_query_context(),
            )
        if isinstance(operation, QueryDefinitionsOperation):
            return QueryDefinitionsOperationResult(
                definitions=self._knowledge_base.query_definitions(
                    operation.query,
                ),
                context=self._knowledge_query_context(),
            )
        if isinstance(operation, QueryEventsOperation):
            return QueryEventsOperationResult(
                events=self._knowledge_base.query_domain_events(
                    operation.query,
                ),
                context=self._knowledge_query_context(),
            )
        if isinstance(operation, QueryRegistryItemsOperation):
            return QueryRegistryItemsOperationResult(
                registry_snapshots=self._knowledge_base.query_registry_items(
                    operation.query,
                ),
                context=self._knowledge_query_context(),
            )
        if isinstance(operation, QueryResourcesOperation):
            return QueryResourcesOperationResult(
                resources=self._knowledge_base.query_resources(
                    operation.query,
                ),
                context=self._knowledge_query_context(),
            )
        return QuerySessionArtifactsOperationResult(
            artifacts=self._knowledge_base.query_session_artifacts(
                operation.query,
            ),
            context=self._knowledge_query_context(),
        )

    def describe_engine_dependencies(self) -> tuple[EngineDependencyRule, ...]:
        active_engine_names = {engine.name for engine in self.engines}
        rules_by_key: dict[str, EngineDependencyRule] = {}
        for engine in self.engines:
            for rule in engine.describe_engine_dependencies():
                if rule.source_engine_name not in active_engine_names:
                    continue

                if rule.target_engine_name not in active_engine_names:
                    continue

                rules_by_key[build_engine_dependency_rule_key(rule)] = rule

        return tuple(rules_by_key[key] for key in sorted(rules_by_key))

    def _query_engine_dependencies(
        self,
        operation: QueryEngineDependenciesOperation,
    ) -> tuple[EngineDependencyRule, ...]:
        return tuple(
            rule
            for rule in self.describe_engine_dependencies()
            if operation.query.matches(rule)
        )

    def _project_engine_dependency_issues(
        self,
        rules: tuple[EngineDependencyRule, ...],
        operation: QueryEngineDependenciesOperation,
    ) -> tuple[ProjectedEngineDependencyIssue, ...]:
        plan_id = operation.query.plan_id
        if plan_id is None:
            latest_plan = self._knowledge_base.snapshot().latest_plan
            plan_id = None if latest_plan is None else latest_plan.plan_id

        if plan_id is None:
            return ()

        failed_tests = self._knowledge_base.query_tests(
            TestKnowledgeQuery(plan_id=plan_id),
        )
        projected_issues: list[ProjectedEngineDependencyIssue] = []
        for rule in rules:
            if rule.projection_policy == 'diagnostic_only':
                continue

            for test in failed_tests:
                if test.engine_name != rule.source_engine_name:
                    continue

                if test.status not in {
                    TestResultStatus.FAILED.value,
                    TestResultStatus.ERROR.value,
                }:
                    continue

                severity: Literal['warning', 'error']
                if rule.projection_policy == 'degrade_to_explain':
                    severity = 'warning'
                else:
                    severity = 'error'

                projected_issues.append(
                    ProjectedEngineDependencyIssue(
                        source_engine_name=rule.source_engine_name,
                        target_engine_name=rule.target_engine_name,
                        dependency_kind=rule.dependency_kind,
                        projection_policy=rule.projection_policy,
                        source_node_stable_id=test.node_stable_id,
                        source_test_name=test.test_name,
                        source_test_path=test.test_path,
                        source_status=test.status,
                        severity=severity,
                        message=(
                            'Failure in '
                            f'{rule.source_engine_name} test '
                            f'{test.test_name!r} is projected to '
                            f'{rule.target_engine_name} as '
                            f'{rule.projection_policy}'
                        ),
                        plan_id=test.plan_id,
                        trace_id=test.trace_id,
                    ),
                )

        return tuple(projected_issues)

    async def _execute_draft_validation_operation(
        self,
        operation: DraftValidationOperation,
    ) -> DraftValidationOperationResult:
        if Path(operation.test_path).name == 'cosecha.toml':
            return DraftValidationOperationResult(
                engine_name=operation.engine_name,
                test_path=operation.test_path,
                validation=self._validate_cosecha_manifest_draft(operation),
                freshness='fresh',
            )

        engine = self._find_engine_by_name(operation.engine_name)
        if engine is None:
            msg = (
                f'Unknown engine for draft validation: {operation.engine_name}'
            )
            raise ValueError(msg)
        if not isinstance(engine, DraftValidatingEngine):
            msg = (
                'Engine does not support draft validation: '
                f'{operation.engine_name}'
            )
            raise TypeError(msg)

        validation = await engine.validate_draft(
            operation.source_content,
            Path(operation.test_path),
        )
        return DraftValidationOperationResult(
            engine_name=engine.name,
            test_path=operation.test_path,
            validation=validation,
            freshness='fresh',
        )

    def _validate_cosecha_manifest_draft(
        self,
        operation: DraftValidationOperation,
    ) -> DraftValidationResult:
        try:
            parse_cosecha_manifest_text(
                operation.source_content,
                manifest_path=Path(operation.test_path),
                resolve_symbols=True,
            )
        except (ManifestValidationError, ValueError) as error:
            return DraftValidationResult(
                test_count=0,
                issues=(
                    DraftValidationIssue(
                        code='cosecha_manifest_invalid',
                        message=str(error),
                        severity='error',
                    ),
                ),
            )

        return DraftValidationResult(test_count=0)

    async def _execute_definition_resolution_operation(
        self,
        operation: ResolveDefinitionOperation,
    ) -> ResolveDefinitionOperationResult:
        engine = self._find_engine_by_name(operation.engine_name)
        if engine is None:
            msg = (
                'Unknown engine for definition resolution: '
                f'{operation.engine_name}'
            )
            raise ValueError(msg)
        if not isinstance(engine, DefinitionResolvingEngine):
            msg = (
                'Engine does not support definition resolution: '
                f'{operation.engine_name}'
            )
            raise TypeError(msg)

        definitions = await engine.resolve_definition(
            test_path=Path(operation.test_path),
            step_type=operation.step_type,
            step_text=operation.step_text,
        )
        return ResolveDefinitionOperationResult(
            definitions=definitions,
            freshness='fresh',
        )

    def _execute_query_capabilities_operation(
        self,
        operation: QueryCapabilitiesOperation,
    ) -> QueryCapabilitiesOperationResult:
        snapshots = self.describe_system_capabilities()
        if operation.component_kind is not None:
            snapshots = tuple(
                snapshot
                for snapshot in snapshots
                if snapshot.component_kind == operation.component_kind
            )
        if operation.component_name is not None:
            snapshots = tuple(
                snapshot
                for snapshot in snapshots
                if snapshot.component_name == operation.component_name
            )
        return QueryCapabilitiesOperationResult(
            snapshots=snapshots,
        )

    def _execute_query_extensions_operation(
        self,
        operation: QueryExtensionsOperation,
    ) -> QueryExtensionsOperationResult:
        snapshots = self.describe_system_extensions()
        if operation.query.extension_kind is not None:
            snapshots = tuple(
                snapshot
                for snapshot in snapshots
                if (
                    snapshot.descriptor.extension_kind
                    == operation.query.extension_kind
                )
            )
        if operation.query.component_name is not None:
            snapshots = tuple(
                snapshot
                for snapshot in snapshots
                if snapshot.component_name == operation.query.component_name
            )
        if operation.query.canonical_name is not None:
            snapshots = tuple(
                snapshot
                for snapshot in snapshots
                if (
                    snapshot.descriptor.canonical_name
                    == operation.query.canonical_name
                )
            )
        if operation.query.stability is not None:
            snapshots = tuple(
                snapshot
                for snapshot in snapshots
                if snapshot.descriptor.stability == operation.query.stability
            )
        return QueryExtensionsOperationResult(snapshots=snapshots)

    async def run_execution_plan(
        self,
        execution_plan: tuple[TestExecutionNode, ...],
        selection_labels: list[str] | None = None,
        test_limit: int | None = None,
    ) -> None:
        await self.run_tests(
            selection_labels,
            test_limit,
            execution_plan=validate_execution_plan(execution_plan),
        )

    def build_injected_execution_plan(
        self,
        engine: Engine,
        tests,
        *,
        source_contents_by_test_path: dict[Path, str] | None = None,
    ) -> tuple[TestExecutionNode, ...]:
        step_directories = self._build_engine_step_directories(engine)
        return validate_execution_plan(
            tuple(
                TestExecutionNode(
                    id=build_execution_node_id(
                        engine.name,
                        build_test_path_label(
                            self.config.root_path,
                            test.path,
                        ),
                        index,
                    ),
                    stable_id=build_execution_node_stable_id(
                        self.config.root_path,
                        engine.name,
                        test,
                    ),
                    engine=engine,
                    test=test,
                    engine_name=engine.name,
                    test_name=self._build_test_name(test),
                    test_path=build_test_path_label(
                        self.config.root_path,
                        test.path,
                    ),
                    source_content=(
                        source_contents_by_test_path.get(test.path)
                        if source_contents_by_test_path is not None
                        and test.path is not None
                        else None
                    ),
                    required_step_texts=self._build_required_step_texts(
                        test,
                    ),
                    step_candidate_files=(
                        self._build_engine_step_candidate_files(
                            engine,
                            test,
                            self._build_required_step_texts(test),
                        )
                    ),
                    step_directories=step_directories,
                    resource_requirements=test.get_resource_requirements(),
                )
                for index, test in enumerate(tests)
            ),
        )

    def has_failures(self) -> bool:
        # Consideramos fallo tanto un test en rojo como un fichero que no se
        # pudo cargar durante la recoleccion.
        return any(
            engine.collector.failed_files
            or any(test.has_failed for test in engine.get_collected_tests())
            for engine in self.engines
        )

    def _count_collected_tests(self) -> int:
        # Tests efectivamente recogidos por los engines. Los ficheros que
        # fallaron en colleccion no cuentan como tests aqui; ya los cubre
        # `has_failures()` como motivo de exit != 0.
        return sum(
            sum(1 for _ in engine.get_collected_tests())
            for engine in self.engines
        )

    def find_engine(self, test_file: str | Path) -> Engine | None:
        """Find the first engine that collected a test file.

        Iterates through the list of engines, returning the first engine
        that has collected the specified test file. If no engine has
        collected the file, returns None.
        """
        for engine in self.engines:
            if engine.is_file_collected(test_file) or engine.is_file_failed(
                test_file,
            ):
                return engine

        return None
