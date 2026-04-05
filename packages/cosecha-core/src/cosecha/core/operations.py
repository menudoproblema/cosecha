from __future__ import annotations

from dataclasses import dataclass, field
from os import fspath
from typing import TYPE_CHECKING, Literal

from cosecha.core.capabilities import (
    CapabilityComponentSnapshot,  # noqa: TC001
    DraftValidationResult,  # noqa: TC001
)
from cosecha.core.domain_events import (
    DomainEvent,
    deserialize_domain_event,
    serialize_domain_event,
)
from cosecha.core.engine_dependencies import (
    EngineDependencyQuery,
    EngineDependencyRule,
    ProjectedEngineDependencyIssue,
)
from cosecha.core.execution_ir import (
    PlanExplanation,  # noqa: TC001
    PlanningAnalysis,  # noqa: TC001
    PlanningMode,
    TestExecutionNodeSnapshot,  # noqa: TC001
)
from cosecha.core.extensions import (
    ExtensionComponentSnapshot,
    ExtensionQuery,
)
from cosecha.core.knowledge_base import (
    DefinitionKnowledge,
    DefinitionKnowledgeQuery,
    DomainEventQuery,
    LiveExecutionSnapshot,
    LiveLogChunk,
    ResourceKnowledge,
    ResourceKnowledgeQuery,
    SessionArtifactQuery,
    TestKnowledge,
    TestKnowledgeQuery,
)
from cosecha.core.registry_knowledge import (
    RegistryKnowledgeQuery,  # noqa: TC001
    RegistryKnowledgeSnapshot,  # noqa: TC001
)
from cosecha.core.serialization import from_builtins_dict, to_builtins_dict
from cosecha.core.session_artifacts import SessionArtifact  # noqa: TC001


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from cosecha.core.execution_ir import PlanningMode
    from cosecha.core.scheduler import SchedulingDecision


type OperationType = Literal[
    'capabilities.query',
    'extensions.query',
    'execution.subscribe',
    'execution.live_status',
    'execution.live_tail',
    'engine_dependencies.query',
    'definition.resolve',
    'draft.validate',
    'knowledge.query_events',
    'knowledge.query_definitions',
    'knowledge.query_resources',
    'knowledge.query_registry_items',
    'knowledge.query_session_artifacts',
    'knowledge.query_tests',
    'plan.analyze',
    'plan.explain',
    'plan.simulate',
    'run',
]
type OperationResultType = Literal[
    'capabilities.snapshots',
    'extensions.snapshots',
    'execution.subscribe',
    'execution.live_status',
    'execution.live_tail',
    'engine.dependencies',
    'definition.resolution',
    'draft.validation',
    'knowledge.events',
    'knowledge.definitions',
    'knowledge.resources',
    'knowledge.registry_items',
    'knowledge.session_artifacts',
    'knowledge.tests',
    'plan.analysis',
    'plan.explanation',
    'plan.simulation',
    'run.result',
]
type OperationIntent = Literal['read_only', 'dry_run', 'mutation']
type KnowledgeFreshness = Literal['fresh', 'partial', 'stale', 'unknown']
type KnowledgeSource = Literal[
    'live_session',
    'persistent_knowledge_base',
]
type LiveExecutionSource = Literal['live_projection']
type LiveExecutionVolatility = Literal['volatile']
type LiveExecutionDeliveryMode = Literal['poll_by_cursor']
type LiveExecutionGranularity = Literal[
    'streaming',
    'consolidated_response',
]
type DefinitionResolutionSource = Literal[
    'runtime_registry',
    'static_catalog',
]
type SimulationResourcePolicy = Literal['non_live_only']


def normalize_operation_paths(
    paths: Iterable[str | Path] | None,
) -> tuple[str, ...]:
    if paths is None:
        return ()

    return tuple(fspath(path) for path in paths)


@dataclass(slots=True, frozen=True)
class KnowledgeQueryContext:
    source: KnowledgeSource
    freshness: KnowledgeFreshness

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> KnowledgeQueryContext:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class LiveExecutionContext:
    source: LiveExecutionSource = 'live_projection'
    volatility: LiveExecutionVolatility = 'volatile'
    delivery_mode: LiveExecutionDeliveryMode = 'poll_by_cursor'
    granularity: LiveExecutionGranularity = 'streaming'
    truncated: bool = False

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LiveExecutionContext:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryCapabilitiesOperation:
    component_kind: str | None = None
    component_name: str | None = None
    operation_type: OperationType = 'capabilities.query'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryCapabilitiesOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryExtensionsOperation:
    query: ExtensionQuery = field(default_factory=ExtensionQuery)
    operation_type: OperationType = 'extensions.query'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryExtensionsOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class LiveStatusQuery:
    session_id: str | None = None
    node_stable_id: str | None = None
    worker_id: int | None = None
    include_engine_snapshots: bool = False

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LiveStatusQuery:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryLiveStatusOperation:
    query: LiveStatusQuery = field(default_factory=LiveStatusQuery)
    operation_type: OperationType = 'execution.live_status'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> QueryLiveStatusOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryLiveTailOperation:
    query: DomainEventQuery = field(default_factory=DomainEventQuery)
    operation_type: OperationType = 'execution.live_tail'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> QueryLiveTailOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryEventsOperation:
    query: DomainEventQuery = field(default_factory=DomainEventQuery)
    operation_type: OperationType = 'knowledge.query_events'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> QueryEventsOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class LiveSubscriptionQuery:
    session_id: str | None = None
    node_stable_id: str | None = None
    worker_id: int | None = None
    after_sequence_number: int | None = None
    limit: int | None = None
    timeout_seconds: float | None = None
    include_engine_snapshots: bool = False

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LiveSubscriptionQuery:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryLiveSubscriptionOperation:
    query: LiveSubscriptionQuery = field(
        default_factory=LiveSubscriptionQuery,
    )
    operation_type: OperationType = 'execution.subscribe'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryLiveSubscriptionOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryEngineDependenciesOperation:
    query: EngineDependencyQuery = field(
        default_factory=EngineDependencyQuery,
    )
    operation_type: OperationType = 'engine_dependencies.query'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryEngineDependenciesOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ResolvedDefinition:
    engine_name: str
    file_path: str
    line: int
    step_type: str
    patterns: tuple[str, ...]
    resolution_source: DefinitionResolutionSource
    column: int | None = None
    function_name: str | None = None
    category: str | None = None
    provider_kind: str | None = None
    provider_name: str | None = None
    runtime_required: bool = False
    runtime_reason: str | None = None
    declaration_origin: str | None = None
    documentation: str | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ResolvedDefinition:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RunOperation:
    paths: tuple[str, ...] = ()
    selection_labels: tuple[str, ...] = ()
    test_limit: int | None = None
    operation_type: OperationType = 'run'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RunOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class AnalyzePlanOperation:
    paths: tuple[str, ...] = ()
    selection_labels: tuple[str, ...] = ()
    test_limit: int | None = None
    mode: PlanningMode = 'strict'
    operation_type: OperationType = 'plan.analyze'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> AnalyzePlanOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ExplainPlanOperation:
    paths: tuple[str, ...] = ()
    selection_labels: tuple[str, ...] = ()
    test_limit: int | None = None
    mode: PlanningMode = 'relaxed'
    operation_type: OperationType = 'plan.explain'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ExplainPlanOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class SimulatePlanOperation:
    paths: tuple[str, ...] = ()
    selection_labels: tuple[str, ...] = ()
    test_limit: int | None = None
    mode: PlanningMode = 'relaxed'
    operation_type: OperationType = 'plan.simulate'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SimulatePlanOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ResolveDefinitionOperation:
    engine_name: str
    test_path: str
    step_type: str
    step_text: str
    operation_type: OperationType = 'definition.resolve'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ResolveDefinitionOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class DraftValidationOperation:
    engine_name: str
    test_path: str
    source_content: str
    operation_type: OperationType = 'draft.validate'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> DraftValidationOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryTestsOperation:
    query: TestKnowledgeQuery
    operation_type: OperationType = 'knowledge.query_tests'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> QueryTestsOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryDefinitionsOperation:
    query: DefinitionKnowledgeQuery
    operation_type: OperationType = 'knowledge.query_definitions'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryDefinitionsOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryRegistryItemsOperation:
    query: RegistryKnowledgeQuery
    operation_type: OperationType = 'knowledge.query_registry_items'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryRegistryItemsOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryResourcesOperation:
    query: ResourceKnowledgeQuery
    operation_type: OperationType = 'knowledge.query_resources'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> QueryResourcesOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QuerySessionArtifactsOperation:
    query: SessionArtifactQuery
    operation_type: OperationType = 'knowledge.query_session_artifacts'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QuerySessionArtifactsOperation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RunOperationResult:
    has_failures: bool
    result_type: OperationResultType = 'run.result'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RunOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class AnalyzePlanOperationResult:
    analysis: PlanningAnalysis
    result_type: OperationResultType = 'plan.analysis'

    def to_dict(self) -> dict[str, object]:
        return {
            'analysis': {
                'executable': self.analysis.executable,
                'explanation': self.analysis.explanation.to_dict(),
                'issues': [issue.to_dict() for issue in self.analysis.issues],
                'mode': self.analysis.mode,
                'node_semantics': [
                    semantics.to_dict()
                    for semantics in self.analysis.node_semantics
                ],
                'plan': [
                    node.snapshot.to_dict() for node in self.analysis.plan
                ],
            },
            'result_type': self.result_type,
        }


@dataclass(slots=True, frozen=True)
class ExplainPlanOperationResult:
    explanation: PlanExplanation
    result_type: OperationResultType = 'plan.explanation'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ExplainPlanOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class HypotheticalSchedulingDecision:
    node_id: str
    node_stable_id: str
    worker_slot: int
    max_attempts: int
    timeout_seconds: float | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> HypotheticalSchedulingDecision:
        return from_builtins_dict(data, target_type=cls)

    @classmethod
    def from_scheduling_decision(
        cls,
        decision: SchedulingDecision,
    ) -> HypotheticalSchedulingDecision:
        return cls(
            node_id=decision.node_id,
            node_stable_id=decision.node_stable_id,
            worker_slot=decision.worker_slot,
            max_attempts=decision.max_attempts,
            timeout_seconds=decision.timeout_seconds,
        )


@dataclass(slots=True, frozen=True)
class SimulatePlanOperationResult:
    explanation: PlanExplanation
    plan: tuple[TestExecutionNodeSnapshot, ...]
    hypothetical_scheduling: tuple[HypotheticalSchedulingDecision, ...]
    resource_policy: SimulationResourcePolicy = 'non_live_only'
    result_type: OperationResultType = 'plan.simulation'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SimulatePlanOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ResolveDefinitionOperationResult:
    definitions: tuple[ResolvedDefinition, ...]
    freshness: KnowledgeFreshness = 'fresh'
    result_type: OperationResultType = 'definition.resolution'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> ResolveDefinitionOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryCapabilitiesOperationResult:
    snapshots: tuple[CapabilityComponentSnapshot, ...]
    result_type: OperationResultType = 'capabilities.snapshots'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryCapabilitiesOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryExtensionsOperationResult:
    snapshots: tuple[ExtensionComponentSnapshot, ...]
    result_type: OperationResultType = 'extensions.snapshots'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryExtensionsOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryLiveStatusOperationResult:
    snapshot: LiveExecutionSnapshot
    context: LiveExecutionContext
    result_type: OperationResultType = 'execution.live_status'

    def to_dict(self) -> dict[str, object]:
        return {
            'context': self.context.to_dict(),
            'result_type': self.result_type,
            'snapshot': self.snapshot.to_dict(),
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryLiveStatusOperationResult:
        return cls(
            snapshot=LiveExecutionSnapshot.from_dict(
                cast_required_mapping(data, 'snapshot'),
            ),
            context=LiveExecutionContext.from_dict(
                cast_required_mapping(data, 'context'),
            ),
        )


@dataclass(slots=True, frozen=True)
class QueryLiveTailOperationResult:
    events: tuple[DomainEvent, ...]
    context: LiveExecutionContext
    log_chunks: tuple[LiveLogChunk, ...] = ()
    result_type: OperationResultType = 'execution.live_tail'

    def to_dict(self) -> dict[str, object]:
        return {
            'context': self.context.to_dict(),
            'events': [serialize_domain_event(event) for event in self.events],
            'log_chunks': [
                log_chunk.to_dict() for log_chunk in self.log_chunks
            ],
            'result_type': self.result_type,
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryLiveTailOperationResult:
        return cls(
            events=tuple(
                deserialize_domain_event(record)
                for record in data.get('events', ())
                if isinstance(record, dict)
            ),
            log_chunks=tuple(
                LiveLogChunk.from_dict(record)
                for record in data.get('log_chunks', ())
                if isinstance(record, dict)
            ),
            context=LiveExecutionContext.from_dict(
                cast_required_mapping(data, 'context'),
            ),
        )


@dataclass(slots=True, frozen=True)
class QueryLiveSubscriptionOperationResult:
    snapshot: LiveExecutionSnapshot
    events: tuple[DomainEvent, ...]
    log_chunks: tuple[LiveLogChunk, ...]
    next_sequence_number: int
    context: LiveExecutionContext
    result_type: OperationResultType = 'execution.subscribe'

    def to_dict(self) -> dict[str, object]:
        return {
            'context': self.context.to_dict(),
            'events': [serialize_domain_event(event) for event in self.events],
            'log_chunks': [
                log_chunk.to_dict() for log_chunk in self.log_chunks
            ],
            'next_sequence_number': self.next_sequence_number,
            'result_type': self.result_type,
            'snapshot': self.snapshot.to_dict(),
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryLiveSubscriptionOperationResult:
        return cls(
            snapshot=LiveExecutionSnapshot.from_dict(
                cast_required_mapping(data, 'snapshot'),
            ),
            events=tuple(
                deserialize_domain_event(record)
                for record in data.get('events', ())
                if isinstance(record, dict)
            ),
            log_chunks=tuple(
                LiveLogChunk.from_dict(record)
                for record in data.get('log_chunks', ())
                if isinstance(record, dict)
            ),
            next_sequence_number=int(data.get('next_sequence_number', 0)),
            context=LiveExecutionContext.from_dict(
                cast_required_mapping(data, 'context'),
            ),
        )


@dataclass(slots=True, frozen=True)
class QueryEngineDependenciesOperationResult:
    rules: tuple[EngineDependencyRule, ...]
    projected_issues: tuple[ProjectedEngineDependencyIssue, ...] = ()
    result_type: OperationResultType = 'engine.dependencies'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryEngineDependenciesOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class DraftValidationOperationResult:
    engine_name: str
    test_path: str
    validation: DraftValidationResult
    freshness: KnowledgeFreshness = 'fresh'
    result_type: OperationResultType = 'draft.validation'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> DraftValidationOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryTestsOperationResult:
    tests: tuple[TestKnowledge, ...]
    context: KnowledgeQueryContext
    result_type: OperationResultType = 'knowledge.tests'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> QueryTestsOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryDefinitionsOperationResult:
    definitions: tuple[DefinitionKnowledge, ...]
    context: KnowledgeQueryContext
    result_type: OperationResultType = 'knowledge.definitions'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryDefinitionsOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryRegistryItemsOperationResult:
    registry_snapshots: tuple[RegistryKnowledgeSnapshot, ...]
    context: KnowledgeQueryContext
    result_type: OperationResultType = 'knowledge.registry_items'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryRegistryItemsOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryResourcesOperationResult:
    resources: tuple[ResourceKnowledge, ...]
    context: KnowledgeQueryContext
    result_type: OperationResultType = 'knowledge.resources'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QueryResourcesOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QuerySessionArtifactsOperationResult:
    artifacts: tuple[SessionArtifact, ...]
    context: KnowledgeQueryContext
    result_type: OperationResultType = 'knowledge.session_artifacts'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> QuerySessionArtifactsOperationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class QueryEventsOperationResult:
    events: tuple[DomainEvent, ...]
    context: KnowledgeQueryContext
    result_type: OperationResultType = 'knowledge.events'

    def to_dict(self) -> dict[str, object]:
        return {
            'context': self.context.to_dict(),
            'events': [serialize_domain_event(event) for event in self.events],
            'result_type': self.result_type,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> QueryEventsOperationResult:
        return cls(
            events=tuple(
                deserialize_domain_event(record)
                for record in data.get('events', ())
                if isinstance(record, dict)
            ),
            context=KnowledgeQueryContext.from_dict(
                cast_required_mapping(data, 'context'),
            ),
        )


type Operation = (
    QueryCapabilitiesOperation
    | QueryExtensionsOperation
    | QueryLiveSubscriptionOperation
    | QueryLiveStatusOperation
    | QueryLiveTailOperation
    | QueryEventsOperation
    | QueryEngineDependenciesOperation
    | RunOperation
    | AnalyzePlanOperation
    | ExplainPlanOperation
    | SimulatePlanOperation
    | ResolveDefinitionOperation
    | DraftValidationOperation
    | QueryTestsOperation
    | QueryDefinitionsOperation
    | QueryRegistryItemsOperation
    | QueryResourcesOperation
    | QuerySessionArtifactsOperation
)
type OperationResult = (
    QueryCapabilitiesOperationResult
    | QueryExtensionsOperationResult
    | QueryLiveSubscriptionOperationResult
    | QueryLiveStatusOperationResult
    | QueryLiveTailOperationResult
    | QueryEventsOperationResult
    | QueryEngineDependenciesOperationResult
    | RunOperationResult
    | AnalyzePlanOperationResult
    | ExplainPlanOperationResult
    | SimulatePlanOperationResult
    | ResolveDefinitionOperationResult
    | DraftValidationOperationResult
    | QueryTestsOperationResult
    | QueryDefinitionsOperationResult
    | QueryRegistryItemsOperationResult
    | QueryResourcesOperationResult
    | QuerySessionArtifactsOperationResult
)


def deserialize_operation(data: dict[str, object]) -> Operation:
    operation_type = str(data.get('operation_type', ''))
    operation_builders: dict[
        str,
        Callable[[dict[str, object]], Operation],
    ] = {
        'capabilities.query': QueryCapabilitiesOperation.from_dict,
        'extensions.query': QueryExtensionsOperation.from_dict,
        'execution.subscribe': QueryLiveSubscriptionOperation.from_dict,
        'execution.live_status': QueryLiveStatusOperation.from_dict,
        'execution.live_tail': QueryLiveTailOperation.from_dict,
        'knowledge.query_events': QueryEventsOperation.from_dict,
        'engine_dependencies.query': (
            QueryEngineDependenciesOperation.from_dict
        ),
        'definition.resolve': ResolveDefinitionOperation.from_dict,
        'draft.validate': DraftValidationOperation.from_dict,
        'knowledge.query_definitions': QueryDefinitionsOperation.from_dict,
        'knowledge.query_registry_items': (
            QueryRegistryItemsOperation.from_dict
        ),
        'knowledge.query_resources': QueryResourcesOperation.from_dict,
        'knowledge.query_session_artifacts': (
            QuerySessionArtifactsOperation.from_dict
        ),
        'knowledge.query_tests': QueryTestsOperation.from_dict,
        'plan.analyze': AnalyzePlanOperation.from_dict,
        'plan.explain': ExplainPlanOperation.from_dict,
        'plan.simulate': SimulatePlanOperation.from_dict,
        'run': RunOperation.from_dict,
    }
    operation_builder = operation_builders.get(operation_type)
    if operation_builder is not None:
        return operation_builder(data)

    msg = f'Unknown operation type: {operation_type}'
    raise ValueError(msg)


def cast_optional_int(value: object) -> int | None:
    if value is None:
        return None

    return int(value)


def cast_optional_float(value: object) -> float | None:
    if value is None:
        return None

    return float(value)


def cast_planning_mode(value: object | None) -> PlanningMode:
    if value in ('strict', 'relaxed'):
        return value

    msg = f'Invalid planning mode: {value!r}'
    raise ValueError(msg)


def cast_knowledge_freshness(
    value: object | None,
) -> KnowledgeFreshness:
    if value in ('fresh', 'partial', 'stale', 'unknown'):
        return value

    msg = f'Invalid knowledge freshness: {value!r}'
    raise ValueError(msg)


def cast_knowledge_source(
    value: object | None,
) -> KnowledgeSource:
    if value in ('live_session', 'persistent_knowledge_base'):
        return value

    msg = f'Invalid knowledge source: {value!r}'
    raise ValueError(msg)


def cast_live_execution_source(
    value: object | None,
) -> LiveExecutionSource:
    if value == 'live_projection':
        return value

    msg = f'Invalid live execution source: {value!r}'
    raise ValueError(msg)


def cast_live_execution_volatility(
    value: object | None,
) -> LiveExecutionVolatility:
    if value == 'volatile':
        return value

    msg = f'Invalid live execution volatility: {value!r}'
    raise ValueError(msg)


def cast_live_execution_delivery_mode(
    value: object | None,
) -> LiveExecutionDeliveryMode:
    if value in (None, 'poll_by_cursor'):
        return 'poll_by_cursor'

    msg = f'Invalid live execution delivery mode: {value!r}'
    raise ValueError(msg)


def cast_simulation_resource_policy(
    value: object | None,
) -> SimulationResourcePolicy:
    if value == 'non_live_only':
        return value

    return 'non_live_only'


def operation_intent(
    operation: Operation,
) -> OperationIntent:
    if isinstance(
        operation,
        (
            QueryCapabilitiesOperation,
            QueryExtensionsOperation,
            QueryEngineDependenciesOperation,
            QueryLiveStatusOperation,
            QueryLiveSubscriptionOperation,
            QueryLiveTailOperation,
            QueryTestsOperation,
            QueryDefinitionsOperation,
            QueryRegistryItemsOperation,
            QueryResourcesOperation,
            QuerySessionArtifactsOperation,
            AnalyzePlanOperation,
            ExplainPlanOperation,
            ResolveDefinitionOperation,
            DraftValidationOperation,
        ),
    ):
        return 'read_only'

    if isinstance(operation, SimulatePlanOperation):
        return 'dry_run'

    return 'mutation'


def cast_live_execution_granularity(
    value: object | None,
) -> LiveExecutionGranularity:
    if value in (None, 'streaming'):
        return 'streaming'
    if value == 'consolidated_response':
        return value

    msg = f'Invalid live execution granularity: {value!r}'
    raise ValueError(msg)


def cast_definition_resolution_source(
    value: object | None,
) -> DefinitionResolutionSource:
    if value in ('runtime_registry', 'static_catalog'):
        return value

    msg = f'Invalid definition resolution source: {value!r}'
    raise ValueError(msg)


def cast_optional_str(value: object) -> str | None:
    if value is None:
        return None

    return str(value)


def cast_required_mapping(
    data: dict[str, object],
    key: str,
) -> dict[str, object]:
    value = data.get(key)
    if isinstance(value, dict):
        return value

    msg = f'Expected a mapping in {key!r}'
    raise TypeError(msg)
