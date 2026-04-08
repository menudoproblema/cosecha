from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path

    from cosecha.core.execution_ir import (
        PlanExplanation,
        PlanningAnalysis,
        PlanningMode,
    )
    from cosecha.core.operations import ResolvedDefinition


type DraftIssueSeverity = Literal['error', 'warning']
type CapabilitySupportLevel = Literal[
    'supported',
    'accepted_noop',
    'unsupported',
]
type CapabilityComponentKind = Literal[
    'engine',
    'reporter',
    'plugin',
    'runtime',
    'instrumentation',
]
type CapabilityAttributeValue = str | bool | int | float | tuple[str, ...]
type CapabilityStability = Literal['stable', 'experimental']

CAPABILITY_API_VERSION = 1

CAPABILITY_DRAFT_VALIDATION = 'draft_validation'
CAPABILITY_ARTIFACT_OUTPUT = 'artifact_output'
CAPABILITY_HUMAN_OUTPUT = 'human_output'
CAPABILITY_LIVE_EXECUTION_OBSERVABILITY = 'live_execution_observability'
CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE = 'library_definition_knowledge'
CAPABILITY_PLAN_EXPLANATION = 'plan_explanation'
CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS = (
    'produces_ephemeral_artifacts'
)
CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE = 'project_definition_knowledge'
CAPABILITY_PROJECT_REGISTRY_KNOWLEDGE = 'project_registry_knowledge'
CAPABILITY_REPORT_LIFECYCLE = 'report_lifecycle'
CAPABILITY_RESULT_PROJECTION = 'result_projection'
CAPABILITY_SELECTION_LABELS = 'selection_labels'
CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY = (
    'static_project_definition_discovery'
)
CAPABILITY_STRUCTURED_OUTPUT = 'structured_output'
CAPABILITY_LAZY_PROJECT_DEFINITION_LOADING = 'lazy_project_definition_loading'


@dataclass(slots=True, frozen=True)
class DraftValidationIssue:
    code: str
    message: str
    severity: DraftIssueSeverity = 'error'
    line: int | None = None
    column: int | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> DraftValidationIssue:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class DraftValidationResult:
    test_count: int
    required_step_texts: tuple[tuple[str, str], ...] = field(
        default_factory=tuple,
    )
    step_candidate_files: tuple[str, ...] = field(default_factory=tuple)
    issues: tuple[DraftValidationIssue, ...] = field(default_factory=tuple)

    @property
    def is_valid(self) -> bool:
        return not any(issue.severity == 'error' for issue in self.issues)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> DraftValidationResult:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class CapabilityAttribute:
    name: str
    value: CapabilityAttributeValue

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> CapabilityAttribute:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class CapabilityOperationBinding:
    operation_type: str
    result_type: str | None = None
    freshness: str | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> CapabilityOperationBinding:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class CapabilityDescriptor:
    name: str
    level: CapabilitySupportLevel
    api_version: int = CAPABILITY_API_VERSION
    stability: CapabilityStability = 'stable'
    summary: str = ''
    attributes: tuple[CapabilityAttribute, ...] = field(
        default_factory=tuple,
    )
    operations: tuple[CapabilityOperationBinding, ...] = field(
        default_factory=tuple,
    )
    delivery_mode: str | None = None
    granularity: str | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> CapabilityDescriptor:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class CapabilityComponentSnapshot:
    component_name: str
    component_kind: CapabilityComponentKind
    capabilities: tuple[CapabilityDescriptor, ...]
    api_version: int = CAPABILITY_API_VERSION

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> CapabilityComponentSnapshot:
        return from_builtins_dict(data, target_type=cls)


@runtime_checkable
class DraftValidatingEngine(Protocol):
    async def validate_draft(
        self,
        source_content: str,
        test_path: Path,
    ) -> DraftValidationResult: ...


@runtime_checkable
class DefinitionResolvingEngine(Protocol):
    async def resolve_definition(
        self,
        *,
        test_path: Path,
        step_type: str,
        step_text: str,
    ) -> tuple[ResolvedDefinition, ...]: ...


@runtime_checkable
class ProjectDefinitionKnowledgeEngine(Protocol):
    def get_project_definition_index(self) -> object: ...


@runtime_checkable
class ExplainablePlanner(Protocol):
    def build_execution_plan_analysis(
        self,
        *paths: Path,
        mode: PlanningMode = 'strict',
    ) -> PlanningAnalysis: ...

    def explain_execution_plan(
        self,
        *paths: Path,
        mode: PlanningMode = 'relaxed',
    ) -> PlanExplanation: ...


@runtime_checkable
class CapabilityDescribingComponent(Protocol):
    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]: ...


def build_capability_map(
    descriptors: tuple[CapabilityDescriptor, ...],
) -> dict[str, CapabilityDescriptor]:
    return {descriptor.name: descriptor for descriptor in descriptors}


def build_component_capability_snapshot(
    *,
    component_name: str,
    component_kind: CapabilityComponentKind,
    descriptors: tuple[CapabilityDescriptor, ...],
) -> CapabilityComponentSnapshot:
    return CapabilityComponentSnapshot(
        component_name=component_name,
        component_kind=component_kind,
        capabilities=descriptors,
    )
