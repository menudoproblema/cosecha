from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from typing import TYPE_CHECKING, Literal

from cosecha.core.config import ConfigSnapshot  # noqa: TC001
from cosecha.core.items import ExecutionPredicateEvaluation
from cosecha.core.resources import (
    ResourceMaterializationSnapshot,
    normalize_resource_scope,
    validate_resource_requirements,
)
from cosecha.core.serialization import from_builtins_dict, to_builtins_dict
from cosecha.core.shadow import EphemeralArtifactCapability


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable
    from pathlib import Path

    from cosecha.core.engines import Engine
    from cosecha.core.items import TestItem
    from cosecha.core.resources import (
        ResourceMaterializationSnapshot,
        ResourceRequirement,
        ResourceTiming,
    )


type PlanningMode = Literal['strict', 'relaxed']
type PlanningIssueSeverity = Literal['error', 'warning']

EXECUTION_IR_SCHEMA_VERSION = 2


@dataclass(slots=True, frozen=True)
class PlanningIssue:
    code: str
    message: str
    severity: PlanningIssueSeverity = 'error'
    node_id: str | None = None
    node_stable_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> PlanningIssue:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class NodePlanningSemantics:
    node_id: str
    node_stable_id: str
    engine_name: str
    estimated_duration: float | None = None
    execution_predicate: ExecutionPredicateEvaluation = field(
        default_factory=ExecutionPredicateEvaluation,
    )
    resource_names: tuple[str, ...] = field(default_factory=tuple)
    required_step_texts: tuple[tuple[str, str], ...] = field(
        default_factory=tuple,
    )
    step_candidate_files: tuple[str, ...] = field(default_factory=tuple)
    step_directories: tuple[str, ...] = field(default_factory=tuple)
    runtime_hints: tuple[str, ...] = field(default_factory=tuple)
    issues: tuple[PlanningIssue, ...] = field(default_factory=tuple)

    @property
    def executable(self) -> bool:
        return not any(issue.severity == 'error' for issue in self.issues)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> NodePlanningSemantics:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class PlanExplanation:
    mode: PlanningMode
    executable: bool
    estimated_total_duration: float | None = None
    issues: tuple[PlanningIssue, ...] = field(default_factory=tuple)
    node_semantics: tuple[NodePlanningSemantics, ...] = field(
        default_factory=tuple,
    )
    execution_lineage: tuple[str, ...] = (
        'knowledge',
        'planning_semantics',
        'execution_ir',
    )
    fallback_reasons: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> PlanExplanation:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class PlanningAnalysis:
    mode: PlanningMode
    plan: tuple[TestExecutionNode, ...]
    issues: tuple[PlanningIssue, ...] = field(default_factory=tuple)
    node_semantics: tuple[NodePlanningSemantics, ...] = field(
        default_factory=tuple,
    )

    @property
    def executable(self) -> bool:
        return not any(issue.severity == 'error' for issue in self.issues)

    @property
    def explanation(self) -> PlanExplanation:
        return PlanExplanation(
            mode=self.mode,
            executable=self.executable,
            estimated_total_duration=sum(
                semantics.estimated_duration or 0.0
                for semantics in self.node_semantics
            )
            or None,
            issues=self.issues,
            node_semantics=self.node_semantics,
            fallback_reasons=(
                ('planning_issues_present',) if self.issues else ()
            ),
        )


@dataclass(slots=True, frozen=True)
class TestExecutionNodeSnapshot:
    __test__ = False
    id: str
    stable_id: str
    engine_name: str
    test_name: str
    test_path: str
    source_content: str | None = None
    required_step_texts: tuple[tuple[str, str], ...] = field(
        default_factory=tuple,
    )
    step_directories: tuple[str, ...] = field(default_factory=tuple)
    step_candidate_files: tuple[str, ...] = field(default_factory=tuple)
    resource_names: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> TestExecutionNodeSnapshot:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class TestExecutionNode:
    __test__ = False
    id: str
    stable_id: str
    engine: Engine
    test: TestItem
    engine_name: str
    test_name: str
    test_path: str
    source_content: str | None = None
    required_step_texts: tuple[tuple[str, str], ...] = field(
        default_factory=tuple,
    )
    step_directories: tuple[str, ...] = field(default_factory=tuple)
    step_candidate_files: tuple[str, ...] = field(default_factory=tuple)
    resource_requirements: tuple[ResourceRequirement, ...] = field(
        default_factory=tuple,
    )
    _snapshot: TestExecutionNodeSnapshot | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    @property
    def snapshot(self) -> TestExecutionNodeSnapshot:
        snapshot = self._snapshot
        if snapshot is None:
            snapshot = TestExecutionNodeSnapshot(
                id=self.id,
                stable_id=self.stable_id,
                engine_name=self.engine_name,
                test_name=self.test_name,
                test_path=self.test_path,
                source_content=self.source_content,
                required_step_texts=self.required_step_texts,
                step_candidate_files=self.step_candidate_files,
                step_directories=self.step_directories,
                resource_names=tuple(
                    requirement.name
                    for requirement in self.resource_requirements
                ),
            )
            object.__setattr__(self, '_snapshot', snapshot)

        return snapshot


@dataclass(slots=True, frozen=True)
class ExecutionRequest:
    __test__ = False
    cwd: str
    root_path: str
    config_snapshot: ConfigSnapshot
    node: TestExecutionNodeSnapshot
    workspace: dict[str, object] | None = None
    execution_context: dict[str, object] | None = None
    schema_version: int = EXECUTION_IR_SCHEMA_VERSION

    @classmethod
    def from_node(
        cls,
        cwd: Path,
        root_path: Path,
        node: TestExecutionNode,
    ) -> ExecutionRequest:
        return cls(
            cwd=str(cwd),
            root_path=str(root_path),
            workspace=(
                None
                if node.engine.config.workspace is None
                else node.engine.config.workspace.to_dict()
            ),
            execution_context=(
                None
                if node.engine.config.execution_context is None
                else node.engine.config.execution_context.to_dict()
            ),
            config_snapshot=node.engine.config.snapshot(),
            node=node.snapshot,
        )

    def __post_init__(self) -> None:
        cast_schema_version(self.schema_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ExecutionRequest:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ExecutionBootstrap:
    __test__ = False
    config_snapshot: ConfigSnapshot
    nodes: tuple[TestExecutionNodeSnapshot, ...]
    workspace: dict[str, object] | None = None
    execution_context: dict[str, object] | None = None
    ephemeral_capabilities: tuple[EphemeralArtifactCapability, ...] = field(
        default_factory=tuple,
    )
    resource_materialization_snapshots: tuple[
        ResourceMaterializationSnapshot,
        ...,
    ] = field(default_factory=tuple)
    schema_version: int = EXECUTION_IR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        cast_schema_version(self.schema_version)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_nodes(
        cls,
        nodes: Iterable[TestExecutionNode],
        *,
        ephemeral_capabilities: tuple[EphemeralArtifactCapability, ...] = (),
        resource_materialization_snapshots: tuple[
            ResourceMaterializationSnapshot,
            ...,
        ] = (),
    ) -> ExecutionBootstrap:
        node_tuple = tuple(nodes)
        if not node_tuple:
            msg = 'ExecutionBootstrap requires at least one node'
            raise ValueError(msg)

        return cls(
            config_snapshot=node_tuple[0].engine.config.snapshot(),
            workspace=(
                None
                if node_tuple[0].engine.config.workspace is None
                else node_tuple[0].engine.config.workspace.to_dict()
            ),
            execution_context=(
                None
                if node_tuple[0].engine.config.execution_context is None
                else node_tuple[0].engine.config.execution_context.to_dict()
            ),
            ephemeral_capabilities=ephemeral_capabilities,
            nodes=tuple(node.snapshot for node in node_tuple),
            resource_materialization_snapshots=(
                resource_materialization_snapshots
            ),
        )

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ExecutionBootstrap:
        return from_builtins_dict(data, target_type=cls)


def validate_execution_plan(
    plan: Iterable[TestExecutionNode],
    *,
    mode: PlanningMode = 'strict',
) -> tuple[TestExecutionNode, ...]:
    analysis = analyze_execution_plan(plan, mode=mode)
    if analysis.executable or mode == 'relaxed':
        return analysis.plan

    first_issue = analysis.issues[0]
    raise ValueError(first_issue.message)


def filter_execution_nodes(
    execution_plan: tuple[TestExecutionNode, ...],
    *,
    skip_labels: list[str],
    execute_labels: list[str],
    test_limit: int,
) -> tuple[TestExecutionNode, ...]:
    selected_nodes: list[TestExecutionNode] = []
    for node in execution_plan:
        if len(selected_nodes) >= test_limit:
            break

        test = node.test
        if any(
            test.has_selection_label(skip_label) for skip_label in skip_labels
        ):
            continue

        if not all(
            test.has_selection_label(label) for label in execute_labels
        ):
            continue

        selected_nodes.append(node)

    return tuple(selected_nodes)


def analyze_execution_plan(
    plan: Iterable[TestExecutionNode],
    *,
    mode: PlanningMode = 'strict',
) -> PlanningAnalysis:
    validated_plan = tuple(plan)
    seen_ids: set[str] = set()
    plan_issues: list[PlanningIssue] = []
    node_semantics: list[NodePlanningSemantics] = []

    for node in validated_plan:
        node_issues: list[PlanningIssue] = []
        if node.id in seen_ids:
            node_issues.append(
                PlanningIssue(
                    code='duplicated_node_id',
                    message=f'Duplicated execution node id: {node.id}',
                    node_id=node.id,
                    node_stable_id=node.stable_id,
                ),
            )
        else:
            seen_ids.add(node.id)

        node_issues.extend(_collect_node_resource_issues(node))
        plan_issues.extend(node_issues)
        node_semantics.append(
            NodePlanningSemantics(
                node_id=node.id,
                node_stable_id=node.stable_id,
                engine_name=node.engine_name,
                execution_predicate=_describe_execution_predicate(
                    node.test,
                ),
                resource_names=tuple(
                    requirement.name
                    for requirement in node.resource_requirements
                ),
                required_step_texts=node.required_step_texts,
                step_candidate_files=node.step_candidate_files,
                step_directories=node.step_directories,
                issues=tuple(node_issues),
            ),
        )

    return PlanningAnalysis(
        mode=mode,
        plan=validated_plan,
        issues=tuple(plan_issues),
        node_semantics=tuple(node_semantics),
    )


def build_test_path_label(
    root_path: Path,
    test_path: Path | None,
) -> str:
    if test_path is None:
        return ''

    try:
        return str(test_path.resolve().relative_to(root_path.resolve()))
    except Exception:
        return str(test_path)


def build_execution_node_id(
    engine_name: str,
    test_path_label: str,
    index: int,
) -> str:
    return f'{engine_name}:{test_path_label}:{index}'


def build_execution_node_stable_id(
    root_path: Path,
    engine_name: str,
    test,
) -> str:
    test_path_label = build_test_path_label(
        root_path,
        getattr(test, 'path', None),
    )
    anchor = _build_test_stable_anchor(test)
    digest = sha256(anchor.encode('utf-8')).hexdigest()[:12]
    return f'{engine_name}:{test_path_label}:{digest}'


def reorder_execution_plan_by_resource_affinity(
    plan: Iterable[TestExecutionNode],
    resource_timings: Iterable[ResourceTiming] = (),
    resource_costs: dict[tuple[str, str], float] | None = None,
) -> tuple[TestExecutionNode, ...]:
    enumerated_plan = tuple(enumerate(plan))
    effective_resource_costs = resource_costs or {
        (
            timing.name,
            normalize_resource_scope(timing.scope),
        ): timing.total_duration
        for timing in resource_timings
    }
    return tuple(
        node
        for _index, node in sorted(
            enumerated_plan,
            key=lambda item: (
                -_build_resource_cost(
                    item[1],
                    scope='run',
                    resource_costs=effective_resource_costs,
                ),
                _build_resource_affinity_signature(item[1], scope='run'),
                -_build_resource_cost(
                    item[1],
                    scope='worker',
                    resource_costs=effective_resource_costs,
                ),
                _build_resource_affinity_signature(item[1], scope='worker'),
                -_build_resource_cost(
                    item[1],
                    scope='test',
                    resource_costs=effective_resource_costs,
                ),
                _build_resource_affinity_signature(item[1], scope='test'),
                item[0],
            ),
        )
    )


def _collect_node_resource_issues(
    node: TestExecutionNode,
) -> tuple[PlanningIssue, ...]:
    seen_resource_names: set[str] = set()
    duplicated_resources: set[str] = set()
    for requirement in node.resource_requirements:
        if requirement.name in seen_resource_names:
            duplicated_resources.add(requirement.name)
            continue

        seen_resource_names.add(requirement.name)

    issues: list[PlanningIssue] = []
    if duplicated_resources:
        issues.append(
            PlanningIssue(
                code='duplicated_resource_requirements',
                message=(
                    'Duplicated resource requirements for '
                    f'{node.id}: {", ".join(sorted(duplicated_resources))}'
                ),
                node_id=node.id,
                node_stable_id=node.stable_id,
            ),
        )

    try:
        validate_resource_requirements(node.resource_requirements)
    except ValueError as error:
        issues.append(
            PlanningIssue(
                code='invalid_resource_requirements',
                message=f'Invalid resources for {node.id}: {error}',
                node_id=node.id,
                node_stable_id=node.stable_id,
            ),
        )

    return tuple(issues)


def cast_schema_version(value: object) -> int:
    if value is None:
        return EXECUTION_IR_SCHEMA_VERSION

    schema_version = int(value)
    if schema_version != EXECUTION_IR_SCHEMA_VERSION:
        msg = (
            'Unsupported execution IR schema version: '
            f'{schema_version} '
            f'(expected {EXECUTION_IR_SCHEMA_VERSION})'
        )
        raise ValueError(msg)

    return schema_version


def _build_resource_affinity_signature(
    node: TestExecutionNode,
    *,
    scope: str,
) -> tuple[str, ...]:
    return tuple(
        sorted(
            requirement.name
            for requirement in node.resource_requirements
            if normalize_resource_scope(requirement.scope) == scope
        ),
    )


def _build_resource_cost(
    node: TestExecutionNode,
    *,
    scope: str,
    resource_costs: dict[tuple[str, str], float],
) -> float:
    return sum(
        resource_costs.get(
            (
                requirement.name,
                normalize_resource_scope(requirement.scope),
            ),
            0.0,
        )
        for requirement in node.resource_requirements
        if normalize_resource_scope(requirement.scope) == scope
    )


def _build_test_stable_anchor(test) -> str:
    location_anchors: list[str] = []
    for label in ('feature', 'scenario', 'example'):
        entity = getattr(test, label, None)
        location = getattr(entity, 'location', None)
        if location is None:
            continue

        location_anchors.append(
            ':'.join(
                (
                    label,
                    str(getattr(location, 'line', 0)),
                    str(getattr(location, 'column', 0) or 0),
                    _extract_outline_row_anchor(
                        getattr(location, 'name', None),
                    ),
                ),
            ),
        )

    if location_anchors:
        return '|'.join(location_anchors)

    path = getattr(test, 'path', None)
    display_name = (
        getattr(test, 'name', None)
        or getattr(test, 'title', None)
        or getattr(test, 'id', None)
        or ''
    )
    return ':'.join(
        (
            test.__class__.__name__,
            str(path or ''),
            str(display_name),
        ),
    )


def _extract_outline_row_anchor(name: str | None) -> str:
    if not name or '#' not in name:
        return ''

    row_index = name.rsplit('#', maxsplit=1)[-1].strip()
    return row_index if row_index.isdigit() else ''


def _describe_execution_predicate(
    test,
) -> ExecutionPredicateEvaluation:
    describe_execution_predicate = getattr(
        test,
        'describe_execution_predicate',
        None,
    )
    if callable(describe_execution_predicate):
        return describe_execution_predicate()

    return ExecutionPredicateEvaluation()
