from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol, runtime_checkable

from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


type EngineDependencyKind = Literal['knowledge', 'planning', 'execution']
type EngineDependencyProjection = Literal[
    'diagnostic_only',
    'degrade_to_explain',
    'block_execution',
]


@dataclass(slots=True, frozen=True)
class EngineDependencyRule:
    source_engine_name: str
    target_engine_name: str
    dependency_kind: EngineDependencyKind
    projection_policy: EngineDependencyProjection
    summary: str = ''
    required_capabilities: tuple[str, ...] = field(
        default_factory=tuple,
    )
    operation_types: tuple[str, ...] = field(default_factory=tuple)
    shared_trace_required: bool = True

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> EngineDependencyRule:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ProjectedEngineDependencyIssue:
    source_engine_name: str
    target_engine_name: str
    dependency_kind: EngineDependencyKind
    projection_policy: EngineDependencyProjection
    source_node_stable_id: str
    source_test_name: str
    source_test_path: str
    source_status: str
    severity: Literal['warning', 'error']
    message: str
    plan_id: str | None = None
    trace_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> ProjectedEngineDependencyIssue:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class EngineDependencyQuery:
    source_engine_name: str | None = None
    target_engine_name: str | None = None
    dependency_kind: EngineDependencyKind | None = None
    projection_policy: EngineDependencyProjection | None = None
    plan_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> EngineDependencyQuery:
        return from_builtins_dict(data, target_type=cls)

    def matches(self, rule: EngineDependencyRule) -> bool:
        if (
            self.source_engine_name is not None
            and rule.source_engine_name != self.source_engine_name
        ):
            return False

        if (
            self.target_engine_name is not None
            and rule.target_engine_name != self.target_engine_name
        ):
            return False

        if (
            self.dependency_kind is not None
            and rule.dependency_kind != self.dependency_kind
        ):
            return False

        return not (
            self.projection_policy is not None
            and rule.projection_policy != self.projection_policy
        )


@runtime_checkable
class EngineDependencyDescribingComponent(Protocol):
    def describe_engine_dependencies(
        self,
    ) -> tuple[EngineDependencyRule, ...]: ...


def build_engine_dependency_rule_key(rule: EngineDependencyRule) -> str:
    return ':'.join(
        (
            rule.source_engine_name,
            rule.target_engine_name,
            rule.dependency_kind,
            rule.projection_policy,
            ','.join(rule.operation_types),
        ),
    )
