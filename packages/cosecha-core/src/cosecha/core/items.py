from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

from cosecha.core.runtime_profiles import RuntimeRequirementSet


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path

    from cosecha.core.resources import ResourceRequirement
    from cosecha.core.types import ExcInfo


class TestResultStatus(Enum):
    # Evita que Pytest intente recolectar esta clase como un test.
    __test__ = False
    PENDING = 'pending'
    FAILED = 'failed'
    PASSED = 'passed'
    SKIPPED = 'skipped'
    ERROR = 'error'


type FailureKind = Literal[
    'test',
    'runtime',
    'infrastructure',
    'hook',
    'bootstrap',
    'collection',
]


def normalize_failure_kind(value: object) -> FailureKind | None:
    if value in {
        'test',
        'runtime',
        'infrastructure',
        'hook',
        'bootstrap',
        'collection',
    }:
        return value
    return None


def resolve_failure_kind(
    error: BaseException | None,
    *,
    default: FailureKind | None = None,
) -> FailureKind | None:
    if error is not None:
        explicit_failure_kind = normalize_failure_kind(
            getattr(error, 'failure_kind', None),
        )
        if explicit_failure_kind is not None:
            return explicit_failure_kind
        if isinstance(error, AssertionError):
            return 'test'
    return default


type ExecutionPredicateState = Literal[
    'unconstrained',
    'runtime_only',
    'statically_skipped',
    'not_executable',
]


@dataclass(slots=True, frozen=True)
class ExecutionPredicateEvaluation:
    state: ExecutionPredicateState = 'unconstrained'
    reason: str | None = None


@dataclass(slots=True, frozen=True)
class TestPreflightDecision:
    __test__ = False
    status: TestResultStatus
    message: str | None = None
    failure_kind: FailureKind | None = None
    error_code: str | None = None


class TestItem(ABC):
    # Evita que Pytest intente recolectar esta clase como un test.
    __test__ = False
    __slots__ = (
        'duration',
        'error_code',
        'exc_info',
        'failure_kind',
        'message',
        'path',
        'status',
    )

    def __init__(self, path: Path | None) -> None:
        self.path = path
        self.status = TestResultStatus.PENDING
        self.message: str | None = None
        self.failure_kind: FailureKind | None = None
        self.error_code: str | None = None
        self.exc_info: ExcInfo | None = None
        self.duration: float = 0.0

    @property
    def has_failed(self) -> bool:
        return self.status in (TestResultStatus.FAILED, TestResultStatus.ERROR)

    @abstractmethod
    async def run(self, context: Any) -> None: ...

    @abstractmethod
    def has_selection_label(self, name: str) -> bool:
        """Check if the test has a given selection label."""

    def get_resource_requirements(self) -> tuple[ResourceRequirement, ...]:
        return ()

    def get_required_step_texts(self) -> tuple[tuple[str, str], ...]:
        return ()

    def get_runtime_requirement_set(self) -> RuntimeRequirementSet:
        return RuntimeRequirementSet()

    def describe_execution_predicate(self) -> ExecutionPredicateEvaluation:
        return ExecutionPredicateEvaluation()
