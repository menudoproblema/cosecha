from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Hashable, Iterable

    from cosecha.core.execution_ir import TestExecutionNode
    from cosecha.core.resources import ResourceRequirement

from cosecha.core.resources import normalize_resource_scope


@dataclass(slots=True, frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    retry_recoverable_infrastructure_errors: bool = True
    backoff_seconds: tuple[float, ...] = field(default_factory=tuple)

    def should_retry(self, attempt: int, error: Exception) -> bool:
        if attempt >= self.max_attempts:
            return False

        if not self.retry_recoverable_infrastructure_errors:
            return False

        return bool(getattr(error, 'recoverable', False))

    def backoff_for_attempt(self, attempt: int) -> float:
        if attempt <= 0:
            return 0.0

        if not self.backoff_seconds:
            return 0.0

        if attempt > len(self.backoff_seconds):
            return self.backoff_seconds[-1]

        return self.backoff_seconds[attempt - 1]


@dataclass(slots=True, frozen=True)
class TimeoutPolicy:
    node_timeout_seconds: float | None = None


class SchedulerInfrastructureError(RuntimeError):
    __slots__ = ('code', 'fatal', 'recoverable')

    def __init__(
        self,
        message: str,
        *,
        code: str,
        recoverable: bool = True,
        fatal: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.recoverable = recoverable
        self.fatal = fatal


class NodeExecutionTimeoutError(SchedulerInfrastructureError):
    __slots__ = ('node_id', 'node_stable_id', 'timeout_seconds')

    def __init__(
        self,
        node_id: str,
        node_stable_id: str,
        timeout_seconds: float,
    ) -> None:
        super().__init__(
            (
                'Node execution exceeded the configured timeout '
                f'({timeout_seconds:.3f}s): {node_id}'
            ),
            code='node_execution_timeout',
        )
        self.node_id = node_id
        self.node_stable_id = node_stable_id
        self.timeout_seconds = timeout_seconds


@dataclass(slots=True, frozen=True)
class SchedulingDecision:
    node_id: str
    node_stable_id: str
    worker_slot: int
    max_attempts: int
    timeout_seconds: float | None = None


@dataclass(slots=True, frozen=True)
class SchedulingPlan:
    worker_count: int
    decisions: tuple[SchedulingDecision, ...]

    def decision_for_node(
        self,
        node_id: str,
        node_stable_id: str,
    ) -> SchedulingDecision | None:
        for decision in self.decisions:
            if (
                decision.node_id == node_id
                or decision.node_stable_id == node_stable_id
            ):
                return decision

        return None


@dataclass(slots=True, frozen=True)
class RuntimeAssignmentStateSnapshot:
    pending_by_worker: tuple[tuple[int, int], ...]
    shared_pending: int
    active_by_worker: tuple[tuple[int, str], ...]
    completed_node_ids: tuple[str, ...]
    requeued_node_ids: tuple[str, ...]


class RuntimeAssignmentState:
    __slots__ = (
        '_active_by_worker',
        '_completed_node_ids',
        '_pending_by_worker',
        '_pinned_node_ids',
        '_requeued_node_ids',
        '_shared_pending',
    )

    def __init__(
        self,
        decisions: tuple[SchedulingDecision, ...],
        *,
        pinned_node_ids: tuple[str, ...] = (),
    ) -> None:
        self._pinned_node_ids = set(pinned_node_ids)
        self._pending_by_worker: dict[int, deque[SchedulingDecision]] = {}
        self._shared_pending: deque[SchedulingDecision] = deque()
        self._active_by_worker: dict[int, SchedulingDecision] = {}
        self._completed_node_ids: set[str] = set()
        self._requeued_node_ids: set[str] = set()
        for decision in decisions:
            self.enqueue(decision)

    def enqueue(
        self,
        decision: SchedulingDecision,
    ) -> None:
        if decision.node_id in self._pinned_node_ids:
            queue = self._pending_by_worker.setdefault(
                decision.worker_slot,
                deque(),
            )
            queue.append(decision)
            return

        self._shared_pending.append(decision)

    def claim_next(
        self,
        worker_slot: int,
    ) -> SchedulingDecision | None:
        if worker_slot in self._active_by_worker:
            return None

        preferred_queue = self._pending_by_worker.get(worker_slot)
        if preferred_queue:
            decision = preferred_queue.popleft()
        elif self._shared_pending:
            decision = self._shared_pending.popleft()
        else:
            return None

        active_decision = SchedulingDecision(
            node_id=decision.node_id,
            node_stable_id=decision.node_stable_id,
            worker_slot=worker_slot,
            max_attempts=decision.max_attempts,
            timeout_seconds=decision.timeout_seconds,
        )
        self._active_by_worker[worker_slot] = active_decision
        return active_decision

    def complete(
        self,
        decision: SchedulingDecision,
    ) -> None:
        self._active_by_worker.pop(decision.worker_slot, None)
        self._completed_node_ids.add(decision.node_id)

    def requeue(
        self,
        decision: SchedulingDecision,
    ) -> None:
        self._active_by_worker.pop(decision.worker_slot, None)
        self._requeued_node_ids.add(decision.node_id)
        self.enqueue(
            SchedulingDecision(
                node_id=decision.node_id,
                node_stable_id=decision.node_stable_id,
                worker_slot=decision.worker_slot,
                max_attempts=decision.max_attempts,
                timeout_seconds=decision.timeout_seconds,
            ),
        )

    def has_pending(self) -> bool:
        return bool(self._shared_pending) or any(
            self._pending_by_worker.values(),
        )

    def is_complete(self) -> bool:
        return not self.has_pending() and not self._active_by_worker

    def snapshot(self) -> RuntimeAssignmentStateSnapshot:
        return RuntimeAssignmentStateSnapshot(
            pending_by_worker=tuple(
                sorted(
                    (
                        worker_slot,
                        len(queue),
                    )
                    for worker_slot, queue in self._pending_by_worker.items()
                    if queue
                ),
            ),
            shared_pending=len(self._shared_pending),
            active_by_worker=tuple(
                sorted(
                    (
                        worker_slot,
                        decision.node_id,
                    )
                    for worker_slot, decision in self._active_by_worker.items()
                ),
            ),
            completed_node_ids=tuple(sorted(self._completed_node_ids)),
            requeued_node_ids=tuple(sorted(self._requeued_node_ids)),
        )


class WorkerSelectionPolicy(ABC):
    @abstractmethod
    def assign_group_slots(
        self,
        group_keys: tuple[Hashable, ...],
        worker_count: int,
    ) -> dict[Hashable, int]: ...


class RoundRobinWorkerSelectionPolicy(WorkerSelectionPolicy):
    def assign_group_slots(
        self,
        group_keys: tuple[Hashable, ...],
        worker_count: int,
    ) -> dict[Hashable, int]:
        effective_worker_count = max(1, worker_count)
        return {
            group_key: index % effective_worker_count
            for index, group_key in enumerate(group_keys)
        }


class ResourceAllocationPolicy(ABC):
    @abstractmethod
    def build_group_key(self, node: TestExecutionNode) -> Hashable: ...


class DefaultResourceAllocationPolicy(ResourceAllocationPolicy):
    def build_group_key(self, node: TestExecutionNode) -> Hashable:
        resource_signature = _build_resource_allocation_signature(
            node.resource_requirements,
        )
        if not resource_signature:
            return node.stable_id

        return (
            node.engine_name,
            resource_signature,
        )


class ExecutionScheduler:
    __slots__ = (
        '_resource_allocation_policy',
        '_retry_policy',
        '_timeout_policy',
        '_worker_selection_policy',
    )

    def __init__(
        self,
        *,
        resource_allocation_policy: ResourceAllocationPolicy | None = None,
        retry_policy: RetryPolicy | None = None,
        timeout_policy: TimeoutPolicy | None = None,
        worker_selection_policy: WorkerSelectionPolicy | None = None,
    ) -> None:
        self._resource_allocation_policy = (
            resource_allocation_policy or DefaultResourceAllocationPolicy()
        )
        self._retry_policy = retry_policy or RetryPolicy()
        self._timeout_policy = timeout_policy or TimeoutPolicy()
        self._worker_selection_policy = (
            worker_selection_policy or RoundRobinWorkerSelectionPolicy()
        )

    @property
    def retry_policy(self) -> RetryPolicy:
        return self._retry_policy

    @property
    def timeout_policy(self) -> TimeoutPolicy:
        return self._timeout_policy

    @property
    def worker_selection_policy(self) -> WorkerSelectionPolicy:
        return self._worker_selection_policy

    @property
    def resource_allocation_policy(self) -> ResourceAllocationPolicy:
        return self._resource_allocation_policy

    def build_plan(
        self,
        nodes: tuple[TestExecutionNode, ...],
        *,
        worker_count: int,
        group_key_builder: Callable[[TestExecutionNode], str] | None = None,
    ) -> SchedulingPlan:
        effective_worker_count = max(1, worker_count)
        effective_group_key_builder = (
            group_key_builder
            or self._resource_allocation_policy.build_group_key
        )
        group_keys = tuple(
            dict.fromkeys(
                [effective_group_key_builder(node) for node in nodes],
            ),
        )
        group_slots = self._worker_selection_policy.assign_group_slots(
            group_keys,
            effective_worker_count,
        )
        decisions = tuple(
            SchedulingDecision(
                node_id=node.id,
                node_stable_id=node.stable_id,
                worker_slot=group_slots[effective_group_key_builder(node)],
                max_attempts=self._retry_policy.max_attempts,
                timeout_seconds=self._timeout_policy.node_timeout_seconds,
            )
            for node in nodes
        )
        return SchedulingPlan(
            worker_count=effective_worker_count,
            decisions=decisions,
        )

    def should_retry(self, attempt: int, error: Exception) -> bool:
        return self._retry_policy.should_retry(attempt, error)

    def backoff_for_attempt(self, attempt: int) -> float:
        return self._retry_policy.backoff_for_attempt(attempt)


def assign_group_slots(
    group_keys: Iterable[Hashable],
    worker_count: int,
    *,
    worker_selection_policy: WorkerSelectionPolicy | None = None,
) -> dict[Hashable, int]:
    effective_policy = (
        worker_selection_policy or RoundRobinWorkerSelectionPolicy()
    )
    return effective_policy.assign_group_slots(
        tuple(group_keys),
        max(1, worker_count),
    )


def _build_node_group_key(node: TestExecutionNode) -> str:
    return node.stable_id


def _build_resource_allocation_signature(
    requirements: tuple[ResourceRequirement, ...],
) -> tuple[tuple[str, str, str], ...]:
    signature_items: list[tuple[str, str, str]] = []
    for requirement in requirements:
        normalized_scope = normalize_resource_scope(requirement.scope)
        if normalized_scope not in {'run', 'worker'}:
            continue

        signature_items.append(
            (
                normalized_scope,
                requirement.name,
                requirement.mode,
            ),
        )

    return tuple(sorted(signature_items))
