from __future__ import annotations

from typing import TYPE_CHECKING

from cosecha.core.items import TestItem
from cosecha.core.resources import ResourceRequirement
from cosecha.core.scheduler import (
    ExecutionScheduler,
    RetryPolicy,
    RoundRobinWorkerSelectionPolicy,
    RuntimeAssignmentState,
    SchedulingDecision,
    TimeoutPolicy,
    assign_group_slots,
)


if TYPE_CHECKING:
    from pathlib import Path


WORKER_COUNT = 2
FIRST_BACKOFF = 0.1
SECOND_BACKOFF = 0.2


class TaggedTestItem(TestItem):
    def __init__(self, path: Path, *tags: str) -> None:
        super().__init__(path)
        self._tags = set(tags)

    async def run(self, context) -> None:
        del context

    def has_selection_label(self, name: str) -> bool:
        return name in self._tags


class NodeStub:
    def __init__(
        self,
        node_id: str,
        stable_id: str,
        test: TestItem,
        *,
        engine_name: str = 'dummy',
        resource_requirements: tuple[ResourceRequirement, ...] = (),
    ) -> None:
        self.id = node_id
        self.stable_id = stable_id
        self.test = test
        self.engine_name = engine_name
        self.resource_requirements = resource_requirements


def test_execution_scheduler_builds_round_robin_worker_plan(
    tmp_path: Path,
) -> None:
    nodes = tuple(
        NodeStub(
            node_id,
            stable_id,
            TaggedTestItem(tmp_path / f'{node_id}.feature'),
        )
        for node_id, stable_id in (
            ('a', 'stable-a'),
            ('b', 'stable-b'),
            ('c', 'stable-c'),
        )
    )
    scheduler = ExecutionScheduler(
        worker_selection_policy=RoundRobinWorkerSelectionPolicy(),
    )

    plan = scheduler.build_plan(nodes, worker_count=WORKER_COUNT)

    assert plan.worker_count == WORKER_COUNT
    assert plan.decisions == (
        SchedulingDecision(
            node_id='a',
            node_stable_id='stable-a',
            worker_slot=0,
            max_attempts=1,
            timeout_seconds=None,
        ),
        SchedulingDecision(
            node_id='b',
            node_stable_id='stable-b',
            worker_slot=1,
            max_attempts=1,
            timeout_seconds=None,
        ),
        SchedulingDecision(
            node_id='c',
            node_stable_id='stable-c',
            worker_slot=0,
            max_attempts=1,
            timeout_seconds=None,
        ),
    )


def test_execution_scheduler_groups_shared_worker_resources(
    tmp_path: Path,
) -> None:
    nodes = (
        NodeStub(
            'a',
            'stable-a',
            TaggedTestItem(tmp_path / 'a.feature'),
            resource_requirements=(
                ResourceRequirement(
                    name='session_db',
                    scope='worker',
                    setup=object,
                ),
            ),
        ),
        NodeStub(
            'b',
            'stable-b',
            TaggedTestItem(tmp_path / 'b.feature'),
            resource_requirements=(
                ResourceRequirement(
                    name='session_db',
                    scope='worker',
                    setup=object,
                ),
            ),
        ),
        NodeStub(
            'c',
            'stable-c',
            TaggedTestItem(tmp_path / 'c.feature'),
            resource_requirements=(
                ResourceRequirement(
                    name='browser',
                    scope='worker',
                    setup=object,
                ),
            ),
        ),
    )
    scheduler = ExecutionScheduler(
        worker_selection_policy=RoundRobinWorkerSelectionPolicy(),
    )

    plan = scheduler.build_plan(nodes, worker_count=WORKER_COUNT)

    assert plan.decision_for_node('a', 'stable-a') is not None
    assert plan.decision_for_node('b', 'stable-b') is not None
    assert plan.decision_for_node('c', 'stable-c') is not None
    assert (
        plan.decision_for_node('a', 'stable-a').worker_slot
        == plan.decision_for_node('b', 'stable-b').worker_slot
    )
    assert (
        plan.decision_for_node('a', 'stable-a').worker_slot
        != plan.decision_for_node('c', 'stable-c').worker_slot
    )


def test_runtime_assignment_state_keeps_affinity_and_shared_stealing() -> None:
    state = RuntimeAssignmentState(
        (
            SchedulingDecision(
                node_id='a',
                node_stable_id='stable-a',
                worker_slot=0,
                max_attempts=1,
            ),
            SchedulingDecision(
                node_id='b',
                node_stable_id='stable-b',
                worker_slot=1,
                max_attempts=1,
            ),
            SchedulingDecision(
                node_id='c',
                node_stable_id='stable-c',
                worker_slot=0,
                max_attempts=1,
            ),
        ),
        pinned_node_ids=('a', 'b'),
    )

    first = state.claim_next(0)
    second = state.claim_next(1)
    state.complete(first)
    state.complete(second)
    third = state.claim_next(1)

    assert first.node_id == 'a'
    assert second.node_id == 'b'
    assert third.node_id == 'c'
    assert state.snapshot().completed_node_ids == ('a', 'b')


def test_scheduler_retry_backoff_and_group_slot_assignment() -> None:
    retry_policy = RetryPolicy(max_attempts=3, backoff_seconds=(0.1, 0.2))
    timeout_policy = TimeoutPolicy(node_timeout_seconds=5.0)
    scheduler = ExecutionScheduler(
        retry_policy=retry_policy,
        timeout_policy=timeout_policy,
    )

    class RecoverableError(RuntimeError):
        recoverable = True

    assert scheduler.should_retry(1, RecoverableError('boom'))
    assert not scheduler.should_retry(3, RecoverableError('boom'))
    assert scheduler.backoff_for_attempt(1) == FIRST_BACKOFF
    assert scheduler.backoff_for_attempt(3) == SECOND_BACKOFF
    assert assign_group_slots(('db', 'browser', 'cache'), 2) == {
        'db': 0,
        'browser': 1,
        'cache': 0,
    }
