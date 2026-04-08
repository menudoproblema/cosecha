from __future__ import annotations

from types import SimpleNamespace

from cosecha.core.resources import ResourceRequirement
from cosecha.core.scheduler import (
    DefaultResourceAllocationPolicy,
    ExecutionScheduler,
    NodeExecutionTimeoutError,
    RetryPolicy,
    RuntimeAssignmentState,
    SchedulingDecision,
    SchedulerInfrastructureError,
    _build_node_group_key,
    _build_resource_allocation_signature,
)


def test_retry_policy_and_timeout_error_paths() -> None:
    policy = RetryPolicy(max_attempts=2, retry_recoverable_infrastructure_errors=False)
    recoverable_error = SchedulerInfrastructureError(
        'recoverable',
        code='recoverable',
        recoverable=True,
    )

    assert policy.should_retry(1, recoverable_error) is False
    assert policy.backoff_for_attempt(0) == 0.0
    assert RetryPolicy(backoff_seconds=()).backoff_for_attempt(1) == 0.0

    timeout_error = NodeExecutionTimeoutError('node-1', 'stable-1', 2.0)
    assert timeout_error.code == 'node_execution_timeout'
    assert timeout_error.node_id == 'node-1'
    assert timeout_error.node_stable_id == 'stable-1'
    assert timeout_error.timeout_seconds == 2.0


def test_runtime_assignment_state_requeue_and_active_guard() -> None:
    decision = SchedulingDecision(
        node_id='a',
        node_stable_id='stable-a',
        worker_slot=0,
        max_attempts=2,
    )
    state = RuntimeAssignmentState((decision,), pinned_node_ids=('a',))

    first = state.claim_next(0)
    assert first is not None
    assert state.claim_next(0) is None

    state.requeue(first)
    snapshot = state.snapshot()
    assert snapshot.requeued_node_ids == ('a',)


def test_execution_scheduler_properties_and_resource_signature_filtering() -> None:
    scheduler = ExecutionScheduler()
    assert scheduler.retry_policy.max_attempts == 1
    assert scheduler.timeout_policy.node_timeout_seconds is None
    assert scheduler.worker_selection_policy is not None
    assert scheduler.resource_allocation_policy is not None

    node = SimpleNamespace(stable_id='stable-x')
    assert _build_node_group_key(node) == 'stable-x'

    signature = _build_resource_allocation_signature(
        (
            ResourceRequirement(name='run-db', scope='run', mode='live', setup=object),
            ResourceRequirement(name='test-db', scope='test', mode='live', setup=object),
        ),
    )
    assert signature == (('run', 'run-db', 'live'),)

    default_policy = DefaultResourceAllocationPolicy()
    no_resource_node = SimpleNamespace(
        stable_id='stable-no-resource',
        engine_name='dummy',
        resource_requirements=(),
    )
    assert default_policy.build_group_key(no_resource_node) == 'stable-no-resource'
