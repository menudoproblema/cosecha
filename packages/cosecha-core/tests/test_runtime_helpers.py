from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from cosecha.core.execution_ir import (
    TestExecutionNode,
    build_execution_node_id,
    build_execution_node_stable_id,
    build_test_path_label,
)
from cosecha.core.resources import ResourceRequirement
from cosecha.core.runtime import (
    _build_bootstrap_correlation_id,
    _build_node_bootstrap_key,
    _collect_run_scoped_requirements,
    _collect_worker_ephemeral_capabilities,
    _group_plan_nodes_for_workers,
    _resolve_group_slots,
    _run_requirements_are_compatible,
)
from cosecha.core.scheduler import (
    RoundRobinWorkerSelectionPolicy,
    SchedulingDecision,
    SchedulingPlan,
)


if TYPE_CHECKING:
    from collections.abc import Iterable


class _DummyTest:
    def __init__(
        self,
        path: Path,
        *,
        label: str | None = None,
        requirements: tuple[ResourceRequirement, ...] = (),
    ) -> None:
        self.path = path
        self.name = label or path.name
        self._requirements = requirements

    def __repr__(self) -> str:
        return self.name

    def get_resource_requirements(self) -> tuple[ResourceRequirement, ...]:
        return self._requirements


class _DummyEngine:
    def __init__(self, name: str) -> None:
        self.name = name

    def describe_capabilities(self) -> tuple[object, ...]:
        return ()


def _build_node(
    root_path: Path,
    *,
    engine_name: str,
    relative_path: str,
    index: int,
    label: str | None = None,
    source_content: str | None = None,
    requirements: tuple[ResourceRequirement, ...] = (),
) -> TestExecutionNode:
    test_path = root_path / relative_path
    test_path.parent.mkdir(parents=True, exist_ok=True)
    test_path.write_text('Feature: runtime helpers\n', encoding='utf-8')
    test = _DummyTest(test_path, label=label, requirements=requirements)
    engine = _DummyEngine(engine_name)
    test_path_label = build_test_path_label(root_path, test_path)
    return TestExecutionNode(
        id=build_execution_node_id(engine_name, test_path_label, index),
        stable_id=build_execution_node_stable_id(root_path, engine_name, test),
        engine=engine,  # type: ignore[arg-type]
        test=test,  # type: ignore[arg-type]
        engine_name=engine_name,
        test_name=repr(test),
        test_path=test_path_label,
        source_content=source_content,
        resource_requirements=requirements,
    )


def _run_requirement(
    name: str,
    *,
    scope: str = 'run',
    mode: str = 'live',
    depends_on: tuple[str, ...] = (),
    conflicts_with: tuple[str, ...] = (),
) -> ResourceRequirement:
    return ResourceRequirement(
        name=name,
        scope=scope,  # type: ignore[arg-type]
        mode=mode,  # type: ignore[arg-type]
        depends_on=depends_on,
        conflicts_with=conflicts_with,
        setup=lambda: object(),
    )


def test_bootstrap_correlation_and_grouping_helpers(tmp_path: Path) -> None:
    node_a = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/a.feature',
        index=0,
        label='scenario-a',
    )
    node_b = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/a.feature',
        index=1,
        label='scenario-b',
    )
    node_c = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/b.feature',
        index=0,
        source_content='Feature: inline',
    )

    empty_correlation_id = _build_bootstrap_correlation_id(())
    assert isinstance(empty_correlation_id, str)
    assert empty_correlation_id

    expected_digest = sha256(
        f'{node_a.stable_id}{node_b.stable_id}'.encode('utf-8'),
    ).hexdigest()
    assert _build_bootstrap_correlation_id((node_a, node_b)) == expected_digest

    grouped = _group_plan_nodes_for_workers((node_a, node_b, node_c))
    assert len(grouped[_build_node_bootstrap_key(node_a)]) == 2
    assert len(grouped[_build_node_bootstrap_key(node_c)]) == 1
    assert _build_node_bootstrap_key(node_c)[2] != ''


def test_resolve_group_slots_with_scheduling_plan_and_fallback(
    tmp_path: Path,
) -> None:
    node_a = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/a.feature',
        index=0,
    )
    node_b = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/b.feature',
        index=0,
    )
    grouped = _group_plan_nodes_for_workers((node_a, node_b))

    without_plan = _resolve_group_slots(
        grouped,
        scheduling_plan=None,
        worker_count=2,
        worker_selection_policy=RoundRobinWorkerSelectionPolicy(),
    )
    assert set(without_plan) == set(grouped)

    scheduling_plan = SchedulingPlan(
        worker_count=4,
        decisions=(
            SchedulingDecision(
                node_id=node_a.id,
                node_stable_id=node_a.stable_id,
                worker_slot=3,
                max_attempts=1,
            ),
        ),
    )
    with_plan = _resolve_group_slots(
        grouped,
        scheduling_plan=scheduling_plan,
        worker_count=2,
        worker_selection_policy=RoundRobinWorkerSelectionPolicy(),
    )
    assert with_plan[_build_node_bootstrap_key(node_a)] == 1
    assert set(with_plan) == set(grouped)


def test_resolve_group_slots_rejects_conflicting_assigned_slots(
    tmp_path: Path,
) -> None:
    node_a = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/a.feature',
        index=0,
        label='scenario-a',
    )
    node_b = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/a.feature',
        index=1,
        label='scenario-b',
    )
    grouped = _group_plan_nodes_for_workers((node_a, node_b))
    conflicting_plan = SchedulingPlan(
        worker_count=2,
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
                worker_slot=1,
                max_attempts=1,
            ),
        ),
    )

    with pytest.raises(ValueError, match='multiple workers'):
        _resolve_group_slots(
            grouped,
            scheduling_plan=conflicting_plan,
            worker_count=2,
            worker_selection_policy=RoundRobinWorkerSelectionPolicy(),
        )


def test_collect_worker_capabilities_and_run_requirements(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_db = _run_requirement('db', depends_on=('auth',))
    run_db_compatible = _run_requirement('db', depends_on=('auth',))
    run_db_incompatible = _run_requirement('db', mode='dry_run')
    run_cache = _run_requirement('cache')

    node_a = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/a.feature',
        index=0,
        requirements=(run_db, run_cache),
    )
    node_b = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/b.feature',
        index=0,
        requirements=(run_db_compatible,),
    )

    monkeypatch.setattr(
        'cosecha.core.runtime.component_id_from_component_type',
        lambda component_type: 'component::dummy',
    )
    monkeypatch.setattr(
        'cosecha.core.runtime.build_ephemeral_artifact_capability',
        lambda _descriptors, declared_component_id: f'cap::{declared_component_id}',
    )

    capabilities = _collect_worker_ephemeral_capabilities((node_a, node_b))
    assert capabilities == ('cap::component::dummy',)

    collected_requirements = _collect_run_scoped_requirements((node_a, node_b))
    assert tuple(requirement.name for requirement in collected_requirements) == (
        'cache',
        'db',
    )
    assert _run_requirements_are_compatible(run_db, run_db_compatible) is True
    assert _run_requirements_are_compatible(run_db, run_db_incompatible) is False

    node_conflict = _build_node(
        tmp_path,
        engine_name='dummy',
        relative_path='features/conflict.feature',
        index=0,
        requirements=(run_db_incompatible,),
    )
    with pytest.raises(ValueError, match='conflicting run-scoped'):
        _collect_run_scoped_requirements((node_a, node_conflict))
