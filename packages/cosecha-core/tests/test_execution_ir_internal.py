from __future__ import annotations

import pytest

from pathlib import Path
from types import SimpleNamespace

from cosecha.core.execution_ir import (
    EXECUTION_IR_SCHEMA_VERSION,
    ExecutionBootstrap,
    NodePlanningSemantics,
    PlanExplanation,
    PlanningIssue,
    TestExecutionNodeSnapshot,
    _build_resource_affinity_signature,
    _build_resource_cost,
    _collect_node_resource_issues,
    _describe_execution_predicate,
    cast_schema_version,
    filter_execution_nodes,
    reorder_execution_plan_by_resource_affinity,
)
from cosecha.core.items import ExecutionPredicateEvaluation, TestItem
from cosecha.core.resources import ResourceRequirement, ResourceTiming
from cosecha_internal.testkit import build_config


class _TaggedTestItem(TestItem):
    def __init__(self, path: Path, tags: tuple[str, ...] = ()) -> None:
        super().__init__(path)
        self._tags = set(tags)

    async def run(self, context) -> None:
        del context

    def has_selection_label(self, name: str) -> bool:
        return name in self._tags


class _PredicateTestItem(_TaggedTestItem):
    def describe_execution_predicate(self) -> ExecutionPredicateEvaluation:
        return ExecutionPredicateEvaluation(state='runtime_only', reason='dynamic')


def _node_stub(
    tmp_path: Path,
    *,
    node_id: str,
    stable_id: str,
    tags: tuple[str, ...] = (),
    resource_requirements: tuple[ResourceRequirement, ...] = (),
):
    test = _TaggedTestItem(tmp_path / f'{node_id}.feature', tags)
    return SimpleNamespace(
        id=node_id,
        stable_id=stable_id,
        engine=SimpleNamespace(config=build_config(tmp_path)),
        test=test,
        engine_name='dummy',
        test_name=f'<{node_id}>',
        test_path=f'{node_id}.feature',
        source_content=None,
        required_step_texts=(),
        step_candidate_files=(),
        step_directories=(),
        resource_requirements=resource_requirements,
    )


def test_execution_ir_roundtrips_and_executable_properties() -> None:
    issue = PlanningIssue(code='code', message='message')
    assert PlanningIssue.from_dict(issue.to_dict()) == issue

    semantics = NodePlanningSemantics(
        node_id='node',
        node_stable_id='stable',
        engine_name='engine',
        issues=(PlanningIssue(code='warn', message='ok', severity='warning'),),
    )
    assert semantics.executable is True
    assert NodePlanningSemantics.from_dict(semantics.to_dict()) == semantics

    explanation = PlanExplanation(
        mode='strict',
        executable=True,
        issues=(PlanningIssue(code='warn', message='ok', severity='warning'),),
        node_semantics=(semantics,),
    )
    assert PlanExplanation.from_dict(explanation.to_dict()) == explanation


def test_execution_bootstrap_helpers_and_schema_casting(tmp_path: Path) -> None:
    node_snapshot = TestExecutionNodeSnapshot(
        id='a',
        stable_id='stable-a',
        engine_name='dummy',
        test_name='<a>',
        test_path='a.feature',
    )
    node = SimpleNamespace(
        engine=SimpleNamespace(config=build_config(tmp_path)),
        snapshot=node_snapshot,
    )
    bootstrap_from_nodes = ExecutionBootstrap.from_nodes((node,))
    assert bootstrap_from_nodes.nodes[0].id == 'a'
    assert node_snapshot.id == 'a'

    bootstrap = ExecutionBootstrap(
        config_snapshot=build_config(tmp_path).snapshot(),
        nodes=(node_snapshot,),
    )
    assert bootstrap.to_dict()['schema_version'] == EXECUTION_IR_SCHEMA_VERSION

    with pytest.raises(ValueError, match='at least one node'):
        ExecutionBootstrap.from_nodes(())

    assert cast_schema_version(None) == EXECUTION_IR_SCHEMA_VERSION


def test_filter_reorder_and_resource_issue_helpers(tmp_path: Path) -> None:
    node_a = _node_stub(tmp_path, node_id='a', stable_id='stable-a', tags=('run',))
    node_b = _node_stub(tmp_path, node_id='b', stable_id='stable-b', tags=('skip',))
    node_c = _node_stub(tmp_path, node_id='c', stable_id='stable-c', tags=())

    filtered = filter_execution_nodes(
        (node_a, node_b, node_c),
        skip_labels=['skip'],
        execute_labels=['run'],
        test_limit=1,
    )
    assert tuple(node.id for node in filtered) == ('a',)

    run_req = ResourceRequirement(name='run-db', scope='run', setup=object)
    worker_req = ResourceRequirement(name='worker-db', scope='worker', setup=object)
    ordered = reorder_execution_plan_by_resource_affinity(
        (
            _node_stub(
                tmp_path,
                node_id='x',
                stable_id='stable-x',
                resource_requirements=(worker_req,),
            ),
            _node_stub(
                tmp_path,
                node_id='y',
                stable_id='stable-y',
                resource_requirements=(run_req,),
            ),
        ),
        resource_timings=(
            ResourceTiming(name='run-db', scope='run', acquire_duration=5.0),
            ResourceTiming(name='worker-db', scope='worker', acquire_duration=1.0),
        ),
    )
    assert ordered[0].id == 'y'

    affinity_signature = _build_resource_affinity_signature(
        _node_stub(
            tmp_path,
            node_id='z',
            stable_id='stable-z',
            resource_requirements=(run_req, worker_req),
        ),
        scope='run',
    )
    assert affinity_signature == ('run-db',)

    affinity_cost = _build_resource_cost(
        _node_stub(
            tmp_path,
            node_id='q',
            stable_id='stable-q',
            resource_requirements=(run_req,),
        ),
        scope='run',
        resource_costs={('run-db', 'run'): 2.0},
    )
    assert affinity_cost == 2.0

    invalid_requirement = ResourceRequirement(
        name='requires-missing',
        setup=object,
        depends_on=('missing',),
    )
    issues = _collect_node_resource_issues(
        _node_stub(
            tmp_path,
            node_id='invalid',
            stable_id='stable-invalid',
            resource_requirements=(invalid_requirement,),
        ),
    )
    assert issues
    assert issues[0].code == 'invalid_resource_requirements'


def test_describe_execution_predicate_fallback_and_custom() -> None:
    default_item = _TaggedTestItem(Path('tests/default.feature'))
    custom_item = _PredicateTestItem(Path('tests/custom.feature'))

    assert _describe_execution_predicate(default_item) == ExecutionPredicateEvaluation()
    assert _describe_execution_predicate(custom_item).state == 'runtime_only'


def test_filter_nodes_skips_and_requires_execute_labels(tmp_path: Path) -> None:
    selected = _node_stub(
        tmp_path,
        node_id='selected',
        stable_id='stable-selected',
        tags=('run',),
    )
    skipped = _node_stub(
        tmp_path,
        node_id='skipped',
        stable_id='stable-skipped',
        tags=('skip',),
    )
    missing_execute = _node_stub(
        tmp_path,
        node_id='missing',
        stable_id='stable-missing',
        tags=(),
    )
    selected_nodes = filter_execution_nodes(
        (skipped, missing_execute, selected),
        skip_labels=['skip'],
        execute_labels=['run'],
        test_limit=10,
    )
    assert tuple(node.id for node in selected_nodes) == ('selected',)


def test_describe_execution_predicate_falls_back_without_method() -> None:
    assert _describe_execution_predicate(SimpleNamespace()) == (
        ExecutionPredicateEvaluation()
    )
