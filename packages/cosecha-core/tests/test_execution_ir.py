from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.execution_ir import (
    EXECUTION_IR_SCHEMA_VERSION,
    ExecutionBootstrap,
    ExecutionRequest,
    PlanningIssue,
    TestExecutionNode,
    TestExecutionNodeSnapshot,
    analyze_execution_plan,
    build_execution_node_stable_id,
    build_test_path_label,
    validate_execution_plan,
)
from cosecha.core.items import TestItem
from cosecha.core.location import Location
from cosecha.core.resources import ResourceRequirement
from cosecha_internal.testkit import DummyReporter, ListCollector, build_config


if TYPE_CHECKING:
    from pathlib import Path


class DummyContext(BaseContext):
    async def cleanup(self) -> None:
        return None


class DummyEngine(Engine):
    async def generate_new_context(self, test: TestItem) -> BaseContext:
        del test
        return DummyContext()


class DummyTestItem(TestItem):
    async def run(self, context) -> None:
        del context

    def has_selection_label(self, name: str) -> bool:
        del name
        return False


def _build_engine(tmp_path: Path) -> DummyEngine:
    engine = DummyEngine(
        'dummy',
        collector=ListCollector(()),
        reporter=DummyReporter(),
    )
    engine.initialize(build_config(tmp_path), '')
    return engine


def _build_node(
    tmp_path: Path,
    *,
    node_id: str = 'dummy:first.feature:0',
    stable_id: str = 'dummy:first.feature:stable',
    test_path: str = 'first.feature',
    resource_requirements: tuple[ResourceRequirement, ...] = (),
) -> TestExecutionNode:
    engine = _build_engine(tmp_path)
    test = DummyTestItem(tmp_path / test_path)
    return TestExecutionNode(
        id=node_id,
        stable_id=stable_id,
        engine=engine,
        test=test,
        engine_name='dummy',
        test_name='<DummyTest>',
        test_path=test_path,
        resource_requirements=resource_requirements,
    )


def test_execution_node_snapshot_roundtrip() -> None:
    snapshot = TestExecutionNodeSnapshot(
        id='gherkin:payments.feature:0',
        stable_id='gherkin:payments.feature:5f2414c06dc0',
        engine_name='gherkin',
        test_name='<GherkinTest payments.feature:2>',
        test_path='payments.feature',
        required_step_texts=(('given', 'the user logs in'),),
        step_candidate_files=('payments/steps/auth_steps.py',),
        step_directories=('payments/steps',),
        resource_names=('session_db', 'browser'),
    )

    restored = TestExecutionNodeSnapshot.from_dict(snapshot.to_dict())

    assert restored == snapshot


def test_execution_request_roundtrip(tmp_path: Path) -> None:
    request = ExecutionRequest(
        cwd=str(tmp_path),
        root_path=str(tmp_path / 'tests'),
        config_snapshot=build_config(tmp_path / 'tests').snapshot(),
        node=TestExecutionNodeSnapshot(
            id='gherkin:payments.feature:0',
            stable_id='gherkin:payments.feature:5f2414c06dc0',
            engine_name='gherkin',
            test_name='<GherkinTest payments.feature:2>',
            test_path='payments.feature',
            required_step_texts=(('given', 'the user logs in'),),
            step_candidate_files=('payments/steps/auth_steps.py',),
            resource_names=('session_db',),
        ),
    )

    restored = ExecutionRequest.from_dict(request.to_dict())

    assert restored == request
    assert restored.schema_version == EXECUTION_IR_SCHEMA_VERSION


def test_execution_request_from_node_uses_snapshot(tmp_path: Path) -> None:
    node = _build_node(tmp_path)

    request = ExecutionRequest.from_node(
        tmp_path,
        tmp_path / 'tests',
        node,
    )

    assert request.cwd == str(tmp_path)
    assert request.root_path == str(tmp_path / 'tests')
    assert request.config_snapshot.root_path == str(tmp_path.resolve())
    assert request.node.id == 'dummy:first.feature:0'
    assert request.node.stable_id == 'dummy:first.feature:stable'


def test_execution_bootstrap_from_nodes_uses_engine_config_snapshot(
    tmp_path: Path,
) -> None:
    node = _build_node(tmp_path)

    bootstrap = ExecutionBootstrap.from_nodes((node,))

    assert bootstrap.config_snapshot.root_path == str(tmp_path.resolve())
    assert bootstrap.nodes[0].id == 'dummy:first.feature:0'
    assert bootstrap.schema_version == EXECUTION_IR_SCHEMA_VERSION


def test_execution_request_rejects_unknown_schema_version(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match='Unsupported execution IR schema'):
        ExecutionRequest.from_dict(
            {
                'cwd': str(tmp_path),
                'root_path': str(tmp_path / 'tests'),
                'config_snapshot': build_config(tmp_path / 'tests')
                .snapshot()
                .to_dict(),
                'node': TestExecutionNodeSnapshot(
                    id='gherkin:payments.feature:0',
                    stable_id='gherkin:payments.feature:5f2414c06dc0',
                    engine_name='gherkin',
                    test_name='<GherkinTest payments.feature:2>',
                    test_path='payments.feature',
                ).to_dict(),
                'schema_version': 999,
            },
        )


def test_build_test_path_label_handles_none_and_external_paths(
    tmp_path: Path,
) -> None:
    external_path = tmp_path.parent / 'external.feature'

    assert build_test_path_label(tmp_path, None) == ''
    assert build_test_path_label(tmp_path, external_path) == str(external_path)


def test_build_execution_node_stable_id_ignores_scenario_text_changes(
    tmp_path: Path,
) -> None:
    class ScenarioStub:
        def __init__(self, name: str) -> None:
            self.location = Location(
                tmp_path / 'draft.feature',
                12,
                name=name,
            )

    class DraftTest:
        path = tmp_path / 'draft.feature'
        feature = type(
            'FeatureStub',
            (),
            {'location': Location(tmp_path / 'draft.feature', 1)},
        )()

        def __init__(self, scenario_name: str) -> None:
            self.scenario = ScenarioStub(scenario_name)
            self.example = None

    first_id = build_execution_node_stable_id(
        tmp_path,
        'gherkin',
        DraftTest('Pay invoice'),
    )
    renamed_id = build_execution_node_stable_id(
        tmp_path,
        'gherkin',
        DraftTest('Pay changed invoice'),
    )

    assert first_id == renamed_id


def test_build_execution_node_stable_id_tracks_outline_row_index(
    tmp_path: Path,
) -> None:
    class ScenarioStub:
        def __init__(self, row_name: str) -> None:
            self.location = Location(
                tmp_path / 'outline.feature',
                12,
                name=row_name,
            )

    class OutlineTest:
        path = tmp_path / 'outline.feature'
        feature = type(
            'FeatureStub',
            (),
            {'location': Location(tmp_path / 'outline.feature', 1)},
        )()

        def __init__(self, row_name: str) -> None:
            self.scenario = ScenarioStub(row_name)
            self.example = None

    first_row = build_execution_node_stable_id(
        tmp_path,
        'gherkin',
        OutlineTest('Example #1'),
    )
    second_row = build_execution_node_stable_id(
        tmp_path,
        'gherkin',
        OutlineTest('Another label #2'),
    )

    assert first_row != second_row


def test_analyze_execution_plan_reports_duplicate_ids_and_resources(
    tmp_path: Path,
) -> None:
    duplicated_resource = ResourceRequirement(name='session_db', setup=object)
    first = _build_node(
        tmp_path,
        node_id='dummy:first.feature:0',
        stable_id='dummy:first.feature:stable-a',
        resource_requirements=(duplicated_resource, duplicated_resource),
    )
    second = _build_node(
        tmp_path,
        node_id='dummy:first.feature:0',
        stable_id='dummy:first.feature:stable-b',
    )

    analysis = analyze_execution_plan((first, second))

    assert analysis.executable is False
    assert analysis.issues == (
        PlanningIssue(
            code='duplicated_resource_requirements',
            message=(
                'Duplicated resource requirements for '
                'dummy:first.feature:0: session_db'
            ),
            node_id='dummy:first.feature:0',
            node_stable_id='dummy:first.feature:stable-a',
        ),
        PlanningIssue(
            code='duplicated_node_id',
            message='Duplicated execution node id: dummy:first.feature:0',
            node_id='dummy:first.feature:0',
            node_stable_id='dummy:first.feature:stable-b',
        ),
    )
    assert analysis.explanation.fallback_reasons == (
        'planning_issues_present',
    )


def test_validate_execution_plan_rejects_duplicate_ids_in_strict_mode(
    tmp_path: Path,
) -> None:
    first = _build_node(
        tmp_path,
        node_id='dummy:first.feature:0',
        stable_id='dummy:first.feature:stable-a',
    )
    second = _build_node(
        tmp_path,
        node_id='dummy:first.feature:0',
        stable_id='dummy:first.feature:stable-b',
    )

    with pytest.raises(ValueError, match='Duplicated execution node id'):
        validate_execution_plan((first, second))

    validated = validate_execution_plan((first, second), mode='relaxed')

    assert validated == (first, second)
