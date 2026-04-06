from __future__ import annotations

import asyncio

from pathlib import Path
from typing import TYPE_CHECKING

from cosecha.core.items import TestResultStatus
from cosecha.core.location import Location
from cosecha.engine.gherkin.items import GherkinTestItem, StepResult
from cosecha.engine.gherkin.models import Feature, Scenario, Step, Tag
from cosecha.engine.gherkin.steps.definition import StepDefinition, StepText
from cosecha.engine.gherkin.steps.registry import StepRegistry


if TYPE_CHECKING:
    from cosecha.engine.gherkin.context import Context


FEATURE_PATH = 'tests/payment.feature'
FEATURE_FILE = Path(FEATURE_PATH)


def _create_step(step_id: str, text: str) -> Step:
    return Step(
        id=step_id,
        location=Location(FEATURE_PATH, 1, 1),
        keyword='Given ',
        keyword_type='Context',
        step_type='given',
        text=text,
    )


def _create_scenario(*steps: Step) -> Scenario:
    return Scenario(
        id='scenario-1',
        location=Location(FEATURE_PATH, 1, 1),
        keyword='Scenario',
        name='payment scenario',
        description='',
        steps=steps,
    )


def _create_feature(scenario: Scenario) -> Feature:
    return Feature(
        location=Location(FEATURE_PATH, 1, 1),
        language='en',
        keyword='Feature',
        name='payments',
        description='',
        scenarios=(scenario,),
    )


class DummyContext:
    def __init__(self, step_registry: StepRegistry) -> None:
        self.step_registry = step_registry
        self.executed_steps: list[str] = []
        self.table = None

    def set_step(
        self,
        feature: Feature,
        scenario: Scenario,
        step: Step,
    ) -> None:
        del feature, scenario
        self.executed_steps.append(step.text)


async def _failing_step(_context: Context) -> None:
    raise AssertionError


async def _passing_step(_context: Context) -> None:
    return None


def test_run_blocks_following_steps_after_first_failure() -> None:
    first_step = _create_step('step-1', 'I pay 10 EUR')
    second_step = _create_step('step-2', 'I confirm the payment')
    scenario = _create_scenario(first_step, second_step)
    feature = _create_feature(scenario)

    registry = StepRegistry()
    registry.add_step_definition(
        StepDefinition(
            'given',
            [StepText('I pay 10 EUR')],
            _failing_step,
        ),
    )
    registry.add_step_definition(
        StepDefinition(
            'given',
            [StepText('I confirm the payment')],
            _passing_step,
        ),
    )

    test_item = GherkinTestItem(feature, scenario, None, FEATURE_FILE)
    context = DummyContext(registry)

    asyncio.run(test_item.run(context))

    assert test_item.status == TestResultStatus.FAILED
    assert context.executed_steps == ['I pay 10 EUR']
    assert [result.status for result in test_item.step_result_list] == [
        TestResultStatus.FAILED,
        TestResultStatus.PENDING,
    ]


def test_has_selection_label_checks_feature_and_scenario_tags() -> None:
    feature_tag = Tag(
        id='tag-1',
        location=Location(FEATURE_PATH, 1, 1),
        name='billing:invoice',
    )
    scenario_tag = Tag(
        id='tag-2',
        location=Location(FEATURE_PATH, 2, 1),
        name='critical',
    )
    scenario = Scenario(
        id='scenario-1',
        location=Location(FEATURE_PATH, 1, 1),
        keyword='Scenario',
        name='payment scenario',
        description='',
        tags=(scenario_tag,),
    )
    feature = Feature(
        location=Location(FEATURE_PATH, 1, 1),
        language='en',
        keyword='Feature',
        name='payments',
        description='',
        scenarios=(scenario,),
        tags=(feature_tag,),
    )

    test_item = GherkinTestItem(feature, scenario, None, FEATURE_FILE)

    assert test_item.has_selection_label('billing:*') is True
    assert test_item.has_selection_label('critical') is True
    assert test_item.has_selection_label('slow') is False


def test_clear_model_keeps_reporting_fields_without_dynamic_types() -> None:
    scenario = _create_scenario(_create_step('step-1', 'I pay 10 EUR'))
    feature = _create_feature(scenario)
    test_item = GherkinTestItem(feature, scenario, None, FEATURE_FILE)
    test_item.step_result_list = [
        StepResult(
            _create_step('step-1', 'I pay 10 EUR'),
            None,
            TestResultStatus.PENDING,
        ),
    ]

    test_item.clear_model()

    assert test_item.feature.name == 'payments'
    assert test_item.scenario.name == 'payment scenario'
    assert test_item.feature.__class__.__name__ == '_StaticFeature'
    assert test_item.scenario.__class__.__name__ == '_StaticScenario'
    assert (
        test_item.step_result_list[0].step.__class__.__name__
        == '_StaticStep'
    )


def test_get_runtime_requirement_set_reads_only_runtime_requirement_tags(
) -> None:
    feature = Feature(
        location=Location(FEATURE_PATH, 1, 1),
        language='en',
        keyword='Feature',
        name='payments',
        description='',
        tags=(
            Tag(
                id='tag-1',
                location=Location(FEATURE_PATH, 1, 1),
                name='@requires:database/mongodb',
            ),
            Tag(
                id='tag-2',
                location=Location(FEATURE_PATH, 1, 1),
                name='@requires:transport/http',
            ),
        ),
    )
    scenario = Scenario(
        id='scenario-1',
        location=Location(FEATURE_PATH, 2, 1),
        keyword='Scenario',
        name='payment scenario',
        description='',
        tags=(
            Tag(
                id='tag-3',
                location=Location(FEATURE_PATH, 2, 1),
                name='@requires_capability:database/mongodb:transactions',
            ),
            Tag(
                id='tag-4',
                location=Location(FEATURE_PATH, 2, 1),
                name='@disallow_mode:database/mongodb:mock',
            ),
        ),
    )

    test_item = GherkinTestItem(feature, scenario, None, FEATURE_FILE)
    requirements = test_item.get_runtime_requirement_set()

    assert requirements.interfaces == (
        'database/mongodb',
        'transport/http',
    )
    assert len(requirements.capabilities) == 1
    assert requirements.capabilities[0].interface_name == 'database/mongodb'
    assert requirements.capabilities[0].capability_name == 'transactions'
    assert len(requirements.disallowed_modes) == 1
    assert (
        requirements.disallowed_modes[0].interface_name
        == 'database/mongodb'
    )
    assert requirements.disallowed_modes[0].mode_name == 'mock'
