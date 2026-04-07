from __future__ import annotations

import asyncio

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from cosecha.core.items import TestResultStatus
from cosecha.core.location import Location
from cosecha.core.resources import ResourceRequirement
from cosecha.engine.gherkin.items import GherkinTestItem, StepResult
from cosecha.engine.gherkin.models import (
    Cell,
    DataTable,
    Example,
    Feature,
    Heading,
    HeadingCell,
    Row,
    Scenario,
    Step,
    Tag,
)
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
        self.table = step.table


class ObservableContext(DummyContext):
    def __init__(self, step_registry: StepRegistry) -> None:
        super().__init__(step_registry)
        self.started_steps: list[str] = []
        self.finished_steps: list[tuple[str, str, str | None]] = []

    async def notify_step_started(self, step: Step) -> None:
        self.started_steps.append(step.text)

    async def notify_step_finished(
        self,
        step: Step,
        *,
        status: str,
        message: str | None,
    ) -> None:
        self.finished_steps.append((step.text, status, message))


async def _failing_step(_context: Context) -> None:
    raise AssertionError


async def _passing_step(_context: Context) -> None:
    return None


def _create_table(row_count: int) -> DataTable:
    heading_cell = HeadingCell(
        location=Location(FEATURE_PATH, 3, 1),
        name='amount',
    )
    heading = Heading(
        id='heading-1',
        location=Location(FEATURE_PATH, 3, 1),
        cells=(heading_cell,),
    )
    rows = []
    for index in range(row_count):
        cell = Cell(
            location=Location(FEATURE_PATH, 4 + index, 1),
            heading=heading_cell,
            value=str(index),
        )
        rows.append(
            Row(
                id=f'row-{index + 1}',
                location=Location(FEATURE_PATH, 4 + index, 1),
                cells=(cell,),
            ),
        )
    return DataTable(
        location=Location(FEATURE_PATH, 3, 1),
        heading=heading,
        rows=tuple(rows),
    )


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


def test_run_step_validates_table_constraints_and_unmatched_steps() -> None:
    registry = StepRegistry()
    test_item = GherkinTestItem(
        _create_feature(_create_scenario()),
        _create_scenario(),
        None,
        FEATURE_FILE,
    )

    unmatched = StepResult(
        _create_step('step-1', 'plain step'),
        None,
        TestResultStatus.PENDING,
    )
    with pytest.raises(RuntimeError, match='unmatched StepResult'):
        asyncio.run(test_item.run_step(unmatched, DummyContext(registry)))

    async def _min_table_step(_context: Context) -> None:
        del _context

    min_table_definition = StepDefinition(
        'given',
        (StepText('needs table', min_table_rows=2),),
        _min_table_step,
    )
    min_table_match = min_table_definition.match('needs table')
    assert min_table_match is not None
    min_table_result = StepResult(
        _create_step('step-2', 'needs table'),
        min_table_match,
        TestResultStatus.PENDING,
    )
    min_table_context = DummyContext(registry)
    asyncio.run(test_item.run_step(min_table_result, min_table_context))
    assert min_table_result.status == TestResultStatus.ERROR
    assert min_table_result.message == 'Error executing step'

    async def _required_table_step(_context: Context) -> None:
        del _context

    required_definition = StepDefinition(
        'given',
        (StepText('needs exact table', required_table_rows=1),),
        _required_table_step,
    )
    required_match = required_definition.match('needs exact table')
    assert required_match is not None
    required_result = StepResult(
        _create_step('step-3', 'needs exact table'),
        required_match,
        TestResultStatus.PENDING,
    )
    required_context = DummyContext(registry)
    required_context.table = _create_table(2)
    asyncio.run(test_item.run_step(required_result, required_context))
    assert required_result.status == TestResultStatus.ERROR

    async def _no_table_step(_context: Context) -> None:
        del _context

    no_table_definition = StepDefinition(
        'given',
        (StepText('no table expected', can_use_table=False),),
        _no_table_step,
    )
    no_table_match = no_table_definition.match('no table expected')
    assert no_table_match is not None
    no_table_result = StepResult(
        _create_step('step-4', 'no table expected'),
        no_table_match,
        TestResultStatus.PENDING,
    )
    no_table_context = DummyContext(registry)
    no_table_context.table = _create_table(1)
    asyncio.run(test_item.run_step(no_table_result, no_table_context))
    assert no_table_result.status == TestResultStatus.ERROR


def test_run_step_passes_arguments_and_handles_assertion_and_runtime_errors(
) -> None:
    captured_kwargs: dict[str, str] = {}

    async def _capture_step(_context: Context, **kwargs) -> None:
        del _context
        captured_kwargs.update(kwargs)

    async def _assertion_step(_context: Context) -> None:
        raise AssertionError('boom')

    class _CodeError(Exception):
        code = 'demo_error'

    async def _runtime_step(_context: Context) -> None:
        raise _CodeError('runtime')

    registry = StepRegistry()
    item = GherkinTestItem(
        _create_feature(_create_scenario()),
        _create_scenario(),
        None,
        FEATURE_FILE,
    )
    context = DummyContext(registry)

    capture_definition = StepDefinition(
        'given',
        (StepText('capture {value}', custom='flag'),),
        _capture_step,
    )
    capture_match = capture_definition.match('capture alpha')
    assert capture_match is not None
    capture_result = StepResult(
        _create_step('step-1', 'capture alpha'),
        capture_match,
        TestResultStatus.PENDING,
    )
    asyncio.run(item.run_step(capture_result, context))
    assert capture_result.status == TestResultStatus.PASSED
    assert captured_kwargs == {'custom': 'flag', 'value': 'alpha'}

    assertion_definition = StepDefinition(
        'given',
        (StepText('assert fails'),),
        _assertion_step,
    )
    assertion_match = assertion_definition.match('assert fails')
    assert assertion_match is not None
    assertion_result = StepResult(
        _create_step('step-2', 'assert fails'),
        assertion_match,
        TestResultStatus.PENDING,
    )
    asyncio.run(item.run_step(assertion_result, context))
    assert assertion_result.status == TestResultStatus.FAILED
    assert assertion_result.message == 'Step failed'
    assert assertion_result.exc_info is not None

    runtime_definition = StepDefinition(
        'given',
        (StepText('runtime fails'),),
        _runtime_step,
    )
    runtime_match = runtime_definition.match('runtime fails')
    assert runtime_match is not None
    runtime_result = StepResult(
        _create_step('step-3', 'runtime fails'),
        runtime_match,
        TestResultStatus.PENDING,
    )
    asyncio.run(item.run_step(runtime_result, context))
    assert runtime_result.status == TestResultStatus.ERROR
    assert runtime_result.message == 'Error executing step'
    assert runtime_result.exc_info is not None


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


def test_get_runtime_requirement_set_includes_required_modes() -> None:
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
                name='@requires_mode:database/mongodb:replica',
            ),
        ),
    )
    scenario = _create_scenario()
    item = GherkinTestItem(feature, scenario, None, FEATURE_FILE)

    requirements = item.get_runtime_requirement_set()

    assert len(requirements.required_modes) == 1
    assert requirements.required_modes[0].interface_name == 'database/mongodb'
    assert requirements.required_modes[0].mode_name == 'replica'


def test_run_skips_when_scenario_has_no_steps() -> None:
    scenario = _create_scenario()
    feature = _create_feature(scenario)
    item = GherkinTestItem(feature, scenario, None, FEATURE_FILE)

    asyncio.run(item.run(DummyContext(StepRegistry())))

    assert item.status == TestResultStatus.SKIPPED
    assert item.message == 'No steps to run'


def test_run_marks_missing_step_implementation_and_notifies_observers() -> None:
    first_step = _create_step('step-1', 'unknown step')
    second_step = _create_step('step-2', 'known step')
    scenario = _create_scenario(first_step, second_step)
    feature = _create_feature(scenario)
    item = GherkinTestItem(feature, scenario, None, FEATURE_FILE)

    registry = StepRegistry()
    registry.add_step_definition(
        StepDefinition('given', (StepText('known step'),), _passing_step),
    )
    context = ObservableContext(registry)

    asyncio.run(item.run(context))

    assert item.status == TestResultStatus.SKIPPED
    assert item.message == 'Missing step impl'
    assert context.started_steps == []
    assert context.finished_steps == []
    assert [step_result.message for step_result in item.step_result_list] == [
        'Missing step impl',
        None,
    ]


def test_run_sets_runtime_failure_kind_and_error_code() -> None:
    class _DomainError(Exception):
        code = 'db_unavailable'

    async def _runtime_step(_context: Context) -> None:
        raise _DomainError('db down')

    step = _create_step('step-1', 'runtime error')
    scenario = _create_scenario(step)
    feature = _create_feature(scenario)
    item = GherkinTestItem(feature, scenario, None, FEATURE_FILE)
    registry = StepRegistry()
    registry.add_step_definition(
        StepDefinition('given', (StepText('runtime error'),), _runtime_step),
    )
    context = ObservableContext(registry)

    asyncio.run(item.run(context))

    assert item.status == TestResultStatus.ERROR
    assert item.failure_kind == 'runtime'
    assert item.error_code == 'db_unavailable'
    assert context.started_steps == ['runtime error']
    assert context.finished_steps == [
        ('runtime error', 'error', 'Error executing step'),
    ]


def test_bind_manifest_resources_clear_model_and_reporting_helpers() -> None:
    step = _create_step('step-1', 'I pay 10 EUR')
    scenario = _create_scenario(step)
    feature = _create_feature(scenario)
    heading_cell = HeadingCell(
        location=Location(FEATURE_PATH, 6, 1),
        name='amount',
    )
    example = Example(
        id='example-1',
        location=Location(FEATURE_PATH, 6, 1),
        name='Example',
        description='',
        keyword='Examples',
        heading=Heading(
            id='heading-1',
            location=Location(FEATURE_PATH, 6, 1),
            cells=(heading_cell,),
        ),
        rows=(),
        tags=(),
    )
    item = GherkinTestItem(feature, scenario, example, FEATURE_FILE)
    resource_requirement = ResourceRequirement(
        name='db',
        setup=lambda _context: object(),
    )
    item.bind_manifest_resources((resource_requirement,))

    item.step_result_list = [
        StepResult(step, None, TestResultStatus.SKIPPED),
    ]
    payload = item.build_engine_report_payload()
    required_step_texts = item.get_required_step_texts()
    item.clear_model()

    assert item.get_resource_requirements() == (resource_requirement,)
    assert payload['feature']['name'] == 'payments'
    assert required_step_texts == (('given', 'I pay 10 EUR'),)
    assert item.example is not None
    assert item.example.__class__.__name__ == '_StaticExample'
    assert repr(item).startswith('<GherkinTest ')
    assert repr(item.step_result_list[0]) == (
        '<StepResult TestResultStatus.SKIPPED>'
    )
