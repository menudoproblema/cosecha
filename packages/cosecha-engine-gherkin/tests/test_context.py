from __future__ import annotations

import asyncio

from cosecha.core.engines.base import ExecutionContextMetadata
from cosecha.core.location import Location
from cosecha.core.manifest_types import ResourceBindingSpec
from cosecha.engine.gherkin.context import Context, ContextRegistry
from cosecha.engine.gherkin.models import (
    Cell,
    DataTable,
    Feature,
    Heading,
    HeadingCell,
    Row,
    Scenario,
    Step,
)
from cosecha.engine.gherkin.steps.registry import StepRegistry


FEATURE_PATH = Location('tests/payment.feature', 1, 1)


def _create_feature() -> Feature:
    return Feature(
        location=FEATURE_PATH,
        language='en',
        keyword='Feature',
        name='payments',
        description='',
    )


def _create_scenario() -> Scenario:
    return Scenario(
        id='scenario-1',
        location=Location('tests/payment.feature', 2, 1),
        keyword='Scenario',
        name='payment scenario',
        description='',
    )


def _create_table() -> DataTable:
    heading_cell = HeadingCell(
        location=Location('tests/payment.feature', 3, 1),
        name='amount',
    )
    heading = Heading(
        id='heading-1',
        location=Location('tests/payment.feature', 3, 1),
        cells=(heading_cell,),
    )
    row_cell = Cell(
        location=Location('tests/payment.feature', 4, 1),
        heading=heading_cell,
        value='10',
    )
    row = Row(
        id='row-1',
        location=Location('tests/payment.feature', 4, 1),
        cells=(row_cell,),
    )
    return DataTable(
        location=Location('tests/payment.feature', 3, 1),
        heading=heading,
        rows=(row,),
    )


def _create_step(table: DataTable | None) -> Step:
    return Step(
        id='step-1',
        location=Location('tests/payment.feature', 3, 1),
        keyword='Given ',
        keyword_type='Context',
        step_type='given',
        text='I pay 10 EUR',
        table=table,
    )


def test_context_table_reuses_step_table() -> None:
    context = Context(ContextRegistry(), StepRegistry(), {})
    feature = _create_feature()
    scenario = _create_scenario()
    table = _create_table()
    step = _create_step(table)

    context.set_step(feature, scenario, step)

    assert context.table is table
    assert context.table is step.table


def test_context_table_returns_none_when_step_has_no_table() -> None:
    context = Context(ContextRegistry(), StepRegistry(), {})
    context.set_step(_create_feature(), _create_scenario(), _create_step(None))

    assert context.table is None


def test_context_registry_copy_is_isolated_but_shallow() -> None:
    shared_item = {'value': 1}
    registry = ContextRegistry()
    registry.add('helper', 'payments', shared_item)

    copied_registry = registry.copy()
    copied_registry.add('resource', 'db', object())

    assert registry.get('resource', 'db') is None
    assert copied_registry.get('helper', 'payments') is shared_item


def test_context_exposes_manifest_resource_aliases() -> None:
    context = Context(
        ContextRegistry(),
        StepRegistry(),
        {},
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='gherkin',
                resource_name='shared_db',
                layout='resource',
                alias='db',
            ),
        ),
    )

    context.set_resources({'shared_db': 'db-handle'})

    assert context['shared_db'] == 'db-handle'
    assert context['db'] == 'db-handle'
    assert context.registry.get('resource', 'db') == 'db-handle'


def test_context_notifies_step_observer_with_execution_metadata() -> None:
    observed: list[tuple[str, object, str | None]] = []

    async def _observer(phase, context, step, status, message) -> None:
        del step, message
        observed.append((phase, context.execution_metadata, status))

    context = Context(
        ContextRegistry(),
        StepRegistry(),
        {},
        step_event_callback=_observer,
    )
    step = _create_step(None)
    context.set_execution_metadata(
        ExecutionContextMetadata(
            node_id='node-1',
            node_stable_id='stable-node-1',
            session_id='session-1',
            trace_id='trace-1',
            worker_id=2,
        ),
    )
    context.set_step(_create_feature(), _create_scenario(), step)

    async def _notify() -> None:
        await context.notify_step_started(step)
        await context.notify_step_finished(
            step,
            status='passed',
            message='ok',
        )

    asyncio.run(_notify())

    assert observed[0][0] == 'started'
    assert observed[0][1] is not None
    assert observed[0][1].node_stable_id == 'stable-node-1'
    assert observed[1][0] == 'finished'
    assert observed[1][2] == 'passed'
