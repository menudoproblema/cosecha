from __future__ import annotations

from pathlib import Path

import pytest

from cosecha.core.location import Location
from cosecha.engine.gherkin.models import (
    Background,
    Cell,
    Heading,
    HeadingCell,
    Row,
    Scenario,
    Step,
)


def test_heading_cell_extracts_coercion_suffix() -> None:
    heading_cell = HeadingCell(
        location=Location(Path('demo.feature'), 1),
        name='count::int',
    )

    assert heading_cell.name == 'count'
    assert heading_cell.coerce == 'int'


def test_row_get_values_normalizes_and_applies_coercions() -> None:
    heading_cell = HeadingCell(
        location=Location(Path('demo.feature'), 2),
        name='count::int',
    )
    row = Row(
        id='row-1',
        location=Location(Path('demo.feature'), 3),
        cells=(
            Cell(
                location=Location(Path('demo.feature'), 3),
                heading=heading_cell,
                value=' 7 ',
            ),
        ),
    )

    values = row.get_values(
        coercions={'int': lambda value, _location: int(value)},
        normalize=lambda value: value.strip() if value is not None else None,
    )

    assert values == {'count': 7}


def test_row_get_values_rejects_unknown_coercion() -> None:
    heading_cell = HeadingCell(
        location=Location(Path('demo.feature'), 2),
        name='count::int',
    )
    row = Row(
        id='row-1',
        location=Location(Path('demo.feature'), 3),
        cells=(
            Cell(
                location=Location(Path('demo.feature'), 3),
                heading=heading_cell,
                value='7',
            ),
        ),
    )

    with pytest.raises(TypeError, match='Unknown coercion "int"'):
        row.get_values()


def test_scenario_all_steps_includes_background_steps() -> None:
    background_step = Step(
        id='bg-step',
        location=Location(Path('demo.feature'), 1),
        keyword='Given ',
        keyword_type='Context',
        step_type='given',
        text='a background',
    )
    scenario_step = Step(
        id='scenario-step',
        location=Location(Path('demo.feature'), 3),
        keyword='When ',
        keyword_type='Action',
        step_type='when',
        text='something happens',
    )
    scenario = Scenario(
        id='scenario-1',
        location=Location(Path('demo.feature'), 2),
        keyword='Scenario',
        name='Demo',
        description='',
        steps=(scenario_step,),
        background=Background(
            id='background-1',
            location=Location(Path('demo.feature'), 1),
            keyword='Background',
            name='Setup',
            description='',
            steps=(background_step,),
        ),
    )

    assert scenario.all_steps == (background_step, scenario_step)


def test_heading_names_and_row_getitem_behaviour() -> None:
    heading_cell = HeadingCell(
        location=Location(Path('demo.feature'), 1),
        name='username',
    )
    heading = Heading(
        id='heading-1',
        location=Location(Path('demo.feature'), 1),
        cells=(heading_cell,),
    )
    row = Row(
        id='row-1',
        location=Location(Path('demo.feature'), 2),
        cells=(
            Cell(
                location=Location(Path('demo.feature'), 2),
                heading=heading_cell,
                value='uve',
            ),
        ),
    )

    assert heading.names == ('username',)
    assert row['username'] == 'uve'
    with pytest.raises(KeyError, match='Unknown field "missing"'):
        _ = row['missing']
