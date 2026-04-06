from __future__ import annotations

from pathlib import Path

from cosecha.core.location import Location
from cosecha.engine.gherkin.models import (
    Background,
    Cell,
    DataTable,
    Heading,
    HeadingCell,
    Row,
    Scenario,
    Step,
    Tag,
)
from cosecha.engine.gherkin.utils import (
    _substitute_placeholder,
    create_background_with_example,
    create_scenario_with_example,
)


FEATURE_PATH = Path('features/login.feature')


def _build_row() -> Row:
    heading_cells = (
        HeadingCell(Location(FEATURE_PATH, 1), 'username'),
        HeadingCell(
            Location(FEATURE_PATH, 1),
            'password',
        ),
    )
    return Row(
        id='row-1',
        location=Location(FEATURE_PATH, 2),
        cells=(
            Cell(Location(FEATURE_PATH, 2), heading_cells[0], 'uve'),
            Cell(Location(FEATURE_PATH, 2), heading_cells[1], 'secret'),
        ),
    )


def _build_scenario() -> Scenario:
    row = _build_row()
    table = DataTable(
        location=Location(FEATURE_PATH, 5),
        heading=Heading(
            id='heading',
            location=Location(FEATURE_PATH, 4),
            cells=tuple(cell.heading for cell in row.cells),
        ),
        rows=(row,),
    )
    background = Background(
        id='background',
        location=Location(FEATURE_PATH, 3),
        keyword='Background',
        name='setup',
        description='',
        steps=(
            Step(
                id='bg-1',
                location=Location(FEATURE_PATH, 3, name='example'),
                keyword='Given ',
                keyword_type='Context',
                step_type='given',
                text='a user <username>',
                table=table,
            ),
        ),
    )
    return Scenario(
        id='scenario',
        location=Location(FEATURE_PATH, 6, name='Scenario Outline'),
        keyword='Scenario Outline',
        name='login for <username>',
        description='',
        steps=(
            Step(
                id='step-1',
                location=Location(FEATURE_PATH, 7, name='example'),
                keyword='When ',
                keyword_type='Action',
                step_type='when',
                text='the user logs in with <password>',
                table=table,
            ),
        ),
        tags=(
            Tag(
                id='tag-1',
                location=Location(FEATURE_PATH, 1),
                name='api',
            ),
        ),
        background=background,
    )


def test_substitute_placeholder_replaces_known_values() -> None:
    assert _substitute_placeholder(
        'login for <username>',
        {'username': 'uve'},
    ) == 'login for uve'


def test_create_background_with_example_substitutes_step_text_and_table(
) -> None:
    background = _build_scenario().background
    assert background is not None
    row = _build_row()

    substituted = create_background_with_example(
        background,
        'example-1',
        row,
    )

    assert substituted is not None
    assert substituted.steps[0].text == 'a user uve'
    assert substituted.steps[0].table.heading.cells[1].name == 'password'
    assert substituted.steps[0].table.rows[0].cells[1].value == 'secret'


def test_create_scenario_with_example_merges_tags_and_names() -> None:
    scenario = _build_scenario()
    row = _build_row()
    example_tags = (
        Tag(id='tag-2', location=Location(FEATURE_PATH, 8), name='db'),
        Tag(
            id='tag-3',
            location=Location(FEATURE_PATH, 8),
            name='api',
        ),
    )

    substituted = create_scenario_with_example(
        scenario,
        'example-1',
        row,
        example_tags,
    )

    assert substituted.name == 'login for uve [example-1]'
    assert substituted.steps[0].text == 'the user logs in with secret'
    assert {tag.name for tag in substituted.tags} == {'api', 'db'}
    assert substituted.background is not None
    assert substituted.background.steps[0].location.name == 'example-1'
