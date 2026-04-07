from __future__ import annotations

import types

from pathlib import Path

import pytest

from cosecha.core.exceptions import CosechaParserError
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
from cosecha.engine.gherkin.steps import given
from cosecha.engine.gherkin.steps.registry import StepRegistry
from cosecha.engine.gherkin.utils import (
    _generate_data_table,
    _generate_example,
    _generate_step,
    _get_step_definitions_from_module,
    _substitute_in_datatable,
    _substitute_placeholder,
    generate_model_from_gherkin,
    get_step_definitions_from_module,
    import_and_load_steps_from_module,
    import_step_modules,
    load_steps_from_module,
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


def test_create_background_with_example_returns_none_when_background_missing(
) -> None:
    assert create_background_with_example(None, 'example-1', _build_row()) is None


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


def test_substitute_in_datatable_preserves_coerce_suffix() -> None:
    row = _build_row()
    heading_with_coerce = HeadingCell(
        location=Location(FEATURE_PATH, 1),
        name='amount::int',
    )
    table = DataTable(
        location=Location(FEATURE_PATH, 5),
        heading=Heading(
            id='heading',
            location=Location(FEATURE_PATH, 4),
            cells=(heading_with_coerce,),
        ),
        rows=(
            Row(
                id='row-1',
                location=row.location,
                cells=(Cell(row.location, heading_with_coerce, '<username>'),),
            ),
        ),
    )

    substituted = _substitute_in_datatable(table, {'username': 'uve'})

    assert substituted.heading.cells[0].name == 'amount'
    assert substituted.heading.cells[0].coerce == 'int'
    assert substituted.rows[0].cells[0].value == 'uve'


def test_generate_data_table_example_and_step_helpers() -> None:
    table_data = {
        'location': {'line': 10, 'column': 1},
        'rows': [
            {
                'id': 'heading',
                'location': {'line': 10, 'column': 1},
                'cells': [
                    {'location': {'line': 10, 'column': 2}, 'value': 'user'},
                ],
            },
            {
                'id': 'row-1',
                'location': {'line': 11, 'column': 1},
                'cells': [
                    {'location': {'line': 11, 'column': 2}, 'value': 'alice'},
                ],
            },
        ],
    }
    example_data = {
        'id': 'example-1',
        'location': {'line': 20, 'column': 1},
        'name': 'Example 1',
        'description': '',
        'keyword': 'Examples',
        'tableHeader': table_data['rows'][0],
        'tableBody': [table_data['rows'][1]],
        'tags': [{'id': 'tag-1', 'name': 'api', 'location': {'line': 20, 'column': 2}}],
    }
    step_data = {
        'id': 'step-1',
        'location': {'line': 30, 'column': 1},
        'keyword': 'Given ',
        'keywordType': 'Context',
        'text': 'the user exists',
        'dataTable': table_data,
    }

    data_table = _generate_data_table(table_data, FEATURE_PATH)
    example = _generate_example(example_data, FEATURE_PATH)
    step = _generate_step(step_data, FEATURE_PATH)

    assert data_table.rows[0].cells[0].value == 'alice'
    assert example.rows[0].cells[0].value == 'alice'
    assert example.tags[0].name == 'api'
    assert step.table is not None
    assert step.step_type == 'given'

    and_step = _generate_step(
        {
            'id': 'step-2',
            'location': {'line': 31, 'column': 1},
            'keyword': 'And ',
            'keywordType': 'Conjunction',
            'text': 'another thing',
        },
        FEATURE_PATH,
        prev_step=step,
    )
    assert and_step.step_type == 'given'

    with pytest.raises(CosechaParserError, match='Unexpected "and" keyword'):
        _generate_step(
            {
                'id': 'step-3',
                'location': {'line': 32, 'column': 1},
                'keyword': 'And ',
                'keywordType': 'Conjunction',
                'text': 'invalid',
            },
            FEATURE_PATH,
        )

    with pytest.raises(CosechaParserError, match='Invalid step_type'):
        _generate_step(
            {
                'id': 'step-4',
                'location': {'line': 33, 'column': 1},
                'keyword': 'Unknown ',
                'keywordType': 'Unknown',
                'text': 'invalid',
            },
            FEATURE_PATH,
        )


def test_generate_model_from_gherkin_handles_background_and_unsupported_child(
) -> None:
    data = {
        'feature': {
            'location': {'line': 1, 'column': 1},
            'language': 'en',
            'keyword': 'Feature',
            'name': 'Login',
            'description': '',
            'tags': [],
            'children': [
                {
                    'background': {
                        'id': 'bg',
                        'location': {'line': 2, 'column': 1},
                        'keyword': 'Background',
                        'name': 'setup',
                        'description': '',
                        'steps': [
                            {
                                'id': 'bg-step',
                                'location': {'line': 3, 'column': 3},
                                'keyword': 'Given ',
                                'keywordType': 'Context',
                                'text': 'a setup step',
                            },
                        ],
                    },
                },
                {
                    'scenario': {
                        'id': 'scenario-1',
                        'location': {'line': 4, 'column': 1},
                        'keyword': 'Scenario',
                        'name': 'happy path',
                        'description': '',
                        'tags': [],
                        'steps': [
                            {
                                'id': 'step-1',
                                'location': {'line': 5, 'column': 3},
                                'keyword': 'When ',
                                'keywordType': 'Action',
                                'text': 'the user logs in',
                            },
                        ],
                        'examples': [
                            {
                                'id': 'example-1',
                                'location': {'line': 6, 'column': 1},
                                'name': '',
                                'description': '',
                                'keyword': 'Examples',
                                'tableHeader': {
                                    'id': 'header',
                                    'location': {'line': 6, 'column': 1},
                                    'cells': [
                                        {'location': {'line': 6, 'column': 2}, 'value': 'username'},
                                    ],
                                },
                                'tableBody': [
                                    {
                                        'id': 'row-1',
                                        'location': {'line': 7, 'column': 1},
                                        'cells': [
                                            {'location': {'line': 7, 'column': 2}, 'value': 'uve'},
                                        ],
                                    },
                                ],
                                'tags': [],
                            },
                        ],
                    },
                },
            ],
        },
    }

    feature = generate_model_from_gherkin(data, FEATURE_PATH)
    assert feature.background is not None
    assert feature.scenarios[0].background is feature.background
    assert feature.scenarios[0].examples[0].rows[0].cells[0].value == 'uve'

    with pytest.raises(CosechaParserError, match='Unsupported child'):
        generate_model_from_gherkin(
            {
                'feature': {
                    **data['feature'],
                    'children': [{'unknown': {}}],
                },
            },
            FEATURE_PATH,
        )


def test_module_step_loading_and_import_helpers(tmp_path: Path, monkeypatch) -> None:
    module = types.ModuleType('demo_steps')
    module_file = tmp_path / 'demo_steps.py'
    module_file.write_text('', encoding='utf-8')
    module.__file__ = str(module_file)

    @given('the user exists')
    async def user_exists(context):
        del context

    def helper():
        return None

    module.user_exists = user_exists
    module.helper = helper

    all_definitions = get_step_definitions_from_module(module)
    filtered_definitions = _get_step_definitions_from_module(
        module,
        function_names=('user_exists', 'missing', 'helper'),
    )
    assert len(all_definitions) == 1
    assert len(filtered_definitions) == 1

    step_registry = StepRegistry()
    load_steps_from_module(module, step_registry)
    assert step_registry.find_match('given', 'the user exists') is not None

    steps_dir = tmp_path / 'steps'
    steps_dir.mkdir()
    (steps_dir / 'a_steps.py').write_text(
        '\n'.join(
            (
                'from cosecha.engine.gherkin.steps import given',
                '@given("alpha step")',
                'async def alpha(context):',
                '    del context',
            ),
        ),
        encoding='utf-8',
    )
    (steps_dir / 'b_steps.py').write_text(
        '\n'.join(
            (
                'from cosecha.engine.gherkin.steps import given',
                '@given("beta step")',
                'async def beta(context):',
                '    del context',
            ),
        ),
        encoding='utf-8',
    )
    invalid_path = tmp_path / 'invalid.txt'
    invalid_path.write_text('', encoding='utf-8')

    with pytest.raises(TypeError, match='is incorrect'):
        import_step_modules(invalid_path)

    imported_from_file = import_step_modules(steps_dir / 'a_steps.py')
    imported_from_dir = import_step_modules(steps_dir)
    assert len(imported_from_file) == 1
    assert len(imported_from_dir) == 2

    module_spec_dir = tmp_path / 'specpkg'
    module_spec_dir.mkdir()
    (module_spec_dir / '__init__.py').write_text('', encoding='utf-8')
    (module_spec_dir / 'mod.py').write_text('VALUE = 1\n', encoding='utf-8')
    monkeypatch.syspath_prepend(str(tmp_path))
    imported_from_spec = import_step_modules('specpkg.mod')
    assert imported_from_spec[0].VALUE == 1

    registry_from_import = StepRegistry()
    import_and_load_steps_from_module(
        steps_dir / 'a_steps.py',
        registry_from_import,
    )
    assert registry_from_import.find_match('given', 'alpha step') is not None
