from __future__ import annotations

import importlib
import re

from contextlib import suppress
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any

from cosecha.core.exceptions import CosechaParserError
from cosecha.core.location import Location
from cosecha.core.utils import import_module_from_path
from cosecha.engine.gherkin.models import (
    Background,
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


if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import ModuleType

    from cosecha.engine.gherkin.steps.registry import StepRegistry


# Cache: (module.__name__, module.__file__, mtime_ns) -> step definitions
# Evita re-inspeccionar vars(module) en cada sesion cuando el fichero no
# ha cambiado.
_steps_cache: dict[tuple[str, str | None, int | None], tuple[Any, ...]] = {}


def _generate_tags(
    data_list: Iterable[dict[str, Any]],
    feature_path: Path,
) -> tuple[Tag, ...]:
    return tuple(
        Tag(
            id=tag_data['id'],
            name=tag_data['name'],
            location=Location(**tag_data['location'], filename=feature_path),
        )
        for tag_data in data_list
    )


def _generate_cells(
    raw_cells: list[dict[str, Any]],
    heading_cells: tuple[HeadingCell, ...],
    feature_path: Path,
) -> tuple[Cell, ...]:
    return tuple(
        Cell(
            location=Location(**cell['location'], filename=feature_path),
            heading=heading,
            value=cell['value'],
        )
        for cell, heading in zip(raw_cells, heading_cells, strict=False)
    )


def _generate_heading_cells(
    raw_cells: list[dict[str, Any]],
    feature_path: Path,
) -> tuple[HeadingCell, ...]:
    return tuple(
        HeadingCell(
            location=Location(**cell['location'], filename=feature_path),
            name=cell['value'],
        )
        for cell in raw_cells
    )


def _generate_data_table(
    table_data: dict[str, Any],
    feature_path: Path,
) -> DataTable:
    heading_row, *rows = table_data['rows']

    heading = Heading(
        id=heading_row['id'],
        location=Location(
            filename=feature_path,
            line=heading_row['location']['line'],
            column=heading_row['location']['column'],
        ),
        cells=_generate_heading_cells(heading_row['cells'], feature_path),
    )

    rows = [
        Row(
            id=row['id'],
            location=Location(
                filename=feature_path,
                line=row['location']['line'],
                column=row['location']['column'],
            ),
            cells=_generate_cells(row['cells'], heading.cells, feature_path),
        )
        for row in rows
    ]
    return DataTable(
        location=Location(**table_data['location'], filename=feature_path),
        heading=heading,
        rows=tuple(rows),
    )


def _generate_example(
    example_data: dict[str, Any],
    feature_path: Path,
) -> Example:
    heading_row = example_data['tableHeader']

    # Generar el encabezado para el Example.
    heading = Heading(
        id=heading_row['id'],
        location=Location(
            filename=feature_path,
            line=heading_row['location']['line'],
            column=heading_row['location']['column'],
        ),
        cells=_generate_heading_cells(heading_row['cells'], feature_path),
    )

    # Generar las filas de datos para el Example.
    rows = [
        Row(
            id=row['id'],
            location=Location(
                filename=feature_path,
                line=row['location']['line'],
                column=row['location']['column'],
            ),
            cells=_generate_cells(row['cells'], heading.cells, feature_path),
        )
        for row in example_data['tableBody']
    ]

    # Construir y retornar la instancia de Examples.
    return Example(
        id=example_data['id'],
        location=Location(**example_data['location'], filename=feature_path),
        name=example_data.get('name', ''),
        description=example_data.get('description', ''),
        keyword=example_data['keyword'],
        heading=heading,
        rows=tuple(rows),
        tags=_generate_tags(example_data.get('tags', []), feature_path),
    )


def _generate_step(
    step_data: dict[str, Any],
    feature_path: Path,
    prev_step: Step | None = None,
) -> Step:
    # Chequear si el paso tiene DataTable
    data_table = None
    if 'dataTable' in step_data:
        data_table = _generate_data_table(step_data['dataTable'], feature_path)

    step_type = step_data['keyword'].strip().lower()
    if step_type == 'and':
        if not prev_step:
            reason = 'Unexpected "and" keyword'
            raise CosechaParserError(
                reason,
                feature_path,
                step_data.get('line', 0),
                step_data.get('column', 0),
            )

        step_type = prev_step.step_type
    elif step_type not in ('given', 'when', 'then', 'but'):
        reason = 'Invalid step_type: "{step_type}"'
        raise CosechaParserError(
            reason,
            feature_path,
            step_data.get('line', 0),
            step_data.get('column', 0),
        )

    return Step(
        id=step_data['id'],
        location=Location(
            **step_data['location'],
            filename=feature_path,
        ),
        keyword=step_data['keyword'],
        keyword_type=step_data.get('keywordType', ''),
        step_type=step_type,
        text=step_data['text'],
        table=data_table,
    )


def _substitute_placeholder(text: str, substitutions: dict[str, str]) -> str:
    # La mayoria de textos de step no contienen placeholders: el chequeo
    # O(n) en C de `in` es mucho mas rapido que compilar y ejecutar el regex.
    if '<' not in text:
        return text

    def replacement(match: re.Match[str]) -> str:
        variable_name = match.group(1)
        return substitutions.get(variable_name, f'<{variable_name}>')

    return re.sub(r'<([^>]+)>', replacement, text)


def _substitute_in_datatable(
    datatable: DataTable,
    substitutions: dict[str, str],
) -> DataTable:
    substituted_heading_cells: list[HeadingCell] = []

    for cell in datatable.heading.cells:
        new_name = _substitute_placeholder(cell.name, substitutions)
        if cell.coerce:
            new_name = f'{new_name}::{cell.coerce}'
        substituted_heading_cells.append(
            HeadingCell(
                location=cell.location,
                name=new_name,
            ),
        )

    substituted_heading = Heading(
        id=datatable.heading.id,
        location=datatable.heading.location,
        cells=tuple(substituted_heading_cells),
    )

    substituted_rows: list[Row] = []
    for row in datatable.rows:
        substituted_cells = [
            Cell(
                location=cell.location,
                heading=heading,
                value=_substitute_placeholder(cell.value, substitutions)
                if cell.value
                else None,
            )
            for cell, heading in zip(
                row.cells,
                substituted_heading_cells,
                strict=True,
            )
        ]
        substituted_rows.append(
            Row(
                id=row.id,
                location=row.location,
                cells=tuple(substituted_cells),
            ),
        )
    return DataTable(
        location=datatable.location,
        heading=substituted_heading,
        rows=tuple(substituted_rows),
    )


def create_background_with_example(
    background: Background | None,
    example_name: str,
    example_row: Row,
) -> Background | None:
    if background is None:
        return None

    substitutions: dict[str, Any] = example_row.get_values()

    background_steps: list[Step] = []
    for step in background.steps:
        background_table = (
            _substitute_in_datatable(step.table, substitutions)
            if step.table
            else None
        )
        background_steps.append(
            Step(
                id=step.id,
                location=step.location.with_name(example_name),
                keyword=step.keyword,
                keyword_type=step.keyword_type,
                step_type=step.step_type,
                text=_substitute_placeholder(step.text, substitutions),
                table=background_table,
            ),
        )

    return Background(
        id=background.id,
        location=background.location,
        keyword=background.keyword,
        name=background.name,
        description=background.description,
        steps=tuple(background_steps),
    )


def create_scenario_with_example(
    scenario: Scenario,
    example_name: str,
    example_row: Row,
    example_tags: tuple[Tag, ...],
) -> Scenario:
    substitutions: dict[str, Any] = example_row.get_values()

    substituted_steps: list[Step] = []
    for step in scenario.steps:
        substituted_table = (
            _substitute_in_datatable(step.table, substitutions)
            if step.table
            else None
        )
        substituted_steps.append(
            Step(
                id=step.id,
                location=step.location.with_name(example_name),
                keyword=step.keyword,
                keyword_type=step.keyword_type,
                step_type=step.step_type,
                text=_substitute_placeholder(step.text, substitutions),
                table=substituted_table,
            ),
        )

    substituted_name = _substitute_placeholder(scenario.name, substitutions)
    existing_tag_names = {tag.name for tag in scenario.tags}
    combined_tags = list(scenario.tags) + [
        tag for tag in example_tags if tag.name not in existing_tag_names
    ]

    return Scenario(
        id=scenario.id,
        location=scenario.location.with_name(example_name),
        keyword=scenario.keyword,
        name=f'{substituted_name} [{example_name}]',
        description=scenario.description,
        steps=tuple(substituted_steps),
        examples=scenario.examples,
        tags=tuple(combined_tags),
        background=create_background_with_example(
            scenario.background,
            example_name,
            example_row,
        ),
    )


def generate_model_from_gherkin(
    data: dict[str, Any],
    feature_path: Path,
) -> Feature:
    feature_data = data['feature']

    # Construyendo la feature:
    feature_tags = _generate_tags(feature_data['tags'], feature_path)
    feature_location = Location(
        **feature_data['location'],
        filename=feature_path,
    )
    background: Background | None = None
    scenario_list: list[Scenario] = []

    for child in feature_data['children']:
        if 'background' in child:
            background_steps: list[Step] = []
            background_data = child['background']

            for step_data in background_data.get('steps', []):
                step = _generate_step(
                    step_data,
                    feature_path,
                    background_steps[-1] if background_steps else None,
                )
                background_steps.append(step)

            background = Background(
                id=background_data['id'],
                location=Location(
                    **background_data['location'],
                    filename=feature_path,
                ),
                keyword=background_data['keyword'],
                name=background_data['name'],
                description=background_data['description'],
                steps=tuple(background_steps),
            )

        elif 'scenario' in child:
            scenario_data = child['scenario']
            scenario_steps: list[Step] = []

            for step_data in scenario_data['steps']:
                step = _generate_step(
                    step_data,
                    feature_path,
                    scenario_steps[-1] if scenario_steps else None,
                )
                scenario_steps.append(step)

            examples = []
            if 'examples' in scenario_data:
                examples = [
                    _generate_example(example, feature_path)
                    for example in scenario_data['examples']
                ]

            # El background en Gherkin siempre precede a los escenarios, por lo
            # que al llegar aqui ya conocemos su valor (None o Background).
            # Lo pasamos directamente al constructor para evitar el
            # dataclasses.replace() posterior que llamaria __post_init__ dos
            # veces por escenario.
            scenario = Scenario(
                id=scenario_data['id'],
                tags=_generate_tags(scenario_data['tags'], feature_path),
                location=Location(
                    **scenario_data['location'],
                    filename=feature_path,
                ),
                keyword=scenario_data['keyword'],
                name=scenario_data['name'],
                description=scenario_data['description'],
                steps=tuple(scenario_steps),
                examples=tuple(examples),
                background=background,
            )

            scenario_list.append(scenario)
        else:
            child_location = child.get('location', feature_data['location'])
            reason = f'Unsupported child: {child}'
            raise CosechaParserError(
                reason,
                feature_path,
                child_location['line'],
                child_location['column'],
            )

    return Feature(
        tags=feature_tags,
        location=feature_location,
        language=feature_data['language'],
        keyword=feature_data['keyword'],
        name=feature_data['name'],
        description=feature_data['description'],
        scenarios=tuple(scenario_list),
        background=background,
    )


def load_steps_from_module(
    module: ModuleType,
    step_registry: StepRegistry,
    function_names: tuple[str, ...] | None = None,
) -> None:
    step_registry.add_step_definitions(
        get_step_definitions_from_module(
            module,
            function_names=function_names,
        ),
    )


def get_step_definitions_from_module(
    module: ModuleType,
    function_names: tuple[str, ...] | None = None,
) -> tuple[Any, ...]:
    return _get_step_definitions_from_module(
        module,
        function_names=function_names,
    )


def _get_step_definitions_from_module(
    module: ModuleType,
    *,
    function_names: tuple[str, ...] | None,
) -> tuple[Any, ...]:
    module_file: str | None = getattr(module, '__file__', None)
    mtime_ns: int | None = None
    if module_file:
        with suppress(OSError):
            mtime_ns = Path(module_file).stat().st_mtime_ns

    cache_key = (module.__name__, module_file, mtime_ns)
    cached = _steps_cache.get(cache_key)
    if cached is None:
        cached = tuple(
            item.__step_definition__
            for item in vars(module).values()
            if hasattr(item, '__step_definition__')
        )
        _steps_cache[cache_key] = cached

    if function_names is None:
        return cached

    selected_definitions: list[Any] = []
    for function_name in function_names:
        function = getattr(module, function_name, None)
        if function is None:
            continue

        step_definition = getattr(function, '__step_definition__', None)
        if step_definition is None:
            continue

        selected_definitions.append(step_definition)

    return tuple(selected_definitions)


def import_step_modules(
    module_spec_or_path: str | Path,
) -> tuple[ModuleType, ...]:
    if isinstance(module_spec_or_path, Path):
        step_path = module_spec_or_path
        if step_path.is_file():
            if step_path.suffix != '.py':
                msg = f'{step_path} is incorrect'
                raise TypeError(msg)
            return (import_module_from_path(step_path),)

        return tuple(
            import_module_from_path(file_path)
            for file_path in step_path.rglob('*.py')
        )

    importlib.invalidate_caches()
    return (importlib.import_module(module_spec_or_path),)


def import_and_load_steps_from_module(
    module_spec_or_path: str | Path,
    step_registry: StepRegistry,
    function_names: tuple[str, ...] | None = None,
) -> None:
    for module in import_step_modules(module_spec_or_path):
        load_steps_from_module(
            module,
            step_registry,
            function_names=function_names,
        )
