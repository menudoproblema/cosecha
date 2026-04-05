from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from cosecha.core.location import Location
from cosecha.engine.gherkin.types import StepType


@dataclass(frozen=True)
class Tag:
    id: str
    location: Location
    name: str


@dataclass(frozen=True)
class HeadingCell:
    location: Location
    name: str
    coerce: str | None = field(init=False)

    def __post_init__(self):
        coerce = None

        if '::' in self.name:
            name, coerce = self.name.split('::', maxsplit=1)

            object.__setattr__(self, 'name', name)

        object.__setattr__(self, 'coerce', coerce)


@dataclass(frozen=True)
class Heading:
    id: str
    location: Location
    cells: tuple[HeadingCell, ...] = field(default_factory=tuple)

    def __post_init__(self):
        object.__setattr__(self, 'cells', tuple(self.cells))

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(cell.name for cell in self.cells)


@dataclass(frozen=True)
class Cell:
    location: Location
    heading: HeadingCell
    value: str | None

    def __post_init__(self):
        # Convertimos '' en None
        object.__setattr__(self, 'value', self.value or None)


@dataclass(frozen=True)
class Row:
    id: str
    location: Location
    cells: tuple[Cell, ...] = field(default_factory=tuple)

    def __post_init__(self):
        object.__setattr__(self, 'cells', tuple(self.cells))

    def __getitem__(self, key: str) -> Any:
        for cell in self.cells:
            if cell.heading.name == key:
                return cell.value

        msg = f'Unknown field "{key}" in row "{self.location}"'
        raise KeyError(msg)

    def get_values(
        self,
        coercions: dict[str, Callable[[str, Location], Any]] | None = None,
        normalize: Callable[[str | None], Any] | None = None,
    ) -> dict[str, Any]:
        coerced_values: dict[str, Any] = {}

        coercions = coercions or {}

        for cell in self.cells:
            field_name = cell.heading.name
            value = cell.value

            if normalize:
                value = normalize(value)

            if value is not None and cell.heading.coerce:
                coerce = coercions.get(cell.heading.coerce, None)
                if not coerce:
                    msg = (
                        f'Unknown coercion "{cell.heading.coerce}" '
                        f'in "{cell.location}"'
                    )
                    raise TypeError(msg)

                value = coerce(value, self.location)

            coerced_values[field_name] = value

        return coerced_values


@dataclass(frozen=True)
class DataTable:
    location: Location
    heading: Heading
    rows: tuple[Row, ...] = field(default_factory=tuple)

    def __post_init__(self):
        object.__setattr__(self, 'rows', tuple(self.rows))


@dataclass(frozen=True)
class Step:
    id: str
    location: Location
    keyword: str
    keyword_type: str
    step_type: StepType
    text: str
    table: DataTable | None = None


@dataclass(frozen=True)
class Background:
    id: str
    location: Location
    keyword: str
    name: str
    description: str
    steps: tuple[Step, ...] = field(default_factory=tuple)

    def __post_init__(self):
        object.__setattr__(self, 'steps', tuple(self.steps))


@dataclass(frozen=True)
class Example:
    id: str
    location: Location
    name: str
    description: str
    keyword: str
    heading: Heading
    rows: tuple[Row, ...] = field(default_factory=tuple)
    tags: tuple[Tag, ...] = field(default_factory=tuple)

    def __post_init__(self):
        object.__setattr__(self, 'rows', tuple(self.rows))
        object.__setattr__(self, 'tags', tuple(self.tags))


@dataclass(frozen=True)
class Scenario:
    id: str
    location: Location
    keyword: str
    name: str
    description: str
    steps: tuple[Step, ...] = field(default_factory=tuple)
    examples: tuple[Example, ...] = field(default_factory=tuple)
    tags: tuple[Tag, ...] = field(default_factory=tuple)
    # NOTE: Heredado de Feature
    background: Background | None = None

    def __post_init__(self):
        object.__setattr__(self, 'steps', tuple(self.steps))
        object.__setattr__(self, 'examples', tuple(self.examples))
        object.__setattr__(self, 'tags', tuple(self.tags))
        # Precalculamos all_steps una sola vez: Scenario es frozen y nunca
        # cambia, así que no tiene sentido reconstruir el tuple en cada acceso.
        # dataclasses.replace() llama a __post_init__ con el nuevo background,
        # así que el cache siempre refleja el estado correcto.
        bg = self.background.steps if self.background else ()
        object.__setattr__(self, '_all_steps_cache', bg + self.steps)

    @property
    def all_steps(self) -> tuple[Step, ...]:
        return self._all_steps_cache  # type: ignore[attr-defined]


@dataclass(frozen=True)
class Feature:
    location: Location
    language: str
    keyword: str
    name: str
    description: str
    background: Background | None = None
    scenarios: tuple[Scenario, ...] = field(default_factory=tuple)
    tags: tuple[Tag, ...] = field(default_factory=tuple)

    def __post_init__(self):
        object.__setattr__(self, 'scenarios', tuple(self.scenarios))
        object.__setattr__(self, 'tags', tuple(self.tags))
