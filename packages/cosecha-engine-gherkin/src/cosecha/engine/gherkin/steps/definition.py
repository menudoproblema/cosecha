from __future__ import annotations

import re

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import parse

from cosecha.core.location import FunctionLocation


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from cosecha.engine.gherkin.types import StepFn, StepType


class MatchException(Exception): ...  # noqa: N818


class MatchError(MatchException): ...


_PLACEHOLDER_PATTERN = re.compile(r'\{[^}]*\}')


def _extract_literal_fragments(text: str) -> tuple[str, ...]:
    return tuple(
        fragment.strip()
        for fragment in _PLACEHOLDER_PATTERN.split(text)
        if fragment.strip()
    )


class Argument:
    __slots__ = ('end_column', 'name', 'start_column', 'value')

    def __init__(
        self,
        name: str,
        value: str,
        start_column: int,
        end_column: int,
    ):
        self.name = name
        self.value = value
        self.start_column = start_column
        self.end_column = end_column

    def __repr__(self) -> str:
        return f'<Argument {self.name}, {self.value}>'


class Match:
    __slots__ = ('arguments', 'step_definition', 'step_text')

    def __init__(
        self,
        step_definition: StepDefinition,
        step_text: StepText,
        arguments: Iterable[Argument] | None = None,
    ):
        self.step_definition = step_definition
        self.step_text = step_text
        self.arguments = tuple(arguments or [])

    def __repr__(self) -> str:
        func = self.step_definition.func
        return f'<Match {func.__qualname__}, arguments={self.arguments}>'


class StepMatcher(ABC):
    __slots__ = ('step_text_list',)

    def __init__(self, step_text_list: tuple[StepText, ...]) -> None:
        self.step_text_list = step_text_list

    @abstractmethod
    def parse_arguments(
        self,
        text: str,
    ) -> tuple[StepText, list[Argument]] | None: ...


class ParseStepMatcher(StepMatcher):
    __slots__ = ('_parser_list',)

    def __init__(self, step_text_list: tuple[StepText, ...]) -> None:
        super().__init__(step_text_list)
        self._parser_list = tuple(
            (step_text, parse.compile(step_text.text))
            for step_text in step_text_list
        )

        for _, parser in self._parser_list:
            if parser.fixed_fields:
                msg = f'Fixed fields is not supported: {parser.fixed_fields}'
                raise NotImplementedError(msg)

    def parse_arguments(
        self,
        text: str,
    ) -> tuple[StepText, list[Argument]] | None:
        for step_text, parser in self._parser_list:
            if result := parser.parse(text):
                return (
                    step_text,
                    [
                        Argument(
                            name,
                            value,
                            result.spans[name][0],
                            result.spans[name][1],
                        )
                        for name, value in result.named.items()
                    ],
                )

        return None


class LayoutRef:
    __slots__ = ('layout', 'place_holder')

    def __init__(self, layout: str, place_holder: str) -> None:
        self.layout = layout
        self.place_holder = place_holder


class StepText:
    __slots__ = (
        'can_use_table',
        'layouts',
        'literal_fragments',
        'literal_prefix',
        'min_table_rows',
        'params',
        'required_table_rows',
        'text',
    )

    def __init__(
        self,
        text: str,
        *,
        layouts: LayoutRef | Iterable[LayoutRef] = (),
        min_table_rows: int | None = None,
        required_table_rows: int | None = None,
        can_use_table: bool | None = None,
        **params: Any,
    ) -> None:
        # Validación de incompatibilidad
        if (
            min_table_rows is not None
            and required_table_rows is not None
            and min_table_rows > 0
            and required_table_rows > 0
        ):
            msg = (
                'Incompatible options: "min_table_rows" and '
                '"required_table_rows" cannot both be non-zero in '
                f'step "{text}"'
            )
            raise ValueError(msg)

        if can_use_table is None:
            can_use_table = bool(min_table_rows or required_table_rows)

        # Validación de obligatoriedad de la tabla
        if not can_use_table and (min_table_rows or required_table_rows):
            msg = (
                'Cannot use table, but row requirements are '
                f'given in step "{text}"'
            )
            raise ValueError(msg)

        self.text = text
        self.min_table_rows = min_table_rows
        self.required_table_rows = required_table_rows
        self.can_use_table = can_use_table
        self.layouts = tuple(
            [layouts] if isinstance(layouts, LayoutRef) else layouts,
        )
        self.params = params

        # Calculamos la parte literal hasta el primer parámetro '{}'
        # o hasta el final si es puramente estático.
        bracket_index = text.find('{')
        if bracket_index == -1:
            self.literal_prefix = text
        else:
            self.literal_prefix = text[:bracket_index]
        self.literal_fragments = _extract_literal_fragments(text)


class StepDefinition:
    __slots__ = (
        '_location',
        'category',
        'func',
        'parser',
        'step_text_list',
        'step_type',
    )

    def __init__(
        self,
        step_type: StepType,
        step_text_list: Iterable[StepText],
        func: StepFn,
        *,
        parser_cls: type[StepMatcher] | None = None,
        category: str | None = None,
    ):
        self.step_type: StepType = step_type
        self.step_text_list = tuple(step_text_list)
        self.func = func
        self._location: FunctionLocation | None = None

        current_parser_cls = parser_cls or ParseStepMatcher
        self.parser: StepMatcher = current_parser_cls(self.step_text_list)
        self.category = category

    @property
    def location(self) -> FunctionLocation:
        if self._location is None:
            self._location = FunctionLocation(self.func)

        return self._location

    def match(self, step_text: str) -> Match | None:
        try:
            result = self.parser.parse_arguments(step_text)
        except Exception as e:
            raise MatchError(self.func, e) from e

        return None if result is None else Match(self, *result)

    def __repr__(self):
        return (
            f'<{self.__class__.__name__}: {self.step_text_list[0].text} '
            f'{self.location}>'
        )

    def __str__(self):
        return (
            f'@{self.step_type}("{self.step_text_list}") '
            f'{self.func.__qualname__}... at {self.location}>'
        )

    def __eq__(self, other: StepDefinition | Any) -> bool:
        if not isinstance(other, StepDefinition):
            return False

        return (self.step_type, self.step_text_list, self.func) == (
            other.step_type,
            other.step_text_list,
            other.func,
        )

    def __hash__(self) -> int:
        return hash((self.step_type, self.step_text_list, self.func))
