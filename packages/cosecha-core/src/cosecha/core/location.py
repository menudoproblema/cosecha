from __future__ import annotations

import contextlib
import inspect

from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable


class BaseLocation:
    __slots__ = ('column', 'filename', 'line', 'name')

    def __init__(
        self,
        filename: Path,
        line: int,
        column: int | None = None,
        name: str | None = None,
    ):
        self.filename = filename
        self.line = line
        self.column = column
        self.name = name

    def __hash__(self) -> int:
        return hash((self.filename, self.line, self.column, self.name))

    def __eq__(self, other: Location | Any) -> bool:
        if not isinstance(other, Location):
            return False
        return (self.filename, self.line) == (other.filename, other.line)

    def __str__(self) -> str:
        text = f'{self.filename}:{self.line}'
        if self.name:
            text = f'{text}::[{self.name}]'
        return text


class Location(BaseLocation):
    def relative_to(self, root: Path) -> Location:
        filename = self.filename
        with contextlib.suppress(Exception):
            filename = filename.relative_to(root)

        return Location(
            filename=filename,
            line=self.line,
            column=self.column,
            name=self.name,
        )

    def with_name(self, name: str) -> Location:
        return Location(
            filename=self.filename,
            line=self.line,
            column=self.column,
            name=name,
        )


class FunctionLocation(BaseLocation):
    __slots__ = ('func',)

    def __init__(self, func: Callable[..., Any] | partial[Any]) -> None:
        original_func = func
        if isinstance(func, partial):
            original_func = func.func

        while hasattr(original_func, '__wrapped__'):
            original_func = original_func.__wrapped__

        source_file = inspect.getsourcefile(original_func)

        if not source_file:
            msg = f'Cannot determine source file for function {func!r}'
            raise ValueError(msg)

        _, lnum = inspect.findsource(original_func)

        self.func = original_func

        super().__init__(
            Path(source_file).resolve(),
            lnum + 1,
            name=original_func.__qualname__,
        )

    def __repr__(self) -> str:
        return (
            f'<FunctionLocation: '
            f'func="{self.func.__qualname__}", '
            f'filename="{self.filename}", '
            f'line={self.line}>'
        )
