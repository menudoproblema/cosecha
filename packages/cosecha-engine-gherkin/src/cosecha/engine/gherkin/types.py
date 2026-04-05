from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Coroutine
    from typing import Any, Literal

    from cosecha.engine.gherkin.models import Location, Row

type StepFn = Callable[..., Coroutine[Any, Any, None]]
type StepType = Literal['given', 'when', 'then', 'but', 'step']
type ParserFunction = Callable[[str, Location], Any]
type DatatableCoercions = dict[str, ParserFunction]
type CheckerFunction = Callable[[Any], bool]
type DatatableCheckers = dict[
    str,
    Callable[[Any, DatatableCoercions, Row], CheckerFunction],
]
