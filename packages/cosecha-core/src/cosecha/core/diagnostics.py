from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.config import Config
    from cosecha.core.console import Justify, Overflow
    from cosecha.core.types import ExcInfo


class DiagnosticSink(ABC):
    @abstractmethod
    def error(
        self,
        message: str,
        *,
        details: str | None = None,
        render_exception: bool = False,
        exc_info: ExcInfo | None = None,
        ignore_traceback: bool = False,
    ) -> None: ...

    @abstractmethod
    def trace(  # noqa: PLR0913
        self,
        *objects: Any,
        sep: str = ' ',
        end: str = '\n',
        style: str | None = None,
        justify: Justify | None = None,
        overflow: Overflow | None = None,
        no_wrap: bool | None = None,
        emoji: bool | None = None,
        markup: bool | None = None,
        highlight: bool | None = None,
        width: int | None = None,
        height: int | None = None,
        crop: bool = True,
        soft_wrap: bool | None = None,
        new_line_start: bool = False,
    ) -> None: ...


class ConsoleDiagnosticSink(DiagnosticSink):
    __slots__ = ('_config',)

    def __init__(self, config: Config) -> None:
        self._config = config

    def error(
        self,
        message: str,
        *,
        details: str | None = None,
        render_exception: bool = False,
        exc_info: ExcInfo | None = None,
        ignore_traceback: bool = False,
    ) -> None:
        console = self._config.console
        console.print(message, style='red')

        should_render_full_failures = getattr(
            console,
            'should_render_full_failures',
            None,
        )
        render_full_failures = (
            bool(should_render_full_failures())
            if callable(should_render_full_failures)
            else False
        )
        should_render_trace_diagnostics = getattr(
            console,
            'should_render_trace_diagnostics',
            None,
        )
        render_trace_diagnostics = (
            bool(should_render_trace_diagnostics())
            if callable(should_render_trace_diagnostics)
            else False
        )

        if render_exception and render_full_failures:
            console.print_exception(
                exc_info=exc_info,
                ignore_traceback=ignore_traceback,
            )

        if details is not None and render_trace_diagnostics:
            console.print(details, style='red')

    def trace(  # noqa: PLR0913
        self,
        *objects: Any,
        sep: str = ' ',
        end: str = '\n',
        style: str | None = None,
        justify: Justify | None = None,
        overflow: Overflow | None = None,
        no_wrap: bool | None = None,
        emoji: bool | None = None,
        markup: bool | None = None,
        highlight: bool | None = None,
        width: int | None = None,
        height: int | None = None,
        crop: bool = True,
        soft_wrap: bool | None = None,
        new_line_start: bool = False,
    ) -> None:
        self._config.console.trace(
            *objects,
            sep=sep,
            end=end,
            style=style,
            justify=justify,
            overflow=overflow,
            no_wrap=no_wrap,
            emoji=emoji,
            markup=markup,
            highlight=highlight,
            width=width,
            height=height,
            crop=crop,
            soft_wrap=soft_wrap,
            new_line_start=new_line_start,
        )
