from __future__ import annotations

import sys
import traceback

from contextlib import AbstractContextManager
from typing import IO, Any, Literal

from cosecha.core.output import OutputDetail, OutputMode


type ColorSystem = Literal['auto', 'standard', '256', 'truecolor', 'windows']
type Justify = Literal['default', 'left', 'center', 'right', 'full']
type Overflow = Literal['fold', 'crop', 'ellipsis', 'ignore']


class _StatusContext(AbstractContextManager[None]):
    __slots__ = ('_console', '_message')

    def __init__(self, console: Console, message: str) -> None:
        self._console = console
        self._message = message

    def __enter__(self) -> None:
        self._console.print(self._message)

    def __exit__(self, exc_type, exc, exc_traceback) -> None:
        del exc_type, exc, exc_traceback


class Console:
    __slots__ = (
        'file',
        'height',
        'output_detail',
        'output_mode',
        'quiet',
        'stderr',
        'width',
    )

    def __init__(  # noqa: PLR0913
        self,
        *,
        output_mode: OutputMode = OutputMode.SUMMARY,
        output_detail: OutputDetail = OutputDetail.STANDARD,
        color_system: ColorSystem | None = 'auto',
        force_terminal: bool | None = None,
        force_jupyter: bool | None = None,
        force_interactive: bool | None = None,
        soft_wrap: bool = False,
        theme: object | None = None,
        stderr: bool = False,
        file: IO[str] | None = None,
        quiet: bool = False,
        width: int | None = None,
        height: int | None = None,
        style: str | None = None,
        no_color: bool | None = None,
        tab_size: int = 8,
        record: bool = False,
        markup: bool = True,
        emoji: bool = True,
        emoji_variant: None | Literal['emoji', 'text'] = None,
        highlight: bool = True,
        log_time: bool = True,
        log_path: bool = True,
        log_time_format: object = '[%X]',
        highlighter: object | None = None,
        legacy_windows: bool | None = None,
        safe_box: bool = True,
        get_datetime: object | None = None,
        get_time: object | None = None,
        _environ: object | None = None,
    ) -> None:
        del (
            color_system,
            emoji,
            emoji_variant,
            force_interactive,
            force_jupyter,
            force_terminal,
            get_datetime,
            get_time,
            highlighter,
            highlight,
            legacy_windows,
            log_path,
            log_time,
            log_time_format,
            markup,
            no_color,
            record,
            safe_box,
            soft_wrap,
            style,
            tab_size,
            theme,
            _environ,
        )
        self.output_mode = output_mode
        self.output_detail = output_detail
        self.stderr = stderr
        self.file = file
        self.quiet = quiet
        self.width = width
        self.height = height

    def is_summary_mode(self) -> bool:
        return self.output_mode == OutputMode.SUMMARY

    def is_live_mode(self) -> bool:
        return self.output_mode == OutputMode.LIVE

    def is_debug_mode(self) -> bool:
        return self.output_mode == OutputMode.DEBUG

    def is_trace_mode(self) -> bool:
        return self.output_mode == OutputMode.TRACE

    def should_render_collection_status(self) -> bool:
        return self.is_summary_mode()

    def should_render_run_status(self) -> bool:
        return self.is_summary_mode()

    def should_render_live_progress(self) -> bool:
        return self.is_live_mode()

    def should_render_full_failures(self) -> bool:
        return (
            self.output_detail == OutputDetail.FULL_FAILURES
            or self.is_debug_mode()
            or self.is_trace_mode()
        )

    def should_render_trace_diagnostics(self) -> bool:
        return self.is_trace_mode()

    def print(  # noqa: PLR0913
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
        del (
            crop,
            emoji,
            height,
            highlight,
            justify,
            markup,
            new_line_start,
            no_wrap,
            overflow,
            soft_wrap,
            style,
            width,
        )
        if self.quiet:
            return
        target = self.file
        if target is None:
            target = sys.stderr if self.stderr else sys.stdout
        rendered = sep.join(str(object_) for object_ in objects)
        target.write(rendered + end)
        target.flush()

    def info(self, *objects: Any, **kwargs: Any) -> None:
        self.print(*objects, **kwargs)

    def debug(self, *objects: Any, **kwargs: Any) -> None:
        if self.is_debug_mode() or self.is_trace_mode():
            self.print(*objects, **kwargs)

    def trace(self, *objects: Any, **kwargs: Any) -> None:
        if self.is_trace_mode():
            self.print(*objects, **kwargs)

    def print_summary(self, title: str, text: str) -> None:
        body_lines = str(text).splitlines() or ('',)
        width = max(len(title) + 10, *(len(line) for line in body_lines))
        top = f'+- {title} ' + '-' * max(width - len(title) - 4, 0) + '+'
        self.print('')
        self.print(top)
        for line in body_lines:
            self.print(f'| {line.ljust(width)} |')
        self.print('+' + '-' * (width + 2) + '+')

    def print_exception(  # noqa: PLR0913
        self,
        *,
        exc_info=None,
        width: int | None = None,
        extra_lines: int = 3,
        theme: str | None = None,
        word_wrap: bool = False,
        show_locals: bool = False,
        suppress=(),
        max_frames: int = 100,
        ignore_traceback: bool = False,
    ) -> None:
        del (
            extra_lines,
            max_frames,
            show_locals,
            suppress,
            theme,
            width,
            word_wrap,
        )
        if ignore_traceback or not self.should_render_full_failures():
            return

        exc_type, exc_value, exc_traceback = (
            exc_info or sys.exc_info()
        )
        if exc_type is None:
            return

        for line in traceback.format_exception(
            exc_type,
            exc_value,
            exc_traceback,
        ):
            self.print(line.rstrip('\n'))

    def status(
        self,
        message: str,
        *,
        spinner: str | None = None,
        transient: bool | None = None,
    ) -> _StatusContext:
        del spinner, transient
        return _StatusContext(self, message)


__all__ = (
    'ColorSystem',
    'Console',
    'Justify',
    'Overflow',
)
