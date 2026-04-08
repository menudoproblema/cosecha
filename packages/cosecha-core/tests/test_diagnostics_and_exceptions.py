from __future__ import annotations

from pathlib import Path

import pytest

from cosecha.core.diagnostics import ConsoleDiagnosticSink
from cosecha.core.exceptions import (
    CosechaException,
    CosechaParserError,
    Skipped,
)


class _FakeConsole:
    def __init__(self) -> None:
        self.print_calls: list[
            tuple[tuple[object, ...], dict[str, object]]
        ] = []
        self.trace_calls: list[
            tuple[tuple[object, ...], dict[str, object]]
        ] = []
        self.print_exception_calls: list[dict[str, object]] = []

    def print(
        self,
        *objects: object,
        **kwargs: object,
    ) -> None:
        self.print_calls.append((objects, kwargs))

    def trace(self, *objects: object, **kwargs: object) -> None:
        self.trace_calls.append((objects, kwargs))

    def print_exception(self, **kwargs: object) -> None:
        self.print_exception_calls.append(kwargs)


class _FakeConsoleWithFlags(_FakeConsole):
    def __init__(self) -> None:
        super().__init__()
        self._should_render_full_failures = False
        self._should_render_trace_diagnostics = False

    def should_render_full_failures(self) -> bool:
        return self._should_render_full_failures

    def should_render_trace_diagnostics(self) -> bool:
        return self._should_render_trace_diagnostics


class _FakeConfig:
    def __init__(self, console: _FakeConsole) -> None:
        self.console = console


def test_console_diagnostic_sink_reports_messages_and_options() -> None:
    console = _FakeConsole()
    sink = ConsoleDiagnosticSink(_FakeConfig(console))

    sink.error('an error')

    assert console.print_calls == [
        (('an error',), {'style': 'red'}),
    ]
    assert console.trace_calls == []
    assert console.print_exception_calls == []


def test_console_diagnostic_sink_forwards_full_trace_and_details_when_enabled() -> (
    None
):
    console = _FakeConsoleWithFlags()
    console._should_render_full_failures = True
    console._should_render_trace_diagnostics = True
    sink = ConsoleDiagnosticSink(_FakeConfig(console))

    sink.error(
        'oops',
        details='trace details',
        render_exception=True,
        exc_info=('exc',),
        ignore_traceback=True,
    )

    assert console.print_calls == [
        (('oops',), {'style': 'red'}),
        (('trace details',), {'style': 'red'}),
    ]
    assert console.print_exception_calls == [
        {
            'exc_info': ('exc',),
            'ignore_traceback': True,
        },
    ]


def test_console_diagnostic_sink_forwards_trace_call() -> None:
    console = _FakeConsole()
    sink = ConsoleDiagnosticSink(_FakeConfig(console))

    sink.trace(
        'hello',
        'world',
        sep='|',
        style='blue',
        new_line_start=True,
    )

    assert console.trace_calls == [
        (
            (
                'hello',
                'world',
            ),
            {
                'sep': '|',
                'end': '\n',
                'style': 'blue',
                'justify': None,
                'overflow': None,
                'no_wrap': None,
                'emoji': None,
                'markup': None,
                'highlight': None,
                'width': None,
                'height': None,
                'crop': True,
                'soft_wrap': None,
                'new_line_start': True,
            },
        ),
    ]


def test_skipped_exception_stores_reason() -> None:
    error = Skipped('no reason')

    assert error.reason == 'no reason'
    assert str(error) == 'no reason'


def test_parser_error_builds_spanish_diagnostic_message() -> None:
    path = Path('/tmp/tests/demo.py')
    error = CosechaParserError('bad grammar', path, line=12, column=4)

    assert isinstance(error, CosechaException)
    assert error.reason == 'bad grammar'
    assert error.filename == path
    assert error.line == 12
    assert error.column == 4
    assert str(error) == 'Error en /tmp/tests/demo.py:12:4 - bad grammar'


@pytest.mark.parametrize(
    'reason',
    (None, ''),
)
def test_skipped_error_keeps_optional_reason(reason: str | None) -> None:
    error = Skipped(reason or '')
    assert error.reason == (reason or '')
