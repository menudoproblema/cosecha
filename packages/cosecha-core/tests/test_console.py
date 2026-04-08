from __future__ import annotations

import io

import pytest

from cosecha.core.console import Console
from cosecha.core.output import OutputDetail, OutputMode


def test_console_mode_helpers_and_render_flags() -> None:
    summary = Console(output_mode=OutputMode.SUMMARY)
    live = Console(output_mode=OutputMode.LIVE)
    debug = Console(output_mode=OutputMode.DEBUG)
    trace = Console(output_mode=OutputMode.TRACE)

    assert summary.is_summary_mode() is True
    assert summary.should_render_collection_status() is True
    assert summary.should_render_run_status() is True
    assert summary.should_render_live_progress() is False
    assert live.should_render_live_progress() is True
    assert debug.is_debug_mode() is True
    assert trace.is_trace_mode() is True
    assert trace.should_render_trace_diagnostics() is True


def test_console_print_respects_quiet_and_debug_trace_modes() -> None:
    stream = io.StringIO()
    quiet = Console(output_mode=OutputMode.SUMMARY, quiet=True, file=stream)
    quiet.print('hidden')
    assert stream.getvalue() == ''

    debug_stream = io.StringIO()
    debug_console = Console(output_mode=OutputMode.DEBUG, file=debug_stream)
    debug_console.debug('debug-line')
    debug_console.trace('trace-hidden')
    assert 'debug-line' in debug_stream.getvalue()
    assert 'trace-hidden' not in debug_stream.getvalue()

    trace_stream = io.StringIO()
    trace_console = Console(output_mode=OutputMode.TRACE, file=trace_stream)
    trace_console.trace('trace-line')
    assert 'trace-line' in trace_stream.getvalue()


def test_console_print_summary_and_status_context() -> None:
    stream = io.StringIO()
    console = Console(file=stream)

    console.print_summary('Timing', 'line-a\nline-b')
    with console.status('running'):
        pass

    rendered = stream.getvalue()
    assert 'Timing' in rendered
    assert 'line-a' in rendered
    assert 'line-b' in rendered
    assert 'running' in rendered


def test_console_print_exception_respects_detail_and_flags() -> None:
    summary_stream = io.StringIO()
    summary_console = Console(
        output_mode=OutputMode.SUMMARY,
        output_detail=OutputDetail.STANDARD,
        file=summary_stream,
    )
    try:
        msg = 'boom'
        raise RuntimeError(msg)
    except RuntimeError:
        summary_console.print_exception(ignore_traceback=False)
    assert summary_stream.getvalue() == ''

    full_stream = io.StringIO()
    full_console = Console(
        output_mode=OutputMode.SUMMARY,
        output_detail=OutputDetail.FULL_FAILURES,
        file=full_stream,
    )
    try:
        msg = 'boom-full'
        raise RuntimeError(msg)
    except RuntimeError:
        full_console.print_exception(ignore_traceback=False)
    assert 'RuntimeError: boom-full' in full_stream.getvalue()

    ignored_stream = io.StringIO()
    ignored_console = Console(
        output_mode=OutputMode.TRACE,
        output_detail=OutputDetail.FULL_FAILURES,
        file=ignored_stream,
    )
    try:
        msg = 'boom-ignored'
        raise RuntimeError(msg)
    except RuntimeError:
        ignored_console.print_exception(ignore_traceback=True)
    assert ignored_stream.getvalue() == ''


def test_console_print_exception_no_exc_info_is_noop() -> None:
    stream = io.StringIO()
    console = Console(
        output_mode=OutputMode.TRACE,
        output_detail=OutputDetail.FULL_FAILURES,
        file=stream,
    )

    console.print_exception(exc_info=(None, None, None))

    assert stream.getvalue() == ''
