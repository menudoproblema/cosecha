from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.items import TestResultStatus
from cosecha.core.output import OutputMode
from cosecha.reporter.console import ConsoleReporter
from cosecha_internal.testkit import build_config, build_generic_report


@pytest.mark.asyncio
async def test_console_reporter_renders_failure_headline_body_and_summary(
    tmp_path,
) -> None:
    config = build_config(tmp_path, output_mode=OutputMode.SUMMARY)
    reporter = ConsoleReporter()
    reporter.initialize(config, SimpleNamespace(name='pytest'))

    await reporter.start()
    await reporter.add_test_result(
        build_generic_report(
            path='tests/test_demo.py',
            status=TestResultStatus.FAILED,
            duration=0.10,
            message='assertion failed',
            failure_kind='test',
            exception_text='AssertionError: boom',
            engine_name='pytest',
        ),
    )
    await reporter.print_report()

    assert config.console.printed_lines[0] == 'FAILED  tests/test_demo.py'
    assert 'Failure kind: test' in config.console.printed_lines[1]
    assert 'assertion failed' in config.console.printed_lines[2]
    assert config.console.summaries == [
        ('pytest', 'Tests (1):, 0 passed, 1 failed'),
    ]


@pytest.mark.asyncio
async def test_console_reporter_renders_success_in_live_mode(
    tmp_path,
) -> None:
    config = build_config(tmp_path, output_mode=OutputMode.LIVE)
    reporter = ConsoleReporter()
    reporter.initialize(config, SimpleNamespace(name='pytest'))

    await reporter.start()
    await reporter.add_test_result(
        build_generic_report(
            path='tests/test_ok.py',
            status=TestResultStatus.PASSED,
            duration=0.01,
            engine_name='pytest',
        ),
    )

    assert config.console.printed_lines == ['PASSED  tests/test_ok.py']
