from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.console_rendering import (
    CodeBlockComponent,
    ExtensionComponent,
    LineComponent,
    SectionComponent,
    StatusBadge,
    TableComponent,
    TextSpan,
)
from cosecha.core.items import TestResultStatus
from cosecha.core.output import OutputDetail, OutputMode
from cosecha.reporter.console import (
    ConsoleOutputPipeline,
    ConsoleReporter,
    OutputCaseEvent,
)
from cosecha_internal.testkit import build_config, build_generic_report


class _RichPresenter:
    contribution_name = 'rich'

    @classmethod
    def build_case_title(cls, _report, *, config):
        del config
        return (TextSpan('rich', emphatic=True),)

    @classmethod
    def build_console_components(cls, _report, *, config):
        del config
        return (
            LineComponent(),
            LineComponent(spans=(TextSpan('line'),)),
            LineComponent(
                spans=(TextSpan('with-badge'),),
                badge=StatusBadge('OK'),
                indent=1,
            ),
            StatusBadge('META'),
            CodeBlockComponent(title='Code', code='alpha\nbeta'),
            TableComponent(
                title='Table',
                columns=('k', 'v'),
                rows=(('a', '1'),),
            ),
            SectionComponent(
                title='Section',
                children=(LineComponent(spans=(TextSpan('child'),)),),
            ),
            ExtensionComponent(
                component_type='x',
                fallback=(LineComponent(spans=(TextSpan('fallback'),)),),
            ),
        )


class _EmptyTitlePresenter:
    contribution_name = 'empty'

    @classmethod
    def build_case_title(cls, _report, *, config):
        del config
        return ()

    @classmethod
    def build_console_components(cls, _report, *, config):
        del config
        return ()


@pytest.mark.asyncio
async def test_console_reporter_contract_add_test_and_summary_segments(
    tmp_path,
) -> None:
    reporter = ConsoleReporter()
    config = build_config(tmp_path, output_mode=OutputMode.SUMMARY)
    reporter.initialize(config, SimpleNamespace(name='pytest'))

    assert ConsoleReporter.reporter_name() == 'console'
    assert ConsoleReporter.reporter_output_kind() == 'console'
    capability_names = {
        descriptor.name
        for descriptor in ConsoleReporter.describe_capabilities()
    }
    assert 'report_lifecycle' in capability_names

    await reporter.start()
    await reporter.add_test(object())

    for status in (
        TestResultStatus.PASSED,
        TestResultStatus.FAILED,
        TestResultStatus.ERROR,
        TestResultStatus.SKIPPED,
        TestResultStatus.PENDING,
    ):
        await reporter.add_test_result(
            build_generic_report(
                path='tests/test_demo.py',
                status=status,
                duration=0.01,
                engine_name='pytest',
                message='demo message',
            ),
        )

    await reporter.print_report()

    _engine_name, summary = config.console.summaries[0]
    assert 'Tests (5):' in summary
    assert '1 errors' in summary
    assert '1 skipped' in summary
    assert '1 pending' in summary


def test_console_pipeline_renders_full_and_trace_branches(tmp_path) -> None:
    trace_config = build_config(
        tmp_path,
        output_mode=OutputMode.TRACE,
        output_detail=OutputDetail.FULL_FAILURES,
    )
    trace_pipeline = ConsoleOutputPipeline(trace_config.console)
    failure_event = OutputCaseEvent(
        title='trace case',
        status=TestResultStatus.FAILED,
        compact_lines=('compact line',),
        full_lines=('full line',),
        trace_lines=('trace line',),
    )
    trace_pipeline.render_case(failure_event)

    assert trace_config.console.printed_lines == [
        'FAILED  trace case',
        '  full line',
        '  trace line',
    ]

    live_config = build_config(tmp_path, output_mode=OutputMode.LIVE)
    live_pipeline = ConsoleOutputPipeline(live_config.console)
    live_pipeline.render_case(failure_event)
    assert live_config.console.printed_lines == [
        'FAILED  trace case',
        '  compact line',
    ]


@pytest.mark.asyncio
async def test_console_reporter_presenter_paths_and_render_helpers(
    tmp_path,
) -> None:
    reporter = ConsoleReporter()
    reporter.initialize(build_config(tmp_path), SimpleNamespace(name='engine'))
    await reporter.start()
    reporter._presenters = {
        'rich': _RichPresenter,
        'empty': _EmptyTitlePresenter,
    }

    rich_report = build_generic_report(
        path='tests/test_rich.py',
        status=TestResultStatus.FAILED,
        duration=0.01,
        exception_text='RuntimeError: rich boom',
        engine_name='rich',
    )
    assert reporter._build_case_title(rich_report) == '*rich*'

    compact_lines, full_lines = reporter._build_case_lines(rich_report)
    assert any(line == '[META]' for line in compact_lines)
    assert any('Code' in line for line in compact_lines)
    assert any('k | v' in line for line in compact_lines)
    assert any('*Section*' in line for line in compact_lines)
    assert full_lines[-1] == 'RuntimeError: rich boom'

    empty_title_report = build_generic_report(
        path='tests/test_empty.py',
        status=TestResultStatus.FAILED,
        duration=0.01,
        engine_name='empty',
    )
    assert (
        reporter._build_case_title(empty_title_report)
        == 'tests/test_empty.py'
    )

    unknown_report = build_generic_report(
        path='tests/test_unknown.py',
        status=TestResultStatus.FAILED,
        duration=0.01,
        engine_name='missing',
    )
    assert (
        reporter._build_case_title(unknown_report)
        == 'tests/test_unknown.py'
    )

    assert reporter._render_line(LineComponent(), depth=0) is None
    assert reporter._render_badge(StatusBadge('PASS')) == '[PASS]'
    assert reporter._render_spans(
        (TextSpan('x', emphatic=True), TextSpan('y')),
    ) == '*x*y'
