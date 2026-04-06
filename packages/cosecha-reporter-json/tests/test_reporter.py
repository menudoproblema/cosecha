from __future__ import annotations

import json

from types import SimpleNamespace

import pytest

from cosecha.core.items import TestResultStatus
from cosecha.reporter.json import JsonReporter
from cosecha_internal.testkit import (
    NullTelemetryStream,
    build_config,
    build_generic_report,
    build_gherkin_report,
)


TOTAL_TESTS = 2
SCENARIO_LINE = 8


@pytest.mark.asyncio
async def test_json_reporter_writes_summary_and_gherkin_fields(
    tmp_path,
) -> None:
    output_path = tmp_path / 'reports' / 'report.json'
    reporter = JsonReporter(output_path)
    reporter.initialize(
        build_config(tmp_path),
        SimpleNamespace(name='gherkin'),
    )

    await reporter.start()
    await reporter.add_test_result(
        build_generic_report(
            path='tests/test_demo.py',
            status=TestResultStatus.PASSED,
            duration=0.12,
            engine_name='pytest',
        ),
    )
    await reporter.add_test_result(
        build_gherkin_report(
            path='features/demo.feature',
            status=TestResultStatus.FAILED,
            scenario_name='Escenario demo',
            feature_name='Feature demo',
            scenario_line=SCENARIO_LINE,
            duration=0.34,
            message='step failed',
            exception_text='Traceback demo',
        ),
    )
    await reporter.print_report()

    payload = json.loads(output_path.read_text(encoding='utf-8'))

    assert payload['reporter'] == 'json'
    assert payload['engine_name'] == 'gherkin'
    assert payload['summary']['total_tests'] == TOTAL_TESTS
    assert payload['summary']['status_counts']['passed'] == 1
    assert payload['summary']['status_counts']['failed'] == 1
    assert payload['tests'][1]['scenario_name'] == 'Escenario demo'
    assert payload['tests'][1]['feature_name'] == 'Feature demo'
    assert payload['tests'][1]['line'] == SCENARIO_LINE


@pytest.mark.asyncio
async def test_json_reporter_emits_telemetry_span_on_output(
    tmp_path,
) -> None:
    output_path = tmp_path / 'report.json'
    reporter = JsonReporter(output_path)
    telemetry_stream = NullTelemetryStream()
    reporter.initialize(build_config(tmp_path), SimpleNamespace(name='pytest'))
    reporter.bind_telemetry_stream(telemetry_stream)

    await reporter.start()
    await reporter.add_test_result(
        build_generic_report(
            path='tests/test_demo.py',
            status=TestResultStatus.SKIPPED,
            duration=0.01,
            engine_name='pytest',
        ),
    )
    await reporter.print_report()

    assert telemetry_stream.spans == [
        (
            'reporter.output.write',
            {
                'cosecha.reporter.name': 'json',
                'cosecha.reporter.output_kind': 'structured',
            },
        ),
    ]
