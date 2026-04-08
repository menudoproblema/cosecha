from __future__ import annotations

import xml.etree.ElementTree as ET

from types import SimpleNamespace

import pytest

from cosecha.core.items import TestResultStatus
from cosecha.reporter.junit import JUnitReporter
from cosecha_internal.testkit import (
    NullTelemetryStream,
    build_config,
    build_generic_report,
)


@pytest.mark.asyncio
async def test_junit_reporter_contract_start_cleanup_and_add_test(
    tmp_path,
) -> None:
    output_path = tmp_path / 'reports' / 'junit.xml'
    reporter = JUnitReporter(output_path)
    reporter.initialize(build_config(tmp_path), SimpleNamespace(name='pytest'))

    assert JUnitReporter.reporter_name() == 'junit'
    assert JUnitReporter.reporter_output_kind() == 'structured'
    capability_names = {
        descriptor.name
        for descriptor in JUnitReporter.describe_capabilities()
    }
    assert 'report_lifecycle' in capability_names

    reporter._temp_dir.mkdir(parents=True, exist_ok=True)
    stale_fragment = reporter._temp_dir / 'stale.xml'
    stale_fragment.write_text('<case/>', encoding='utf-8')

    await reporter.start()
    assert reporter._temp_dir.exists()
    assert not stale_fragment.exists()

    await reporter.add_test(object())


@pytest.mark.asyncio
async def test_junit_reporter_error_case_and_telemetry_branch(
    tmp_path,
) -> None:
    output_path = tmp_path / 'junit.xml'
    reporter = JUnitReporter(output_path)
    telemetry_stream = NullTelemetryStream()
    reporter.initialize(build_config(tmp_path), SimpleNamespace(name='pytest'))
    reporter.bind_telemetry_stream(telemetry_stream)

    await reporter.start()
    await reporter.add_test_result(
        build_generic_report(
            path='tests/test_error.py',
            status=TestResultStatus.ERROR,
            duration=0.02,
            message='runtime crash',
            exception_text='Traceback: boom',
            engine_name='pytest',
        ),
    )
    await reporter.print_report()

    root = ET.fromstring(output_path.read_text(encoding='utf-8'))  # noqa: S314
    suites = root.findall('testsuite')
    assert len(suites) == 1

    suite = suites[0]
    case = suite.find('testcase')
    assert suite.attrib['errors'] == '1'
    assert case is not None

    error = case.find('error')
    assert error is not None
    assert error.attrib['message'] == 'runtime crash'
    assert error.text == 'Traceback: boom'
    assert telemetry_stream.spans == [
        (
            'reporter.output.write',
            {
                'cosecha.reporter.name': 'junit',
                'cosecha.reporter.output_kind': 'structured',
            },
        ),
    ]
