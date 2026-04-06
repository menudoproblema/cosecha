from __future__ import annotations

import xml.etree.ElementTree as ET

from types import SimpleNamespace

import pytest

from cosecha.core.items import TestResultStatus
from cosecha.reporter.junit import JUnitReporter
from cosecha_internal.testkit import (
    build_config,
    build_generic_report,
    build_gherkin_report,
)


SUITE_COUNT = 2


@pytest.mark.asyncio
async def test_junit_reporter_writes_grouped_suites_and_failures(
    tmp_path,
) -> None:
    output_path = tmp_path / 'reports' / 'junit.xml'
    reporter = JUnitReporter(output_path)
    reporter.initialize(
        build_config(tmp_path),
        SimpleNamespace(name='gherkin'),
    )

    await reporter.start()
    await reporter.add_test_result(
        build_gherkin_report(
            path='features/demo.feature',
            status=TestResultStatus.FAILED,
            scenario_name='Escenario demo',
            feature_name='Feature demo',
            scenario_line=4,
            duration=0.20,
            message='failed step',
            exception_text='AssertionError: boom',
            step_results=(
                {
                    'status': 'failed',
                    'message': 'failed step',
                    'step': {
                        'keyword': 'When ',
                        'text': 'something happens',
                        'location': {
                            'text': 'features/demo.feature',
                            'line': 4,
                        },
                        'implementation_location': None,
                    },
                    'exception_text': 'AssertionError: boom',
                },
            ),
        ),
    )
    await reporter.add_test_result(
        build_generic_report(
            path='tests/test_demo.py',
            status=TestResultStatus.SKIPPED,
            duration=0.05,
            message='not applicable',
            engine_name='pytest',
        ),
    )
    await reporter.print_report()

    root = ET.fromstring(  # noqa: S314
        output_path.read_text(encoding='utf-8'),
    )
    suites = root.findall('testsuite')

    assert root.tag == 'testsuites'
    assert len(suites) == SUITE_COUNT

    gherkin_suite = next(
        suite
        for suite in suites
        if suite.attrib['name'] == 'features/demo.feature'
    )
    gherkin_case = gherkin_suite.find('testcase')
    assert gherkin_suite.attrib['failures'] == '1'
    assert gherkin_case is not None
    assert gherkin_case.attrib['classname'] == 'Feature demo'
    assert gherkin_case.attrib['name'] == 'Escenario demo'
    assert gherkin_case.find('failure') is not None
    assert 'FAILED: When something happens' in (
        gherkin_case.findtext('system-out') or ''
    )

    pytest_suite = next(
        suite
        for suite in suites
        if suite.attrib['name'] == 'tests/test_demo.py'
    )
    pytest_case = pytest_suite.find('testcase')
    assert pytest_suite.attrib['skipped'] == '1'
    assert pytest_case is not None
    assert pytest_case.find('skipped') is not None
