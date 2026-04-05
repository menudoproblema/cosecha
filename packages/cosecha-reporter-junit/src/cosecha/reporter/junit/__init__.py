from __future__ import annotations

import importlib
import shutil
import time
import xml.etree.ElementTree as ET

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from xml.sax.saxutils import quoteattr

from cosecha.core.items import TestResultStatus
from cosecha.core.reporter import Reporter
from cosecha.core.reporting_ir import TestReport, ensure_test_report


if TYPE_CHECKING:
    from pathlib import Path

    from cosecha.core.config import Config
    from cosecha.core.engines.base import Engine
    from cosecha.core.items import TestItem
    from cosecha.core.reporter import ReportSubject


@dataclass(slots=True)
class _JUnitSuiteState:
    name: str
    fragment_path: Path
    tests: int = 0
    failures: int = 0
    errors: int = 0
    skipped: int = 0
    duration: float = 0.0
    timestamp: str = ''


class JUnitReporter(Reporter):
    __slots__ = (
        '_suite_states',
        '_temp_dir',
        'output_path',
        'start_time',
    )
    contribution_name = 'junit'

    @classmethod
    def reporter_name(cls) -> str:
        return 'junit'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'structured'

    def __init__(self, output_path: Path):
        self.output_path = output_path
        self._suite_states: dict[str, _JUnitSuiteState] = {}
        self._temp_dir = output_path.parent / f'.{output_path.name}.parts'
        self.start_time = 0.0

    def initialize(self, config: Config, engine: Engine | None = None) -> None:
        self.config = config
        self.engine = engine

    async def start(self):
        self.start_time = time.perf_counter()
        self._suite_states = {}
        if self._temp_dir.exists():
            shutil.rmtree(self._temp_dir)
        self._temp_dir.mkdir(parents=True, exist_ok=True)

    async def add_test(self, test: TestItem):
        del test

    async def add_test_result(self, test: ReportSubject):
        report = ensure_test_report(test, self.config.root_path)
        suite_name = str(report.path or 'unknown')
        suite_state = self._suite_states.get(suite_name)
        if suite_state is None:
            suite_state = _JUnitSuiteState(
                name=suite_name,
                fragment_path=self._temp_dir
                / f'{len(self._suite_states)}.xml',
                timestamp=time.strftime('%Y-%m-%dT%H:%M:%S'),
            )
            self._suite_states[suite_name] = suite_state

        suite_state.tests += 1
        suite_state.duration += report.duration
        if report.status == TestResultStatus.FAILED:
            suite_state.failures += 1
        elif report.status == TestResultStatus.ERROR:
            suite_state.errors += 1
        elif report.status == TestResultStatus.SKIPPED:
            suite_state.skipped += 1

        testcase_xml = ET.tostring(
            _build_testcase_element(report),
            encoding='unicode',
        )
        suite_state.fragment_path.parent.mkdir(parents=True, exist_ok=True)
        with suite_state.fragment_path.open('a', encoding='utf-8') as stream:
            stream.write(testcase_xml)
            stream.write('\n')

    async def print_report(self):
        total_duration = time.perf_counter() - self.start_time
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open('w', encoding='utf-8') as stream:
            stream.write('<?xml version="1.0" encoding="utf-8"?>\n')
            stream.write(
                f'<testsuites time={quoteattr(f"{total_duration:.3f}")}>\n',
            )
            for suite_name in sorted(self._suite_states):
                suite_state = self._suite_states[suite_name]
                stream.write(
                    _build_testsuite_open_tag(suite_state),
                )
                stream.write('\n')
                if suite_state.fragment_path.exists():
                    stream.write(
                        suite_state.fragment_path.read_text(encoding='utf-8'),
                    )
                stream.write('</testsuite>\n')
            stream.write('</testsuites>\n')

        if self._temp_dir.exists():
            shutil.rmtree(self._temp_dir)


def _build_testsuite_open_tag(suite_state: _JUnitSuiteState) -> str:
    attributes = (
        ('name', suite_state.name),
        ('tests', str(suite_state.tests)),
        ('failures', str(suite_state.failures)),
        ('errors', str(suite_state.errors)),
        ('skipped', str(suite_state.skipped)),
        ('time', f'{suite_state.duration:.3f}'),
        ('timestamp', suite_state.timestamp),
    )
    rendered_attributes = ' '.join(
        f'{name}={quoteattr(value)}' for name, value in attributes
    )
    return f'<testsuite {rendered_attributes}>'


def _build_testcase_element(test: TestReport) -> ET.Element:
    classname = 'cosecha'
    name = str(test.path) if test.path else 'unknown'
    line = '0'

    if test.engine_name == 'gherkin':
        reporting = _import_gherkin_reporting()

        classname = reporting.gherkin_feature_name(test) or classname
        name = reporting.gherkin_scenario_name(test) or name
        scenario_line = reporting.gherkin_scenario_line(test)
        if scenario_line is not None:
            line = str(scenario_line)

    case = ET.Element(
        'testcase',
        classname=classname,
        name=name,
        file=str(test.path) if test.path else '',
        line=line,
        time=f'{test.duration:.3f}',
    )

    if test.status == TestResultStatus.FAILED:
        failure = ET.SubElement(
            case,
            'failure',
            message=test.message or 'Test failed',
            type='AssertionError',
        )
        if test.exception_text:
            failure.text = test.exception_text

    elif test.status == TestResultStatus.ERROR:
        error = ET.SubElement(
            case,
            'error',
            message=test.message or 'Test error',
            type='Exception',
        )
        if test.exception_text:
            error.text = test.exception_text

    elif test.status == TestResultStatus.SKIPPED:
        ET.SubElement(
            case,
            'skipped',
            message=test.message or 'Skipped',
        )

    if test.engine_name == 'gherkin':
        reporting = _import_gherkin_reporting()
        system_out = ET.SubElement(case, 'system-out')
        trace = []
        for step_res in reporting.gherkin_step_result_reports(test):
            status_str = step_res.status.value.upper()
            kw = step_res.step.keyword
            txt = step_res.step.text
            trace.append(f'{status_str}: {kw}{txt}')
        system_out.text = cast('str', '\n'.join(trace))

    return case


def _import_gherkin_reporting():
    return importlib.import_module('cosecha.engine.gherkin.reporting')


__all__ = ('JUnitReporter',)
