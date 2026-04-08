from __future__ import annotations

import importlib
import json
import time

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from cosecha.core.capabilities import (
    CAPABILITY_ARTIFACT_OUTPUT,
    CAPABILITY_RESULT_PROJECTION,
    CAPABILITY_STRUCTURED_OUTPUT,
    CapabilityAttribute,
    CapabilityDescriptor,
    CapabilityOperationBinding,
)
from cosecha.core.items import TestResultStatus
from cosecha.core.reporter import Reporter
from cosecha.core.reporting_ir import ensure_test_report


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path

    from cosecha.core.config import Config
    from cosecha.core.engines.base import Engine
    from cosecha.core.reporter import ReportSubject


@dataclass(slots=True)
class _JsonReportCase:
    status: str
    duration: float
    path: str | None
    message: str | None = None
    failure_kind: str | None = None
    error_code: str | None = None
    exception_text: str | None = None
    scenario_name: str | None = None
    feature_name: str | None = None
    line: int | None = None


class JsonReporter(Reporter):
    __slots__ = ('_cases', 'output_path', 'start_time')
    contribution_name = 'json'

    @classmethod
    def reporter_name(cls) -> str:
        return 'json'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'structured'

    @classmethod
    def describe_capabilities(cls) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name='report_lifecycle',
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='reporter.start',
                    ),
                    CapabilityOperationBinding(
                        operation_type='reporter.print_report',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_RESULT_PROJECTION,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='supports_engine_specific_projection',
                        value=True,
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='reporter.add_test',
                    ),
                    CapabilityOperationBinding(
                        operation_type='reporter.add_test_result',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_ARTIFACT_OUTPUT,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='artifact_formats',
                        value=('json',),
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='reporter.print_report',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_STRUCTURED_OUTPUT,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='output_kind',
                        value='structured',
                    ),
                    CapabilityAttribute(
                        name='artifact_formats',
                        value=('json',),
                    ),
                    CapabilityAttribute(
                        name='supports_engine_specific_projection',
                        value=True,
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='reporter.print_report',
                    ),
                ),
            ),
        )

    def __init__(self, output_path: Path):
        self.output_path = output_path
        self._cases: list[_JsonReportCase] = []
        self.start_time = 0.0

    def initialize(self, config: Config, engine: Engine | None = None) -> None:
        super().initialize(config, engine)

    async def start(self) -> None:
        self._cases = []
        self.start_time = time.perf_counter()

    async def add_test(self, test):
        del test

    async def add_test_result(self, test: ReportSubject) -> None:
        report = ensure_test_report(test, self.config.root_path)
        case = _JsonReportCase(
            status=report.status.value,
            duration=report.duration,
            path=(str(report.path) if report.path is not None else None),
            message=report.message,
            failure_kind=report.failure_kind,
            error_code=report.error_code,
            exception_text=report.exception_text,
        )
        if report.engine_name == 'gherkin':
            reporting = _import_gherkin_reporting()

            case.scenario_name = reporting.gherkin_scenario_name(report)
            case.feature_name = reporting.gherkin_feature_name(report)
            case.line = reporting.gherkin_scenario_line(report)

        self._cases.append(case)

    async def print_report(self) -> None:
        async def _write() -> None:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            duration = time.perf_counter() - self.start_time
            counts = {
                status.value: 0
                for status in (
                    TestResultStatus.PASSED,
                    TestResultStatus.FAILED,
                    TestResultStatus.ERROR,
                    TestResultStatus.SKIPPED,
                    TestResultStatus.PENDING,
                )
            }
            for case in self._cases:
                counts[case.status] = counts.get(case.status, 0) + 1

            payload = {
                'schema_version': 1,
                'reporter': 'json',
                'engine_name': self.engine.name
                if self.engine is not None
                else None,
                'root_path': str(self.config.root_path),
                'duration': duration,
                'summary': {
                    'total_tests': len(self._cases),
                    'status_counts': counts,
                },
                'tests': [asdict(case) for case in self._cases],
            }
            self.output_path.write_text(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ),
                encoding='utf-8',
            )

        if self.telemetry_stream is None:
            await _write()
            return

        async with self.telemetry_stream.span(
            'reporter.output.write',
            attributes={
                'cosecha.reporter.name': self.reporter_name(),
                'cosecha.reporter.output_kind': self.reporter_output_kind(),
            },
        ):
            await _write()


def _import_gherkin_reporting():
    return importlib.import_module('cosecha.engine.gherkin.reporting')


__all__ = ('JsonReporter',)
