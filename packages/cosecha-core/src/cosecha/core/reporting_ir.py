from __future__ import annotations

import contextlib
import traceback

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cosecha.core.items import TestResultStatus
from cosecha.core.location import Location


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path

    from cosecha.core.items import TestItem
    from cosecha.core.location import BaseLocation
    from cosecha.core.types import ExcInfo


@dataclass(slots=True, frozen=True)
class LocationReport:
    text: str
    line: int


@dataclass(slots=True, frozen=True)
class EntityReport:
    name: str
    location: LocationReport
    tags: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class ExampleReport:
    location: LocationReport


@dataclass(slots=True, frozen=True)
class StepReport:
    keyword: str
    text: str
    location: LocationReport
    implementation_location: LocationReport | None = None


@dataclass(slots=True, frozen=True)
class StepResultReport:
    status: TestResultStatus
    message: str | None
    step: StepReport
    exception_text: str | None = None


@dataclass(slots=True, frozen=True)
class TestReport:
    __test__ = False
    path: str | None
    status: TestResultStatus
    message: str | None
    duration: float
    failure_kind: str | None = None
    error_code: str | None = None
    exception_text: str | None = None
    engine_name: str | None = None
    engine_payload: dict[str, object] = field(default_factory=dict)


def build_location_report(
    location: BaseLocation,
    root_path: Path | None = None,
) -> LocationReport:
    normalized_location = location
    if root_path and isinstance(location, Location):
        with contextlib.suppress(Exception):
            normalized_location = location.relative_to(root_path)

    return LocationReport(
        text=str(normalized_location),
        line=normalized_location.line,
    )


def build_test_report(
    test: TestItem,
    root_path: Path | None = None,
) -> TestReport:
    engine_name = getattr(test, 'engine_name', None)
    engine_payload: dict[str, object] = {}
    if hasattr(test, 'build_engine_report_payload'):
        payload_builder = test.build_engine_report_payload
        if callable(payload_builder):
            engine_payload = dict(payload_builder(root_path=root_path))

    return TestReport(
        path=str(test.path) if test.path else None,
        status=test.status,
        message=test.message,
        duration=test.duration,
        failure_kind=test.failure_kind,
        error_code=test.error_code,
        exception_text=_format_exception_text(test.exc_info),
        engine_name=engine_name,
        engine_payload=engine_payload,
    )


def ensure_test_report(
    subject: TestItem | TestReport,
    root_path: Path | None = None,
) -> TestReport:
    if isinstance(subject, TestReport):
        return subject

    return build_test_report(subject, root_path)


def reconcile_test_report(
    subject: TestItem | TestReport,
    test: TestItem,
    root_path: Path | None = None,
) -> TestReport:
    if not isinstance(subject, TestReport):
        return build_test_report(test, root_path)

    return TestReport(
        path=subject.path if subject.path is not None else str(test.path),
        status=test.status,
        message=test.message,
        duration=test.duration,
        failure_kind=test.failure_kind,
        error_code=test.error_code,
        exception_text=_format_exception_text(test.exc_info),
        engine_name=subject.engine_name,
        engine_payload=subject.engine_payload,
    )


def serialize_test_report(report: TestReport) -> dict[str, object]:
    return {
        'duration': report.duration,
        'engine_name': report.engine_name,
        'engine_payload': report.engine_payload,
        'error_code': report.error_code,
        'exception_text': report.exception_text,
        'failure_kind': report.failure_kind,
        'message': report.message,
        'path': report.path,
        'status': report.status.value,
    }


def deserialize_test_report(data: dict[str, object]) -> TestReport:
    return TestReport(
        duration=float(data['duration']),
        engine_name=(
            str(data['engine_name'])
            if data.get('engine_name') is not None
            else None
        ),
        engine_payload=_normalize_engine_payload(data.get('engine_payload')),
        error_code=(
            str(data['error_code'])
            if data.get('error_code') is not None
            else None
        ),
        exception_text=(
            str(data['exception_text'])
            if data.get('exception_text') is not None
            else None
        ),
        failure_kind=(
            str(data['failure_kind'])
            if data.get('failure_kind') is not None
            else None
        ),
        message=(
            str(data['message']) if data.get('message') is not None else None
        ),
        path=str(data['path']) if data.get('path') is not None else None,
        status=TestResultStatus(data['status']),
    )


def _normalize_engine_payload(data: object) -> dict[str, object]:
    return data.copy() if isinstance(data, dict) else {}


def _format_exception_text(exc_info: ExcInfo | None) -> str | None:
    if not exc_info or exc_info == (None, None, None):
        return None

    return ''.join(traceback.format_exception(*exc_info))


def _location_report_to_dict(location: LocationReport) -> dict[str, object]:
    return {
        'line': location.line,
        'text': location.text,
    }


def _location_report_from_dict(data: object) -> LocationReport:
    normalized = data if isinstance(data, dict) else {}
    return LocationReport(
        text=str(normalized.get('text', '')),
        line=int(normalized.get('line', 0)),
    )

