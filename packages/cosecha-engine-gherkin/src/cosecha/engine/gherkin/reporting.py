from __future__ import annotations

import traceback

from cosecha.core.items import TestResultStatus
from cosecha.core.reporting_ir import (
    EntityReport,
    ExampleReport,
    LocationReport,
    StepReport,
    StepResultReport,
    TestReport,
    build_location_report,
)


def build_gherkin_engine_payload(
    test,
    *,
    root_path=None,
) -> dict[str, object]:
    return {
        'feature': _entity_report_to_dict(
            EntityReport(
                name=test.feature.name,
                location=build_location_report(
                    test.feature.location,
                    root_path,
                ),
                tags=tuple(tag.name for tag in test.feature.tags),
            ),
        ),
        'scenario': _entity_report_to_dict(
            EntityReport(
                name=test.scenario.name,
                location=build_location_report(
                    test.scenario.location,
                    root_path,
                ),
                tags=tuple(tag.name for tag in test.scenario.tags),
            ),
        ),
        'example': (
            _example_report_to_dict(
                ExampleReport(
                    location=build_location_report(
                        test.example.location,
                        root_path,
                    ),
                ),
            )
            if test.example
            else None
        ),
        'step_result_list': [
            _step_result_report_to_dict(
                StepResultReport(
                    status=step_result.status,
                    message=step_result.message,
                    step=StepReport(
                        keyword=step_result.step.keyword,
                        text=step_result.step.text,
                        location=build_location_report(
                            step_result.step.location,
                            root_path,
                        ),
                        implementation_location=(
                            build_location_report(
                                step_result.match.step_definition.location,
                                root_path,
                            )
                            if step_result.match
                            else None
                        ),
                    ),
                    exception_text=_format_exception_text(
                        step_result.exc_info,
                    ),
                ),
            )
            for step_result in test.step_result_list
        ],
    }


def feature_location_text(report: TestReport) -> str | None:
    payload = report.engine_payload if report.engine_name == 'gherkin' else {}
    feature = payload.get('feature')
    if not isinstance(feature, dict):
        return None
    location = feature.get('location')
    if not isinstance(location, dict):
        return None
    text = location.get('text')
    return str(text) if text is not None else None


def gherkin_scenario_name(report: TestReport) -> str | None:
    payload = report.engine_payload if report.engine_name == 'gherkin' else {}
    scenario = payload.get('scenario')
    if not isinstance(scenario, dict):
        return None
    name = scenario.get('name')
    return str(name) if name is not None else None


def gherkin_feature_name(report: TestReport) -> str | None:
    payload = report.engine_payload if report.engine_name == 'gherkin' else {}
    feature = payload.get('feature')
    if not isinstance(feature, dict):
        return None
    name = feature.get('name')
    return str(name) if name is not None else None


def gherkin_scenario_line(report: TestReport) -> int | None:
    payload = report.engine_payload if report.engine_name == 'gherkin' else {}
    scenario = payload.get('scenario')
    if not isinstance(scenario, dict):
        return None
    location = scenario.get('location')
    if not isinstance(location, dict):
        return None
    line = location.get('line')
    return int(line) if line is not None else None


def gherkin_step_result_reports(
    report: TestReport,
) -> tuple[StepResultReport, ...]:
    if report.engine_name != 'gherkin':
        return ()
    return tuple(
        _step_result_report_from_dict(step_result)
        for step_result in report.engine_payload.get('step_result_list', [])
    )


def _format_exception_text(exc_info) -> str | None:
    if not exc_info or exc_info == (None, None, None):
        return None
    return ''.join(traceback.format_exception(*exc_info))


def _location_report_to_dict(location: LocationReport) -> dict[str, object]:
    return {'line': location.line, 'text': location.text}


def _location_report_from_dict(data: object) -> LocationReport:
    normalized = data if isinstance(data, dict) else {}
    return LocationReport(
        text=str(normalized.get('text', '')),
        line=int(normalized.get('line', 0)),
    )


def _entity_report_to_dict(entity: EntityReport) -> dict[str, object]:
    return {
        'location': _location_report_to_dict(entity.location),
        'name': entity.name,
        'tags': list(entity.tags),
    }


def _example_report_to_dict(example: ExampleReport) -> dict[str, object]:
    return {'location': _location_report_to_dict(example.location)}


def _step_report_to_dict(step: StepReport) -> dict[str, object]:
    return {
        'implementation_location': (
            _location_report_to_dict(step.implementation_location)
            if step.implementation_location is not None
            else None
        ),
        'keyword': step.keyword,
        'location': _location_report_to_dict(step.location),
        'text': step.text,
    }


def _step_report_from_dict(data: object) -> StepReport:
    normalized = data if isinstance(data, dict) else {}
    implementation_location = normalized.get('implementation_location')
    return StepReport(
        keyword=str(normalized.get('keyword', '')),
        text=str(normalized.get('text', '')),
        location=_location_report_from_dict(normalized.get('location')),
        implementation_location=(
            _location_report_from_dict(implementation_location)
            if implementation_location is not None
            else None
        ),
    )


def _step_result_report_to_dict(
    step_result: StepResultReport,
) -> dict[str, object]:
    return {
        'exception_text': step_result.exception_text,
        'message': step_result.message,
        'status': step_result.status.value,
        'step': _step_report_to_dict(step_result.step),
    }


def _step_result_report_from_dict(data: object) -> StepResultReport:
    normalized = data if isinstance(data, dict) else {}
    return StepResultReport(
        status=TestResultStatus(normalized.get('status', 'pending')),
        message=(
            str(normalized.get('message'))
            if normalized.get('message') is not None
            else None
        ),
        step=_step_report_from_dict(normalized.get('step')),
        exception_text=(
            str(normalized.get('exception_text'))
            if normalized.get('exception_text') is not None
            else None
        ),
    )
