from __future__ import annotations

import sys

from types import SimpleNamespace

from cosecha.core.items import TestResultStatus
from cosecha.core.location import Location
from cosecha.core.reporting_ir import (
    EntityReport,
    ExampleReport,
    LocationReport,
    StepReport,
    StepResultReport,
    TestReport,
)
from cosecha.engine.gherkin.reporting import (
    _entity_report_to_dict,
    _example_report_to_dict,
    _format_exception_text,
    _location_report_from_dict,
    _location_report_to_dict,
    _step_report_from_dict,
    _step_report_to_dict,
    _step_result_report_from_dict,
    _step_result_report_to_dict,
    build_gherkin_engine_payload,
    feature_location_text,
    gherkin_feature_name,
    gherkin_scenario_line,
    gherkin_scenario_name,
    gherkin_step_result_reports,
)


def _build_location(line: int) -> Location:
    return Location('features/demo.feature', line, 1)


def test_build_gherkin_engine_payload_serializes_feature_scenario_example_and_steps(
) -> None:
    try:
        raise RuntimeError('step failed')
    except RuntimeError:
        exc_info = sys.exc_info()

    first_step = SimpleNamespace(
        status=TestResultStatus.PASSED,
        message='ok',
        step=SimpleNamespace(
            keyword='Given ',
            text='a user exists',
            location=_build_location(3),
        ),
        match=SimpleNamespace(
            step_definition=SimpleNamespace(location=_build_location(99)),
        ),
        exc_info=(None, None, None),
    )
    second_step = SimpleNamespace(
        status=TestResultStatus.FAILED,
        message='boom',
        step=SimpleNamespace(
            keyword='When ',
            text='the user pays',
            location=_build_location(4),
        ),
        match=None,
        exc_info=exc_info,
    )
    test = SimpleNamespace(
        feature=SimpleNamespace(
            name='Payments',
            location=_build_location(1),
            tags=(SimpleNamespace(name='@billing'),),
        ),
        scenario=SimpleNamespace(
            name='Payment succeeds',
            location=_build_location(2),
            tags=(SimpleNamespace(name='@critical'),),
        ),
        example=SimpleNamespace(location=_build_location(20)),
        step_result_list=(first_step, second_step),
    )

    payload = build_gherkin_engine_payload(test)

    assert payload['feature']['name'] == 'Payments'
    assert payload['scenario']['name'] == 'Payment succeeds'
    assert payload['example']['location']['line'] == 20
    assert payload['step_result_list'][0]['step']['implementation_location'][
        'line'
    ] == 99
    assert payload['step_result_list'][1]['step']['implementation_location'] is None
    assert 'RuntimeError: step failed' in (
        payload['step_result_list'][1]['exception_text'] or ''
    )


def test_report_accessors_handle_invalid_payloads_and_non_gherkin_reports() -> None:
    non_gherkin = TestReport(
        path='x',
        status=TestResultStatus.PASSED,
        message=None,
        duration=0.1,
        engine_name='pytest',
        engine_payload={'feature': {'location': {'text': 'x:1'}}},
    )
    malformed = TestReport(
        path='x',
        status=TestResultStatus.PASSED,
        message=None,
        duration=0.1,
        engine_name='gherkin',
        engine_payload={
            'feature': {'location': 'invalid'},
            'scenario': {'location': []},
        },
    )
    gherkin = TestReport(
        path='features/demo.feature',
        status=TestResultStatus.PASSED,
        message='done',
        duration=0.2,
        engine_name='gherkin',
        engine_payload={
            'feature': {'name': 'Payments', 'location': {'text': 'demo:1'}},
            'scenario': {'name': 'A scenario', 'location': {'line': '12'}},
        },
    )

    assert feature_location_text(non_gherkin) is None
    assert gherkin_feature_name(non_gherkin) is None
    assert gherkin_scenario_name(non_gherkin) is None
    assert gherkin_scenario_line(non_gherkin) is None
    assert gherkin_step_result_reports(non_gherkin) == ()

    assert feature_location_text(malformed) is None
    assert gherkin_feature_name(malformed) is None
    assert gherkin_scenario_name(malformed) is None
    assert gherkin_scenario_line(malformed) is None

    assert feature_location_text(gherkin) == 'demo:1'
    assert gherkin_feature_name(gherkin) == 'Payments'
    assert gherkin_scenario_name(gherkin) == 'A scenario'
    assert gherkin_scenario_line(gherkin) == 12


def test_step_result_report_roundtrip_and_defaults() -> None:
    location = LocationReport(text='demo.feature:10', line=10)
    entity = EntityReport(name='Feature', location=location, tags=('tag',))
    example = ExampleReport(location=location)
    step = StepReport(
        keyword='Given ',
        text='a precondition',
        location=location,
        implementation_location=None,
    )
    step_result = StepResultReport(
        status=TestResultStatus.ERROR,
        message='failed',
        step=step,
        exception_text='traceback...',
    )

    assert _location_report_from_dict(None) == LocationReport(text='', line=0)
    assert _location_report_to_dict(location) == {
        'line': 10,
        'text': 'demo.feature:10',
    }
    assert _entity_report_to_dict(entity) == {
        'location': {'line': 10, 'text': 'demo.feature:10'},
        'name': 'Feature',
        'tags': ['tag'],
    }
    assert _example_report_to_dict(example) == {
        'location': {'line': 10, 'text': 'demo.feature:10'},
    }
    assert _step_report_from_dict(None).text == ''
    assert _step_report_to_dict(step)['implementation_location'] is None
    assert _step_result_report_to_dict(step_result)['status'] == 'error'
    assert _step_result_report_from_dict(None).status == TestResultStatus.PENDING
    assert _format_exception_text(None) is None
    assert _format_exception_text((None, None, None)) is None


def test_gherkin_step_result_reports_rebuilds_step_results_from_payload() -> None:
    report = TestReport(
        path='features/demo.feature',
        status=TestResultStatus.FAILED,
        message='failed',
        duration=0.1,
        engine_name='gherkin',
        engine_payload={
            'step_result_list': [
                {
                    'status': 'failed',
                    'message': 'boom',
                    'exception_text': 'trace',
                    'step': {
                        'keyword': 'When ',
                        'text': 'it fails',
                        'location': {'text': 'demo.feature:12', 'line': 12},
                        'implementation_location': {
                            'text': 'steps.py:8',
                            'line': 8,
                        },
                    },
                },
            ],
        },
    )

    step_results = gherkin_step_result_reports(report)

    assert len(step_results) == 1
    assert step_results[0].status == TestResultStatus.FAILED
    assert step_results[0].step.implementation_location is not None
    assert step_results[0].step.implementation_location.line == 8
