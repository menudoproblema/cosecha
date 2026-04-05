from __future__ import annotations

from pathlib import Path

from cosecha.core.items import TestItem, TestResultStatus
from cosecha.core.reporting_ir import TestReport, reconcile_test_report


HOOK_FAILURE_DURATION = 1.25


class _DummyTestItem(TestItem):
    def __init__(self) -> None:
        super().__init__(Path('tests/example.feature'))
        self.engine_name = 'gherkin'

    async def run(self, context) -> None:
        del context

    def has_selection_label(self, name: str) -> bool:
        del name
        return False


def test_reconcile_test_report_uses_final_test_state() -> None:
    test = _DummyTestItem()
    test.status = TestResultStatus.ERROR
    test.message = 'Error in after_test_run hook'
    test.failure_kind = 'hook'
    test.duration = HOOK_FAILURE_DURATION

    original_report = TestReport(
        path='tests/example.feature',
        status=TestResultStatus.PASSED,
        message=None,
        duration=0.75,
        failure_kind=None,
        error_code=None,
        exception_text=None,
        engine_name='gherkin',
        engine_payload={
            'scenario': {'name': 'Example scenario'},
            'step_result_list': [],
        },
    )

    reconciled = reconcile_test_report(original_report, test)

    assert reconciled.status is TestResultStatus.ERROR
    assert reconciled.message == 'Error in after_test_run hook'
    assert reconciled.failure_kind == 'hook'
    assert reconciled.duration == HOOK_FAILURE_DURATION
    assert reconciled.engine_name == 'gherkin'
    assert reconciled.engine_payload == original_report.engine_payload
