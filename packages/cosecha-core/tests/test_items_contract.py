from __future__ import annotations

from pathlib import Path

from cosecha.core.items import TestItem, TestResultStatus


class _DummyTestItem(TestItem):
    async def run(self, context) -> None:
        del context

    def has_selection_label(self, name: str) -> bool:
        del name
        return False


def test_has_failed_is_true_only_for_failed_and_error_statuses() -> None:
    test_item = _DummyTestItem(Path('tests/payment.feature'))

    test_item.status = TestResultStatus.PASSED
    assert test_item.has_failed is False

    test_item.status = TestResultStatus.SKIPPED
    assert test_item.has_failed is False

    test_item.status = TestResultStatus.FAILED
    assert test_item.has_failed is True

    test_item.status = TestResultStatus.ERROR
    assert test_item.has_failed is True


def test_has_selection_label_is_the_core_selection_contract() -> None:
    class _TaggedDummyTestItem(TestItem):
        async def run(self, context) -> None:
            del context

        def has_selection_label(self, name: str) -> bool:
            return name == 'critical'

    test_item = _TaggedDummyTestItem(Path('tests/payment.feature'))

    assert test_item.has_selection_label('critical') is True
    assert test_item.has_selection_label('slow') is False
