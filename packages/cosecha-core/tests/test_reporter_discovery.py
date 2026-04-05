from __future__ import annotations

from typing import TYPE_CHECKING

from cosecha.core.reporter import Reporter
from cosecha.core.runner import Runner


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path


class _StructuredReporter(Reporter):
    @classmethod
    def reporter_name(cls) -> str:
        return 'json'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'structured'

    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path

    async def add_test(self, test):
        del test

    async def add_test_result(self, test):
        del test

    async def print_report(self):
        return None


class _ConsoleReporter(Reporter):
    @classmethod
    def reporter_name(cls) -> str:
        return 'console'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'console'

    async def add_test(self, test):
        del test

    async def add_test_result(self, test):
        del test

    async def print_report(self):
        return None


def test_runner_available_reporter_types_only_exposes_structured_contributions(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        'cosecha.core.runner.iter_shell_reporting_contributions',
        lambda: (_StructuredReporter, _ConsoleReporter),
    )

    assert Runner.available_reporter_types() == {'json': _StructuredReporter}
