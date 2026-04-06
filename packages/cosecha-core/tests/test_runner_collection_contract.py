from __future__ import annotations

import asyncio

from contextlib import nullcontext
from typing import TYPE_CHECKING

from cosecha.core.collector import Collector
from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.hooks import Hook
from cosecha.core.items import TestItem, TestResultStatus
from cosecha.core.output import OutputMode
from cosecha.core.reporter import Reporter
from cosecha.core.runner import Runner
from cosecha_internal.testkit import build_config


if TYPE_CHECKING:
    from pathlib import Path


class DummyContext(BaseContext):
    async def cleanup(self) -> None:
        return None


class DummyReporter(Reporter):
    async def add_test(self, test: TestItem):
        del test

    async def add_test_result(self, test: TestItem):
        del test

    async def print_report(self):
        return None


class DummyTestItem(TestItem):
    async def run(self, context) -> None:
        del context
        self.status = TestResultStatus.PASSED

    def has_selection_label(self, name: str) -> bool:
        del name
        return False


class TrackingCollectHook(Hook):
    def __init__(self) -> None:
        super().__init__()
        self.before_collect_called = False

    async def before_collect(self, path) -> None:
        del path
        self.before_collect_called = True


class ContractCollector(Collector):
    def __init__(self, hook: TrackingCollectHook, test_path: Path) -> None:
        super().__init__('feature')
        self.hook = hook
        self.test_path = test_path

    async def find_test_files(self, base_path: Path) -> list[Path]:
        assert self.hook.before_collect_called is True
        assert base_path == self.base_path
        return [self.test_path]

    async def load_tests_from_file(
        self,
        test_path: Path,
    ) -> list[TestItem] | None:
        assert self._session_timing is not None
        return [DummyTestItem(test_path)]


class DummyEngine(Engine):
    async def generate_new_context(self, test: TestItem) -> BaseContext:
        del test
        return DummyContext()


class TrackingConsole:
    def __init__(self, **_kwargs) -> None:
        self.status_calls: list[
            tuple[tuple[object, ...], dict[str, object]]
        ] = []

    def status(self, *args, **kwargs):
        self.status_calls.append((args, kwargs))
        return nullcontext()


class LegacyTrackingConsole:
    def __init__(self, **_kwargs) -> None:
        self.status_calls: list[
            tuple[tuple[object, ...], dict[str, object]]
        ] = []

    def status(self, *args, spinner=None):
        kwargs = {}
        if spinner is not None:
            kwargs['spinner'] = spinner
        self.status_calls.append((args, kwargs))
        return nullcontext()


class LiveTrackingConsole(TrackingConsole):
    def should_render_run_status(self) -> bool:
        return False

    def should_render_collection_status(self) -> bool:
        return False

    def should_render_live_progress(self) -> bool:
        return True


def test_runner_restores_collection_contract_before_loading_files(
    tmp_path: Path,
) -> None:
    test_path = tmp_path / 'sample.feature'
    test_path.write_text('Feature: sample\n', encoding='utf-8')
    hook = TrackingCollectHook()
    engine = DummyEngine(
        'dummy',
        collector=ContractCollector(hook, test_path),
        reporter=DummyReporter(),
    )
    runner = Runner(build_config(tmp_path), {'': engine}, hooks=(hook,))

    failed = asyncio.run(runner.run())

    assert failed is False
    assert hook.before_collect_called is True
    assert engine._session_timing is runner.session_timing
    assert engine.collector._session_timing is runner.session_timing


def test_runner_wraps_collection_with_status_spinner(tmp_path: Path) -> None:
    test_path = tmp_path / 'sample.feature'
    test_path.write_text('Feature: sample\n', encoding='utf-8')
    hook = TrackingCollectHook()
    engine = DummyEngine(
        'dummy',
        collector=ContractCollector(hook, test_path),
        reporter=DummyReporter(),
    )
    config = build_config(tmp_path)
    config.console = TrackingConsole()
    runner = Runner(config, {'': engine}, hooks=(hook,))

    failed = asyncio.run(runner.run())

    assert failed is False
    assert config.console.status_calls == [
        (
            ('Collecting tests...',),
            {'spinner': 'monkey', 'transient': True},
        ),
        (
            ('Running tests...',),
            {'spinner': 'circle', 'transient': True},
        ),
    ]


def test_runner_falls_back_when_console_lacks_transient(
    tmp_path: Path,
) -> None:
    test_path = tmp_path / 'sample.feature'
    test_path.write_text('Feature: sample\n', encoding='utf-8')
    hook = TrackingCollectHook()
    engine = DummyEngine(
        'dummy',
        collector=ContractCollector(hook, test_path),
        reporter=DummyReporter(),
    )
    config = build_config(tmp_path)
    config.console = LegacyTrackingConsole()
    runner = Runner(config, {'': engine}, hooks=(hook,))

    failed = asyncio.run(runner.run())

    assert failed is False
    assert config.console.status_calls == [
        (
            ('Collecting tests...',),
            {'spinner': 'monkey'},
        ),
        (
            ('Running tests...',),
            {'spinner': 'circle'},
        ),
    ]


def test_runner_skips_status_spinners_in_live_mode(tmp_path: Path) -> None:
    test_path = tmp_path / 'sample.feature'
    test_path.write_text('Feature: sample\n', encoding='utf-8')
    hook = TrackingCollectHook()
    engine = DummyEngine(
        'dummy',
        collector=ContractCollector(hook, test_path),
        reporter=DummyReporter(),
    )
    config = build_config(tmp_path, output_mode=OutputMode.LIVE)
    config.console = LiveTrackingConsole()
    runner = Runner(config, {'': engine}, hooks=(hook,))

    failed = asyncio.run(runner.run())

    assert failed is False
    assert config.console.status_calls == []
