from __future__ import annotations

import asyncio

from pathlib import Path

from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.items import TestItem
from cosecha_internal.testkit import DummyReporter, build_config


class _DummyContext(BaseContext):
    async def cleanup(self) -> None:
        return None


class _DummyTestItem(TestItem):
    async def run(self, context) -> None:
        del context

    def has_selection_label(self, name: str) -> bool:
        del name
        return False


class _Collector:
    file_type = 'feature'

    def __init__(self) -> None:
        self.collected_files: set[Path] = set()
        self.failed_files: set[Path] = set()
        self.collected_tests: tuple[TestItem, ...] = ()

    def initialize(self, config, base_path) -> None:
        del config, base_path

    def bind_session_timing(self, session_timing, engine_name: str) -> None:
        del session_timing, engine_name

    def bind_domain_event_stream(self, domain_event_stream) -> None:
        del domain_event_stream

    async def collect(self, path, excluded_paths) -> None:
        del path, excluded_paths
        self.collected_files = {Path('tests/example.feature')}
        self.failed_files = {Path('tests/failed.feature')}


class _Hook:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def before_collect(self, base_path, collector, engine) -> None:
        del base_path, collector, engine
        self.calls.append('before_collect')

    async def after_collect(self, base_path, collector, engine) -> None:
        del base_path, collector, engine
        self.calls.append('after_collect')

    async def before_session_start(self, engine) -> None:
        del engine
        self.calls.append('before_session_start')

    async def after_session_finish(self, engine) -> None:
        del engine
        self.calls.append('after_session_finish')

    async def before_test_run(self, test, engine) -> None:
        del test, engine
        self.calls.append('before_test_run')

    async def after_test_run(self, test, engine) -> None:
        del test, engine
        self.calls.append('after_test_run')


class _DummyEngine(Engine):
    async def generate_new_context(self, test: TestItem) -> BaseContext:
        del test
        return _DummyContext()


def test_engine_base_paths_and_hooks(tmp_path: Path) -> None:
    hook = _Hook()
    collector = _Collector()
    engine = _DummyEngine(
        'dummy',
        collector=collector,
        reporter=DummyReporter(),
        hooks=(hook,),
    )
    config = build_config(tmp_path)
    trace_calls: list[tuple[object, ...]] = []
    config.diagnostics.trace = lambda *args, **kwargs: trace_calls.append(args)
    engine.initialize(config, '')

    test = _DummyTestItem(tmp_path / 'tests' / 'example.feature')

    async def _run() -> None:
        await engine.collect()
        await engine.start_session()
        await engine.start_test(test)
        await engine.finish_test(test)
        await engine.finish_session()

    asyncio.run(_run())

    assert hook.calls == [
        'before_collect',
        'after_collect',
        'before_session_start',
        'before_test_run',
        'after_test_run',
        'after_session_finish',
    ]

    assert engine.is_file_collected(tmp_path / 'tests' / 'example.feature') is True
    assert engine.is_file_failed(tmp_path / 'tests' / 'failed.feature') is True
    assert engine.is_file_collected(123) is False
    assert engine.is_file_failed(123) is False

    assert engine.describe_capabilities() == ()
    assert engine.describe_engine_dependencies() == ()

    engine.log('trace-line')
    assert trace_calls == [('trace-line',)]


def test_engine_base_file_checks_are_safe_before_collect(
    tmp_path: Path,
) -> None:
    engine = _DummyEngine(
        'dummy',
        collector=_Collector(),
        reporter=DummyReporter(),
    )
    config = build_config(tmp_path)
    engine.initialize(config, '')

    assert engine.is_file_collected(tmp_path / 'tests' / 'example.feature') is False
    assert engine.is_file_failed(tmp_path / 'tests' / 'failed.feature') is False
