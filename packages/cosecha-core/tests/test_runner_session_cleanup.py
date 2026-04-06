from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

import pytest

from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.hooks import Hook
from cosecha.core.items import TestItem, TestResultStatus
from cosecha.core.plugins.base import Plugin
from cosecha.core.runner import Runner, capture_handler, root_logger
from cosecha_internal.testkit import (
    DummyReporter,
    ListCollector,
    build_config,
)


if TYPE_CHECKING:
    from argparse import ArgumentParser, Namespace
    from pathlib import Path


class DummyContext(BaseContext):
    def __init__(self) -> None:
        self.cleaned = False

    async def cleanup(self) -> None:
        self.cleaned = True


class PassingTestItem(TestItem):
    async def run(self, context) -> None:
        del context
        self.status = TestResultStatus.PASSED

    def has_selection_label(self, name: str) -> bool:
        del name
        return False


class TrackingEngine(Engine):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.started = False
        self.finished = False
        self.last_context: DummyContext | None = None

    async def generate_new_context(self, test: TestItem) -> BaseContext:
        del test
        self.last_context = DummyContext()
        return self.last_context

    async def finish_session(self):
        self.finished = True
        await super().finish_session()


class FailingStartEngine(TrackingEngine):
    async def start_session(self) -> None:
        self.started = True
        msg = 'engine boom'
        raise RuntimeError(msg)


class TrackingHook(Hook):
    def __init__(self) -> None:
        super().__init__()
        self.after_called = False

    async def after_session_finish(self) -> None:
        self.after_called = True


class TrackingPlugin(Plugin):
    def __init__(self) -> None:
        self.started = False
        self.finished = False

    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args: Namespace):
        del args

    async def start(self):
        self.started = True

    async def finish(self) -> None:
        self.finished = True


class FailingStartPlugin(TrackingPlugin):
    async def start(self):
        self.started = True
        msg = 'plugin boom'
        raise RuntimeError(msg)


def _write_feature(root_path: Path, file_name: str = 'broken.feature') -> Path:
    feature_path = root_path / file_name
    feature_path.write_text('Feature: broken\n', encoding='utf-8')
    return feature_path


def test_runner_restores_logging_and_finishes_started_plugins_on_failure(
    tmp_path: Path,
) -> None:
    feature_path = _write_feature(tmp_path)
    engine = TrackingEngine(
        'dummy',
        collector=ListCollector([PassingTestItem(feature_path)]),
        reporter=DummyReporter(),
    )
    plugin = FailingStartPlugin()
    config = build_config(tmp_path)
    original_handlers = tuple(root_logger.handlers)
    runner = Runner(config, {'': engine}, plugins=(plugin,))

    with pytest.raises(RuntimeError, match='plugin boom'):
        asyncio.run(runner.run())

    assert plugin.started is True
    assert plugin.finished is True
    assert tuple(root_logger.handlers) == original_handlers
    assert capture_handler not in root_logger.handlers


def test_runner_finishes_engine_and_hooks_when_engine_start_fails(
    tmp_path: Path,
) -> None:
    feature_path = _write_feature(tmp_path, 'engine.feature')
    engine = FailingStartEngine(
        'dummy',
        collector=ListCollector([PassingTestItem(feature_path)]),
        reporter=DummyReporter(),
    )
    hook = TrackingHook()
    config = build_config(tmp_path)
    original_handlers = tuple(root_logger.handlers)
    runner = Runner(config, {'': engine}, hooks=(hook,))

    with pytest.raises(RuntimeError, match='engine boom'):
        asyncio.run(runner.run())

    assert engine.started is True
    assert engine.finished is True
    assert hook.after_called is True
    assert tuple(root_logger.handlers) == original_handlers
    assert capture_handler not in root_logger.handlers
