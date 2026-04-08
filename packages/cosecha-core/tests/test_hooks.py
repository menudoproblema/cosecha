from __future__ import annotations

import asyncio

from pathlib import Path

from cosecha.core.hooks import EngineHook, Hook


class _TrackingHook(Hook):
    def __init__(self) -> None:
        super().__init__()
        self.setup_calls = 0

    def setup_argparse(self, parser) -> None:
        del parser
        self.setup_calls += 1

    def parse_args(self, parser, args) -> None:
        del parser, args

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


class _TrackingEngineHook(EngineHook):
    async def before_collect(self, path: Path, collector, engine):
        del path, collector, engine

    async def after_collect(self, path: Path, collector, engine):
        del path, collector, engine

    async def before_session_start(self, engine) -> None:
        del engine

    async def after_session_finish(self, engine) -> None:
        del engine

    async def before_test_run(self, test, engine):
        del test, engine

    async def after_test_run(self, test, engine):
        del test, engine


def test_hook_setup_registers_arguments_only_once() -> None:
    hook = _TrackingHook()

    hook.setup(parser=object())
    hook.setup(parser=object())

    assert hook.setup_calls == 1


def test_hook_set_config_stores_reference() -> None:
    hook = _TrackingHook()
    config = object()

    hook.set_config(config)  # type: ignore[arg-type]

    assert hook.config is config


def test_engine_hook_default_contract_methods_are_callable() -> None:
    hook = _TrackingEngineHook()

    asyncio.run(hook.before_collect(Path('.'), object(), object()))
    asyncio.run(hook.after_collect(Path('.'), object(), object()))
    asyncio.run(hook.before_session_start(object()))
    asyncio.run(hook.after_session_finish(object()))
    asyncio.run(hook.before_test_run(object(), object()))
    asyncio.run(hook.after_test_run(object(), object()))
