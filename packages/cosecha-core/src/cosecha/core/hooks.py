from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING


if TYPE_CHECKING:  # pragma: no cover
    from argparse import ArgumentParser, Namespace
    from pathlib import Path

    from cosecha.core.collector import Collector
    from cosecha.core.config import Config
    from cosecha.core.engines.base import Engine
    from cosecha.core.items import TestItem


class Hook(ABC):  # noqa: B024
    __slots__ = ('_parse_options_added', 'config')

    def __init__(self) -> None:
        self._parse_options_added = False

    def set_config(self, config: Config) -> None:
        self.config = config

    def setup(self, parser: ArgumentParser) -> None:
        if not self._parse_options_added:
            self.setup_argparse(parser)
            self._parse_options_added = True

    def setup_argparse(self, parser: ArgumentParser) -> None: ...  # noqa: B027

    def parse_args(self, parser: ArgumentParser, args: Namespace) -> None: ...  # noqa: B027

    async def before_collect(self, path: Path | None): ...  # noqa: B027

    async def before_session_start(self) -> None: ...  # noqa: B027

    async def after_session_finish(self) -> None: ...  # noqa: B027

    async def before_test_run(self, test: TestItem, engine: Engine): ...  # noqa: B027

    async def after_test_run(self, test: TestItem, engine: Engine): ...  # noqa: B027


class EngineHook(ABC):  # noqa: B024
    __slots__ = ()

    async def before_collect(  # noqa: B027
        self,
        path: Path,
        collector: Collector,
        engine: Engine,
    ): ...

    async def after_collect(  # noqa: B027
        self,
        path: Path,
        collector: Collector,
        engine: Engine,
    ): ...

    async def before_session_start(self, engine: Engine) -> None: ...  # noqa: B027

    async def after_session_finish(self, engine: Engine) -> None: ...  # noqa: B027

    async def before_test_run(self, test: TestItem, engine: Engine): ...  # noqa: B027

    async def after_test_run(self, test: TestItem, engine: Engine): ...  # noqa: B027
