from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.config import Config
    from cosecha.core.engines import Engine
    from cosecha.core.items import TestItem
    from cosecha.core.reporter import Reporter, ReportSubject


class ReportingCoordinator:
    __slots__ = ('_extra_reporters',)

    def __init__(
        self,
        extra_reporters: tuple[Reporter, ...] = (),
    ) -> None:
        self._extra_reporters = extra_reporters

    def initialize_engine_reporter(
        self,
        config: Config,
        engine: Engine,
    ) -> None:
        engine.reporter.initialize(config, engine)

    async def start_extra_reporters(self) -> None:
        await asyncio.gather(
            *(reporter.start() for reporter in self._extra_reporters),
        )

    async def finish_extra_reporters(self) -> None:
        await asyncio.gather(
            *(reporter.print_report() for reporter in self._extra_reporters),
        )

    async def start_engine_reporter(self, engine: Engine) -> None:
        await engine.reporter.start()

    async def finish_engine_reporter(self, engine: Engine) -> None:
        await engine.reporter.print_report()

    async def record_engine_test_start(
        self,
        engine: Engine,
        test: TestItem,
    ) -> None:
        await engine.reporter.add_test(test)

    async def record_engine_test_result(
        self,
        engine: Engine,
        report_subject: ReportSubject,
    ) -> None:
        await engine.reporter.add_test_result(report_subject)

    async def record_extra_test_result(
        self,
        report_subject: ReportSubject,
    ) -> None:
        await asyncio.gather(
            *(
                reporter.add_test_result(report_subject)
                for reporter in self._extra_reporters
            ),
        )
