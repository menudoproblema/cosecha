from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.config import Config
    from cosecha.core.engines import Engine
    from cosecha.core.items import TestItem
    from cosecha.core.reporter import Reporter, ReportSubject
    from cosecha.core.telemetry import TelemetryStream


class ReportingCoordinator:
    __slots__ = ('_engine_reporters', '_extra_reporters', '_telemetry_stream')

    def __init__(
        self,
        extra_reporters: tuple[Reporter, ...] = (),
    ) -> None:
        self._engine_reporters: list[Reporter] = []
        self._extra_reporters = extra_reporters
        self._telemetry_stream: TelemetryStream | None = None

    def bind_telemetry_stream(
        self,
        telemetry_stream: TelemetryStream,
    ) -> None:
        self._telemetry_stream = telemetry_stream
        for reporter in (*self._extra_reporters, *self._engine_reporters):
            reporter.bind_telemetry_stream(telemetry_stream)

    def initialize_engine_reporter(
        self,
        config: Config,
        engine: Engine,
    ) -> None:
        engine.reporter.initialize(config, engine)
        if not any(
            reporter is engine.reporter
            for reporter in self._engine_reporters
        ):
            self._engine_reporters.append(engine.reporter)
        if self._telemetry_stream is not None:
            engine.reporter.bind_telemetry_stream(self._telemetry_stream)

    async def start_extra_reporters(self) -> None:
        await asyncio.gather(
            *(
                self._start_reporter(reporter)
                for reporter in self._extra_reporters
            ),
        )

    async def finish_extra_reporters(self) -> None:
        await asyncio.gather(
            *(
                self._print_reporter_report(reporter)
                for reporter in self._extra_reporters
            ),
        )

    async def start_engine_reporter(self, engine: Engine) -> None:
        await self._start_reporter(engine.reporter)

    async def finish_engine_reporter(self, engine: Engine) -> None:
        await self._print_reporter_report(engine.reporter)

    async def record_engine_test_start(
        self,
        engine: Engine,
        test: TestItem,
    ) -> None:
        await self._reporter_add_test(engine.reporter, test)

    async def record_engine_test_result(
        self,
        engine: Engine,
        report_subject: ReportSubject,
    ) -> None:
        await self._reporter_add_test_result(engine.reporter, report_subject)

    async def record_extra_test_result(
        self,
        report_subject: ReportSubject,
    ) -> None:
        await asyncio.gather(
            *(
                self._reporter_add_test_result(reporter, report_subject)
                for reporter in self._extra_reporters
            ),
        )

    async def _start_reporter(self, reporter: Reporter) -> None:
        if self._telemetry_stream is None:
            await reporter.start()
            return

        async with self._telemetry_stream.span(
            'reporter.start',
            attributes=_reporter_telemetry_attributes(reporter),
        ):
            await reporter.start()

    async def _print_reporter_report(self, reporter: Reporter) -> None:
        if self._telemetry_stream is None:
            await reporter.print_report()
            return

        async with self._telemetry_stream.span(
            'reporter.print_report',
            attributes=_reporter_telemetry_attributes(reporter),
        ):
            await reporter.print_report()

    async def _reporter_add_test(
        self,
        reporter: Reporter,
        test: TestItem,
    ) -> None:
        if self._telemetry_stream is None:
            await reporter.add_test(test)
            return

        async with self._telemetry_stream.span(
            'reporter.add_test',
            attributes=_reporter_telemetry_attributes(reporter),
        ):
            await reporter.add_test(test)

    async def _reporter_add_test_result(
        self,
        reporter: Reporter,
        report_subject: ReportSubject,
    ) -> None:
        if self._telemetry_stream is None:
            await reporter.add_test_result(report_subject)
            return

        async with self._telemetry_stream.span(
            'reporter.add_test_result',
            attributes=_reporter_telemetry_attributes(reporter),
        ):
            await reporter.add_test_result(report_subject)


def _reporter_telemetry_attributes(reporter: Reporter) -> dict[str, object]:
    descriptor_reporter = reporter.descriptor_target()
    return {
        'cosecha.reporter.name': descriptor_reporter.reporter_name(),
        'cosecha.reporter.output_kind': (
            descriptor_reporter.reporter_output_kind()
        ),
    }
