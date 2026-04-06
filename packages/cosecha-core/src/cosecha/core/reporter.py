from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from cosecha.core.event_bus import AsyncEventBus
from cosecha.core.reporting_ir import TestReport


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.config import Config
    from cosecha.core.engines.base import Engine
    from cosecha.core.items import TestItem


type ReportSubject = TestItem | TestReport


class Reporter(ABC):
    __slots__ = ('config', 'console', 'engine', 'telemetry_stream')

    @classmethod
    def reporter_api_version(cls) -> int:
        return 1

    @classmethod
    def reporter_name(cls) -> str:
        return cls.__name__

    @classmethod
    def reporter_stability(cls) -> str:
        return 'stable'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'other'

    def descriptor_target(self) -> Reporter:
        return self

    def initialize(
        self,
        config: Config,
        engine: Engine | None = None,
    ) -> None:
        self.config = config
        self.console = self.config.console
        self.engine = engine
        self.telemetry_stream = None

    def bind_telemetry_stream(self, telemetry_stream) -> None:
        self.telemetry_stream = telemetry_stream

    async def start(self): ...  # noqa: B027

    async def finish(self): ...  # noqa: B027

    @abstractmethod
    async def add_test(self, test: TestItem): ...

    @abstractmethod
    async def add_test_result(self, test: ReportSubject): ...

    @abstractmethod
    async def print_report(self): ...


class NullReporter(Reporter):
    __slots__ = ()

    async def add_test(self, test: TestItem):
        del test

    async def add_test_result(self, test: ReportSubject):
        del test

    async def print_report(self):
        return None


@dataclass(slots=True, frozen=True)
class ReporterEvent:
    method: Literal['add_test', 'add_test_result']
    test: ReportSubject


class QueuedReporter(Reporter):
    __slots__ = ('_bus', '_queue_add_test', '_wrapped')

    def __init__(
        self,
        wrapped: Reporter,
        *,
        queue_add_test: bool = False,
    ) -> None:
        self._wrapped = wrapped
        self._queue_add_test = queue_add_test
        self._bus = AsyncEventBus(self._consume_event)

    def initialize(
        self,
        config: Config,
        engine: Engine | None = None,
    ) -> None:
        super().initialize(config, engine)
        self._wrapped.initialize(config, engine)

    def bind_telemetry_stream(self, telemetry_stream) -> None:
        super().bind_telemetry_stream(telemetry_stream)
        self._wrapped.bind_telemetry_stream(telemetry_stream)

    async def start(self) -> None:
        await self._wrapped.start()
        await self._bus.start()

    async def finish(self) -> None:
        await self._bus.flush()
        await self._bus.close()
        await self._wrapped.finish()

    async def add_test(self, test: TestItem):
        if self._queue_add_test:
            await self._enqueue('add_test', test)
            return

        await self._wrapped.add_test(test)

    async def add_test_result(self, test: ReportSubject):
        await self._enqueue('add_test_result', test)

    async def print_report(self):
        await self._bus.flush()
        await self._bus.close()
        await self._wrapped.print_report()

    def descriptor_target(self) -> Reporter:
        return self._wrapped.descriptor_target()

    def with_wrapped(self, wrapped: Reporter) -> QueuedReporter:
        return QueuedReporter(
            wrapped,
            queue_add_test=self._queue_add_test,
        )

    async def _enqueue(
        self,
        method: Literal['add_test', 'add_test_result'],
        test: ReportSubject,
    ) -> None:
        await self._bus.publish(ReporterEvent(method, test))

    async def _consume_event(self, event: ReporterEvent) -> None:
        await getattr(self._wrapped, event.method)(event.test)
