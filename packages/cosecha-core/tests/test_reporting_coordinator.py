from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.config import Config
from cosecha.core.reporter import QueuedReporter, Reporter
from cosecha.core.reporting_coordinator import ReportingCoordinator
from cosecha.core.telemetry import InMemoryTelemetrySink, TelemetryStream


class _TelemetryAwareReporter(Reporter):
    __slots__ = ('print_calls', 'seen_streams')

    @classmethod
    def reporter_name(cls) -> str:
        return 'telemetry-aware'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'structured'

    def __init__(self) -> None:
        self.print_calls = 0
        self.seen_streams: list[object | None] = []

    async def add_test(self, test):
        del test

    async def add_test_result(self, test):
        del test

    async def print_report(self):
        self.print_calls += 1
        self.seen_streams.append(getattr(self, 'telemetry_stream', None))
        assert self.telemetry_stream is not None


@pytest.mark.asyncio
async def test_bind_telemetry_stream_reaches_wrapped_extra_reporters(
    tmp_path,
) -> None:
    config = Config(root_path=tmp_path)
    wrapped = _TelemetryAwareReporter()
    reporter = QueuedReporter(wrapped)
    reporter.initialize(config)
    coordinator = ReportingCoordinator((reporter,))
    telemetry_stream = TelemetryStream()
    sink = InMemoryTelemetrySink()
    telemetry_stream.add_sink(sink)

    coordinator.bind_telemetry_stream(telemetry_stream)

    await coordinator.finish_extra_reporters()

    assert wrapped.print_calls == 1
    assert wrapped.seen_streams == [telemetry_stream]
    assert [span.name for span in sink.spans] == [
        'reporter.print_report',
    ]


@pytest.mark.asyncio
async def test_rebinds_engine_reporters_initialized_before_telemetry(
    tmp_path,
) -> None:
    config = Config(root_path=tmp_path)
    wrapped = _TelemetryAwareReporter()
    reporter = QueuedReporter(wrapped)
    engine = SimpleNamespace(reporter=reporter)
    coordinator = ReportingCoordinator()
    telemetry_stream = TelemetryStream()
    sink = InMemoryTelemetrySink()
    telemetry_stream.add_sink(sink)

    coordinator.initialize_engine_reporter(config, engine)
    coordinator.bind_telemetry_stream(telemetry_stream)

    await coordinator.finish_engine_reporter(engine)

    assert wrapped.print_calls == 1
    assert wrapped.seen_streams == [telemetry_stream]
    assert [span.name for span in sink.spans] == [
        'reporter.print_report',
    ]
