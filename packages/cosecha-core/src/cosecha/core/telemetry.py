from __future__ import annotations

import asyncio
import time

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import uuid4

from cosecha.core.event_bus import AsyncEventBus
from cosecha.core.serialization import encode_json_text_lossy


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import AsyncIterator
    from io import TextIOWrapper
    from pathlib import Path


@dataclass(slots=True, frozen=True)
class TelemetrySpan:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    start_time: float
    end_time: float
    attributes: dict[str, object] = field(default_factory=dict)

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time


class TelemetrySink:
    async def start(self) -> None: ...

    async def emit(self, span: TelemetrySpan) -> None: ...

    async def flush(self) -> None: ...

    async def close(self) -> None: ...


class TelemetryStream:
    __slots__ = (
        '_closed',
        '_emitted_span_count',
        '_sinks',
        '_span_name_counts',
        '_trace_id',
    )

    def __init__(self) -> None:
        self._sinks: list[TelemetrySink] = []
        self._trace_id: str | None = None
        self._closed = False
        self._emitted_span_count = 0
        self._span_name_counts: dict[str, int] = {}

    def add_sink(self, sink: TelemetrySink) -> None:
        self._sinks.append(sink)

    @property
    def trace_id(self) -> str:
        return self._get_trace_id()

    @asynccontextmanager
    async def span(
        self,
        name: str,
        *,
        parent_span_id: str | None = None,
        attributes: dict[str, object] | None = None,
    ) -> AsyncIterator[str]:
        if self._closed or not self._sinks:
            yield ''
            return

        start_time = time.perf_counter()
        span_id = uuid4().hex

        try:
            yield span_id
        finally:
            span = TelemetrySpan(
                trace_id=self._get_trace_id(),
                span_id=span_id,
                parent_span_id=parent_span_id,
                name=name,
                start_time=start_time,
                end_time=time.perf_counter(),
                attributes=attributes or {},
            )
            await self.emit(span)

    async def emit(self, span: TelemetrySpan) -> None:
        if self._closed or not self._sinks:
            return

        self._emitted_span_count += 1
        self._span_name_counts[span.name] = (
            self._span_name_counts.get(span.name, 0) + 1
        )

        await asyncio.gather(
            *(sink.emit(span) for sink in self._sinks),
        )

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        await asyncio.gather(
            *(sink.close() for sink in self._sinks),
        )

    async def flush(self) -> None:
        if self._closed or not self._sinks:
            return

        await asyncio.gather(
            *(sink.flush() for sink in self._sinks),
        )

    def _get_trace_id(self) -> str:
        if self._trace_id is None:
            self._trace_id = uuid4().hex

        return self._trace_id

    def summary(
        self,
        *,
        max_span_names: int = 10,
    ) -> dict[str, object]:
        top_span_names = tuple(
            sorted(
                self._span_name_counts.items(),
                key=lambda item: (item[1], item[0]),
                reverse=True,
            )[:max_span_names],
        )
        return {
            'distinct_span_names': len(self._span_name_counts),
            'span_count': self._emitted_span_count,
            'top_span_names': top_span_names,
        }


class InMemoryTelemetrySink(TelemetrySink):
    __slots__ = ('spans',)

    def __init__(self) -> None:
        self.spans: list[TelemetrySpan] = []

    async def emit(self, span: TelemetrySpan) -> None:
        self.spans.append(span)

    async def flush(self) -> None:
        return None


class JsonlTelemetrySink(TelemetrySink):
    __slots__ = ('_bus', '_file', '_path')

    def __init__(self, path: Path) -> None:
        self._path = path
        self._file: TextIOWrapper | None = None
        self._bus = AsyncEventBus(self._write_span)

    async def start(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._path.open('w', encoding='utf-8')
        await self._bus.start()

    async def emit(self, span: TelemetrySpan) -> None:
        await self._bus.publish(span)

    async def flush(self) -> None:
        if self._file is None:
            return

        await self._bus.flush()

    async def close(self) -> None:
        if self._file is None:
            return

        await self.flush()
        await self._bus.close()
        await asyncio.to_thread(self._file.close)
        self._file = None

    async def _write_span(self, span: TelemetrySpan) -> None:
        if self._file is None:
            msg = 'JsonlTelemetrySink.start() must run before emitting spans'
            raise RuntimeError(msg)

        payload = {
            'attributes': span.attributes,
            'duration': span.duration,
            'end_time': span.end_time,
            'name': span.name,
            'parent_span_id': span.parent_span_id,
            'span_id': span.span_id,
            'start_time': span.start_time,
            'trace_id': span.trace_id,
        }
        line = encode_json_text_lossy(payload)
        await asyncio.to_thread(self._write_line, line)

    def _write_line(self, line: str) -> None:
        if self._file is None:
            msg = 'JsonlTelemetrySink.start() must run before writing spans'
            raise RuntimeError(msg)

        self._file.write(f'{line}\n')
