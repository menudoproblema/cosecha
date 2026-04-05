from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.domain_events import DomainEvent


class DomainEventSink:
    async def start(self) -> None: ...

    async def emit(self, event: DomainEvent) -> None: ...

    async def close(self) -> None: ...


class DomainEventStream:
    __slots__ = ('_closed', '_next_sequence_number', '_sinks')

    def __init__(self) -> None:
        self._sinks: list[DomainEventSink] = []
        self._closed = False
        self._next_sequence_number = 1

    def add_sink(self, sink: DomainEventSink) -> None:
        self._sinks.append(sink)

    async def emit(self, event: DomainEvent) -> None:
        if self._closed:
            return

        metadata = getattr(event, 'metadata', None)
        if metadata is not None and metadata.sequence_number is None:
            object.__setattr__(
                metadata,
                'sequence_number',
                self._next_sequence_number,
            )
            self._next_sequence_number += 1

        tasks: list[asyncio.Task[None]] = []
        try:
            tasks.extend(
                asyncio.create_task(sink.emit(event)) for sink in self._sinks
            )

            if tasks:
                await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        tasks: list[asyncio.Task[None]] = []
        try:
            tasks.extend(
                asyncio.create_task(sink.close()) for sink in self._sinks
            )

            if tasks:
                await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            raise


class InMemoryDomainEventSink(DomainEventSink):
    __slots__ = ('events',)

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def emit(self, event: DomainEvent) -> None:
        self.events.append(event)
