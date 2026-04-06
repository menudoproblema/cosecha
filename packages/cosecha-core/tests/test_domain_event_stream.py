from __future__ import annotations

import asyncio

import pytest

from cosecha.core.domain_event_stream import (
    DomainEventSink,
    DomainEventStream,
    InMemoryDomainEventSink,
)
from cosecha.core.domain_events import (
    DomainEventMetadata,
    TestStartedEvent as StartedEvent,
)


EXISTING_SEQUENCE_NUMBER = 7


class RecordingSink(DomainEventSink):
    def __init__(self) -> None:
        self.events: list[object] = []
        self.closed = False

    async def emit(self, event) -> None:
        self.events.append(event)

    async def close(self) -> None:
        self.closed = True


def test_domain_event_stream_assigns_sequence_numbers_and_fan_outs() -> None:
    stream = DomainEventStream()
    first_sink = RecordingSink()
    second_sink = InMemoryDomainEventSink()
    stream.add_sink(first_sink)
    stream.add_sink(second_sink)
    event = StartedEvent(
        node_id='node-1',
        node_stable_id='stable-1',
        engine_name='gherkin',
        test_name='Scenario: auth',
        test_path='features/auth.feature',
        metadata=DomainEventMetadata(sequence_number=None),
    )

    asyncio.run(stream.emit(event))

    assert event.metadata.sequence_number == 1
    assert first_sink.events == [event]
    assert second_sink.events == [event]


def test_domain_event_stream_preserves_existing_sequence_number() -> None:
    stream = DomainEventStream()
    sink = RecordingSink()
    stream.add_sink(sink)
    event = StartedEvent(
        node_id='node-1',
        node_stable_id='stable-1',
        engine_name='gherkin',
        test_name='Scenario: auth',
        test_path='features/auth.feature',
        metadata=DomainEventMetadata(
            sequence_number=EXISTING_SEQUENCE_NUMBER,
        ),
    )

    asyncio.run(stream.emit(event))

    assert event.metadata.sequence_number == EXISTING_SEQUENCE_NUMBER
    assert sink.events == [event]


def test_domain_event_stream_cancels_peer_sinks_on_failure() -> None:
    stream = DomainEventStream()
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    class BlockingSink(DomainEventSink):
        async def emit(self, event) -> None:
            del event
            entered.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    class FailingSink(DomainEventSink):
        async def emit(self, event) -> None:
            del event
            msg = 'sink boom'
            raise RuntimeError(msg)

    stream.add_sink(BlockingSink())
    stream.add_sink(FailingSink())
    event = StartedEvent(
        node_id='node-1',
        node_stable_id='stable-1',
        engine_name='gherkin',
        test_name='Scenario: auth',
        test_path='features/auth.feature',
    )

    async def _run() -> None:
        task = asyncio.create_task(stream.emit(event))
        await asyncio.wait_for(entered.wait(), 1.0)
        with pytest.raises(RuntimeError, match='sink boom'):
            await task

    asyncio.run(_run())

    assert cancelled.is_set()


def test_domain_event_stream_close_closes_sinks_and_ignores_future_emits(
) -> None:
    stream = DomainEventStream()
    sink = RecordingSink()
    stream.add_sink(sink)

    asyncio.run(stream.close())
    asyncio.run(
        stream.emit(
            StartedEvent(
                node_id='node-1',
                node_stable_id='stable-1',
                engine_name='gherkin',
                test_name='Scenario: auth',
                test_path='features/auth.feature',
            ),
        ),
    )

    assert sink.closed is True
    assert sink.events == []
