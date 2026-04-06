from __future__ import annotations

import asyncio

import pytest

from cosecha.core.event_bus import AsyncEventBus


@pytest.mark.asyncio
async def test_async_event_bus_flush_waits_for_pending_events() -> None:
    consumed: list[str] = []
    consumer_entered = asyncio.Event()
    release_consumer = asyncio.Event()

    async def consume(event: str) -> None:
        consumer_entered.set()
        await release_consumer.wait()
        consumed.append(event)

    bus = AsyncEventBus(consume)
    await bus.start()
    await bus.publish('one')
    await asyncio.wait_for(consumer_entered.wait(), 1.0)

    flush_task = asyncio.create_task(bus.flush())
    await asyncio.sleep(0)

    assert flush_task.done() is False

    release_consumer.set()
    await flush_task

    assert consumed == ['one']
    await bus.close()


@pytest.mark.asyncio
async def test_async_event_bus_raises_consumer_errors_on_flush() -> None:
    async def consume(_event: str) -> None:
        msg = 'boom'
        raise RuntimeError(msg)

    bus = AsyncEventBus(consume)
    await bus.start()
    await bus.publish('one')

    with pytest.raises(RuntimeError, match='boom'):
        await bus.flush()

    with pytest.raises(RuntimeError, match='boom'):
        await bus.close()


@pytest.mark.asyncio
async def test_async_event_bus_applies_queue_backpressure() -> None:
    consumer_entered = asyncio.Event()
    release_consumer = asyncio.Event()

    async def consume(_event: str) -> None:
        consumer_entered.set()
        await release_consumer.wait()

    bus = AsyncEventBus(consume, maxsize=1)
    await bus.start()
    await bus.publish('one')
    await asyncio.wait_for(consumer_entered.wait(), 1.0)
    await bus.publish('two')

    publish_task = asyncio.create_task(bus.publish('three'))
    await asyncio.sleep(0)

    assert publish_task.done() is False

    release_consumer.set()
    await asyncio.wait_for(publish_task, 1.0)
    await bus.close()
