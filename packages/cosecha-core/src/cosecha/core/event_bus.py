from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Awaitable, Callable


DEFAULT_ASYNC_EVENT_BUS_MAXSIZE = 1024


class AsyncEventBus[EventT]:
    __slots__ = (
        '_consumer',
        '_first_error',
        '_maxsize',
        '_queue',
        '_worker_task',
    )

    def __init__(
        self,
        consumer: Callable[[EventT], Awaitable[None]],
        *,
        maxsize: int = DEFAULT_ASYNC_EVENT_BUS_MAXSIZE,
    ) -> None:
        self._consumer = consumer
        self._maxsize = maxsize
        self._queue: asyncio.Queue[EventT | None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._first_error: Exception | None = None

    async def start(self) -> None:
        self._first_error = None
        self._queue = asyncio.Queue(maxsize=self._maxsize)
        self._worker_task = asyncio.create_task(self._consume())

    async def publish(self, event: EventT) -> None:
        self._raise_if_failed()
        if self._queue is None:
            msg = 'AsyncEventBus.start() must run before publishing events'
            raise RuntimeError(msg)

        await self._queue.put(event)

    async def flush(self) -> None:
        if self._queue is None:
            return

        await self._queue.join()
        self._raise_if_failed()

    async def close(self) -> None:
        if self._queue is None or self._worker_task is None:
            return

        await self._queue.put(None)
        await self._worker_task
        self._queue = None
        self._worker_task = None
        self._raise_if_failed()

    async def _consume(self) -> None:
        if self._queue is None:
            return

        while True:
            event = await self._queue.get()
            try:
                if event is None:
                    return

                if self._first_error is None:
                    await self._consumer(event)
            except Exception as error:  # pragma: no cover - via integration
                if self._first_error is None:
                    self._first_error = error
            finally:
                self._queue.task_done()

    def _raise_if_failed(self) -> None:
        if self._first_error is not None:
            raise self._first_error
