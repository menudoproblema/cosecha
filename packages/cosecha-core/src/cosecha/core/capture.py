import contextlib
import contextvars
import logging

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class CapturedLogContext:
    session_id: str | None = None
    plan_id: str | None = None
    trace_id: str | None = None
    node_id: str | None = None
    node_stable_id: str | None = None
    worker_id: int | None = None


type LogEmitCallback = Callable[
    [logging.LogRecord, str, CapturedLogContext],
    None,
]


class CaptureLogHandler(logging.Handler):
    def __init__(self, level: int = logging.NOTSET):
        super().__init__(level)
        self.captured_logs: list[str] = []
        self._emit_callback: LogEmitCallback | None = None
        self._live_context: contextvars.ContextVar[
            CapturedLogContext | None
        ] = contextvars.ContextVar('cosecha_capture_log_context', default=None)

    def set_emit_callback(
        self,
        callback: LogEmitCallback | None,
    ) -> None:
        self._emit_callback = callback

    @contextmanager
    def bind_live_context(
        self,
        context: CapturedLogContext | None,
    ):
        token = self._live_context.set(context)
        try:
            yield
        finally:
            self._live_context.reset(token)

    def emit(self, record: logging.LogRecord):
        with contextlib.suppress(Exception):
            msg = self.format(record)
            self.captured_logs.append(msg)
            live_context = self._live_context.get()
            if live_context is not None and self._emit_callback is not None:
                self._emit_callback(record, msg, live_context)
