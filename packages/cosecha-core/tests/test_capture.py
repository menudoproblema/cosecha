from __future__ import annotations

import logging

from cosecha.core.capture import CaptureLogHandler, CapturedLogContext


def test_capture_log_handler_records_messages_and_emits_live_context() -> None:
    handler = CaptureLogHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s:%(message)s'))
    captured: list[tuple[str, CapturedLogContext]] = []

    def _on_emit(record, message: str, context: CapturedLogContext) -> None:
        del record
        captured.append((message, context))

    handler.set_emit_callback(_on_emit)
    logger = logging.getLogger('cosecha.test.capture')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    try:
        with handler.bind_live_context(
            CapturedLogContext(
                session_id='session-1',
                node_id='node-1',
                worker_id=2,
            ),
        ):
            logger.info('hello')
        logger.info('world')
    finally:
        logger.removeHandler(handler)

    assert handler.captured_logs == ['INFO:hello', 'INFO:world']
    assert captured == [
        (
            'INFO:hello',
            CapturedLogContext(
                session_id='session-1',
                node_id='node-1',
                worker_id=2,
            ),
        ),
    ]


def test_capture_log_handler_suppresses_emit_failures() -> None:
    handler = CaptureLogHandler()
    logger = logging.getLogger('cosecha.test.capture.suppress')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    class BrokenFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:  # noqa: D401
            del record
            msg = 'format exploded'
            raise RuntimeError(msg)

    handler.setFormatter(BrokenFormatter())
    try:
        logger.info('ignored')
    finally:
        logger.removeHandler(handler)

    assert handler.captured_logs == []
