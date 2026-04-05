from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path


class CosechaException(Exception): ...  # noqa: N818


class Skipped(CosechaException):
    def __init__(self, reason: str = '') -> None:
        self.reason = reason
        super().__init__(reason)


class CosechaParserError(CosechaException):
    def __init__(
        self,
        reason: str,
        filename: Path,
        line: int,
        column: int,
    ) -> None:
        self.reason = reason
        self.filename = filename
        self.line = line
        self.column = column
        msg = f'Error en {filename}:{line}:{column} - {reason}'
        super().__init__(msg)
