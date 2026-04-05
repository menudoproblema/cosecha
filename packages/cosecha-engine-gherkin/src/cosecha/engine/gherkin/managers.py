from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, NotRequired, TypedDict, Unpack, override
from unittest.mock import MagicMock, Mock, patch


class BaseContextManager(ABC):
    @abstractmethod
    def cleanup(self) -> None: ...


class PatchKwargs(TypedDict, total=False):
    new: NotRequired[Mock | MagicMock | Any]
    spec: NotRequired[Any]
    create: NotRequired[bool]
    autospec: NotRequired[bool | Any]
    side_effect: NotRequired[Callable[..., Any] | list[Any] | Exception]


class PatchContextManager(BaseContextManager):
    def __init__(self):
        self._patches: dict[str, Any] = {}

    def add_patch(self, target: str, **kwargs: Unpack[PatchKwargs]):
        if target in self._patches:
            msg = f'Duplicated patch target "{target}"'
            raise ValueError(msg)
        mock = patch(target, **kwargs)
        started_mock = mock.start()
        self._patches[target] = started_mock

        return started_mock

    def stop_patch(self, target: str) -> None:
        if target not in self._patches:
            msg = f'Patch target "{target}" not found'
            raise ValueError(msg)

        mock = self._patches.pop(target)
        mock.stop()

    @override
    def cleanup(self):
        """Detiene y limpia todos los patches registrados."""
        for mock in self._patches.values():
            mock.stop()
        self._patches.clear()


class TempPathManager(BaseContextManager):
    def __init__(self):
        self._temp_dir: TemporaryDirectory[str] | None = None
        self._path: Path | None = None

    def get_path(self) -> Path:
        if self._temp_dir is None:
            self._temp_dir = TemporaryDirectory(prefix='cosecha-')
            self._path = Path(self._temp_dir.name).resolve()

        return self._path  # type: ignore[return-value]

    @override
    def cleanup(self):
        """Elimina el directorio temporal y sus contenidos."""
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None
            self._path = None
