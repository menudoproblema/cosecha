from __future__ import annotations

import logging
import pickle
import shutil

from contextlib import suppress
from typing import TYPE_CHECKING, Any

from cosecha.core._version import __version__


if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


class DiskCache:
    def __init__(self, root_path: Path, name: str):
        self.cache_dir = root_path / '.cosecha' / 'cache'
        self.cache_file = self.cache_dir / f'{name}.pickle'
        self._legacy_cache_dir = root_path / '.cosecha_cache'
        self._legacy_cache_file = self._legacy_cache_dir / f'{name}.pickle'
        self._data: dict[Any, Any] = {}
        self._loaded = False
        self._migrate_legacy_cache()

    def _migrate_legacy_cache(self) -> None:
        if self.cache_file.exists() or not self._legacy_cache_file.exists():
            return

        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(self._legacy_cache_file), str(self.cache_file))
        except Exception as e:
            logger.debug(
                'Failed to migrate legacy disk cache %s -> %s: %s',
                self._legacy_cache_file,
                self.cache_file,
                e,
            )

    def load(self) -> dict[Any, Any]:
        if self._loaded:
            return self._data

        if not self.cache_file.exists():
            self._loaded = True
            return self._data

        try:
            with self.cache_file.open('rb') as f:
                # NOTE: We use pickle for speed and simplicity.
                # Only trusted local data is loaded from .cosecha/cache.
                payload = pickle.load(f)  # noqa: S301

                # Invalida cache si la versión del framework ha cambiado
                if payload.get('version') != __version__:
                    logger.debug('Cache version mismatch, ignoring disk cache')
                    self._loaded = True
                    return self._data

                self._data = payload.get('data', {})
        except Exception as e:
            logger.debug(f'Failed to load disk cache {self.cache_file}: {e}')
            self._data = {}

        self._loaded = True
        return self._data

    def save(self, data: dict[Any, Any]):
        self._data = data
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                'version': __version__,
                'data': self._data,
            }
            with self.cache_file.open('wb') as f:
                pickle.dump(payload, f)
        except Exception as e:
            logger.debug(f'Failed to save disk cache {self.cache_file}: {e}')

    def clear(self):
        self._data = {}
        if self.cache_file.exists():
            with suppress(Exception):
                self.cache_file.unlink()
