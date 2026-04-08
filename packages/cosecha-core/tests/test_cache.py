from __future__ import annotations

import pickle

import pytest

from cosecha.core._version import __version__
from cosecha.core.cache import DiskCache


def test_disk_cache_load_save_clear_and_loaded_short_circuit(
    tmp_path,
) -> None:
    cache = DiskCache(tmp_path, 'engine-cache')

    assert cache.load() == {}

    cache.save({'value': 1})
    loaded = cache.load()
    assert loaded == {'value': 1}

    cache.clear()
    assert cache.load() == {}
    assert cache.cache_file.exists() is False


def test_disk_cache_migrates_legacy_location(tmp_path) -> None:
    legacy_dir = tmp_path / '.cosecha_cache'
    legacy_dir.mkdir(parents=True)
    legacy_file = legacy_dir / 'legacy.pickle'
    legacy_file.write_bytes(
        pickle.dumps({'version': __version__, 'data': {'from_legacy': True}}),
    )

    cache = DiskCache(tmp_path, 'legacy')

    assert cache.cache_file.exists()
    assert legacy_file.exists() is False
    assert cache.load() == {'from_legacy': True}


def test_disk_cache_ignores_legacy_migration_errors(tmp_path, monkeypatch) -> None:
    legacy_dir = tmp_path / '.cosecha_cache'
    legacy_dir.mkdir(parents=True)
    legacy_file = legacy_dir / 'broken-move.pickle'
    legacy_file.write_bytes(
        pickle.dumps({'version': __version__, 'data': {'value': 2}}),
    )

    def _raise_move(*args, **kwargs):
        del args, kwargs
        msg = 'move failed'
        raise OSError(msg)

    monkeypatch.setattr('cosecha.core.cache.shutil.move', _raise_move)

    cache = DiskCache(tmp_path, 'broken-move')

    assert cache.cache_file.exists() is False
    assert legacy_file.exists() is True


def test_disk_cache_invalidates_mismatched_versions(tmp_path) -> None:
    cache = DiskCache(tmp_path, 'versioned')
    cache.cache_dir.mkdir(parents=True, exist_ok=True)
    cache.cache_file.write_bytes(
        pickle.dumps({'version': 'old-version', 'data': {'stale': True}}),
    )

    assert cache.load() == {}


def test_disk_cache_load_handles_invalid_payload(tmp_path) -> None:
    cache = DiskCache(tmp_path, 'invalid')
    cache.cache_dir.mkdir(parents=True, exist_ok=True)
    cache.cache_file.write_text('not a pickle', encoding='utf-8')

    assert cache.load() == {}


def test_disk_cache_save_swallows_write_failures(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = DiskCache(tmp_path, 'write-error')

    def _raise_dump(*args, **kwargs):
        del args, kwargs
        msg = 'cannot dump'
        raise OSError(msg)

    monkeypatch.setattr('cosecha.core.cache.pickle.dump', _raise_dump)

    cache.save({'value': 3})

    assert cache._data == {'value': 3}
