from __future__ import annotations

import json

from types import SimpleNamespace

import pytest

from cosecha.core.resources import ResourceError, ResourceRequirement
from cosecha.provider.mongodb import (
    MongoResourceConfig,
    MongoResourceHandle,
    MongoResourceProvider,
    provider as mongo_provider_module,
)


MONGO_DB_ERROR_MESSAGE = 'db error'


def _build_requirement(
    *,
    config: dict[str, object],
    mode: str = 'ephemeral',
    scope: str = 'test',
) -> ResourceRequirement:
    return ResourceRequirement(
        name='mongo',
        provider=MongoResourceProvider(),
        scope=scope,  # type: ignore[arg-type]
        mode=mode,  # type: ignore[arg-type]
        config=config,
    )


class _FakeDatabase:
    def __init__(self, name: str = 'demo') -> None:
        self.name = name
        self.collections: dict[str, object] = {}

    def __getitem__(self, collection_name: str):
        return self.collections[collection_name]


class _FakeMongoClient:
    def __init__(self, database: _FakeDatabase) -> None:
        self._database = database
        self.admin = SimpleNamespace(command=lambda _name: {'ok': 1})

    def __getitem__(self, _database_name: str) -> _FakeDatabase:
        return self._database

    def close(self) -> None:
        return


def test_provider_mode_and_guard_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(config={})
    monkeypatch.setattr(
        mongo_provider_module,
        '_require_mongo_runtime_dependencies',
        lambda: None,
    )

    assert provider.supports_mode('live') is True
    assert provider.supports_mode('ephemeral') is True
    assert provider.supports_mode('dry_run') is False
    assert provider.supported_initialization_modes(requirement) == (
        'data_seed',
        'state_snapshot',
    )
    assert (
        provider.health_check(object(), requirement, mode='ephemeral') is False
    )
    assert (
        provider.verify_integrity(object(), requirement, mode='ephemeral')
        is False
    )
    assert (
        provider.describe_external_handle(
            object(),
            requirement,
            mode='ephemeral',
        )
        is None
    )
    provider.release(object(), requirement, mode='ephemeral')
    provider.revoke_orphan_access('handle', requirement, mode='ephemeral')

    with pytest.raises(
        TypeError, match='snapshot requires MongoResourceHandle',
    ):
        provider.snapshot_materialization(
            object(), requirement, mode='ephemeral',
        )
    with pytest.raises(TypeError, match='requires MongoResourceHandle target'):
        provider.initialize_from(
            object(),
            requirement,
            {'seed': object()},
            mode='ephemeral',
            initialization_mode='data_seed',
        )


def test_handle_name_getattr_and_snapshot_non_mock() -> None:
    database = _FakeDatabase('suite_db')
    database.some_attr = 'value'
    client = _FakeMongoClient(database)
    handle = MongoResourceHandle(
        client=client,
        database_name='suite_db',
        backend='live',
        mongoeco_engine=None,
        sqlite_path=None,
        uri='mongodb://localhost:27017',
        cleanup_policy='preserve',
        generated_database=False,
    )
    provider = MongoResourceProvider()
    requirement = _build_requirement(config={})

    assert handle.name == 'suite_db'
    assert handle.some_attr == 'value'
    snapshot = provider.snapshot_materialization(
        handle,
        requirement,
        mode='ephemeral',
    )
    assert snapshot == {
        'backend': 'live',
        'cleanup_policy': 'preserve',
        'database_name': 'suite_db',
        'generated_database': False,
        'uri': 'mongodb://localhost:27017',
    }


def test_health_and_verify_integrity_handle_pymongo_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(config={})
    database = _FakeDatabase('suite_db')

    class _FakePyMongoError(Exception):
        pass

    client = _FakeMongoClient(database)
    client.admin = SimpleNamespace(
        command=lambda _name: (_ for _ in ()).throw(
            _FakePyMongoError('ping failed'),
        ),
    )
    handle = MongoResourceHandle(
        client=client,
        database_name='suite_db',
        backend='mock',
        mongoeco_engine='memory',
        sqlite_path=None,
        uri=None,
        cleanup_policy='drop',
        generated_database=True,
    )
    monkeypatch.setattr(
        mongo_provider_module,
        '_require_mongo_runtime_dependencies',
        lambda: None,
    )
    monkeypatch.setattr(
        mongo_provider_module, 'PyMongoError', _FakePyMongoError,
    )

    healthy_handle = MongoResourceHandle(
        client=_FakeMongoClient(_FakeDatabase('suite_db')),
        database_name='suite_db',
        backend='mock',
        mongoeco_engine='memory',
        sqlite_path=None,
        uri=None,
        cleanup_policy='drop',
        generated_database=True,
    )
    assert (
        provider.health_check(healthy_handle, requirement, mode='ephemeral')
        is True
    )
    assert (
        provider.health_check(handle, requirement, mode='ephemeral') is False
    )

    database_error = _FakeDatabase('suite_db')

    class _BrokenClient(_FakeMongoClient):
        def __getitem__(self, _database_name: str):
            raise _FakePyMongoError(MONGO_DB_ERROR_MESSAGE)

    broken_handle = MongoResourceHandle(
        client=_BrokenClient(database_error),
        database_name='suite_db',
        backend='mock',
        mongoeco_engine='memory',
        sqlite_path=None,
        uri=None,
        cleanup_policy='drop',
        generated_database=True,
    )
    assert (
        provider.verify_integrity(broken_handle, requirement, mode='ephemeral')
        is False
    )


def test_rehydrate_live_materialization_uses_mongo_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(config={})
    calls: list[tuple[str, int]] = []
    fake_client = object()
    monkeypatch.setattr(
        mongo_provider_module,
        '_require_mongo_runtime_dependencies',
        lambda: None,
    )
    monkeypatch.setattr(
        mongo_provider_module,
        '_default_server_selection_timeout_ms',
        lambda: 1234,
    )
    monkeypatch.setattr(
        mongo_provider_module,
        'MongoClient',
        lambda uri, **kwargs: (
            calls.append((uri, kwargs['serverSelectionTimeoutMS']))
            or fake_client
        ),
    )

    handle = provider.rehydrate_materialization(
        {
            'backend': 'live',
            'database_name': 'suite_db',
            'cleanup_policy': 'preserve',
            'generated_database': True,
            'uri': 'mongodb://live-host:27017',
        },
        requirement,
        mode='live',
    )

    assert calls == [('mongodb://live-host:27017', 1234)]
    assert handle.client is fake_client
    assert handle.borrowed is True


def test_initialize_from_requires_handle_sources_for_data_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(config={})
    target_database = _FakeDatabase('target')
    target_client = _FakeMongoClient(target_database)
    target_client.drop_database = lambda _name: None
    target = MongoResourceHandle(
        client=target_client,
        database_name='target',
        backend='mock',
        mongoeco_engine='memory',
        sqlite_path=None,
        uri=None,
        cleanup_policy='drop',
        generated_database=True,
    )
    monkeypatch.setattr(
        mongo_provider_module,
        '_require_mongo_runtime_dependencies',
        lambda: None,
    )

    with pytest.raises(
        TypeError, match='data_seed initialization requires live',
    ):
        provider.initialize_from(
            target,
            requirement,
            {'seed': object()},
            mode='ephemeral',
            initialization_mode='data_seed',
        )

    source_database = _FakeDatabase('source')
    target_database.collections['items'] = SimpleNamespace()
    source_handle = MongoResourceHandle(
        client=_FakeMongoClient(source_database),
        database_name='source',
        backend='mock',
        mongoeco_engine='memory',
        sqlite_path=None,
        uri=None,
        cleanup_policy='drop',
        generated_database=True,
    )
    clone_calls: list[tuple[object, object]] = []
    monkeypatch.setattr(
        mongo_provider_module,
        '_clone_database',
        lambda source, target_db: clone_calls.append((source, target_db)),
    )
    provider.initialize_from(
        target,
        requirement,
        {'seed': source_handle},
        mode='ephemeral',
        initialization_mode='data_seed',
    )
    assert len(clone_calls) == 1


def test_reap_orphan_drops_live_database_and_cleans_tempdir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(config={})
    drop_calls: list[str] = []
    close_calls: list[str] = []
    terminated_pids: list[int] = []
    tempdir = tmp_path / 'mongo-temp'
    tempdir.mkdir()

    class _Client:
        def drop_database(self, name: str) -> None:
            drop_calls.append(name)

        def close(self) -> None:
            close_calls.append('closed')

    monkeypatch.setattr(
        mongo_provider_module,
        '_require_mongo_runtime_dependencies',
        lambda: None,
    )
    monkeypatch.setattr(
        mongo_provider_module,
        '_default_server_selection_timeout_ms',
        lambda: 2000,
    )
    monkeypatch.setattr(
        mongo_provider_module,
        'MongoClient',
        lambda *_args, **_kwargs: _Client(),
    )
    def _record_terminated_pid(pid: int) -> None:
        terminated_pids.append(pid)

    monkeypatch.setattr(
        mongo_provider_module,
        '_terminate_pid',
        _record_terminated_pid,
    )

    provider.reap_orphan(
        json.dumps(
            {
                'backend': 'live',
                'cleanup_policy': 'drop',
                'database_name': 'suite_db',
                'uri': 'mongodb://live-host:27017',
                'pid': 4321,
                'tempdir': str(tempdir),
            },
        ),
        requirement,
        mode='ephemeral',
    )

    assert drop_calls == ['suite_db']
    assert close_calls == ['closed']
    assert terminated_pids == [4321]
    assert not tempdir.exists()


def test_acquire_live_and_standalone_error_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = MongoResourceProvider()

    class _FakePyMongoError(Exception):
        pass

    class _Client:
        def __init__(self) -> None:
            self.admin = SimpleNamespace(
                command=lambda _name: (_ for _ in ()).throw(
                    _FakePyMongoError('fail'),
                ),
            )

    monkeypatch.setattr(
        mongo_provider_module, 'PyMongoError', _FakePyMongoError,
    )
    monkeypatch.setattr(
        mongo_provider_module,
        'MongoClient',
        lambda *_args, **_kwargs: _Client(),
    )
    with pytest.raises(
        ResourceError, match='Failed to connect to live Mongo server',
    ):
        provider._acquire_live(
            MongoResourceConfig(
                backend='live',
                uri='mongodb://live-host:27017',
            ),
            database_name='db',
            generated_database=False,
            cleanup_policy='preserve',
        )

    class _Process:
        def poll(self):
            return None

    terminated: list[str] = []
    monkeypatch.setattr(
        mongo_provider_module,
        '_terminate_process',
        lambda _process: terminated.append('terminated'),
    )
    monkeypatch.setattr(
        mongo_provider_module.subprocess,
        'Popen',
        lambda *_args, **_kwargs: _Process(),
    )
    monkeypatch.setattr(
        MongoResourceProvider,
        '_wait_for_standalone_client',
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError('boom')),
    )
    with pytest.raises(RuntimeError, match='boom'):
        provider._acquire_standalone(
            MongoResourceConfig(
                backend='standalone',
                uri='mongodb://127.0.0.1:27017/?directConnection=true',
                standalone_port=27017,
            ),
            database_name='db',
            generated_database=True,
            cleanup_policy='drop',
        )
    assert terminated == ['terminated']


def test_wait_for_standalone_client_retry_and_timeout_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = MongoResourceProvider()
    config = MongoResourceConfig(
        backend='standalone',
        startup_timeout_seconds=0.3,
        server_selection_timeout_ms=100,
    )

    class _ExitedProcess:
        def poll(self):
            return 1

    with pytest.raises(ResourceError, match='exited during startup'):
        provider._wait_for_standalone_client(
            config,
            'mongodb://127.0.0.1:27017',
            _ExitedProcess(),
        )

    class _FakeTimeoutError(Exception):
        pass

    class _PendingProcess:
        def poll(self):
            return None

    class _Client:
        def __init__(self) -> None:
            self.admin = SimpleNamespace(
                command=lambda _name: (_ for _ in ()).throw(
                    _FakeTimeoutError('timeout'),
                ),
            )
            self.closed = False

        def close(self) -> None:
            self.closed = True

    monotonic_values = iter((0.0, 0.1, 0.2, 0.35))
    monkeypatch.setattr(
        mongo_provider_module.time,
        'monotonic',
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        mongo_provider_module.time, 'sleep', lambda _seconds: None,
    )
    monkeypatch.setattr(
        mongo_provider_module,
        'ServerSelectionTimeoutError',
        _FakeTimeoutError,
    )
    monkeypatch.setattr(
        mongo_provider_module,
        'MongoClient',
        lambda *_args, **_kwargs: _Client(),
    )

    with pytest.raises(ResourceError, match='did not become ready'):
        provider._wait_for_standalone_client(
            config,
            'mongodb://127.0.0.1:27017',
            _PendingProcess(),
        )
