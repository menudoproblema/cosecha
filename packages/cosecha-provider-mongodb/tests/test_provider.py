from __future__ import annotations

import shutil

from pathlib import Path

import pytest

from cosecha.core.resources import ResourceRequirement
from cosecha.provider.mongodb import (
    MongoResourceHandle,
    MongoResourceProvider,
)


SHARED_VALUE = 7


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


def test_mock_resource_can_snapshot_and_rehydrate() -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'database_name': 'mock_suite',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')
    assert resource.mongoeco_engine == 'memory'
    resource['items'].insert_one({'kind': 'seed', 'value': 1})

    snapshot = provider.snapshot_materialization(
        resource,
        requirement,
        mode='ephemeral',
    )
    provider.release(resource, requirement, mode='ephemeral')

    rehydrated = provider.rehydrate_materialization(
        snapshot,
        requirement,
        mode='ephemeral',
    )

    assert rehydrated['items'].find_one({'kind': 'seed'})['value'] == 1

    provider.release(rehydrated, requirement, mode='ephemeral')


def test_mock_resource_can_initialize_from_state_snapshot() -> None:
    provider = MongoResourceProvider()
    source_requirement = _build_requirement(
        config={
            'backend': 'mock',
            'database_name': 'source_db',
        },
        scope='run',
    )
    target_requirement = _build_requirement(
        config={
            'backend': 'mock',
            'database_name': 'target_db',
        },
    )

    source = provider.acquire(source_requirement, mode='ephemeral')
    target = provider.acquire(target_requirement, mode='ephemeral')
    source['items'].insert_many(
        [
            {'_id': 'a', 'value': 1},
            {'_id': 'b', 'value': 2},
        ],
    )

    snapshot = provider.snapshot_materialization(
        source,
        source_requirement,
        mode='ephemeral',
    )
    provider.initialize_from(
        target,
        target_requirement,
        {'seed': snapshot},
        mode='ephemeral',
        initialization_mode='state_snapshot',
    )

    assert list(target['items'].find({}, {'_id': 1, 'value': 1})) == [
        {'_id': 'a', 'value': 1},
        {'_id': 'b', 'value': 2},
    ]

    provider.release(target, target_requirement, mode='ephemeral')
    provider.release(source, source_requirement, mode='ephemeral')


def test_mock_resource_uses_explicit_database_name() -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'database_name': 'mi_suite_db',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')

    assert resource.database_name == 'mi_suite_db'
    assert resource.generated_database is False

    provider.release(resource, requirement, mode='ephemeral')


def test_mock_resource_accepts_canonical_resource_name() -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'resource_name': 'canonical_db',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')

    assert resource.database_name == 'canonical_db'
    assert resource.generated_database is False

    provider.release(resource, requirement, mode='ephemeral')


def test_mock_resource_generates_database_name_from_prefix() -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'database_prefix': 'suite',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')

    assert resource.database_name.startswith('suite_mongo_')
    assert resource.generated_database is True

    provider.release(resource, requirement, mode='ephemeral')


def test_mock_resource_generates_database_name_from_resource_prefix() -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'resource_prefix': 'suite',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')

    assert resource.database_name.startswith('suite_mongo_')
    assert resource.generated_database is True

    provider.release(resource, requirement, mode='ephemeral')


def test_mock_resource_defaults_to_memory_mongoeco_engine() -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'database_name': 'memory_default_db',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')

    assert resource.mongoeco_engine == 'memory'

    provider.release(resource, requirement, mode='ephemeral')


def test_mock_resource_supports_sqlite_with_provider_temp_storage() -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'mongoeco_engine': 'sqlite',
            'database_name': 'sqlite_temp_db',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')
    tempdir_name = (
        resource.tempdir.name if resource.tempdir is not None else None
    )
    resource['items'].insert_one({'kind': 'sqlite', 'value': 1})

    assert resource.mongoeco_engine == 'sqlite'
    assert tempdir_name is not None
    assert Path(tempdir_name).exists()

    provider.release(resource, requirement, mode='ephemeral')

    assert not Path(tempdir_name).exists()


def test_mock_resource_supports_sqlite_with_explicit_storage_path(
    tmp_path: Path,
) -> None:
    provider = MongoResourceProvider()
    sqlite_path = tmp_path / 'runtime' / 'mongoeco.db'
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'mongoeco_engine': 'sqlite',
            'mongoeco_sqlite_path': str(sqlite_path),
            'database_name': 'sqlite_file_db',
            'cleanup_policy': 'preserve',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')
    resource['items'].insert_one({'kind': 'sqlite', 'value': 1})

    assert resource.mongoeco_engine == 'sqlite'
    assert resource.tempdir is None
    assert sqlite_path.exists()

    provider.release(resource, requirement, mode='ephemeral')

    assert sqlite_path.exists()


def test_environment_variables_override_manifest_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider = MongoResourceProvider()
    sqlite_path = tmp_path / 'env' / 'mongoeco.db'
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'mongoeco_engine': 'memory',
            'database_name': 'manifest_db',
        },
    )
    monkeypatch.setenv('COSECHA_MONGO_BACKEND', 'mock')
    monkeypatch.setenv('COSECHA_MONGO_DATABASE_NAME', 'env_db')
    monkeypatch.setenv('COSECHA_MONGO_MONGOECO_ENGINE', 'sqlite')
    monkeypatch.setenv(
        'COSECHA_MONGO_MONGOECO_SQLITE_PATH',
        str(sqlite_path),
    )

    resource = provider.acquire(requirement, mode='ephemeral')
    resource['items'].insert_one({'kind': 'env', 'value': 1})

    assert resource.database_name == 'env_db'
    assert resource.mongoeco_engine == 'sqlite'
    assert sqlite_path.exists()

    provider.release(resource, requirement, mode='ephemeral')


def test_canonical_environment_variables_override_legacy_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = MongoResourceProvider()
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'database_name': 'legacy_manifest_db',
        },
    )
    monkeypatch.setenv('COSECHA_MONGO_DATABASE_NAME', 'legacy_env_db')
    monkeypatch.setenv('COSECHA_MONGO_RESOURCE_NAME', 'canonical_env_db')

    resource = provider.acquire(requirement, mode='ephemeral')

    assert resource.database_name == 'canonical_env_db'

    provider.release(resource, requirement, mode='ephemeral')


def test_reap_orphan_drops_database_for_mock_sqlite_storage(
    tmp_path: Path,
) -> None:
    provider = MongoResourceProvider()
    sqlite_path = tmp_path / 'orphan' / 'mongoeco.db'
    requirement = _build_requirement(
        config={
            'backend': 'mock',
            'mongoeco_engine': 'sqlite',
            'mongoeco_sqlite_path': str(sqlite_path),
            'database_name': 'orphan_db',
            'cleanup_policy': 'drop',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')
    resource['items'].insert_one({'kind': 'orphan', 'value': 1})
    external_handle = provider.describe_external_handle(
        resource,
        requirement,
        mode='ephemeral',
    )
    assert external_handle is not None

    resource.client.close()
    provider.reap_orphan(
        external_handle,
        requirement,
        mode='ephemeral',
    )

    rehydrated = provider.acquire(
        _build_requirement(
            config={
                'backend': 'mock',
                'mongoeco_engine': 'sqlite',
                'mongoeco_sqlite_path': str(sqlite_path),
                'database_name': 'orphan_db',
                'cleanup_policy': 'preserve',
            },
        ),
        mode='ephemeral',
    )

    assert sqlite_path.exists()
    assert rehydrated['items'].find_one({'kind': 'orphan'}) is None

    provider.release(rehydrated, requirement, mode='ephemeral')


@pytest.mark.skipif(
    shutil.which('mongod') is None,
    reason='mongod is required for standalone integration test',
)
def test_standalone_resource_can_be_consumed_as_live_server() -> None:
    provider = MongoResourceProvider()
    standalone_requirement = _build_requirement(
        config={
            'backend': 'standalone',
            'database_name': 'shared_suite',
            'cleanup_policy': 'drop',
            'startup_timeout_seconds': 15,
        },
        scope='run',
    )

    standalone = provider.acquire(standalone_requirement, mode='ephemeral')
    assert isinstance(standalone, MongoResourceHandle)
    standalone['items'].insert_one({'kind': 'shared', 'value': SHARED_VALUE})

    live_requirement = _build_requirement(
        config={
            'backend': 'live',
            'uri': standalone.uri,
            'database_name': standalone.database_name,
            'cleanup_policy': 'preserve',
        },
    )

    live = provider.acquire(live_requirement, mode='live')

    assert live['items'].find_one({'kind': 'shared'})['value'] == SHARED_VALUE

    provider.release(live, live_requirement, mode='live')
    pid = standalone.process.pid if standalone.process is not None else None
    provider.release(standalone, standalone_requirement, mode='ephemeral')

    assert pid is not None
    assert standalone.process.poll() is not None
