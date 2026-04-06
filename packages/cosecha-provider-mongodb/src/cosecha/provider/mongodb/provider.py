from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from cosecha.core.resources import ResourceError


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Mapping

try:  # pragma: no cover - optional at import time
    from bson import json_util
except ModuleNotFoundError:  # pragma: no cover
    json_util = None

try:  # pragma: no cover - optional at import time
    from mongoeco import MongoClient as MongoEcoClient
    from mongoeco.engines.memory import MemoryEngine
    from mongoeco.engines.sqlite import SQLiteEngine
except ModuleNotFoundError:  # pragma: no cover
    MongoEcoClient = None
    MemoryEngine = None
    SQLiteEngine = None

try:  # pragma: no cover - optional at import time
    from pymongo import IndexModel, MongoClient
    from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
except ModuleNotFoundError:  # pragma: no cover
    IndexModel = None
    MongoClient = None
    PyMongoError = Exception
    ServerSelectionTimeoutError = Exception


type MongoBackendKind = Literal['mock', 'standalone', 'live']
type MongoEcoEngineKind = Literal['memory', 'sqlite']
type MongoCleanupPolicy = Literal['auto', 'drop', 'preserve']


@dataclass(slots=True, frozen=True)
class MongoResourceConfig:
    backend: MongoBackendKind = 'live'
    mongoeco_engine: MongoEcoEngineKind = 'memory'
    mongoeco_sqlite_path: str | None = None
    uri: str | None = None
    database_name: str | None = None
    database_prefix: str = 'cosecha'
    cleanup_policy: MongoCleanupPolicy = 'auto'
    startup_timeout_seconds: float = 10.0
    server_selection_timeout_ms: int = 2_000
    mongod_binary: str = 'mongod'
    standalone_bind_host: str = '127.0.0.1'
    standalone_port: int | None = None
    standalone_args: tuple[str, ...] = ()

    @classmethod
    def from_requirement(cls, requirement) -> MongoResourceConfig:
        config = requirement.config
        backend = _normalize_backend(
            _read_config_value(
                config,
                'backend',
                aliases=('backend_kind',),
                env_names=('COSECHA_MONGO_BACKEND',),
                default='live',
            ),
        )
        cleanup_policy = _normalize_cleanup_policy(
            _read_config_value(
                config,
                'cleanup_policy',
                env_names=('COSECHA_MONGO_CLEANUP_POLICY',),
                default='auto',
            ),
        )
        mongoeco_engine = _normalize_mongoeco_engine(
            _read_config_value(
                config,
                'mongoeco_engine',
                aliases=('mock_engine',),
                env_names=(
                    'COSECHA_MONGO_MONGOECO_ENGINE',
                    'COSECHA_MONGO_MOCK_ENGINE',
                ),
                default='memory',
            ),
        )
        mongoeco_sqlite_path = _read_optional_str(
            _read_config_value(
                config,
                'mongoeco_sqlite_path',
                aliases=('mock_sqlite_path',),
                env_names=(
                    'COSECHA_MONGO_MONGOECO_SQLITE_PATH',
                    'COSECHA_MONGO_MOCK_SQLITE_PATH',
                ),
            ),
        )
        database_name = _read_optional_str(
            _read_config_value(
                config,
                'resource_name',
                aliases=('database_name',),
                env_names=(
                    'COSECHA_MONGO_RESOURCE_NAME',
                    'COSECHA_MONGO_DATABASE_NAME',
                ),
            ),
        )
        database_prefix = _read_non_empty_str(
            _read_config_value(
                config,
                'resource_prefix',
                aliases=('database_prefix',),
                env_names=(
                    'COSECHA_MONGO_RESOURCE_PREFIX',
                    'COSECHA_MONGO_DATABASE_PREFIX',
                ),
                default='cosecha',
            ),
            field_name='resource_prefix',
        )
        uri = _read_optional_str(
            _read_config_value(
                config,
                'uri',
                env_names=('COSECHA_MONGO_URI',),
            ),
        )
        startup_timeout_seconds = _read_non_negative_float(
            _read_config_value(
                config,
                'startup_timeout_seconds',
                env_names=('COSECHA_MONGO_STARTUP_TIMEOUT_SECONDS',),
                default=10.0,
            ),
            field_name='startup_timeout_seconds',
        )
        server_selection_timeout_ms = _read_positive_int(
            _read_config_value(
                config,
                'server_selection_timeout_ms',
                env_names=('COSECHA_MONGO_SERVER_SELECTION_TIMEOUT_MS',),
                default=2_000,
            ),
            field_name='server_selection_timeout_ms',
        )
        mongod_binary = _read_non_empty_str(
            _read_config_value(
                config,
                'mongod_binary',
                env_names=('COSECHA_MONGO_MONGOD_BINARY',),
                default='mongod',
            ),
            field_name='mongod_binary',
        )
        standalone_bind_host = _read_non_empty_str(
            _read_config_value(
                config,
                'standalone_bind_host',
                env_names=('COSECHA_MONGO_STANDALONE_BIND_HOST',),
                default='127.0.0.1',
            ),
            field_name='standalone_bind_host',
        )
        standalone_port = _read_optional_int(
            _read_config_value(
                config,
                'standalone_port',
                env_names=('COSECHA_MONGO_STANDALONE_PORT',),
            ),
            field_name='standalone_port',
        )
        standalone_args = _read_tuple_of_str(
            config.get('standalone_args', ()),
            field_name='standalone_args',
        )

        return cls(
            backend=backend,
            mongoeco_engine=mongoeco_engine,
            mongoeco_sqlite_path=mongoeco_sqlite_path,
            uri=uri,
            database_name=database_name,
            database_prefix=database_prefix,
            cleanup_policy=cleanup_policy,
            startup_timeout_seconds=startup_timeout_seconds,
            server_selection_timeout_ms=server_selection_timeout_ms,
            mongod_binary=mongod_binary,
            standalone_bind_host=standalone_bind_host,
            standalone_port=standalone_port,
            standalone_args=standalone_args,
        )

    def resolve_database_name(
        self,
        *,
        requirement_name: str,
    ) -> tuple[str, bool]:
        if self.database_name is not None:
            return (self.database_name, False)

        suffix = uuid4().hex[:12]
        normalized_name = requirement_name.replace('/', '_').replace('-', '_')
        return (f'{self.database_prefix}_{normalized_name}_{suffix}', True)

    def resolve_cleanup_policy(
        self,
        *,
        generated_database: bool,
    ) -> Literal['drop', 'preserve']:
        if self.cleanup_policy == 'drop':
            return 'drop'
        if self.cleanup_policy == 'preserve':
            return 'preserve'
        return 'drop' if generated_database else 'preserve'


@dataclass(slots=True)
class MongoResourceHandle:
    client: Any
    database_name: str
    backend: MongoBackendKind
    mongoeco_engine: MongoEcoEngineKind | None
    sqlite_path: str | None
    uri: str | None
    cleanup_policy: Literal['drop', 'preserve']
    generated_database: bool
    process: subprocess.Popen[str] | None = None
    tempdir: tempfile.TemporaryDirectory[str] | None = None
    borrowed: bool = False

    @property
    def database(self) -> Any:
        return self.client[self.database_name]

    @property
    def name(self) -> str:
        return self.database_name

    def __getitem__(self, collection_name: str) -> Any:
        return self.database[collection_name]

    def __getattr__(self, name: str) -> Any:
        return getattr(self.database, name)

    def build_external_handle(self) -> str:
        payload = {
            'backend': self.backend,
            'borrowed': self.borrowed,
            'cleanup_policy': self.cleanup_policy,
            'database_name': self.database_name,
            'generated_database': self.generated_database,
            'mongoeco_engine': self.mongoeco_engine,
            'pid': self.process.pid if self.process is not None else None,
            'sqlite_path': self.sqlite_path,
            'tempdir': self.tempdir.name if self.tempdir is not None else None,
            'uri': self.uri,
        }
        return json.dumps(payload, sort_keys=True)


class MongoResourceProvider:
    """Materializa Mongo como mock, standalone o live."""

    def supports_mode(self, mode: str) -> bool:
        return mode in {'live', 'ephemeral'}

    def supported_initialization_modes(self, requirement) -> tuple[str, ...]:
        del requirement
        return ('data_seed', 'state_snapshot')

    def acquire(self, requirement, *, mode: str) -> MongoResourceHandle:
        del mode
        _require_mongo_runtime_dependencies()
        config = MongoResourceConfig.from_requirement(requirement)
        database_name, generated_database = config.resolve_database_name(
            requirement_name=requirement.name,
        )
        cleanup_policy = config.resolve_cleanup_policy(
            generated_database=generated_database,
        )

        if config.backend == 'mock':
            return self._acquire_mock(
                config,
                database_name=database_name,
                generated_database=generated_database,
                cleanup_policy=cleanup_policy,
            )

        if config.backend == 'standalone':
            return self._acquire_standalone(
                config,
                database_name=database_name,
                generated_database=generated_database,
                cleanup_policy=cleanup_policy,
            )

        return self._acquire_live(
            config,
            database_name=database_name,
            generated_database=generated_database,
            cleanup_policy=cleanup_policy,
        )

    def release(self, resource, requirement, *, mode: str) -> None:
        del requirement, mode
        _require_mongo_runtime_dependencies()
        if not isinstance(resource, MongoResourceHandle):
            return

        if resource.cleanup_policy == 'drop':
            with suppress(PyMongoError):
                resource.client.drop_database(resource.database_name)

        with suppress(PyMongoError):
            resource.client.close()

        if resource.process is not None:
            _terminate_process(resource.process)

        if resource.tempdir is not None:
            resource.tempdir.cleanup()

    def health_check(self, resource, requirement, *, mode: str) -> bool:
        del requirement, mode
        _require_mongo_runtime_dependencies()
        if not isinstance(resource, MongoResourceHandle):
            return False
        try:
            response = resource.client.admin.command('ping')
        except PyMongoError:
            return False
        return bool(response.get('ok'))

    def verify_integrity(self, resource, requirement, *, mode: str) -> bool:
        del requirement, mode
        _require_mongo_runtime_dependencies()
        if not isinstance(resource, MongoResourceHandle):
            return False
        try:
            return resource.database.name == resource.database_name
        except PyMongoError:
            return False

    def describe_external_handle(
        self,
        resource,
        requirement,
        *,
        mode: str,
    ) -> str | None:
        del requirement, mode
        if not isinstance(resource, MongoResourceHandle):
            return None
        return resource.build_external_handle()

    def snapshot_materialization(
        self,
        resource,
        requirement,
        *,
        mode: str,
    ) -> dict[str, object]:
        del requirement, mode
        _require_mongo_runtime_dependencies()
        if not isinstance(resource, MongoResourceHandle):
            msg = 'Mongo resource snapshot requires MongoResourceHandle'
            raise TypeError(msg)

        if resource.backend == 'mock':
            return {
                'backend': 'mock',
                'cleanup_policy': 'drop',
                'database_dump': _dump_database(resource.database),
                'database_name': resource.database_name,
                'generated_database': resource.generated_database,
                'mongoeco_engine': resource.mongoeco_engine,
                'uri': resource.uri,
            }

        return {
            'backend': resource.backend,
            'cleanup_policy': 'preserve',
            'database_name': resource.database_name,
            'generated_database': resource.generated_database,
            'uri': resource.uri,
        }

    def rehydrate_materialization(
        self,
        connection_data,
        requirement,
        *,
        mode: str,
    ) -> MongoResourceHandle:
        del requirement, mode
        _require_mongo_runtime_dependencies()
        snapshot = _require_mapping(connection_data)
        backend = _normalize_backend(snapshot.get('backend'))
        database_name = _read_non_empty_str(
            snapshot.get('database_name'),
            field_name='database_name',
        )
        cleanup_policy = _normalize_release_policy(
            snapshot.get('cleanup_policy'),
        )
        generated_database = bool(snapshot.get('generated_database', False))

        if backend == 'mock':
            uri = _read_optional_str(snapshot.get('uri'))
            mongoeco_engine = _normalize_mongoeco_engine(
                snapshot.get('mongoeco_engine', 'memory'),
            )
            client, tempdir, sqlite_path = _build_mongoeco_client(
                mongoeco_engine=mongoeco_engine,
                sqlite_path=None,
                uri=uri,
            )
            handle = MongoResourceHandle(
                client=client,
                database_name=database_name,
                backend='mock',
                mongoeco_engine=mongoeco_engine,
                sqlite_path=sqlite_path,
                uri=uri,
                cleanup_policy=cleanup_policy,
                generated_database=generated_database,
                tempdir=tempdir,
            )
            database_dump = snapshot.get('database_dump')
            if database_dump is not None:
                _restore_database(handle.database, database_dump)
            return handle

        uri = _read_non_empty_str(snapshot.get('uri'), field_name='uri')
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=_default_server_selection_timeout_ms(),
        )
        return MongoResourceHandle(
            client=client,
            database_name=database_name,
            backend=backend,
            mongoeco_engine=None,
            sqlite_path=None,
            uri=uri,
            cleanup_policy=cleanup_policy,
            generated_database=generated_database,
            borrowed=True,
        )

    def initialize_from(
        self,
        resource,
        requirement,
        initialization_sources,
        *,
        mode: str,
        initialization_mode: str,
    ) -> None:
        del mode
        _require_mongo_runtime_dependencies()
        if not isinstance(resource, MongoResourceHandle):
            msg = 'Mongo initialization requires MongoResourceHandle target'
            raise TypeError(msg)

        with suppress(PyMongoError):
            resource.client.drop_database(resource.database_name)

        for source in initialization_sources.values():
            if initialization_mode == 'state_snapshot':
                source_snapshot = getattr(source, 'connection_data', source)
                source_resource = self.rehydrate_materialization(
                    source_snapshot,
                    requirement,
                    mode='live',
                )
                try:
                    _clone_database(
                        source_resource.database,
                        resource.database,
                    )
                finally:
                    self.release(source_resource, requirement, mode='live')
                continue

            if not isinstance(source, MongoResourceHandle):
                msg = 'Mongo data_seed initialization requires live sources'
                raise TypeError(msg)
            _clone_database(source.database, resource.database)

    def reap_orphan(
        self,
        external_handle: str,
        requirement,
        *,
        mode: str,
    ) -> None:
        del requirement, mode
        _require_mongo_runtime_dependencies()
        payload = _decode_external_handle(external_handle)
        cleanup_policy = _normalize_release_policy(
            payload.get('cleanup_policy'),
        )
        database_name = _read_non_empty_str(
            payload.get('database_name'),
            field_name='database_name',
        )
        uri = _read_optional_str(payload.get('uri'))
        backend = _normalize_backend(payload.get('backend'))
        mongoeco_engine = payload.get('mongoeco_engine')
        sqlite_path = _read_optional_str(payload.get('sqlite_path'))

        if cleanup_policy == 'drop':
            if backend == 'mock' and mongoeco_engine == 'sqlite':
                with suppress(Exception):
                    client, _tempdir, _sqlite_path = _build_mongoeco_client(
                        mongoeco_engine='sqlite',
                        sqlite_path=sqlite_path,
                        uri=uri,
                    )
                    try:
                        client.drop_database(database_name)
                    finally:
                        client.close()
            elif uri is not None:
                with suppress(PyMongoError):
                    client = MongoClient(
                        uri,
                        serverSelectionTimeoutMS=_default_server_selection_timeout_ms(),
                    )
                    try:
                        client.drop_database(database_name)
                    finally:
                        client.close()

        pid = _read_optional_int(payload.get('pid'), field_name='pid')
        if pid is not None:
            _terminate_pid(pid)

        tempdir = _read_optional_str(payload.get('tempdir'))
        if tempdir:
            with suppress(OSError):
                shutil.rmtree(tempdir)

    def revoke_orphan_access(
        self,
        external_handle: str,
        requirement,
        *,
        mode: str,
    ) -> None:
        del external_handle, requirement, mode

    def _acquire_live(
        self,
        config: MongoResourceConfig,
        *,
        database_name: str,
        generated_database: bool,
        cleanup_policy: Literal['drop', 'preserve'],
    ) -> MongoResourceHandle:
        uri = config.uri or 'mongodb://127.0.0.1:27017/?directConnection=true'
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=config.server_selection_timeout_ms,
        )
        resource_name = 'database/mongodb'
        try:
            client.admin.command('ping')
        except PyMongoError as error:
            msg = f'Failed to connect to live Mongo server at {uri!r}'
            raise ResourceError(
                resource_name,
                msg,
                code='mongo_live_connection_failed',
                unhealthy=False,
            ) from error

        return MongoResourceHandle(
            client=client,
            database_name=database_name,
            backend='live',
            mongoeco_engine=None,
            sqlite_path=None,
            uri=uri,
            cleanup_policy=cleanup_policy,
            generated_database=generated_database,
        )

    def _acquire_standalone(
        self,
        config: MongoResourceConfig,
        *,
        database_name: str,
        generated_database: bool,
        cleanup_policy: Literal['drop', 'preserve'],
    ) -> MongoResourceHandle:
        port = config.standalone_port or _find_free_tcp_port()
        bind_host = config.standalone_bind_host
        uri = (
            config.uri
            or f'mongodb://{bind_host}:{port}/?directConnection=true'
        )
        tempdir = tempfile.TemporaryDirectory(prefix='cosecha-mongo-')
        dbpath = Path(tempdir.name) / 'db'
        dbpath.mkdir(parents=True, exist_ok=True)
        logpath = Path(tempdir.name) / 'mongod.log'
        process = subprocess.Popen(  # noqa: S603
            [
                config.mongod_binary,
                '--bind_ip',
                bind_host,
                '--port',
                str(port),
                '--dbpath',
                str(dbpath),
                '--logpath',
                str(logpath),
                '--quiet',
                *config.standalone_args,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )

        try:
            client = self._wait_for_standalone_client(config, uri, process)
        except Exception:
            _terminate_process(process)
            tempdir.cleanup()
            raise

        return MongoResourceHandle(
            client=client,
            database_name=database_name,
            backend='standalone',
            mongoeco_engine=None,
            sqlite_path=None,
            uri=uri,
            cleanup_policy=cleanup_policy,
            generated_database=generated_database,
            process=process,
            tempdir=tempdir,
        )

    def _acquire_mock(
        self,
        config: MongoResourceConfig,
        *,
        database_name: str,
        generated_database: bool,
        cleanup_policy: Literal['drop', 'preserve'],
    ) -> MongoResourceHandle:
        client, tempdir, sqlite_path = _build_mongoeco_client(
            mongoeco_engine=config.mongoeco_engine,
            sqlite_path=config.mongoeco_sqlite_path,
            uri=config.uri,
        )
        return MongoResourceHandle(
            client=client,
            database_name=database_name,
            backend='mock',
            mongoeco_engine=config.mongoeco_engine,
            sqlite_path=sqlite_path,
            uri=config.uri,
            cleanup_policy=cleanup_policy,
            generated_database=generated_database,
            tempdir=tempdir,
        )

    def _wait_for_standalone_client(
        self,
        config: MongoResourceConfig,
        uri: str,
        process: subprocess.Popen[str],
    ) -> MongoClient:
        deadline = time.monotonic() + config.startup_timeout_seconds
        last_error: Exception | None = None
        resource_name = 'database/mongodb'
        while time.monotonic() < deadline:
            if process.poll() is not None:
                msg = 'Standalone Mongo process exited during startup'
                raise ResourceError(
                    resource_name,
                    msg,
                    code='mongo_standalone_exited',
                    unhealthy=False,
                )
            client = MongoClient(
                uri,
                serverSelectionTimeoutMS=config.server_selection_timeout_ms,
            )
            try:
                client.admin.command('ping')
            except ServerSelectionTimeoutError as error:
                client.close()
                last_error = error
                time.sleep(0.1)
                continue
            return client

        msg = f'Standalone Mongo did not become ready at {uri!r}'
        raise ResourceError(
            resource_name,
            msg,
            code='mongo_standalone_timeout',
            unhealthy=False,
        ) from last_error


def _normalize_backend(value: object) -> MongoBackendKind:
    if value in {'mock', 'standalone', 'live'}:
        return value

    msg = "Mongo backend must be one of 'mock', 'standalone' or 'live'"
    raise ValueError(msg)


def _normalize_mongoeco_engine(value: object) -> MongoEcoEngineKind:
    if value in {'memory', 'sqlite'}:
        return value

    msg = "Mongo mongoeco_engine must be one of 'memory' or 'sqlite'"
    raise ValueError(msg)


def _require_mongo_runtime_dependencies() -> None:
    if (
        json_util is not None
        and MongoEcoClient is not None
        and MemoryEngine is not None
        and SQLiteEngine is not None
        and MongoClient is not None
        and IndexModel is not None
    ):
        return

    msg = (
        'Mongo resource provider requires optional runtime dependencies '
        '`bson`, `mongoeco` and `pymongo`'
    )
    raise ModuleNotFoundError(msg)


def _normalize_cleanup_policy(value: object) -> MongoCleanupPolicy:
    if value in {'auto', 'drop', 'preserve'}:
        return value

    msg = "Mongo cleanup_policy must be one of 'auto', 'drop' or 'preserve'"
    raise ValueError(msg)


def _normalize_release_policy(value: object) -> Literal['drop', 'preserve']:
    if value in {'drop', 'preserve'}:
        return value

    msg = "Mongo release policy must be 'drop' or 'preserve'"
    raise ValueError(msg)


def _read_config_value(
    config: Mapping[str, object],
    key: str,
    *,
    aliases: tuple[str, ...] = (),
    env_names: tuple[str, ...] = (),
    default: object | None = None,
) -> object | None:
    for env_name in env_names:
        if env_name in os.environ:
            return os.environ[env_name]
    if key in config:
        return config[key]
    for alias in aliases:
        if alias in config:
            return config[alias]
    return default


def _read_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value

    msg = 'Expected optional non-empty string'
    raise ValueError(msg)


def _read_non_empty_str(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and value:
        return value

    msg = f'Mongo config field {field_name!r} must be a non-empty string'
    raise ValueError(msg)


def _read_non_negative_float(value: object, *, field_name: str) -> float:
    if isinstance(value, int | float) and not isinstance(value, bool):
        if value < 0:
            msg = f'Mongo config field {field_name!r} must be non-negative'
            raise ValueError(msg)
        return float(value)
    if isinstance(value, str):
        return _read_non_negative_float(float(value), field_name=field_name)

    msg = f'Mongo config field {field_name!r} must be numeric'
    raise ValueError(msg)


def _read_positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        if value <= 0:
            msg = (
                f'Mongo config field {field_name!r} must be greater than zero'
            )
            raise ValueError(msg)
        return value
    if isinstance(value, str) and value:
        return _read_positive_int(int(value), field_name=field_name)

    msg = f'Mongo config field {field_name!r} must be a positive integer'
    raise ValueError(msg)


def _read_optional_int(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value:
        return int(value)

    msg = f'Mongo config field {field_name!r} must be an integer'
    raise ValueError(msg)


def _read_tuple_of_str(value: object, *, field_name: str) -> tuple[str, ...]:
    if value in (None, ()):
        return ()
    if isinstance(value, list | tuple) and all(
        isinstance(item, str) and item for item in value
    ):
        return tuple(value)

    msg = f'Mongo config field {field_name!r} must be a list of strings'
    raise ValueError(msg)


def _require_mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, dict):
        return value

    msg = 'Mongo materialization snapshot must be a dict'
    raise TypeError(msg)


def _dump_database(database: Any) -> dict[str, object]:
    collections: dict[str, object] = {}
    for collection_name in database.list_collection_names():
        collection = database[collection_name]
        dumped_documents = json.loads(
            json_util.dumps(list(collection.find({}))),
        )
        collections[collection_name] = {
            'documents': dumped_documents,
            'indexes': _dump_indexes(collection),
        }
    return {'collections': collections}


def _dump_indexes(collection: Any) -> list[dict[str, object]]:
    dumped_indexes: list[dict[str, object]] = []
    for index in collection.list_indexes():
        document = dict(index)
        if document.get('name') == '_id_':
            continue
        dumped_indexes.append(
            json.loads(
                json_util.dumps(document),
            ),
        )
    return dumped_indexes


def _restore_database(database: Any, database_dump: object) -> None:
    dump = _require_mapping(database_dump)
    raw_collections = dump.get('collections', {})
    collections = _require_mapping(raw_collections)
    for collection_name, payload in collections.items():
        if not isinstance(collection_name, str):
            continue
        collection_dump = _require_mapping(payload)
        collection = database[collection_name]
        documents = collection_dump.get('documents', [])
        parsed_documents = json_util.loads(json.dumps(documents))
        if isinstance(parsed_documents, list) and parsed_documents:
            collection.insert_many(parsed_documents)
        _restore_indexes(collection, collection_dump.get('indexes', ()))


def _restore_indexes(collection: Any, indexes: object) -> None:
    if not isinstance(indexes, list):
        return
    models: list[IndexModel] = []
    for raw_index in indexes:
        if not isinstance(raw_index, dict):
            continue
        keys = raw_index.get('key')
        if not isinstance(keys, dict):
            continue
        key_items = [(str(name), value) for name, value in keys.items()]
        options = {
            key: value
            for key, value in raw_index.items()
            if key not in {'key', 'ns', 'v'}
        }
        models.append(IndexModel(key_items, **options))
    if models:
        collection.create_indexes(models)


def _clone_database(source_database: Any, target_database: Any) -> None:
    _restore_database(target_database, _dump_database(source_database))


def _build_mongoeco_client(
    *,
    mongoeco_engine: MongoEcoEngineKind,
    sqlite_path: str | None,
    uri: str | None,
) -> tuple[Any, tempfile.TemporaryDirectory[str] | None, str | None]:
    if mongoeco_engine == 'memory':
        return (MongoEcoClient(MemoryEngine(), uri=uri), None, None)

    tempdir: tempfile.TemporaryDirectory[str] | None = None
    resolved_sqlite_path = sqlite_path
    if resolved_sqlite_path is None:
        tempdir = tempfile.TemporaryDirectory(prefix='cosecha-mongoeco-')
        resolved_sqlite_path = str(Path(tempdir.name) / 'mongoeco.db')

    sqlite_file_path = Path(resolved_sqlite_path)
    sqlite_file_path.parent.mkdir(parents=True, exist_ok=True)
    return (
        MongoEcoClient(SQLiteEngine(str(sqlite_file_path)), uri=uri),
        tempdir,
        str(sqlite_file_path),
    )


def _find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _terminate_pid(pid: int) -> None:
    with suppress(OSError):
        os.kill(pid, signal.SIGTERM)


def _decode_external_handle(external_handle: str) -> dict[str, object]:
    try:
        decoded = json.loads(external_handle)
    except json.JSONDecodeError as error:
        msg = 'Invalid Mongo external handle'
        raise ValueError(msg) from error
    if isinstance(decoded, dict):
        return {str(key): value for key, value in decoded.items()}

    msg = 'Invalid Mongo external handle payload'
    raise ValueError(msg)


def _default_server_selection_timeout_ms() -> int:
    value = os.environ.get('COSECHA_MONGO_SERVER_SELECTION_TIMEOUT_MS', '2000')
    return int(value)
