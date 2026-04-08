# ruff: noqa: PLR0915
from __future__ import annotations

import json
import subprocess

from types import SimpleNamespace

import pytest

from cosecha.provider.mongodb import provider as mongo_provider_module


NON_NEGATIVE_TIMEOUT = 1.5
POSITIVE_PORT = 5
OPTIONAL_PORT = 8
DEFAULT_SERVER_SELECTION_TIMEOUT_MS = 2000
ENV_SERVER_SELECTION_TIMEOUT_MS = 3210
TERMINATE_ASSERTION_MESSAGE = 'should not terminate exited process'


def test_helper_functions_cover_remaining_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    assert (
        mongo_provider_module._read_non_negative_float(
            str(NON_NEGATIVE_TIMEOUT),
            field_name='timeout',
        )
        == NON_NEGATIVE_TIMEOUT
    )
    assert (
        mongo_provider_module._read_positive_int(
            str(POSITIVE_PORT),
            field_name='port',
        )
        == POSITIVE_PORT
    )
    assert (
        mongo_provider_module._read_optional_int(
            str(OPTIONAL_PORT),
            field_name='port',
        )
        == OPTIONAL_PORT
    )
    assert mongo_provider_module._read_tuple_of_str(
        ['--quiet', '--ipv6'],
        field_name='args',
    ) == ('--quiet', '--ipv6')
    assert (
        mongo_provider_module._default_server_selection_timeout_ms()
        == DEFAULT_SERVER_SELECTION_TIMEOUT_MS
    )

    with pytest.raises(ValueError, match='backend'):
        mongo_provider_module._normalize_backend('invalid')
    with pytest.raises(ValueError, match='mongoeco_engine'):
        mongo_provider_module._normalize_mongoeco_engine('invalid')
    with pytest.raises(ValueError, match='cleanup_policy'):
        mongo_provider_module._normalize_cleanup_policy('invalid')
    with pytest.raises(ValueError, match='release policy'):
        mongo_provider_module._normalize_release_policy('invalid')
    with pytest.raises(ValueError, match='optional non-empty string'):
        mongo_provider_module._read_optional_str('')
    with pytest.raises(ValueError, match='must be a non-empty string'):
        mongo_provider_module._read_non_empty_str('', field_name='name')
    with pytest.raises(ValueError, match='must be non-negative'):
        mongo_provider_module._read_non_negative_float(
            -1,
            field_name='timeout',
        )
    with pytest.raises(ValueError, match='must be numeric'):
        mongo_provider_module._read_non_negative_float(
            object(),
            field_name='timeout',
        )
    with pytest.raises(ValueError, match='greater than zero'):
        mongo_provider_module._read_positive_int(0, field_name='port')
    with pytest.raises(ValueError, match='positive integer'):
        mongo_provider_module._read_positive_int([], field_name='port')
    with pytest.raises(ValueError, match='must be an integer'):
        mongo_provider_module._read_optional_int([], field_name='port')
    with pytest.raises(ValueError, match='list of strings'):
        mongo_provider_module._read_tuple_of_str([1], field_name='args')
    with pytest.raises(TypeError, match='must be a dict'):
        mongo_provider_module._require_mapping([])
    with pytest.raises(ValueError, match='Invalid Mongo external handle'):
        mongo_provider_module._decode_external_handle('{invalid')
    with pytest.raises(
        ValueError,
        match='Invalid Mongo external handle payload',
    ):
        mongo_provider_module._decode_external_handle('[]')

    monkeypatch.setenv(
        'COSECHA_MONGO_SERVER_SELECTION_TIMEOUT_MS',
        str(ENV_SERVER_SELECTION_TIMEOUT_MS),
    )
    assert (
        mongo_provider_module._default_server_selection_timeout_ms()
        == ENV_SERVER_SELECTION_TIMEOUT_MS
    )

    dependency_state = {
        'json_util': mongo_provider_module.json_util,
        'MongoEcoClient': mongo_provider_module.MongoEcoClient,
        'MemoryEngine': mongo_provider_module.MemoryEngine,
        'SQLiteEngine': mongo_provider_module.SQLiteEngine,
        'MongoClient': mongo_provider_module.MongoClient,
        'IndexModel': mongo_provider_module.IndexModel,
    }
    try:
        monkeypatch.setattr(mongo_provider_module, 'json_util', None)
        monkeypatch.setattr(mongo_provider_module, 'MongoEcoClient', None)
        monkeypatch.setattr(mongo_provider_module, 'MemoryEngine', None)
        monkeypatch.setattr(mongo_provider_module, 'SQLiteEngine', None)
        monkeypatch.setattr(mongo_provider_module, 'MongoClient', None)
        monkeypatch.setattr(mongo_provider_module, 'IndexModel', None)
        with pytest.raises(
            ModuleNotFoundError,
            match='requires optional runtime',
        ):
            mongo_provider_module._require_mongo_runtime_dependencies()
    finally:
        for key, value in dependency_state.items():
            monkeypatch.setattr(mongo_provider_module, key, value)

    class _ExitedProcess:
        def poll(self):
            return 0

        def terminate(self):
            raise AssertionError(TERMINATE_ASSERTION_MESSAGE)

    mongo_provider_module._terminate_process(_ExitedProcess())

    class _TimeoutProcess:
        def __init__(self) -> None:
            self.killed = False

        def poll(self):
            return None

        def terminate(self):
            return None

        def wait(self, *, timeout: int):
            if not self.killed:
                raise subprocess.TimeoutExpired(cmd='mongod', timeout=timeout)

        def kill(self):
            self.killed = True

    mongo_provider_module._terminate_process(_TimeoutProcess())

    kill_calls: list[tuple[int, int]] = []
    monkeypatch.setattr(
        mongo_provider_module.os,
        'kill',
        lambda pid, sig: kill_calls.append((pid, int(sig))),
    )
    mongo_provider_module._terminate_pid(123)
    assert kill_calls

    class _IndexModel:
        def __init__(self, key_items, **options) -> None:
            self.key_items = key_items
            self.options = options

    class _Collection:
        def __init__(self) -> None:
            self.created_indexes: list[list[_IndexModel]] = []
            self.inserted_documents: list[dict[str, object]] = []

        def list_indexes(self):
            return [
                {'name': '_id_', 'key': {'_id': 1}},
                {
                    'name': 'idx_name',
                    'key': {'field': 1},
                    'v': 2,
                    'unique': True,
                },
            ]

        def find(self, _query):
            return [{'field': 1}]

        def create_indexes(self, models):
            self.created_indexes.append(list(models))

        def insert_many(self, docs):
            self.inserted_documents.extend(docs)

    class _Database:
        def __init__(self) -> None:
            self.collections = {'items': _Collection()}

        def list_collection_names(self):
            return ['items']

        def __getitem__(self, collection_name: str):
            return self.collections.setdefault(collection_name, _Collection())

    mongo_provider_module.json_util = SimpleNamespace(
        dumps=json.dumps,
        loads=json.loads,
    )
    mongo_provider_module.IndexModel = _IndexModel

    database = _Database()
    dumped = mongo_provider_module._dump_database(database)
    assert dumped['collections']['items']['indexes'] == [
        {'name': 'idx_name', 'key': {'field': 1}, 'v': 2, 'unique': True},
    ]

    mongo_provider_module._restore_indexes(database['items'], None)
    mongo_provider_module._restore_indexes(
        database['items'],
        [1, {'key': 'bad'}, {'key': {'x': -1}, 'name': 'idx_x', 'v': 2}],
    )
    assert len(database['items'].created_indexes) == 1
    assert database['items'].created_indexes[0][0].key_items == [('x', -1)]

    mongo_provider_module._restore_database(
        database,
        {
            'collections': {
                123: {'documents': []},
                'items': {'documents': [{'field': 2}], 'indexes': []},
            },
        },
    )
    assert {'field': 2} in database['items'].inserted_documents
    mongo_provider_module._clone_database(database, _Database())
