from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from cosecha.engine.gherkin.step_ast_discovery import (
    StaticDiscoveredStepFile,
    StaticStepDescriptor,
)
from cosecha.engine.gherkin.step_catalog import (
    GHERKIN_STEP_INDEX_SCHEMA_VERSION,
    InMemoryKnowledgeStore,
    PersistentKnowledgeStore,
    StepCatalog,
    StepQuery,
    _build_module_import_path,
    _build_static_step_descriptor_from_knowledge,
    _collect_match_scores,
    _descriptor_may_match_query,
    _is_allowed_definition_path,
    _is_within_root,
    _normalize_discovery_mode,
    _tokenize_step_text,
    get_conflicting_step_types,
)


def _descriptor(
    file_path: Path,
    *,
    function_name: str,
    step_type: str,
    patterns: tuple[str, ...],
    literal_prefixes: tuple[str, ...] = (),
    literal_suffixes: tuple[str, ...] = (),
    literal_fragments: tuple[str, ...] = (),
    anchor_tokens: tuple[str, ...] = (),
) -> StaticStepDescriptor:
    return StaticStepDescriptor(
        step_type=step_type,  # type: ignore[arg-type]
        patterns=patterns,
        source_file=file_path,
        source_line=1,
        function_name=function_name,
        file_path=file_path,
        module_import_path='steps.demo',
        literal_prefixes=literal_prefixes,
        literal_suffixes=literal_suffixes,
        literal_fragments=literal_fragments,
        anchor_tokens=anchor_tokens,
        discovery_mode='ast',
        mtime_ns=1,
        file_size=1,
    )


def _discovered_file(
    file_path: Path,
    descriptors: tuple[StaticStepDescriptor, ...],
    *,
    fallback: bool = False,
    digest: str = 'hash',
) -> StaticDiscoveredStepFile:
    return StaticDiscoveredStepFile(
        file_path=file_path,
        module_import_path='steps.demo',
        descriptors=descriptors,
        discovery_mode='fallback_import' if fallback else 'ast',
        requires_fallback_import=fallback,
        content_digest=digest,
        mtime_ns=1,
        file_size=1,
    )


def test_in_memory_knowledge_store_roundtrip_setters(tmp_path: Path) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _descriptor(
        file_path,
        function_name='step_fn',
        step_type='given',
        patterns=('a user',),
    )
    discovered = _discovered_file(file_path, (descriptor,))
    store = InMemoryKnowledgeStore()
    custom_index = StepCatalog()

    store.set_project_step_index(custom_index)
    store.set_discovered_step_files((discovered,))

    assert store.get_project_step_index() is custom_index
    assert store.get_discovered_step_files() == (discovered,)


def test_persistent_knowledge_store_uses_disk_cache_and_setters(
    tmp_path: Path,
    monkeypatch,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _descriptor(
        file_path,
        function_name='step_fn',
        step_type='given',
        patterns=('a user',),
        literal_prefixes=('a user',),
    )
    discovered = _discovered_file(file_path, (descriptor,), digest='digest')

    @dataclass
    class _FakeDiskCache:
        loaded: object
        saved: object | None = None
        cleared: bool = False

        def load(self):
            return self.loaded

        def save(self, payload):
            self.saved = payload

        def clear(self):
            self.cleared = True

    fake_disk_cache = _FakeDiskCache(
        loaded={
            'schema_version': GHERKIN_STEP_INDEX_SCHEMA_VERSION,
            'discovered_step_files': (discovered,),
        },
    )
    monkeypatch.setattr(
        'cosecha.engine.gherkin.step_catalog.DiskCache',
        lambda *_args, **_kwargs: fake_disk_cache,
    )

    store = PersistentKnowledgeStore(tmp_path)
    new_index = StepCatalog()
    store.set_project_step_index(new_index)
    store.set_discovered_step_files((discovered,))

    assert store.get_project_step_index() is new_index
    assert store.get_discovered_step_files() == (discovered,)
    assert fake_disk_cache.saved == {
        'discovered_step_files': (discovered,),
        'schema_version': GHERKIN_STEP_INDEX_SCHEMA_VERSION,
    }


def test_persistent_store_loads_from_knowledge_base_on_cache_miss_or_invalid(
    tmp_path: Path,
    monkeypatch,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _descriptor(
        file_path,
        function_name='step_fn',
        step_type='given',
        patterns=('a user',),
    )
    discovered = _discovered_file(file_path, (descriptor,))

    class _FakeDiskCache:
        def __init__(self, loaded):
            self.loaded = loaded
            self.clear_called = False

        def load(self):
            return self.loaded

        def save(self, _payload):
            return

        def clear(self):
            self.clear_called = True

    fallback_calls: list[str] = []
    monkeypatch.setattr(
        PersistentKnowledgeStore,
        '_load_discovered_step_files_from_knowledge_base',
        lambda self: fallback_calls.append('called') or (discovered,),
    )

    for loaded in (
        {},
        {'schema_version': 0, 'discovered_step_files': ()},
        {'schema_version': GHERKIN_STEP_INDEX_SCHEMA_VERSION, 'discovered_step_files': []},
    ):
        fake_disk_cache = _FakeDiskCache(loaded)
        monkeypatch.setattr(
            'cosecha.engine.gherkin.step_catalog.DiskCache',
            lambda *_args, **_kwargs: fake_disk_cache,
        )
        PersistentKnowledgeStore(tmp_path)
        if loaded:
            assert fake_disk_cache.clear_called is True

    assert fallback_calls


def test_persistent_store_filters_invalid_cached_entries_and_missing_db(
    tmp_path: Path,
    monkeypatch,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _descriptor(
        file_path,
        function_name='step_fn',
        step_type='given',
        patterns=('a user',),
    )
    discovered = _discovered_file(file_path, (descriptor,))

    class _FakeDiskCache:
        def __init__(self):
            self.cleared = False

        def load(self):
            return {
                'schema_version': GHERKIN_STEP_INDEX_SCHEMA_VERSION,
                'discovered_step_files': (discovered, object()),
            }

        def save(self, _payload):
            return

        def clear(self):
            self.cleared = True

    monkeypatch.setattr(
        'cosecha.engine.gherkin.step_catalog.DiskCache',
        lambda *_args, **_kwargs: _FakeDiskCache(),
    )
    store = PersistentKnowledgeStore(tmp_path)
    assert store.get_discovered_step_files() == (discovered,)

    store_without_init = PersistentKnowledgeStore.__new__(PersistentKnowledgeStore)
    store_without_init._root_path = tmp_path  # type: ignore[attr-defined]
    store_without_init._engine_name = 'gherkin'  # type: ignore[attr-defined]
    store_without_init._definition_paths = ()  # type: ignore[attr-defined]
    monkeypatch.setattr(
        'cosecha.engine.gherkin.step_catalog.resolve_knowledge_base_path',
        lambda _root_path: tmp_path / 'missing.db',
    )
    assert (
        PersistentKnowledgeStore._load_discovered_step_files_from_knowledge_base(
            store_without_init,
        )
        == ()
    )


def test_persistent_store_loads_discovered_files_from_knowledge_base(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir()
    definition_file = (root_path / 'steps.py').resolve()
    definition_file.write_text('', encoding='utf-8')
    outside_file = (tmp_path / 'outside.py').resolve()
    outside_file.write_text('', encoding='utf-8')
    db_path = (tmp_path / 'kb.db').resolve()
    db_path.write_text('', encoding='utf-8')

    descriptor_record = SimpleNamespace(
        step_type='given',
        patterns=('a user',),
        source_line=1,
        function_name='step_fn',
        literal_prefixes=('a user',),
        literal_suffixes=(),
        literal_fragments=('a user',),
        anchor_tokens=('user',),
        dynamic_fragment_count=0,
        documentation=None,
        parser_cls_name=None,
        category=None,
    )
    definitions = (
        SimpleNamespace(
            file_path=str(definition_file),
            descriptors=(descriptor_record,),
            discovery_mode='ast',
            content_hash='hash-1',
        ),
        SimpleNamespace(
            file_path=str(outside_file),
            descriptors=(descriptor_record,),
            discovery_mode='fallback_import',
            content_hash='hash-2',
        ),
    )

    class _FakeKB:
        closed = False

        def __init__(self, _db_path):
            return

        def query_definitions(self, _query):
            return definitions

        def close(self):
            self.closed = True

    monkeypatch.setattr(
        'cosecha.engine.gherkin.step_catalog.resolve_knowledge_base_path',
        lambda _root_path: db_path,
    )
    monkeypatch.setattr(
        'cosecha.engine.gherkin.step_catalog.ReadOnlyPersistentKnowledgeBase',
        _FakeKB,
    )

    store = PersistentKnowledgeStore(
        root_path,
        definition_paths=(outside_file,),
    )
    discovered = store.get_discovered_step_files()

    assert len(discovered) == 2
    assert discovered[0].file_path in {definition_file, outside_file}
    assert discovered[1].file_path in {definition_file, outside_file}

    store_without_external_paths = PersistentKnowledgeStore(
        root_path,
        definition_paths=(),
    )
    internal_only = store_without_external_paths.get_discovered_step_files()
    assert {entry.file_path for entry in internal_only} == {definition_file}


def test_step_catalog_registers_descriptors_and_candidate_queries(
    tmp_path: Path,
) -> None:
    file_a = (tmp_path / 'a_steps.py').resolve()
    file_b = (tmp_path / 'b_steps.py').resolve()
    descriptor_a = _descriptor(
        file_a,
        function_name='step_a',
        step_type='given',
        patterns=('the user logs in',),
        literal_prefixes=('the user',),
        literal_suffixes=('logs in',),
        literal_fragments=('user', 'logs'),
    )
    descriptor_b = _descriptor(
        file_b,
        function_name='step_b',
        step_type='step',
        patterns=('{name} performs action',),
        literal_prefixes=('',),
        literal_fragments=('performs action',),
        anchor_tokens=('performs', 'action'),
    )
    catalog = StepCatalog()
    catalog.extend(
        (
            _discovered_file(file_a, (descriptor_a,)),
            _discovered_file(file_b, (descriptor_b,), fallback=True),
        ),
    )

    assert catalog.discovered_files == (file_a, file_b)
    assert catalog.fallback_files == (file_b,)
    assert catalog.ast_resolved_file_count == 1
    assert catalog.fallback_file_count == 1
    assert catalog.requires_fallback_import(file_b)
    assert catalog.file_fingerprint(file_a) == 'hash'
    assert catalog.descriptors_for_file(file_a) == (descriptor_a,)
    assert catalog.find_candidate_files('given', 'the user logs in') == (
        file_a,
        file_b,
    )
    assert catalog.find_candidate_files('given', 'the user logs in') == (
        file_a,
        file_b,
    )
    assert catalog.find_candidate_files_for_steps(
        (
            StepQuery('given', 'the user logs in'),
            StepQuery('step', 'alice performs action'),
        ),
    ) == (file_a, file_b)

    catalog.clear()
    assert catalog.discovered_files == ()


def test_step_catalog_helper_functions_cover_edge_cases(tmp_path: Path) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _descriptor(
        file_path,
        function_name='step_fn',
        step_type='given',
        patterns=('the user logs in',),
        literal_prefixes=('the user',),
        literal_suffixes=('logs in',),
        literal_fragments=('user',),
        anchor_tokens=('user',),
    )
    assert get_conflicting_step_types('step') == (
        'step',
        'given',
        'when',
        'then',
        'but',
    )
    assert get_conflicting_step_types('given') == ('given', 'step')
    assert _build_module_import_path(tmp_path, file_path) == 'steps'
    assert _build_module_import_path(tmp_path, tmp_path) is None

    class _DummyRelative:
        def with_suffix(self, _suffix: str):
            return self

        @property
        def parts(self) -> tuple[str, ...]:
            return ()

    class _DummyPath:
        def relative_to(self, _root):
            return _DummyRelative()

    assert _build_module_import_path(tmp_path, _DummyPath()) is None  # type: ignore[arg-type]
    assert _collect_match_scores(
        keys=('user', 'admin'),
        files_by_key={'user': {file_path}},
    ) == {file_path: 1}
    assert _descriptor_may_match_query(
        descriptor,
        step_text='the user logs in',
        normalized_step_tokens={'the', 'user', 'logs', 'in'},
    )
    assert not _descriptor_may_match_query(
        descriptor,
        step_text='other',
        normalized_step_tokens={'other'},
    )

    knowledge_descriptor = SimpleNamespace(
        step_type='given',
        patterns=('x',),
        source_line=2,
        function_name='step_knowledge',
        literal_prefixes=('x',),
        literal_suffixes=(),
        literal_fragments=('x',),
        anchor_tokens=('x',),
        dynamic_fragment_count=0,
        documentation='doc',
        parser_cls_name=None,
        category='cat',
    )
    static_descriptor = _build_static_step_descriptor_from_knowledge(
        file_path=file_path,
        root_path=tmp_path,
        descriptor=knowledge_descriptor,
        discovery_mode='fallback_import',
    )
    assert static_descriptor.discovery_mode == 'fallback_import'
    assert _normalize_discovery_mode('fallback_import') == 'fallback_import'
    assert _normalize_discovery_mode('ast') == 'ast'
    assert _is_within_root(file_path, tmp_path)
    assert not _is_within_root(file_path, tmp_path / 'other')
    assert _is_allowed_definition_path(
        file_path,
        root_path=tmp_path / 'other',
        definition_paths=(tmp_path,),
    )
    non_matching_definition_file = (tmp_path / 'other_file.py').resolve()
    non_matching_definition_file.write_text('', encoding='utf-8')
    assert not _is_allowed_definition_path(
        file_path,
        root_path=tmp_path / 'other',
        definition_paths=(non_matching_definition_file,),
    )
    assert _tokenize_step_text('A user logs-in, now!') == (
        'user',
        'logs',
        'now',
    )


def test_step_catalog_branches_for_empty_literals_and_fragment_lengths(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _descriptor(
        file_path,
        function_name='step_fn',
        step_type='given',
        patterns=('x',),
        literal_prefixes=('',),
        literal_suffixes=('',),
        literal_fragments=('',),
        anchor_tokens=('user',),
    )
    catalog = StepCatalog()
    catalog.extend((_discovered_file(file_path, (descriptor,)),))
    assert catalog.find_candidate_files('given', 'x') == (file_path,)

    catalog._files_by_literal_fragment['given']['ab'].add(file_path)  # noqa: SLF001
    catalog._fragment_lengths_by_type['given'].add(2)  # noqa: SLF001
    catalog._files_by_literal_fragment['given']['abc'].add(file_path)  # noqa: SLF001
    catalog._fragment_lengths_by_type['given'].add(3)  # noqa: SLF001
    assert catalog._collect_literal_fragment_match_scores(  # noqa: SLF001
        step_type='given',
        normalized_step_text='abc',
    )[file_path] >= 1

    mismatch_descriptor = _descriptor(
        file_path,
        function_name='step_then',
        step_type='then',
        patterns=('result',),
    )
    catalog._descriptors_by_file[file_path] = (mismatch_descriptor,)  # noqa: SLF001
    assert not catalog._file_may_match_query(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='the user',
    )
