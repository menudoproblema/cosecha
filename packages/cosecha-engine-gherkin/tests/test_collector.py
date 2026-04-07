from __future__ import annotations

import hashlib

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.domain_events import (
    KnowledgeIndexedEvent,
    KnowledgeInvalidatedEvent,
    TestKnowledgeIndexedEvent as DomainTestKnowledgeIndexedEvent,
    TestKnowledgeInvalidatedEvent as DomainTestKnowledgeInvalidatedEvent,
)
from cosecha.core.exceptions import CosechaParserError
from cosecha.engine.gherkin import collector as collector_module
from cosecha.engine.gherkin.collector import (
    GherkinCollector,
    _build_definition_descriptor_knowledge,
    _build_test_content_hash,
    _build_test_descriptor_knowledge,
    _build_test_knowledge_version,
    _discover_removed_test_files,
    _discover_step_directories,
    _find_step_impl_files,
    _fingerprint_step_file,
    _get_cached_feature,
    _get_thread_local_parser,
    _is_path_within_collect_scope,
    _load_step_impl_files,
    _normalize_configured_step_directories,
    _parse_and_generate_model,
    _parse_feature_content,
    _resolve_collect_scope_path,
    _restore_cached_features,
    _should_use_disk_feature_cache,
    _snapshot_step_files,
    _store_cached_feature,
)
from cosecha.engine.gherkin.items import GherkinTestItem
from cosecha.engine.gherkin.step_ast_discovery import (
    StaticDiscoveredStepFile,
    StaticStepDescriptor,
)
from cosecha.engine.gherkin.step_catalog import StepCatalog
from cosecha_internal.testkit import build_config


class _FakeDomainEventStream:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def emit(self, event: object) -> None:
        self.events.append(event)


class _FakeKnowledgeStore:
    def __init__(self, discovered=()) -> None:
        self._discovered = tuple(discovered)
        self._project_index = StepCatalog()

    def get_discovered_step_files(self):
        return self._discovered

    def set_discovered_step_files(self, discovered_files):
        self._discovered = tuple(discovered_files)

    def set_project_step_index(self, step_index) -> None:
        self._project_index = step_index

    def get_project_step_index(self):
        return self._project_index


class _FakeRegistry:
    @contextmanager
    def bulk_load(self):
        yield


def _build_static_descriptor(file_path: Path) -> StaticStepDescriptor:
    return StaticStepDescriptor(
        step_type='given',
        patterns=('a step',),
        source_file=file_path,
        source_line=1,
        function_name='step_fn',
        file_path=file_path,
        module_import_path='steps.demo',
        literal_prefixes=('a step',),
        literal_suffixes=('a step',),
        literal_fragments=('a step',),
        anchor_tokens=('step',),
        discovery_mode='ast',
    )


def _build_discovered_file(
    file_path: Path,
    *,
    digest: str,
    discovery_mode: str = 'ast',
    mtime_ns: int = 1,
    file_size: int = 1,
) -> StaticDiscoveredStepFile:
    return StaticDiscoveredStepFile(
        file_path=file_path,
        module_import_path='steps.demo',
        descriptors=(_build_static_descriptor(file_path),),
        discovery_mode=discovery_mode,  # type: ignore[arg-type]
        requires_fallback_import=discovery_mode == 'fallback_import',
        content_digest=digest,
        mtime_ns=mtime_ns,
        file_size=file_size,
    )


def _build_test_item(root_path: Path, file_path: Path) -> GherkinTestItem:
    feature = _parse_feature_content(
        '\n'.join(
            (
                '@feature-tag',
                'Feature: Demo',
                '  @scenario-tag @scenario-tag',
                '  Scenario: Works',
                '    Given a step',
            ),
        ),
        file_path,
        root_path.parent,
    )
    return GherkinTestItem(
        feature=feature,
        scenario=feature.scenarios[0],
        example=None,
        path=file_path,
    )


def test_cache_and_parser_helpers(monkeypatch, tmp_path: Path) -> None:
    collector_module._feature_cache.clear()
    parser = _get_thread_local_parser()
    assert _get_thread_local_parser() is parser

    key_a = (tmp_path / 'a.feature', 1, 1)
    key_b = (tmp_path / 'b.feature', 2, 2)
    feature_a = SimpleNamespace(name='a')
    feature_b = SimpleNamespace(name='b')
    monkeypatch.setattr(collector_module, '_FEATURE_CACHE_LIMIT', 1)

    _store_cached_feature(key_a, feature_a)
    _store_cached_feature(key_b, feature_b)
    assert _get_cached_feature(key_a) is None
    assert _get_cached_feature(key_b) is feature_b

    _restore_cached_features({key_a: feature_a, key_b: feature_b})
    assert _get_cached_feature(key_b) is feature_b
    _restore_cached_features({})
    assert collector_module._feature_cache == {}


def test_file_and_directory_helpers(tmp_path: Path) -> None:
    steps_dir = tmp_path / 'tests' / 'steps'
    steps_dir.mkdir(parents=True)
    step_file = steps_dir / 'demo_steps.py'
    step_file.write_text('print("ok")\n', encoding='utf-8')
    missing_file = steps_dir / 'missing.py'

    files = _find_step_impl_files((steps_dir, steps_dir))
    assert files == [step_file]

    snapshots = _snapshot_step_files((step_file, missing_file))
    assert snapshots[0][0] == step_file.resolve()
    assert len(snapshots) == 1

    assert _fingerprint_step_file(step_file) == hashlib.sha256(
        step_file.read_bytes(),
    ).hexdigest()

    normalized = _normalize_configured_step_directories(
        (
            steps_dir,
            step_file,
            tmp_path / 'README.md',
        ),
    )
    assert normalized == (steps_dir.resolve(),)


def test_scope_helpers_cover_all_branches(tmp_path: Path) -> None:
    root_path = (tmp_path / 'project').resolve()
    nested = root_path / 'suite' / 'subsuite'
    (nested / 'steps').mkdir(parents=True)
    (root_path / 'steps').mkdir(parents=True)
    test_file = nested / 'demo.feature'
    test_file.write_text('Feature: demo\n', encoding='utf-8')

    discovered, traversed = _discover_step_directories(
        test_file,
        root_path,
        frozenset(),
    )
    assert (nested / 'steps').resolve() in discovered
    assert root_path.resolve() in traversed

    no_discovery = _discover_step_directories(
        Path('/'),
        root_path,
        frozenset(),
    )
    assert no_discovery[0] == ()
    scanned_only = _discover_step_directories(
        test_file,
        root_path,
        frozenset({test_file.parent.resolve()}),
    )
    assert scanned_only == ((), ())

    relative_scope_path = _resolve_collect_scope_path(root_path, Path('suite'))
    absolute_scope_path = _resolve_collect_scope_path(root_path, root_path)
    assert relative_scope_path == (root_path / 'suite').resolve()
    assert absolute_scope_path == root_path

    inside_file = (root_path / 'suite' / 'demo.feature').resolve()
    excluded = (root_path / 'suite' / 'excluded').resolve()
    assert _is_path_within_collect_scope(
        inside_file,
        collect_paths=((root_path / 'suite').resolve(),),
        excluded_paths=(excluded,),
    )
    assert not _is_path_within_collect_scope(
        (excluded / 'x.feature').resolve(),
        collect_paths=((root_path / 'suite').resolve(),),
        excluded_paths=(excluded,),
    )
    assert not _is_path_within_collect_scope(
        (root_path / 'outside.feature').resolve(),
        collect_paths=((root_path / 'suite').resolve(),),
        excluded_paths=(),
    )


def test_parse_helpers_and_disk_cache_strategy(
    monkeypatch,
    tmp_path: Path,
) -> None:
    root_parent = tmp_path
    feature_file = tmp_path / 'project' / 'demo.feature'
    feature_file.parent.mkdir(parents=True)
    feature_file.write_text(
        '\n'.join(
            (
                'Feature: Demo',
                '  Scenario: Example',
                '    Given a step',
            ),
        ),
        encoding='utf-8',
    )

    parsed_feature = _parse_and_generate_model(feature_file, root_parent)
    assert parsed_feature is not None
    assert parsed_feature.name == 'Demo'

    parsed_from_content = _parse_feature_content(
        feature_file.read_text(encoding='utf-8'),
        feature_file,
        root_parent,
    )
    assert parsed_from_content.name == 'Demo'

    class _BrokenParser:
        def parse(self, _text: str):
            raise RuntimeError('broken')

    monkeypatch.setattr(
        collector_module,
        '_get_thread_local_parser',
        lambda: _BrokenParser(),
    )
    with pytest.raises(RuntimeError, match='broken'):
        _parse_and_generate_model(feature_file, root_parent)

    assert _should_use_disk_feature_cache(()) is True
    assert _should_use_disk_feature_cache((feature_file,)) is False
    feature_file.with_suffix('.txt').write_text('', encoding='utf-8')
    assert _should_use_disk_feature_cache(
        (feature_file.with_suffix('.txt'),),
    ) is False

    many_dir = tmp_path / 'many'
    many_dir.mkdir()
    for index in range(20):
        (many_dir / f'{index}.feature').write_text(
            'Feature: x\n',
            encoding='utf-8',
        )
    assert _should_use_disk_feature_cache((many_dir,)) is True


def test_load_step_impl_files_reports_failures(monkeypatch, tmp_path: Path) -> None:
    file_ok = tmp_path / 'ok.py'
    file_ok.write_text('', encoding='utf-8')
    file_fail = tmp_path / 'fail.py'
    file_fail.write_text('', encoding='utf-8')

    def _loader(file_path, _registry):
        if file_path == file_fail:
            raise RuntimeError('boom')

    monkeypatch.setattr(
        collector_module,
        'import_and_load_steps_from_module',
        _loader,
    )

    failures = _load_step_impl_files((file_ok, file_fail), _FakeRegistry())
    assert len(failures) == 1
    assert failures[0][0] == file_fail
    assert 'RuntimeError: boom' in failures[0][1]


@pytest.mark.asyncio
async def test_collector_initialize_and_collect_flow(monkeypatch, tmp_path: Path) -> None:
    fake_store = _FakeKnowledgeStore()
    monkeypatch.setattr(
        collector_module,
        'PersistentKnowledgeStore',
        lambda *args, **kwargs: fake_store,
    )
    config = build_config(tmp_path)
    steps_dir = tmp_path / 'steps'
    steps_dir.mkdir()
    config.definition_paths = (steps_dir,)

    collector = GherkinCollector()
    collector.initialize(config)
    assert collector.knowledge_store is fake_store
    assert collector._configured_step_directories == (steps_dir.resolve(),)

    test_item = _build_test_item(tmp_path, tmp_path / 'suite' / 'demo.feature')

    async def _fake_super_collect(self, _path, _excluded_paths):
        self.collected_files = {Path('suite/demo.feature')}
        self.collected_tests = (test_item,)
        self.failed_files = {Path('suite/failed.feature')}

    monkeypatch.setattr(
        'cosecha.core.collector.Collector.collect',
        _fake_super_collect,
    )

    discovered_calls: list[Path] = []

    async def _fake_find_steps(self, path: Path) -> None:
        del self
        discovered_calls.append(path)

    monkeypatch.setattr(
        GherkinCollector,
        'find_step_impl_directories',
        _fake_find_steps,
    )

    emitted_calls: list[tuple[tuple[Path, ...], tuple[Path, ...]]] = []

    async def _fake_emit_tests(
        self,
        *,
        collect_paths,
        excluded_paths,
    ) -> None:
        del self
        emitted_calls.append((collect_paths, excluded_paths))

    monkeypatch.setattr(
        GherkinCollector,
        '_emit_test_knowledge_events',
        _fake_emit_tests,
    )

    class _FakeDiskCache:
        def __init__(self, *_args):
            self.saved_payload = None

        def load(self):
            return {}

        def save(self, payload):
            self.saved_payload = dict(payload)

    monkeypatch.setattr(collector_module, 'DiskCache', _FakeDiskCache)

    collector.skip_step_catalog_discovery = True
    await collector.collect(path=(tmp_path,), excluded_paths=(tmp_path / 'nope',))
    assert discovered_calls
    assert emitted_calls
    assert collector._disk_cache is not None

    build_step_catalog_calls: list[bool] = []

    async def _fake_build_step_catalog(self) -> None:
        del self
        build_step_catalog_calls.append(True)

    monkeypatch.setattr(
        GherkinCollector,
        'build_step_catalog',
        _fake_build_step_catalog,
    )
    collector.skip_step_catalog_discovery = False
    await collector.collect(path=(tmp_path,), excluded_paths=())
    assert build_step_catalog_calls == [True]


@pytest.mark.asyncio
async def test_collector_find_test_files_and_load_feature_branches(
    monkeypatch,
    tmp_path: Path,
) -> None:
    collector = GherkinCollector()
    collector.initialize(build_config(tmp_path))
    collector.knowledge_store = _FakeKnowledgeStore()

    async def _fake_find_test_files(self, _base_path):
        return [tmp_path / 'a.feature']

    monkeypatch.setattr(
        'cosecha.core.collector.Collector.find_test_files',
        _fake_find_test_files,
    )

    discovered: list[Path] = []

    async def _fake_find_steps(self, path: Path) -> None:
        del self
        discovered.append(path)

    monkeypatch.setattr(
        GherkinCollector,
        'find_step_impl_directories',
        _fake_find_steps,
    )
    assert await collector.find_test_files(tmp_path) == [tmp_path / 'a.feature']
    assert discovered == [tmp_path]

    async def _fake_empty_find_test_files(self, _base_path):
        return []

    monkeypatch.setattr(
        'cosecha.core.collector.Collector.find_test_files',
        _fake_empty_find_test_files,
    )
    assert await collector.find_test_files(tmp_path) == []

    cached_feature = SimpleNamespace(name='cached')
    monkeypatch.setattr(
        collector_module,
        '_get_cached_feature',
        lambda _cache_key: cached_feature,
    )
    assert await collector._load_feature(tmp_path / 'missing.feature') is cached_feature

    errors: list[tuple[str, str | None]] = []
    collector.config.diagnostics.error = lambda message, **kwargs: errors.append(  # type: ignore[method-assign]
        (message, kwargs.get('details')),
    )
    monkeypatch.setattr(
        collector_module,
        '_get_cached_feature',
        lambda _cache_key: None,
    )
    parser_error = CosechaParserError('bad', tmp_path / 'a.feature', 2, 3)
    monkeypatch.setattr(
        collector_module,
        '_parse_and_generate_model',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(parser_error),
    )
    assert await collector._load_feature(tmp_path / 'a.feature') is None

    monkeypatch.setattr(
        collector_module,
        '_parse_and_generate_model',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError('boom')),
    )
    assert await collector._load_feature(tmp_path / 'a.feature') is None
    assert any('Invalid format in test file' in message for message, _ in errors)
    assert any('Fail loading test from' in message for message, _ in errors)

    monkeypatch.setattr(
        collector_module,
        '_parse_feature_content',
        lambda *_args, **_kwargs: _parse_feature_content(
            '\n'.join(
                (
                    'Feature: Demo',
                    '  Scenario: Works',
                    '    Given a step',
                ),
            ),
            tmp_path / 'suite' / 'inline.feature',
            tmp_path.parent,
        ),
    )
    loaded_from_content = await collector.load_tests_from_content(
        'Feature: ignored',
        tmp_path / 'suite' / 'inline.feature',
    )
    assert loaded_from_content

    async def _fake_load_feature(self, _path):
        del self, _path
        return None

    monkeypatch.setattr(GherkinCollector, '_load_feature', _fake_load_feature)
    assert await collector.load_tests_from_file(tmp_path / 'suite' / 'x.feature') is None


@pytest.mark.asyncio
async def test_build_step_catalog_emits_invalidations_and_updates_store(
    monkeypatch,
    tmp_path: Path,
) -> None:
    collector = GherkinCollector()
    collector.initialize(build_config(tmp_path))
    file_a = (tmp_path / 'steps' / 'a.py').resolve()
    file_b = (tmp_path / 'steps' / 'b.py').resolve()
    file_c = (tmp_path / 'steps' / 'c.py').resolve()
    file_d = (tmp_path / 'steps' / 'd.py').resolve()
    file_removed = (tmp_path / 'steps' / 'removed.py').resolve()
    for file_path in (file_a, file_b, file_c, file_d):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text('', encoding='utf-8')

    cached_a = _build_discovered_file(file_a, digest='hash-a', mtime_ns=1, file_size=1)
    cached_b = _build_discovered_file(file_b, digest='hash-old', mtime_ns=2, file_size=2)
    cached_c = _build_discovered_file(file_c, digest='hash-c', mtime_ns=3, file_size=3)
    cached_d = _build_discovered_file(file_d, digest='hash-d', mtime_ns=10, file_size=10)
    cached_removed = _build_discovered_file(file_removed, digest='hash-r')
    collector.knowledge_store = _FakeKnowledgeStore(
        discovered=(cached_a, cached_b, cached_c, cached_d, cached_removed),
    )
    collector.steps_directories = {tmp_path / 'steps'}

    monkeypatch.setattr(
        collector_module,
        '_find_step_impl_files',
        lambda _dirs: [file_a, file_b, file_c],
    )
    monkeypatch.setattr(
        collector_module,
        '_snapshot_step_files',
        lambda _files: (
            (file_a, 1, 1),
            (file_b, 2, 2),
            (file_c, 3, 3),
            (file_d, 4, 4),
        ),
    )

    def _fingerprint(path: Path) -> str:
        if path == file_c:
            raise OSError('cannot read')
        return 'hash-a' if path == file_a else 'hash-new'

    monkeypatch.setattr(collector_module, '_fingerprint_step_file', _fingerprint)

    class _FakeDiscoveryService:
        def __init__(self, _root_path: Path) -> None:
            return

        def discover_step_files(self, files: tuple[Path, ...]):
            return tuple(
                _build_discovered_file(file_path, digest=f'discovered-{file_path.name}')
                for file_path in files
            )

    monkeypatch.setattr(
        collector_module,
        'StepDiscoveryService',
        _FakeDiscoveryService,
    )

    emitted_arguments: list[tuple[tuple[StaticDiscoveredStepFile, ...], tuple[tuple[Path, str], ...]]] = []

    async def _fake_emit(self, discovered_files, invalidated_files):
        del self
        emitted_arguments.append((discovered_files, invalidated_files))

    monkeypatch.setattr(
        GherkinCollector,
        '_emit_step_catalog_events',
        _fake_emit,
    )
    await collector.build_step_catalog()

    assert collector.knowledge_store.get_discovered_step_files()
    assert collector.get_project_step_index() is collector.knowledge_store.get_project_step_index()
    assert emitted_arguments
    invalidated = dict(emitted_arguments[0][1])
    assert invalidated[file_b] == 'content_changed'
    assert invalidated[file_d] == 'metadata_changed'
    assert invalidated[file_removed] == 'file_removed'
    assert any(
        discovered_file.file_path == file_c
        for discovered_file in emitted_arguments[0][0]
    )


@pytest.mark.asyncio
async def test_emit_step_catalog_events_and_test_knowledge_events(
    monkeypatch,
    tmp_path: Path,
) -> None:
    collector = GherkinCollector()
    collector.initialize(build_config(tmp_path))
    collector.knowledge_store = _FakeKnowledgeStore()
    file_path = (tmp_path / 'suite' / 'demo.feature').resolve()
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(
        '\n'.join(
            (
                'Feature: Demo',
                '  Scenario: Works',
                '    Given a step',
            ),
        ),
        encoding='utf-8',
    )
    test_item = _build_test_item(tmp_path, file_path)
    collector.collected_files = {Path('suite/demo.feature')}
    collector.collected_tests = (
        test_item,
        SimpleNamespace(path=file_path),
    )
    collector.failed_files = {Path('suite/failed.feature')}
    collector._engine_name = 'gherkin'

    stream = _FakeDomainEventStream()
    collector._domain_event_stream = stream

    discovered_file = _build_discovered_file(
        (tmp_path / 'steps' / 'defs.py').resolve(),
        digest='digest',
    )
    await collector._emit_step_catalog_events(
        (discovered_file,),
        (((tmp_path / 'steps' / 'old.py').resolve(), 'file_removed'),),
    )
    assert any(isinstance(event, KnowledgeInvalidatedEvent) for event in stream.events)
    assert any(isinstance(event, KnowledgeIndexedEvent) for event in stream.events)

    monkeypatch.setattr(
        collector_module,
        '_discover_removed_test_files',
        lambda **_kwargs: (Path('suite/removed.feature'),),
    )
    await collector._emit_test_knowledge_events(
        collect_paths=(tmp_path,),
        excluded_paths=(),
    )
    assert any(
        isinstance(event, DomainTestKnowledgeIndexedEvent)
        for event in stream.events
    )
    assert any(
        isinstance(event, DomainTestKnowledgeInvalidatedEvent)
        for event in stream.events
    )

    collector._domain_event_stream = None
    await collector._emit_step_catalog_events((), ())
    await collector._emit_test_knowledge_events(
        collect_paths=(tmp_path,),
        excluded_paths=(),
    )


@pytest.mark.asyncio
async def test_load_step_impl_records_failures(monkeypatch, tmp_path: Path) -> None:
    collector = GherkinCollector()
    collector.initialize(build_config(tmp_path))
    collector.knowledge_store = _FakeKnowledgeStore()
    collector.steps_directories = {tmp_path}
    collector.failed_files = set()
    recorded_errors: list[str] = []
    collector.config.diagnostics.error = lambda message, **_kwargs: recorded_errors.append(  # type: ignore[method-assign]
        message,
    )

    failing_file = (tmp_path / 'steps.py').resolve()
    monkeypatch.setattr(collector_module, '_find_step_impl_files', lambda _dirs: [failing_file])
    monkeypatch.setattr(
        collector_module,
        '_load_step_impl_files',
        lambda _files, _registry: [(failing_file, 'traceback')],
    )
    await collector.load_step_impl(_FakeRegistry())
    assert failing_file in collector.failed_files
    assert any('Fail loading steps from' in message for message in recorded_errors)


def test_definition_and_test_descriptor_helpers(tmp_path: Path) -> None:
    feature_file = (tmp_path / 'suite' / 'demo.feature').resolve()
    feature_file.parent.mkdir(parents=True, exist_ok=True)
    feature_file.write_text('Feature: Demo\n', encoding='utf-8')

    descriptor = _build_static_descriptor((tmp_path / 'steps.py').resolve())
    definition_record = _build_definition_descriptor_knowledge(descriptor)
    assert definition_record.payload['step_type'] == 'given'

    test_item = _build_test_item(tmp_path, feature_file)
    test_knowledge = _build_test_descriptor_knowledge(
        test_item,
        root_path=tmp_path,
        engine_name='gherkin',
        file_path_label='suite/demo.feature',
    )
    assert test_knowledge.selection_labels == ('@feature-tag', '@scenario-tag')
    assert _build_test_content_hash(feature_file) == hashlib.sha256(
        feature_file.read_bytes(),
    ).hexdigest()
    assert _build_test_knowledge_version(feature_file).startswith(
        'gherkin_test_index:',
    )


def test_discover_removed_test_files(monkeypatch, tmp_path: Path) -> None:
    root_path = tmp_path
    missing_db_path = tmp_path / 'missing.db'
    monkeypatch.setattr(
        collector_module,
        'resolve_knowledge_base_path',
        lambda _root_path: missing_db_path,
    )
    assert (
        _discover_removed_test_files(
            root_path=root_path,
            engine_name='gherkin',
            collect_scope=((root_path,), ()),
            indexed_files=(),
            failed_files=(),
        )
        == ()
    )

    db_path = tmp_path / 'kb.db'
    db_path.write_text('', encoding='utf-8')
    removed_relative = Path('suite/removed.feature')
    existing_relative = Path('suite/existing.feature')
    excluded_relative = Path('suite/excluded.feature')
    indexed_relative = Path('suite/indexed.feature')
    outside_relative = Path('other/outside.feature')

    existing_absolute = (root_path / existing_relative).resolve()
    excluded_absolute = (root_path / excluded_relative).resolve()
    existing_absolute.parent.mkdir(parents=True, exist_ok=True)
    existing_absolute.write_text('Feature: existing\n', encoding='utf-8')
    excluded_absolute.parent.mkdir(parents=True, exist_ok=True)
    excluded_absolute.write_text('Feature: excluded\n', encoding='utf-8')

    class _FakeKB:
        def __init__(self, _db_path: Path) -> None:
            self.closed = False

        def query_tests(self, _query):
            return (
                SimpleNamespace(test_path=str(removed_relative)),
                SimpleNamespace(test_path=str(existing_relative)),
                SimpleNamespace(test_path=str(excluded_relative)),
                SimpleNamespace(test_path=str(indexed_relative)),
                SimpleNamespace(test_path=str(outside_relative)),
            )

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        collector_module,
        'resolve_knowledge_base_path',
        lambda _root_path: db_path,
    )
    monkeypatch.setattr(
        collector_module,
        'ReadOnlyPersistentKnowledgeBase',
        _FakeKB,
    )

    removed_paths = _discover_removed_test_files(
        root_path=root_path,
        engine_name='gherkin',
        collect_scope=((root_path / 'suite',), (root_path / 'suite' / 'excluded',)),
        indexed_files=(indexed_relative,),
        failed_files=(Path('suite/failed.feature'),),
    )
    assert removed_paths == (removed_relative,)
