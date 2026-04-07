from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.engine.gherkin.step_materialization import LazyStepResolver
from cosecha.engine.gherkin.steps.registry import StepRegistry


@dataclass(frozen=True)
class _Descriptor:
    function_name: str
    step_type: str
    patterns: tuple[str, ...]
    parser_cls_name: str | None = None
    literal_prefixes: tuple[str, ...] = ()
    literal_suffixes: tuple[str, ...] = ()
    literal_fragments: tuple[str, ...] = ()


class _FakeStepIndex:
    def __init__(
        self,
        *,
        candidate_files: dict[tuple[str, str], tuple[Path, ...]] | None = None,
        descriptors_by_file: dict[Path, tuple[_Descriptor, ...]] | None = None,
        fallback_files: set[Path] | None = None,
        fingerprints: dict[Path, str | None] | None = None,
    ) -> None:
        self.candidate_files = candidate_files or {}
        self.descriptors_by_file = descriptors_by_file or {}
        self.fallback_files = fallback_files or set()
        self.fingerprints = fingerprints or {}

    def find_candidate_files(
        self,
        step_type: str,
        step_text: str,
    ) -> tuple[Path, ...]:
        return self.candidate_files.get((step_type, step_text), ())

    def requires_fallback_import(self, file_path: Path) -> bool:
        return file_path.resolve() in {path.resolve() for path in self.fallback_files}

    def descriptors_for_file(self, file_path: Path):
        return self.descriptors_by_file.get(file_path.resolve(), ())

    def file_fingerprint(self, file_path: Path) -> str | None:
        return self.fingerprints.get(file_path.resolve())


def test_find_candidate_files_and_prime_cache_behaviour(tmp_path: Path) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    index = _FakeStepIndex(
        candidate_files={('given', 'a step'): (file_path,)},
    )
    resolver = LazyStepResolver(index)

    first = resolver.find_candidate_files('given', 'a step')
    index.candidate_files[('given', 'a step')] = ()
    second = resolver.find_candidate_files('given', 'a step')

    assert first == (file_path,)
    assert second == (file_path,)

    resolver.prime_candidate_files(
        (('given', 'a step'), ('when', 'another step')),
        (file_path, file_path),
    )
    assert resolver.find_candidate_files('when', 'another step') == (file_path,)


def test_build_relevant_function_names_and_descriptor_matching_paths(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    matching_descriptor = _Descriptor(
        function_name='step_given_user',
        step_type='given',
        patterns=('a user named {name}',),
        parser_cls_name='ParseStepMatcher',
        literal_prefixes=('a user',),
        literal_fragments=('user', 'named'),
    )
    non_matching_parser_descriptor = _Descriptor(
        function_name='step_runtime',
        step_type='given',
        patterns=('runtime {name}',),
        parser_cls_name='CustomMatcher',
        literal_fragments=('runtime',),
    )
    index = _FakeStepIndex(
        descriptors_by_file={
            file_path: (
                matching_descriptor,
                non_matching_parser_descriptor,
            ),
        },
    )
    resolver = LazyStepResolver(index)

    relevant = resolver._build_relevant_function_names(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='a user named alice',
    )
    assert relevant == ('step_given_user',)
    assert resolver._descriptor_supports_text_matching(matching_descriptor) is True  # noqa: SLF001
    assert resolver._descriptor_supports_text_matching(non_matching_parser_descriptor) is False  # noqa: SLF001
    assert resolver._descriptor_matches_step_text(  # noqa: SLF001
        matching_descriptor,
        step_type='given',
        step_text='a user named alice',
    )
    assert not resolver._descriptor_matches_step_text(  # noqa: SLF001
        matching_descriptor,
        step_type='then',
        step_text='a user named alice',
    )
    assert not resolver._descriptor_may_match_by_structure(  # noqa: SLF001
        _Descriptor(
            function_name='step',
            step_type='given',
            patterns=('x',),
            literal_prefixes=('prefix',),
        ),
        step_text='different text',
    )

    no_descriptor_index = _FakeStepIndex(descriptors_by_file={})
    no_descriptor_resolver = LazyStepResolver(no_descriptor_index)
    assert no_descriptor_resolver._build_relevant_function_names(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='x',
    ) == ()

    fallback_index = _FakeStepIndex(
        descriptors_by_file={},
        fallback_files={file_path},
    )
    fallback_resolver = LazyStepResolver(fallback_index)
    assert fallback_resolver._build_relevant_function_names(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='x',
    ) is None


def test_materialize_for_query_success_and_terminal_cache_pruning(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _Descriptor(
        function_name='step_given_user',
        step_type='given',
        patterns=('a user',),
        parser_cls_name='ParseStepMatcher',
    )
    index = _FakeStepIndex(
        candidate_files={('given', 'a user'): (file_path,)},
        descriptors_by_file={file_path: (descriptor,)},
    )
    imported: list[tuple[Path, tuple[str, ...] | None]] = []
    materialized: list[Path] = []
    resolver = LazyStepResolver(
        index,
        import_step_file=lambda path, _registry, function_names: imported.append(
            (path.resolve(), function_names),
        ),
        on_materialized=lambda path: materialized.append(path.resolve()),
    )
    step_registry = StepRegistry()

    assert resolver.materialize_for_query('given', 'a user', step_registry) is True
    assert imported == [(file_path, ('step_given_user',))]
    assert materialized == [file_path]
    assert resolver.loaded_files == (file_path,)
    assert resolver.materialize_for_query('given', 'a user', step_registry) is False


def test_materialize_for_query_failure_paths_and_negative_cache(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _Descriptor(
        function_name='step_runtime',
        step_type='given',
        patterns=('runtime {name}',),
        parser_cls_name='CustomMatcher',
        literal_fragments=('runtime',),
    )
    index = _FakeStepIndex(
        candidate_files={('given', 'runtime alpha'): (file_path,)},
        descriptors_by_file={file_path: (descriptor,)},
        fallback_files={file_path},
        fingerprints={file_path: 'fingerprint'},
    )
    failures: list[tuple[Path, str]] = []
    resolver = LazyStepResolver(
        index,
        import_step_file=lambda *_args: (_ for _ in ()).throw(RuntimeError('boom')),
        on_load_failure=lambda path, text: failures.append((path.resolve(), text)),
    )

    assert resolver.materialize_for_query('given', 'runtime alpha', StepRegistry())
    assert resolver.failed_files == (file_path,)
    assert failures and 'RuntimeError: boom' in failures[0][1]

    resolver._record_negative_query(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='runtime alpha',
    )
    assert resolver._is_negative_cached(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='runtime alpha',
    )

    resolver_without_handler = LazyStepResolver(
        index,
        import_step_file=lambda *_args: (_ for _ in ()).throw(RuntimeError('boom')),
    )
    with pytest.raises(RuntimeError, match='boom'):
        resolver_without_handler.materialize_for_query(
            'given',
            'runtime alpha',
            StepRegistry(),
        )


def test_internal_materialization_helpers_cover_all_paths(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor_a = _Descriptor(
        function_name='step_a',
        step_type='given',
        patterns=('a',),
    )
    descriptor_b = _Descriptor(
        function_name='step_b',
        step_type='given',
        patterns=('b',),
    )
    index = _FakeStepIndex(
        descriptors_by_file={file_path: (descriptor_a, descriptor_b)},
        fingerprints={file_path: None},
    )
    resolver = LazyStepResolver(index)

    resolver._mark_file_as_materialized(file_path, ('step_a',))  # noqa: SLF001
    assert resolver._are_query_functions_materialized(  # noqa: SLF001
        file_path,
        ('step_a',),
    )
    assert not resolver._are_all_step_functions_materialized(file_path)  # noqa: SLF001

    resolver._mark_file_as_materialized(file_path, ('step_b',))  # noqa: SLF001
    assert resolver._are_all_step_functions_materialized(file_path)  # noqa: SLF001
    assert file_path in resolver.loaded_files

    resolver._candidate_files_by_query[('given', 'a')] = (file_path,)  # noqa: SLF001
    resolver._prune_terminal_query_cache()  # noqa: SLF001
    assert ('given', 'a') not in resolver._candidate_files_by_query  # noqa: SLF001

    unresolved = (tmp_path / 'unresolved.py').resolve()
    unresolved_index = _FakeStepIndex(
        descriptors_by_file={},
        fallback_files={unresolved},
    )
    unresolved_resolver = LazyStepResolver(unresolved_index)
    assert not unresolved_resolver._is_query_terminal_for_file(  # noqa: SLF001
        unresolved,
        step_type='given',
        step_text='b',
        terminal_files=set(),
    )
    assert resolver._are_query_functions_materialized(unresolved, ())  # noqa: SLF001
    assert not resolver._are_query_functions_materialized(  # noqa: SLF001
        unresolved,
        None,
    )


def test_materialize_for_query_skips_failed_loaded_negative_and_preloaded(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _Descriptor(
        function_name='step_given',
        step_type='given',
        patterns=('a user',),
    )
    index = _FakeStepIndex(
        candidate_files={('given', 'a user'): (file_path,)},
        descriptors_by_file={file_path: (descriptor,)},
        fingerprints={file_path: 'fp'},
    )
    imported: list[Path] = []
    resolver = LazyStepResolver(
        index,
        import_step_file=lambda path, *_args: imported.append(path.resolve()),
    )
    registry = StepRegistry()

    resolver._failed_files.add(file_path)  # noqa: SLF001
    assert not resolver.materialize_for_query('given', 'a user', registry)
    resolver._failed_files.clear()  # noqa: SLF001

    resolver._loaded_files.add(file_path)  # noqa: SLF001
    assert not resolver.materialize_for_query('given', 'a user', registry)
    resolver._loaded_files.clear()  # noqa: SLF001

    resolver._record_negative_query(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='a user',
    )
    assert not resolver.materialize_for_query('given', 'a user', registry)
    resolver._negative_query_cache.clear()  # noqa: SLF001

    resolver._loaded_step_functions_by_file[file_path] = {'step_given'}  # noqa: SLF001
    assert not resolver.materialize_for_query('given', 'a user', registry)
    assert imported == []


def test_materialize_records_negative_queries_for_non_importable_candidates(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    non_matching_descriptor = _Descriptor(
        function_name='step_runtime',
        step_type='given',
        patterns=('runtime {name}',),
        parser_cls_name='CustomMatcher',
    )
    index = _FakeStepIndex(
        candidate_files={('given', 'runtime alpha'): (file_path,)},
        descriptors_by_file={file_path: (non_matching_descriptor,)},
        fingerprints={file_path: 'fp'},
    )
    resolver = LazyStepResolver(index)

    assert not resolver.materialize_for_query(
        'given',
        'runtime alpha',
        StepRegistry(),
    )
    assert resolver._is_negative_cached(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='runtime alpha',
    )


def test_materialize_records_negative_queries_when_relevant_functions_empty(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    index = _FakeStepIndex(
        candidate_files={('given', 'alpha'): (file_path,)},
        descriptors_by_file={},
        fingerprints={file_path: 'fp'},
    )
    resolver = LazyStepResolver(index)

    assert not resolver.materialize_for_query('given', 'alpha', StepRegistry())
    assert resolver._is_negative_cached(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='alpha',
    )


def test_trim_query_cache_respects_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    index = _FakeStepIndex()
    resolver = LazyStepResolver(index)
    resolver._candidate_files_by_query[('given', 'a')] = ()  # noqa: SLF001
    resolver._candidate_files_by_query[('given', 'b')] = ()  # noqa: SLF001
    monkeypatch.setattr(
        'cosecha.engine.gherkin.step_materialization.QUERY_CACHE_LIMIT',
        1,
    )

    resolver._trim_query_cache()  # noqa: SLF001

    assert list(resolver._candidate_files_by_query) == [('given', 'b')]  # noqa: SLF001


def test_build_relevant_function_names_skips_non_conflicting_and_duplicates(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    index = _FakeStepIndex(
        descriptors_by_file={
            file_path: (
                _Descriptor(
                    function_name='step_then',
                    step_type='then',
                    patterns=('result',),
                ),
                _Descriptor(
                    function_name='step_given',
                    step_type='given',
                    patterns=('a user',),
                ),
                _Descriptor(
                    function_name='step_given',
                    step_type='given',
                    patterns=('a user',),
                ),
                _Descriptor(
                    function_name='step_given_mismatch',
                    step_type='given',
                    patterns=('another user',),
                ),
            ),
        },
    )
    resolver = LazyStepResolver(index)

    relevant = resolver._build_relevant_function_names(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='a user',
    )

    assert relevant == ('step_given',)


def test_materialization_helpers_cover_remaining_terminal_and_query_paths(
    tmp_path: Path,
) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    index = _FakeStepIndex(
        descriptors_by_file={file_path: ()},
        fingerprints={file_path: None},
    )
    resolver = LazyStepResolver(index)

    resolver._record_negative_query(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='a',
    )
    assert not resolver._negative_query_cache  # noqa: SLF001
    assert not resolver._is_negative_cached(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='a',
    )
    assert resolver._build_relevant_function_names(  # noqa: SLF001
        file_path,
        step_type='given',
        step_text='a',
    ) == ()
    assert resolver._descriptor_matches_step_text(  # noqa: SLF001
        _Descriptor(
            function_name='step',
            step_type='given',
            patterns=('x',),
            literal_suffixes=('z',),
        ),
        step_type='given',
        step_text='abc',
    ) is False

    unresolved = (tmp_path / 'unresolved.py').resolve()
    unresolved_index = _FakeStepIndex(
        descriptors_by_file={},
        fallback_files={unresolved},
    )
    unresolved_resolver = LazyStepResolver(unresolved_index)
    assert unresolved_resolver._build_relevant_function_names(  # noqa: SLF001
        unresolved,
        step_type='given',
        step_text='x',
    ) is None

    resolver._mark_file_as_materialized(file_path, None)  # noqa: SLF001
    assert file_path in resolver.loaded_files
    assert resolver._are_all_step_functions_materialized(file_path)  # noqa: SLF001

    descriptor_file = (tmp_path / 'descriptor.py').resolve()
    descriptor_index = _FakeStepIndex(
        descriptors_by_file={
            descriptor_file: (
                _Descriptor(
                    function_name='step_descriptor',
                    step_type='given',
                    patterns=('descriptor',),
                ),
            ),
        },
    )
    descriptor_resolver = LazyStepResolver(descriptor_index)
    descriptor_resolver._mark_file_as_materialized(descriptor_file, None)  # noqa: SLF001
    assert descriptor_resolver._loaded_step_functions_by_file[descriptor_file] == {  # noqa: SLF001
        'step_descriptor',
    }

    query_file = (tmp_path / 'query.py').resolve()
    query_index = _FakeStepIndex(
        descriptors_by_file={
            query_file: (
                _Descriptor(
                    function_name='step_query',
                    step_type='given',
                    patterns=('query',),
                ),
            ),
        },
    )
    query_resolver = LazyStepResolver(query_index)
    assert query_resolver._is_query_terminal_for_file(  # noqa: SLF001
        query_file,
        step_type='given',
        step_text='unknown',
        terminal_files=set(),
    )
    query_resolver._loaded_step_functions_by_file[query_file] = {'step_query'}  # noqa: SLF001
    assert query_resolver._is_query_terminal_for_file(  # noqa: SLF001
        query_file,
        step_type='given',
        step_text='query',
        terminal_files=set(),
    )
    assert not query_resolver._are_query_functions_materialized(  # noqa: SLF001
        query_file,
        None,
    )
