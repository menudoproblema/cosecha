from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from cosecha.core.cache import DiskCache
from cosecha.core.knowledge_base import (
    DefinitionKnowledgeQuery,
    ReadOnlyPersistentKnowledgeBase,
    resolve_knowledge_base_path,
)
from cosecha.engine.gherkin.step_ast_discovery import (
    StaticDiscoveredStepFile,
    StaticStepDescriptor,
)


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.engine.gherkin.types import StepType


GHERKIN_STEP_INDEX_SCHEMA_VERSION = 4
_MIN_QUERY_TOKEN_LENGTH = 3


def get_conflicting_step_types(
    step_type: StepType,
) -> tuple[StepType, ...]:
    if step_type == 'step':
        return ('step', 'given', 'when', 'then', 'but')

    return (step_type, 'step')


@dataclass(slots=True, frozen=True)
class StepQuery:
    step_type: StepType
    step_text: str


class ProjectStepIndex(Protocol):
    def descriptors_for_file(
        self,
        file_path: Path,
    ) -> tuple[StaticStepDescriptor, ...]: ...

    def find_candidate_files(
        self,
        step_type: StepType,
        step_text: str,
    ) -> tuple[Path, ...]: ...

    def find_candidate_files_for_steps(
        self,
        step_queries: tuple[StepQuery, ...],
    ) -> tuple[Path, ...]: ...

    def requires_fallback_import(
        self,
        file_path: Path,
    ) -> bool: ...

    def file_fingerprint(
        self,
        file_path: Path,
    ) -> str | None: ...


class KnowledgeStore(Protocol):
    def get_project_step_index(self) -> ProjectStepIndex: ...

    def get_discovered_step_files(
        self,
    ) -> tuple[StaticDiscoveredStepFile, ...]: ...

    def set_project_step_index(
        self,
        index: ProjectStepIndex,
    ) -> None: ...

    def set_discovered_step_files(
        self,
        discovered_files: tuple[StaticDiscoveredStepFile, ...],
    ) -> None: ...


class InMemoryKnowledgeStore:
    __slots__ = ('_discovered_step_files', '_project_step_index')

    def __init__(self) -> None:
        self._project_step_index: ProjectStepIndex = StepCatalog()
        self._discovered_step_files: tuple[StaticDiscoveredStepFile, ...] = ()

    def get_project_step_index(self) -> ProjectStepIndex:
        return self._project_step_index

    def get_discovered_step_files(
        self,
    ) -> tuple[StaticDiscoveredStepFile, ...]:
        return self._discovered_step_files

    def set_project_step_index(
        self,
        index: ProjectStepIndex,
    ) -> None:
        self._project_step_index = index

    def set_discovered_step_files(
        self,
        discovered_files: tuple[StaticDiscoveredStepFile, ...],
    ) -> None:
        self._discovered_step_files = discovered_files


class PersistentKnowledgeStore:
    __slots__ = (
        '_definition_paths',
        '_discovered_step_files',
        '_disk_cache',
        '_engine_name',
        '_project_step_index',
        '_root_path',
    )

    def __init__(
        self,
        root_path: Path,
        *,
        engine_name: str = 'gherkin',
        definition_paths: tuple[Path, ...] = (),
    ) -> None:
        self._root_path = root_path.resolve()
        self._engine_name = engine_name
        self._definition_paths = tuple(
            path.resolve() for path in definition_paths
        )
        self._disk_cache = DiskCache(self._root_path, 'gherkin_step_index')
        self._discovered_step_files = self._load_discovered_step_files()
        catalog = StepCatalog()
        catalog.update(self._discovered_step_files)
        self._project_step_index: ProjectStepIndex = catalog

    def get_project_step_index(self) -> ProjectStepIndex:
        return self._project_step_index

    def get_discovered_step_files(
        self,
    ) -> tuple[StaticDiscoveredStepFile, ...]:
        return self._discovered_step_files

    def set_project_step_index(
        self,
        index: ProjectStepIndex,
    ) -> None:
        self._project_step_index = index

    def set_discovered_step_files(
        self,
        discovered_files: tuple[StaticDiscoveredStepFile, ...],
    ) -> None:
        self._discovered_step_files = discovered_files
        self._disk_cache.save(
            {
                'discovered_step_files': discovered_files,
                'schema_version': GHERKIN_STEP_INDEX_SCHEMA_VERSION,
            },
        )

    def _load_discovered_step_files(
        self,
    ) -> tuple[StaticDiscoveredStepFile, ...]:
        cached_data = self._disk_cache.load()
        if not cached_data:
            return self._load_discovered_step_files_from_knowledge_base()
        if (
            cached_data.get('schema_version')
            != GHERKIN_STEP_INDEX_SCHEMA_VERSION
        ):
            self._disk_cache.clear()
            return self._load_discovered_step_files_from_knowledge_base()

        raw_discovered_files = cached_data.get('discovered_step_files')
        if not isinstance(raw_discovered_files, tuple):
            self._disk_cache.clear()
            return self._load_discovered_step_files_from_knowledge_base()

        discovered_files: list[StaticDiscoveredStepFile] = []
        for discovered_file in raw_discovered_files:
            if not isinstance(discovered_file, StaticDiscoveredStepFile):
                continue

            discovered_files.append(discovered_file)

        return tuple(
            sorted(
                discovered_files,
                key=lambda discovered_file: str(discovered_file.file_path),
            ),
        )

    def _load_discovered_step_files_from_knowledge_base(
        self,
    ) -> tuple[StaticDiscoveredStepFile, ...]:
        db_path = resolve_knowledge_base_path(self._root_path)
        if not db_path.exists():
            return ()

        knowledge_base = ReadOnlyPersistentKnowledgeBase(db_path)
        try:
            definitions = knowledge_base.query_definitions(
                DefinitionKnowledgeQuery(
                    engine_name=self._engine_name,
                    include_invalidated=False,
                ),
            )
        finally:
            knowledge_base.close()

        discovered_files: list[StaticDiscoveredStepFile] = []
        for definition in definitions:
            file_path = Path(definition.file_path).resolve()
            if not _is_allowed_definition_path(
                file_path,
                root_path=self._root_path,
                definition_paths=self._definition_paths,
            ):
                continue

            descriptors = tuple(
                _build_static_step_descriptor_from_knowledge(
                    file_path=file_path,
                    root_path=self._root_path,
                    descriptor=descriptor,
                    discovery_mode=definition.discovery_mode,
                )
                for descriptor in definition.descriptors
            )
            discovered_files.append(
                StaticDiscoveredStepFile(
                    file_path=file_path,
                    module_import_path=_build_module_import_path(
                        self._root_path,
                        file_path,
                    ),
                    descriptors=descriptors,
                    discovery_mode=_normalize_discovery_mode(
                        definition.discovery_mode,
                    ),
                    requires_fallback_import=(
                        definition.discovery_mode != 'ast'
                    ),
                    content_digest=definition.content_hash or '',
                    mtime_ns=0,
                    file_size=0,
                ),
            )

        return tuple(
            sorted(
                discovered_files,
                key=lambda discovered_file: str(discovered_file.file_path),
            ),
        )


class StepCatalog:
    __slots__ = (
        '_candidate_files_by_query',
        '_descriptors_by_file',
        '_dynamic_files_by_type',
        '_fallback_files',
        '_file_fingerprints',
        '_files_by_anchor_token',
        '_files_by_literal_fragment',
        '_files_by_prefix',
        '_files_by_suffix',
        '_fragment_lengths_by_type',
        '_prefix_lengths_by_type',
        '_suffix_lengths_by_type',
    )

    def __init__(self) -> None:
        self._candidate_files_by_query: dict[
            tuple[StepType, str],
            tuple[Path, ...],
        ] = {}
        self._descriptors_by_file: dict[
            Path,
            tuple[StaticStepDescriptor, ...],
        ] = {}
        self._file_fingerprints: dict[Path, str] = {}
        self._files_by_prefix: dict[
            StepType,
            dict[str, set[Path]],
        ] = {
            'given': defaultdict(set),
            'when': defaultdict(set),
            'then': defaultdict(set),
            'but': defaultdict(set),
            'step': defaultdict(set),
        }
        self._files_by_anchor_token: dict[
            StepType,
            dict[str, set[Path]],
        ] = {
            'given': defaultdict(set),
            'when': defaultdict(set),
            'then': defaultdict(set),
            'but': defaultdict(set),
            'step': defaultdict(set),
        }
        self._files_by_literal_fragment: dict[
            StepType,
            dict[str, set[Path]],
        ] = {
            'given': defaultdict(set),
            'when': defaultdict(set),
            'then': defaultdict(set),
            'but': defaultdict(set),
            'step': defaultdict(set),
        }
        self._files_by_suffix: dict[
            StepType,
            dict[str, set[Path]],
        ] = {
            'given': defaultdict(set),
            'when': defaultdict(set),
            'then': defaultdict(set),
            'but': defaultdict(set),
            'step': defaultdict(set),
        }
        self._dynamic_files_by_type: dict[StepType, set[Path]] = {
            'given': set(),
            'when': set(),
            'then': set(),
            'but': set(),
            'step': set(),
        }
        self._prefix_lengths_by_type: dict[StepType, set[int]] = {
            'given': set(),
            'when': set(),
            'then': set(),
            'but': set(),
            'step': set(),
        }
        self._fragment_lengths_by_type: dict[StepType, set[int]] = {
            'given': set(),
            'when': set(),
            'then': set(),
            'but': set(),
            'step': set(),
        }
        self._suffix_lengths_by_type: dict[StepType, set[int]] = {
            'given': set(),
            'when': set(),
            'then': set(),
            'but': set(),
            'step': set(),
        }
        self._fallback_files: set[Path] = set()

    @property
    def discovered_files(self) -> tuple[Path, ...]:
        return tuple(sorted(self._descriptors_by_file))

    @property
    def fallback_files(self) -> tuple[Path, ...]:
        return tuple(sorted(self._fallback_files))

    @property
    def ast_resolved_file_count(self) -> int:
        return len(self._descriptors_by_file) - len(self._fallback_files)

    @property
    def fallback_file_count(self) -> int:
        return len(self._fallback_files)

    def descriptors_for_file(
        self,
        file_path: Path,
    ) -> tuple[StaticStepDescriptor, ...]:
        return self._descriptors_by_file.get(file_path.resolve(), ())

    def requires_fallback_import(
        self,
        file_path: Path,
    ) -> bool:
        return file_path.resolve() in self._fallback_files

    def file_fingerprint(
        self,
        file_path: Path,
    ) -> str | None:
        return self._file_fingerprints.get(file_path.resolve())

    def update(
        self,
        discovered_files: tuple[StaticDiscoveredStepFile, ...],
    ) -> None:
        self.clear()
        self.extend(discovered_files)

    def extend(
        self,
        discovered_files: tuple[StaticDiscoveredStepFile, ...],
    ) -> None:
        for discovered_file in discovered_files:
            self._register_discovered_file(discovered_file)

    def clear(self) -> None:
        self._candidate_files_by_query.clear()
        self._descriptors_by_file.clear()
        self._file_fingerprints.clear()
        for step_type in self._files_by_prefix:
            self._files_by_prefix[step_type].clear()
            self._files_by_anchor_token[step_type].clear()
            self._files_by_literal_fragment[step_type].clear()
            self._files_by_suffix[step_type].clear()
            self._dynamic_files_by_type[step_type].clear()
            self._prefix_lengths_by_type[step_type].clear()
            self._fragment_lengths_by_type[step_type].clear()
            self._suffix_lengths_by_type[step_type].clear()
        self._fallback_files.clear()

    def find_candidate_files(
        self,
        step_type: StepType,
        step_text: str,
    ) -> tuple[Path, ...]:
        key = (step_type, step_text)
        cached = self._candidate_files_by_query.get(key)
        if cached is not None:
            return cached

        candidate_files: set[Path] = set(self._fallback_files)
        for current_step_type in get_conflicting_step_types(step_type):
            candidate_files.update(
                self._find_candidate_files_in_type(
                    current_step_type,
                    step_text,
                ),
            )

        matches = tuple(sorted(candidate_files))
        self._candidate_files_by_query[key] = matches
        return matches

    def find_candidate_files_for_steps(
        self,
        step_queries: tuple[StepQuery, ...],
    ) -> tuple[Path, ...]:
        candidate_files: set[Path] = set()
        for step_query in step_queries:
            candidate_files.update(
                self.find_candidate_files(
                    step_query.step_type,
                    step_query.step_text,
                ),
            )

        return tuple(sorted(candidate_files))

    def _find_candidate_files_in_type(
        self,
        step_type: StepType,
        step_text: str,
    ) -> tuple[Path, ...]:
        candidate_files: set[Path] = set()
        files_by_prefix = self._files_by_prefix[step_type]
        files_by_suffix = self._files_by_suffix[step_type]
        normalized_step_text = step_text.lower()
        anchor_match_scores = _collect_match_scores(
            keys=_tokenize_step_text(step_text),
            files_by_key=self._files_by_anchor_token[step_type],
        )
        fragment_match_scores = self._collect_literal_fragment_match_scores(
            step_type=step_type,
            normalized_step_text=normalized_step_text,
        )
        candidate_files.update(
            self._collect_edge_candidates(
                step_text=step_text,
                files_by_key=files_by_prefix,
                lengths=self._prefix_lengths_by_type[step_type],
                from_end=False,
            ),
        )
        candidate_files.update(
            self._collect_edge_candidates(
                step_text=step_text,
                files_by_key=files_by_suffix,
                lengths=self._suffix_lengths_by_type[step_type],
                from_end=True,
            ),
        )
        if anchor_match_scores:
            strongest_anchor_match_count = max(anchor_match_scores.values())
            candidate_files.update(
                file_path
                for file_path, match_count in anchor_match_scores.items()
                if match_count == strongest_anchor_match_count
            )
        if fragment_match_scores:
            strongest_fragment_match_count = max(
                fragment_match_scores.values(),
            )
            candidate_files.update(
                file_path
                for file_path, match_count in fragment_match_scores.items()
                if match_count == strongest_fragment_match_count
            )
        if not candidate_files:
            candidate_files.update(self._dynamic_files_by_type[step_type])

        structurally_matching_files = tuple(
            file_path
            for file_path in candidate_files
            if self._file_may_match_query(
                file_path,
                step_type=step_type,
                step_text=step_text,
            )
        )
        if structurally_matching_files:
            return tuple(sorted(structurally_matching_files))

        return tuple(sorted(candidate_files))

    def _collect_edge_candidates(
        self,
        *,
        step_text: str,
        files_by_key: dict[str, set[Path]],
        lengths: set[int],
        from_end: bool,
    ) -> set[Path]:
        candidate_files: set[Path] = set()
        for length in sorted(lengths):
            if length > len(step_text):
                break
            segment = step_text[-length:] if from_end else step_text[:length]
            candidate_files.update(files_by_key.get(segment, ()))
        return candidate_files

    def _collect_literal_fragment_match_scores(
        self,
        *,
        step_type: StepType,
        normalized_step_text: str,
    ) -> dict[Path, int]:
        files_by_literal_fragment = self._files_by_literal_fragment[step_type]
        fragment_match_scores: dict[Path, int] = {}
        for fragment_length in sorted(
            self._fragment_lengths_by_type[step_type],
            reverse=True,
        ):
            if fragment_length > len(normalized_step_text):
                continue
            matched_any_fragment = False
            for (
                literal_fragment,
                file_paths,
            ) in files_by_literal_fragment.items():
                if len(literal_fragment) != fragment_length:
                    continue
                if literal_fragment not in normalized_step_text:
                    continue
                matched_any_fragment = True
                for file_path in file_paths:
                    fragment_match_scores[file_path] = (
                        fragment_match_scores.get(file_path, 0) + 1
                    )
            if matched_any_fragment:
                break
        return fragment_match_scores

    def _file_may_match_query(
        self,
        file_path: Path,
        *,
        step_type: StepType,
        step_text: str,
    ) -> bool:
        normalized_step_tokens = set(_tokenize_step_text(step_text))
        for descriptor in self._descriptors_by_file.get(
            file_path.resolve(),
            (),
        ):
            if descriptor.step_type not in get_conflicting_step_types(
                step_type,
            ):
                continue
            if _descriptor_may_match_query(
                descriptor,
                step_text=step_text,
                normalized_step_tokens=normalized_step_tokens,
            ):
                return True

        return False

    def _register_discovered_file(
        self,
        discovered_file: StaticDiscoveredStepFile,
    ) -> None:
        self._candidate_files_by_query.clear()
        file_path = discovered_file.file_path.resolve()
        self._descriptors_by_file[file_path] = discovered_file.descriptors
        self._file_fingerprints[file_path] = discovered_file.content_digest
        if discovered_file.requires_fallback_import:
            self._fallback_files.add(file_path)

        for descriptor in discovered_file.descriptors:
            has_dynamic_prefix = False
            for literal_prefix in descriptor.literal_prefixes:
                if not literal_prefix:
                    has_dynamic_prefix = True
                    continue

                self._files_by_prefix[descriptor.step_type][
                    literal_prefix
                ].add(file_path)
                self._prefix_lengths_by_type[descriptor.step_type].add(
                    len(literal_prefix),
                )
            for literal_suffix in descriptor.literal_suffixes:
                if not literal_suffix:
                    continue
                self._files_by_suffix[descriptor.step_type][
                    literal_suffix
                ].add(file_path)
                self._suffix_lengths_by_type[descriptor.step_type].add(
                    len(literal_suffix),
                )
            for literal_fragment in descriptor.literal_fragments:
                if not literal_fragment:
                    continue
                self._files_by_literal_fragment[descriptor.step_type][
                    literal_fragment
                ].add(file_path)
                self._fragment_lengths_by_type[descriptor.step_type].add(
                    len(literal_fragment),
                )
            if has_dynamic_prefix:
                for token in descriptor.anchor_tokens:
                    self._files_by_anchor_token[descriptor.step_type][
                        token
                    ].add(file_path)
                self._dynamic_files_by_type[descriptor.step_type].add(
                    file_path,
                )


def _build_module_import_path(
    root_path: Path,
    file_path: Path,
) -> str | None:
    try:
        relative_parts = file_path.relative_to(root_path).with_suffix('').parts
    except Exception:
        return None

    if not relative_parts:
        return None

    return '.'.join(relative_parts)


def _collect_match_scores(
    *,
    keys: tuple[str, ...],
    files_by_key: dict[str, set[Path]],
) -> dict[Path, int]:
    match_scores: dict[Path, int] = {}
    for key in keys:
        for file_path in files_by_key.get(key, ()):
            match_scores[file_path] = match_scores.get(file_path, 0) + 1
    return match_scores


def _descriptor_may_match_query(
    descriptor: StaticStepDescriptor,
    *,
    step_text: str,
    normalized_step_tokens: set[str],
) -> bool:
    non_empty_prefixes = tuple(
        prefix for prefix in descriptor.literal_prefixes if prefix
    )
    if non_empty_prefixes and not any(
        step_text.startswith(prefix) for prefix in non_empty_prefixes
    ):
        return False

    non_empty_suffixes = tuple(
        suffix for suffix in descriptor.literal_suffixes if suffix
    )
    if non_empty_suffixes and not any(
        step_text.endswith(suffix) for suffix in non_empty_suffixes
    ):
        return False

    if descriptor.literal_fragments and not all(
        fragment in step_text.lower()
        for fragment in descriptor.literal_fragments
    ):
        return False

    return not descriptor.anchor_tokens or all(
        token in normalized_step_tokens for token in descriptor.anchor_tokens
    )


def _build_static_step_descriptor_from_knowledge(
    *,
    file_path: Path,
    root_path: Path,
    descriptor,
    discovery_mode: str,
) -> StaticStepDescriptor:
    normalized_discovery_mode = _normalize_discovery_mode(discovery_mode)
    return StaticStepDescriptor(
        step_type=descriptor.step_type,  # type: ignore[arg-type]
        patterns=descriptor.patterns,
        source_file=file_path,
        source_line=descriptor.source_line,
        function_name=descriptor.function_name,
        file_path=file_path,
        module_import_path=_build_module_import_path(root_path, file_path),
        literal_prefixes=descriptor.literal_prefixes,
        literal_suffixes=descriptor.literal_suffixes,
        literal_fragments=descriptor.literal_fragments,
        anchor_tokens=descriptor.anchor_tokens,
        dynamic_fragment_count=descriptor.dynamic_fragment_count,
        documentation=descriptor.documentation,
        parser_cls_name=descriptor.parser_cls_name,
        category=descriptor.category,
        discovery_mode=normalized_discovery_mode,
    )


def _normalize_discovery_mode(discovery_mode: str) -> str:
    return 'fallback_import' if discovery_mode == 'fallback_import' else 'ast'


def _is_within_root(file_path: Path, root_path: Path) -> bool:
    try:
        file_path.relative_to(root_path)
    except ValueError:
        return False

    return True


def _is_allowed_definition_path(
    file_path: Path,
    *,
    root_path: Path,
    definition_paths: tuple[Path, ...],
) -> bool:
    if _is_within_root(file_path, root_path):
        return True

    for definition_path in definition_paths:
        resolved_definition_path = definition_path.resolve()
        if resolved_definition_path.is_file():
            if file_path == resolved_definition_path:
                return True
            continue
        if _is_within_root(file_path, resolved_definition_path):
            return True

    return False


def _tokenize_step_text(step_text: str) -> tuple[str, ...]:
    normalized_tokens = [
        token
        for token in ''.join(
            character.lower() if character.isalnum() else ' '
            for character in step_text
        ).split()
        if len(token) >= _MIN_QUERY_TOKEN_LENGTH
    ]
    return tuple(dict.fromkeys(normalized_tokens))
