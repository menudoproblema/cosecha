from __future__ import annotations

import traceback

from pathlib import Path
from typing import TYPE_CHECKING

import parse

from cosecha.engine.gherkin.step_catalog import get_conflicting_step_types
from cosecha.engine.gherkin.utils import import_and_load_steps_from_module


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from cosecha.engine.gherkin.step_ast_discovery import StaticStepDescriptor
    from cosecha.engine.gherkin.step_catalog import ProjectStepIndex
    from cosecha.engine.gherkin.steps.registry import StepRegistry
    from cosecha.engine.gherkin.types import StepType

type StepImporter = Callable[
    [Path, StepRegistry, tuple[str, ...] | None],
    None,
]
type StepLoadFailureHandler = Callable[[Path, str], None]
type StepMaterializedHandler = Callable[[Path], None]

QUERY_CACHE_LIMIT = 512


class LazyStepResolver:
    __slots__ = (
        '_candidate_files_by_query',
        '_failed_files',
        '_import_step_file',
        '_loaded_files',
        '_loaded_step_functions_by_file',
        '_negative_query_cache',
        '_on_load_failure',
        '_on_materialized',
        '_step_index',
    )

    def __init__(
        self,
        step_index: ProjectStepIndex,
        *,
        import_step_file: StepImporter = import_and_load_steps_from_module,
        on_load_failure: StepLoadFailureHandler | None = None,
        on_materialized: StepMaterializedHandler | None = None,
    ) -> None:
        self._step_index = step_index
        self._import_step_file = import_step_file
        self._on_load_failure = on_load_failure
        self._on_materialized = on_materialized
        self._loaded_files: set[Path] = set()
        self._failed_files: set[Path] = set()
        self._loaded_step_functions_by_file: dict[Path, set[str]] = {}
        self._negative_query_cache: dict[
            tuple[Path, str],
            set[tuple[StepType, str]],
        ] = {}
        self._candidate_files_by_query: dict[
            tuple[StepType, str],
            tuple[Path, ...],
        ] = {}

    @property
    def loaded_files(self) -> tuple[Path, ...]:
        return tuple(sorted(self._loaded_files))

    @property
    def failed_files(self) -> tuple[Path, ...]:
        return tuple(sorted(self._failed_files))

    def find_candidate_files(
        self,
        step_type: StepType,
        step_text: str,
    ) -> tuple[Path, ...]:
        key = (step_type, step_text)
        cached = self._candidate_files_by_query.get(key)
        if cached is not None:
            return cached

        candidate_files = tuple(
            file_path.resolve()
            for file_path in self._step_index.find_candidate_files(
                step_type,
                step_text,
            )
        )
        self._candidate_files_by_query[key] = candidate_files
        return candidate_files

    def prime_candidate_files(
        self,
        required_step_texts: tuple[tuple[str, str], ...],
        candidate_files: tuple[Path, ...],
    ) -> None:
        normalized_candidate_files = tuple(
            file_path.resolve() for file_path in candidate_files
        )
        for step_type, step_text in required_step_texts:
            key = (step_type, step_text)
            cached = self._candidate_files_by_query.get(key, ())
            self._candidate_files_by_query[key] = tuple(
                sorted({*cached, *normalized_candidate_files}),
            )
        self._trim_query_cache()

    def materialize_for_query(
        self,
        step_type: StepType,
        step_text: str,
        step_registry: StepRegistry,
    ) -> bool:
        candidate_files = self.find_candidate_files(step_type, step_text)
        candidate_loads: list[tuple[Path, tuple[str, ...] | None]] = []
        for file_path in candidate_files:
            if file_path in self._failed_files:
                continue
            if file_path in self._loaded_files:
                continue
            if self._is_negative_cached(
                file_path,
                step_type=step_type,
                step_text=step_text,
            ):
                continue

            requires_fallback_import = (
                self._step_index.requires_fallback_import(file_path)
            )
            relevant_function_names = self._build_relevant_function_names(
                file_path,
                step_type=step_type,
                step_text=step_text,
            )
            if relevant_function_names == ():
                self._record_negative_query(
                    file_path,
                    step_type=step_type,
                    step_text=step_text,
                )
                continue
            if (
                relevant_function_names is None
                and not requires_fallback_import
            ):
                self._record_negative_query(
                    file_path,
                    step_type=step_type,
                    step_text=step_text,
                )
                continue
            if self._are_query_functions_materialized(
                file_path,
                relevant_function_names,
            ):
                continue

            candidate_loads.append((file_path, relevant_function_names))

        files_to_load = tuple(file_path for file_path, _ in candidate_loads)
        if not files_to_load:
            self._prune_terminal_query_cache()
            return False

        with step_registry.bulk_load():
            for file_path, relevant_function_names in candidate_loads:
                try:
                    self._import_step_file(
                        file_path,
                        step_registry,
                        relevant_function_names,
                    )
                except Exception:
                    self._failed_files.add(file_path)
                    formatted_traceback = traceback.format_exc()
                    if self._on_load_failure is None:
                        raise

                    self._on_load_failure(file_path, formatted_traceback)
                    continue

                self._mark_file_as_materialized(
                    file_path,
                    relevant_function_names,
                )
                if self._on_materialized is not None:
                    self._on_materialized(file_path)

        self._prune_terminal_query_cache()
        return True

    def _prune_terminal_query_cache(self) -> None:
        terminal_files = self._loaded_files | self._failed_files
        if not terminal_files:
            return

        removable_keys = tuple(
            key
            for key, candidate_files in self._candidate_files_by_query.items()
            if candidate_files
            and all(
                self._is_query_terminal_for_file(
                    file_path,
                    step_type=key[0],
                    step_text=key[1],
                    terminal_files=terminal_files,
                )
                for file_path in candidate_files
            )
        )
        for key in removable_keys:
            self._candidate_files_by_query.pop(key, None)

        self._trim_query_cache()

    def _trim_query_cache(self) -> None:
        while len(self._candidate_files_by_query) > QUERY_CACHE_LIMIT:
            oldest_key = next(iter(self._candidate_files_by_query))
            self._candidate_files_by_query.pop(oldest_key, None)

    def _is_negative_cached(
        self,
        file_path: Path,
        *,
        step_type: StepType,
        step_text: str,
    ) -> bool:
        fingerprint = self._step_index.file_fingerprint(file_path)
        if fingerprint is None:
            return False
        return (step_type, step_text) in self._negative_query_cache.get(
            (file_path.resolve(), fingerprint),
            set(),
        )

    def _record_negative_query(
        self,
        file_path: Path,
        *,
        step_type: StepType,
        step_text: str,
    ) -> None:
        fingerprint = self._step_index.file_fingerprint(file_path)
        if fingerprint is None:
            return
        self._negative_query_cache.setdefault(
            (file_path.resolve(), fingerprint),
            set(),
        ).add((step_type, step_text))

    def _build_relevant_function_names(
        self,
        file_path: Path,
        *,
        step_type: StepType,
        step_text: str,
    ) -> tuple[str, ...] | None:
        requires_fallback_import = self._step_index.requires_fallback_import(
            file_path,
        )
        descriptors = tuple(self._step_index.descriptors_for_file(file_path))
        if not descriptors:
            return None if requires_fallback_import else ()

        relevant_function_names: list[str] = []
        seen_function_names: set[str] = set()
        requires_runtime_matching = False
        for descriptor in descriptors:
            if descriptor.step_type not in get_conflicting_step_types(
                step_type,
            ):
                continue
            if not self._descriptor_supports_text_matching(descriptor):
                if self._descriptor_may_match_by_structure(
                    descriptor,
                    step_text=step_text,
                ):
                    requires_runtime_matching = True
                continue
            if not self._descriptor_matches_step_text(
                descriptor,
                step_type=step_type,
                step_text=step_text,
            ):
                continue

            if descriptor.function_name in seen_function_names:
                continue

            seen_function_names.add(descriptor.function_name)
            relevant_function_names.append(descriptor.function_name)

        if relevant_function_names:
            return tuple(relevant_function_names)

        if requires_fallback_import or requires_runtime_matching:
            return None

        return ()

    def _descriptor_supports_text_matching(
        self,
        descriptor: StaticStepDescriptor,
    ) -> bool:
        return descriptor.parser_cls_name in (None, 'ParseStepMatcher')

    def _descriptor_matches_step_text(
        self,
        descriptor: StaticStepDescriptor,
        *,
        step_type: StepType,
        step_text: str,
    ) -> bool:
        if descriptor.step_type not in get_conflicting_step_types(step_type):
            return False
        if not self._descriptor_may_match_by_structure(
            descriptor,
            step_text=step_text,
        ):
            return False

        return any(
            parse.compile(pattern).parse(step_text) is not None
            for pattern in descriptor.patterns
        )

    def _descriptor_may_match_by_structure(
        self,
        descriptor: StaticStepDescriptor,
        *,
        step_text: str,
    ) -> bool:
        normalized_step_text = step_text.lower()
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

        return not descriptor.literal_fragments or all(
            fragment in normalized_step_text
            for fragment in descriptor.literal_fragments
        )

    def _mark_file_as_materialized(
        self,
        file_path: Path,
        function_names: tuple[str, ...] | None,
    ) -> None:
        resolved_file_path = file_path.resolve()
        if function_names is None:
            self._loaded_files.add(resolved_file_path)
            descriptor_function_names = {
                descriptor.function_name
                for descriptor in self._step_index.descriptors_for_file(
                    resolved_file_path,
                )
            }
            if descriptor_function_names:
                self._loaded_step_functions_by_file[resolved_file_path] = (
                    descriptor_function_names
                )
            return

        loaded_function_names = self._loaded_step_functions_by_file.setdefault(
            resolved_file_path,
            set(),
        )
        loaded_function_names.update(function_names)
        if self._are_all_step_functions_materialized(resolved_file_path):
            self._loaded_files.add(resolved_file_path)

    def _are_all_step_functions_materialized(
        self,
        file_path: Path,
    ) -> bool:
        descriptors = self._step_index.descriptors_for_file(file_path)
        if not descriptors:
            return file_path in self._loaded_files

        expected_function_names = {
            descriptor.function_name for descriptor in descriptors
        }
        return expected_function_names.issubset(
            self._loaded_step_functions_by_file.get(file_path, set()),
        )

    def _is_query_terminal_for_file(
        self,
        file_path: Path,
        *,
        step_type: StepType,
        step_text: str,
        terminal_files: set[Path],
    ) -> bool:
        resolved_file_path = file_path.resolve()
        if resolved_file_path in terminal_files:
            return True

        relevant_function_names = self._build_relevant_function_names(
            resolved_file_path,
            step_type=step_type,
            step_text=step_text,
        )
        if relevant_function_names == ():
            return True
        if relevant_function_names is None:
            return False

        return self._are_query_functions_materialized(
            resolved_file_path,
            relevant_function_names,
        )

    def _are_query_functions_materialized(
        self,
        file_path: Path,
        relevant_function_names: tuple[str, ...] | None,
    ) -> bool:
        if relevant_function_names == ():
            return True
        if relevant_function_names is None:
            return file_path in self._loaded_files

        return set(relevant_function_names).issubset(
            self._loaded_step_functions_by_file.get(file_path, set()),
        )
