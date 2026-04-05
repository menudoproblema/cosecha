from __future__ import annotations

import asyncio
import hashlib
import threading
import time
import traceback

from collections import OrderedDict
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from cosecha.core.cache import DiskCache
from cosecha.core.collector import Collector
from cosecha.core.domain_events import (
    KnowledgeIndexedEvent,
    KnowledgeInvalidatedEvent,
    TestKnowledgeIndexedEvent,
    TestKnowledgeInvalidatedEvent,
)
from cosecha.core.exceptions import CosechaParserError
from cosecha.core.execution_ir import (
    build_execution_node_stable_id,
    build_test_path_label,
)
from cosecha.core.knowledge_base import (
    ReadOnlyPersistentKnowledgeBase,
    TestKnowledgeQuery,
    resolve_knowledge_base_path,
)
from cosecha.core.knowledge_test_descriptor import TestDescriptorKnowledge
from cosecha.core.utils import is_subpath
from cosecha.engine.gherkin.definition_knowledge import (
    build_gherkin_definition_record,
)
from cosecha.engine.gherkin.items import GherkinTestItem
from cosecha.engine.gherkin.step_ast_discovery import (
    StaticDiscoveredStepFile,
    StepDiscoveryService,
)
from cosecha.engine.gherkin.step_catalog import (
    GHERKIN_STEP_INDEX_SCHEMA_VERSION,
    PersistentKnowledgeStore,
    ProjectStepIndex,
    StepCatalog,
)
from cosecha.engine.gherkin.utils import (
    create_scenario_with_example,
    generate_model_from_gherkin,
    import_and_load_steps_from_module,
)
from gherkin.parser import Parser


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.items import TestItem
    from cosecha.engine.gherkin.models import Feature
    from cosecha.engine.gherkin.step_ast_discovery import (
        StaticStepDescriptor,
    )
    from cosecha.engine.gherkin.steps.registry import StepRegistry


# Cache: (absolute path, mtime_ns, file size) -> Feature
# Evita re-parsear y re-construir modelos Feature cuando el fichero no
# ha cambiado entre ejecuciones del mismo proceso.
_feature_cache: OrderedDict[tuple[Path, int, int], Feature] = OrderedDict()
_FEATURE_CACHE_LIMIT = 500
_SMALL_SELECTIVE_FEATURE_COLLECT_LIMIT = 16
_PARSER_LOCAL = threading.local()
GHERKIN_TEST_INDEX_SCHEMA_VERSION = 1


def _get_thread_local_parser() -> Parser:
    parser = getattr(_PARSER_LOCAL, 'parser', None)
    if parser is None:
        parser = Parser()
        _PARSER_LOCAL.parser = parser

    return parser


def _get_cached_feature(
    cache_key: tuple[Path, int, int],
) -> Feature | None:
    feature = _feature_cache.get(cache_key)
    if feature is None:
        return None

    _feature_cache.move_to_end(cache_key)
    return feature


def _store_cached_feature(
    cache_key: tuple[Path, int, int],
    feature: Feature,
) -> None:
    _feature_cache[cache_key] = feature
    _feature_cache.move_to_end(cache_key)

    while len(_feature_cache) > _FEATURE_CACHE_LIMIT:
        _feature_cache.popitem(last=False)


def _restore_cached_features(
    cached_features: dict[tuple[Path, int, int], Feature],
) -> None:
    _feature_cache.clear()
    if not cached_features:
        return

    for cache_key, feature in list(cached_features.items())[
        -_FEATURE_CACHE_LIMIT:
    ]:
        _feature_cache[cache_key] = feature


def _find_step_impl_files(step_directories: tuple[Path, ...]) -> list[Path]:
    seen_paths: set[Path] = set()
    step_files: list[Path] = []

    for directory in step_directories:
        for file_path in sorted(directory.rglob('*.py')):
            resolved_path = file_path.resolve()
            if resolved_path in seen_paths:
                continue

            seen_paths.add(resolved_path)
            step_files.append(file_path)

    return step_files


def _snapshot_step_files(
    step_files: tuple[Path, ...],
) -> tuple[tuple[Path, int, int], ...]:
    snapshots: list[tuple[Path, int, int]] = []
    for file_path in step_files:
        with suppress(OSError):
            stat = file_path.resolve().stat()
            snapshots.append(
                (
                    file_path.resolve(),
                    stat.st_mtime_ns,
                    stat.st_size,
                ),
            )

    return tuple(snapshots)


def _fingerprint_step_file(file_path: Path) -> str:
    return hashlib.sha256(
        file_path.read_bytes(),
    ).hexdigest()


def _normalize_configured_step_directories(
    definition_paths: tuple[Path, ...],
) -> tuple[Path, ...]:
    normalized_directories: dict[Path, None] = {}
    for definition_path in definition_paths:
        resolved_path = definition_path.resolve()
        if resolved_path.is_dir():
            normalized_directories.setdefault(resolved_path, None)
            continue

        if resolved_path.suffix == '.py':
            normalized_directories.setdefault(resolved_path.parent, None)

    return tuple(sorted(normalized_directories, key=str))


def _discover_step_directories(
    test_path: Path,
    root_path: Path,
    scanned_directories: frozenset[Path],
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    candidate_path = (
        test_path if test_path.is_absolute() else root_path / test_path
    )
    directory = (
        candidate_path if candidate_path.is_dir() else candidate_path.parent
    ).resolve()
    discovered: list[Path] = []
    traversed: list[Path] = []

    while True:
        if directory in scanned_directories:
            break

        traversed.append(directory)
        steps_directory = directory / 'steps'
        if steps_directory.is_dir():
            discovered.append(steps_directory)

        if directory == root_path:
            break

        if directory.parent == directory:
            break

        directory = directory.parent

    return (tuple(discovered), tuple(traversed))


def _load_step_impl_files(
    step_files: tuple[Path, ...],
    step_registry: StepRegistry,
) -> list[tuple[Path, str]]:
    failures: list[tuple[Path, str]] = []

    with step_registry.bulk_load():
        for file_path in step_files:
            try:
                import_and_load_steps_from_module(file_path, step_registry)
            except Exception:
                failures.append((file_path, traceback.format_exc()))

    return failures


def _parse_and_generate_model(
    test_path: Path,
    root_path_parent: Path,
) -> Feature | None:
    # Agrupamos lectura, parsing y generacion del modelo en un solo hilo
    # para evitar saltos innecesarios al event loop y minimizar el bloqueo.
    try:
        data = cast(
            'dict[str, Any]',
            _get_thread_local_parser().parse(test_path.read_text()),
        )
        feature_path = test_path.relative_to(root_path_parent)
        return generate_model_from_gherkin(data, feature_path)
    except Exception:
        # Los errores se gestionan en el collector para no romper el gather.
        raise


def _parse_feature_content(
    feature_content: str,
    feature_path: Path,
    root_path_parent: Path,
) -> Feature:
    data = cast(
        'dict[str, Any]',
        _get_thread_local_parser().parse(feature_content),
    )
    return generate_model_from_gherkin(
        data,
        feature_path.relative_to(root_path_parent),
    )


def _should_use_disk_feature_cache(
    collect_paths: tuple[Path, ...],
) -> bool:
    if not collect_paths:
        return True

    requested_feature_count = 0
    for path in collect_paths:
        if path.is_file():
            if path.suffix == '.feature':
                requested_feature_count += 1
        elif path.is_dir():
            for _feature_path in path.rglob('*.feature'):
                requested_feature_count += 1
                if (
                    requested_feature_count
                    > _SMALL_SELECTIVE_FEATURE_COLLECT_LIMIT
                ):
                    return True

    if 0 < requested_feature_count <= _SMALL_SELECTIVE_FEATURE_COLLECT_LIMIT:
        return False

    return not all(path.is_file() for path in collect_paths)


class GherkinCollector(Collector):
    __slots__ = (
        '_configured_step_directories',
        '_disk_cache',
        'knowledge_store',
        'scanned_directories',
        'skip_step_catalog_discovery',
        'step_catalog',
        'steps_directories',
    )

    def __init__(self):
        super().__init__('feature')
        # Guardamos los directorios de steps sin duplicados para no cargar
        # varias veces la misma libreria al recorrer varios tests.
        self.steps_directories: set[Path] = set()
        # Recordamos los directorios ya inspeccionados para no volver a subir
        # el mismo arbol de carpetas una y otra vez.
        self.scanned_directories: set[Path] = set()
        self._disk_cache: DiskCache | None = None
        self._configured_step_directories: tuple[Path, ...] = ()
        self.skip_step_catalog_discovery = False
        self.step_catalog = StepCatalog()

    def initialize(
        self,
        config,
        base_path: str | Path | None = None,
    ) -> None:
        super().initialize(config, base_path)
        self.knowledge_store = PersistentKnowledgeStore(
            self.config.root_path,
            engine_name=self._engine_name,
            definition_paths=tuple(self.config.definition_paths),
        )
        self._configured_step_directories = tuple(
            _normalize_configured_step_directories(
                self.config.definition_paths,
            ),
        )

    async def collect(
        self,
        path: Path | tuple[Path, ...] | None,
        excluded_paths: tuple[Path, ...] = (),
    ):
        collect_paths = tuple(
            candidate_path
            for candidate_path in self._normalize_collect_paths(path)
            if is_subpath(self.base_path, candidate_path)
        )
        normalized_excluded_paths = self._normalize_collect_paths(
            tuple(excluded_paths),
        )
        use_disk_feature_cache = _should_use_disk_feature_cache(collect_paths)
        # Inicializamos y cargamos la cache de disco al empezar la recoleccion.
        _t = time.perf_counter()
        if use_disk_feature_cache and self._disk_cache is None:
            self._disk_cache = DiskCache(
                self.config.root_path,
                'gherkin_features',
            )
            # Sincronizamos la cache de memoria con la de disco.
            _restore_cached_features(self._disk_cache.load())
        if use_disk_feature_cache:
            self._record_phase('disk_cache_load', time.perf_counter() - _t)

        # Reiniciamos la cache de discovery en cada nueva recoleccion para no
        # arrastrar rutas de `steps/` de sesiones anteriores.
        self.steps_directories = set(self._configured_step_directories)
        self.scanned_directories = set()
        await super().collect(path, excluded_paths)

        # Tras recolectar todos los ficheros, aseguramos que hemos
        # descubierto todos los directorios de steps basandonos en los
        # archivos encontrados.
        _t = time.perf_counter()
        parent_directories = {p.parent for p in self.collected_files}
        if parent_directories:
            await asyncio.gather(
                *(
                    self.find_step_impl_directories(p)
                    for p in parent_directories
                ),
            )
        self._record_phase(
            'find_step_impl_directories',
            time.perf_counter() - _t,
        )

        if self.skip_step_catalog_discovery:
            self.step_catalog.clear()
            self.knowledge_store.set_project_step_index(self.step_catalog)
        else:
            _t = time.perf_counter()
            await self.build_step_catalog()
            self._record_phase(
                'step_catalog_discovery',
                time.perf_counter() - _t,
            )
        await self._emit_test_knowledge_events(
            collect_paths=collect_paths,
            excluded_paths=normalized_excluded_paths,
        )

        # Persistimos la cache en disco tras una recoleccion exitosa.
        if use_disk_feature_cache and self._disk_cache:
            _t = time.perf_counter()
            await asyncio.to_thread(
                self._disk_cache.save,
                _feature_cache,
            )
            self._record_phase('disk_cache_save', time.perf_counter() - _t)

    async def find_test_files(self, base_path: Path) -> list[Path]:
        path_list = await super().find_test_files(base_path)

        if path_list:
            # Buscamos steps en la base una sola vez.
            await self.find_step_impl_directories(base_path)

        return path_list

    async def _load_feature(
        self,
        test_path: Path,
    ) -> Feature | None:
        # Intentamos reutilizar el modelo Feature si el fichero no ha cambiado
        # desde la ultima vez que fue procesado en este proceso.
        cache_key: tuple[Path, int, int] | None = None
        feature: Feature | None = None
        with suppress(OSError):
            stat = await asyncio.to_thread(test_path.stat)
            cache_key = (test_path, stat.st_mtime_ns, stat.st_size)
        feature = _get_cached_feature(cache_key)

        if feature is not None:
            return feature

        try:
            # Offloadeamos todo el procesado pesado (I/O + Parsing + Modeling)
            # al thread pool. Esto evita que la CPU de
            # generate_model_from_gherkin bloquee el event loop.
            feature = await asyncio.to_thread(
                _parse_and_generate_model,
                test_path,
                self.config.root_path.parent,
            )
        except CosechaParserError as e:
            self.config.diagnostics.error(
                f'Invalid format in test file: {test_path}',
                details=(
                    f'Reason: {e.reason}, Line: {e.line}, Column: {e.column}'
                ),
                render_exception=True,
                ignore_traceback=True,
            )
            return None
        except Exception:
            self.config.diagnostics.error(
                f'Fail loading test from: {test_path}',
                render_exception=True,
            )
            return None

        if cache_key is not None:
            _store_cached_feature(cache_key, feature)

        return feature

    def _build_tests_from_feature(
        self,
        feature: Feature,
        test_path: Path,
    ) -> list[TestItem]:
        test_list: list[TestItem] = []

        for scenario in feature.scenarios:
            if scenario.examples:
                for example in scenario.examples:
                    for row_index, row in enumerate(example.rows, start=1):
                        example_name = (
                            f'{example.name or "Example"} #{row_index}'
                        )
                        test_list.append(
                            GherkinTestItem(
                                feature,
                                create_scenario_with_example(
                                    scenario,
                                    example_name,
                                    row,
                                    example.tags,
                                ),
                                example,
                                test_path,
                            ),
                        )
            else:
                test_list.append(
                    GherkinTestItem(feature, scenario, None, test_path),
                )

        return test_list

    async def load_tests_from_content(
        self,
        feature_content: str,
        test_path: Path,
    ) -> list[TestItem]:
        resolved_test_path = await asyncio.to_thread(test_path.resolve)
        feature = await asyncio.to_thread(
            _parse_feature_content,
            feature_content,
            resolved_test_path,
            self.config.root_path.parent,
        )
        return self._build_tests_from_feature(
            test_path=test_path,
            feature=feature,
        )

    async def load_tests_from_file(
        self,
        test_path: Path,
    ) -> list[TestItem] | None:
        feature = await self._load_feature(test_path)
        if feature is None:
            return None

        # Ya no llamamos a find_step_impl_directories aqui para evitar
        # redundancia masiva. Se halla al final de collect().

        return self._build_tests_from_feature(
            test_path=test_path,
            feature=feature,
        )

    async def find_step_impl_directories(self, test_path: Path) -> None:
        self.steps_directories.update(self._configured_step_directories)
        discovered_steps, scanned_directories = await asyncio.to_thread(
            _discover_step_directories,
            test_path,
            self.config.root_path,
            frozenset(self.scanned_directories),
        )
        self.scanned_directories.update(scanned_directories)
        self.steps_directories.update(discovered_steps)

    async def build_step_catalog(self) -> None:
        step_files = await asyncio.to_thread(
            _find_step_impl_files,
            tuple(sorted(self.steps_directories)),
        )
        step_file_snapshots = await asyncio.to_thread(
            _snapshot_step_files,
            tuple(step_files),
        )
        cached_files = {
            discovered_file.file_path.resolve(): discovered_file
            for discovered_file in (
                self.knowledge_store.get_discovered_step_files()
            )
        }
        current_step_paths = {
            file_path.resolve() for file_path, _, _ in step_file_snapshots
        }
        files_to_discover: list[Path] = []
        discovered_files: list[StaticDiscoveredStepFile] = []
        invalidated_files: list[tuple[Path, str]] = []

        for file_path, mtime_ns, file_size in step_file_snapshots:
            cached_file = cached_files.get(file_path)
            if (
                cached_file is not None
                and cached_file.mtime_ns == mtime_ns
                and cached_file.file_size == file_size
            ):
                try:
                    current_content_digest = await asyncio.to_thread(
                        _fingerprint_step_file,
                        file_path,
                    )
                except OSError:
                    files_to_discover.append(file_path)
                    continue

                if cached_file.content_digest == current_content_digest:
                    discovered_files.append(cached_file)
                    continue

                invalidated_files.append((file_path, 'content_changed'))
            elif cached_file is not None:
                invalidated_files.append((file_path, 'metadata_changed'))

            files_to_discover.append(file_path)

        invalidated_files.extend(
            (
                file_path,
                'file_removed',
            )
            for file_path in sorted(cached_files)
            if file_path not in current_step_paths
        )

        discovery_service = StepDiscoveryService(self.config.root_path)
        if files_to_discover:
            discovered_files.extend(
                await asyncio.to_thread(
                    discovery_service.discover_step_files,
                    tuple(files_to_discover),
                ),
            )

        ordered_discovered_files = tuple(
            sorted(
                discovered_files,
                key=lambda discovered_file: str(discovered_file.file_path),
            ),
        )
        self.step_catalog.update(ordered_discovered_files)
        self.knowledge_store.set_discovered_step_files(
            ordered_discovered_files,
        )
        self.knowledge_store.set_project_step_index(self.step_catalog)
        await self._emit_step_catalog_events(
            ordered_discovered_files,
            tuple(invalidated_files),
        )

    def get_project_step_index(self) -> ProjectStepIndex:
        return self.knowledge_store.get_project_step_index()

    async def _emit_step_catalog_events(
        self,
        discovered_files: tuple[StaticDiscoveredStepFile, ...],
        invalidated_files: tuple[tuple[Path, str], ...],
    ) -> None:
        if self._domain_event_stream is None:
            return

        knowledge_version = (
            f'gherkin_step_index:v{GHERKIN_STEP_INDEX_SCHEMA_VERSION}'
        )
        invalidation_events = tuple(
            KnowledgeInvalidatedEvent(
                engine_name=self._engine_name,
                file_path=str(file_path),
                reason=reason,
                knowledge_version=knowledge_version,
            )
            for file_path, reason in invalidated_files
        )
        indexed_events = tuple(
            KnowledgeIndexedEvent(
                engine_name=self._engine_name,
                file_path=str(discovered_file.file_path),
                definition_count=len(discovered_file.descriptors),
                discovery_mode=discovered_file.discovery_mode,
                knowledge_version=knowledge_version,
                content_hash=discovered_file.content_digest,
                descriptors=tuple(
                    _build_definition_descriptor_knowledge(descriptor)
                    for descriptor in discovered_file.descriptors
                ),
            )
            for discovered_file in discovered_files
        )
        await asyncio.gather(
            *(
                self._domain_event_stream.emit(event)
                for event in (
                    *invalidation_events,
                    *indexed_events,
                )
            ),
        )

    async def _emit_test_knowledge_events(
        self,
        *,
        collect_paths: tuple[Path, ...],
        excluded_paths: tuple[Path, ...],
    ) -> None:
        if self._domain_event_stream is None:
            return

        indexed_files: dict[Path, list[GherkinTestItem]] = {
            (self.config.root_path / file_path).resolve(): []
            for file_path in self.collected_files
        }
        for test in self.collected_tests:
            if not isinstance(test, GherkinTestItem) or test.path is None:
                continue

            indexed_files.setdefault(test.path.resolve(), []).append(test)

        for file_path, tests in sorted(
            indexed_files.items(),
            key=lambda item: str(item[0]),
        ):
            test_path_label = build_test_path_label(
                self.config.root_path,
                file_path,
            )
            await self._domain_event_stream.emit(
                TestKnowledgeIndexedEvent(
                    engine_name=self._engine_name,
                    file_path=test_path_label,
                    tests=tuple(
                        _build_test_descriptor_knowledge(
                            test,
                            root_path=self.config.root_path,
                            engine_name=self._engine_name,
                            file_path_label=test_path_label,
                        )
                        for test in tests
                    ),
                    discovery_mode='parser',
                    knowledge_version=_build_test_knowledge_version(
                        file_path,
                    ),
                    content_hash=_build_test_content_hash(file_path),
                ),
            )

        for file_path in sorted(self.failed_files, key=str):
            await self._domain_event_stream.emit(
                TestKnowledgeInvalidatedEvent(
                    engine_name=self._engine_name,
                    file_path=str(file_path),
                    reason='parse_failed',
                    knowledge_version='gherkin_test_index:invalid',
                ),
            )

        for file_path in _discover_removed_test_files(
            root_path=self.config.root_path,
            engine_name=self._engine_name,
            collect_scope=(collect_paths, excluded_paths),
            indexed_files=tuple(self.collected_files),
            failed_files=tuple(self.failed_files),
        ):
            await self._domain_event_stream.emit(
                TestKnowledgeInvalidatedEvent(
                    engine_name=self._engine_name,
                    file_path=str(file_path),
                    reason='file_removed',
                    knowledge_version='gherkin_test_index:invalid',
                ),
            )

    async def load_step_impl(self, step_registry: StepRegistry):
        # Descubrimos y cargamos todos los steps en lote para evitar
        # mutaciones concurrentes sobre `StepRegistry` y `sys.modules`.
        step_files = await asyncio.to_thread(
            _find_step_impl_files,
            tuple(sorted(self.steps_directories)),
        )

        failures = await asyncio.to_thread(
            _load_step_impl_files,
            tuple(step_files),
            step_registry,
        )

        for file_path, formatted_traceback in failures:
            self.config.diagnostics.error(
                f'Fail loading steps from: {file_path}',
                details=formatted_traceback,
            )
            self.failed_files.add(file_path)


def _build_definition_descriptor_knowledge(
    descriptor: StaticStepDescriptor,
):
    return build_gherkin_definition_record(
        source_line=descriptor.source_line,
        function_name=descriptor.function_name,
        step_type=descriptor.step_type,
        patterns=descriptor.patterns,
        literal_prefixes=descriptor.literal_prefixes,
        literal_suffixes=descriptor.literal_suffixes,
        literal_fragments=descriptor.literal_fragments,
        anchor_tokens=descriptor.anchor_tokens,
        dynamic_fragment_count=descriptor.dynamic_fragment_count,
        documentation=descriptor.documentation,
        parser_cls_name=descriptor.parser_cls_name,
        category=descriptor.category,
        discovery_mode=descriptor.discovery_mode,
    )


def _build_test_knowledge_version(file_path: Path) -> str:
    return f'gherkin_test_index:{_build_test_content_hash(file_path)}'


def _build_test_content_hash(file_path: Path) -> str:
    return hashlib.sha256(file_path.read_bytes()).hexdigest()


def _build_test_descriptor_knowledge(
    test: GherkinTestItem,
    *,
    root_path: Path,
    engine_name: str,
    file_path_label: str,
) -> TestDescriptorKnowledge:
    selection_labels = tuple(
        dict.fromkeys(
            tag.name for tag in (*test.feature.tags, *test.scenario.tags)
        ),
    )
    return TestDescriptorKnowledge(
        stable_id=build_execution_node_stable_id(
            root_path,
            engine_name,
            test,
        ),
        test_name=test.test_name,
        file_path=file_path_label,
        source_line=int(test.scenario.location.line),
        selection_labels=selection_labels,
    )


def _discover_removed_test_files(
    *,
    root_path: Path,
    engine_name: str,
    collect_scope: tuple[tuple[Path, ...], tuple[Path, ...]],
    indexed_files: tuple[Path, ...],
    failed_files: tuple[Path, ...],
) -> tuple[Path, ...]:
    db_path = resolve_knowledge_base_path(root_path)
    if not db_path.exists():
        return ()

    collect_paths, excluded_paths = collect_scope
    selected_collect_paths = tuple(
        _resolve_collect_scope_path(root_path, path) for path in collect_paths
    )
    selected_excluded_paths = tuple(
        _resolve_collect_scope_path(root_path, path) for path in excluded_paths
    )
    indexed_file_set = {Path(path) for path in indexed_files}
    failed_file_set = {Path(path) for path in failed_files}

    knowledge_base = ReadOnlyPersistentKnowledgeBase(db_path)
    try:
        known_paths = {
            Path(test.test_path)
            for test in knowledge_base.query_tests(
                TestKnowledgeQuery(engine_name=engine_name),
            )
        }
    finally:
        knowledge_base.close()

    removed_paths = []
    for known_path in sorted(known_paths, key=str):
        if known_path in indexed_file_set or known_path in failed_file_set:
            continue

        absolute_path = (root_path / known_path).resolve()
        if absolute_path.exists():
            continue

        if not _is_path_within_collect_scope(
            absolute_path,
            collect_paths=selected_collect_paths,
            excluded_paths=selected_excluded_paths,
        ):
            continue

        removed_paths.append(known_path)

    return tuple(removed_paths)


def _resolve_collect_scope_path(
    root_path: Path,
    path: Path,
) -> Path:
    return (
        path.resolve() if path.is_absolute() else (root_path / path).resolve()
    )


def _is_path_within_collect_scope(
    file_path: Path,
    *,
    collect_paths: tuple[Path, ...],
    excluded_paths: tuple[Path, ...],
) -> bool:
    if not any(
        is_subpath(collect_path, file_path) for collect_path in collect_paths
    ):
        return False

    return not any(
        is_subpath(excluded_path, file_path)
        for excluded_path in excluded_paths
    )
