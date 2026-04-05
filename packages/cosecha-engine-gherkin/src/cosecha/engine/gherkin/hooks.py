from __future__ import annotations

import importlib
import inspect
import pkgutil
import re

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cosecha.core.hooks import EngineHook
from cosecha.engine.gherkin.utils import import_and_load_steps_from_module


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path

    from cosecha.core.collector import Collector
    from cosecha.core.cosecha_manifest import RegistryLayoutSpec
    from cosecha.core.engines.base import Engine
    from cosecha.engine.gherkin.step_ast_discovery import (
        StaticDiscoveredStepFile,
    )


type RegistryEntry = tuple[str, str, object]


_registry_loader_cache: dict[str, tuple[RegistryEntry, ...]] = {}
_registry_loader_futures: dict[str, object] = {}
_scan_futures: dict[str, object] = {}
_scan_refs_cache: dict[str, tuple[Path, ...]] = {}
_MODULE_GLOB_DOUBLE_STAR = re.compile(r'\\\*\\\*')
_MODULE_GLOB_SINGLE_STAR = re.compile(r'\\\*')


def _module_glob_to_regex(module_glob: str) -> re.Pattern[str]:
    pattern = re.escape(module_glob)
    pattern = _MODULE_GLOB_DOUBLE_STAR.sub(r'.*', pattern)
    pattern = _MODULE_GLOB_SINGLE_STAR.sub(r'[^.]+', pattern)
    return re.compile(f'^{pattern}$')


def _iter_compatible_module_globs(module_glob: str) -> tuple[str, ...]:
    variants = [module_glob]
    segments = module_glob.split('.')
    last_segment = segments[-1]
    if last_segment.endswith('s') and len(last_segment) > 1:
        singular_segments = [*segments[:-1], last_segment[:-1]]
        variants.append('.'.join(singular_segments))
    return tuple(dict.fromkeys(variants))


def _extract_root_package(module_glob: str) -> str:
    root_package, *_ = module_glob.split('.')
    return root_package


def _iter_module_names(
    layouts: tuple[RegistryLayoutSpec, ...],
) -> tuple[str, ...]:
    regexes = tuple(
        _module_glob_to_regex(candidate_glob)
        for layout in layouts
        for module_glob in layout.module_globs
        for candidate_glob in _iter_compatible_module_globs(module_glob)
    )
    discovered_module_names: set[str] = set()
    for root_package in {
        _extract_root_package(module_glob)
        for layout in layouts
        for module_glob in layout.module_globs
    }:
        module = importlib.import_module(root_package)
        discovered_module_names.add(module.__name__)
        module_paths = getattr(module, '__path__', None)
        if module_paths is None:
            continue
        for _finder, module_name, _is_pkg in pkgutil.walk_packages(
            module_paths,
            prefix=f'{module.__name__}.',
        ):
            discovered_module_names.add(module_name)

    return tuple(
        module_name
        for module_name in sorted(discovered_module_names)
        if any(regex.match(module_name) for regex in regexes)
    )


def _matches_layout_item(
    item: object,
    *,
    base_class: type[object],
    match_mode: str,
) -> bool:
    if not inspect.isclass(item):
        return False

    if match_mode == 'exact':
        return item is base_class

    return issubclass(item, base_class)


def _load_registry_entries_sync(
    layouts: tuple[RegistryLayoutSpec, ...],
    *,
    root_path: Path,
) -> tuple[RegistryEntry, ...]:
    entries_by_key: dict[tuple[str, str], object] = {}
    for layout in layouts:
        base_class = layout.base.resolve(root_path=root_path)
        if not inspect.isclass(base_class):
            msg = (
                f'Registry layout base must resolve to a class for '
                f'{layout.name!r}'
            )
            raise TypeError(msg)

        for module_name in _iter_module_names((layout,)):
            module = importlib.import_module(module_name)
            for item in vars(module).values():
                if not _matches_layout_item(
                    item,
                    base_class=base_class,
                    match_mode=layout.match,
                ):
                    continue
                entries_by_key.setdefault((layout.name, item.__name__), item)

    return tuple(
        (layout, name, item)
        for (layout, name), item in sorted(entries_by_key.items())
    )


@dataclass(slots=True, frozen=True)
class GherkinRegistryLoader:
    layouts: tuple[RegistryLayoutSpec, ...] = ()

    def cache_key(self) -> str:
        return repr(self.layouts)

    async def load(self, root_path: Path) -> tuple[RegistryEntry, ...]:
        cache_key = self.cache_key()
        cached = _registry_loader_cache.get(cache_key)
        if cached is not None:
            return cached
        loaded_entries = _load_registry_entries_sync(
            self.layouts,
            root_path=root_path,
        )
        _registry_loader_cache[cache_key] = loaded_entries
        return loaded_entries


@dataclass(slots=True)
class GherkinLibraryHook(EngineHook):
    step_library_modules: tuple[str, ...] = ()
    registry_loaders: tuple[GherkinRegistryLoader, ...] = ()
    library_discovered_step_files: tuple[StaticDiscoveredStepFile, ...] = ()
    library_import_targets_by_file: dict[Path, str | Path] = field(
        default_factory=dict,
    )
    registry_entries: tuple[RegistryEntry, ...] = ()

    async def before_collect(
        self,
        path: Path,
        collector: Collector,
        engine: Engine,
    ) -> None:
        del path, collector
        step_registry = getattr(engine, 'step_registry', None)
        if step_registry is not None:
            for module_spec in self.step_library_modules:
                import_and_load_steps_from_module(
                    module_spec,
                    step_registry,
                )
        self.library_discovered_step_files = ()

    async def before_session_start(self, engine: Engine) -> None:
        if self.registry_entries:
            return

        root_path = engine.config.root_path
        loaded_entries: list[RegistryEntry] = []
        for registry_loader in self.registry_loaders:
            loaded_entries.extend(await registry_loader.load(root_path))
        self.registry_entries = tuple(loaded_entries)

        context_registry = getattr(engine, 'context_registry', None)
        if context_registry is None:
            return

        for layout, name, item in self.registry_entries:
            context_registry.add(layout, name, item)
