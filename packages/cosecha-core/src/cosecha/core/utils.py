from __future__ import annotations

import importlib
import sys

from contextlib import contextmanager, suppress
from datetime import datetime, time
from pathlib import Path
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from cosecha.core.discovery import (
    create_loaded_discovery_registry,
    get_current_discovery_registry,
    get_default_discovery_registry,
    iter_plugin_types,
    using_discovery_registry,
)
from cosecha.core.module_loading import (
    import_module_from_path as _load_from_path,
)
from cosecha.core.plugins.base import PLUGIN_API_VERSION, Plugin


if TYPE_CHECKING:  # pragma: no cover
    from argparse import ArgumentParser, Namespace
    from collections.abc import Iterator
    from types import ModuleType

    from cosecha.core.config import Config
    from cosecha.core.engines.base import Engine
    from cosecha.core.hooks import Hook


def import_module_from_path(module_path: str | Path) -> ModuleType:
    return _load_from_path(
        module_path,
        prepare_import_paths=_temporary_import_paths,
    )


def _discover_import_search_paths(module_path: Path) -> tuple[Path, ...]:
    search_paths: list[Path] = []

    project_root = next(
        (
            parent
            for parent in module_path.parents
            if parent.name in {'tests', 'src'}
        ),
        None,
    )
    if project_root is not None:
        project_root = project_root.parent

    if project_root is None:
        project_root = module_path.parent

    workspace_root = project_root.parent
    for candidate in (
        project_root / 'src',
        project_root / 'tests',
        project_root,
    ):
        search_paths.extend(
            candidate
            for candidate in (
                project_root / 'src',
                project_root / 'tests',
                project_root,
            )
            if candidate.exists()
        )

    if workspace_root.exists():
        for sibling in sorted(workspace_root.iterdir()):
            if not sibling.is_dir() or sibling == project_root:
                continue

            for child_name in ('src', 'tests'):
                candidate = sibling / child_name
                if candidate.exists():
                    search_paths.append(candidate)

        search_paths.extend(_discover_workspace_site_packages(workspace_root))

    deduped_paths: list[Path] = []
    seen: set[Path] = set()
    for path in search_paths:
        resolved_path = path.resolve()
        if resolved_path in seen:
            continue
        seen.add(resolved_path)
        deduped_paths.append(resolved_path)

    return tuple(deduped_paths)


def _discover_workspace_site_packages(
    workspace_root: Path,
) -> tuple[Path, ...]:
    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    search_patterns = (
        f'.venv/lib/{version_name}/site-packages',
        f'venv/lib/{version_name}/site-packages',
        f'venv{sys.version_info.major}.{sys.version_info.minor}/lib/{version_name}/site-packages',
        f'venv{sys.version_info.major}{sys.version_info.minor}/lib/{version_name}/site-packages',
    )

    discovered_paths: list[Path] = []
    for pattern in search_patterns:
        discovered_paths.extend(
            candidate
            for candidate in sorted(workspace_root.glob(pattern))
            if candidate.exists()
        )

    if discovered_paths:
        return tuple(discovered_paths)

    fallback_patterns = (
        '.venv/lib/python*/site-packages',
        'venv/lib/python*/site-packages',
        'venv*/lib/python*/site-packages',
    )
    for pattern in fallback_patterns:
        for candidate in sorted(workspace_root.glob(pattern)):
            if not candidate.exists():
                continue
            if candidate.parent.name != version_name:
                continue
            discovered_paths.append(candidate)

    return tuple(discovered_paths)


@contextmanager
def _temporary_import_paths(module_path: Path) -> Iterator[None]:
    inserted_paths: list[str] = []
    try:
        for search_path in reversed(
            _discover_import_search_paths(module_path),
        ):
            rendered_path = str(search_path)
            if rendered_path in sys.path:
                continue
            sys.path.insert(0, rendered_path)
            inserted_paths.append(rendered_path)
        yield
    finally:
        for rendered_path in inserted_paths:
            with suppress(ValueError):
                sys.path.remove(rendered_path)


def import_module(module_spec_or_path: str | Path) -> ModuleType:
    if isinstance(module_spec_or_path, str):
        try:
            return importlib.import_module(module_spec_or_path)
        except ImportError:
            module_spec_or_path = Path(module_spec_or_path)

    return import_module_from_path(module_spec_or_path)


def is_subpath(root_path: Path, other_path: Path) -> bool:
    root_path = root_path.resolve()
    other_path = other_path.resolve()

    root_parts = root_path.parts
    collect_parts = other_path.parts

    return len(root_parts) <= len(collect_parts) and all(
        r == c for r, c in zip(root_parts, collect_parts, strict=False)
    )


def setup_engines(
    config: Config,
    *,
    args: Namespace | None = None,
    manifest_file: Path | None = None,
    selected_engine_names: set[str] | None = None,
    requested_paths: tuple[str, ...] = (),
) -> tuple[list[Hook], dict[str, Engine]]:
    manifest_module = importlib.import_module(
        'cosecha.core.cosecha_manifest',
    )

    current_registry = get_current_discovery_registry()
    if current_registry is get_default_discovery_registry():
        registry = create_loaded_discovery_registry()
        with using_discovery_registry(registry):
            return setup_engines(
                config,
                args=args,
                manifest_file=manifest_file,
                selected_engine_names=selected_engine_names,
                requested_paths=requested_paths,
            )

    manifest = manifest_module.load_cosecha_manifest(manifest_file)
    if manifest is None:
        return ([], {})

    if args is not None:
        manifest = manifest_module.apply_manifest_cli_overrides(
            manifest,
            args,
        )

    hooks, engines = manifest_module.materialize_runtime_components(
        manifest,
        config=config,
        selected_engine_names=selected_engine_names,
        requested_paths=requested_paths,
    )

    received_engine_names: set[str] = set()
    for engine in engines.values():
        if engine.name in received_engine_names:
            msg = f'Duplicated engine name: {engine.name}'
            raise ValueError(msg)

        received_engine_names.add(engine.name)

    return (hooks, engines)


def setup_available_plugins(parser: ArgumentParser) -> list[type[Plugin]]:
    current_registry = get_current_discovery_registry()
    if current_registry is get_default_discovery_registry():
        registry = create_loaded_discovery_registry()
        with using_discovery_registry(registry):
            return setup_available_plugins(parser)

    available_plugins: list[type[Plugin]] = []
    for plugin in iter_plugin_types():
        validate_plugin_class(plugin)
        plugin.register_arguments(parser)
        available_plugins.append(plugin)

    return available_plugins


def validate_plugin_class(plugin: type[Plugin]) -> None:
    if not issubclass(plugin, Plugin):
        msg = f'Invalid plugin type: {plugin!r}'
        raise TypeError(msg)

    plugin_api_version = plugin.plugin_api_version()
    if plugin_api_version != PLUGIN_API_VERSION:
        msg = (
            'Unsupported plugin API version for '
            f'{plugin.plugin_name()}: {plugin_api_version} '
            f'(expected {PLUGIN_API_VERSION})'
        )
        raise ValueError(msg)


def get_today(tz: str | None = None) -> datetime:
    """Obtiene el día para la zona horaria especificada.

    Args:
        tz (str | None): La zona horaria en formato IANA, como 'Europe/Madrid'
                         o 'UTC'. Si es None, se usa 'Europe/Madrid' por
                         defecto.

    Returns:
        datetime: Un objeto datetime que representa la medianoche de hoy en la
                  zona horaria especificada.

    Ejemplos:
        >>> get_today('Europe/Madrid')
        datetime.datetime(2024, 5, 19, 0, 0, tzinfo=datetime.timezone.utc)
    """
    zone = ZoneInfo(tz or 'Europe/Madrid')

    now = datetime.now(zone)
    return datetime.combine(now.date(), time.min)
