from __future__ import annotations

import importlib
import tomllib

from pathlib import Path

from cosecha.core.plugins.base import Plugin
from cosecha.core.reporter import Reporter


WORKSPACE_ENTRYPOINT_GROUPS = {
    'cosecha.console.presenters',
    'cosecha.engines',
    'cosecha.hooks',
    'cosecha.knowledge.query',
    'cosecha.plugins',
    'cosecha.shell.lsp',
    'cosecha.shell.reporting',
    'pytest11',
}


def _iter_pyproject_paths() -> tuple[Path, ...]:
    workspace_root = Path(__file__).resolve().parents[3]
    package_paths = sorted((workspace_root / 'packages').glob('*/pyproject.toml'))
    internal_paths = sorted((workspace_root / 'internal').glob('*/pyproject.toml'))
    return tuple(package_paths + internal_paths)


def _iter_entry_points() -> tuple[tuple[str, str, str, str], ...]:
    entry_points: list[tuple[str, str, str, str]] = []
    for pyproject_path in _iter_pyproject_paths():
        project = tomllib.loads(pyproject_path.read_text(encoding='utf-8')).get(
            'project',
            {},
        )
        for group_name, group_entries in project.get('entry-points', {}).items():
            if group_name not in WORKSPACE_ENTRYPOINT_GROUPS:
                continue
            for entry_name, target in group_entries.items():
                entry_points.append(
                    (pyproject_path.parent.name, group_name, entry_name, target),
                )
    return tuple(entry_points)


def _load_target(target: str) -> object:
    if ':' not in target:
        return importlib.import_module(target)

    module_name, attribute_name = target.split(':', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute_name)


def test_workspace_entry_points_resolve_to_importable_targets() -> None:
    for _, _, _, target in _iter_entry_points():
        assert _load_target(target) is not None


def test_workspace_entry_points_expose_expected_contract_shapes() -> None:
    seen_entry_names: set[tuple[str, str]] = set()

    for package_name, group_name, entry_name, target in _iter_entry_points():
        assert (group_name, entry_name) not in seen_entry_names
        seen_entry_names.add((group_name, entry_name))
        loaded_target = _load_target(target)

        if group_name == 'cosecha.engines':
            assert getattr(loaded_target, 'engine_type', None) == entry_name
            assert callable(getattr(loaded_target, 'materialize', None))
            assert callable(
                getattr(loaded_target, 'validate_resource_binding', None),
            )
            continue

        if group_name == 'cosecha.hooks':
            assert getattr(loaded_target, 'hook_type', None) == entry_name
            assert callable(
                getattr(loaded_target, 'register_arguments', None),
            )
            assert callable(getattr(loaded_target, 'materialize', None))
            continue

        if group_name == 'cosecha.knowledge.query':
            assert getattr(loaded_target, 'engine_name', None) == entry_name
            assert callable(
                getattr(loaded_target, 'matching_descriptors', None),
            )
            continue

        if group_name == 'cosecha.plugins':
            assert issubclass(loaded_target, Plugin)
            continue

        if group_name == 'cosecha.shell.reporting':
            assert issubclass(loaded_target, Reporter)
            continue

        if group_name in {
            'cosecha.console.presenters',
            'cosecha.shell.lsp',
        }:
            assert getattr(loaded_target, 'contribution_name', None) == (
                entry_name
            )
            continue

        if group_name == 'pytest11':
            assert package_name == 'cosecha-engine-pytest'
            assert callable(getattr(loaded_target, 'pytest_configure', None))
