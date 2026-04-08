from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from cosecha.core.plugins.base import PLUGIN_API_VERSION, Plugin
from cosecha.core.utils import (
    _discover_import_search_paths,
    _discover_import_search_paths_legacy,
    _discover_workspace_site_packages,
    _temporary_import_paths,
    get_today,
    import_module,
    import_module_from_path,
    setup_available_plugins,
    setup_engines,
    validate_plugin_class,
)
from cosecha.workspace import WorkspaceResolutionError
from cosecha_internal.testkit import build_config


if TYPE_CHECKING:
    from argparse import ArgumentParser, Namespace


class _ValidPlugin(Plugin):
    registered_parsers: list[object] = []

    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None:
        cls.registered_parsers.append(parser)

    @classmethod
    def parse_args(cls, args: Namespace):
        del args
        return cls()

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


def test_discover_import_search_paths_prefers_workspace_locations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    included = tmp_path / 'included'
    included.mkdir()
    excluded = tmp_path / 'excluded'

    workspace = SimpleNamespace(
        import_environment=SimpleNamespace(
            locations=(
                SimpleNamespace(path=included, importable=True),
                SimpleNamespace(path=excluded, importable=True),
                SimpleNamespace(path=included, importable=False),
            ),
        ),
    )

    monkeypatch.setattr('cosecha.core.utils.resolve_workspace', lambda **_kwargs: workspace)
    paths = _discover_import_search_paths(tmp_path / 'pkg' / 'module.py')
    assert paths == (included.resolve(),)


def test_discover_import_search_paths_falls_back_to_legacy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fallback = (tmp_path / 'fallback',)
    monkeypatch.setattr(
        'cosecha.core.utils.resolve_workspace',
        lambda **_kwargs: (_ for _ in ()).throw(WorkspaceResolutionError('no workspace')),
    )
    monkeypatch.setattr(
        'cosecha.core.utils._discover_import_search_paths_legacy',
        lambda _path: fallback,
    )

    assert _discover_import_search_paths(tmp_path / 'x.py') == fallback


def test_discover_import_search_paths_legacy_and_site_packages(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    project_root = workspace_root / 'project'
    module_path = project_root / 'src' / 'pkg' / 'module.py'
    module_path.parent.mkdir(parents=True)
    module_path.write_text('', encoding='utf-8')
    (project_root / 'tests').mkdir(parents=True)

    sibling = workspace_root / 'shared'
    (sibling / 'src').mkdir(parents=True)
    (sibling / 'tests').mkdir(parents=True)

    site_packages = workspace_root / '.venv' / 'lib' / (
        f'python{__import__("sys").version_info.major}.{__import__("sys").version_info.minor}'
    ) / 'site-packages'
    site_packages.mkdir(parents=True)

    discovered = _discover_import_search_paths_legacy(module_path)

    assert (project_root / 'src').resolve() in discovered
    assert (project_root / 'tests').resolve() in discovered
    assert (sibling / 'src').resolve() in discovered
    assert (sibling / 'tests').resolve() in discovered
    assert site_packages.resolve() in discovered


def test_discover_workspace_site_packages_supports_fallback_patterns(
    tmp_path: Path,
) -> None:
    import sys

    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    fallback_site_packages = (
        tmp_path
        / 'venv-custom'
        / 'lib'
        / version_name
        / 'site-packages'
    )
    fallback_site_packages.mkdir(parents=True)

    assert _discover_workspace_site_packages(tmp_path) == (
        fallback_site_packages,
    )


def test_temporary_import_paths_inserts_and_removes_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = tmp_path / 'first'
    second = tmp_path / 'second'
    first.mkdir()
    second.mkdir()

    original_sys_path = list(__import__('sys').path)
    monkeypatch.setattr(
        'cosecha.core.utils._discover_import_search_paths',
        lambda _module_path: (first, second),
    )
    with _temporary_import_paths(tmp_path / 'module.py'):
        assert __import__('sys').path[0] == str(first)
        assert __import__('sys').path[1] == str(second)
        __import__('sys').path.remove(str(first))

    assert __import__('sys').path == original_sys_path


def test_import_module_variants_and_import_module_from_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel_module = object()
    module_path = tmp_path / 'module.py'
    module_path.write_text('', encoding='utf-8')

    monkeypatch.setattr(
        'cosecha.core.utils._load_from_path',
        lambda path, prepare_import_paths: (
            sentinel_module
            if path == module_path
            and prepare_import_paths is _temporary_import_paths
            else None
        ),
    )
    assert import_module_from_path(module_path) is sentinel_module

    monkeypatch.setattr(
        'cosecha.core.utils.importlib.import_module',
        lambda spec: sentinel_module if spec == 'json' else (_ for _ in ()).throw(ImportError('boom')),
    )
    monkeypatch.setattr(
        'cosecha.core.utils.import_module_from_path',
        lambda path: ('from-path', Path(path)),
    )

    assert import_module('json') is sentinel_module
    assert import_module(str(module_path)) == ('from-path', module_path)
    assert import_module(module_path) == ('from-path', module_path)


def test_setup_engines_and_plugins_and_plugin_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    default_registry = object()
    current_registry = {'value': default_registry}

    @contextmanager
    def _using_registry(registry):
        previous = current_registry['value']
        current_registry['value'] = registry
        try:
            yield
        finally:
            current_registry['value'] = previous

    manifest = object()
    config = build_config(tmp_path)
    args = SimpleNamespace(debug=True)

    def _materialize_runtime_components(
        _manifest,
        *,
        config,
        selected_engine_names,
        requested_paths,
    ):
        del _manifest, config, selected_engine_names, requested_paths
        return (
            ['hook-1'],
            {'a': SimpleNamespace(name='engine-a')},
        )

    manifest_module = SimpleNamespace(
        load_cosecha_manifest=lambda _manifest_file: manifest,
        apply_manifest_cli_overrides=lambda loaded_manifest, _args: loaded_manifest,
        materialize_runtime_components=_materialize_runtime_components,
    )

    monkeypatch.setattr(
        'cosecha.core.utils.importlib.import_module',
        lambda _name: manifest_module,
    )
    monkeypatch.setattr(
        'cosecha.core.utils.get_default_discovery_registry',
        lambda: default_registry,
    )
    monkeypatch.setattr(
        'cosecha.core.utils.get_current_discovery_registry',
        lambda: current_registry['value'],
    )
    monkeypatch.setattr(
        'cosecha.core.utils.create_loaded_discovery_registry',
        lambda: object(),
    )
    monkeypatch.setattr('cosecha.core.utils.using_discovery_registry', _using_registry)

    hooks, engines = setup_engines(
        config,
        args=args,
        requested_paths=('tests',),
    )
    assert hooks == ['hook-1']
    assert tuple(engines.keys()) == ('a',)

    manifest_module.load_cosecha_manifest = lambda _manifest_file: None
    assert setup_engines(config) == ([], {})

    manifest_module.load_cosecha_manifest = lambda _manifest_file: manifest
    manifest_module.materialize_runtime_components = lambda *args, **kwargs: (
        [],
        {
            'one': SimpleNamespace(name='dup'),
            'two': SimpleNamespace(name='dup'),
        },
    )
    with pytest.raises(ValueError, match='Duplicated engine name'):
        setup_engines(config)

    parser = SimpleNamespace()
    monkeypatch.setattr(
        'cosecha.core.utils.iter_plugin_types',
        lambda: (_ValidPlugin,),
    )
    plugins = setup_available_plugins(parser)
    assert plugins == [_ValidPlugin]
    assert _ValidPlugin.registered_parsers[-1] is parser

    class _WrongApiPlugin(_ValidPlugin):
        @classmethod
        def plugin_api_version(cls) -> int:
            return PLUGIN_API_VERSION + 1

    with pytest.raises(TypeError, match='Invalid plugin type'):
        validate_plugin_class(object)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match='Unsupported plugin API version'):
        validate_plugin_class(_WrongApiPlugin)


def test_get_today_returns_midnight_for_requested_timezone() -> None:
    madrid_today = get_today('Europe/Madrid')
    default_today = get_today()

    assert madrid_today.hour == 0
    assert madrid_today.minute == 0
    assert default_today.hour == 0
    assert default_today.minute == 0
    assert abs(default_today - madrid_today) <= timedelta(days=1)
