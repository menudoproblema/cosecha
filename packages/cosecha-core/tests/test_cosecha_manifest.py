from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from cosecha.core.config import Config
from cosecha.core.cosecha_manifest import (
    ManifestValidationError,
    discover_cosecha_manifest,
    explain_cosecha_manifest,
    load_cosecha_manifest,
    materialize_runtime_components,
    parse_cosecha_manifest_text,
)


if TYPE_CHECKING:
    from pathlib import Path


PYTHON_ENGINE_SUPPORT = """
from pathlib import Path

from cosecha.core.collector import Collector
from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.reporter import Reporter


def _record(name: str) -> None:
    path = Path(__file__).with_name('calls.txt')
    previous = path.read_text(encoding='utf-8') if path.exists() else ''
    path.write_text(previous + name + '\\n', encoding='utf-8')


class DummyContext(BaseContext):
    async def cleanup(self) -> None:
        return None


class DummyCollector(Collector):
    def __init__(self) -> None:
        super().__init__('feature')

    async def find_test_files(self, base_path):
        del base_path
        return []

    async def load_tests_from_file(self, test_path):
        del test_path
        return []


class DummyReporter(Reporter):
    async def add_test(self, test):
        del test

    async def add_test_result(self, test):
        del test

    async def print_report(self):
        return None


class DummyEngine(Engine):
    async def generate_new_context(self, test):
        del test
        return DummyContext()


def build_alpha_engine():
    _record('alpha')
    return DummyEngine(
        'alpha',
        collector=DummyCollector(),
        reporter=DummyReporter(),
    )


def build_beta_engine():
    _record('beta')
    return DummyEngine(
        'beta',
        collector=DummyCollector(),
        reporter=DummyReporter(),
    )
"""


def test_discover_manifest_prefers_tests_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tests_manifest = tmp_path / 'tests' / 'cosecha.toml'
    root_manifest = tmp_path / 'cosecha.toml'
    tests_manifest.parent.mkdir()
    tests_manifest.write_text(
        '[manifest]\nschema_version = 1\n',
        encoding='utf-8',
    )
    root_manifest.write_text(
        '[manifest]\nschema_version = 1\n',
        encoding='utf-8',
    )

    monkeypatch.chdir(tmp_path)

    assert discover_cosecha_manifest() == tests_manifest.resolve()


def test_load_cosecha_manifest_parses_python_engine_factory(
    tmp_path: Path,
) -> None:
    tests_path = tmp_path / 'tests'
    manifest_path = tests_path / 'cosecha.toml'
    tests_path.mkdir()
    (tests_path / 'support.py').write_text(
        PYTHON_ENGINE_SUPPORT,
        encoding='utf-8',
    )
    manifest_path.write_text(
        '\n'.join(
            (
                '[manifest]',
                'schema_version = 1',
                '',
                '[[engines]]',
                'id = "alpha"',
                'type = "python"',
                'name = "alpha"',
                'path = ""',
                'factory = "support.py:build_alpha_engine"',
            ),
        ),
        encoding='utf-8',
    )

    manifest = load_cosecha_manifest(manifest_path)

    assert manifest is not None
    assert manifest.engines[0].id == 'alpha'
    assert manifest.engines[0].factory is not None
    assert manifest.engines[0].factory.module == 'support.py'
    assert manifest.engines[0].factory.qualname == 'build_alpha_engine'


def test_materialize_runtime_components_selects_only_requested_engine(
    tmp_path: Path,
) -> None:
    tests_path = tmp_path / 'tests'
    manifest_path = tests_path / 'cosecha.toml'
    tests_path.mkdir()
    (tests_path / 'support.py').write_text(
        PYTHON_ENGINE_SUPPORT,
        encoding='utf-8',
    )
    manifest_path.write_text(
        '\n'.join(
            (
                '[manifest]',
                'schema_version = 1',
                '',
                '[[engines]]',
                'id = "alpha"',
                'type = "python"',
                'name = "alpha"',
                'path = "alpha"',
                'factory = "support.py:build_alpha_engine"',
                '',
                '[[engines]]',
                'id = "beta"',
                'type = "python"',
                'name = "beta"',
                'path = "beta"',
                'factory = "support.py:build_beta_engine"',
            ),
        ),
        encoding='utf-8',
    )
    manifest = load_cosecha_manifest(manifest_path)
    assert manifest is not None

    hooks, engines = materialize_runtime_components(
        manifest,
        config=Config(root_path=tests_path),
        selected_engine_names={'alpha'},
    )

    assert hooks == []
    assert tuple(engines) == ('alpha',)
    assert (tests_path / 'calls.txt').read_text(encoding='utf-8') == 'alpha\n'


def test_parse_cosecha_manifest_rejects_invalid_symbol_ref(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / 'tests' / 'cosecha.toml'
    manifest_path.parent.mkdir()
    (manifest_path.parent / 'support.py').write_text(
        'def build_alpha_engine():\n    return None\n',
        encoding='utf-8',
    )

    with pytest.raises(ManifestValidationError):
        parse_cosecha_manifest_text(
            '\n'.join(
                (
                    '[manifest]',
                    'schema_version = 1',
                    '',
                    '[[engines]]',
                    'id = "alpha"',
                    'type = "python"',
                    'name = "alpha"',
                    'path = ""',
                    'factory = "support.py:missing_factory"',
                ),
            ),
            manifest_path=manifest_path,
            resolve_symbols=True,
        )


def test_parse_cosecha_manifest_parses_registry_loader_layouts(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / 'tests' / 'cosecha.toml'
    manifest_path.parent.mkdir()
    manifest_path.write_text(
        '\n'.join(
            (
                '[manifest]',
                'schema_version = 1',
                '',
                '[[engines]]',
                'id = "commands"',
                'type = "gherkin"',
                'name = "commands"',
                'path = "unit/commands"',
                '',
                '[[engines.registry_loaders]]',
                '[engines.registry_loaders.layouts.models]',
                'base = "builtins:object"',
                'module_globs = ["academo.**.models", "bosque.**.models"]',
                '',
                '[engines.registry_loaders.layouts.modules]',
                'base = "builtins:object"',
                'module_globs = ["bosque.**.modules"]',
            ),
        ),
        encoding='utf-8',
    )

    manifest = load_cosecha_manifest(manifest_path)

    assert manifest is not None
    loader = manifest.engines[0].registry_loaders[0]
    assert tuple(layout.name for layout in loader.layouts) == (
        'models',
        'modules',
    )
    assert loader.layouts[0].module_globs == (
        'academo.**.models',
        'bosque.**.models',
    )
    assert loader.layouts[1].module_globs == ('bosque.**.modules',)


def test_explain_manifest_reports_active_profiles_and_resources(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / 'cosecha.toml'
    manifest_path.write_text(
        '\n'.join(
            (
                '[manifest]',
                'schema_version = 1',
                '',
                '[[engines]]',
                'id = "pytest"',
                'type = "pytest"',
                'name = "pytest"',
                'path = "tests"',
                'runtime_profile_ids = ["web"]',
                '',
                '[[runtime_profiles]]',
                'id = "web"',
                '',
                '[[runtime_profiles.services]]',
                'interface = "execution/engine"',
                'provider = "demo"',
                '',
                '[[resources]]',
                'name = "workspace"',
                'provider = "demo:provider"',
                'scope = "test"',
                'mode = "ephemeral"',
                '',
                '[[resource_bindings]]',
                'engine_type = "pytest"',
                'resource_name = "workspace"',
                'fixture_name = "cosecha_workspace"',
            ),
        ),
        encoding='utf-8',
    )
    manifest = load_cosecha_manifest(manifest_path)
    assert manifest is not None

    explanation = explain_cosecha_manifest(
        manifest,
        config=Config(root_path=tmp_path),
        requested_paths=('tests/test_demo.py',),
    )

    assert explanation.active_runtime_profile_ids == ('web',)
    assert explanation.active_resource_names == ('workspace',)
    assert explanation.active_engines[0].id == 'pytest'
