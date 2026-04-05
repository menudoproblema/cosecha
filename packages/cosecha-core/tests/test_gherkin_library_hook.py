from __future__ import annotations

import asyncio

from types import SimpleNamespace

from cosecha.core.manifest_symbols import SymbolRef
from cosecha.core.manifest_types import RegistryLayoutSpec
from cosecha.engine.gherkin.context import ContextRegistry
from cosecha.engine.gherkin.hooks import (
    GherkinLibraryHook,
    GherkinRegistryLoader,
)


def test_gherkin_library_hook_loads_step_library_modules(
    monkeypatch,
) -> None:
    loaded: list[tuple[str, object]] = []

    def fake_import_and_load_steps_from_module(
        module_spec_or_path,
        step_registry,
        function_names=None,
    ) -> None:
        del function_names
        loaded.append((module_spec_or_path, step_registry))

    monkeypatch.setattr(
        'cosecha.engine.gherkin.hooks.import_and_load_steps_from_module',
        fake_import_and_load_steps_from_module,
    )

    step_registry = object()
    hook = GherkinLibraryHook(
        step_library_modules=(
            'mochuelo_testing.cosecha.steps.commands',
            'mochuelo_testing.cosecha.steps.models',
        ),
    )

    asyncio.run(
        hook.before_collect(
            path=SimpleNamespace(),
            collector=SimpleNamespace(),
            engine=SimpleNamespace(step_registry=step_registry),
        ),
    )

    assert loaded == [
        ('mochuelo_testing.cosecha.steps.commands', step_registry),
        ('mochuelo_testing.cosecha.steps.models', step_registry),
    ]


def test_gherkin_library_hook_populates_context_registry_from_registry_loaders(
    tmp_path,
    monkeypatch,
) -> None:
    package_path = tmp_path / 'demo_registry_pkg'
    package_path.mkdir()
    (package_path / '__init__.py').write_text('', encoding='utf-8')
    (package_path / 'base.py').write_text(
        'class BaseItem:\n    pass\n',
        encoding='utf-8',
    )
    (package_path / 'helpers.py').write_text(
        '\n'.join(
            (
                'from .base import BaseItem',
                '',
                'class RootHelper(BaseItem):',
                '    pass',
            ),
        ),
        encoding='utf-8',
    )
    (package_path / 'submodule.py').write_text(
        '\n'.join(
            (
                'from .base import BaseItem',
                '',
                'class ChildHelper(BaseItem):',
                '    pass',
            ),
        ),
        encoding='utf-8',
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    hook = GherkinLibraryHook(
        registry_loaders=(
            GherkinRegistryLoader(
                layouts=(
                    RegistryLayoutSpec(
                        name='helper',
                        base=SymbolRef(
                            module='demo_registry_pkg.base',
                            qualname='BaseItem',
                        ),
                        module_globs=('demo_registry_pkg.**',),
                    ),
                ),
            ),
        ),
    )
    context_registry = ContextRegistry()
    engine = SimpleNamespace(
        config=SimpleNamespace(root_path=tmp_path),
        context_registry=context_registry,
    )

    asyncio.run(hook.before_session_start(engine))

    assert context_registry.get('helper', 'BaseItem') is not None
    assert context_registry.get('helper', 'RootHelper') is not None
    assert context_registry.get('helper', 'ChildHelper') is not None


def test_gherkin_registry_loader_accepts_plural_glob_for_singular_module_file(
    tmp_path,
    monkeypatch,
) -> None:
    package_path = tmp_path / 'demo_plural_pkg'
    package_path.mkdir()
    (package_path / '__init__.py').write_text('', encoding='utf-8')
    (package_path / 'base.py').write_text(
        'class BaseItem:\n    pass\n',
        encoding='utf-8',
    )
    feature_path = package_path / 'feature'
    feature_path.mkdir()
    (feature_path / '__init__.py').write_text('', encoding='utf-8')
    (feature_path / 'module.py').write_text(
        '\n'.join(
            (
                'from demo_plural_pkg.base import BaseItem',
                '',
                'class PlayersModule(BaseItem):',
                '    pass',
            ),
        ),
        encoding='utf-8',
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    loader = GherkinRegistryLoader(
        layouts=(
            RegistryLayoutSpec(
                name='modules',
                base=SymbolRef(
                    module='demo_plural_pkg.base',
                    qualname='BaseItem',
                ),
                module_globs=('demo_plural_pkg.**.modules',),
            ),
        ),
    )

    loaded = asyncio.run(loader.load(tmp_path))

    assert any(name == 'PlayersModule' for _layout, name, _item in loaded)
