from __future__ import annotations

import asyncio

from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from cosecha.core.manifest_symbols import SymbolRef
from cosecha.core.manifest_types import RegistryLayoutSpec
from cosecha.engine.gherkin.engine import GherkinEngine
from cosecha.engine.gherkin.hooks import (
    GherkinLibraryHook,
    GherkinRegistryLoader,
    _iter_compatible_module_globs,
    _iter_module_names,
    _load_registry_entries_sync,
    _matches_layout_item,
    _registry_loader_cache,
)
from cosecha_internal.testkit import DummyReporter, build_config


if TYPE_CHECKING:
    from pathlib import Path


def _build_layout(module_spec: str) -> RegistryLayoutSpec:
    return RegistryLayoutSpec(
        name='helper',
        base=SymbolRef.parse(f'{module_spec}.base:BaseItem'),
        module_globs=(f'{module_spec}.**',),
        match='subclass',
    )


def test_registry_loader_discovers_items_and_reuses_cache(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module_spec = 'demo_pkg_cache'
    package_path = tmp_path / module_spec
    package_path.mkdir()
    (package_path / '__init__.py').write_text('', encoding='utf-8')
    (package_path / 'base.py').write_text(
        'class BaseItem:\n    pass\n',
        encoding='utf-8',
    )
    (package_path / 'submodule.py').write_text(
        '\n'.join(
            (
                'from .base import BaseItem',
                '',
                'class ChildItem(BaseItem):',
                '    pass',
            ),
        ),
        encoding='utf-8',
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    _registry_loader_cache.clear()

    loader = GherkinRegistryLoader((_build_layout(module_spec),))
    first_items = asyncio.run(loader.load(tmp_path))

    assert {(layout, name) for layout, name, _item in first_items} == {
        ('helper', 'BaseItem'),
        ('helper', 'ChildItem'),
    }

    monkeypatch.setattr(
        'cosecha.engine.gherkin.hooks._load_registry_entries_sync',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError('cache should avoid rescanning package'),
        ),
    )

    second_items = asyncio.run(loader.load(tmp_path))

    assert {(layout, name) for layout, name, _item in second_items} == {
        ('helper', 'BaseItem'),
        ('helper', 'ChildItem'),
    }


def test_library_hook_loads_step_library_modules_before_collect(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module_path = tmp_path / 'demo_steps.py'
    module_path.write_text(
        '\n'.join(
            (
                'from cosecha.engine.gherkin.steps import given',
                '',
                '@given("the user is authenticated")',
                'async def user_is_authenticated(context):',
                '    del context',
            ),
        ),
        encoding='utf-8',
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    engine = GherkinEngine(
        'gherkin',
        reporter=DummyReporter(),
        hooks=(GherkinLibraryHook(step_library_modules=('demo_steps',)),),
    )
    engine.initialize(build_config(tmp_path), '')
    hook = engine.hooks[0]

    asyncio.run(hook.before_collect(tmp_path, engine.collector, engine))

    assert (
        engine.step_registry.find_match('given', 'the user is authenticated')
        is not None
    )


def test_library_hook_populates_context_registry_from_registry_loader(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module_spec = 'demo_pkg_registry'
    package_path = tmp_path / module_spec
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
    monkeypatch.syspath_prepend(str(tmp_path))

    hook = GherkinLibraryHook(
        registry_loaders=(GherkinRegistryLoader((_build_layout(module_spec),)),),
    )
    engine = GherkinEngine(
        'gherkin',
        reporter=DummyReporter(),
        hooks=(hook,),
    )
    engine.initialize(build_config(tmp_path), '')

    asyncio.run(hook.before_session_start(engine))

    loaded_item = engine.context_registry.get('helper', 'RootHelper')
    assert loaded_item is not None
    assert loaded_item.__name__ == 'RootHelper'


def test_hooks_helpers_and_guard_branches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    assert _iter_compatible_module_globs('demo.items') == (
        'demo.items',
        'demo.item',
    )
    assert _iter_compatible_module_globs('demo.one') == ('demo.one',)

    class _Base:
        pass

    class _Child(_Base):
        pass

    assert _matches_layout_item(_Base, base_class=_Base, match_mode='exact')
    assert not _matches_layout_item(_Child, base_class=_Base, match_mode='exact')

    fake_layout = SimpleNamespace(module_globs=('demo_pkg', 'demo_pkg.**'))
    fake_module = SimpleNamespace(__name__='demo_pkg', __path__=None)
    monkeypatch.setattr(
        'cosecha.engine.gherkin.hooks.importlib.import_module',
        lambda _name: fake_module,
    )
    module_names = _iter_module_names((fake_layout,))
    assert module_names == ('demo_pkg',)

    invalid_layout = SimpleNamespace(
        name='invalid',
        base=SimpleNamespace(resolve=lambda **_kwargs: 123),
        module_globs=('demo_pkg.**',),
        match='subclass',
    )
    with pytest.raises(
        TypeError,
        match='Registry layout base must resolve to a class',
    ):
        _load_registry_entries_sync((invalid_layout,), root_path=tmp_path)


def test_library_hook_before_session_start_short_circuits(
    tmp_path: Path,
) -> None:
    hook = GherkinLibraryHook(
        registry_entries=(('layout', 'Item', object()),),
    )
    engine = GherkinEngine(
        'gherkin',
        reporter=DummyReporter(),
        hooks=(hook,),
    )
    engine.initialize(build_config(tmp_path), '')

    original_entries = hook.registry_entries
    asyncio.run(hook.before_session_start(engine))
    assert hook.registry_entries is original_entries


def test_library_hook_before_session_start_skips_when_context_registry_missing(
    tmp_path: Path,
) -> None:
    hook = GherkinLibraryHook()
    engine = GherkinEngine(
        'gherkin',
        reporter=DummyReporter(),
        hooks=(hook,),
    )
    engine.initialize(build_config(tmp_path), '')
    engine.context_registry = None  # type: ignore[assignment]

    asyncio.run(hook.before_session_start(engine))
    assert hook.registry_entries == ()
