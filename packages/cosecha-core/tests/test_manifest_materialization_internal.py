from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from cosecha.core.manifest_materialization import (
    build_registry_layout_include_patterns,
    collect_engine_resource_names,
    group_registry_layouts_by_root_package,
    iter_manifest_hook_descriptors,
    materialize_gherkin_registry_loaders,
    materialize_runtime_components,
    materialize_runtime_profile_hook_specs,
    module_glob_to_path_patterns,
)
from cosecha.core.manifest_symbols import ManifestValidationError


class _DummyHook:
    def __init__(self, hook_id: str) -> None:
        self.id = hook_id
        self.configs: list[object] = []

    def set_config(self, config) -> None:
        self.configs.append(config)


class _DummyHookDescriptor:
    def __init__(self, suffix: str) -> None:
        self.suffix = suffix

    def build_runtime_profile_hook_specs(self, profile, *, engine_ids):
        if self.suffix == 'none':
            return []
        return [
            SimpleNamespace(
                id=f'{profile.id}:{self.suffix}',
                type=f'hook/{self.suffix}',
                engine_ids=engine_ids,
            ),
        ]

    def materialize(self, hook_spec, *, manifest_dir):
        del hook_spec, manifest_dir
        return _DummyHook(self.suffix)


class _DummyEngineDescriptor:
    def materialize(
        self,
        engine_spec,
        *,
        manifest,
        config,
        active_profiles,
        shared_requirements,
    ):
        del manifest, config
        engine = SimpleNamespace(
            id=engine_spec.id,
            path=engine_spec.path,
            active_profiles=tuple(active_profiles),
            shared_requirements=tuple(shared_requirements),
        )
        return engine


@dataclass(slots=True, frozen=True)
class _LayoutBase:
    module: str
    qualname: str


@dataclass(slots=True, frozen=True)
class _LayoutSpec:
    name: str
    base: _LayoutBase
    module_globs: tuple[str, ...]
    match: str | None = None


def test_materialize_runtime_profile_hook_specs_and_runtime_components(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = SimpleNamespace(
        id='profile-a',
        services=(
            SimpleNamespace(
                interface='storage/mongo',
                provider='mongodb',
                mode='live',
                canonical_binding_name='runtime.storage.mongo',
                capabilities=('query',),
                degraded_capabilities=('query',),
            ),
        ),
    )
    manifest = SimpleNamespace(
        manifest_dir='.',
        resources=(
            SimpleNamespace(
                name='mongo',
                build_requirement=lambda root_path: (
                    f'mongo@{root_path}'
                ),
            ),
        ),
        resource_bindings=(
            SimpleNamespace(engine_type='python', resource_name='mongo'),
        ),
        runtime_profiles=(
            profile,
        ),
        engines=(
            SimpleNamespace(id='engine-a', runtime_profile_ids=('profile-a',)),
        ),
        find_runtime_profile=lambda profile_id: (
            profile if profile_id == 'profile-a' else None
        ),
    )
    engine_spec = SimpleNamespace(
        id='engine-a',
        type='python',
        path='engine.py',
        runtime_profile_ids=('profile-a',),
    )
    config = SimpleNamespace(name='cfg')

    monkeypatch.setattr(
        'cosecha.core.manifest_materialization.iter_hook_descriptors',
        lambda: (_DummyHookDescriptor('main'),),
    )
    hook_specs = materialize_runtime_profile_hook_specs(manifest)
    assert hook_specs and hook_specs[0].id.startswith('profile-a')

    hooks, engines = materialize_runtime_components(
        manifest,
        config=config,
        selected_engine_names=None,
        requested_paths=(),
        select_engine_specs=lambda *args, **kwargs: (engine_spec,),
        resolve_hook_descriptor=lambda _type: _DummyHookDescriptor('main'),
        resolve_engine_descriptor=lambda _type: _DummyEngineDescriptor(),
    )
    assert hooks
    assert hooks[0].configs == [config, config]
    assert 'engine.py' in engines
    assert engines['engine.py'].shared_requirements == ('mongo@.',)

    class _FlakyEngineIds:
        def __init__(self) -> None:
            self._calls = 0

        def __contains__(self, item: object) -> bool:
            del item
            self._calls += 1
            return self._calls == 1

    bad_hook = SimpleNamespace(
        id='bad-hook',
        type='hook/bad',
        engine_ids=_FlakyEngineIds(),
    )
    monkeypatch.setattr(
        'cosecha.core.manifest_materialization.materialize_runtime_profile_hook_specs',
        lambda _manifest: (bad_hook,),
    )
    with pytest.raises(ManifestValidationError, match='not attached to engine'):
        materialize_runtime_components(
            manifest,
            config=config,
            selected_engine_names=None,
            requested_paths=(),
            select_engine_specs=lambda *args, **kwargs: (engine_spec,),
            resolve_hook_descriptor=lambda _type: _DummyHookDescriptor('main'),
            resolve_engine_descriptor=lambda _type: _DummyEngineDescriptor(),
        )


def test_runtime_profile_hook_specs_errors_and_hook_descriptor_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = SimpleNamespace(
        runtime_profiles=(
            SimpleNamespace(
                id='profile-a',
                services=(SimpleNamespace(provider='unsupported'),),
            ),
        ),
        engines=(),
    )
    monkeypatch.setattr(
        'cosecha.core.manifest_materialization.iter_hook_descriptors',
        lambda: (_DummyHookDescriptor('none'),),
    )
    with pytest.raises(ManifestValidationError, match='Unsupported runtime profile'):
        materialize_runtime_profile_hook_specs(manifest)

    hook_descriptors = iter_manifest_hook_descriptors(
        (
            SimpleNamespace(type='hook/a'),
            SimpleNamespace(type='hook/a'),
            SimpleNamespace(type='hook/b'),
        ),
        resolve_hook_descriptor=lambda hook_type: f'descriptor::{hook_type}',
    )
    assert sorted(hook_descriptors) == [
        'descriptor::hook/a',
        'descriptor::hook/b',
    ]


def test_resource_name_collection_layout_grouping_and_loader_materialization(
) -> None:
    bindings = (
        SimpleNamespace(engine_type='python', resource_name='mongo'),
        SimpleNamespace(engine_type='python', resource_name='mongo'),
        SimpleNamespace(engine_type='python', resource_name='redis'),
        SimpleNamespace(engine_type='gherkin', resource_name='ignored'),
    )
    assert collect_engine_resource_names(bindings, engine_type='python') == (
        'mongo',
        'redis',
    )

    layout_specs = (
        _LayoutSpec(
            name='steps',
            base=_LayoutBase(module='pkg.steps', qualname='Steps'),
            module_globs=('pkg.steps.*', 'pkg.steps.extra.**'),
        ),
        _LayoutSpec(
            name='hooks',
            base=_LayoutBase(module='pkg.hooks', qualname='Hooks'),
            module_globs=('pkg.hooks',),
        ),
    )
    grouped = group_registry_layouts_by_root_package(layout_specs)
    assert grouped
    assert grouped[0][0] == 'pkg'
    include_patterns = build_registry_layout_include_patterns(
        grouped[0][1][0],
        module_spec='pkg',
    )
    assert include_patterns

    assert module_glob_to_path_patterns('pkg', module_spec='pkg') == (
        '__init__.py',
    )
    assert module_glob_to_path_patterns('pkg.mod.*', module_spec='pkg') == (
        'mod/*.py',
        'mod/*/__init__.py',
    )
    with pytest.raises(ManifestValidationError, match='does not belong'):
        module_glob_to_path_patterns('other.mod', module_spec='pkg')

    class _RegistryLoader:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    loaders = materialize_gherkin_registry_loaders(
        (SimpleNamespace(layouts=layout_specs),),
        registry_loader_cls=_RegistryLoader,
    )
    assert loaders
    assert loaders[0].kwargs['module_spec'] == 'pkg'
