from __future__ import annotations

from pathlib import Path

import pytest

from cosecha.core.manifest_symbols import ManifestValidationError, SymbolRef
from cosecha.core.manifest_types import (
    CosechaManifest,
    EngineSpec,
    HookSpec,
    ManifestEngineDecision,
    ManifestEngineExplanation,
    ManifestMaterializationExplanation,
    ManifestResourceBindingDecision,
    ManifestResourceDecision,
    RegistryLayoutSpec,
    RegistryLoaderSpec,
    ResourceBindingSpec,
    ResourceSpec,
    RuntimeProfileDecision,
)
from cosecha.core.runtime_profiles import RuntimeProfileSpec, RuntimeServiceSpec


class _ResolvedRef:
    def __init__(self, value: object) -> None:
        self._value = value

    def resolve(self, *, root_path: Path) -> object:
        del root_path
        return self._value


def test_manifest_types_roundtrip_serialization() -> None:
    layout = RegistryLayoutSpec(
        name='models',
        base=SymbolRef(module='builtins', qualname='object'),
        module_globs=('demo.**.models',),
    )
    assert RegistryLayoutSpec.from_dict(layout.to_dict()) == layout

    loader = RegistryLoaderSpec(layouts=(layout,))
    assert RegistryLoaderSpec.from_dict(loader.to_dict()) == loader

    engine = EngineSpec(
        id='pytest',
        type='pytest',
        name='Pytest',
        path='tests',
        runtime_profile_ids=('web',),
        definition_paths=('tests/unit',),
        step_library_modules=('demo.steps',),
        coercions=(('ts', SymbolRef(module='builtins', qualname='str')),),
        registry_loaders=(loader,),
        factory=SymbolRef(module='builtins', qualname='object'),
    )
    assert EngineSpec.from_dict(engine.to_dict()) == engine

    hook = HookSpec(
        id='hook-a',
        type='python',
        engine_ids=('pytest',),
        config={'x': 1},
    )
    assert HookSpec.from_dict(hook.to_dict()) == hook

    binding = ResourceBindingSpec(
        engine_type='pytest',
        resource_name='db',
        fixture_name='db_fixture',
        layout='steps',
        alias='database',
    )
    assert ResourceBindingSpec.from_dict(binding.to_dict()) == binding


def test_resource_spec_build_requirement_provider_and_factory_paths() -> None:
    class _ProviderType:
        pass

    provider_from_type = ResourceSpec(name='from-type', provider=_ResolvedRef(_ProviderType))
    req_from_type = provider_from_type.build_requirement(root_path=Path('.'))
    assert isinstance(req_from_type.provider, _ProviderType)

    def provider_factory() -> object | None:
        return None

    provider_from_callable = ResourceSpec(
        name='from-callable',
        provider=_ResolvedRef(provider_factory),
    )
    req_from_callable = provider_from_callable.build_requirement(root_path=Path('.'))
    assert req_from_callable.provider is provider_factory

    provider_object = object()
    provider_from_object = ResourceSpec(
        name='from-object',
        provider=_ResolvedRef(provider_object),
    )
    req_from_object = provider_from_object.build_requirement(root_path=Path('.'))
    assert req_from_object.provider is provider_object

    with pytest.raises(ManifestValidationError, match='must be callable'):
        ResourceSpec(
            name='bad-factory',
            factory=_ResolvedRef(123),
        ).build_requirement(root_path=Path('.'))

    setup_callable = lambda: None
    from_factory = ResourceSpec(name='good-factory', factory=_ResolvedRef(setup_callable))
    req_from_factory = from_factory.build_requirement(root_path=Path('.'))
    assert req_from_factory.setup is setup_callable


def test_manifest_lookup_and_explanation_dict_serialization() -> None:
    profile = RuntimeProfileSpec(
        id='web',
        services=(RuntimeServiceSpec(interface='execution/engine', provider='demo'),),
    )
    resource = ResourceSpec(name='db')
    manifest = CosechaManifest(
        path='tests/cosecha.toml',
        schema_version=1,
        engines=(
            EngineSpec(
                id='pytest',
                type='pytest',
                name='Pytest',
                path='tests',
                runtime_profile_ids=('web',),
            ),
        ),
        runtime_profiles=(profile,),
        resources=(resource,),
        resource_bindings=(
            ResourceBindingSpec(engine_type='pytest', resource_name='db'),
        ),
    )

    assert manifest.to_dict()['path'] == 'tests/cosecha.toml'
    assert manifest.find_runtime_profile('web') is profile
    assert manifest.find_resource('db') is resource

    with pytest.raises(ManifestValidationError, match='Unknown runtime profile id'):
        manifest.find_runtime_profile('missing')

    with pytest.raises(ManifestValidationError, match='Unknown resource'):
        manifest.find_resource('missing')

    runtime_profile_decision = RuntimeProfileDecision(
        id='web',
        active=True,
        referenced_engine_ids=('pytest',),
        active_engine_ids=('pytest',),
    )
    assert runtime_profile_decision.to_dict()['id'] == 'web'

    engine_explanation = ManifestEngineExplanation(
        id='pytest',
        name='Pytest',
        type='pytest',
        path='tests',
    )
    assert engine_explanation.to_dict()['id'] == 'pytest'

    engine_decision = ManifestEngineDecision(
        id='pytest',
        name='Pytest',
        type='pytest',
        path='tests',
        active=True,
    )
    assert engine_decision.to_dict()['active'] is True

    binding_decision = ManifestResourceBindingDecision(
        engine_type='pytest',
        active=True,
        fixture_name='db',
    )
    assert binding_decision.to_dict()['fixture_name'] == 'db'

    resource_decision = ManifestResourceDecision(
        name='db',
        scope='test',
        mode='live',
        active=True,
    )
    assert resource_decision.to_dict()['name'] == 'db'

    explanation = ManifestMaterializationExplanation(
        manifest_path='tests/cosecha.toml',
        schema_version=1,
        root_path='.',
        selected_engine_names=('pytest',),
        requested_paths=('tests/test_a.py',),
        normalized_paths=('tests/test_a.py',),
        active_engines=(engine_explanation,),
        evaluated_engines=(engine_decision,),
        active_runtime_profile_ids=('web',),
        inactive_runtime_profile_ids=(),
        active_resource_names=('db',),
        inactive_resource_names=(),
        inactive_engine_ids=(),
        evaluated_runtime_profiles=(runtime_profile_decision,),
        evaluated_resources=(resource_decision,),
        workspace=None,
        execution_context=None,
    )
    serialized = explanation.to_dict()
    assert serialized['manifest_path'] == 'tests/cosecha.toml'
    assert serialized['active_resource_names'] == ('db',)
