from __future__ import annotations

from argparse import ArgumentParser, Namespace
from types import SimpleNamespace

import pytest

import cosecha.core.cosecha_manifest as manifest_module
from cosecha.core.manifest_symbols import ManifestValidationError, SymbolRef
from cosecha.core.manifest_types import CosechaManifest, EngineSpec, HookSpec
from cosecha.core.runtime_profiles import RuntimeProfileSpec, RuntimeServiceSpec


def _minimal_manifest(*, profile: RuntimeProfileSpec | None = None) -> CosechaManifest:
    profiles = () if profile is None else (profile,)
    runtime_profile_ids = () if profile is None else (profile.id,)
    return CosechaManifest(
        path='tests/cosecha.toml',
        schema_version=1,
        engines=(
            EngineSpec(
                id='pytest',
                type='pytest',
                name='Pytest',
                path='tests',
                runtime_profile_ids=runtime_profile_ids,
            ),
        ),
        runtime_profiles=profiles,
    )


def test_python_hook_descriptor_paths(tmp_path) -> None:
    parser = ArgumentParser(add_help=False)
    manifest_module.PythonHookDescriptor.register_arguments(parser)

    hook_spec = HookSpec(id='hook-a', type='python')
    assert manifest_module.PythonHookDescriptor.apply_cli_overrides(
        hook_spec,
        Namespace(),
    ) == hook_spec

    with pytest.raises(ManifestValidationError, match='require config.factory'):
        manifest_module.PythonHookDescriptor.materialize(
            HookSpec(id='hook-a', type='python', config={'factory': 1}),
            manifest_dir=tmp_path,
        )

    support_path = tmp_path / 'hook_support.py'
    support_path.write_text(
        '\n'.join(
            (
                'not_callable = 1',
                '',
                'def build_hook():',
                '    return {"kind": "hook"}',
            ),
        ),
        encoding='utf-8',
    )

    with pytest.raises(ManifestValidationError, match='not callable'):
        manifest_module.PythonHookDescriptor.materialize(
            HookSpec(
                id='hook-a',
                type='python',
                config={'factory': 'hook_support.py:not_callable'},
            ),
            manifest_dir=tmp_path,
        )

    hook = manifest_module.PythonHookDescriptor.materialize(
        HookSpec(
            id='hook-a',
            type='python',
            config={'factory': 'hook_support.py:build_hook'},
        ),
        manifest_dir=tmp_path,
    )
    assert hook == {'kind': 'hook'}

    assert manifest_module.PythonHookDescriptor.build_runtime_profile_hook_specs(
        RuntimeProfileSpec(
            id='web',
            services=(
                RuntimeServiceSpec(interface='execution/engine', provider='demo'),
            ),
        ),
        engine_ids=('pytest',),
    ) == ()


def test_python_engine_descriptor_paths(tmp_path) -> None:
    with pytest.raises(ManifestValidationError, match='does not support declarative resource bindings'):
        manifest_module.PythonEngineDescriptor.validate_resource_binding(
            SimpleNamespace(resource_name='db'),
            manifest=_minimal_manifest(),
        )

    with pytest.raises(ManifestValidationError, match='require factory symbol ref'):
        manifest_module.PythonEngineDescriptor.materialize(
            EngineSpec(
                id='python-engine',
                type='python',
                name='Python',
                path='tests',
            ),
            manifest=SimpleNamespace(manifest_dir=tmp_path),
            config=SimpleNamespace(),
            active_profiles=(),
            shared_requirements=(),
        )

    support_path = tmp_path / 'engine_support.py'
    support_path.write_text(
        '\n'.join(
            (
                'not_callable = 1',
                '',
                'def build_engine():',
                '    return {"kind": "engine"}',
            ),
        ),
        encoding='utf-8',
    )

    with pytest.raises(ManifestValidationError, match='Engine factory is not callable'):
        manifest_module.PythonEngineDescriptor.materialize(
            EngineSpec(
                id='python-engine',
                type='python',
                name='Python',
                path='tests',
                factory=SymbolRef(module='engine_support.py', qualname='not_callable'),
            ),
            manifest=SimpleNamespace(manifest_dir=tmp_path),
            config=SimpleNamespace(),
            active_profiles=(),
            shared_requirements=(),
        )

    engine = manifest_module.PythonEngineDescriptor.materialize(
        EngineSpec(
            id='python-engine',
            type='python',
            name='Python',
            path='tests',
            factory=SymbolRef(module='engine_support.py', qualname='build_engine'),
        ),
        manifest=SimpleNamespace(manifest_dir=tmp_path),
        config=SimpleNamespace(),
        active_profiles=(),
        shared_requirements=(),
    )
    assert engine == {'kind': 'engine'}


def test_manifest_helpers_and_resolvers(monkeypatch: pytest.MonkeyPatch) -> None:
    original_resolve_hook_descriptor = manifest_module._resolve_hook_descriptor

    monkeypatch.setattr(
        manifest_module,
        'discover_cosecha_manifest',
        lambda manifest_file=None: None,
    )
    assert manifest_module.load_cosecha_manifest() is None

    monkeypatch.setattr(
        manifest_module._manifest_materialization,
        'materialize_runtime_profile_hook_specs',
        lambda _manifest: ('hook-spec',),
    )
    assert manifest_module._materialize_runtime_profile_hook_specs(_minimal_manifest()) == (
        'hook-spec',
    )

    monkeypatch.setattr(
        manifest_module,
        'iter_manifest_hook_descriptors',
        lambda hooks, resolve_hook_descriptor: ('descriptor',),
    )
    assert manifest_module._iter_hook_descriptors((HookSpec(id='h', type='x'),)) == (
        'descriptor',
    )

    class _ArgumentDescriptor:
        @classmethod
        def register_arguments(cls, parser: ArgumentParser) -> None:
            parser.add_argument('--runtime-flag')

    monkeypatch.setattr(
        manifest_module,
        '_materialize_runtime_profile_hook_specs',
        lambda _manifest: (HookSpec(id='hook-a', type='custom'),),
    )
    monkeypatch.setattr(
        manifest_module,
        '_iter_hook_descriptors',
        lambda hooks: (_ArgumentDescriptor,),
    )

    parser = ArgumentParser(add_help=False)
    manifest_module.register_manifest_hook_arguments(
        parser,
        _minimal_manifest(),
    )
    assert parser.parse_known_args(['--runtime-flag', 'on'])[1] == []

    profile = RuntimeProfileSpec(
        id='web',
        services=(RuntimeServiceSpec(interface='execution/engine', provider='demo'),),
    )
    manifest = _minimal_manifest(profile=profile)

    class _InvalidProfileDescriptor:
        @classmethod
        def apply_cli_overrides(cls, spec: HookSpec, args: Namespace) -> HookSpec:
            del args
            return HookSpec(id=spec.id, type=spec.type, config={})

    monkeypatch.setattr(
        manifest_module,
        '_materialize_runtime_profile_hook_specs',
        lambda _manifest: (HookSpec(id='hook-a', type='custom'),),
    )
    monkeypatch.setattr(
        manifest_module,
        '_resolve_hook_descriptor',
        lambda _hook_type: _InvalidProfileDescriptor,
    )

    with pytest.raises(ManifestValidationError, match='must preserve config.profile'):
        manifest_module.apply_manifest_cli_overrides(manifest, Namespace())

    new_profile = RuntimeProfileSpec(
        id='web',
        services=(RuntimeServiceSpec(interface='execution/worker', provider='demo'),),
    )

    class _ValidProfileDescriptor:
        @classmethod
        def apply_cli_overrides(cls, spec: HookSpec, args: Namespace) -> HookSpec:
            del args
            return HookSpec(
                id=spec.id,
                type=spec.type,
                config={'profile': new_profile.to_dict()},
            )

    monkeypatch.setattr(
        manifest_module,
        '_resolve_hook_descriptor',
        lambda _hook_type: _ValidProfileDescriptor,
    )
    overridden = manifest_module.apply_manifest_cli_overrides(manifest, Namespace())
    assert overridden.runtime_profiles[0].services[0].interface == 'execution/worker'
    monkeypatch.setattr(
        manifest_module,
        '_resolve_hook_descriptor',
        original_resolve_hook_descriptor,
    )

    monkeypatch.setattr(
        manifest_module,
        '_validate_manifest',
        lambda *args, **kwargs: (_ for _ in ()).throw(ManifestValidationError('bad manifest')),
    )
    assert manifest_module.validate_cosecha_manifest(manifest) == ('bad manifest',)

    monkeypatch.setattr(manifest_module, '_validate_manifest', lambda *args, **kwargs: None)
    assert manifest_module.validate_cosecha_manifest(manifest) == ()

    custom_hook_descriptor = object()
    monkeypatch.setattr(
        manifest_module,
        'get_hook_descriptor',
        lambda hook_type: custom_hook_descriptor if hook_type == 'custom' else None,
    )
    assert manifest_module._resolve_hook_descriptor('custom') is custom_hook_descriptor
    assert (
        manifest_module._resolve_hook_descriptor('python')
        is manifest_module.PythonHookDescriptor
    )
    with pytest.raises(ManifestValidationError, match='Unsupported hook type'):
        manifest_module._resolve_hook_descriptor('missing')

    custom_engine_descriptor = object()
    monkeypatch.setattr(
        manifest_module,
        'get_engine_descriptor',
        lambda engine_type: (
            custom_engine_descriptor if engine_type == 'custom-engine' else None
        ),
    )
    assert (
        manifest_module._resolve_engine_descriptor('custom-engine')
        is custom_engine_descriptor
    )
    assert (
        manifest_module._resolve_engine_descriptor('python')
        is manifest_module.PythonEngineDescriptor
    )
    with pytest.raises(ManifestValidationError, match='Unsupported engine type'):
        manifest_module._resolve_engine_descriptor('missing-engine')
