from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from cosecha.core.manifest_loader import parse_cosecha_manifest_text
from cosecha.core.manifest_symbols import ManifestValidationError, SymbolRef
from cosecha.core.manifest_types import (
    CosechaManifest,
    EngineSpec,
    RegistryLayoutSpec,
    RegistryLoaderSpec,
    ResourceBindingSpec,
    ResourceSpec,
)
from cosecha.core.manifest_validation import (
    extract_module_glob_root_package,
    validate_manifest,
    validate_registry_loader_patterns,
)
from cosecha.core.runtime_profiles import (
    RuntimeProfileSpec,
    RuntimeServiceSpec,
)


def _resolve_engine_descriptor(_engine_type: str) -> type[object]:
    class _EngineDescriptor:
        @staticmethod
        def validate_resource_binding(
            *_args: object, **_kwargs: object,
        ) -> None:
            return None

    return _EngineDescriptor


def test_validate_manifest_rejects_empty_manifest_engines() -> None:
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(),
    )

    with pytest.raises(
        ManifestValidationError,
        match='must declare at least one engine',
    ):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_rejects_unknown_runtime_profile_reference() -> None:
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(
            EngineSpec(
                id='pytest',
                type='pytest',
                name='Pytest',
                path='tests',
                runtime_profile_ids=('missing',),
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match=r'references unknown runtime profiles: missing',
    ):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_rejects_duplicate_engine_ids() -> None:
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(
            EngineSpec(id='dup', type='pytest', name='A', path='tests'),
            EngineSpec(id='dup', type='pytest', name='B', path='tests'),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match='Duplicated engine ids: dup',
    ):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_rejects_profile_with_empty_services() -> None:
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(
            EngineSpec(
                id='pytest', type='pytest', name='Pytest', path='tests',
            ),
        ),
        runtime_profiles=(RuntimeProfileSpec(id='web', services=()),),
    )

    with pytest.raises(
        ManifestValidationError,
        match='must declare at least one service',
    ):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_rejects_unsupported_worker_isolation() -> None:
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(
            EngineSpec(
                id='pytest', type='pytest', name='Pytest', path='tests',
            ),
        ),
        runtime_profiles=(
            RuntimeProfileSpec(
                id='web',
                worker_isolation='broken',
                services=(
                    RuntimeServiceSpec(
                        interface='execution/engine',
                        provider='demo',
                    ),
                ),
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match='Unsupported worker isolation for runtime profile',
    ):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_rejects_duplicate_profile_service_interface() -> (
    None
):
    manifest = CosechaManifest(
        path='manifest.toml',
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
        runtime_profiles=(
            RuntimeProfileSpec(
                id='web',
                services=(
                    RuntimeServiceSpec(
                        interface='execution/engine',
                        provider='demo',
                    ),
                    RuntimeServiceSpec(
                        interface='execution/engine',
                        provider='demo',
                    ),
                ),
            ),
        ),
        resources=(ResourceSpec(name='db'),),
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='db',
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match=r'duplicates interface|composes duplicate runtime interface',
    ):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_rejects_duplicate_profile_interfaces_via_engine_merge() -> (
    None
):
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(
            EngineSpec(
                id='pytest',
                type='pytest',
                name='Pytest',
                path='tests',
                runtime_profile_ids=('web-a', 'web-b'),
            ),
        ),
        runtime_profiles=(
            RuntimeProfileSpec(
                id='web-a',
                services=(
                    RuntimeServiceSpec(
                        interface='execution/engine',
                        provider='demo',
                    ),
                ),
            ),
            RuntimeProfileSpec(
                id='web-b',
                services=(
                    RuntimeServiceSpec(
                        interface='execution/engine',
                        provider='demo',
                    ),
                ),
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match='composes duplicate runtime interface',
    ):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_rejects_unknown_binding_resource_reference() -> (
    None
):
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(
            EngineSpec(
                id='pytest', type='pytest', name='Pytest', path='tests',
            ),
        ),
        runtime_profiles=(
            RuntimeProfileSpec(
                id='web',
                services=(
                    RuntimeServiceSpec(
                        interface='execution/engine',
                        provider='demo',
                    ),
                ),
            ),
        ),
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='unknown-resource',
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match='Resource binding references unknown resource',
    ):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_accepts_valid_manifest_and_resolves_symbols() -> (
    None
):
    engine_calls = 0
    hook_calls = 0

    class _EngineDescriptor:
        @staticmethod
        def validate_resource_binding(
            *_args: object, **_kwargs: object,
        ) -> None:
            return None

    def _resolve_engine_descriptor_with_counter(_engine_type: str) -> Any:
        nonlocal engine_calls
        engine_calls += 1
        return _EngineDescriptor

    def _iter_hook_descriptors() -> tuple[object]:
        nonlocal hook_calls
        hook_calls += 1
        return ()

    manifest = CosechaManifest(
        path='/tmp/manifest.toml',
        schema_version=1,
        engines=(
            EngineSpec(
                id='pytest',
                type='pytest',
                name='Pytest',
                path='tests',
                runtime_profile_ids=('web',),
                factory=SymbolRef(module='builtins', qualname='object'),
            ),
        ),
        runtime_profiles=(
            RuntimeProfileSpec(
                id='web',
                services=(
                    RuntimeServiceSpec(
                        interface='execution/engine',
                        provider='demo',
                    ),
                ),
            ),
        ),
        resources=(ResourceSpec(name='db'),),
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='db',
            ),
        ),
    )

    validate_manifest(
        manifest,
        resolve_symbols=True,
        resolve_engine_descriptor=_resolve_engine_descriptor_with_counter,
        iter_hook_descriptors=_iter_hook_descriptors,
    )

    assert engine_calls == 2
    assert hook_calls == 1


def test_validate_registry_loader_patterns_checks_supported_match() -> None:
    engine = SimpleNamespace(
        id='pytest',
        registry_loaders=(
            RegistryLoaderSpec(
                layouts=(
                    RegistryLayoutSpec(
                        name='models',
                        base=SymbolRef(module='builtins', qualname='object'),
                        module_globs=('demo.**',),
                        match='invalid',
                    ),
                ),
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match='Unsupported registry layout match mode',
    ):
        validate_registry_loader_patterns(engine)


def test_validate_manifest_extract_root_package_guard() -> None:
    assert extract_module_glob_root_package('demo.pkg') == 'demo'

    with pytest.raises(
        ManifestValidationError,
        match='module_globs must start with a literal root package',
    ):
        extract_module_glob_root_package('*.models')


def test_parse_manifest_with_custom_parser_accepts_minimal_valid_inputs(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'tmp-valid-manifest.toml'
    path.write_text(
        '\n'.join(
            (
                '[manifest]',
                'schema_version = 1',
                '',
                '[[engines]]',
                'id = "pytest"',
                'type = "pytest"',
                'name = "Pytest"',
                'path = "tests"',
            ),
        ),
        encoding='utf-8',
    )

    parsed = parse_cosecha_manifest_text(
        path.read_text(encoding='utf-8'),
        manifest_path=path,
        schema_version=1,
        iter_hook_descriptors=lambda: (),
        resolve_engine_descriptor=_resolve_engine_descriptor,
    )

    assert parsed.path == str(path.resolve())
    assert parsed.engines[0].id == 'pytest'
