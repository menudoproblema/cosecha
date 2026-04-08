from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.manifest_symbols import ManifestValidationError, SymbolRef
from cosecha.core.manifest_types import (
    CosechaManifest,
    EngineSpec,
    RegistryLayoutSpec,
    RegistryLoaderSpec,
    ResourceSpec,
)
from cosecha.core.manifest_validation import (
    raise_if_runtime_interface_is_invalid,
    validate_manifest,
    validate_registry_loader_patterns,
    validate_resource_readiness_policy,
    validate_runtime_profile_service_graph,
    validate_runtime_readiness_policy,
)
from cosecha.core.runtime_profiles import (
    RuntimeProfileSpec,
    RuntimeReadinessPolicy,
    RuntimeServiceSpec,
)


class _EngineDescriptor:
    @staticmethod
    def validate_resource_binding(*_args: object, **_kwargs: object) -> None:
        return None


def _resolve_engine_descriptor(_engine_type: str) -> type[_EngineDescriptor]:
    return _EngineDescriptor


def _base_engine(*, runtime_profile_ids: tuple[str, ...] = ()) -> EngineSpec:
    return EngineSpec(
        id='pytest',
        type='pytest',
        name='Pytest',
        path='tests',
        runtime_profile_ids=runtime_profile_ids,
    )


def test_validate_manifest_rejects_duplicate_runtime_profiles_and_resources() -> None:
    duplicated_profiles = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(_base_engine(),),
        runtime_profiles=(
            RuntimeProfileSpec(
                id='dup',
                services=(
                    RuntimeServiceSpec(interface='execution/engine', provider='demo'),
                ),
            ),
            RuntimeProfileSpec(
                id='dup',
                services=(
                    RuntimeServiceSpec(interface='storage/db', provider='demo'),
                ),
            ),
        ),
    )

    with pytest.raises(ManifestValidationError, match='Duplicated runtime profile ids'):
        validate_manifest(
            duplicated_profiles,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )

    duplicated_resources = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(_base_engine(),),
        resources=(ResourceSpec(name='db'), ResourceSpec(name='db')),
    )

    with pytest.raises(ManifestValidationError, match='Duplicated resource ids'):
        validate_manifest(
            duplicated_resources,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


@pytest.mark.parametrize(
    ('service_kwargs', 'error_message'),
    (
        ({'scope': 'invalid'}, 'Unsupported runtime service scope'),
        (
            {'supported_initialization_modes': ('data_seed', 'invalid')},
            'Unsupported initialization mode in runtime profile',
        ),
        (
            {'initialization_mode': 'invalid'},
            'Unsupported initialization_mode in runtime profile',
        ),
        (
            {'supported_worker_isolation_modes': ('invalid',)},
            'Unsupported worker isolation mode in runtime profile',
        ),
    ),
)
def test_validate_manifest_rejects_invalid_runtime_service_fields(
    service_kwargs: dict[str, object],
    error_message: str,
) -> None:
    service = RuntimeServiceSpec(
        interface='execution/engine',
        provider='demo',
        **service_kwargs,
    )
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(_base_engine(runtime_profile_ids=('web',)),),
        runtime_profiles=(RuntimeProfileSpec(id='web', services=(service,)),),
    )

    with pytest.raises(ManifestValidationError, match=error_message):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_rejects_duplicate_runtime_interface_inside_profile() -> None:
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(_base_engine(),),
        runtime_profiles=(
            RuntimeProfileSpec(
                id='web',
                services=(
                    RuntimeServiceSpec(interface='execution/engine', provider='demo'),
                    RuntimeServiceSpec(interface='execution/engine', provider='demo'),
                ),
            ),
        ),
    )

    with pytest.raises(ManifestValidationError, match='duplicates interface'):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_calls_engine_descriptor_validate_engine_spec() -> None:
    validate_calls: list[tuple[str, str]] = []

    class _DescriptorWithEngineValidation:
        @staticmethod
        def validate_resource_binding(*_args: object, **_kwargs: object) -> None:
            return None

        @staticmethod
        def validate_engine_spec(engine, *, manifest) -> None:
            validate_calls.append((engine.id, manifest.path))

    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(_base_engine(),),
    )

    validate_manifest(
        manifest,
        resolve_engine_descriptor=lambda _engine_type: _DescriptorWithEngineValidation,
        iter_hook_descriptors=lambda: (),
    )

    assert validate_calls == [('pytest', 'manifest.toml')]


@pytest.mark.parametrize(
    ('resource_kwargs', 'error_message'),
    (
        ({'scope': 'invalid'}, 'Unsupported resource scope'),
        ({'mode': 'invalid'}, 'Unsupported resource mode'),
        ({'initialization_mode': 'invalid'}, 'Unsupported initialization_mode'),
    ),
)
def test_validate_manifest_rejects_invalid_resource_fields(
    resource_kwargs: dict[str, object],
    error_message: str,
) -> None:
    manifest = CosechaManifest(
        path='manifest.toml',
        schema_version=1,
        engines=(_base_engine(),),
        resources=(ResourceSpec(name='db', **resource_kwargs),),
    )

    with pytest.raises(ManifestValidationError, match=error_message):
        validate_manifest(
            manifest,
            resolve_engine_descriptor=_resolve_engine_descriptor,
            iter_hook_descriptors=lambda: (),
        )


def test_validate_manifest_resolve_symbols_covers_coercions_layouts_and_resources() -> None:
    hook_descriptor_calls = 0

    def _iter_hook_descriptors() -> tuple[()]:
        nonlocal hook_descriptor_calls
        hook_descriptor_calls += 1
        return ()

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
                factory=SymbolRef(module='builtins', qualname='object'),
                coercions=(('normalize', SymbolRef(module='builtins', qualname='str')),),
                registry_loaders=(
                    RegistryLoaderSpec(
                        layouts=(
                            RegistryLayoutSpec(
                                name='models',
                                base=SymbolRef(module='builtins', qualname='object'),
                                module_globs=('demo.models',),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        runtime_profiles=(
            RuntimeProfileSpec(
                id='web',
                services=(
                    RuntimeServiceSpec(interface='execution/engine', provider='demo'),
                ),
            ),
        ),
        resources=(
            ResourceSpec(name='provider-db', provider=SymbolRef('builtins', 'object')),
            ResourceSpec(name='factory-db', factory=SymbolRef('builtins', 'object')),
        ),
    )

    validate_manifest(
        manifest,
        resolve_symbols=True,
        resolve_engine_descriptor=_resolve_engine_descriptor,
        iter_hook_descriptors=_iter_hook_descriptors,
    )

    assert hook_descriptor_calls == 1


def test_validate_runtime_profile_service_graph_additional_errors() -> None:
    shared_profile = RuntimeProfileSpec(
        id='web',
        worker_isolation='shared',
        services=(
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                supported_worker_isolation_modes=('strict',),
            ),
        ),
    )
    with pytest.raises(ManifestValidationError, match='do not support it'):
        validate_runtime_profile_service_graph(shared_profile)

    missing_dependency = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                depends_on=('storage/missing',),
            ),
        ),
    )
    with pytest.raises(ManifestValidationError, match='depends_on interface'):
        validate_runtime_profile_service_graph(missing_dependency)

    missing_initializer = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                initializes_from=('storage/missing',),
            ),
        ),
    )
    with pytest.raises(ManifestValidationError, match='initializes_from interface'):
        validate_runtime_profile_service_graph(missing_initializer)

    incompatible_initializer = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='storage/seed',
                provider='demo',
                supported_initialization_modes=('data_seed',),
            ),
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                initializes_from=('storage/seed',),
                initialization_mode='state_snapshot',
            ),
        ),
    )
    with pytest.raises(ManifestValidationError, match='only supports'):
        validate_runtime_profile_service_graph(incompatible_initializer)


def test_runtime_interface_and_readiness_policy_guards() -> None:
    with pytest.raises(ManifestValidationError, match='uses invalid interface'):
        raise_if_runtime_interface_is_invalid(
            profile_id='web',
            interface_name='execution/not-real',
        )

    negative_numeric_service = SimpleNamespace(
        interface='execution/engine',
        readiness_policy=RuntimeReadinessPolicy(initial_delay_seconds=-1),
    )
    with pytest.raises(ManifestValidationError, match='negative readiness_policy.initial_delay_seconds'):
        validate_runtime_readiness_policy(
            profile_id='web',
            service=negative_numeric_service,
        )

    negative_optional_service = SimpleNamespace(
        interface='execution/engine',
        readiness_policy=RuntimeReadinessPolicy(max_wait_seconds=-1),
    )
    with pytest.raises(ManifestValidationError, match='negative readiness_policy.max_wait_seconds'):
        validate_runtime_readiness_policy(
            profile_id='web',
            service=negative_optional_service,
        )

    negative_numeric_resource = SimpleNamespace(
        name='db',
        readiness_policy=RuntimeReadinessPolicy(initial_delay_seconds=-1),
    )
    with pytest.raises(ManifestValidationError, match="Resource 'db' declares negative readiness_policy.initial_delay_seconds"):
        validate_resource_readiness_policy(negative_numeric_resource)

    negative_optional_resource = SimpleNamespace(
        name='db',
        readiness_policy=RuntimeReadinessPolicy(max_wait_seconds=-1),
    )
    with pytest.raises(ManifestValidationError, match="Resource 'db' declares negative readiness_policy.max_wait_seconds"):
        validate_resource_readiness_policy(negative_optional_resource)


def test_validate_registry_loader_patterns_rejects_empty_module_globs() -> None:
    engine = SimpleNamespace(
        id='pytest',
        registry_loaders=(
            SimpleNamespace(
                layouts=(
                    SimpleNamespace(
                        name='models',
                        match='subclass',
                        module_globs=(),
                    ),
                ),
            ),
        ),
    )

    with pytest.raises(ManifestValidationError, match='require module_globs'):
        validate_registry_loader_patterns(engine)
