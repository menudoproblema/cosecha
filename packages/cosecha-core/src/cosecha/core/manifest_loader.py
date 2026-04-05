from __future__ import annotations

import tomllib

from typing import cast

from cosecha.core.manifest_symbols import ManifestValidationError, SymbolRef
from cosecha.core.manifest_types import (
    CosechaManifest,
    EngineSpec,
    RegistryLayoutSpec,
    RegistryLoaderSpec,
    ResourceBindingSpec,
    ResourceSpec,
)
from cosecha.core.manifest_validation import validate_manifest
from cosecha.core.runtime_interop import build_runtime_canonical_binding_name
from cosecha.core.runtime_profiles import (
    RuntimeProfileSpec,
    RuntimeReadinessPolicy,
    RuntimeServiceSpec,
)


def parse_cosecha_manifest_text(  # noqa: PLR0913
    source_content: str,
    *,
    manifest_path,
    schema_version: int,
    resolve_symbols: bool = False,
    iter_hook_descriptors,
    resolve_engine_descriptor,
) -> CosechaManifest:
    payload = tomllib.loads(source_content)
    if 'hooks' in payload:
        msg = (
            'Legacy [[hooks]] are no longer supported; use '
            '[[runtime_profiles]]'
        )
        raise ManifestValidationError(msg)

    manifest_payload = payload.get('manifest', {})
    loaded_schema_version = manifest_payload.get('schema_version')
    if loaded_schema_version != schema_version:
        msg = (
            'Unsupported cosecha.toml schema version: '
            f'{loaded_schema_version!r}'
        )
        raise ManifestValidationError(msg)

    engines = tuple(
        _parse_engine_spec(engine_payload)
        for engine_payload in payload.get('engines', ())
    )
    runtime_profiles = tuple(
        _parse_runtime_profile_spec(profile_payload)
        for profile_payload in payload.get('runtime_profiles', ())
    )
    resources = tuple(
        _parse_resource_spec(resource_payload)
        for resource_payload in payload.get('resources', ())
    )
    resource_bindings = tuple(
        _parse_resource_binding_spec(binding_payload)
        for binding_payload in payload.get('resource_bindings', ())
    )
    manifest = CosechaManifest(
        path=str(manifest_path.resolve()),
        schema_version=loaded_schema_version,
        engines=engines,
        runtime_profiles=runtime_profiles,
        resources=resources,
        resource_bindings=resource_bindings,
    )
    validate_manifest(
        manifest,
        resolve_symbols=resolve_symbols,
        iter_hook_descriptors=iter_hook_descriptors,
        resolve_engine_descriptor=resolve_engine_descriptor,
    )
    return manifest


def _parse_engine_spec(payload: dict[str, object]) -> EngineSpec:
    engine_id = _require_str(payload, 'id')
    engine_type = _require_str(payload, 'type')
    name = _require_str(payload, 'name')
    path = _require_engine_path(payload, 'path')
    if 'hook_ids' in payload:
        msg = (
            'Legacy engine hook_ids are no longer supported; use '
            'runtime_profile_ids'
        )
        raise ManifestValidationError(msg)
    runtime_profile_ids = _require_tuple_of_str(
        payload.get('runtime_profile_ids'),
    )
    definition_paths = _require_tuple_of_str(payload.get('definition_paths'))
    step_library_modules = _require_tuple_of_str(
        payload.get('step_library_modules'),
    )
    coercions = _parse_symbol_mapping(payload.get('coercions'))
    factory = _parse_optional_symbol_ref(payload.get('factory'))
    registry_loaders = _parse_registry_loader_specs(
        payload.get('registry_loaders'),
    )
    return EngineSpec(
        id=engine_id,
        type=engine_type,
        name=name,
        path=path,
        runtime_profile_ids=runtime_profile_ids,
        definition_paths=definition_paths,
        step_library_modules=step_library_modules,
        coercions=coercions,
        registry_loaders=registry_loaders,
        factory=factory,
    )


def _parse_runtime_profile_spec(
    payload: dict[str, object],
) -> RuntimeProfileSpec:
    services_payload = _require_tuple_of_dict(payload.get('services'))
    return RuntimeProfileSpec(
        id=_require_str(payload, 'id'),
        worker_isolation=str(payload.get('worker_isolation', 'strict')),
        services=tuple(
            _parse_runtime_service_spec(service_payload)
            for service_payload in services_payload
        ),
    )


def _parse_runtime_service_spec(
    payload: dict[str, object],
) -> RuntimeServiceSpec:
    known_keys = {
        'interface',
        'provider',
        'canonical_binding_name',
        'mode',
        'scope',
        'depends_on',
        'initializes_from',
        'initialization_mode',
        'supported_initialization_modes',
        'public_bindings',
        'supports_worker_rebind',
        'supports_orphan_cleanup',
        'readiness_policy',
        'supported_worker_isolation_modes',
        'capabilities',
        'degraded_capabilities',
        'telemetry_capabilities',
    }
    config = {
        key: value for key, value in payload.items() if key not in known_keys
    }
    return RuntimeServiceSpec(
        interface=_require_str(payload, 'interface'),
        provider=_require_str(payload, 'provider'),
        canonical_binding_name=(
            _require_optional_str(payload.get('canonical_binding_name'))
            or build_runtime_canonical_binding_name(
                _require_str(payload, 'interface'),
            )
        ),
        mode=_require_optional_str(payload.get('mode')),
        scope=str(payload.get('scope', 'test')),
        depends_on=_require_tuple_of_str(payload.get('depends_on')),
        initializes_from=_require_tuple_of_str(
            payload.get('initializes_from'),
        ),
        initialization_mode=cast(
            'str',
            payload.get('initialization_mode', 'data_seed'),
        ),
        supported_initialization_modes=cast(
            'tuple[str, ...]',
            _require_tuple_of_str(
                payload.get('supported_initialization_modes'),
            ),
        ),
        public_bindings=_require_tuple_of_str(payload.get('public_bindings')),
        supports_worker_rebind=bool(
            payload.get('supports_worker_rebind', False),
        ),
        supports_orphan_cleanup=bool(
            payload.get('supports_orphan_cleanup', False),
        ),
        readiness_policy=_parse_runtime_readiness_policy(
            payload.get('readiness_policy'),
        ),
        supported_worker_isolation_modes=cast(
            'tuple[str, ...]',
            _require_tuple_of_str(
                payload.get('supported_worker_isolation_modes'),
            )
            or ('strict',),
        ),
        capabilities=_require_tuple_of_str(payload.get('capabilities')),
        degraded_capabilities=_require_tuple_of_str(
            payload.get('degraded_capabilities'),
        ),
        telemetry_capabilities=_require_tuple_of_str(
            payload.get('telemetry_capabilities'),
        ),
        config=config,
    )


def _parse_runtime_readiness_policy(
    value: object,
) -> RuntimeReadinessPolicy:
    if value is None:
        return RuntimeReadinessPolicy()

    readiness_policy = _require_dict(value)
    return RuntimeReadinessPolicy.from_dict(readiness_policy)


def _parse_resource_spec(payload: dict[str, object]) -> ResourceSpec:
    known_keys = {
        'name',
        'provider',
        'factory',
        'scope',
        'mode',
        'config',
        'depends_on',
        'initializes_from',
        'initialization_mode',
        'initialization_timeout_seconds',
        'readiness_policy',
        'conflicts_with',
        'requires_orphan_fencing',
    }
    config = _require_dict(payload.get('config'))
    extra_config = {
        key: value for key, value in payload.items() if key not in known_keys
    }
    return ResourceSpec(
        name=_require_str(payload, 'name'),
        provider=_parse_optional_symbol_ref(payload.get('provider')),
        factory=_parse_optional_symbol_ref(payload.get('factory')),
        scope=str(payload.get('scope', 'test')),
        mode=str(payload.get('mode', 'live')),
        config={**config, **extra_config},
        depends_on=_require_tuple_of_str(payload.get('depends_on')),
        initializes_from=_require_tuple_of_str(
            payload.get('initializes_from'),
        ),
        initialization_mode=cast(
            'str',
            payload.get('initialization_mode', 'data_seed'),
        ),
        initialization_timeout_seconds=_require_optional_float(
            payload.get('initialization_timeout_seconds'),
        ),
        readiness_policy=_parse_runtime_readiness_policy(
            payload.get('readiness_policy'),
        ),
        conflicts_with=_require_tuple_of_str(payload.get('conflicts_with')),
        requires_orphan_fencing=bool(
            payload.get('requires_orphan_fencing', False),
        ),
    )


def _parse_resource_binding_spec(
    payload: dict[str, object],
) -> ResourceBindingSpec:
    return ResourceBindingSpec(
        engine_type=_require_str(payload, 'engine_type'),
        resource_name=_require_str(payload, 'resource_name'),
        fixture_name=_require_optional_str(payload.get('fixture_name')),
        layout=_require_optional_str(payload.get('layout')),
        alias=_require_optional_str(payload.get('alias')),
    )


def _parse_symbol_mapping(
    raw_mapping: object,
) -> tuple[tuple[str, SymbolRef], ...]:
    mapping = _require_dict(raw_mapping)
    return tuple(
        sorted(
            (
                (name, SymbolRef.parse(symbol_ref))
                for name, symbol_ref in mapping.items()
                if isinstance(symbol_ref, str)
            ),
            key=lambda item: item[0],
        ),
    )


def _parse_registry_loader_specs(
    raw_loader_specs: object,
) -> tuple[RegistryLoaderSpec, ...]:
    return tuple(
        _parse_registry_loader_spec(loader_payload)
        for loader_payload in _require_tuple_of_dict(raw_loader_specs)
    )


def _parse_registry_loader_spec(
    payload: dict[str, object],
) -> RegistryLoaderSpec:
    raw_layouts = _require_dict(payload.get('layouts'))
    if not _registry_layouts_use_structured_mapping(raw_layouts):
        msg = (
            'Registry loader layouts must use structured layout tables with '
            'base and module_globs'
        )
        raise ManifestValidationError(msg)

    return RegistryLoaderSpec(
        layouts=_parse_structured_registry_layout_specs(raw_layouts),
    )


def _registry_layouts_use_structured_mapping(
    raw_layouts: dict[str, object],
) -> bool:
    return any(
        isinstance(layout_payload, dict)
        for layout_payload in raw_layouts.values()
    )


def _parse_structured_registry_layout_specs(
    raw_layouts: dict[str, object],
) -> tuple[RegistryLayoutSpec, ...]:
    layout_specs: list[RegistryLayoutSpec] = []
    for layout_name, layout_payload in raw_layouts.items():
        if not isinstance(layout_name, str) or not layout_name:
            msg = 'Expected non-empty layout name in registry loader'
            raise ManifestValidationError(msg)
        if not isinstance(layout_payload, dict):
            msg = (
                'Structured registry layouts require tables/dicts for each '
                f'layout, got {type(layout_payload).__name__} for '
                f'{layout_name!r}'
            )
            raise ManifestValidationError(msg)

        layout_specs.append(
            RegistryLayoutSpec(
                name=layout_name,
                base=SymbolRef.parse(_require_str(layout_payload, 'base')),
                module_globs=_require_tuple_of_str(
                    layout_payload.get('module_globs'),
                ),
                match=str(layout_payload.get('match', 'subclass')),
            ),
        )

    return tuple(sorted(layout_specs, key=lambda item: item.name))


def _parse_optional_symbol_ref(raw_value: object) -> SymbolRef | None:
    if raw_value is None:
        return None

    if not isinstance(raw_value, str):
        msg = f'Expected symbol ref string, got {type(raw_value).__name__}'
        raise ManifestValidationError(msg)

    return SymbolRef.parse(raw_value)


def _require_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        msg = f'Missing or invalid string field {key!r}'
        raise ManifestValidationError(msg)
    return value


def _require_engine_path(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        msg = f'Missing or invalid string field {key!r}'
        raise ManifestValidationError(msg)
    return value


def _require_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        msg = 'Expected optional string'
        raise ManifestValidationError(msg)
    return value


def _require_tuple_of_str(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        msg = 'Expected list of strings'
        raise ManifestValidationError(msg)
    if not all(isinstance(item, str) and item for item in value):
        msg = 'Expected non-empty strings'
        raise ManifestValidationError(msg)
    return tuple(value)


def _require_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)

    msg = 'Expected optional number'
    raise ManifestValidationError(msg)


def _require_dict(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        msg = f'Expected table/dict, got {type(value).__name__}'
        raise ManifestValidationError(msg)
    return value


def _require_tuple_of_dict(value: object) -> tuple[dict[str, object], ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        msg = 'Expected list of tables'
        raise ManifestValidationError(msg)
    if not all(isinstance(item, dict) for item in value):
        msg = 'Expected list of tables'
        raise ManifestValidationError(msg)
    return tuple(value)
