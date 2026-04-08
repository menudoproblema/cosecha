from __future__ import annotations

from cosecha.core.manifest_symbols import ManifestValidationError
from cosecha.core.resources import validate_resource_requirements
from cosecha.core.runtime_interop import (
    build_runtime_capability_validation_messages,
    validate_runtime_interface_name,
)
from cosecha.core.runtime_profiles import (
    build_runtime_service_shadow_requirements,
    format_runtime_service_invariant_error,
)


def validate_manifest(  # noqa: PLR0912, PLR0915
    manifest,
    *,
    resolve_symbols: bool = False,
    iter_hook_descriptors,
    resolve_engine_descriptor,
) -> None:
    if not manifest.engines:
        msg = 'cosecha.toml must declare at least one engine'
        raise ManifestValidationError(msg)

    validate_unique_ids(
        [engine.id for engine in manifest.engines],
        kind='engine',
    )
    validate_unique_ids(
        [profile.id for profile in manifest.runtime_profiles],
        kind='runtime profile',
    )
    validate_unique_ids(
        [resource.name for resource in manifest.resources],
        kind='resource',
    )

    runtime_profile_ids = {profile.id for profile in manifest.runtime_profiles}
    resource_names = {resource.name for resource in manifest.resources}

    for engine in manifest.engines:
        missing_runtime_profile_ids = tuple(
            sorted(
                profile_id
                for profile_id in engine.runtime_profile_ids
                if profile_id not in runtime_profile_ids
            ),
        )
        if missing_runtime_profile_ids:
            msg = (
                f'Engine {engine.id!r} references unknown runtime profiles: '
                f'{", ".join(missing_runtime_profile_ids)}'
            )
            raise ManifestValidationError(msg)
        validate_registry_loader_patterns(engine)
        validate_engine_runtime_profile_interfaces(manifest, engine)
        engine_descriptor = resolve_engine_descriptor(engine.type)
        validate_engine_spec = getattr(
            engine_descriptor,
            'validate_engine_spec',
            None,
        )
        if callable(validate_engine_spec):
            validate_engine_spec(
                engine,
                manifest=manifest,
            )

    for profile in manifest.runtime_profiles:
        if profile.worker_isolation not in {'strict', 'shared'}:
            msg = (
                'Unsupported worker isolation for runtime profile '
                f'{profile.id!r}: {profile.worker_isolation!r}'
            )
            raise ManifestValidationError(msg)
        if not profile.services:
            msg = (
                f'Runtime profile {profile.id!r} must declare at least '
                'one service'
            )
            raise ManifestValidationError(msg)
        seen_interfaces: set[str] = set()
        for service in profile.services:
            if service.interface in seen_interfaces:
                msg = (
                    f'Runtime profile {profile.id!r} duplicates interface '
                    f'{service.interface!r}'
                )
                raise ManifestValidationError(msg)
            seen_interfaces.add(service.interface)
            if service.scope not in {'run', 'worker', 'test'}:
                msg = (
                    'Unsupported runtime service scope '
                    f'{service.scope!r} in profile {profile.id!r}'
                )
                raise ManifestValidationError(msg)
            if any(
                mode not in {'data_seed', 'state_snapshot'}
                for mode in service.supported_initialization_modes
            ):
                msg = (
                    'Unsupported initialization mode in runtime profile '
                    f'{profile.id!r} for interface {service.interface!r}'
                )
                raise ManifestValidationError(msg)
            if service.initialization_mode not in {
                'data_seed',
                'state_snapshot',
            }:
                msg = (
                    'Unsupported initialization_mode in runtime profile '
                    f'{profile.id!r} for interface {service.interface!r}'
                )
                raise ManifestValidationError(msg)
            if any(
                mode not in {'strict', 'shared'}
                for mode in service.supported_worker_isolation_modes
            ):
                msg = (
                    'Unsupported worker isolation mode in runtime profile '
                    f'{profile.id!r} for interface {service.interface!r}'
                )
                raise ManifestValidationError(msg)
            validate_runtime_readiness_policy(
                profile_id=profile.id,
                service=service,
            )
        validate_runtime_profile_service_graph(profile)

    for resource in manifest.resources:
        if resource.scope not in {'run', 'worker', 'test', 'session'}:
            msg = (
                f'Unsupported resource scope {resource.scope!r} '
                f'for resource {resource.name!r}'
            )
            raise ManifestValidationError(msg)
        if resource.mode not in {'live', 'ephemeral', 'dry_run'}:
            msg = (
                f'Unsupported resource mode {resource.mode!r} '
                f'for resource {resource.name!r}'
            )
            raise ManifestValidationError(msg)
        if resource.initialization_mode not in {
            'data_seed',
            'state_snapshot',
        }:
            msg = (
                'Unsupported initialization_mode '
                f'{resource.initialization_mode!r} for resource '
                f'{resource.name!r}'
            )
            raise ManifestValidationError(msg)
        validate_resource_readiness_policy(resource)

    for binding in manifest.resource_bindings:
        if binding.resource_name not in resource_names:
            msg = (
                'Resource binding references unknown resource: '
                f'{binding.resource_name!r}'
            )
            raise ManifestValidationError(msg)
        engine_descriptor = resolve_engine_descriptor(binding.engine_type)
        engine_descriptor.validate_resource_binding(
            binding,
            manifest=manifest,
        )

    if not resolve_symbols:
        return

    for engine in manifest.engines:
        if engine.factory is not None:
            engine.factory.resolve(root_path=manifest.manifest_dir)
        for _name, coercion_ref in engine.coercions:
            coercion_ref.resolve(root_path=manifest.manifest_dir)
        for loader in engine.registry_loaders:
            for layout_spec in loader.layouts:
                layout_spec.base.resolve(root_path=manifest.manifest_dir)

    if manifest.runtime_profiles:
        tuple(iter_hook_descriptors())

    for resource in manifest.resources:
        if resource.provider is not None:
            resource.provider.resolve(root_path=manifest.manifest_dir)
        if resource.factory is not None:
            resource.factory.resolve(root_path=manifest.manifest_dir)


def validate_engine_runtime_profile_interfaces(
    manifest,
    engine,
) -> None:
    profile_ids_by_interface: dict[str, str] = {}
    for profile_id in engine.runtime_profile_ids:
        profile = manifest.find_runtime_profile(profile_id)
        for service in profile.services:
            existing_profile_id = profile_ids_by_interface.get(
                service.interface,
            )
            if existing_profile_id is None:
                profile_ids_by_interface[service.interface] = profile.id
                continue

            msg = (
                f'Engine {engine.id!r} composes duplicate runtime interface '
                f'{service.interface!r} from profiles '
                f'{existing_profile_id!r} and {profile.id!r}'
            )
            raise ManifestValidationError(msg)


def validate_runtime_profile_service_graph(profile) -> None:
    services_by_interface = {
        service.interface: service for service in profile.services
    }

    if profile.worker_isolation == 'shared':
        unsupported_shared_interfaces = tuple(
            service.interface
            for service in profile.services
            if 'shared' not in service.supported_worker_isolation_modes
        )
        if unsupported_shared_interfaces:
            msg = (
                'Runtime profile '
                f'{profile.id!r} requests worker_isolation="shared" but '
                'these services do not support it: '
                f'{", ".join(unsupported_shared_interfaces)}'
            )
            raise ManifestValidationError(msg)

    for service in profile.services:
        raise_if_runtime_interface_is_invalid(
            profile_id=profile.id,
            interface_name=service.interface,
        )
        validate_runtime_service_capability_surface(
            profile_id=profile.id,
            service=service,
            capability_names=service.capabilities,
            surface_name='capabilities',
        )
        validate_runtime_service_capability_surface(
            profile_id=profile.id,
            service=service,
            capability_names=service.degraded_capabilities,
            surface_name='degraded_capabilities',
        )
        validate_runtime_service_capability_surface(
            profile_id=profile.id,
            service=service,
            capability_names=service.telemetry_capabilities,
            surface_name='telemetry_capabilities',
        )

        for dependency_interface in service.depends_on:
            raise_if_runtime_interface_is_invalid(
                profile_id=profile.id,
                interface_name=dependency_interface,
            )
            dependency = services_by_interface.get(dependency_interface)
            if dependency is None:
                msg = (
                    f'Runtime profile {profile.id!r} references unknown '
                    'depends_on interface '
                    f'{dependency_interface!r} for {service.interface!r}'
                )
                raise ManifestValidationError(msg)

        for initializer_interface in service.initializes_from:
            raise_if_runtime_interface_is_invalid(
                profile_id=profile.id,
                interface_name=initializer_interface,
            )
            initializer = services_by_interface.get(initializer_interface)
            if initializer is None:
                msg = (
                    f'Runtime profile {profile.id!r} references unknown '
                    'initializes_from interface '
                    f'{initializer_interface!r} for {service.interface!r}'
                )
                raise ManifestValidationError(msg)
            if (
                initializer.supported_initialization_modes
                and service.initialization_mode
                not in initializer.supported_initialization_modes
            ):
                msg = (
                    f'Runtime profile {profile.id!r} requires '
                    f'initialization_mode {service.initialization_mode!r} '
                    f'for {service.interface!r}, but '
                    f'{initializer.interface!r} only supports '
                    f'{", ".join(initializer.supported_initialization_modes)}'
                )
                raise ManifestValidationError(msg)
    try:
        validate_resource_requirements(
            build_runtime_service_shadow_requirements(profile),
        )
    except ValueError as error:
        msg = (
            f'Runtime profile {profile.id!r} declares invalid service '
            'graph: '
            f'{format_runtime_service_invariant_error(error)}'
        )
        raise ManifestValidationError(msg) from error


def validate_runtime_service_capability_surface(
    *,
    profile_id: str,
    service,
    capability_names: tuple[str, ...],
    surface_name: str,
) -> None:
    messages = build_runtime_capability_validation_messages(
        service.interface,
        capability_names,
    )
    if not messages:
        return

    msg = (
        f'Runtime profile {profile_id!r} declares invalid {surface_name} '
        f'for {service.interface!r}: {"; ".join(messages)}'
    )
    raise ManifestValidationError(msg)


def raise_if_runtime_interface_is_invalid(
    *,
    profile_id: str,
    interface_name: str,
) -> None:
    interface_error = validate_runtime_interface_name(interface_name)
    if interface_error is None:
        return

    msg = (
        f'Runtime profile {profile_id!r} uses invalid interface: '
        f'{interface_error}.'
    )
    raise ManifestValidationError(msg)

def validate_runtime_readiness_policy(
    *,
    profile_id: str,
    service,
) -> None:
    readiness_policy = service.readiness_policy
    numeric_fields = {
        'initial_delay_seconds': readiness_policy.initial_delay_seconds,
        'retry_interval_seconds': readiness_policy.retry_interval_seconds,
        'worker_lost_timeout_seconds': (
            readiness_policy.worker_lost_timeout_seconds
        ),
        'heartbeat_interval_seconds': (
            readiness_policy.heartbeat_interval_seconds
        ),
    }
    optional_numeric_fields = {
        'max_wait_seconds': readiness_policy.max_wait_seconds,
        'degraded_timeout_seconds': readiness_policy.degraded_timeout_seconds,
        'local_health_check_interval_seconds': (
            readiness_policy.local_health_check_interval_seconds
        ),
    }
    for field_name, value in numeric_fields.items():
        if value < 0:
            msg = (
                f'Runtime profile {profile_id!r} declares negative '
                f'readiness_policy.{field_name} for {service.interface!r}'
            )
            raise ManifestValidationError(msg)
    for field_name, value in optional_numeric_fields.items():
        if value is None:
            continue
        if value < 0:
            msg = (
                f'Runtime profile {profile_id!r} declares negative '
                f'readiness_policy.{field_name} for {service.interface!r}'
            )
            raise ManifestValidationError(msg)


def validate_resource_readiness_policy(resource) -> None:
    readiness_policy = resource.readiness_policy
    numeric_fields = {
        'initial_delay_seconds': readiness_policy.initial_delay_seconds,
        'retry_interval_seconds': readiness_policy.retry_interval_seconds,
        'worker_lost_timeout_seconds': (
            readiness_policy.worker_lost_timeout_seconds
        ),
        'heartbeat_interval_seconds': (
            readiness_policy.heartbeat_interval_seconds
        ),
    }
    optional_numeric_fields = {
        'max_wait_seconds': readiness_policy.max_wait_seconds,
        'degraded_timeout_seconds': readiness_policy.degraded_timeout_seconds,
        'local_health_check_interval_seconds': (
            readiness_policy.local_health_check_interval_seconds
        ),
    }
    for field_name, value in numeric_fields.items():
        if value < 0:
            msg = (
                f'Resource {resource.name!r} declares negative '
                f'readiness_policy.{field_name}'
            )
            raise ManifestValidationError(msg)
    for field_name, value in optional_numeric_fields.items():
        if value is None:
            continue
        if value < 0:
            msg = (
                f'Resource {resource.name!r} declares negative '
                f'readiness_policy.{field_name}'
            )
            raise ManifestValidationError(msg)


def validate_registry_loader_patterns(engine) -> None:
    for loader in engine.registry_loaders:
        for layout_spec in loader.layouts:
            if layout_spec.match != 'subclass':
                msg = (
                    'Unsupported registry layout match mode '
                    f'{layout_spec.match!r} in engine {engine.id!r} '
                    f'for layout {layout_spec.name!r}'
                )
                raise ManifestValidationError(msg)
            if not layout_spec.module_globs:
                msg = (
                    'Registry layout specs require module_globs for '
                    f'layout {layout_spec.name!r} '
                    f'in engine {engine.id!r}'
                )
                raise ManifestValidationError(msg)
            for module_glob in layout_spec.module_globs:
                extract_module_glob_root_package(module_glob)


def validate_unique_ids(
    ids: list[str],
    *,
    kind: str,
) -> None:
    duplicated_ids = sorted(
        {item_id for item_id in ids if ids.count(item_id) > 1},
    )
    if not duplicated_ids:
        return

    msg = f'Duplicated {kind} ids: {", ".join(duplicated_ids)}'
    raise ManifestValidationError(msg)


def extract_module_glob_root_package(module_glob: str) -> str:
    root_package, *_rest = module_glob.split('.')
    if not root_package or '*' in root_package:
        msg = (
            'module_globs must start with a literal root package, got '
            f'{module_glob!r}'
        )
        raise ManifestValidationError(msg)
    return root_package
