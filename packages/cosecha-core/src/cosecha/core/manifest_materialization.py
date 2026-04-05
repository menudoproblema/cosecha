from __future__ import annotations

from cosecha.core.discovery import iter_hook_descriptors
from cosecha.core.manifest_symbols import ManifestValidationError
from cosecha.core.manifest_validation import extract_module_glob_root_package
from cosecha.core.runtime_profiles import build_runtime_service_offerings


def materialize_runtime_components(  # noqa: PLR0913
    manifest,
    *,
    config,
    selected_engine_names: set[str] | None = None,
    requested_paths: tuple[str, ...] = (),
    select_engine_specs,
    resolve_hook_descriptor,
    resolve_engine_descriptor,
):
    active_specs = select_engine_specs(
        manifest,
        config=config,
        selected_engine_names=selected_engine_names,
        requested_paths=requested_paths,
    )
    runtime_profile_hook_specs = materialize_runtime_profile_hook_specs(
        manifest,
    )
    hooks_by_id = {hook.id: hook for hook in runtime_profile_hook_specs}

    materialized_hooks_by_id: dict[str, object] = {}
    hooks: list[object] = []
    engines: dict[str, object] = {}
    requirements_by_name = {
        resource.name: resource.build_requirement(
            root_path=manifest.manifest_dir,
        )
        for resource in manifest.resources
    }

    for engine_spec in active_specs:
        effective_hook_ids = tuple(
            hook.id
            for hook in runtime_profile_hook_specs
            if engine_spec.id in hook.engine_ids
        )
        active_profiles = tuple(
            manifest.find_runtime_profile(profile_id)
            for profile_id in engine_spec.runtime_profile_ids
        )
        for hook_id in effective_hook_ids:
            hook_spec = hooks_by_id[hook_id]
            if (
                hook_spec.engine_ids
                and engine_spec.id not in hook_spec.engine_ids
            ):
                msg = (
                    f'Hook {hook_id!r} is not attached to engine '
                    f'{engine_spec.id!r}'
                )
                raise ManifestValidationError(msg)

            if hook_id not in materialized_hooks_by_id:
                descriptor = resolve_hook_descriptor(hook_spec.type)
                materialized_hook = descriptor.materialize(
                    hook_spec,
                    manifest_dir=manifest.manifest_dir,
                )
                materialized_hook.set_config(config)
                materialized_hooks_by_id[hook_id] = materialized_hook
                hooks.append(materialized_hook)

        shared_requirements = tuple(
            requirements_by_name[resource_name]
            for resource_name in collect_engine_resource_names(
                manifest.resource_bindings,
                engine_type=engine_spec.type,
            )
        )
        engine_descriptor = resolve_engine_descriptor(engine_spec.type)
        engine = engine_descriptor.materialize(
            engine_spec,
            manifest=manifest,
            config=config,
            active_profiles=active_profiles,
            shared_requirements=shared_requirements,
        )

        engine.runtime_profile_ids = tuple(
            profile.id for profile in active_profiles
        )
        engine.runtime_profiles = active_profiles
        engine.runtime_service_offerings = build_runtime_service_offerings(
            active_profiles,
        )
        engines[engine_spec.path] = engine

    for hook in hooks:
        hook.set_config(config)

    return (hooks, engines)


def materialize_runtime_profile_hook_specs(
    manifest,
):
    if not manifest.runtime_profiles:
        return ()

    referencing_engine_ids_by_profile: dict[str, list[str]] = {
        profile.id: [] for profile in manifest.runtime_profiles
    }
    for engine in manifest.engines:
        for profile_id in engine.runtime_profile_ids:
            referencing_engine_ids_by_profile.setdefault(
                profile_id,
                [],
            ).append(engine.id)

    hook_specs: list[object] = []
    for profile in manifest.runtime_profiles:
        engine_ids = tuple(
            dict.fromkeys(
                referencing_engine_ids_by_profile.get(profile.id, ()),
            ),
        )
        matched_specs: list[object] = []
        for descriptor in iter_hook_descriptors():
            matched_specs.extend(
                descriptor.build_runtime_profile_hook_specs(
                    profile,
                    engine_ids=engine_ids,
                ),
            )
        if matched_specs:
            hook_specs.extend(matched_specs)
            continue

        msg = (
            'Unsupported runtime profile providers for '
            f'{profile.id!r}: '
            f'{", ".join(service.provider for service in profile.services)}'
        )
        raise ManifestValidationError(msg)

    return tuple(hook_specs)


def iter_manifest_hook_descriptors(
    hooks,
    *,
    resolve_hook_descriptor,
) -> tuple[type[object], ...]:
    return tuple(
        {resolve_hook_descriptor(hook.type) for hook in hooks},
    )


def collect_engine_resource_names(
    bindings,
    *,
    engine_type: str,
) -> tuple[str, ...]:
    ordered_names: list[str] = []
    seen_names: set[str] = set()
    for binding in bindings:
        if binding.engine_type != engine_type:
            continue
        if binding.resource_name in seen_names:
            continue
        seen_names.add(binding.resource_name)
        ordered_names.append(binding.resource_name)
    return tuple(ordered_names)


def materialize_gherkin_registry_loaders(
    loader_specs,
    *,
    registry_loader_cls,
) -> tuple[object, ...]:
    materialized_loaders: list[object] = []
    for loader_spec in loader_specs:
        grouped_layouts = group_registry_layouts_by_root_package(
            loader_spec.layouts,
        )
        for module_spec, grouped_layout_specs in grouped_layouts:
            materialized_loaders.append(
                registry_loader_cls(
                    layouts=tuple(
                        (
                            layout_spec.name,
                            f'{layout_spec.base.module}.'
                            f'{layout_spec.base.qualname}',
                        )
                        for layout_spec in grouped_layout_specs
                    ),
                    layout_patterns=tuple(
                        (
                            layout_spec.name,
                            build_registry_layout_include_patterns(
                                layout_spec,
                                module_spec=module_spec,
                            ),
                        )
                        for layout_spec in grouped_layout_specs
                    ),
                    module_spec=module_spec,
                ),
            )
    return tuple(materialized_loaders)


def group_registry_layouts_by_root_package(
    layout_specs,
):
    grouped: dict[str, dict[str, object]] = {}
    for layout_spec in layout_specs:
        for module_glob in layout_spec.module_globs:
            root_package = extract_module_glob_root_package(module_glob)
            grouped.setdefault(root_package, {})
            existing = grouped[root_package].get(layout_spec.name)
            if existing is None:
                grouped[root_package][layout_spec.name] = type(layout_spec)(
                    name=layout_spec.name,
                    base=layout_spec.base,
                    module_globs=(module_glob,),
                    match=layout_spec.match,
                )
                continue

            grouped[root_package][layout_spec.name] = type(layout_spec)(
                name=layout_spec.name,
                base=layout_spec.base,
                module_globs=(*existing.module_globs, module_glob),
                match=layout_spec.match,
            )

    return tuple(
        (
            root_package,
            tuple(
                sorted(layout_map.values(), key=lambda layout: layout.name),
            ),
        )
        for root_package, layout_map in sorted(grouped.items())
    )


def build_registry_layout_include_patterns(
    layout_spec,
    *,
    module_spec: str,
) -> tuple[str, ...]:
    patterns: set[str] = set()
    for module_glob in layout_spec.module_globs:
        patterns.update(
            module_glob_to_path_patterns(
                module_glob,
                module_spec=module_spec,
            ),
        )
    return tuple(sorted(patterns))


def module_glob_to_path_patterns(
    module_glob: str,
    *,
    module_spec: str,
) -> tuple[str, ...]:
    segments = module_glob.split('.')
    module_segments = module_spec.split('.')
    if segments[: len(module_segments)] != module_segments:
        msg = (
            'module_glob does not belong to the materialized module root: '
            f'{module_glob!r} vs {module_spec!r}'
        )
        raise ManifestValidationError(msg)

    relative_segments = segments[len(module_segments) :]
    if not relative_segments:
        return ('__init__.py',)

    path_segments = tuple(
        '**' if segment == '**' else '*' if segment == '*' else segment
        for segment in relative_segments
    )
    relative_path = '/'.join(path_segments)
    return (
        f'{relative_path}.py',
        f'{relative_path}/__init__.py',
    )
