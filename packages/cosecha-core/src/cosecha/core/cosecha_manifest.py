from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from cosecha.core import manifest_materialization as _manifest_materialization
from cosecha.core.discovery import (
    get_engine_descriptor,
    get_hook_descriptor,
    iter_hook_descriptors,
    register_engine_descriptor,
    register_hook_descriptor,
)
from cosecha.core.manifest_loader import (
    parse_cosecha_manifest_text as _parse_cosecha_manifest_text,
)
from cosecha.core.manifest_materialization import (
    collect_engine_resource_names,
    iter_manifest_hook_descriptors,
    materialize_runtime_components as _materialize_runtime_components,
)
from cosecha.core.manifest_selection import (
    evaluate_engine_selection,
    evaluate_resource_selection,
    evaluate_runtime_profile_selection,
    select_engine_specs,
)
from cosecha.core.manifest_symbols import (
    ManifestValidationError,
    SymbolRef,
)
from cosecha.core.manifest_types import (
    CosechaManifest,
    EngineSpec,
    HookSpec,
    ManifestEngineExplanation,
    ManifestMaterializationExplanation,
    ResourceBindingSpec,
)
from cosecha.core.manifest_validation import (
    validate_manifest as _validate_manifest,
)
from cosecha.core.runtime_profiles import (
    RuntimeProfileSpec,
)


if TYPE_CHECKING:  # pragma: no cover
    from argparse import ArgumentParser, Namespace

    from cosecha.core.config import Config
    from cosecha.core.engines.base import Engine
    from cosecha.core.hooks import Hook
    from cosecha.core.resources import ResourceRequirement


COSECHA_MANIFEST_SCHEMA_VERSION = 1


class ManifestHookDescriptor(Protocol):
    hook_type: str

    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None: ...

    @classmethod
    def apply_cli_overrides(
        cls,
        spec: HookSpec,
        args: Namespace,
    ) -> HookSpec: ...

    @classmethod
    def materialize(
        cls,
        spec: HookSpec,
        *,
        manifest_dir: Path,
    ) -> Hook: ...

    @classmethod
    def build_runtime_profile_hook_specs(
        cls,
        profile: RuntimeProfileSpec,
        *,
        engine_ids: tuple[str, ...],
    ) -> tuple[HookSpec, ...]: ...


class PythonHookDescriptor:
    hook_type = 'python'

    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None:
        del parser

    @classmethod
    def apply_cli_overrides(
        cls,
        spec: HookSpec,
        args: Namespace,
    ) -> HookSpec:
        del args
        return spec

    @classmethod
    def materialize(
        cls,
        spec: HookSpec,
        *,
        manifest_dir: Path,
    ) -> Hook:
        raw_factory = spec.config.get('factory')
        if not isinstance(raw_factory, str):
            msg = (
                'Python hook specs require config.factory as symbol ref '
                f'for hook {spec.id!r}'
            )
            raise ManifestValidationError(msg)

        factory = SymbolRef.parse(raw_factory).resolve(root_path=manifest_dir)
        if not callable(factory):
            msg = f'Hook factory is not callable for {spec.id!r}'
            raise ManifestValidationError(msg)

        return factory()

    @classmethod
    def build_runtime_profile_hook_specs(
        cls,
        profile: RuntimeProfileSpec,
        *,
        engine_ids: tuple[str, ...],
    ) -> tuple[HookSpec, ...]:
        del profile, engine_ids
        return ()


class PythonEngineDescriptor:
    engine_type = 'python'

    @classmethod
    def validate_resource_binding(
        cls,
        binding: ResourceBindingSpec,
        *,
        manifest: CosechaManifest,
    ) -> None:
        del manifest
        msg = (
            'Python engine does not support declarative resource bindings: '
            f'{binding.resource_name!r}'
        )
        raise ManifestValidationError(msg)

    @classmethod
    def materialize(
        cls,
        engine_spec: EngineSpec,
        *,
        manifest: CosechaManifest,
        config: Config,
        active_profiles: tuple[RuntimeProfileSpec, ...],
        shared_requirements: tuple[ResourceRequirement, ...],
    ) -> Engine:
        del config, active_profiles, shared_requirements
        if engine_spec.factory is None:
            msg = (
                'Python engine specs require factory symbol ref for '
                f'engine {engine_spec.id!r}'
            )
            raise ManifestValidationError(msg)

        factory = engine_spec.factory.resolve(
            root_path=manifest.manifest_dir,
        )
        if not callable(factory):
            msg = f'Engine factory is not callable for {engine_spec.id!r}'
            raise ManifestValidationError(msg)

        return factory()


register_hook_descriptor(PythonHookDescriptor)
register_engine_descriptor(PythonEngineDescriptor)


def discover_cosecha_manifest(
    *,
    manifest_file: Path | None = None,
) -> Path | None:
    if manifest_file is not None:
        return manifest_file.resolve() if manifest_file.exists() else None

    for candidate in (Path('tests/cosecha.toml'), Path('cosecha.toml')):
        if candidate.exists():
            return candidate.resolve()

    return None


def load_cosecha_manifest(
    manifest_file: Path | None = None,
) -> CosechaManifest | None:
    resolved_manifest = discover_cosecha_manifest(manifest_file=manifest_file)
    if resolved_manifest is None:
        return None

    return parse_cosecha_manifest_text(
        resolved_manifest.read_text(encoding='utf-8'),
        manifest_path=resolved_manifest,
    )


def parse_cosecha_manifest_text(
    source_content: str,
    *,
    manifest_path: Path,
    resolve_symbols: bool = False,
) -> CosechaManifest:
    return _parse_cosecha_manifest_text(
        source_content,
        manifest_path=manifest_path,
        schema_version=COSECHA_MANIFEST_SCHEMA_VERSION,
        resolve_symbols=resolve_symbols,
        iter_hook_descriptors=iter_hook_descriptors,
        resolve_engine_descriptor=_resolve_engine_descriptor,
    )


def register_manifest_hook_arguments(
    parser: ArgumentParser,
    manifest: CosechaManifest,
) -> None:
    effective_hooks = _materialize_runtime_profile_hook_specs(manifest)
    for descriptor in _iter_hook_descriptors(effective_hooks):
        descriptor.register_arguments(parser)


def apply_manifest_cli_overrides(
    manifest: CosechaManifest,
    args: Namespace,
) -> CosechaManifest:
    effective_hooks = _materialize_runtime_profile_hook_specs(manifest)
    overridden_profiles_by_id: dict[str, RuntimeProfileSpec] = {}
    for hook in effective_hooks:
        descriptor = _resolve_hook_descriptor(hook.type)
        overridden_hook = descriptor.apply_cli_overrides(hook, args)
        raw_profile = overridden_hook.config.get('profile')
        if not isinstance(raw_profile, dict):
            msg = (
                'Runtime profile hook override must preserve config.profile '
                f'for hook {overridden_hook.id!r}'
            )
            raise ManifestValidationError(msg)
        overridden_profiles_by_id[
            RuntimeProfileSpec.from_dict(raw_profile).id
        ] = RuntimeProfileSpec.from_dict(raw_profile)

    return CosechaManifest(
        path=manifest.path,
        schema_version=manifest.schema_version,
        engines=manifest.engines,
        runtime_profiles=tuple(
            overridden_profiles_by_id.get(profile.id, profile)
            for profile in manifest.runtime_profiles
        ),
        resources=manifest.resources,
        resource_bindings=manifest.resource_bindings,
    )


def materialize_runtime_components(
    manifest: CosechaManifest,
    *,
    config: Config,
    selected_engine_names: set[str] | None = None,
    requested_paths: tuple[str, ...] = (),
) -> tuple[list[Hook], dict[str, Engine]]:
    return _materialize_runtime_components(
        manifest,
        config=config,
        selected_engine_names=selected_engine_names,
        requested_paths=requested_paths,
        select_engine_specs=select_engine_specs,
        resolve_hook_descriptor=_resolve_hook_descriptor,
        resolve_engine_descriptor=_resolve_engine_descriptor,
    )


def _materialize_runtime_profile_hook_specs(
    manifest: CosechaManifest,
) -> tuple[HookSpec, ...]:
    return _manifest_materialization.materialize_runtime_profile_hook_specs(
        manifest,
    )


def validate_cosecha_manifest(
    manifest: CosechaManifest,
) -> tuple[str, ...]:
    try:
        _validate_manifest(
            manifest,
            resolve_symbols=True,
            iter_hook_descriptors=iter_hook_descriptors,
            resolve_engine_descriptor=_resolve_engine_descriptor,
        )
    except ManifestValidationError as error:
        return (str(error),)

    return ()


def explain_cosecha_manifest(
    manifest: CosechaManifest,
    *,
    config: Config,
    selected_engine_names: set[str] | None = None,
    requested_paths: tuple[str, ...] = (),
) -> ManifestMaterializationExplanation:
    evaluated_engines = evaluate_engine_selection(
        manifest,
        config=config,
        selected_engine_names=selected_engine_names,
        requested_paths=requested_paths,
    )
    active_specs = select_engine_specs(
        manifest,
        config=config,
        selected_engine_names=selected_engine_names,
        requested_paths=requested_paths,
    )
    active_engine_ids = {engine.id for engine in active_specs}
    active_runtime_profile_ids = tuple(
        sorted(
            {
                profile_id
                for engine in active_specs
                for profile_id in engine.runtime_profile_ids
            },
        ),
    )
    inactive_runtime_profile_ids = tuple(
        profile.id
        for profile in manifest.runtime_profiles
        if profile.id not in active_runtime_profile_ids
    )
    evaluated_runtime_profiles = evaluate_runtime_profile_selection(
        manifest,
        active_specs=active_specs,
    )
    active_resource_names = tuple(
        sorted(
            {
                resource_name
                for engine in active_specs
                for resource_name in collect_engine_resource_names(
                    manifest.resource_bindings,
                    engine_type=engine.type,
                )
            },
        ),
    )
    inactive_resource_names = tuple(
        resource.name
        for resource in manifest.resources
        if resource.name not in active_resource_names
    )
    evaluated_resources = evaluate_resource_selection(
        manifest,
        active_specs=active_specs,
    )
    return ManifestMaterializationExplanation(
        manifest_path=manifest.path,
        schema_version=manifest.schema_version,
        root_path=str(config.root_path),
        selected_engine_names=tuple(sorted(selected_engine_names or ())),
        requested_paths=requested_paths,
        normalized_paths=requested_paths,
        active_engines=tuple(
            ManifestEngineExplanation(
                id=engine.id,
                name=engine.name,
                type=engine.type,
                path=engine.path,
                runtime_profile_ids=engine.runtime_profile_ids,
                definition_paths=engine.definition_paths,
                resource_names=collect_engine_resource_names(
                    manifest.resource_bindings,
                    engine_type=engine.type,
                ),
                step_library_modules=engine.step_library_modules,
                registry_layouts=tuple(
                    sorted(
                        {
                            layout.name
                            for loader in engine.registry_loaders
                            for layout in loader.layouts
                        },
                    ),
                ),
            )
            for engine in active_specs
        ),
        evaluated_engines=evaluated_engines,
        active_runtime_profile_ids=active_runtime_profile_ids,
        inactive_runtime_profile_ids=inactive_runtime_profile_ids,
        active_resource_names=active_resource_names,
        inactive_resource_names=inactive_resource_names,
        inactive_engine_ids=tuple(
            engine.id
            for engine in manifest.engines
            if engine.id not in active_engine_ids
        ),
        evaluated_runtime_profiles=evaluated_runtime_profiles,
        evaluated_resources=evaluated_resources,
    )


def _iter_hook_descriptors(
    hooks: tuple[HookSpec, ...],
) -> tuple[type[ManifestHookDescriptor], ...]:
    return iter_manifest_hook_descriptors(
        hooks,
        resolve_hook_descriptor=_resolve_hook_descriptor,
    )


def _resolve_hook_descriptor(
    hook_type: str,
) -> type[ManifestHookDescriptor]:
    descriptor = get_hook_descriptor(hook_type)
    if descriptor is not None:
        return cast('type[ManifestHookDescriptor]', descriptor)

    msg = f'Unsupported hook type: {hook_type!r}'
    raise ManifestValidationError(msg)

def _resolve_engine_descriptor(engine_type: str):
    descriptor = get_engine_descriptor(engine_type)
    if descriptor is not None:
        return descriptor

    msg = f'Unsupported engine type: {engine_type!r}'
    raise ManifestValidationError(msg)
