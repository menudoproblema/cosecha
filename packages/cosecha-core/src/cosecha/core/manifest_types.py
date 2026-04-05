from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from cosecha.core.manifest_symbols import ManifestValidationError, SymbolRef
from cosecha.core.resources import ResourceRequirement
from cosecha.core.runtime_profiles import (
    RuntimeProfileSpec,
    RuntimeReadinessPolicy,
)
from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


@dataclass(slots=True, frozen=True)
class RegistryLayoutSpec:
    name: str
    base: SymbolRef
    module_globs: tuple[str, ...] = ()
    match: str = 'subclass'

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RegistryLayoutSpec:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RegistryLoaderSpec:
    layouts: tuple[RegistryLayoutSpec, ...]

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RegistryLoaderSpec:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class EngineSpec:
    id: str
    type: str
    name: str
    path: str
    runtime_profile_ids: tuple[str, ...] = ()
    definition_paths: tuple[str, ...] = ()
    step_library_modules: tuple[str, ...] = ()
    coercions: tuple[tuple[str, SymbolRef], ...] = ()
    registry_loaders: tuple[RegistryLoaderSpec, ...] = ()
    factory: SymbolRef | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> EngineSpec:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class HookSpec:
    id: str
    type: str
    engine_ids: tuple[str, ...] = ()
    config: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> HookSpec:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeProfileDecision:
    id: str
    active: bool
    referenced_engine_ids: tuple[str, ...] = ()
    active_engine_ids: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class ResourceSpec:
    name: str
    provider: SymbolRef | None = None
    factory: SymbolRef | None = None
    scope: str = 'test'
    mode: str = 'live'
    config: dict[str, object] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    initializes_from: tuple[str, ...] = ()
    initialization_mode: str = 'data_seed'
    initialization_timeout_seconds: float | None = None
    readiness_policy: RuntimeReadinessPolicy = field(
        default_factory=RuntimeReadinessPolicy,
    )
    conflicts_with: tuple[str, ...] = ()
    requires_orphan_fencing: bool = False

    def build_requirement(self, *, root_path: Path) -> ResourceRequirement:
        provider = None
        setup = None
        if self.provider is not None:
            resolved = self.provider.resolve(root_path=root_path)
            if isinstance(resolved, type):
                provider = resolved()
            elif callable(resolved):
                candidate = resolved()
                provider = candidate if candidate is not None else resolved
            else:
                provider = resolved
        elif self.factory is not None:
            resolved = self.factory.resolve(root_path=root_path)
            if not callable(resolved):
                msg = (
                    'Resource factory symbol must be callable for '
                    f'{self.name!r}'
                )
                raise ManifestValidationError(msg)
            setup = resolved

        return ResourceRequirement(
            name=self.name,
            provider=provider,
            setup=setup,
            scope=self.scope,  # type: ignore[arg-type]
            mode=self.mode,  # type: ignore[arg-type]
            config=self.config,
            depends_on=self.depends_on,
            initializes_from=self.initializes_from,
            initialization_mode=self.initialization_mode,  # type: ignore[arg-type]
            initialization_timeout_seconds=(
                self.initialization_timeout_seconds
            ),
            readiness_policy=self.readiness_policy,
            conflicts_with=self.conflicts_with,
            requires_orphan_fencing=self.requires_orphan_fencing,
        )


@dataclass(slots=True, frozen=True)
class ResourceBindingSpec:
    engine_type: str
    resource_name: str
    fixture_name: str | None = None
    layout: str | None = None
    alias: str | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ResourceBindingSpec:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class CosechaManifest:
    path: str
    schema_version: int
    engines: tuple[EngineSpec, ...]
    runtime_profiles: tuple[RuntimeProfileSpec, ...] = ()
    resources: tuple[ResourceSpec, ...] = ()
    resource_bindings: tuple[ResourceBindingSpec, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @property
    def manifest_path(self) -> Path:
        return Path(self.path)

    @property
    def manifest_dir(self) -> Path:
        return self.manifest_path.parent

    def find_runtime_profile(self, profile_id: str) -> RuntimeProfileSpec:
        for profile in self.runtime_profiles:
            if profile.id == profile_id:
                return profile

        msg = f'Unknown runtime profile id: {profile_id}'
        raise ManifestValidationError(msg)

    def find_resource(self, resource_name: str) -> ResourceSpec:
        for resource in self.resources:
            if resource.name == resource_name:
                return resource

        msg = f'Unknown resource: {resource_name}'
        raise ManifestValidationError(msg)


@dataclass(slots=True, frozen=True)
class ManifestEngineExplanation:
    id: str
    name: str
    type: str
    path: str
    runtime_profile_ids: tuple[str, ...] = ()
    definition_paths: tuple[str, ...] = ()
    resource_names: tuple[str, ...] = ()
    step_library_modules: tuple[str, ...] = ()
    registry_layouts: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class ManifestEngineDecision:
    id: str
    name: str
    type: str
    path: str
    active: bool
    reasons: tuple[str, ...] = ()
    matched_requested_paths: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class ManifestResourceBindingDecision:
    engine_type: str
    active: bool
    fixture_name: str | None = None
    layout: str | None = None
    alias: str | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class ManifestResourceDecision:
    name: str
    scope: str
    mode: str
    active: bool
    binding_engine_types: tuple[str, ...] = ()
    active_binding_engine_types: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    bindings: tuple[ManifestResourceBindingDecision, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)


@dataclass(slots=True, frozen=True)
class ManifestMaterializationExplanation:
    manifest_path: str
    schema_version: int
    root_path: str
    selected_engine_names: tuple[str, ...] = ()
    requested_paths: tuple[str, ...] = ()
    normalized_paths: tuple[str, ...] = ()
    active_engines: tuple[ManifestEngineExplanation, ...] = ()
    evaluated_engines: tuple[ManifestEngineDecision, ...] = ()
    active_runtime_profile_ids: tuple[str, ...] = ()
    inactive_runtime_profile_ids: tuple[str, ...] = ()
    active_resource_names: tuple[str, ...] = ()
    inactive_resource_names: tuple[str, ...] = ()
    inactive_engine_ids: tuple[str, ...] = ()
    evaluated_runtime_profiles: tuple[RuntimeProfileDecision, ...] = ()
    evaluated_resources: tuple[ManifestResourceDecision, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)
