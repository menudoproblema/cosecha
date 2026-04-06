from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.capabilities import CapabilityDescriptor
    from cosecha.core.engines.base import Engine
    from cosecha.core.plugins.base import Plugin
    from cosecha.core.reporter import Reporter
    from cosecha.core.runtime import RuntimeProvider


type ExtensionKind = Literal['engine', 'plugin', 'reporter', 'runtime']
type ExtensionStability = Literal['stable', 'experimental']
type ExtensionCompatibilityValue = str | bool | int | tuple[str, ...]

ENGINE_EXTENSION_API_VERSION = 1
PLUGIN_EXTENSION_API_VERSION = 1
REPORTER_EXTENSION_API_VERSION = 1
RUNTIME_EXTENSION_API_VERSION = 1


@dataclass(slots=True, frozen=True)
class ExtensionCompatibilityConstraint:
    name: str
    value: ExtensionCompatibilityValue

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> ExtensionCompatibilityConstraint:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ExtensionDescriptor:
    canonical_name: str
    extension_kind: ExtensionKind
    api_version: int
    stability: ExtensionStability = 'stable'
    implementation: str = ''
    cxp_interface: str | None = None
    published_capabilities: tuple[str, ...] = field(default_factory=tuple)
    compatibility: tuple[ExtensionCompatibilityConstraint, ...] = field(
        default_factory=tuple,
    )
    surfaces: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ExtensionDescriptor:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ExtensionComponentSnapshot:
    component_name: str
    descriptor: ExtensionDescriptor

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> ExtensionComponentSnapshot:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ExtensionQuery:
    extension_kind: ExtensionKind | None = None
    component_name: str | None = None
    canonical_name: str | None = None
    stability: ExtensionStability | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ExtensionQuery:
        return from_builtins_dict(data, target_type=cls)


def build_engine_extension_snapshot(
    engine: Engine,
    *,
    descriptors: tuple[CapabilityDescriptor, ...],
) -> ExtensionComponentSnapshot:
    primary_file_types = (engine.collector.file_type,)
    compatibility = (
        ExtensionCompatibilityConstraint(
            name='primary_file_types',
            value=primary_file_types,
        ),
    )
    return ExtensionComponentSnapshot(
        component_name=engine.name,
        descriptor=ExtensionDescriptor(
            canonical_name=engine.name,
            extension_kind='engine',
            api_version=engine.engine_api_version(),
            stability=engine.engine_stability(),
            implementation=_build_implementation_name(engine.__class__),
            cxp_interface='cosecha/engine',
            published_capabilities=_capability_names(descriptors),
            compatibility=compatibility,
        ),
    )


def build_runtime_extension_snapshot(
    runtime_provider: RuntimeProvider,
    *,
    descriptors: tuple[CapabilityDescriptor, ...],
) -> ExtensionComponentSnapshot:
    compatibility = (
        ExtensionCompatibilityConstraint(
            name='legacy_session_scope',
            value=runtime_provider.legacy_session_scope(),
        ),
        ExtensionCompatibilityConstraint(
            name='worker_model',
            value=runtime_provider.runtime_worker_model(),
        ),
    )
    return ExtensionComponentSnapshot(
        component_name=runtime_provider.runtime_name(),
        descriptor=ExtensionDescriptor(
            canonical_name=runtime_provider.runtime_name(),
            extension_kind='runtime',
            api_version=runtime_provider.runtime_api_version(),
            stability=runtime_provider.runtime_stability(),
            implementation=_build_implementation_name(
                runtime_provider.__class__,
            ),
            published_capabilities=_capability_names(descriptors),
            compatibility=compatibility,
        ),
    )


def build_reporter_extension_snapshot(
    reporter: Reporter,
) -> ExtensionComponentSnapshot:
    descriptor_reporter = reporter.descriptor_target()
    compatibility = (
        ExtensionCompatibilityConstraint(
            name='output_kind',
            value=descriptor_reporter.reporter_output_kind(),
        ),
    )
    return ExtensionComponentSnapshot(
        component_name=descriptor_reporter.reporter_name(),
        descriptor=ExtensionDescriptor(
            canonical_name=descriptor_reporter.reporter_name(),
            extension_kind='reporter',
            api_version=descriptor_reporter.reporter_api_version(),
            stability=descriptor_reporter.reporter_stability(),
            implementation=_build_implementation_name(
                descriptor_reporter.__class__,
            ),
            cxp_interface='cosecha/reporter',
            compatibility=compatibility,
        ),
    )


def build_plugin_extension_snapshot(
    plugin: Plugin,
    *,
    descriptors: tuple[CapabilityDescriptor, ...],
) -> ExtensionComponentSnapshot:
    compatibility = []
    required_capabilities = tuple(sorted(plugin.required_capabilities()))
    if required_capabilities:
        compatibility.append(
            ExtensionCompatibilityConstraint(
                name='required_capabilities',
                value=required_capabilities,
            ),
        )
    return ExtensionComponentSnapshot(
        component_name=plugin.plugin_name(),
        descriptor=ExtensionDescriptor(
            canonical_name=plugin.plugin_name(),
            extension_kind='plugin',
            api_version=plugin.plugin_api_version(),
            stability=plugin.plugin_stability(),
            implementation=_build_implementation_name(plugin.__class__),
            cxp_interface='cosecha/plugin',
            published_capabilities=_capability_names(descriptors),
            compatibility=tuple(compatibility),
            surfaces=plugin.provided_surfaces(),
        ),
    )


def _capability_names(
    descriptors: tuple[CapabilityDescriptor, ...],
) -> tuple[str, ...]:
    return tuple(sorted(descriptor.name for descriptor in descriptors))


def _build_implementation_name(extension_type: type[object]) -> str:
    return f'{extension_type.__module__}:{extension_type.__qualname__}'
