from __future__ import annotations

from dataclasses import dataclass, field
from importlib import import_module
from typing import TYPE_CHECKING, Literal, cast

from cosecha.core.runtime_interop import (
    build_runtime_canonical_binding_name,
    validate_runtime_capability_matrix,
    validate_runtime_interface_name,
)
from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.resources import ResourceRequirement


type WorkerIsolationMode = Literal['strict', 'shared']
type InitializationMode = Literal['data_seed', 'state_snapshot']
type ServiceReadinessStatus = Literal[
    'starting',
    'ready',
    'degraded',
    'unhealthy',
    'unhealthy_local',
]


def _build_runtime_shadow_requirement(
    service: RuntimeServiceSpec,
) -> ResourceRequirement:
    resources_module = import_module('cosecha.core.resources')
    return resources_module.ResourceRequirement(
        name=service.interface,
        provider=resources_module.CallableResourceProvider(lambda: None),
        scope=cast('str', service.scope),
        mode='live',
        depends_on=service.depends_on,
        initializes_from=service.initializes_from,
        initialization_mode=cast('str', service.initialization_mode),
        readiness_policy=service.readiness_policy,
    )


@dataclass(slots=True, frozen=True)
class RuntimeCapabilityRequirement:
    interface_name: str
    capability_name: str

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> RuntimeCapabilityRequirement:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeModeDisallowance:
    interface_name: str
    mode_name: str

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> RuntimeModeDisallowance:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeModeRequirement:
    interface_name: str
    mode_name: str

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> RuntimeModeRequirement:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeRequirementSet:
    interfaces: tuple[str, ...] = ()
    capabilities: tuple[RuntimeCapabilityRequirement, ...] = ()
    required_modes: tuple[RuntimeModeRequirement, ...] = ()
    disallowed_modes: tuple[RuntimeModeDisallowance, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            'interfaces',
            tuple(dict.fromkeys(self.interfaces)),
        )

    def merge(
        self,
        other: RuntimeRequirementSet,
    ) -> RuntimeRequirementSet:
        merged_interfaces = tuple(
            dict.fromkeys((*self.interfaces, *other.interfaces)),
        )
        merged_capabilities = tuple(
            dict.fromkeys((*self.capabilities, *other.capabilities)),
        )
        merged_required_modes = tuple(
            dict.fromkeys((*self.required_modes, *other.required_modes)),
        )
        merged_disallowed_modes = tuple(
            dict.fromkeys((*self.disallowed_modes, *other.disallowed_modes)),
        )
        return RuntimeRequirementSet(
            interfaces=merged_interfaces,
            capabilities=merged_capabilities,
            required_modes=merged_required_modes,
            disallowed_modes=merged_disallowed_modes,
        )

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RuntimeRequirementSet:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeReadinessPolicy:
    initial_delay_seconds: float = 0.0
    retry_interval_seconds: float = 0.0
    max_wait_seconds: float | None = None
    degraded_timeout_seconds: float | None = None
    local_health_check_interval_seconds: float | None = None
    worker_lost_timeout_seconds: float = 30.0
    heartbeat_interval_seconds: float = 5.0

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RuntimeReadinessPolicy:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeServiceSpec:
    interface: str
    provider: str
    canonical_binding_name: str | None = None
    mode: str | None = None
    scope: str = 'test'
    depends_on: tuple[str, ...] = ()
    initializes_from: tuple[str, ...] = ()
    initialization_mode: InitializationMode = 'data_seed'
    supported_initialization_modes: tuple[InitializationMode, ...] = ()
    public_bindings: tuple[str, ...] = ()
    supports_worker_rebind: bool = False
    supports_orphan_cleanup: bool = False
    readiness_policy: RuntimeReadinessPolicy = field(
        default_factory=RuntimeReadinessPolicy,
    )
    supported_worker_isolation_modes: tuple[WorkerIsolationMode, ...] = (
        'strict',
    )
    capabilities: tuple[str, ...] = ()
    degraded_capabilities: tuple[str, ...] = ()
    telemetry_capabilities: tuple[str, ...] = ()
    config: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.canonical_binding_name is None:
            object.__setattr__(
                self,
                'canonical_binding_name',
                build_runtime_canonical_binding_name(self.interface),
            )

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RuntimeServiceSpec:
        return from_builtins_dict(data, target_type=cls)

    def build_shadow_requirement(self) -> ResourceRequirement:
        return _build_runtime_shadow_requirement(self)


@dataclass(slots=True, frozen=True)
class RuntimeProfileSpec:
    id: str
    worker_isolation: WorkerIsolationMode = 'strict'
    services: tuple[RuntimeServiceSpec, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RuntimeProfileSpec:
        return from_builtins_dict(data, target_type=cls)

    def build_shadow_requirements(self) -> tuple[ResourceRequirement, ...]:
        return build_runtime_service_shadow_requirements(self)


@dataclass(slots=True, frozen=True)
class RuntimeServiceOffering:
    interface_name: str
    provider_name: str
    mode: str | None = None
    canonical_binding_name: str | None = None
    capabilities: tuple[str, ...] = ()
    degraded_capabilities: tuple[str, ...] = ()
    readiness_state: ServiceReadinessStatus = 'ready'
    readiness_reason: str | None = None

    def __post_init__(self) -> None:
        if self.canonical_binding_name is None:
            object.__setattr__(
                self,
                'canonical_binding_name',
                build_runtime_canonical_binding_name(self.interface_name),
            )

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> RuntimeServiceOffering:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeServiceReadinessState:
    interface_name: str
    provider_name: str
    status: ServiceReadinessStatus
    reason: str | None = None
    available_capabilities: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> RuntimeServiceReadinessState:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeServiceMaterializationSnapshot:
    interface_name: str
    provider_name: str
    mode: str | None = None
    scope: str = 'test'
    effective_config: dict[str, object] = field(default_factory=dict)
    connection_data: dict[str, object] = field(default_factory=dict)
    binding_metadata: dict[str, object] = field(default_factory=dict)
    worker_rebind_data: dict[str, object] = field(default_factory=dict)
    readiness_state: RuntimeServiceReadinessState = field(
        default_factory=lambda: RuntimeServiceReadinessState(
            interface_name='',
            provider_name='',
            status='ready',
        ),
    )

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> RuntimeServiceMaterializationSnapshot:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeMaterializationSnapshot:
    snapshot_version: int = 1
    producer_version: str | None = None
    profile_id: str | None = None
    services: tuple[RuntimeServiceMaterializationSnapshot, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> RuntimeMaterializationSnapshot:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RuntimeMaterializationExplanation:
    active_profile_ids: tuple[str, ...] = ()
    inactive_profile_ids: tuple[str, ...] = ()
    service_readiness_states: tuple[RuntimeServiceReadinessState, ...] = ()
    reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> RuntimeMaterializationExplanation:
        return from_builtins_dict(data, target_type=cls)


def build_runtime_service_shadow_requirements(
    profile: RuntimeProfileSpec,
) -> tuple[ResourceRequirement, ...]:
    return tuple(
        service.build_shadow_requirement()
        for service in profile.services
    )


def format_runtime_service_invariant_error(error: ValueError) -> str:
    message = str(error)
    if message.startswith('Resource '):
        message = 'Runtime service ' + message.removeprefix('Resource ')
    return message.replace(
        'missing resources',
        'missing services',
    ).replace(
        'Cyclic resource dependency',
        'Cyclic runtime service dependency',
    ).replace(
        'Cyclic resource initialization',
        'Cyclic runtime service initialization',
    )


def build_runtime_service_offerings(
    profiles: tuple[RuntimeProfileSpec, ...],
) -> tuple[RuntimeServiceOffering, ...]:
    offerings: list[RuntimeServiceOffering] = []
    for profile in profiles:
        offerings.extend(
            RuntimeServiceOffering(
                interface_name=service.interface,
                provider_name=service.provider,
                mode=service.mode,
                canonical_binding_name=(
                    service.canonical_binding_name
                    or build_runtime_canonical_binding_name(
                        service.interface,
                    )
                ),
                capabilities=service.capabilities,
                degraded_capabilities=service.degraded_capabilities,
                readiness_state='ready',
            )
            for service in profile.services
        )

    return tuple(offerings)


def resolve_runtime_requirement_issues(
    requirements: RuntimeRequirementSet,
    offerings: tuple[RuntimeServiceOffering, ...],
) -> tuple[str, ...]:
    offerings_by_interface: dict[str, RuntimeServiceOffering] = {
        offering.interface_name: offering for offering in offerings
    }
    issues: list[str] = []
    for interface_name in requirements.interfaces:
        issue = _resolve_runtime_interface_requirement_issue(
            interface_name=interface_name,
            offerings_by_interface=offerings_by_interface,
        )
        if issue is not None:
            issues.append(issue)

    for required_mode in requirements.required_modes:
        issue = _resolve_runtime_mode_requirement_issue(
            required_mode=required_mode,
            offerings_by_interface=offerings_by_interface,
        )
        if issue is not None:
            issues.append(issue)

    for disallowed_mode in requirements.disallowed_modes:
        issue = _resolve_runtime_mode_disallowance_issue(
            disallowed_mode=disallowed_mode,
            offerings_by_interface=offerings_by_interface,
        )
        if issue is not None:
            issues.append(issue)

    for capability_requirement in requirements.capabilities:
        issue = _resolve_runtime_capability_requirement_issue(
            capability_requirement=capability_requirement,
            offerings_by_interface=offerings_by_interface,
        )
        if issue is not None:
            issues.append(issue)

    return tuple(issues)


def _resolve_runtime_interface_requirement_issue(
    *,
    interface_name: str,
    offerings_by_interface: dict[str, RuntimeServiceOffering],
) -> str | None:
    interface_error = validate_runtime_interface_name(interface_name)
    if interface_error is not None:
        return interface_error

    offering = offerings_by_interface.get(interface_name)
    if offering is None:
        return (
            f'Missing runtime interface {interface_name!r} in active profile'
        )
    if offering.readiness_state in {'unhealthy', 'unhealthy_local'}:
        return (
            f'Runtime interface {interface_name!r} '
            f'is {offering.readiness_state}'
        )
    return None


def _resolve_runtime_mode_disallowance_issue(
    *,
    disallowed_mode: RuntimeModeDisallowance,
    offerings_by_interface: dict[str, RuntimeServiceOffering],
) -> str | None:
    interface_error = validate_runtime_interface_name(
        disallowed_mode.interface_name,
    )
    if interface_error is not None:
        return interface_error

    offering = offerings_by_interface.get(disallowed_mode.interface_name)
    if offering is None or offering.mode != disallowed_mode.mode_name:
        return None

    return (
        'Runtime interface '
        f'{disallowed_mode.interface_name!r} uses disallowed mode '
        f'{disallowed_mode.mode_name!r}'
    )


def _resolve_runtime_mode_requirement_issue(
    *,
    required_mode: RuntimeModeRequirement,
    offerings_by_interface: dict[str, RuntimeServiceOffering],
) -> str | None:
    interface_error = validate_runtime_interface_name(
        required_mode.interface_name,
    )
    if interface_error is not None:
        return interface_error

    offering = offerings_by_interface.get(required_mode.interface_name)
    if offering is None:
        return (
            'Missing runtime interface '
            f'{required_mode.interface_name!r} required in mode '
            f'{required_mode.mode_name!r}'
        )

    if offering.readiness_state in {'unhealthy', 'unhealthy_local'}:
        return (
            'Runtime interface '
            f'{required_mode.interface_name!r} is '
            f'{offering.readiness_state} and cannot satisfy required mode '
            f'{required_mode.mode_name!r}'
        )

    if offering.mode == required_mode.mode_name:
        return None

    return (
        'Runtime interface '
        f'{required_mode.interface_name!r} requires mode '
        f'{required_mode.mode_name!r} in active profile'
    )


def _resolve_runtime_capability_requirement_issue(
    *,
    capability_requirement: RuntimeCapabilityRequirement,
    offerings_by_interface: dict[str, RuntimeServiceOffering],
) -> str | None:
    error_message: str | None = None
    interface_error = validate_runtime_interface_name(
        capability_requirement.interface_name,
    )
    if interface_error is not None:
        return interface_error

    offering = offerings_by_interface.get(
        capability_requirement.interface_name,
    )
    if offering is None:
        error_message = (
            'Missing runtime interface '
            f'{capability_requirement.interface_name!r} required by '
            f'capability {capability_requirement.capability_name!r}'
        )
    elif offering.readiness_state in {'unhealthy', 'unhealthy_local'}:
        error_message = (
            'Runtime interface '
            f'{capability_requirement.interface_name!r} is '
            f'{offering.readiness_state} and cannot satisfy capability '
            f'{capability_requirement.capability_name!r}'
        )
    else:
        try:
            validation = validate_runtime_capability_matrix(
                capability_requirement.interface_name,
                (capability_requirement.capability_name,),
            )
        except ValueError as error:
            error_message = (
                'Capability discovery error for runtime interface '
                f'{capability_requirement.interface_name!r}: '
                f'{error}'
            )
        else:
            unknown_capabilities = (
                () if validation is None else validation.unknown_capabilities
            )
            if (
                capability_requirement.capability_name
                not in offering.capabilities
                and capability_requirement.capability_name
                not in offering.degraded_capabilities
                and unknown_capabilities
            ):
                validation_message = (
                    'unknown capability '
                    f'{capability_requirement.capability_name!r}'
                    if validation is None
                    else '; '.join(validation.messages())
                )
                error_message = (
                    'Capability discovery error for runtime interface '
                    f'{capability_requirement.interface_name!r}: '
                    f'{validation_message}'
                )
            else:
                available_capabilities = offering.capabilities
                if offering.readiness_state == 'degraded':
                    available_capabilities = offering.degraded_capabilities

                if (
                    capability_requirement.capability_name
                    not in available_capabilities
                ):
                    error_message = (
                        'Missing runtime capability '
                        f'{capability_requirement.interface_name!r}:'
                        f'{capability_requirement.capability_name!r} '
                        'in active profile'
                    )

    return error_message
