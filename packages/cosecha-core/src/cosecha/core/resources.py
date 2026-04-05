from __future__ import annotations

import asyncio
import time

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

from cosecha.core.domain_events import (
    DomainEventMetadata,
    ResourceLifecycleEvent,
    ResourceReadinessTransitionEvent,
)
from cosecha.core.runtime_profiles import RuntimeReadinessPolicy
from cosecha.core.serialization import (
    decode_json,
    encode_json_bytes,
    from_builtins_dict,
    to_builtins_dict,
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Awaitable, Callable, Iterable

    from cosecha.core.domain_event_stream import DomainEventStream
    from cosecha.core.telemetry import TelemetryStream


type ResourceScope = Literal['run', 'worker', 'test', 'session']
type EffectiveResourceScope = Literal['run', 'worker', 'test']
type ResourceProvisionMode = Literal['live', 'ephemeral', 'dry_run']
type ResourceInitializationMode = Literal['data_seed', 'state_snapshot']
type ResourceFactory = Callable[[], Awaitable[Any] | Any]
type ResourceCleanup = Callable[[Any], Awaitable[None] | None]
type ResourceEventMetadataProvider = Callable[
    [ResourceRequirement, str, str | None],
    DomainEventMetadata,
]
type ResourceStateRecorder = Callable[
    [
        Literal['pending', 'pending_cleared', 'acquired', 'released'],
        str,
        EffectiveResourceScope,
        str | None,
    ],
    None,
]
RESOURCE_TIMING_PATH = Path('.cosecha/resource_timings.json')
RESOURCE_MATERIALIZATION_SNAPSHOT_VERSION = 1


class ResourceError(RuntimeError):
    __slots__ = (
        'code',
        'resource_name',
        'unhealthy',
    )
    recoverable = False
    fatal = False

    def __init__(
        self,
        resource_name: str,
        message: str,
        *,
        code: str,
        unhealthy: bool = True,
    ) -> None:
        super().__init__(message)
        self.resource_name = resource_name
        self.code = code
        self.unhealthy = unhealthy


class ResourceProvider(Protocol):
    def supports_mode(
        self,
        mode: ResourceProvisionMode,
    ) -> bool: ...

    def acquire(
        self,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[Any] | Any: ...

    def reserve_external_handle(
        self,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[str | None] | str | None: ...

    def discard_reserved_external_handle(
        self,
        external_handle: str,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[None] | None: ...

    def release(
        self,
        resource: object,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[None] | None: ...

    def health_check(
        self,
        resource: object,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[bool] | bool: ...

    def verify_integrity(
        self,
        resource: object,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[bool] | bool: ...

    def describe_external_handle(
        self,
        resource: object,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> str | None: ...

    def reap_orphan(
        self,
        external_handle: str,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[None] | None: ...

    def revoke_orphan_access(
        self,
        external_handle: str,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[None] | None: ...


@dataclass(slots=True, frozen=True)
class CallableResourceProvider:
    setup: ResourceFactory
    cleanup: ResourceCleanup | None = None

    def supports_mode(
        self,
        mode: ResourceProvisionMode,
    ) -> bool:
        return mode == 'live'

    def acquire(
        self,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[Any] | Any:
        del requirement, mode
        return self.setup()

    def reserve_external_handle(
        self,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> str | None:
        del requirement, mode
        return None

    def discard_reserved_external_handle(
        self,
        external_handle: str,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> None:
        del external_handle, requirement, mode

    def release(
        self,
        resource: object,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> Awaitable[None] | None:
        del requirement, mode
        if self.cleanup is None:
            return None

        return self.cleanup(resource)

    def health_check(
        self,
        resource: object,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> bool:
        del resource, requirement, mode
        return True

    def verify_integrity(
        self,
        resource: object,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> bool:
        del resource, requirement, mode
        return True

    def describe_external_handle(
        self,
        resource: object,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> str | None:
        del resource, requirement, mode
        return None

    def reap_orphan(
        self,
        external_handle: str,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> None:
        del external_handle, requirement, mode

    def revoke_orphan_access(
        self,
        external_handle: str,
        requirement: ResourceRequirement,
        *,
        mode: ResourceProvisionMode,
    ) -> None:
        del external_handle, requirement, mode


@dataclass(slots=True, frozen=True)
class ResourceRequirement:
    name: str
    setup: ResourceFactory | None = None
    cleanup: ResourceCleanup | None = None
    provider: ResourceProvider | None = None
    scope: ResourceScope = 'test'
    mode: ResourceProvisionMode = 'live'
    config: dict[str, object] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    initializes_from: tuple[str, ...] = ()
    initialization_mode: ResourceInitializationMode = 'data_seed'
    initialization_timeout_seconds: float | None = None
    readiness_policy: RuntimeReadinessPolicy = field(
        default_factory=RuntimeReadinessPolicy,
    )
    conflicts_with: tuple[str, ...] = ()
    requires_orphan_fencing: bool = False

    def __post_init__(self) -> None:
        if self.provider is None and self.setup is None:
            msg = f'Resource {self.name!r} requires either a provider or setup'
            raise ValueError(msg)

        if self.provider is not None and self.setup is not None:
            msg = (
                f'Resource {self.name!r} cannot define both provider and '
                'setup/cleanup'
            )
            raise ValueError(msg)
        if self.initialization_timeout_seconds is not None and (
            self.initialization_timeout_seconds < 0
        ):
            msg = (
                f'Resource {self.name!r} defines a negative '
                'initialization_timeout_seconds'
            )
            raise ValueError(msg)
        if self.requires_orphan_fencing and self.scope not in {
            'worker',
            'test',
            'session',
        }:
            msg = (
                f'Resource {self.name!r} requires orphan fencing but uses '
                f'unsupported scope {self.scope!r}'
            )
            raise ValueError(msg)

    def resolve_provider(self) -> ResourceProvider:
        if self.provider is not None:
            return self.provider

        setup = self.setup
        if setup is None:  # pragma: no cover
            msg = f'Resource {self.name!r} is missing setup'
            raise ValueError(msg)

        return CallableResourceProvider(setup, self.cleanup)


@dataclass(slots=True, frozen=True)
class ResourceTiming:
    name: str
    scope: ResourceScope
    acquire_count: int = 0
    acquire_duration: float = 0.0
    release_count: int = 0
    release_duration: float = 0.0

    @property
    def total_duration(self) -> float:
        return self.acquire_duration + self.release_duration

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ResourceTiming:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ResourceReadinessState:
    name: str
    scope: EffectiveResourceScope
    status: Literal[
        'starting',
        'ready',
        'degraded',
        'unhealthy',
        'unhealthy_local',
    ]
    reason: str | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ResourceReadinessState:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class ResourceMaterializationSnapshot:
    name: str
    scope: EffectiveResourceScope
    mode: ResourceProvisionMode
    connection_data: object
    provider_name: str | None = None
    external_handle: str | None = None
    snapshot_version: int = RESOURCE_MATERIALIZATION_SNAPSHOT_VERSION

    def __post_init__(self) -> None:
        if self.snapshot_version != RESOURCE_MATERIALIZATION_SNAPSHOT_VERSION:
            msg = (
                'Unsupported resource materialization snapshot version: '
                f'{self.snapshot_version}'
            )
            raise ValueError(msg)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> ResourceMaterializationSnapshot:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class _PendingResourceAcquisition:
    requirement: ResourceRequirement
    provider: ResourceProvider
    normalized_scope: EffectiveResourceScope
    resource: object
    timing: ResourceTiming
    created: bool
    external_handle: str | None = None


class ResourceManager:
    __slots__ = (
        '_domain_event_stream',
        '_event_metadata_provider',
        '_legacy_session_scope',
        '_mark_local_failures',
        '_materialization_snapshots',
        '_readiness_states',
        '_resource_timings',
        '_resource_state_recorder',
        '_shared_cleanup',
        '_shared_locks',
        '_shared_resources',
        '_telemetry_stream',
        '_test_cleanup',
        '_test_observed_timings',
        '_test_resources',
        '_unhealthy_resources',
        '_unsupported_scopes',
    )

    def __init__(
        self,
        *,
        legacy_session_scope: EffectiveResourceScope = 'run',
        mark_local_failures: bool = False,
        unsupported_scopes: tuple[EffectiveResourceScope, ...] = (),
    ) -> None:
        self._legacy_session_scope = legacy_session_scope
        self._mark_local_failures = mark_local_failures
        self._unsupported_scopes = frozenset(unsupported_scopes)
        self._resource_timings: dict[
            tuple[str, EffectiveResourceScope],
            ResourceTiming,
        ] = {}
        self._materialization_snapshots: dict[
            EffectiveResourceScope,
            dict[str, ResourceMaterializationSnapshot],
        ] = {'run': {}, 'worker': {}, 'test': {}}
        self._readiness_states: dict[
            tuple[str, EffectiveResourceScope],
            ResourceReadinessState,
        ] = {}
        self._shared_locks: dict[
            tuple[EffectiveResourceScope, str],
            asyncio.Lock,
        ] = {}
        self._shared_resources: dict[
            EffectiveResourceScope,
            dict[str, object],
        ] = {'run': {}, 'worker': {}}
        self._shared_cleanup: dict[
            EffectiveResourceScope,
            list[tuple[ResourceRequirement, ResourceProvider, object]],
        ] = {'run': [], 'worker': []}
        self._test_resources: dict[str, dict[str, object]] = {}
        self._test_cleanup: dict[
            str,
            list[tuple[ResourceRequirement, ResourceProvider, object]],
        ] = {}
        self._test_observed_timings: dict[
            str,
            dict[tuple[str, EffectiveResourceScope], ResourceTiming],
        ] = {}
        self._unhealthy_resources: dict[str, ResourceError] = {}
        self._domain_event_stream: DomainEventStream | None = None
        self._event_metadata_provider: ResourceEventMetadataProvider | None = (
            None
        )
        self._telemetry_stream: TelemetryStream | None = None
        self._resource_state_recorder: ResourceStateRecorder | None = None

    def bind_domain_event_stream(
        self,
        domain_event_stream: DomainEventStream,
    ) -> None:
        self._domain_event_stream = domain_event_stream

    def bind_domain_event_metadata_provider(
        self,
        metadata_provider: ResourceEventMetadataProvider,
    ) -> None:
        self._event_metadata_provider = metadata_provider

    def bind_telemetry_stream(
        self,
        telemetry_stream: TelemetryStream,
    ) -> None:
        self._telemetry_stream = telemetry_stream

    def bind_resource_state_recorder(
        self,
        recorder: ResourceStateRecorder,
    ) -> None:
        self._resource_state_recorder = recorder

    def pop_test_observed_timings(
        self,
        test_id: str,
    ) -> tuple[ResourceTiming, ...]:
        observed = self._test_observed_timings.pop(test_id, {})
        return tuple(
            observed[key]
            for key in sorted(observed, key=lambda item: (item[1], item[0]))
        )

    def build_resource_timing_snapshot(self) -> tuple[ResourceTiming, ...]:
        return tuple(
            self._resource_timings[key]
            for key in sorted(
                self._resource_timings,
                key=lambda item: (item[1], item[0]),
            )
        )

    def build_unhealthy_resource_snapshot(self) -> tuple[str, ...]:
        return tuple(sorted(self._unhealthy_resources))

    async def probe_local_health(self) -> tuple[ResourceError, ...]:
        failures: list[ResourceError] = []
        seen_requirements: set[tuple[str, EffectiveResourceScope]] = set()
        cleanup_groups = (
            tuple(self._shared_cleanup['run']),
            tuple(self._shared_cleanup['worker']),
            *tuple(
                tuple(cleanup_items)
                for cleanup_items in self._test_cleanup.values()
            ),
        )
        for cleanup_items in cleanup_groups:
            for requirement, provider, resource in cleanup_items:
                normalized_scope = normalize_resource_scope(
                    requirement.scope,
                    legacy_session_scope=self._legacy_session_scope,
                )
                key = (requirement.name, normalized_scope)
                if key in seen_requirements:
                    continue
                seen_requirements.add(key)
                try:
                    await self._check_provider_health(
                        provider,
                        requirement,
                        resource,
                        local=self._mark_local_failures,
                    )
                except ResourceError as error:
                    failures.append(error)
        return tuple(failures)

    def build_readiness_snapshot(self) -> tuple[ResourceReadinessState, ...]:
        return tuple(
            self._readiness_states[key]
            for key in sorted(
                self._readiness_states,
                key=lambda item: (item[1], item[0]),
            )
        )

    def bind_materialization_snapshots(
        self,
        snapshots: Iterable[ResourceMaterializationSnapshot],
    ) -> None:
        self._materialization_snapshots = {'run': {}, 'worker': {}, 'test': {}}
        for snapshot in snapshots:
            self._materialization_snapshots[snapshot.scope][snapshot.name] = (
                snapshot
            )

    async def build_materialization_snapshot(
        self,
        *,
        scopes: Iterable[EffectiveResourceScope] = ('run', 'worker'),
    ) -> tuple[ResourceMaterializationSnapshot, ...]:
        snapshots: list[ResourceMaterializationSnapshot] = []
        for scope in scopes:
            if scope not in {'run', 'worker'}:
                continue

            for requirement, provider, resource in sorted(
                self._shared_cleanup[scope],
                key=lambda item: item[0].name,
            ):
                snapshots.append(
                    await _build_resource_materialization_snapshot(
                        provider,
                        resource,
                        requirement,
                    ),
                )

        return tuple(snapshots)

    def merge_observed_timings(
        self,
        resource_timings: Iterable[ResourceTiming],
    ) -> None:
        for timing in resource_timings:
            self._record_global_timing(timing)

    async def acquire_for_test(
        self,
        test_id: str,
        requirements: Iterable[ResourceRequirement],
        *,
        parent_span_id: str | None = None,
        telemetry_attributes: dict[str, object] | None = None,
    ) -> dict[str, object]:
        acquired: dict[str, object] = {}
        requirement_levels = build_resource_dependency_levels(requirements)
        for requirement_level in requirement_levels:
            pending = await self._acquire_requirement_level(
                test_id,
                requirement_level,
                parent_span_id=parent_span_id,
                telemetry_attributes=telemetry_attributes,
            )
            for item in pending:
                acquired[item.requirement.name] = item.resource

        return acquired

    async def release_for_test(
        self,
        test_id: str,
        *,
        parent_span_id: str | None = None,
        telemetry_attributes: dict[str, object] | None = None,
    ) -> None:
        cleanup_items = self._test_cleanup.pop(test_id, [])
        cleanup_by_name = {
            requirement.name: (requirement, provider, resource)
            for requirement, provider, resource in cleanup_items
        }
        if cleanup_by_name:
            requirement_levels = _build_resource_release_levels(
                tuple(
                    requirement
                    for requirement, _provider, _resource in (
                        cleanup_by_name.values()
                    )
                ),
            )
            for requirement_level in reversed(requirement_levels):
                await self._release_requirement_level(
                    test_id,
                    tuple(
                        cleanup_by_name[requirement.name]
                        for requirement in requirement_level
                    ),
                    parent_span_id=parent_span_id,
                    telemetry_attributes=telemetry_attributes,
                )
        self._test_resources.pop(test_id, None)

    async def close(self) -> None:
        for scope in ('worker', 'run'):
            cleanup_items = self._shared_cleanup[scope]
            cleanup_by_name = {
                requirement.name: (requirement, provider, resource)
                for requirement, provider, resource in cleanup_items
            }
            if cleanup_by_name:
                requirement_levels = _build_resource_release_levels(
                    tuple(
                        requirement
                        for requirement, _provider, _resource in (
                            cleanup_by_name.values()
                        )
                    ),
                )
                for requirement_level in reversed(requirement_levels):
                    await self._release_requirement_level(
                        None,
                        tuple(
                            cleanup_by_name[requirement.name]
                            for requirement in requirement_level
                        ),
                    )
            cleanup_items.clear()
            self._shared_resources[scope].clear()

    async def _acquire_requirement_level(
        self,
        test_id: str,
        requirements: tuple[ResourceRequirement, ...],
        *,
        parent_span_id: str | None = None,
        telemetry_attributes: dict[str, object] | None = None,
    ) -> tuple[_PendingResourceAcquisition, ...]:
        for requirement in requirements:
            self._ensure_resource_available(requirement)

        results = await asyncio.gather(
            *(
                self._prepare_requirement_acquisition(
                    test_id,
                    requirement,
                    parent_span_id=parent_span_id,
                    telemetry_attributes=telemetry_attributes,
                )
                for requirement in requirements
            ),
            return_exceptions=True,
        )
        failures = tuple(
            result for result in results if isinstance(result, BaseException)
        )
        pending = tuple(
            result
            for result in results
            if isinstance(result, _PendingResourceAcquisition)
        )
        if failures:
            await asyncio.gather(
                *(
                    self._rollback_pending_acquisition(item)
                    for item in pending
                    if item.created
                ),
                return_exceptions=True,
            )
            if len(failures) == 1:
                raise failures[0]

            msg = 'Resource acquisition level failed'
            raise ExceptionGroup(msg, list(failures))

        for item in pending:
            await self._commit_pending_acquisition(
                test_id,
                item,
            )

        return pending

    async def _prepare_requirement_acquisition(
        self,
        test_id: str,
        requirement: ResourceRequirement,
        *,
        parent_span_id: str | None = None,
        telemetry_attributes: dict[str, object] | None = None,
    ) -> _PendingResourceAcquisition:
        normalized_scope = normalize_resource_scope(
            requirement.scope,
            legacy_session_scope=self._legacy_session_scope,
        )
        provider = requirement.resolve_provider()
        self._ensure_supported_scope(normalized_scope, requirement.name)
        self._ensure_supported_mode(
            provider,
            requirement,
            normalized_scope=normalized_scope,
        )
        if normalized_scope in {'run', 'worker'}:
            return await self._prepare_shared_requirement_acquisition(
                requirement,
                normalized_scope=normalized_scope,
                parent_span_id=parent_span_id,
                telemetry_attributes=telemetry_attributes,
            )

        if test_id not in self._test_resources:
            self._test_resources[test_id] = {}
            self._test_cleanup[test_id] = []

        if requirement.name in self._test_resources[test_id]:
            return _PendingResourceAcquisition(
                requirement=requirement,
                provider=provider,
                normalized_scope='test',
                resource=self._test_resources[test_id][requirement.name],
                timing=ResourceTiming(
                    name=requirement.name,
                    scope='test',
                ),
                created=False,
                external_handle=None,
            )

        resource, timing = await self._create_resource(
            test_id,
            requirement,
            normalized_scope='test',
            parent_span_id=parent_span_id,
            telemetry_attributes=telemetry_attributes,
        )
        external_handle = _describe_external_handle(
            provider,
            resource,
            requirement,
        )
        self._record_resource_state(
            action='acquired',
            name=requirement.name,
            scope='test',
            external_handle=external_handle,
        )
        return _PendingResourceAcquisition(
            requirement=requirement,
            provider=provider,
            normalized_scope='test',
            resource=resource,
            timing=timing,
            created=True,
            external_handle=external_handle,
        )

    async def _prepare_shared_requirement_acquisition(
        self,
        requirement: ResourceRequirement,
        *,
        normalized_scope: EffectiveResourceScope,
        parent_span_id: str | None = None,
        telemetry_attributes: dict[str, object] | None = None,
    ) -> _PendingResourceAcquisition:
        provider = requirement.resolve_provider()
        shared_resources = self._shared_resources[normalized_scope]
        if requirement.name in shared_resources:
            await self._mark_resource_ready(
                requirement.name,
                normalized_scope,
            )
            return _PendingResourceAcquisition(
                requirement=requirement,
                provider=provider,
                normalized_scope=normalized_scope,
                resource=shared_resources[requirement.name],
                timing=ResourceTiming(
                    name=requirement.name,
                    scope=normalized_scope,
                ),
                created=False,
                external_handle=None,
            )

        lock = self._shared_locks.setdefault(
            (normalized_scope, requirement.name),
            asyncio.Lock(),
        )
        lock_acquired = False
        try:
            await _acquire_initialization_lock(lock, requirement)
            lock_acquired = True
            if requirement.name in shared_resources:
                await self._mark_resource_ready(
                    requirement.name,
                    normalized_scope,
                )
                return _PendingResourceAcquisition(
                    requirement=requirement,
                    provider=provider,
                    normalized_scope=normalized_scope,
                    resource=shared_resources[requirement.name],
                    timing=ResourceTiming(
                        name=requirement.name,
                        scope=normalized_scope,
                    ),
                    created=False,
                    external_handle=None,
                )

            materialization_snapshot = self._materialization_snapshots[
                normalized_scope
            ].get(requirement.name)
            if materialization_snapshot is not None:
                resource, timing = await self._rehydrate_resource(
                    requirement,
                    materialization_snapshot,
                    normalized_scope=normalized_scope,
                )
            else:
                resource, timing = await self._create_resource(
                    None,
                    requirement,
                    normalized_scope=normalized_scope,
                    parent_span_id=parent_span_id,
                    telemetry_attributes=telemetry_attributes,
                )
            external_handle = _describe_external_handle(
                provider,
                resource,
                requirement,
            )
            self._record_resource_state(
                action='acquired',
                name=requirement.name,
                scope=normalized_scope,
                external_handle=external_handle,
            )
            return _PendingResourceAcquisition(
                requirement=requirement,
                provider=provider,
                normalized_scope=normalized_scope,
                resource=resource,
                timing=timing,
                created=True,
                external_handle=external_handle,
            )
        finally:
            if lock_acquired:
                lock.release()

    async def _rehydrate_resource(
        self,
        requirement: ResourceRequirement,
        snapshot: ResourceMaterializationSnapshot,
        *,
        normalized_scope: EffectiveResourceScope,
    ) -> tuple[object, ResourceTiming]:
        provider = requirement.resolve_provider()
        setup_start = time.perf_counter()
        try:
            resource = await _rehydrate_resource_materialization(
                provider,
                snapshot,
                requirement,
            )
        except ResourceError:
            raise
        except Exception as error:
            msg = (
                f'Failed to rehydrate resource {requirement.name!r} from '
                'materialization snapshot'
            )
            raise ResourceError(
                requirement.name,
                msg,
                code='resource_rehydrate_failed',
            ) from error

        await self._check_provider_health(
            provider,
            requirement,
            resource,
            local=self._mark_local_failures,
        )
        return (
            resource,
            ResourceTiming(
                name=requirement.name,
                scope=normalized_scope,
                acquire_count=1,
                acquire_duration=time.perf_counter() - setup_start,
            ),
        )

    async def _commit_pending_acquisition(
        self,
        test_id: str,
        item: _PendingResourceAcquisition,
    ) -> None:
        requirement = item.requirement
        if not item.created:
            return

        if item.normalized_scope in {'run', 'worker'}:
            self._shared_resources[item.normalized_scope][requirement.name] = (
                item.resource
            )
            self._shared_cleanup[item.normalized_scope].append(
                (requirement, item.provider, item.resource),
            )
        else:
            self._test_resources[test_id][requirement.name] = item.resource
            self._test_cleanup[test_id].append(
                (requirement, item.provider, item.resource),
            )

        self._record_timing(test_id, item.timing)
        if self._domain_event_stream is None:
            return

        test_id_value = test_id if item.normalized_scope == 'test' else None
        scope = item.normalized_scope
        external_handle = item.external_handle
        await self._domain_event_stream.emit(
            ResourceLifecycleEvent(
                action='acquired',
                name=requirement.name,
                scope=scope,
                test_id=test_id_value,
                external_handle=external_handle,
                metadata=self._build_event_metadata(
                    requirement,
                    scope,
                    test_id_value,
                ),
            ),
        )

    async def _rollback_pending_acquisition(
        self,
        item: _PendingResourceAcquisition,
    ) -> None:
        await _await_if_needed(
            item.provider.release(
                item.resource,
                item.requirement,
                mode=item.requirement.mode,
            ),
        )
        self._record_resource_state(
            action='released',
            name=item.requirement.name,
            scope=item.normalized_scope,
            external_handle=item.external_handle,
        )

    async def _release_requirement_level(
        self,
        test_id: str | None,
        cleanup_items: tuple[
            tuple[ResourceRequirement, ResourceProvider, object],
            ...,
        ],
        *,
        parent_span_id: str | None = None,
        telemetry_attributes: dict[str, object] | None = None,
    ) -> None:
        results = await asyncio.gather(
            *(
                self._release_single_requirement(
                    test_id,
                    (requirement, provider, resource),
                    parent_span_id=parent_span_id,
                    telemetry_attributes=telemetry_attributes,
                )
                for requirement, provider, resource in cleanup_items
            ),
            return_exceptions=True,
        )
        failures = [
            result for result in results if isinstance(result, BaseException)
        ]
        if not failures:
            return

        if len(failures) == 1:
            raise failures[0]

        msg = 'Resource release level failed'
        raise ExceptionGroup(msg, failures)

    async def _release_single_requirement(
        self,
        test_id: str | None,
        cleanup_item: tuple[ResourceRequirement, ResourceProvider, object],
        *,
        parent_span_id: str | None = None,
        telemetry_attributes: dict[str, object] | None = None,
    ) -> None:
        requirement, provider, resource = cleanup_item
        cleanup_start = time.perf_counter()
        name = requirement.name
        normalized_scope = normalize_resource_scope(
            requirement.scope,
            legacy_session_scope=self._legacy_session_scope,
        )
        if self._telemetry_stream is None:
            await _await_if_needed(
                provider.release(
                    resource,
                    requirement,
                    mode=requirement.mode,
                ),
            )
        else:
            attributes = {
                'resource': name,
                'resource_mode': requirement.mode,
                'scope': normalized_scope,
                **(telemetry_attributes or {}),
            }
            async with self._telemetry_stream.span(
                f'resource.release.{name}',
                parent_span_id=parent_span_id,
                attributes=attributes,
            ):
                await _await_if_needed(
                    provider.release(
                        resource,
                        requirement,
                        mode=requirement.mode,
                    ),
                )

        timing = ResourceTiming(
            name=name,
            scope=normalized_scope,
            release_count=1,
            release_duration=time.perf_counter() - cleanup_start,
        )
        if test_id is None:
            self._record_global_timing(timing)
        else:
            self._record_timing(test_id, timing)
        if self._domain_event_stream is not None:
            external_handle = _describe_external_handle(
                provider,
                resource,
                requirement,
            )
            self._record_resource_state(
                action='released',
                name=name,
                scope=normalized_scope,
                external_handle=external_handle,
            )
            await self._domain_event_stream.emit(
                ResourceLifecycleEvent(
                    action='released',
                    name=name,
                    scope=normalized_scope,
                    test_id=test_id,
                    external_handle=external_handle,
                    metadata=self._build_event_metadata(
                        requirement,
                        normalized_scope,
                        test_id,
                    ),
                ),
            )

    async def _create_resource(
        self,
        test_id: str | None,
        requirement: ResourceRequirement,
        normalized_scope: EffectiveResourceScope,
        *,
        parent_span_id: str | None = None,
        telemetry_attributes: dict[str, object] | None = None,
    ) -> tuple[object, ResourceTiming]:
        provider = requirement.resolve_provider()
        self._ensure_orphan_fencing_preconditions(
            provider,
            requirement,
            normalized_scope=normalized_scope,
        )
        setup_start = time.perf_counter()
        reserved_external_handle = await _reserve_external_handle(
            provider,
            requirement,
        )
        self._record_resource_state(
            action='pending',
            name=requirement.name,
            scope=normalized_scope,
            external_handle=reserved_external_handle,
        )
        try:
            if self._telemetry_stream is None:
                resource = await _await_if_needed(
                    provider.acquire(
                        requirement,
                        mode=requirement.mode,
                    ),
                )
                await self._initialize_resource_if_needed(
                    test_id,
                    requirement,
                    resource,
                )
                await self._check_provider_health(
                    provider,
                    requirement,
                    resource,
                    local=self._mark_local_failures,
                )
                return (
                    resource,
                    ResourceTiming(
                        name=requirement.name,
                        scope=normalized_scope,
                        acquire_count=1,
                        acquire_duration=time.perf_counter() - setup_start,
                    ),
                )

            attributes = {
                'resource': requirement.name,
                'resource_mode': requirement.mode,
                'scope': normalized_scope,
                **(telemetry_attributes or {}),
            }
            async with self._telemetry_stream.span(
                f'resource.acquire.{requirement.name}',
                parent_span_id=parent_span_id,
                attributes=attributes,
            ):
                resource = await _await_if_needed(
                    provider.acquire(
                        requirement,
                        mode=requirement.mode,
                    ),
                )
                await self._initialize_resource_if_needed(
                    test_id,
                    requirement,
                    resource,
                )
                await self._check_provider_health(
                    provider,
                    requirement,
                    resource,
                    local=self._mark_local_failures,
                )

            return (
                resource,
                ResourceTiming(
                    name=requirement.name,
                    scope=normalized_scope,
                    acquire_count=1,
                    acquire_duration=time.perf_counter() - setup_start,
                ),
            )
        except Exception:
            if reserved_external_handle is not None:
                await _discard_reserved_external_handle(
                    provider,
                    reserved_external_handle,
                    requirement,
                )
            self._record_resource_state(
                action='pending_cleared',
                name=requirement.name,
                scope=normalized_scope,
                external_handle=reserved_external_handle,
            )
            raise

    async def _initialize_resource_if_needed(
        self,
        test_id: str,
        requirement: ResourceRequirement,
        resource: object,
    ) -> None:
        if not requirement.initializes_from:
            return

        provider = requirement.resolve_provider()
        initialize_from = getattr(provider, 'initialize_from', None)
        if initialize_from is None:
            msg = (
                f'Resource {requirement.name!r} declares initializes_from '
                'but its provider does not implement initialize_from()'
            )
            raise ResourceError(
                requirement.name,
                msg,
                code='resource_initialize_from_unsupported',
            )

        supported_modes = getattr(
            provider,
            'supported_initialization_modes',
            None,
        )
        if supported_modes is not None:
            declared_modes = tuple(
                str(mode_name)
                for mode_name in _call_supported_initialization_modes(
                    supported_modes,
                    requirement,
                )
            )
            if requirement.initialization_mode not in declared_modes:
                msg = (
                    f'Resource {requirement.name!r} does not support '
                    f'initialization mode {requirement.initialization_mode!r}'
                )
                raise ResourceError(
                    requirement.name,
                    msg,
                    code='resource_initialization_mode_unsupported',
                )

        await self._mark_resource_starting(
            requirement.name,
            normalize_resource_scope(
                requirement.scope,
                legacy_session_scope=self._legacy_session_scope,
            ),
        )
        initialization_sources = await self._resolve_initialization_sources(
            test_id,
            requirement,
        )
        try:
            await _await_initialization(
                initialize_from(
                    resource,
                    requirement,
                    initialization_sources,
                    mode=requirement.mode,
                    initialization_mode=requirement.initialization_mode,
                ),
                requirement,
            )
        except TimeoutError as error:
            raise ResourceError(
                requirement.name,
                (
                    f'Resource {requirement.name!r} initialization timed out '
                    f'after {requirement.initialization_timeout_seconds}s'
                ),
                code='resource_initialization_timeout',
            ) from error

    def _record_timing(
        self,
        test_id: str,
        timing: ResourceTiming,
    ) -> None:
        self._record_global_timing(timing)
        test_timings = self._test_observed_timings.setdefault(test_id, {})
        key = (timing.name, timing.scope)
        test_timings[key] = _merge_resource_timing(
            test_timings.get(key),
            timing,
        )

    def _record_global_timing(
        self,
        timing: ResourceTiming,
    ) -> None:
        normalized_timing = _normalize_resource_timing(
            timing,
            legacy_session_scope=self._legacy_session_scope,
        )
        key = (normalized_timing.name, normalized_timing.scope)
        self._resource_timings[key] = _merge_resource_timing(
            self._resource_timings.get(key),
            normalized_timing,
        )

    def _record_resource_state(
        self,
        *,
        action: Literal['pending', 'pending_cleared', 'acquired', 'released'],
        name: str,
        scope: EffectiveResourceScope,
        external_handle: str | None,
    ) -> None:
        if self._resource_state_recorder is None:
            return

        self._resource_state_recorder(action, name, scope, external_handle)

    def _ensure_supported_scope(
        self,
        scope: EffectiveResourceScope,
        name: str,
    ) -> None:
        if scope not in self._unsupported_scopes:
            return

        msg = (
            f'Resource {name!r} uses unsupported scope {scope!r} '
            'for this runtime'
        )
        raise ValueError(msg)

    def _ensure_supported_mode(
        self,
        provider: ResourceProvider,
        requirement: ResourceRequirement,
        *,
        normalized_scope: EffectiveResourceScope,
    ) -> None:
        del normalized_scope
        if provider.supports_mode(requirement.mode):
            return

        msg = (
            f'Resource {requirement.name!r} does not support mode '
            f'{requirement.mode!r}'
        )
        raise ValueError(msg)

    def _ensure_orphan_fencing_preconditions(
        self,
        provider: ResourceProvider,
        requirement: ResourceRequirement,
        *,
        normalized_scope: EffectiveResourceScope,
    ) -> None:
        del normalized_scope
        if not requirement.requires_orphan_fencing:
            return

        if not _supports_orphan_fencing(provider, requirement):
            msg = (
                f'Resource {requirement.name!r} requires orphan fencing but '
                'its provider does not support revoke_orphan_access()'
            )
            raise ResourceError(
                requirement.name,
                msg,
                code='resource_orphan_fencing_unsupported',
            )

        if not _supports_external_handle_reservation(provider, requirement):
            msg = (
                f'Resource {requirement.name!r} requires orphan fencing but '
                'its provider does not support reserve_external_handle()'
            )
            raise ResourceError(
                requirement.name,
                msg,
                code='resource_orphan_handle_reservation_unsupported',
            )

    def _ensure_resource_available(
        self,
        requirement: ResourceRequirement,
    ) -> None:
        error = self._unhealthy_resources.get(requirement.name)
        if error is not None:
            raise error

    async def _check_provider_health(
        self,
        provider: ResourceProvider,
        requirement: ResourceRequirement,
        resource: object,
        *,
        local: bool = False,
    ) -> None:
        normalized_scope = normalize_resource_scope(
            requirement.scope,
            legacy_session_scope=self._legacy_session_scope,
        )
        policy = requirement.readiness_policy

        if policy.initial_delay_seconds > 0:
            await asyncio.sleep(policy.initial_delay_seconds)

        started_at = time.perf_counter()
        while True:
            is_healthy = await _await_if_needed(
                provider.health_check(
                    resource,
                    requirement,
                    mode=requirement.mode,
                ),
            )
            if not is_healthy:
                error = ResourceError(
                    requirement.name,
                    (
                        f'Resource {requirement.name!r} failed health check '
                        f'in mode {requirement.mode!r}'
                    ),
                    code='resource_health_check_failed',
                )
                if not await self._retry_readiness_check_if_needed(
                    requirement,
                    started_at=started_at,
                    error=error,
                ):
                    self._unhealthy_resources[requirement.name] = error
                    if local:
                        await self._mark_resource_unhealthy_local(
                            requirement.name,
                            normalized_scope,
                            error.code,
                        )
                    else:
                        await self._mark_resource_unhealthy(
                            requirement.name,
                            normalized_scope,
                            error.code,
                        )
                    raise error
                continue

            is_integral = await _await_if_needed(
                provider.verify_integrity(
                    resource,
                    requirement,
                    mode=requirement.mode,
                ),
            )
            if is_integral:
                await self._mark_resource_ready(
                    requirement.name,
                    normalized_scope,
                )
                return

            elapsed = time.perf_counter() - started_at
            if policy.degraded_timeout_seconds is not None and (
                elapsed >= policy.degraded_timeout_seconds
            ):
                await self._mark_resource_degraded(
                    requirement.name,
                    normalized_scope,
                    'resource_integrity_check_failed',
                )
                return

            error = ResourceError(
                requirement.name,
                (
                    f'Resource {requirement.name!r} failed integrity check '
                    f'in mode {requirement.mode!r}'
                ),
                code='resource_integrity_check_failed',
            )
            if not await self._retry_readiness_check_if_needed(
                requirement,
                started_at=started_at,
                error=error,
            ):
                self._unhealthy_resources[requirement.name] = error
                if local:
                    await self._mark_resource_unhealthy_local(
                        requirement.name,
                        normalized_scope,
                        error.code,
                    )
                else:
                    await self._mark_resource_unhealthy(
                        requirement.name,
                        normalized_scope,
                        error.code,
                    )
                raise error

    async def _retry_readiness_check_if_needed(
        self,
        requirement: ResourceRequirement,
        *,
        started_at: float,
        error: ResourceError,
    ) -> bool:
        policy = requirement.readiness_policy
        retry_interval = policy.retry_interval_seconds
        max_wait = policy.max_wait_seconds
        if retry_interval <= 0:
            return False

        if max_wait is None:
            return False

        remaining = max_wait - (time.perf_counter() - started_at)
        if remaining <= 0:
            return False

        await asyncio.sleep(min(retry_interval, remaining))
        return True

    async def _resolve_initialization_sources(
        self,
        test_id: str,
        requirement: ResourceRequirement,
    ) -> dict[str, object]:
        sources: dict[str, object] = {}
        scope_rank = {'test': 0, 'worker': 1, 'run': 2}
        for source_name in requirement.initializes_from:
            source_requirement = self._find_requirement_by_name(source_name)
            if source_requirement is None:
                msg = (
                    f'Resource {requirement.name!r} could not resolve '
                    f'initialization requirement for {source_name!r}'
                )
                raise ResourceError(
                    requirement.name,
                    msg,
                    code='resource_initialization_source_missing',
                )
            if (
                scope_rank[str(source_requirement.scope)]
                < scope_rank[str(requirement.scope)]
            ):
                msg = (
                    f'Resource {requirement.name!r} cannot initialize from '
                    f'narrower-scoped source {source_name!r}'
                )
                raise ResourceError(
                    requirement.name,
                    msg,
                    code='resource_initialization_source_unreachable',
                )

            source_resource = self._shared_resources['run'].get(source_name)
            if source_resource is None:
                source_resource = self._shared_resources['worker'].get(
                    source_name,
                )
            if source_resource is None:
                source_resource = self._test_resources.get(test_id, {}).get(
                    source_name,
                )
            if source_resource is None:
                msg = (
                    f'Resource {requirement.name!r} initializes from missing '
                    f'source {source_name!r}'
                )
                raise ResourceError(
                    requirement.name,
                    msg,
                    code='resource_initialization_source_missing',
                )

            if requirement.initialization_mode == 'state_snapshot':
                source_provider = source_requirement.resolve_provider()
                sources[
                    source_name
                ] = await _build_resource_materialization_snapshot(
                    source_provider,
                    source_resource,
                    source_requirement,
                )
                continue

            sources[source_name] = source_resource

        return sources

    def _find_requirement_by_name(
        self,
        name: str,
    ) -> ResourceRequirement | None:
        for scope in ('run', 'worker'):
            for requirement, _provider, _resource in self._shared_cleanup[
                scope
            ]:
                if requirement.name == name:
                    return requirement

        for cleanup_items in self._test_cleanup.values():
            for requirement, _provider, _resource in cleanup_items:
                if requirement.name == name:
                    return requirement

        return None

    async def _mark_resource_starting(
        self,
        name: str,
        scope: EffectiveResourceScope,
    ) -> None:
        self._readiness_states[(name, scope)] = ResourceReadinessState(
            name=name,
            scope=scope,
            status='starting',
        )
        await self._emit_resource_readiness_transition(
            name=name,
            scope=scope,
            status='starting',
        )

    async def _mark_resource_ready(
        self,
        name: str,
        scope: EffectiveResourceScope,
    ) -> None:
        self._unhealthy_resources.pop(name, None)
        self._readiness_states[(name, scope)] = ResourceReadinessState(
            name=name,
            scope=scope,
            status='ready',
        )
        await self._emit_resource_readiness_transition(
            name=name,
            scope=scope,
            status='ready',
        )

    async def _mark_resource_degraded(
        self,
        name: str,
        scope: EffectiveResourceScope,
        reason: str,
    ) -> None:
        self._readiness_states[(name, scope)] = ResourceReadinessState(
            name=name,
            scope=scope,
            status='degraded',
            reason=reason,
        )
        await self._emit_resource_readiness_transition(
            name=name,
            scope=scope,
            status='degraded',
            reason=reason,
        )

    async def _mark_resource_unhealthy(
        self,
        name: str,
        scope: EffectiveResourceScope,
        reason: str,
    ) -> None:
        self._readiness_states[(name, scope)] = ResourceReadinessState(
            name=name,
            scope=scope,
            status='unhealthy',
            reason=reason,
        )
        await self._emit_resource_readiness_transition(
            name=name,
            scope=scope,
            status='unhealthy',
            reason=reason,
        )

    async def _mark_resource_unhealthy_local(
        self,
        name: str,
        scope: EffectiveResourceScope,
        reason: str,
    ) -> None:
        self._readiness_states[(name, scope)] = ResourceReadinessState(
            name=name,
            scope=scope,
            status='unhealthy_local',
            reason=reason,
        )
        await self._emit_resource_readiness_transition(
            name=name,
            scope=scope,
            status='unhealthy_local',
            reason=reason,
        )

    async def _emit_resource_readiness_transition(
        self,
        *,
        name: str,
        scope: EffectiveResourceScope,
        status: Literal[
            'starting',
            'ready',
            'degraded',
            'unhealthy',
            'unhealthy_local',
        ],
        reason: str | None = None,
    ) -> None:
        if self._domain_event_stream is None:
            return

        await self._domain_event_stream.emit(
            ResourceReadinessTransitionEvent(
                name=name,
                scope=scope,
                status=status,
                reason=reason,
                metadata=self._build_event_metadata(
                    ResourceRequirement(
                        name=name,
                        provider=CallableResourceProvider(lambda: None),
                        scope=scope,
                    ),
                    scope,
                    None,
                ),
            ),
        )

    def _build_event_metadata(
        self,
        requirement: ResourceRequirement,
        scope: str,
        test_id: str | None,
    ) -> DomainEventMetadata:
        if self._event_metadata_provider is None:
            return DomainEventMetadata()

        return self._event_metadata_provider(requirement, scope, test_id)


async def _await_if_needed(value: Awaitable[Any] | Any) -> Any:
    if hasattr(value, '__await__'):
        return await value

    return value


async def _acquire_initialization_lock(
    lock: asyncio.Lock,
    requirement: ResourceRequirement,
) -> None:
    timeout_seconds = requirement.initialization_timeout_seconds
    if timeout_seconds is None:
        await lock.acquire()
        return

    try:
        await asyncio.wait_for(lock.acquire(), timeout_seconds)
    except TimeoutError as error:
        raise ResourceError(
            requirement.name,
            (
                f'Resource {requirement.name!r} initialization lock timed '
                f'out after {timeout_seconds}s'
            ),
            code='resource_initialization_timeout',
        ) from error


async def _await_initialization(
    value: Awaitable[Any] | Any,
    requirement: ResourceRequirement,
) -> Any:
    timeout_seconds = requirement.initialization_timeout_seconds
    if timeout_seconds is None:
        return await _await_if_needed(value)

    return await asyncio.wait_for(
        _await_if_needed(value),
        timeout_seconds,
    )


def _call_supported_initialization_modes(
    supported_modes,
    requirement: ResourceRequirement,
):
    try:
        modes = supported_modes(requirement)
    except TypeError:
        modes = supported_modes()
    return modes


def _describe_external_handle(
    provider: ResourceProvider,
    resource: object,
    requirement: ResourceRequirement,
) -> str | None:
    describe = getattr(provider, 'describe_external_handle', None)
    if describe is None:
        return None

    return describe(
        resource,
        requirement,
        mode=requirement.mode,
    )


async def _reserve_external_handle(
    provider: ResourceProvider,
    requirement: ResourceRequirement,
) -> str | None:
    reserve = getattr(provider, 'reserve_external_handle', None)
    if reserve is None:
        return None

    return await _await_if_needed(
        reserve(
            requirement,
            mode=requirement.mode,
        ),
    )


async def _discard_reserved_external_handle(
    provider: ResourceProvider,
    external_handle: str,
    requirement: ResourceRequirement,
) -> None:
    discard = getattr(provider, 'discard_reserved_external_handle', None)
    if discard is None:
        return

    await _await_if_needed(
        discard(
            external_handle,
            requirement,
            mode=requirement.mode,
        ),
    )


async def _build_resource_materialization_snapshot(
    provider: ResourceProvider,
    resource: object,
    requirement: ResourceRequirement,
) -> ResourceMaterializationSnapshot:
    snapshot_builder = getattr(provider, 'snapshot_materialization', None)
    try:
        if snapshot_builder is None:
            connection_data = _normalize_materialization_connection_data(
                resource,
            )
        else:
            connection_data = _normalize_materialization_connection_data(
                await _await_if_needed(
                    snapshot_builder(
                        resource,
                        requirement,
                        mode=requirement.mode,
                    ),
                ),
            )
    except ValueError as error:
        msg = (
            f'Resource {requirement.name!r} cannot be materialized for '
            'worker rebind'
        )
        raise ResourceError(
            requirement.name,
            msg,
            code='resource_materialization_failed',
        ) from error

    return ResourceMaterializationSnapshot(
        name=requirement.name,
        scope=normalize_resource_scope(requirement.scope),
        mode=requirement.mode,
        connection_data=connection_data,
        provider_name=type(provider).__name__,
        external_handle=_describe_external_handle(
            provider,
            resource,
            requirement,
        ),
    )


async def _rehydrate_resource_materialization(
    provider: ResourceProvider,
    snapshot: ResourceMaterializationSnapshot,
    requirement: ResourceRequirement,
) -> object:
    if snapshot.snapshot_version != RESOURCE_MATERIALIZATION_SNAPSHOT_VERSION:
        msg = (
            f'Resource {requirement.name!r} uses unsupported materialization '
            f'snapshot version {snapshot.snapshot_version}'
        )
        raise ResourceError(
            requirement.name,
            msg,
            code='resource_snapshot_version_mismatch',
        )

    rehydrate = getattr(provider, 'rehydrate_materialization', None)
    if rehydrate is None:
        return snapshot.connection_data

    return await _await_if_needed(
        rehydrate(
            snapshot.connection_data,
            requirement,
            mode=requirement.mode,
        ),
    )


def _normalize_materialization_connection_data(
    value: object,
) -> object:
    try:
        return decode_json(encode_json_bytes(value))
    except Exception as error:
        msg = (
            'Resource materialization connection data must be JSON '
            'serializable'
        )
        raise ValueError(msg) from error


async def reap_orphaned_resource(
    provider: ResourceProvider,
    external_handle: str,
    requirement: ResourceRequirement,
) -> None:
    revoke = getattr(provider, 'revoke_orphan_access', None)
    if requirement.requires_orphan_fencing and not _supports_orphan_fencing(
        provider,
        requirement,
    ):
        msg = (
            f'Resource {requirement.name!r} requires orphan fencing but '
            'its provider does not support revoke_orphan_access()'
        )
        raise ResourceError(
            requirement.name,
            msg,
            code='resource_orphan_fencing_unsupported',
        )

    if revoke is not None:
        await _await_if_needed(
            revoke(
                external_handle,
                requirement,
                mode=requirement.mode,
            ),
        )

    reap = getattr(provider, 'reap_orphan', None)
    if reap is None:
        return

    await _await_if_needed(
        reap(
            external_handle,
            requirement,
            mode=requirement.mode,
        ),
    )


def _supports_orphan_fencing(
    provider: ResourceProvider,
    requirement: ResourceRequirement,
) -> bool:
    declared_support = getattr(provider, 'supports_orphan_fencing', None)
    if declared_support is not None:
        if callable(declared_support):
            try:
                result = declared_support(
                    requirement,
                    mode=requirement.mode,
                )
            except TypeError:
                result = declared_support()
            return bool(result)
        return bool(declared_support)

    revoke = getattr(provider, 'revoke_orphan_access', None)
    if revoke is None:
        return False

    if isinstance(provider, CallableResourceProvider):
        return (
            type(provider).revoke_orphan_access
            is not CallableResourceProvider.revoke_orphan_access
        )

    return True


def _supports_external_handle_reservation(
    provider: ResourceProvider,
    requirement: ResourceRequirement,
) -> bool:
    del requirement
    reserve = getattr(provider, 'reserve_external_handle', None)
    if reserve is None:
        return False

    if isinstance(provider, CallableResourceProvider):
        return (
            type(provider).reserve_external_handle
            is not CallableResourceProvider.reserve_external_handle
        )

    return callable(reserve)


def _merge_resource_timing(
    current: ResourceTiming | None,
    new: ResourceTiming,
) -> ResourceTiming:
    if current is None:
        return new

    return ResourceTiming(
        name=new.name,
        scope=new.scope,
        acquire_count=current.acquire_count + new.acquire_count,
        acquire_duration=current.acquire_duration + new.acquire_duration,
        release_count=current.release_count + new.release_count,
        release_duration=current.release_duration + new.release_duration,
    )


def normalize_resource_scope(
    scope: ResourceScope,
    *,
    legacy_session_scope: EffectiveResourceScope = 'run',
) -> EffectiveResourceScope:
    if scope == 'session':
        return legacy_session_scope

    return scope


def _normalize_resource_timing(
    timing: ResourceTiming,
    *,
    legacy_session_scope: EffectiveResourceScope,
) -> ResourceTiming:
    normalized_scope = normalize_resource_scope(
        timing.scope,
        legacy_session_scope=legacy_session_scope,
    )
    if normalized_scope == timing.scope:
        return timing

    return ResourceTiming(
        name=timing.name,
        scope=normalized_scope,
        acquire_count=timing.acquire_count,
        acquire_duration=timing.acquire_duration,
        release_count=timing.release_count,
        release_duration=timing.release_duration,
    )


def validate_resource_requirements(
    requirements: Iterable[ResourceRequirement],
) -> tuple[ResourceRequirement, ...]:
    normalized_requirements = tuple(requirements)
    requirements_by_name = {
        requirement.name: requirement
        for requirement in normalized_requirements
    }

    for requirement in normalized_requirements:
        missing_dependencies = sorted(
            dependency
            for dependency in requirement.depends_on
            if dependency not in requirements_by_name
        )
        if missing_dependencies:
            msg = (
                f'Resource {requirement.name!r} depends on missing '
                f'resources: {", ".join(missing_dependencies)}'
            )
            raise ValueError(msg)

        missing_initializers = sorted(
            dependency
            for dependency in requirement.initializes_from
            if dependency not in requirements_by_name
        )
        if missing_initializers:
            msg = (
                f'Resource {requirement.name!r} initializes from missing '
                f'resources: {", ".join(missing_initializers)}'
            )
            raise ValueError(msg)

        for dependency_name in requirement.depends_on:
            dependency = requirements_by_name[dependency_name]
            if _resource_scope_rank(dependency.scope) < _resource_scope_rank(
                requirement.scope,
            ):
                msg = (
                    f'Resource {requirement.name!r} has invalid scope '
                    f'dependency: {requirement.name!r} '
                    f'({requirement.scope}) depends_on '
                    f'{dependency.name!r} ({dependency.scope})'
                )
                raise ValueError(msg)

        for initializer_name in requirement.initializes_from:
            initializer = requirements_by_name[initializer_name]
            if _resource_scope_rank(initializer.scope) < _resource_scope_rank(
                requirement.scope,
            ):
                msg = (
                    f'Resource {requirement.name!r} has invalid '
                    'initialization source scope: '
                    f'{requirement.name!r} ({requirement.scope}) '
                    f'initializes_from {initializer.name!r} '
                    f'({initializer.scope})'
                )
                raise ValueError(msg)

        active_conflicts = sorted(
            conflict
            for conflict in requirement.conflicts_with
            if conflict in requirements_by_name
        )
        if active_conflicts:
            msg = (
                f'Resource {requirement.name!r} conflicts with '
                f'{", ".join(active_conflicts)}'
            )
            raise ValueError(msg)

    _validate_resource_dependency_cycles(
        normalized_requirements,
        requirements_by_name,
        relation_name='resource dependency',
        dependency_getter=lambda requirement: requirement.depends_on,
    )
    _validate_resource_dependency_cycles(
        normalized_requirements,
        requirements_by_name,
        relation_name='resource initialization',
        dependency_getter=lambda requirement: requirement.initializes_from,
    )
    _validate_resource_dependency_cycles(
        normalized_requirements,
        requirements_by_name,
        relation_name='resource dependency',
        dependency_getter=lambda requirement: (
            *requirement.depends_on,
            *requirement.initializes_from,
        ),
    )
    return normalized_requirements


def order_resource_requirements(
    requirements: Iterable[ResourceRequirement],
) -> tuple[ResourceRequirement, ...]:
    requirement_levels = build_resource_dependency_levels(requirements)
    return tuple(
        requirement
        for requirement_level in requirement_levels
        for requirement in requirement_level
    )


def build_resource_dependency_levels(
    requirements: Iterable[ResourceRequirement],
) -> tuple[tuple[ResourceRequirement, ...], ...]:
    return _build_resource_levels(
        requirements,
        include_initializers=True,
    )


def _build_resource_release_levels(
    requirements: Iterable[ResourceRequirement],
) -> tuple[tuple[ResourceRequirement, ...], ...]:
    return _build_resource_levels(
        requirements,
        include_initializers=False,
    )


def _build_resource_levels(
    requirements: Iterable[ResourceRequirement],
    *,
    include_initializers: bool,
) -> tuple[tuple[ResourceRequirement, ...], ...]:
    normalized_requirements = validate_resource_requirements(requirements)
    requirements_by_name = {
        requirement.name: requirement
        for requirement in normalized_requirements
    }
    dependency_depths: dict[str, int] = {}

    def _iter_dependencies(
        requirement: ResourceRequirement,
    ) -> tuple[str, ...]:
        if include_initializers:
            return (
                *requirement.depends_on,
                *requirement.initializes_from,
            )
        return requirement.depends_on

    def _resolve_depth(requirement: ResourceRequirement) -> int:
        cached_depth = dependency_depths.get(requirement.name)
        if cached_depth is not None:
            return cached_depth

        dependencies = _iter_dependencies(requirement)
        if not dependencies:
            dependency_depths[requirement.name] = 0
            return 0

        depth = 1 + max(
            _resolve_depth(requirements_by_name[dependency])
            for dependency in dependencies
        )
        dependency_depths[requirement.name] = depth
        return depth

    levels: dict[int, list[ResourceRequirement]] = {}
    for requirement in normalized_requirements:
        depth = _resolve_depth(requirement)
        levels.setdefault(depth, []).append(requirement)

    return tuple(
        tuple(levels[current_depth]) for current_depth in sorted(levels)
    )


def _validate_resource_dependency_cycles(
    requirements: tuple[ResourceRequirement, ...],
    requirements_by_name: dict[str, ResourceRequirement],
    *,
    relation_name: str,
    dependency_getter: Callable[[ResourceRequirement], tuple[str, ...]],
) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def _visit(requirement: ResourceRequirement) -> None:
        if requirement.name in visited:
            return

        if requirement.name in visiting:
            msg = f'Cyclic {relation_name} detected at {requirement.name!r}'
            raise ValueError(msg)

        visiting.add(requirement.name)
        for dependency in dependency_getter(requirement):
            _visit(requirements_by_name[dependency])
        visiting.remove(requirement.name)
        visited.add(requirement.name)

    for requirement in requirements:
        _visit(requirement)


def _resource_scope_rank(scope: ResourceScope) -> int:
    rank = {
        'test': 0,
        'worker': 1,
        'run': 2,
        'session': 2,
    }.get(str(scope))
    if rank is None:
        msg = f'Unsupported resource scope: {scope!r}'
        raise ValueError(msg)
    return rank
