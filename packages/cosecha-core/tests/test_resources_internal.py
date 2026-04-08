from __future__ import annotations

import asyncio
import time

from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from cosecha.core.resources import (
    CallableResourceProvider,
    ResourceDependencyContext,
    ResourceError,
    ResourceManager,
    ResourceMaterializationSnapshot,
    ResourceRequirement,
    ResourceTiming,
    ResolvedResourceDependency,
    _build_resource_materialization_snapshot,
    _callable_accepts_named_parameter,
    _normalize_materialization_connection_data,
    _rehydrate_resource_materialization,
    _resolve_provider_dependency_names,
    _call_supported_initialization_modes,
    _supports_external_handle_reservation,
    _supports_orphan_fencing,
    validate_resource_requirements,
)
from cosecha.core.runtime_profiles import RuntimeReadinessPolicy


if TYPE_CHECKING:
    from pathlib import Path


class _ProbeProvider:
    def __init__(
        self,
        *,
        health_results: list[bool] | None = None,
        integrity_results: list[bool] | None = None,
    ) -> None:
        self.health_results = list(health_results or [True])
        self.integrity_results = list(integrity_results or [True])
        self.discarded_handles: list[str] = []
        self.initialize_calls: list[tuple[object, str, dict[str, object]]] = (
            []
        )

    def supports_mode(self, mode):
        del mode
        return True

    def acquire(self, requirement, *, mode):
        del requirement, mode
        return {'handle': 'resource-1'}

    def reserve_external_handle(self, requirement, *, mode):
        del requirement, mode
        return 'reserved-1'

    def discard_reserved_external_handle(
        self,
        external_handle,
        requirement,
        *,
        mode,
    ):
        del requirement, mode
        self.discarded_handles.append(external_handle)

    def release(self, resource, requirement, *, mode):
        del resource, requirement, mode

    def health_check(self, resource, requirement, *, mode):
        del resource, requirement, mode
        if len(self.health_results) > 1:
            return self.health_results.pop(0)
        return self.health_results[0]

    def verify_integrity(self, resource, requirement, *, mode):
        del resource, requirement, mode
        if len(self.integrity_results) > 1:
            return self.integrity_results.pop(0)
        return self.integrity_results[0]

    def describe_external_handle(self, resource, requirement, *, mode):
        del requirement, mode
        if isinstance(resource, dict):
            return str(resource.get('handle', 'resource-1'))
        return 'resource-1'

    def reap_orphan(self, external_handle, requirement, *, mode):
        del external_handle, requirement, mode

    def revoke_orphan_access(self, external_handle, requirement, *, mode):
        del external_handle, requirement, mode

    def initialize_from(
        self,
        resource,
        requirement,
        sources,
        *,
        mode,
        initialization_mode,
    ):
        del mode, initialization_mode
        self.initialize_calls.append((resource, requirement.name, dict(sources)))
        return None


class _CapturedEventStream:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def emit(self, event) -> None:
        self.events.append(event)


class _CapturedSpan:
    def __init__(self, owner, name: str, attributes: dict[str, object]) -> None:
        self.owner = owner
        self.name = name
        self.attributes = attributes

    async def __aenter__(self):
        self.owner.spans.append((self.name, self.attributes))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        return False


class _CapturedTelemetry:
    def __init__(self) -> None:
        self.spans: list[tuple[str, dict[str, object]]] = []

    def span(self, name: str, *, parent_span_id=None, attributes=None):
        del parent_span_id
        return _CapturedSpan(self, name, dict(attributes or {}))


def test_resource_requirement_validation_and_dependency_context_get() -> None:
    with pytest.raises(ValueError, match='requires either a provider or setup'):
        ResourceRequirement(name='broken')

    with pytest.raises(ValueError, match='cannot define both provider and setup'):
        ResourceRequirement(
            name='broken',
            provider=_ProbeProvider(),
            setup=lambda: object(),
        )

    with pytest.raises(ValueError, match='negative'):
        ResourceRequirement(
            name='broken',
            provider=_ProbeProvider(),
            initialization_timeout_seconds=-1,
        )

    with pytest.raises(ValueError, match='unsupported scope'):
        ResourceRequirement(
            name='broken',
            provider=_ProbeProvider(),
            scope='run',
            requires_orphan_fencing=True,
        )

    requirement = ResourceRequirement(
        name='dep',
        provider=_ProbeProvider(),
        scope='test',
    )
    dependency = ResolvedResourceDependency(
        requirement=requirement,
        provider=requirement.resolve_provider(),
        resource={'value': 1},
        scope='test',
    )
    context = ResourceDependencyContext({'dep': dependency})
    assert context.get('dep') == {'value': 1}
    assert context.get('missing', {'fallback': True}) == {'fallback': True}
    assert context.names() == ('dep',)


def test_support_helpers_for_orphan_fencing_and_reservation() -> None:
    requirement = ResourceRequirement(
        name='mongo',
        provider=_ProbeProvider(),
        scope='test',
        requires_orphan_fencing=True,
    )

    class _LegacyFencingProvider(_ProbeProvider):
        def supports_orphan_fencing(self):
            return True

    class _FlagProvider(_ProbeProvider):
        supports_orphan_fencing = False

    assert _supports_orphan_fencing(_LegacyFencingProvider(), requirement) is True
    assert _supports_orphan_fencing(_FlagProvider(), requirement) is False

    callable_provider = CallableResourceProvider(lambda: object())
    assert _supports_orphan_fencing(callable_provider, requirement) is False
    assert _supports_external_handle_reservation(callable_provider, requirement) is False

    class _ExtendedCallableProvider(CallableResourceProvider):
        def reserve_external_handle(self, requirement, *, mode):
            del requirement, mode
            return 'reserved'

        def revoke_orphan_access(self, external_handle, requirement, *, mode):
            del external_handle, requirement, mode

    extended = _ExtendedCallableProvider(lambda: object())
    assert _supports_orphan_fencing(extended, requirement) is True
    assert _supports_external_handle_reservation(extended, requirement) is True


def test_call_supported_initialization_modes_handles_legacy_signature() -> None:
    requirement = ResourceRequirement(
        name='mongo',
        provider=_ProbeProvider(),
    )

    def _modes_new(req):
        del req
        return ('data_seed',)

    def _modes_old():
        return ('state_snapshot',)

    assert _call_supported_initialization_modes(_modes_new, requirement) == (
        'data_seed',
    )
    assert _call_supported_initialization_modes(_modes_old, requirement) == (
        'state_snapshot',
    )


@pytest.mark.asyncio
async def test_check_provider_health_marks_local_unhealthy_and_degraded() -> None:
    manager = ResourceManager(mark_local_failures=True)
    unhealthy_provider = _ProbeProvider(health_results=[False])
    unhealthy_requirement = ResourceRequirement(
        name='mongo',
        provider=unhealthy_provider,
        scope='worker',
        readiness_policy=RuntimeReadinessPolicy(),
    )
    with pytest.raises(ResourceError, match='failed health check'):
        await manager._check_provider_health(
            unhealthy_provider,
            unhealthy_requirement,
            {'handle': 'resource-1'},
            local=True,
        )
    assert manager._readiness_states[('mongo', 'worker')].status == (
        'unhealthy_local'
    )

    degraded_provider = _ProbeProvider(
        health_results=[True],
        integrity_results=[False],
    )
    degraded_requirement = ResourceRequirement(
        name='cache',
        provider=degraded_provider,
        scope='worker',
        readiness_policy=RuntimeReadinessPolicy(degraded_timeout_seconds=0.0),
    )
    await manager._check_provider_health(
        degraded_provider,
        degraded_requirement,
        {'handle': 'resource-2'},
    )
    readiness = manager._readiness_states[('cache', 'worker')]
    assert readiness.status == 'degraded'
    assert readiness.reason == 'resource_integrity_check_failed'


@pytest.mark.asyncio
async def test_retry_readiness_check_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ResourceManager()
    requirement_no_retry = ResourceRequirement(
        name='mongo',
        provider=_ProbeProvider(),
        readiness_policy=RuntimeReadinessPolicy(retry_interval_seconds=0),
    )
    assert (
        await manager._retry_readiness_check_if_needed(
            requirement_no_retry,
            started_at=time.perf_counter(),
            error=ResourceError(
                'mongo',
                'failed',
                code='resource_health_check_failed',
            ),
        )
        is False
    )

    requirement_unbounded = ResourceRequirement(
        name='mongo',
        provider=_ProbeProvider(),
        readiness_policy=RuntimeReadinessPolicy(
            retry_interval_seconds=0.1,
            max_wait_seconds=None,
        ),
    )
    assert (
        await manager._retry_readiness_check_if_needed(
            requirement_unbounded,
            started_at=time.perf_counter(),
            error=ResourceError(
                'mongo',
                'failed',
                code='resource_health_check_failed',
            ),
        )
        is False
    )

    requirement_retry = ResourceRequirement(
        name='mongo',
        provider=_ProbeProvider(),
        readiness_policy=RuntimeReadinessPolicy(
            retry_interval_seconds=0.1,
            max_wait_seconds=1.0,
        ),
    )
    sleep_calls: list[float] = []

    async def _capture_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr('cosecha.core.resources.asyncio.sleep', _capture_sleep)
    assert (
        await manager._retry_readiness_check_if_needed(
            requirement_retry,
            started_at=time.perf_counter(),
            error=ResourceError(
                'mongo',
                'failed',
                code='resource_health_check_failed',
            ),
        )
        is True
    )
    assert sleep_calls


@pytest.mark.asyncio
async def test_probe_local_health_deduplicates_cleanup_entries() -> None:
    manager = ResourceManager()
    provider = _ProbeProvider(health_results=[False, False])
    requirement = ResourceRequirement(
        name='mongo',
        provider=provider,
        scope='worker',
    )
    manager._shared_cleanup['worker'] = [
        (requirement, provider, {'handle': 'resource-1'}),
        (requirement, provider, {'handle': 'resource-2'}),
    ]

    failures = await manager.probe_local_health()

    assert len(failures) == 1
    assert failures[0].code == 'resource_health_check_failed'


@pytest.mark.asyncio
async def test_resolve_initialization_sources_and_initialize_resource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ResourceManager()
    provider = _ProbeProvider()
    source_run = ResourceRequirement(name='seed_run', provider=_ProbeProvider(), scope='run')
    source_worker = ResourceRequirement(
        name='seed_worker',
        provider=_ProbeProvider(),
        scope='worker',
    )
    source_test = ResourceRequirement(
        name='seed_test',
        provider=_ProbeProvider(),
        scope='test',
    )
    manager._shared_cleanup['run'].append((source_run, source_run.resolve_provider(), {'value': 'run'}))
    manager._shared_cleanup['worker'].append((source_worker, source_worker.resolve_provider(), {'value': 'worker'}))
    manager._test_cleanup['test-1'] = [
        (source_test, source_test.resolve_provider(), {'value': 'test'}),
    ]
    manager._shared_resources['run']['seed_run'] = {'value': 'run'}
    manager._shared_resources['worker']['seed_worker'] = {'value': 'worker'}
    manager._test_resources['test-1'] = {'seed_test': {'value': 'test'}}

    requirement = ResourceRequirement(
        name='target',
        provider=provider,
        scope='test',
        initializes_from=('seed_run', 'seed_worker', 'seed_test'),
    )
    sources = await manager._resolve_initialization_sources('test-1', requirement)
    assert sorted(sources) == ['seed_run', 'seed_test', 'seed_worker']

    snapshot_requirement = ResourceRequirement(
        name='target_snapshot',
        provider=provider,
        scope='test',
        initializes_from=('seed_run',),
        initialization_mode='state_snapshot',
    )

    async def _fake_snapshot(provider, resource, requirement):
        del provider, resource
        return {'snapshot': requirement.name}

    monkeypatch.setattr(
        'cosecha.core.resources._build_resource_materialization_snapshot',
        _fake_snapshot,
    )
    snapshot_sources = await manager._resolve_initialization_sources(
        'test-1',
        snapshot_requirement,
    )
    assert snapshot_sources['seed_run'] == {'snapshot': 'seed_run'}

    class _NoInitializationProvider(_ProbeProvider):
        initialize_from = None  # type: ignore[assignment]

    unsupported_provider = _NoInitializationProvider()
    unsupported_requirement = ResourceRequirement(
        name='unsupported',
        provider=unsupported_provider,
        scope='test',
        initializes_from=('seed_run',),
    )
    with pytest.raises(ResourceError, match='does not implement initialize_from'):
        await manager._initialize_resource_if_needed(
            'test-1',
            unsupported_requirement,
            {'handle': 'resource'},
        )

    provider.supported_initialization_modes = lambda: ('state_snapshot',)
    mode_requirement = ResourceRequirement(
        name='mode_check',
        provider=provider,
        scope='test',
        initializes_from=('seed_run',),
        initialization_mode='data_seed',
    )
    with pytest.raises(ResourceError, match='does not support initialization mode'):
        await manager._initialize_resource_if_needed(
            'test-1',
            mode_requirement,
            {'handle': 'resource'},
        )

    timeout_requirement = ResourceRequirement(
        name='timeout_check',
        provider=provider,
        scope='test',
        initializes_from=('seed_run',),
        initialization_timeout_seconds=0.1,
    )
    provider.supported_initialization_modes = lambda: ('data_seed',)

    async def _raise_timeout(*args, **kwargs):
        del args, kwargs
        raise TimeoutError()

    monkeypatch.setattr(
        'cosecha.core.resources._await_initialization',
        _raise_timeout,
    )
    with pytest.raises(ResourceError, match='initialization timed out'):
        await manager._initialize_resource_if_needed(
            'test-1',
            timeout_requirement,
            {'handle': 'resource'},
        )

    missing_source_requirement = ResourceRequirement(
        name='missing_source',
        provider=provider,
        scope='test',
        initializes_from=('unknown',),
    )
    with pytest.raises(ResourceError, match='initialization requirement'):
        await manager._resolve_initialization_sources(
            'test-1',
            missing_source_requirement,
        )

    unreachable_requirement = ResourceRequirement(
        name='unreachable',
        provider=provider,
        scope='run',
        initializes_from=('seed_test',),
    )
    with pytest.raises(ResourceError, match='cannot initialize from narrower-scoped source'):
        await manager._resolve_initialization_sources(
            'test-1',
            unreachable_requirement,
        )


@pytest.mark.asyncio
async def test_rehydrate_and_resolve_materialized_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ResourceManager()
    provider = _ProbeProvider()
    dependency_requirement = ResourceRequirement(
        name='seed',
        provider=provider,
        scope='run',
    )
    manager._shared_cleanup['run'].append(
        (dependency_requirement, provider, {'value': 'run-resource'}),
    )
    consumer = ResourceRequirement(
        name='consumer',
        provider=_ProbeProvider(),
        scope='test',
        depends_on=('seed',),
    )

    resolved = await manager._resolve_materialized_dependency(
        None,
        consumer,
        'seed',
    )
    assert resolved.scope == 'run'
    assert resolved.resource == {'value': 'run-resource'}

    with pytest.raises(ResourceError, match='could not resolve dependency'):
        await manager._resolve_materialized_dependency(
            None,
            consumer,
            'missing',
        )

    snapshot = ResourceMaterializationSnapshot(
        name='seed',
        scope='run',
        mode='live',
        connection_data={'rehydrated': True},
    )

    async def _return_rehydrated(provider, snapshot, requirement):
        del provider, snapshot, requirement
        return {'rehydrated': True}

    monkeypatch.setattr(
        'cosecha.core.resources._rehydrate_resource_materialization',
        _return_rehydrated,
    )
    resource, timing = await manager._rehydrate_resource(
        dependency_requirement,
        snapshot,
        normalized_scope='run',
    )
    assert resource == {'rehydrated': True}
    assert timing.acquire_count == 1

    async def _raise_rehydration_error(provider, snapshot, requirement):
        del provider, snapshot, requirement
        raise RuntimeError('boom')

    monkeypatch.setattr(
        'cosecha.core.resources._rehydrate_resource_materialization',
        _raise_rehydration_error,
    )
    with pytest.raises(ResourceError, match='Failed to rehydrate resource'):
        await manager._rehydrate_resource(
            dependency_requirement,
            snapshot,
            normalized_scope='run',
        )


@pytest.mark.asyncio
async def test_prepare_shared_acquisition_and_telemetry_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ResourceManager()
    provider = _ProbeProvider()
    requirement = ResourceRequirement(
        name='mongo',
        provider=provider,
        scope='run',
    )
    manager._shared_resources['run']['mongo'] = {'handle': 'existing'}

    reused = await manager._prepare_shared_requirement_acquisition(
        requirement,
        normalized_scope='run',
    )
    assert reused.created is False
    assert reused.resource == {'handle': 'existing'}

    manager._shared_resources['run'].clear()

    async def _lock_and_publish(lock: asyncio.Lock, requirement):
        del requirement
        await lock.acquire()
        manager._shared_resources['run']['mongo'] = {'handle': 'late'}

    monkeypatch.setattr(
        'cosecha.core.resources._acquire_initialization_lock',
        _lock_and_publish,
    )
    late = await manager._prepare_shared_requirement_acquisition(
        requirement,
        normalized_scope='run',
    )
    assert late.created is False
    assert late.resource == {'handle': 'late'}

    manager._shared_resources['run'].clear()
    manager._materialization_snapshots['run']['mongo'] = (
        ResourceMaterializationSnapshot(
            name='mongo',
            scope='run',
            mode='live',
            connection_data={'rehydrated': True},
        )
    )
    manager._shared_locks.clear()

    async def _only_lock(lock: asyncio.Lock, requirement):
        del requirement
        await lock.acquire()

    monkeypatch.setattr(
        'cosecha.core.resources._acquire_initialization_lock',
        _only_lock,
    )

    async def _rehydrate(self, requirement, snapshot, *, normalized_scope):
        del self, requirement, snapshot
        return (
            {'rehydrated': True},
            ResourceTiming(
                name='mongo',
                scope=normalized_scope,
                acquire_count=1,
            ),
        )

    monkeypatch.setattr(ResourceManager, '_rehydrate_resource', _rehydrate)
    rehydrated = await manager._prepare_shared_requirement_acquisition(
        requirement,
        normalized_scope='run',
    )
    assert rehydrated.created is True
    assert rehydrated.resource == {'rehydrated': True}

    telemetry = _CapturedTelemetry()
    stream = _CapturedEventStream()
    state_events: list[tuple[str, str, str, str | None]] = []
    manager.bind_telemetry_stream(telemetry)
    manager.bind_domain_event_stream(stream)
    manager.bind_resource_state_recorder(
        lambda action, name, scope, external_handle: state_events.append(
            (action, name, scope, external_handle),
        ),
    )
    created_resource, _ = await manager._create_resource(
        None,
        ResourceRequirement(name='redis', provider=provider, scope='worker'),
        'worker',
        parent_span_id='parent',
        telemetry_attributes={'source': 'test'},
    )
    await manager._release_single_requirement(
        None,
        (
            ResourceRequirement(name='redis', provider=provider, scope='worker'),
            provider,
            created_resource,
        ),
        parent_span_id='parent',
        telemetry_attributes={'phase': 'release'},
    )
    await manager._release_single_requirement(
        'test-1',
        (
            ResourceRequirement(name='redis', provider=provider, scope='worker'),
            provider,
            created_resource,
        ),
        parent_span_id='parent',
        telemetry_attributes={'phase': 'release-test'},
    )
    assert telemetry.spans
    assert state_events
    assert stream.events


@pytest.mark.asyncio
async def test_release_levels_close_and_validation_error_paths() -> None:
    manager = ResourceManager()

    class _FailingProvider(_ProbeProvider):
        def __init__(self, message: str) -> None:
            super().__init__()
            self._message = message

        def release(self, resource, requirement, *, mode):
            del resource, requirement, mode
            raise RuntimeError(self._message)

    requirement_one = ResourceRequirement(
        name='mongo',
        provider=_FailingProvider('one'),
        scope='worker',
    )
    requirement_two = ResourceRequirement(
        name='redis',
        provider=_FailingProvider('two'),
        scope='worker',
    )
    with pytest.raises(RuntimeError, match='one'):
        await manager._release_requirement_level(
            None,
            ((requirement_one, requirement_one.resolve_provider(), object()),),
        )
    with pytest.raises(ExceptionGroup, match='Resource release level failed'):
        await manager._release_requirement_level(
            None,
            (
                (requirement_one, requirement_one.resolve_provider(), object()),
                (requirement_two, requirement_two.resolve_provider(), object()),
            ),
        )

    healthy_requirement = ResourceRequirement(
        name='ok',
        provider=_ProbeProvider(),
        scope='run',
    )
    manager._shared_cleanup['run'].append(
        (healthy_requirement, healthy_requirement.resolve_provider(), {'handle': 'ok'}),
    )
    await manager.close()
    assert manager._shared_cleanup['run'] == []

    with pytest.raises(ValueError, match='initializes from missing'):
        validate_resource_requirements(
            (
                ResourceRequirement(
                    name='consumer',
                    provider=_ProbeProvider(),
                    initializes_from=('missing',),
                ),
            ),
        )

    with pytest.raises(ValueError, match='conflicts with'):
        validate_resource_requirements(
            (
                ResourceRequirement(
                    name='a',
                    provider=_ProbeProvider(),
                    conflicts_with=('b',),
                ),
                ResourceRequirement(
                    name='b',
                    provider=_ProbeProvider(),
                ),
            ),
        )


@pytest.mark.asyncio
async def test_materialization_helpers_and_provider_dependency_resolution() -> None:
    requirement = ResourceRequirement(
        name='mongo',
        provider=_ProbeProvider(),
        scope='worker',
    )

    invalid_snapshot = SimpleNamespace(
        snapshot_version=999,
        connection_data={'value': 1},
    )
    with pytest.raises(ResourceError, match='unsupported materialization'):
        await _rehydrate_resource_materialization(
            _ProbeProvider(),
            invalid_snapshot,  # type: ignore[arg-type]
            requirement,
        )

    snapshot = ResourceMaterializationSnapshot(
        name='mongo',
        scope='worker',
        mode='live',
        connection_data={'value': 1},
    )
    assert (
        await _rehydrate_resource_materialization(
            _ProbeProvider(),
            snapshot,
            requirement,
        )
        == {'value': 1}
    )

    class _RehydratingProvider(_ProbeProvider):
        def rehydrate_materialization(self, connection_data, requirement, *, mode):
            del requirement, mode
            return {'rehydrated': connection_data}

    rehydrated = await _rehydrate_resource_materialization(
        _RehydratingProvider(),
        snapshot,
        requirement,
    )
    assert rehydrated == {'rehydrated': {'value': 1}}

    with pytest.raises(ValueError, match='JSON serializable'):
        _normalize_materialization_connection_data({'bad': object()})

    class _BrokenSnapshotProvider(_ProbeProvider):
        def snapshot_materialization(self, resource, requirement, *, mode):
            del resource, requirement, mode
            return {'bad': object()}

    with pytest.raises(ResourceError, match='cannot be materialized'):
        await _build_resource_materialization_snapshot(
            _BrokenSnapshotProvider(),
            {'handle': 'resource-1'},
            requirement,
        )

    class _InvalidDependencyProvider(_ProbeProvider):
        def resolve_dependency_names(self, requirement, *, mode):
            del requirement, mode
            return ('valid', 123)

    invalid_requirement = ResourceRequirement(
        name='invalid',
        provider=_InvalidDependencyProvider(),
    )
    with pytest.raises(ValueError, match='invalid dependency names'):
        _resolve_provider_dependency_names(invalid_requirement)

    assert _callable_accepts_named_parameter(object(), 'mode') is False
    assert _callable_accepts_named_parameter(lambda **kwargs: None, 'mode') is True
