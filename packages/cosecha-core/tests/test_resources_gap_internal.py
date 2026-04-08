from __future__ import annotations

import asyncio
import contextlib
import time

import pytest

from types import SimpleNamespace

from cosecha.core.resources import (
    CallableResourceProvider,
    ResourceDependencyContext,
    ResourceError,
    ResourceManager,
    ResourceMaterializationSnapshot,
    ResourceReadinessState,
    ResourceRequirement,
    ResourceTiming,
    ResolvedResourceDependency,
    _PendingResourceAcquisition,
    _acquire_initialization_lock,
    _await_initialization,
    _describe_dependency_capabilities,
    _describe_external_handle,
    _discard_reserved_external_handle,
    _iter_effective_dependency_names,
    _normalize_resource_timing,
    _resource_scope_rank,
    reap_orphaned_resource,
)
from cosecha.core.runtime_profiles import RuntimeReadinessPolicy


class _Provider:
    def __init__(self) -> None:
        self.released: list[str] = []

    def supports_mode(self, mode: str) -> bool:
        return mode == 'live'

    def acquire(self, requirement, *, mode, dependency_context=None):
        del requirement, mode, dependency_context
        return {'value': 1}

    def release(self, resource, requirement, *, mode):
        del requirement, mode
        self.released.append(str(resource))

    def health_check(self, resource, requirement, *, mode):
        del resource, requirement, mode
        return True

    def verify_integrity(self, resource, requirement, *, mode):
        del resource, requirement, mode
        return True


def _requirement(name: str, *, provider: object | None = None, scope='test'):
    return ResourceRequirement(
        name=name,
        provider=provider or _Provider(),
        scope=scope,  # type: ignore[arg-type]
    )


def test_callable_resource_provider_and_dependency_context_noop_branches() -> None:
    provider = CallableResourceProvider(lambda: {'ok': True})
    requirement = _requirement('dep', provider=provider)
    assert provider.release({'ok': True}, requirement, mode='live') is None
    provider.discard_reserved_external_handle('h', requirement, mode='live')
    provider.reap_orphan('h', requirement, mode='live')
    provider.revoke_orphan_access('h', requirement, mode='live')

    context = ResourceDependencyContext()
    assert context.get_capabilities('missing') == {}
    with pytest.raises(ResourceError, match='could not resolve dependency'):
        context.require('missing', requirement_name='consumer')


def test_resource_snapshot_serialization_and_version_validation() -> None:
    readiness = ResourceReadinessState(name='db', scope='run', status='ready')
    assert ResourceReadinessState.from_dict(readiness.to_dict()) == readiness

    snapshot = ResourceMaterializationSnapshot(
        name='db',
        scope='run',
        mode='live',
        connection_data={'dsn': 'memory://'},
    )
    assert ResourceMaterializationSnapshot.from_dict(snapshot.to_dict()) == snapshot

    with pytest.raises(ValueError, match='Unsupported resource materialization'):
        ResourceMaterializationSnapshot(
            name='db',
            scope='run',
            mode='live',
            connection_data={},
            snapshot_version=999,
        )


@pytest.mark.asyncio
async def test_resource_manager_misc_branches_for_snapshots_acquisition_and_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ResourceManager()
    requirement = _requirement('db', scope='run')
    provider = requirement.resolve_provider()
    manager._shared_cleanup['run'].append((requirement, provider, {'dsn': 'run'}))
    manager.bind_materialization_snapshots(
        (
            ResourceMaterializationSnapshot(
                name='db',
                scope='run',
                mode='live',
                connection_data={'dsn': 'run'},
            ),
        ),
    )
    snapshots = await manager.build_materialization_snapshot(
        scopes=('test', 'run', 'worker'),
    )
    assert snapshots

    async def _always_fail(self, *args, **kwargs):
        del self, args, kwargs
        raise RuntimeError('boom')

    monkeypatch.setattr(
        ResourceManager,
        '_prepare_requirement_acquisition',
        _always_fail,
    )
    with pytest.raises(ExceptionGroup, match='Resource acquisition level failed'):
        await manager._acquire_requirement_level(
            'test-1',
            (_requirement('a'), _requirement('b')),
        )

    not_created = _PendingResourceAcquisition(
        requirement=_requirement('existing'),
        provider=_Provider(),
        normalized_scope='test',
        resource={'cached': True},
        timing=ResourceTiming(name='existing', scope='test'),
        created=False,
    )
    await manager._commit_pending_acquisition('test-1', not_created)
    assert manager._test_cleanup == {}

    states: list[tuple[str, str, str, str | None]] = []
    manager.bind_resource_state_recorder(
        lambda action, name, scope, external_handle: states.append(
            (action, name, scope, external_handle),
        ),
    )
    pending = _PendingResourceAcquisition(
        requirement=_requirement('released'),
        provider=_Provider(),
        normalized_scope='worker',
        resource={'released': True},
        timing=ResourceTiming(name='released', scope='worker'),
        created=True,
        external_handle='handle-1',
    )
    await manager._rollback_pending_acquisition(pending)
    assert states[-1] == ('released', 'released', 'worker', 'handle-1')


@pytest.mark.asyncio
async def test_resource_manager_error_paths_for_scope_mode_health_and_initialization_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ResourceManager(unsupported_scopes=('worker',))
    with pytest.raises(ValueError, match='unsupported scope'):
        manager._ensure_supported_scope('worker', 'db')

    provider = _Provider()
    dry_requirement = ResourceRequirement(
        name='db',
        provider=provider,
        mode='snapshot',  # type: ignore[arg-type]
    )
    with pytest.raises(ValueError, match='does not support mode'):
        manager._ensure_supported_mode(provider, dry_requirement, normalized_scope='run')

    orphan_requirement = ResourceRequirement(
        name='db',
        provider=_Provider(),
        scope='worker',
        requires_orphan_fencing=True,
    )
    with pytest.raises(ResourceError, match='requires orphan fencing'):
        manager._ensure_orphan_fencing_preconditions(
            orphan_requirement.resolve_provider(),
            orphan_requirement,
            normalized_scope='worker',
        )

    unhealthy = ResourceError('db', 'failed', code='resource_health_check_failed')
    manager._unhealthy_resources['db'] = unhealthy
    with pytest.raises(ResourceError, match='failed'):
        manager._ensure_resource_available(_requirement('db'))

    sleep_calls: list[float] = []

    async def _capture_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr('cosecha.core.resources.asyncio.sleep', _capture_sleep)
    delayed_requirement = ResourceRequirement(
        name='delayed',
        provider=_Provider(),
        readiness_policy=RuntimeReadinessPolicy(initial_delay_seconds=0.5),
    )
    await manager._check_provider_health(
        delayed_requirement.resolve_provider(),
        delayed_requirement,
        {'value': True},
    )
    assert sleep_calls == [0.5]

    retry_provider = _Provider()
    retry_provider.health_check = lambda *args, **kwargs: False  # type: ignore[method-assign]
    retry_requirement = ResourceRequirement(
        name='retry',
        provider=retry_provider,
        scope='worker',
        readiness_policy=RuntimeReadinessPolicy(
            retry_interval_seconds=0.01,
            max_wait_seconds=0.02,
        ),
    )
    retries = iter([True, False])

    async def _retry_once(self, *args, **kwargs):
        del self, args, kwargs
        return next(retries)

    with monkeypatch.context() as context:
        context.setattr(
            ResourceManager,
            '_retry_readiness_check_if_needed',
            _retry_once,
        )
        with pytest.raises(ResourceError, match='failed health check'):
            await manager._check_provider_health(
                retry_provider,
                retry_requirement,
                {'value': True},
            )

    integrity_provider = _Provider()
    integrity_provider.verify_integrity = lambda *args, **kwargs: False  # type: ignore[method-assign]
    integrity_requirement = ResourceRequirement(
        name='integrity',
        provider=integrity_provider,
        scope='worker',
        readiness_policy=RuntimeReadinessPolicy(
            retry_interval_seconds=0.0,
            max_wait_seconds=0.0,
            degraded_timeout_seconds=None,
        ),
    )
    with pytest.raises(ResourceError, match='failed integrity check'):
        await manager._check_provider_health(
            integrity_provider,
            integrity_requirement,
            {'value': True},
        )
    with pytest.raises(ResourceError, match='failed integrity check'):
        await manager._check_provider_health(
            integrity_provider,
            integrity_requirement,
            {'value': True},
            local=True,
        )

    stale_requirement = ResourceRequirement(
        name='stale',
        provider=_Provider(),
        readiness_policy=RuntimeReadinessPolicy(
            retry_interval_seconds=0.1,
            max_wait_seconds=0.01,
        ),
    )
    assert (
        await manager._retry_readiness_check_if_needed(
            stale_requirement,
            started_at=time.perf_counter() - 1.0,
            error=ResourceError('stale', 'x', code='resource_health_check_failed'),
        )
        is False
    )

    seed = _requirement('seed')
    manager._shared_cleanup['run'].append((seed, seed.resolve_provider(), {'seed': 1}))
    missing_source_requirement = ResourceRequirement(
        name='consumer',
        provider=_Provider(),
        scope='test',
        initializes_from=('seed',),
    )
    with pytest.raises(ResourceError, match='initializes from missing source'):
        await manager._resolve_initialization_sources('test-1', missing_source_requirement)

    manager.bind_domain_event_metadata_provider(
        lambda requirement, scope, test_id: SimpleNamespace(
            requirement_name=requirement.name,
            scope=scope,
            test_id=test_id,
        ),
    )
    metadata = manager._build_event_metadata(_requirement('meta'), 'run', 'test-1')
    assert metadata.requirement_name == 'meta'


@pytest.mark.asyncio
async def test_resource_helpers_cover_additional_branching(monkeypatch) -> None:
    requirement = ResourceRequirement(
        name='db',
        provider=_Provider(),
        initialization_timeout_seconds=0.1,
    )

    lock = asyncio.Lock()

    async def _timeout(*args, **kwargs):
        awaitable = args[0]
        if hasattr(awaitable, 'close'):
            awaitable.close()
        with contextlib.suppress(Exception):
            await awaitable
        del kwargs
        raise TimeoutError()

    with monkeypatch.context() as context:
        context.setattr('cosecha.core.resources.asyncio.wait_for', _timeout)
        with pytest.raises(ResourceError, match='initialization lock timed out'):
            await _acquire_initialization_lock(lock, requirement)

    assert await _await_initialization(asyncio.sleep(0, result='ok'), requirement) == 'ok'

    with_timeout = ResourceRequirement(
        name='timed',
        provider=_Provider(),
        initialization_timeout_seconds=0.1,
    )
    assert (
        await _await_initialization(asyncio.sleep(0, result='timed'), with_timeout)
        == 'timed'
    )

    class _CapabilityProvider(_Provider):
        def describe_capabilities(self, resource, requirement, *, mode):
            del resource, requirement, mode
            return None

    assert (
        await _describe_dependency_capabilities(
            _CapabilityProvider(),
            object(),
            _requirement('none'),
        )
        == {}
    )

    class _InvalidCapabilityProvider(_Provider):
        def describe_capabilities(self, resource, requirement, *, mode):
            del resource, requirement, mode
            return ['invalid']

    with pytest.raises(ResourceError, match='invalid capabilities'):
        await _describe_dependency_capabilities(
            _InvalidCapabilityProvider(),
            object(),
            _requirement('invalid'),
        )

    class _ResolverNoneProvider(_Provider):
        def resolve_dependency_names(self, requirement, *, mode):
            del requirement, mode
            return None

    assert _iter_effective_dependency_names(
        ResourceRequirement(name='a', provider=_ResolverNoneProvider()),
    ) == ()

    class _ResolverStringProvider(_Provider):
        def resolve_dependency_names(self, requirement, *, mode):
            del requirement, mode
            return 'seed'

    dedup_requirement = ResourceRequirement(
        name='consumer',
        provider=_ResolverStringProvider(),
        depends_on=('seed',),
    )
    assert _iter_effective_dependency_names(dedup_requirement) == ('seed',)

    assert _describe_external_handle(_Provider(), object(), _requirement('none')) is None
    await _discard_reserved_external_handle(_Provider(), 'h', _requirement('none'))
    await reap_orphaned_resource(_Provider(), 'h', _requirement('none'))

    normalized = _normalize_resource_timing(
        ResourceTiming(name='db', scope='session'),
        legacy_session_scope='worker',
    )
    assert normalized.scope == 'worker'

    with pytest.raises(ValueError, match='Unsupported resource scope'):
        _resource_scope_rank('invalid')  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_resolve_materialized_dependency_skips_non_matching_test_entries() -> None:
    manager = ResourceManager()
    consumer = ResourceRequirement(
        name='consumer',
        provider=_Provider(),
        scope='test',
        depends_on=('seed',),
    )
    manager._test_cleanup['test-1'] = [(_requirement('other'), _Provider(), object())]
    with pytest.raises(ResourceError, match='could not resolve dependency'):
        await manager._resolve_materialized_dependency('test-1', consumer, 'seed')


@pytest.mark.asyncio
async def test_prepare_test_scope_reuses_existing_resource_and_rehydrate_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ResourceManager()
    test_requirement = _requirement('db', scope='test')
    manager._test_resources['test-1'] = {'db': {'cached': True}}
    manager._test_cleanup['test-1'] = []
    pending = await manager._prepare_requirement_acquisition(
        'test-1',
        test_requirement,
    )
    assert pending.created is False
    assert pending.resource == {'cached': True}

    run_requirement = _requirement('run-db', scope='run')
    snapshot = ResourceMaterializationSnapshot(
        name='run-db',
        scope='run',
        mode='live',
        connection_data={'dsn': 'run'},
    )

    async def _raise_resource_error(*args, **kwargs):
        del args, kwargs
        raise ResourceError(
            'run-db',
            'rehydrate failed',
            code='resource_rehydrate_failed',
        )

    monkeypatch.setattr(
        'cosecha.core.resources._rehydrate_resource_materialization',
        _raise_resource_error,
    )
    with pytest.raises(ResourceError, match='rehydrate failed'):
        await manager._rehydrate_resource(
            run_requirement,
            snapshot,
            normalized_scope='run',
        )


@pytest.mark.asyncio
async def test_await_initialization_without_timeout_uses_await_if_needed() -> None:
    requirement = _requirement('plain')
    assert await _await_initialization(
        asyncio.sleep(0, result='plain'),
        requirement,
    ) == 'plain'
