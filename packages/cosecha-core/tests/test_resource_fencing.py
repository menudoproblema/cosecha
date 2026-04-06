from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from cosecha.core.resources import (
    ResourceError,
    ResourceManager,
    ResourceRequirement,
    validate_resource_requirements,
)
from cosecha.core.runtime import ProcessRuntimeProvider


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path


class _FakeProvider:
    def supports_mode(self, mode):
        del mode
        return True

    def acquire(self, requirement, *, mode):
        del requirement, mode
        return {'handle': 'resource-1'}

    def release(self, resource, requirement, *, mode):
        del resource, requirement, mode

    def health_check(self, resource, requirement, *, mode):
        del resource, requirement, mode
        return True

    def verify_integrity(self, resource, requirement, *, mode):
        del resource, requirement, mode
        return True

    def describe_external_handle(self, resource, requirement, *, mode):
        del requirement, mode
        return resource['handle']

    def reap_orphan(self, external_handle, requirement, *, mode):
        del external_handle, requirement, mode


class _BrokenEventStream:
    async def emit(self, event):
        if getattr(event, 'event_type', None) != 'resource.lifecycle':
            return
        msg = 'stream offline'
        raise RuntimeError(msg)


class _BrokenProvider(_FakeProvider):
    def acquire(self, requirement, *, mode):
        del requirement, mode
        msg = 'acquire failed'
        raise RuntimeError(msg)


class _FencingOnlyProvider(_FakeProvider):
    def revoke_orphan_access(self, external_handle, requirement, *, mode):
        del external_handle, requirement, mode


class _ReservedHandleProvider(_FakeProvider):
    def __init__(self) -> None:
        self.discarded_handles: list[str] = []
        self.reaped_handles: list[str] = []
        self.revoked_handles: list[str] = []

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

    def reap_orphan(self, external_handle, requirement, *, mode):
        del requirement, mode
        self.reaped_handles.append(external_handle)

    def revoke_orphan_access(self, external_handle, requirement, *, mode):
        del requirement, mode
        self.revoked_handles.append(external_handle)

    def acquire(self, requirement, *, mode):
        del requirement, mode
        return {'handle': 'reserved-1'}


class _ReservedHandleBrokenProvider(_ReservedHandleProvider):
    def acquire(self, requirement, *, mode):
        del requirement, mode
        msg = 'acquire failed'
        raise RuntimeError(msg)


class _DependencyAwareProvider(_FakeProvider):
    def __init__(self) -> None:
        self.observed_dependency = None
        self.observed_names: tuple[str, ...] = ()

    def acquire(self, requirement, *, mode, dependency_context):
        del mode
        self.observed_names = dependency_context.names()
        self.observed_dependency = dependency_context.require(
            'seed',
            requirement_name=requirement.name,
        )
        return {
            'handle': 'resource-2',
            'seed': self.observed_dependency,
        }


class _CapabilitySeedProvider(_FakeProvider):
    def describe_capabilities(self, resource, requirement, *, mode):
        del resource, requirement, mode
        return {'kind': 'seed'}


class _CapabilityAwareProvider(_FakeProvider):
    def validate_dependency_capabilities(
        self,
        requirement,
        *,
        mode,
        dependency_context,
    ):
        del mode
        if dependency_context.get_capabilities('seed').get('kind') == 'seed':
            return
        msg = 'missing seed capability'
        raise ResourceError(
            requirement.name,
            msg,
            code='missing_seed_capability',
            unhealthy=False,
        )


class _ImplicitDependencyAwareProvider(_FakeProvider):
    def resolve_dependency_names(self, requirement, *, mode):
        del requirement, mode
        return ('seed',)

    def acquire(self, requirement, *, mode, dependency_context):
        del mode
        return {
            'handle': 'resource-3',
            'seed': dependency_context.require(
                'seed',
                requirement_name=requirement.name,
            ),
        }


class _LegacyImplicitDependencyProvider(_FakeProvider):
    def resolve_dependency_names(self, requirement):
        del requirement
        return ('seed',)


class _BrokenImplicitDependencyProvider(_FakeProvider):
    def resolve_dependency_names(self, requirement, *, mode):
        del requirement, mode
        msg = 'internal bug from provider'
        raise TypeError(msg)


@pytest.mark.asyncio
async def test_resource_manager_records_external_handle_before_event_emit(
) -> None:
    manager = ResourceManager()
    observed_states: list[tuple[str, str, str, str | None]] = []
    manager.bind_resource_state_recorder(
        lambda action, name, scope, external_handle: observed_states.append(
            (action, name, scope, external_handle),
        ),
    )
    manager.bind_domain_event_stream(_BrokenEventStream())

    requirement = ResourceRequirement(
        name='mongo',
        provider=_FakeProvider(),
        scope='test',
        requires_orphan_fencing=False,
    )

    with pytest.raises(RuntimeError, match='stream offline'):
        await manager.acquire_for_test('test-1', (requirement,))

    assert observed_states == [
        ('pending', 'mongo', 'test', None),
        ('acquired', 'mongo', 'test', 'resource-1'),
    ]


@pytest.mark.asyncio
async def test_resource_manager_clears_pending_state_when_acquire_fails(
) -> None:
    manager = ResourceManager()
    observed_states: list[tuple[str, str, str, str | None]] = []
    manager.bind_resource_state_recorder(
        lambda action, name, scope, external_handle: observed_states.append(
            (action, name, scope, external_handle),
        ),
    )

    requirement = ResourceRequirement(
        name='mongo',
        provider=_BrokenProvider(),
        scope='test',
        requires_orphan_fencing=False,
    )

    with pytest.raises(RuntimeError, match='acquire failed'):
        await manager.acquire_for_test('test-1', (requirement,))

    assert observed_states == [
        ('pending', 'mongo', 'test', None),
        ('pending_cleared', 'mongo', 'test', None),
    ]


@pytest.mark.asyncio
async def test_resource_manager_rejects_fenced_provider_without_reservation(
) -> None:
    manager = ResourceManager()

    requirement = ResourceRequirement(
        name='mongo',
        provider=_FencingOnlyProvider(),
        scope='test',
        requires_orphan_fencing=True,
    )

    with pytest.raises(
        ResourceError,
        match='reserve_external_handle',
    ) as error_info:
        await manager.acquire_for_test('test-1', (requirement,))

    assert getattr(error_info.value, 'code', None) == (
        'resource_orphan_handle_reservation_unsupported'
    )


@pytest.mark.asyncio
async def test_resource_manager_records_reserved_handle_during_pending_window(
) -> None:
    manager = ResourceManager()
    observed_states: list[tuple[str, str, str, str | None]] = []
    manager.bind_resource_state_recorder(
        lambda action, name, scope, external_handle: observed_states.append(
            (action, name, scope, external_handle),
        ),
    )
    manager.bind_domain_event_stream(_BrokenEventStream())

    requirement = ResourceRequirement(
        name='mongo',
        provider=_ReservedHandleProvider(),
        scope='test',
        requires_orphan_fencing=True,
    )

    with pytest.raises(RuntimeError, match='stream offline'):
        await manager.acquire_for_test('test-1', (requirement,))

    assert observed_states == [
        ('pending', 'mongo', 'test', 'reserved-1'),
        ('acquired', 'mongo', 'test', 'reserved-1'),
    ]


@pytest.mark.asyncio
async def test_resource_manager_discards_reserved_handle_when_acquire_fails(
) -> None:
    manager = ResourceManager()
    observed_states: list[tuple[str, str, str, str | None]] = []
    provider = _ReservedHandleBrokenProvider()
    manager.bind_resource_state_recorder(
        lambda action, name, scope, external_handle: observed_states.append(
            (action, name, scope, external_handle),
        ),
    )

    requirement = ResourceRequirement(
        name='mongo',
        provider=provider,
        scope='test',
        requires_orphan_fencing=True,
    )

    with pytest.raises(RuntimeError, match='acquire failed'):
        await manager.acquire_for_test('test-1', (requirement,))

    assert observed_states == [
        ('pending', 'mongo', 'test', 'reserved-1'),
        ('pending_cleared', 'mongo', 'test', 'reserved-1'),
    ]
    assert provider.discarded_handles == ['reserved-1']


@pytest.mark.asyncio
async def test_resource_manager_passes_materialized_dependencies_to_provider(
) -> None:
    manager = ResourceManager()
    dependency_provider = _FakeProvider()
    consumer_provider = _DependencyAwareProvider()

    seed_requirement = ResourceRequirement(
        name='seed',
        provider=dependency_provider,
        scope='test',
    )
    consumer_requirement = ResourceRequirement(
        name='mongo',
        provider=consumer_provider,
        scope='test',
        depends_on=('seed',),
    )

    acquired = await manager.acquire_for_test(
        'test-1',
        (seed_requirement, consumer_requirement),
    )

    assert consumer_provider.observed_names == ('seed',)
    assert consumer_provider.observed_dependency == {'handle': 'resource-1'}
    assert acquired['mongo'] == {
        'handle': 'resource-2',
        'seed': {'handle': 'resource-1'},
    }


@pytest.mark.asyncio
async def test_resource_manager_validates_dependency_capabilities(
) -> None:
    manager = ResourceManager()

    seed_requirement = ResourceRequirement(
        name='seed',
        provider=_CapabilitySeedProvider(),
        scope='test',
    )
    consumer_requirement = ResourceRequirement(
        name='mongo',
        provider=_CapabilityAwareProvider(),
        scope='test',
        depends_on=('seed',),
    )

    acquired = await manager.acquire_for_test(
        'test-1',
        (seed_requirement, consumer_requirement),
    )

    assert acquired['mongo'] == {'handle': 'resource-1'}


@pytest.mark.asyncio
async def test_resource_manager_orders_provider_declared_dependencies(
) -> None:
    manager = ResourceManager()

    seed_requirement = ResourceRequirement(
        name='seed',
        provider=_FakeProvider(),
        scope='test',
    )
    consumer_requirement = ResourceRequirement(
        name='mongo',
        provider=_ImplicitDependencyAwareProvider(),
        scope='test',
    )

    acquired = await manager.acquire_for_test(
        'test-1',
        (seed_requirement, consumer_requirement),
    )

    assert acquired['mongo'] == {
        'handle': 'resource-3',
        'seed': {'handle': 'resource-1'},
    }


def test_validate_resource_requirements_accepts_legacy_dependency_resolver(
) -> None:
    requirement = ResourceRequirement(
        name='mongo',
        provider=_LegacyImplicitDependencyProvider(),
        scope='test',
    )
    seed_requirement = ResourceRequirement(
        name='seed',
        provider=_FakeProvider(),
        scope='test',
    )

    validated = validate_resource_requirements(
        (seed_requirement, requirement),
    )

    assert validated == (seed_requirement, requirement)


def test_validate_resource_requirements_preserves_dependency_resolver_errors(
) -> None:
    requirement = ResourceRequirement(
        name='mongo',
        provider=_BrokenImplicitDependencyProvider(),
        scope='test',
    )

    with pytest.raises(TypeError, match='internal bug from provider'):
        validate_resource_requirements((requirement,))


def test_process_runtime_provider_reports_pending_resource_window(
    tmp_path: Path,
) -> None:
    provider = ProcessRuntimeProvider()
    provider._runtime_state_dir = tmp_path
    provider._session_id = 'session-1'
    state_dir = tmp_path / 'session-1'
    state_dir.mkdir(parents=True)
    state_path = state_dir / 'worker-3.json'
    state_path.write_text(
        (
            '{"status":"ready","active_resources":[],'
            '"pending_resources":[{"name":"mongo","scope":"worker"}],'
            '"worker_id":3}'
        ),
        encoding='utf-8',
    )

    error = provider._build_worker_state_error(3)

    assert error is not None
    assert error.code == 'worker_pending_resource_handle_missing'
    assert 'mongo' in str(error)


@pytest.mark.asyncio
async def test_process_runtime_provider_reaps_pending_reserved_handle(
    tmp_path: Path,
) -> None:
    provider = ProcessRuntimeProvider()
    reserved_provider = _ReservedHandleProvider()
    requirement = ResourceRequirement(
        name='mongo',
        provider=reserved_provider,
        scope='worker',
        requires_orphan_fencing=True,
    )
    provider._orphaned_resources.register_plan(
        (
            SimpleNamespace(
                resource_requirements=(requirement,),
            ),
        ),
    )

    state_path = tmp_path / 'worker-3.json'
    state_path.write_text(
        (
            '{"status":"ready","active_resources":[],'
            '"pending_resources":[{"name":"mongo","scope":"worker",'
            '"external_handle":"reserved-1"}],"worker_id":3}'
        ),
        encoding='utf-8',
    )

    reaped = await provider._orphaned_resources.reap_worker(
        3,
        state_path=state_path,
    )

    assert tuple(
        (resource.name, resource.scope, resource.external_handle)
        for resource in reaped
    ) == (('mongo', 'worker', 'reserved-1'),)
    assert reserved_provider.revoked_handles == ['reserved-1']
    assert reserved_provider.reaped_handles == ['reserved-1']
