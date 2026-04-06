from __future__ import annotations

import asyncio

import pytest

from cosecha.core.resources import (
    ResourceError,
    ResourceRequirement,
    build_resource_dependency_levels,
    normalize_resource_scope,
    order_resource_requirements,
    reap_orphaned_resource,
    validate_resource_requirements,
)


class _TrackingProvider:
    def __init__(self) -> None:
        self.reaped_handles: list[str] = []
        self.revoked_handles: list[str] = []

    def supports_mode(self, mode):
        del mode
        return True

    def acquire(self, requirement, *, mode):
        del requirement, mode
        return object()

    async def release(self, resource, requirement, *, mode):
        del resource, requirement, mode

    def health_check(self, resource, requirement, *, mode):
        del resource, requirement, mode
        return True

    def verify_integrity(self, resource, requirement, *, mode):
        del resource, requirement, mode
        return True

    def describe_external_handle(self, resource, requirement, *, mode):
        del resource, requirement, mode

    def reap_orphan(self, external_handle, requirement, *, mode):
        del requirement, mode
        self.reaped_handles.append(external_handle)

    def revoke_orphan_access(self, external_handle, requirement, *, mode):
        del requirement, mode
        self.revoked_handles.append(external_handle)


class _NoFencingProvider(_TrackingProvider):
    revoke_orphan_access = None  # type: ignore[assignment]


def test_order_resource_requirements_respects_dependencies() -> None:
    ordered = order_resource_requirements(
        (
            ResourceRequirement(
                name='browser',
                setup=object,
                depends_on=('session_db',),
            ),
            ResourceRequirement(
                name='session_db',
                setup=object,
            ),
        ),
    )

    assert [requirement.name for requirement in ordered] == [
        'session_db',
        'browser',
    ]


def test_build_resource_dependency_levels_groups_independent_resources(
) -> None:
    levels = build_resource_dependency_levels(
        (
            ResourceRequirement(
                name='db',
                setup=object,
            ),
            ResourceRequirement(
                name='cache',
                setup=object,
            ),
            ResourceRequirement(
                name='browser',
                setup=object,
                depends_on=('db',),
            ),
        ),
    )

    assert [
        [requirement.name for requirement in level]
        for level in levels
    ] == [
        ['db', 'cache'],
        ['browser'],
    ]


def test_validate_resource_requirements_rejects_missing_dependencies() -> None:
    with pytest.raises(ValueError, match='depends on missing resources'):
        validate_resource_requirements(
            (
                ResourceRequirement(
                    name='browser',
                    setup=object,
                    depends_on=('session_db',),
                ),
            ),
        )


def test_normalize_resource_scope_maps_legacy_session_scope() -> None:
    assert normalize_resource_scope('session') == 'run'
    assert (
        normalize_resource_scope('session', legacy_session_scope='worker')
        == 'worker'
    )
    assert normalize_resource_scope('test') == 'test'


def test_reap_orphaned_resource_revokes_and_reaps_when_supported() -> None:
    provider = _TrackingProvider()
    requirement = ResourceRequirement(
        name='mongo',
        provider=provider,
        requires_orphan_fencing=True,
        scope='test',
    )

    asyncio.run(
        reap_orphaned_resource(provider, 'resource-1', requirement),
    )

    assert provider.revoked_handles == ['resource-1']
    assert provider.reaped_handles == ['resource-1']


def test_reap_orphaned_resource_rejects_missing_fencing_support() -> None:
    provider = _NoFencingProvider()
    requirement = ResourceRequirement(
        name='mongo',
        provider=provider,
        requires_orphan_fencing=True,
        scope='test',
    )

    with pytest.raises(
        ResourceError,
        match='requires orphan fencing',
    ) as error_info:
        asyncio.run(
            reap_orphaned_resource(provider, 'resource-1', requirement),
        )

    assert error_info.value.code == 'resource_orphan_fencing_unsupported'
