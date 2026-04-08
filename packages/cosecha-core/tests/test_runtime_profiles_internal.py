from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.runtime_profiles import (
    RuntimeCapabilityRequirement,
    RuntimeModeDisallowance,
    RuntimeModeRequirement,
    RuntimeProfileSpec,
    RuntimeReadinessPolicy,
    RuntimeRequirementSet,
    RuntimeServiceOffering,
    RuntimeServiceSpec,
    _resolve_runtime_capability_requirement_issue,
    _resolve_runtime_interface_requirement_issue,
    _resolve_runtime_mode_disallowance_issue,
    _resolve_runtime_mode_requirement_issue,
    resolve_runtime_requirement_issues,
)


def test_runtime_profile_dataclass_roundtrips_and_merge() -> None:
    requirement = RuntimeCapabilityRequirement(
        interface_name='storage/mongo',
        capability_name='query',
    )
    required_mode = RuntimeModeRequirement(
        interface_name='storage/mongo',
        mode_name='live',
    )
    disallowed_mode = RuntimeModeDisallowance(
        interface_name='storage/mongo',
        mode_name='dry_run',
    )
    requirements = RuntimeRequirementSet(
        interfaces=('storage/mongo',),
        capabilities=(requirement,),
        required_modes=(required_mode,),
        disallowed_modes=(disallowed_mode,),
    )
    merged = requirements.merge(
        RuntimeRequirementSet(
            interfaces=('storage/mongo', 'queue/kafka'),
            capabilities=(requirement,),
            required_modes=(required_mode,),
            disallowed_modes=(disallowed_mode,),
        ),
    )
    assert merged.interfaces == ('storage/mongo', 'queue/kafka')
    assert RuntimeRequirementSet.from_dict(requirements.to_dict()) == requirements
    assert RuntimeCapabilityRequirement.from_dict(requirement.to_dict()) == requirement
    assert RuntimeModeRequirement.from_dict(required_mode.to_dict()) == required_mode
    assert (
        RuntimeModeDisallowance.from_dict(disallowed_mode.to_dict())
        == disallowed_mode
    )

    policy = RuntimeReadinessPolicy(retry_interval_seconds=0.5)
    assert RuntimeReadinessPolicy.from_dict(policy.to_dict()) == policy

    service = RuntimeServiceSpec(
        interface='storage/mongo',
        provider='mongodb',
        capabilities=('query',),
        degraded_capabilities=('query',),
        readiness_policy=policy,
    )
    profile = RuntimeProfileSpec(id='default', services=(service,))
    offering = RuntimeServiceOffering(
        interface_name='storage/mongo',
        provider_name='mongodb',
        mode='live',
        capabilities=('query',),
        degraded_capabilities=('query',),
        readiness_state='ready',
    )
    assert RuntimeServiceSpec.from_dict(service.to_dict()) == service
    assert RuntimeProfileSpec.from_dict(profile.to_dict()) == profile
    assert RuntimeServiceOffering.from_dict(offering.to_dict()) == offering


def test_runtime_requirement_issue_resolvers_cover_error_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'cosecha.core.runtime_profiles.validate_runtime_interface_name',
        lambda interface_name: (
            'invalid runtime interface'
            if interface_name == 'invalid/interface'
            else None
        ),
    )

    offerings = {
        'storage/mongo': RuntimeServiceOffering(
            interface_name='storage/mongo',
            provider_name='mongodb',
            mode='live',
            capabilities=('query',),
            degraded_capabilities=('query',),
            readiness_state='ready',
        ),
        'storage/unhealthy': RuntimeServiceOffering(
            interface_name='storage/unhealthy',
            provider_name='mongodb',
            mode='live',
            capabilities=('query',),
            degraded_capabilities=('query',),
            readiness_state='unhealthy',
        ),
        'storage/degraded': RuntimeServiceOffering(
            interface_name='storage/degraded',
            provider_name='mongodb',
            mode='live',
            capabilities=(),
            degraded_capabilities=('query',),
            readiness_state='degraded',
        ),
    }

    assert _resolve_runtime_interface_requirement_issue(
        interface_name='invalid/interface',
        offerings_by_interface=offerings,
    ) == 'invalid runtime interface'
    assert 'Missing runtime interface' in _resolve_runtime_interface_requirement_issue(
        interface_name='missing/interface',
        offerings_by_interface=offerings,
    )
    assert 'is unhealthy' in _resolve_runtime_interface_requirement_issue(
        interface_name='storage/unhealthy',
        offerings_by_interface=offerings,
    )

    assert _resolve_runtime_mode_disallowance_issue(
        disallowed_mode=RuntimeModeDisallowance(
            interface_name='invalid/interface',
            mode_name='live',
        ),
        offerings_by_interface=offerings,
    ) == 'invalid runtime interface'
    assert (
        _resolve_runtime_mode_disallowance_issue(
            disallowed_mode=RuntimeModeDisallowance(
                interface_name='storage/mongo',
                mode_name='dry_run',
            ),
            offerings_by_interface=offerings,
        )
        is None
    )
    assert 'uses disallowed mode' in _resolve_runtime_mode_disallowance_issue(
        disallowed_mode=RuntimeModeDisallowance(
            interface_name='storage/mongo',
            mode_name='live',
        ),
        offerings_by_interface=offerings,
    )

    assert _resolve_runtime_mode_requirement_issue(
        required_mode=RuntimeModeRequirement(
            interface_name='invalid/interface',
            mode_name='live',
        ),
        offerings_by_interface=offerings,
    ) == 'invalid runtime interface'
    assert 'Missing runtime interface' in _resolve_runtime_mode_requirement_issue(
        required_mode=RuntimeModeRequirement(
            interface_name='missing/interface',
            mode_name='live',
        ),
        offerings_by_interface=offerings,
    )
    assert 'cannot satisfy required mode' in _resolve_runtime_mode_requirement_issue(
        required_mode=RuntimeModeRequirement(
            interface_name='storage/unhealthy',
            mode_name='live',
        ),
        offerings_by_interface=offerings,
    )
    assert 'requires mode' in _resolve_runtime_mode_requirement_issue(
        required_mode=RuntimeModeRequirement(
            interface_name='storage/mongo',
            mode_name='dry_run',
        ),
        offerings_by_interface=offerings,
    )


def test_runtime_capability_requirement_resolution_and_aggregation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'cosecha.core.runtime_profiles.validate_runtime_interface_name',
        lambda interface_name: None,
    )

    def _raise_capability_error(interface_name: str, capabilities: tuple[str, ...]):
        del interface_name, capabilities
        raise ValueError('capability registry offline')

    monkeypatch.setattr(
        'cosecha.core.runtime_profiles.validate_runtime_capability_matrix',
        _raise_capability_error,
    )

    offerings = {
        'storage/mongo': RuntimeServiceOffering(
            interface_name='storage/mongo',
            provider_name='mongodb',
            mode='live',
            capabilities=('query',),
            degraded_capabilities=('query',),
            readiness_state='ready',
        ),
        'storage/degraded': RuntimeServiceOffering(
            interface_name='storage/degraded',
            provider_name='mongodb',
            mode='live',
            capabilities=(),
            degraded_capabilities=('query',),
            readiness_state='degraded',
        ),
    }

    missing_interface_issue = _resolve_runtime_capability_requirement_issue(
        capability_requirement=RuntimeCapabilityRequirement(
            interface_name='missing/interface',
            capability_name='query',
        ),
        offerings_by_interface=offerings,
    )
    assert 'Missing runtime interface' in str(missing_interface_issue)

    error_issue = _resolve_runtime_capability_requirement_issue(
        capability_requirement=RuntimeCapabilityRequirement(
            interface_name='storage/mongo',
            capability_name='query',
        ),
        offerings_by_interface=offerings,
    )
    assert 'capability registry offline' in str(error_issue)

    monkeypatch.setattr(
        'cosecha.core.runtime_profiles.validate_runtime_capability_matrix',
        lambda interface_name, capabilities: SimpleNamespace(
            unknown_capabilities=capabilities,
            messages=lambda: (
                f'unknown capability {capabilities[0]!r} for {interface_name}'
            ,),
        ),
    )
    unknown_issue = _resolve_runtime_capability_requirement_issue(
        capability_requirement=RuntimeCapabilityRequirement(
            interface_name='storage/mongo',
            capability_name='missing_capability',
        ),
        offerings_by_interface=offerings,
    )
    assert 'Capability discovery error' in str(unknown_issue)

    ok_issue = _resolve_runtime_capability_requirement_issue(
        capability_requirement=RuntimeCapabilityRequirement(
            interface_name='storage/degraded',
            capability_name='query',
        ),
        offerings_by_interface=offerings,
    )
    assert ok_issue is None

    requirements = RuntimeRequirementSet(
        interfaces=('storage/mongo', 'missing/interface'),
        required_modes=(
            RuntimeModeRequirement(
                interface_name='storage/mongo',
                mode_name='dry_run',
            ),
        ),
        disallowed_modes=(
            RuntimeModeDisallowance(
                interface_name='storage/mongo',
                mode_name='live',
            ),
        ),
        capabilities=(
            RuntimeCapabilityRequirement(
                interface_name='storage/mongo',
                capability_name='missing_capability',
            ),
        ),
    )
    issues = resolve_runtime_requirement_issues(
        requirements,
        tuple(offerings.values()),
    )
    assert issues
