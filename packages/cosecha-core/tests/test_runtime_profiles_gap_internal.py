from __future__ import annotations

from types import SimpleNamespace

import pytest

import cosecha.core.runtime_profiles as runtime_profiles

from cosecha.core.runtime_profiles import (
    RuntimeCapabilityRequirement,
    RuntimeMaterializationExplanation,
    RuntimeMaterializationSnapshot,
    RuntimeProfileSpec,
    RuntimeRequirementSet,
    RuntimeServiceMaterializationSnapshot,
    RuntimeServiceOffering,
    RuntimeServiceReadinessState,
    RuntimeServiceSpec,
    resolve_runtime_requirement_issues,
)


def test_runtime_profile_snapshot_roundtrips_and_shadow_requirements() -> None:
    service = RuntimeServiceSpec(interface='execution/engine', provider='demo')
    profile = RuntimeProfileSpec(id='web', services=(service,))

    shadow_requirements = profile.build_shadow_requirements()
    assert len(shadow_requirements) == 1
    assert shadow_requirements[0].name == 'execution/engine'

    readiness_state = RuntimeServiceReadinessState(
        interface_name='execution/engine',
        provider_name='demo',
        status='ready',
    )
    assert RuntimeServiceReadinessState.from_dict(readiness_state.to_dict()) == readiness_state

    service_snapshot = RuntimeServiceMaterializationSnapshot(
        interface_name='execution/engine',
        provider_name='demo',
        readiness_state=readiness_state,
    )
    assert (
        RuntimeServiceMaterializationSnapshot.from_dict(service_snapshot.to_dict())
        == service_snapshot
    )

    materialization_snapshot = RuntimeMaterializationSnapshot(
        profile_id='web',
        services=(service_snapshot,),
    )
    assert (
        RuntimeMaterializationSnapshot.from_dict(materialization_snapshot.to_dict())
        == materialization_snapshot
    )

    explanation = RuntimeMaterializationExplanation(
        active_profile_ids=('web',),
        inactive_profile_ids=(),
        service_readiness_states=(readiness_state,),
        reasons=('ok',),
    )
    assert RuntimeMaterializationExplanation.from_dict(explanation.to_dict()) == explanation


def test_runtime_capability_requirement_issue_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalid_interface_issues = resolve_runtime_requirement_issues(
        RuntimeRequirementSet(
            capabilities=(
                RuntimeCapabilityRequirement(
                    interface_name='database/not-real',
                    capability_name='read',
                ),
            ),
        ),
        offerings=(),
    )
    assert invalid_interface_issues
    assert 'Unknown runtime interface' in invalid_interface_issues[0]

    unhealthy_issues = resolve_runtime_requirement_issues(
        RuntimeRequirementSet(
            capabilities=(
                RuntimeCapabilityRequirement(
                    interface_name='execution/engine',
                    capability_name='run',
                ),
            ),
        ),
        offerings=(
            RuntimeServiceOffering(
                interface_name='execution/engine',
                provider_name='demo',
                readiness_state='unhealthy',
            ),
        ),
    )
    assert unhealthy_issues
    assert 'cannot satisfy capability' in unhealthy_issues[0]

    class _Validation:
        unknown_capabilities = ()

        def messages(self) -> tuple[str, ...]:
            return ()

    monkeypatch.setattr(
        runtime_profiles,
        'validate_runtime_capability_matrix',
        lambda _interface, _capabilities: _Validation(),
    )

    missing_capability_issues = resolve_runtime_requirement_issues(
        RuntimeRequirementSet(
            capabilities=(
                RuntimeCapabilityRequirement(
                    interface_name='execution/engine',
                    capability_name='run',
                ),
            ),
        ),
        offerings=(
            RuntimeServiceOffering(
                interface_name='execution/engine',
                provider_name='demo',
                readiness_state='ready',
                capabilities=(),
                degraded_capabilities=(),
            ),
        ),
    )
    assert missing_capability_issues
    assert 'Missing runtime capability' in missing_capability_issues[0]
