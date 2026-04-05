from __future__ import annotations

import os
import subprocess
import sys

from pathlib import Path

import pytest

from cosecha.core.manifest_symbols import ManifestValidationError
from cosecha.core.manifest_validation import (
    validate_runtime_profile_service_graph,
)
from cosecha.core.runtime_interop import validate_runtime_interface_name
from cosecha.core.runtime_profiles import (
    RuntimeModeDisallowance,
    RuntimeModeRequirement,
    RuntimeProfileSpec,
    RuntimeReadinessPolicy,
    RuntimeRequirementSet,
    RuntimeServiceOffering,
    RuntimeServiceSpec,
    build_runtime_service_shadow_requirements,
    resolve_runtime_requirement_issues,
)


def test_required_mode_is_satisfied_when_offering_matches() -> None:
    requirements = RuntimeRequirementSet(
        required_modes=(
            RuntimeModeRequirement(
                interface_name='application/http',
                mode_name='asgi',
            ),
        ),
    )
    offerings = (
        RuntimeServiceOffering(
            interface_name='application/http',
            provider_name='demo',
            mode='asgi',
        ),
    )

    issues = resolve_runtime_requirement_issues(requirements, offerings)

    assert issues == ()


def test_required_mode_reports_not_executable_issue_when_mode_differs(
) -> None:
    requirements = RuntimeRequirementSet(
        required_modes=(
            RuntimeModeRequirement(
                interface_name='application/http',
                mode_name='asgi',
            ),
        ),
    )
    offerings = (
        RuntimeServiceOffering(
            interface_name='application/http',
            provider_name='demo',
            mode='wsgi',
        ),
    )

    issues = resolve_runtime_requirement_issues(requirements, offerings)

    assert issues == (
        "Runtime interface 'application/http' requires mode 'asgi' "
        'in active profile',
    )


def test_disallow_mode_keeps_existing_behavior() -> None:
    requirements = RuntimeRequirementSet(
        disallowed_modes=(
            RuntimeModeDisallowance(
                interface_name='application/http',
                mode_name='wsgi',
            ),
        ),
    )
    offerings = (
        RuntimeServiceOffering(
            interface_name='application/http',
            provider_name='demo',
            mode='wsgi',
        ),
    )

    issues = resolve_runtime_requirement_issues(requirements, offerings)

    assert issues == (
        "Runtime interface 'application/http' uses disallowed mode 'wsgi'",
    )


def test_runtime_requirement_set_from_dict_is_backward_compatible() -> None:
    requirement_set = RuntimeRequirementSet.from_dict(
        {
            'interfaces': ['application/http'],
            'capabilities': [],
            'disallowed_modes': [],
        },
    )

    assert requirement_set.interfaces == ('application/http',)
    assert requirement_set.required_modes == ()


def test_runtime_profile_service_graph_reuses_resource_scope_invariants(
) -> None:
    profile = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='database/mongodb',
                provider='demo',
                scope='test',
            ),
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                scope='run',
                depends_on=('database/mongodb',),
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match=(
            r"Runtime profile 'web' declares invalid service graph: "
            r"Runtime service 'execution/engine' has invalid scope "
            r"dependency: 'execution/engine' \(run\) depends_on "
            r"'database/mongodb' \(test\)"
        ),
    ):
        validate_runtime_profile_service_graph(profile)


def test_runtime_profile_service_graph_reuses_resource_cycle_invariants(
) -> None:
    profile = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                depends_on=('database/mongodb',),
            ),
            RuntimeServiceSpec(
                interface='database/mongodb',
                provider='demo',
                depends_on=('execution/engine',),
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match=(
            r"Runtime profile 'web' declares invalid service graph: "
            r"Cyclic runtime service dependency detected at "
            r"'execution/engine'"
        ),
    ):
        validate_runtime_profile_service_graph(profile)


def test_build_runtime_service_shadow_requirements_preserves_declared_graph(
) -> None:
    profile = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                scope='worker',
                depends_on=('database/mongodb',),
                initializes_from=('transport/http',),
                initialization_mode='state_snapshot',
            ),
        ),
    )

    requirements = build_runtime_service_shadow_requirements(profile)

    assert len(requirements) == 1
    requirement = requirements[0]
    assert requirement.name == 'execution/engine'
    assert requirement.scope == 'worker'
    assert requirement.depends_on == ('database/mongodb',)
    assert requirement.initializes_from == ('transport/http',)
    assert requirement.initialization_mode == 'state_snapshot'


def test_build_runtime_service_shadow_requirements_preserves_readiness_policy(
) -> None:
    readiness_policy = RuntimeReadinessPolicy(
        initial_delay_seconds=2.5,
        max_wait_seconds=15.0,
    )
    profile = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                readiness_policy=readiness_policy,
            ),
        ),
    )

    requirement = build_runtime_service_shadow_requirements(profile)[0]

    assert requirement.readiness_policy == readiness_policy


def test_runtime_profile_service_capabilities_validate_via_execution_family(
) -> None:
    profile = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                capabilities=('run',),
            ),
        ),
    )

    validate_runtime_profile_service_graph(profile)


def test_cold_import_of_items_succeeds() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env = os.environ.copy()
    env['PYTHONPATH'] = str(repo_root / 'packages' / 'cosecha-core' / 'src')

    result = subprocess.run(
        [sys.executable, '-c', 'import cosecha.core.items'],
        cwd=repo_root,
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_runtime_profile_service_graph_reuses_initializer_scope_invariants(
) -> None:
    profile = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='database/mongodb',
                provider='demo',
                scope='test',
            ),
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                scope='worker',
                initializes_from=('database/mongodb',),
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match=(
            r"Runtime profile 'web' declares invalid service graph: "
            r"Runtime service 'execution/engine' has invalid "
            r"initialization source scope: 'execution/engine' "
            r"\(worker\) initializes_from 'database/mongodb' \(test\)"
        ),
    ):
        validate_runtime_profile_service_graph(profile)


def test_runtime_profile_service_graph_detects_mixed_dependency_cycle() -> (
    None
):
    profile = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='execution/engine',
                provider='demo',
                depends_on=('database/mongodb',),
            ),
            RuntimeServiceSpec(
                interface='database/mongodb',
                provider='demo',
                initializes_from=('execution/engine',),
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match=(
            r"Runtime profile 'web' declares invalid service graph: "
            r"Cyclic runtime service dependency detected at "
            r"'execution/engine'"
        ),
    ):
        validate_runtime_profile_service_graph(profile)


def test_runtime_profile_service_rejects_legacy_plan_run_capability() -> None:
    if validate_runtime_interface_name('execution/plan-run') is not None:
        pytest.skip('Installed cxp does not expose execution/plan-run yet')

    profile = RuntimeProfileSpec(
        id='web',
        services=(
            RuntimeServiceSpec(
                interface='execution/plan-run',
                provider='demo',
                capabilities=('draft_validation',),
            ),
        ),
    )

    with pytest.raises(
        ManifestValidationError,
        match=(
            r"Runtime profile 'web' declares invalid capabilities for "
            r"'execution/plan-run': Unknown capabilities: draft_validation"
        ),
    ):
        validate_runtime_profile_service_graph(profile)
