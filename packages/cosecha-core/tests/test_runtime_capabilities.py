from __future__ import annotations

from cosecha.core.capabilities import (
    CAPABILITY_LIVE_EXECUTION_OBSERVABILITY,
    build_capability_map,
)
from cosecha.core.runtime import LocalRuntimeProvider, ProcessRuntimeProvider


def _attribute_map(descriptor) -> dict[str, object]:
    return {
        attribute.name: attribute.value
        for attribute in descriptor.attributes
    }


def _operation_types(descriptor) -> set[str]:
    return {
        operation.operation_type
        for operation in descriptor.operations
    }


def test_runtime_providers_publish_expected_capability_matrix() -> None:
    local_capabilities = build_capability_map(
        LocalRuntimeProvider().describe_capabilities(),
    )
    process_capabilities = build_capability_map(
        ProcessRuntimeProvider().describe_capabilities(),
    )

    local_observability = local_capabilities[
        CAPABILITY_LIVE_EXECUTION_OBSERVABILITY
    ]
    process_observability = process_capabilities[
        CAPABILITY_LIVE_EXECUTION_OBSERVABILITY
    ]

    assert local_capabilities['isolated_processes'].level == 'unsupported'
    assert local_capabilities['run_scoped_resources'].level == 'supported'
    assert local_capabilities['worker_scoped_resources'].level == (
        'accepted_noop'
    )
    assert local_observability.level == 'supported'
    assert _attribute_map(local_observability)['granularity'] == 'streaming'
    assert _attribute_map(local_observability)['live_channels'] == (
        'events',
        'logs',
    )
    assert _operation_types(local_observability) == {
        'execution.subscribe',
        'execution.live_status',
        'execution.live_tail',
    }

    assert process_capabilities['isolated_processes'].level == 'supported'
    assert _attribute_map(process_capabilities['isolated_processes'])[
        'isolation_unit'
    ] == 'worker_process'
    assert process_capabilities['persistent_workers'].level == 'supported'
    assert process_capabilities['worker_scoped_resources'].level == (
        'supported'
    )
    assert process_observability.level == 'supported'
    assert _attribute_map(process_observability)[
        'granularity'
    ] == 'consolidated_response'
    assert _attribute_map(process_observability)['live_channels'] == (
        'events',
        'logs',
    )
    assert _operation_types(process_observability) == {
        'execution.subscribe',
        'execution.live_status',
        'execution.live_tail',
    }
