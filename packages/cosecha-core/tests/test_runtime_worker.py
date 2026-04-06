from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

from cosecha.core.resources import (
    ResourceError,
    ResourceManager,
    ResourceRequirement,
)
from cosecha.core.runtime_protocol import RuntimeEnvelopeMetadata
from cosecha.core.runtime_worker import (
    _build_worker_error_response,
    _WorkerStateRegistrySink,
)
from cosecha.core.serialization import decode_json_dict


if TYPE_CHECKING:
    from pathlib import Path


WORKER_ID = 3


def test_worker_state_registry_sink_persists_runtime_snapshots(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / 'worker-state.json'
    sink = _WorkerStateRegistrySink(state_path, worker_id=WORKER_ID)
    manager = ResourceManager()

    async def _run() -> None:
        await manager.acquire_for_test(
            'node-1',
            (
                ResourceRequirement(
                    name='session_db',
                    scope='run',
                    setup=lambda: {'dsn': 'mongo://localhost/test'},
                ),
            ),
        )

    asyncio.run(_run())
    sink.sync_runtime_state(manager)

    payload = decode_json_dict(state_path.read_bytes())

    assert payload['worker_id'] == WORKER_ID
    assert payload['status'] == 'ready'
    assert payload['unhealthy_resources'] == []
    assert payload['readiness_states'] == [
        {
            'name': 'session_db',
            'reason': None,
            'scope': 'run',
            'status': 'ready',
        },
    ]
    assert payload['resource_timings'][0]['name'] == 'session_db'
    assert payload['resource_timings'][0]['scope'] == 'run'


def test_worker_state_registry_sink_tracks_pending_and_active_resources(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / 'worker-state.json'
    sink = _WorkerStateRegistrySink(state_path, worker_id=WORKER_ID)

    sink.record_resource_state(
        action='pending',
        name='browser',
        scope='worker',
        external_handle='browser-1',
    )
    pending_payload = decode_json_dict(state_path.read_bytes())

    assert pending_payload['pending_resources'] == [
        {
            'external_handle': 'browser-1',
            'name': 'browser',
            'scope': 'worker',
        },
    ]

    sink.record_resource_state(
        action='acquired',
        name='browser',
        scope='worker',
        external_handle='browser-1',
    )
    active_payload = decode_json_dict(state_path.read_bytes())

    assert active_payload['pending_resources'] == []
    assert active_payload['active_resources'] == [
        {
            'external_handle': 'browser-1',
            'name': 'browser',
            'scope': 'worker',
        },
    ]


def test_build_worker_error_response_preserves_typed_error_code() -> None:
    response = _build_worker_error_response(
        ResourceError(
            'session_db',
            'health check failed',
            code='resource_health_check_failed',
        ),
        metadata=RuntimeEnvelopeMetadata(in_reply_to='request-1'),
    )

    assert response.error.code == 'resource_health_check_failed'
    assert response.error.fatal is False
    assert response.error.recoverable is False
    assert response.metadata.in_reply_to == 'request-1'


def test_build_worker_error_response_uses_local_unhealthy_fallback() -> None:
    class LocalUnhealthyError(RuntimeError):
        unhealthy = True

    response = _build_worker_error_response(
        LocalUnhealthyError('disk full'),
        metadata=RuntimeEnvelopeMetadata(),
    )

    assert response.error.code == 'worker_local_unhealthy'

