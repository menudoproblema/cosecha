from __future__ import annotations

import asyncio
import json
import sys

from cosecha.core.operations import (
    AnalyzePlanOperation,
    QueryCapabilitiesOperation,
    QueryDefinitionsOperation,
    QueryEventsOperation,
    QueryRegistryItemsOperation,
    QueryResourcesOperation,
    QuerySessionArtifactsOperation,
    QueryTestsOperation,
    RunOperation,
)
from cosecha_mcp.service import CosechaMcpService
from cosecha_mcp.workspace import resolve_cosecha_workspace


def _deserialize_operation(payload: dict[str, object]):
    operation_type = payload.get('operation_type')
    factories = {
        'capabilities.query': QueryCapabilitiesOperation.from_dict,
        'knowledge.query_definitions': QueryDefinitionsOperation.from_dict,
        'knowledge.query_events': QueryEventsOperation.from_dict,
        'knowledge.query_registry_items': (
            QueryRegistryItemsOperation.from_dict
        ),
        'knowledge.query_resources': QueryResourcesOperation.from_dict,
        'knowledge.query_session_artifacts': (
            QuerySessionArtifactsOperation.from_dict
        ),
        'knowledge.query_tests': QueryTestsOperation.from_dict,
        'plan.analyze': AnalyzePlanOperation.from_dict,
        'run': RunOperation.from_dict,
    }
    factory = factories.get(operation_type)
    if factory is None:
        msg = f'Unsupported runner operation: {operation_type!r}'
        raise ValueError(msg)
    return factory(payload)


async def _execute_request(request: dict[str, object]) -> dict[str, object]:
    start_path = request.get('start_path')
    workspace = resolve_cosecha_workspace(
        None if start_path is None else str(start_path),
    )

    operation_payload = request.get('operation')
    if not isinstance(operation_payload, dict):
        msg = 'Worker request requires an operation object'
        raise ValueError(msg)

    selected_engine_names = request.get('selected_engine_names')
    normalized_selected_engine_names = (
        {
            str(engine_name)
            for engine_name in selected_engine_names
        }
        if isinstance(selected_engine_names, list)
        else None
    )

    service = CosechaMcpService()
    runner = service._build_runner_for_selection(
        workspace,
        selected_engine_names=normalized_selected_engine_names,
    )
    try:
        result = await runner.execute_operation(
            _deserialize_operation(operation_payload),
        )
        return {
            'engine_names': sorted(engine.name for engine in runner.engines),
            'result': result.to_dict(),
            'workspace': workspace.to_dict(),
        }
    finally:
        service._close_runner(runner)


def main() -> None:
    try:
        request = json.load(sys.stdin)
        if not isinstance(request, dict):
            msg = 'Worker request must be a JSON object'
            raise ValueError(msg)
        response = asyncio.run(_execute_request(request))
    except Exception as error:  # pragma: no cover - surfaced to parent stderr
        sys.stderr.write(f'{error}\n')
        raise SystemExit(1) from error

    json.dump(response, sys.stdout, ensure_ascii=False)


if __name__ == '__main__':
    main()
