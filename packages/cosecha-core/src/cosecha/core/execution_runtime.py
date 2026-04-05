from __future__ import annotations

import sys
import time

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cosecha.core.engines.base import ExecutionContextMetadata
from cosecha.core.items import TestResultStatus, resolve_failure_kind
from cosecha.core.reporting_ir import build_test_report
from cosecha.core.runtime import ExecutionBodyResult


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    from cosecha.core.execution_ir import TestExecutionNode
    from cosecha.core.resources import ResourceManager
    from cosecha.core.telemetry import TelemetryStream


@dataclass(slots=True, frozen=True)
class ExecutionBodyOptions:
    root_path: Path | None
    telemetry_stream: TelemetryStream | None = None
    parent_span_id: str | None = None
    telemetry_attributes: dict[str, object] = field(default_factory=dict)
    session_id: str | None = None
    plan_id: str | None = None
    trace_id: str | None = None
    worker_id: int | None = None


async def execute_test_body(
    node: TestExecutionNode,
    resource_manager: ResourceManager,
    options: ExecutionBodyOptions,
) -> ExecutionBodyResult:
    test = node.test
    engine = node.engine
    context = None
    resources: dict[str, object] = {}
    phase_durations: dict[str, float] = {}

    async def _run_phase(
        name: str,
        callback: Callable[[], Awaitable[None]],
    ) -> float:
        phase_start = time.perf_counter()
        if options.telemetry_stream is None:
            await callback()
            return time.perf_counter() - phase_start

        async with options.telemetry_stream.span(
            f'test.{name}',
            parent_span_id=options.parent_span_id,
            attributes=options.telemetry_attributes | {'phase': name},
        ):
            await callback()
        return time.perf_counter() - phase_start

    async def _acquire_resources() -> None:
        nonlocal resources
        resources = await resource_manager.acquire_for_test(
            node.id,
            node.resource_requirements,
            parent_span_id=options.parent_span_id,
            telemetry_attributes=options.telemetry_attributes,
        )

    async def _generate_context() -> None:
        nonlocal context
        context = await engine.generate_new_context(test)
        context.set_resources(resources)
        context.set_execution_metadata(
            ExecutionContextMetadata(
                node_id=node.id,
                node_stable_id=node.stable_id,
                session_id=options.session_id,
                plan_id=options.plan_id,
                trace_id=options.trace_id,
                worker_id=options.worker_id,
            ),
        )

    if test.status != TestResultStatus.SKIPPED:
        try:
            phase_durations['resource_acquire'] = await _run_phase(
                'resource_acquire',
                _acquire_resources,
            )
            phase_durations['generate_context'] = await _run_phase(
                'generate_context',
                _generate_context,
            )
            phase_durations['run'] = await _run_phase(
                'run',
                lambda: test.run(context),
            )
        except Exception:
            if test.status in (
                TestResultStatus.PENDING,
                TestResultStatus.PASSED,
            ):
                error = sys.exc_info()[1]
                test.status = (
                    TestResultStatus.FAILED
                    if isinstance(error, AssertionError)
                    else TestResultStatus.ERROR
                )
                test.message = (
                    'Test failed'
                    if isinstance(error, AssertionError)
                    else 'Error running test'
                )
                test.failure_kind = resolve_failure_kind(
                    error,
                    default='runtime',
                )
                test.error_code = getattr(error, 'code', None)
            test.exc_info = sys.exc_info()

    if context is not None:
        try:
            phase_durations['cleanup'] = await _run_phase(
                'cleanup',
                context.cleanup,
            )
        except Exception:
            if test.status in (
                TestResultStatus.PENDING,
                TestResultStatus.PASSED,
            ):
                test.status = TestResultStatus.ERROR
                test.message = 'Error cleaning up test context'
                error = sys.exc_info()[1]
                test.failure_kind = resolve_failure_kind(
                    error,
                    default='runtime',
                )
                test.error_code = getattr(error, 'code', None)
            test.exc_info = sys.exc_info()

    if resources:
        phase_durations['resource_release'] = await _run_phase(
            'resource_release',
            lambda: resource_manager.release_for_test(
                node.id,
                parent_span_id=options.parent_span_id,
                telemetry_attributes=options.telemetry_attributes,
            ),
        )

    return ExecutionBodyResult(
        report=build_test_report(test, options.root_path),
        phase_durations=phase_durations,
        resource_timings=resource_manager.pop_test_observed_timings(node.id),
    )
