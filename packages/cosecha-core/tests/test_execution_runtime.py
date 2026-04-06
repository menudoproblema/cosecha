from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.execution_ir import TestExecutionNode
from cosecha.core.execution_runtime import (
    ExecutionBodyOptions,
    execute_test_body,
)
from cosecha.core.items import TestItem, TestResultStatus
from cosecha.core.reporting_ir import TestReport
from cosecha.core.resources import ResourceManager, ResourceRequirement
from cosecha.core.runtime import ExecutionBodyResult, LocalRuntimeProvider
from cosecha_internal.testkit import DummyReporter, ListCollector, build_config


if TYPE_CHECKING:
    from pathlib import Path


class CleanupFailingContext(BaseContext):
    def __init__(self) -> None:
        self.resources: dict[str, object] = {}

    async def cleanup(self) -> None:
        msg = 'cleanup boom'
        raise RuntimeError(msg)

    def set_resources(self, resources: dict[str, object]) -> None:
        self.resources = resources.copy()


class DummyEngine(Engine):
    async def generate_new_context(self, test: TestItem) -> BaseContext:
        del test
        return CleanupFailingContext()


class RuntimeTestItem(TestItem):
    cleanup_calls = 0

    async def run(self, context) -> None:
        assert context.resources['session_db'] == 'session-db'
        self.status = TestResultStatus.PASSED

    def has_selection_label(self, name: str) -> bool:
        del name
        return False

    def get_resource_requirements(self) -> tuple[ResourceRequirement, ...]:
        return (
            ResourceRequirement(
                name='session_db',
                scope='test',
                setup=lambda: 'session-db',
                cleanup=self._cleanup_session_resource,
            ),
        )

    @classmethod
    async def _cleanup_session_resource(cls, resource: object) -> None:
        del resource
        cls.cleanup_calls += 1


def test_execute_test_body_handles_cleanup_error_and_releases_resources(
    tmp_path: Path,
) -> None:
    RuntimeTestItem.cleanup_calls = 0
    test = RuntimeTestItem(tmp_path / 'runtime-body.feature')
    engine = DummyEngine(
        'dummy',
        collector=ListCollector([test]),
        reporter=DummyReporter(),
    )
    engine.initialize(build_config(tmp_path), '')
    node = TestExecutionNode(
        id='dummy:runtime-body.feature:0',
        stable_id='dummy:runtime-body.feature:a1b2c3d4e5f6',
        engine=engine,
        test=test,
        engine_name='dummy',
        test_name='RuntimeTestItem',
        test_path='runtime-body.feature',
        resource_requirements=test.get_resource_requirements(),
    )

    result = asyncio.run(
        execute_test_body(
            node,
            ResourceManager(),
            ExecutionBodyOptions(root_path=tmp_path),
        ),
    )

    assert result.report.status == TestResultStatus.ERROR
    assert result.report.failure_kind == 'runtime'
    assert result.phase_durations['resource_release'] >= 0.0
    assert 'cleanup' not in result.phase_durations
    assert RuntimeTestItem.cleanup_calls == 1


def test_local_runtime_provider_preserves_executor_result() -> None:
    runtime_provider = LocalRuntimeProvider()
    expected_result = ExecutionBodyResult(
        report=TestReport(
            path='runtime-body.feature',
            status=TestResultStatus.PASSED,
            message='ok',
            duration=0.1,
        ),
        phase_durations={'run': 0.05},
        resource_timings=(),
    )

    async def _executor(_node) -> ExecutionBodyResult:
        return expected_result

    class DummyNode:
        id = 'dummy:runtime-body.feature:0'

    result = asyncio.run(runtime_provider.execute(DummyNode(), _executor))

    assert result == expected_result
