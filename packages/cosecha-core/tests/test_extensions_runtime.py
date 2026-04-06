from __future__ import annotations

import asyncio

from cosecha.core.capabilities import CapabilityDescriptor
from cosecha.core.collector import Collector
from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.extensions import ExtensionQuery
from cosecha.core.operations import (
    QueryExtensionsOperation,
    QueryExtensionsOperationResult,
)
from cosecha.core.plugins.base import CapabilityPublisher
from cosecha.core.runner import Runner
from cosecha_internal.testkit import DummyReporter, build_config


class _EmptyCollector(Collector):
    def __init__(self) -> None:
        super().__init__('feature')

    async def find_test_files(self, base_path):
        del base_path
        return []

    async def load_tests_from_file(self, test_path):
        del test_path
        return []


class _DummyContext(BaseContext):
    async def cleanup(self) -> None:
        return None


class _DummyEngine(Engine):
    async def generate_new_context(self, test) -> BaseContext:
        del test
        return _DummyContext()


class _ExtensionTestPlugin(CapabilityPublisher):
    @classmethod
    def register_arguments(cls, parser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args):
        del args
        return cls()

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name='extension_test_capability',
                level='supported',
            ),
        )

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


def _build_runner(tmp_path) -> Runner:
    engine = _DummyEngine(
        'gherkin',
        _EmptyCollector(),
        DummyReporter(),
    )
    return Runner(
        build_config(tmp_path),
        {'': engine},
        plugins=[_ExtensionTestPlugin()],
    )


def test_runner_exposes_public_extension_snapshots(tmp_path) -> None:
    runner = _build_runner(tmp_path)

    snapshots = runner.describe_system_extensions()
    kinds = {snapshot.descriptor.extension_kind for snapshot in snapshots}

    assert kinds == {'engine', 'plugin', 'reporter', 'runtime'}
    plugin_snapshot = next(
        snapshot
        for snapshot in snapshots
        if snapshot.descriptor.extension_kind == 'plugin'
    )
    assert plugin_snapshot.component_name == '_ExtensionTestPlugin'
    assert plugin_snapshot.descriptor.surfaces == ('capability_publisher',)
    assert plugin_snapshot.descriptor.published_capabilities == (
        'extension_test_capability',
    )


def test_runner_can_query_extension_snapshots(tmp_path) -> None:
    runner = _build_runner(tmp_path)

    result = asyncio.run(
        runner.execute_operation(
            QueryExtensionsOperation(
                query=ExtensionQuery(extension_kind='plugin'),
            ),
        ),
    )

    assert isinstance(result, QueryExtensionsOperationResult)
    assert len(result.snapshots) == 1
    assert result.snapshots[0].component_name == '_ExtensionTestPlugin'
