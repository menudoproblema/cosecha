from __future__ import annotations

import asyncio

from cosecha.core.capabilities import CapabilityDescriptor
from cosecha.core.collector import Collector
from cosecha.core.discovery import (
    create_discovery_registry,
    using_discovery_registry,
)
from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.extensions import ExtensionQuery
from cosecha.core.capabilities import CapabilityOperationBinding
from cosecha.core.operations import (
    QueryCapabilitiesOperation,
    QueryCapabilitiesOperationResult,
    QueryExtensionsOperation,
    QueryExtensionsOperationResult,
)
from cosecha.core.plugins.base import CapabilityPublisher
from cosecha.core.runner import Runner
from cosecha.core.shadow import get_active_shadow
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


class _ShadowAwarePlugin(CapabilityPublisher):
    observed_shadow_root = None

    @classmethod
    def register_arguments(cls, parser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args):
        del args
        return cls()

    async def start(self):
        type(self).observed_shadow_root = get_active_shadow().root_path

    async def finish(self) -> None:
        return None


class _CoverageInstrumentation:
    @classmethod
    def instrumentation_name(cls) -> str:
        return 'coverage'

    @classmethod
    def instrumentation_api_version(cls) -> int:
        return 1

    @classmethod
    def instrumentation_stability(cls) -> str:
        return 'stable'

    @classmethod
    def describe_capabilities(cls) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name='instrumentation_bootstrap',
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='instrumentation.prepare',
                        result_type='instrumentation.contribution',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name='session_summary',
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='instrumentation.collect',
                        result_type='instrumentation.summary',
                    ),
                ),
            ),
        )


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
    registry = create_discovery_registry()
    registry.register_instrumentation_type(
        'coverage',
        _CoverageInstrumentation,
    )

    with using_discovery_registry(registry):
        runner = _build_runner(tmp_path)

        snapshots = runner.describe_system_extensions()
        kinds = {snapshot.descriptor.extension_kind for snapshot in snapshots}

        assert kinds == {
            'engine',
            'instrumentation',
            'plugin',
            'reporter',
            'runtime',
        }
        plugin_snapshot = next(
            snapshot
            for snapshot in snapshots
            if snapshot.descriptor.extension_kind == 'plugin'
        )
        assert plugin_snapshot.component_name == '_ExtensionTestPlugin'
        assert plugin_snapshot.descriptor.surfaces == (
            'capability_publisher',
        )
        assert plugin_snapshot.descriptor.published_capabilities == (
            'extension_test_capability',
        )
        instrumentation_snapshot = next(
            snapshot
            for snapshot in snapshots
            if snapshot.descriptor.extension_kind == 'instrumentation'
        )
        assert instrumentation_snapshot.component_name == 'coverage'
        assert instrumentation_snapshot.descriptor.cxp_interface == (
            'cosecha/instrumentation'
        )
        assert {
            *instrumentation_snapshot.descriptor.published_capabilities,
        } >= {
            'instrumentation_bootstrap',
            'session_summary',
        }
        assert (
            'produces_ephemeral_artifacts'
            not in instrumentation_snapshot.descriptor.published_capabilities
        )


def test_runner_can_query_extension_snapshots(tmp_path) -> None:
    registry = create_discovery_registry()
    registry.register_instrumentation_type(
        'coverage',
        _CoverageInstrumentation,
    )

    with using_discovery_registry(registry):
        runner = _build_runner(tmp_path)

        result = asyncio.run(
            runner.execute_operation(
                QueryExtensionsOperation(
                    query=ExtensionQuery(extension_kind='instrumentation'),
                ),
            ),
        )

        assert isinstance(result, QueryExtensionsOperationResult)
        assert len(result.snapshots) == 1
        assert result.snapshots[0].component_name == 'coverage'


def test_runner_exposes_instrumentation_capability_snapshots(tmp_path) -> None:
    registry = create_discovery_registry()
    registry.register_instrumentation_type(
        'coverage',
        _CoverageInstrumentation,
    )

    with using_discovery_registry(registry):
        runner = _build_runner(tmp_path)

        result = asyncio.run(
            runner.execute_operation(
                QueryCapabilitiesOperation(component_kind='instrumentation'),
            ),
        )

        assert isinstance(result, QueryCapabilitiesOperationResult)
        assert len(result.snapshots) == 1
        assert result.snapshots[0].component_name == 'coverage'


def test_runner_binds_shadow_for_controller_side_plugins(tmp_path) -> None:
    _ShadowAwarePlugin.observed_shadow_root = None
    registry = create_discovery_registry()
    registry.register_instrumentation_type(
        'coverage',
        _CoverageInstrumentation,
    )

    with using_discovery_registry(registry):
        runner = Runner(
            build_config(tmp_path),
            {'': _DummyEngine('gherkin', _EmptyCollector(), DummyReporter())},
            plugins=[_ShadowAwarePlugin()],
        )
        asyncio.run(runner.start_session())
        asyncio.run(runner.finish_session())

    assert _ShadowAwarePlugin.observed_shadow_root == (
        tmp_path / '.cosecha' / 'shadow' / runner._domain_event_session_id
    ).resolve()


def test_runner_run_does_not_write_outside_knowledge_storage_root(tmp_path) -> None:
    registry = create_discovery_registry()
    registry.register_instrumentation_type(
        'coverage',
        _CoverageInstrumentation,
    )

    with using_discovery_registry(registry):
        runner = _build_runner(tmp_path)
        before = {
            path.relative_to(tmp_path)
            for path in tmp_path.rglob('*')
            if path.is_file()
        }
        asyncio.run(runner.run())
        after = {
            path.relative_to(tmp_path)
            for path in tmp_path.rglob('*')
            if path.is_file()
        }

    created = after - before
    assert all(str(path).startswith('.cosecha/') for path in created)
