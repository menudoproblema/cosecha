from __future__ import annotations

import sys

from pathlib import Path

from cosecha.core.capabilities import (
    CAPABILITY_ARTIFACT_OUTPUT,
    CAPABILITY_DRAFT_VALIDATION,
    CAPABILITY_HUMAN_OUTPUT,
    CAPABILITY_LAZY_PROJECT_DEFINITION_LOADING,
    CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE,
    CAPABILITY_PLAN_EXPLANATION,
    CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
    CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
    CAPABILITY_PROJECT_REGISTRY_KNOWLEDGE,
    CAPABILITY_REPORT_LIFECYCLE,
    CAPABILITY_RESULT_PROJECTION,
    CAPABILITY_SELECTION_LABELS,
    CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY,
    CAPABILITY_STRUCTURED_OUTPUT,
    CapabilityAttribute,
    CapabilityDescriptor,
    CapabilityOperationBinding,
)
from cosecha.core.cxp_adapters import (
    build_cxp_engine_component_snapshot,
    build_cxp_instrumentation_component_snapshot,
    build_cxp_plugin_component_snapshot,
    build_cxp_reporter_component_snapshot,
    build_cxp_runtime_component_snapshot,
)
from cosecha.core.knowledge_base import (
    LIVE_EXECUTION_EVENT_TAIL_LIMIT,
    LIVE_EXECUTION_RESOURCE_LIMIT,
    LIVE_EXECUTION_RUNNING_TEST_LIMIT,
    LIVE_EXECUTION_WORKER_LIMIT,
)
from cosecha.core.extensions import (
    ExtensionDescriptor,
    build_engine_extension_snapshot,
    build_instrumentation_extension_snapshot,
    build_plugin_extension_snapshot,
    build_reporter_extension_snapshot,
)


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[4] / 'cxp' / 'src'),
)
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[2]
        / 'cosecha-engine-gherkin'
        / 'src',
    ),
)
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[2]
        / 'cosecha-engine-pytest'
        / 'src',
    ),
)
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[2]
        / 'cosecha-instrumentation-coverage'
        / 'src',
    ),
)
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[2]
        / 'cosecha-plugin-timing'
        / 'src',
    ),
)
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[2]
        / 'cosecha-reporter-console'
        / 'src',
    ),
)
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[2]
        / 'cosecha-reporter-json'
        / 'src',
    ),
)
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[2]
        / 'cosecha-reporter-junit'
        / 'src',
    ),
)

from cxp import (
    COSECHA_ENGINE_CATALOG,
    COSECHA_INSTRUMENTATION_CATALOG,
    COSECHA_PLUGIN_CATALOG,
    COSECHA_REPORTER_CATALOG,
)
from cxp.catalogs.interfaces.cosecha.runtime import COSECHA_RUNTIME_CATALOG

from cosecha.core.plugins.telemetry import TelemetryPlugin
from cosecha.core.plugins.base import Plugin
from cosecha.core.reporter import Reporter
from cosecha.core.runtime import LocalRuntimeProvider, ProcessRuntimeProvider
from cosecha.engine.gherkin.engine import GherkinEngine
from cosecha.engine.pytest.engine import PytestEngine
from cosecha.instrumentation.coverage import CoverageInstrumenter
from cosecha.plugin.timing import TimingPlugin
from cosecha.reporter.console import ConsoleReporter
from cosecha.reporter.json import JsonReporter
from cosecha.reporter.junit import JUnitReporter


class _FakeCollector:
    file_type = '.feature'


class _FakeEngine:
    def __init__(self) -> None:
        self.name = 'gherkin'
        self.collector = _FakeCollector()

    @classmethod
    def engine_api_version(cls) -> int:
        return 1

    @classmethod
    def engine_stability(cls) -> str:
        return 'stable'

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name=CAPABILITY_DRAFT_VALIDATION,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='draft.validate',
                        result_type='draft.validation',
                        freshness='fresh',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_SELECTION_LABELS,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='label_sources',
                        value=('feature_tag', 'scenario_tag'),
                    ),
                    CapabilityAttribute(
                        name='supports_glob_matching',
                        value=True,
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='plan.analyze',
                        result_type='plan.analysis',
                    ),
                    CapabilityOperationBinding(
                        operation_type='plan.explain',
                        result_type='plan.explanation',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='definition.resolve',
                        result_type='definition.resolution',
                        freshness='fresh',
                    ),
                    CapabilityOperationBinding(
                        operation_type='knowledge.query_definitions',
                        result_type='knowledge.definitions',
                        freshness='knowledge_base',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='knowledge.query_definitions',
                        result_type='knowledge.definitions',
                        freshness='knowledge_base',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_PROJECT_REGISTRY_KNOWLEDGE,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='knowledge.query_registry_items',
                        result_type='knowledge.registry_items',
                        freshness='knowledge_base',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_PLAN_EXPLANATION,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='plan.explain',
                        result_type='plan.explanation',
                    ),
                    CapabilityOperationBinding(
                        operation_type='plan.simulate',
                        result_type='plan.simulation',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='discovery_backend',
                        value='ast',
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='knowledge.query_tests',
                        result_type='knowledge.tests',
                        freshness='fresh',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_LAZY_PROJECT_DEFINITION_LOADING,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='materialization_granularity',
                        value='file',
                    ),
                ),
            ),
        )

    def describe_engine_dependencies(self) -> tuple[object, ...]:
        return (object(),)


class _InferentialStructuredReporter(Reporter):
    @classmethod
    def reporter_name(cls) -> str:
        return 'inferential-structured'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'structured'

    async def add_test(self, test):
        del test

    async def add_test_result(self, test):
        del test

    async def print_report(self):
        return None


class _HumanArtifactReporter(Reporter):
    @classmethod
    def reporter_name(cls) -> str:
        return 'html'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'html'

    @classmethod
    def describe_capabilities(cls) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name=CAPABILITY_REPORT_LIFECYCLE,
                level='supported',
                operations=(
                    CapabilityOperationBinding('reporter.start'),
                    CapabilityOperationBinding('reporter.print_report'),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_RESULT_PROJECTION,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='supports_engine_specific_projection',
                        value=False,
                    ),
                ),
                operations=(
                    CapabilityOperationBinding('reporter.add_test'),
                    CapabilityOperationBinding('reporter.add_test_result'),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_ARTIFACT_OUTPUT,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='artifact_formats',
                        value=('html',),
                    ),
                ),
                operations=(
                    CapabilityOperationBinding('reporter.print_report'),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_HUMAN_OUTPUT,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='output_kind',
                        value='html',
                    ),
                    CapabilityAttribute(
                        name='supports_engine_specific_projection',
                        value=False,
                    ),
                ),
                operations=(
                    CapabilityOperationBinding('reporter.print_report'),
                ),
            ),
        )

    async def add_test(self, test):
        del test

    async def add_test_result(self, test):
        del test

    async def print_report(self):
        return None


class _ForwardCompatibleRuntimeProvider(LocalRuntimeProvider):
    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            *super().describe_capabilities(),
            CapabilityDescriptor(
                name='future_runtime_capability',
                level='supported',
            ),
        )


class _ForwardCompatibleInstrumentation:
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
            *CoverageInstrumenter.describe_capabilities(),
            CapabilityDescriptor(
                name='future_instrumentation_capability',
                level='supported',
            ),
        )


class _TelemetryNamedPlugin(Plugin):
    @classmethod
    def plugin_name(cls) -> str:
        return 'telemetry-ish'

    @classmethod
    def register_arguments(cls, parser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args):
        del args
        return cls()

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


def test_extension_descriptor_from_dict_is_backward_compatible() -> None:
    descriptor = ExtensionDescriptor.from_dict(
        {
            'canonical_name': 'json',
            'extension_kind': 'reporter',
            'api_version': 1,
            'stability': 'stable',
            'implementation': 'pkg:Reporter',
            'published_capabilities': [],
            'compatibility': [],
            'surfaces': [],
        },
    )

    assert descriptor.cxp_interface is None


def test_extension_snapshot_builders_publish_cxp_interface() -> None:
    engine_snapshot = build_engine_extension_snapshot(
        _FakeEngine(),
        descriptors=_FakeEngine().describe_capabilities(),
    )
    reporter_snapshot = build_reporter_extension_snapshot(
        JsonReporter(Path('report.json')),
    )
    plugin_snapshot = build_plugin_extension_snapshot(
        TelemetryPlugin(Path('telemetry.jsonl')),
        descriptors=(),
    )
    instrumentation_snapshot = build_instrumentation_extension_snapshot(
        CoverageInstrumenter,
        descriptors=CoverageInstrumenter.describe_capabilities(),
    )

    assert engine_snapshot.descriptor.cxp_interface == 'cosecha/engine'
    assert reporter_snapshot.descriptor.cxp_interface == 'cosecha/reporter'
    assert plugin_snapshot.descriptor.cxp_interface == 'cosecha/plugin'
    assert (
        instrumentation_snapshot.descriptor.cxp_interface
        == 'cosecha/instrumentation'
    )
    from cosecha.core.extensions import build_runtime_extension_snapshot

    runtime_snapshot = build_runtime_extension_snapshot(
        LocalRuntimeProvider(),
        descriptors=LocalRuntimeProvider().describe_capabilities(),
    )

    assert runtime_snapshot.descriptor.cxp_interface == 'cosecha/runtime'


def test_engine_adapter_builds_snapshot_valid_for_cxp_catalog() -> None:
    snapshot = build_cxp_engine_component_snapshot(_FakeEngine())

    assert COSECHA_ENGINE_CATALOG.is_component_snapshot_compliant(snapshot)
    test_lifecycle = next(
        capability
        for capability in snapshot.capabilities
        if capability.name == 'test_lifecycle'
    )
    assert tuple(
        operation.operation_name for operation in test_lifecycle.operations
    ) == (
        'test.start',
        'test.finish',
        'test.execute',
        'test.phase',
    )


def test_reporter_and_plugin_adapters_build_valid_cxp_snapshots() -> None:
    reporter_snapshot = build_cxp_reporter_component_snapshot(
        JsonReporter(Path('report.json')),
    )
    plugin_snapshot = build_cxp_plugin_component_snapshot(
        TelemetryPlugin(Path('telemetry.jsonl')),
    )

    assert COSECHA_REPORTER_CATALOG.is_component_snapshot_compliant(
        reporter_snapshot,
    )
    assert COSECHA_PLUGIN_CATALOG.is_component_snapshot_compliant(
        plugin_snapshot,
    )


def test_runtime_adapter_builds_valid_cxp_snapshots() -> None:
    for runtime_provider in (
        LocalRuntimeProvider(),
        ProcessRuntimeProvider(),
    ):
        snapshot = build_cxp_runtime_component_snapshot(runtime_provider)

        assert COSECHA_RUNTIME_CATALOG.is_component_snapshot_compliant(
            snapshot,
        )


def test_instrumentation_adapter_builds_valid_cxp_snapshot() -> None:
    snapshot = build_cxp_instrumentation_component_snapshot(
        CoverageInstrumenter,
    )

    assert COSECHA_INSTRUMENTATION_CATALOG.is_component_snapshot_compliant(
        snapshot,
    )


def test_real_engine_packages_build_valid_cxp_snapshots() -> None:
    for engine in (GherkinEngine('gherkin'), PytestEngine('pytest')):
        snapshot = build_cxp_engine_component_snapshot(engine)

        assert COSECHA_ENGINE_CATALOG.is_component_snapshot_compliant(snapshot)
        selection_labels = next(
            capability
            for capability in snapshot.capabilities
            if capability.name == 'selection_labels'
        )
        assert tuple(
            operation.operation_name
            for operation in selection_labels.operations
        ) == (
            'run',
            'plan.analyze',
            'plan.explain',
            'plan.simulate',
        )


def test_real_reporter_packages_build_valid_cxp_snapshots() -> None:
    for reporter in (
        ConsoleReporter(),
        JsonReporter(Path('report.json')),
        JUnitReporter(Path('report.xml')),
    ):
        snapshot = build_cxp_reporter_component_snapshot(reporter)

        assert COSECHA_REPORTER_CATALOG.is_component_snapshot_compliant(
            snapshot,
        )


def test_reporter_adapter_uses_explicit_capabilities_instead_of_output_inference() -> None:
    snapshot = build_cxp_reporter_component_snapshot(
        _InferentialStructuredReporter(),
    )

    assert {
        capability.name for capability in snapshot.capabilities
    } == {
        'report_lifecycle',
        'result_projection',
    }
    assert COSECHA_REPORTER_CATALOG.is_component_snapshot_compliant(snapshot)


def test_reporter_adapter_supports_human_and_artifact_capabilities_together() -> None:
    snapshot = build_cxp_reporter_component_snapshot(_HumanArtifactReporter())

    assert {
        capability.name for capability in snapshot.capabilities
    } == {
        'report_lifecycle',
        'result_projection',
        'artifact_output',
        'human_output',
    }
    assert COSECHA_REPORTER_CATALOG.is_component_snapshot_compliant(snapshot)


def test_real_plugin_packages_build_valid_cxp_snapshots() -> None:
    for plugin in (
        TimingPlugin(),
        TelemetryPlugin(Path('telemetry.jsonl')),
    ):
        snapshot = build_cxp_plugin_component_snapshot(plugin)

        assert COSECHA_PLUGIN_CATALOG.is_component_snapshot_compliant(
            snapshot,
        )


def test_plugin_adapter_publishes_only_declared_optional_capabilities() -> None:
    telemetry_snapshot = build_cxp_plugin_component_snapshot(
        TelemetryPlugin(Path('telemetry.jsonl')),
    )
    timing_snapshot = build_cxp_plugin_component_snapshot(TimingPlugin())

    assert {
        capability.name for capability in telemetry_snapshot.capabilities
    } == {
        'plugin_lifecycle',
        'surface_publication',
        'capability_requirements',
        'telemetry_export',
    }
    assert {
        capability.name for capability in timing_snapshot.capabilities
    } == {
        'plugin_lifecycle',
        'surface_publication',
        'capability_requirements',
        'timing_summary',
    }


def test_plugin_adapter_does_not_infer_capabilities_from_plugin_name() -> None:
    snapshot = build_cxp_plugin_component_snapshot(_TelemetryNamedPlugin())

    assert {
        capability.name for capability in snapshot.capabilities
    } == {
        'plugin_lifecycle',
        'surface_publication',
        'capability_requirements',
    }


def test_runtime_adapter_preserves_live_observability_metadata_shape() -> None:
    snapshot = build_cxp_runtime_component_snapshot(LocalRuntimeProvider())

    live_capability = next(
        capability
        for capability in snapshot.capabilities
        if capability.name == 'live_execution_observability'
    )
    assert live_capability.metadata == {
        'read_only': True,
        'live_source': 'live_projection',
        'delivery_mode': 'poll_by_cursor',
        'granularity': 'streaming',
        'live_channels': ['events', 'logs'],
        'running_test_limit': LIVE_EXECUTION_RUNNING_TEST_LIMIT,
        'worker_limit': LIVE_EXECUTION_WORKER_LIMIT,
        'resource_limit': LIVE_EXECUTION_RESOURCE_LIMIT,
        'event_tail_limit': LIVE_EXECUTION_EVENT_TAIL_LIMIT,
    }


def test_runtime_adapter_preserves_unknown_declared_capabilities() -> None:
    snapshot = build_cxp_runtime_component_snapshot(
        _ForwardCompatibleRuntimeProvider(),
    )

    assert any(
        capability.name == 'future_runtime_capability'
        for capability in snapshot.capabilities
    )
    assert COSECHA_RUNTIME_CATALOG.is_component_snapshot_compliant(snapshot) is False


def test_instrumentation_adapter_excludes_internal_shadow_capability_only() -> None:
    snapshot = build_cxp_instrumentation_component_snapshot(
        _ForwardCompatibleInstrumentation,
    )

    assert {
        capability.name for capability in snapshot.capabilities
    } >= {
        'instrumentation_bootstrap',
        'session_summary',
        'structured_summary',
        'future_instrumentation_capability',
    }
    assert CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS not in {
        capability.name for capability in snapshot.capabilities
    }
    assert (
        COSECHA_INSTRUMENTATION_CATALOG.is_component_snapshot_compliant(
            snapshot,
        )
        is False
    )
