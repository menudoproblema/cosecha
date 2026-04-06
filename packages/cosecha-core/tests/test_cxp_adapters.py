from __future__ import annotations

import sys

from pathlib import Path

from cosecha.core.capabilities import (
    CAPABILITY_DRAFT_VALIDATION,
    CAPABILITY_LAZY_PROJECT_DEFINITION_LOADING,
    CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE,
    CAPABILITY_PLAN_EXPLANATION,
    CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
    CAPABILITY_PROJECT_REGISTRY_KNOWLEDGE,
    CAPABILITY_SELECTION_LABELS,
    CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY,
    CapabilityAttribute,
    CapabilityDescriptor,
    CapabilityOperationBinding,
)
from cosecha.core.cxp_adapters import (
    build_cxp_engine_component_snapshot,
    build_cxp_plugin_component_snapshot,
    build_cxp_reporter_component_snapshot,
)
from cosecha.core.extensions import (
    ExtensionDescriptor,
    build_engine_extension_snapshot,
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
    COSECHA_PLUGIN_CATALOG,
    COSECHA_REPORTER_CATALOG,
)

from cosecha.core.plugins.telemetry import TelemetryPlugin
from cosecha.engine.gherkin.engine import GherkinEngine
from cosecha.engine.pytest.engine import PytestEngine
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

    assert engine_snapshot.descriptor.cxp_interface == 'cosecha/engine'
    assert reporter_snapshot.descriptor.cxp_interface == 'cosecha/reporter'
    assert plugin_snapshot.descriptor.cxp_interface == 'cosecha/plugin'


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


def test_real_plugin_packages_build_valid_cxp_snapshots() -> None:
    for plugin in (
        TimingPlugin(),
        TelemetryPlugin(Path('telemetry.jsonl')),
    ):
        snapshot = build_cxp_plugin_component_snapshot(plugin)

        assert COSECHA_PLUGIN_CATALOG.is_component_snapshot_compliant(
            snapshot,
        )
