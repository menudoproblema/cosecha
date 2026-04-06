from __future__ import annotations

from typing import TYPE_CHECKING

from cxp import (
    COSECHA_ENGINE_INTERFACE,
    COSECHA_PLUGIN_INTERFACE,
    COSECHA_REPORTER_INTERFACE,
    CapabilityDescriptor as CxpCapabilityDescriptor,
    CapabilityOperationBinding as CxpCapabilityOperationBinding,
    ComponentCapabilitySnapshot as CxpComponentCapabilitySnapshot,
    ComponentIdentity,
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from cosecha.core.capabilities import CapabilityDescriptor
    from cosecha.core.engines.base import Engine
    from cosecha.core.plugins.base import Plugin
    from cosecha.core.reporter import Reporter


_PROJECT_DEFINITION_KNOWLEDGE = 'project_definition_knowledge'
_LIBRARY_DEFINITION_KNOWLEDGE = 'library_definition_knowledge'
_PROJECT_REGISTRY_KNOWLEDGE = 'project_registry_knowledge'
_STATIC_PROJECT_DEFINITION_DISCOVERY = 'static_project_definition_discovery'
_LAZY_PROJECT_DEFINITION_LOADING = 'lazy_project_definition_loading'


def build_cxp_engine_component_snapshot(
    engine: Engine,
) -> CxpComponentCapabilitySnapshot:
    descriptors = engine.describe_capabilities()
    dependency_rules = engine.describe_engine_dependencies()

    cxp_descriptors = [
        CxpCapabilityDescriptor(
            name='engine_lifecycle',
            level='supported',
            operations=(
                CxpCapabilityOperationBinding('collect'),
                CxpCapabilityOperationBinding('session.start'),
                CxpCapabilityOperationBinding('session.finish'),
            ),
        ),
        CxpCapabilityDescriptor(
            name='test_lifecycle',
            level='supported',
            operations=(
                CxpCapabilityOperationBinding('test.start'),
                CxpCapabilityOperationBinding('test.finish'),
                CxpCapabilityOperationBinding('test.execute'),
                CxpCapabilityOperationBinding('test.phase'),
            ),
        ),
    ]

    selection_labels = _find_descriptor(descriptors, 'selection_labels')
    if selection_labels is not None:
        label_sources = tuple(
            str(item)
            for item in _attribute_value(
                selection_labels,
                'label_sources',
                (),
            )
        )
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='selection_labels',
                level=selection_labels.level,
                summary=selection_labels.summary,
                operations=_filtered_cxp_operations(
                    selection_labels.operations,
                    {
                        'plan.analyze',
                        'plan.explain',
                        'plan.simulate',
                    },
                ),
                metadata={
                    'label_sources': list(label_sources),
                    'supports_glob_matching': bool(
                        _attribute_value(
                            selection_labels,
                            'supports_glob_matching',
                            default=False,
                        ),
                    ),
                },
            ),
        )

    draft_validation = _find_descriptor(descriptors, 'draft_validation')
    if draft_validation is not None:
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='draft_validation',
                level=draft_validation.level,
                summary=draft_validation.summary,
                operations=_cxp_operations(draft_validation.operations),
            ),
        )

    plan_explanation = _find_descriptor(descriptors, 'plan_explanation')
    if plan_explanation is not None:
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='plan_explanation',
                level=plan_explanation.level,
                summary=plan_explanation.summary,
                operations=_cxp_operations(plan_explanation.operations),
            ),
        )

    static_discovery = _find_descriptor(
        descriptors,
        _STATIC_PROJECT_DEFINITION_DISCOVERY,
    )
    if static_discovery is not None:
        backend = _attribute_value(
            static_discovery,
            'discovery_backend',
            'collector',
        )
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='static_definition_discovery',
                level=static_discovery.level,
                summary=static_discovery.summary,
                operations=_cxp_operations(static_discovery.operations),
                metadata={'discovery_backends': [str(backend)]},
            ),
        )

    lazy_materialization = _find_descriptor(
        descriptors,
        _LAZY_PROJECT_DEFINITION_LOADING,
    )
    if lazy_materialization is not None:
        granularity = _attribute_value(
            lazy_materialization,
            'materialization_granularity',
            'file',
        )
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='on_demand_definition_materialization',
                level=lazy_materialization.level,
                summary=lazy_materialization.summary,
                operations=(
                    CxpCapabilityOperationBinding('definition.resolve'),
                ),
                metadata={
                    'materialization_granularities': [str(granularity)],
                },
            ),
        )

    definition_knowledge = _build_definition_knowledge_descriptor(descriptors)
    if definition_knowledge is not None:
        cxp_descriptors.append(definition_knowledge)

    if dependency_rules:
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='engine_dependency_knowledge',
                level='supported',
                summary=(
                    'Cross-engine dependency rules published by the engine.'
                ),
                operations=(
                    CxpCapabilityOperationBinding(
                        'dependencies.describe',
                        result_type='engine.dependencies',
                    ),
                ),
            ),
        )

    return CxpComponentCapabilitySnapshot(
        component_name=engine.name,
        component_kind='engine',
        identity=ComponentIdentity(
            interface=COSECHA_ENGINE_INTERFACE,
            provider=engine.name,
            version=str(engine.engine_api_version()),
        ),
        capabilities=tuple(cxp_descriptors),
    )


def build_cxp_reporter_component_snapshot(
    reporter: Reporter,
) -> CxpComponentCapabilitySnapshot:
    descriptor_target = reporter.descriptor_target()
    reporter_name = descriptor_target.reporter_name()
    supports_engine_specific_projection = reporter_name in {
        'console',
        'json',
        'junit',
    }
    artifact_formats = ()
    if reporter_name == 'json':
        artifact_formats = ('json',)
    elif reporter_name == 'junit':
        artifact_formats = ('junit_xml',)

    return CxpComponentCapabilitySnapshot(
        component_name=reporter_name,
        component_kind='reporter',
        identity=ComponentIdentity(
            interface=COSECHA_REPORTER_INTERFACE,
            provider=reporter_name,
            version=str(descriptor_target.reporter_api_version()),
        ),
        capabilities=(
            CxpCapabilityDescriptor(
                name='report_lifecycle',
                level='supported',
                operations=(
                    CxpCapabilityOperationBinding('reporter.start'),
                    CxpCapabilityOperationBinding('reporter.print_report'),
                ),
            ),
            CxpCapabilityDescriptor(
                name='result_projection',
                level='supported',
                operations=(
                    CxpCapabilityOperationBinding('reporter.add_test'),
                    CxpCapabilityOperationBinding('reporter.add_test_result'),
                ),
                metadata={
                    'supports_engine_specific_projection': (
                        supports_engine_specific_projection
                    ),
                },
            ),
            CxpCapabilityDescriptor(
                name='artifact_output',
                level='supported',
                operations=(
                    CxpCapabilityOperationBinding('reporter.print_report'),
                ),
                metadata={
                    'output_kind': descriptor_target.reporter_output_kind(),
                    'artifact_formats': list(artifact_formats),
                    'supports_engine_specific_projection': (
                        supports_engine_specific_projection
                    ),
                },
            ),
        ),
    )


def build_cxp_plugin_component_snapshot(
    plugin: Plugin,
) -> CxpComponentCapabilitySnapshot:
    plugin_name = plugin.plugin_name()
    capabilities = [
        CxpCapabilityDescriptor(
            name='plugin_lifecycle',
            level='supported',
            operations=(
                CxpCapabilityOperationBinding('plugin.initialize'),
                CxpCapabilityOperationBinding('plugin.start'),
                CxpCapabilityOperationBinding('plugin.finish'),
                CxpCapabilityOperationBinding('plugin.after_session_closed'),
            ),
        ),
        CxpCapabilityDescriptor(
            name='surface_publication',
            level='supported',
            metadata={
                'provided_surfaces': list(plugin.provided_surfaces()),
            },
        ),
        CxpCapabilityDescriptor(
            name='capability_requirements',
            level='supported',
            metadata={
                'required_capabilities': list(plugin.required_capabilities()),
            },
        ),
    ]

    if 'coverage' in plugin_name.lower():
        capabilities.append(
            CxpCapabilityDescriptor(
                name='coverage_summary',
                level='supported',
                metadata={'output_formats': ['term', 'term-missing']},
            ),
        )
    if 'timing' in plugin_name.lower():
        capabilities.append(
            CxpCapabilityDescriptor(
                name='timing_summary',
                level='supported',
                metadata={'output_formats': ['console_summary']},
            ),
        )
    if 'telemetry' in plugin_name.lower():
        capabilities.append(
            CxpCapabilityDescriptor(
                name='telemetry_export',
                level='supported',
                metadata={'output_formats': ['jsonl']},
            ),
        )

    return CxpComponentCapabilitySnapshot(
        component_name=plugin_name,
        component_kind='plugin',
        identity=ComponentIdentity(
            interface=COSECHA_PLUGIN_INTERFACE,
            provider=plugin_name,
            version=str(plugin.plugin_api_version()),
        ),
        capabilities=tuple(capabilities),
    )


def _build_definition_knowledge_descriptor(
    descriptors: tuple[CapabilityDescriptor, ...],
) -> CxpCapabilityDescriptor | None:
    knowledge_descriptors = tuple(
        descriptor
        for descriptor in descriptors
        if descriptor.name
        in (
            _PROJECT_DEFINITION_KNOWLEDGE,
            _LIBRARY_DEFINITION_KNOWLEDGE,
            _PROJECT_REGISTRY_KNOWLEDGE,
        )
    )
    if not knowledge_descriptors:
        return None

    origin_kinds: list[str] = []
    scopes: list[str] = []
    operations: list[CxpCapabilityOperationBinding] = []
    supports_fresh_resolution = False
    supports_kb_projection = False

    for descriptor in knowledge_descriptors:
        if descriptor.name == _PROJECT_DEFINITION_KNOWLEDGE:
            origin_kinds.append('project_definitions')
            scopes.append('project')
        elif descriptor.name == _LIBRARY_DEFINITION_KNOWLEDGE:
            origin_kinds.append('library_definitions')
            scopes.append('library')
        elif descriptor.name == _PROJECT_REGISTRY_KNOWLEDGE:
            origin_kinds.append('project_registry')
            scopes.append('project')

        for operation in descriptor.operations:
            operations.append(
                CxpCapabilityOperationBinding(
                    operation_type_to_name(operation.operation_type),
                    result_type=operation.result_type,
                    freshness=operation.freshness,
                ),
            )
            if operation.freshness == 'fresh':
                supports_fresh_resolution = True
            if operation.freshness == 'knowledge_base':
                supports_kb_projection = True

    normalized_operations = tuple(
        dict.fromkeys(
            operation
            for operation in operations
            if operation.operation_name
            in {
                'definition.resolve',
                'knowledge.query_tests',
                'knowledge.query_definitions',
                'knowledge.query_registry_items',
            }
        ),
    )
    return CxpCapabilityDescriptor(
        name='definition_knowledge',
        level='supported',
        operations=normalized_operations,
        metadata={
            'knowledge_origin_kinds': list(dict.fromkeys(origin_kinds)),
            'knowledge_scopes': list(dict.fromkeys(scopes)),
            'supports_fresh_resolution': supports_fresh_resolution,
            'supports_knowledge_base_projection': supports_kb_projection,
        },
    )


def _find_descriptor(
    descriptors: Iterable[CapabilityDescriptor],
    name: str,
):
    for descriptor in descriptors:
        if descriptor.name == name:
            return descriptor
    return None


def _attribute_value(
    descriptor,
    name: str,
    default,
):
    for attribute in descriptor.attributes:
        if attribute.name == name:
            return attribute.value
    return default


def _cxp_operations(
    operations,
) -> tuple[CxpCapabilityOperationBinding, ...]:
    return tuple(
        CxpCapabilityOperationBinding(
            operation_type_to_name(operation.operation_type),
            result_type=operation.result_type,
            freshness=operation.freshness,
        )
        for operation in operations
    )


def _filtered_cxp_operations(
    operations,
    allowed_operation_names: set[str],
) -> tuple[CxpCapabilityOperationBinding, ...]:
    return tuple(
        operation
        for operation in _cxp_operations(operations)
        if operation.operation_name in allowed_operation_names
    )


def operation_type_to_name(operation_type: str) -> str:
    return operation_type


__all__ = (
    'build_cxp_engine_component_snapshot',
    'build_cxp_plugin_component_snapshot',
    'build_cxp_reporter_component_snapshot',
)
