from __future__ import annotations

from typing import TYPE_CHECKING

from cosecha.core.capabilities import CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS
from cxp import (
    CapabilityDescriptor as CxpCapabilityDescriptor,
    CapabilityOperationBinding as CxpCapabilityOperationBinding,
    ComponentCapabilitySnapshot as CxpComponentCapabilitySnapshot,
    ComponentIdentity,
)
from cxp.catalogs.interfaces.cosecha import (
    COSECHA_ENGINE_INTERFACE,
    COSECHA_INSTRUMENTATION_INTERFACE,
    COSECHA_PLUGIN_INTERFACE,
    COSECHA_REPORTER_INTERFACE,
    COSECHA_RUNTIME_INTERFACE,
)


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.capabilities import CapabilityDescriptor
    from cosecha.core.engines.base import Engine
    from cosecha.core.instrumentation import InstrumentationComponent
    from cosecha.core.plugins.base import Plugin
    from cosecha.core.reporter import Reporter
    from cosecha.core.runtime import RuntimeProvider


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
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='selection_labels',
                level=selection_labels.level,
                summary=selection_labels.summary,
                operations=_filtered_cxp_operations(
                    selection_labels.operations,
                    {
                        'run',
                        'plan.analyze',
                        'plan.explain',
                        'plan.simulate',
                    },
                ),
                metadata={
                    'label_sources': list(
                        _attribute_value(
                            selection_labels,
                            'label_sources',
                            (),
                        ),
                    ),
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

    project_knowledge = _find_descriptor(
        descriptors,
        _PROJECT_DEFINITION_KNOWLEDGE,
    )
    if project_knowledge is not None:
        cxp_descriptors.append(
            _build_definition_descriptor(
                descriptor=project_knowledge,
                name='project_definition_knowledge',
                origin_kind='project',
                default_scopes=('project',),
                allowed_operations={
                    'definition.resolve',
                    'knowledge.query_tests',
                    'knowledge.query_definitions',
                },
            ),
        )

    library_knowledge = _find_descriptor(
        descriptors,
        _LIBRARY_DEFINITION_KNOWLEDGE,
    )
    if library_knowledge is not None:
        cxp_descriptors.append(
            _build_definition_descriptor(
                descriptor=library_knowledge,
                name='library_definition_knowledge',
                origin_kind='library',
                default_scopes=('library',),
                allowed_operations={
                    'definition.resolve',
                    'knowledge.query_definitions',
                },
            ),
        )

    registry_knowledge = _find_descriptor(
        descriptors,
        _PROJECT_REGISTRY_KNOWLEDGE,
    )
    if registry_knowledge is not None:
        cxp_descriptors.append(
            _build_registry_descriptor(
                descriptor=registry_knowledge,
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
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='static_definition_discovery',
                level=static_discovery.level,
                summary=static_discovery.summary,
                operations=_cxp_operations(static_discovery.operations),
                metadata={
                    'discovery_backends': [
                        str(
                            _attribute_value(
                                static_discovery,
                                'discovery_backend',
                                'collector',
                            ),
                        ),
                    ],
                },
            ),
        )

    lazy_materialization = _find_descriptor(
        descriptors,
        _LAZY_PROJECT_DEFINITION_LOADING,
    )
    if lazy_materialization is not None:
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='on_demand_definition_materialization',
                level=lazy_materialization.level,
                summary=lazy_materialization.summary,
                operations=(
                    CxpCapabilityOperationBinding('definition.resolve'),
                ),
                metadata={
                    'materialization_granularities': [
                        str(
                            _attribute_value(
                                lazy_materialization,
                                'materialization_granularity',
                                'file',
                            ),
                        ),
                    ],
                },
            ),
        )

    if dependency_rules:
        cxp_descriptors.append(
            CxpCapabilityDescriptor(
                name='engine_dependency_knowledge',
                level='supported',
                summary='Cross-engine dependency rules published by the engine.',
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


def build_cxp_runtime_component_snapshot(
    runtime_provider: RuntimeProvider,
) -> CxpComponentCapabilitySnapshot:
    return CxpComponentCapabilitySnapshot(
        component_name=runtime_provider.runtime_name(),
        component_kind='runtime',
        identity=ComponentIdentity(
            interface=COSECHA_RUNTIME_INTERFACE,
            provider=runtime_provider.runtime_name(),
            version=str(runtime_provider.runtime_api_version()),
        ),
        capabilities=_passthrough_cxp_capabilities(
            runtime_provider.describe_capabilities(),
        ),
    )


def build_cxp_instrumentation_component_snapshot(
    instrumentation_type: type[InstrumentationComponent],
) -> CxpComponentCapabilitySnapshot:
    return CxpComponentCapabilitySnapshot(
        component_name=instrumentation_type.instrumentation_name(),
        component_kind='instrumentation',
        identity=ComponentIdentity(
            interface=COSECHA_INSTRUMENTATION_INTERFACE,
            provider=instrumentation_type.instrumentation_name(),
            version=str(instrumentation_type.instrumentation_api_version()),
        ),
        capabilities=_passthrough_cxp_capabilities(
            instrumentation_type.describe_capabilities(),
            excluded_names={CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS},
        ),
    )


def build_cxp_reporter_component_snapshot(
    reporter: Reporter,
) -> CxpComponentCapabilitySnapshot:
    descriptor_target = reporter.descriptor_target()
    return CxpComponentCapabilitySnapshot(
        component_name=descriptor_target.reporter_name(),
        component_kind='reporter',
        identity=ComponentIdentity(
            interface=COSECHA_REPORTER_INTERFACE,
            provider=descriptor_target.reporter_name(),
            version=str(descriptor_target.reporter_api_version()),
        ),
        capabilities=_passthrough_cxp_capabilities(
            descriptor_target.describe_capabilities(),
        ),
    )


def build_cxp_plugin_component_snapshot(
    plugin: Plugin,
) -> CxpComponentCapabilitySnapshot:
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

    for descriptor in plugin.describe_capabilities():
        if descriptor.name not in {'timing_summary', 'telemetry_export'}:
            continue
        capabilities.append(
            CxpCapabilityDescriptor(
                name=descriptor.name,
                level=descriptor.level,
                summary=descriptor.summary,
                operations=_cxp_operations(descriptor.operations),
                metadata=_attributes_metadata(descriptor),
            ),
        )

    return CxpComponentCapabilitySnapshot(
        component_name=plugin.plugin_name(),
        component_kind='plugin',
        identity=ComponentIdentity(
            interface=COSECHA_PLUGIN_INTERFACE,
            provider=plugin.plugin_name(),
            version=str(plugin.plugin_api_version()),
        ),
        capabilities=tuple(capabilities),
    )


def _build_definition_descriptor(
    *,
    descriptor: CapabilityDescriptor,
    name: str,
    origin_kind: str,
    default_scopes: tuple[str, ...],
    allowed_operations: set[str],
) -> CxpCapabilityDescriptor:
    operations = tuple(
        binding
        for binding in _cxp_operations(descriptor.operations)
        if binding.operation_name in allowed_operations
    )
    supports_fresh_resolution = any(
        operation.freshness == 'fresh'
        for operation in descriptor.operations
    )
    supports_knowledge_base_projection = any(
        operation.freshness == 'knowledge_base'
        for operation in descriptor.operations
    )
    return CxpCapabilityDescriptor(
        name=name,
        level=descriptor.level,
        summary=descriptor.summary,
        operations=operations,
        metadata={
            'knowledge_origin_kind': [origin_kind],
            'knowledge_scopes': list(
                _attribute_value(
                    descriptor,
                    'knowledge_scopes',
                    default_scopes,
                ),
            ),
            'supports_fresh_resolution': supports_fresh_resolution,
            'supports_knowledge_base_projection': (
                supports_knowledge_base_projection
            ),
        },
    )


def _build_registry_descriptor(
    *,
    descriptor: CapabilityDescriptor,
) -> CxpCapabilityDescriptor:
    operations = tuple(
        binding
        for binding in _cxp_operations(descriptor.operations)
        if binding.operation_name == 'knowledge.query_registry_items'
    )
    supports_knowledge_base_projection = any(
        operation.freshness == 'knowledge_base'
        for operation in descriptor.operations
    )
    return CxpCapabilityDescriptor(
        name='project_registry_knowledge',
        level=descriptor.level,
        summary=descriptor.summary,
        operations=operations,
        metadata={
            'registry_scopes': list(
                _attribute_value(
                    descriptor,
                    'registry_scopes',
                    ('project',),
                ),
            ),
            'supports_knowledge_base_projection': (
                supports_knowledge_base_projection
            ),
        },
    )


def _passthrough_cxp_capabilities(
    descriptors: tuple[CapabilityDescriptor, ...],
    *,
    excluded_names: set[str] | None = None,
) -> tuple[CxpCapabilityDescriptor, ...]:
    blocked = excluded_names or set()
    capabilities: list[CxpCapabilityDescriptor] = []
    for descriptor in descriptors:
        if descriptor.name in blocked:
            continue
        metadata = _attributes_metadata(descriptor)
        if descriptor.delivery_mode is not None:
            metadata.setdefault('delivery_mode', descriptor.delivery_mode)
        if descriptor.granularity is not None:
            metadata.setdefault('granularity', descriptor.granularity)
        capabilities.append(
            CxpCapabilityDescriptor(
                name=descriptor.name,
                level=descriptor.level,
                summary=descriptor.summary,
                operations=_cxp_operations(descriptor.operations),
                metadata=metadata,
            ),
        )
    return tuple(capabilities)


def _attributes_metadata(
    descriptor: CapabilityDescriptor,
) -> dict[str, object]:
    return {
        attribute.name: _normalize_attribute_value(attribute.value)
        for attribute in descriptor.attributes
    }


def _normalize_attribute_value(value: object) -> object:
    if isinstance(value, tuple):
        return [
            _normalize_attribute_value(item)
            for item in value
        ]
    return value


def _find_descriptor(
    descriptors: tuple[CapabilityDescriptor, ...],
    name: str,
) -> CapabilityDescriptor | None:
    for descriptor in descriptors:
        if descriptor.name == name:
            return descriptor
    return None


def _attribute_value(
    descriptor: CapabilityDescriptor,
    name: str,
    default: object,
) -> object:
    for attribute in descriptor.attributes:
        if attribute.name == name:
            return attribute.value
    return default


def _filtered_cxp_operations(
    operations,
    allowed_names: set[str],
) -> tuple[CxpCapabilityOperationBinding, ...]:
    return tuple(
        binding
        for binding in _cxp_operations(operations)
        if binding.operation_name in allowed_names
    )


def _cxp_operations(
    operations,
) -> tuple[CxpCapabilityOperationBinding, ...]:
    return tuple(
        dict.fromkeys(
            CxpCapabilityOperationBinding(
                operation.operation_type,
                result_type=operation.result_type,
                freshness=operation.freshness,
            )
            for operation in operations
        ),
    )


__all__ = (
    'build_cxp_engine_component_snapshot',
    'build_cxp_instrumentation_component_snapshot',
    'build_cxp_plugin_component_snapshot',
    'build_cxp_reporter_component_snapshot',
    'build_cxp_runtime_component_snapshot',
)
