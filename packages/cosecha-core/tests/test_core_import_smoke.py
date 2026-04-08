from __future__ import annotations

import importlib
import runpy

import pytest


@pytest.mark.parametrize(
    ('module_name',),
    (
        ('cosecha.core.collector',),
        ('cosecha.core.config',),
        ('cosecha.core.console',),
        ('cosecha.core.capabilities',),
        ('cosecha.core.cosecha_manifest',),
        ('cosecha.core.definition_knowledge',),
        ('cosecha.core.diagnostics',),
        ('cosecha.core.discovery',),
        ('cosecha.core.domain_event_stream',),
        ('cosecha.core.domain_events',),
        ('cosecha.core.engine_dependencies',),
        ('cosecha.core.engines.base',),
        ('cosecha.core.event_bus',),
        ('cosecha.core.execution_ir',),
        ('cosecha.core.extensions',),
        ('cosecha.core.hooks',),
        ('cosecha.core.items',),
        ('cosecha.core.knowledge_base',),
        ('cosecha.core.knowledge_test_descriptor',),
        ('cosecha.core.location',),
        ('cosecha.core.manifest_loader',),
        ('cosecha.core.manifest_materialization',),
        ('cosecha.core.manifest_selection',),
        ('cosecha.core.manifest_symbols',),
        ('cosecha.core.manifest_types',),
        ('cosecha.core.manifest_validation',),
        ('cosecha.core.module_loading',),
        ('cosecha.core.operations',),
        ('cosecha.core.plugins.base',),
        ('cosecha.core.plugins.telemetry',),
        ('cosecha.core.plugins.timing',),
        ('cosecha.core.registry_knowledge',),
        ('cosecha.core.reporter',),
        ('cosecha.core.reporting_coordinator',),
        ('cosecha.core.reporting_ir',),
        ('cosecha.core.resources',),
        ('cosecha.core.runner',),
        ('cosecha.core.runtime',),
        ('cosecha.core.runtime_interop',),
        ('cosecha.core.runtime_profiles',),
        ('cosecha.core.runtime_protocol',),
        ('cosecha.core.runtime_worker',),
        ('cosecha.core.scheduler',),
        ('cosecha.core.serialization',),
        ('cosecha.core.session_artifacts',),
        ('cosecha.core.session_timing',),
        ('cosecha.core.shadow',),
        ('cosecha.core.shadow_execution',),
        ('cosecha.core.telemetry',),
        ('cosecha.core.utils',),
    ),
)
def test_core_modules_are_importable_and_reloadable(module_name: str) -> None:
    module = importlib.import_module(module_name)
    spec = module.__spec__
    assert spec is not None
    origin = spec.origin
    assert origin is not None
    namespace = runpy.run_path(origin, run_name=f'__cosecha_smoke__:{module_name}')

    assert namespace
