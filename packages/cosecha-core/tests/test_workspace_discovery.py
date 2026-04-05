from __future__ import annotations

from pathlib import Path

import pytest

from cosecha.core import discovery, reporting_ir
from cosecha.core.discovery import (
    get_definition_query_provider,
    get_engine_descriptor,
    get_hook_descriptor,
    iter_console_presenter_contributions,
    iter_plugin_types,
)
from cosecha.core.runtime_interop import validate_runtime_interface_name


DISCOVERY_RELOAD_MIN_CALLS = 8
CORE_MANIFEST_SIZE_LIMIT_BYTES = 40000
BROKEN_MODULE_NAME = 'boom'


def test_workspace_discovery_registers_engines_catalogs_and_hooks() -> None:
    assert get_engine_descriptor('gherkin') is not None
    assert get_engine_descriptor('pytest') is not None
    assert get_definition_query_provider('gherkin') is not None
    assert get_hook_descriptor('python') is not None
    assert get_hook_descriptor('mochuelo_runtime_service') is not None
    assert any(
        contribution.contribution_name == 'gherkin'
        for contribution in iter_console_presenter_contributions()
    )
    assert validate_runtime_interface_name('database/mongodb') is None


def test_workspace_discovery_registers_core_and_optional_plugins() -> None:
    plugin_names = {plugin.__name__ for plugin in iter_plugin_types()}

    assert 'TimingPlugin' in plugin_names


def test_runtime_interface_validation_uses_cxp_catalogs() -> None:
    assert validate_runtime_interface_name('database/mongodb') is None
    error = validate_runtime_interface_name('database/unknown')
    assert error is not None
    assert 'database/unknown' in error


def test_discovery_raises_when_an_entry_point_cannot_be_loaded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenEntryPoint:
        name = 'broken'

        def load(self):
            raise ModuleNotFoundError(BROKEN_MODULE_NAME)

    monkeypatch.setattr(
        discovery,
        'entry_points',
        lambda *, group: (BrokenEntryPoint(),)
        if group == discovery.ENGINE_ENTRYPOINT_GROUP
        else (),
    )
    discovery.clear_discovery_registry()

    try:
        with pytest.raises(
            discovery.DiscoveryLoadError,
            match="Failed to load discovery entry point 'broken'",
        ):
            discovery.iter_engine_descriptors()
    finally:
        discovery.clear_discovery_registry()


def test_discovery_registry_can_be_overridden_by_context() -> None:
    discovery.clear_discovery_registry()

    class LocalDescriptor:
        engine_type = 'local'

    registry = discovery.create_discovery_registry()
    registry.register_engine_descriptor(LocalDescriptor)

    assert discovery.get_engine_descriptor('local') is None
    with discovery.using_discovery_registry(registry):
        assert discovery.get_engine_descriptor('local') is LocalDescriptor
    assert discovery.get_engine_descriptor('local') is None


def test_loaded_discovery_registries_reuse_entry_point_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        discovery,
        'entry_points',
        lambda *, group: calls.append(group) or (),
    )
    discovery.clear_discovery_registry()
    try:
        discovery.create_loaded_discovery_registry()
        discovery.create_loaded_discovery_registry()

        assert calls.count(discovery.ENGINE_ENTRYPOINT_GROUP) == 1
        assert calls.count(discovery.PLUGIN_ENTRYPOINT_GROUP) == 1
    finally:
        discovery.clear_discovery_registry()


def test_clearing_discovery_registry_invalidates_entry_point_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call_count = 0

    def fake_entry_points(*, group):
        nonlocal call_count
        del group
        call_count += 1
        return ()

    monkeypatch.setattr(discovery, 'entry_points', fake_entry_points)
    discovery.clear_discovery_registry()
    try:
        discovery.create_loaded_discovery_registry()
        discovery.clear_discovery_registry()
        discovery.create_loaded_discovery_registry()

        assert call_count > DISCOVERY_RELOAD_MIN_CALLS
    finally:
        discovery.clear_discovery_registry()


def test_core_manifest_no_longer_contains_hardcoded_engine_or_hook_branches(
) -> None:
    manifest_path = Path(
        'packages/cosecha-core/src/cosecha/core/cosecha_manifest.py',
    )
    source = manifest_path.read_text(encoding='utf-8')

    assert "if engine_spec.type == 'gherkin'" not in source
    assert "if engine_spec.type == 'pytest'" not in source
    assert "if hook_type == 'mochuelo_runtime_service'" not in source


def test_core_reporting_ir_exposes_only_generic_test_report() -> None:
    assert hasattr(reporting_ir, 'TestReport')
    assert not hasattr(reporting_ir, 'GherkinTestReport')


def test_core_manifest_is_split_into_loader_and_validation_modules() -> None:
    manifest_path = Path(
        'packages/cosecha-core/src/cosecha/core/cosecha_manifest.py',
    )
    loader_path = Path(
        'packages/cosecha-core/src/cosecha/core/manifest_loader.py',
    )
    validation_path = Path(
        'packages/cosecha-core/src/cosecha/core/manifest_validation.py',
    )

    assert loader_path.exists()
    assert validation_path.exists()
    assert manifest_path.stat().st_size < CORE_MANIFEST_SIZE_LIMIT_BYTES
