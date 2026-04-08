from __future__ import annotations

import importlib.util
import sys
import types

from pathlib import Path

import pytest

from cosecha.core.capabilities import CapabilityDescriptor
from cosecha.core.capabilities import (
    CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
    CapabilityAttribute,
)
from cosecha.core.shadow import (
    EphemeralArtifactCapability,
    PersistentArtifactsNotEnabledError,
    ShadowComponentIdResolutionError,
    acquire_shadow_handle,
    binding_shadow,
    build_ephemeral_artifact_capability,
    component_id_from_component_type,
    use_detached_shadow,
)
from cosecha.core.shadow_execution import ShadowExecutionContext
import cosecha.core.shadow as shadow_module


def test_shadow_handle_directory_helpers_cover_ephemeral_and_persistent_paths(
    tmp_path: Path,
) -> None:
    shadow = ShadowExecutionContext(
        root_path=tmp_path / 'shadow',
        knowledge_storage_root=tmp_path / '.cosecha',
    ).materialize()
    persistent_capability = EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
        produces_persistent=True,
    )
    ephemeral_only_capability = EphemeralArtifactCapability(
        component_id='cosecha.provider.ssl',
        ephemeral_domain='runtime',
    )
    with binding_shadow(
        shadow,
        ephemeral_capabilities={
            persistent_capability.component_id: persistent_capability,
            ephemeral_only_capability.component_id: ephemeral_only_capability,
        },
    ):
        persistent_handle = acquire_shadow_handle(persistent_capability.component_id)
        assert persistent_handle.ephemeral_dir('raw').exists()
        assert persistent_handle.persistent_dir('artifacts').exists()
        ephemeral_only_handle = acquire_shadow_handle(
            ephemeral_only_capability.component_id,
        )
        with pytest.raises(PersistentArtifactsNotEnabledError):
            ephemeral_only_handle.persistent_file('result.json')


def test_shadow_module_sugar_helpers_cover_wrapper_calls_and_cache(
    tmp_path: Path,
) -> None:
    module_path = tmp_path / 'shadow_sugar_helpers.py'
    module_path.write_text(
        '\n'.join(
            (
                "COSECHA_COMPONENT_ID = 'cosecha.provider.ssl'",
                (
                    'from cosecha.core.shadow import '
                    'ephemeral_dir, persistent_dir, persistent_file'
                ),
                '',
                'def build_paths():',
                "    return (ephemeral_dir('scratch'), persistent_dir('out'), "
                "persistent_file('result.txt'))",
            ),
        ),
        encoding='utf-8',
    )
    spec = importlib.util.spec_from_file_location('shadow_sugar_helpers', module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules['shadow_sugar_helpers'] = module
    spec.loader.exec_module(module)

    capability = EphemeralArtifactCapability(
        component_id='cosecha.provider.ssl',
        ephemeral_domain='runtime',
        produces_persistent=True,
    )
    with use_detached_shadow(granted_capabilities=(capability,)) as shadow:
        first = module.build_paths()
        second = module.build_paths()

    assert first[0] == second[0]
    assert first[0] == shadow.runtime_component_dir('cosecha.provider.ssl') / 'scratch'
    assert first[1] == (
        shadow.persistent_component_dir('cosecha.provider.ssl') / 'out'
    )
    assert first[2] == (
        shadow.persistent_component_dir('cosecha.provider.ssl') / 'result.txt'
    )


def test_build_ephemeral_artifact_capability_handles_missing_descriptor_and_invalid_domain() -> (
    None
):
    assert build_ephemeral_artifact_capability(
        (CapabilityDescriptor(name='other', level='supported'),),
        declared_component_id='cosecha.provider.ssl',
    ) is None

    descriptors = (
        CapabilityDescriptor(
            name=CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
            level='supported',
            attributes=(
                CapabilityAttribute(
                    name='component_id',
                    value='cosecha.provider.ssl',
                ),
                CapabilityAttribute(
                    name='ephemeral_domain',
                    value='INVALID/DOMAIN',
                ),
            ),
        ),
    )
    with pytest.raises(ValueError, match='safe path segment'):
        build_ephemeral_artifact_capability(
            descriptors,
            declared_component_id='cosecha.provider.ssl',
        )


def test_component_id_resolution_error_when_frame_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(shadow_module.inspect, 'currentframe', lambda: None)
    with pytest.raises(ShadowComponentIdResolutionError, match='caller module'):
        shadow_module._resolve_caller_component_id()


def test_component_id_from_component_type_reads_module_level_component_id() -> None:
    module = types.ModuleType('shadow_component_module')
    module.COSECHA_COMPONENT_ID = 'cosecha.module.component'
    component_type = type('ComponentType', (), {})
    component_type.__module__ = module.__name__
    sys.modules[module.__name__] = module
    try:
        assert (
            component_id_from_component_type(component_type)
            == 'cosecha.module.component'
        )
    finally:
        sys.modules.pop(module.__name__, None)
