from __future__ import annotations

import importlib.util
import sys

from pathlib import Path

import pytest

from cosecha.core.capabilities import (
    CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
    CapabilityAttribute,
    CapabilityDescriptor,
)
from cosecha.core.shadow import (
    DuplicateCapabilityGrantError,
    EphemeralArtifactCapability,
    EphemeralCapabilityNotGrantedError,
    PersistentArtifactsNotEnabledError,
    ShadowCapabilityRegistryNotBoundError,
    ShadowComponentIdResolutionError,
    ShadowNotBoundError,
    acquire_shadow_handle,
    binding_shadow,
    build_ephemeral_artifact_capability,
    build_ephemeral_capability_registry,
    get_active_ephemeral_capabilities,
    get_active_shadow,
    use_detached_shadow,
)
from cosecha.core.shadow_execution import ShadowExecutionContext


def test_shadow_binding_exposes_active_shadow_and_registry(tmp_path) -> None:
    shadow = ShadowExecutionContext(root_path=tmp_path / 'shadow').materialize()
    capability = EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
    )

    with binding_shadow(
        shadow,
        ephemeral_capabilities={capability.component_id: capability},
    ):
        assert get_active_shadow() == shadow
        assert get_active_ephemeral_capabilities() == {
            capability.component_id: capability,
        }


def test_acquire_shadow_handle_requires_active_shadow_and_grant(
    tmp_path,
) -> None:
    with pytest.raises(ShadowNotBoundError):
        get_active_shadow()
    with pytest.raises(ShadowCapabilityRegistryNotBoundError):
        get_active_ephemeral_capabilities()

    shadow = ShadowExecutionContext(root_path=tmp_path / 'shadow').materialize()
    with binding_shadow(shadow, ephemeral_capabilities={}):
        with pytest.raises(EphemeralCapabilityNotGrantedError):
            acquire_shadow_handle('cosecha.instrumentation.coverage')


def test_shadow_handle_blocks_persistent_access_without_permission(
    tmp_path,
) -> None:
    shadow = ShadowExecutionContext(root_path=tmp_path / 'shadow').materialize()
    capability = EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
        produces_persistent=False,
    )

    with binding_shadow(
        shadow,
        ephemeral_capabilities={capability.component_id: capability},
    ):
        handle = acquire_shadow_handle(capability.component_id)
        assert handle.ephemeral_root == shadow.coverage_dir
        with pytest.raises(PersistentArtifactsNotEnabledError):
            handle.persistent_dir('artifacts')


def test_build_ephemeral_capability_registry_rejects_duplicates() -> None:
    capability = EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
    )

    with pytest.raises(DuplicateCapabilityGrantError):
        build_ephemeral_capability_registry((capability, capability))


def test_build_ephemeral_artifact_capability_validates_component_id() -> None:
    descriptors = (
        CapabilityDescriptor(
            name=CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
            level='supported',
            attributes=(
                CapabilityAttribute(
                    name='component_id',
                    value='cosecha.instrumentation.coverage',
                ),
                CapabilityAttribute(
                    name='ephemeral_domain',
                    value='instrumentation',
                ),
            ),
        ),
    )

    capability = build_ephemeral_artifact_capability(
        descriptors,
        declared_component_id='cosecha.instrumentation.coverage',
    )

    assert capability == EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
    )

    with pytest.raises(ValueError):
        build_ephemeral_artifact_capability(
            descriptors,
            declared_component_id='cosecha.instrumentation.other',
        )


def test_build_ephemeral_artifact_capability_accepts_custom_domain() -> None:
    descriptors = (
        CapabilityDescriptor(
            name=CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
            level='supported',
            attributes=(
                CapabilityAttribute(
                    name='component_id',
                    value='cosecha.engine.cache',
                ),
                CapabilityAttribute(
                    name='ephemeral_domain',
                    value='cache',
                ),
            ),
        ),
    )

    capability = build_ephemeral_artifact_capability(
        descriptors,
        declared_component_id='cosecha.engine.cache',
    )

    assert capability == EphemeralArtifactCapability(
        component_id='cosecha.engine.cache',
        ephemeral_domain='cache',
    )


def test_use_detached_shadow_and_shadow_sugar(tmp_path) -> None:
    module_path = tmp_path / 'shadow_user_ok.py'
    module_path.write_text(
        '\n'.join(
            (
                "COSECHA_COMPONENT_ID = 'cosecha.instrumentation.coverage'",
                'from cosecha.core.shadow import ephemeral_file',
                '',
                'def build_path():',
                "    return ephemeral_file('coverage.data')",
            ),
        ),
        encoding='utf-8',
    )
    spec = importlib.util.spec_from_file_location(
        'shadow_user_ok',
        module_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules['shadow_user_ok'] = module
    spec.loader.exec_module(module)

    capability = EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
    )
    with use_detached_shadow(granted_capabilities=(capability,)) as shadow:
        resolved = module.build_path()

        assert resolved == (
            shadow.instrumentation_component_dir(
                'cosecha.instrumentation.coverage',
            )
            / 'coverage.data'
        )


def test_use_detached_shadow_routes_persistent_paths_inside_temp_root() -> None:
    capability = EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
        produces_persistent=True,
    )

    with use_detached_shadow(granted_capabilities=(capability,)) as shadow:
        handle = acquire_shadow_handle(capability.component_id)
        persistent_path = handle.persistent_file('coverage.json')

    assert persistent_path.resolve() == (
        shadow.knowledge_storage_root
        / 'components'
        / 'cosecha.instrumentation.coverage'
        / 'coverage.json'
    ).resolve()


def test_shadow_handle_routes_custom_domain_to_shadow_root(tmp_path) -> None:
    shadow = ShadowExecutionContext(root_path=tmp_path / 'shadow').materialize()
    capability = EphemeralArtifactCapability(
        component_id='cosecha.engine.cache',
        ephemeral_domain='cache',
    )

    with binding_shadow(
        shadow,
        ephemeral_capabilities={capability.component_id: capability},
    ):
        handle = acquire_shadow_handle(capability.component_id)

    assert handle.ephemeral_root == (
        shadow.root_path / 'cache' / 'cosecha.engine.cache'
    )


def test_shadow_sugar_resolves_component_id_from_parent_package(tmp_path) -> None:
    package_dir = tmp_path / 'shadow_pkg'
    package_dir.mkdir()
    (package_dir / '__init__.py').write_text(
        "COSECHA_COMPONENT_ID = 'cosecha.instrumentation.coverage'\n",
        encoding='utf-8',
    )
    (package_dir / 'utils.py').write_text(
        '\n'.join(
            (
                'from cosecha.core.shadow import ephemeral_file',
                '',
                'def build_path():',
                "    return ephemeral_file('coverage.data')",
            ),
        ),
        encoding='utf-8',
    )
    sys.path.insert(0, str(tmp_path))
    try:
        __import__('shadow_pkg')
        utils = __import__('shadow_pkg.utils', fromlist=['build_path'])
        capability = EphemeralArtifactCapability(
            component_id='cosecha.instrumentation.coverage',
            ephemeral_domain='instrumentation',
        )
        with use_detached_shadow(granted_capabilities=(capability,)) as shadow:
            assert utils.build_path() == (
                shadow.coverage_dir / 'coverage.data'
            )
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop('shadow_pkg', None)
        sys.modules.pop('shadow_pkg.utils', None)


def test_shadow_cleanup_moves_preserved_namespaces_on_success(tmp_path) -> None:
    shadow = ShadowExecutionContext(
        root_path=tmp_path / 'shadow' / 'session-1',
        knowledge_storage_root=tmp_path / '.cosecha',
    ).materialize()
    capability = EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
        cleanup_on_success=False,
    )
    namespace = shadow.coverage_dir
    namespace.mkdir(parents=True, exist_ok=True)
    (namespace / 'coverage.data').write_text('data', encoding='utf-8')

    shadow.cleanup(
        preserve=False,
        session_succeeded=True,
        capabilities={capability.component_id: capability},
    )

    preserved = (
        tmp_path
        / '.cosecha'
        / 'preserved_artifacts'
        / 'session-1'
        / 'cosecha.instrumentation.coverage'
        / 'coverage.data'
    )
    assert preserved.exists()
    assert shadow.root_path.exists() is False


def test_shadow_cleanup_drops_non_preserved_namespaces_on_failure(tmp_path) -> None:
    shadow = ShadowExecutionContext(
        root_path=tmp_path / 'shadow' / 'session-1',
        knowledge_storage_root=tmp_path / '.cosecha',
    ).materialize()
    removable = EphemeralArtifactCapability(
        component_id='cosecha.provider.ssl',
        ephemeral_domain='runtime',
        preserve_on_failure=False,
    )
    kept = EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
        preserve_on_failure=True,
    )
    removable_dir = shadow.runtime_component_dir(removable.component_id)
    removable_dir.mkdir(parents=True, exist_ok=True)
    (removable_dir / 'cert.pem').write_text('cert', encoding='utf-8')
    kept_dir = shadow.coverage_dir
    (kept_dir / 'coverage.data').write_text('data', encoding='utf-8')

    shadow.cleanup(
        preserve=True,
        session_succeeded=False,
        capabilities={
            removable.component_id: removable,
            kept.component_id: kept,
        },
    )

    assert removable_dir.exists() is False
    assert (kept_dir / 'coverage.data').exists()


def test_shadow_cleanup_supports_custom_ephemeral_domains(tmp_path) -> None:
    shadow = ShadowExecutionContext(
        root_path=tmp_path / 'shadow' / 'session-1',
        knowledge_storage_root=tmp_path / '.cosecha',
    ).materialize()
    capability = EphemeralArtifactCapability(
        component_id='cosecha.engine.cache',
        ephemeral_domain='cache',
        preserve_on_failure=False,
    )
    namespace = shadow.component_ephemeral_dir(
        capability.component_id,
        capability.ephemeral_domain,
    )
    namespace.mkdir(parents=True, exist_ok=True)
    (namespace / 'cache.bin').write_text('cache', encoding='utf-8')

    shadow.cleanup(
        preserve=True,
        session_succeeded=False,
        capabilities={capability.component_id: capability},
    )

    assert namespace.exists() is False


def test_shadow_sugar_requires_component_id(tmp_path) -> None:
    module_path = tmp_path / 'shadow_user_invalid.py'
    module_path.write_text(
        '\n'.join(
            (
                'from cosecha.core.shadow import ephemeral_file',
                '',
                'def build_path():',
                "    return ephemeral_file('coverage.data')",
            ),
        ),
        encoding='utf-8',
    )
    spec = importlib.util.spec_from_file_location(
        'shadow_user_invalid',
        module_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules['shadow_user_invalid'] = module
    spec.loader.exec_module(module)

    capability = EphemeralArtifactCapability(
        component_id='cosecha.instrumentation.coverage',
        ephemeral_domain='instrumentation',
    )
    with use_detached_shadow(granted_capabilities=(capability,)):
        with pytest.raises(ShadowComponentIdResolutionError):
            module.build_path()
