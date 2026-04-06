from __future__ import annotations

from cosecha.core.discovery import (
    create_loaded_discovery_registry,
    get_hook_descriptor,
    using_discovery_registry,
)
from cosecha.engine.pytest.items import _register_builtin_manifest_descriptors


def test_internal_fast_path_registry_seeds_builtin_manifest_hooks() -> None:
    registry = create_loaded_discovery_registry()

    with using_discovery_registry(registry):
        assert get_hook_descriptor('python') is None

        _register_builtin_manifest_descriptors()

        assert get_hook_descriptor('python') is not None
