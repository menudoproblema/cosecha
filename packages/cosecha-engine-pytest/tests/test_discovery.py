from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.cosecha_manifest import ManifestValidationError
from cosecha.core.manifest_types import EngineSpec, ResourceBindingSpec
from cosecha.engine.pytest.discovery import PytestEngineDescriptor
from cosecha.engine.pytest import PytestEngine


def test_validate_resource_binding_requires_fixture_name() -> None:
    binding = SimpleNamespace(fixture_name=None, resource_name='workspace')

    with pytest.raises(
        ManifestValidationError,
        match="Pytest resource bindings require fixture_name for 'workspace'",
    ):
        PytestEngineDescriptor.validate_resource_binding(binding, manifest=None)


def test_validate_resource_binding_accepts_fixture_name() -> None:
    binding = SimpleNamespace(fixture_name='cosecha_workspace', resource_name='x')

    assert (
        PytestEngineDescriptor.validate_resource_binding(
            binding,
            manifest=None,
        )
        is None
    )


def test_materialize_filters_pytest_bindings_and_resolves_definition_paths(
    tmp_path,
) -> None:
    relative_definition_path = 'fixture-defs'
    absolute_definition_path = tmp_path / 'absolute-defs'
    absolute_definition_path.mkdir()

    config = SimpleNamespace(root_path=tmp_path)
    engine_spec = EngineSpec(
        id='pytest',
        type='pytest',
        name='pytest',
        path='tests',
        definition_paths=(
            relative_definition_path,
            str(absolute_definition_path),
        ),
    )
    manifest = SimpleNamespace(
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='db',
                fixture_name='cosecha_db',
            ),
            ResourceBindingSpec(
                engine_type='other',
                resource_name='cache',
            ),
        ),
    )

    engine = PytestEngineDescriptor.materialize(
        engine_spec,
        manifest=manifest,
        config=config,
        active_profiles=(),
        shared_requirements=(),
    )

    assert isinstance(engine, PytestEngine)
    assert engine.resource_bindings == (
        manifest.resource_bindings[0],
    )
    assert engine.definition_path_overrides == (
        (tmp_path / relative_definition_path).resolve(),
        absolute_definition_path.resolve(),
    )
