from __future__ import annotations

from pathlib import Path

from cosecha.core.cosecha_manifest import ManifestValidationError
from cosecha.core.discovery import register_engine_descriptor
from cosecha.engine.pytest.engine import PytestEngine


class PytestEngineDescriptor:
    engine_type = 'pytest'

    @classmethod
    def validate_resource_binding(
        cls,
        binding,
        *,
        manifest,
    ) -> None:
        del manifest
        if binding.fixture_name is None:
            msg = (
                'Pytest resource bindings require fixture_name for '
                f'{binding.resource_name!r}'
            )
            raise ManifestValidationError(msg)

    @classmethod
    def materialize(
        cls,
        engine_spec,
        *,
        manifest,
        config,
        active_profiles,
        shared_requirements,
    ):
        del active_profiles
        resource_bindings = tuple(
            binding
            for binding in manifest.resource_bindings
            if binding.engine_type == cls.engine_type
        )
        definition_paths = tuple(
            (
                config.root_path / definition_path
                if not Path(definition_path).is_absolute()
                else Path(definition_path)
            ).resolve()
            for definition_path in engine_spec.definition_paths
        )
        return PytestEngine(
            name=engine_spec.name,
            hooks=(),
            shared_resource_requirements=shared_requirements,
            resource_bindings=resource_bindings,
            definition_paths=definition_paths,
        )


register_engine_descriptor(PytestEngineDescriptor)
