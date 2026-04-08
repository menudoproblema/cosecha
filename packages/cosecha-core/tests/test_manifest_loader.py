from __future__ import annotations

from pathlib import Path

import pytest

from cosecha.core.manifest_loader import (
    _parse_optional_symbol_ref,
    _parse_registry_loader_spec,
    _parse_resource_spec,
    _parse_structured_registry_layout_specs,
    _parse_symbol_mapping,
    _require_dict,
    _require_engine_path,
    _require_tuple_of_dict,
    _require_tuple_of_str,
    parse_cosecha_manifest_text,
)
from cosecha.core.manifest_symbols import ManifestValidationError, SymbolRef


def _resolve_engine_descriptor(_engine_type: str) -> type[object]:
    class _EngineDescriptor:
        @staticmethod
        def validate_resource_binding(
            *_args: object, **_kwargs: object,
        ) -> None:
            return None

    return _EngineDescriptor


def _parse_manifest(content: str) -> None:
    parse_cosecha_manifest_text(
        content,
        manifest_path=Path('/tmp/cosecha.toml'),
        schema_version=1,
        iter_hook_descriptors=lambda: (),
        resolve_engine_descriptor=_resolve_engine_descriptor,
    )


def test_parse_manifest_rejects_legacy_hooks_block() -> None:
    with pytest.raises(
        ManifestValidationError,
        match=r'Legacy \[\[hooks\]\]',
    ):
        _parse_manifest(
            """
[manifest]
schema_version = 1

[hooks]
noop = true
""",
        )


def test_parse_manifest_rejects_schema_mismatch() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Unsupported cosecha.toml schema version',
    ):
        _parse_manifest(
            """
[manifest]
schema_version = 2
""",
        )


def test_parse_manifest_rejects_legacy_engine_hook_ids() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Legacy engine hook_ids are no longer supported',
    ):
        _parse_manifest(
            """
[manifest]
schema_version = 1

[[engines]]
id = "pytest"
type = "pytest"
name = "Pytest"
path = "tests"
hook_ids = ["old"]
""",
        )


def test_parse_manifest_rejects_non_structured_registry_layouts() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Registry loader layouts must use structured layout tables',
    ):
        _parse_manifest(
            """
[manifest]
schema_version = 1

[[engines]]
id = "pytest"
type = "pytest"
name = "Pytest"
path = "tests"

[[engines.registry_loaders]]
[engines.registry_loaders.layouts]
models = "bad"
""",
        )


def test_parse_manifest_rejects_invalid_symbol_reference_type() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Expected symbol ref string',
    ):
        _parse_manifest(
            """
[manifest]
schema_version = 1

[[engines]]
id = "pytest"
type = "pytest"
name = "Pytest"
path = "tests"

[[resources]]
name = "cache"
provider = 123
""",
        )


def test_parse_manifest_rejects_bad_engine_path_type() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Missing or invalid string field',
    ):
        _parse_manifest(
            """
[manifest]
schema_version = 1

[[engines]]
id = "pytest"
type = "pytest"
name = "Pytest"
path = 123
""",
        )


def test_parse_symbol_mapping_ignores_non_string_values_and_keeps_rest() -> (
    None
):
    payload = {
        'valid': 'cosecha.core.config:load_config',
        'invalid': 12,
    }

    parsed = _parse_symbol_mapping(payload)

    assert parsed == (
        ('valid', SymbolRef('cosecha.core.config', 'load_config')),
    )


def test_parse_optional_symbol_ref_requires_string_or_none() -> None:
    assert _parse_optional_symbol_ref(None) is None

    with pytest.raises(
        ManifestValidationError, match='Expected symbol ref string',
    ):
        _parse_optional_symbol_ref(123)


def test_require_tuple_of_str_rejects_non_list_values() -> None:
    with pytest.raises(
        ManifestValidationError, match='Expected list of strings',
    ):
        _require_tuple_of_str('not-a-list')


def test_parse_structured_registry_layout_specs_rejects_non_dict() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Structured registry layouts require tables/dicts',
    ):
        _parse_structured_registry_layout_specs({'models': 'not-a-table'})


def test_parse_registry_loader_spec_requires_dict_layouts() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Expected table/dict, got',
    ):
        _parse_registry_loader_spec({'layouts': []})


def test_require_tuple_of_dict_rejects_non_tables() -> None:
    with pytest.raises(
        ManifestValidationError, match='Expected list of tables',
    ):
        _require_tuple_of_dict(['not', 12, True])


def test_require_dict_rejects_scalar_input() -> None:
    with pytest.raises(ManifestValidationError, match='Expected table/dict'):
        _require_dict('nope')


def test_parse_resource_spec_rejects_non_dict_config() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Expected table/dict',
    ):
        _parse_resource_spec({'name': 'db', 'config': 10})


def test_parse_engine_path_requires_non_none_string() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Missing or invalid string field',
    ):
        _require_engine_path({}, 'path')
