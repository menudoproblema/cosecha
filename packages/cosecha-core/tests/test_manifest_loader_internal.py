from __future__ import annotations

import pytest

from cosecha.core.manifest_loader import (
    _parse_structured_registry_layout_specs,
    _parse_workspace_declaration,
    _require_optional_float,
    _require_optional_str,
    _require_str,
    _require_tuple_of_dict,
    _require_tuple_of_str,
)
from cosecha.core.manifest_symbols import ManifestValidationError


def test_parse_workspace_declaration_with_locations() -> None:
    workspace = _parse_workspace_declaration(
        {
            'root': 'workspace',
            'knowledge_anchor': 'tests',
            'locations': [
                {
                    'path': 'src/pkg',
                    'role': 'source',
                    'importable': False,
                },
            ],
        },
    )

    assert workspace.root == 'workspace'
    assert workspace.knowledge_anchor == 'tests'
    assert workspace.locations[0].path == 'src/pkg'
    assert workspace.locations[0].importable is False


def test_parse_structured_registry_layout_specs_rejects_empty_layout_name() -> None:
    with pytest.raises(ManifestValidationError, match='non-empty layout name'):
        _parse_structured_registry_layout_specs(
            {
                '': {
                    'base': 'builtins:object',
                    'module_globs': ['demo.models'],
                },
            },
        )


def test_manifest_loader_require_helpers_cover_error_branches() -> None:
    with pytest.raises(ManifestValidationError, match='Missing or invalid string field'):
        _require_str({'name': ''}, 'name')

    with pytest.raises(ManifestValidationError, match='Expected optional string'):
        _require_optional_str('')

    with pytest.raises(ManifestValidationError, match='Expected non-empty strings'):
        _require_tuple_of_str(['valid', ''])

    with pytest.raises(ManifestValidationError, match='Expected optional number'):
        _require_optional_float(True)

    with pytest.raises(ManifestValidationError, match='Expected list of tables'):
        _require_tuple_of_dict({'table': 'not-a-list'})
