from __future__ import annotations

import pytest

from cosecha.core.manifest_symbols import ManifestValidationError, SymbolRef


def test_symbol_ref_parse_and_dict_roundtrip() -> None:
    ref = SymbolRef(module='builtins', qualname='str')
    payload = ref.to_dict()
    restored = SymbolRef.from_dict(payload)

    assert restored == ref


def test_symbol_ref_parse_rejects_invalid_references() -> None:
    with pytest.raises(ManifestValidationError, match='Invalid symbol reference'):
        SymbolRef.parse('missing-separator')
