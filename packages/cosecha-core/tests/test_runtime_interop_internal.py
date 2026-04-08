from __future__ import annotations

import pytest

import cosecha.core.runtime_interop as runtime_interop


class _Catalog:
    def __init__(self, *, abstract: bool) -> None:
        self.abstract = abstract

    def validate_capability_matrix(self, _matrix):
        msg = 'broken validation'
        raise ValueError(msg)


def test_iter_registered_catalog_interfaces_handles_non_dict_catalogs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _RegistryWithInvalidCatalogs:
        _catalogs = ()

    monkeypatch.setattr(
        runtime_interop,
        'DEFAULT_CATALOG_REGISTRY',
        _RegistryWithInvalidCatalogs(),
    )
    assert runtime_interop._iter_registered_catalog_interfaces() == ()


def test_resolve_runtime_validation_catalog_prefers_single_concrete_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    abstract_catalog = _Catalog(abstract=True)
    concrete_candidate = _Catalog(abstract=False)

    catalogs = {
        'execution/engine': abstract_catalog,
        'custom/one': concrete_candidate,
    }
    monkeypatch.setattr(runtime_interop, 'get_catalog', lambda name: catalogs.get(name))
    monkeypatch.setattr(
        runtime_interop,
        '_iter_registered_catalog_interfaces',
        lambda: ('custom/one',),
    )
    monkeypatch.setattr(
        runtime_interop,
        '_catalog_satisfies_interface',
        lambda offered, required: offered == 'custom/one' and required == 'execution/engine',
    )

    resolved = runtime_interop._resolve_runtime_validation_catalog('execution/engine')
    assert resolved is concrete_candidate


def test_resolve_runtime_validation_catalog_falls_back_to_abstract_when_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    abstract_catalog = _Catalog(abstract=True)
    candidate_a = _Catalog(abstract=False)
    candidate_b = _Catalog(abstract=False)

    catalogs = {
        'execution/engine': abstract_catalog,
        'custom/one': candidate_a,
        'custom/two': candidate_b,
    }
    monkeypatch.setattr(runtime_interop, 'get_catalog', lambda name: catalogs.get(name))
    monkeypatch.setattr(
        runtime_interop,
        '_iter_registered_catalog_interfaces',
        lambda: ('custom/one', 'custom/two'),
    )
    monkeypatch.setattr(runtime_interop, '_catalog_satisfies_interface', lambda _o, _r: True)

    resolved = runtime_interop._resolve_runtime_validation_catalog('execution/engine')
    assert resolved is abstract_catalog


def test_validate_runtime_capability_matrix_reraises_non_abstract_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime_interop,
        '_resolve_runtime_validation_catalog',
        lambda _interface: _Catalog(abstract=False),
    )

    with pytest.raises(ValueError, match='broken validation'):
        runtime_interop.validate_runtime_capability_matrix(
            'execution/engine',
            ('run',),
        )
