from __future__ import annotations

import pytest

import cosecha.core.runtime_interop as runtime_interop

from cosecha.core.runtime_interop import (
    build_runtime_canonical_binding_name,
    build_runtime_capability_validation_messages,
    validate_runtime_capability_matrix,
    validate_runtime_interface_name,
)


def test_validate_runtime_interface_name_rejects_unknown_reserved_name() -> (
    None
):
    assert validate_runtime_interface_name('database/mongodb') is None
    assert validate_runtime_interface_name('core/system') is None
    assert validate_runtime_interface_name('database/mongo') == (
        "Unknown runtime interface 'database/mongo' for reserved "
        'interoperable namespace'
    )


def test_runtime_binding_name_uses_declared_interface_name() -> None:
    assert build_runtime_canonical_binding_name('application/http') == (
        'runtime__application_http'
    )
    assert build_runtime_canonical_binding_name('database/mongodb') == (
        'runtime__database_mongodb'
    )


def test_validate_runtime_capability_matrix_uses_catalogs() -> None:
    validation = validate_runtime_capability_matrix(
        'database/mongodb',
        ('read', 'transactioons'),
    )

    assert validation is not None
    assert validation.is_valid() is False
    assert validation.unknown_capabilities == ('transactioons',)


def test_validate_runtime_capability_matrix_supports_execution_engine_family(
) -> None:
    validation = validate_runtime_capability_matrix(
        'execution/engine',
        ('run',),
    )

    assert validation is not None
    assert validation.is_valid() is True


def test_runtime_capability_validation_messages_do_not_raise_for_family_alias(
) -> None:
    assert build_runtime_capability_validation_messages(
        'execution/engine',
        (),
    ) == ()


def test_validate_runtime_capability_matrix_rejects_legacy_plan_run_capability(
) -> None:
    if validate_runtime_interface_name('execution/plan-run') is not None:
        pytest.skip('Installed cxp does not expose execution/plan-run yet')

    validation = validate_runtime_capability_matrix(
        'execution/plan-run',
        ('draft_validation',),
    )

    assert validation is not None
    assert validation.is_valid() is False
    assert validation.unknown_capabilities == ('draft_validation',)


def test_runtime_interop_registered_interfaces_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_interop, 'DEFAULT_CATALOG_REGISTRY', None)
    assert runtime_interop._iter_registered_catalog_interfaces() == ()

    class _RegistryWithNames:
        def interface_names(self):
            return ['a/x', 'b/y']

    monkeypatch.setattr(
        runtime_interop,
        'DEFAULT_CATALOG_REGISTRY',
        _RegistryWithNames(),
    )
    assert runtime_interop._iter_registered_catalog_interfaces() == (
        'a/x',
        'b/y',
    )

    class _RegistryWithCatalogs:
        _catalogs = {'z/one': object(), 'a/two': object()}

    monkeypatch.setattr(
        runtime_interop,
        'DEFAULT_CATALOG_REGISTRY',
        _RegistryWithCatalogs(),
    )
    assert runtime_interop._iter_registered_catalog_interfaces() == (
        'a/two',
        'z/one',
    )


def test_runtime_interop_catalog_satisfies_runtime_interface_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_interop, '_catalog_satisfies_interface', None)

    assert runtime_interop._catalog_satisfies_runtime_interface(
        'a/b',
        'a/b',
    ) is True
    assert runtime_interop._catalog_satisfies_runtime_interface(
        'a/b',
        'x/y',
    ) is False


def test_runtime_interop_validate_capability_matrix_handles_missing_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_interop, 'get_catalog', lambda _name: None)

    assert validate_runtime_capability_matrix('database/unknown', ('read',)) is None


def test_runtime_interop_reports_abstract_catalog_validation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _AbstractCatalog:
        abstract = True

        def validate_capability_matrix(self, _matrix):
            msg = 'abstract cannot validate'
            raise ValueError(msg)

    monkeypatch.setattr(
        runtime_interop,
        '_resolve_runtime_validation_catalog',
        lambda _interface: _AbstractCatalog(),
    )

    messages = build_runtime_capability_validation_messages(
        'execution/engine',
        ('run',),
    )

    assert messages == (
        "Runtime interface 'execution/engine' resolves to an abstract catalog "
        'and cannot validate capability surfaces',
    )


def test_runtime_interop_resolve_validation_catalog_prefers_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    abstract_engine = type('Catalog', (), {'abstract': True})()
    concrete_alias = type(
        'Catalog',
        (),
        {
            'abstract': False,
            'validate_capability_matrix': lambda self, _matrix: object(),
        },
    )()

    catalogs = {
        'execution/engine': abstract_engine,
        'execution/plan-run': concrete_alias,
    }
    monkeypatch.setattr(runtime_interop, 'get_catalog', lambda name: catalogs.get(name))
    monkeypatch.setattr(
        runtime_interop,
        '_catalog_satisfies_interface',
        lambda offered, required: offered == 'execution/plan-run'
        and required == 'execution/engine',
    )

    resolved = runtime_interop._resolve_runtime_validation_catalog(
        'execution/engine',
    )
    assert resolved is concrete_alias
