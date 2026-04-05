from __future__ import annotations

from cosecha.core.runtime_interop import (
    build_runtime_canonical_binding_name,
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
