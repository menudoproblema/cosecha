from __future__ import annotations

import pytest

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
