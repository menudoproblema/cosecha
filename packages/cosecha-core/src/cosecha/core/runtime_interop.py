from __future__ import annotations

import re

from typing import TYPE_CHECKING

from cxp.capabilities import Capability, CapabilityMatrix
from cxp.catalogs.base import get_catalog
from cxp.catalogs.interfaces.application import (  # noqa: F401
    HTTP_APPLICATION_CATALOG,
)
from cxp.catalogs.interfaces.database import MONGODB_CATALOG  # noqa: F401
from cxp.catalogs.interfaces.execution import (  # noqa: F401
    EXECUTION_ENGINE_CATALOG,
)
from cxp.catalogs.interfaces.transport import (
    HTTP_TRANSPORT_CATALOG,  # noqa: F401
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable


RUNTIME_INTERFACE_PREFIXES = (
    'application/',
    'database/',
    'execution/',
    'transport/',
)


def build_runtime_canonical_binding_name(interface_name: str) -> str:
    sanitized_name = re.sub(r'[^0-9A-Za-z]+', '_', interface_name).strip('_')
    return f'runtime__{sanitized_name}'


def requires_runtime_catalog(interface_name: str) -> bool:
    return interface_name.startswith(RUNTIME_INTERFACE_PREFIXES)


def validate_runtime_interface_name(interface_name: str) -> str | None:
    if not requires_runtime_catalog(interface_name):
        return None
    if get_catalog(interface_name) is not None:
        return None
    return (
        f'Unknown runtime interface {interface_name!r} for reserved '
        'interoperable namespace'
    )


def validate_runtime_capability_matrix(
    interface_name: str,
    capability_names: Iterable[str],
):
    catalog = get_catalog(interface_name)
    if catalog is None:
        return None

    matrix = CapabilityMatrix(
        capabilities=tuple(
            Capability(name=capability_name)
            for capability_name in capability_names
        ),
    )
    return catalog.validate_capability_matrix(matrix)


def build_runtime_capability_validation_messages(
    interface_name: str,
    capability_names: Iterable[str],
) -> tuple[str, ...]:
    validation = validate_runtime_capability_matrix(
        interface_name,
        capability_names,
    )
    if validation is None or validation.is_valid():
        return ()
    return tuple(validation.messages())


__all__ = (
    'RUNTIME_INTERFACE_PREFIXES',
    'build_runtime_canonical_binding_name',
    'build_runtime_capability_validation_messages',
    'requires_runtime_catalog',
    'validate_runtime_capability_matrix',
    'validate_runtime_interface_name',
)
