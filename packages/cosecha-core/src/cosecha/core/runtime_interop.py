from __future__ import annotations

import re

from typing import TYPE_CHECKING

from cxp.capabilities import Capability, CapabilityMatrix
from cxp.catalogs import base as cxp_catalogs_base
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
RUNTIME_CONCRETE_INTERFACE_ALIASES = {
    'execution/engine': 'execution/plan-run',
}
DEFAULT_CATALOG_REGISTRY = getattr(
    cxp_catalogs_base,
    'DEFAULT_CATALOG_REGISTRY',
    None,
)
get_catalog = cxp_catalogs_base.get_catalog
_catalog_satisfies_interface = getattr(
    cxp_catalogs_base,
    'catalog_satisfies_interface',
    None,
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


def _iter_registered_catalog_interfaces() -> tuple[str, ...]:
    if DEFAULT_CATALOG_REGISTRY is None:
        return ()

    interface_names = getattr(
        DEFAULT_CATALOG_REGISTRY,
        'interface_names',
        None,
    )
    if callable(interface_names):
        return tuple(interface_names())

    catalogs = getattr(DEFAULT_CATALOG_REGISTRY, '_catalogs', None)
    if isinstance(catalogs, dict):
        return tuple(sorted(catalogs))
    return ()


def _catalog_satisfies_runtime_interface(
    offered_interface_name: str,
    required_interface_name: str,
) -> bool:
    if _catalog_satisfies_interface is None:
        return offered_interface_name == required_interface_name
    return _catalog_satisfies_interface(
        offered_interface_name,
        required_interface_name,
    )


def _resolve_runtime_validation_catalog(interface_name: str):
    catalog = get_catalog(interface_name)
    if catalog is None or not getattr(catalog, 'abstract', False):
        return catalog

    concrete_interface_name = RUNTIME_CONCRETE_INTERFACE_ALIASES.get(
        interface_name,
    )
    if concrete_interface_name is not None:
        concrete_catalog = get_catalog(concrete_interface_name)
        if (
            concrete_catalog is not None
            and not getattr(concrete_catalog, 'abstract', False)
            and _catalog_satisfies_runtime_interface(
                concrete_interface_name,
                interface_name,
            )
        ):
            return concrete_catalog

    concrete_catalogs = tuple(
        candidate_catalog
        for candidate_interface_name in _iter_registered_catalog_interfaces()
        if candidate_interface_name != interface_name
        for candidate_catalog in (get_catalog(candidate_interface_name),)
        if candidate_catalog is not None
        and not getattr(candidate_catalog, 'abstract', False)
        and _catalog_satisfies_runtime_interface(
            candidate_interface_name,
            interface_name,
        )
    )
    if len(concrete_catalogs) == 1:
        return concrete_catalogs[0]
    return catalog


def validate_runtime_capability_matrix(
    interface_name: str,
    capability_names: Iterable[str],
):
    catalog = _resolve_runtime_validation_catalog(interface_name)
    if catalog is None:
        return None

    matrix = CapabilityMatrix(
        capabilities=tuple(
            Capability(name=capability_name)
            for capability_name in capability_names
        ),
    )
    try:
        return catalog.validate_capability_matrix(matrix)
    except ValueError as error:
        if getattr(catalog, 'abstract', False):
            msg = (
                f'Runtime interface {interface_name!r} resolves to an '
                'abstract '
                'catalog and cannot validate capability surfaces'
            )
            raise ValueError(msg) from error
        raise


def build_runtime_capability_validation_messages(
    interface_name: str,
    capability_names: Iterable[str],
) -> tuple[str, ...]:
    try:
        validation = validate_runtime_capability_matrix(
            interface_name,
            capability_names,
        )
    except ValueError as error:
        return (str(error),)
    if validation is None or validation.is_valid():
        return ()
    return tuple(validation.messages())


__all__ = (
    'RUNTIME_CONCRETE_INTERFACE_ALIASES',
    'RUNTIME_INTERFACE_PREFIXES',
    'build_runtime_canonical_binding_name',
    'build_runtime_capability_validation_messages',
    'requires_runtime_catalog',
    'validate_runtime_capability_matrix',
    'validate_runtime_interface_name',
)
