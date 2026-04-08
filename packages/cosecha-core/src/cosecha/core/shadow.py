from __future__ import annotations

import inspect
import re
import sys
import tempfile

from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING

from cosecha.core.capabilities import (
    CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
    CapabilityDescriptor,
)
from cosecha.core.instrumentation import (
    COSECHA_COVERAGE_ACTIVE_ENV,
    COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
    COSECHA_KNOWLEDGE_STORAGE_ROOT_ENV,
    COSECHA_RUNTIME_STATE_DIR_ENV,
    COSECHA_SHADOW_ROOT_ENV,
)
from cosecha.core.shadow_execution import ShadowExecutionContext


if TYPE_CHECKING:  # pragma: no cover
    from _contextvars import Token

type EphemeralArtifactDomain = str
_EPHEMERAL_DOMAIN_RE = re.compile(r'^[a-z0-9][a-z0-9._-]*$')

_active_shadow: ContextVar[ShadowExecutionContext | None] = ContextVar(
    'cosecha_active_shadow',
    default=None,
)
_active_ephemeral_capabilities: ContextVar[
    Mapping[str, EphemeralArtifactCapability] | None
] = ContextVar(
    'cosecha_active_ephemeral_capabilities',
    default=None,
)
_CALLER_COMPONENT_CACHE: dict[str, str] = {}


class ShadowNotBoundError(RuntimeError):
    pass


class ShadowCapabilityRegistryNotBoundError(RuntimeError):
    pass


class EphemeralCapabilityNotGrantedError(PermissionError):
    pass


class PersistentArtifactsNotEnabledError(PermissionError):
    pass


class DuplicateCapabilityGrantError(ValueError):
    pass


class ShadowComponentIdResolutionError(RuntimeError):
    pass


@dataclass(slots=True, frozen=True)
class EphemeralArtifactCapability:
    component_id: str
    ephemeral_domain: EphemeralArtifactDomain
    produces_persistent: bool = False
    cleanup_on_success: bool = True
    preserve_on_failure: bool = True
    description: str = ''


@dataclass(slots=True, frozen=True)
class ShadowHandle:
    component_id: str
    ephemeral_domain: EphemeralArtifactDomain
    ephemeral_root: Path
    persistent_root: Path | None

    def ephemeral_dir(self, *parts: str) -> Path:
        target = self.ephemeral_root.joinpath(*parts)
        target.mkdir(parents=True, exist_ok=True)
        return target

    def ephemeral_file(self, name: str) -> Path:
        self.ephemeral_root.mkdir(parents=True, exist_ok=True)
        return self.ephemeral_root / name

    def persistent_dir(self, *parts: str) -> Path:
        if self.persistent_root is None:
            raise PersistentArtifactsNotEnabledError(self.component_id)
        target = self.persistent_root.joinpath(*parts)
        target.mkdir(parents=True, exist_ok=True)
        return target

    def persistent_file(self, name: str) -> Path:
        if self.persistent_root is None:
            raise PersistentArtifactsNotEnabledError(self.component_id)
        self.persistent_root.mkdir(parents=True, exist_ok=True)
        return self.persistent_root / name


class _ShadowBinding:
    __slots__ = (
        '_capabilities',
        '_capability_token',
        '_previous_capabilities',
        '_previous_shadow',
        '_shadow',
        '_shadow_token',
    )

    def __init__(
        self,
        shadow: ShadowExecutionContext,
        *,
        ephemeral_capabilities: Mapping[str, EphemeralArtifactCapability],
    ) -> None:
        self._shadow = shadow
        self._capabilities = MappingProxyType(
            build_ephemeral_capability_registry(
                tuple(ephemeral_capabilities.values()),
            ),
        )
        self._previous_shadow: ShadowExecutionContext | None = None
        self._previous_capabilities: Mapping[
            str,
            EphemeralArtifactCapability,
        ] | None = None
        self._shadow_token: Token[ShadowExecutionContext | None] | None = None
        self._capability_token: Token[
            Mapping[str, EphemeralArtifactCapability] | None
        ] | None = None

    def __enter__(self) -> _ShadowBinding:
        self._previous_shadow = _active_shadow.get()
        self._previous_capabilities = _active_ephemeral_capabilities.get()
        self._shadow_token = _active_shadow.set(self._shadow)
        self._capability_token = _active_ephemeral_capabilities.set(
            self._capabilities,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self._capability_token is not None:
            try:
                _active_ephemeral_capabilities.reset(self._capability_token)
            except ValueError:
                _active_ephemeral_capabilities.set(
                    self._previous_capabilities,
                )
            self._capability_token = None
        if self._shadow_token is not None:
            try:
                _active_shadow.reset(self._shadow_token)
            except ValueError:
                _active_shadow.set(self._previous_shadow)
            self._shadow_token = None
        self._previous_shadow = None
        self._previous_capabilities = None
        return False


def get_active_shadow() -> ShadowExecutionContext:
    shadow = _active_shadow.get()
    if shadow is None:
        raise ShadowNotBoundError(
            'No active ShadowExecutionContext. This component is being '
            'invoked outside the runtime bootstrap.',
        )
    return shadow


def get_active_ephemeral_capabilities(
) -> Mapping[str, EphemeralArtifactCapability]:
    capabilities = _active_ephemeral_capabilities.get()
    if capabilities is None:
        raise ShadowCapabilityRegistryNotBoundError(
            'No active ephemeral capability registry. This component is '
            'being invoked outside the runtime bootstrap.',
        )
    return capabilities


def binding_shadow(
    shadow: ShadowExecutionContext,
    *,
    ephemeral_capabilities: Mapping[str, EphemeralArtifactCapability],
) -> _ShadowBinding:
    return _ShadowBinding(
        shadow,
        ephemeral_capabilities=ephemeral_capabilities,
    )


def acquire_shadow_handle(component_id: str) -> ShadowHandle:
    shadow = get_active_shadow()
    capabilities = get_active_ephemeral_capabilities()
    capability = capabilities.get(component_id)
    if capability is None:
        raise EphemeralCapabilityNotGrantedError(component_id)

    ephemeral_root = shadow.component_ephemeral_dir(
        component_id,
        capability.ephemeral_domain,
    )

    persistent_root = None
    if capability.produces_persistent:
        persistent_root = shadow.persistent_component_dir(component_id)

    return ShadowHandle(
        component_id=capability.component_id,
        ephemeral_domain=capability.ephemeral_domain,
        ephemeral_root=ephemeral_root,
        persistent_root=persistent_root,
    )


def ephemeral_dir(*parts: str) -> Path:
    return acquire_shadow_handle(_resolve_caller_component_id()).ephemeral_dir(
        *parts,
    )


def ephemeral_file(name: str) -> Path:
    return acquire_shadow_handle(_resolve_caller_component_id()).ephemeral_file(
        name,
    )


def persistent_dir(*parts: str) -> Path:
    return acquire_shadow_handle(_resolve_caller_component_id()).persistent_dir(
        *parts,
    )


def persistent_file(name: str) -> Path:
    return acquire_shadow_handle(_resolve_caller_component_id()).persistent_file(
        name,
    )


def build_ephemeral_artifact_capability(
    descriptors: tuple[CapabilityDescriptor, ...],
    *,
    declared_component_id: str,
) -> EphemeralArtifactCapability | None:
    descriptor = next(
        (
            descriptor
            for descriptor in descriptors
            if descriptor.name == CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS
        ),
        None,
    )
    if descriptor is None:
        return None

    attributes = {
        attribute.name: attribute.value
        for attribute in descriptor.attributes
    }
    component_id = attributes.get('component_id')
    if component_id != declared_component_id:
        msg = (
            'Mismatch between COSECHA_COMPONENT_ID and '
            'produces_ephemeral_artifacts.component_id '
            f'({declared_component_id!r} != {component_id!r})'
        )
        raise ValueError(msg)

    ephemeral_domain = _normalize_ephemeral_domain(
        attributes.get('ephemeral_domain'),
    )

    return EphemeralArtifactCapability(
        component_id=declared_component_id,
        ephemeral_domain=ephemeral_domain,
        produces_persistent=bool(
            attributes.get('produces_persistent', False),
        ),
        cleanup_on_success=bool(
            attributes.get('cleanup_on_success', True),
        ),
        preserve_on_failure=bool(
            attributes.get('preserve_on_failure', True),
        ),
        description=str(attributes.get('description', '')),
    )


def build_ephemeral_capability_registry(
    granted_capabilities: tuple[EphemeralArtifactCapability, ...],
) -> dict[str, EphemeralArtifactCapability]:
    registry: dict[str, EphemeralArtifactCapability] = {}
    for capability in granted_capabilities:
        if capability.component_id in registry:
            raise DuplicateCapabilityGrantError(capability.component_id)
        registry[capability.component_id] = capability
    return registry


@contextmanager
def use_detached_shadow(
    *,
    granted_capabilities: tuple[EphemeralArtifactCapability, ...] = (),
) -> Iterator[ShadowExecutionContext]:
    detached_root = Path(tempfile.mkdtemp(prefix='cosecha-shadow-'))
    shadow = ShadowExecutionContext(
        root_path=detached_root / 'shadow',
        knowledge_storage_root=detached_root / '.knowledge_storage',
    ).materialize()
    registry = build_ephemeral_capability_registry(granted_capabilities)
    try:
        with binding_shadow(shadow, ephemeral_capabilities=registry):
            yield shadow
    finally:
        shadow.cleanup(preserve=False)


def strip_shadow_environment(env: dict[str, str]) -> dict[str, str]:
    cleaned = dict(env)
    for key in (
        COSECHA_COVERAGE_ACTIVE_ENV,
        COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
        COSECHA_KNOWLEDGE_STORAGE_ROOT_ENV,
        COSECHA_RUNTIME_STATE_DIR_ENV,
        COSECHA_SHADOW_ROOT_ENV,
    ):
        cleaned.pop(key, None)
    return cleaned


def _resolve_caller_component_id() -> str:
    frame = inspect.currentframe()
    try:
        caller = None if frame is None else frame.f_back
        while caller is not None:
            module_name = caller.f_globals.get('__name__')
            if module_name != __name__:
                break
            caller = caller.f_back

        if caller is None:
            raise ShadowComponentIdResolutionError(
                'Unable to resolve caller module for shadow access.',
            )

        module_name = str(caller.f_globals.get('__name__', ''))
        cached = _CALLER_COMPONENT_CACHE.get(module_name)
        if cached is not None:
            return cached

        component_id = _resolve_component_id_from_module_hierarchy(
            module_name,
            caller.f_globals,
        )
        if component_id is None:
            raise ShadowComponentIdResolutionError(
                'Module '
                f'{module_name!r} or one of its parent packages must define '
                'COSECHA_COMPONENT_ID to use shadow sugar.',
            )

        _CALLER_COMPONENT_CACHE[module_name] = component_id
        return component_id
    finally:
        del frame


def _resolve_component_id_from_module_hierarchy(
    module_name: str,
    module_globals: dict[str, object],
) -> str | None:
    candidate_globals = module_globals
    current_name = module_name
    while current_name:
        component_id = candidate_globals.get('COSECHA_COMPONENT_ID')
        if isinstance(component_id, str) and component_id:
            return component_id
        current_name, _, _ = current_name.rpartition('.')
        if not current_name:
            break
        parent_module = sys.modules.get(current_name)
        candidate_globals = (
            {}
            if parent_module is None
            else vars(parent_module)
        )
    return None


def component_id_from_component_type(component_type: type[object]) -> str:
    component_id = getattr(component_type, 'COSECHA_COMPONENT_ID', None)
    if isinstance(component_id, str) and component_id:
        return component_id

    module = sys.modules.get(component_type.__module__)
    if module is not None:
        component_id = getattr(module, 'COSECHA_COMPONENT_ID', None)
        if isinstance(component_id, str) and component_id:
            return component_id

    raise ShadowComponentIdResolutionError(
        f'{component_type.__module__}.{component_type.__qualname__} must '
        'declare COSECHA_COMPONENT_ID.',
    )


def _normalize_ephemeral_domain(value: object) -> str:
    if not isinstance(value, str) or not _EPHEMERAL_DOMAIN_RE.match(value):
        msg = (
            'produces_ephemeral_artifacts.ephemeral_domain must be a safe '
            "path segment matching '^[a-z0-9][a-z0-9._-]*$'"
        )
        raise ValueError(msg)
    return value


__all__ = (
    'DuplicateCapabilityGrantError',
    'EphemeralArtifactCapability',
    'EphemeralCapabilityNotGrantedError',
    'PersistentArtifactsNotEnabledError',
    'ShadowCapabilityRegistryNotBoundError',
    'ShadowComponentIdResolutionError',
    'ShadowHandle',
    'ShadowNotBoundError',
    'acquire_shadow_handle',
    'binding_shadow',
    'build_ephemeral_artifact_capability',
    'build_ephemeral_capability_registry',
    'component_id_from_component_type',
    'ephemeral_dir',
    'ephemeral_file',
    'get_active_ephemeral_capabilities',
    'get_active_shadow',
    'persistent_dir',
    'persistent_file',
    'strip_shadow_environment',
    'use_detached_shadow',
)
