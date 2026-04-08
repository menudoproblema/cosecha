from __future__ import annotations

import os
import re
import shutil

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from cosecha.core.instrumentation import (
    COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
    COSECHA_KNOWLEDGE_STORAGE_ROOT_ENV,
    COSECHA_RUNTIME_STATE_DIR_ENV,
    COSECHA_SHADOW_ROOT_ENV,
)
from cosecha.workspace import ExecutionContext

_COVERAGE_COMPONENT_ID = 'cosecha.instrumentation.coverage'
_EPHEMERAL_DOMAIN_RE = re.compile(r'^[a-z0-9][a-z0-9._-]*$')


@dataclass(slots=True, frozen=True)
class ShadowExecutionContext:
    root_path: Path
    knowledge_storage_root: Path | None = None

    @property
    def runtime_state_dir(self) -> Path:
        return self.root_path / 'runtime'

    @property
    def instrumentation_dir(self) -> Path:
        return self.root_path / 'instrumentation'

    @property
    def metadata_file(self) -> Path:
        return self.instrumentation_dir / 'run-metadata.json'

    @property
    def coverage_dir(self) -> Path:
        return self.instrumentation_component_dir(_COVERAGE_COMPONENT_ID)

    @property
    def coverage_legacy_alias_dir(self) -> Path:
        return self.instrumentation_dir / 'coverage'

    def ephemeral_domain_dir(self, domain: str) -> Path:
        self._validate_ephemeral_domain(domain)
        if domain == 'instrumentation':
            return self.instrumentation_dir
        if domain == 'runtime':
            return self.runtime_state_dir
        return self.root_path / domain

    def component_ephemeral_dir(self, component_id: str, domain: str) -> Path:
        return self.ephemeral_domain_dir(domain) / component_id

    def instrumentation_component_dir(self, component_id: str) -> Path:
        return self.component_ephemeral_dir(component_id, 'instrumentation')

    def runtime_component_dir(self, component_id: str) -> Path:
        return self.component_ephemeral_dir(component_id, 'runtime')

    def persistent_component_dir(self, component_id: str) -> Path:
        return (
            self._effective_knowledge_storage_root()
            / 'components'
            / component_id
        ).resolve()

    def preserved_artifacts_component_dir(self, component_id: str) -> Path:
        return (
            self._effective_knowledge_storage_root()
            / 'preserved_artifacts'
            / self.root_path.name
            / component_id
        ).resolve()

    def materialize(self) -> ShadowExecutionContext:
        self.instrumentation_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_state_dir.mkdir(parents=True, exist_ok=True)
        self.coverage_dir.mkdir(parents=True, exist_ok=True)
        self._materialize_coverage_alias()
        return self

    def cleanup(
        self,
        *,
        preserve: bool,
        session_succeeded: bool | None = None,
        capabilities: Mapping[str, object] | None = None,
    ) -> None:
        if (
            session_succeeded is not None
            and capabilities
        ):
            self._apply_namespace_cleanup_policy(
                session_succeeded=session_succeeded,
                capabilities=capabilities,
            )
        if preserve:
            return
        shutil.rmtree(self.root_path, ignore_errors=True)

    def env(self) -> dict[str, str]:
        payload = {
            COSECHA_INSTRUMENTATION_METADATA_FILE_ENV: str(self.metadata_file),
            COSECHA_RUNTIME_STATE_DIR_ENV: str(self.runtime_state_dir),
            COSECHA_SHADOW_ROOT_ENV: str(self.root_path),
        }
        if self.knowledge_storage_root is not None:
            payload[COSECHA_KNOWLEDGE_STORAGE_ROOT_ENV] = str(
                self.knowledge_storage_root,
            )
        return payload

    def to_dict(self) -> dict[str, object]:
        return {
            'coverage_dir': str(self.coverage_dir),
            'instrumentation_dir': str(self.instrumentation_dir),
            'knowledge_storage_root': (
                None
                if self.knowledge_storage_root is None
                else str(self.knowledge_storage_root)
            ),
            'metadata_file': str(self.metadata_file),
            'root_path': str(self.root_path),
            'runtime_state_dir': str(self.runtime_state_dir),
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ShadowExecutionContext:
        return cls(
            root_path=Path(str(data['root_path'])),
            knowledge_storage_root=(
                None
                if data.get('knowledge_storage_root') is None
                else Path(str(data['knowledge_storage_root']))
            ),
        )

    @classmethod
    def for_session(
        cls,
        *,
        knowledge_storage_root: Path,
        session_id: str,
    ) -> ShadowExecutionContext:
        return cls(
            root_path=(
                knowledge_storage_root / 'shadow' / session_id
            ).resolve(),
            knowledge_storage_root=knowledge_storage_root.resolve(),
        )

    def _effective_knowledge_storage_root(self) -> Path:
        if self.knowledge_storage_root is not None:
            return self.knowledge_storage_root.resolve()
        msg = (
            'ShadowExecutionContext needs an explicit knowledge_storage_root '
            'to resolve persistent component paths.'
        )
        raise RuntimeError(msg)

    def _materialize_coverage_alias(self) -> None:
        alias_path = self.coverage_legacy_alias_dir
        if alias_path.exists() or alias_path.is_symlink():
            return
        try:
            alias_path.symlink_to(
                Path(_COVERAGE_COMPONENT_ID),
                target_is_directory=True,
            )
        except OSError:
            alias_path.mkdir(parents=True, exist_ok=True)

    def _apply_namespace_cleanup_policy(
        self,
        *,
        session_succeeded: bool,
        capabilities: Mapping[str, object],
    ) -> None:
        for capability in capabilities.values():
            component_id = getattr(capability, 'component_id', None)
            ephemeral_domain = getattr(capability, 'ephemeral_domain', None)
            if not isinstance(component_id, str):
                continue
            if not isinstance(ephemeral_domain, str):
                continue
            namespace = self.component_ephemeral_dir(
                component_id,
                ephemeral_domain,
            )
            if not namespace.exists():
                continue
            if session_succeeded:
                if bool(getattr(capability, 'cleanup_on_success', True)):
                    continue
                target = self.preserved_artifacts_component_dir(component_id)
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    shutil.rmtree(target, ignore_errors=True)
                shutil.move(str(namespace), str(target))
                continue
            if not bool(getattr(capability, 'preserve_on_failure', True)):
                shutil.rmtree(namespace, ignore_errors=True)

    @staticmethod
    def _validate_ephemeral_domain(domain: str) -> None:
        if _EPHEMERAL_DOMAIN_RE.match(domain):
            return
        msg = (
            'ephemeral_domain must be a safe path segment matching '
            "'^[a-z0-9][a-z0-9._-]*$'"
        )
        raise ValueError(msg)


def shadow_execution_context_from_env(
    env: dict[str, str] | None = None,
) -> ShadowExecutionContext | None:
    source_env = os.environ if env is None else env
    raw_root = source_env.get(COSECHA_SHADOW_ROOT_ENV)
    if not raw_root:
        return None
    raw_knowledge_storage_root = source_env.get(
        COSECHA_KNOWLEDGE_STORAGE_ROOT_ENV,
    )
    return ShadowExecutionContext(
        root_path=Path(raw_root).resolve(),
        knowledge_storage_root=(
            None
            if raw_knowledge_storage_root is None
            else Path(raw_knowledge_storage_root).resolve()
        ),
    )


def bind_shadow_execution_context(
    execution_context: ExecutionContext,
    shadow_context: ShadowExecutionContext,
) -> ExecutionContext:
    return ExecutionContext(
        execution_root=execution_context.execution_root,
        knowledge_storage_root=execution_context.knowledge_storage_root,
        shadow_root=shadow_context.root_path,
        invocation_id=execution_context.invocation_id,
        workspace_fingerprint=execution_context.workspace_fingerprint,
    )


def resolve_shadow_execution_context(
    execution_context: ExecutionContext,
    *,
    session_id: str,
    env: dict[str, str] | None = None,
) -> tuple[ExecutionContext, ShadowExecutionContext]:
    shadow_context = shadow_execution_context_from_env(env)
    if shadow_context is not None:
        shadow_context.materialize()
        return (
            bind_shadow_execution_context(execution_context, shadow_context),
            shadow_context,
        )

    if execution_context.shadow_root is not None:
        shadow_context = ShadowExecutionContext(
            root_path=execution_context.shadow_root.resolve(),
        )
        shadow_context.materialize()
        return (execution_context, shadow_context)

    shadow_context = ShadowExecutionContext.for_session(
        knowledge_storage_root=execution_context.knowledge_storage_root,
        session_id=session_id,
    )
    shadow_context.materialize()
    return (
        bind_shadow_execution_context(execution_context, shadow_context),
        shadow_context,
    )
