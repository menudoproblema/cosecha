from __future__ import annotations

import os
import shutil

from dataclasses import dataclass
from pathlib import Path

from cosecha.core.instrumentation import (
    COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
    COSECHA_RUNTIME_STATE_DIR_ENV,
    COSECHA_SHADOW_ROOT_ENV,
)
from cosecha.workspace import ExecutionContext


@dataclass(slots=True, frozen=True)
class ShadowExecutionContext:
    root_path: Path

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
        return self.instrumentation_dir / 'coverage'

    def materialize(self) -> ShadowExecutionContext:
        self.runtime_state_dir.mkdir(parents=True, exist_ok=True)
        self.coverage_dir.mkdir(parents=True, exist_ok=True)
        return self

    def cleanup(self, *, preserve: bool) -> None:
        if preserve:
            return
        shutil.rmtree(self.root_path, ignore_errors=True)

    def env(self) -> dict[str, str]:
        return {
            COSECHA_INSTRUMENTATION_METADATA_FILE_ENV: str(self.metadata_file),
            COSECHA_RUNTIME_STATE_DIR_ENV: str(self.runtime_state_dir),
            COSECHA_SHADOW_ROOT_ENV: str(self.root_path),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            'coverage_dir': str(self.coverage_dir),
            'instrumentation_dir': str(self.instrumentation_dir),
            'metadata_file': str(self.metadata_file),
            'root_path': str(self.root_path),
            'runtime_state_dir': str(self.runtime_state_dir),
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ShadowExecutionContext:
        return cls(root_path=Path(str(data['root_path'])))

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
        )


def shadow_execution_context_from_env(
    env: dict[str, str] | None = None,
) -> ShadowExecutionContext | None:
    source_env = os.environ if env is None else env
    raw_root = source_env.get(COSECHA_SHADOW_ROOT_ENV)
    if not raw_root:
        return None
    return ShadowExecutionContext(root_path=Path(raw_root).resolve())


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
