from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence
    from pathlib import Path

    from cosecha.core.capabilities import CapabilityDescriptor
    from cosecha.core.session_artifacts import InstrumentationSummary


COSECHA_COVERAGE_ACTIVE_ENV = 'COSECHA_COVERAGE_ACTIVE'
COSECHA_INSTRUMENTATION_METADATA_FILE_ENV = (
    'COSECHA_INSTRUMENTATION_METADATA_FILE'
)
COSECHA_KNOWLEDGE_STORAGE_ROOT_ENV = 'COSECHA_KNOWLEDGE_STORAGE_ROOT'
COSECHA_RUNTIME_STATE_DIR_ENV = 'COSECHA_RUNTIME_STATE_DIR'
COSECHA_SHADOW_ROOT_ENV = 'COSECHA_SHADOW_ROOT'
type InstrumentationStability = Literal['stable', 'experimental']


@dataclass(slots=True, frozen=True)
class Contribution:
    """Describe bootstrap changes for a single instrumented child process.

    `Contribution` is intentionally narrow:

    - `env` adds or overrides child environment variables.
    - `argv_prefix` wraps the child command before the stripped CLI argv.
    - `workdir_files` materializes temp files under the instrumenter workdir.
    - `warnings` are rendered by the shell before the child starts.

    It must not describe reporting, IPC, or mutations outside the workdir.
    """

    env: dict[str, str] = field(default_factory=dict)
    argv_prefix: tuple[str, ...] = ()
    workdir_files: dict[str, str] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()


class ExecutionInstrumenter(Protocol):
    """Internal contract for process bootstrap instrumentation.

    Implementations are shell-owned and intentionally internal to Cosecha.
    They may prepare a child process and harvest a structured summary after
    the child exits, but they do not own reporting or session persistence.
    """

    def strip_bootstrap_options(self, argv: Sequence[str]) -> list[str]:
        """Return CLI argv without instrumenter-specific bootstrap flags.

        The method must be pure and idempotent: calling it repeatedly with
        the same argv must return the same stripped result.
        """
        ...

    def prepare(self, *, workdir: Path) -> Contribution:
        """Build child-process bootstrap changes scoped to `workdir`.

        Implementations may create temp file contents and command prefixes,
        but must not perform side effects outside `workdir`.
        """
        ...

    def collect(self, *, workdir: Path) -> InstrumentationSummary:
        """Collect a structured summary from `workdir` after child exit.

        This method must tolerate test failures in the child process. It is
        only allowed to assume that the child finished and left behind any
        instrumentation data it could persist.
        """
        ...


class InstrumentationComponent(ExecutionInstrumenter, Protocol):
    """Public contract for instrumentation packages published by Cosecha."""

    @classmethod
    def instrumentation_name(cls) -> str: ...

    @classmethod
    def instrumentation_api_version(cls) -> int: ...

    @classmethod
    def instrumentation_stability(cls) -> InstrumentationStability: ...

    @classmethod
    def describe_capabilities(cls) -> tuple[CapabilityDescriptor, ...]: ...
