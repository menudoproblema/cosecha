from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path

    from cosecha.core.session_artifacts import InstrumentationSummary


COSECHA_COVERAGE_ACTIVE_ENV = 'COSECHA_COVERAGE_ACTIVE'
COSECHA_INSTRUMENTATION_METADATA_FILE_ENV = (
    'COSECHA_INSTRUMENTATION_METADATA_FILE'
)


@dataclass(slots=True, frozen=True)
class Contribution:
    env: dict[str, str] = field(default_factory=dict)
    argv_prefix: tuple[str, ...] = ()
    workdir_files: dict[str, str] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()


class ExecutionInstrumenter(Protocol):
    def prepare(self, *, workdir: Path) -> Contribution: ...

    def collect(self, *, workdir: Path) -> InstrumentationSummary: ...
