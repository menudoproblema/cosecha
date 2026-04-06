from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import TypedDict

from cosecha.core.console import Console
from cosecha.core.diagnostics import ConsoleDiagnosticSink
from cosecha.core.items import TestResultStatus
from cosecha.core.output import OutputDetail, OutputMode
from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


class StatusColorType(TypedDict):
    style: str
    highlight: str


type Theme = dict[TestResultStatus, StatusColorType]


@dataclass(slots=True, frozen=True)
class ConfigSnapshot:
    root_path: str
    output_mode: str
    output_detail: str
    capture_log: bool
    stop_on_error: bool
    concurrency: int
    strict_step_ambiguity: bool
    persist_live_engine_snapshots: bool = False
    reports: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    definition_paths: tuple[str, ...] = field(default_factory=tuple)

    @property
    def fingerprint(self) -> str:
        payload = (
            self.root_path,
            self.output_mode,
            self.output_detail,
            self.capture_log,
            self.stop_on_error,
            self.concurrency,
            self.strict_step_ambiguity,
            self.persist_live_engine_snapshots,
            self.reports,
            self.definition_paths,
        )
        return sha256(repr(payload).encode('utf-8')).hexdigest()

    def to_dict(self) -> dict[str, object]:
        payload = to_builtins_dict(self)
        payload['fingerprint'] = self.fingerprint
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ConfigSnapshot:
        normalized = dict(data)
        normalized.pop('fingerprint', None)
        return from_builtins_dict(normalized, target_type=cls)


class Config:
    __slots__ = (
        'capture_log',
        'concurrency',
        'console',
        'definition_paths',
        'diagnostics',
        'output_detail',
        'output_mode',
        'persist_live_engine_snapshots',
        'reports',
        'root_path',
        'stop_on_error',
        'strict_step_ambiguity',
        'theme',
    )

    def __init__(  # noqa: PLR0913
        self,
        root_path: Path,
        output_mode: OutputMode = OutputMode.SUMMARY,
        output_detail: OutputDetail = OutputDetail.STANDARD,
        capture_log: bool = True,  # noqa: FBT001, FBT002
        stop_on_error: bool = False,  # noqa: FBT001, FBT002
        concurrency: int = 1,
        strict_step_ambiguity: bool = False,  # noqa: FBT001, FBT002
        persist_live_engine_snapshots: bool = False,  # noqa: FBT001, FBT002
        console_cls: type[Console] | None = None,
        reports: dict[str, Path] | None = None,
        definition_paths: tuple[Path, ...] = (),
    ) -> None:
        self.root_path = root_path.resolve()

        self.output_mode = output_mode
        self.output_detail = output_detail
        self.capture_log = capture_log
        self.stop_on_error = stop_on_error
        self.concurrency = concurrency
        self.strict_step_ambiguity = strict_step_ambiguity
        self.persist_live_engine_snapshots = persist_live_engine_snapshots
        self.reports = reports or {}
        self.definition_paths = tuple(
            definition_path.resolve()
            if definition_path.is_absolute()
            else (self.root_path / definition_path).resolve()
            for definition_path in definition_paths
        )

        effective_console_cls = console_cls or Console
        self.console = effective_console_cls(
            output_mode=self.output_mode,
            output_detail=self.output_detail,
        )
        self.diagnostics = ConsoleDiagnosticSink(self)

        self.theme: Theme = {
            TestResultStatus.PENDING: {
                'style': 'blue',
                'highlight': 'bold bright_blue',
            },
            TestResultStatus.FAILED: {
                'style': 'red',
                'highlight': 'bold bright_red',
            },
            TestResultStatus.PASSED: {
                'style': 'green',
                'highlight': 'bold bright_green',
            },
            TestResultStatus.SKIPPED: {
                'style': 'yellow',
                'highlight': 'bold bright_yellow',
            },
            TestResultStatus.ERROR: {
                'style': 'red1',
                'highlight': 'bold bright_red',
            },
        }

    def snapshot(self) -> ConfigSnapshot:
        return ConfigSnapshot(
            root_path=str(self.root_path),
            output_mode=str(self.output_mode),
            output_detail=str(self.output_detail),
            capture_log=self.capture_log,
            stop_on_error=self.stop_on_error,
            concurrency=self.concurrency,
            strict_step_ambiguity=self.strict_step_ambiguity,
            persist_live_engine_snapshots=self.persist_live_engine_snapshots,
            reports=tuple(
                sorted(
                    (name, str(path)) for name, path in self.reports.items()
                ),
            ),
            definition_paths=tuple(
                str(path) for path in self.definition_paths
            ),
        )

    @classmethod
    def from_snapshot(
        cls,
        snapshot: ConfigSnapshot,
        *,
        console_cls: type[Console] | None = None,
    ) -> Config:
        return cls(
            root_path=Path(snapshot.root_path),
            output_mode=OutputMode(snapshot.output_mode),
            output_detail=OutputDetail(snapshot.output_detail),
            capture_log=snapshot.capture_log,
            stop_on_error=snapshot.stop_on_error,
            concurrency=snapshot.concurrency,
            strict_step_ambiguity=snapshot.strict_step_ambiguity,
            persist_live_engine_snapshots=(
                snapshot.persist_live_engine_snapshots
            ),
            console_cls=console_cls,
            reports={name: Path(path) for name, path in snapshot.reports},
            definition_paths=tuple(
                Path(path) for path in snapshot.definition_paths
            ),
        )

    @classmethod
    def console_from_snapshot(
        cls,
        snapshot: ConfigSnapshot,
        *,
        console_cls: type[Console] | None = None,
    ) -> Console:
        effective_console_cls = console_cls or Console
        return effective_console_cls(
            output_mode=OutputMode(snapshot.output_mode),
            output_detail=OutputDetail(snapshot.output_detail),
        )
