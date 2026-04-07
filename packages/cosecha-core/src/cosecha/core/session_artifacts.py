from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal

from cosecha.core.capabilities import (
    CapabilityComponentSnapshot,  # noqa: TC001
)
from cosecha.core.config import ConfigSnapshot  # noqa: TC001
from cosecha.core.execution_ir import PlanExplanation  # noqa: TC001
from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.session_timing import SessionTiming


SESSION_ARTIFACT_RETENTION_SCOPE_ROOT_PATH = 'root_path'
SESSION_ARTIFACT_RETENTION_SCOPE_WORKSPACE_FINGERPRINT = (
    'workspace_fingerprint'
)
DEFAULT_SESSION_ARTIFACT_RETAINED_SECTIONS = (
    'config_snapshot',
    'capability_snapshots',
    'plan_explanation',
    'timing',
    'has_failures',
    'report_summary',
    'telemetry_summary',
)
DEFAULT_SESSION_ARTIFACT_RETENTION_LIMIT = 20
DEFAULT_SESSION_ARTIFACT_MAX_TIMING_TESTS = 200
DEFAULT_SESSION_ARTIFACT_MAX_FAILURE_EXAMPLES = 50
DEFAULT_SESSION_ARTIFACT_MAX_FAILED_FILES = 50
DEFAULT_SESSION_ARTIFACT_MAX_ENGINE_SNAPSHOTS = 100
DEFAULT_SESSION_ARTIFACT_MAX_AGE_SECONDS = 30.0 * 24.0 * 60.0 * 60.0


@dataclass(slots=True, frozen=True)
class SessionArtifactPersistencePolicy:
    retained_sections: tuple[str, ...] = (
        DEFAULT_SESSION_ARTIFACT_RETAINED_SECTIONS
    )
    retention_scope: str = (
        SESSION_ARTIFACT_RETENTION_SCOPE_WORKSPACE_FINGERPRINT
    )
    max_artifacts_per_scope: int = DEFAULT_SESSION_ARTIFACT_RETENTION_LIMIT
    max_timing_tests: int = DEFAULT_SESSION_ARTIFACT_MAX_TIMING_TESTS
    max_failure_examples: int = DEFAULT_SESSION_ARTIFACT_MAX_FAILURE_EXAMPLES
    max_failed_files: int = DEFAULT_SESSION_ARTIFACT_MAX_FAILED_FILES
    max_engine_snapshots: int = DEFAULT_SESSION_ARTIFACT_MAX_ENGINE_SNAPSHOTS
    max_artifact_age_seconds: float | None = (
        DEFAULT_SESSION_ARTIFACT_MAX_AGE_SECONDS
    )

    def __post_init__(self) -> None:
        if self.retention_scope not in {
            SESSION_ARTIFACT_RETENTION_SCOPE_ROOT_PATH,
            SESSION_ARTIFACT_RETENTION_SCOPE_WORKSPACE_FINGERPRINT,
        }:
            msg = (
                'Unsupported session artifact retention scope: '
                f'{self.retention_scope!r}'
            )
            raise ValueError(msg)
        if self.max_artifacts_per_scope < 1:
            msg = 'Session artifact retention limit must be at least 1'
            raise ValueError(msg)
        if self.max_timing_tests < 1:
            msg = 'Session artifact timing limit must be at least 1'
            raise ValueError(msg)
        if self.max_failure_examples < 1:
            msg = 'Session artifact failure example limit must be at least 1'
            raise ValueError(msg)
        if self.max_failed_files < 1:
            msg = 'Session artifact failed file limit must be at least 1'
            raise ValueError(msg)
        if self.max_engine_snapshots < 1:
            msg = 'Session artifact engine snapshot limit must be at least 1'
            raise ValueError(msg)
        if (
            self.max_artifact_age_seconds is not None
            and self.max_artifact_age_seconds <= 0
        ):
            msg = 'Session artifact max age must be positive when provided'
            raise ValueError(msg)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> SessionArtifactPersistencePolicy:
        return from_builtins_dict(data, target_type=cls)


def default_session_artifact_persistence_policy() -> (
    SessionArtifactPersistencePolicy
):
    return SessionArtifactPersistencePolicy()


@dataclass(slots=True, frozen=True)
class TestTimingSnapshot:
    __test__ = False
    name: str
    duration: float
    phases: tuple[tuple[str, float], ...] = field(default_factory=tuple)

    @classmethod
    def from_session_timing_test(
        cls,
        test_timing,
    ) -> TestTimingSnapshot:
        return cls(
            name=test_timing.name,
            duration=test_timing.duration,
            phases=tuple(sorted(test_timing.phases.items())),
        )

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> TestTimingSnapshot:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class SessionTimingSnapshot:
    session_start: float | None = None
    session_end: float | None = None
    run_end: float | None = None
    shutdown_start: float | None = None
    shutdown_end: float | None = None
    collect_start: float | None = None
    collect_end: float | None = None
    tests: tuple[TestTimingSnapshot, ...] = field(default_factory=tuple)
    collect_phases: tuple[tuple[str, tuple[tuple[str, float], ...]], ...] = ()
    session_phases: tuple[tuple[str, tuple[tuple[str, float], ...]], ...] = ()
    shutdown_phases: tuple[tuple[str, float], ...] = field(
        default_factory=tuple,
    )
    truncated_test_count: int = 0

    @classmethod
    def from_session_timing(
        cls,
        session_timing: SessionTiming | None,
    ) -> SessionTimingSnapshot | None:
        if session_timing is None:
            return None

        return cls(
            session_start=session_timing.session_start,
            session_end=session_timing.session_end,
            run_end=session_timing.run_end,
            shutdown_start=session_timing.shutdown_start,
            shutdown_end=session_timing.shutdown_end,
            collect_start=session_timing.collect_start,
            collect_end=session_timing.collect_end,
            tests=tuple(
                TestTimingSnapshot.from_session_timing_test(test)
                for test in session_timing.tests
            ),
            collect_phases=tuple(
                (
                    engine_name,
                    tuple(sorted(phases.items())),
                )
                for engine_name, phases in sorted(
                    session_timing.collect_phases.items(),
                )
            ),
            session_phases=tuple(
                (
                    engine_name,
                    tuple(sorted(phases.items())),
                )
                for engine_name, phases in sorted(
                    session_timing.session_phases.items(),
                )
            ),
            shutdown_phases=tuple(
                sorted(session_timing.shutdown_phases.items()),
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> SessionTimingSnapshot:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class EngineReportSummary:
    engine_name: str
    total_tests: int
    status_counts: tuple[tuple[str, int], ...]
    failure_kind_counts: tuple[tuple[str, int], ...] = ()
    detail_counts: tuple[tuple[str, int], ...] = ()
    failed_examples: tuple[str, ...] = ()
    failed_files: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> EngineReportSummary:
        return from_builtins_dict(data, target_type=cls)


type CoverageMeasurementScope = Literal['controller_process']


@dataclass(slots=True, frozen=True)
class SessionCoverageSummary:
    total_coverage: float
    report_type: str
    measurement_scope: CoverageMeasurementScope = 'controller_process'
    branch: bool = False
    engine_names: tuple[str, ...] = ()
    source_targets: tuple[str, ...] = ()
    includes_python_subprocesses: bool = False
    includes_worker_processes: bool = False

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SessionCoverageSummary:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class InstrumentationSummary:
    instrumentation_name: str
    summary_kind: str
    payload: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> InstrumentationSummary:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True)
class SessionReportState:
    instrumentation_summaries: dict[str, InstrumentationSummary] = field(
        default_factory=dict,
    )


@dataclass(slots=True, frozen=True)
class LiveEngineSnapshotSummary:
    engine_name: str
    snapshot_kind: str
    node_stable_id: str
    payload: dict[str, object]
    payload_keys: tuple[str, ...] = ()
    payload_size: int = 0
    update_count: int = 1
    worker_id: int | None = None
    last_updated_at: float | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> LiveEngineSnapshotSummary:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class SessionReportSummary:
    total_tests: int
    status_counts: tuple[tuple[str, int], ...]
    failure_kind_counts: tuple[tuple[str, int], ...] = ()
    engine_summaries: tuple[EngineReportSummary, ...] = ()
    live_engine_snapshots: tuple[LiveEngineSnapshotSummary, ...] = ()
    failed_examples: tuple[str, ...] = ()
    failed_files: tuple[str, ...] = ()
    instrumentation_summaries: dict[str, InstrumentationSummary] = field(
        default_factory=dict,
    )

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SessionReportSummary:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class SessionTelemetrySummary:
    span_count: int
    distinct_span_names: int
    top_span_names: tuple[tuple[str, int], ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SessionTelemetrySummary:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class SessionArtifact:
    session_id: str
    root_path: str
    config_snapshot: ConfigSnapshot
    capability_snapshots: tuple[CapabilityComponentSnapshot, ...]
    recorded_at: float
    workspace_fingerprint: str | None = None
    trace_id: str | None = None
    plan_id: str | None = None
    plan_explanation: PlanExplanation | None = None
    timing: SessionTimingSnapshot | None = None
    has_failures: bool | None = None
    report_summary: SessionReportSummary | None = None
    telemetry_summary: SessionTelemetrySummary | None = None
    persistence_policy: SessionArtifactPersistencePolicy = field(
        default_factory=default_session_artifact_persistence_policy,
    )

    def apply_persistence_policy(self) -> SessionArtifact:
        retained_sections = set(self.persistence_policy.retained_sections)
        compacted_timing = self.timing
        if compacted_timing is not None:
            compacted_timing = _compact_session_timing_snapshot(
                compacted_timing,
                self.persistence_policy.max_timing_tests,
            )
        compacted_report_summary = self.report_summary
        if compacted_report_summary is not None:
            compacted_report_summary = _compact_session_report_summary(
                compacted_report_summary,
                self.persistence_policy.max_failure_examples,
                self.persistence_policy.max_failed_files,
                self.persistence_policy.max_engine_snapshots,
            )

        return replace(
            self,
            capability_snapshots=(
                self.capability_snapshots
                if 'capability_snapshots' in retained_sections
                else ()
            ),
            plan_explanation=(
                self.plan_explanation
                if 'plan_explanation' in retained_sections
                else None
            ),
            timing=(
                compacted_timing if 'timing' in retained_sections else None
            ),
            has_failures=(
                self.has_failures
                if 'has_failures' in retained_sections
                else None
            ),
            report_summary=(
                compacted_report_summary
                if 'report_summary' in retained_sections
                else None
            ),
            telemetry_summary=(
                self.telemetry_summary
                if 'telemetry_summary' in retained_sections
                else None
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SessionArtifact:
        return from_builtins_dict(data, target_type=cls)


def _compact_session_timing_snapshot(
    timing: SessionTimingSnapshot,
    max_timing_tests: int,
) -> SessionTimingSnapshot:
    if len(timing.tests) <= max_timing_tests:
        return timing

    retained_tests = tuple(
        sorted(
            timing.tests,
            key=lambda test: (test.duration, test.name),
            reverse=True,
        )[:max_timing_tests],
    )
    return replace(
        timing,
        tests=retained_tests,
        truncated_test_count=(len(timing.tests) - len(retained_tests)),
    )


def _compact_session_report_summary(
    report_summary: SessionReportSummary,
    max_failure_examples: int,
    max_failed_files: int,
    max_engine_snapshots: int,
) -> SessionReportSummary:
    compacted_engine_summaries = tuple(
        replace(
            engine_summary,
            failed_examples=engine_summary.failed_examples[
                :max_failure_examples
            ],
            failed_files=engine_summary.failed_files[:max_failed_files],
        )
        for engine_summary in report_summary.engine_summaries
    )
    compacted_live_engine_snapshots = report_summary.live_engine_snapshots[
        :max_engine_snapshots
    ]

    if (
        len(report_summary.failed_examples) <= max_failure_examples
        and len(report_summary.failed_files) <= max_failed_files
        and len(report_summary.live_engine_snapshots) <= max_engine_snapshots
        and compacted_engine_summaries == report_summary.engine_summaries
    ):
        return report_summary

    return replace(
        report_summary,
        failed_examples=report_summary.failed_examples[:max_failure_examples],
        failed_files=report_summary.failed_files[:max_failed_files],
        engine_summaries=compacted_engine_summaries,
        live_engine_snapshots=compacted_live_engine_snapshots,
    )
