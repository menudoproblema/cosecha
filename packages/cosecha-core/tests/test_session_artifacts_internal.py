from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.config import ConfigSnapshot
from cosecha.core.session_artifacts import (
    EngineReportSummary,
    InstrumentationSummary,
    LiveEngineSnapshotSummary,
    SessionArtifact,
    SessionArtifactPersistencePolicy,
    SessionCoverageSummary,
    SessionReportSummary,
    SessionTelemetrySummary,
    SessionTimingSnapshot,
    TestTimingSnapshot,
)


@pytest.mark.parametrize(
    ('kwargs', 'message'),
    (
        ({'retention_scope': 'invalid'}, 'Unsupported session artifact retention scope'),
        ({'max_artifacts_per_scope': 0}, 'retention limit must be at least 1'),
        ({'max_failure_examples': 0}, 'failure example limit must be at least 1'),
        ({'max_failed_files': 0}, 'failed file limit must be at least 1'),
        ({'max_engine_snapshots': 0}, 'engine snapshot limit must be at least 1'),
        ({'max_artifact_age_seconds': 0}, 'max age must be positive'),
    ),
)
def test_session_artifact_policy_rejects_invalid_values(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        SessionArtifactPersistencePolicy(**kwargs)


def test_session_artifact_serialization_helpers_roundtrip() -> None:
    policy = SessionArtifactPersistencePolicy(retained_sections=('timing',))
    assert SessionArtifactPersistencePolicy.from_dict(policy.to_dict()) == policy

    timing_test = TestTimingSnapshot(
        name='scenario',
        duration=3.5,
        phases=(('setup', 1.0), ('run', 2.5)),
    )
    assert TestTimingSnapshot.from_dict(timing_test.to_dict()) == timing_test

    timing = SessionTimingSnapshot(tests=(timing_test,))
    assert SessionTimingSnapshot.from_dict(timing.to_dict()) == timing

    engine_summary = EngineReportSummary(
        engine_name='gherkin',
        total_tests=1,
        status_counts=(('passed', 1),),
    )
    assert EngineReportSummary.from_dict(engine_summary.to_dict()) == engine_summary

    coverage = SessionCoverageSummary(total_coverage=100.0, report_type='term')
    assert SessionCoverageSummary.from_dict(coverage.to_dict()) == coverage

    instrumentation = InstrumentationSummary(
        instrumentation_name='coverage',
        summary_kind='coverage.py',
        payload={'total_coverage': 100.0},
    )
    assert (
        InstrumentationSummary.from_dict(instrumentation.to_dict())
        == instrumentation
    )

    live_snapshot = LiveEngineSnapshotSummary(
        engine_name='gherkin',
        snapshot_kind='status',
        node_stable_id='node-1',
        payload={'state': 'done'},
    )
    assert (
        LiveEngineSnapshotSummary.from_dict(live_snapshot.to_dict())
        == live_snapshot
    )

    report_summary = SessionReportSummary(
        total_tests=1,
        status_counts=(('passed', 1),),
        instrumentation_summaries={'coverage': instrumentation},
    )
    assert (
        SessionReportSummary.from_dict(report_summary.to_dict())
        == report_summary
    )

    telemetry = SessionTelemetrySummary(span_count=1, distinct_span_names=1)
    assert SessionTelemetrySummary.from_dict(telemetry.to_dict()) == telemetry


def test_session_timing_snapshot_and_artifact_roundtrips_without_compaction() -> (
    None
):
    assert SessionTimingSnapshot.from_session_timing(None) is None

    timing = SessionTimingSnapshot.from_session_timing(
        SimpleNamespace(
            session_start=1.0,
            session_end=2.0,
            run_end=2.1,
            shutdown_start=2.2,
            shutdown_end=2.3,
            collect_start=1.1,
            collect_end=1.2,
            tests=[
                SimpleNamespace(
                    name='slow',
                    duration=3.0,
                    phases={'prepare': 1.0},
                ),
            ],
            collect_phases={'gherkin': {'discover': 1.0}},
            session_phases={'gherkin': {'execute': 2.0}},
            shutdown_phases={'cleanup': 0.5},
        ),
    )
    assert timing is not None

    report_summary = SessionReportSummary(
        total_tests=1,
        status_counts=(('passed', 1),),
        failed_examples=('example-1',),
        failed_files=('suite.feature',),
    )
    artifact = SessionArtifact(
        session_id='session-1',
        root_path='/workspace/demo',
        config_snapshot=ConfigSnapshot(
            root_path='/workspace/demo',
            output_mode='summary',
            output_detail='standard',
            capture_log=True,
            stop_on_error=False,
            concurrency=1,
            strict_step_ambiguity=False,
        ),
        capability_snapshots=(),
        recorded_at=10.0,
        timing=timing,
        report_summary=report_summary,
        persistence_policy=SessionArtifactPersistencePolicy(
            retained_sections=('timing', 'report_summary'),
            max_timing_tests=10,
            max_failure_examples=10,
            max_failed_files=10,
            max_engine_snapshots=10,
        ),
    )

    compacted = artifact.apply_persistence_policy()
    assert compacted.report_summary == report_summary
    assert compacted.timing == timing

    payload = artifact.to_dict()
    restored = SessionArtifact.from_dict(payload)
    assert restored == artifact
