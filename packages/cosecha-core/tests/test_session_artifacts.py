from __future__ import annotations

import pytest

from cosecha.core.config import ConfigSnapshot
from cosecha.core.session_artifacts import (
    EngineReportSummary,
    InstrumentationSummary,
    LiveEngineSnapshotSummary,
    SessionArtifact,
    SessionArtifactPersistencePolicy,
    SessionReportSummary,
    SessionTelemetrySummary,
    SessionTimingSnapshot,
    TestTimingSnapshot,
)


def test_session_artifact_persistence_policy_rejects_invalid_limits() -> None:
    with pytest.raises(
        ValueError,
        match='Session artifact timing limit must be at least 1',
    ):
        SessionArtifactPersistencePolicy(max_timing_tests=0)


def test_session_artifact_apply_persistence_policy_compacts_snapshots(
) -> None:
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
        recorded_at=1.0,
        timing=SessionTimingSnapshot(
            tests=(
                TestTimingSnapshot(name='slow', duration=3.0),
                TestTimingSnapshot(name='medium', duration=2.0),
                TestTimingSnapshot(name='fast', duration=1.0),
            ),
        ),
        report_summary=SessionReportSummary(
            total_tests=3,
            status_counts=(('passed', 2), ('failed', 1)),
            engine_summaries=(
                EngineReportSummary(
                    engine_name='pytest',
                    total_tests=3,
                    status_counts=(('passed', 2), ('failed', 1)),
                    failed_examples=('a', 'b', 'c'),
                    failed_files=('one.py', 'two.py', 'three.py'),
                ),
            ),
            live_engine_snapshots=(
                LiveEngineSnapshotSummary(
                    engine_name='pytest',
                    snapshot_kind='node',
                    node_stable_id='node-1',
                    payload={'a': 1},
                ),
                LiveEngineSnapshotSummary(
                    engine_name='pytest',
                    snapshot_kind='node',
                    node_stable_id='node-2',
                    payload={'b': 2},
                ),
            ),
            failed_examples=('a', 'b', 'c'),
            failed_files=('one.py', 'two.py', 'three.py'),
            instrumentation_summaries={
                'coverage': InstrumentationSummary(
                    instrumentation_name='coverage',
                    summary_kind='coverage.py',
                    payload={
                        'total_coverage': 98.0,
                        'report_type': 'term',
                    },
                ),
            },
        ),
        telemetry_summary=SessionTelemetrySummary(
            span_count=5,
            distinct_span_names=2,
        ),
        persistence_policy=SessionArtifactPersistencePolicy(
            retained_sections=(
                'config_snapshot',
                'timing',
                'report_summary',
            ),
            max_timing_tests=2,
            max_failure_examples=1,
            max_failed_files=1,
            max_engine_snapshots=1,
        ),
    )

    compacted = artifact.apply_persistence_policy()

    assert compacted.telemetry_summary is None
    assert compacted.timing is not None
    assert compacted.report_summary is not None
    assert tuple(test.name for test in compacted.timing.tests) == (
        'slow',
        'medium',
    )
    assert compacted.timing.truncated_test_count == 1
    assert compacted.report_summary.failed_examples == ('a',)
    assert compacted.report_summary.failed_files == ('one.py',)
    assert len(compacted.report_summary.live_engine_snapshots) == 1
