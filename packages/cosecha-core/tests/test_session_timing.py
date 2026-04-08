from __future__ import annotations

from cosecha.core.session_timing import SessionTiming, TestTiming


def test_session_timing_aggregates_durations_and_phase_totals() -> None:
    timing = SessionTiming(
        session_start=10.0,
        collect_start=11.0,
        collect_end=13.5,
        run_end=20.0,
        shutdown_start=20.0,
        shutdown_end=21.25,
        session_end=22.0,
        tests=[
            TestTiming(
                name='test-1',
                duration=2.0,
                phases={'setup': 0.5, 'run': 1.0},
            ),
            TestTiming(
                name='test-2',
                duration=1.5,
                phases={'setup': 0.25, 'teardown': 0.75},
            ),
        ],
    )
    timing.record_collect_phase('gherkin', 'discover', 0.3)
    timing.record_session_phase('gherkin', 'start', 0.2)
    timing.record_shutdown_phase('runtime_close', 0.4)

    assert timing.collect_duration == 2.5
    assert timing.tests_duration == 3.5
    assert timing.run_duration == 10.0
    assert timing.shutdown_duration == 1.25
    assert timing.total_duration == 12.0
    assert timing.test_phase_totals == {
        'setup': 0.75,
        'run': 1.0,
        'teardown': 0.75,
    }
    assert timing.collect_phases == {'gherkin': {'discover': 0.3}}
    assert timing.session_phases == {'gherkin': {'start': 0.2}}
    assert timing.shutdown_phases == {'runtime_close': 0.4}


def test_session_timing_returns_none_when_boundaries_are_missing() -> None:
    timing = SessionTiming()

    assert timing.collect_duration is None
    assert timing.run_duration is None
    assert timing.shutdown_duration is None
    assert timing.total_duration is None
