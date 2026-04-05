from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TestTiming:
    # Evita que Pytest intente recolectar esta clase como un test.
    __test__ = False
    name: str
    duration: float
    phases: dict[str, float] = field(default_factory=dict)


@dataclass
class SessionTiming:
    session_start: float | None = None
    session_end: float | None = None
    run_end: float | None = None
    shutdown_start: float | None = None
    shutdown_end: float | None = None
    collect_start: float | None = None
    collect_end: float | None = None
    tests: list[TestTiming] = field(default_factory=list)
    # Duraciones de sub-fases por engine: {engine_name: {phase: seconds}}
    collect_phases: dict[str, dict[str, float]] = field(default_factory=dict)
    # Duraciones de fases de sesion por engine: {engine_name: {phase: seconds}}
    session_phases: dict[str, dict[str, float]] = field(default_factory=dict)
    shutdown_phases: dict[str, float] = field(default_factory=dict)

    def record_collect_phase(
        self,
        engine: str,
        phase: str,
        duration: float,
    ) -> None:
        if engine not in self.collect_phases:
            self.collect_phases[engine] = {}
        self.collect_phases[engine][phase] = duration

    def record_session_phase(
        self,
        engine: str,
        phase: str,
        duration: float,
    ) -> None:
        if engine not in self.session_phases:
            self.session_phases[engine] = {}
        self.session_phases[engine][phase] = duration

    def record_shutdown_phase(
        self,
        phase: str,
        duration: float,
    ) -> None:
        self.shutdown_phases[phase] = duration

    @property
    def collect_duration(self) -> float | None:
        if self.collect_start is not None and self.collect_end is not None:
            return self.collect_end - self.collect_start
        return None

    @property
    def tests_duration(self) -> float:
        return sum(t.duration for t in self.tests)

    @property
    def run_duration(self) -> float | None:
        if self.session_start is not None and self.run_end is not None:
            return self.run_end - self.session_start
        return None

    @property
    def shutdown_duration(self) -> float | None:
        if self.shutdown_start is not None and self.shutdown_end is not None:
            return self.shutdown_end - self.shutdown_start
        return None

    @property
    def test_phase_totals(self) -> dict[str, float]:
        totals: dict[str, float] = {}
        for test in self.tests:
            for phase, duration in test.phases.items():
                totals[phase] = totals.get(phase, 0.0) + duration

        return totals

    @property
    def total_duration(self) -> float | None:
        if self.session_start is not None and self.session_end is not None:
            return self.session_end - self.session_start
        return None
