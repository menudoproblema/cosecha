from __future__ import annotations

import sqlite3
import sys

from types import SimpleNamespace

import pytest

from cosecha.core.config import ConfigSnapshot
from cosecha.core.session_artifacts import (
    InstrumentationSummary,
    SessionArtifact,
    SessionReportSummary,
    SessionTelemetrySummary,
    SessionTimingSnapshot,
    SessionArtifactPersistencePolicy,
)
from cosecha.shell.launcher import (
    _bootstrap_coverage,
    _update_session_artifact,
    _should_bootstrap_coverage,
    _strip_coverage_options,
    main,
)


def test_should_bootstrap_coverage_only_for_run_commands_with_cov(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('COSECHA_COVERAGE_ACTIVE', raising=False)
    assert _should_bootstrap_coverage(['run', '--cov', 'src/demo']) is True
    assert _should_bootstrap_coverage(['plan', '--cov', 'src/demo']) is False
    assert _should_bootstrap_coverage(['run', '--path', 'tests/unit']) is False

    monkeypatch.setenv('COSECHA_COVERAGE_ACTIVE', '1')
    assert _should_bootstrap_coverage(['run', '--cov', 'src/demo']) is False


def test_strip_coverage_options_removes_bootstrap_flags() -> None:
    assert _strip_coverage_options(
        [
            'run',
            '--cov',
            'src/demo',
            '--cov-report',
            'term-missing',
            '--cov-branch',
            '--path',
            'tests/unit',
        ],
    ) == ['run', '--path', 'tests/unit']
    assert _strip_coverage_options(
        ['run', '--cov=src/demo', '--path', 'tests/unit'],
    ) == ['run', '--path', 'tests/unit']


def test_bootstrap_coverage_reexecutes_under_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, object] = {}

    def fake_run(command, *, check, env):
        recorded['command'] = command
        recorded['check'] = check
        recorded['env'] = env
        metadata_path = env['COSECHA_INSTRUMENTATION_METADATA_FILE']
        with open(metadata_path, 'w', encoding='utf-8') as handle:
            handle.write(
                '{"knowledge_base_path": null, "session_id": "session-1"}',
            )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr('cosecha.shell.launcher.subprocess.run', fake_run)
    monkeypatch.setattr(
        'cosecha.shell.launcher._update_session_artifact',
        lambda metadata, *, summary: recorded.update(
            {'metadata': metadata, 'summary': summary},
        )
        or (
            SimpleNamespace(
                config_snapshot=ConfigSnapshot(
                    root_path='/workspace/demo',
                    output_mode='summary',
                    output_detail='standard',
                    capture_log=True,
                    stop_on_error=False,
                    concurrency=1,
                    strict_step_ambiguity=False,
                ),
            ),
            None,
        ),
    )
    monkeypatch.setattr(
        'cosecha.shell.launcher._render_coverage_summary',
        lambda summary, *, config_snapshot: recorded.update(
            {
                'config_snapshot': config_snapshot,
                'rendered_summary': summary,
            },
        ),
    )
    monkeypatch.setattr(
        'cosecha.plugin.coverage.CoverageInstrumenter.collect',
        lambda self, *, workdir: SimpleNamespace(
            instrumentation_name='coverage',
            payload={'total_coverage': 87.5},
        ),
    )

    exit_code = _bootstrap_coverage(
        ['run', '--cov', 'src/demo', '--cov-branch', '--path', 'tests/unit'],
    )

    assert exit_code == 0
    assert recorded['command'][0:4] == [
        sys.executable,
        '-m',
        'coverage',
        'run',
    ]
    assert any(
        argument.startswith('--rcfile=')
        for argument in recorded['command']
    )
    assert recorded['env']['COVERAGE_PROCESS_START'].endswith(
        '.cosecha.coveragerc',
    )
    runner_module_index = recorded['command'].index(
        'cosecha.shell.runner_cli',
    )
    assert recorded['command'][runner_module_index - 1 : runner_module_index + 2] == [
        '-m',
        'cosecha.shell.runner_cli',
        'run',
    ]
    assert recorded['env']['COSECHA_COVERAGE_ACTIVE'] == '1'
    assert recorded['metadata']['session_id'] == 'session-1'
    assert recorded['summary'].instrumentation_name == 'coverage'
    assert recorded['config_snapshot'].output_mode == 'summary'
    assert recorded['rendered_summary'].instrumentation_name == 'coverage'


def test_bootstrap_coverage_warns_when_summary_is_not_persisted(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_run(command, *, check, env):
        del command, check
        metadata_path = env['COSECHA_INSTRUMENTATION_METADATA_FILE']
        with open(metadata_path, 'w', encoding='utf-8') as handle:
            handle.write(
                '{"knowledge_base_path": "/tmp/kb.db", "session_id": "session-1"}',
            )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr('cosecha.shell.launcher.subprocess.run', fake_run)
    monkeypatch.setattr(
        'cosecha.shell.launcher._update_session_artifact',
        lambda metadata, *, summary: (None, 'session artifact not found'),
    )
    monkeypatch.setattr(
        'cosecha.shell.launcher._render_coverage_summary',
        lambda summary, *, config_snapshot: pytest.fail(
            'coverage should not render when persistence fails',
        ),
    )
    monkeypatch.setattr(
        'cosecha.plugin.coverage.CoverageInstrumenter.collect',
        lambda self, *, workdir: SimpleNamespace(
            instrumentation_name='coverage',
            payload={'total_coverage': 87.5},
        ),
    )

    exit_code = _bootstrap_coverage(['run', '--cov', 'src/demo'])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert 'coverage was collected but not persisted' in captured.out


def test_launcher_main_delegates_to_runner_cli_when_no_launcher_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'cosecha.shell.launcher._run_runner_cli',
        lambda argv: 7 if tuple(argv) == ('run', '--path', 'tests/unit') else 3,
    )

    with pytest.raises(SystemExit) as error:
        main(['run', '--path', 'tests/unit'])

    assert error.value.code == 7


def test_update_session_artifact_retries_when_artifact_is_not_visible_yet(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    calls = {'count': 0}
    summary = InstrumentationSummary(
        instrumentation_name='coverage',
        summary_kind='coverage.py',
        payload={'total_coverage': 87.5},
    )
    artifact = SessionArtifact(
        session_id='session-1',
        trace_id='trace-1',
        root_path=str(tmp_path),
        plan_id=None,
        config_snapshot=ConfigSnapshot(
            root_path=str(tmp_path),
            output_mode='summary',
            output_detail='standard',
            capture_log=True,
            stop_on_error=False,
            concurrency=1,
            strict_step_ambiguity=False,
        ),
        capability_snapshots=(),
        plan_explanation=None,
        timing=SessionTimingSnapshot(),
        has_failures=False,
        report_summary=SessionReportSummary(
            total_tests=1,
            status_counts=(('passed', 1),),
            failure_kind_counts=(),
            engine_summaries=(),
            live_engine_snapshots=(),
            failed_examples=(),
            failed_files=(),
            instrumentation_summaries={},
        ),
        telemetry_summary=SessionTelemetrySummary(
            span_count=0,
            distinct_span_names=0,
        ),
        persistence_policy=SessionArtifactPersistencePolicy(),
        recorded_at=0.0,
    )

    class _FakeKnowledgeBase:
        def query_session_artifacts(self, query) -> list[SessionArtifact]:
            del query
            calls['count'] += 1
            if calls['count'] < 2:
                return []
            return [artifact]

        def store_session_artifact(self, updated_artifact) -> None:
            calls['stored'] = updated_artifact

        def close(self) -> None:
            calls['closed'] = calls.get('closed', 0) + 1

    monkeypatch.setattr(
        'cosecha.shell.launcher._open_knowledge_base',
        lambda db_path: _FakeKnowledgeBase(),
    )
    monkeypatch.setattr(
        'cosecha.shell.launcher.time.sleep',
        lambda seconds: calls.setdefault('sleeps', []).append(seconds),
    )

    updated_artifact, warning = _update_session_artifact(
        {
            'knowledge_base_path': str(tmp_path / 'kb.db'),
            'session_id': 'session-1',
        },
        summary=summary,
    )

    assert warning is None
    assert updated_artifact is not None
    assert calls['count'] == 2
    assert calls['sleeps'] == [0.05]
    assert (
        calls['stored'].report_summary.instrumentation_summaries['coverage']
        == summary
    )


def test_update_session_artifact_retries_on_operational_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    calls = {'count': 0}

    class _FakeKnowledgeBase:
        def query_session_artifacts(self, query) -> list[object]:
            del query
            return []

        def close(self) -> None:
            return None

    def fake_open(db_path):
        del db_path
        calls['count'] += 1
        if calls['count'] < 3:
            raise sqlite3.OperationalError('database is locked')
        return _FakeKnowledgeBase()

    monkeypatch.setattr('cosecha.shell.launcher._open_knowledge_base', fake_open)
    monkeypatch.setattr(
        'cosecha.shell.launcher.time.sleep',
        lambda seconds: calls.setdefault('sleeps', []).append(seconds),
    )

    updated_artifact, warning = _update_session_artifact(
        {
            'knowledge_base_path': str(tmp_path / 'kb.db'),
            'session_id': 'session-1',
        },
        summary=InstrumentationSummary(
            instrumentation_name='coverage',
            summary_kind='coverage.py',
            payload={'total_coverage': 87.5},
        ),
    )

    assert updated_artifact is None
    assert warning == 'session artifact not found for session-1'
    assert calls['count'] == 3
    assert calls['sleeps'] == [0.05, 0.05]


def test_launcher_main_uses_internal_bootstrap_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'cosecha.shell.launcher._iter_bootstrap_handlers',
        lambda argv: (
            (
                lambda received: received == argv,
                lambda received: 11 if received == argv else 3,
            ),
        ),
    )

    with pytest.raises(SystemExit) as error:
        main(['run', '--cov', 'src/demo'])

    assert error.value.code == 11
