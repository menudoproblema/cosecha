from __future__ import annotations

import sys

from types import SimpleNamespace

import pytest

from cosecha.core.config import ConfigSnapshot
from cosecha.shell.launcher import (
    _bootstrap_coverage,
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
    assert '--parallel-mode' in recorded['command']
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
