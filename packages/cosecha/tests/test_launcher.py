from __future__ import annotations

import os

from types import SimpleNamespace

import pytest

from cosecha.shell.launcher import (
    CoverageLauncher,
    _ACTIVE_EXECUTION_LAUNCHER_ENV,
    _parse_coverage_launch_spec,
    main,
)


def test_parse_coverage_launch_spec_collects_sources_and_branch() -> None:
    spec = _parse_coverage_launch_spec(
        (
            'run',
            '--cov',
            'src/one,src/two',
            '--cov=src/three',
            '--cov-branch',
        ),
    )

    assert spec is not None
    assert spec.source_targets == ('src/one', 'src/two', 'src/three')
    assert spec.branch is True


def test_coverage_launcher_matches_only_run_commands_with_cov(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = CoverageLauncher()

    monkeypatch.delenv(_ACTIVE_EXECUTION_LAUNCHER_ENV, raising=False)
    assert launcher.matches(('run', '--cov', 'src/demo')) is True
    assert launcher.matches(('plan', '--cov', 'src/demo')) is False
    assert launcher.matches(('run', '--path', 'tests/unit')) is False

    monkeypatch.setenv(_ACTIVE_EXECUTION_LAUNCHER_ENV, 'coverage')
    assert launcher.matches(('run', '--cov', 'src/demo')) is False


def test_coverage_launcher_reexecutes_under_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = CoverageLauncher()
    recorded: dict[str, object] = {}

    def fake_run(command, *, check, env):
        recorded['command'] = command
        recorded['check'] = check
        recorded['env'] = env
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr('cosecha.shell.launcher.subprocess.run', fake_run)

    exit_code = launcher.launch(
        ('run', '--cov', 'src/demo', '--cov-branch', '--path', 'tests/unit'),
    )

    assert exit_code == 0
    assert recorded['command'][0:5] == [
        os.sys.executable,
        '-m',
        'coverage',
        'run',
        recorded['command'][4],
    ]
    assert recorded['command'][5:8] == [
        '-m',
        'cosecha.shell.runner_cli',
        'run',
    ]
    assert recorded['env'][_ACTIVE_EXECUTION_LAUNCHER_ENV] == 'coverage'
    assert recorded['env']['COVERAGE_PROCESS_START'].endswith('.coveragerc')


def test_launcher_main_delegates_to_runner_cli_when_no_launcher_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'cosecha.shell.launcher._iter_execution_launchers',
        lambda: (),
    )
    monkeypatch.setattr(
        'cosecha.shell.launcher._run_runner_cli',
        lambda argv: 7 if tuple(argv) == ('run', '--path', 'tests/unit') else 3,
    )

    with pytest.raises(SystemExit) as error:
        main(['run', '--path', 'tests/unit'])

    assert error.value.code == 7
