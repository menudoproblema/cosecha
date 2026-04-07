from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.operations import RunOperationResult
from cosecha.core.runner import RunnerRuntimeError
from cosecha.shell import runner_cli


def test_parse_args_without_arguments_exits_with_usage_error() -> None:
    with pytest.raises(SystemExit, match='2'):
        runner_cli.parse_args([])


def test_parse_args_accepts_run_without_selection_flags(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)

    request = runner_cli.parse_args(['run'])

    assert isinstance(request, runner_cli.RunCliRequest)
    assert request.context.selection.include_paths == ()
    assert request.context.selection.exclude_paths == ()


def test_parse_args_accepts_manifest_show_without_preloading_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    manifest_path = tmp_path / 'cosecha.toml'
    manifest_path.write_text('[manifest]\nschema_version = 1\n', encoding='utf-8')
    monkeypatch.chdir(tmp_path)

    request = runner_cli.parse_args(['manifest', 'show'])

    assert isinstance(request, runner_cli.ManifestShowCliRequest)
    assert request.manifest_file is None


def _build_run_request() -> runner_cli.RunCliRequest:
    return runner_cli.RunCliRequest(
        context=runner_cli.RuntimeCliContext(
            args=SimpleNamespace(),
            config=SimpleNamespace(),
            plugins=(),
            runtime_provider=None,
            selection=runner_cli.CliSelection(),
        ),
    )


def _install_dummy_runner(
    monkeypatch: pytest.MonkeyPatch,
    *,
    result: RunOperationResult,
) -> None:
    class DummyRunner:
        def __init__(self, *_args, **_kwargs) -> None:
            return None

        async def execute_operation(self, operation):
            del operation
            return result

    monkeypatch.setattr(runner_cli, 'Runner', DummyRunner)


def test_execute_runtime_request_exits_with_code_1_when_run_has_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_dummy_runner(
        monkeypatch,
        result=RunOperationResult(has_failures=True, total_tests=3),
    )

    with pytest.raises(SystemExit, match='1'):
        runner_cli._execute_runtime_request(_build_run_request())


def test_execute_runtime_request_exits_with_code_5_when_no_tests_collected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_dummy_runner(
        monkeypatch,
        result=RunOperationResult(has_failures=False, total_tests=0),
    )

    with pytest.raises(SystemExit, match='5'):
        runner_cli._execute_runtime_request(_build_run_request())


def test_execute_runtime_request_exits_cleanly_when_tests_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_dummy_runner(
        monkeypatch,
        result=RunOperationResult(has_failures=False, total_tests=7),
    )

    # No SystemExit: a clean passing run just returns.
    runner_cli._execute_runtime_request(_build_run_request())


def test_execute_runtime_request_prefers_failure_exit_over_empty_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Collection errors surface as has_failures=True + total_tests=0:
    # exit 1 must win over exit 5 so the failure is not hidden.
    _install_dummy_runner(
        monkeypatch,
        result=RunOperationResult(has_failures=True, total_tests=0),
    )

    with pytest.raises(SystemExit, match='1'):
        runner_cli._execute_runtime_request(_build_run_request())


def test_main_exits_with_usage_error_for_value_errors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        runner_cli,
        'parse_args',
        lambda: (_ for _ in ()).throw(ValueError('invalid input')),
    )

    with pytest.raises(SystemExit, match='2'):
        runner_cli.main()

    assert 'invalid input' in capsys.readouterr().out


def test_main_exits_with_runtime_error_for_runner_runtime_errors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        runner_cli,
        'parse_args',
        lambda: SimpleNamespace(),
    )
    monkeypatch.setattr(
        runner_cli,
        '_execute_non_runtime_request',
        lambda request: False,
    )
    monkeypatch.setattr(
        runner_cli,
        '_execute_runtime_request',
        lambda request: (_ for _ in ()).throw(
            RunnerRuntimeError('runtime exploded'),
        ),
    )

    with pytest.raises(SystemExit, match='3'):
        runner_cli.main()

    assert 'runtime exploded' in capsys.readouterr().out
