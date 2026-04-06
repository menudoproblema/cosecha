from __future__ import annotations

import os
import subprocess
import sys
import tempfile

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence


_ACTIVE_EXECUTION_LAUNCHER_ENV = 'COSECHA_ACTIVE_EXECUTION_LAUNCHER'


class ExecutionLauncher(Protocol):
    launcher_name: str

    def matches(self, argv: Sequence[str]) -> bool: ...

    def launch(self, argv: Sequence[str]) -> int: ...


@dataclass(slots=True, frozen=True)
class CoverageLaunchSpec:
    source_targets: tuple[str, ...]
    branch: bool = False

    def build_rcfile_content(self) -> str:
        source_lines = '\n'.join(
            f'    {source_target}'
            for source_target in self.source_targets
        )
        branch_value = 'True' if self.branch else 'False'
        return '\n'.join(
            (
                '[run]',
                f'branch = {branch_value}',
                'parallel = True',
                'patch = subprocess',
                'source =',
                source_lines,
                '',
            ),
        )


class CoverageLauncher:
    launcher_name = 'coverage'

    def matches(self, argv: Sequence[str]) -> bool:
        if os.environ.get(_ACTIVE_EXECUTION_LAUNCHER_ENV) == self.launcher_name:
            return False
        if not argv or argv[0] != 'run':
            return False
        return _extract_option_values(argv, '--cov') != ()

    def launch(self, argv: Sequence[str]) -> int:
        spec = _parse_coverage_launch_spec(argv)
        if spec is None:
            msg = 'CoverageLauncher requires at least one --cov target'
            raise ValueError(msg)

        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.coveragerc',
            encoding='utf-8',
            delete=False,
        ) as rcfile:
            rcfile.write(spec.build_rcfile_content())
            rcfile_path = Path(rcfile.name)

        try:
            command = [
                sys.executable,
                '-m',
                'coverage',
                'run',
                f'--rcfile={rcfile_path}',
                '-m',
                'cosecha.shell.runner_cli',
                *argv,
            ]
            env = os.environ.copy()
            env[_ACTIVE_EXECUTION_LAUNCHER_ENV] = self.launcher_name
            env['COVERAGE_PROCESS_START'] = str(rcfile_path)
            completed = subprocess.run(  # noqa: S603
                command,
                check=False,
                env=env,
            )
        finally:
            rcfile_path.unlink(missing_ok=True)

        return completed.returncode


def _extract_option_values(
    argv: Sequence[str],
    option_name: str,
) -> tuple[str, ...]:
    values: list[str] = []
    iterator = iter(range(len(argv)))
    for index in iterator:
        argument = argv[index]
        prefix = f'{option_name}='
        if argument.startswith(prefix):
            values.append(argument.removeprefix(prefix))
            continue
        if argument != option_name:
            continue
        next_index = index + 1
        if next_index >= len(argv):
            break
        values.append(argv[next_index])
        next(iterator, None)
    return tuple(values)


def _parse_coverage_launch_spec(
    argv: Sequence[str],
) -> CoverageLaunchSpec | None:
    source_targets = tuple(
        target.strip()
        for raw_value in _extract_option_values(argv, '--cov')
        for target in raw_value.split(',')
        if target.strip()
    )
    if not source_targets:
        return None
    return CoverageLaunchSpec(
        source_targets=source_targets,
        branch='--cov-branch' in argv,
    )


def _iter_execution_launchers() -> tuple[ExecutionLauncher, ...]:
    return (CoverageLauncher(),)


def _run_runner_cli(argv: Sequence[str]) -> int:
    from cosecha.shell import runner_cli

    runner_cli.main(list(argv))
    return 0


def main(argv: list[str] | None = None) -> None:
    argv_list = list(sys.argv[1:] if argv is None else argv)
    for launcher in _iter_execution_launchers():
        if not launcher.matches(argv_list):
            continue
        raise SystemExit(launcher.launch(argv_list))

    raise SystemExit(_run_runner_cli(argv_list))
