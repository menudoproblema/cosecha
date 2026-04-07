from __future__ import annotations

import os
import runpy
import subprocess
import sys

from pathlib import Path
from types import SimpleNamespace

import coverage

from cosecha.plugin.coverage import (
    CoverageInstrumenter,
    CoverageRequest,
    parse_coverage_request,
)


def test_parse_coverage_request_collects_sources_branch_and_report_type(
) -> None:
    request = parse_coverage_request(
        (
            'run',
            '--cov',
            'src/one,src/two',
            '--cov=src/three',
            '--cov-branch',
            '--cov-report',
            'term-missing',
        ),
    )

    assert request == CoverageRequest(
        source_targets=('src/one', 'src/two', 'src/three'),
        branch=True,
        report_type='term-missing',
    )


def test_instrumenter_strips_only_coverage_bootstrap_flags() -> None:
    instrumenter = CoverageInstrumenter(
        CoverageRequest(source_targets=('src/demo',)),
    )

    assert instrumenter.strip_bootstrap_options(
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
    assert instrumenter.strip_bootstrap_options(
        ['run', '--cov=src/demo', '--path', 'tests/unit'],
    ) == ['run', '--path', 'tests/unit']


def test_prepare_builds_coverage_run_prefix(tmp_path) -> None:
    instrumenter = CoverageInstrumenter(
        CoverageRequest(source_targets=('src/demo',), branch=True),
    )

    contribution = instrumenter.prepare(workdir=tmp_path)

    assert contribution.argv_prefix[0:4] == (
        sys.executable,
        '-m',
        'coverage',
        'run',
    )
    assert contribution.env['COVERAGE_PROCESS_START'].endswith(
        '.cosecha.coveragerc',
    )
    assert '--rcfile=' in contribution.argv_prefix[4]
    assert contribution.argv_prefix[-2:] == ('-m', 'cosecha.shell.runner_cli')
    assert '.cosecha.coveragerc' in contribution.workdir_files
    assert 'patch = subprocess' in contribution.workdir_files[
        '.cosecha.coveragerc'
    ]
    assert 'source =' in contribution.workdir_files['.cosecha.coveragerc']


def test_collect_builds_instrumentation_summary_from_parallel_data(
    tmp_path,
) -> None:
    source_path = tmp_path / 'demo_pkg'
    source_path.mkdir()
    module_path = source_path / 'demo_module.py'
    module_path.write_text('VALUE = 1\nRESULT = VALUE + 1\n', encoding='utf-8')

    data_file = tmp_path / '.coverage'
    cov = coverage.Coverage(
        data_file=str(data_file),
        data_suffix=True,
        source=[str(source_path)],
    )
    cov.start()
    runpy.run_path(str(module_path))
    cov.stop()
    cov.save()

    instrumenter = CoverageInstrumenter(
        CoverageRequest(
            source_targets=(str(source_path),),
            report_type='term-missing',
        ),
    )
    contribution = instrumenter.prepare(workdir=tmp_path)
    for relative_path, contents in contribution.workdir_files.items():
        (tmp_path / relative_path).write_text(contents, encoding='utf-8')

    summary = instrumenter.collect(workdir=tmp_path)

    assert summary.instrumentation_name == 'coverage'
    assert summary.summary_kind == 'coverage.py'
    assert summary.payload['report_type'] == 'term-missing'
    assert summary.payload['total_coverage'] > 0.0
    assert str(source_path) in summary.payload['source_targets']


def test_collect_respects_branch_from_user_config_when_flag_not_passed(
    tmp_path,
    monkeypatch,
) -> None:
    recorded_kwargs: dict[str, object] = {}

    class _FakeCoverage:
        def __init__(self, **kwargs) -> None:
            recorded_kwargs.update(kwargs)
            self.config = type(
                '_Config',
                (),
                {
                    'branch': True,
                    'source': (str(tmp_path / 'demo_pkg'),),
                },
            )()

        def combine(self, *, data_paths) -> None:
            assert data_paths == [str(tmp_path)]

        def save(self) -> None:
            return None

        def report(self, **kwargs) -> float:
            del kwargs
            return 88.0

    monkeypatch.setattr(
        'cosecha.plugin.coverage._coverage_module',
        lambda: SimpleNamespace(Coverage=_FakeCoverage),
    )

    instrumenter = CoverageInstrumenter(
        CoverageRequest(source_targets=(str(tmp_path / 'demo_pkg'),)),
    )

    summary = instrumenter.collect(workdir=tmp_path)

    assert 'branch' not in recorded_kwargs
    assert summary.payload['branch'] is True


def test_prepare_instruments_python_subprocesses_with_process_start(
    tmp_path,
) -> None:
    package_path = tmp_path / 'demo_pkg'
    package_path.mkdir()
    (package_path / '__init__.py').write_text('', encoding='utf-8')
    child_path = package_path / 'child.py'
    child_path.write_text(
        'VALUE = 1\nRESULT = VALUE + 1\n',
        encoding='utf-8',
    )
    parent_path = tmp_path / 'parent.py'
    parent_path.write_text(
        '\n'.join(
            (
                'import subprocess',
                'import sys',
                '',
                'subprocess.run(',
                "    [sys.executable, '-m', 'demo_pkg.child'],",
                '    check=True,',
                ')',
            ),
        )
        + '\n',
        encoding='utf-8',
    )

    instrumenter = CoverageInstrumenter(
        CoverageRequest(source_targets=(str(package_path),)),
    )
    contribution = instrumenter.prepare(workdir=tmp_path)
    for relative_path, contents in contribution.workdir_files.items():
        (tmp_path / relative_path).write_text(contents, encoding='utf-8')

    env = os.environ.copy()
    env.update(contribution.env)
    subprocess.run(  # noqa: S603
        [
            sys.executable,
            '-m',
            'coverage',
            'run',
            f'--rcfile={tmp_path / ".cosecha.coveragerc"}',
            str(parent_path),
        ],
        check=True,
        cwd=tmp_path,
        env=env,
    )

    summary = instrumenter.collect(workdir=tmp_path)

    coverage_runtime = coverage.Coverage(
        config_file=str(tmp_path / '.cosecha.coveragerc'),
        data_file=str(tmp_path / '.coverage'),
    )
    coverage_runtime.load()
    measured_files = {
        Path(measured_file).resolve()
        for measured_file in coverage_runtime.get_data().measured_files()
    }

    assert child_path.resolve() in measured_files
    assert summary.payload['includes_python_subprocesses'] is True
    assert summary.payload['includes_worker_processes'] is False
