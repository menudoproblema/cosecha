from __future__ import annotations

import runpy
import sys

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
    assert '--parallel-mode' in contribution.argv_prefix
    assert '--branch' in contribution.argv_prefix
    assert '--source=src/demo' in contribution.argv_prefix
    assert contribution.argv_prefix[-2:] == ('-m', 'cosecha.shell.runner_cli')


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

    summary = instrumenter.collect(workdir=tmp_path)

    assert summary.instrumentation_name == 'coverage'
    assert summary.summary_kind == 'coverage.py'
    assert summary.payload['report_type'] == 'term-missing'
    assert summary.payload['total_coverage'] > 0.0
    assert str(source_path) in summary.payload['source_targets']
