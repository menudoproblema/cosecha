from __future__ import annotations

import argparse
import runpy

import pytest

from cosecha.core.session_artifacts import SessionReportState
from cosecha.plugin.coverage import CoveragePlugin
from cosecha_internal.testkit import build_config, build_plugin_context


EXTERNAL_COVERAGE_PERCENT = 87.5


def test_coverage_plugin_parse_args_returns_plugin_when_cov_present() -> None:
    parser = argparse.ArgumentParser()
    CoveragePlugin.register_arguments(parser)

    plugin = CoveragePlugin.parse_args(
        parser.parse_args(['--cov', 'src/demo_pkg', '--cov-report', 'term']),
    )

    assert isinstance(plugin, CoveragePlugin)
    assert plugin.report_type == 'term'


@pytest.mark.asyncio
async def test_coverage_plugin_finish_persists_summary_into_report_state(
    tmp_path,
) -> None:
    source_path = tmp_path / 'demo_pkg'
    source_path.mkdir()
    module_path = source_path / 'demo_module.py'
    module_path.write_text('VALUE = 1\nRESULT = VALUE + 1\n', encoding='utf-8')

    plugin = CoveragePlugin(
        source=[str(source_path)],
        report_type='term-missing',
    )
    config = build_config(tmp_path)
    report_state = SessionReportState()
    await plugin.initialize(
        build_plugin_context(
            config,
            engine_names=('pytest',),
            session_report_state=report_state,
        ),
    )

    await plugin.start()
    runpy.run_path(str(module_path))
    await plugin.finish()

    assert report_state.coverage_summary is not None
    assert report_state.coverage_summary.report_type == 'term-missing'
    assert report_state.coverage_summary.engine_names == ('pytest',)
    assert str(source_path) in report_state.coverage_summary.source_targets
    assert report_state.coverage_summary.total_coverage > 0.0
    assert config.console.summaries == []


@pytest.mark.asyncio
async def test_coverage_plugin_prints_summary_without_report_state(
    tmp_path,
) -> None:
    source_path = tmp_path / 'demo_pkg'
    source_path.mkdir()
    module_path = source_path / 'demo_module.py'
    module_path.write_text('VALUE = 3\nRESULT = VALUE * 2\n', encoding='utf-8')

    plugin = CoveragePlugin(source=[str(source_path)])
    config = build_config(tmp_path)
    await plugin.initialize(build_plugin_context(config))

    await plugin.start()
    runpy.run_path(str(module_path))
    await plugin.finish()

    assert config.console.summaries
    assert config.console.summaries[0][0] == 'Coverage'


@pytest.mark.asyncio
async def test_coverage_plugin_reuses_active_launcher_coverage(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeCoverage:
        def __init__(self) -> None:
            self.started = 0
            self.stopped = 0
            self.saved = 0
            self.report_calls = 0
            self.config = type(
                '_Config',
                (),
                {'branch': False, 'source': (str(tmp_path / 'demo_pkg'),)},
            )()

        def start(self) -> None:
            self.started += 1

        def stop(self) -> None:
            self.stopped += 1

        def save(self) -> None:
            self.saved += 1

        def report(self, **kwargs) -> float:
            del kwargs
            self.report_calls += 1
            return EXTERNAL_COVERAGE_PERCENT

    fake_coverage = _FakeCoverage()
    plugin = CoveragePlugin(source=[str(tmp_path / 'demo_pkg')])
    config = build_config(tmp_path)
    report_state = SessionReportState()

    monkeypatch.setenv('COSECHA_ACTIVE_EXECUTION_LAUNCHER', 'coverage')
    monkeypatch.setattr(
        'cosecha.plugin.coverage.coverage.Coverage.current',
        lambda: fake_coverage,
    )

    await plugin.initialize(
        build_plugin_context(
            config,
            engine_names=('pytest',),
            session_report_state=report_state,
        ),
    )
    await plugin.start()
    await plugin.finish()

    assert plugin.cov is fake_coverage
    assert fake_coverage.started == 0
    assert fake_coverage.stopped == 0
    assert fake_coverage.saved == 0
    assert fake_coverage.report_calls == 1
    assert report_state.coverage_summary is not None
    assert (
        report_state.coverage_summary.total_coverage
        == EXTERNAL_COVERAGE_PERCENT
    )
