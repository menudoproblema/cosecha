from __future__ import annotations

import argparse
import runpy

import pytest

from cosecha.core.session_artifacts import SessionReportState
from cosecha.plugin.coverage import CoveragePlugin
from cosecha_internal.testkit import build_config, build_plugin_context


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
