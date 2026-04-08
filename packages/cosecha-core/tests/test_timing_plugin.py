from __future__ import annotations

import argparse
import asyncio

from types import SimpleNamespace

from cosecha.core.output import OutputMode
from cosecha.core.plugins.timing import TimingPlugin
from cosecha.core.session_timing import SessionTiming, TestTiming
from cosecha_internal.testkit import build_config, build_plugin_context


def test_timing_plugin_parse_args_and_describe_capabilities() -> None:
    parser = argparse.ArgumentParser()
    TimingPlugin.register_arguments(parser)

    disabled = TimingPlugin.parse_args(parser.parse_args([]))
    enabled = TimingPlugin.parse_args(parser.parse_args(['--timing']))

    assert disabled is None
    assert isinstance(enabled, TimingPlugin)
    assert TimingPlugin.finish_priority() == 100
    assert enabled.describe_capabilities()[0].name == 'timing_summary'


def test_timing_plugin_prints_report_with_detailed_sections(tmp_path) -> None:
    config = build_config(tmp_path, output_mode=OutputMode.DEBUG)
    session_timing = SessionTiming(
        collect_start=1.0,
        collect_end=2.0,
        session_start=2.0,
        run_end=5.0,
        shutdown_start=5.0,
        shutdown_end=6.0,
        session_end=6.5,
        tests=(
            [
                TestTiming('slow', 1.2, phases={'setup': 0.2, 'run': 1.0}),
                TestTiming('fast', 0.4, phases={'setup': 0.1, 'run': 0.3}),
            ]
        ),
    )
    session_timing.collect_phases = {'gherkin': {'scan': 0.3}}
    session_timing.session_phases = {'gherkin': {'bootstrap': 0.2}}
    session_timing.shutdown_phases = {'close_runtime': 0.1}
    plugin = TimingPlugin()
    context = build_plugin_context(
        config,
        session_timing=session_timing,
    )

    asyncio.run(plugin.initialize(context))
    asyncio.run(plugin.after_session_closed())

    assert config.console.summaries
    title, report = config.console.summaries[-1]
    assert title == 'Timing'
    assert 'Collection phases:' in report
    assert 'Session phases:' in report
    assert 'Shutdown phases:' in report
    assert 'Test phases:' in report
    assert 'Per test:' in report


def test_timing_plugin_handles_missing_timing_data(tmp_path) -> None:
    config = build_config(tmp_path, output_mode=OutputMode.DEBUG)
    plugin = TimingPlugin()
    context = build_plugin_context(config)
    plugin.context = SimpleNamespace(
        session_timing=None,
        telemetry_stream=context.telemetry_stream,
    )
    plugin.config = config

    plugin._print_timing_report()

    assert config.console.summaries[-1] == ('Timing', 'No timing data available.')
