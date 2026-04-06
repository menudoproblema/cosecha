from __future__ import annotations

import argparse

import pytest

from cosecha.core.output import OutputMode
from cosecha.core.session_timing import SessionTiming, TestTiming
from cosecha.plugin.timing import TimingPlugin
from cosecha_internal.testkit import (
    NullTelemetryStream,
    build_config,
    build_plugin_context,
)


TIMING_FINISH_PRIORITY = 100


def test_timing_plugin_parse_args_and_priority() -> None:
    parser = argparse.ArgumentParser()
    TimingPlugin.register_arguments(parser)

    plugin = TimingPlugin.parse_args(parser.parse_args(['--timing']))

    assert isinstance(plugin, TimingPlugin)
    assert TimingPlugin.finish_priority() == TIMING_FINISH_PRIORITY


@pytest.mark.asyncio
async def test_timing_plugin_prints_detailed_report_after_session_closed(
    tmp_path,
) -> None:
    session_timing = SessionTiming(
        session_start=0.0,
        collect_start=0.0,
        collect_end=1.0,
        run_end=3.0,
        shutdown_start=3.0,
        shutdown_end=3.5,
        session_end=3.5,
        tests=[
            TestTiming(
                name='tests/test_demo.py::test_case',
                duration=1.5,
                phases={'execute': 1.5},
            ),
        ],
        collect_phases={'pytest': {'discover': 1.0}},
        session_phases={'pytest': {'execute': 2.0}},
        shutdown_phases={'resources': 0.5},
    )
    telemetry_stream = NullTelemetryStream()
    plugin = TimingPlugin()
    config = build_config(tmp_path, output_mode=OutputMode.DEBUG)
    await plugin.initialize(
        build_plugin_context(
            config,
            telemetry_stream=telemetry_stream,
            session_timing=session_timing,
        ),
    )

    await plugin.after_session_closed()

    assert telemetry_stream.spans == [
        (
            'plugin.timing.print_report',
            {'cosecha.plugin.name': 'TimingPlugin'},
        ),
    ]
    title, text = config.console.summaries[0]
    assert title == 'Timing'
    assert 'Collection:  1.000s' in text
    assert 'Collection phases:' in text
    assert 'Session phases:' in text
    assert 'Shutdown phases:' in text
    assert 'Per test:' in text
