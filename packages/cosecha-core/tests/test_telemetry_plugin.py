from __future__ import annotations

import argparse
import asyncio
import json

from typing import TYPE_CHECKING

from cosecha.core.plugins.telemetry import TelemetryPlugin
from cosecha.core.telemetry import JsonlTelemetrySink, TelemetryStream
from cosecha_internal.testkit import build_config, build_plugin_context


if TYPE_CHECKING:
    from pathlib import Path


def test_telemetry_plugin_parse_args_returns_instance() -> None:
    parser = argparse.ArgumentParser()
    TelemetryPlugin.register_arguments(parser)

    plugin = TelemetryPlugin.parse_args(
        parser.parse_args(['--telemetry-jsonl', 'trace.jsonl']),
    )

    assert isinstance(plugin, TelemetryPlugin)


def test_jsonl_telemetry_sink_writes_spans(tmp_path: Path) -> None:
    sink = JsonlTelemetrySink(tmp_path / 'telemetry.jsonl')
    stream = TelemetryStream()
    stream.add_sink(sink)

    async def _run() -> None:
        await sink.start()
        async with stream.span(
            'demo.operation',
            attributes={'kind': 'unit'},
        ):
            pass
        await stream.close()

    asyncio.run(_run())

    lines = (
        (tmp_path / 'telemetry.jsonl')
        .read_text(encoding='utf-8')
        .splitlines()
    )
    assert len(lines) == 1

    payload = json.loads(lines[0])
    assert payload['name'] == 'demo.operation'
    assert payload['attributes'] == {'kind': 'unit'}
    assert payload['duration'] >= 0.0


def test_telemetry_stream_summary_tracks_span_counts(tmp_path: Path) -> None:
    sink = JsonlTelemetrySink(tmp_path / 'telemetry-summary.jsonl')
    stream = TelemetryStream()
    stream.add_sink(sink)

    async def _run() -> None:
        await sink.start()
        async with stream.span('collect'):
            pass
        async with stream.span('collect'):
            pass
        async with stream.span('run'):
            pass
        await stream.close()

    asyncio.run(_run())

    assert stream.summary() == {
        'distinct_span_names': 2,
        'span_count': 3,
        'top_span_names': (('collect', 2), ('run', 1)),
    }


def test_telemetry_plugin_start_opens_sink_and_emits_span(
    tmp_path: Path,
) -> None:
    telemetry_path = tmp_path / 'plugin-telemetry.jsonl'
    plugin = TelemetryPlugin(telemetry_path)
    telemetry_stream = TelemetryStream()

    async def _run() -> None:
        await plugin.initialize(
            build_plugin_context(
                build_config(tmp_path),
                telemetry_stream=telemetry_stream,
            ),
        )
        await plugin.start()
        await telemetry_stream.close()

    asyncio.run(_run())

    lines = telemetry_path.read_text(encoding='utf-8').splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload['name'] == 'plugin.telemetry.sink.start'
    assert payload['attributes'] == {
        'cosecha.plugin.name': 'TelemetryPlugin',
    }
