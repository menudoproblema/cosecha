from __future__ import annotations

import argparse
import asyncio
import io
import sys

from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core import domain_events
from cosecha.core.console import Console
from cosecha.core.cxp_adapters import build_cxp_plugin_component_snapshot
from cosecha.core.extensions import (
    ExtensionCompatibilityConstraint,
    ExtensionComponentSnapshot,
    ExtensionDescriptor,
    ExtensionQuery,
    build_plugin_extension_snapshot,
)
from cosecha.core.items import TestItem, TestResultStatus, normalize_failure_kind, resolve_failure_kind
from cosecha.core.output import OutputMode
from cosecha.core.plugins.base import PlanMiddleware, Plugin
from cosecha.core.plugins.telemetry import TelemetryPlugin
from cosecha.core.plugins.timing import TimingPlugin
from cosecha.core.reporter import NullReporter, QueuedReporter, Reporter
from cosecha.core.reporting_coordinator import ReportingCoordinator
from cosecha.core.reporting_ir import (
    LocationReport,
    TestReport,
    _location_report_from_dict,
    _location_report_to_dict,
    ensure_test_report,
    reconcile_test_report,
)
from cosecha.core.runtime_protocol import (
    RuntimeBootstrapResponse,
    RuntimeProtocolError,
    serialize_resource_lifecycle_event,
)
from cosecha.core.serialization import to_builtins_list
from cosecha.core.telemetry import (
    InMemoryTelemetrySink,
    JsonlTelemetrySink,
    TelemetrySpan,
    TelemetryStream,
)
from cosecha.core.utils import (
    _discover_import_search_paths_legacy,
    _discover_workspace_site_packages,
)


class _UnknownCapabilityPlugin(Plugin):
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args: argparse.Namespace):
        del args
        return cls()

    def describe_capabilities(self):
        from cosecha.core.capabilities import CapabilityDescriptor

        return (
            CapabilityDescriptor(name='unknown_plugin_capability', level='supported'),
        )

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


class _RequiredCapabilityPlugin(Plugin):
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args: argparse.Namespace):
        del args
        return cls()

    @classmethod
    def required_capabilities(cls) -> tuple[str, ...]:
        return ('runtime.capability',)

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


class _PlanPlugin(PlanMiddleware):
    @classmethod
    def register_arguments(cls, parser: argparse.ArgumentParser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args: argparse.Namespace):
        del args
        return cls()

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


class _RecorderReporter(Reporter):
    def __init__(self) -> None:
        self.bound_telemetry_stream = None
        self.add_test_calls = 0
        self.add_test_result_calls = 0
        self.finish_calls = 0
        self.print_report_calls = 0

    def bind_telemetry_stream(self, telemetry_stream) -> None:
        super().bind_telemetry_stream(telemetry_stream)
        self.bound_telemetry_stream = telemetry_stream

    async def start(self):
        return None

    async def finish(self):
        self.finish_calls += 1

    async def add_test(self, test):
        del test
        self.add_test_calls += 1

    async def add_test_result(self, test):
        del test
        self.add_test_result_calls += 1

    async def print_report(self):
        self.print_report_calls += 1


class _DummyTestItem(TestItem):
    async def run(self, context) -> None:
        del context

    def has_selection_label(self, name: str) -> bool:
        del name
        return False


def test_small_sync_branches_cover_console_items_and_serialization() -> None:
    stream = io.StringIO()
    console = Console(output_mode=OutputMode.DEBUG, file=stream)
    console.info('hello-info')
    assert 'hello-info' in stream.getvalue()

    class _FailureError(Exception):
        failure_kind = 'hook'

    assert normalize_failure_kind('runtime') == 'runtime'
    assert resolve_failure_kind(_FailureError()) == 'hook'
    assert to_builtins_list([1, 2, 3]) == [1, 2, 3]


def test_domain_events_and_runtime_protocol_small_paths() -> None:
    test_started = domain_events.TestStartedEvent(
        node_id='node-1',
        node_stable_id='stable-1',
        engine_name='gherkin',
        test_name='Scenario',
        test_path='features/demo.feature',
    )
    restored_test_started = domain_events.deserialize_domain_event(
        domain_events.serialize_domain_event(test_started),
    )
    assert restored_test_started == test_started

    step_started = domain_events.StepStartedEvent(
        node_id='node-1',
        node_stable_id='stable-1',
        engine_name='gherkin',
        test_name='Scenario',
        test_path='features/demo.feature',
        step_type='given',
        step_keyword='Given ',
        step_text='a step',
    )
    restored_step_started = domain_events.deserialize_domain_event(
        domain_events.serialize_domain_event(step_started),
    )
    assert restored_step_started == step_started

    assert RuntimeProtocolError(code='x', message='y').to_dict()['code'] == 'x'
    assert RuntimeBootstrapResponse().to_dict()['response_type'] == 'bootstrap'

    lifecycle_event = domain_events.ResourceLifecycleEvent(
        action='acquired',
        name='db',
        scope='worker',
    )
    assert serialize_resource_lifecycle_event(lifecycle_event)['event_type'] == (
        'resource.lifecycle'
    )


def test_reporting_ir_helpers_cover_passthrough_and_normalization() -> None:
    subject = TestReport(
        path='tests/example.feature',
        status=TestResultStatus.PASSED,
        message=None,
        duration=0.1,
    )
    assert ensure_test_report(subject) is subject

    test = _DummyTestItem(Path('tests/example.feature'))
    test.status = TestResultStatus.PASSED
    test.duration = 0.2
    reconciled = reconcile_test_report(test, test)
    assert reconciled.path == 'tests/example.feature'

    location = LocationReport(text='file.py', line=7)
    assert _location_report_to_dict(location) == {'line': 7, 'text': 'file.py'}
    empty_location = _location_report_from_dict('not-a-dict')
    assert empty_location.text == ''
    assert empty_location.line == 0


def test_extensions_and_cxp_plugin_snapshot_filtering() -> None:
    snapshot = build_cxp_plugin_component_snapshot(_UnknownCapabilityPlugin())
    capability_names = {capability.name for capability in snapshot.capabilities}
    assert 'unknown_plugin_capability' not in capability_names

    constraint = ExtensionCompatibilityConstraint(name='a', value=('x',))
    assert ExtensionCompatibilityConstraint.from_dict(constraint.to_dict()) == constraint

    descriptor = ExtensionDescriptor(
        canonical_name='plugin.required',
        extension_kind='plugin',
        api_version=1,
    )
    assert ExtensionDescriptor.from_dict(descriptor.to_dict()) == descriptor

    component = ExtensionComponentSnapshot(
        component_name='plugin.required',
        descriptor=descriptor,
    )
    assert ExtensionComponentSnapshot.from_dict(component.to_dict()) == component

    query = ExtensionQuery(extension_kind='plugin')
    assert ExtensionQuery.from_dict(query.to_dict()) == query

    plugin_snapshot = build_plugin_extension_snapshot(
        _RequiredCapabilityPlugin(),
        descriptors=(),
    )
    assert plugin_snapshot.descriptor.compatibility[0].name == 'required_capabilities'


def test_plugin_and_reporter_small_async_paths(tmp_path: Path) -> None:
    class _Diagnostics:
        def __init__(self) -> None:
            self.calls = 0

        def trace(self, *args, **kwargs) -> None:
            del args, kwargs
            self.calls += 1

    diagnostics = _Diagnostics()
    plugin = _UnknownCapabilityPlugin()
    plugin.config = SimpleNamespace(diagnostics=diagnostics)
    plugin.log('trace-message')
    assert diagnostics.calls == 1

    analysis = SimpleNamespace(kind='analysis')
    transformed = asyncio.run(_PlanPlugin().transform_planning_analysis(analysis))
    assert transformed is analysis

    parser = argparse.ArgumentParser()
    TelemetryPlugin.register_arguments(parser)
    assert TelemetryPlugin.parse_args(parser.parse_args([])) is None
    assert asyncio.run(TelemetryPlugin(tmp_path / 'x.jsonl').finish()) is None

    timing_plugin = TimingPlugin()
    assert asyncio.run(timing_plugin.finish()) is None
    timing_plugin.config = SimpleNamespace(
        console=SimpleNamespace(is_summary_mode=lambda: True),
    )
    lines: list[str] = []
    timing_plugin._append_phase_section(lines, 'Collection', {'e': {'phase': 1.0}})
    assert lines == []

    reporter = _RecorderReporter()
    coordinator = ReportingCoordinator()
    telemetry_stream = TelemetryStream()
    coordinator.bind_telemetry_stream(telemetry_stream)
    coordinator.initialize_engine_reporter(
        SimpleNamespace(console=None),
        SimpleNamespace(reporter=reporter),
    )
    assert reporter.bound_telemetry_stream is telemetry_stream

    async def _exercise_reporters() -> None:
        null_reporter = NullReporter()
        await null_reporter.add_test('x')
        await null_reporter.add_test_result('y')
        assert await null_reporter.print_report() is None

        wrapped = _RecorderReporter()
        queued = QueuedReporter(wrapped, queue_add_test=True)
        queued.initialize(SimpleNamespace(console=None), None)
        await queued.start()
        await queued.add_test('queued-test')
        await queued.add_test_result('queued-result')
        await queued.finish()
        replacement = queued.with_wrapped(_RecorderReporter())

        assert wrapped.add_test_calls == 1
        assert wrapped.add_test_result_calls == 1
        assert wrapped.finish_calls == 1
        assert isinstance(replacement, QueuedReporter)

    asyncio.run(_exercise_reporters())


def test_telemetry_and_utils_small_error_paths(tmp_path: Path) -> None:
    async def _exercise_telemetry() -> None:
        span = TelemetrySpan(
            trace_id='trace',
            span_id='span',
            parent_span_id=None,
            name='demo',
            start_time=0.0,
            end_time=1.0,
        )

        stream = TelemetryStream()
        await stream.emit(span)
        await stream.flush()
        stream.add_sink(InMemoryTelemetrySink())
        await stream.flush()
        await stream.close()
        await stream.close()
        await stream.emit(span)

        sink = InMemoryTelemetrySink()
        assert await sink.flush() is None

        jsonl_sink = JsonlTelemetrySink(tmp_path / 'telemetry.jsonl')
        assert await jsonl_sink.flush() is None
        assert await jsonl_sink.close() is None

        try:
            await jsonl_sink._write_span(span)
        except RuntimeError as error:
            assert 'must run before emitting spans' in str(error)
        else:  # pragma: no cover
            raise AssertionError('Expected RuntimeError when emitting without start')

        try:
            jsonl_sink._write_line('line')
        except RuntimeError as error:
            assert 'must run before writing spans' in str(error)
        else:  # pragma: no cover
            raise AssertionError('Expected RuntimeError when writing without start')

    asyncio.run(_exercise_telemetry())

    lonely_module = tmp_path / 'standalone' / 'module.py'
    lonely_module.parent.mkdir(parents=True)
    lonely_module.write_text('', encoding='utf-8')
    discovered = _discover_import_search_paths_legacy(lonely_module)
    assert lonely_module.parent.resolve() in discovered

    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    mismatch_site_packages = (
        tmp_path
        / '.venv'
        / 'lib'
        / f'{version_name}-mismatch'
        / 'site-packages'
    )
    mismatch_site_packages.mkdir(parents=True)
    assert _discover_workspace_site_packages(tmp_path) == ()


def test_discover_workspace_site_packages_skips_nonexistent_glob_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ghost_path = tmp_path / 'ghost' / 'site-packages'
    monkeypatch.setattr(Path, 'glob', lambda self, pattern: [ghost_path])

    assert _discover_workspace_site_packages(tmp_path) == ()
