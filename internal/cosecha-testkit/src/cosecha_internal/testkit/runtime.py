from __future__ import annotations

from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from cosecha.core.collector import Collector
from cosecha.core.config import Config
from cosecha.core.items import TestResultStatus
from cosecha.core.output import OutputDetail, OutputMode
from cosecha.core.plugins.base import PluginContext
from cosecha.core.reporter import Reporter
from cosecha.core.reporting_ir import TestReport
from cosecha.core.session_timing import SessionTiming


if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from cosecha.core.items import TestItem
    from cosecha.core.session_artifacts import SessionReportState


class CapturingConsole:
    __slots__ = (
        'output_detail',
        'output_mode',
        'printed_lines',
        'summaries',
    )

    def __init__(
        self,
        *,
        output_mode: OutputMode = OutputMode.SUMMARY,
        output_detail: OutputDetail = OutputDetail.STANDARD,
        **_kwargs,
    ) -> None:
        self.output_mode = output_mode
        self.output_detail = output_detail
        self.printed_lines: list[str] = []
        self.summaries: list[tuple[str, str]] = []

    def is_summary_mode(self) -> bool:
        return self.output_mode == OutputMode.SUMMARY

    def is_live_mode(self) -> bool:
        return self.output_mode == OutputMode.LIVE

    def is_debug_mode(self) -> bool:
        return self.output_mode == OutputMode.DEBUG

    def is_trace_mode(self) -> bool:
        return self.output_mode == OutputMode.TRACE

    def should_render_live_progress(self) -> bool:
        return self.is_live_mode()

    def should_render_full_failures(self) -> bool:
        return (
            self.output_detail == OutputDetail.FULL_FAILURES
            or self.is_debug_mode()
            or self.is_trace_mode()
        )

    def should_render_trace_diagnostics(self) -> bool:
        return self.is_trace_mode()

    def print(
        self,
        *objects: Any,
        sep: str = ' ',
        end: str = '\n',
        **_kwargs,
    ) -> None:
        rendered = sep.join(str(object_) for object_ in objects)
        if end.endswith('\n'):
            rendered = rendered.removesuffix('\n')
        self.printed_lines.append(rendered)

    def info(self, *objects: Any, **kwargs: Any) -> None:
        self.print(*objects, **kwargs)

    def debug(self, *objects: Any, **kwargs: Any) -> None:
        if self.is_debug_mode() or self.is_trace_mode():
            self.print(*objects, **kwargs)

    def trace(self, *objects: Any, **kwargs: Any) -> None:
        if self.is_trace_mode():
            self.print(*objects, **kwargs)

    def print_summary(self, title: str, text: str) -> None:
        self.summaries.append((title, text))

    def print_exception(self, *, exc_info=None, **_kwargs) -> None:
        if exc_info is None:
            return
        exc_type, exc_value, _traceback = exc_info
        if exc_type is None:
            return
        self.print(f'{exc_type.__name__}: {exc_value}')

    def status(self, *_args, **_kwargs):
        return nullcontext()


class NullTelemetryStream:
    __slots__ = ('spans',)

    def __init__(self) -> None:
        self.spans: list[tuple[str, dict[str, object]]] = []

    @asynccontextmanager
    async def span(
        self,
        name: str,
        attributes: dict[str, object] | None = None,
    ):
        self.spans.append((name, attributes or {}))
        yield


class DummyReporter(Reporter):
    __slots__ = ('recorded_results', 'recorded_tests')

    def __init__(self) -> None:
        self.recorded_tests: list[object] = []
        self.recorded_results: list[object] = []

    async def add_test(self, test) -> None:
        self.recorded_tests.append(test)

    async def add_test_result(self, test) -> None:
        self.recorded_results.append(test)

    async def print_report(self) -> None:
        return None


class ListCollector(Collector):
    __slots__ = ('_tests',)

    def __init__(
        self,
        tests: Iterable[TestItem],
        *,
        file_type: str = 'feature',
    ) -> None:
        super().__init__(file_type)
        self._tests = tuple(tests)

    async def find_test_files(self, base_path):
        del base_path
        return [test.path for test in self._tests if test.path is not None]

    async def load_tests_from_file(self, test_path):
        return [test for test in self._tests if test.path == test_path]


def build_config(
    root_path: Path,
    *,
    output_mode: OutputMode = OutputMode.SUMMARY,
    output_detail: OutputDetail = OutputDetail.STANDARD,
) -> Config:
    return Config(
        root_path=root_path,
        output_mode=output_mode,
        output_detail=output_detail,
        console_cls=CapturingConsole,
    )


def build_plugin_context(
    config: Config,
    *,
    telemetry_stream: NullTelemetryStream | None = None,
    session_timing: SessionTiming | None = None,
    engine_names: tuple[str, ...] = (),
    session_report_state: SessionReportState | None = None,
) -> PluginContext:
    return PluginContext(
        config=config,
        session_timing=session_timing or SessionTiming(),
        telemetry_stream=telemetry_stream or NullTelemetryStream(),
        domain_event_stream=SimpleNamespace(),
        knowledge_base=SimpleNamespace(),
        resource_manager=SimpleNamespace(),
        engine_names=engine_names,
        session_report_state=session_report_state,
    )


def build_generic_report(  # noqa: PLR0913
    *,
    path: str,
    status: TestResultStatus,
    duration: float = 0.0,
    message: str | None = None,
    failure_kind: str | None = None,
    error_code: str | None = None,
    exception_text: str | None = None,
    engine_name: str | None = None,
) -> TestReport:
    return TestReport(
        path=path,
        status=status,
        message=message,
        duration=duration,
        failure_kind=failure_kind,
        error_code=error_code,
        exception_text=exception_text,
        engine_name=engine_name,
    )


def build_gherkin_report(  # noqa: PLR0913
    *,
    path: str,
    status: TestResultStatus,
    scenario_name: str,
    feature_name: str = 'Feature Demo',
    scenario_line: int = 1,
    duration: float = 0.0,
    message: str | None = None,
    exception_text: str | None = None,
    step_results: tuple[dict[str, object], ...] = (),
) -> TestReport:
    normalized_step_results = list(step_results)
    if not normalized_step_results:
        normalized_step_results.append(
            {
                'status': TestResultStatus.PASSED.value,
                'message': None,
                'step': {
                    'keyword': 'Given ',
                    'text': 'a reusable workspace',
                    'location': {'text': path, 'line': scenario_line},
                    'implementation_location': None,
                },
                'exception_text': None,
            },
        )

    return TestReport(
        path=path,
        status=status,
        message=message,
        duration=duration,
        exception_text=exception_text,
        engine_name='gherkin',
        engine_payload={
            'feature': {
                'name': feature_name,
                'location': {'text': path, 'line': 1},
                'tags': [],
            },
            'scenario': {
                'name': scenario_name,
                'location': {'text': path, 'line': scenario_line},
                'tags': [],
            },
            'example': None,
            'step_result_list': normalized_step_results,
        },
    )


def write_text_tree(
    base_path: Path,
    files: dict[str, str],
) -> tuple[Path, ...]:
    written_paths: list[Path] = []
    for relative_path, content in files.items():
        file_path = base_path / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding='utf-8')
        written_paths.append(file_path)
    return tuple(written_paths)
