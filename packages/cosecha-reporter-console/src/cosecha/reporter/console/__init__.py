from __future__ import annotations

from dataclasses import dataclass, field

from cosecha.core.console_rendering import (
    CodeBlockComponent,
    ConsoleRenderComponent,
    ExtensionComponent,
    LineComponent,
    SectionComponent,
    StatusBadge,
    TableComponent,
    TextSpan,
)
from cosecha.core.discovery import iter_console_presenter_contributions
from cosecha.core.items import TestResultStatus
from cosecha.core.reporter import Reporter
from cosecha.core.reporting_ir import ensure_test_report


@dataclass(slots=True, frozen=True)
class OutputCaseEvent:
    title: str
    status: TestResultStatus
    compact_lines: tuple[str, ...] = field(default_factory=tuple)
    full_lines: tuple[str, ...] = field(default_factory=tuple)
    trace_lines: tuple[str, ...] = field(default_factory=tuple)
    show_when_success: bool = False

    @property
    def is_failure(self) -> bool:
        return self.status in (
            TestResultStatus.FAILED,
            TestResultStatus.ERROR,
        )


class ConsoleOutputPipeline:
    __slots__ = ('console',)

    def __init__(self, console) -> None:
        self.console = console

    def render_case(self, event: OutputCaseEvent) -> None:
        if self.console.should_render_live_progress():
            self._render_headline(event)
            if event.is_failure:
                self._render_body(event)
            return

        if event.is_failure or event.show_when_success:
            self._render_headline(event)
            if event.is_failure:
                self._render_body(event)

    def _render_headline(self, event: OutputCaseEvent) -> None:
        self.console.print(f'{event.status.value.upper():<7} {event.title}')

    def _render_body(self, event: OutputCaseEvent) -> None:
        body_lines = event.compact_lines
        if self.console.should_render_full_failures() and event.full_lines:
            body_lines = event.full_lines

        for line in body_lines:
            self.console.print(f'  {line}')

        if self.console.should_render_trace_diagnostics():
            for line in event.trace_lines:
                self.console.print(f'  {line}')


class ConsoleReporter(Reporter):
    __slots__ = ('_cases', '_pipeline', '_presenters')
    contribution_name = 'console'

    @classmethod
    def reporter_name(cls) -> str:
        return 'console'

    @classmethod
    def reporter_output_kind(cls) -> str:
        return 'console'

    async def start(self) -> None:
        self._cases = {
            TestResultStatus.PASSED: 0,
            TestResultStatus.FAILED: 0,
            TestResultStatus.ERROR: 0,
            TestResultStatus.SKIPPED: 0,
            TestResultStatus.PENDING: 0,
        }
        self._pipeline = ConsoleOutputPipeline(self.console)
        self._presenters = {
            contribution.contribution_name: contribution
            for contribution in iter_console_presenter_contributions()
        }

    async def add_test(self, test):
        del test

    async def add_test_result(self, test):
        report = ensure_test_report(test, self.config.root_path)
        self._cases[report.status] += 1
        title = self._build_case_title(report)
        compact_lines, full_lines = self._build_case_lines(report)
        self._pipeline.render_case(
            OutputCaseEvent(
                title=title,
                status=report.status,
                compact_lines=compact_lines,
                full_lines=full_lines,
                show_when_success=self.console.should_render_live_progress(),
            ),
        )

    async def print_report(self):
        total = sum(self._cases.values())
        segments = [
            f'Tests ({total}):',
            f'{self._cases[TestResultStatus.PASSED]} passed',
            f'{self._cases[TestResultStatus.FAILED]} failed',
        ]
        if self._cases[TestResultStatus.ERROR]:
            segments.append(f'{self._cases[TestResultStatus.ERROR]} errors')
        if self._cases[TestResultStatus.SKIPPED]:
            segments.append(
                f'{self._cases[TestResultStatus.SKIPPED]} skipped',
            )
        if self._cases[TestResultStatus.PENDING]:
            segments.append(
                f'{self._cases[TestResultStatus.PENDING]} pending',
            )
        engine_name = self.engine.name if self.engine is not None else 'tests'
        self.console.print_summary(engine_name, ', '.join(segments))

    def _build_case_title(self, report) -> str:
        presenter = self._presenters.get(report.engine_name or '')
        if presenter is None:
            return str(report.path or 'unknown')
        title_spans = presenter.build_case_title(report, config=self.config)
        if not title_spans:
            return str(report.path or 'unknown')
        return self._render_spans(title_spans)

    def _build_case_lines(
        self,
        report,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        presenter = self._presenters.get(report.engine_name or '')
        if presenter is None:
            return self._build_generic_case_lines(report)

        rendered_components = self._render_components(
            presenter.build_console_components(
                report,
                config=self.config,
            ),
        )
        compact_lines = tuple(
            line for line in rendered_components if line is not None
        )
        full_lines = compact_lines
        if report.exception_text:
            full_lines = (*full_lines, report.exception_text)
        return compact_lines, full_lines

    def _build_generic_case_lines(
        self,
        report,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        compact_lines: tuple[str, ...] = ()
        full_lines: tuple[str, ...] = ()
        compact_line_items: list[str] = []
        if report.failure_kind:
            compact_line_items.append(f'Failure kind: {report.failure_kind}')
        if report.message:
            compact_line_items.append(report.message)
        if compact_line_items:
            compact_lines = tuple(compact_line_items)
            full_lines = compact_lines
        if report.exception_text:
            full_lines = (*full_lines, report.exception_text)
        return compact_lines, full_lines

    def _render_components(
        self,
        components: tuple[ConsoleRenderComponent, ...],
        *,
        depth: int = 0,
    ) -> tuple[str | None, ...]:
        rendered: list[str | None] = []
        for component in components:
            if isinstance(component, LineComponent):
                line = self._render_line(component, depth=depth)
                if line is not None:
                    rendered.append(line)
                continue
            if isinstance(component, StatusBadge):
                rendered.append(self._render_badge(component))
                continue
            if isinstance(component, CodeBlockComponent):
                if component.title:
                    rendered.append(
                        self._render_line(
                            LineComponent(
                                spans=(
                                    TextSpan(
                                        component.title,
                                        tone='muted',
                                    ),
                                ),
                            ),
                            depth=depth,
                        ),
                    )
                rendered.extend(
                    '  ' * depth + line
                    for line in (
                        component.code.splitlines()
                        or (component.code,)
                    )
                )
                continue
            if isinstance(component, TableComponent):
                if component.title:
                    rendered.append(
                        self._render_line(
                            LineComponent(
                                spans=(
                                    TextSpan(
                                        component.title,
                                        emphatic=True,
                                    ),
                                ),
                            ),
                            depth=depth,
                        ),
                    )
                header = ' | '.join(component.columns)
                rendered.append('  ' * depth + header)
                rendered.extend(
                    '  ' * depth + ' | '.join(row)
                    for row in component.rows
                )
                continue
            if isinstance(component, SectionComponent):
                section_title = LineComponent(
                    spans=(TextSpan(component.title, emphatic=True),),
                )
                rendered.append(
                    self._render_line(section_title, depth=depth),
                )
                rendered.extend(
                    self._render_components(
                        component.children,
                        depth=depth + 1,
                    ),
                )
                continue
            if isinstance(component, ExtensionComponent):
                rendered.extend(
                    self._render_components(
                        component.fallback,
                        depth=depth,
                    ),
                )
        return tuple(rendered)

    def _render_line(
        self,
        component: LineComponent,
        *,
        depth: int,
    ) -> str | None:
        if not component.spans and component.badge is None:
            return None
        line = ' ' * ((depth * 2) + component.indent)
        if component.badge is not None:
            line += self._render_badge(component.badge)
            if component.spans:
                line += ' '
        line += self._render_spans(component.spans)
        return line

    def _render_badge(self, badge: StatusBadge) -> str:
        return f'[{badge.label}]'

    def _render_spans(self, spans: tuple[TextSpan, ...]) -> str:
        text = ''
        for span in spans:
            rendered = span.text
            if span.emphatic:
                rendered = f'*{rendered}*'
            text += rendered
        return text


__all__ = ('ConsoleReporter',)
