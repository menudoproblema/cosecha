from __future__ import annotations

from typing import TYPE_CHECKING, Self, override

from cosecha.core.capabilities import (
    CapabilityAttribute,
    CapabilityDescriptor,
)
from cosecha.core.plugins.base import Plugin


if TYPE_CHECKING:  # pragma: no cover
    from argparse import ArgumentParser, Namespace

    from cosecha.core.plugins.base import PluginContext


class TimingPlugin(Plugin):
    @override
    @classmethod
    def finish_priority(cls) -> int:
        return 100

    @override
    async def initialize(self, context: PluginContext) -> None:
        await super().initialize(context)

    @override
    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None:
        parser.add_argument(
            '--timing',
            action='store_true',
            help='Activates timing analysis',
        )

    @override
    @classmethod
    def parse_args(cls, args: Namespace) -> Self | None:
        if not args.timing:
            return None
        return cls()

    @override
    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name='timing_summary',
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='output_formats',
                        value=('console_summary',),
                    ),
                ),
            ),
        )

    @override
    async def start(self): ...

    @override
    async def finish(self):
        return None

    @override
    async def after_session_closed(self) -> None:
        async with self.context.telemetry_stream.span(
            'plugin.timing.print_report',
            attributes={'cosecha.plugin.name': self.plugin_name()},
        ):
            self._print_timing_report()

    def _append_phase_section(
        self,
        lines: list[str],
        title: str,
        phases_by_engine: dict[str, dict[str, float]],
    ) -> None:
        if not phases_by_engine or self.config.console.is_summary_mode():
            return

        lines.append('')
        lines.append(f'{title}:')
        for engine_name, phases in sorted(phases_by_engine.items()):
            lines.append(f'  [{engine_name}]')
            for phase, duration in phases.items():
                lines.append(f'    {phase:<22} {duration:.3f}s')

    def _print_timing_report(self):
        st = self.context.session_timing
        lines: list[str] = []

        if st is None:
            self.config.console.print_summary(
                'Timing',
                'No timing data available.',
            )
            return

        collect = st.collect_duration
        run = st.run_duration
        shutdown = st.shutdown_duration
        tests = st.tests_duration
        total = st.total_duration

        if collect is not None:
            lines.append(f'Collection:  {collect:.3f}s')
        if run is not None:
            lines.append(f'Run:         {run:.3f}s')
        lines.append(f'Tests:       {tests:.3f}s')
        if shutdown is not None:
            lines.append(f'Shutdown:    {shutdown:.3f}s')
        if total is not None:
            lines.append(f'Total:       {total:.3f}s')

        self._append_phase_section(
            lines,
            'Collection phases',
            st.collect_phases,
        )
        self._append_phase_section(
            lines,
            'Session phases',
            st.session_phases,
        )
        if st.shutdown_phases and not self.config.console.is_summary_mode():
            lines.append('')
            lines.append('Shutdown phases:')
            for phase, duration in sorted(st.shutdown_phases.items()):
                lines.append(f'  {phase:<24} {duration:.3f}s')

        if st.tests and not self.config.console.is_summary_mode():
            if st.test_phase_totals:
                lines.append('')
                lines.append('Test phases:')
                for phase, duration in sorted(st.test_phase_totals.items()):
                    lines.append(f'  {phase:<24} {duration:.3f}s')

            lines.append('')
            lines.append('Per test:')
            lines.extend(
                f'  {test.duration:.3f}s  {test.name}'
                for test in sorted(
                    st.tests,
                    key=lambda current_test: current_test.duration,
                    reverse=True,
                )
            )

        self.config.console.print_summary('Timing', '\n'.join(lines))
