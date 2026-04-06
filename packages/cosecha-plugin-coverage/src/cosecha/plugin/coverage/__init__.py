import io

from argparse import ArgumentParser, Namespace
from collections.abc import Iterable
from typing import Literal, NotRequired, Self, TypedDict, Unpack, override

import coverage
import coverage.types

from cosecha.core.plugins.base import Plugin
from cosecha.core.session_artifacts import SessionCoverageSummary


class CoverageOptions(TypedDict):
    data_file: NotRequired[coverage.types.FilePath | None]
    data_suffix: NotRequired[str | bool | None]
    cover_pylib: NotRequired[bool | None]
    auto_data: NotRequired[bool]
    timid: NotRequired[bool | None]
    branch: NotRequired[bool | None]
    config_file: NotRequired[coverage.types.FilePath | bool]
    source: NotRequired[Iterable[str] | None]
    source_pkgs: NotRequired[Iterable[str] | None]
    omit: NotRequired[str | Iterable[str] | None]
    include: NotRequired[str | Iterable[str] | None]
    debug: NotRequired[Iterable[str] | None]
    concurrency: NotRequired[str | Iterable[str] | None]
    check_preimported: NotRequired[bool]
    context: NotRequired[str | None]
    messages: NotRequired[bool]


class CoveragePlugin(Plugin):
    __slots__ = (
        'cov',
        'report_type',
        'threshold_excellent',
        'threshold_fair',
        'threshold_poor',
    )

    def __init__(
        self,
        threshold_excellent: int = 95,
        threshold_fair: int = 50,
        threshold_poor: int = 30,
        report_type: Literal['term', 'term-missing'] = 'term',
        **kwargs: Unpack[CoverageOptions],
    ):
        self.cov = coverage.Coverage(**kwargs)
        self.threshold_excellent = threshold_excellent
        self.threshold_fair = threshold_fair
        self.threshold_poor = threshold_poor
        self.report_type = report_type

    @override
    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None:
        parser.add_argument(
            '--cov',
            help='Specifies the source for coverage analysis',
        )

        parser.add_argument(
            '--cov-branch',
            action='store_true',
            help='Activates the coverage plugin',
        )

        parser.add_argument(
            '--cov-report',
            type=str,
            choices=['term', 'term-missing'],
            default='term',
        )

    @override
    @classmethod
    def parse_args(cls, args: Namespace) -> Self | None:
        if not args.cov:
            return None

        return cls(
            source=args.cov.split(','),
            branch=bool(args.cov_branch),
            report_type=args.cov_report,
        )

    @override
    async def start(self):
        self.cov.start()

    @override
    async def finish(self):
        self.cov.stop()
        self.cov.save()
        async with self.context.telemetry_stream.span(
            'plugin.coverage.build_summary',
            attributes={'cosecha.plugin.name': self.plugin_name()},
        ):
            summary = self.build_coverage_summary()
        if self.context.session_report_state is not None:
            self.context.session_report_state.coverage_summary = summary
            return
        async with self.context.telemetry_stream.span(
            'plugin.coverage.print_report',
            attributes={'cosecha.plugin.name': self.plugin_name()},
        ):
            self.print_coverage_report(summary)

    def build_coverage_summary(self) -> SessionCoverageSummary:
        file = io.StringIO()

        match self.report_type:
            case 'term':
                show_missing = True
                skip_covered = False

            case 'term-missing':
                show_missing = True
                skip_covered = True

            case _:
                show_missing = True
                skip_covered = False

        total_coverage = self.cov.report(
            file=file,
            show_missing=show_missing,
            skip_covered=skip_covered,
        )
        source_targets = tuple(
            sorted(
                source
                for source in (self.cov.config.source or ())
                if isinstance(source, str)
            ),
        )
        return SessionCoverageSummary(
            total_coverage=float(total_coverage),
            report_type=self.report_type,
            measurement_scope='controller_process',
            branch=bool(self.cov.config.branch),
            engine_names=tuple(sorted(self.context.engine_names)),
            source_targets=source_targets,
            includes_worker_processes=False,
        )

    def print_coverage_report(
        self,
        summary: SessionCoverageSummary | None = None,
    ) -> None:
        """Genera un reporte de cobertura en la consola."""
        file = io.StringIO()

        match self.report_type:
            case 'term':
                show_missing = True
                skip_covered = False

            case 'term-missing':
                show_missing = True
                skip_covered = True

            case _:
                show_missing = True
                skip_covered = False

        del summary
        self.cov.report(
            file=file,
            show_missing=show_missing,
            skip_covered=skip_covered,
        )
        file.seek(0)
        lines = file.readlines()
        coverage_text = '\n' + ''.join(lines)
        self.config.console.print_summary('Coverage', coverage_text)
