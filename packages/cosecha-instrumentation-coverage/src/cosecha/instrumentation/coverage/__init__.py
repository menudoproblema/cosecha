from __future__ import annotations

import importlib
import io
import sys

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from cosecha.core.capabilities import (
    CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
    CapabilityAttribute,
    CapabilityDescriptor,
    CapabilityOperationBinding,
)
from cosecha.core.instrumentation import Contribution
from cosecha.core.session_artifacts import (
    InstrumentationSummary,
    SessionCoverageSummary,
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence
    from pathlib import Path


COSECHA_COMPONENT_ID = 'cosecha.instrumentation.coverage'


@dataclass(slots=True, frozen=True)
class CoverageRequest:
    source_targets: tuple[str, ...]
    branch: bool = False
    report_type: Literal['term', 'term-missing'] = 'term'


class CoverageInstrumenter:
    __slots__ = ('request',)
    COSECHA_COMPONENT_ID = COSECHA_COMPONENT_ID

    def __init__(self, request: CoverageRequest) -> None:
        self.request = request

    @classmethod
    def instrumentation_name(cls) -> str:
        return 'coverage'

    @classmethod
    def instrumentation_api_version(cls) -> int:
        return 1

    @classmethod
    def instrumentation_stability(cls) -> str:
        return 'stable'

    @classmethod
    def describe_capabilities(cls) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name='instrumentation_bootstrap',
                level='supported',
                summary='Prepare coverage bootstrap files and env for a child run.',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='instrumentation.prepare',
                        result_type='instrumentation.contribution',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name='session_summary',
                level='supported',
                summary='Collect a coverage session summary after the child exits.',
                attributes=(
                    CapabilityAttribute(
                        name='instrumentation_name',
                        value='coverage',
                    ),
                    CapabilityAttribute(
                        name='summary_kind',
                        value='coverage.py',
                    ),
                    CapabilityAttribute(
                        name='measurement_scope',
                        value='controller_process',
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='instrumentation.collect',
                        result_type='instrumentation.summary',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name='structured_summary',
                level='supported',
                summary='Expose a serializable coverage summary payload.',
                attributes=(
                    CapabilityAttribute(
                        name='payload_formats',
                        value=('json',),
                    ),
                    CapabilityAttribute(
                        name='serializable',
                        value=True,
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='instrumentation.collect',
                        result_type='instrumentation.summary',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
                level='supported',
                summary='Coverage writes ephemeral bootstrap and data files into the session shadow.',
                attributes=(
                    CapabilityAttribute(
                        name='component_id',
                        value=COSECHA_COMPONENT_ID,
                    ),
                    CapabilityAttribute(
                        name='ephemeral_domain',
                        value='instrumentation',
                    ),
                    CapabilityAttribute(
                        name='produces_persistent',
                        value=False,
                    ),
                    CapabilityAttribute(
                        name='cleanup_on_success',
                        value=True,
                    ),
                    CapabilityAttribute(
                        name='preserve_on_failure',
                        value=True,
                    ),
                    CapabilityAttribute(
                        name='description',
                        value='coverage bootstrap files and data artifacts',
                    ),
                ),
            ),
        )

    def strip_bootstrap_options(self, argv: Sequence[str]) -> list[str]:
        return _strip_coverage_options(argv)

    def prepare(self, *, workdir: Path) -> Contribution:
        coverage_module = _coverage_module()
        config = coverage_module.Coverage(config_file=True).config
        data_file = workdir / '.coverage'
        rcfile_path = workdir / '.cosecha.coveragerc'
        effective_branch = self.request.branch or bool(config.branch)
        argv_prefix = [
            sys.executable,
            '-m',
            'coverage',
            'run',
            f'--rcfile={rcfile_path}',
            '-m',
            'cosecha.shell.runner_cli',
        ]

        warnings: list[str] = []
        configured_sources = tuple(
            source
            for source in (config.source or ())
            if isinstance(source, str)
        )
        if (
            configured_sources
            and configured_sources != self.request.source_targets
        ):
            warnings.append(
                'overriding configured coverage sources with --cov targets',
            )
        if self.request.branch and not bool(config.branch):
            warnings.append(
                'overriding configured branch setting with --cov-branch',
            )
        if not bool(config.parallel):
            warnings.append(
                'forcing coverage parallel data mode for Cosecha integration',
            )

        return Contribution(
            env={
                'COVERAGE_PROCESS_START': str(rcfile_path),
            },
            argv_prefix=tuple(argv_prefix),
            workdir_files={
                str(rcfile_path.relative_to(workdir)): _build_rcfile_content(
                    branch=effective_branch,
                    concurrency=tuple(
                        str(item) for item in config.concurrency
                    ),
                    data_file=str(data_file),
                    omit=tuple(str(item) for item in config.run_omit),
                    source_targets=self.request.source_targets,
                ),
            },
            warnings=tuple(warnings),
        )

    def collect(self, *, workdir: Path) -> InstrumentationSummary:
        data_file = workdir / '.coverage'
        coverage_module = _coverage_module()
        coverage_kwargs: dict[str, object] = {
            'config_file': str(workdir / '.cosecha.coveragerc'),
            'data_file': str(data_file),
        }
        cov = coverage_module.Coverage(**coverage_kwargs)
        cov.combine(data_paths=[str(workdir)])
        cov.save()
        typed_summary = build_coverage_summary(
            cov,
            report_type=self.request.report_type,
        )
        return InstrumentationSummary(
            instrumentation_name='coverage',
            summary_kind='coverage.py',
            payload=typed_summary.to_dict(),
        )

    @classmethod
    def from_argv(
        cls,
        argv: tuple[str, ...] | list[str],
    ) -> CoverageInstrumenter | None:
        request = parse_coverage_request(argv)
        if request is None:
            return None
        return cls(request)


def parse_coverage_request(
    argv: tuple[str, ...] | list[str],
) -> CoverageRequest | None:
    source_targets = tuple(
        target.strip()
        for raw_value in _extract_option_values(argv, '--cov')
        for target in raw_value.split(',')
        if target.strip()
    )
    if not source_targets:
        return None
    report_values = _extract_option_values(argv, '--cov-report')
    report_type: Literal['term', 'term-missing'] = 'term'
    if report_values and report_values[-1] == 'term-missing':
        report_type = 'term-missing'
    return CoverageRequest(
        source_targets=source_targets,
        branch='--cov-branch' in argv,
        report_type=report_type,
    )


def _strip_coverage_options(argv: Sequence[str]) -> list[str]:
    stripped: list[str] = []
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument.startswith('--cov=') or argument == '--cov-branch':
            index += 1
            continue
        if argument in {'--cov', '--cov-report'}:
            index += 2
            continue
        if argument.startswith('--cov-report='):
            index += 1
            continue
        stripped.append(argument)
        index += 1
    return stripped


def build_coverage_summary(
    cov,
    *,
    report_type: Literal['term', 'term-missing'],
) -> SessionCoverageSummary:
    file = io.StringIO()
    show_missing = True
    skip_covered = report_type == 'term-missing'
    total_coverage = cov.report(
        file=file,
        show_missing=show_missing,
        skip_covered=skip_covered,
    )
    source_targets = tuple(
        sorted(
            source
            for source in (cov.config.source or ())
            if isinstance(source, str)
        ),
    )
    return SessionCoverageSummary(
        total_coverage=float(total_coverage),
        report_type=report_type,
        measurement_scope='controller_process',
        branch=bool(cov.config.branch),
        source_targets=source_targets,
        includes_python_subprocesses=True,
        includes_worker_processes=False,
    )


def _build_rcfile_content(
    *,
    branch: bool,
    concurrency: tuple[str, ...],
    data_file: str,
    omit: tuple[str, ...],
    source_targets: tuple[str, ...],
) -> str:
    lines = [
        '[run]',
        f'branch = {"True" if branch else "False"}',
        f'data_file = {data_file}',
        'parallel = True',
        'patch = subprocess',
    ]
    if source_targets:
        lines.append('source =')
        lines.extend(f'    {target}' for target in source_targets)
    if concurrency:
        lines.append('concurrency = ' + ','.join(concurrency))
    if omit:
        lines.append('omit =')
        lines.extend(f'    {pattern}' for pattern in omit)
    lines.append('')
    return '\n'.join(lines)


def _coverage_module():
    return importlib.import_module('coverage')


def _extract_option_values(
    argv: tuple[str, ...] | list[str],
    option_name: str,
) -> tuple[str, ...]:
    values: list[str] = []
    iterator = iter(range(len(argv)))
    for index in iterator:
        argument = argv[index]
        prefix = f'{option_name}='
        if argument.startswith(prefix):
            values.append(argument.removeprefix(prefix))
            continue
        if argument != option_name:
            continue
        next_index = index + 1
        if next_index >= len(argv):
            break
        values.append(argv[next_index])
        next(iterator, None)
    return tuple(values)
