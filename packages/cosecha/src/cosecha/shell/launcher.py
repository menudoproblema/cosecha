from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile

from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

from cosecha.core.config import Config
from cosecha.core.instrumentation import (
    COSECHA_COVERAGE_ACTIVE_ENV,
    COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
)
from cosecha.core.knowledge_base import (
    PersistentKnowledgeBase,
    SessionArtifactQuery,
)


def _strip_coverage_options(argv: list[str]) -> list[str]:
    stripped: list[str] = []
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument.startswith('--cov=') or argument in {
            '--cov-branch',
        }:
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


def _run_runner_cli(argv: Sequence[str]) -> int:
    from cosecha.shell import runner_cli

    runner_cli.main(list(argv))
    return 0


def _should_bootstrap_coverage(argv: list[str]) -> bool:
    if os.environ.get(COSECHA_COVERAGE_ACTIVE_ENV) == '1':
        return False
    if not argv or argv[0] != 'run':
        return False
    return any(
        argument == '--cov' or argument.startswith('--cov=')
        for argument in argv
    )


def _load_metadata(metadata_path: Path) -> dict[str, object] | None:
    if not metadata_path.exists():
        return None
    return json.loads(metadata_path.read_text(encoding='utf-8'))


def _update_session_artifact(
    metadata: dict[str, object],
    *,
    summary,
) -> tuple[object | None, str | None]:
    knowledge_base_path = metadata.get('knowledge_base_path')
    session_id = metadata.get('session_id')
    if not isinstance(knowledge_base_path, str) or not isinstance(
        session_id,
        str,
    ):
        return (None, 'session metadata is incomplete')

    knowledge_base = PersistentKnowledgeBase(Path(knowledge_base_path))
    try:
        artifacts = knowledge_base.query_session_artifacts(
            SessionArtifactQuery(session_id=session_id, limit=1),
        )
        if not artifacts:
            return (None, f'session artifact not found for {session_id}')
        artifact = artifacts[0]
        report_summary = artifact.report_summary
        if report_summary is None:
            return (
                None,
                f'session artifact {session_id} has no report summary',
            )
        updated_report_summary = replace(
            report_summary,
            instrumentation_summaries={
                **report_summary.instrumentation_summaries,
                summary.instrumentation_name: summary,
            },
        )
        updated_artifact = replace(
            artifact,
            report_summary=updated_report_summary,
        )
        knowledge_base.store_session_artifact(updated_artifact)
        return (updated_artifact, None)
    finally:
        knowledge_base.close()


def _render_coverage_summary(summary, *, config_snapshot) -> None:
    payload = summary.payload
    total_coverage = payload.get('total_coverage')
    measurement_scope = payload.get('measurement_scope')
    if not isinstance(total_coverage, int | float):
        return
    if not isinstance(measurement_scope, str):
        measurement_scope = 'controller_process'

    lines = [
        (
            'Coverage: '
            f'{float(total_coverage):.2f}% '
            f'[{measurement_scope}]'
        ),
    ]
    engine_names = payload.get('engine_names')
    if isinstance(engine_names, list) and engine_names:
        lines.append('  engines: ' + ', '.join(str(name) for name in engine_names))
    source_targets = payload.get('source_targets')
    if isinstance(source_targets, list) and source_targets:
        lines.append(
            '  sources: ' + ', '.join(str(target) for target in source_targets),
        )
    if payload.get('includes_worker_processes') is False:
        lines.append('  worker processes are not included in this measurement')
    console = Config.from_snapshot(config_snapshot).console
    console.print_summary('Coverage', '\n'.join(lines))


def _bootstrap_coverage(argv: list[str]) -> int:
    try:
        from cosecha.plugin.coverage import CoverageInstrumenter
    except ImportError as error:
        print(
            'Coverage support is not installed. Install cosecha[coverage].',
        )
        raise SystemExit(2) from error

    instrumenter = CoverageInstrumenter.from_argv(argv)
    if instrumenter is None:
        return _run_runner_cli(argv)

    stripped_argv = _strip_coverage_options(argv)
    workdir = Path(tempfile.mkdtemp(prefix='cosecha-coverage-'))
    cleanup_workdir = True
    try:
        metadata_path = workdir / 'run-metadata.json'
        contribution = instrumenter.prepare(workdir=workdir)
        for relative_path, contents in contribution.workdir_files.items():
            target_path = workdir / relative_path
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(contents, encoding='utf-8')
        for warning in contribution.warnings:
            print(f'Coverage warning: {warning}')

        env = os.environ.copy()
        env.update(contribution.env)
        env[COSECHA_COVERAGE_ACTIVE_ENV] = '1'
        env[COSECHA_INSTRUMENTATION_METADATA_FILE_ENV] = str(metadata_path)
        command = [*contribution.argv_prefix, *stripped_argv]
        completed = subprocess.run(  # noqa: S603
            command,
            check=False,
            env=env,
        )

        metadata = _load_metadata(metadata_path)
        if metadata is None:
            print(
                'Coverage warning: no session metadata was written; '
                'coverage was not persisted.',
            )
            return int(completed.returncode)

        try:
            summary = instrumenter.collect(workdir=workdir)
            updated_artifact, warning = _update_session_artifact(
                metadata,
                summary=summary,
            )
            if updated_artifact is None and warning is not None:
                print(
                    'Coverage warning: coverage was collected but not '
                    f'persisted ({warning}).',
                )
            if updated_artifact is not None:
                _render_coverage_summary(
                    summary,
                    config_snapshot=updated_artifact.config_snapshot,
                )
        except Exception as error:
            cleanup_workdir = False
            print(
                'Coverage warning: failed to collect coverage '
                f'({error}). Preserved workdir: {workdir}',
            )
        return int(completed.returncode)
    finally:
        if cleanup_workdir:
            shutil.rmtree(workdir, ignore_errors=True)


def main(argv: list[str] | None = None) -> None:
    argv_list = list(sys.argv[1:] if argv is None else argv)
    if _should_bootstrap_coverage(argv_list):
        raise SystemExit(_bootstrap_coverage(argv_list))

    raise SystemExit(_run_runner_cli(argv_list))
