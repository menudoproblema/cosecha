from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

from cosecha.core.config import Config, ConfigSnapshot
from cosecha.core.instrumentation import (
    COSECHA_COVERAGE_ACTIVE_ENV,
    COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
)
from cosecha.core.knowledge_base import (
    PersistentKnowledgeBase,
    SessionArtifactQuery,
)

_SESSION_ARTIFACT_RETRY_ATTEMPTS = 3
_SESSION_ARTIFACT_RETRY_DELAY_SECONDS = 0.05


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


def _config_snapshot_from_metadata(
    metadata: dict[str, object],
) -> ConfigSnapshot | None:
    snapshot_payload = metadata.get('config_snapshot')
    if not isinstance(snapshot_payload, dict):
        return None
    try:
        return ConfigSnapshot.from_dict(snapshot_payload)
    except (TypeError, ValueError):
        return None


def _update_session_artifact(
    metadata: dict[str, object],
    *,
    summary,
) -> tuple[object | None, str | None]:
    knowledge_base_path = metadata.get('knowledge_base_path')
    session_id = metadata.get('session_id')
    if not isinstance(session_id, str):
        return (None, 'session metadata is incomplete')
    if not isinstance(knowledge_base_path, str):
        return (None, 'session metadata has no persistent knowledge base')

    db_path = Path(knowledge_base_path)
    for attempt in range(_SESSION_ARTIFACT_RETRY_ATTEMPTS):
        knowledge_base = None
        try:
            knowledge_base = PersistentKnowledgeBase(db_path)
            artifacts = knowledge_base.query_session_artifacts(
                SessionArtifactQuery(session_id=session_id, limit=1),
            )
            if not artifacts:
                if attempt + 1 < _SESSION_ARTIFACT_RETRY_ATTEMPTS:
                    time.sleep(_SESSION_ARTIFACT_RETRY_DELAY_SECONDS)
                    continue
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
        except sqlite3.OperationalError as error:
            if attempt + 1 < _SESSION_ARTIFACT_RETRY_ATTEMPTS:
                time.sleep(_SESSION_ARTIFACT_RETRY_DELAY_SECONDS)
                continue
            return (
                None,
                'failed to reopen knowledge base for coverage persistence '
                f'({error})',
            )
        finally:
            if knowledge_base is not None:
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
    if payload.get('includes_python_subprocesses') is True:
        lines.append(
            '  python subprocesses are included in this measurement',
        )
    if payload.get('includes_worker_processes') is True:
        lines.append('  worker processes are included in this measurement')
    else:
        lines.append('  worker processes are not included in this measurement')
    console = Config.console_from_snapshot(config_snapshot)
    console.print_summary('Coverage', '\n'.join(lines))


def _emit_coverage_warning(
    message: str,
    *,
    config_snapshot=None,
) -> None:
    if config_snapshot is None:
        print(f'Coverage warning: {message}')
        return
    console = Config.console_from_snapshot(config_snapshot)
    console.print_summary('Coverage Warning', message)


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

    stripped_argv = instrumenter.strip_bootstrap_options(argv)
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
            _emit_coverage_warning(warning)

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
            _emit_coverage_warning(
                'no session metadata was written; coverage was not persisted.',
            )
            return int(completed.returncode)
        metadata_config_snapshot = _config_snapshot_from_metadata(metadata)

        try:
            summary = instrumenter.collect(workdir=workdir)
            updated_artifact, warning = _update_session_artifact(
                metadata,
                summary=summary,
            )
            if updated_artifact is None and warning is not None:
                _emit_coverage_warning(
                    f'coverage was collected but not persisted ({warning}).',
                    config_snapshot=metadata_config_snapshot,
                )
                if metadata_config_snapshot is not None:
                    _render_coverage_summary(
                        summary,
                        config_snapshot=metadata_config_snapshot,
                    )
            if updated_artifact is not None:
                _render_coverage_summary(
                    summary,
                    config_snapshot=updated_artifact.config_snapshot,
                )
        except Exception as error:
            cleanup_workdir = False
            _emit_coverage_warning(
                'failed to collect coverage '
                f'({error}). Preserved workdir: {workdir}',
            )
        return int(completed.returncode)
    finally:
        if cleanup_workdir:
            shutil.rmtree(workdir, ignore_errors=True)


_BOOTSTRAP_HANDLERS = ((_should_bootstrap_coverage, _bootstrap_coverage),)


def main(argv: list[str] | None = None) -> None:
    argv_list = list(sys.argv[1:] if argv is None else argv)
    for should_bootstrap, bootstrap in _BOOTSTRAP_HANDLERS:
        if should_bootstrap(argv_list):
            raise SystemExit(bootstrap(argv_list))

    raise SystemExit(_run_runner_cli(argv_list))
