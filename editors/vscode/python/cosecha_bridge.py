from __future__ import annotations

import json
import sqlite3
import sys

from dataclasses import asdict, is_dataclass
from pathlib import Path

from cosecha.core.knowledge_base import (
    DefinitionKnowledgeQuery,
    ReadOnlyPersistentKnowledgeBase,
    SessionArtifactQuery,
    TestKnowledgeQuery,
    resolve_knowledge_base_path,
)
from cosecha.workspace import (
    build_execution_context,
    resolve_workspace as resolve_effective_workspace,
)


def resolve_workspace_payload(start_path: str) -> dict[str, object]:
    effective_workspace = resolve_effective_workspace(
        start_path=Path(start_path),
    )
    execution_context = build_execution_context(effective_workspace)
    knowledge_base_path = resolve_knowledge_base_path(
        effective_workspace.workspace_root,
        knowledge_storage_root=execution_context.knowledge_storage_root,
    )
    return {
        'project_path': str(effective_workspace.workspace_root),
        'root_path': str(effective_workspace.knowledge_anchor),
        'workspace_root': str(effective_workspace.workspace_root),
        'knowledge_anchor': str(effective_workspace.knowledge_anchor),
        'execution_root': str(execution_context.execution_root),
        'manifest_path': (
            None
            if effective_workspace.manifest_path is None
            else str(effective_workspace.manifest_path)
        ),
        'knowledge_base_path': str(knowledge_base_path),
        'workspace_fingerprint': effective_workspace.fingerprint,
    }


def normalize_workspace_path(root_path: Path, raw_path: str | None) -> str | None:
    if raw_path is None:
        return None

    input_path = Path(raw_path)
    if input_path.is_absolute():
        resolved_path = input_path.resolve()
        try:
            return resolved_path.relative_to(root_path.resolve()).as_posix()
        except ValueError:
            return resolved_path.as_posix()

    if input_path.parts and input_path.parts[0] == root_path.name:
        return Path(*input_path.parts[1:]).as_posix()

    return input_path.as_posix()


def session_matches_workspace(
    artifact: object,
    workspace_payload: dict[str, object],
) -> bool:
    workspace_fingerprint = workspace_payload.get('workspace_fingerprint')
    artifact_fingerprint = getattr(artifact, 'workspace_fingerprint', None)
    if (
        isinstance(workspace_fingerprint, str)
        and workspace_fingerprint
        and isinstance(artifact_fingerprint, str)
        and artifact_fingerprint
    ):
        return artifact_fingerprint == workspace_fingerprint

    workspace_root = str(workspace_payload['root_path'])
    artifact_root = getattr(artifact, 'root_path', None)
    return isinstance(artifact_root, str) and artifact_root == workspace_root


def filtered_session_artifacts(
    knowledge_base: ReadOnlyPersistentKnowledgeBase,
    workspace_payload: dict[str, object],
    *,
    session_id: str | None = None,
    trace_id: str | None = None,
    limit: int | None = None,
) -> list[object]:
    query = SessionArtifactQuery(
        session_id=session_id,
        trace_id=trace_id,
    )
    artifacts: list[object] = []
    for artifact in knowledge_base.query_session_artifacts(query):
        if not session_matches_workspace(artifact, workspace_payload):
            continue
        artifacts.append(artifact)
        if limit is not None and len(artifacts) >= limit:
            break
    return artifacts


def serialize_structured_value(value: object | None) -> object | None:
    if value is None:
        return None

    if hasattr(value, 'to_dict'):
        return value.to_dict()

    if is_dataclass(value):
        return asdict(value)

    if hasattr(value, '__dict__'):
        return value.__dict__

    return value


def read_kb_metadata(db_path: Path) -> dict[str, object]:
    if not db_path.exists():
        return {
            'domain_event_count': 0,
            'latest_event_sequence_number': None,
            'latest_event_timestamp': None,
            'schema_version': None,
        }

    connection = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    try:
        schema_version_row = connection.execute(
            'SELECT value FROM meta WHERE key = ?',
            ('schema_version',),
        ).fetchone()
        domain_event_row = connection.execute(
            'SELECT COUNT(*), MAX(sequence_number), MAX(timestamp) '
            'FROM domain_event_log',
        ).fetchone()
    finally:
        connection.close()

    return {
        'domain_event_count': int(domain_event_row[0]),
        'latest_event_sequence_number': domain_event_row[1],
        'latest_event_timestamp': domain_event_row[2],
        'schema_version': (
            None if schema_version_row is None else schema_version_row[0]
        ),
    }


def serialize_summary_counts(
    counts: tuple[tuple[str, int], ...],
) -> dict[str, int]:
    return dict(counts)


def read_session_event_stats(
    db_path: Path,
    session_id: str | None,
) -> dict[str, object]:
    if session_id is None or not db_path.exists():
        return {
            'has_finished_event': False,
            'has_node_or_test_activity': False,
        }

    connection = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    try:
        rows = connection.execute(
            '''
            SELECT event_type, COUNT(*)
            FROM domain_event_log
            WHERE session_id = ?
            GROUP BY event_type
            ''',
            (session_id,),
        ).fetchall()
    finally:
        connection.close()

    counts = {
        str(event_type): int(count)
        for event_type, count in rows
    }
    return {
        'has_finished_event': counts.get('session.finished', 0) > 0,
        'has_node_or_test_activity': any(
            event_type.startswith('node.')
            or event_type.startswith('test.')
            or event_type.startswith('step.')
            for event_type in counts
        ),
    }


def _normalize_terminal_status_counts(
    status_counts: dict[str, int],
) -> dict[str, int]:
    pending_count = int(status_counts.get('pending', 0))
    if pending_count <= 0:
        return status_counts

    normalized = dict(status_counts)
    normalized['pending'] = 0
    normalized['skipped'] = int(normalized.get('skipped', 0)) + pending_count
    return normalized


def serialize_engine_summary(engine_summary: object) -> dict[str, object]:
    return {
        'detail_counts': serialize_summary_counts(
            getattr(engine_summary, 'detail_counts', ()),
        ),
        'engine_name': getattr(engine_summary, 'engine_name', ''),
        'failed_examples': list(getattr(engine_summary, 'failed_examples', ())),
        'failed_files': list(getattr(engine_summary, 'failed_files', ())),
        'failure_kind_counts': serialize_summary_counts(
            getattr(engine_summary, 'failure_kind_counts', ()),
        ),
        'status_counts': serialize_summary_counts(
            getattr(engine_summary, 'status_counts', ()),
        ),
        'total_tests': getattr(engine_summary, 'total_tests', 0),
    }


def serialize_session_summary(artifact: object) -> dict[str, object]:
    report_summary = getattr(artifact, 'report_summary', None)
    instrumentation_summaries: dict[str, object] = {}
    coverage_summary: dict[str, object] | None = None
    engine_summaries = ()
    live_engine_snapshots = ()
    failure_kind_counts: tuple[tuple[str, int], ...] = ()
    status_counts: tuple[tuple[str, int], ...] = ()
    failed_examples: tuple[str, ...] = ()
    failed_files: tuple[str, ...] = ()
    total_tests = 0
    if report_summary is not None:
        engine_summaries = getattr(report_summary, 'engine_summaries', ())
        live_engine_snapshots = getattr(
            report_summary,
            'live_engine_snapshots',
            (),
        )
        failure_kind_counts = getattr(
            report_summary,
            'failure_kind_counts',
            (),
        )
        status_counts = getattr(report_summary, 'status_counts', ())
        failed_examples = getattr(report_summary, 'failed_examples', ())
        failed_files = getattr(report_summary, 'failed_files', ())
        total_tests = getattr(report_summary, 'total_tests', 0)
        instrumentation_summaries = {
            name: summary.to_dict()
            for name, summary in getattr(
                report_summary,
                'instrumentation_summaries',
                {},
            ).items()
        }
        coverage_summary = instrumentation_summaries.get('coverage')

    coverage_total = None
    if isinstance(coverage_summary, dict):
        total_coverage = coverage_summary.get('payload', {}).get(
            'total_coverage',
        )
        if isinstance(total_coverage, int | float):
            coverage_total = float(total_coverage)

    live_snapshot_breakdown: dict[str, int] = {}
    for snapshot in live_engine_snapshots:
        breakdown_key = (
            f'{getattr(snapshot, "engine_name", "")}:'
            f'{getattr(snapshot, "snapshot_kind", "")}'
        )
        live_snapshot_breakdown[breakdown_key] = (
            live_snapshot_breakdown.get(breakdown_key, 0) + 1
        )

    return {
        'coverage_summary': coverage_summary,
        'coverage_total': coverage_total,
        'engine_count': len(engine_summaries),
        'engine_summaries': [
            serialize_engine_summary(engine_summary)
            for engine_summary in engine_summaries
        ],
        'live_engine_snapshot_summaries': [
            snapshot.to_dict() for snapshot in live_engine_snapshots
        ],
        'live_snapshot_breakdown': live_snapshot_breakdown,
        'live_snapshot_count': len(live_engine_snapshots),
        'failed_example_count': len(failed_examples),
        'failed_examples': list(failed_examples),
        'failed_file_count': len(failed_files),
        'failed_files': list(failed_files),
        'failure_kind_counts': serialize_summary_counts(
            failure_kind_counts,
        ),
        'has_failures': getattr(artifact, 'has_failures', None),
        'instrumentation_summaries': instrumentation_summaries,
        'plan_id': getattr(artifact, 'plan_id', None),
        'recorded_at': getattr(artifact, 'recorded_at', None),
        'root_path': getattr(artifact, 'root_path', None),
        'session_id': getattr(artifact, 'session_id', None),
        'status_counts': serialize_summary_counts(status_counts),
        'total_tests': total_tests,
        'trace_id': getattr(artifact, 'trace_id', None),
        'workspace_fingerprint': getattr(
            artifact,
            'workspace_fingerprint',
            None,
        ),
    }


def normalize_terminal_session_artifact_payload(
    payload: dict[str, object],
    *,
    db_path: Path,
) -> dict[str, object]:
    report_summary = payload.get('report_summary')
    if not isinstance(report_summary, dict):
        return payload

    status_counts = report_summary.get('status_counts')
    if not isinstance(status_counts, dict):
        return payload
    if int(status_counts.get('pending', 0)) <= 0:
        return payload

    session_stats = read_session_event_stats(
        db_path,
        getattr(payload, 'get', lambda _key, _default=None: None)(
            'session_id',
            None,
        ),
    )
    if not bool(session_stats.get('has_finished_event')):
        return payload
    if bool(session_stats.get('has_node_or_test_activity')):
        return payload

    normalized_payload = dict(payload)
    normalized_summary = dict(report_summary)
    normalized_summary['status_counts'] = _normalize_terminal_status_counts(
        status_counts,
    )

    engine_summaries = normalized_summary.get('engine_summaries')
    if isinstance(engine_summaries, list):
        normalized_engine_summaries: list[dict[str, object]] = []
        for engine_summary in engine_summaries:
            if not isinstance(engine_summary, dict):
                normalized_engine_summaries.append(engine_summary)
                continue
            engine_status_counts = engine_summary.get('status_counts')
            if not isinstance(engine_status_counts, dict):
                normalized_engine_summaries.append(dict(engine_summary))
                continue
            normalized_engine_summary = dict(engine_summary)
            normalized_engine_summary['status_counts'] = (
                _normalize_terminal_status_counts(engine_status_counts)
            )
            normalized_engine_summaries.append(normalized_engine_summary)
        normalized_summary['engine_summaries'] = normalized_engine_summaries

    normalized_payload['report_summary'] = normalized_summary
    return normalized_payload


def serialize_session_artifact(
    artifact: object,
    *,
    db_path: Path,
) -> dict[str, object]:
    payload = artifact.to_dict()
    payload['report_summary'] = serialize_session_summary(artifact)
    return normalize_terminal_session_artifact_payload(payload, db_path=db_path)


def main() -> int:
    request = json.load(sys.stdin)
    workspace = resolve_workspace_payload(str(request['start_path']))
    root_path = Path(str(workspace['root_path']))
    knowledge_base_path = Path(str(workspace['knowledge_base_path']))
    operation = str(request['operation'])

    if operation == 'describe_workspace':
        print(json.dumps(workspace, ensure_ascii=False))
        return 0

    if operation == 'describe_knowledge_base':
        exists = knowledge_base_path.exists()
        file_stats = knowledge_base_path.stat() if exists else None
        payload: dict[str, object] = {
            'exists': exists,
            'knowledge_base_path': str(knowledge_base_path),
            'manifest_path': workspace['manifest_path'],
            'project_path': workspace['project_path'],
            'root_path': workspace['root_path'],
            'size_bytes': None if file_stats is None else file_stats.st_size,
            'updated_at': None if file_stats is None else file_stats.st_mtime,
        }
        payload.update(read_kb_metadata(knowledge_base_path))

        if exists:
            knowledge_base = ReadOnlyPersistentKnowledgeBase(knowledge_base_path)
            try:
                snapshot = knowledge_base.snapshot()
                latest_artifacts = filtered_session_artifacts(
                    knowledge_base,
                    workspace,
                    limit=1,
                )
            finally:
                knowledge_base.close()

            payload['current_snapshot_counts'] = {
                'definitions': len(snapshot.definitions),
                'registry_snapshots': len(snapshot.registry_snapshots),
                'resources': len(snapshot.resources),
                'tests': len(snapshot.tests),
            }
            payload['latest_session'] = (
                serialize_structured_value(snapshot.session)
            )
            payload['latest_plan'] = (
                serialize_structured_value(snapshot.latest_plan)
            )
            payload['latest_session_artifact'] = (
                None
                if not latest_artifacts
                else serialize_session_artifact(
                    latest_artifacts[0],
                    db_path=knowledge_base_path,
                )
            )

        print(json.dumps(payload, ensure_ascii=False))
        return 0

    if not knowledge_base_path.exists():
        empty_payloads = {
            'query_tests': {'tests': [], 'workspace': workspace},
            'query_definitions': {'definitions': [], 'workspace': workspace},
            'list_recent_sessions': {'artifacts': [], 'workspace': workspace},
            'read_session_artifact': {'artifacts': [], 'workspace': workspace},
        }
        print(json.dumps(empty_payloads[operation], ensure_ascii=False))
        return 0

    knowledge_base = ReadOnlyPersistentKnowledgeBase(knowledge_base_path)
    try:
        if operation == 'query_tests':
            query = TestKnowledgeQuery(
                test_path=normalize_workspace_path(
                    root_path,
                    request.get('test_path'),
                ),
                limit=request.get('limit'),
            )
            payload = {
                'tests': [
                    test.to_dict() for test in knowledge_base.query_tests(query)
                ],
                'workspace': workspace,
            }
            print(json.dumps(payload, ensure_ascii=False))
            return 0

        if operation == 'query_definitions':
            query = DefinitionKnowledgeQuery(
                file_path=normalize_workspace_path(
                    root_path,
                    request.get('file_path'),
                ),
                limit=request.get('limit'),
            )
            payload = {
                'definitions': [
                    definition.to_dict()
                    for definition in knowledge_base.query_definitions(query)
                ],
                'workspace': workspace,
            }
            print(json.dumps(payload, ensure_ascii=False))
            return 0

        if operation == 'list_recent_sessions':
            payload = {
                'artifacts': [
                    serialize_session_artifact(
                        artifact,
                        db_path=knowledge_base_path,
                    )
                    for artifact in filtered_session_artifacts(
                        knowledge_base,
                        workspace,
                        limit=request.get('limit'),
                    )
                ],
                'workspace': workspace,
            }
            print(json.dumps(payload, ensure_ascii=False))
            return 0

        if operation == 'read_session_artifact':
            payload = {
                'artifacts': [
                    serialize_session_artifact(
                        artifact,
                        db_path=knowledge_base_path,
                    )
                    for artifact in filtered_session_artifacts(
                        knowledge_base,
                        workspace,
                        session_id=request.get('session_id'),
                        trace_id=request.get('trace_id'),
                        limit=request.get('limit'),
                    )
                ],
                'workspace': workspace,
            }
            print(json.dumps(payload, ensure_ascii=False))
            return 0

        raise ValueError('Unsupported operation: ' + operation)
    finally:
        knowledge_base.close()


if __name__ == '__main__':
    raise SystemExit(main())
