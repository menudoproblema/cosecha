from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import subprocess
import sys

from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Literal

from cosecha.core.config import Config
from cosecha.core.discovery import (
    create_loaded_discovery_registry,
    using_discovery_registry,
)
from cosecha.core.domain_events import deserialize_domain_event
from cosecha.core.knowledge_base import (
    DefinitionKnowledge,
    DefinitionKnowledgeQuery,
    DomainEventQuery,
    ReadOnlyPersistentKnowledgeBase,
    ResourceKnowledgeQuery,
    SessionArtifactQuery,
    TestKnowledge,
    TestKnowledgeQuery,
    decode_json_dict,
    iter_knowledge_base_file_paths,
)
from cosecha.core.operations import (
    AnalyzePlanOperation,
    KnowledgeQueryContext,
    OperationResult,
    QueryCapabilitiesOperation,
    QueryDefinitionsOperationResult,
    QueryEventsOperationResult,
    QueryRegistryItemsOperationResult,
    QueryResourcesOperationResult,
    QuerySessionArtifactsOperationResult,
    QueryTestsOperationResult,
    RunOperation,
)
from cosecha.core.registry_knowledge import RegistryKnowledgeQuery
from cosecha.core.runner import Runner
from cosecha.core.runtime import LocalRuntimeProvider
from cosecha.core.utils import setup_engines
from cosecha_mcp.workspace import (
    CosechaWorkspacePaths,
    normalize_workspace_relative_paths,
    resolve_cosecha_workspace,
)


type PlanningModeLiteral = Literal['relaxed', 'strict']
type SearchKind = Literal['definitions', 'registry', 'resources', 'tests']

_MCP_RESPONSE_BYTE_BUDGET = 48 * 1024
_MCP_INLINE_STRING_LENGTH = 320
_MCP_INLINE_LIST_ITEMS = 12
_MCP_INLINE_DICT_ITEMS = 20
_RUNNER_ENABLE_ENV = 'COSECHA_MCP_ENABLE_RUN_TESTS'


class CosechaMcpService:
    __slots__ = ('_default_start_path',)

    def __init__(
        self,
        default_start_path: str | Path | None = None,
    ) -> None:
        env_default = os.getenv('COSECHA_MCP_ROOT')
        if default_start_path is None and env_default:
            default_start_path = env_default
        self._default_start_path = default_start_path

    def describe_workspace(
        self,
        *,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        return workspace.to_dict()

    def describe_knowledge_base(
        self,
        *,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        db_path = workspace.knowledge_base_path
        exists = db_path.exists()
        file_stats = db_path.stat() if exists else None
        file_metadata = self._read_knowledge_base_file_metadata(db_path)

        payload: dict[str, object] = {
            'exists': exists,
            'knowledge_base_path': str(db_path),
            'manifest_path': (
                None
                if workspace.manifest_path is None
                else str(workspace.manifest_path)
            ),
            'project_path': str(workspace.project_path),
            'root_path': str(workspace.root_path),
            'size_bytes': None if file_stats is None else file_stats.st_size,
            'updated_at': (
                None if file_stats is None else file_stats.st_mtime
            ),
        }
        payload.update(file_metadata)

        if not exists:
            return payload

        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            snapshot = knowledge_base.snapshot()
            latest_artifact = knowledge_base.query_session_artifacts(
                SessionArtifactQuery(limit=1),
            )

        payload['current_snapshot_counts'] = {
            'definitions': len(snapshot.definitions),
            'registry_snapshots': len(snapshot.registry_snapshots),
            'resources': len(snapshot.resources),
            'tests': len(snapshot.tests),
        }
        payload['latest_session'] = (
            None if snapshot.session is None else asdict(snapshot.session)
        )
        payload['latest_plan'] = (
            None
            if snapshot.latest_plan is None
            else asdict(snapshot.latest_plan)
        )
        payload['latest_session_artifact'] = (
            None
            if not latest_artifact
            else latest_artifact[0].to_dict()
        )
        return payload

    def describe_path_freshness(
        self,
        *,
        path: str,
        engine_name: str | None = None,
        include_children: bool = True,
        limit: int = 200,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        scope_path = self._normalize_workspace_reference(
            workspace=workspace,
            raw_path=path,
        )

        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            tests = knowledge_base.query_tests(
                TestKnowledgeQuery(
                    engine_name=engine_name,
                ),
            )
            definitions = knowledge_base.query_definitions(
                DefinitionKnowledgeQuery(
                    engine_name=engine_name,
                ),
            )

        test_reports = self._build_freshness_reports(
            workspace=workspace,
            entries=tests,
            path_getter=lambda test: test.test_path,
            indexed_at_getter=lambda test: test.indexed_at,
            invalidated_at_getter=lambda test: test.invalidated_at,
            content_hash_getter=lambda test: test.content_hash,
            metadata_builder=self._build_test_freshness_metadata,
            scope_path=scope_path,
            include_children=include_children,
        )
        definition_reports = self._build_freshness_reports(
            workspace=workspace,
            entries=definitions,
            path_getter=lambda definition: definition.file_path,
            indexed_at_getter=lambda definition: definition.indexed_at,
            invalidated_at_getter=lambda definition: definition.invalidated_at,
            content_hash_getter=lambda definition: definition.content_hash,
            metadata_builder=self._build_definition_freshness_metadata,
            scope_path=scope_path,
            include_children=include_children,
        )

        return {
            'definitions': definition_reports[:limit],
            'scope_path': scope_path,
            'tests': test_reports[:limit],
            'workspace': workspace.to_dict(),
        }

    def search_catalog(
        self,
        query: str,
        *,
        kinds: list[SearchKind] | None = None,
        engine_name: str | None = None,
        limit: int = 20,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        normalized_query = query.strip().lower()
        if not normalized_query:
            msg = 'search_catalog requires a non-empty query'
            raise ValueError(msg)
        enabled_kinds = tuple(
            kinds or ('tests', 'definitions', 'resources', 'registry'),
        )
        search_limit = max(limit * 4, limit)
        matches: list[dict[str, object]] = []

        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            if 'tests' in enabled_kinds:
                tests = knowledge_base.query_tests(
                    TestKnowledgeQuery(
                        engine_name=engine_name,
                        limit=search_limit,
                    ),
                )
                matches.extend(
                    self._filter_search_matches(
                        kind='test',
                        entries=(test.to_dict() for test in tests),
                        query=normalized_query,
                        label_builder=lambda payload: str(
                            payload.get('test_name')
                            or payload.get('test_path'),
                        ),
                    ),
                )
            if 'definitions' in enabled_kinds:
                definitions = knowledge_base.query_definitions(
                    DefinitionKnowledgeQuery(
                        engine_name=engine_name,
                        limit=search_limit,
                    ),
                )
                matches.extend(
                    self._filter_search_matches(
                        kind='definition',
                        entries=(
                            definition.to_dict() for definition in definitions
                        ),
                        query=normalized_query,
                        label_builder=lambda payload: str(
                            payload.get('file_path') or '<unknown>',
                        ),
                    ),
                )
            if 'resources' in enabled_kinds:
                resources = knowledge_base.query_resources(
                    ResourceKnowledgeQuery(limit=search_limit),
                )
                matches.extend(
                    self._filter_search_matches(
                        kind='resource',
                        entries=(
                            resource.to_dict() for resource in resources
                        ),
                        query=normalized_query,
                        label_builder=lambda payload: str(
                            payload.get('name') or '<unknown>',
                        ),
                    ),
                )
            if 'registry' in enabled_kinds:
                registry_items = knowledge_base.query_registry_items(
                    RegistryKnowledgeQuery(
                        engine_name=engine_name,
                        limit=search_limit,
                    ),
                )
                matches.extend(
                    self._filter_search_matches(
                        kind='registry',
                        entries=(
                            snapshot.to_dict() for snapshot in registry_items
                        ),
                        query=normalized_query,
                        label_builder=lambda payload: str(
                            payload.get('module_spec')
                            or payload.get('layout_key'),
                        ),
                    ),
                )

        matches.sort(key=lambda match: (match['kind'], match['label']))
        payload = {
            'matches': matches[:limit],
            'matches_total_count': len(matches),
            'query': query,
            'workspace': workspace.to_dict(),
        }
        self._compact_list_response_field(
            payload,
            'matches',
            total_count=len(matches),
        )
        return payload

    async def query_tests(
        self,
        *,
        engine_name: str | None = None,
        test_path: str | None = None,
        status: str | None = None,
        failure_kind: str | None = None,
        node_stable_id: str | None = None,
        plan_id: str | None = None,
        limit: int | None = None,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        query = TestKnowledgeQuery(
            engine_name=engine_name,
            test_path=test_path,
            status=status,
            failure_kind=failure_kind,
            node_stable_id=node_stable_id,
            plan_id=plan_id,
            limit=limit,
        )
        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            result = QueryTestsOperationResult(
                tests=knowledge_base.query_tests(query),
                context=self._persistent_knowledge_query_context(),
            )
        return self._serialize_operation_result(workspace, result)

    async def query_definitions(
        self,
        *,
        engine_name: str | None = None,
        file_path: str | None = None,
        step_type: str | None = None,
        step_text: str | None = None,
        discovery_mode: str | None = None,
        include_invalidated: bool = True,
        limit: int | None = None,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        query = DefinitionKnowledgeQuery(
            engine_name=engine_name,
            file_path=file_path,
            step_type=step_type,
            step_text=step_text,
            discovery_mode=discovery_mode,
            include_invalidated=include_invalidated,
            limit=limit,
        )
        registry = create_loaded_discovery_registry()
        with (
            using_discovery_registry(registry),
            self._open_readonly_knowledge_base(workspace) as knowledge_base,
        ):
            result = QueryDefinitionsOperationResult(
                definitions=knowledge_base.query_definitions(query),
                context=self._persistent_knowledge_query_context(),
            )
        return self._serialize_operation_result(workspace, result)

    async def query_registry_items(
        self,
        *,
        engine_name: str | None = None,
        module_spec: str | None = None,
        package_hash: str | None = None,
        layout_key: str | None = None,
        loader_schema_version: str | None = None,
        limit: int | None = None,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        query = RegistryKnowledgeQuery(
            engine_name=engine_name,
            module_spec=module_spec,
            package_hash=package_hash,
            layout_key=layout_key,
            loader_schema_version=loader_schema_version,
            limit=limit,
        )
        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            result = QueryRegistryItemsOperationResult(
                registry_snapshots=knowledge_base.query_registry_items(query),
                context=self._persistent_knowledge_query_context(),
            )
        return self._serialize_operation_result(workspace, result)

    async def query_resources(
        self,
        *,
        name: str | None = None,
        scope: str | None = None,
        last_test_id: str | None = None,
        limit: int | None = None,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        query = ResourceKnowledgeQuery(
            name=name,
            scope=scope,
            last_test_id=last_test_id,
            limit=limit,
        )
        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            result = QueryResourcesOperationResult(
                resources=knowledge_base.query_resources(query),
                context=self._persistent_knowledge_query_context(),
            )
        return self._serialize_operation_result(workspace, result)

    async def read_session_artifacts(
        self,
        *,
        session_id: str | None = 'last',
        trace_id: str | None = None,
        limit: int | None = None,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        resolved_session_id = self._resolve_session_id(
            workspace,
            requested_session_id=session_id,
        )
        query = SessionArtifactQuery(
            session_id=resolved_session_id,
            trace_id=trace_id,
            limit=limit,
        )
        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            result = QuerySessionArtifactsOperationResult(
                artifacts=knowledge_base.query_session_artifacts(query),
                context=self._persistent_knowledge_query_context(),
            )
        return self._serialize_operation_result(workspace, result)

    async def list_recent_sessions(
        self,
        *,
        limit: int = 20,
        start_path: str | None = None,
    ) -> dict[str, object]:
        return await self.read_session_artifacts(
            session_id=None,
            limit=limit,
            start_path=start_path,
        )

    async def describe_session_coverage(
        self,
        *,
        session_id: str | None = 'last',
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        if session_id is None:
            return {
                'workspace': workspace.to_dict(),
                'session_id': None,
                'has_coverage': False,
                'coverage_summary': None,
                'total_coverage': None,
                'recorded_at': None,
                'reason': (
                    "session_id must be 'last' or an explicit session id; "
                    'use list_coverage_history to enumerate sessions'
                ),
            }
        resolved_session_id = self._resolve_session_id(
            workspace,
            requested_session_id=session_id,
        )
        payload: dict[str, object] = {
            'workspace': workspace.to_dict(),
            'session_id': resolved_session_id,
            'has_coverage': False,
            'coverage_summary': None,
            'total_coverage': None,
            'recorded_at': None,
        }
        if resolved_session_id is None:
            payload['reason'] = 'no session recorded yet'
            return payload

        query = SessionArtifactQuery(
            session_id=resolved_session_id,
            limit=1,
        )
        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            artifacts = knowledge_base.query_session_artifacts(query)
        if not artifacts:
            payload['reason'] = (
                f'session artifact not found for {resolved_session_id}'
            )
            return payload

        artifact = artifacts[0]
        payload['recorded_at'] = artifact.recorded_at
        coverage_summary = self._extract_coverage_summary(artifact)
        if coverage_summary is None:
            payload['reason'] = 'session has no coverage instrumentation'
            return payload

        payload['has_coverage'] = True
        payload['coverage_summary'] = coverage_summary.to_dict()
        payload['total_coverage'] = coverage_summary.payload.get(
            'total_coverage',
        )
        return payload

    async def list_coverage_history(
        self,
        *,
        limit: int = 20,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        query = SessionArtifactQuery(limit=limit)
        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            artifacts = knowledge_base.query_session_artifacts(query)

        entries: list[dict[str, object]] = []
        for artifact in artifacts:
            coverage_summary = self._extract_coverage_summary(artifact)
            if coverage_summary is None:
                continue
            entry: dict[str, object] = {
                'session_id': artifact.session_id,
                'recorded_at': artifact.recorded_at,
                'has_failures': artifact.has_failures,
                'total_coverage': coverage_summary.payload.get(
                    'total_coverage',
                ),
                'branch': coverage_summary.payload.get('branch'),
                'source_targets': coverage_summary.payload.get(
                    'source_targets',
                ),
                'measurement_scope': coverage_summary.payload.get(
                    'measurement_scope',
                ),
            }
            entries.append(entry)

        return {
            'workspace': workspace.to_dict(),
            'entries': entries,
            'entries_returned_count': len(entries),
        }

    @staticmethod
    def _extract_coverage_summary(artifact):
        # Hardcoded for the single v1 instrumenter. Do NOT generalize this
        # into a `_extract_instrumentation_summary(kind=...)` helper until a
        # second real instrumenter exists in cosecha.instrumentation: the
        # right shape of that helper (and of the MCP surface) can only be
        # decided from a concrete second use case, not from speculation.
        report_summary = getattr(artifact, 'report_summary', None)
        if report_summary is None:
            return None
        summaries = getattr(
            report_summary,
            'instrumentation_summaries',
            None,
        )
        if not summaries:
            return None
        return summaries.get('coverage')

    async def inspect_test_plan(
        self,
        *,
        test_path: str | None = None,
        paths: list[str] | None = None,
        selection_labels: list[str] | None = None,
        test_limit: int | None = None,
        mode: PlanningModeLiteral = 'relaxed',
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        selected_paths = list(paths or ())
        if test_path is not None:
            selected_paths.append(test_path)
        operation = AnalyzePlanOperation(
            paths=normalize_workspace_relative_paths(
                root_path=workspace.root_path,
                raw_paths=selected_paths,
            ),
            selection_labels=tuple(selection_labels or ()),
            test_limit=test_limit,
            mode=mode,
        )
        response = await self._execute_runner_operation_in_subprocess(
            workspace,
            operation=operation,
        )
        return self._serialize_operation_result(
            workspace,
            response['result'],
        )

    async def get_execution_timeline(
        self,
        *,
        session_id: str | None = 'last',
        plan_id: str | None = None,
        node_stable_id: str | None = None,
        event_type: str | None = None,
        limit: int | None = 200,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        resolved_session_id = self._resolve_session_id(
            workspace,
            requested_session_id=session_id,
        )
        query = DomainEventQuery(
            event_type=event_type,
            session_id=resolved_session_id,
            plan_id=plan_id,
            node_stable_id=node_stable_id,
            limit=limit,
        )
        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            result = QueryEventsOperationResult(
                events=knowledge_base.query_domain_events(query),
                context=self._persistent_knowledge_query_context(),
            )
        return self._serialize_operation_result(workspace, result)

    def list_test_execution_history(
        self,
        *,
        test_path: str | None = None,
        engine_name: str | None = None,
        status: str | None = None,
        session_id: str | None = None,
        limit: int = 100,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        resolved_session_id = self._resolve_session_id(
            workspace,
            requested_session_id=session_id,
        )
        events = self._load_recent_test_finished_events(
            workspace=workspace,
            session_id=resolved_session_id,
            limit=limit * 4,
        )

        history: list[dict[str, object]] = []
        for event in events:
            if engine_name is not None and event.engine_name != engine_name:
                continue
            if test_path is not None and event.test_path != test_path:
                continue
            if status is not None and event.status != status:
                continue
            history.append(
                {
                    'duration': event.duration,
                    'engine_name': event.engine_name,
                    'error_code': event.error_code,
                    'failure_kind': event.failure_kind,
                    'node_id': event.node_id,
                    'node_stable_id': event.node_stable_id,
                    'plan_id': event.metadata.plan_id,
                    'session_id': event.metadata.session_id,
                    'status': event.status,
                    'test_name': event.test_name,
                    'test_path': event.test_path,
                    'timestamp': event.timestamp,
                    'trace_id': event.metadata.trace_id,
                },
            )
            if len(history) >= limit:
                break

        payload = {
            'history': history,
            'workspace': workspace.to_dict(),
        }
        self._compact_list_response_field(payload, 'history')
        return payload

    async def list_engines_and_capabilities(
        self,
        *,
        selected_engines: list[str] | None = None,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        response = await self._execute_runner_operation_in_subprocess(
            workspace,
            operation=QueryCapabilitiesOperation(),
            selected_engine_names=(
                set(selected_engines) if selected_engines else None
            ),
        )
        serialized = self._serialize_operation_result(
            workspace,
            response['result'],
        )
        serialized['engine_names'] = sorted(
            str(engine_name)
            for engine_name in response.get('engine_names', [])
        )
        return serialized

    async def refresh_knowledge_base(
        self,
        *,
        paths: list[str] | None = None,
        selection_labels: list[str] | None = None,
        test_limit: int | None = None,
        mode: PlanningModeLiteral = 'strict',
        rebuild: bool = False,
        start_path: str | None = None,
    ) -> dict[str, object]:
        workspace = self._resolve_workspace(start_path=start_path)
        if rebuild:
            for candidate in iter_knowledge_base_file_paths(
                workspace.knowledge_base_path,
            ):
                if candidate.exists():
                    candidate.unlink()

        operation = AnalyzePlanOperation(
            paths=normalize_workspace_relative_paths(
                root_path=workspace.root_path,
                raw_paths=paths,
            ),
            selection_labels=tuple(selection_labels or ()),
            test_limit=test_limit,
            mode=mode,
        )
        response = await self._execute_runner_operation_in_subprocess(
            workspace,
            operation=operation,
        )
        payload = self._serialize_operation_result(
            workspace,
            response['result'],
        )
        payload['knowledge_base'] = self.describe_knowledge_base(
            start_path=start_path,
        )
        payload['rebuild'] = rebuild
        return payload

    async def run_tests(
        self,
        *,
        paths: list[str] | None = None,
        selection_labels: list[str] | None = None,
        test_limit: int | None = None,
        selected_engines: list[str] | None = None,
        start_path: str | None = None,
    ) -> dict[str, object]:
        self._ensure_run_tests_enabled()
        workspace = self._resolve_workspace(start_path=start_path)
        operation = RunOperation(
            paths=normalize_workspace_relative_paths(
                root_path=workspace.root_path,
                raw_paths=paths,
            ),
            selection_labels=tuple(selection_labels or ()),
            test_limit=test_limit,
        )
        response = await self._execute_runner_operation_in_subprocess(
            workspace,
            operation=operation,
            selected_engine_names=(
                set(selected_engines) if selected_engines else None
            ),
        )
        payload = self._serialize_operation_result(
            workspace,
            response['result'],
        )
        payload['knowledge_base'] = self.describe_knowledge_base(
            start_path=start_path,
        )
        payload['selected_engines'] = selected_engines or []
        return payload

    def _resolve_workspace(
        self,
        *,
        start_path: str | None = None,
    ) -> CosechaWorkspacePaths:
        return resolve_cosecha_workspace(
            start_path or self._default_start_path,
        )

    @contextmanager
    def _open_readonly_knowledge_base(
        self,
        workspace: CosechaWorkspacePaths,
    ):
        knowledge_base = ReadOnlyPersistentKnowledgeBase(
            workspace.knowledge_base_path,
        )
        try:
            yield knowledge_base
        finally:
            knowledge_base.close()

    def _build_config(self, workspace: CosechaWorkspacePaths) -> Config:
        return Config(
            root_path=workspace.root_path,
            capture_log=False,
        )

    def _build_runner(self, workspace: CosechaWorkspacePaths) -> Runner:
        return self._build_runner_for_selection(
            workspace,
            selected_engine_names=None,
        )

    def _build_runner_for_selection(
        self,
        workspace: CosechaWorkspacePaths,
        *,
        selected_engine_names: set[str] | None,
    ) -> Runner:
        config = self._build_config(workspace)
        registry = create_loaded_discovery_registry()
        with using_discovery_registry(registry):
            hooks, engines = setup_engines(
                config,
                manifest_file=workspace.manifest_path,
                selected_engine_names=selected_engine_names,
            )
            return Runner(
                config,
                engines,
                hooks,
                runtime_provider=LocalRuntimeProvider(),
            )

    def _resolve_session_id(
        self,
        workspace: CosechaWorkspacePaths,
        *,
        requested_session_id: str | None,
    ) -> str | None:
        if requested_session_id not in {'last', None}:
            return requested_session_id

        with self._open_readonly_knowledge_base(workspace) as knowledge_base:
            session = knowledge_base.snapshot().session
        if requested_session_id is None:
            return None
        if session is None:
            return None
        return session.session_id

    def _persistent_knowledge_query_context(self) -> KnowledgeQueryContext:
        return KnowledgeQueryContext(
            source='persistent_knowledge_base',
            freshness='unknown',
        )

    def _serialize_operation_result(
        self,
        workspace: CosechaWorkspacePaths,
        result: OperationResult | dict[str, object],
    ) -> dict[str, object]:
        serialized_result = (
            result.to_dict()
            if hasattr(result, 'to_dict')
            else dict(result)
        )
        self._compact_known_response_fields(serialized_result)
        serialized_result['workspace'] = workspace.to_dict()
        return serialized_result

    async def _execute_runner_operation_in_subprocess(
        self,
        workspace: CosechaWorkspacePaths,
        *,
        operation,
        selected_engine_names: set[str] | None = None,
    ) -> dict[str, object]:
        python_executable = self._resolve_workspace_python_executable(workspace)
        request_payload = {
            'operation': operation.to_dict(),
            'selected_engine_names': sorted(selected_engine_names or ()),
            'start_path': str(workspace.project_path),
        }
        return await self._run_runner_subprocess_request(
            workspace,
            python_executable=python_executable,
            request_payload=request_payload,
        )

    async def _run_runner_subprocess_request(
        self,
        workspace: CosechaWorkspacePaths,
        *,
        python_executable: str,
        request_payload: dict[str, object],
    ) -> dict[str, object]:
        process = await asyncio.create_subprocess_exec(
            python_executable,
            '-m',
            'cosecha_mcp.worker',
            cwd=str(workspace.project_path),
            env=self._build_workspace_subprocess_env(
                workspace,
                python_executable=python_executable,
            ),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate(
            json.dumps(request_payload, ensure_ascii=False).encode('utf-8'),
        )
        return self._parse_runner_subprocess_response(
            workspace,
            python_executable=python_executable,
            returncode=process.returncode,
            stdout=stdout,
            stderr=stderr,
        )

    def _parse_runner_subprocess_response(
        self,
        workspace: CosechaWorkspacePaths,
        *,
        python_executable: str,
        returncode: int | None,
        stdout: bytes,
        stderr: bytes,
    ) -> dict[str, object]:
        stderr_text = stderr.decode('utf-8', errors='replace').strip()
        stdout_text = stdout.decode('utf-8', errors='replace').strip()
        if returncode != 0:
            msg = (
                'Cosecha MCP worker process failed for '
                f'{workspace.project_path} using {python_executable}'
            )
            if stderr_text:
                msg = f'{msg}.\nstderr:\n{stderr_text}'
            raise RuntimeError(msg)

        if not stdout_text:
            msg = (
                'Cosecha MCP worker process returned an empty payload for '
                f'{workspace.project_path}'
            )
            if stderr_text:
                msg = f'{msg}.\nstderr:\n{stderr_text}'
            raise RuntimeError(msg)

        try:
            parsed = json.loads(stdout_text)
        except json.JSONDecodeError as error:
            msg = (
                'Cosecha MCP worker process returned invalid JSON for '
                f'{workspace.project_path}'
            )
            if stderr_text:
                msg = f'{msg}.\nstderr:\n{stderr_text}'
            raise RuntimeError(msg) from error

        if not isinstance(parsed, dict):
            msg = 'Cosecha MCP worker response must be a JSON object'
            raise RuntimeError(msg)
        return parsed

    def _compact_known_response_fields(
        self,
        payload: dict[str, object],
    ) -> None:
        for field_name in (
            'events',
            'tests',
            'definitions',
            'registry_snapshots',
            'resources',
            'artifacts',
            'snapshots',
            'matches',
            'history',
        ):
            self._compact_list_response_field(payload, field_name)

        analysis = payload.get('analysis')
        if isinstance(analysis, dict):
            for field_name in ('issues', 'node_semantics', 'plan'):
                self._compact_list_response_field(analysis, field_name)

    def _compact_list_response_field(
        self,
        payload: dict[str, object],
        field_name: str,
        *,
        total_count: int | None = None,
    ) -> None:
        records = payload.get(field_name)
        if not isinstance(records, list):
            return

        compacted_records = [
            self._compact_json_value(record) for record in records
        ]
        base_payload = dict(payload)
        base_payload[field_name] = []
        byte_budget = (
            _MCP_RESPONSE_BYTE_BUDGET
            - self._json_size_bytes(base_payload)
        )
        byte_budget = max(0, byte_budget)

        returned_records: list[object] = []
        consumed_bytes = 0
        for record in compacted_records:
            record_size = self._json_size_bytes(record)
            if returned_records and consumed_bytes + record_size > byte_budget:
                break
            returned_records.append(record)
            consumed_bytes += record_size

        payload[field_name] = returned_records
        effective_total_count = (
            len(records) if total_count is None else total_count
        )
        payload[f'{field_name}_returned_count'] = len(returned_records)
        payload[f'{field_name}_total_count'] = effective_total_count
        omitted_count = max(0, effective_total_count - len(returned_records))
        if omitted_count == 0:
            return

        payload[f'{field_name}_omitted_count'] = omitted_count
        payload['truncated'] = True
        if field_name != 'events' or not returned_records:
            return

        last_event = returned_records[-1]
        if not isinstance(last_event, dict):
            return
        metadata = last_event.get('metadata')
        if not isinstance(metadata, dict):
            return
        sequence_number = metadata.get('sequence_number')
        if isinstance(sequence_number, int):
            payload['next_after_sequence_number'] = sequence_number

    def _compact_json_value(
        self,
        value: object,
        *,
        depth: int = 0,
    ) -> object:
        if isinstance(value, dict):
            compacted: dict[str, object] = {}
            items = list(value.items())
            max_items = max(4, _MCP_INLINE_DICT_ITEMS - (depth * 4))
            for key, child in items[:max_items]:
                compacted[str(key)] = self._compact_json_value(
                    child,
                    depth=depth + 1,
                )
            omitted_count = len(items) - len(compacted)
            if omitted_count > 0:
                compacted['_truncated_key_count'] = omitted_count
            return compacted

        if isinstance(value, (list, tuple)):
            sequence = list(value)
            max_items = max(4, _MCP_INLINE_LIST_ITEMS - (depth * 2))
            compacted_sequence = [
                self._compact_json_value(item, depth=depth + 1)
                for item in sequence[:max_items]
            ]
            omitted_count = len(sequence) - len(compacted_sequence)
            if omitted_count > 0:
                compacted_sequence.append(
                    {'_truncated_item_count': omitted_count},
                )
            return compacted_sequence

        if isinstance(value, str) and len(value) > _MCP_INLINE_STRING_LENGTH:
            return value[:_MCP_INLINE_STRING_LENGTH] + '...'

        return value

    def _json_size_bytes(self, value: object) -> int:
        return len(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
            ).encode('utf-8'),
        )

    def _ensure_run_tests_enabled(self) -> None:
        raw_value = os.getenv(_RUNNER_ENABLE_ENV, '')
        if raw_value.lower() in {'1', 'true', 'yes', 'on'}:
            return
        msg = (
            'run_tests is disabled by default in cosecha-mcp. '
            f'Set {_RUNNER_ENABLE_ENV}=1 to allow test execution.'
        )
        raise PermissionError(msg)

    def _read_knowledge_base_file_metadata(
        self,
        db_path: Path,
    ) -> dict[str, object]:
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

    def _filter_search_matches(
        self,
        *,
        kind: str,
        entries,
        query: str,
        label_builder,
    ) -> list[dict[str, object]]:
        matches: list[dict[str, object]] = []
        for payload in entries:
            rendered_payload = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
            ).lower()
            if query not in rendered_payload:
                continue
            matches.append(
                {
                    'kind': kind,
                    'label': label_builder(payload),
                    'payload': payload,
                },
            )
        return matches

    def _normalize_workspace_reference(
        self,
        *,
        workspace: CosechaWorkspacePaths,
        raw_path: str,
    ) -> str:
        input_path = Path(raw_path)
        if input_path.is_absolute():
            return os.path.relpath(
                str(input_path.resolve()),
                workspace.root_path,
            )
        return input_path.as_posix()

    def _matches_scope(
        self,
        *,
        candidate_path: str,
        scope_path: str,
        include_children: bool,
    ) -> bool:
        if candidate_path == scope_path:
            return True
        if not include_children:
            return False
        return candidate_path.startswith(scope_path.rstrip('/') + '/')

    def _build_freshness_reports(
        self,
        *,
        workspace: CosechaWorkspacePaths,
        entries,
        path_getter,
        indexed_at_getter,
        invalidated_at_getter,
        content_hash_getter,
        metadata_builder,
        scope_path: str,
        include_children: bool,
    ) -> list[dict[str, object]]:
        grouped_entries: dict[str, list[object]] = {}
        for entry in entries:
            entry_path = path_getter(entry)
            if not self._matches_scope(
                candidate_path=entry_path,
                scope_path=scope_path,
                include_children=include_children,
            ):
                continue
            grouped_entries.setdefault(entry_path, []).append(entry)

        reports: list[dict[str, object]] = []
        for entry_path, grouped in sorted(grouped_entries.items()):
            resolved_file_path = (workspace.root_path / entry_path).resolve()
            file_exists = resolved_file_path.exists()
            file_mtime = (
                resolved_file_path.stat().st_mtime if file_exists else None
            )
            current_hash = (
                self._build_file_hash(resolved_file_path)
                if file_exists and resolved_file_path.is_file()
                else None
            )
            indexed_at_values = [
                indexed_at
                for indexed_at in (
                    indexed_at_getter(entry) for entry in grouped
                )
                if indexed_at is not None
            ]
            invalidated_at_values = [
                invalidated_at
                for invalidated_at in (
                    invalidated_at_getter(entry) for entry in grouped
                )
                if invalidated_at is not None
            ]
            persisted_hashes = sorted(
                {
                    persisted_hash
                    for persisted_hash in (
                        content_hash_getter(entry) for entry in grouped
                    )
                    if persisted_hash is not None
                },
            )
            latest_indexed_at = (
                max(indexed_at_values) if indexed_at_values else None
            )
            latest_invalidated_at = (
                max(invalidated_at_values) if invalidated_at_values else None
            )
            content_hash_matches = (
                None
                if current_hash is None or not persisted_hashes
                else current_hash in persisted_hashes
            )
            stale_reasons: list[str] = []
            if not file_exists:
                stale_reasons.append('missing_on_disk')
            if latest_invalidated_at is not None:
                stale_reasons.append('invalidated')
            if (
                file_mtime is not None
                and latest_indexed_at is not None
                and file_mtime > latest_indexed_at
            ):
                stale_reasons.append('mtime_after_index')
            if content_hash_matches is False:
                stale_reasons.append('content_hash_mismatch')

            reports.append(
                {
                    'content_hash_matches': content_hash_matches,
                    'current_hash': current_hash,
                    'entry_count': len(grouped),
                    'exists_on_disk': file_exists,
                    'file_mtime': file_mtime,
                    'file_path': entry_path,
                    'freshness': (
                        'fresh' if not stale_reasons else 'stale'
                    ),
                    'latest_indexed_at': latest_indexed_at,
                    'latest_invalidated_at': latest_invalidated_at,
                    'persisted_hashes': persisted_hashes,
                    'resolved_file_path': str(resolved_file_path),
                    'stale_reasons': stale_reasons,
                    **metadata_builder(grouped),
                },
            )

        return reports

    def _build_test_freshness_metadata(
        self,
        entries: list[TestKnowledge],
    ) -> dict[str, object]:
        status_counts = Counter(
            entry.status or 'unknown' for entry in entries
        )
        return {
            'engine_names': sorted({entry.engine_name for entry in entries}),
            'status_counts': dict(sorted(status_counts.items())),
            'test_names': sorted({entry.test_name for entry in entries}),
        }

    def _build_definition_freshness_metadata(
        self,
        entries: list[DefinitionKnowledge],
    ) -> dict[str, object]:
        descriptor_count = sum(
            len(entry.descriptors)
            for entry in entries
        )
        return {
            'definition_count': descriptor_count,
            'discovery_modes': sorted(
                {entry.discovery_mode for entry in entries},
            ),
            'engine_names': sorted({entry.engine_name for entry in entries}),
            'payload_kinds': sorted(
                {
                    descriptor.payload_kind
                    for entry in entries
                    for descriptor in entry.descriptors
                },
            ),
        }

    def _build_file_hash(self, file_path: Path) -> str:
        return hashlib.sha256(file_path.read_bytes()).hexdigest()

    def _load_recent_test_finished_events(
        self,
        *,
        workspace: CosechaWorkspacePaths,
        session_id: str | None,
        limit: int,
    ):
        connection = sqlite3.connect(
            f'file:{workspace.knowledge_base_path}?mode=ro',
            uri=True,
        )
        try:
            sql = [
                'SELECT payload_json FROM domain_event_log',
                'WHERE event_type = ?',
            ]
            parameters: list[object] = ['test.finished']
            if session_id is not None:
                sql.append('AND session_id = ?')
                parameters.append(session_id)
            sql.append('ORDER BY sequence_number DESC LIMIT ?')
            parameters.append(limit)
            rows = connection.execute(
                ' '.join(sql),
                tuple(parameters),
            ).fetchall()
        finally:
            connection.close()

        return tuple(
            deserialize_domain_event(decode_json_dict(str(row[0])))
            for row in rows
        )

    def _close_runner(self, runner: Runner) -> None:
        knowledge_base = getattr(runner, '_knowledge_base', None)
        if knowledge_base is not None:
            knowledge_base.close()

    def _discover_workspace_source_paths(
        self,
        workspace: CosechaWorkspacePaths,
    ) -> tuple[Path, ...]:
        search_paths: list[Path] = []

        workspace_root = workspace.project_path.parent
        for candidate in (
            workspace.project_path / 'src',
            workspace.project_path / 'tests',
            workspace.project_path,
        ):
            if candidate.exists():
                search_paths.append(candidate.resolve())

        if workspace_root.exists():
            for sibling in sorted(workspace_root.iterdir()):
                if not sibling.is_dir() or sibling == workspace.project_path:
                    continue
                for child_name in ('src', 'tests'):
                    candidate = sibling / child_name
                    if candidate.exists():
                        search_paths.append(candidate.resolve())

        return self._dedupe_paths(search_paths)

    def _discover_workspace_site_packages(
        self,
        workspace: CosechaWorkspacePaths,
    ) -> tuple[Path, ...]:
        version_name = (
            f'python{sys.version_info.major}.{sys.version_info.minor}'
        )
        search_paths: list[Path] = []

        for venv_root in (
            workspace.project_path,
            workspace.project_path.parent,
        ):
            if not venv_root.exists():
                continue
            for pattern in (
                f'.venv/lib/{version_name}/site-packages',
                f'venv/lib/{version_name}/site-packages',
                (
                    'venv'
                    f'{sys.version_info.major}.{sys.version_info.minor}'
                    f'/lib/{version_name}/site-packages'
                ),
                (
                    'venv'
                    f'{sys.version_info.major}{sys.version_info.minor}'
                    f'/lib/{version_name}/site-packages'
                ),
            ):
                search_paths.extend(
                    candidate.resolve()
                    for candidate in sorted(venv_root.glob(pattern))
                    if candidate.exists()
                )

        return self._dedupe_paths(search_paths)

    def _discover_workspace_import_paths(
        self,
        workspace: CosechaWorkspacePaths,
    ) -> tuple[Path, ...]:
        return self._dedupe_paths(
            (
                *self._discover_workspace_source_paths(workspace),
                *self._discover_workspace_site_packages(workspace),
            ),
        )

    def _discover_workspace_python_executables(
        self,
        workspace: CosechaWorkspacePaths,
    ) -> tuple[Path, ...]:
        candidates: list[Path] = []
        for venv_root in (
            workspace.project_path,
            workspace.project_path.parent,
        ):
            if not venv_root.exists():
                continue
            for relative_path in (
                '.venv/bin/python',
                'venv/bin/python',
                f'venv{sys.version_info.major}.{sys.version_info.minor}/bin/python',
                f'venv{sys.version_info.major}{sys.version_info.minor}/bin/python',
                '.venv/Scripts/python.exe',
                'venv/Scripts/python.exe',
                f'venv{sys.version_info.major}.{sys.version_info.minor}/Scripts/python.exe',
                f'venv{sys.version_info.major}{sys.version_info.minor}/Scripts/python.exe',
            ):
                candidate = (venv_root / relative_path).resolve()
                if candidate.exists() and os.access(candidate, os.X_OK):
                    candidates.append(candidate)

        return self._dedupe_paths(candidates)

    def _resolve_workspace_python_executable(
        self,
        workspace: CosechaWorkspacePaths,
    ) -> str:
        current_version = f'{sys.version_info.major}.{sys.version_info.minor}'
        discovered_candidates = self._discover_workspace_python_executables(
            workspace,
        )
        if not discovered_candidates:
            return sys.executable

        mismatched_versions: list[str] = []
        for candidate in discovered_candidates:
            candidate_version = self._read_python_version(candidate)
            if candidate_version == current_version:
                return str(candidate)
            mismatched_versions.append(
                f'{candidate} ({candidate_version or "unknown"})',
            )

        msg = (
            'Cosecha MCP found workspace virtualenvs but none match the MCP '
            f'interpreter ABI {current_version}. '
            'Runner-backed tools require a matching interpreter to avoid '
            'cross-version imports. Candidates: '
            + ', '.join(mismatched_versions)
        )
        raise RuntimeError(msg)

    def _read_python_version(self, python_executable: Path) -> str | None:
        completed = subprocess.run(
            [
                str(python_executable),
                '-c',
                (
                    'import sys; '
                    'print(f"{sys.version_info.major}.{sys.version_info.minor}")'
                ),
            ],
            capture_output=True,
            check=False,
            text=True,
        )
        if completed.returncode != 0:
            return None
        version = completed.stdout.strip()
        return version or None

    def _build_workspace_subprocess_env(
        self,
        workspace: CosechaWorkspacePaths,
        *,
        python_executable: str,
    ) -> dict[str, str]:
        env = os.environ.copy()
        pythonpath_entries = [
            str(path)
            for path in self._discover_workspace_source_paths(workspace)
        ]
        if self._is_monorepo_checkout():
            pythonpath_entries.extend(
                str(path) for path in self._discover_repo_source_paths()
            )
        if Path(python_executable).resolve() == Path(sys.executable).resolve():
            pythonpath_entries.extend(
                str(path)
                for path in self._discover_workspace_site_packages(
                    workspace,
                )
            )

        current_pythonpath = env.get('PYTHONPATH')
        if current_pythonpath:
            pythonpath_entries.extend(
                entry
                for entry in current_pythonpath.split(os.pathsep)
                if entry
            )

        env['PYTHONPATH'] = os.pathsep.join(
            str(path) for path in self._dedupe_paths(
                Path(entry) for entry in pythonpath_entries if entry
            )
        )
        return env

    def _is_monorepo_checkout(self) -> bool:
        repo_root = Path(__file__).resolve().parents[4]
        packages_root = repo_root / 'packages'
        pyproject_path = repo_root / 'pyproject.toml'
        return (
            packages_root.exists()
            and pyproject_path.exists()
            and (packages_root / 'cosecha-mcp' / 'src').exists()
            and (packages_root / 'cosecha-core' / 'src').exists()
        )

    def _discover_repo_source_paths(self) -> tuple[Path, ...]:
        repo_root = Path(__file__).resolve().parents[4]
        packages_root = repo_root / 'packages'
        if not packages_root.exists():
            return ()

        candidates: list[Path] = []
        for package_dir in sorted(packages_root.iterdir()):
            if not package_dir.is_dir():
                continue
            src_dir = package_dir / 'src'
            if src_dir.exists():
                candidates.append(src_dir.resolve())
        return self._dedupe_paths(candidates)

    def _dedupe_paths(
        self,
        candidates,
    ) -> tuple[Path, ...]:
        deduped_paths: list[Path] = []
        seen: set[Path] = set()
        for candidate in candidates:
            resolved_candidate = Path(candidate).resolve()
            if resolved_candidate in seen:
                continue
            seen.add(resolved_candidate)
            deduped_paths.append(resolved_candidate)
        return tuple(deduped_paths)
