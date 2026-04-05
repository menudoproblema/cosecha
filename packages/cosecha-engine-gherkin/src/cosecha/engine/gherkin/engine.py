from __future__ import annotations

import asyncio

from pathlib import Path
from typing import TYPE_CHECKING, override

import parse

from cosecha.core.capabilities import (
    CAPABILITY_DRAFT_VALIDATION,
    CAPABILITY_LAZY_PROJECT_DEFINITION_LOADING,
    CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE,
    CAPABILITY_PLAN_EXPLANATION,
    CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
    CAPABILITY_PROJECT_REGISTRY_KNOWLEDGE,
    CAPABILITY_SELECTION_LABELS,
    CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY,
    CapabilityAttribute,
    CapabilityDescriptor,
    CapabilityOperationBinding,
    DraftValidationIssue,
    DraftValidationResult,
)
from cosecha.core.domain_events import (
    DefinitionMaterializedEvent,
    DomainEventMetadata,
    EngineSnapshotUpdatedEvent,
    KnowledgeIndexedEvent,
    KnowledgeInvalidatedEvent,
    StepFinishedEvent,
    StepStartedEvent,
)
from cosecha.core.engine_dependencies import EngineDependencyRule
from cosecha.core.engines.base import Engine
from cosecha.core.exceptions import CosechaParserError
from cosecha.core.items import TestPreflightDecision, TestResultStatus
from cosecha.core.operations import ResolvedDefinition
from cosecha.core.reporter import NullReporter, QueuedReporter
from cosecha.engine.gherkin.coercions import DEFAULT_COERCIONS
from cosecha.engine.gherkin.collector import GherkinCollector
from cosecha.engine.gherkin.completion import (
    CompletionSuggestion,
    StepCompletionRequest,
    build_step_completion_suggestions,
)
from cosecha.engine.gherkin.context import Context, ContextRegistry
from cosecha.engine.gherkin.definition_knowledge import (
    build_gherkin_definition_record,
)
from cosecha.engine.gherkin.step_catalog import (
    GHERKIN_STEP_INDEX_SCHEMA_VERSION,
    StepCatalog,
    StepQuery,
    get_conflicting_step_types,
)
from cosecha.engine.gherkin.step_materialization import LazyStepResolver
from cosecha.engine.gherkin.steps import StepRegistry
from cosecha.engine.gherkin.utils import import_and_load_steps_from_module


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from cosecha.core.collector import Collector
    from cosecha.core.config import Config
    from cosecha.core.cosecha_manifest import ResourceBindingSpec
    from cosecha.core.hooks import EngineHook
    from cosecha.core.items import TestItem
    from cosecha.core.reporter import Reporter
    from cosecha.core.resources import ResourceRequirement
    from cosecha.engine.gherkin.step_ast_discovery import (
        StaticDiscoveredStepFile,
        StaticStepDescriptor,
    )
    from cosecha.engine.gherkin.types import DatatableCoercions


class GherkinEngine(Engine):
    __slots__ = (
        'coercions',
        'collector',
        'context_registry',
        'definition_catalog_directories',
        'definition_path_overrides',
        'lazy_step_resolver',
        'library_definition_knowledge_loaded',
        'pending_domain_event_tasks',
        'reporter',
        'resource_bindings',
        'shared_resource_requirements',
        'step_registry',
    )

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        collector: Collector | None = None,
        reporter: Reporter | None = None,
        hooks: Iterable[EngineHook] = (),
        coercions: DatatableCoercions | None = None,
        shared_resource_requirements: tuple[ResourceRequirement, ...] = (),
        resource_bindings: tuple[ResourceBindingSpec, ...] = (),
        definition_paths: tuple[Path, ...] = (),
    ) -> None:
        if collector and not isinstance(collector, GherkinCollector):
            msg = 'Gherkin collector must be a subclass of GherkinCollector'
            raise TypeError(msg)

        super().__init__(
            name,
            collector or GherkinCollector(),
            reporter or NullReporter(),
            hooks,
        )

        self.coercions = DEFAULT_COERCIONS.copy()
        self.coercions.update(coercions or {})
        self.definition_path_overrides = definition_paths
        self.shared_resource_requirements = shared_resource_requirements
        self.resource_bindings = resource_bindings

    @override
    def initialize(self, config: Config, path: str) -> None:
        if self.definition_path_overrides:
            config.definition_paths = tuple(
                {
                    *config.definition_paths,
                    *self.definition_path_overrides,
                },
            )
        if config.concurrency > 1 and not isinstance(
            self.reporter,
            QueuedReporter,
        ):
            self.reporter = QueuedReporter(
                self.reporter,
                queue_add_test=True,
            )

        super().initialize(config, path)
        self.context_registry = ContextRegistry()
        self.definition_catalog_directories: tuple[Path, ...] = ()
        self.lazy_step_resolver = None
        self.library_definition_knowledge_loaded = False
        self.pending_domain_event_tasks: set[asyncio.Task[None]] = set()
        self.step_registry = StepRegistry(
            strict_ambiguity=config.strict_step_ambiguity,
        )

    @override
    async def collect(
        self,
        path: Path | tuple[Path, ...] | None = None,
        excluded_paths: tuple[Path, ...] = (),
    ):
        await super().collect(path, excluded_paths)
        for test in self.collector.collected_tests:
            bind_manifest_resources = getattr(
                test,
                'bind_manifest_resources',
                None,
            )
            if callable(bind_manifest_resources):
                bind_manifest_resources(self.shared_resource_requirements)
        self.definition_catalog_directories = tuple(
            sorted(self.collector.steps_directories),
        )
        self.library_definition_knowledge_loaded = False
        self.refresh_lazy_step_resolver()

    def refresh_lazy_step_resolver(self) -> None:
        self.lazy_step_resolver = LazyStepResolver(
            self._build_combined_step_index(),
            import_step_file=self._import_step_definition_file,
            on_materialized=self._handle_definition_materialized,
            on_load_failure=self._handle_step_materialization_failure,
        )
        self.step_registry.set_lazy_step_resolver(self.lazy_step_resolver)

    def _ensure_lazy_step_resolver(self) -> None:
        if self.lazy_step_resolver is None:
            self.refresh_lazy_step_resolver()
            return
        self.step_registry.set_lazy_step_resolver(self.lazy_step_resolver)

    def _build_combined_step_index(self) -> StepCatalog:
        step_catalog = StepCatalog()
        step_catalog.extend(
            self.collector.knowledge_store.get_discovered_step_files(),
        )
        for hook in self.hooks:
            step_catalog.extend(
                tuple(
                    getattr(
                        hook,
                        'library_discovered_step_files',
                        (),
                    ),
                ),
            )

        return step_catalog

    def _resolve_library_import_target(
        self,
        file_path: Path,
    ) -> str | Path:
        resolved_file_path = file_path.resolve()
        for hook in self.hooks:
            import_targets_by_file = getattr(
                hook,
                'library_import_targets_by_file',
                None,
            )
            if not isinstance(import_targets_by_file, dict):
                continue

            import_target = import_targets_by_file.get(resolved_file_path)
            if import_target is not None:
                return import_target

        return resolved_file_path

    def _import_step_definition_file(
        self,
        file_path: Path,
        step_registry: StepRegistry,
        function_names: tuple[str, ...] | None,
    ) -> None:
        import_and_load_steps_from_module(
            self._resolve_library_import_target(file_path),
            step_registry,
            function_names=function_names,
        )

    @override
    async def start_session(self) -> None:
        self._ensure_lazy_step_resolver()
        await super().start_session()

    @override
    async def finish_session(self):
        if self.pending_domain_event_tasks:
            await asyncio.gather(
                *tuple(self.pending_domain_event_tasks),
                return_exceptions=True,
            )
            self.pending_domain_event_tasks.clear()

        await super().finish_session()

    @override
    async def generate_new_context(self, test: TestItem) -> Context:
        await self._ensure_context_registry_loaded()

        def _build_step_snapshot_payload(step) -> dict[str, object] | None:
            if self.lazy_step_resolver is None:
                return None

            candidate_files = self.lazy_step_resolver.find_candidate_files(
                str(step.step_type),
                str(step.text),
            )
            return {
                'ambiguity_bucket_size': len(candidate_files),
                'candidate_count': len(candidate_files),
                'current_step_text': str(step.text),
                'current_step_type': str(step.step_type),
                'feature_name': test.feature.name,
                'loaded_definition_files': tuple(
                    str(path) for path in self.lazy_step_resolver.loaded_files
                ),
                'scenario_name': test.scenario.name,
            }

        async def _emit_step_event(
            phase: str,
            context: Context,
            step,
            status: str | None,
            message: str | None,
        ) -> None:
            if self._domain_event_stream is None:
                return

            execution_metadata = context.execution_metadata
            if execution_metadata is None:
                return
            node_id = execution_metadata.node_id
            node_stable_id = execution_metadata.node_stable_id

            metadata = DomainEventMetadata(
                session_id=_cast_optional_str(
                    execution_metadata.session_id,
                ),
                plan_id=_cast_optional_str(
                    execution_metadata.plan_id,
                ),
                trace_id=_cast_optional_str(
                    execution_metadata.trace_id,
                ),
                node_id=node_id,
                node_stable_id=node_stable_id,
                worker_id=_cast_optional_int(
                    execution_metadata.worker_id,
                ),
            )
            source_line = getattr(step.location, 'line', None)
            if phase == 'started':
                await self._domain_event_stream.emit(
                    StepStartedEvent(
                        node_id=node_id,
                        node_stable_id=node_stable_id,
                        engine_name=self.name,
                        test_name=getattr(test, 'test_name', repr(test)),
                        test_path=str(test.path)
                        if test.path is not None
                        else '',
                        step_type=str(step.step_type),
                        step_keyword=str(step.keyword),
                        step_text=str(step.text),
                        source_line=(
                            int(source_line)
                            if isinstance(source_line, int | float)
                            else None
                        ),
                        metadata=metadata,
                    ),
                )
                await self._domain_event_stream.emit(
                    EngineSnapshotUpdatedEvent(
                        engine_name=self.name,
                        snapshot_kind='gherkin_execution',
                        payload=_build_step_snapshot_payload(step) or {},
                        metadata=metadata,
                    ),
                )
                return

            await self._domain_event_stream.emit(
                StepFinishedEvent(
                    node_id=node_id,
                    node_stable_id=node_stable_id,
                    engine_name=self.name,
                    test_name=getattr(test, 'test_name', repr(test)),
                    test_path=str(test.path) if test.path is not None else '',
                    step_type=str(step.step_type),
                    step_keyword=str(step.keyword),
                    step_text=str(step.text),
                    status=status or 'unknown',
                    source_line=(
                        int(source_line)
                        if isinstance(source_line, int | float)
                        else None
                    ),
                    message=message,
                    metadata=metadata,
                ),
            )
            snapshot_payload = _build_step_snapshot_payload(step)
            if snapshot_payload is not None:
                await self._domain_event_stream.emit(
                    EngineSnapshotUpdatedEvent(
                        engine_name=self.name,
                        snapshot_kind='gherkin_execution',
                        payload=snapshot_payload,
                        metadata=metadata,
                    ),
                )

        return Context(
            self.context_registry.copy(),
            self.step_registry,
            self.coercions,
            resource_bindings=self.resource_bindings,
            step_event_callback=_emit_step_event,
        )

    @override
    def preflight_test(
        self,
        test: TestItem,
    ) -> TestPreflightDecision | None:
        all_steps = getattr(getattr(test, 'scenario', None), 'all_steps', ())
        if not all_steps:
            return TestPreflightDecision(
                status=TestResultStatus.SKIPPED,
                message='No steps to run',
            )

        return None

    async def load_tests_from_content(
        self,
        feature_content: str,
        test_path: Path,
    ) -> list[TestItem]:
        return await self.collector.load_tests_from_content(
            feature_content,
            test_path,
        )

    def get_project_definition_index(self):
        return self.collector.get_project_step_index()

    def get_library_definition_index(self) -> StepCatalog:
        library_step_catalog = StepCatalog()
        for hook in self.hooks:
            discovered_step_files = getattr(
                hook,
                'library_discovered_step_files',
                None,
            )
            if discovered_step_files is None:
                continue

            library_step_catalog.extend(tuple(discovered_step_files))

        return library_step_catalog

    @override
    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name=CAPABILITY_DRAFT_VALIDATION,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='draft.validate',
                        result_type='draft.validation',
                        freshness='fresh',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_SELECTION_LABELS,
                level='supported',
                summary='Labels derived from feature/scenario tags',
                attributes=(
                    CapabilityAttribute(
                        name='label_sources',
                        value=('feature_tag', 'scenario_tag'),
                    ),
                    CapabilityAttribute(
                        name='supports_glob_matching',
                        value=True,
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='run',
                        result_type='run.result',
                    ),
                    CapabilityOperationBinding(
                        operation_type='plan.analyze',
                        result_type='plan.analysis',
                    ),
                    CapabilityOperationBinding(
                        operation_type='plan.explain',
                        result_type='plan.explanation',
                    ),
                    CapabilityOperationBinding(
                        operation_type='plan.simulate',
                        result_type='plan.simulation',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='definition.resolve',
                        result_type='definition.resolution',
                        freshness='fresh',
                    ),
                    CapabilityOperationBinding(
                        operation_type='knowledge.query_definitions',
                        result_type='knowledge.definitions',
                        freshness='knowledge_base',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_PROJECT_REGISTRY_KNOWLEDGE,
                level='supported',
                summary='Persisted declarative context registry snapshots',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='knowledge.query_registry_items',
                        result_type='knowledge.registry_items',
                        freshness='knowledge_base',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_PLAN_EXPLANATION,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='plan.explain',
                        result_type='plan.explanation',
                    ),
                    CapabilityOperationBinding(
                        operation_type='plan.simulate',
                        result_type='plan.simulation',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_LAZY_PROJECT_DEFINITION_LOADING,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='materialization_granularity',
                        value='file',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='discovery_backend',
                        value='ast',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='definition.resolve',
                        result_type='definition.resolution',
                        freshness='fresh',
                    ),
                    CapabilityOperationBinding(
                        operation_type='knowledge.query_definitions',
                        result_type='knowledge.definitions',
                        freshness='knowledge_base',
                    ),
                ),
            ),
        )

    @override
    def describe_engine_dependencies(self) -> tuple[EngineDependencyRule, ...]:
        return (
            EngineDependencyRule(
                source_engine_name=self.name,
                target_engine_name='pytest',
                dependency_kind='knowledge',
                projection_policy='diagnostic_only',
                summary=(
                    'Gherkin puede correlacionar explain y diagnóstico con '
                    'tests y definiciones publicados por Pytest.'
                ),
                required_capabilities=(
                    CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
                    CAPABILITY_PLAN_EXPLANATION,
                ),
                operation_types=(
                    'knowledge.query_tests',
                    'knowledge.query_definitions',
                    'plan.explain',
                ),
            ),
            EngineDependencyRule(
                source_engine_name=self.name,
                target_engine_name='pytest',
                dependency_kind='execution',
                projection_policy='degrade_to_explain',
                summary=(
                    'Un fallo funcional de Gherkin puede degradar explain y '
                    'diagnóstico en Pytest dentro del mismo plan mixto.'
                ),
                required_capabilities=(CAPABILITY_PLAN_EXPLANATION,),
                operation_types=(
                    'engine_dependencies.query',
                    'plan.explain',
                ),
            ),
        )

    async def validate_draft(
        self,
        source_content: str,
        test_path: Path,
    ) -> DraftValidationResult:
        resolved_test_path = self._resolve_snapshot_path(str(test_path))
        await self._ensure_definition_catalog_loaded(resolved_test_path)

        try:
            tests = await self.collector.load_tests_from_content(
                source_content,
                resolved_test_path,
            )
        except CosechaParserError as error:
            return DraftValidationResult(
                test_count=0,
                issues=(
                    DraftValidationIssue(
                        code='gherkin_parser_error',
                        message=error.reason,
                        severity='error',
                        line=error.line,
                        column=error.column,
                    ),
                ),
            )

        step_queries = tuple(
            dict.fromkeys(
                StepQuery(step_type, step_text)
                for test in tests
                for step_type, step_text in test.get_required_step_texts()
            ),
        )
        if not tests:
            return DraftValidationResult(
                test_count=0,
                issues=(
                    DraftValidationIssue(
                        code='no_executable_tests',
                        message='Draft does not produce any executable tests',
                        severity='error',
                    ),
                ),
            )

        step_index = self.collector.get_project_step_index()
        library_index = self.get_library_definition_index()
        step_candidate_files = tuple(
            str(path)
            for path in {
                *step_index.find_candidate_files_for_steps(step_queries),
                *library_index.find_candidate_files_for_steps(step_queries),
            }
        )
        missing_step_queries = tuple(
            step_query
            for step_query in step_queries
            if not {
                *step_index.find_candidate_files(
                    step_query.step_type,
                    step_query.step_text,
                ),
                *library_index.find_candidate_files(
                    step_query.step_type,
                    step_query.step_text,
                ),
            }
        )

        issues = list(
            self._build_draft_validation_issues_for_tests(tests),
        )
        issues.extend(
            DraftValidationIssue(
                code='missing_step_candidates',
                message=(
                    'No definition candidates found for '
                    f'@{step_query.step_type}({step_query.step_text!r})'
                ),
                severity='warning',
            )
            for step_query in missing_step_queries
        )

        return DraftValidationResult(
            test_count=len(tests),
            required_step_texts=tuple(
                (step_query.step_type, step_query.step_text)
                for step_query in step_queries
            ),
            step_candidate_files=step_candidate_files,
            issues=tuple(issues),
        )

    async def resolve_definition(
        self,
        *,
        test_path: Path,
        step_type: str,
        step_text: str,
    ) -> tuple[ResolvedDefinition, ...]:
        resolved_test_path = self._resolve_snapshot_path(str(test_path))
        await self._ensure_definition_catalog_loaded(resolved_test_path)

        static_matches = self._resolve_static_definitions(
            self.collector.get_project_step_index(),
            step_type=step_type,
            step_text=step_text,
        )
        if static_matches:
            return static_matches

        match = self.step_registry.find_match(step_type, step_text)
        if match is None:
            return ()

        definition = match.step_definition
        location = definition.location
        return (
            ResolvedDefinition(
                engine_name=self.name,
                file_path=str(location.filename),
                line=location.line,
                column=location.column,
                function_name=definition.func.__name__,
                category=definition.category,
                documentation=definition.func.__doc__,
                step_type=definition.step_type,
                patterns=tuple(
                    current_step_text.text
                    for current_step_text in definition.step_text_list
                ),
                resolution_source='runtime_registry',
            ),
        )

    def _build_draft_validation_issues_for_tests(
        self,
        tests: list[TestItem],
    ) -> tuple[DraftValidationIssue, ...]:
        issues: list[DraftValidationIssue] = []
        for test in tests:
            scenario = getattr(test, 'scenario', None)
            if scenario is None:
                continue

            all_steps = scenario.all_steps
            matches = [
                self.step_registry.find_match(step.step_type, step.text)
                for step in all_steps
            ]

            for step, match in zip(all_steps, matches, strict=True):
                issues.extend(
                    self._build_draft_validation_step_issues(step, match),
                )

        return tuple(issues)

    def _build_draft_validation_step_issues(
        self,
        step,
        match,
    ) -> tuple[DraftValidationIssue, ...]:
        start_column = 0
        if step.location.column:
            start_column = step.location.column - 1 + len(step.keyword)

        line_num = step.location.line - 1
        if match is None:
            return (
                DraftValidationIssue(
                    code='missing_step_definition',
                    message=f'Step `{step.text}` not found.',
                    severity='warning',
                    line=line_num,
                    column=start_column,
                ),
            )

        step_text = match.step_text
        table_issue = self._build_draft_validation_table_issue(
            step,
            step_text,
            line_num,
            start_column,
        )
        if table_issue is not None:
            return (table_issue,)

        return self._build_draft_validation_layout_issues(
            match,
            line_num,
            start_column,
        )

    def _build_draft_validation_table_issue(
        self,
        step,
        step_text,
        line_num: int,
        start_column: int,
    ) -> DraftValidationIssue | None:
        if step_text.min_table_rows:
            if not step.table:
                return DraftValidationIssue(
                    code='missing_data_table',
                    message='Data table is required.',
                    severity='error',
                    line=line_num,
                    column=start_column,
                )

            if len(step.table.rows) < step_text.min_table_rows:
                return DraftValidationIssue(
                    code='min_table_rows_not_met',
                    message=(
                        'This step requires at least '
                        f'{step_text.min_table_rows} rows in the '
                        'data table.'
                    ),
                    severity='error',
                    line=line_num,
                    column=start_column,
                )

        if step_text.required_table_rows:
            if not step.table:
                return DraftValidationIssue(
                    code='missing_data_table',
                    message='Data table is required.',
                    severity='error',
                    line=line_num,
                    column=start_column,
                )

            if len(step.table.rows) != step_text.required_table_rows:
                return DraftValidationIssue(
                    code='required_table_rows_not_met',
                    message=(
                        'This step requires exactly '
                        f'{step_text.required_table_rows} rows in the '
                        'data table.'
                    ),
                    severity='error',
                    line=line_num,
                    column=start_column,
                )

        if step.table and not step_text.can_use_table:
            return DraftValidationIssue(
                code='unexpected_data_table',
                message='No data table expected in this step.',
                severity='error',
                line=line_num,
                column=start_column,
            )

        return None

    def _build_draft_validation_layout_issues(
        self,
        match,
        line_num: int,
        start_column: int,
    ) -> tuple[DraftValidationIssue, ...]:
        issues: list[DraftValidationIssue] = []
        for layout_ref in match.step_text.layouts:
            for argument in match.arguments:
                if argument.name != layout_ref.place_holder:
                    continue

                if argument.value.startswith('<') and argument.value.endswith(
                    '>',
                ):
                    continue

                item = self.context_registry.get(
                    layout_ref.layout,
                    argument.value,
                )
                if item is not None:
                    continue

                issues.append(
                    DraftValidationIssue(
                        code='unknown_layout_reference',
                        message=(
                            f'"{argument.value}" not found in layout '
                            f'"{layout_ref.layout}".'
                        ),
                        severity='error',
                        line=line_num,
                        column=start_column + argument.start_column,
                    ),
                )

        return tuple(issues)

    def prime_execution_node(self, node) -> None:
        self._ensure_lazy_step_resolver()
        required_step_texts = tuple(
            (str(step_type), str(step_text))
            for step_type, step_text in getattr(
                node,
                'required_step_texts',
                (),
            )
        )
        if not required_step_texts:
            return

        candidate_files = tuple(
            self._resolve_snapshot_path(path)
            for path in getattr(node, 'step_candidate_files', ())
        )
        self.lazy_step_resolver.prime_candidate_files(
            required_step_texts,
            candidate_files,
        )

    def suggest_step_completions(
        self,
        *,
        step_type: str,
        initial_text: str,
        cursor_column: int,
        start_step_text_column: int,
    ) -> tuple[CompletionSuggestion, ...]:
        return build_step_completion_suggestions(
            StepCompletionRequest(
                step_type=step_type,
                initial_text=initial_text,
                cursor_column=cursor_column,
                start_step_text_column=start_step_text_column,
            ),
            context_registry=self.context_registry,
            step_registry=self.step_registry,
        )

    def _resolve_static_definitions(
        self,
        step_index: StepCatalog,
        *,
        step_type: str,
        step_text: str,
    ) -> tuple[ResolvedDefinition, ...]:
        candidate_files = step_index.find_candidate_files(
            step_type,
            step_text,
        )
        if not candidate_files:
            return ()

        matches: list[ResolvedDefinition] = []
        seen_definitions: set[tuple[str, int, str]] = set()
        for file_path in candidate_files:
            for descriptor in step_index.descriptors_for_file(file_path):
                if descriptor.step_type not in get_conflicting_step_types(
                    step_type,
                ):
                    continue
                if not self._descriptor_supports_static_resolution(
                    descriptor,
                ):
                    return ()
                if not self._descriptor_matches_step_text(
                    descriptor,
                    step_text,
                ):
                    continue

                key = (
                    str(descriptor.file_path),
                    descriptor.source_line,
                    descriptor.function_name,
                )
                if key in seen_definitions:
                    continue

                seen_definitions.add(key)
                matches.append(
                    ResolvedDefinition(
                        engine_name=self.name,
                        file_path=str(descriptor.file_path),
                        line=descriptor.source_line,
                        function_name=descriptor.function_name,
                        category=descriptor.category,
                        documentation=descriptor.documentation,
                        step_type=descriptor.step_type,
                        patterns=descriptor.patterns,
                        resolution_source='static_catalog',
                    ),
                )

        return tuple(matches)

    def _descriptor_matches_step_text(
        self,
        descriptor: StaticStepDescriptor,
        step_text: str,
    ) -> bool:
        return any(
            parse.compile(pattern).parse(step_text) is not None
            for pattern in descriptor.patterns
        )

    def _descriptor_supports_static_resolution(
        self,
        descriptor: StaticStepDescriptor,
    ) -> bool:
        return descriptor.discovery_mode == 'ast' and (
            descriptor.parser_cls_name in (None, 'ParseStepMatcher')
        )

    def _resolve_snapshot_path(self, path: str) -> Path:
        candidate_path = Path(path)
        if candidate_path.is_absolute():
            return candidate_path.resolve()

        return (self.config.root_path / candidate_path).resolve()

    def _handle_step_materialization_failure(
        self,
        file_path: Path,
        formatted_traceback: str,
    ) -> None:
        failed_files = getattr(self.collector, 'failed_files', None)
        if isinstance(failed_files, set):
            failed_files.add(file_path)
        self.config.diagnostics.error(
            f'Fail loading steps from: {file_path}',
            details=formatted_traceback,
        )

    def _handle_definition_materialized(self, file_path: Path) -> None:
        if self._domain_event_stream is None:
            return

        is_fallback_file = (
            file_path in self.collector.step_catalog.fallback_files
        )
        task = asyncio.create_task(
            self._domain_event_stream.emit(
                DefinitionMaterializedEvent(
                    engine_name=self.name,
                    file_path=str(file_path),
                    definition_count=len(
                        self.collector.step_catalog.descriptors_for_file(
                            file_path,
                        ),
                    ),
                    discovery_mode=(
                        'fallback_import' if is_fallback_file else 'ast'
                    ),
                ),
            ),
        )
        self.pending_domain_event_tasks.add(task)
        task.add_done_callback(self.pending_domain_event_tasks.discard)

    async def _load_library_definition_knowledge(self) -> None:
        if self._domain_event_stream is None:
            for hook in self.hooks:
                load_library_definition_knowledge = getattr(
                    hook,
                    'load_library_definition_knowledge',
                    None,
                )
                if load_library_definition_knowledge is None:
                    continue

                await load_library_definition_knowledge(
                    self.config.root_path,
                )
            self.library_definition_knowledge_loaded = True
            return

        previous_files_by_path: dict[str, StaticDiscoveredStepFile] = {}
        for hook in self.hooks:
            for discovered_file in getattr(
                hook,
                'library_discovered_step_files',
                (),
            ):
                previous_files_by_path.setdefault(
                    str(discovered_file.file_path),
                    discovered_file,
                )

            load_library_definition_knowledge = getattr(
                hook,
                'load_library_definition_knowledge',
                None,
            )
            if load_library_definition_knowledge is None:
                continue

            await load_library_definition_knowledge(
                self.config.root_path,
            )

        current_files_by_path: dict[str, StaticDiscoveredStepFile] = {}
        for hook in self.hooks:
            for discovered_file in getattr(
                hook,
                'library_discovered_step_files',
                (),
            ):
                current_files_by_path.setdefault(
                    str(discovered_file.file_path),
                    discovered_file,
                )

        knowledge_version = (
            f'gherkin_step_index:v{GHERKIN_STEP_INDEX_SCHEMA_VERSION}'
        )
        for file_path in sorted(
            set(previous_files_by_path).difference(current_files_by_path),
        ):
            await self._domain_event_stream.emit(
                KnowledgeInvalidatedEvent(
                    engine_name=self.name,
                    file_path=file_path,
                    reason='library_definition_removed',
                    knowledge_version=knowledge_version,
                ),
            )

        for discovered_file in tuple(
            current_files_by_path[file_path]
            for file_path in sorted(current_files_by_path)
        ):
            await self._domain_event_stream.emit(
                KnowledgeIndexedEvent(
                    engine_name=self.name,
                    file_path=str(discovered_file.file_path),
                    definition_count=len(discovered_file.descriptors),
                    discovery_mode=discovered_file.discovery_mode,
                    knowledge_version=knowledge_version,
                    content_hash=discovered_file.content_digest,
                    descriptors=tuple(
                        _build_definition_descriptor_knowledge(descriptor)
                        for descriptor in discovered_file.descriptors
                    ),
                ),
            )
        self.library_definition_knowledge_loaded = True

    async def _ensure_definition_catalog_loaded(
        self,
        test_path: Path,
    ) -> None:
        await self.collector.find_step_impl_directories(test_path)
        current_directories = tuple(
            sorted(self.collector.steps_directories),
        )
        if current_directories != self.definition_catalog_directories:
            await self.collector.build_step_catalog()
            self.definition_catalog_directories = current_directories
            self.refresh_lazy_step_resolver()

        if not self.library_definition_knowledge_loaded:
            await self._load_library_definition_knowledge()
            self.refresh_lazy_step_resolver()

    async def _ensure_context_registry_loaded(self) -> None:
        for hook in self.hooks:
            ensure_context_registry_loaded = getattr(
                hook,
                'ensure_context_registry_loaded',
                None,
            )
            if ensure_context_registry_loaded is None:
                continue

            await ensure_context_registry_loaded(self)


def _build_definition_descriptor_knowledge(
    descriptor: StaticStepDescriptor,
) -> object:
    return build_gherkin_definition_record(
        source_line=descriptor.source_line,
        function_name=descriptor.function_name,
        step_type=descriptor.step_type,
        patterns=descriptor.patterns,
        literal_prefixes=descriptor.literal_prefixes,
        literal_suffixes=descriptor.literal_suffixes,
        literal_fragments=descriptor.literal_fragments,
        anchor_tokens=descriptor.anchor_tokens,
        dynamic_fragment_count=descriptor.dynamic_fragment_count,
        documentation=descriptor.documentation,
        parser_cls_name=descriptor.parser_cls_name,
        category=descriptor.category,
        discovery_mode=descriptor.discovery_mode,
    )


def _cast_optional_str(value: object) -> str | None:
    if value is None:
        return None

    return str(value)


def _cast_optional_int(value: object) -> int | None:
    if value is None:
        return None

    return int(value)
