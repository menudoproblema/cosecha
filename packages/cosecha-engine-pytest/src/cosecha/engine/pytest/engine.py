from __future__ import annotations

import ast
import asyncio

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, override

from cosecha.core.capabilities import (
    CAPABILITY_DRAFT_VALIDATION,
    CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE,
    CAPABILITY_PLAN_EXPLANATION,
    CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
    CAPABILITY_SELECTION_LABELS,
    CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY,
    CapabilityAttribute,
    CapabilityDescriptor,
    CapabilityOperationBinding,
    DraftValidationIssue,
    DraftValidationResult,
)
from cosecha.core.cosecha_manifest import discover_cosecha_manifest
from cosecha.core.engine_dependencies import EngineDependencyRule
from cosecha.core.engines.base import Engine
from cosecha.core.items import TestResultStatus
from cosecha.core.operations import ResolvedDefinition
from cosecha.core.reporter import NullReporter, QueuedReporter
from cosecha.engine.pytest.collector import (
    PytestCollector,
    PytestModuleDiscoveryContext,
    _build_external_pytest_plugin_file_path,
    _build_fixture_source_metadata,
    _build_static_skip_decision,
    _build_static_xfail_decision,
    _build_usefixtures_decision,
    _discover_configured_definition_source_paths,
    _discover_conftest_paths,
    _discover_external_pytest_plugin_references,
    _discover_fixture_aliases,
    _discover_fixture_definitions,
    _discover_fixture_knowledge_records,
    _discover_imported_fixture_bindings,
    _discover_marker_aliases,
    _discover_nonlocal_fixture_definitions,
    _discover_pytest_plugins_runtime_reason,
    _discover_pytest_tests,
    _discover_static_binding_context,
    _discover_visible_fixture_source_paths,
    _get_fixture_support_reason,
    _is_supported_fixture_definition,
    _parse_parametrize_specs,
    _supports_pytest_callable_signature,
    discover_pytest_tests_from_module,
)
from cosecha.engine.pytest.context import PytestContext
from cosecha.engine.pytest.items import (
    PytestTestDefinition,
    PytestTestItem,
    _build_pytest_test_name,
    reset_pytest_runtime_batch_cache,
)


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.cosecha_manifest import ResourceBindingSpec
    from cosecha.core.resources import ResourceRequirement


@dataclass(slots=True, frozen=True)
class _PytestDefinitionResolutionContext:
    engine_name: str
    test_path: Path
    root_path: Path
    configured_definition_paths: tuple[Path, ...] = ()
    resource_bindings: tuple[ResourceBindingSpec, ...] = ()


@dataclass(slots=True, frozen=True)
class _PytestDraftValidationContext:
    marker_aliases: set[str]
    literal_bindings: dict[str, object]
    expression_bindings: dict[str, ast.expr]


@dataclass(slots=True, frozen=True)
class _PytestDraftInheritanceContext:
    issue_messages: tuple[str | None, str | None, str | None] = (
        None,
        None,
        None,
    )
    runtime_messages: tuple[str | None, str | None] = (
        None,
        None,
    )
    pytest_plugins_runtime_reason: str | None = None


class PytestEngine(Engine):
    __slots__ = (
        'definition_path_overrides',
        'resource_bindings',
        'shared_resource_requirements',
    )

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        reporter=None,
        hooks=(),
        shared_resource_requirements: tuple[ResourceRequirement, ...] = (),
        resource_bindings: tuple[ResourceBindingSpec, ...] = (),
        definition_paths: tuple[Path, ...] = (),
    ) -> None:
        super().__init__(
            name=name,
            collector=PytestCollector(),
            reporter=QueuedReporter(reporter or NullReporter()),
            hooks=hooks,
        )
        self.definition_path_overrides = definition_paths
        self.shared_resource_requirements = shared_resource_requirements
        self.resource_bindings = resource_bindings

    @override
    def initialize(self, config, path: str) -> None:
        if self.definition_path_overrides:
            config.definition_paths = tuple(
                {
                    *config.definition_paths,
                    *self.definition_path_overrides,
                },
            )
        super().initialize(config, path)
        self.collector.resource_fixture_names = tuple(
            binding.fixture_name
            for binding in self.resource_bindings
            if binding.fixture_name is not None
        )

    @override
    async def generate_new_context(self, test) -> PytestContext:
        del test
        return PytestContext(resource_bindings=self.resource_bindings)

    @override
    async def collect(
        self,
        path: Path | tuple[Path, ...] | None = None,
        excluded_paths: tuple[Path, ...] = (),
    ):
        reset_pytest_runtime_batch_cache(root_path=self.config.root_path)
        await super().collect(path, excluded_paths)
        for test in self.collector.collected_tests:
            bind_manifest_resources = getattr(
                test,
                'bind_manifest_resources',
                None,
            )
            if callable(bind_manifest_resources):
                bind_manifest_resources(self.shared_resource_requirements)
            bind_resource_bindings = getattr(
                test,
                'bind_resource_bindings',
                None,
            )
            if callable(bind_resource_bindings):
                bind_resource_bindings(self.resource_bindings)
            bind_runtime_adapter_profiles = getattr(
                test,
                'bind_runtime_adapter_profiles',
                None,
            )
            if callable(bind_runtime_adapter_profiles):
                bind_runtime_adapter_profiles(
                    self.name,
                    self.runtime_profiles,
                )

    @override
    async def start_test(self, test) -> None:
        await super().start_test(test)
        if not isinstance(test, PytestTestItem):
            return

        if test.definition.xfail_issue is not None:
            msg = test.definition.xfail_issue
            test.status = TestResultStatus.ERROR
            test.message = msg
            test.failure_kind = 'collection'
            test.error_code = 'pytest_case_xfail_issue'
            raise RuntimeError(msg)

        if test.definition.skip_issue is not None:
            msg = test.definition.skip_issue
            test.status = TestResultStatus.ERROR
            test.message = msg
            test.failure_kind = 'collection'
            test.error_code = 'pytest_case_skip_issue'
            raise RuntimeError(msg)

        if test.definition.skip_reason is not None:
            test.status = TestResultStatus.SKIPPED
            test.message = test.definition.skip_reason
            return

        if (
            test.definition.xfail_reason is not None
            and not test.definition.xfail_run
        ):
            test.status = TestResultStatus.SKIPPED
            test.message = (
                f'Expected failure (not run): {test.definition.xfail_reason}'
            )

    @override
    async def start_session(self) -> None:
        reset_pytest_runtime_batch_cache(
            root_path=self.config.root_path,
            clear_registrations=False,
        )
        await super().start_session()

    @override
    async def finish_session(self):
        try:
            await super().finish_session()
        finally:
            reset_pytest_runtime_batch_cache(root_path=self.config.root_path)

    def get_project_definition_index(self):
        return self.collector.definition_index

    @override
    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name=CAPABILITY_SELECTION_LABELS,
                level='supported',
                summary='Labels derived from pytest markers',
                attributes=(
                    CapabilityAttribute(
                        name='label_sources',
                        value=('pytest_marker',),
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
                name=CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY,
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='knowledge.query_tests',
                        result_type='knowledge.tests',
                        freshness='fresh',
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
                        operation_type='knowledge.query_tests',
                        result_type='knowledge.tests',
                        freshness='knowledge_base',
                    ),
                    CapabilityOperationBinding(
                        operation_type='knowledge.query_definitions',
                        result_type='knowledge.definitions',
                        freshness='knowledge_base',
                    ),
                ),
            ),
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
                name=CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE,
                level='supported',
                summary=(
                    'Typed fixture knowledge from external definition roots '
                    'and statically resolved pytest plugins'
                ),
                attributes=(
                    CapabilityAttribute(
                        name='definition_categories',
                        value=(
                            'configured_definition_fixture',
                            'external_pytest_plugin',
                            'pytest_plugin_fixture',
                        ),
                    ),
                    CapabilityAttribute(
                        name='provider_kinds',
                        value=(
                            'definition_path',
                            'pytest_plugin',
                            'pytest_plugin_external',
                        ),
                    ),
                    CapabilityAttribute(
                        name='external_reference_fields',
                        value=(
                            'runtime_required',
                            'runtime_reason',
                            'declaration_origin',
                        ),
                    ),
                ),
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
        )

    @override
    def describe_engine_dependencies(self) -> tuple[EngineDependencyRule, ...]:
        return (
            EngineDependencyRule(
                source_engine_name=self.name,
                target_engine_name='gherkin',
                dependency_kind='knowledge',
                projection_policy='diagnostic_only',
                summary=(
                    'Pytest puede correlacionar diagnóstico y explain con '
                    'escenarios y definiciones publicados por Gherkin.'
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
                target_engine_name='gherkin',
                dependency_kind='execution',
                projection_policy='degrade_to_explain',
                summary=(
                    'Un fallo funcional de Pytest puede degradar explain y '
                    'diagnóstico en Gherkin dentro del mismo plan mixto.'
                ),
                required_capabilities=(CAPABILITY_PLAN_EXPLANATION,),
                operation_types=(
                    'engine_dependencies.query',
                    'plan.explain',
                ),
            ),
        )

    async def resolve_definition(
        self,
        *,
        test_path: Path,
        step_type: str,
        step_text: str,
    ) -> tuple[ResolvedDefinition, ...]:
        return await asyncio.to_thread(
            _resolve_pytest_definitions,
            _PytestDefinitionResolutionContext(
                engine_name=self.name,
                test_path=test_path,
                root_path=self.config.root_path,
                configured_definition_paths=tuple(
                    _discover_configured_definition_source_paths(
                        self.config.definition_paths,
                    ),
                ),
                resource_bindings=self.resource_bindings,
            ),
            step_type=step_type,
            step_text=step_text,
        )

    async def validate_draft(
        self,
        source_content: str,
        test_path: Path,
    ) -> DraftValidationResult:
        try:
            module = ast.parse(source_content)
        except SyntaxError as error:
            return DraftValidationResult(
                test_count=0,
                issues=(
                    DraftValidationIssue(
                        code='pytest_syntax_error',
                        message=error.msg,
                        severity='error',
                        line=error.lineno,
                        column=error.offset,
                    ),
                ),
            )

        fixture_aliases = _discover_fixture_aliases(module)
        configured_definition_source_paths = tuple(
            _discover_configured_definition_source_paths(
                self.config.definition_paths,
            ),
        )
        (
            literal_bindings,
            expression_bindings,
        ) = _discover_static_binding_context(
            module,
            source_path=test_path,
            root_path=self.config.root_path,
            configured_definition_paths=configured_definition_source_paths,
        )
        fixtures = {
            **_discover_nonlocal_fixture_definitions(
                test_path,
                root_path=self.config.root_path,
                configured_definition_paths=configured_definition_source_paths,
            ),
            **_discover_fixture_definitions(
                module,
                fixture_aliases,
                source_path=test_path,
            ),
        }

        definitions = discover_pytest_tests_from_module(
            module,
            discovery_context=PytestModuleDiscoveryContext(
                root_path=self.config.root_path,
                fixtures=fixtures,
                source_path=test_path,
                configured_definition_paths=tuple(
                    str(path) for path in configured_definition_source_paths
                ),
                resource_fixture_names=tuple(
                    binding.fixture_name
                    for binding in self.resource_bindings
                    if binding.fixture_name is not None
                ),
            ),
        )
        return DraftValidationResult(
            test_count=len(definitions),
            issues=self._build_draft_validation_issues(
                module,
                fixtures,
                literal_bindings=literal_bindings,
                expression_bindings=expression_bindings,
                pytest_plugins_runtime_reason=(
                    _discover_pytest_plugins_runtime_reason(
                        module,
                        source_path=test_path,
                        root_path=self.config.root_path,
                        configured_definition_paths=(
                            configured_definition_source_paths
                        ),
                        literal_bindings=literal_bindings,
                        expression_bindings=expression_bindings,
                    )
                ),
            ),
        )

    def _build_draft_validation_issues(
        self,
        module: ast.Module,
        fixtures,
        *,
        literal_bindings: dict[str, object],
        expression_bindings: dict[str, ast.expr],
        pytest_plugins_runtime_reason: str | None = None,
    ) -> tuple[DraftValidationIssue, ...]:
        issues: list[DraftValidationIssue] = []
        validation_context = _PytestDraftValidationContext(
            marker_aliases=_discover_marker_aliases(module),
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )

        for fixture in fixtures.values():
            issue = self._build_fixture_issue(
                fixture,
                fixtures=fixtures,
            )
            if issue is not None:
                issues.append(issue)

        for statement in module.body:
            if isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
                issues.extend(
                    self._build_test_node_issues(
                        statement,
                        fixtures=fixtures,
                        validation_context=validation_context,
                        inheritance_context=_PytestDraftInheritanceContext(
                            pytest_plugins_runtime_reason=(
                                pytest_plugins_runtime_reason
                            ),
                        ),
                    ),
                )
                continue

            if not isinstance(statement, ast.ClassDef):
                continue

            if not statement.name.startswith('Test'):
                continue

            class_skip_decision = _build_static_skip_decision(
                statement.decorator_list,
                validation_context.marker_aliases,
                literal_bindings=validation_context.literal_bindings,
                expression_bindings=validation_context.expression_bindings,
            )
            class_xfail_decision = _build_static_xfail_decision(
                statement.decorator_list,
                validation_context.marker_aliases,
                literal_bindings=validation_context.literal_bindings,
                expression_bindings=validation_context.expression_bindings,
            )
            class_usefixtures_decision = _build_usefixtures_decision(
                statement.decorator_list,
                validation_context.marker_aliases,
            )

            for child in statement.body:
                if not isinstance(
                    child,
                    ast.FunctionDef | ast.AsyncFunctionDef,
                ):
                    continue

                issues.extend(
                    self._build_test_node_issues(
                        child,
                        class_name=statement.name,
                        fixtures=fixtures,
                        validation_context=validation_context,
                        inheritance_context=_PytestDraftInheritanceContext(
                            issue_messages=(
                                class_usefixtures_decision.issue_message,
                                class_skip_decision.issue_message,
                                class_xfail_decision.issue_message,
                            ),
                            runtime_messages=(
                                class_skip_decision.runtime_reason,
                                class_xfail_decision.runtime_reason,
                            ),
                            pytest_plugins_runtime_reason=(
                                pytest_plugins_runtime_reason
                            ),
                        ),
                    ),
                )

        return tuple(issues)

    def _build_test_node_issues(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        fixtures,
        validation_context: _PytestDraftValidationContext,
        class_name: str | None = None,
        inheritance_context: _PytestDraftInheritanceContext | None = None,
    ) -> tuple[DraftValidationIssue, ...]:
        if inheritance_context is None:
            inheritance_context = _PytestDraftInheritanceContext()
        (
            inherited_usefixtures_issue,
            inherited_skip_issue,
            inherited_xfail_issue,
        ) = inheritance_context.issue_messages
        (
            inherited_skip_runtime_reason,
            inherited_xfail_runtime_reason,
        ) = inheritance_context.runtime_messages
        inherited_pytest_plugins_runtime_reason = (
            inheritance_context.pytest_plugins_runtime_reason
        )
        issues: list[DraftValidationIssue] = []
        pytest_plugins_runtime_issue = (
            self._build_pytest_plugins_runtime_issue(
                node,
                runtime_reason=inherited_pytest_plugins_runtime_reason,
            )
        )
        if pytest_plugins_runtime_issue is not None:
            issues.append(pytest_plugins_runtime_issue)
        usefixtures_issue = self._build_usefixtures_issue(
            node,
            marker_aliases=validation_context.marker_aliases,
            inherited_usefixtures_issue=inherited_usefixtures_issue,
        )
        if usefixtures_issue is not None:
            issues.append(usefixtures_issue)

        xfail_issue = self._build_xfail_issue(
            node,
            marker_aliases=validation_context.marker_aliases,
            validation_context=validation_context,
            inherited_runtime_reason=inherited_xfail_runtime_reason,
            inherited_xfail_issue=inherited_xfail_issue,
        )
        if xfail_issue is not None:
            issues.append(xfail_issue)

        skip_issue = self._build_skip_issue(
            node,
            marker_aliases=validation_context.marker_aliases,
            validation_context=validation_context,
            inherited_runtime_reason=inherited_skip_runtime_reason,
            inherited_skip_issue=inherited_skip_issue,
        )
        if skip_issue is not None:
            issues.append(skip_issue)

        parametrize_issue = self._build_parametrize_issue(
            node,
            marker_aliases=validation_context.marker_aliases,
            literal_bindings=validation_context.literal_bindings,
            expression_bindings=validation_context.expression_bindings,
        )
        if parametrize_issue is not None:
            issues.append(parametrize_issue)
            return tuple(issues)

        signature_issue = self._build_signature_issue(
            node,
            class_name=class_name,
            fixtures=fixtures,
            validation_context=validation_context,
        )
        if signature_issue is not None:
            issues.append(signature_issue)

        return tuple(issues)

    def _build_pytest_plugins_runtime_issue(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        runtime_reason: str | None,
    ) -> DraftValidationIssue | None:
        if not node.name.startswith('test_') or runtime_reason is None:
            return None

        return DraftValidationIssue(
            code='pytest_runtime_pytest_plugins',
            message=runtime_reason,
            severity='warning',
            line=node.lineno,
            column=getattr(node, 'col_offset', 0) + 1,
        )

    def _build_signature_issue(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        class_name: str | None = None,
        fixtures,
        validation_context: _PytestDraftValidationContext,
    ) -> DraftValidationIssue | None:
        if not node.name.startswith('test_'):
            return None

        parameter_names = tuple(
            parameter_name
            for spec in _parse_parametrize_specs(
                node.decorator_list,
                validation_context.marker_aliases,
                literal_bindings=validation_context.literal_bindings,
                expression_bindings=validation_context.expression_bindings,
            ).specs
            for parameter_name in spec.arg_names
        )
        if _supports_pytest_callable_signature(
            node,
            class_name=class_name,
            fixtures=fixtures,
            parameter_names=parameter_names,
        ):
            return None

        if class_name is None:
            message = (
                'PytestEngine v1 only supports top-level test functions '
                'with positional fixture parameters, optional pytest '
                'runtime fixture resolution and literal parametrization'
            )
        else:
            message = (
                'PytestEngine v1 only supports test methods with a single '
                '`self` parameter plus fixture parameters resolvable '
                'directly or through the pytest runtime adapter and '
                'literal parametrization'
            )

        return DraftValidationIssue(
            code='pytest_unsupported_test_signature',
            message=message,
            severity='error',
            line=node.lineno,
            column=getattr(node, 'col_offset', 0) + 1,
        )

    def _build_parametrize_issue(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        marker_aliases,
        literal_bindings,
        expression_bindings,
    ) -> DraftValidationIssue | None:
        if not node.name.startswith('test_'):
            return None

        parametrize_result = _parse_parametrize_specs(
            node.decorator_list,
            marker_aliases,
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )
        if parametrize_result.issue_code is None:
            return None

        return DraftValidationIssue(
            code=parametrize_result.issue_code,
            message=parametrize_result.issue_message
            or 'Unsupported @pytest.mark.parametrize usage',
            severity='error',
            line=parametrize_result.issue_line or node.lineno,
            column=getattr(node, 'col_offset', 0) + 1,
        )

    def _build_usefixtures_issue(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        marker_aliases,
        inherited_usefixtures_issue: str | None = None,
    ) -> DraftValidationIssue | None:
        if not node.name.startswith('test_'):
            return None

        if inherited_usefixtures_issue is not None:
            return DraftValidationIssue(
                code='pytest_runtime_usefixtures',
                message=inherited_usefixtures_issue,
                severity='warning',
                line=node.lineno,
                column=getattr(node, 'col_offset', 0) + 1,
            )

        usefixtures_decision = _build_usefixtures_decision(
            node.decorator_list,
            marker_aliases,
        )
        if usefixtures_decision.issue_code is None:
            return None

        return DraftValidationIssue(
            code=usefixtures_decision.issue_code,
            message=usefixtures_decision.issue_message
            or 'Unsupported pytest usefixtures usage',
            severity='warning',
            line=usefixtures_decision.issue_line or node.lineno,
            column=getattr(node, 'col_offset', 0) + 1,
        )

    def _build_xfail_issue(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        marker_aliases,
        validation_context: _PytestDraftValidationContext,
        inherited_runtime_reason: str | None = None,
        inherited_xfail_issue: str | None = None,
    ) -> DraftValidationIssue | None:
        if not node.name.startswith('test_'):
            return None

        if inherited_xfail_issue is not None:
            return DraftValidationIssue(
                code='pytest_unsupported_xfail_condition',
                message=inherited_xfail_issue,
                severity='error',
                line=node.lineno,
                column=getattr(node, 'col_offset', 0) + 1,
            )
        if inherited_runtime_reason is not None:
            return DraftValidationIssue(
                code='pytest_runtime_xfail_condition',
                message=inherited_runtime_reason,
                severity='warning',
                line=node.lineno,
                column=getattr(node, 'col_offset', 0) + 1,
            )

        xfail_decision = _build_static_xfail_decision(
            node.decorator_list,
            marker_aliases,
            literal_bindings=validation_context.literal_bindings,
            expression_bindings=validation_context.expression_bindings,
        )
        if (
            xfail_decision.issue_code is None
            and not xfail_decision.requires_pytest_runtime
        ):
            return None

        return DraftValidationIssue(
            code=xfail_decision.issue_code or 'pytest_runtime_xfail_condition',
            message=xfail_decision.issue_message
            or xfail_decision.runtime_reason
            or 'Unsupported pytest xfail condition',
            severity=(
                'error' if xfail_decision.issue_code is not None else 'warning'
            ),
            line=xfail_decision.issue_line or node.lineno,
            column=getattr(node, 'col_offset', 0) + 1,
        )

    def _build_skip_issue(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        marker_aliases,
        validation_context: _PytestDraftValidationContext,
        inherited_runtime_reason: str | None = None,
        inherited_skip_issue: str | None = None,
    ) -> DraftValidationIssue | None:
        if not node.name.startswith('test_'):
            return None

        if inherited_skip_issue is not None:
            return DraftValidationIssue(
                code='pytest_unsupported_skip_condition',
                message=inherited_skip_issue,
                severity='error',
                line=node.lineno,
                column=getattr(node, 'col_offset', 0) + 1,
            )
        if inherited_runtime_reason is not None:
            return DraftValidationIssue(
                code='pytest_runtime_skip_condition',
                message=inherited_runtime_reason,
                severity='warning',
                line=node.lineno,
                column=getattr(node, 'col_offset', 0) + 1,
            )

        skip_decision = _build_static_skip_decision(
            node.decorator_list,
            marker_aliases,
            literal_bindings=validation_context.literal_bindings,
            expression_bindings=validation_context.expression_bindings,
        )
        if (
            skip_decision.issue_code is None
            and not skip_decision.requires_pytest_runtime
        ):
            return None

        return DraftValidationIssue(
            code=skip_decision.issue_code or 'pytest_runtime_skip_condition',
            message=skip_decision.issue_message
            or skip_decision.runtime_reason
            or 'Unsupported pytest skip condition',
            severity=(
                'error' if skip_decision.issue_code is not None else 'warning'
            ),
            line=skip_decision.issue_line or node.lineno,
            column=getattr(node, 'col_offset', 0) + 1,
        )

    def _build_fixture_issue(
        self,
        fixture,
        *,
        fixtures,
    ) -> DraftValidationIssue | None:
        if _is_supported_fixture_definition(
            fixture.function_name,
            fixture,
            fixtures,
            resource_fixture_names=tuple(
                binding.fixture_name
                for binding in self.resource_bindings
                if binding.fixture_name is not None
            ),
        ):
            return None

        support_reason = _get_fixture_support_reason(
            fixture.function_name,
            fixtures,
            resource_fixture_names=tuple(
                binding.fixture_name
                for binding in self.resource_bindings
                if binding.fixture_name is not None
            ),
        )
        if support_reason == 'cyclic_dependency':
            message = (
                'PytestEngine v1 does not support cyclic fixture dependencies'
            )
        else:
            message = (
                'PytestEngine v1 only supports fixtures whose unresolved '
                'dependencies can degrade to the pytest runtime adapter'
            )

        return DraftValidationIssue(
            code='pytest_unsupported_fixture_definition',
            message=message,
            severity='error',
            line=fixture.line,
            column=1,
        )


def _resolve_pytest_definitions(
    context: _PytestDefinitionResolutionContext,
    *,
    step_type: str,
    step_text: str,
) -> tuple[ResolvedDefinition, ...]:
    if step_type == 'fixture':
        return _resolve_fixture_definitions(
            context=context,
            test_path=context.test_path,
            fixture_name=step_text,
        )

    if step_type == 'plugin':
        return _resolve_external_pytest_plugin_definitions(
            context=context,
            plugin_spec=step_text,
        )

    if step_type == 'test':
        return _resolve_test_definitions(
            engine_name=context.engine_name,
            test_path=context.test_path,
            test_name=step_text,
            root_path=context.root_path,
        )

    return ()


def _resolve_fixture_definitions(
    *,
    context: _PytestDefinitionResolutionContext,
    test_path: Path,
    fixture_name: str,
) -> tuple[ResolvedDefinition, ...]:
    imported_definition = _resolve_imported_fixture_binding_definition(
        test_path=test_path,
        fixture_name=fixture_name,
        context=context,
    )
    if imported_definition is not None:
        return (imported_definition,)

    visible_sources = _discover_visible_fixture_source_paths(
        test_path,
        root_path=context.root_path,
        configured_definition_paths=context.configured_definition_paths,
    )
    conftest_source_paths = tuple(
        path.resolve()
        for path in _discover_conftest_paths(
            test_path,
            root_path=context.root_path,
        )
    )
    configured_source_paths = tuple(
        path.resolve() for path in context.configured_definition_paths
    )
    for source_path in visible_sources:
        source_metadata = _build_fixture_source_metadata(
            source_path,
            root_path=context.root_path,
            configured_definition_paths=context.configured_definition_paths,
            visible_test_paths=(test_path,),
            conftest_source_paths=conftest_source_paths,
            configured_source_paths=configured_source_paths,
        )
        for record in _discover_fixture_knowledge_records(
            source_path,
            source_metadata=source_metadata,
        ):
            if record.function_name != fixture_name:
                continue

            return (
                ResolvedDefinition(
                    engine_name=context.engine_name,
                    file_path=str(source_path.resolve()),
                    line=record.line,
                    step_type='fixture',
                    patterns=(record.function_name,),
                    resolution_source='static_catalog',
                    function_name=record.function_name,
                    category=record.source_category,
                    provider_kind=record.provider_kind,
                    provider_name=record.provider_name,
                    documentation=record.documentation,
                ),
            )

    binding = next(
        (
            current_binding
            for current_binding in context.resource_bindings
            if current_binding.fixture_name == fixture_name
        ),
        None,
    )
    if binding is None:
        return ()

    manifest_path = discover_cosecha_manifest()
    resolved_file_path = (
        str(manifest_path.resolve())
        if manifest_path is not None
        else str(test_path.resolve())
    )
    return (
        ResolvedDefinition(
            engine_name=context.engine_name,
            file_path=resolved_file_path,
            line=1,
            step_type='fixture',
            patterns=(fixture_name,),
            resolution_source='manifest_resource_binding',
            function_name=fixture_name,
            category='resource_fixture',
            provider_kind='resource_binding',
            provider_name=binding.resource_name,
            documentation=(
                f'Fixture bridge to shared resource {binding.resource_name!r}'
            ),
        ),
    )


def _resolve_imported_fixture_binding_definition(
    *,
    test_path: Path,
    fixture_name: str,
    context: _PytestDefinitionResolutionContext,
) -> ResolvedDefinition | None:
    source_paths = (
        test_path,
        *reversed(
            _discover_conftest_paths(
                test_path,
                root_path=context.root_path,
            ),
        ),
        *context.configured_definition_paths,
    )
    for source_path in source_paths:
        binding = _discover_imported_fixture_bindings(
            source_path,
            root_path=context.root_path,
            configured_definition_paths=context.configured_definition_paths,
        ).get(fixture_name)
        if binding is None or binding.source_path is None:
            continue

        binding_source_path = Path(binding.source_path).resolve()
        source_metadata = _build_fixture_source_metadata(
            binding_source_path,
            root_path=context.root_path,
            configured_definition_paths=context.configured_definition_paths,
            visible_test_paths=(test_path,),
        )
        for record in _discover_fixture_knowledge_records(
            binding_source_path,
            source_metadata=source_metadata,
        ):
            if record.function_name != binding.function_name:
                continue

            return ResolvedDefinition(
                engine_name=context.engine_name,
                file_path=str(binding_source_path),
                line=record.line,
                step_type='fixture',
                patterns=(fixture_name,),
                resolution_source='static_catalog',
                function_name=fixture_name,
                category=record.source_category,
                provider_kind=record.provider_kind,
                provider_name=record.provider_name,
                documentation=record.documentation,
            )

    return None


def _resolve_external_pytest_plugin_definitions(
    *,
    context: _PytestDefinitionResolutionContext,
    plugin_spec: str,
) -> tuple[ResolvedDefinition, ...]:
    references = _discover_external_pytest_plugin_references(
        (context.test_path,),
        root_path=context.root_path,
        configured_definition_paths=context.configured_definition_paths,
    )
    matching_reference = next(
        (
            reference
            for reference in references
            if reference.module_spec == plugin_spec
        ),
        None,
    )
    if matching_reference is None:
        return ()

    return (
        ResolvedDefinition(
            engine_name=context.engine_name,
            file_path=_build_external_pytest_plugin_file_path(plugin_spec),
            line=1,
            step_type='plugin',
            patterns=(plugin_spec,),
            resolution_source='static_catalog',
            function_name='pytest_plugins',
            category='external_pytest_plugin',
            provider_kind='pytest_plugin_external',
            provider_name=plugin_spec,
            runtime_required=True,
            runtime_reason=matching_reference.runtime_reason,
            declaration_origin='pytest_plugins',
            documentation=matching_reference.runtime_reason,
        ),
    )


def _resolve_test_definitions(
    *,
    engine_name: str,
    test_path: Path,
    test_name: str,
    root_path: Path,
) -> tuple[ResolvedDefinition, ...]:
    definitions = _discover_pytest_tests(
        test_path,
        root_path=root_path,
    )
    documentation_by_name = _discover_test_documentation(test_path)
    matches: list[ResolvedDefinition] = []
    for definition in definitions:
        patterns = _build_test_resolution_patterns(definition)
        if test_name not in patterns:
            continue

        matches.append(
            ResolvedDefinition(
                engine_name=engine_name,
                file_path=str(test_path.resolve()),
                line=definition.line,
                step_type='test',
                patterns=patterns,
                resolution_source='static_catalog',
                function_name=definition.function_name,
                category='test',
                documentation=documentation_by_name.get(
                    (definition.class_name, definition.function_name),
                ),
            ),
        )

    return tuple(matches)


def _discover_test_documentation(
    test_path: Path,
) -> dict[tuple[str | None, str], str | None]:
    module = ast.parse(test_path.read_text(encoding='utf-8'))
    documentation: dict[tuple[str | None, str], str | None] = {}
    for statement in module.body:
        if isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
            documentation[(None, statement.name)] = ast.get_docstring(
                statement,
            )
            continue

        if not isinstance(statement, ast.ClassDef):
            continue

        for child in statement.body:
            if not isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                continue

            documentation[(statement.name, child.name)] = ast.get_docstring(
                child,
            )

    return documentation


def _build_test_resolution_patterns(
    definition: PytestTestDefinition,
) -> tuple[str, ...]:
    patterns = [_build_pytest_test_name(definition)]
    if definition.class_name is None:
        patterns.append(definition.function_name)
    else:
        patterns.append(
            f'{definition.class_name}.{definition.function_name}',
        )
        patterns.append(definition.function_name)

    seen_patterns: set[str] = set()
    ordered_patterns: list[str] = []
    for pattern in patterns:
        if pattern in seen_patterns:
            continue

        seen_patterns.add(pattern)
        ordered_patterns.append(pattern)

    return tuple(ordered_patterns)
