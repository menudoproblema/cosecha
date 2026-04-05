from __future__ import annotations

import ast
import asyncio
import hashlib
import operator as operator_module
import os
import sys

from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from cosecha.core.collector import Collector
from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord
from cosecha.core.domain_events import (
    KnowledgeIndexedEvent,
    KnowledgeInvalidatedEvent,
    TestKnowledgeIndexedEvent,
    TestKnowledgeInvalidatedEvent,
)
from cosecha.core.execution_ir import (
    build_execution_node_stable_id,
    build_test_path_label,
)
from cosecha.core.knowledge_test_descriptor import TestDescriptorKnowledge
from cosecha.engine.pytest.items import (
    PytestTestDefinition,
    PytestTestItem,
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from cosecha.core.items import TestItem


PARAMETRIZE_MIN_ARG_COUNT = 2
PARAMETRIZE_IDS_POSITION = 2
PARAMETRIZE_IDS_ARG_COUNT = 3
PARSE_FAILURE = object()


@dataclass(slots=True, frozen=True)
class PytestDefinitionIndex:
    tests: tuple[PytestTestDefinitionRecord, ...]


@dataclass(slots=True, frozen=True)
class PytestTestDefinitionRecord:
    file_path: str
    function_name: str
    line: int
    class_name: str | None = None
    fixture_names: tuple[str, ...] = ()
    parameter_case_id: str | None = None
    selection_labels: tuple[str, ...] = ()
    required_runtime_interfaces: tuple[str, ...] = ()
    required_runtime_capabilities: tuple[tuple[str, str], ...] = ()
    required_runtime_modes: tuple[tuple[str, str], ...] = ()
    disallowed_runtime_modes: tuple[tuple[str, str], ...] = ()


@dataclass(slots=True, frozen=True)
class PytestFixtureDefinition:
    function_name: str
    line: int
    fixture_names: tuple[str, ...] = ()
    is_async: bool = False
    uses_yield: bool = False
    source_path: str | None = None
    source_category: str = 'fixture'
    provider_kind: str | None = None
    provider_name: str | None = None


@dataclass(slots=True, frozen=True)
class PytestFixtureKnowledgeRecord:
    source_path: str
    function_name: str
    line: int
    fixture_names: tuple[str, ...] = ()
    source_category: str = 'fixture'
    provider_kind: str | None = None
    provider_name: str | None = None
    documentation: str | None = None


@dataclass(slots=True, frozen=True)
class PytestParametrizeCase:
    values: tuple[object, ...]
    case_id: str | None = None
    selection_labels: tuple[str, ...] = ()
    usefixture_names: tuple[str, ...] = ()
    skip_reason: str | None = None
    skip_issue: str | None = None
    xfail_reason: str | None = None
    xfail_issue: str | None = None
    xfail_strict: bool = False
    xfail_run: bool = True
    xfail_raises_paths: tuple[str, ...] = ()
    requires_pytest_runtime: bool = False
    pytest_runtime_reason: str | None = None


@dataclass(slots=True, frozen=True)
class PytestParametrizeSpec:
    arg_names: tuple[str, ...]
    cases: tuple[PytestParametrizeCase, ...]
    line: int
    indirect_arg_names: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class PytestParametrizeParseResult:
    specs: tuple[PytestParametrizeSpec, ...] = ()
    issue_code: str | None = None
    issue_message: str | None = None
    issue_line: int | None = None


@dataclass(slots=True, frozen=True)
class _PytestParametrizeContext:
    marker_aliases: set[str]
    literal_bindings: dict[str, object]
    expression_bindings: dict[str, ast.expr]


@dataclass(slots=True, frozen=True)
class PytestExpandedParametrizeCase:
    parameter_values: tuple[tuple[str, object], ...] = ()
    indirect_fixture_names: tuple[str, ...] = ()
    case_id: str | None = None
    selection_labels: tuple[str, ...] = ()
    usefixture_names: tuple[str, ...] = ()
    skip_reason: str | None = None
    skip_issue: str | None = None
    xfail_reason: str | None = None
    xfail_issue: str | None = None
    xfail_strict: bool = False
    xfail_run: bool = True
    xfail_raises_paths: tuple[str, ...] = ()
    requires_pytest_runtime: bool = False
    pytest_runtime_reason: str | None = None


@dataclass(slots=True, frozen=True)
class PytestDefinitionBuildContext:
    class_name: str | None = None
    conftest_paths: tuple[str, ...] = ()
    configured_definition_paths: tuple[str, ...] = ()
    imported_definition_paths: tuple[str, ...] = ()
    resource_fixture_names: tuple[str, ...] = ()
    literal_bindings: dict[str, object] | None = None
    expression_bindings: dict[str, ast.expr] | None = None
    inherited_selection_labels: tuple[str, ...] = ()
    inherited_runtime_interfaces: tuple[str, ...] = ()
    inherited_runtime_capabilities: tuple[tuple[str, str], ...] = ()
    inherited_required_runtime_modes: tuple[tuple[str, str], ...] = ()
    inherited_disallowed_runtime_modes: tuple[tuple[str, str], ...] = ()
    inherited_usefixture_names: tuple[str, ...] = ()
    inherited_usefixture_issue: str | None = None
    inherited_requires_pytest_runtime: bool = False
    inherited_pytest_runtime_reason: str | None = None
    inherited_skip_reason: str | None = None
    inherited_skip_issue: str | None = None
    inherited_skip_runtime_reason: str | None = None
    inherited_xfail: PytestStaticXfailDecision | None = None


@dataclass(slots=True, frozen=True)
class PytestModuleDiscoveryContext:
    source_path: Path | None = None
    root_path: Path | None = None
    fixtures: dict[str, PytestFixtureDefinition] | None = None
    conftest_paths: tuple[str, ...] = ()
    configured_definition_paths: tuple[str, ...] = ()
    resource_fixture_names: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class PytestFixtureSourceMetadata:
    category: str = 'fixture'
    provider_kind: str | None = None
    provider_name: str | None = None


@dataclass(slots=True, frozen=True)
class PytestPluginSource:
    module_spec: str
    source_path: Path


@dataclass(slots=True, frozen=True)
class PytestExternalPluginReference:
    module_spec: str
    runtime_reason: str


@dataclass(slots=True, frozen=True)
class PytestStaticSkipDecision:
    skip_reason: str | None = None
    issue_code: str | None = None
    issue_message: str | None = None
    issue_line: int | None = None
    requires_pytest_runtime: bool = False
    runtime_reason: str | None = None


@dataclass(slots=True, frozen=True)
class PytestStaticXfailDecision:
    xfail_reason: str | None = None
    issue_code: str | None = None
    issue_message: str | None = None
    issue_line: int | None = None
    strict: bool = False
    run: bool = True
    raises_paths: tuple[str, ...] = ()
    requires_pytest_runtime: bool = False
    runtime_reason: str | None = None


@dataclass(slots=True, frozen=True)
class PytestUsefixturesDecision:
    fixture_names: tuple[str, ...] = ()
    issue_code: str | None = None
    issue_message: str | None = None
    issue_line: int | None = None
    requires_pytest_runtime: bool = False
    runtime_reason: str | None = None


@dataclass(slots=True, frozen=True)
class PytestFilterwarningsDecision:
    requires_pytest_runtime: bool = False
    runtime_reason: str | None = None


class PytestCollector(Collector):
    __slots__ = (
        '_configured_definition_paths',
        'definition_index',
        'resource_fixture_names',
    )

    def __init__(self) -> None:
        super().__init__('py')
        self.definition_index = PytestDefinitionIndex(())
        self._configured_definition_paths: tuple[Path, ...] = ()
        self.resource_fixture_names: tuple[str, ...] = ()

    def initialize(
        self,
        config,
        base_path: str | Path | None = None,
    ) -> None:
        super().initialize(config, base_path)
        self._configured_definition_paths = tuple(
            _discover_configured_definition_source_paths(
                self.config.definition_paths,
            ),
        )

    async def find_test_files(self, base_path: Path) -> list[Path]:
        candidates = await super().find_test_files(base_path)
        return [
            candidate
            for candidate in candidates
            if _is_pytest_test_file(candidate)
        ]

    async def load_tests_from_file(
        self,
        test_path: Path,
    ) -> list[TestItem] | None:
        try:
            definitions = await asyncio.to_thread(
                _discover_pytest_tests,
                test_path,
                root_path=self.config.root_path,
                configured_definition_paths=self._configured_definition_paths,
                resource_fixture_names=self.resource_fixture_names,
            )
        except Exception:
            return None

        return [
            PytestTestItem(
                test_path,
                definition,
                self.config.root_path,
            )
            for definition in definitions
        ]

    async def collect(
        self,
        path: Path | tuple[Path, ...] | None,
        excluded_paths: tuple[Path, ...] = (),
    ):
        await super().collect(path, excluded_paths)
        self.definition_index = PytestDefinitionIndex(
            tuple(
                PytestTestDefinitionRecord(
                    file_path=str(test.path),
                    function_name=test.definition.function_name,
                    line=test.definition.line,
                    class_name=test.definition.class_name,
                    fixture_names=(
                        test.definition.fixture_names
                        + test.definition.usefixture_names
                    ),
                    parameter_case_id=test.definition.parameter_case_id,
                    selection_labels=test.definition.selection_labels,
                    required_runtime_interfaces=(
                        test.definition.required_runtime_interfaces
                    ),
                    required_runtime_capabilities=(
                        test.definition.required_runtime_capabilities
                    ),
                    required_runtime_modes=(
                        test.definition.required_runtime_modes
                    ),
                    disallowed_runtime_modes=(
                        test.definition.disallowed_runtime_modes
                    ),
                )
                for test in self.collected_tests
                if isinstance(test, PytestTestItem) and test.path is not None
            ),
        )
        await self._emit_knowledge_events()

    async def _emit_knowledge_events(self) -> None:
        if self._domain_event_stream is None:
            return

        indexed_files: dict[Path, list[PytestTestItem]] = {}
        for test in self.collected_tests:
            if not isinstance(test, PytestTestItem) or test.path is None:
                continue

            indexed_files.setdefault(test.path.resolve(), []).append(test)

        for file_path, tests in sorted(
            indexed_files.items(),
            key=lambda item: str(item[0]),
        ):
            test_path_label = build_test_path_label(
                self.config.root_path,
                file_path,
            )
            await self._domain_event_stream.emit(
                TestKnowledgeIndexedEvent(
                    engine_name=self._engine_name,
                    file_path=test_path_label,
                    tests=tuple(
                        TestDescriptorKnowledge(
                            stable_id=build_execution_node_stable_id(
                                self.config.root_path,
                                self._engine_name,
                                test,
                            ),
                            test_name=test.test_name,
                            file_path=test_path_label,
                            source_line=test.definition.line,
                            selection_labels=test.definition.selection_labels,
                        )
                        for test in tests
                    ),
                    discovery_mode='ast',
                    knowledge_version=_build_test_knowledge_version(
                        file_path,
                    ),
                    content_hash=_build_test_content_hash(file_path),
                ),
            )

        indexed_definitions = _discover_fixture_knowledge_by_source(
            tuple(indexed_files),
            root_path=self.config.root_path,
            configured_definition_paths=self._configured_definition_paths,
        )
        for source_path, definitions in sorted(
            indexed_definitions.items(),
            key=lambda item: str(item[0]),
        ):
            source_path_label = build_test_path_label(
                self.config.root_path,
                source_path,
            )
            await self._domain_event_stream.emit(
                KnowledgeIndexedEvent(
                    engine_name=self._engine_name,
                    file_path=source_path_label,
                    definition_count=len(definitions),
                    discovery_mode='ast',
                    knowledge_version=_build_definition_knowledge_version(
                        source_path,
                    ),
                    content_hash=_build_test_content_hash(source_path),
                    descriptors=tuple(
                        _build_fixture_definition_descriptor(record)
                        for record in definitions
                    ),
                ),
            )

        external_plugin_references = (
            _discover_external_pytest_plugin_references(
                tuple(indexed_files),
                root_path=self.config.root_path,
                configured_definition_paths=self._configured_definition_paths,
            )
        )
        for plugin_reference in external_plugin_references:
            await self._domain_event_stream.emit(
                KnowledgeIndexedEvent(
                    engine_name=self._engine_name,
                    file_path=_build_external_pytest_plugin_file_path(
                        plugin_reference.module_spec,
                    ),
                    definition_count=1,
                    discovery_mode='external_reference',
                    knowledge_version=(
                        f'pytest_external_plugin:{plugin_reference.module_spec}'
                    ),
                    content_hash=plugin_reference.module_spec,
                    descriptors=(
                        DefinitionKnowledgeRecord(
                            source_line=1,
                            function_name='pytest_plugins',
                            category='external_pytest_plugin',
                            provider_kind='pytest_plugin_external',
                            provider_name=plugin_reference.module_spec,
                            runtime_required=True,
                            runtime_reason=plugin_reference.runtime_reason,
                            declaration_origin='pytest_plugins',
                            documentation=plugin_reference.runtime_reason,
                            discovery_mode='external_reference',
                            payload_kind='pytest.plugin_reference',
                            payload_version=1,
                            payload={
                                'patterns': (plugin_reference.module_spec,),
                                'step_type': 'plugin',
                            },
                        ),
                    ),
                ),
            )

        for file_path in sorted(self.failed_files, key=str):
            file_path_label = str(file_path)
            await self._domain_event_stream.emit(
                TestKnowledgeInvalidatedEvent(
                    engine_name=self._engine_name,
                    file_path=file_path_label,
                    reason='parse_failed',
                    knowledge_version='pytest_test_index:invalid',
                ),
            )
            await self._domain_event_stream.emit(
                KnowledgeInvalidatedEvent(
                    engine_name=self._engine_name,
                    file_path=file_path_label,
                    reason='parse_failed',
                    knowledge_version='pytest_definition_index:invalid',
                ),
            )


def _is_pytest_test_file(path: Path) -> bool:
    file_name = path.name
    return file_name.startswith('test_') or file_name.endswith('_test.py')


def _discover_pytest_tests(
    test_path: Path,
    *,
    root_path: Path | None = None,
    configured_definition_paths: tuple[Path, ...] = (),
    resource_fixture_names: tuple[str, ...] = (),
) -> tuple[PytestTestDefinition, ...]:
    module = ast.parse(test_path.read_text(encoding='utf-8'))
    conftest_paths = _discover_conftest_paths(
        test_path,
        root_path=root_path,
    )
    nonlocal_fixtures = _discover_nonlocal_fixture_definitions(
        test_path,
        root_path=root_path,
        configured_definition_paths=configured_definition_paths,
    )
    return discover_pytest_tests_from_module(
        module,
        discovery_context=PytestModuleDiscoveryContext(
            source_path=test_path,
            root_path=root_path,
            fixtures=nonlocal_fixtures,
            conftest_paths=tuple(str(path) for path in conftest_paths),
            configured_definition_paths=tuple(
                str(path) for path in configured_definition_paths
            ),
            resource_fixture_names=resource_fixture_names,
        ),
    )


def discover_pytest_tests_from_content(
    source_content: str,
) -> tuple[PytestTestDefinition, ...]:
    return discover_pytest_tests_from_module(
        ast.parse(source_content),
    )


def discover_pytest_tests_from_module(
    module: ast.Module,
    *,
    discovery_context: PytestModuleDiscoveryContext | None = None,
) -> tuple[PytestTestDefinition, ...]:
    effective_context = (
        PytestModuleDiscoveryContext()
        if discovery_context is None
        else discovery_context
    )
    marker_aliases = _discover_marker_aliases(module)
    fixture_aliases = _discover_fixture_aliases(module)
    configured_definition_source_paths = tuple(
        Path(path) for path in effective_context.configured_definition_paths
    )
    literal_bindings, expression_bindings = _discover_static_binding_context(
        module,
        source_path=effective_context.source_path,
        root_path=effective_context.root_path,
        configured_definition_paths=configured_definition_source_paths,
    )
    imported_fixture_bindings = {}
    imported_definition_paths: tuple[str, ...] = ()
    pytest_plugins_runtime_reason: str | None = None
    if effective_context.source_path is not None:
        visible_source_paths = _discover_visible_fixture_source_paths(
            effective_context.source_path,
            root_path=effective_context.root_path,
            configured_definition_paths=configured_definition_source_paths,
        )
        imported_fixture_bindings = _discover_imported_fixture_bindings(
            effective_context.source_path,
            root_path=effective_context.root_path,
            configured_definition_paths=configured_definition_source_paths,
        )
        pytest_plugins_runtime_reason = (
            _discover_pytest_plugins_runtime_reason(
                module,
                source_path=effective_context.source_path,
                root_path=effective_context.root_path,
                configured_definition_paths=configured_definition_source_paths,
                literal_bindings=literal_bindings,
                expression_bindings=expression_bindings,
            )
        )
        excluded_source_paths = {
            effective_context.source_path.resolve(),
            *(
                Path(path).resolve()
                for path in effective_context.conftest_paths
            ),
            *(path.resolve() for path in configured_definition_source_paths),
        }
        imported_definition_paths = tuple(
            sorted(
                {
                    str(source_path.resolve())
                    for source_path in visible_source_paths
                    if source_path.resolve() not in excluded_source_paths
                },
            ),
        )
    effective_fixtures = {
        **(effective_context.fixtures or {}),
        **_discover_fixture_definitions_for_source_paths(
            tuple(Path(path) for path in imported_definition_paths),
            root_path=effective_context.root_path,
            configured_definition_paths=configured_definition_source_paths,
        ),
        **imported_fixture_bindings,
        **_discover_fixture_definitions(module, fixture_aliases),
    }
    return tuple(
        definition
        for statement in module.body
        for definition in _discover_statement_definitions(
            statement,
            marker_aliases,
            effective_fixtures,
            build_context=PytestDefinitionBuildContext(
                conftest_paths=effective_context.conftest_paths,
                configured_definition_paths=(
                    effective_context.configured_definition_paths
                ),
                imported_definition_paths=imported_definition_paths,
                resource_fixture_names=(
                    effective_context.resource_fixture_names
                ),
                literal_bindings=literal_bindings,
                expression_bindings=expression_bindings,
                inherited_requires_pytest_runtime=(
                    pytest_plugins_runtime_reason is not None
                ),
                inherited_pytest_runtime_reason=(
                    pytest_plugins_runtime_reason
                ),
            ),
        )
    )


def _discover_statement_definitions(
    statement: ast.stmt,
    marker_aliases: Iterable[str],
    fixtures: dict[str, PytestFixtureDefinition],
    *,
    build_context: PytestDefinitionBuildContext,
) -> tuple[PytestTestDefinition, ...]:
    if isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
        return _build_test_definitions(
            statement,
            marker_aliases,
            fixtures=fixtures,
            build_context=build_context,
        )

    if not isinstance(statement, ast.ClassDef):
        return ()

    if not statement.name.startswith('Test'):
        return ()

    class_labels = _extract_selection_labels(
        statement.decorator_list,
        marker_aliases,
    )
    (
        class_runtime_interfaces,
        class_runtime_capabilities,
        class_required_runtime_modes,
        class_disallowed_runtime_modes,
    ) = _extract_runtime_requirements(
        statement.decorator_list,
        marker_aliases,
    )
    class_skip_decision = _build_static_skip_decision(
        statement.decorator_list,
        marker_aliases,
        literal_bindings=build_context.literal_bindings or {},
        expression_bindings=build_context.expression_bindings or {},
    )
    class_xfail_decision = _build_static_xfail_decision(
        statement.decorator_list,
        marker_aliases,
        literal_bindings=build_context.literal_bindings or {},
        expression_bindings=build_context.expression_bindings or {},
    )
    class_usefixtures_decision = _build_usefixtures_decision(
        statement.decorator_list,
        marker_aliases,
    )
    class_filterwarnings_decision = _build_filterwarnings_decision(
        statement.decorator_list,
        marker_aliases,
    )
    class_requires_pytest_runtime = (
        class_usefixtures_decision.requires_pytest_runtime
        or class_filterwarnings_decision.requires_pytest_runtime
        or _requires_pytest_runtime(
            class_usefixtures_decision.fixture_names,
            fixtures,
            resource_fixture_names=build_context.resource_fixture_names,
        )
    )
    class_pytest_runtime_reason = _merge_pytest_runtime_reasons(
        class_skip_decision.runtime_reason,
        class_xfail_decision.runtime_reason,
        class_usefixtures_decision.runtime_reason,
        class_filterwarnings_decision.runtime_reason,
        _get_pytest_runtime_reason(
            class_usefixtures_decision.fixture_names,
            fixtures,
            resource_fixture_names=build_context.resource_fixture_names,
        ),
    )
    definitions: list[PytestTestDefinition] = []
    for child in statement.body:
        if not isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
            continue

        definitions.extend(
            _build_test_definitions(
                child,
                marker_aliases,
                fixtures=fixtures,
                build_context=PytestDefinitionBuildContext(
                    class_name=statement.name,
                    conftest_paths=build_context.conftest_paths,
                    configured_definition_paths=(
                        build_context.configured_definition_paths
                    ),
                    imported_definition_paths=(
                        build_context.imported_definition_paths
                    ),
                    resource_fixture_names=(
                        build_context.resource_fixture_names
                    ),
                    literal_bindings=build_context.literal_bindings,
                    expression_bindings=build_context.expression_bindings,
                    inherited_selection_labels=class_labels,
                    inherited_runtime_interfaces=(class_runtime_interfaces),
                    inherited_runtime_capabilities=(
                        class_runtime_capabilities
                    ),
                    inherited_required_runtime_modes=(
                        class_required_runtime_modes
                    ),
                    inherited_disallowed_runtime_modes=(
                        class_disallowed_runtime_modes
                    ),
                    inherited_usefixture_names=(
                        class_usefixtures_decision.fixture_names
                    ),
                    inherited_usefixture_issue=(
                        class_usefixtures_decision.issue_message
                    ),
                    inherited_requires_pytest_runtime=(
                        class_requires_pytest_runtime
                    ),
                    inherited_pytest_runtime_reason=(
                        class_pytest_runtime_reason
                    ),
                    inherited_skip_reason=class_skip_decision.skip_reason,
                    inherited_skip_issue=class_skip_decision.issue_message,
                    inherited_skip_runtime_reason=(
                        class_skip_decision.runtime_reason
                    ),
                    inherited_xfail=class_xfail_decision,
                ),
            ),
        )

    return tuple(definitions)


def _discover_marker_aliases(module: ast.Module) -> set[str]:
    aliases = {'pytest'}
    for statement in module.body:
        if isinstance(statement, ast.Import):
            for alias in statement.names:
                if alias.name == 'pytest':
                    aliases.add(alias.asname or alias.name)
        elif isinstance(statement, ast.ImportFrom):
            if statement.module != 'pytest':
                continue
            for alias in statement.names:
                if alias.name == 'mark':
                    aliases.add(alias.asname or alias.name)

    return aliases


def _discover_fixture_aliases(module: ast.Module) -> set[str]:
    aliases = {'fixture'}
    pytest_aliases = {'pytest'}
    for statement in module.body:
        if isinstance(statement, ast.Import):
            for alias in statement.names:
                if alias.name == 'pytest':
                    pytest_aliases.add(alias.asname or alias.name)
        elif isinstance(statement, ast.ImportFrom):
            if statement.module != 'pytest':
                continue

            for alias in statement.names:
                if alias.name == 'fixture':
                    aliases.add(alias.asname or alias.name)

    return aliases | pytest_aliases


def _discover_fixture_definitions(
    module: ast.Module,
    fixture_aliases: Iterable[str],
    *,
    source_path: Path | None = None,
    source_metadata: PytestFixtureSourceMetadata | None = None,
) -> dict[str, PytestFixtureDefinition]:
    effective_source_metadata = (
        PytestFixtureSourceMetadata()
        if source_metadata is None
        else source_metadata
    )
    fixtures: dict[str, PytestFixtureDefinition] = {}
    for statement in module.body:
        if not isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
            continue

        if not _is_fixture_definition(statement, fixture_aliases):
            continue

        fixtures[statement.name] = PytestFixtureDefinition(
            function_name=statement.name,
            line=statement.lineno,
            fixture_names=_extract_fixture_names(
                statement,
                class_name=None,
            ),
            is_async=isinstance(statement, ast.AsyncFunctionDef),
            uses_yield=_uses_yield(statement),
            source_path=(
                None if source_path is None else str(source_path.resolve())
            ),
            source_category=effective_source_metadata.category,
            provider_kind=effective_source_metadata.provider_kind,
            provider_name=effective_source_metadata.provider_name,
        )

    return fixtures


def _discover_conftest_paths(
    test_path: Path,
    *,
    root_path: Path | None,
) -> tuple[Path, ...]:
    resolved_test_path = test_path.resolve()
    stop_path = _resolve_conftest_stop_path(
        resolved_test_path,
        root_path=root_path,
    )

    conftest_paths: list[Path] = []
    current_dir = resolved_test_path.parent
    while True:
        conftest_path = current_dir / 'conftest.py'
        if conftest_path.exists():
            conftest_paths.append(conftest_path)

        if current_dir in {stop_path, current_dir.parent}:
            break

        current_dir = current_dir.parent

    return tuple(reversed(conftest_paths))


def _resolve_conftest_stop_path(
    test_path: Path,
    *,
    root_path: Path | None,
) -> Path:
    if root_path is None:
        return test_path.parent

    resolved_root_path = root_path.resolve()
    try:
        test_path.relative_to(resolved_root_path)
    except ValueError:
        return test_path.parent

    return resolved_root_path


def _discover_configured_definition_source_paths(
    definition_paths: tuple[Path, ...],
) -> tuple[Path, ...]:
    source_paths: dict[Path, None] = {}
    for definition_path in definition_paths:
        resolved_path = definition_path.resolve()
        if resolved_path.is_file() and resolved_path.suffix == '.py':
            source_paths.setdefault(resolved_path, None)
            continue

        if not resolved_path.is_dir():
            continue

        for source_path in sorted(resolved_path.rglob('*.py')):
            source_paths.setdefault(source_path.resolve(), None)

    return tuple(sorted(source_paths, key=str))


def _discover_nonlocal_fixture_definitions(
    test_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
) -> dict[str, PytestFixtureDefinition]:
    fixtures: dict[str, PytestFixtureDefinition] = {}
    fixtures.update(
        _discover_pytest_plugin_fixture_definitions(
            test_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
        ),
    )
    for source_path in configured_definition_paths:
        fixtures.update(
            _discover_fixture_definitions_for_source_path(
                source_path,
                root_path=root_path,
                configured_definition_paths=configured_definition_paths,
                source_metadata=PytestFixtureSourceMetadata(
                    category='configured_definition_fixture',
                    provider_kind='definition_path',
                    provider_name=str(source_path.resolve()),
                ),
            ),
        )

    for conftest_path in _discover_conftest_paths(
        test_path,
        root_path=root_path,
    ):
        fixtures.update(
            _discover_fixture_definitions_for_source_path(
                conftest_path,
                root_path=root_path,
                configured_definition_paths=configured_definition_paths,
                source_metadata=PytestFixtureSourceMetadata(
                    category='conftest_fixture',
                ),
            ),
        )

    return fixtures


def _discover_fixture_definitions_for_source_paths(
    source_paths: tuple[Path, ...],
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
) -> dict[str, PytestFixtureDefinition]:
    fixtures: dict[str, PytestFixtureDefinition] = {}
    for source_path in source_paths:
        fixtures.update(
            _discover_fixture_definitions_for_source_path(
                source_path,
                root_path=root_path,
                configured_definition_paths=configured_definition_paths,
            ),
        )

    return fixtures


def _discover_fixture_definitions_for_source_path(
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
    source_metadata: PytestFixtureSourceMetadata | None = None,
) -> dict[str, PytestFixtureDefinition]:
    resolved_source_path = source_path.resolve()
    effective_source_metadata = (
        PytestFixtureSourceMetadata()
        if source_metadata is None
        else source_metadata
    )
    imported_bindings = _discover_imported_fixture_bindings(
        resolved_source_path,
        root_path=root_path,
        configured_definition_paths=configured_definition_paths,
    )
    fixtures = {
        **_discover_fixture_definitions_from_imported_sources(
            imported_bindings.values(),
        ),
        **imported_bindings,
    }
    module = ast.parse(resolved_source_path.read_text(encoding='utf-8'))
    fixture_aliases = _discover_fixture_aliases(module)
    fixtures.update(
        _discover_fixture_definitions(
            module,
            fixture_aliases,
            source_path=resolved_source_path,
            source_metadata=effective_source_metadata,
        ),
    )
    return fixtures


def _discover_pytest_plugin_fixture_definitions(
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
    active_sources: tuple[Path, ...] = (),
) -> dict[str, PytestFixtureDefinition]:
    fixtures: dict[str, PytestFixtureDefinition] = {}
    for plugin_source in _discover_pytest_plugin_sources(
        source_path,
        root_path=root_path,
        configured_definition_paths=configured_definition_paths,
        active_sources=active_sources,
    ):
        fixtures.update(
            _discover_fixture_definitions_for_source_path(
                plugin_source.source_path,
                root_path=root_path,
                configured_definition_paths=configured_definition_paths,
                source_metadata=PytestFixtureSourceMetadata(
                    category='pytest_plugin_fixture',
                    provider_kind='pytest_plugin',
                    provider_name=plugin_source.module_spec,
                ),
            ),
        )

    return fixtures


def _discover_fixture_definitions_from_imported_sources(
    fixture_definitions: Iterable[PytestFixtureDefinition],
) -> dict[str, PytestFixtureDefinition]:
    fixtures: dict[str, PytestFixtureDefinition] = {}
    source_paths = tuple(
        sorted(
            {
                Path(source_path).resolve()
                for fixture in fixture_definitions
                if fixture.source_path is not None
                for source_path in (fixture.source_path,)
            },
            key=str,
        ),
    )
    for source_path in source_paths:
        module = ast.parse(source_path.read_text(encoding='utf-8'))
        fixture_aliases = _discover_fixture_aliases(module)
        fixtures.update(
            _discover_fixture_definitions(
                module,
                fixture_aliases,
                source_path=source_path,
            ),
        )

    return fixtures


def _discover_fixture_knowledge_by_source(
    test_paths: tuple[Path, ...],
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
) -> dict[Path, tuple[PytestFixtureKnowledgeRecord, ...]]:
    records_by_source: dict[Path, dict[str, PytestFixtureKnowledgeRecord]] = {}
    parsed_records_by_source: dict[
        Path,
        tuple[PytestFixtureKnowledgeRecord, ...],
    ] = {}
    visible_source_paths: dict[Path, None] = {}
    for source_path in configured_definition_paths:
        for visible_source_path in _discover_visible_fixture_source_paths(
            source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
        ):
            visible_source_paths[visible_source_path.resolve()] = None

    for test_path in test_paths:
        for visible_source_path in _discover_visible_fixture_source_paths(
            test_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
        ):
            visible_source_paths[visible_source_path.resolve()] = None

    for source_path in visible_source_paths:
        parsed_records = parsed_records_by_source.get(source_path)
        if parsed_records is None:
            source_metadata = _build_fixture_source_metadata(
                source_path,
                root_path=root_path,
                configured_definition_paths=configured_definition_paths,
                visible_test_paths=test_paths,
            )
            parsed_records = _discover_fixture_knowledge_records(
                source_path,
                source_metadata=source_metadata,
            )
            parsed_records_by_source[source_path] = parsed_records
        if not parsed_records:
            continue

        target_records = records_by_source.setdefault(source_path, {})
        for record in parsed_records:
            target_records.setdefault(record.function_name, record)

    return {
        source_path: tuple(
            records_by_name[name] for name in sorted(records_by_name)
        )
        for source_path, records_by_name in records_by_source.items()
    }


def _discover_fixture_knowledge_records(
    source_path: Path,
    *,
    source_metadata: PytestFixtureSourceMetadata | None = None,
) -> tuple[PytestFixtureKnowledgeRecord, ...]:
    effective_source_metadata = (
        PytestFixtureSourceMetadata()
        if source_metadata is None
        else source_metadata
    )
    module = ast.parse(source_path.read_text(encoding='utf-8'))
    fixture_aliases = _discover_fixture_aliases(module)
    fixtures = _discover_fixture_definitions(
        module,
        fixture_aliases,
        source_path=source_path,
        source_metadata=effective_source_metadata,
    )
    return tuple(
        PytestFixtureKnowledgeRecord(
            source_path=str(source_path),
            function_name=fixture.function_name,
            line=fixture.line,
            fixture_names=fixture.fixture_names,
            source_category=effective_source_metadata.category,
            provider_kind=effective_source_metadata.provider_kind,
            provider_name=effective_source_metadata.provider_name,
            documentation=_discover_function_documentation(
                module,
                fixture.function_name,
            ),
        )
        for fixture in fixtures.values()
    )


def _build_test_definitions(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    marker_aliases: Iterable[str],
    fixtures: dict[str, PytestFixtureDefinition],
    *,
    build_context: PytestDefinitionBuildContext,
) -> tuple[PytestTestDefinition, ...]:
    if not node.name.startswith('test_'):
        return ()

    parametrize_result = _parse_parametrize_specs(
        node.decorator_list,
        marker_aliases,
        literal_bindings=build_context.literal_bindings or {},
        expression_bindings=build_context.expression_bindings or {},
    )
    if parametrize_result.issue_code is not None:
        return ()

    skip_decision = _merge_skip_decisions(
        inherited_skip_reason=build_context.inherited_skip_reason,
        inherited_skip_issue=build_context.inherited_skip_issue,
        inherited_runtime_reason=build_context.inherited_skip_runtime_reason,
        local_decision=_build_static_skip_decision(
            node.decorator_list,
            marker_aliases,
            literal_bindings=build_context.literal_bindings or {},
            expression_bindings=build_context.expression_bindings or {},
        ),
    )
    xfail_decision = _merge_xfail_decisions(
        inherited_xfail=build_context.inherited_xfail,
        local_decision=_build_static_xfail_decision(
            node.decorator_list,
            marker_aliases,
            literal_bindings=build_context.literal_bindings or {},
            expression_bindings=build_context.expression_bindings or {},
        ),
    )
    usefixtures_decision = _build_usefixtures_decision(
        node.decorator_list,
        marker_aliases,
    )
    filterwarnings_decision = _build_filterwarnings_decision(
        node.decorator_list,
        marker_aliases,
    )

    parameter_names = tuple(
        parameter_name
        for spec in parametrize_result.specs
        for parameter_name in spec.arg_names
    )
    indirect_parameter_names = tuple(
        parameter_name
        for spec in parametrize_result.specs
        for parameter_name in spec.indirect_arg_names
    )
    if not _supports_pytest_callable_signature(
        node,
        class_name=build_context.class_name,
        fixtures=fixtures,
        parameter_names=parameter_names,
    ):
        return ()

    direct_parameter_name_set = set(parameter_names) - set(
        indirect_parameter_names,
    )
    fixture_names = tuple(
        fixture_name
        for fixture_name in _extract_fixture_names(
            node,
            class_name=build_context.class_name,
        )
        if fixture_name not in direct_parameter_name_set
    )
    usefixture_names = _merge_usefixture_names(
        build_context.inherited_usefixture_names,
        usefixtures_decision.fixture_names,
    )
    fixture_runtime_reason = _get_pytest_runtime_reason(
        fixture_names + usefixture_names,
        fixtures,
        resource_fixture_names=build_context.resource_fixture_names,
    )
    requires_pytest_runtime = _requires_pytest_runtime(
        fixture_names + usefixture_names,
        fixtures,
        resource_fixture_names=build_context.resource_fixture_names,
    ) or (
        build_context.inherited_requires_pytest_runtime
        or skip_decision.requires_pytest_runtime
        or xfail_decision.requires_pytest_runtime
        or usefixtures_decision.requires_pytest_runtime
        or filterwarnings_decision.requires_pytest_runtime
    )
    pytest_runtime_reason = _merge_pytest_runtime_reasons(
        build_context.inherited_pytest_runtime_reason,
        skip_decision.runtime_reason,
        xfail_decision.runtime_reason,
        usefixtures_decision.runtime_reason,
        filterwarnings_decision.runtime_reason,
        fixture_runtime_reason,
    )
    selection_labels = _extract_selection_labels(
        node.decorator_list,
        marker_aliases,
        inherited_labels=build_context.inherited_selection_labels,
    )
    (
        required_runtime_interfaces,
        required_runtime_capabilities,
        required_runtime_modes,
        disallowed_runtime_modes,
    ) = _extract_runtime_requirements(
        node.decorator_list,
        marker_aliases,
        inherited_interfaces=build_context.inherited_runtime_interfaces,
        inherited_capabilities=(build_context.inherited_runtime_capabilities),
        inherited_required_modes=(
            build_context.inherited_required_runtime_modes
        ),
        inherited_disallowed_modes=(
            build_context.inherited_disallowed_runtime_modes
        ),
    )
    expanded_cases = _build_parametrized_execution_cases(
        parametrize_result.specs,
    )
    definitions: list[PytestTestDefinition] = []
    for case in expanded_cases:
        final_usefixture_names = _merge_usefixture_names(
            usefixture_names,
            case.usefixture_names,
        )
        final_skip_decision = _merge_skip_decisions(
            inherited_skip_reason=skip_decision.skip_reason,
            inherited_skip_issue=skip_decision.issue_message,
            inherited_runtime_reason=skip_decision.runtime_reason,
            local_decision=PytestStaticSkipDecision(
                skip_reason=case.skip_reason,
                issue_code=case.skip_issue and 'pytest_case_skip_issue',
                issue_message=case.skip_issue,
            ),
        )
        final_xfail_decision = _merge_xfail_decisions(
            inherited_xfail=xfail_decision,
            local_decision=PytestStaticXfailDecision(
                xfail_reason=case.xfail_reason,
                issue_code=case.xfail_issue and 'pytest_case_xfail_issue',
                issue_message=case.xfail_issue,
                strict=case.xfail_strict,
                run=case.xfail_run,
                raises_paths=case.xfail_raises_paths,
            ),
        )
        definitions.append(
            PytestTestDefinition(
                function_name=node.name,
                line=node.lineno,
                class_name=build_context.class_name,
                fixture_names=fixture_names,
                usefixture_names=final_usefixture_names,
                conftest_paths=build_context.conftest_paths,
                configured_definition_paths=(
                    build_context.configured_definition_paths
                ),
                imported_definition_paths=(
                    build_context.imported_definition_paths
                ),
                skip_reason=final_skip_decision.skip_reason,
                skip_issue=final_skip_decision.issue_message,
                xfail_reason=final_xfail_decision.xfail_reason,
                xfail_issue=final_xfail_decision.issue_message,
                xfail_strict=final_xfail_decision.strict,
                xfail_run=final_xfail_decision.run,
                xfail_raises_paths=final_xfail_decision.raises_paths,
                requires_pytest_runtime=(
                    requires_pytest_runtime or case.requires_pytest_runtime
                ),
                pytest_runtime_reason=_merge_pytest_runtime_reasons(
                    pytest_runtime_reason,
                    case.pytest_runtime_reason,
                ),
                parameter_values=case.parameter_values,
                indirect_fixture_names=case.indirect_fixture_names,
                parameter_case_id=case.case_id,
                selection_labels=_merge_selection_labels(
                    selection_labels,
                    case.selection_labels,
                ),
                required_runtime_interfaces=required_runtime_interfaces,
                required_runtime_capabilities=required_runtime_capabilities,
                required_runtime_modes=required_runtime_modes,
                disallowed_runtime_modes=disallowed_runtime_modes,
            ),
        )

    return tuple(definitions)


def _extract_selection_labels(
    decorators: list[ast.expr],
    marker_aliases: Iterable[str],
    *,
    inherited_labels: tuple[str, ...] = (),
) -> tuple[str, ...]:
    labels: list[str] = list(inherited_labels)
    seen_labels: set[str] = set(inherited_labels)
    for decorator in decorators:
        label = _extract_marker_name(decorator, marker_aliases)
        if label is None or label in seen_labels:
            continue

        seen_labels.add(label)
        labels.append(label)

    return tuple(labels)


def _extract_runtime_requirements(  # noqa: PLR0913, PLR0912, PLR0915
    decorators: list[ast.expr],
    marker_aliases: Iterable[str],
    *,
    inherited_interfaces: tuple[str, ...] = (),
    inherited_capabilities: tuple[tuple[str, str], ...] = (),
    inherited_required_modes: tuple[tuple[str, str], ...] = (),
    inherited_disallowed_modes: tuple[tuple[str, str], ...] = (),
) -> tuple[
    tuple[str, ...],
    tuple[tuple[str, str], ...],
    tuple[tuple[str, str], ...],
    tuple[tuple[str, str], ...],
]:
    interfaces = list(inherited_interfaces)
    seen_interfaces = set(inherited_interfaces)
    capabilities = list(inherited_capabilities)
    seen_capabilities = set(inherited_capabilities)
    required_modes = list(inherited_required_modes)
    seen_required_modes = set(inherited_required_modes)
    disallowed_modes = list(inherited_disallowed_modes)
    seen_disallowed_modes = set(inherited_disallowed_modes)
    marker_alias_set = set(marker_aliases)
    for decorator in decorators:
        if not isinstance(decorator, ast.Call):
            continue

        marker_name = _extract_special_marker_name(
            decorator,
            marker_alias_set,
            marker_names={
                'requires',
                'requires_capability',
                'requires_mode',
                'disallow_mode',
            },
        )
        if marker_name == 'requires':
            interface_name = _extract_string_literal_argument(
                decorator,
                index=0,
            )
            if interface_name is None:
                continue
            if interface_name in seen_interfaces:
                continue
            seen_interfaces.add(interface_name)
            interfaces.append(interface_name)
            continue
        if marker_name == 'requires_capability':
            interface_name = _extract_string_literal_argument(
                decorator,
                index=0,
            )
            capability_name = _extract_string_literal_argument(
                decorator,
                index=1,
            )
            if interface_name is None or capability_name is None:
                continue
            capability = (interface_name, capability_name)
            if capability in seen_capabilities:
                continue
            seen_capabilities.add(capability)
            capabilities.append(capability)
            continue
        if marker_name == 'requires_mode':
            interface_name = _extract_string_literal_argument(
                decorator,
                index=0,
            )
            mode_name = _extract_string_literal_argument(
                decorator,
                index=1,
            )
            if interface_name is None or mode_name is None:
                continue
            required_mode = (interface_name, mode_name)
            if required_mode in seen_required_modes:
                continue
            seen_required_modes.add(required_mode)
            required_modes.append(required_mode)
            continue
        if marker_name == 'disallow_mode':
            interface_name = _extract_string_literal_argument(
                decorator,
                index=0,
            )
            mode_name = _extract_string_literal_argument(
                decorator,
                index=1,
            )
            if interface_name is None or mode_name is None:
                continue
            disallowed_mode = (interface_name, mode_name)
            if disallowed_mode in seen_disallowed_modes:
                continue
            seen_disallowed_modes.add(disallowed_mode)
            disallowed_modes.append(disallowed_mode)

    return (
        tuple(interfaces),
        tuple(capabilities),
        tuple(required_modes),
        tuple(disallowed_modes),
    )


def _extract_special_marker_name(
    decorator: ast.expr,
    marker_aliases: set[str],
    *,
    marker_names: set[str],
) -> str | None:
    current = decorator.func if isinstance(decorator, ast.Call) else decorator
    if not isinstance(current, ast.Attribute):
        return None

    if current.attr not in marker_names:
        return None

    if not isinstance(current.value, ast.Attribute):
        return None

    if current.value.attr != 'mark':
        return None

    base = current.value.value
    if not isinstance(base, ast.Name) or base.id not in marker_aliases:
        return None

    return current.attr


def _extract_string_literal_argument(
    decorator: ast.Call,
    *,
    index: int,
) -> str | None:
    if index >= len(decorator.args):
        return None

    argument = decorator.args[index]
    if not isinstance(argument, ast.Constant):
        return None

    return argument.value if isinstance(argument.value, str) else None


def _merge_selection_labels(
    inherited_labels: tuple[str, ...],
    local_labels: tuple[str, ...],
) -> tuple[str, ...]:
    labels = list(inherited_labels)
    seen_labels = set(inherited_labels)
    for label in local_labels:
        if label in seen_labels:
            continue

        seen_labels.add(label)
        labels.append(label)

    return tuple(labels)


def _build_static_skip_decision(
    decorators: Iterable[ast.expr],
    marker_aliases: Iterable[str],
    *,
    literal_bindings: dict[str, object] | None = None,
    expression_bindings: dict[str, ast.expr] | None = None,
) -> PytestStaticSkipDecision:
    for decorator in decorators:
        if _is_skip_decorator(decorator, set(marker_aliases)):
            return _parse_skip_decorator(
                decorator,
                literal_bindings=literal_bindings or {},
            )

        if _is_skipif_decorator(decorator, set(marker_aliases)):
            return _parse_skipif_decorator(
                decorator,
                literal_bindings=literal_bindings or {},
                expression_bindings=expression_bindings or {},
            )

    return PytestStaticSkipDecision()


def _build_static_xfail_decision(
    decorators: Iterable[ast.expr],
    marker_aliases: Iterable[str],
    *,
    literal_bindings: dict[str, object] | None = None,
    expression_bindings: dict[str, ast.expr] | None = None,
) -> PytestStaticXfailDecision:
    for decorator in decorators:
        if _is_xfail_decorator(decorator, set(marker_aliases)):
            return _parse_xfail_decorator(
                decorator,
                literal_bindings=literal_bindings or {},
                expression_bindings=expression_bindings or {},
            )

    return PytestStaticXfailDecision()


def _build_usefixtures_decision(
    decorators: Iterable[ast.expr],
    marker_aliases: Iterable[str],
) -> PytestUsefixturesDecision:
    fixture_names: list[str] = []
    seen_names: set[str] = set()
    marker_alias_set = set(marker_aliases)
    for decorator in decorators:
        if not _is_usefixtures_decorator(decorator, marker_alias_set):
            continue

        parsed_decision = _parse_usefixtures_decorator(decorator)
        for fixture_name in parsed_decision.fixture_names:
            if fixture_name in seen_names:
                continue

            seen_names.add(fixture_name)
            fixture_names.append(fixture_name)

        if parsed_decision.issue_code is not None:
            return PytestUsefixturesDecision(
                fixture_names=tuple(fixture_names),
                issue_code=parsed_decision.issue_code,
                issue_message=parsed_decision.issue_message,
                issue_line=parsed_decision.issue_line,
                requires_pytest_runtime=True,
                runtime_reason=parsed_decision.runtime_reason,
            )

    return PytestUsefixturesDecision(
        fixture_names=tuple(fixture_names),
    )


def _build_filterwarnings_decision(
    decorators: Iterable[ast.expr],
    marker_aliases: Iterable[str],
) -> PytestFilterwarningsDecision:
    marker_alias_set = set(marker_aliases)
    for decorator in decorators:
        if _is_filterwarnings_decorator(decorator, marker_alias_set):
            return PytestFilterwarningsDecision(
                requires_pytest_runtime=True,
                runtime_reason='filterwarnings mark requires pytest runtime',
            )

    return PytestFilterwarningsDecision()


def _merge_usefixture_names(
    inherited_fixture_names: tuple[str, ...],
    local_fixture_names: tuple[str, ...],
) -> tuple[str, ...]:
    merged_names = list(inherited_fixture_names)
    seen_names = set(inherited_fixture_names)
    for fixture_name in local_fixture_names:
        if fixture_name in seen_names:
            continue

        seen_names.add(fixture_name)
        merged_names.append(fixture_name)

    return tuple(merged_names)


def _merge_skip_decisions(
    *,
    inherited_skip_reason: str | None,
    inherited_skip_issue: str | None,
    inherited_runtime_reason: str | None = None,
    local_decision: PytestStaticSkipDecision,
) -> PytestStaticSkipDecision:
    if inherited_skip_reason is not None:
        return PytestStaticSkipDecision(skip_reason=inherited_skip_reason)

    if local_decision.skip_reason is not None:
        return local_decision

    if inherited_skip_issue is not None:
        return PytestStaticSkipDecision(
            issue_code='pytest_unsupported_skip_condition',
            issue_message=inherited_skip_issue,
        )

    if inherited_runtime_reason is not None:
        return PytestStaticSkipDecision(
            requires_pytest_runtime=True,
            runtime_reason=inherited_runtime_reason,
        )

    return local_decision


def _merge_xfail_decisions(
    *,
    inherited_xfail: PytestStaticXfailDecision | None,
    local_decision: PytestStaticXfailDecision,
) -> PytestStaticXfailDecision:
    if inherited_xfail is not None and (
        inherited_xfail.xfail_reason is not None
        or inherited_xfail.issue_code is not None
        or inherited_xfail.requires_pytest_runtime
    ):
        return inherited_xfail

    return local_decision


def _parse_skip_decorator(
    decorator: ast.expr,
    *,
    literal_bindings: dict[str, object] | None = None,
) -> PytestStaticSkipDecision:
    if not isinstance(decorator, ast.Call):
        return PytestStaticSkipDecision(skip_reason='Skipped by pytest mark')

    reason = _extract_marker_reason(
        decorator,
        default='Skipped by pytest mark',
        literal_bindings=literal_bindings or {},
    )
    return PytestStaticSkipDecision(skip_reason=reason)


def _parse_xfail_decorator(
    decorator: ast.expr,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> PytestStaticXfailDecision:
    if not isinstance(decorator, ast.Call):
        return PytestStaticXfailDecision(
            xfail_reason='Expected failure by pytest xfail mark',
        )

    condition_issue = _validate_xfail_condition(
        decorator,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    if condition_issue is not None:
        return condition_issue
    strict = _extract_bool_marker_keyword(
        decorator,
        keyword_name='strict',
        default=False,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    if strict is None:
        return _build_runtime_xfail_decision(
            decorator,
            'dynamic xfail strict flag requires pytest runtime adapter',
        )

    run = _extract_bool_marker_keyword(
        decorator,
        keyword_name='run',
        default=True,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    if run is None:
        return _build_runtime_xfail_decision(
            decorator,
            'dynamic xfail run flag requires pytest runtime adapter',
        )

    reason = _extract_marker_reason(
        decorator,
        default='Expected failure by pytest xfail mark',
        literal_bindings=literal_bindings,
    )
    raises_paths = _extract_xfail_raises_paths(
        decorator,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    if raises_paths is None:
        return _build_runtime_xfail_decision(
            decorator,
            'dynamic xfail raises filter requires pytest runtime adapter',
        )

    return PytestStaticXfailDecision(
        xfail_reason=reason,
        strict=strict,
        run=run,
        raises_paths=raises_paths,
    )


def _validate_xfail_condition(
    decorator: ast.Call,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> PytestStaticXfailDecision | None:
    condition_node = _extract_marker_condition_node(decorator)
    if condition_node is None:
        return None

    condition_result = _evaluate_static_skip_condition(
        condition_node,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    if _is_runtime_string_condition(
        condition_node,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    ):
        return _build_runtime_xfail_decision(
            decorator,
            'string xfail condition requires pytest runtime adapter',
        )
    if condition_result is None:
        return _build_runtime_xfail_decision(
            decorator,
            'dynamic xfail condition requires pytest runtime adapter',
        )

    if not condition_result:
        return PytestStaticXfailDecision()

    return None


def _build_xfail_issue(
    decorator: ast.Call,
    message: str,
) -> PytestStaticXfailDecision:
    return PytestStaticXfailDecision(
        issue_code='pytest_unsupported_xfail_condition',
        issue_message=message,
        issue_line=decorator.lineno,
    )


def _build_runtime_xfail_decision(
    decorator: ast.Call,
    runtime_reason: str,
) -> PytestStaticXfailDecision:
    return PytestStaticXfailDecision(
        requires_pytest_runtime=True,
        runtime_reason=runtime_reason,
        issue_line=decorator.lineno,
    )


def _parse_usefixtures_decorator(
    decorator: ast.expr,
) -> PytestUsefixturesDecision:
    if not isinstance(decorator, ast.Call):
        return PytestUsefixturesDecision(
            issue_code='pytest_runtime_usefixtures',
            issue_message=(
                'PytestEngine v1 degrades unsupported '
                '@pytest.mark.usefixtures(...) forms to the pytest '
                'runtime adapter'
            ),
            requires_pytest_runtime=True,
            runtime_reason='dynamic usefixtures requires pytest runtime',
        )

    if decorator.keywords:
        return PytestUsefixturesDecision(
            issue_code='pytest_runtime_usefixtures',
            issue_message=(
                'PytestEngine v1 degrades keyword-based '
                '@pytest.mark.usefixtures(...) forms to the pytest '
                'runtime adapter'
            ),
            issue_line=decorator.lineno,
            requires_pytest_runtime=True,
            runtime_reason='dynamic usefixtures requires pytest runtime',
        )

    fixture_names: list[str] = []
    for argument in decorator.args:
        if not (
            isinstance(argument, ast.Constant)
            and isinstance(argument.value, str)
        ):
            return PytestUsefixturesDecision(
                issue_code='pytest_runtime_usefixtures',
                issue_message=(
                    'PytestEngine v1 degrades non-literal '
                    '@pytest.mark.usefixtures(...) forms to the pytest '
                    'runtime adapter'
                ),
                issue_line=decorator.lineno,
                requires_pytest_runtime=True,
                runtime_reason='dynamic usefixtures requires pytest runtime',
            )

        fixture_names.append(argument.value)

    return PytestUsefixturesDecision(
        fixture_names=tuple(fixture_names),
    )


def _parse_skipif_decorator(
    decorator: ast.expr,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> PytestStaticSkipDecision:
    if not isinstance(decorator, ast.Call):
        return PytestStaticSkipDecision(
            issue_code='pytest_unsupported_skip_condition',
            issue_message=(
                'PytestEngine v1 requires a statically evaluable condition '
                'in @pytest.mark.skipif(...)'
            ),
            issue_line=getattr(decorator, 'lineno', None),
        )

    condition_node = _extract_marker_condition_node(decorator)
    if condition_node is None:
        return PytestStaticSkipDecision(
            issue_code='pytest_unsupported_skip_condition',
            issue_message=(
                'PytestEngine v1 requires a statically evaluable condition '
                'in @pytest.mark.skipif(...)'
            ),
            issue_line=decorator.lineno,
        )

    condition_result = _evaluate_static_skip_condition(
        condition_node,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    if _is_runtime_string_condition(
        condition_node,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    ):
        return PytestStaticSkipDecision(
            requires_pytest_runtime=True,
            runtime_reason=(
                'string skipif condition requires pytest runtime adapter'
            ),
            issue_line=decorator.lineno,
        )
    if condition_result is None:
        return PytestStaticSkipDecision(
            requires_pytest_runtime=True,
            runtime_reason='dynamic skipif requires pytest runtime adapter',
            issue_line=decorator.lineno,
        )

    if not condition_result:
        return PytestStaticSkipDecision()

    reason = _extract_marker_reason(
        decorator,
        default='Skipped by pytest skipif mark',
        literal_bindings=literal_bindings,
    )
    return PytestStaticSkipDecision(skip_reason=reason)


def _extract_marker_reason(
    decorator: ast.Call,
    *,
    default: str,
    literal_bindings: dict[str, object],
) -> str:
    for keyword in decorator.keywords:
        if keyword.arg != 'reason':
            continue

        reason = _resolve_literal_reference(
            keyword.value,
            literal_bindings=literal_bindings,
        )
        return default if not isinstance(reason, str) else reason

    if decorator.args[1:]:
        reason = _resolve_literal_reference(
            decorator.args[1],
            literal_bindings=literal_bindings,
        )
        if isinstance(reason, str):
            return reason

    return default


def _extract_marker_condition_node(
    decorator: ast.Call,
) -> ast.expr | None:
    if decorator.args:
        return decorator.args[0]

    for keyword in decorator.keywords:
        if keyword.arg == 'condition':
            return keyword.value

    return None


def _extract_bool_marker_keyword(
    decorator: ast.Call,
    *,
    keyword_name: str,
    default: bool,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> bool | None:
    for keyword in decorator.keywords:
        if keyword.arg != keyword_name:
            continue

        value = _resolve_literal_reference(
            keyword.value,
            literal_bindings=literal_bindings,
        )
        if isinstance(value, bool):
            return value

        return _evaluate_static_skip_condition(
            keyword.value,
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )

    return default


def _is_runtime_string_condition(
    condition_node: ast.expr,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> bool:
    resolved_condition_node = _resolve_bound_expression(
        condition_node,
        expression_bindings=expression_bindings,
    )
    literal_value = _resolve_literal_reference(
        resolved_condition_node,
        literal_bindings=literal_bindings,
    )
    return isinstance(literal_value, str)


def _extract_xfail_raises_paths(
    decorator: ast.Call,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> tuple[str, ...] | None:
    raises_node: ast.expr | None = None
    for keyword in decorator.keywords:
        if keyword.arg == 'raises':
            raises_node = keyword.value
            break

    if raises_node is None:
        return ()

    resolved_raises_node = _resolve_bound_expression(
        raises_node,
        expression_bindings=expression_bindings,
    )
    literal_raises = _resolve_literal_reference(
        resolved_raises_node,
        literal_bindings=literal_bindings,
    )
    if isinstance(literal_raises, type) and issubclass(
        literal_raises,
        BaseException,
    ):
        return (literal_raises.__name__,)

    return _extract_exception_symbol_paths(resolved_raises_node)


def _extract_exception_symbol_paths(
    node: ast.expr,
) -> tuple[str, ...] | None:
    if isinstance(node, ast.Tuple):
        paths: list[str] = []
        for element in node.elts:
            path = _extract_exception_symbol_path(element)
            if path is None:
                return None

            paths.append(path)

        return tuple(paths)

    path = _extract_exception_symbol_path(node)
    if path is None:
        return None

    return (path,)


def _extract_exception_symbol_path(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id

    if not isinstance(node, ast.Attribute):
        return None

    path_parts: list[str] = [node.attr]
    current: ast.expr = node.value
    while isinstance(current, ast.Attribute):
        path_parts.append(current.attr)
        current = current.value

    if not isinstance(current, ast.Name):
        return None

    path_parts.append(current.id)
    return '.'.join(reversed(path_parts))


def _evaluate_static_skip_condition(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> bool | None:
    resolved_node = _resolve_bound_expression(
        node,
        expression_bindings=expression_bindings,
    )
    literal_value = _resolve_literal_reference(
        resolved_node,
        literal_bindings=literal_bindings,
    )
    if isinstance(literal_value, bool):
        return literal_value

    if isinstance(resolved_node, ast.BoolOp):
        operand_results = tuple(
            _evaluate_static_skip_condition(
                value,
                literal_bindings=literal_bindings,
                expression_bindings=expression_bindings,
            )
            for value in resolved_node.values
        )
        if any(result is None for result in operand_results):
            return None
        if isinstance(resolved_node.op, ast.And):
            boolop_result: bool | None = all(
                bool(result) for result in operand_results
            )
        elif isinstance(resolved_node.op, ast.Or):
            boolop_result = any(bool(result) for result in operand_results)
        else:
            boolop_result = None
        return boolop_result

    if isinstance(resolved_node, ast.UnaryOp) and isinstance(
        resolved_node.op,
        ast.Not,
    ):
        operand_result = _evaluate_static_skip_condition(
            resolved_node.operand,
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )
        if operand_result is None:
            return None
        return not operand_result

    return _evaluate_static_skip_compare(
        resolved_node,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )


def _evaluate_static_skip_compare(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> bool | None:
    if not _is_supported_static_compare(node):
        return None

    current_left_value = _evaluate_skip_operand(
        node.left,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    if current_left_value is None:
        return None

    comparison_result: bool | None = True
    for cmp_operator, comparator_node in zip(
        node.ops,
        node.comparators,
        strict=True,
    ):
        right_value = _evaluate_skip_operand(
            comparator_node,
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )
        if right_value is None:
            return None

        comparator = _build_skip_comparator(cmp_operator)
        if comparator is None:
            return None

        try:
            comparison_result = comparator(current_left_value, right_value)
        except TypeError:
            return None
        if comparison_result is False:
            break

        current_left_value = right_value

    return comparison_result


def _is_supported_static_compare(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Compare)
        and bool(node.ops)
        and len(node.ops) == len(node.comparators)
    )


def _build_skip_comparator(
    cmp_operator: ast.cmpop,
):
    comparator_map = {
        ast.Eq: operator_module.eq,
        ast.NotEq: operator_module.ne,
        ast.Lt: operator_module.lt,
        ast.LtE: operator_module.le,
        ast.Gt: operator_module.gt,
        ast.GtE: operator_module.ge,
        ast.Is: operator_module.is_,
        ast.IsNot: operator_module.is_not,
    }
    comparator = comparator_map.get(type(cmp_operator))
    if comparator is not None:
        return comparator
    if isinstance(cmp_operator, ast.In):
        return _skip_membership_compare
    if isinstance(cmp_operator, ast.NotIn):
        return _skip_non_membership_compare
    return None


def _skip_membership_compare(
    left_value: object,
    right_value: object,
) -> bool | None:
    return _evaluate_membership_skip_compare(
        left_value,
        right_value,
        negate=False,
    )


def _skip_non_membership_compare(
    left_value: object,
    right_value: object,
) -> bool | None:
    return _evaluate_membership_skip_compare(
        left_value,
        right_value,
        negate=True,
    )


def _evaluate_membership_skip_compare(
    left_value: object,
    right_value: object,
    *,
    negate: bool,
) -> bool | None:
    if not isinstance(right_value, tuple | list | set):
        return None

    is_member = left_value in right_value
    return not is_member if negate else is_member


def _evaluate_skip_operand(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> object | None:
    resolved_node = _resolve_bound_expression(
        node,
        expression_bindings=expression_bindings,
    )
    if (
        isinstance(resolved_node, ast.Attribute)
        and isinstance(resolved_node.value, ast.Name)
        and resolved_node.value.id == 'os'
        and resolved_node.attr == 'name'
    ):
        return os.name
    if (
        isinstance(resolved_node, ast.Attribute)
        and isinstance(resolved_node.value, ast.Name)
        and resolved_node.value.id == 'sys'
        and resolved_node.attr == 'platform'
    ):
        return sys.platform
    if (
        isinstance(resolved_node, ast.Attribute)
        and isinstance(resolved_node.value, ast.Name)
        and resolved_node.value.id == 'sys'
        and resolved_node.attr == 'version_info'
    ):
        return sys.version_info
    if (
        isinstance(resolved_node, ast.Attribute)
        and isinstance(resolved_node.value, ast.Attribute)
        and isinstance(resolved_node.value.value, ast.Name)
        and resolved_node.value.value.id == 'sys'
        and resolved_node.value.attr == 'implementation'
        and resolved_node.attr == 'name'
    ):
        return sys.implementation.name

    literal_value = _resolve_literal_reference(
        resolved_node,
        literal_bindings=literal_bindings,
    )
    if isinstance(literal_value, str | tuple | list | set | bool):
        return literal_value

    return None


def _parse_parametrize_specs(
    decorators: Iterable[ast.expr],
    marker_aliases: Iterable[str],
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> PytestParametrizeParseResult:
    parametrize_context = _PytestParametrizeContext(
        marker_aliases=set(marker_aliases),
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    specs: list[PytestParametrizeSpec] = []
    seen_parameter_names: set[str] = set()
    for decorator in decorators:
        if not _is_parametrize_decorator(
            decorator,
            parametrize_context.marker_aliases,
        ):
            continue

        if not isinstance(decorator, ast.Call):
            return PytestParametrizeParseResult(
                issue_code='pytest_unsupported_parametrize',
                issue_message=(
                    'PytestEngine v1 only supports literal '
                    '@pytest.mark.parametrize(...) decorators'
                ),
                issue_line=getattr(decorator, 'lineno', None),
            )

        parsed_spec = _parse_parametrize_spec(
            decorator,
            parametrize_context=parametrize_context,
        )
        if parsed_spec.issue_code is not None:
            return parsed_spec

        for spec in parsed_spec.specs:
            duplicated_names = seen_parameter_names.intersection(
                spec.arg_names,
            )
            if duplicated_names:
                return PytestParametrizeParseResult(
                    issue_code='pytest_unsupported_parametrize',
                    issue_message=(
                        'PytestEngine v1 does not support repeated '
                        'parameter names across parametrize decorators'
                    ),
                    issue_line=spec.line,
                )

            seen_parameter_names.update(spec.arg_names)
            specs.append(spec)

    return PytestParametrizeParseResult(specs=tuple(specs))


def _parse_parametrize_spec(
    decorator: ast.Call,
    *,
    parametrize_context: _PytestParametrizeContext,
) -> PytestParametrizeParseResult:
    if len(decorator.args) < PARAMETRIZE_MIN_ARG_COUNT:
        return _unsupported_parametrize_result(
            decorator,
            (
                'PytestEngine v1 requires literal arg names and values in '
                '@pytest.mark.parametrize'
            ),
        )

    arg_names = _parse_parametrize_arg_names(
        decorator.args[0],
        literal_bindings=parametrize_context.literal_bindings,
    )
    if arg_names is None:
        return _unsupported_parametrize_result(
            decorator,
            (
                'PytestEngine v1 only supports literal parameter names in '
                '@pytest.mark.parametrize'
            ),
        )

    keyword_issue = _validate_parametrize_keywords(decorator)
    if keyword_issue is not None:
        return _unsupported_parametrize_result(
            decorator,
            keyword_issue,
        )

    case_ids, indirect_arg_names, metadata_issue = _parse_parametrize_metadata(
        decorator,
        arg_names=arg_names,
        literal_bindings=parametrize_context.literal_bindings,
    )
    if metadata_issue is not None:
        return _unsupported_parametrize_result(
            decorator,
            metadata_issue,
        )

    cases = _parse_parametrize_cases(
        decorator.args[1],
        arg_names=arg_names,
        case_ids=case_ids,
        parametrize_context=parametrize_context,
    )
    if cases is None:
        return _unsupported_parametrize_result(
            decorator,
            (
                'PytestEngine v1 only supports literal parameter rows in '
                '@pytest.mark.parametrize'
            ),
        )

    return PytestParametrizeParseResult(
        specs=(
            PytestParametrizeSpec(
                arg_names=arg_names,
                indirect_arg_names=indirect_arg_names,
                cases=cases,
                line=decorator.lineno,
            ),
        ),
    )


def _unsupported_parametrize_result(
    decorator: ast.Call,
    message: str,
) -> PytestParametrizeParseResult:
    return PytestParametrizeParseResult(
        issue_code='pytest_unsupported_parametrize',
        issue_message=message,
        issue_line=decorator.lineno,
    )


def _validate_parametrize_keywords(
    decorator: ast.Call,
) -> str | None:
    for keyword in decorator.keywords:
        if keyword.arg not in {'ids', 'indirect'}:
            return (
                'PytestEngine v1 does not support keyword arguments '
                'other than ids and indirect in @pytest.mark.parametrize'
            )

    return None


def _parse_parametrize_metadata(
    decorator: ast.Call,
    *,
    arg_names: tuple[str, ...],
    literal_bindings: dict[str, object],
) -> tuple[tuple[str | None, ...] | None, tuple[str, ...], str | None]:
    case_ids = _parse_parametrize_ids(
        decorator,
        literal_bindings=literal_bindings,
    )
    if case_ids is False:
        return (
            None,
            (),
            (
                'PytestEngine v1 only supports literal ids in '
                '@pytest.mark.parametrize'
            ),
        )

    indirect_arg_names = _parse_parametrize_indirect(
        decorator,
        arg_names=arg_names,
        literal_bindings=literal_bindings,
    )
    if indirect_arg_names is False:
        return (
            None,
            (),
            (
                'PytestEngine v1 only supports literal indirect values '
                'as False, True or a literal list/tuple of parameter '
                'names in @pytest.mark.parametrize'
            ),
        )

    return (case_ids, indirect_arg_names, None)


def _parse_parametrize_arg_names(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
) -> tuple[str, ...] | None:
    literal_value = _resolve_literal_reference(
        node,
        literal_bindings=literal_bindings,
    )
    if isinstance(literal_value, str):
        names = tuple(
            name.strip() for name in literal_value.split(',') if name.strip()
        )
        return names or None

    if not isinstance(literal_value, list | tuple):
        return None

    names = tuple(
        name.strip()
        for name in literal_value
        if isinstance(name, str) and name.strip()
    )
    if len(names) != len(literal_value):
        return None

    return names or None


def _parse_parametrize_ids(
    decorator: ast.Call,
    *,
    literal_bindings: dict[str, object],
) -> tuple[str | None, ...] | None | bool:
    ids_node: ast.expr | None = None
    if len(decorator.args) >= PARAMETRIZE_IDS_ARG_COUNT:
        ids_node = decorator.args[PARAMETRIZE_IDS_POSITION]

    for keyword in decorator.keywords:
        if keyword.arg == 'ids':
            ids_node = keyword.value
            break

    if ids_node is None:
        return None

    literal_value = _resolve_literal_reference(
        ids_node,
        literal_bindings=literal_bindings,
    )
    if not isinstance(literal_value, list | tuple):
        return False

    case_ids: list[str | None] = []
    for value in literal_value:
        if value is None:
            case_ids.append(None)
            continue

        if not isinstance(value, str):
            return False

        case_ids.append(value)

    return tuple(case_ids)


def _parse_parametrize_indirect(
    decorator: ast.Call,
    *,
    arg_names: tuple[str, ...],
    literal_bindings: dict[str, object],
) -> tuple[str, ...] | bool:
    indirect_node: ast.expr | None = None
    for keyword in decorator.keywords:
        if keyword.arg == 'indirect':
            indirect_node = keyword.value
            break

    if indirect_node is None:
        return ()

    literal_value = _resolve_literal_reference(
        indirect_node,
        literal_bindings=literal_bindings,
    )
    if isinstance(literal_value, bool):
        return arg_names if literal_value else ()

    if not isinstance(literal_value, list | tuple):
        return False

    indirect_arg_names: list[str] = []
    for value in literal_value:
        if not isinstance(value, str) or value not in arg_names:
            return False
        if value in indirect_arg_names:
            continue
        indirect_arg_names.append(value)

    return tuple(indirect_arg_names)


def _parse_parametrize_cases(
    node: ast.expr,
    *,
    arg_names: tuple[str, ...],
    case_ids: tuple[str | None, ...] | None,
    parametrize_context: _PytestParametrizeContext,
) -> tuple[PytestParametrizeCase, ...] | None:
    resolved_node = _resolve_parametrize_rows_node(
        node,
        literal_bindings=parametrize_context.literal_bindings,
        expression_bindings=parametrize_context.expression_bindings,
    )
    if resolved_node is PARSE_FAILURE or not isinstance(
        resolved_node,
        ast.List | ast.Tuple,
    ):
        return None

    rows: list[PytestParametrizeCase] = []
    for row_index, row_node in enumerate(resolved_node.elts):
        case_id = None
        if case_ids is not None:
            if row_index >= len(case_ids):
                return None

            case_id = case_ids[row_index]

        parametrized_row = _parse_parametrize_row(
            row_node,
            width=len(arg_names),
            marker_aliases=parametrize_context.marker_aliases,
            default_case_id=case_id,
            parametrize_context=parametrize_context,
        )
        if parametrized_row is None:
            return None

        rows.append(parametrized_row)

    if case_ids is not None and len(case_ids) != len(rows):
        return None

    return tuple(rows)


def _parse_parametrize_row(
    row_node: ast.expr,
    *,
    width: int,
    marker_aliases: set[str],
    default_case_id: str | None,
    parametrize_context: _PytestParametrizeContext,
) -> PytestParametrizeCase | None:
    pytest_param = _parse_pytest_param_call(
        row_node,
        width=width,
        marker_aliases=marker_aliases,
        parametrize_context=parametrize_context,
    )
    if pytest_param is PARSE_FAILURE:
        return None

    if isinstance(pytest_param, PytestParametrizeCase):
        return PytestParametrizeCase(
            values=pytest_param.values,
            case_id=(
                pytest_param.case_id
                if pytest_param.case_id is not None
                else default_case_id
            ),
            selection_labels=pytest_param.selection_labels,
            usefixture_names=pytest_param.usefixture_names,
            skip_reason=pytest_param.skip_reason,
            skip_issue=pytest_param.skip_issue,
            xfail_reason=pytest_param.xfail_reason,
            xfail_issue=pytest_param.xfail_issue,
            xfail_strict=pytest_param.xfail_strict,
            xfail_run=pytest_param.xfail_run,
            xfail_raises_paths=pytest_param.xfail_raises_paths,
            requires_pytest_runtime=pytest_param.requires_pytest_runtime,
            pytest_runtime_reason=pytest_param.pytest_runtime_reason,
        )

    row_value = _resolve_literal_reference_with_sentinel(
        row_node,
        literal_bindings=parametrize_context.literal_bindings,
    )
    if row_value is PARSE_FAILURE:
        return None

    values = _normalize_parametrize_row_values(
        row_value,
        width=width,
    )
    if values is None:
        return None

    return PytestParametrizeCase(
        values=values,
        case_id=default_case_id,
    )


def _parse_pytest_param_call(
    row_node: ast.expr,
    *,
    width: int,
    marker_aliases: set[str],
    parametrize_context: _PytestParametrizeContext,
) -> PytestParametrizeCase | object | None:
    resolved_row_node = _resolve_bound_expression(
        row_node,
        expression_bindings=parametrize_context.expression_bindings,
    )
    if not _is_pytest_param_call(resolved_row_node, marker_aliases):
        return None

    if not isinstance(resolved_row_node, ast.Call):
        return PARSE_FAILURE

    parsed_metadata = _parse_pytest_param_metadata(
        resolved_row_node,
        marker_aliases=marker_aliases,
        literal_bindings=parametrize_context.literal_bindings,
        expression_bindings=parametrize_context.expression_bindings,
    )
    if parsed_metadata is PARSE_FAILURE:
        return PARSE_FAILURE

    values = tuple(
        _resolve_literal_reference_with_sentinel(
            argument,
            literal_bindings=parametrize_context.literal_bindings,
        )
        for argument in resolved_row_node.args
    )
    if any(value is PARSE_FAILURE for value in values):
        return PARSE_FAILURE

    normalized_values = _normalize_parametrize_row_values(
        values,
        width=width,
    )
    if normalized_values is None:
        return PARSE_FAILURE

    return PytestParametrizeCase(
        values=normalized_values,
        case_id=parsed_metadata.case_id,
        selection_labels=parsed_metadata.selection_labels,
        usefixture_names=parsed_metadata.usefixture_names,
        skip_reason=parsed_metadata.skip_reason,
        skip_issue=parsed_metadata.skip_issue,
        xfail_reason=parsed_metadata.xfail_reason,
        xfail_issue=parsed_metadata.xfail_issue,
        xfail_strict=parsed_metadata.xfail_strict,
        xfail_run=parsed_metadata.xfail_run,
        xfail_raises_paths=parsed_metadata.xfail_raises_paths,
        requires_pytest_runtime=parsed_metadata.requires_pytest_runtime,
        pytest_runtime_reason=parsed_metadata.pytest_runtime_reason,
    )


def _parse_pytest_param_metadata(
    param_call: ast.Call,
    *,
    marker_aliases: set[str],
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> PytestParametrizeCase | object:
    case_id = None
    selection_labels: tuple[str, ...] = ()
    usefixture_names: tuple[str, ...] = ()
    skip_reason = None
    skip_issue = None
    xfail_reason = None
    xfail_issue = None
    xfail_strict = False
    xfail_run = True
    xfail_raises_paths: tuple[str, ...] = ()
    requires_pytest_runtime = False
    for keyword in param_call.keywords:
        if keyword.arg == 'id':
            parsed_id = _resolve_literal_reference_with_sentinel(
                keyword.value,
                literal_bindings=literal_bindings,
            )
            if not isinstance(parsed_id, str):
                return PARSE_FAILURE

            case_id = parsed_id
            continue

        if keyword.arg != 'marks':
            return PARSE_FAILURE

        parsed_marks = _parse_pytest_param_marks(
            keyword.value,
            marker_aliases=marker_aliases,
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )
        if parsed_marks is PARSE_FAILURE:
            return PARSE_FAILURE

        selection_labels = parsed_marks.selection_labels
        usefixture_names = parsed_marks.usefixture_names
        skip_reason = parsed_marks.skip_reason
        skip_issue = parsed_marks.skip_issue
        xfail_reason = parsed_marks.xfail_reason
        xfail_issue = parsed_marks.xfail_issue
        xfail_strict = parsed_marks.xfail_strict
        xfail_run = parsed_marks.xfail_run
        xfail_raises_paths = parsed_marks.xfail_raises_paths
        requires_pytest_runtime = parsed_marks.requires_pytest_runtime

    return PytestParametrizeCase(
        values=(),
        case_id=case_id,
        selection_labels=selection_labels,
        usefixture_names=usefixture_names,
        skip_reason=skip_reason,
        skip_issue=skip_issue,
        xfail_reason=xfail_reason,
        xfail_issue=xfail_issue,
        xfail_strict=xfail_strict,
        xfail_run=xfail_run,
        xfail_raises_paths=xfail_raises_paths,
        requires_pytest_runtime=requires_pytest_runtime,
        pytest_runtime_reason=(
            'pytest.param marks require pytest runtime adapter'
            if requires_pytest_runtime
            else None
        ),
    )


def _parse_pytest_param_marks(
    marks_node: ast.expr,
    *,
    marker_aliases: set[str],
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> PytestParametrizeCase | object:
    mark_nodes = _extract_pytest_param_mark_nodes(
        marks_node,
        expression_bindings=expression_bindings,
    )
    if mark_nodes is None:
        return PARSE_FAILURE

    if any(
        _extract_marker_name(mark, marker_aliases) is None
        and not _is_skip_decorator(mark, marker_aliases)
        and not _is_skipif_decorator(mark, marker_aliases)
        and not _is_xfail_decorator(mark, marker_aliases)
        and not _is_usefixtures_decorator(mark, marker_aliases)
        and not _is_filterwarnings_decorator(mark, marker_aliases)
        for mark in mark_nodes
    ):
        return PARSE_FAILURE

    skip_decision = _build_static_skip_decision(
        mark_nodes,
        marker_aliases,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    xfail_decision = _build_static_xfail_decision(
        mark_nodes,
        marker_aliases,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    usefixtures_decision = _build_usefixtures_decision(
        mark_nodes,
        marker_aliases,
    )
    filterwarnings_decision = _build_filterwarnings_decision(
        mark_nodes,
        marker_aliases,
    )
    if (
        skip_decision.issue_code is not None
        or xfail_decision.issue_code is not None
        or usefixtures_decision.issue_code is not None
    ):
        return PARSE_FAILURE

    return PytestParametrizeCase(
        values=(),
        selection_labels=_extract_selection_labels(mark_nodes, marker_aliases),
        usefixture_names=usefixtures_decision.fixture_names,
        skip_reason=skip_decision.skip_reason,
        xfail_reason=xfail_decision.xfail_reason,
        xfail_strict=xfail_decision.strict,
        xfail_run=xfail_decision.run,
        xfail_raises_paths=xfail_decision.raises_paths,
        requires_pytest_runtime=(
            skip_decision.requires_pytest_runtime
            or xfail_decision.requires_pytest_runtime
            or usefixtures_decision.requires_pytest_runtime
            or filterwarnings_decision.requires_pytest_runtime
        ),
        pytest_runtime_reason=_merge_pytest_runtime_reasons(
            skip_decision.runtime_reason,
            xfail_decision.runtime_reason,
            usefixtures_decision.runtime_reason,
            filterwarnings_decision.runtime_reason,
        ),
    )


def _extract_pytest_param_mark_nodes(
    marks_node: ast.expr,
    *,
    expression_bindings: dict[str, ast.expr],
) -> tuple[ast.expr, ...] | None:
    resolved_marks_node = _resolve_bound_expression(
        marks_node,
        expression_bindings=expression_bindings,
    )
    if isinstance(resolved_marks_node, ast.List | ast.Tuple | ast.Set):
        mark_nodes: list[ast.expr] = []
        for mark_node in resolved_marks_node.elts:
            nested_mark_nodes = _extract_pytest_param_mark_nodes(
                mark_node,
                expression_bindings=expression_bindings,
            )
            if nested_mark_nodes is None:
                return None
            mark_nodes.extend(nested_mark_nodes)
        return tuple(mark_nodes)

    if isinstance(resolved_marks_node, ast.BinOp) and isinstance(
        resolved_marks_node.op,
        ast.Add,
    ):
        left_mark_nodes = _extract_pytest_param_mark_nodes(
            resolved_marks_node.left,
            expression_bindings=expression_bindings,
        )
        if left_mark_nodes is None:
            return None
        right_mark_nodes = _extract_pytest_param_mark_nodes(
            resolved_marks_node.right,
            expression_bindings=expression_bindings,
        )
        if right_mark_nodes is None:
            return None
        return (*left_mark_nodes, *right_mark_nodes)

    return (resolved_marks_node,)


def _normalize_parametrize_row_values(
    row_value: object,
    *,
    width: int,
) -> tuple[object, ...] | None:
    if width == 1:
        if not isinstance(row_value, tuple | list):
            return (row_value,)

        if len(row_value) != 1:
            return None

        return (row_value[0],)

    if not isinstance(row_value, tuple | list):
        return None

    if len(row_value) != width:
        return None

    return tuple(row_value)


def _is_pytest_param_call(
    node: ast.expr,
    pytest_aliases: set[str],
) -> bool:
    if not isinstance(node, ast.Call):
        return False

    current = node.func
    if not isinstance(current, ast.Attribute):
        return False

    return (
        current.attr == 'param'
        and isinstance(current.value, ast.Name)
        and current.value.id in pytest_aliases
    )


def _build_parametrized_execution_cases(
    specs: tuple[PytestParametrizeSpec, ...],
) -> tuple[PytestExpandedParametrizeCase, ...]:
    if not specs:
        return (PytestExpandedParametrizeCase(),)

    expanded_cases: list[PytestExpandedParametrizeCase] = [
        PytestExpandedParametrizeCase(),
    ]
    for spec in specs:
        next_cases: list[PytestExpandedParametrizeCase] = []
        for previous_case in expanded_cases:
            for case_index, case in enumerate(spec.cases, start=1):
                current_id = _build_parametrized_case_id(case, case_index)
                next_cases.append(
                    _merge_parametrized_execution_case(
                        previous_case,
                        spec.arg_names,
                        spec.indirect_arg_names,
                        case,
                        current_id=current_id,
                    ),
                )

        expanded_cases = next_cases

    return tuple(expanded_cases)


def _merge_parametrized_execution_case(
    previous_case: PytestExpandedParametrizeCase,
    arg_names: tuple[str, ...],
    indirect_arg_names: tuple[str, ...],
    case: PytestParametrizeCase,
    *,
    current_id: str,
) -> PytestExpandedParametrizeCase:
    previous_xfail = PytestStaticXfailDecision(
        xfail_reason=previous_case.xfail_reason,
        issue_code=previous_case.xfail_issue and 'pytest_case_xfail_issue',
        issue_message=previous_case.xfail_issue,
        strict=previous_case.xfail_strict,
        run=previous_case.xfail_run,
        raises_paths=previous_case.xfail_raises_paths,
    )
    merged_xfail = _merge_xfail_decisions(
        inherited_xfail=previous_xfail,
        local_decision=PytestStaticXfailDecision(
            xfail_reason=case.xfail_reason,
            issue_code=case.xfail_issue and 'pytest_case_xfail_issue',
            issue_message=case.xfail_issue,
            strict=case.xfail_strict,
            run=case.xfail_run,
            raises_paths=case.xfail_raises_paths,
        ),
    )
    merged_skip = _merge_skip_decisions(
        inherited_skip_reason=previous_case.skip_reason,
        inherited_skip_issue=previous_case.skip_issue,
        local_decision=PytestStaticSkipDecision(
            skip_reason=case.skip_reason,
            issue_code=case.skip_issue and 'pytest_case_skip_issue',
            issue_message=case.skip_issue,
        ),
    )
    return PytestExpandedParametrizeCase(
        parameter_values=previous_case.parameter_values
        + tuple(zip(arg_names, case.values, strict=True)),
        indirect_fixture_names=_merge_usefixture_names(
            previous_case.indirect_fixture_names,
            indirect_arg_names,
        ),
        case_id=(
            current_id
            if previous_case.case_id is None
            else f'{previous_case.case_id}-{current_id}'
        ),
        selection_labels=_merge_selection_labels(
            previous_case.selection_labels,
            case.selection_labels,
        ),
        usefixture_names=_merge_usefixture_names(
            previous_case.usefixture_names,
            case.usefixture_names,
        ),
        skip_reason=merged_skip.skip_reason,
        skip_issue=merged_skip.issue_message,
        xfail_reason=merged_xfail.xfail_reason,
        xfail_issue=merged_xfail.issue_message,
        xfail_strict=merged_xfail.strict,
        xfail_run=merged_xfail.run,
        xfail_raises_paths=merged_xfail.raises_paths,
        requires_pytest_runtime=(
            previous_case.requires_pytest_runtime
            or case.requires_pytest_runtime
        ),
        pytest_runtime_reason=_merge_pytest_runtime_reasons(
            previous_case.pytest_runtime_reason,
            case.pytest_runtime_reason,
        ),
    )


def _build_parametrized_case_id(
    case: PytestParametrizeCase,
    case_index: int,
) -> str:
    if case.case_id is not None:
        return case.case_id

    case_id = '-'.join(_render_parameter_value(value) for value in case.values)
    if case_id:
        return case_id

    return str(case_index)


def _render_parameter_value(value: object) -> str:
    if isinstance(value, str):
        return value

    return repr(value)


def _safe_literal_eval(node: ast.expr) -> object | None:
    try:
        return ast.literal_eval(node)
    except Exception:
        return None


def _safe_literal_eval_with_sentinel(node: ast.expr) -> object:
    try:
        return ast.literal_eval(node)
    except Exception:
        return PARSE_FAILURE


def _discover_literal_bindings(
    module: ast.Module,
    *,
    initial_bindings: dict[str, object] | None = None,
) -> dict[str, object]:
    bindings = dict(initial_bindings or {})
    for statement in module.body:
        target_name = _extract_literal_binding_target(statement)
        if target_name is None:
            continue

        value_node = _extract_literal_binding_value(statement)
        if value_node is None:
            continue

        resolved_value = _resolve_literal_reference_with_sentinel(
            value_node,
            literal_bindings=bindings,
        )
        if resolved_value is PARSE_FAILURE:
            continue

        bindings[target_name] = resolved_value

    return bindings


def _discover_expression_bindings(
    module: ast.Module,
    *,
    initial_bindings: dict[str, ast.expr] | None = None,
) -> dict[str, ast.expr]:
    bindings = dict(initial_bindings or {})
    for statement in module.body:
        target_name = _extract_literal_binding_target(statement)
        if target_name is None:
            continue

        value_node = _extract_literal_binding_value(statement)
        if value_node is None:
            continue

        bindings[target_name] = value_node

    return bindings


def _extract_literal_binding_target(
    statement: ast.stmt,
) -> str | None:
    if isinstance(statement, ast.Assign):
        if len(statement.targets) != 1:
            return None
        target = statement.targets[0]
        if isinstance(target, ast.Name):
            return target.id
        return None

    if isinstance(statement, ast.AnnAssign) and isinstance(
        statement.target,
        ast.Name,
    ):
        return statement.target.id

    return None


def _extract_literal_binding_value(
    statement: ast.stmt,
) -> ast.expr | None:
    if isinstance(statement, ast.Assign):
        return statement.value

    if isinstance(statement, ast.AnnAssign):
        return statement.value

    return None


def _resolve_literal_reference(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
) -> object | None:
    resolved_value = _resolve_literal_reference_with_sentinel(
        node,
        literal_bindings=literal_bindings,
    )
    if resolved_value is PARSE_FAILURE:
        return None

    return resolved_value


def _resolve_literal_reference_with_sentinel(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
) -> object:
    if isinstance(node, ast.Name):
        return literal_bindings.get(node.id, PARSE_FAILURE)

    sequence_value = _resolve_literal_sequence(
        node,
        literal_bindings=literal_bindings,
    )
    if sequence_value is not PARSE_FAILURE:
        return sequence_value

    mapping_value = _resolve_literal_mapping(
        node,
        literal_bindings=literal_bindings,
    )
    if mapping_value is not PARSE_FAILURE:
        return mapping_value

    return _safe_literal_eval_with_sentinel(node)


def _resolve_literal_sequence(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
) -> object:
    if not isinstance(node, ast.List | ast.Tuple | ast.Set):
        return PARSE_FAILURE

    values = [
        _resolve_literal_reference_with_sentinel(
            element,
            literal_bindings=literal_bindings,
        )
        for element in node.elts
    ]
    if any(value is PARSE_FAILURE for value in values):
        return PARSE_FAILURE

    if isinstance(node, ast.List):
        return values
    if isinstance(node, ast.Tuple):
        return tuple(values)
    return set(values)


def _resolve_literal_mapping(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
) -> object:
    if not isinstance(node, ast.Dict):
        return PARSE_FAILURE

    keys = [
        _resolve_literal_reference_with_sentinel(
            key,
            literal_bindings=literal_bindings,
        )
        if key is not None
        else PARSE_FAILURE
        for key in node.keys
    ]
    values = [
        _resolve_literal_reference_with_sentinel(
            value,
            literal_bindings=literal_bindings,
        )
        for value in node.values
    ]
    if any(key is PARSE_FAILURE for key in keys) or any(
        value is PARSE_FAILURE for value in values
    ):
        return PARSE_FAILURE

    return dict(zip(keys, values, strict=True))


def _resolve_parametrize_rows_node(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> ast.expr | object:
    resolved_node = _resolve_bound_expression(
        node,
        expression_bindings=expression_bindings,
    )
    if not isinstance(resolved_node, ast.Name):
        return resolved_node

    resolved_value = _resolve_literal_reference_with_sentinel(
        resolved_node,
        literal_bindings=literal_bindings,
    )
    if not isinstance(resolved_value, list | tuple):
        return PARSE_FAILURE

    return ast.parse(repr(resolved_value), mode='eval').body


def _resolve_bound_expression(
    node: ast.expr,
    *,
    expression_bindings: dict[str, ast.expr],
    active_names: tuple[str, ...] = (),
) -> ast.expr:
    current_node = node
    active_name_set = set(active_names)
    while isinstance(current_node, ast.Name):
        bound_node = expression_bindings.get(current_node.id)
        if bound_node is None or current_node.id in active_name_set:
            return current_node

        active_name_set.add(current_node.id)
        current_node = bound_node

    return current_node


def _discover_static_binding_context(
    module: ast.Module,
    *,
    source_path: Path | None,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...],
    active_sources: tuple[Path, ...] = (),
) -> tuple[dict[str, object], dict[str, ast.expr]]:
    imported_literals: dict[str, object] = {}
    imported_expressions: dict[str, ast.expr] = {}
    if source_path is not None:
        (
            imported_literals,
            imported_expressions,
        ) = _discover_imported_static_bindings(
            source_path.resolve(),
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
            active_sources=active_sources,
        )

    expression_bindings = _discover_expression_bindings(
        module,
        initial_bindings=imported_expressions,
    )
    literal_bindings = _discover_literal_bindings(
        module,
        initial_bindings=imported_literals,
    )
    return (literal_bindings, expression_bindings)


def _discover_imported_static_bindings(
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...],
    active_sources: tuple[Path, ...] = (),
) -> tuple[dict[str, object], dict[str, ast.expr]]:
    resolved_source_path = source_path.resolve()
    if (
        resolved_source_path in active_sources
        or not resolved_source_path.exists()
    ):
        return ({}, {})

    module = ast.parse(resolved_source_path.read_text(encoding='utf-8'))
    next_active_sources = (*active_sources, resolved_source_path)
    imported_literals: dict[str, object] = {}
    imported_expressions: dict[str, ast.expr] = {}
    for statement in module.body:
        if not isinstance(statement, ast.ImportFrom):
            continue

        imported_source_path = _resolve_imported_source_path(
            statement,
            source_path=resolved_source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
        )
        if imported_source_path is None:
            continue

        source_literals, source_expressions = _discover_source_static_bindings(
            imported_source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
            active_sources=next_active_sources,
        )
        if not source_literals and not source_expressions:
            continue

        for alias in statement.names:
            if alias.name == '*':
                for binding_name, binding_value in source_literals.items():
                    imported_literals.setdefault(binding_name, binding_value)
                for binding_name, binding_value in source_expressions.items():
                    imported_expressions.setdefault(
                        binding_name,
                        binding_value,
                    )
                continue

            binding_name = alias.asname or alias.name
            if alias.name in source_literals:
                imported_literals.setdefault(
                    binding_name,
                    source_literals[alias.name],
                )
            if alias.name in source_expressions:
                imported_expressions.setdefault(
                    binding_name,
                    source_expressions[alias.name],
                )

    return (imported_literals, imported_expressions)


def _discover_source_static_bindings(
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...],
    active_sources: tuple[Path, ...] = (),
) -> tuple[dict[str, object], dict[str, ast.expr]]:
    resolved_source_path = source_path.resolve()
    if (
        resolved_source_path in active_sources
        or not resolved_source_path.exists()
    ):
        return ({}, {})

    module = ast.parse(resolved_source_path.read_text(encoding='utf-8'))
    return _discover_static_binding_context(
        module,
        source_path=resolved_source_path,
        root_path=root_path,
        configured_definition_paths=configured_definition_paths,
        active_sources=(*active_sources, resolved_source_path),
    )


def _extract_marker_name(
    decorator: ast.expr,
    marker_aliases: Iterable[str],
) -> str | None:
    marker_alias_set = set(marker_aliases)
    if (
        _is_parametrize_decorator(decorator, marker_alias_set)
        or _is_skip_decorator(decorator, marker_alias_set)
        or _is_skipif_decorator(decorator, marker_alias_set)
        or _is_xfail_decorator(decorator, marker_alias_set)
        or _is_usefixtures_decorator(decorator, marker_alias_set)
        or _is_filterwarnings_decorator(decorator, marker_alias_set)
        or _is_requires_decorator(decorator, marker_alias_set)
        or _is_requires_capability_decorator(
            decorator,
            marker_alias_set,
        )
        or _is_requires_mode_decorator(
            decorator,
            marker_alias_set,
        )
        or _is_disallow_mode_decorator(
            decorator,
            marker_alias_set,
        )
    ):
        return None

    current = decorator.func if isinstance(decorator, ast.Call) else decorator
    if not isinstance(current, ast.Attribute):
        return None

    marker_parent = current.value
    if not isinstance(marker_parent, ast.Attribute):
        return None

    base = marker_parent.value
    is_supported_marker = (
        isinstance(base, ast.Name)
        and base.id in set(marker_aliases)
        and marker_parent.attr == 'mark'
    )
    return current.attr if is_supported_marker else None


def _is_parametrize_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='parametrize',
    )


def _is_skip_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='skip',
    )


def _is_skipif_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='skipif',
    )


def _is_xfail_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='xfail',
    )


def _is_usefixtures_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='usefixtures',
    )


def _is_filterwarnings_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='filterwarnings',
    )


def _is_requires_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='requires',
    )


def _is_requires_capability_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='requires_capability',
    )


def _is_disallow_mode_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='disallow_mode',
    )


def _is_requires_mode_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
) -> bool:
    return _is_special_marker_decorator(
        decorator,
        marker_aliases,
        marker_name='requires_mode',
    )


def _is_special_marker_decorator(
    decorator: ast.expr,
    marker_aliases: set[str],
    *,
    marker_name: str,
) -> bool:
    current = decorator.func if isinstance(decorator, ast.Call) else decorator
    if not isinstance(current, ast.Attribute):
        return False

    if current.attr != marker_name:
        return False

    if not isinstance(current.value, ast.Attribute):
        return False

    if current.value.attr != 'mark':
        return False

    base = current.value.value
    return isinstance(base, ast.Name) and base.id in marker_aliases


def _supports_pytest_callable_signature(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    class_name: str | None,
    fixtures: dict[str, PytestFixtureDefinition],
    parameter_names: tuple[str, ...] = (),
) -> bool:
    del fixtures, parameter_names
    if node.args.kwonlyargs or node.args.vararg or node.args.kwarg:
        return False

    positional_args = tuple(node.args.args)
    if class_name is None:
        return True

    return bool(positional_args) and positional_args[0].arg == 'self'


def _extract_fixture_names(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    class_name: str | None,
) -> tuple[str, ...]:
    positional_args = tuple(arg.arg for arg in node.args.args)
    if class_name is None:
        return positional_args

    return positional_args[1:]


def _is_fixture_definition(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    fixture_aliases: Iterable[str],
) -> bool:
    alias_set = set(fixture_aliases)
    for decorator in node.decorator_list:
        current = (
            decorator.func if isinstance(decorator, ast.Call) else decorator
        )
        if isinstance(current, ast.Name) and current.id in alias_set:
            return True

        if not isinstance(current, ast.Attribute):
            continue

        if not isinstance(current.value, ast.Name):
            continue

        if current.value.id not in alias_set:
            continue

        if current.attr == 'fixture':
            return True

    return False


def _is_supported_fixture_reference(
    fixture_name: str,
    fixtures: dict[str, PytestFixtureDefinition],
    *,
    resource_fixture_names: tuple[str, ...] = (),
) -> bool:
    return (
        _get_fixture_support_reason(
            fixture_name,
            fixtures,
            resource_fixture_names=resource_fixture_names,
        )
        is None
    )


def _is_supported_fixture_definition(
    fixture_name: str,
    fixture: PytestFixtureDefinition,
    fixtures: dict[str, PytestFixtureDefinition],
    *,
    resource_fixture_names: tuple[str, ...] = (),
) -> bool:
    del fixture
    support_reason = _get_fixture_support_reason(
        fixture_name,
        fixtures,
        resource_fixture_names=resource_fixture_names,
    )
    return support_reason in {None, 'missing_dependency'}


def _get_fixture_support_reason(
    fixture_name: str,
    fixtures: dict[str, PytestFixtureDefinition],
    *,
    active_fixtures: tuple[str, ...] = (),
    support_cache: dict[str, str | None] | None = None,
    resource_fixture_names: tuple[str, ...] = (),
) -> str | None:
    if fixture_name == 'request' or fixture_name in resource_fixture_names:
        return None

    if support_cache is not None and fixture_name in support_cache:
        return support_cache[fixture_name]

    fixture = fixtures.get(fixture_name)
    support_reason: str | None
    if fixture is None:
        support_reason = 'missing_dependency'
    elif fixture_name in active_fixtures:
        support_reason = 'cyclic_dependency'
    else:
        active_path = (*active_fixtures, fixture_name)
        support_reason = None
        for dependency_name in fixture.fixture_names:
            support_reason = _get_fixture_support_reason(
                dependency_name,
                fixtures,
                active_fixtures=active_path,
                support_cache=support_cache,
                resource_fixture_names=resource_fixture_names,
            )
            if support_reason is not None:
                break

    if support_cache is not None:
        support_cache[fixture_name] = support_reason

    return support_reason


def _requires_pytest_runtime(
    fixture_names: tuple[str, ...],
    fixtures: dict[str, PytestFixtureDefinition],
    *,
    resource_fixture_names: tuple[str, ...] = (),
) -> bool:
    return (
        _get_pytest_runtime_reason(
            fixture_names,
            fixtures,
            resource_fixture_names=resource_fixture_names,
        )
        is not None
    )


def _get_pytest_runtime_reason(
    fixture_names: tuple[str, ...],
    fixtures: dict[str, PytestFixtureDefinition],
    *,
    resource_fixture_names: tuple[str, ...] = (),
) -> str | None:
    for fixture_name in fixture_names:
        support_reason = _get_fixture_support_reason(
            fixture_name,
            fixtures,
            resource_fixture_names=resource_fixture_names,
        )
        if support_reason == 'missing_dependency':
            return f'fixture requires pytest runtime: {fixture_name}'

    return None


def _merge_pytest_runtime_reasons(
    *reasons: str | None,
) -> str | None:
    for reason in reasons:
        if reason is not None:
            return reason

    return None


def _uses_yield(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> bool:
    return any(
        isinstance(statement, ast.Yield | ast.YieldFrom)
        for statement in ast.walk(node)
    )


def _build_test_knowledge_version(file_path: Path) -> str:
    return f'pytest_test_index:{_build_test_content_hash(file_path)}'


def _build_definition_knowledge_version(file_path: Path) -> str:
    return f'pytest_definition_index:{_build_test_content_hash(file_path)}'


def _build_test_content_hash(file_path: Path) -> str:
    return hashlib.sha256(file_path.read_bytes()).hexdigest()


def _build_fixture_definition_descriptor(
    record: PytestFixtureKnowledgeRecord,
) -> DefinitionKnowledgeRecord:
    return DefinitionKnowledgeRecord(
        source_line=record.line,
        function_name=record.function_name,
        documentation=record.documentation,
        category=record.source_category,
        provider_kind=record.provider_kind,
        provider_name=record.provider_name,
        discovery_mode='ast',
        payload_kind='pytest.fixture',
        payload_version=1,
        payload={
            'fixture_name': record.function_name,
            'patterns': (record.function_name,),
            'step_type': 'fixture',
        },
    )


def _build_fixture_source_metadata(  # noqa: PLR0913
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...],
    visible_test_paths: tuple[Path, ...],
    conftest_source_paths: tuple[Path, ...] = (),
    configured_source_paths: tuple[Path, ...] = (),
) -> PytestFixtureSourceMetadata:
    resolved_source_path = source_path.resolve()
    effective_configured_source_paths = (
        set(configured_source_paths)
        if configured_source_paths
        else {
            configured_path.resolve()
            for configured_path in configured_definition_paths
        }
    )
    if resolved_source_path in effective_configured_source_paths:
        return PytestFixtureSourceMetadata(
            category='configured_definition_fixture',
            provider_kind='definition_path',
            provider_name=str(resolved_source_path),
        )

    for visible_test_path in visible_test_paths:
        for plugin_source in _discover_pytest_plugin_sources(
            visible_test_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
        ):
            if resolved_source_path == plugin_source.source_path.resolve():
                return PytestFixtureSourceMetadata(
                    category='pytest_plugin_fixture',
                    provider_kind='pytest_plugin',
                    provider_name=plugin_source.module_spec,
                )

    effective_conftest_source_paths = (
        set(conftest_source_paths)
        if conftest_source_paths
        else {
            path.resolve()
            for visible_test_path in visible_test_paths
            for path in _discover_conftest_paths(
                visible_test_path,
                root_path=root_path,
            )
        }
    )
    if (
        resolved_source_path.name == 'conftest.py'
        or resolved_source_path in effective_conftest_source_paths
    ):
        return PytestFixtureSourceMetadata(category='conftest_fixture')

    return PytestFixtureSourceMetadata()


def _discover_imported_fixture_bindings(
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
    active_sources: tuple[Path, ...] = (),
) -> dict[str, PytestFixtureDefinition]:
    resolved_source_path = source_path.resolve()
    if (
        resolved_source_path in active_sources
        or not resolved_source_path.exists()
    ):
        return {}

    module = ast.parse(resolved_source_path.read_text(encoding='utf-8'))
    imported_bindings: dict[str, PytestFixtureDefinition] = {}
    next_active_sources = (*active_sources, resolved_source_path)
    for statement in module.body:
        if not isinstance(statement, ast.ImportFrom):
            continue

        imported_source_path = _resolve_imported_source_path(
            statement,
            source_path=resolved_source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
        )
        if imported_source_path is None:
            continue

        target_bindings = _discover_fixture_export_bindings(
            imported_source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
            active_sources=next_active_sources,
        )
        if not target_bindings:
            continue

        for alias in statement.names:
            if alias.name == '*':
                for (
                    fixture_name,
                    fixture_definition,
                ) in target_bindings.items():
                    imported_bindings.setdefault(
                        fixture_name,
                        fixture_definition,
                    )
                continue

            fixture_definition = target_bindings.get(alias.name)
            if fixture_definition is None:
                continue

            imported_bindings.setdefault(
                alias.asname or alias.name,
                fixture_definition,
            )

    return imported_bindings


def _discover_fixture_export_bindings(
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
    active_sources: tuple[Path, ...] = (),
) -> dict[str, PytestFixtureDefinition]:
    resolved_source_path = source_path.resolve()
    module = ast.parse(resolved_source_path.read_text(encoding='utf-8'))
    fixture_aliases = _discover_fixture_aliases(module)
    imported_bindings = _discover_imported_fixture_bindings(
        resolved_source_path,
        root_path=root_path,
        configured_definition_paths=configured_definition_paths,
        active_sources=active_sources,
    )
    return {
        **imported_bindings,
        **_discover_fixture_definitions(
            module,
            fixture_aliases,
            source_path=resolved_source_path,
        ),
    }


def _discover_imported_fixture_source_paths(
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
    active_sources: tuple[Path, ...] = (),
) -> tuple[Path, ...]:
    imported_bindings = _discover_imported_fixture_bindings(
        source_path,
        root_path=root_path,
        configured_definition_paths=configured_definition_paths,
        active_sources=active_sources,
    )
    source_paths = {
        Path(fixture.source_path).resolve()
        for fixture in imported_bindings.values()
        if fixture.source_path is not None
    }
    return tuple(sorted(source_paths, key=str))


def _discover_visible_fixture_source_paths(
    test_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
) -> tuple[Path, ...]:
    source_paths: dict[Path, None] = {}
    pending_source_paths = deque(
        [
            test_path.resolve(),
            *reversed(
                _discover_conftest_paths(
                    test_path,
                    root_path=root_path,
                ),
            ),
            *configured_definition_paths,
        ],
    )
    while pending_source_paths:
        source_path = pending_source_paths.popleft()
        resolved_source_path = source_path.resolve()
        if (
            resolved_source_path in source_paths
            or not resolved_source_path.exists()
        ):
            continue

        active_sources = tuple(source_paths)
        source_paths[resolved_source_path] = None
        for imported_source_path in _discover_imported_fixture_source_paths(
            resolved_source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
            active_sources=active_sources,
        ):
            pending_source_paths.append(imported_source_path.resolve())
        for plugin_source in _discover_pytest_plugin_sources(
            resolved_source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
            active_sources=active_sources,
        ):
            pending_source_paths.append(plugin_source.source_path.resolve())

    return tuple(source_paths)


def _discover_pytest_plugin_source_paths(
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
    active_sources: tuple[Path, ...] = (),
) -> tuple[Path, ...]:
    return tuple(
        plugin_source.source_path
        for plugin_source in _discover_pytest_plugin_sources(
            source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
            active_sources=active_sources,
        )
    )


def _discover_pytest_plugin_sources(
    source_path: Path,
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...] = (),
    active_sources: tuple[Path, ...] = (),
) -> tuple[PytestPluginSource, ...]:
    resolved_source_path = source_path.resolve()
    if (
        resolved_source_path in active_sources
        or not resolved_source_path.exists()
    ):
        return ()

    module = ast.parse(resolved_source_path.read_text(encoding='utf-8'))
    literal_bindings, expression_bindings = _discover_static_binding_context(
        module,
        source_path=resolved_source_path,
        root_path=root_path,
        configured_definition_paths=configured_definition_paths,
        active_sources=active_sources,
    )
    plugin_specs = _resolve_pytest_plugin_specs(
        module,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    if not plugin_specs:
        return ()

    next_active_sources = (*active_sources, resolved_source_path)
    source_paths: dict[Path, PytestPluginSource] = {}
    for plugin_spec in plugin_specs:
        plugin_source_path = _resolve_module_spec_source_path(
            plugin_spec,
            source_path=resolved_source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
        )
        if plugin_source_path is None:
            continue
        resolved_plugin_source_path = plugin_source_path.resolve()
        source_paths.setdefault(
            resolved_plugin_source_path,
            PytestPluginSource(
                module_spec=plugin_spec,
                source_path=resolved_plugin_source_path,
            ),
        )
        for nested_source in _discover_pytest_plugin_sources(
            resolved_plugin_source_path,
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
            active_sources=next_active_sources,
        ):
            source_paths.setdefault(
                nested_source.source_path.resolve(),
                nested_source,
            )

    return tuple(source_paths[path] for path in sorted(source_paths, key=str))


def _resolve_imported_source_path(
    statement: ast.ImportFrom,
    *,
    source_path: Path,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...],
) -> Path | None:
    if statement.module == 'pytest':
        return None

    module_parts = (
        ()
        if statement.module is None
        else tuple(part for part in statement.module.split('.') if part)
    )
    if statement.level:
        base_directory = source_path.parent
        for _ in range(max(0, statement.level - 1)):
            base_directory = base_directory.parent
        return _resolve_import_module_candidate(base_directory, module_parts)

    search_roots = {source_path.parent.resolve()}
    if root_path is not None:
        search_roots.add(root_path.resolve())
    for configured_source_path in configured_definition_paths:
        search_roots.add(configured_source_path.resolve().parent)
        search_roots.add(configured_source_path.resolve().parent.parent)

    for search_root in sorted(search_roots, key=str):
        candidate = _resolve_import_module_candidate(
            search_root,
            module_parts,
        )
        if candidate is not None:
            return candidate

    return None


def _resolve_import_module_candidate(
    search_root: Path,
    module_parts: tuple[str, ...],
) -> Path | None:
    candidate_base = search_root.joinpath(*module_parts)
    file_candidate = candidate_base.with_suffix('.py')
    if file_candidate.exists():
        return file_candidate.resolve()

    init_candidate = candidate_base / '__init__.py'
    if init_candidate.exists():
        return init_candidate.resolve()

    return None


def _resolve_pytest_plugin_specs(
    module: ast.Module,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> tuple[str, ...] | None:
    plugin_node = _extract_pytest_plugins_node(module)

    if plugin_node is None:
        return ()

    return _resolve_pytest_plugin_spec_sequence(
        plugin_node,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )


def _resolve_pytest_plugin_spec_sequence(
    node: ast.expr,
    *,
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> tuple[str, ...] | None:
    resolved_node = _resolve_bound_expression(
        node,
        expression_bindings=expression_bindings,
    )
    if isinstance(resolved_node, ast.BinOp) and isinstance(
        resolved_node.op,
        ast.Add,
    ):
        left_specs = _resolve_pytest_plugin_spec_sequence(
            resolved_node.left,
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )
        right_specs = _resolve_pytest_plugin_spec_sequence(
            resolved_node.right,
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )
        if left_specs is None or right_specs is None:
            return None
        return (*left_specs, *right_specs)

    literal_value = _resolve_literal_reference(
        resolved_node,
        literal_bindings=literal_bindings,
    )
    if isinstance(literal_value, str):
        return (literal_value,)
    if not isinstance(literal_value, tuple | list | set):
        return None
    if not all(isinstance(value, str) for value in literal_value):
        return None

    return tuple(str(value) for value in literal_value)


def _extract_pytest_plugins_node(
    module: ast.Module,
) -> ast.expr | None:
    plugin_node: ast.expr | None = None
    for statement in module.body:
        target_name = _extract_literal_binding_target(statement)
        if target_name != 'pytest_plugins':
            continue
        plugin_node = _extract_literal_binding_value(statement)

    return plugin_node


def _discover_pytest_plugins_runtime_reason(  # noqa: PLR0913
    module: ast.Module,
    *,
    source_path: Path,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...],
    literal_bindings: dict[str, object],
    expression_bindings: dict[str, ast.expr],
) -> str | None:
    plugin_node = _extract_pytest_plugins_node(module)
    if plugin_node is None:
        return None

    plugin_specs = _resolve_pytest_plugin_spec_sequence(
        plugin_node,
        literal_bindings=literal_bindings,
        expression_bindings=expression_bindings,
    )
    if plugin_specs is None:
        return 'dynamic pytest_plugins require pytest runtime adapter'

    for plugin_spec in plugin_specs:
        plugin_source_path = _resolve_module_spec_source_path(
            plugin_spec,
            source_path=source_path.resolve(),
            root_path=root_path,
            configured_definition_paths=configured_definition_paths,
        )
        if plugin_source_path is None:
            return (
                'external pytest plugin requires pytest runtime adapter: '
                f'{plugin_spec}'
            )

    return None


def _discover_external_pytest_plugin_references(
    test_paths: tuple[Path, ...],
    *,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...],
) -> tuple[PytestExternalPluginReference, ...]:
    references: dict[str, PytestExternalPluginReference] = {}
    for test_path in test_paths:
        module = ast.parse(test_path.read_text(encoding='utf-8'))
        literal_bindings, expression_bindings = (
            _discover_static_binding_context(
                module,
                source_path=test_path.resolve(),
                root_path=root_path,
                configured_definition_paths=configured_definition_paths,
            )
        )
        plugin_specs = _resolve_pytest_plugin_specs(
            module,
            literal_bindings=literal_bindings,
            expression_bindings=expression_bindings,
        )
        if plugin_specs is None:
            continue

        for plugin_spec in plugin_specs:
            plugin_source_path = _resolve_module_spec_source_path(
                plugin_spec,
                source_path=test_path.resolve(),
                root_path=root_path,
                configured_definition_paths=configured_definition_paths,
            )
            if plugin_source_path is not None:
                continue

            references.setdefault(
                plugin_spec,
                PytestExternalPluginReference(
                    module_spec=plugin_spec,
                    runtime_reason=(
                        'External pytest plugin requires runtime adapter: '
                        f'{plugin_spec}'
                    ),
                ),
            )

    return tuple(references[key] for key in sorted(references))


def _build_external_pytest_plugin_file_path(module_spec: str) -> str:
    return f'pytest-plugin://{module_spec}'


def _resolve_module_spec_source_path(
    module_spec: str,
    *,
    source_path: Path,
    root_path: Path | None,
    configured_definition_paths: tuple[Path, ...],
) -> Path | None:
    module_parts = tuple(part for part in module_spec.split('.') if part)
    if not module_parts:
        return None

    search_roots = {source_path.parent.resolve()}
    if root_path is not None:
        search_roots.add(root_path.resolve())
    for configured_source_path in configured_definition_paths:
        search_roots.add(configured_source_path.resolve().parent)
        search_roots.add(configured_source_path.resolve().parent.parent)

    for search_root in sorted(search_roots, key=str):
        candidate = _resolve_import_module_candidate(
            search_root,
            module_parts,
        )
        if candidate is not None:
            return candidate

    return None


def _discover_function_documentation(
    module: ast.Module,
    function_name: str,
) -> str | None:
    for statement in module.body:
        if (
            isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef)
            and statement.name == function_name
        ):
            return ast.get_docstring(statement)

    return None
