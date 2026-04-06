from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, assert_never

from cosecha.core.config import Config
from cosecha.core.cosecha_manifest import (
    CosechaManifest,
    ManifestValidationError,
    apply_manifest_cli_overrides,
    explain_cosecha_manifest,
    load_cosecha_manifest,
    register_manifest_hook_arguments,
    validate_cosecha_manifest,
)
from cosecha.core.instrumentation import (
    COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
)
from cosecha.core.discovery import (
    create_loaded_discovery_registry,
    using_discovery_registry,
)
from cosecha.core.knowledge_base import (
    KNOWLEDGE_BASE_PATH,
    LEGACY_KNOWLEDGE_BASE_PATH,
    DefinitionKnowledgeQuery,
    DomainEventQuery,
    ReadOnlyPersistentKnowledgeBase,
    ResourceKnowledgeQuery,
    SessionArtifactQuery,
    TestKnowledgeQuery,
    iter_knowledge_base_file_paths as build_knowledge_base_file_paths,
    resolve_knowledge_base_path,
)
from cosecha.core.operations import (
    AnalyzePlanOperation,
    DraftValidationOperation,
    ExplainPlanOperation,
    QueryDefinitionsOperation,
    QueryEventsOperation,
    QueryLiveStatusOperation,
    QueryLiveSubscriptionOperation,
    QueryLiveTailOperation,
    QueryRegistryItemsOperation,
    QueryResourcesOperation,
    QuerySessionArtifactsOperation,
    QuerySessionArtifactsOperationResult,
    QueryTestsOperation,
    RunOperation,
    RunOperationResult,
    SimulatePlanOperation,
)
from cosecha.core.output import OutputDetail, OutputMode
from cosecha.core.registry_knowledge import RegistryKnowledgeQuery
from cosecha.core.runner import (
    Runner,
    RunnerRuntimeError,
    capture_handler,
    root_logger,
)
from cosecha.core.runtime import LocalRuntimeProvider, ProcessRuntimeProvider
from cosecha.core.utils import setup_available_plugins, setup_engines


if TYPE_CHECKING:  # pragma: no cover
    from argparse import Namespace

    from cosecha.core.engines.base import Engine
    from cosecha.core.execution_ir import PlanningMode
    from cosecha.core.hooks import Hook
    from cosecha.core.plugins.base import Plugin
    from cosecha.core.runtime import RuntimeProvider


@dataclass(slots=True, frozen=True)
class CliSelection:
    engines: tuple[str, ...] = ()
    include_paths: tuple[str, ...] = ()
    exclude_paths: tuple[str, ...] = ()
    include_labels: tuple[str, ...] = ()
    exclude_labels: tuple[str, ...] = ()
    test_limit: int | None = None

    def requested_paths(self) -> tuple[str, ...]:
        return (
            *self.include_paths,
            *(f'~{path}' for path in self.exclude_paths),
        )

    def selection_labels(self) -> tuple[str, ...]:
        return (
            *self.include_labels,
            *(f'~{label}' for label in self.exclude_labels),
        )

    def selected_engine_names(self) -> set[str] | None:
        if not self.engines:
            return None
        return set(self.engines)


@dataclass(slots=True, frozen=True)
class RuntimeCliContext:
    args: Namespace
    config: Config
    plugins: tuple[Plugin, ...]
    runtime_provider: RuntimeProvider
    selection: CliSelection

    def setup_runtime_components(self) -> tuple[list[Hook], dict[str, Engine]]:
        return setup_engines(
            self.config,
            args=self.args,
            selected_engine_names=self.selection.selected_engine_names(),
            requested_paths=self.selection.requested_paths(),
        )


@dataclass(slots=True, frozen=True)
class RunCliRequest:
    context: RuntimeCliContext

    def build_operation(self) -> RunOperation:
        return RunOperation(
            paths=self.context.selection.requested_paths(),
            selection_labels=self.context.selection.selection_labels(),
            test_limit=self.context.selection.test_limit,
        )


@dataclass(slots=True, frozen=True)
class AnalyzeCliRequest:
    context: RuntimeCliContext
    mode: PlanningMode = 'strict'

    def build_operation(self) -> AnalyzePlanOperation:
        return AnalyzePlanOperation(
            paths=self.context.selection.requested_paths(),
            selection_labels=self.context.selection.selection_labels(),
            test_limit=self.context.selection.test_limit,
            mode=self.mode,
        )


@dataclass(slots=True, frozen=True)
class ExplainCliRequest:
    context: RuntimeCliContext
    mode: PlanningMode = 'relaxed'

    def build_operation(self) -> ExplainPlanOperation:
        return ExplainPlanOperation(
            paths=self.context.selection.requested_paths(),
            selection_labels=self.context.selection.selection_labels(),
            test_limit=self.context.selection.test_limit,
            mode=self.mode,
        )


@dataclass(slots=True, frozen=True)
class SimulateCliRequest:
    context: RuntimeCliContext
    mode: PlanningMode = 'relaxed'

    def build_operation(self) -> SimulatePlanOperation:
        return SimulatePlanOperation(
            paths=self.context.selection.requested_paths(),
            selection_labels=self.context.selection.selection_labels(),
            test_limit=self.context.selection.test_limit,
            mode=self.mode,
        )


@dataclass(slots=True, frozen=True)
class ManifestValidateCliRequest:
    manifest_file: Path | None = None


@dataclass(slots=True, frozen=True)
class KnowledgeResetCliRequest:
    root_path: Path


@dataclass(slots=True, frozen=True)
class KnowledgeRebuildCliRequest:
    context: RuntimeCliContext


@dataclass(slots=True, frozen=True)
class GherkinFormatCliRequest:
    root_path: Path
    paths: tuple[str, ...]
    check: bool = False


@dataclass(slots=True, frozen=True)
class GherkinValidateCliRequest:
    context: RuntimeCliContext
    paths: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class GherkinPreCommitCliRequest:
    context: RuntimeCliContext
    paths: tuple[str, ...]
    write: bool = True


@dataclass(slots=True, frozen=True)
class PytestValidateCliRequest:
    context: RuntimeCliContext
    paths: tuple[str, ...]


type RuntimeCliRequest = (
    RunCliRequest | AnalyzeCliRequest | ExplainCliRequest | SimulateCliRequest
)


@dataclass(slots=True, frozen=True)
class ManifestShowCliRequest:
    manifest_file: Path | None = None


@dataclass(slots=True, frozen=True)
class ManifestExplainCliRequest:
    args: Namespace
    manifest_file: Path | None
    selection: CliSelection
    root_path: Path


@dataclass(slots=True, frozen=True)
class QueryRenderOptions:
    page_size: int | None = None
    offset: int = 0
    sort_by: str | None = None
    sort_order: str = 'asc'
    fields: tuple[str, ...] = ()
    view: str = 'full'
    preset: str | None = None


def _write_instrumentation_metadata_from_environment(
    artifact,
    db_path: Path | None,
) -> None:
    metadata_path_raw = os.environ.get(
        COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
    )
    if not metadata_path_raw:
        return

    metadata_path = Path(metadata_path_raw)
    payload = {
        'knowledge_base_path': None if db_path is None else str(db_path),
        'root_path': artifact.root_path,
        'session_id': artifact.session_id,
    }
    temp_path = metadata_path.with_name(f'{metadata_path.name}.tmp')
    temp_path.write_text(json.dumps(payload), encoding='utf-8')
    temp_path.replace(metadata_path)


@dataclass(slots=True, frozen=True)
class KnowledgeQueryCliRequest:
    config: Config
    operation: (
        QueryTestsOperation
        | QueryDefinitionsOperation
        | QueryEventsOperation
        | QueryRegistryItemsOperation
        | QueryResourcesOperation
        | QuerySessionArtifactsOperation
    )
    render_options: QueryRenderOptions = QueryRenderOptions()


@dataclass(slots=True, frozen=True)
class DoctorCliRequest:
    args: Namespace
    manifest_file: Path | None
    selection: CliSelection
    root_path: Path


@dataclass(slots=True, frozen=True)
class SessionQueryCliRequest:
    config: Config
    operation: (
        QuerySessionArtifactsOperation
        | QueryEventsOperation
        | QueryLiveStatusOperation
        | QueryLiveTailOperation
        | QueryLiveSubscriptionOperation
    )
    render_options: QueryRenderOptions = QueryRenderOptions()


@dataclass(slots=True, frozen=True)
class SessionSummaryCliRequest:
    config: Config
    operation: QuerySessionArtifactsOperation
    render_options: QueryRenderOptions = QueryRenderOptions()


type CliRequest = (
    RuntimeCliRequest
    | ManifestShowCliRequest
    | ManifestExplainCliRequest
    | ManifestValidateCliRequest
    | KnowledgeQueryCliRequest
    | SessionQueryCliRequest
    | SessionSummaryCliRequest
    | KnowledgeResetCliRequest
    | KnowledgeRebuildCliRequest
    | GherkinFormatCliRequest
    | GherkinValidateCliRequest
    | GherkinPreCommitCliRequest
    | PytestValidateCliRequest
    | DoctorCliRequest
)


EXIT_TEST_FAILURES = 1
EXIT_USAGE_ERROR = 2
EXIT_RUNTIME_ERROR = 3


def _create_bootstrap_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    subparsers = parser.add_subparsers(
        dest='command_group',
    )

    subparsers.add_parser('run', add_help=False)
    plan_parser = subparsers.add_parser('plan', add_help=False)
    plan_subparsers = plan_parser.add_subparsers(
        dest='plan_command',
    )
    for name in ('analyze', 'explain', 'simulate'):
        plan_subparsers.add_parser(name, add_help=False)

    manifest_parser = subparsers.add_parser('manifest', add_help=False)
    manifest_subparsers = manifest_parser.add_subparsers(
        dest='manifest_command',
    )
    manifest_subparsers.add_parser('show', add_help=False)
    manifest_subparsers.add_parser('explain', add_help=False)
    manifest_subparsers.add_parser('validate', add_help=False)
    gherkin_parser = subparsers.add_parser('gherkin', add_help=False)
    gherkin_subparsers = gherkin_parser.add_subparsers(
        dest='gherkin_command',
    )
    gherkin_subparsers.add_parser('fmt', add_help=False)
    gherkin_subparsers.add_parser('validate', add_help=False)
    gherkin_subparsers.add_parser('pre-commit', add_help=False)
    pytest_parser = subparsers.add_parser('pytest', add_help=False)
    pytest_subparsers = pytest_parser.add_subparsers(
        dest='pytest_command',
    )
    pytest_subparsers.add_parser('validate', add_help=False)

    knowledge_parser = subparsers.add_parser('knowledge', add_help=False)
    knowledge_subparsers = knowledge_parser.add_subparsers(
        dest='knowledge_command',
    )
    knowledge_query_parser = knowledge_subparsers.add_parser(
        'query',
        add_help=False,
    )
    knowledge_query_subparsers = knowledge_query_parser.add_subparsers(
        dest='knowledge_query_target',
    )
    for name in (
        'tests',
        'definitions',
        'registry',
        'resources',
        'artifacts',
        'events',
    ):
        knowledge_query_subparsers.add_parser(name, add_help=False)
    knowledge_subparsers.add_parser('reset', add_help=False)
    knowledge_subparsers.add_parser('rebuild', add_help=False)
    session_parser = subparsers.add_parser('session', add_help=False)
    session_subparsers = session_parser.add_subparsers(
        dest='session_command',
    )
    session_subparsers.add_parser('artifacts', add_help=False)
    session_subparsers.add_parser('events', add_help=False)
    session_subparsers.add_parser('summary', add_help=False)
    subparsers.add_parser('doctor', add_help=False)

    return parser


def _create_parser(
    *,
    include_manifest_hook_arguments: bool,
) -> tuple[argparse.ArgumentParser, list[type[Plugin]]]:
    parser = argparse.ArgumentParser(
        description='Cosecha CLI',
        epilog=_build_parser_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        dest='command_group',
        required=True,
    )

    shared_parser = argparse.ArgumentParser(add_help=False)
    _register_config_arguments(shared_parser)
    _register_reporting_arguments(shared_parser)
    _register_output_arguments(shared_parser)
    _register_runtime_arguments(shared_parser)

    available_plugins = setup_available_plugins(shared_parser)
    manifest = (
        _load_parser_manifest() if include_manifest_hook_arguments else None
    )
    if manifest is not None:
        register_manifest_hook_arguments(shared_parser, manifest)

    _create_run_parser(subparsers, shared_parser)
    _create_plan_parser(subparsers, shared_parser)
    manifest_explain_parser = _create_manifest_parser(subparsers)
    _create_gherkin_parser(subparsers, shared_parser)
    _create_pytest_parser(subparsers, shared_parser)
    _create_knowledge_parser(subparsers, shared_parser)
    _create_session_parser(subparsers)
    doctor_parser = _create_doctor_parser(subparsers)

    if include_manifest_hook_arguments and manifest is not None:
        register_manifest_hook_arguments(manifest_explain_parser, manifest)
        register_manifest_hook_arguments(doctor_parser, manifest)

    return parser, available_plugins


def _build_parser_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha run --engine commands --path unit/commands',
            '  cosecha plan analyze --engine commands --label api --limit 20',
            '  cosecha manifest explain --engine commands',
            '  cosecha gherkin fmt --check tests/unit/login.feature',
            '  cosecha gherkin pre-commit tests/unit/login.feature',
            '  cosecha pytest validate tests/unit/test_login.py',
            (
                '  cosecha knowledge query tests --engine gherkin '
                '--path login.feature'
            ),
            '  cosecha session artifacts --limit 5',
            '  cosecha doctor --engine commands',
            '',
            'Notas:',
            '  --path acepta absoluto, tests/... o relativo al root de tests.',
        ),
    )


def _build_run_parser_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha run --engine commands --path unit/commands',
            '  cosecha run --path unit/commands --exclude-label slow',
            '  cosecha run --output debug --report junit:report.xml',
        ),
    )


def _build_plan_parser_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha plan analyze --engine commands --label api --limit 20',
            '  cosecha plan explain --path unit/commands',
            '  cosecha plan simulate --engine commands --mode relaxed',
        ),
    )


def _build_plan_action_epilog(action: str) -> str:
    if action == 'analyze':
        return '\n'.join(
            (
                'Ejemplos:',
                '  cosecha plan analyze --engine commands --label api',
                '  cosecha plan analyze --path unit/commands --limit 10',
            ),
        )
    if action == 'explain':
        return '\n'.join(
            (
                'Ejemplos:',
                '  cosecha plan explain --engine commands',
                '  cosecha plan explain --path unit/commands',
            ),
        )

    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha plan simulate --engine commands',
            '  cosecha plan simulate --path unit/commands --mode relaxed',
        ),
    )


def _build_manifest_parser_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha manifest show',
            '  cosecha manifest validate',
            '  cosecha manifest explain --engine commands',
        ),
    )


def _build_manifest_explain_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha manifest explain --engine commands',
            '  cosecha manifest explain --path unit/commands',
        ),
    )


def _build_knowledge_parser_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha knowledge query tests --engine commands --limit 5',
            '  cosecha knowledge rebuild --engine commands',
            '  cosecha knowledge reset',
        ),
    )


def _build_session_parser_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha session artifacts --limit 5',
            '  cosecha session events --session-id abc123 --limit 20',
            '  cosecha session summary --limit 5',
            (
                '  cosecha session events --preset latest --view compact '
                '--limit 10'
            ),
        ),
    )


def _build_session_artifacts_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha session artifacts --limit 5',
            '  cosecha session artifacts --trace-id trace-123',
        ),
    )


def _build_session_events_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha session events --session-id abc123 --limit 20',
            (
                '  cosecha session events --plan-id plan-123 '
                '--event-type test.finished'
            ),
        ),
    )


def _build_session_summary_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha session summary --limit 5',
            '  cosecha session summary --trace-id trace-123 --view full',
        ),
    )


def _build_knowledge_query_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha knowledge query tests --engine commands --limit 5',
            (
                '  cosecha knowledge query definitions --engine pytest '
                '--path tests/test_api.py'
            ),
            '  cosecha knowledge query registry --engine commands --limit 3',
            '  cosecha knowledge query events --session-id abc123 --limit 20',
            (
                '  cosecha knowledge query tests --preset failures '
                '--view compact --fields node_stable_id,status'
            ),
        ),
    )


def _build_knowledge_tests_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            (
                '  cosecha knowledge query tests --engine commands '
                '--path unit/commands/features/login.feature'
            ),
            (
                '  cosecha knowledge query tests --engine pytest '
                '--status passed --limit 10'
            ),
        ),
    )


def _build_knowledge_definitions_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            (
                '  cosecha knowledge query definitions --engine pytest '
                '--path test_api.py'
            ),
            (
                '  cosecha knowledge query definitions --step-type given '
                '--step-text "un usuario autenticado"'
            ),
        ),
    )


def _build_knowledge_registry_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha knowledge query registry --engine commands --limit 3',
            (
                '  cosecha knowledge query registry --module-spec academo '
                '--layout-key commands'
            ),
        ),
    )


def _build_knowledge_resources_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha knowledge query resources --limit 10',
            '  cosecha knowledge query resources --name mongodb --scope test',
        ),
    )


def _build_knowledge_artifacts_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha knowledge query artifacts --limit 5',
            '  cosecha knowledge query artifacts --session-id abc123',
        ),
    )


def _build_knowledge_events_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha knowledge query events --limit 20',
            (
                '  cosecha knowledge query events --session-id abc123 '
                '--event-type test.finished'
            ),
        ),
    )


def _build_knowledge_rebuild_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha knowledge rebuild',
            (
                '  cosecha knowledge rebuild --engine commands '
                '--path unit/commands'
            ),
        ),
    )


def _build_manifest_show_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha manifest show',
            '  cosecha manifest show --file tests/cosecha.toml',
        ),
    )


def _build_manifest_validate_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha manifest validate',
            '  cosecha manifest validate --file tests/cosecha.toml',
        ),
    )


def _build_gherkin_parser_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha gherkin fmt tests/unit/commands',
            '  cosecha gherkin fmt --check tests/unit/login.feature',
            (
                '  cosecha gherkin validate --engine commands '
                'tests/unit/login.feature'
            ),
            '  cosecha gherkin pre-commit tests/unit/login.feature',
        ),
    )


def _build_gherkin_validate_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha gherkin validate tests/unit/login.feature',
            '  cosecha gherkin validate --engine commands tests/unit/commands',
        ),
    )


def _build_gherkin_pre_commit_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha gherkin pre-commit tests/unit/login.feature',
            (
                '  cosecha gherkin pre-commit --engine commands '
                'tests/unit/commands'
            ),
        ),
    )


def _build_pytest_parser_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha pytest validate tests/unit/test_login.py',
            '  cosecha pytest validate --engine pytest tests/unit',
            '  cosecha pytest validate tests/unit/conftest.py',
        ),
    )


def _build_doctor_epilog() -> str:
    return '\n'.join(
        (
            'Ejemplos:',
            '  cosecha doctor',
            '  cosecha doctor --engine commands --path unit/commands',
        ),
    )


def _load_parser_manifest() -> CosechaManifest | None:
    try:
        return load_cosecha_manifest()
    except ManifestValidationError:
        return None


def _create_run_parser(
    subparsers,
    shared_parser: argparse.ArgumentParser,
) -> None:
    run_parser = subparsers.add_parser(
        'run',
        parents=[shared_parser],
        help='Ejecuta tests seleccionados',
        description='Ejecuta tests seleccionados con salida humana.',
        epilog=_build_run_parser_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_selection_arguments(run_parser)
    _register_run_arguments(run_parser)


def _create_plan_parser(
    subparsers,
    shared_parser: argparse.ArgumentParser,
) -> None:
    plan_parser = subparsers.add_parser(
        'plan',
        help='Opera sobre el plan compilado',
        description='Analiza, explica o simula el plan compilado.',
        epilog=_build_plan_parser_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    plan_subparsers = plan_parser.add_subparsers(
        dest='plan_command',
        required=True,
    )
    _create_plan_action_parser(
        plan_subparsers,
        shared_parser,
        action='analyze',
        help_text='Analiza el plan seleccionado',
        default_mode='strict',
    )
    _create_plan_action_parser(
        plan_subparsers,
        shared_parser,
        action='explain',
        help_text='Explica el plan seleccionado',
        default_mode='relaxed',
    )
    _create_plan_action_parser(
        plan_subparsers,
        shared_parser,
        action='simulate',
        help_text='Simula el plan sin mutaciones',
        default_mode='relaxed',
    )


def _create_plan_action_parser(
    plan_subparsers,
    shared_parser: argparse.ArgumentParser,
    *,
    action: str,
    help_text: str,
    default_mode: PlanningMode,
) -> None:
    parser = plan_subparsers.add_parser(
        action,
        parents=[shared_parser],
        help=help_text,
        description=help_text,
        epilog=_build_plan_action_epilog(action),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_selection_arguments(parser)
    _register_label_arguments(parser)
    _register_limit_argument(parser)
    _register_planning_mode_argument(
        parser,
        default=default_mode,
    )


def _create_manifest_parser(subparsers) -> argparse.ArgumentParser:
    manifest_parser = subparsers.add_parser(
        'manifest',
        help='Opera sobre cosecha.toml',
        description='Valida, muestra o explica el manifiesto canónico.',
        epilog=_build_manifest_parser_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    manifest_subparsers = manifest_parser.add_subparsers(
        dest='manifest_command',
        required=True,
    )
    manifest_show_parser = manifest_subparsers.add_parser(
        'show',
        help='Muestra el manifiesto normalizado',
        description='Renderiza el manifiesto normalizado en JSON.',
        epilog=_build_manifest_show_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_manifest_file_argument(manifest_show_parser)
    manifest_explain_parser = manifest_subparsers.add_parser(
        'explain',
        help=(
            'Explica qué engines, runtime profiles y recursos quedarían '
            'activos'
        ),
        epilog=_build_manifest_explain_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_manifest_file_argument(manifest_explain_parser)
    _register_selection_arguments(manifest_explain_parser)
    manifest_validate_parser = manifest_subparsers.add_parser(
        'validate',
        help='Valida tests/cosecha.toml o cosecha.toml',
        description='Valida el manifiesto y falla si encuentra errores.',
        epilog=_build_manifest_validate_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_manifest_file_argument(manifest_validate_parser)
    return manifest_explain_parser


def _create_gherkin_parser(
    subparsers,
    shared_parser: argparse.ArgumentParser,
) -> None:
    gherkin_parser = subparsers.add_parser(
        'gherkin',
        help='Formatea y valida features Gherkin',
        description=(
            'Opera sobre ficheros .feature usando el formatter y la '
            'validación real del framework.'
        ),
        epilog=_build_gherkin_parser_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    gherkin_subparsers = gherkin_parser.add_subparsers(
        dest='gherkin_command',
        required=True,
    )

    gherkin_fmt_parser = gherkin_subparsers.add_parser(
        'fmt',
        help='Formatea ficheros .feature',
        description='Formatea features Gherkin en sitio o en modo check.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_gherkin_paths_argument(gherkin_fmt_parser)
    gherkin_fmt_parser.add_argument(
        '--check',
        action='store_true',
        default=False,
        dest='check_only',
        help='No escribe cambios; falla si algún fichero necesita formato',
    )

    gherkin_validate_parser = gherkin_subparsers.add_parser(
        'validate',
        parents=[shared_parser],
        help='Valida sintaxis y resolución de steps',
        description=(
            'Valida features Gherkin usando draft.validate del engine real.'
        ),
        epilog=_build_gherkin_validate_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_engine_argument(gherkin_validate_parser)
    _register_gherkin_paths_argument(gherkin_validate_parser)

    gherkin_precommit_parser = gherkin_subparsers.add_parser(
        'pre-commit',
        parents=[shared_parser],
        help='Formatea y valida staged features para pre-commit',
        description=(
            'Formatea ficheros .feature en sitio y valida sintaxis y steps.'
        ),
        epilog=_build_gherkin_pre_commit_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_engine_argument(gherkin_precommit_parser)
    _register_gherkin_paths_argument(gherkin_precommit_parser)
    gherkin_precommit_parser.add_argument(
        '--no-write',
        action='store_false',
        default=True,
        dest='write_changes',
        help='No escribe formato; valida y falla si detecta cambios',
    )


def _create_pytest_parser(
    subparsers,
    shared_parser: argparse.ArgumentParser,
) -> None:
    pytest_parser = subparsers.add_parser(
        'pytest',
        help='Valida drafts y módulos Pytest',
        description=(
            'Opera sobre ficheros Python usando draft.validate del engine '
            'Pytest real.'
        ),
        epilog=_build_pytest_parser_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pytest_subparsers = pytest_parser.add_subparsers(
        dest='pytest_command',
        required=True,
    )
    pytest_validate_parser = pytest_subparsers.add_parser(
        'validate',
        parents=[shared_parser],
        help='Valida sintaxis y semántica soportada de Pytest',
        description=(
            'Valida tests, conftest y módulos Pytest usando draft.validate.'
        ),
        epilog=_build_pytest_parser_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_engine_argument(pytest_validate_parser)
    _register_python_validation_paths_argument(pytest_validate_parser)


def _create_knowledge_parser(
    subparsers,
    shared_parser: argparse.ArgumentParser,
) -> None:
    knowledge_parser = subparsers.add_parser(
        'knowledge',
        help='Opera sobre la base de conocimiento persistente',
        description='Consulta o mantiene la KB persistente del workspace.',
        epilog=_build_knowledge_parser_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    knowledge_subparsers = knowledge_parser.add_subparsers(
        dest='knowledge_command',
        required=True,
    )
    _create_knowledge_query_parser(knowledge_subparsers)
    knowledge_subparsers.add_parser(
        'reset',
        help='Elimina la base de conocimiento persistente del workspace',
        description=(
            'Elimina el SQLite persistente y sus sidecars del workspace.'
        ),
    )
    knowledge_rebuild_parser = knowledge_subparsers.add_parser(
        'rebuild',
        parents=[shared_parser],
        help='Reconstruye la base de conocimiento persistente',
        description=(
            'Recrea la KB persistente y vuelve a indexar el workspace.'
        ),
        epilog=_build_knowledge_rebuild_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_selection_arguments(knowledge_rebuild_parser)


def _create_knowledge_query_parser(knowledge_subparsers) -> None:
    knowledge_query_parser = knowledge_subparsers.add_parser(
        'query',
        help='Consulta snapshots persistidos de la base de conocimiento',
        description=(
            'Consulta conocimiento persistido sin arrancar una sesión viva.'
        ),
        epilog=_build_knowledge_query_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    knowledge_query_subparsers = knowledge_query_parser.add_subparsers(
        dest='knowledge_query_target',
        required=True,
    )
    knowledge_query_tests_parser = knowledge_query_subparsers.add_parser(
        'tests',
        help='Consulta conocimiento persistido de tests',
        description='Consulta tests persistidos en la KB.',
        epilog=_build_knowledge_tests_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_knowledge_test_query_arguments(knowledge_query_tests_parser)
    knowledge_query_definitions_parser = knowledge_query_subparsers.add_parser(
        'definitions',
        help='Consulta conocimiento persistido de definiciones',
        description=(
            'Consulta definiciones persistidas de steps, fixtures o tests.'
        ),
        epilog=_build_knowledge_definitions_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_knowledge_definition_query_arguments(
        knowledge_query_definitions_parser,
    )
    knowledge_query_registry_parser = knowledge_query_subparsers.add_parser(
        'registry',
        help='Consulta snapshots persistidos de registry Gherkin',
        description=(
            'Consulta snapshots persistidos de layouts y registries Gherkin.'
        ),
        epilog=_build_knowledge_registry_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_knowledge_registry_query_arguments(
        knowledge_query_registry_parser,
    )
    knowledge_query_resources_parser = knowledge_query_subparsers.add_parser(
        'resources',
        help='Consulta conocimiento persistido de recursos',
        description='Consulta conocimiento persistido de recursos y scopes.',
        epilog=_build_knowledge_resources_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_knowledge_resource_query_arguments(
        knowledge_query_resources_parser,
    )
    knowledge_query_artifacts_parser = knowledge_query_subparsers.add_parser(
        'artifacts',
        help='Consulta artefactos persistidos de sesión',
        description='Consulta artefactos persistidos de sesión en la KB.',
        epilog=_build_knowledge_artifacts_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_session_artifact_query_arguments(
        knowledge_query_artifacts_parser,
    )
    knowledge_query_events_parser = knowledge_query_subparsers.add_parser(
        'events',
        help='Consulta eventos persistidos en la KB',
        description='Consulta eventos persistidos ordenados por secuencia.',
        epilog=_build_knowledge_events_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_session_event_query_arguments(knowledge_query_events_parser)


def _create_session_parser(subparsers) -> None:
    session_parser = subparsers.add_parser(
        'session',
        help='Inspecciona sesiones persistidas y sus eventos',
        description='Consulta artefactos y eventos persistidos de sesiones.',
        epilog=_build_session_parser_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    session_subparsers = session_parser.add_subparsers(
        dest='session_command',
        required=True,
    )
    session_artifacts_parser = session_subparsers.add_parser(
        'artifacts',
        help='Consulta artefactos persistidos de sesión',
        description='Consulta snapshots persistidos de sesiones cerradas.',
        epilog=_build_session_artifacts_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_session_artifact_query_arguments(session_artifacts_parser)
    session_events_parser = session_subparsers.add_parser(
        'events',
        help='Consulta eventos persistidos de sesión',
        description='Consulta eventos persistidos ordenados por secuencia.',
        epilog=_build_session_events_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_session_event_query_arguments(session_events_parser)
    session_summary_parser = session_subparsers.add_parser(
        'summary',
        help='Consulta resúmenes persistidos de sesión',
        description=(
            'Consulta la proyección resumida de report_summary y coverage '
            'de sesiones persistidas.'
        ),
        epilog=_build_session_summary_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_session_artifact_query_arguments(session_summary_parser)
    session_summary_parser.set_defaults(
        query_view='compact',
        query_preset='latest',
    )


def _create_doctor_parser(subparsers) -> argparse.ArgumentParser:
    doctor_parser = subparsers.add_parser(
        'doctor',
        help='Comprueba manifiesto, materialización y KB del workspace',
        description=(
            'Valida el manifiesto, explica la selección activa y comprueba '
            'la legibilidad de la KB del workspace.'
        ),
        epilog=_build_doctor_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _register_manifest_file_argument(doctor_parser)
    _register_selection_arguments(doctor_parser)
    return doctor_parser


def _register_config_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--stop-on-error',
        action='store_true',
        default=False,
        dest='stop_on_error',
        help='Detiene la ejecución tras el primer error o fallo',
    )
    parser.add_argument(
        '--no-capture-log',
        action='store_true',
        default=False,
        dest='no_capture_log',
        help='No captura logs durante la ejecución',
    )
    parser.add_argument(
        '-c',
        '--concurrency',
        type=int,
        dest='concurrency',
        default=1,
        help='Número de workers o tests paralelos',
    )
    parser.add_argument(
        '--strict-step-ambiguity',
        action='store_true',
        default=False,
        help='Valida ambigüedad de steps al arrancar',
    )
    parser.add_argument(
        '--definition-path',
        action='append',
        default=[],
        dest='definition_paths',
        help='Ruta adicional de definiciones externas',
    )
    parser.add_argument(
        '--persist-live-engine-snapshots',
        action='store_true',
        default=False,
        dest='persist_live_engine_snapshots',
        help=(
            'Persiste snapshots vivos tipados por engine en session artifacts'
        ),
    )


def _register_reporting_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--report',
        type=str,
        action='append',
        dest='extra_reports',
        default=[],
        help='Salida estructurada name:path, por ejemplo junit:report.xml',
    )


def _register_output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--output',
        type=str,
        choices=[mode.value for mode in OutputMode],
        default=OutputMode.SUMMARY.value,
        help='Modo de salida humana: summary, live, debug o trace',
    )
    parser.add_argument(
        '--detail',
        type=str,
        choices=[detail.value for detail in OutputDetail],
        default=OutputDetail.STANDARD.value,
        help='Detalle adicional: standard o full-failures',
    )


def _register_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--runtime',
        type=str,
        choices=['local', 'process'],
        default='local',
        help='Runtime de ejecución: local o multiproceso',
    )


def _register_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--engine',
        action='append',
        default=[],
        dest='selected_engines',
        help='Engine a activar. Puede repetirse',
    )
    parser.add_argument(
        '--path',
        action='append',
        default=[],
        dest='include_paths',
        help=(
            'Ruta incluida en la selección. Acepta absoluto, tests/... o '
            'ruta relativa al root de tests. Puede repetirse'
        ),
    )
    parser.add_argument(
        '--exclude-path',
        action='append',
        default=[],
        dest='exclude_paths',
        help=(
            'Ruta excluida de la selección. Acepta absoluto, tests/... o '
            'ruta relativa al root de tests. Puede repetirse'
        ),
    )


def _register_label_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--label',
        action='append',
        default=[],
        dest='include_labels',
        help='Label incluida en la selección. Puede repetirse',
    )
    parser.add_argument(
        '--exclude-label',
        action='append',
        default=[],
        dest='exclude_labels',
        help='Label excluida de la selección. Puede repetirse',
    )


def _register_limit_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        dest='test_limit',
        help='Límite máximo de tests a ejecutar o simular',
    )


def _register_query_limit_argument(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        dest='test_limit',
        help='Límite máximo de resultados a devolver',
    )


def _register_query_render_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        '--offset',
        type=int,
        default=0,
        dest='query_offset',
        help='Desplazamiento inicial dentro del resultado filtrado',
    )
    parser.add_argument(
        '--sort-by',
        default=None,
        dest='query_sort_by',
        help=(
            'Campo por el que ordenar; admite rutas tipo '
            'metadata.sequence_number'
        ),
    )
    parser.add_argument(
        '--sort-order',
        choices=('asc', 'desc'),
        default='asc',
        dest='query_sort_order',
        help='Orden ascendente o descendente',
    )
    parser.add_argument(
        '--fields',
        default=None,
        dest='query_fields',
        help='Lista separada por comas de campos visibles en la salida',
    )
    parser.add_argument(
        '--view',
        choices=('full', 'compact'),
        default='full',
        dest='query_view',
        help='Vista completa o compacta del resultado',
    )
    parser.add_argument(
        '--preset',
        choices=('latest', 'failures'),
        default=None,
        dest='query_preset',
        help='Preset operativo: latest o failures',
    )


def _register_run_arguments(parser: argparse.ArgumentParser) -> None:
    _register_label_arguments(parser)
    _register_limit_argument(parser)


def _register_planning_mode_argument(
    parser: argparse.ArgumentParser,
    *,
    default: PlanningMode,
) -> None:
    parser.add_argument(
        '--mode',
        type=str,
        choices=['strict', 'relaxed'],
        default=default,
        help='Modo de planificación',
    )


def _register_manifest_file_argument(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        '--file',
        type=Path,
        default=None,
        dest='manifest_file',
        help='Ruta explícita al manifiesto a usar',
    )


def _register_engine_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--engine',
        action='append',
        default=[],
        dest='selected_engines',
        help='Engine Gherkin a activar. Puede repetirse',
    )


def _register_gherkin_paths_argument(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        'paths',
        nargs='+',
        help=(
            'Ficheros o directorios .feature. Acepta absoluto, tests/... o '
            'ruta relativa al root de tests'
        ),
    )


def _register_python_validation_paths_argument(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        'paths',
        nargs='+',
        help=(
            'Ficheros o directorios Python. Acepta absoluto, tests/... o '
            'ruta relativa al root de tests'
        ),
    )


def _register_knowledge_test_query_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        '--engine',
        default=None,
        dest='query_engine_name',
        help='Filtra por engine',
    )
    parser.add_argument(
        '--path',
        default=None,
        dest='query_test_path',
        help=(
            'Filtra por ruta del test. Acepta absoluto, tests/... o '
            'ruta relativa al root de tests'
        ),
    )
    parser.add_argument(
        '--status',
        default=None,
        dest='query_status',
        help='Filtra por estado persistido',
    )
    parser.add_argument(
        '--failure-kind',
        choices=(
            'test',
            'runtime',
            'infrastructure',
            'hook',
            'bootstrap',
            'collection',
        ),
        default=None,
        dest='query_failure_kind',
        help='Filtra por tipo de fallo persistido',
    )
    parser.add_argument(
        '--node-stable-id',
        default=None,
        dest='query_node_stable_id',
        help='Filtra por node_stable_id',
    )
    parser.add_argument(
        '--plan-id',
        default=None,
        dest='query_plan_id',
        help='Filtra por plan_id',
    )
    _register_query_limit_argument(parser)
    _register_query_render_arguments(parser)


def _register_knowledge_definition_query_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        '--engine',
        default=None,
        dest='query_engine_name',
        help='Filtra por engine',
    )
    parser.add_argument(
        '--path',
        default=None,
        dest='query_file_path',
        help='Filtra por ruta relativa del fichero',
    )
    parser.add_argument(
        '--step-type',
        default=None,
        dest='query_step_type',
        help='Filtra por tipo de step o fixture',
    )
    parser.add_argument(
        '--step-text',
        default=None,
        dest='query_step_text',
        help='Filtra por texto del step',
    )
    parser.add_argument(
        '--discovery-mode',
        default=None,
        dest='query_discovery_mode',
        help='Filtra por modo de discovery',
    )
    parser.add_argument(
        '--only-valid',
        action='store_true',
        default=False,
        dest='query_only_valid',
        help='Excluye definiciones invalidadas',
    )
    _register_query_limit_argument(parser)
    _register_query_render_arguments(parser)


def _register_knowledge_registry_query_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        '--engine',
        default=None,
        dest='query_engine_name',
        help='Filtra por engine',
    )
    parser.add_argument(
        '--module-spec',
        default=None,
        dest='query_module_spec',
        help='Filtra por module_spec materializado',
    )
    parser.add_argument(
        '--package-hash',
        default=None,
        dest='query_package_hash',
        help='Filtra por package_hash',
    )
    parser.add_argument(
        '--layout-key',
        default=None,
        dest='query_layout_key',
        help='Filtra por layout_key',
    )
    parser.add_argument(
        '--loader-schema-version',
        default=None,
        dest='query_loader_schema_version',
        help='Filtra por loader_schema_version',
    )
    _register_query_limit_argument(parser)
    _register_query_render_arguments(parser)


def _register_knowledge_resource_query_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        '--name',
        default=None,
        dest='query_resource_name',
        help='Filtra por nombre del recurso',
    )
    parser.add_argument(
        '--scope',
        default=None,
        dest='query_resource_scope',
        help='Filtra por scope del recurso',
    )
    parser.add_argument(
        '--last-test-id',
        default=None,
        dest='query_last_test_id',
        help='Filtra por último test asociado',
    )
    _register_query_limit_argument(parser)
    _register_query_render_arguments(parser)


def _register_session_artifact_query_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        '--session-id',
        default=None,
        dest='query_session_id',
        help='Filtra por session_id',
    )
    parser.add_argument(
        '--trace-id',
        default=None,
        dest='query_trace_id',
        help='Filtra por trace_id',
    )
    _register_query_limit_argument(parser)
    _register_query_render_arguments(parser)


def _register_session_event_query_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        '--session-id',
        default=None,
        dest='query_session_id',
        help='Filtra por session_id',
    )
    parser.add_argument(
        '--plan-id',
        default=None,
        dest='query_plan_id',
        help='Filtra por plan_id',
    )
    parser.add_argument(
        '--event-type',
        default=None,
        dest='query_event_type',
        help='Filtra por tipo de evento',
    )
    parser.add_argument(
        '--node-stable-id',
        default=None,
        dest='query_node_stable_id',
        help='Filtra por node_stable_id',
    )
    parser.add_argument(
        '--after-sequence-number',
        type=int,
        default=None,
        dest='query_after_sequence_number',
        help='Devuelve eventos posteriores a ese sequence_number',
    )
    _register_query_limit_argument(parser)
    _register_query_render_arguments(parser)


def _resolve_root_path() -> Path:
    root_path = Path()
    if root_path.is_dir():
        tests_path = root_path / 'tests'
        if tests_path.exists():
            return tests_path
    return root_path


def _normalize_cli_path_selector(
    raw_path: str,
    *,
    root_path: Path,
) -> str:
    root_path_abs = root_path.resolve()
    cwd_abs = Path.cwd().resolve()
    input_path = Path(raw_path)
    candidates: list[Path] = []
    canonical_paths: list[str] = []

    if input_path.is_absolute():
        candidates.append(input_path)
    else:
        candidates.append(cwd_abs / input_path)
        if input_path.parts and input_path.parts[0] == root_path.name:
            candidates.append(root_path_abs.joinpath(*input_path.parts[1:]))
        else:
            candidates.append(root_path_abs / input_path)

    for candidate in candidates:
        try:
            relative_path = candidate.resolve().relative_to(root_path_abs)
        except ValueError:
            continue
        canonical_paths.append(relative_path.as_posix())

    if canonical_paths:
        unique_paths = tuple(dict.fromkeys(canonical_paths))
        if len(unique_paths) > 1:
            candidate_list = ', '.join(unique_paths)
            msg = (
                'Ambiguous --path value. Please use a path relative to the '
                f'test root {root_path}: {raw_path!r}. Candidates: '
                f'{candidate_list}'
            )
            raise ValueError(msg)
        return unique_paths[0]

    if not input_path.is_absolute() and input_path.parts:
        if input_path.parts[0] == root_path.name:
            return Path(*input_path.parts[1:]).as_posix()
        return input_path.as_posix()

    try:
        return input_path.relative_to(root_path_abs).as_posix()
    except ValueError:
        msg = (
            'Path selector must point inside the active test root '
            f'{root_path}: {raw_path!r}. Expected examples: '
            f'{root_path.name}/unit/example.feature, unit/example.feature'
        )
        raise ValueError(msg) from None


def _normalize_cli_path_selectors(
    paths: tuple[str, ...],
    *,
    root_path: Path,
) -> tuple[str, ...]:
    return tuple(
        _normalize_cli_path_selector(path, root_path=root_path)
        for path in paths
    )


def _build_reports(args: Namespace) -> dict[str, Path]:
    reports: dict[str, Path] = {}
    reporter_types = Runner.available_reporter_types()
    supported_reports = tuple(sorted(reporter_types))
    for report_str in args.extra_reports:
        if ':' not in report_str:
            msg = (
                'Invalid --report value. Expected name:path, got '
                f'{report_str!r}'
            )
            raise ValueError(msg)
        name, path = report_str.split(':', 1)
        if name not in reporter_types:
            msg = (
                'Unsupported report type. Supported values: '
                f'{", ".join(supported_reports)}'
            )
            raise ValueError(msg)
        if name in reports:
            msg = f'Duplicate report type is not supported: {name!r}'
            raise ValueError(msg)
        reports[name] = Path(path)
    return reports


def _build_config(args: Namespace) -> Config:
    output_mode = OutputMode(args.output)
    output_detail = OutputDetail(args.detail)
    if output_mode in {OutputMode.DEBUG, OutputMode.TRACE}:
        output_detail = OutputDetail.FULL_FAILURES

    root_path = _resolve_root_path()
    reports = _build_reports(args)
    return Config(
        root_path=root_path,
        output_mode=output_mode,
        output_detail=output_detail,
        stop_on_error=args.stop_on_error,
        capture_log=not args.no_capture_log,
        concurrency=args.concurrency,
        strict_step_ambiguity=args.strict_step_ambiguity,
        persist_live_engine_snapshots=args.persist_live_engine_snapshots,
        reports=reports,
        definition_paths=tuple(
            Path(definition_path) for definition_path in args.definition_paths
        ),
    )


def _build_runtime_provider(args: Namespace) -> RuntimeProvider:
    if args.runtime == 'process':
        return ProcessRuntimeProvider()
    return LocalRuntimeProvider()


def _build_selection(args: Namespace) -> CliSelection:
    root_path = _resolve_root_path()
    return CliSelection(
        engines=tuple(args.selected_engines),
        include_paths=_normalize_cli_path_selectors(
            tuple(args.include_paths),
            root_path=root_path,
        ),
        exclude_paths=_normalize_cli_path_selectors(
            tuple(args.exclude_paths),
            root_path=root_path,
        ),
        include_labels=tuple(getattr(args, 'include_labels', ())),
        exclude_labels=tuple(getattr(args, 'exclude_labels', ())),
        test_limit=getattr(args, 'test_limit', None),
    )


def _build_query_config() -> Config:
    return Config(root_path=_resolve_root_path())


def _build_query_render_options(args: Namespace) -> QueryRenderOptions:
    raw_fields = getattr(args, 'query_fields', None)
    fields = ()
    if isinstance(raw_fields, str) and raw_fields.strip():
        fields = tuple(
            field.strip() for field in raw_fields.split(',') if field.strip()
        )

    return QueryRenderOptions(
        page_size=getattr(args, 'test_limit', None),
        offset=getattr(args, 'query_offset', 0),
        sort_by=getattr(args, 'query_sort_by', None),
        sort_order=getattr(args, 'query_sort_order', 'asc'),
        fields=fields,
        view=getattr(args, 'query_view', 'full'),
        preset=getattr(args, 'query_preset', None),
    )


def _print_json_payload(payload: dict[str, object]) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=False,
        ),
    )


def _build_runtime_context(
    args: Namespace,
    *,
    available_plugins: list[type[Plugin]],
) -> RuntimeCliContext:
    active_plugins: list[Plugin] = []
    for plugin in available_plugins:
        plugin_instance = plugin.parse_args(args)
        if plugin_instance:
            active_plugins.append(plugin_instance)

    return RuntimeCliContext(
        args=args,
        config=_build_config(args),
        plugins=tuple(active_plugins),
        runtime_provider=_build_runtime_provider(args),
        selection=_build_selection(args),
    )


def _build_gherkin_runtime_context(
    args: Namespace,
    *,
    available_plugins: list[type[Plugin]],
) -> RuntimeCliContext:
    active_plugins: list[Plugin] = []
    for plugin in available_plugins:
        plugin_instance = plugin.parse_args(args)
        if plugin_instance:
            active_plugins.append(plugin_instance)

    return RuntimeCliContext(
        args=args,
        config=_build_config(args),
        plugins=tuple(active_plugins),
        runtime_provider=_build_runtime_provider(args),
        selection=CliSelection(engines=tuple(args.selected_engines)),
    )


def _should_include_manifest_hook_arguments(
    bootstrap_args: Namespace,
) -> bool:
    if bootstrap_args.command_group == 'manifest' and getattr(
        bootstrap_args,
        'manifest_command',
        None,
    ) in {'show', 'validate'}:
        return False
    if (
        bootstrap_args.command_group == 'gherkin'
        and getattr(
            bootstrap_args,
            'gherkin_command',
            None,
        )
        == 'fmt'
    ):
        return False
    if bootstrap_args.command_group == 'pytest':
        return False

    return not (
        (
            bootstrap_args.command_group == 'knowledge'
            and getattr(
                bootstrap_args,
                'knowledge_command',
                None,
            )
            in {'query', 'reset'}
        )
        or bootstrap_args.command_group == 'session'
    )


def _build_runtime_request(
    args: Namespace,
    *,
    available_plugins: list[type[Plugin]],
) -> RuntimeCliRequest:
    runtime_context = _build_runtime_context(
        args,
        available_plugins=available_plugins,
    )
    if args.command_group == 'run':
        return RunCliRequest(context=runtime_context)

    if args.plan_command == 'analyze':
        return AnalyzeCliRequest(
            context=runtime_context,
            mode=args.mode,
        )
    if args.plan_command == 'explain':
        return ExplainCliRequest(
            context=runtime_context,
            mode=args.mode,
        )
    if args.plan_command == 'simulate':
        return SimulateCliRequest(
            context=runtime_context,
            mode=args.mode,
        )

    assert_never(args.plan_command)


def _build_manifest_explain_request(
    args: Namespace,
) -> ManifestExplainCliRequest:
    return ManifestExplainCliRequest(
        args=args,
        manifest_file=args.manifest_file,
        selection=_build_selection(args),
        root_path=_resolve_root_path(),
    )


def _build_knowledge_query_request(
    args: Namespace,
) -> KnowledgeQueryCliRequest:
    config = _build_query_config()
    render_options = _build_query_render_options(args)
    query_test_path = None
    if args.knowledge_query_target == 'tests' and args.query_test_path:
        query_test_path = _normalize_cli_path_selector(
            args.query_test_path,
            root_path=config.root_path,
        )
    if args.knowledge_query_target == 'tests':
        operation = QueryTestsOperation(
            query=TestKnowledgeQuery(
                engine_name=args.query_engine_name,
                test_path=query_test_path,
                status=args.query_status,
                failure_kind=args.query_failure_kind,
                node_stable_id=args.query_node_stable_id,
                plan_id=args.query_plan_id,
                limit=None,
            ),
        )
    elif args.knowledge_query_target == 'definitions':
        operation = QueryDefinitionsOperation(
            query=DefinitionKnowledgeQuery(
                engine_name=args.query_engine_name,
                file_path=args.query_file_path,
                step_type=args.query_step_type,
                step_text=args.query_step_text,
                discovery_mode=args.query_discovery_mode,
                include_invalidated=not args.query_only_valid,
                limit=None,
            ),
        )
    elif args.knowledge_query_target == 'registry':
        operation = QueryRegistryItemsOperation(
            query=RegistryKnowledgeQuery(
                engine_name=args.query_engine_name,
                module_spec=args.query_module_spec,
                package_hash=args.query_package_hash,
                layout_key=args.query_layout_key,
                loader_schema_version=args.query_loader_schema_version,
                limit=None,
            ),
        )
    elif args.knowledge_query_target == 'resources':
        operation = QueryResourcesOperation(
            query=ResourceKnowledgeQuery(
                name=args.query_resource_name,
                scope=args.query_resource_scope,
                last_test_id=args.query_last_test_id,
                limit=None,
            ),
        )
    elif args.knowledge_query_target == 'artifacts':
        operation = QuerySessionArtifactsOperation(
            query=SessionArtifactQuery(
                session_id=args.query_session_id,
                trace_id=args.query_trace_id,
                limit=None,
            ),
        )
    elif args.knowledge_query_target == 'events':
        operation = QueryEventsOperation(
            query=DomainEventQuery(
                event_type=args.query_event_type,
                session_id=args.query_session_id,
                plan_id=args.query_plan_id,
                node_stable_id=args.query_node_stable_id,
                after_sequence_number=args.query_after_sequence_number,
                limit=None,
            ),
        )
    else:  # pragma: no cover
        assert_never(args.knowledge_query_target)

    return KnowledgeQueryCliRequest(
        config=config,
        operation=operation,
        render_options=render_options,
    )


def _build_session_query_request(
    args: Namespace,
) -> SessionQueryCliRequest | SessionSummaryCliRequest:
    config = _build_query_config()
    render_options = _build_query_render_options(args)
    if args.session_command == 'artifacts':
        operation = QuerySessionArtifactsOperation(
            query=SessionArtifactQuery(
                session_id=args.query_session_id,
                trace_id=args.query_trace_id,
                limit=None,
            ),
        )
        return SessionQueryCliRequest(
            config=config,
            operation=operation,
            render_options=render_options,
        )
    if args.session_command == 'events':
        operation = QueryEventsOperation(
            query=DomainEventQuery(
                event_type=args.query_event_type,
                session_id=args.query_session_id,
                plan_id=args.query_plan_id,
                node_stable_id=args.query_node_stable_id,
                after_sequence_number=args.query_after_sequence_number,
                limit=None,
            ),
        )
        return SessionQueryCliRequest(
            config=config,
            operation=operation,
            render_options=render_options,
        )
    if args.session_command == 'summary':
        return SessionSummaryCliRequest(
            config=config,
            operation=QuerySessionArtifactsOperation(
                query=SessionArtifactQuery(
                    session_id=args.query_session_id,
                    trace_id=args.query_trace_id,
                    limit=None,
                ),
            ),
            render_options=render_options,
        )
    assert_never(args.session_command)


def _build_maintenance_request(
    args: Namespace,
    *,
    available_plugins: list[type[Plugin]],
) -> CliRequest:
    if args.command_group == 'manifest':
        return _build_manifest_request(args)

    if args.command_group == 'knowledge':
        return _build_knowledge_request(
            args,
            available_plugins=available_plugins,
        )

    if args.command_group == 'session':
        return _build_session_query_request(args)

    if args.command_group == 'doctor':
        return DoctorCliRequest(
            args=args,
            manifest_file=args.manifest_file,
            selection=_build_selection(args),
            root_path=_resolve_root_path(),
        )
    if args.command_group == 'gherkin':
        return _build_gherkin_request(
            args,
            available_plugins=available_plugins,
        )
    if args.command_group == 'pytest':
        return _build_pytest_request(
            args,
            available_plugins=available_plugins,
        )

    assert_never(args.command_group)


def _build_manifest_request(args: Namespace) -> CliRequest:
    if args.manifest_command == 'show':
        return ManifestShowCliRequest(
            manifest_file=args.manifest_file,
        )
    if args.manifest_command == 'explain':
        return _build_manifest_explain_request(args)
    if args.manifest_command == 'validate':
        return ManifestValidateCliRequest(
            manifest_file=args.manifest_file,
        )

    assert_never(args.manifest_command)


def _build_knowledge_request(
    args: Namespace,
    *,
    available_plugins: list[type[Plugin]],
) -> CliRequest:
    if args.knowledge_command == 'query':
        return _build_knowledge_query_request(args)
    if args.knowledge_command == 'reset':
        return KnowledgeResetCliRequest(
            root_path=_resolve_root_path(),
        )
    if args.knowledge_command == 'rebuild':
        return KnowledgeRebuildCliRequest(
            context=_build_runtime_context(
                args,
                available_plugins=available_plugins,
            ),
        )

    assert_never(args.knowledge_command)


def _build_gherkin_request(
    args: Namespace,
    *,
    available_plugins: list[type[Plugin]],
) -> CliRequest:
    if args.gherkin_command == 'fmt':
        return GherkinFormatCliRequest(
            root_path=_resolve_root_path(),
            paths=tuple(args.paths),
            check=args.check_only,
        )
    if args.gherkin_command == 'validate':
        return GherkinValidateCliRequest(
            context=_build_gherkin_runtime_context(
                args,
                available_plugins=available_plugins,
            ),
            paths=tuple(args.paths),
        )
    if args.gherkin_command == 'pre-commit':
        return GherkinPreCommitCliRequest(
            context=_build_gherkin_runtime_context(
                args,
                available_plugins=available_plugins,
            ),
            paths=tuple(args.paths),
            write=args.write_changes,
        )

    assert_never(args.gherkin_command)


def _build_pytest_request(
    args: Namespace,
    *,
    available_plugins: list[type[Plugin]],
) -> CliRequest:
    if args.pytest_command == 'validate':
        return PytestValidateCliRequest(
            context=_build_gherkin_runtime_context(
                args,
                available_plugins=available_plugins,
            ),
            paths=tuple(args.paths),
        )

    assert_never(args.pytest_command)


def parse_args(argv: list[str] | None = None) -> CliRequest:
    argv_list = list(sys.argv[1:] if argv is None else argv)
    if not argv_list:
        parser, _ = _create_parser(
            include_manifest_hook_arguments=False,
        )
        parser.print_help()
        raise SystemExit(2)

    if argv_list[0] in {'-h', '--help'}:
        parser, _ = _create_parser(
            include_manifest_hook_arguments=False,
        )
        parser.parse_args(argv_list)
        msg = 'Argument parser returned without exiting for help'
        raise RuntimeError(msg)

    bootstrap_parser = _create_bootstrap_parser()
    bootstrap_args, _ = bootstrap_parser.parse_known_args(argv_list)
    include_manifest_hook_arguments = _should_include_manifest_hook_arguments(
        bootstrap_args,
    )

    parser, available_plugins = _create_parser(
        include_manifest_hook_arguments=include_manifest_hook_arguments,
    )
    args = parser.parse_args(argv_list)

    if args.command_group in {'run', 'plan'}:
        return _build_runtime_request(
            args,
            available_plugins=available_plugins,
        )

    return _build_maintenance_request(
        args,
        available_plugins=available_plugins,
    )


def _resolve_gherkin_input_path(
    raw_path: str,
    *,
    root_path: Path,
) -> Path:
    normalized_path = _normalize_cli_path_selector(
        raw_path,
        root_path=root_path,
    )
    return (root_path / normalized_path).resolve()


def _collect_gherkin_feature_paths(
    raw_paths: tuple[str, ...],
    *,
    root_path: Path,
    allow_missing: bool = False,
    ignore_non_feature_inputs: bool = False,
) -> tuple[Path, ...]:
    feature_paths: list[Path] = []
    for raw_path in raw_paths:
        resolved_path = _resolve_gherkin_input_path(
            raw_path,
            root_path=root_path,
        )
        if not resolved_path.exists():
            if allow_missing:
                continue
            msg = f'Gherkin path does not exist: {raw_path!r}'
            raise ValueError(msg)

        if resolved_path.is_dir():
            feature_paths.extend(sorted(resolved_path.rglob('*.feature')))
            continue

        if resolved_path.suffix == '.feature':
            feature_paths.append(resolved_path)
            continue

        if ignore_non_feature_inputs:
            continue

        msg = (
            'Expected a .feature file or directory containing .feature '
            f'files: {raw_path!r}'
        )
        raise ValueError(msg)

    return tuple(dict.fromkeys(feature_paths))


def _collect_python_validation_paths(
    raw_paths: tuple[str, ...],
    *,
    root_path: Path,
) -> tuple[Path, ...]:
    python_paths: list[Path] = []
    for raw_path in raw_paths:
        resolved_path = _resolve_gherkin_input_path(
            raw_path,
            root_path=root_path,
        )
        if not resolved_path.exists():
            msg = f'Pytest path does not exist: {raw_path!r}'
            raise ValueError(msg)

        if resolved_path.is_dir():
            python_paths.extend(sorted(resolved_path.rglob('*.py')))
            continue

        if resolved_path.suffix != '.py':
            msg = (
                'Expected a .py file or directory containing Python files: '
                f'{raw_path!r}'
            )
            raise ValueError(msg)

        python_paths.append(resolved_path)

    return tuple(dict.fromkeys(python_paths))


def _apply_gherkin_format_edits(
    content: str,
    *,
    file_path: Path,
) -> str:
    try:
        from cosecha.engine.gherkin.formatter import (
            GherkinDocumentFormattingEditProvider,
            PlainTextDocument,
        )
    except ModuleNotFoundError as error:
        msg = (
            'Gherkin formatting requires the optional package '
            '`cosecha-engine-gherkin`.'
        )
        raise RuntimeError(msg) from error

    formatter = GherkinDocumentFormattingEditProvider()
    document = PlainTextDocument(
        uri=str(file_path),
        source=content,
        version=1,
    )
    edits = formatter.provide_document_formatting_edits(document)
    if not edits:
        return content

    lines = content.splitlines(keepends=True)
    for edit in edits:
        start = edit.range.start
        end = edit.range.end
        lines[start.line : end.line + 1] = [edit.new_text + '\n']

    return ''.join(lines)


def _execute_gherkin_format(request: GherkinFormatCliRequest) -> None:
    feature_paths = _collect_gherkin_feature_paths(
        request.paths,
        root_path=request.root_path,
    )
    if not feature_paths:
        print('No Gherkin feature files found.')
        return

    changed_paths: list[Path] = []
    for file_path in feature_paths:
        original_content = file_path.read_text(encoding='utf-8')
        formatted_content = _apply_gherkin_format_edits(
            original_content,
            file_path=file_path,
        )
        if formatted_content == original_content:
            continue

        changed_paths.append(file_path)
        if not request.check:
            file_path.write_text(formatted_content, encoding='utf-8')
            print(f'Formatted: {file_path}')

    if request.check and changed_paths:
        for file_path in changed_paths:
            print(f'Needs format: {file_path}')
        sys.exit(EXIT_TEST_FAILURES)

    if request.check:
        print(f'Gherkin format OK: {len(feature_paths)} file(s)')


def _format_validation_issue(issue) -> str:
    location = ''
    if issue.line is not None:
        location = f' line={issue.line}'
        if issue.column is not None:
            location += f' column={issue.column}'
    severity = issue.severity.upper()
    return f'[{severity}] {issue.code}:{location} {issue.message}'


async def _validate_gherkin_features(
    request: GherkinValidateCliRequest | GherkinPreCommitCliRequest,
) -> tuple[tuple[Path, tuple[str, ...]], ...]:
    context = request.context
    root_path = context.config.root_path
    feature_paths = _collect_gherkin_feature_paths(
        request.paths,
        root_path=root_path,
        allow_missing=isinstance(request, GherkinPreCommitCliRequest),
        ignore_non_feature_inputs=isinstance(
            request,
            GherkinPreCommitCliRequest,
        ),
    )
    if not feature_paths:
        return ()

    requested_paths = tuple(
        path.relative_to(root_path).as_posix() for path in feature_paths
    )
    hooks, engines = setup_engines(
        context.config,
        args=context.args,
        selected_engine_names=context.selection.selected_engine_names(),
        requested_paths=requested_paths,
    )
    runner = Runner(
        context.config,
        engines,
        hooks,
        list(context.plugins),
        runtime_provider=context.runtime_provider,
        session_artifact_metadata_writer=(
            _write_instrumentation_metadata_from_environment
        ),
    )

    issues_by_file: list[tuple[Path, tuple[str, ...]]] = []
    started = False
    try:
        await runner.start_session(requested_paths or None)
        started = True
        for feature_path in feature_paths:
            engine = runner.find_engine(feature_path)
            if getattr(engine, 'name', None) != 'gherkin':
                issues_by_file.append(
                    (
                        feature_path,
                        ('No active Gherkin engine matched this file',),
                    ),
                )
                continue

            try:
                operation_test_path = feature_path.relative_to(
                    root_path,
                ).as_posix()
            except ValueError:
                operation_test_path = str(feature_path)

            result = await runner.execute_operation(
                DraftValidationOperation(
                    engine_name=engine.name,
                    test_path=operation_test_path,
                    source_content=feature_path.read_text(encoding='utf-8'),
                ),
            )
            validation_issues = tuple(
                _format_validation_issue(issue)
                for issue in result.validation.issues
            )
            issues_by_file.append((feature_path, validation_issues))
    finally:
        try:
            if started:
                await runner.finish_session()
        finally:
            runner._stop_log_capture()
            if capture_handler in root_logger.handlers:
                root_logger.removeHandler(capture_handler)
            capture_handler.set_emit_callback(None)

    return tuple(issues_by_file)


def _execute_gherkin_validate(
    request: GherkinValidateCliRequest,
) -> None:
    issues_by_file = asyncio.run(_validate_gherkin_features(request))
    failing_files = 0
    for file_path, issues in issues_by_file:
        if not issues:
            print(f'OK: {file_path}')
            continue
        failing_files += 1
        print(f'INVALID: {file_path}')
        for issue in issues:
            print(f'  - {issue}')

    if failing_files:
        sys.exit(EXIT_TEST_FAILURES)

    if issues_by_file:
        print(f'Validated Gherkin files: {len(issues_by_file)}')
    else:
        print('No Gherkin feature files found.')


def _execute_gherkin_pre_commit(
    request: GherkinPreCommitCliRequest,
) -> None:
    feature_paths = _collect_gherkin_feature_paths(
        request.paths,
        root_path=request.context.config.root_path,
        allow_missing=True,
        ignore_non_feature_inputs=True,
    )
    if not feature_paths:
        print('No Gherkin feature files to process.')
        return

    changed_paths: list[Path] = []
    for file_path in feature_paths:
        original_content = file_path.read_text(encoding='utf-8')
        formatted_content = _apply_gherkin_format_edits(
            original_content,
            file_path=file_path,
        )
        if formatted_content == original_content:
            continue
        changed_paths.append(file_path)
        if request.write:
            file_path.write_text(formatted_content, encoding='utf-8')
            print(f'Formatted: {file_path}')
        else:
            print(f'Needs format: {file_path}')

    issues_by_file = asyncio.run(_validate_gherkin_features(request))
    failing_files = 0
    for file_path, issues in issues_by_file:
        if not issues:
            continue
        failing_files += 1
        print(f'INVALID: {file_path}')
        for issue in issues:
            print(f'  - {issue}')

    if changed_paths or failing_files:
        sys.exit(EXIT_TEST_FAILURES)

    print(f'Gherkin pre-commit OK: {len(feature_paths)} file(s)')


async def _validate_pytest_modules(
    request: PytestValidateCliRequest,
) -> tuple[tuple[Path, tuple[str, ...]], ...]:
    context = request.context
    root_path = context.config.root_path
    python_paths = _collect_python_validation_paths(
        request.paths,
        root_path=root_path,
    )
    if not python_paths:
        return ()

    requested_paths = tuple(
        path.relative_to(root_path).as_posix() for path in python_paths
    )
    hooks, engines = setup_engines(
        context.config,
        args=context.args,
        selected_engine_names=context.selection.selected_engine_names(),
        requested_paths=requested_paths,
    )
    runner = Runner(
        context.config,
        engines,
        hooks,
        list(context.plugins),
        runtime_provider=context.runtime_provider,
        session_artifact_metadata_writer=(
            _write_instrumentation_metadata_from_environment
        ),
    )

    issues_by_file: list[tuple[Path, tuple[str, ...]]] = []
    started = False
    try:
        await runner.start_session(requested_paths or None)
        started = True
        for python_path in python_paths:
            engine = runner.find_engine(python_path)
            if engine is None or engine.name == 'gherkin':
                issues_by_file.append(
                    (
                        python_path,
                        ('No active Pytest engine matched this file',),
                    ),
                )
                continue

            try:
                operation_test_path = python_path.relative_to(
                    root_path,
                ).as_posix()
            except ValueError:
                operation_test_path = str(python_path)

            result = await runner.execute_operation(
                DraftValidationOperation(
                    engine_name=engine.name,
                    test_path=operation_test_path,
                    source_content=python_path.read_text(encoding='utf-8'),
                ),
            )
            validation_issues = tuple(
                _format_validation_issue(issue)
                for issue in result.validation.issues
            )
            issues_by_file.append((python_path, validation_issues))
    finally:
        try:
            if started:
                await runner.finish_session()
        finally:
            runner._stop_log_capture()
            if capture_handler in root_logger.handlers:
                root_logger.removeHandler(capture_handler)
            capture_handler.set_emit_callback(None)

    return tuple(issues_by_file)


def _execute_pytest_validate(
    request: PytestValidateCliRequest,
) -> None:
    issues_by_file = asyncio.run(_validate_pytest_modules(request))
    failing_files = 0
    for file_path, issues in issues_by_file:
        if not issues:
            print(f'OK: {file_path}')
            continue
        failing_files += 1
        print(f'INVALID: {file_path}')
        for issue in issues:
            print(f'  - {issue}')

    if failing_files:
        sys.exit(EXIT_TEST_FAILURES)

    if issues_by_file:
        print(f'Validated Pytest files: {len(issues_by_file)}')
    else:
        print('No Python files found.')


def _execute_manifest_validate(
    request: ManifestValidateCliRequest,
) -> None:
    manifest = load_cosecha_manifest(request.manifest_file)
    if manifest is None:
        print('No se ha encontrado cosecha.toml')
        sys.exit(EXIT_USAGE_ERROR)

    validation_errors = validate_cosecha_manifest(manifest)
    if validation_errors:
        for error in validation_errors:
            print(error)
        sys.exit(EXIT_USAGE_ERROR)

    print(f'Valid manifest: {manifest.path}')


def _execute_manifest_show(
    request: ManifestShowCliRequest,
) -> None:
    manifest = load_cosecha_manifest(request.manifest_file)
    if manifest is None:
        print('No se ha encontrado cosecha.toml')
        sys.exit(EXIT_USAGE_ERROR)

    _print_json_payload(manifest.to_dict())


def _serialize_manifest_explanation_payload(
    explanation,
) -> dict[str, object]:
    return {
        'manifest': {
            'path': explanation.manifest_path,
            'schema_version': explanation.schema_version,
            'root_path': explanation.root_path,
        },
        'selection': {
            'selected_engine_names': list(explanation.selected_engine_names),
            'requested_paths': list(explanation.requested_paths),
            'normalized_paths': list(explanation.normalized_paths),
        },
        'engines': {
            'active': [
                engine.to_dict() for engine in explanation.active_engines
            ],
            'inactive_ids': list(explanation.inactive_engine_ids),
            'evaluated': [
                engine.to_dict() for engine in explanation.evaluated_engines
            ],
        },
        'runtime_profiles': {
            'active_ids': list(explanation.active_runtime_profile_ids),
            'inactive_ids': list(explanation.inactive_runtime_profile_ids),
            'evaluated': [
                profile.to_dict()
                for profile in explanation.evaluated_runtime_profiles
            ],
        },
        'resources': {
            'active_names': list(explanation.active_resource_names),
            'inactive_names': list(explanation.inactive_resource_names),
            'evaluated': [
                resource.to_dict()
                for resource in explanation.evaluated_resources
            ],
        },
    }


def _execute_manifest_explain(
    request: ManifestExplainCliRequest,
) -> None:
    manifest = load_cosecha_manifest(request.manifest_file)
    if manifest is None:
        print('No se ha encontrado cosecha.toml')
        sys.exit(EXIT_USAGE_ERROR)

    manifest = apply_manifest_cli_overrides(manifest, request.args)
    explanation = explain_cosecha_manifest(
        manifest,
        config=Config(root_path=request.root_path),
        selected_engine_names=request.selection.selected_engine_names(),
        requested_paths=request.selection.requested_paths(),
    )
    _print_json_payload(_serialize_manifest_explanation_payload(explanation))


def _iter_knowledge_base_file_paths(root_path: Path) -> tuple[Path, ...]:
    all_paths: list[Path] = []
    for relative_db_path in (KNOWLEDGE_BASE_PATH, LEGACY_KNOWLEDGE_BASE_PATH):
        for file_path in build_knowledge_base_file_paths(
            root_path / relative_db_path,
        ):
            if file_path not in all_paths:
                all_paths.append(file_path)

    return tuple(all_paths)


def _delete_knowledge_base_files(root_path: Path) -> tuple[Path, ...]:
    removed_paths: list[Path] = []
    for file_path in _iter_knowledge_base_file_paths(root_path):
        if file_path.exists():
            file_path.unlink()
            removed_paths.append(file_path)

    return tuple(removed_paths)


def _execute_knowledge_reset(request: KnowledgeResetCliRequest) -> None:
    db_path = resolve_knowledge_base_path(
        request.root_path,
        migrate_legacy=False,
    )
    removed_paths = _delete_knowledge_base_files(request.root_path)
    if not removed_paths:
        print(f'Knowledge base already absent: {db_path}')
        return

    print(f'Knowledge base reset: {db_path}')


def _execute_knowledge_rebuild(request: KnowledgeRebuildCliRequest) -> None:
    context = request.context
    db_path = resolve_knowledge_base_path(
        context.config.root_path,
        migrate_legacy=False,
    )
    _delete_knowledge_base_files(context.config.root_path)

    hooks, engines = context.setup_runtime_components()
    runner = Runner(
        context.config,
        engines,
        hooks,
        list(context.plugins),
        runtime_provider=context.runtime_provider,
        session_artifact_metadata_writer=(
            _write_instrumentation_metadata_from_environment
        ),
    )

    async def _rebuild_snapshot():
        started = False
        try:
            await runner.start_session(
                context.selection.requested_paths() or None,
            )
            started = True
        finally:
            if started:
                await runner.finish_session()

        return runner.knowledge_base.snapshot()

    snapshot = asyncio.run(_rebuild_snapshot())
    print(f'Rebuilt knowledge base: {db_path}')
    print(f'Tests: {len(snapshot.tests)}')
    print(f'Definitions: {len(snapshot.definitions)}')
    print(f'Registry snapshots: {len(snapshot.registry_snapshots)}')
    print(f'Resources: {len(snapshot.resources)}')


_QUERY_ITEM_KEYS = (
    'tests',
    'definitions',
    'registry_snapshots',
    'resources',
    'artifacts',
    'events',
    'summaries',
)

_COMPACT_FIELDS_BY_KEY = {
    'tests': (
        'node_stable_id',
        'engine_name',
        'test_name',
        'test_path',
        'status',
        'failure_kind',
        'last_error_code',
        'finished_at',
    ),
    'definitions': (
        'engine_name',
        'file_path',
        'definition_count',
        'discovery_mode',
        'indexed_at',
        'invalidated_at',
    ),
    'registry_snapshots': (
        'engine_name',
        'module_spec',
        'layout_key',
        'package_hash',
        'source_count',
        'created_at',
    ),
    'resources': (
        'name',
        'scope',
        'last_test_id',
        'owner_node_stable_id',
        'trace_id',
    ),
    'artifacts': (
        'session_id',
        'trace_id',
        'plan_id',
        'recorded_at',
        'has_failures',
    ),
    'events': (
        'event_type',
        'timestamp',
        'sequence_number',
        'session_id',
        'plan_id',
        'node_stable_id',
        'failure_kind',
        'error_code',
    ),
    'summaries': (
        'session_id',
        'trace_id',
        'plan_id',
        'recorded_at',
        'has_failures',
        'total_tests',
        'engine_count',
        'live_snapshot_count',
        'live_snapshot_breakdown',
        'coverage_total',
    ),
}

_LATEST_SORT_FIELDS_BY_KEY = {
    'tests': ('finished_at', 'indexed_at', 'started_at'),
    'definitions': ('indexed_at', 'last_materialized_at'),
    'registry_snapshots': ('created_at',),
    'artifacts': ('recorded_at',),
    'events': ('sequence_number', 'timestamp'),
    'summaries': ('recorded_at',),
}


def _latest_preset_sort(
    items: list[dict[str, object]],
    *,
    items_key: str,
) -> tuple[list[dict[str, object]], str, str]:
    for field_name in _LATEST_SORT_FIELDS_BY_KEY.get(items_key, ()):
        has_field_values = any(
            _extract_nested_value(item, field_name) is not None
            for item in items
        )
        if has_field_values:
            return items, field_name, 'desc'
    msg = f"Preset 'latest' is not supported for result set {items_key!r}"
    raise ValueError(msg)


def _event_failure_items(
    items: list[dict[str, object]],
) -> list[dict[str, object]]:
    filtered_events: list[dict[str, object]] = []
    for item in items:
        event_type = item.get('event_type')
        if event_type in {'node.requeued', 'node.retrying'}:
            filtered_events.append(item)
            continue
        if (
            event_type == 'session.finished'
            and item.get('has_failures') is True
        ):
            filtered_events.append(item)
            continue
        if event_type == 'test.finished' and item.get('status') in {
            'failed',
            'error',
        }:
            filtered_events.append(item)
            continue
        if item.get('error_code') is not None:
            filtered_events.append(item)
    return filtered_events


def _failures_preset_items(
    items: list[dict[str, object]],
    *,
    items_key: str,
) -> list[dict[str, object]]:
    if items_key == 'tests':
        return [
            item for item in items if item.get('status') in {'failed', 'error'}
        ]
    if items_key == 'artifacts':
        return [item for item in items if item.get('has_failures') is True]
    if items_key == 'events':
        return _event_failure_items(items)
    msg = f"Preset 'failures' is not supported for result set {items_key!r}"
    raise ValueError(msg)


def _extract_nested_value(item: dict[str, object], field_path: str) -> object:
    value: object = item
    for segment in field_path.split('.'):
        if not isinstance(value, dict):
            return None
        value = value.get(segment)
    return value


def _compact_query_item(
    item: dict[str, object],
    *,
    items_key: str,
) -> dict[str, object]:
    if items_key != 'events':
        return {
            field: item.get(field)
            for field in _COMPACT_FIELDS_BY_KEY[items_key]
            if field in item
        }

    metadata = item.get('metadata')
    metadata_dict = metadata if isinstance(metadata, dict) else {}
    return {
        'event_type': item.get('event_type'),
        'timestamp': item.get('timestamp'),
        'sequence_number': metadata_dict.get('sequence_number'),
        'session_id': metadata_dict.get('session_id'),
        'plan_id': metadata_dict.get('plan_id'),
        'node_stable_id': metadata_dict.get('node_stable_id'),
        'failure_kind': item.get('failure_kind'),
        'error_code': item.get('error_code'),
    }


def _apply_query_preset(
    items: list[dict[str, object]],
    *,
    items_key: str,
    preset: str | None,
) -> tuple[list[dict[str, object]], str | None, str]:
    if preset is None:
        return items, None, 'asc'
    if preset == 'latest':
        return _latest_preset_sort(items, items_key=items_key)
    if preset == 'failures':
        return _failures_preset_items(items, items_key=items_key), None, 'asc'
    return items, None, 'asc'


def _sort_query_items(
    items: list[dict[str, object]],
    *,
    sort_by: str | None,
    sort_order: str,
) -> list[dict[str, object]]:
    if sort_by is None:
        return items
    reverse = sort_order == 'desc'

    def _sort_key(item: dict[str, object]) -> tuple[int, str]:
        value = _extract_nested_value(item, sort_by)
        return (value is None, repr(value))

    return sorted(items, key=_sort_key, reverse=reverse)


def _project_query_items(
    items: list[dict[str, object]],
    *,
    fields: tuple[str, ...],
) -> list[dict[str, object]]:
    if not fields:
        return items
    return [{field: item.get(field) for field in fields} for item in items]


def _coerce_query_items(value: object) -> list[dict[str, object]] | None:
    if not isinstance(value, list | tuple):
        return None
    return [item for item in value if isinstance(item, dict)]


def _find_query_items_key(payload: dict[str, object]) -> str | None:
    for items_key in _QUERY_ITEM_KEYS:
        if _coerce_query_items(payload.get(items_key)) is not None:
            return items_key
    return None


def _format_query_payload(
    payload: dict[str, object],
    *,
    render_options: QueryRenderOptions,
) -> dict[str, object]:
    items_key = _find_query_items_key(payload)
    if items_key is None:
        return payload

    items = _coerce_query_items(payload.get(items_key))
    if items is None:
        return payload

    items, preset_sort_by, preset_sort_order = _apply_query_preset(
        items,
        items_key=items_key,
        preset=render_options.preset,
    )
    sort_by = render_options.sort_by or preset_sort_by
    sort_order = (
        render_options.sort_order
        if render_options.sort_by is not None
        else preset_sort_order
    )
    items = _sort_query_items(
        items,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    total_items = len(items)
    offset = min(render_options.offset, total_items)
    page_size = render_options.page_size
    if page_size is None:
        paged_items = items[offset:]
    else:
        paged_items = items[offset : offset + page_size]
    if render_options.view == 'compact':
        paged_items = [
            _compact_query_item(item, items_key=items_key)
            for item in paged_items
        ]
    paged_items = _project_query_items(
        paged_items,
        fields=render_options.fields,
    )
    return {
        'result_type': payload.get('result_type'),
        'context': payload.get('context'),
        'query': {
            'items_key': items_key,
            'view': render_options.view,
            'preset': render_options.preset,
            'sort_by': sort_by,
            'sort_order': sort_order,
            'fields': list(render_options.fields),
        },
        'page': {
            'offset': offset,
            'limit': page_size,
            'returned': len(paged_items),
            'total': total_items,
            'has_more': offset + len(paged_items) < total_items,
        },
        items_key: paged_items,
    }


def _execute_query_request(
    request: KnowledgeQueryCliRequest | SessionQueryCliRequest,
) -> None:
    runner = Runner(request.config, {})
    result = asyncio.run(runner.execute_operation(request.operation))
    _print_json_payload(
        _format_query_payload(
            result.to_dict(),
            render_options=request.render_options,
        ),
    )


def _serialize_summary_counts(
    counts: tuple[tuple[str, int], ...],
) -> dict[str, int]:
    return dict(counts)


def _serialize_engine_summary(engine_summary) -> dict[str, object]:
    return {
        'detail_counts': _serialize_summary_counts(
            engine_summary.detail_counts,
        ),
        'engine_name': engine_summary.engine_name,
        'failed_examples': list(engine_summary.failed_examples),
        'failed_files': list(engine_summary.failed_files),
        'failure_kind_counts': _serialize_summary_counts(
            engine_summary.failure_kind_counts,
        ),
        'status_counts': _serialize_summary_counts(
            engine_summary.status_counts,
        ),
        'total_tests': engine_summary.total_tests,
    }


def _serialize_session_summary_artifact(artifact) -> dict[str, object]:
    report_summary = artifact.report_summary
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
        engine_summaries = report_summary.engine_summaries
        live_engine_snapshots = report_summary.live_engine_snapshots
        failure_kind_counts = report_summary.failure_kind_counts
        status_counts = report_summary.status_counts
        failed_examples = report_summary.failed_examples
        failed_files = report_summary.failed_files
        total_tests = report_summary.total_tests
        instrumentation_summaries = {
            name: summary.to_dict()
            for name, summary in report_summary.instrumentation_summaries.items()
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
        breakdown_key = f'{snapshot.engine_name}:{snapshot.snapshot_kind}'
        live_snapshot_breakdown[breakdown_key] = (
            live_snapshot_breakdown.get(breakdown_key, 0) + 1
        )

    return {
        'coverage_summary': (
            coverage_summary
        ),
        'coverage_total': coverage_total,
        'engine_count': len(engine_summaries),
        'engine_summaries': [
            _serialize_engine_summary(engine_summary)
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
        'failure_kind_counts': _serialize_summary_counts(
            failure_kind_counts,
        ),
        'has_failures': artifact.has_failures,
        'instrumentation_summaries': instrumentation_summaries,
        'plan_id': artifact.plan_id,
        'recorded_at': artifact.recorded_at,
        'root_path': artifact.root_path,
        'session_id': artifact.session_id,
        'status_counts': _serialize_summary_counts(status_counts),
        'total_tests': total_tests,
        'trace_id': artifact.trace_id,
    }


def _execute_session_summary_request(
    request: SessionSummaryCliRequest,
) -> None:
    runner = Runner(request.config, {})
    result = asyncio.run(runner.execute_operation(request.operation))
    operation_result = QuerySessionArtifactsOperationResult.from_dict(
        result.to_dict(),
    )
    payload = {
        'context': operation_result.context.to_dict(),
        'result_type': 'session.summary',
        'summaries': [
            _serialize_session_summary_artifact(artifact)
            for artifact in operation_result.artifacts
        ],
    }
    _print_json_payload(
        _format_query_payload(
            payload,
            render_options=request.render_options,
        ),
    )


def _print_doctor_selection(selection: CliSelection) -> None:
    if selection.include_paths:
        print(
            'Normalized include paths: ' + ', '.join(selection.include_paths),
        )
    if selection.exclude_paths:
        print(
            'Normalized exclude paths: ' + ', '.join(selection.exclude_paths),
        )


def _print_inactive_decisions(
    title: str,
    decisions: list[tuple[str, tuple[str, ...]]],
) -> None:
    if not decisions:
        return
    print(title + ': ' + ', '.join(name for name, _ in decisions))
    for name, reasons in decisions:
        print(f'  - {name}: ' + ', '.join(reasons or ('inactive',)))


def _check_doctor_manifest(
    request: DoctorCliRequest,
    *,
    issues: list[str],
) -> None:
    manifest = load_cosecha_manifest(request.manifest_file)
    if manifest is None:
        issues.append('No se ha encontrado cosecha.toml')
        return

    validation_errors = validate_cosecha_manifest(manifest)
    if validation_errors:
        issues.extend(validation_errors)
        return

    manifest = apply_manifest_cli_overrides(manifest, request.args)
    explanation = explain_cosecha_manifest(
        manifest,
        config=Config(root_path=request.root_path),
        selected_engine_names=request.selection.selected_engine_names(),
        requested_paths=request.selection.requested_paths(),
    )
    print(f'Manifest: {manifest.path}')
    print(f'Manifest root: {manifest.manifest_dir}')
    print(
        'Active engines: '
        + ', '.join(engine.name for engine in explanation.active_engines),
    )
    inactive_engines = [
        (engine.name, engine.reasons)
        for engine in explanation.evaluated_engines
        if not engine.active
    ]
    _print_inactive_decisions('Inactive engines', inactive_engines)
    if explanation.active_runtime_profile_ids:
        print(
            'Active runtime profiles: '
            + ', '.join(explanation.active_runtime_profile_ids),
        )
    inactive_runtime_profiles = [
        (profile.id, profile.reasons)
        for profile in explanation.evaluated_runtime_profiles
        if not profile.active
    ]
    _print_inactive_decisions(
        'Inactive runtime profiles',
        inactive_runtime_profiles,
    )
    if explanation.active_resource_names:
        print(
            'Active resources: '
            + ', '.join(explanation.active_resource_names),
        )
    inactive_resources = [
        (resource.name, resource.reasons)
        for resource in explanation.evaluated_resources
        if not resource.active
    ]
    _print_inactive_decisions(
        'Inactive resources',
        inactive_resources,
    )

    try:
        setup_engines(
            Config(root_path=request.root_path),
            args=request.args,
            selected_engine_names=request.selection.selected_engine_names(),
            requested_paths=request.selection.requested_paths(),
        )
    except Exception as error:  # pragma: no cover - exercised in CLI
        issues.append(f'Runtime materialization failed: {error}')


def _check_doctor_knowledge_base(
    request: DoctorCliRequest,
    *,
    issues: list[str],
) -> None:
    db_path = resolve_knowledge_base_path(
        request.root_path,
        migrate_legacy=False,
    )
    if not db_path.exists():
        print(f'Knowledge base: absent ({db_path})')
        return

    try:
        knowledge_base = ReadOnlyPersistentKnowledgeBase(db_path)
    except Exception as error:  # pragma: no cover - exercised in CLI
        issues.append(f'Knowledge base is not readable: {error}')
        return

    try:
        snapshot = knowledge_base.snapshot()
        artifacts = knowledge_base.query_session_artifacts(
            SessionArtifactQuery(),
        )
    finally:
        knowledge_base.close()

    print(f'Knowledge base: OK ({db_path})')
    print(
        'Knowledge counts: '
        f'tests={len(snapshot.tests)}, '
        f'definitions={len(snapshot.definitions)}, '
        f'registry={len(snapshot.registry_snapshots)}, '
        f'resources={len(snapshot.resources)}, '
        f'artifacts={len(artifacts)}',
    )
    if artifacts:
        latest_artifact = artifacts[0]
        print(
            'Latest artifact: '
            f'session_id={latest_artifact.session_id}, '
            f'trace_id={latest_artifact.trace_id or "-"}, '
            f'plan_id={latest_artifact.plan_id or "-"}, '
            f'has_failures={latest_artifact.has_failures}',
        )


def _execute_doctor(
    request: DoctorCliRequest,
) -> None:
    issues: list[str] = []
    print(f'Root path: {request.root_path}')
    _print_doctor_selection(request.selection)
    _check_doctor_manifest(request, issues=issues)
    _check_doctor_knowledge_base(request, issues=issues)

    if issues:
        print('Doctor status: FAILED')
        for issue in issues:
            print(f'- {issue}')
        sys.exit(EXIT_USAGE_ERROR)

    print('Doctor status: OK')


def _execute_runtime_request(request: CliRequest) -> None:
    if isinstance(
        request,
        (
            ManifestShowCliRequest,
            ManifestExplainCliRequest,
            ManifestValidateCliRequest,
            KnowledgeQueryCliRequest,
            SessionQueryCliRequest,
            SessionSummaryCliRequest,
            KnowledgeResetCliRequest,
            KnowledgeRebuildCliRequest,
            GherkinFormatCliRequest,
            GherkinValidateCliRequest,
            GherkinPreCommitCliRequest,
            PytestValidateCliRequest,
            DoctorCliRequest,
        ),
    ):
        msg = 'Non-runtime requests are handled separately'
        raise RuntimeError(msg)

    context = request.context
    hooks, engines = context.setup_runtime_components()
    runner = Runner(
        context.config,
        engines,
        hooks,
        list(context.plugins),
        runtime_provider=context.runtime_provider,
        session_artifact_metadata_writer=(
            _write_instrumentation_metadata_from_environment
        ),
    )

    if isinstance(
        request,
        (
            RunCliRequest,
            AnalyzeCliRequest,
            ExplainCliRequest,
            SimulateCliRequest,
        ),
    ):
        operation = request.build_operation()
    else:  # pragma: no cover
        assert_never(request)

    result = asyncio.run(runner.execute_operation(operation))
    if isinstance(result, RunOperationResult) and result.has_failures:
        sys.exit(EXIT_TEST_FAILURES)
    if not isinstance(result, RunOperationResult):
        _print_json_payload(result.to_dict())


def _execute_non_runtime_request(request: CliRequest) -> bool:
    handler_map = {
        ManifestShowCliRequest: _execute_manifest_show,
        ManifestExplainCliRequest: _execute_manifest_explain,
        ManifestValidateCliRequest: _execute_manifest_validate,
        KnowledgeQueryCliRequest: _execute_query_request,
        SessionQueryCliRequest: _execute_query_request,
        SessionSummaryCliRequest: _execute_session_summary_request,
        KnowledgeResetCliRequest: _execute_knowledge_reset,
        KnowledgeRebuildCliRequest: _execute_knowledge_rebuild,
        GherkinFormatCliRequest: _execute_gherkin_format,
        GherkinValidateCliRequest: _execute_gherkin_validate,
        GherkinPreCommitCliRequest: _execute_gherkin_pre_commit,
        PytestValidateCliRequest: _execute_pytest_validate,
        DoctorCliRequest: _execute_doctor,
    }
    handler = handler_map.get(type(request))
    if handler is None:
        return False

    handler(request)
    return True


def main(argv: list[str] | None = None) -> None:
    """Execute the Cosecha CLI."""
    try:
        registry = create_loaded_discovery_registry()
        with using_discovery_registry(registry):
            request = parse_args() if argv is None else parse_args(argv)
            if _execute_non_runtime_request(request):
                return

            _execute_runtime_request(request)
    except ValueError as error:
        print(error)
        sys.exit(EXIT_USAGE_ERROR)
    except RunnerRuntimeError as error:
        print(error)
        sys.exit(EXIT_RUNTIME_ERROR)


if __name__ == '__main__':
    main()
