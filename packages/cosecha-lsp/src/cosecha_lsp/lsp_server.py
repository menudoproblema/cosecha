from __future__ import annotations

import logging
import sys

from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING, Final, TextIO
from urllib.parse import unquote, urlparse

from lsprotocol.types import (
    TEXT_DOCUMENT_COMPLETION,
    TEXT_DOCUMENT_DEFINITION,
    TEXT_DOCUMENT_DID_OPEN,
    TEXT_DOCUMENT_DID_SAVE,
    TEXT_DOCUMENT_FORMATTING,
    TEXT_DOCUMENT_HOVER,
    CompletionItem,
    CompletionItemKind,
    CompletionList,
    CompletionOptions,
    CompletionParams,
    DefinitionParams,
    Diagnostic,
    DiagnosticSeverity,
    DidOpenTextDocumentParams,
    DidSaveTextDocumentParams,
    DocumentFormattingParams,
    ExecuteCommandParams,
    Hover,
    HoverParams,
    InsertTextFormat,
    Location,
    MarkupContent,
    MarkupKind,
    NotebookDocumentSyncOptions,
    Position,
    Range,
    TextDocumentSyncKind,
    TextEdit,
)
from pygls.protocol import LanguageServerProtocol, default_converter
from pygls.server import LanguageServer, StdOutTransportAdapter, aio_readline

from cosecha.core.config import Config
from cosecha.core.discovery import (
    create_loaded_discovery_registry,
    iter_shell_lsp_contributions,
    using_discovery_registry,
)
from cosecha.core.execution_ir import build_test_path_label
from cosecha.core.knowledge_base import (
    DefinitionKnowledge,
    DefinitionKnowledgeQuery,
    ReadOnlyPersistentKnowledgeBase,
    TestKnowledgeQuery,
    resolve_knowledge_base_path,
)
from cosecha.core.operations import (
    DraftValidationOperation,
    QueryDefinitionsOperation,
    ResolvedDefinition,
    ResolveDefinitionOperation,
)
from cosecha.core.runner import Runner
from cosecha.core.utils import setup_engines
from cosecha.engine.gherkin.definition_knowledge import get_gherkin_payload
from cosecha.engine.gherkin.formatter import (
    DocumentTextEdit,
    GherkinDocumentFormattingEditProvider,
    PlainTextDocument,
)
from cosecha.workspace import build_execution_context, resolve_workspace


if TYPE_CHECKING:  # pragma: no cover
    from pygls.workspace.text_document import TextDocument

    from cosecha.core.engines.base import Engine
    from cosecha.core.hooks import Hook
    from cosecha.engine.gherkin.completion import CompletionSuggestion
    from cosecha.engine.gherkin.types import StepType


SERVER_NAME: Final[str] = 'cosecha-lsp'

logger = logging.getLogger(__name__)


class CosechaLanguageServer(LanguageServer):
    read_only_knowledge_base: ReadOnlyPersistentKnowledgeBase | None

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        version: str,
        loop=None,
        protocol_cls: type[LanguageServerProtocol] = LanguageServerProtocol,
        converter_factory=default_converter,
        text_document_sync_kind: TextDocumentSyncKind = (
            TextDocumentSyncKind.Incremental
        ),
        notebook_document_sync: NotebookDocumentSyncOptions | None = None,
        max_workers: int = 2,
    ):
        super().__init__(
            name,
            version,
            loop,
            protocol_cls,
            converter_factory,
            text_document_sync_kind,
            notebook_document_sync,
            max_workers,
        )

        self.gherkin_edit_provider = GherkinDocumentFormattingEditProvider()
        self.read_only_knowledge_base = None

    async def start(
        self,
        config: Config,
        hooks: list[Hook],
        engines: dict[str, Engine],
        stdin: TextIO | None = None,
        stdout: TextIO | None = None,
    ):
        self.config = config
        self.engines = engines
        self.read_only_knowledge_base = self._open_read_only_knowledge_base()
        self.runner = Runner(config, engines, hooks, [])
        for engine in self.engines.values():
            logger.info(f'Using engine: {engine.__class__.__name__}')

        """Starts IO server."""
        logger.info('Starting IO server')

        self._stop_event = Event()
        transport = StdOutTransportAdapter(
            stdin or sys.stdin.buffer,
            stdout or sys.stdout.buffer,
        )
        self.lsp.connection_made(transport)  # type: ignore[arg-type]

        try:
            await aio_readline(
                self.loop,
                self.thread_pool_executor,
                self._stop_event,
                stdin or sys.stdin.buffer,
                self.lsp.data_received,
            )
        except BrokenPipeError:
            logger.error(
                'Connection to the client is lost! Shutting down the server.',
            )
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            if self.read_only_knowledge_base is not None:
                self.read_only_knowledge_base.close()
            self.shutdown()

    def _open_read_only_knowledge_base(
        self,
    ) -> ReadOnlyPersistentKnowledgeBase | None:
        db_path = resolve_knowledge_base_path(
            self.config.workspace_root_path,
            knowledge_storage_root=self.config.knowledge_storage_root_path,
        )
        if not db_path.exists():
            return None

        try:
            return ReadOnlyPersistentKnowledgeBase(db_path)
        except Exception:
            logger.warning(
                'Unable to open read-only knowledge base at %s',
                db_path,
                exc_info=True,
            )
            return None

    def _find_gherkin_engine_from_knowledge_base(
        self,
        full_path: Path,
    ):
        if self.read_only_knowledge_base is None:
            return None

        test_path_label = build_test_path_label(
            self.config.root_path,
            full_path,
        )
        matching_tests = self.read_only_knowledge_base.query_tests(
            TestKnowledgeQuery(
                engine_name='gherkin',
                test_path=test_path_label,
                limit=1,
            ),
        )
        if not matching_tests:
            return None

        for engine in self.engines.values():
            if _is_gherkin_engine(engine):
                return engine

        return None

    def find_gherkin_engine(self, uri: str):
        full_path = uri_to_path(uri).resolve()
        try:
            log_path = full_path.relative_to(self.config.root_path)
        except ValueError:
            log_path = full_path
        logger.debug(f'Looking engine for document "{log_path}"')
        engine = self.runner.find_engine(full_path)

        if engine and not _is_gherkin_engine(engine):
            msg = f'Invalid engine: {engine}'
            raise TypeError(msg)

        if engine is not None:
            return engine

        return self._find_gherkin_engine_from_knowledge_base(full_path)

    async def validate_gherkin_document(
        self,
        document: TextDocument,
    ) -> list[Diagnostic]:
        engine = self.find_gherkin_engine(document.uri)
        if engine is None:
            return []

        result = await self.runner.execute_operation(
            DraftValidationOperation(
                engine_name=engine.name,
                test_path=str(uri_to_path(document.uri)),
                source_content=document.source,
            ),
        )
        validation = result.validation
        return build_diagnostics_from_draft_validation(validation.issues)

    async def resolve_gherkin_step_definitions(
        self,
        *,
        document: TextDocument,
        step_type: str,
        step_text: str,
    ) -> tuple[ResolvedDefinition, ...]:
        engine = self.find_gherkin_engine(document.uri)
        if engine is None:
            return ()

        result = await self.runner.execute_operation(
            ResolveDefinitionOperation(
                engine_name=engine.name,
                test_path=str(uri_to_path(document.uri)),
                step_type=step_type,
                step_text=step_text,
            ),
        )
        if result.definitions:
            return result.definitions

        knowledge_result = await self.runner.execute_operation(
            QueryDefinitionsOperation(
                query=DefinitionKnowledgeQuery(
                    engine_name=engine.name,
                    step_type=step_type,
                    step_text=step_text,
                    include_invalidated=False,
                ),
            ),
        )
        return build_resolved_definitions_from_knowledge(
            definitions=knowledge_result.definitions,
            engine_name=engine.name,
            step_type=step_type,
            step_text=step_text,
        )

    async def suggest_gherkin_step_completions(
        self,
        *,
        document: TextDocument,
        step_type: str,
        initial_text: str,
        cursor_column: int,
        start_step_text_column: int,
    ) -> tuple[object, ...]:
        engine = self.find_gherkin_engine(document.uri)
        if engine is None:
            return ()

        knowledge_result = await self.runner.execute_operation(
            QueryDefinitionsOperation(
                query=DefinitionKnowledgeQuery(
                    engine_name=engine.name,
                    step_type=step_type,
                    include_invalidated=False,
                ),
            ),
        )
        contribution = _get_gherkin_lsp_contribution()
        if contribution is None:
            return ()
        return contribution.build_step_completion_suggestions_from_knowledge(
            definitions=knowledge_result.definitions,
            step_type=step_type,
            initial_text=initial_text,
        )


def build_resolved_definitions_from_knowledge(
    *,
    definitions: tuple[DefinitionKnowledge, ...],
    engine_name: str,
    step_type: str,
    step_text: str,
) -> tuple[ResolvedDefinition, ...]:
    matches: list[ResolvedDefinition] = []
    seen_definitions: set[tuple[str, int, str]] = set()
    for definition in definitions:
        for descriptor in definition.matching_descriptors(
            step_type=step_type,
            step_text=step_text,
        ):
            payload = get_gherkin_payload(descriptor)
            if payload is None:
                continue
            key = (
                definition.file_path,
                descriptor.source_line,
                descriptor.function_name,
            )
            if key in seen_definitions:
                continue

            seen_definitions.add(key)
            matches.append(
                ResolvedDefinition(
                    engine_name=engine_name,
                    file_path=definition.file_path,
                    line=descriptor.source_line,
                    step_type=payload.step_type,
                    patterns=payload.patterns,
                    function_name=descriptor.function_name,
                    category=descriptor.category,
                    documentation=descriptor.documentation,
                    resolution_source='static_catalog',
                ),
            )

    return tuple(matches)


def build_completion_items_from_suggestions(
    suggestions: tuple[CompletionSuggestion, ...],
) -> list[CompletionItem]:
    return [
        CompletionItem(
            label=suggestion.label,
            kind=(
                CompletionItemKind.Snippet
                if suggestion.kind == 'snippet'
                else CompletionItemKind.Text
            ),
            insert_text=suggestion.insert_text,
            insert_text_format=(
                InsertTextFormat.Snippet
                if suggestion.kind == 'snippet'
                else None
            ),
            detail=suggestion.detail,
            documentation=(
                None
                if suggestion.documentation is None
                else MarkupContent(
                    kind=MarkupKind.Markdown,
                    value=suggestion.documentation,
                )
            ),
            sort_text=suggestion.sort_text,
        )
        for suggestion in suggestions
    ]


server = CosechaLanguageServer(SERVER_NAME, 'v0.1')

#
#
# Common lsp features
#
#


@server.feature(TEXT_DOCUMENT_DID_OPEN)
async def feature_did_open(params: DidOpenTextDocumentParams):
    uri = params.text_document.uri
    # Obtener el Document a partir del URI
    document = server.workspace.get_text_document(uri)

    if document.uri.endswith('.feature'):
        diagnostic_list = await server.validate_gherkin_document(
            document,
        )
        server.publish_diagnostics(uri, diagnostic_list)


@server.feature(TEXT_DOCUMENT_DID_SAVE)
async def feature_did_save(params: DidSaveTextDocumentParams):
    uri = params.text_document.uri
    # Obtener el Document a partir del URI
    document = server.workspace.get_text_document(uri)

    if document.uri.endswith('.feature'):
        diagnostic_list = await server.validate_gherkin_document(
            document,
        )
        server.publish_diagnostics(uri, diagnostic_list)


@server.feature(
    TEXT_DOCUMENT_COMPLETION,
    CompletionOptions(trigger_characters=[' ']),
)
async def feature_completions(params: CompletionParams):
    document = server.workspace.get_text_document(params.text_document.uri)

    items: list[CompletionItem] = []

    if document.uri.endswith('.feature'):
        engine = server.find_gherkin_engine(document.uri)
        if engine:
            step_match_line = get_gherkin_step_info(
                params.position.line,
                document.lines,
            )

            if step_match_line:
                suggestions = await server.suggest_gherkin_step_completions(
                    document=document,
                    step_type=step_match_line.step_type,
                    initial_text=step_match_line.step_text,
                    cursor_column=params.position.character,
                    start_step_text_column=(
                        step_match_line.start_step_text_line
                    ),
                )
                items.extend(build_completion_items_from_suggestions(suggestions))

    return CompletionList(is_incomplete=False, items=items)


@server.feature(TEXT_DOCUMENT_FORMATTING)
def feature_document_formatting(
    params: DocumentFormattingParams,
) -> list[TextEdit]:
    document = server.workspace.get_text_document(params.text_document.uri)

    if document.uri.endswith('.feature'):
        plain_document = PlainTextDocument(
            uri=document.uri,
            source=document.source,
            version=document.version,
        )
        formatting_edits = (
            server.gherkin_edit_provider.provide_document_formatting_edits(
                plain_document,
            )
        )
        return [
            _build_lsp_text_edit(edit)
            for edit in formatting_edits
        ]

    return []


#
#
# Gherkin lsp features
#
#


@server.feature(TEXT_DOCUMENT_DEFINITION)
async def feature_open_gherkin_step_definitions(
    params: DefinitionParams,
) -> Location | list[Location] | None:
    document = server.workspace.get_text_document(params.text_document.uri)

    step_match_line = get_gherkin_step_info(
        params.position.line,
        document.lines,
    )

    if step_match_line:
        definitions = await server.resolve_gherkin_step_definitions(
            document=document,
            step_type=step_match_line.step_type,
            step_text=step_match_line.step_text,
        )
        if definitions:
            locations = build_locations_from_resolved_definitions(
                definitions,
            )
            if len(locations) == 1:
                return locations[0]

            return locations

    return None


@server.feature(TEXT_DOCUMENT_HOVER)
async def feature_hover(params: HoverParams) -> Hover | None:
    document = server.workspace.get_text_document(params.text_document.uri)

    if not document.uri.endswith('.feature'):
        return None

    step_info = get_gherkin_step_info(params.position.line, document.lines)
    if not step_info:
        return None

    definitions = await server.resolve_gherkin_step_definitions(
        document=document,
        step_type=step_info.step_type,
        step_text=step_info.step_text,
    )
    if not definitions:
        return None

    return build_hover_from_resolved_definition(definitions[0])


#
#
# Common Commands
#
#


@server.command('getTemplates')
async def command_get_templates(params: ExecuteCommandParams):
    del params
    contribution = _get_gherkin_lsp_contribution()
    if contribution is None:
        return []
    return list(contribution.templates())


@server.command('gherkinCreateDataTable')
async def command_create_gherkin_table(params) -> str:
    try:
        rows, columns = params
        rows, columns = int(rows), int(columns)
    except (ValueError, TypeError):
        return (
            'Invalid arguments. Please provide rows and columns as integers.'
        )

    contribution = _get_gherkin_lsp_contribution()
    if contribution is None:
        return ''
    return contribution.generate_data_table(rows, columns)


def _get_gherkin_lsp_contribution():
    for contribution in iter_shell_lsp_contributions():
        if contribution.contribution_name == 'gherkin':
            return contribution
    return None


def _is_gherkin_engine(engine: object) -> bool:
    return bool(
        getattr(engine, 'name', None) == 'gherkin'
        or hasattr(engine, 'suggest_step_completions'),
    )


#
#
# Common Utils
#
#


def uri_to_path(uri: str) -> Path:
    # Analizar el URI
    parsed_uri = urlparse(uri)

    # Decodificar porcentajes (por ejemplo, espacios codificados como %20)
    path = unquote(parsed_uri.path)

    return Path(path)


def _build_lsp_text_edit(edit: DocumentTextEdit) -> TextEdit:
    return TextEdit(
        range=Range(
            start=Position(
                line=edit.range.start.line,
                character=edit.range.start.character,
            ),
            end=Position(
                line=edit.range.end.line,
                character=edit.range.end.character,
            ),
        ),
        new_text=edit.new_text,
    )


def build_diagnostics_from_draft_validation(issues) -> list[Diagnostic]:
    diagnostics: list[Diagnostic] = []
    for issue in issues:
        line = issue.line or 0
        column = issue.column or 0
        severity = (
            DiagnosticSeverity.Error
            if issue.severity == 'error'
            else DiagnosticSeverity.Warning
        )
        diagnostics.append(
            Diagnostic(
                range=Range(
                    start=Position(line=line, character=column),
                    end=Position(line=line, character=column),
                ),
                message=issue.message,
                severity=severity,
                source=SERVER_NAME,
            ),
        )

    return diagnostics


def build_locations_from_resolved_definitions(
    definitions: tuple[ResolvedDefinition, ...],
) -> list[Location]:
    locations: list[Location] = []
    for definition in definitions:
        character = definition.column or 0
        if character:
            character -= 1

        position = Position(
            line=definition.line - 1,
            character=character,
        )
        locations.append(
            Location(
                uri=Path(definition.file_path).resolve().as_uri(),
                range=Range(start=position, end=position),
            ),
        )

    return locations


def build_hover_from_resolved_definition(
    definition: ResolvedDefinition,
) -> Hover:
    documentation = definition.documentation or 'No documentation available.'
    patterns = '\n'.join(f'* `{pattern}`' for pattern in definition.patterns)
    content = (
        f'**{definition.step_type.capitalize()}**\n\n'
        f'{documentation}\n\n'
        '---\n'
        f'**Patterns:**\n{patterns}'
    )
    return Hover(
        contents=MarkupContent(
            kind=MarkupKind.Markdown,
            value=content,
        ),
    )


class StepMatchLine:
    def __init__(
        self,
        step_type: StepType,
        keyword: str,
        step_text: str,
        start_line: int,
        start_step_text_line: int,
    ):
        self.step_type: StepType = step_type
        self.keyword = keyword
        self.step_text = step_text
        self.start_line = start_line
        self.start_step_text_line = start_step_text_line


def get_gherkin_step_info(  # noqa: PLR0912
    current_line_number: int,
    lines: list[str],
) -> StepMatchLine | None:
    current_line = lines[current_line_number]

    step_type: StepType | None = None

    first_word, *remaining_words = (
        current_line.lstrip().rstrip('\n').split(' ')
    )

    if not first_word:
        return None

    step_text = ' '.join(remaining_words)

    match first_word:
        case 'Given':
            step_type = 'given'
        case 'When':
            step_type = 'when'
        case 'Then':
            step_type = 'then'
        case 'But':
            step_type = 'but'
        case 'And':
            line_number = current_line_number - 1
            while line_number >= 0:
                prev_line = lines[line_number].strip(' \n')
                if not prev_line or prev_line.startswith(('#', '|')):
                    line_number -= 1
                    continue

                prev_first_word = prev_line.split(' ')[0]
                match prev_first_word:
                    case 'Given':
                        step_type = 'given'
                        break
                    case 'When':
                        step_type = 'when'
                        break
                    case 'Then':
                        step_type = 'then'
                        break
                    case 'But':
                        step_type = 'but'
                        break
                    case 'And':
                        line_number -= 1
                        continue
                    case _:
                        break
            else:
                return None
        case _:
            return None

    if not step_type:
        return None

    start_line = current_line.find(first_word)
    start_step_text_line = current_line.find(step_text)

    return StepMatchLine(
        step_type,
        first_word,
        step_text,
        start_line,
        start_step_text_line,
    )


def main() -> None:
    logger.info(f'cwd: {Path.cwd()}')

    registry = create_loaded_discovery_registry()
    with using_discovery_registry(registry):
        workspace = resolve_workspace()
        config = Config(
            workspace.knowledge_anchor,
            capture_log=False,
            workspace=workspace,
            execution_context=build_execution_context(workspace),
        )
        hooks, engines = setup_engines(config)
        logger.info(f'Config root path: {config.root_path}')

        server.loop.run_until_complete(server.start(config, hooks, engines))


def resolve_workspace_root_path(start_path: Path | None = None) -> Path:
    return resolve_workspace(start_path=start_path).knowledge_anchor


if __name__ == '__main__':
    main()
