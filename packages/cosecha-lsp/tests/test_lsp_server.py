from __future__ import annotations

import asyncio

from types import SimpleNamespace

from cosecha.core.capabilities import (
    DraftValidationIssue,
    DraftValidationResult,
)
from cosecha.core.domain_events import (
    TestKnowledgeIndexedEvent as IndexedEvent,
)
from cosecha.core.execution_ir import build_test_path_label
from cosecha.core.knowledge_base import (
    DefinitionKnowledge,
    PersistentKnowledgeBase,
)
from cosecha.core.knowledge_test_descriptor import (
    TestDescriptorKnowledge as DescriptorKnowledge,
)
from cosecha.core.operations import (
    DraftValidationOperationResult,
    KnowledgeQueryContext,
    QueryDefinitionsOperationResult,
    ResolvedDefinition,
    ResolveDefinitionOperationResult,
)
from cosecha.engine.gherkin.completion import CompletionSuggestion
from cosecha.engine.gherkin.definition_knowledge import (
    build_gherkin_definition_record,
)
from cosecha_internal.provider.workspace import CosechaWorkspaceBuilder
from cosecha_internal.testkit import build_config
from cosecha_lsp.lsp_server import (
    CosechaLanguageServer,
    build_completion_items_from_suggestions,
    build_resolved_definitions_from_knowledge,
)


EXPECTED_DIAGNOSTIC_LINE = 2


class _DraftValidatingRunnerStub:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def find_engine(self, _test_file):
        return None

    async def execute_operation(self, operation):
        self.calls.append(operation)
        return DraftValidationOperationResult(
            engine_name='gherkin',
            test_path=str(operation.test_path),
            validation=DraftValidationResult(
                test_count=1,
                issues=(
                    DraftValidationIssue(
                        code='missing_step_definition',
                        message='Step `missing step` not found.',
                        severity='warning',
                        line=2,
                        column=4,
                    ),
                ),
            ),
        )


class _DefinitionResolvingRunnerStub:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def find_engine(self, _test_file):
        return None

    async def execute_operation(self, operation):
        self.calls.append(operation)
        return ResolveDefinitionOperationResult(
            definitions=(
                ResolvedDefinition(
                    engine_name='gherkin',
                    file_path=str(operation.test_path),
                    line=12,
                    step_type=operation.step_type,
                    patterns=(operation.step_text,),
                    function_name='missing_step_definition',
                    documentation='Definition docs.',
                    resolution_source='static_catalog',
                ),
            ),
        )


class _QueryingDefinitionRunnerStub:
    def __init__(self, definition: DefinitionKnowledge) -> None:
        self.query_calls: list[object] = []
        self.definition = definition

    def find_engine(self, _test_file):
        return None

    async def execute_operation(self, operation):
        self.query_calls.append(operation)
        if hasattr(operation, 'step_text'):
            return ResolveDefinitionOperationResult(definitions=())

        return QueryDefinitionsOperationResult(
            definitions=(self.definition,),
            context=KnowledgeQueryContext(
                source='persistent_knowledge_base',
                freshness='unknown',
            ),
        )


def test_lsp_server_opens_read_only_knowledge_base_from_workspace(
    tmp_path,
) -> None:
    workspace = CosechaWorkspaceBuilder(
        tmp_path,
        layout='root',
        with_knowledge_base=True,
    ).build()
    knowledge_base = PersistentKnowledgeBase(workspace.knowledge_base_path)
    knowledge_base.close()
    server = CosechaLanguageServer('cosecha-lsp-test', '0.1')
    server.config = build_config(workspace.root_path)

    read_only_knowledge_base = server._open_read_only_knowledge_base()

    assert read_only_knowledge_base is not None
    assert read_only_knowledge_base.db_path == workspace.knowledge_base_path
    read_only_knowledge_base.close()


def test_lsp_server_falls_back_to_knowledge_base_for_gherkin_engine(
    tmp_path,
) -> None:
    workspace = CosechaWorkspaceBuilder(
        tmp_path,
        layout='root',
        with_knowledge_base=True,
        root_files={'features/demo.feature': 'Feature: Demo\n'},
    ).build()
    persistent_knowledge_base = PersistentKnowledgeBase(
        workspace.knowledge_base_path,
    )
    test_path = workspace.root_path / 'features' / 'demo.feature'
    test_path_label = build_test_path_label(workspace.root_path, test_path)
    persistent_knowledge_base.apply(
        IndexedEvent(
            engine_name='gherkin',
            file_path=test_path_label,
            tests=(
                DescriptorKnowledge(
                    stable_id='gherkin:demo',
                    test_name='Escenario demo',
                    file_path=test_path_label,
                    source_line=1,
                ),
            ),
            discovery_mode='ast',
            knowledge_version='1',
        ),
    )
    persistent_knowledge_base.close()

    gherkin_engine = SimpleNamespace(name='gherkin')
    server = CosechaLanguageServer('cosecha-lsp-test', '0.1')
    server.config = build_config(workspace.root_path)
    server.runner = SimpleNamespace(find_engine=lambda _test_file: None)
    server.engines = {'gherkin': gherkin_engine}
    server.read_only_knowledge_base = server._open_read_only_knowledge_base()

    resolved_engine = server.find_gherkin_engine(test_path.as_uri())

    assert resolved_engine is gherkin_engine
    assert server.read_only_knowledge_base is not None
    server.read_only_knowledge_base.close()


def test_build_resolved_definitions_from_knowledge_dedupes_records() -> None:
    duplicate_record = build_gherkin_definition_record(
        source_line=7,
        function_name='step_demo',
        step_type='given',
        patterns=('a reusable workspace',),
    )
    definition = DefinitionKnowledge(
        engine_name='gherkin',
        file_path='steps/demo.py',
        definition_count=2,
        discovery_mode='ast',
        descriptors=(duplicate_record, duplicate_record),
    )

    resolved = build_resolved_definitions_from_knowledge(
        definitions=(definition,),
        engine_name='gherkin',
        step_type='given',
        step_text='a reusable workspace',
    )

    assert len(resolved) == 1
    assert resolved[0].file_path == 'steps/demo.py'
    assert resolved[0].function_name == 'step_demo'
    assert resolved[0].resolution_source == 'static_catalog'


def test_build_completion_items_from_suggestions_preserves_metadata() -> None:
    items = build_completion_items_from_suggestions(
        (
            CompletionSuggestion(
                label='Given a demo',
                insert_text='a demo ${1:value}',
                kind='snippet',
                detail='demo detail',
                documentation='demo docs',
                sort_text='01',
            ),
            CompletionSuggestion(
                label='plain text',
                insert_text='plain text',
                kind='text',
            ),
        ),
    )

    assert items[0].label == 'Given a demo'
    assert items[0].insert_text == 'a demo ${1:value}'
    assert items[0].detail == 'demo detail'
    assert items[0].documentation.value == 'demo docs'
    assert items[1].label == 'plain text'


def test_language_server_uses_typed_draft_validation_for_diagnostics(
    tmp_path,
) -> None:
    server = CosechaLanguageServer('cosecha-lsp-test', '0.1')
    server.config = build_config(tmp_path)
    server.runner = _DraftValidatingRunnerStub()
    server.engines = {'gherkin': SimpleNamespace(name='gherkin')}
    server.find_gherkin_engine = lambda uri: server.engines['gherkin']  # type: ignore[method-assign]

    document = SimpleNamespace(
        uri=(tmp_path / 'features' / 'example.feature').as_uri(),
        source='\n'.join(
            (
                'Feature: Example',
                '  Scenario: Missing step',
                '    Given missing step',
            ),
        ),
    )

    diagnostics = asyncio.run(server.validate_gherkin_document(document))

    assert len(diagnostics) == 1
    assert diagnostics[0].message == 'Step `missing step` not found.'
    assert diagnostics[0].range.start.line == EXPECTED_DIAGNOSTIC_LINE
    assert server.runner.calls


def test_language_server_uses_typed_definition_resolution(
    tmp_path,
) -> None:
    server = CosechaLanguageServer('cosecha-lsp-test', '0.1')
    server.config = build_config(tmp_path)
    server.runner = _DefinitionResolvingRunnerStub()
    server.engines = {'gherkin': SimpleNamespace(name='gherkin')}
    server.find_gherkin_engine = lambda uri: server.engines['gherkin']  # type: ignore[method-assign]

    document = SimpleNamespace(
        uri=(tmp_path / 'features' / 'example.feature').as_uri(),
    )

    definitions = asyncio.run(
        server.resolve_gherkin_step_definitions(
            document=document,
            step_type='given',
            step_text='missing step',
        ),
    )

    assert len(definitions) == 1
    assert definitions[0].documentation == 'Definition docs.'
    assert server.runner.calls


def test_language_server_falls_back_to_persistent_definition_knowledge(
    tmp_path,
) -> None:
    definition = DefinitionKnowledge(
        engine_name='gherkin',
        file_path='tests/steps/auth.py',
        definition_count=1,
        discovery_mode='ast',
        descriptors=(
            build_gherkin_definition_record(
                source_line=12,
                function_name='missing_step_definition',
                step_type='given',
                patterns=('missing step',),
                documentation='Definition docs.',
            ),
        ),
    )
    server = CosechaLanguageServer('cosecha-lsp-test', '0.1')
    server.config = build_config(tmp_path)
    server.runner = _QueryingDefinitionRunnerStub(definition)
    server.engines = {'gherkin': SimpleNamespace(name='gherkin')}
    server.find_gherkin_engine = lambda uri: server.engines['gherkin']  # type: ignore[method-assign]

    document = SimpleNamespace(
        uri=(tmp_path / 'features' / 'example.feature').as_uri(),
    )

    definitions = asyncio.run(
        server.resolve_gherkin_step_definitions(
            document=document,
            step_type='given',
            step_text='missing step',
        ),
    )

    assert len(definitions) == 1
    assert definitions[0].file_path == 'tests/steps/auth.py'
    assert definitions[0].documentation == 'Definition docs.'
    assert server.runner.query_calls


def test_language_server_builds_completions_from_definition_knowledge(
    tmp_path,
    monkeypatch,
) -> None:
    definition = DefinitionKnowledge(
        engine_name='gherkin',
        file_path='tests/steps/auth.py',
        definition_count=1,
        discovery_mode='ast',
        descriptors=(
            build_gherkin_definition_record(
                source_line=12,
                function_name='missing_step_definition',
                step_type='given',
                patterns=('missing step',),
                documentation='Definition docs.',
            ),
        ),
    )
    server = CosechaLanguageServer('cosecha-lsp-test', '0.1')
    server.config = build_config(tmp_path)
    server.runner = _QueryingDefinitionRunnerStub(definition)
    server.engines = {'gherkin': SimpleNamespace(name='gherkin')}
    server.find_gherkin_engine = lambda uri: server.engines['gherkin']  # type: ignore[method-assign]
    monkeypatch.setattr(
        'cosecha_lsp.lsp_server._get_gherkin_lsp_contribution',
        lambda: SimpleNamespace(
            build_step_completion_suggestions_from_knowledge=(
                lambda **_kwargs: (
                    CompletionSuggestion(
                        label='Given missing step',
                        insert_text='missing step',
                        kind='text',
                        documentation='Definition docs.',
                    ),
                )
            ),
        ),
    )

    document = SimpleNamespace(
        uri=(tmp_path / 'features' / 'example.feature').as_uri(),
    )

    suggestions = asyncio.run(
        server.suggest_gherkin_step_completions(
            document=document,
            step_type='given',
            initial_text='mis',
            cursor_column=5,
            start_step_text_column=4,
        ),
    )

    assert len(suggestions) == 1
    assert suggestions[0].label == 'Given missing step'
