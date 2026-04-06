from __future__ import annotations

from types import SimpleNamespace

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
