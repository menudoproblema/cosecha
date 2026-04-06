from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord
from cosecha.core.domain_event_stream import DomainEventStream
from cosecha.core.domain_events import (
    DomainEventMetadata,
    KnowledgeIndexedEvent,
    SessionFinishedEvent,
    SessionStartedEvent,
    TestFinishedEvent as FinishedEvent,
    TestKnowledgeIndexedEvent as IndexedEvent,
)
from cosecha.core.execution_ir import build_test_path_label
from cosecha.core.knowledge_base import (
    KNOWLEDGE_BASE_PATH,
    LEGACY_KNOWLEDGE_BASE_PATH,
    DefinitionKnowledgeQuery,
    KnowledgeBaseDomainEventSink,
    PersistentKnowledgeBase,
    ReadOnlyPersistentKnowledgeBase,
    TestKnowledgeQuery,
    iter_knowledge_base_file_paths,
    resolve_knowledge_base_path,
)
from cosecha.core.knowledge_test_descriptor import (
    TestDescriptorKnowledge as DescriptorKnowledge,
)


if TYPE_CHECKING:
    from pathlib import Path


def _build_metadata(correlation_id: str) -> DomainEventMetadata:
    return DomainEventMetadata(
        correlation_id=correlation_id,
        session_id='session-1',
        trace_id='trace-1',
    )


def test_resolve_knowledge_base_path_migrates_legacy_database_files(
    tmp_path: Path,
) -> None:
    legacy_path = tmp_path / LEGACY_KNOWLEDGE_BASE_PATH
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    for file_path in iter_knowledge_base_file_paths(legacy_path):
        file_path.write_text('legacy', encoding='utf-8')

    resolved_path = resolve_knowledge_base_path(tmp_path)

    assert resolved_path == tmp_path / KNOWLEDGE_BASE_PATH
    for file_path in iter_knowledge_base_file_paths(resolved_path):
        assert file_path.exists()
    for file_path in iter_knowledge_base_file_paths(legacy_path):
        assert not file_path.exists()


def test_knowledge_base_domain_event_sink_tracks_session_tests_and_definitions(
    tmp_path: Path,
) -> None:
    knowledge_base = PersistentKnowledgeBase(tmp_path / '.cosecha' / 'kb.db')
    stream = DomainEventStream()
    stream.add_sink(KnowledgeBaseDomainEventSink(knowledge_base))
    test_path = 'tests/demo.feature'

    async def _emit() -> None:
        await stream.emit(
            SessionStartedEvent(
                root_path=str(tmp_path),
                concurrency=1,
                metadata=_build_metadata('session-started'),
            ),
        )
        await stream.emit(
            IndexedEvent(
                engine_name='gherkin',
                file_path=test_path,
                tests=(
                    DescriptorKnowledge(
                        stable_id='gherkin:demo',
                        test_name='Escenario demo',
                        file_path=test_path,
                        source_line=3,
                    ),
                ),
                discovery_mode='ast',
                knowledge_version='1',
                metadata=_build_metadata('test-knowledge'),
            ),
        )
        await stream.emit(
            KnowledgeIndexedEvent(
                engine_name='gherkin',
                file_path='steps/demo.py',
                definition_count=1,
                discovery_mode='ast',
                knowledge_version='1',
                descriptors=(
                    DefinitionKnowledgeRecord(
                        source_line=10,
                        function_name='step_demo',
                    ),
                ),
                metadata=_build_metadata('definition-knowledge'),
            ),
        )
        await stream.emit(
            FinishedEvent(
                node_id='gherkin:demo:0',
                node_stable_id='gherkin:demo',
                engine_name='gherkin',
                test_name='Escenario demo',
                test_path=test_path,
                status='passed',
                duration=0.2,
                metadata=_build_metadata('test-finished'),
            ),
        )
        await stream.emit(
            SessionFinishedEvent(
                has_failures=False,
                metadata=_build_metadata('session-finished'),
            ),
        )
        await stream.close()

    asyncio.run(_emit())
    snapshot = knowledge_base.snapshot()

    assert snapshot.session is not None
    assert snapshot.session.has_failures is False
    assert snapshot.tests[0].status == 'passed'
    assert snapshot.tests[0].trace_id == 'trace-1'
    assert snapshot.definitions[0].file_path == 'steps/demo.py'
    assert snapshot.definitions[0].descriptors[0].function_name == 'step_demo'
    knowledge_base.close()


def test_persistent_knowledge_base_roundtrips_to_read_only_queries(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / '.cosecha' / 'kb.db'
    knowledge_base = PersistentKnowledgeBase(db_path)
    test_path = build_test_path_label(
        tmp_path,
        tmp_path / 'tests' / 'demo.py',
    )
    knowledge_base.apply(
        IndexedEvent(
            engine_name='pytest',
            file_path=test_path,
            tests=(
                DescriptorKnowledge(
                    stable_id='pytest:demo',
                    test_name='test_demo',
                    file_path=test_path,
                    source_line=4,
                    selection_labels=('api',),
                ),
            ),
            discovery_mode='ast',
            knowledge_version='1',
        ),
    )
    knowledge_base.apply(
        KnowledgeIndexedEvent(
            engine_name='pytest',
            file_path='tests/conftest.py',
            definition_count=1,
            discovery_mode='ast',
            knowledge_version='1',
            descriptors=(
                DefinitionKnowledgeRecord(
                    source_line=2,
                    function_name='shared_fixture',
                ),
            ),
        ),
    )
    knowledge_base.close()

    read_only = ReadOnlyPersistentKnowledgeBase(db_path)
    tests = read_only.query_tests(
        TestKnowledgeQuery(engine_name='pytest', test_path=test_path),
    )
    definitions = read_only.query_definitions(
        DefinitionKnowledgeQuery(engine_name='pytest'),
    )

    assert tests[0].test_name == 'test_demo'
    assert tests[0].selection_labels == ('api',)
    assert definitions[0].descriptors[0].function_name == 'shared_fixture'
    read_only.close()
