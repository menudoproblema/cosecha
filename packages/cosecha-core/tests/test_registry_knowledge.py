from __future__ import annotations

from cosecha.core.registry_knowledge import (
    REGISTRY_KNOWLEDGE_LOADER_SCHEMA_VERSION,
    RegistryKnowledgeEntry,
    RegistryKnowledgeQuery,
    RegistryKnowledgeSnapshot,
)


def test_registry_knowledge_contract_roundtrips() -> None:
    entry = RegistryKnowledgeEntry(
        layout_name='tests-root',
        module_import_path='tests.steps.auth',
        qualname='AuthSteps',
        class_name='AuthSteps',
    )
    snapshot = RegistryKnowledgeSnapshot(
        engine_name='gherkin',
        module_spec='tests.steps.auth',
        package_hash='pkg-1',
        layout_key='tests-root',
        loader_schema_version=REGISTRY_KNOWLEDGE_LOADER_SCHEMA_VERSION,
        entries=(entry,),
        source_count=1,
        created_at=123.4,
    )
    query = RegistryKnowledgeQuery(
        engine_name='gherkin',
        module_spec='tests.steps.auth',
        package_hash='pkg-1',
        layout_key='tests-root',
        loader_schema_version=REGISTRY_KNOWLEDGE_LOADER_SCHEMA_VERSION,
        limit=25,
    )

    assert RegistryKnowledgeEntry.from_dict(entry.to_dict()) == entry
    assert RegistryKnowledgeSnapshot.from_dict(snapshot.to_dict()) == snapshot
    assert RegistryKnowledgeQuery.from_dict(query.to_dict()) == query
