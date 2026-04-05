from __future__ import annotations

import pytest

from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord
from cosecha.engine.gherkin.definition_knowledge import (
    UnsupportedGherkinPayloadVersionError,
    build_gherkin_definition_record,
    get_gherkin_payload,
)


def test_definition_knowledge_record_requires_positive_payload_version(
) -> None:
    with pytest.raises(
        ValueError,
        match='payload_version must be a positive integer',
    ):
        DefinitionKnowledgeRecord(
            source_line=1,
            function_name='example',
            payload_version=0,
        )


def test_gherkin_payload_rejects_unsupported_versions() -> None:
    record = build_gherkin_definition_record(
        source_line=1,
        function_name='step_impl',
        step_type='given',
        patterns=('a step',),
    )
    incompatible = DefinitionKnowledgeRecord(
        source_line=record.source_line,
        function_name=record.function_name,
        category=record.category,
        provider_kind=record.provider_kind,
        provider_name=record.provider_name,
        runtime_required=record.runtime_required,
        runtime_reason=record.runtime_reason,
        declaration_origin=record.declaration_origin,
        discovery_mode=record.discovery_mode,
        documentation=record.documentation,
        payload_kind=record.payload_kind,
        payload_version=record.payload_version + 1,
        payload=record.payload,
    )

    with pytest.raises(
        UnsupportedGherkinPayloadVersionError,
        match='Unsupported gherkin definition payload version',
    ):
        get_gherkin_payload(incompatible)
