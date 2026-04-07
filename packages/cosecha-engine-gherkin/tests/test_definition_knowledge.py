from __future__ import annotations

import pytest

from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord
from cosecha.engine.gherkin.definition_knowledge import (
    GHERKIN_STEP_PAYLOAD_KIND,
    GHERKIN_STEP_PAYLOAD_VERSION,
    UnsupportedGherkinPayloadVersionError,
    build_gherkin_definition_record,
    descriptor_matches_step,
    descriptor_may_match_by_structure,
    descriptor_patterns,
    descriptor_supports_text_matching,
    get_gherkin_payload,
    matching_descriptors,
)


def _build_given_descriptor(
    *,
    parser_cls_name: str | None = None,
) -> DefinitionKnowledgeRecord:
    return build_gherkin_definition_record(
        source_line=10,
        function_name='step_given_user',
        step_type='given',
        patterns=('a user named {name}',),
        literal_prefixes=('a user',),
        literal_suffixes=(),
        literal_fragments=('user', 'named'),
        anchor_tokens=('user', 'named'),
        dynamic_fragment_count=1,
        parser_cls_name=parser_cls_name,
    )


def test_get_gherkin_payload_roundtrip_and_non_gherkin_payload_handling() -> None:
    descriptor = _build_given_descriptor()
    payload = get_gherkin_payload(descriptor)
    assert payload is not None
    assert payload.step_type == 'given'
    assert payload.patterns == ('a user named {name}',)
    assert payload.anchor_tokens == ('user', 'named')

    generic_descriptor = DefinitionKnowledgeRecord(
        source_line=1,
        function_name='other',
        payload_kind='other',
    )
    assert get_gherkin_payload(generic_descriptor) is None


def test_get_gherkin_payload_rejects_unsupported_payload_versions() -> None:
    descriptor = DefinitionKnowledgeRecord(
        source_line=1,
        function_name='invalid',
        payload_kind=GHERKIN_STEP_PAYLOAD_KIND,
        payload_version=GHERKIN_STEP_PAYLOAD_VERSION + 1,
        payload={'step_type': 'given', 'patterns': ('x',)},
    )

    with pytest.raises(
        UnsupportedGherkinPayloadVersionError,
        match='Unsupported gherkin definition payload version',
    ):
        get_gherkin_payload(descriptor)


def test_descriptor_supports_text_matching_and_patterns() -> None:
    parse_descriptor = _build_given_descriptor(parser_cls_name='ParseStepMatcher')
    custom_descriptor = _build_given_descriptor(parser_cls_name='CustomParser')
    generic_descriptor = DefinitionKnowledgeRecord(
        source_line=1,
        function_name='other',
        payload_kind='other',
    )

    assert descriptor_supports_text_matching(parse_descriptor) is True
    assert descriptor_supports_text_matching(custom_descriptor) is False
    assert descriptor_supports_text_matching(generic_descriptor) is False
    assert descriptor_patterns(parse_descriptor) == ('a user named {name}',)
    assert descriptor_patterns(generic_descriptor) == ()


def test_descriptor_structure_matching_and_full_matching_paths() -> None:
    descriptor = _build_given_descriptor()
    custom_parser_descriptor = _build_given_descriptor(
        parser_cls_name='CustomParser',
    )
    generic_descriptor = DefinitionKnowledgeRecord(
        source_line=1,
        function_name='other',
        payload_kind='other',
    )

    assert descriptor_may_match_by_structure(descriptor, step_type='then') is False
    assert descriptor_may_match_by_structure(descriptor, step_type='given') is True
    assert descriptor_may_match_by_structure(generic_descriptor) is False
    assert (
        descriptor_may_match_by_structure(
            descriptor,
            step_type='given',
            step_text='a user named jane',
        )
        is True
    )
    assert (
        descriptor_may_match_by_structure(
            descriptor,
            step_type='given',
            step_text='another sentence',
        )
        is False
    )
    assert descriptor_matches_step(
        descriptor,
        step_type='given',
        step_text='a user named jane',
    )
    assert not descriptor_matches_step(
        descriptor,
        step_type='given',
        step_text='a user',
    )
    assert descriptor_matches_step(
        descriptor,
        step_type='given',
        step_text=None,
    )
    assert not descriptor_matches_step(
        custom_parser_descriptor,
        step_type='given',
        step_text='a user named jane',
    )
    assert not descriptor_matches_step(generic_descriptor, step_type='given')


def test_matching_descriptors_returns_filtered_or_original_sequence() -> None:
    given_descriptor = _build_given_descriptor()
    then_descriptor = build_gherkin_definition_record(
        source_line=20,
        function_name='step_then_result',
        step_type='then',
        patterns=('result is {value}',),
    )
    descriptors = (given_descriptor, then_descriptor)

    assert matching_descriptors(descriptors) == descriptors
    assert matching_descriptors(
        descriptors,
        step_type='then',
    ) == (then_descriptor,)
    assert matching_descriptors(
        descriptors,
        step_type='given',
        step_text='a user named jane',
    ) == (given_descriptor,)
