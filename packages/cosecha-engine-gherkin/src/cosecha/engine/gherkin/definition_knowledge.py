from __future__ import annotations

from dataclasses import dataclass

import parse

from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord


GHERKIN_STEP_PAYLOAD_KIND = 'gherkin.step'
GHERKIN_STEP_PAYLOAD_VERSION = 1


class UnsupportedGherkinPayloadVersionError(ValueError):
    pass


@dataclass(slots=True, frozen=True)
class GherkinStepDefinitionKnowledgePayload:
    step_type: str
    patterns: tuple[str, ...]
    literal_prefixes: tuple[str, ...] = ()
    literal_suffixes: tuple[str, ...] = ()
    literal_fragments: tuple[str, ...] = ()
    anchor_tokens: tuple[str, ...] = ()
    dynamic_fragment_count: int = 0
    parser_cls_name: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'anchor_tokens': self.anchor_tokens,
            'dynamic_fragment_count': self.dynamic_fragment_count,
            'literal_fragments': self.literal_fragments,
            'literal_prefixes': self.literal_prefixes,
            'literal_suffixes': self.literal_suffixes,
            'parser_cls_name': self.parser_cls_name,
            'patterns': self.patterns,
            'step_type': self.step_type,
        }

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, object],
    ) -> GherkinStepDefinitionKnowledgePayload:
        patterns = payload.get('patterns', ())
        literal_fragments = payload.get('literal_fragments', ())
        return cls(
            step_type=str(payload.get('step_type', 'step')),
            patterns=tuple(str(pattern) for pattern in patterns),
            literal_prefixes=tuple(
                str(prefix) for prefix in payload.get('literal_prefixes', ())
            ),
            literal_suffixes=tuple(
                str(suffix) for suffix in payload.get('literal_suffixes', ())
            ),
            literal_fragments=tuple(
                str(fragment) for fragment in literal_fragments
            ),
            anchor_tokens=tuple(
                str(token) for token in payload.get('anchor_tokens', ())
            ),
            dynamic_fragment_count=int(
                payload.get('dynamic_fragment_count', 0),
            ),
            parser_cls_name=(
                None
                if payload.get('parser_cls_name') is None
                else str(payload.get('parser_cls_name'))
            ),
        )


def build_gherkin_definition_record(  # noqa: PLR0913
    *,
    source_line: int,
    function_name: str,
    step_type: str,
    patterns: tuple[str, ...],
    literal_prefixes: tuple[str, ...] = (),
    literal_suffixes: tuple[str, ...] = (),
    literal_fragments: tuple[str, ...] = (),
    anchor_tokens: tuple[str, ...] = (),
    dynamic_fragment_count: int = 0,
    documentation: str | None = None,
    parser_cls_name: str | None = None,
    category: str | None = None,
    provider_kind: str | None = None,
    provider_name: str | None = None,
    runtime_required: bool = False,
    runtime_reason: str | None = None,
    declaration_origin: str | None = None,
    discovery_mode: str = 'ast',
) -> DefinitionKnowledgeRecord:
    payload = GherkinStepDefinitionKnowledgePayload(
        step_type=step_type,
        patterns=patterns,
        literal_prefixes=literal_prefixes,
        literal_suffixes=literal_suffixes,
        literal_fragments=literal_fragments,
        anchor_tokens=anchor_tokens,
        dynamic_fragment_count=dynamic_fragment_count,
        parser_cls_name=parser_cls_name,
    )
    return DefinitionKnowledgeRecord(
        source_line=source_line,
        function_name=function_name,
        category=category,
        provider_kind=provider_kind,
        provider_name=provider_name,
        runtime_required=runtime_required,
        runtime_reason=runtime_reason,
        declaration_origin=declaration_origin,
        discovery_mode=discovery_mode,
        documentation=documentation,
        payload_kind=GHERKIN_STEP_PAYLOAD_KIND,
        payload_version=GHERKIN_STEP_PAYLOAD_VERSION,
        payload=payload.to_dict(),
    )


def get_gherkin_payload(
    descriptor: DefinitionKnowledgeRecord,
) -> GherkinStepDefinitionKnowledgePayload | None:
    if descriptor.payload_kind != GHERKIN_STEP_PAYLOAD_KIND:
        return None
    if descriptor.payload_version != GHERKIN_STEP_PAYLOAD_VERSION:
        msg = (
            'Unsupported gherkin definition payload version: '
            f'{descriptor.payload_version} '
            f'(expected {GHERKIN_STEP_PAYLOAD_VERSION})'
        )
        raise UnsupportedGherkinPayloadVersionError(msg)
    return GherkinStepDefinitionKnowledgePayload.from_dict(descriptor.payload)


def descriptor_supports_text_matching(
    descriptor: DefinitionKnowledgeRecord,
) -> bool:
    payload = get_gherkin_payload(descriptor)
    return payload is not None and payload.parser_cls_name in (
        None,
        'ParseStepMatcher',
    )


def descriptor_may_match_by_structure(
    descriptor: DefinitionKnowledgeRecord,
    *,
    step_type: str | None = None,
    step_text: str | None = None,
) -> bool:
    payload = get_gherkin_payload(descriptor)
    if payload is None:
        return False

    step_type_matches = step_type is None or payload.step_type in (
        step_type,
        'step',
    )
    if not step_type_matches:
        return False
    if step_text is None:
        return True

    normalized_step_text = step_text.lower()
    normalized_step_tokens = set(_tokenize_step_text(step_text))
    return (
        _matches_literal_edges(payload, step_text)
        and _matches_literal_fragments(payload, normalized_step_text)
        and _matches_anchor_tokens(payload, normalized_step_tokens)
    )


def descriptor_matches_step(
    descriptor: DefinitionKnowledgeRecord,
    *,
    step_type: str | None = None,
    step_text: str | None = None,
) -> bool:
    payload = get_gherkin_payload(descriptor)
    if payload is None:
        return False
    if not descriptor_may_match_by_structure(
        descriptor,
        step_type=step_type,
        step_text=step_text,
    ):
        return False
    if step_text is None:
        return True
    if not descriptor_supports_text_matching(descriptor):
        return False

    return any(
        parse.compile(pattern).parse(step_text) is not None
        for pattern in payload.patterns
    )


def matching_descriptors(
    descriptors: tuple[DefinitionKnowledgeRecord, ...],
    *,
    step_type: str | None = None,
    step_text: str | None = None,
) -> tuple[DefinitionKnowledgeRecord, ...]:
    if step_type is None and step_text is None:
        return descriptors

    return tuple(
        descriptor
        for descriptor in descriptors
        if descriptor_matches_step(
            descriptor,
            step_type=step_type,
            step_text=step_text,
        )
    )


def descriptor_patterns(
    descriptor: DefinitionKnowledgeRecord,
) -> tuple[str, ...]:
    payload = get_gherkin_payload(descriptor)
    if payload is None:
        return ()
    return payload.patterns


def _tokenize_step_text(step_text: str) -> tuple[str, ...]:
    return tuple(
        token
        for token in (chunk.strip().lower() for chunk in step_text.split())
        if token
    )


def _matches_literal_edges(
    payload: GherkinStepDefinitionKnowledgePayload,
    step_text: str,
) -> bool:
    non_empty_prefixes = tuple(
        prefix for prefix in payload.literal_prefixes if prefix
    )
    if non_empty_prefixes and not any(
        step_text.startswith(prefix) for prefix in non_empty_prefixes
    ):
        return False

    non_empty_suffixes = tuple(
        suffix for suffix in payload.literal_suffixes if suffix
    )
    return not non_empty_suffixes or any(
        step_text.endswith(suffix) for suffix in non_empty_suffixes
    )


def _matches_literal_fragments(
    payload: GherkinStepDefinitionKnowledgePayload,
    normalized_step_text: str,
) -> bool:
    return not payload.literal_fragments or all(
        fragment in normalized_step_text
        for fragment in payload.literal_fragments
    )


def _matches_anchor_tokens(
    payload: GherkinStepDefinitionKnowledgePayload,
    normalized_step_tokens: set[str],
) -> bool:
    return not payload.anchor_tokens or all(
        token in normalized_step_tokens for token in payload.anchor_tokens
    )
