from __future__ import annotations

import re

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.definition_knowledge import DefinitionKnowledgeRecord
    from cosecha.core.knowledge_base import DefinitionKnowledge
    from cosecha.engine.gherkin.context import ContextRegistry
    from cosecha.engine.gherkin.steps import StepRegistry
    from cosecha.engine.gherkin.types import StepType

from cosecha.engine.gherkin.definition_knowledge import (
    descriptor_patterns,
    matching_descriptors,
)


type CompletionSuggestionKind = Literal['snippet', 'text']

_PLACEHOLDER_REGEX = re.compile(r'\{([^}]+)\}')


@dataclass(slots=True, frozen=True)
class CompletionSuggestion:
    label: str
    insert_text: str
    kind: CompletionSuggestionKind
    detail: str | None = None
    documentation: str | None = None
    sort_text: str | None = None


@dataclass(slots=True, frozen=True)
class StepCompletionRequest:
    step_type: StepType
    initial_text: str
    cursor_column: int
    start_step_text_column: int


def build_step_completion_suggestions(
    request: StepCompletionRequest,
    *,
    context_registry: ContextRegistry,
    step_registry: StepRegistry,
) -> tuple[CompletionSuggestion, ...]:
    match = step_registry.find_match(
        request.step_type,
        request.initial_text,
    )
    if match and match.arguments:
        suggestions = _build_layout_completion_suggestions(
            context_registry=context_registry,
            match=match,
            cursor_column=request.cursor_column,
            start_step_text_column=request.start_step_text_column,
        )
        if suggestions:
            return suggestions

    return _build_definition_completion_suggestions(
        step_registry=step_registry,
        step_type=request.step_type,
        initial_text=request.initial_text,
    )


def build_step_completion_suggestions_from_knowledge(
    *,
    definitions: tuple[DefinitionKnowledge, ...],
    step_type: StepType,
    initial_text: str,
) -> tuple[CompletionSuggestion, ...]:
    descriptors = tuple(
        descriptor
        for definition in definitions
        for descriptor in matching_descriptors(
            definition.descriptors,
            step_type=step_type,
        )
    )
    return _build_descriptor_completion_suggestions(
        descriptors=descriptors,
        initial_text=initial_text,
    )


def _build_layout_completion_suggestions(
    *,
    context_registry: ContextRegistry,
    match,
    cursor_column: int,
    start_step_text_column: int,
) -> tuple[CompletionSuggestion, ...]:
    suggestions: list[CompletionSuggestion] = []
    for layout_ref in match.step_text.layouts:
        for argument in match.arguments:
            if argument.name != layout_ref.place_holder:
                continue

            start_argument_column = (
                start_step_text_column + argument.start_column
            )
            end_argument_column = start_step_text_column + argument.end_column
            if not (
                start_argument_column <= cursor_column <= end_argument_column
            ):
                continue

            for name, _ in context_registry.get_items(layout_ref.layout):
                suggestions.append(
                    CompletionSuggestion(
                        label=name,
                        insert_text=name,
                        kind='text',
                        sort_text='01',
                    ),
                )

    return tuple(suggestions)


def _build_definition_completion_suggestions(
    *,
    step_registry: StepRegistry,
    step_type: StepType,
    initial_text: str,
) -> tuple[CompletionSuggestion, ...]:
    suggestions: list[CompletionSuggestion] = []
    for step_definition in step_registry.iter_completion_definitions(
        step_type,
    ):
        for step_text in step_definition.step_text_list:
            counter, insert_text = replace_with_placeholders(step_text.text)
            insert_text = insert_text.removeprefix(initial_text)
            rows = step_text.min_table_rows or step_text.required_table_rows
            if rows:
                insert_text += '\n' + generate_gherkin_data_table(
                    rows,
                    3,
                    placeholder_start=counter['value'],
                    padding=4,
                )

            label = (
                step_text.text
                if not step_definition.category
                else f'{step_definition.category}: {step_text.text}'
            )
            suggestions.append(
                CompletionSuggestion(
                    label=label,
                    insert_text=insert_text,
                    kind='snippet',
                    detail=step_definition.func.__doc__,
                    documentation=step_text.text,
                    sort_text=step_definition.category or 'zzz',
                ),
            )

    return tuple(suggestions)


def _build_descriptor_completion_suggestions(
    *,
    descriptors: tuple[DefinitionKnowledgeRecord, ...],
    initial_text: str,
) -> tuple[CompletionSuggestion, ...]:
    suggestions: list[CompletionSuggestion] = []
    seen_suggestions: set[tuple[str, str]] = set()
    for descriptor in descriptors:
        for pattern in descriptor_patterns(descriptor):
            counter, insert_text = replace_with_placeholders(pattern)
            del counter
            insert_text = insert_text.removeprefix(initial_text)
            label = (
                pattern
                if not descriptor.category
                else f'{descriptor.category}: {pattern}'
            )
            key = (label, insert_text)
            if key in seen_suggestions:
                continue

            seen_suggestions.add(key)
            suggestions.append(
                CompletionSuggestion(
                    label=label,
                    insert_text=insert_text,
                    kind='snippet',
                    detail=descriptor.documentation,
                    documentation=pattern,
                    sort_text=descriptor.category or 'zzz',
                ),
            )

    return tuple(suggestions)


def replace_with_placeholders(text: str) -> tuple[dict[str, int], str]:
    counter = {'value': 0}

    def replacement(match: re.Match[str]) -> str:
        counter['value'] += 1
        return f'${{{counter["value"]}:{match.group(1)}}}'

    return counter, _PLACEHOLDER_REGEX.sub(replacement, text)


def generate_gherkin_data_table(
    rows: int,
    columns: int,
    *,
    placeholder_start: int = 0,
    padding: int = 0,
) -> str:
    header = (
        ' ' * padding
        + '| '
        + ' | '.join(
            f'${{{index + placeholder_start + 1}:column}}'
            for index in range(columns)
        )
        + ' |'
    )
    row = (
        ' ' * padding + '| ' + ' | '.join('   ' for _ in range(columns)) + ' |'
    )
    return '\n'.join([header, *(row for _ in range(rows))])
