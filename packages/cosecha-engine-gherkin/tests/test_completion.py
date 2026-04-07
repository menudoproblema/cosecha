from __future__ import annotations

from cosecha.core.knowledge_base import DefinitionKnowledge
from cosecha.engine.gherkin.completion import (
    StepCompletionRequest,
    build_step_completion_suggestions_from_knowledge,
    build_step_completion_suggestions,
    generate_gherkin_data_table,
    replace_with_placeholders,
)
from cosecha.engine.gherkin.definition_knowledge import (
    build_gherkin_definition_record,
)
from cosecha.engine.gherkin.context import ContextRegistry
from cosecha.engine.gherkin.steps.definition import LayoutRef, StepDefinition, StepText
from cosecha.engine.gherkin.steps.registry import StepRegistry


def test_completion_suggestions_from_knowledge_filter_by_step_type() -> None:
    definitions = (
        DefinitionKnowledge(
            engine_name='gherkin',
            file_path='steps/demo.py',
            definition_count=2,
            discovery_mode='ast',
            descriptors=(
                build_gherkin_definition_record(
                    source_line=1,
                    function_name='step_given_demo',
                    step_type='given',
                    patterns=('a reusable workspace',),
                    documentation='Given documentation',
                    category='workspace',
                ),
                build_gherkin_definition_record(
                    source_line=2,
                    function_name='step_then_demo',
                    step_type='then',
                    patterns=('a rendered report',),
                ),
            ),
        ),
    )

    suggestions = build_step_completion_suggestions_from_knowledge(
        definitions=definitions,
        step_type='given',
        initial_text='a re',
    )

    assert len(suggestions) == 1
    suggestion = suggestions[0]
    assert suggestion.label == 'workspace: a reusable workspace'
    assert suggestion.kind == 'snippet'
    assert suggestion.insert_text == 'usable workspace'
    assert suggestion.detail == 'Given documentation'


async def _noop_step(_context) -> None:
    del _context


def test_build_step_completion_prefers_layout_items_for_argument_at_cursor(
) -> None:
    step_registry = StepRegistry()
    step_registry.add_step_definition(
        StepDefinition(
            'given',
            (
                StepText(
                    'a user named {name}',
                    layouts=LayoutRef('users', 'name'),
                ),
            ),
            _noop_step,
        ),
    )
    context_registry = ContextRegistry()
    context_registry.add('users', 'alice', object())
    context_registry.add('users', 'bob', object())

    suggestions = build_step_completion_suggestions(
        StepCompletionRequest(
            step_type='given',
            initial_text='a user named alice',
            cursor_column=15,
            start_step_text_column=0,
        ),
        context_registry=context_registry,
        step_registry=step_registry,
    )

    assert tuple(suggestion.label for suggestion in suggestions) == (
        'alice',
        'bob',
    )
    assert all(suggestion.kind == 'text' for suggestion in suggestions)
    assert all(suggestion.sort_text == '01' for suggestion in suggestions)


def test_build_step_completion_falls_back_to_definition_snippets(
) -> None:
    step_registry = StepRegistry()
    step_registry.add_step_definition(
        StepDefinition(
            'given',
            (
                StepText(
                    'I register {name}',
                    min_table_rows=1,
                ),
            ),
            _noop_step,
            category='users',
        ),
    )

    suggestions = build_step_completion_suggestions(
        StepCompletionRequest(
            step_type='given',
            initial_text='I ',
            cursor_column=0,
            start_step_text_column=0,
        ),
        context_registry=ContextRegistry(),
        step_registry=step_registry,
    )

    assert len(suggestions) == 1
    suggestion = suggestions[0]
    assert suggestion.label == 'users: I register {name}'
    assert suggestion.kind == 'snippet'
    assert suggestion.insert_text.startswith('register ${1:name}')
    assert '| ${2:column} | ${3:column} | ${4:column} |' in suggestion.insert_text
    assert suggestion.sort_text == 'users'


def test_build_step_completion_layout_guards_fall_back_to_definitions(
) -> None:
    step_registry = StepRegistry()
    step_registry.add_step_definition(
        StepDefinition(
            'given',
            (
                StepText(
                    'a user named {name}',
                    layouts=LayoutRef('users', 'other'),
                ),
            ),
            _noop_step,
        ),
    )

    mismatched_placeholder_suggestions = build_step_completion_suggestions(
        StepCompletionRequest(
            step_type='given',
            initial_text='a user named alice',
            cursor_column=15,
            start_step_text_column=0,
        ),
        context_registry=ContextRegistry(),
        step_registry=step_registry,
    )
    assert len(mismatched_placeholder_suggestions) == 1
    assert mismatched_placeholder_suggestions[0].kind == 'snippet'

    step_registry = StepRegistry()
    step_registry.add_step_definition(
        StepDefinition(
            'given',
            (
                StepText(
                    'a user named {name}',
                    layouts=LayoutRef('users', 'name'),
                ),
            ),
            _noop_step,
        ),
    )
    out_of_range_cursor_suggestions = build_step_completion_suggestions(
        StepCompletionRequest(
            step_type='given',
            initial_text='a user named alice',
            cursor_column=1,
            start_step_text_column=0,
        ),
        context_registry=ContextRegistry(),
        step_registry=step_registry,
    )
    assert len(out_of_range_cursor_suggestions) == 1
    assert out_of_range_cursor_suggestions[0].kind == 'snippet'


def test_completion_suggestions_from_knowledge_deduplicate_patterns() -> None:
    duplicated_pattern = 'a reusable workspace'
    definitions = (
        DefinitionKnowledge(
            engine_name='gherkin',
            file_path='steps/demo.py',
            definition_count=2,
            discovery_mode='ast',
            descriptors=(
                build_gherkin_definition_record(
                    source_line=1,
                    function_name='step_given_demo',
                    step_type='given',
                    patterns=(duplicated_pattern,),
                    category='workspace',
                ),
                build_gherkin_definition_record(
                    source_line=2,
                    function_name='step_given_demo_2',
                    step_type='given',
                    patterns=(duplicated_pattern,),
                    category='workspace',
                ),
            ),
        ),
    )

    suggestions = build_step_completion_suggestions_from_knowledge(
        definitions=definitions,
        step_type='given',
        initial_text='a ',
    )

    assert len(suggestions) == 1
    assert suggestions[0].label == 'workspace: a reusable workspace'


def test_replace_with_placeholders_and_data_table_generation() -> None:
    counter, replaced = replace_with_placeholders('a {thing} with {value}')

    assert counter == {'value': 2}
    assert replaced == 'a ${1:thing} with ${2:value}'
    assert generate_gherkin_data_table(
        2,
        2,
        placeholder_start=2,
        padding=2,
    ) == '\n'.join(
        (
            '  | ${3:column} | ${4:column} |',
            '  |     |     |',
            '  |     |     |',
        ),
    )
