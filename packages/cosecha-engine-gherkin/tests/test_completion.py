from __future__ import annotations

from cosecha.core.knowledge_base import DefinitionKnowledge
from cosecha.engine.gherkin.completion import (
    build_step_completion_suggestions_from_knowledge,
)
from cosecha.engine.gherkin.definition_knowledge import (
    build_gherkin_definition_record,
)


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
