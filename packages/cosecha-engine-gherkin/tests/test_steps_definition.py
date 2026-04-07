from __future__ import annotations

import pytest

from cosecha.engine.gherkin.steps.definition import (
    Argument,
    LayoutRef,
    Match,
    MatchError,
    ParseStepMatcher,
    StepDefinition,
    StepMatcher,
    StepText,
)


async def _noop_step(_context) -> None:
    del _context


def test_argument_and_match_repr_include_debug_information() -> None:
    argument = Argument('name', 'alice', 2, 7)
    step_definition = StepDefinition(
        'given',
        (StepText('a user named {name}'),),
        _noop_step,
    )
    match = Match(step_definition, step_definition.step_text_list[0], (argument,))

    assert repr(argument) == '<Argument name, alice>'
    assert '<Match _noop_step' in repr(match)


def test_parse_step_matcher_validates_fixed_fields_and_parse_none_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FixedParser:
        fixed_fields = ('x',)

    monkeypatch.setattr(
        'cosecha.engine.gherkin.steps.definition.parse.compile',
        lambda _text: _FixedParser(),
    )
    with pytest.raises(NotImplementedError, match='Fixed fields is not supported'):
        ParseStepMatcher((StepText('literal step'),))

    monkeypatch.undo()
    matcher = ParseStepMatcher((StepText('literal step'),))
    assert matcher.parse_arguments('other text') is None


def test_step_text_validates_table_configuration_and_layouts() -> None:
    with pytest.raises(ValueError, match='Incompatible options'):
        StepText(
            'a table',
            min_table_rows=1,
            required_table_rows=1,
        )

    with pytest.raises(ValueError, match='Cannot use table'):
        StepText(
            'a table',
            min_table_rows=1,
            can_use_table=False,
        )

    layout_ref = LayoutRef('users', 'name')
    step_text = StepText(
        'a user named {name}',
        layouts=layout_ref,
    )
    assert step_text.layouts == (layout_ref,)
    assert step_text.literal_prefix == 'a user named '
    assert step_text.literal_fragments == ('a user named',)


def test_step_definition_match_error_repr_str_eq_and_hash() -> None:
    class _ExplodingParser(StepMatcher):
        def parse_arguments(self, text: str):
            del text
            raise RuntimeError('boom')

    step_definition = StepDefinition(
        'given',
        (StepText('a step'),),
        _noop_step,
    )
    exploding_definition = StepDefinition(
        'when',
        (StepText('a failing step'),),
        _noop_step,
        parser_cls=_ExplodingParser,
    )

    with pytest.raises(MatchError):
        exploding_definition.match('a failing step')

    assert step_definition.match('non matching') is None
    assert 'StepDefinition' in repr(step_definition)
    assert '@given("' in str(step_definition)
    assert step_definition == step_definition
    assert step_definition != object()
    assert hash(step_definition) == hash(step_definition)


def test_step_definition_match_returns_argument_spans() -> None:
    step_definition = StepDefinition(
        'given',
        (StepText('a user named {name}'),),
        _noop_step,
    )

    match = step_definition.match('a user named alice')

    assert match is not None
    assert len(match.arguments) == 1
    assert match.arguments[0].name == 'name'
    assert match.arguments[0].start_column < match.arguments[0].end_column
    first_location = step_definition.location
    assert step_definition.location is first_location
