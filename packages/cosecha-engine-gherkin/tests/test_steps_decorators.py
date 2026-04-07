from __future__ import annotations

import pytest

from cosecha.engine.gherkin import steps as steps_module
from cosecha.engine.gherkin.steps import but, given, step, then, when
from cosecha.engine.gherkin.steps.definition import StepMatcher, StepText


class _NoopParser(StepMatcher):
    def parse_arguments(self, text: str):
        del text
        return None


@pytest.mark.parametrize(
    ('decorator_factory', 'step_type'),
    (
        (step, 'step'),
        (given, 'given'),
        (when, 'when'),
        (then, 'then'),
        (but, 'but'),
    ),
)
def test_decorators_attach_step_definition_with_expected_type(
    decorator_factory,
    step_type: str,
) -> None:
    async def _step_fn(_context) -> None:
        del _context

    decorated = decorator_factory(
        'a plain step',
        parser_cls=_NoopParser,
        category='category',
    )(_step_fn)
    step_definition = decorated.__step_definition__

    assert step_definition.step_type == step_type
    assert step_definition.category == 'category'
    assert step_definition.parser.__class__ is _NoopParser


@pytest.mark.parametrize(
    'decorator_factory',
    (step, given, when, then, but),
)
def test_decorators_accept_step_text_instances(
    decorator_factory,
) -> None:
    async def _step_fn(_context) -> None:
        del _context

    step_text = StepText('a composed {value}')
    decorated = decorator_factory(step_text)(_step_fn)

    assert decorated.__step_definition__.step_text_list[0] is step_text


@pytest.mark.parametrize(
    ('decorator_factory', 'name'),
    (
        (step, 'step'),
        (given, 'given'),
        (when, 'when'),
        (then, 'then'),
        (but, 'but'),
    ),
)
def test_decorators_reject_double_application(
    decorator_factory,
    name: str,
) -> None:
    async def _function(_context) -> None:
        del _context

    decorated = decorator_factory('first')(_function)
    with pytest.raises(
        ValueError,
        match=rf'@{name} is being applied more than once',
    ):
        decorator_factory('second')(decorated)


def test_steps_module_exports_expected_public_api() -> None:
    assert set(steps_module.__all__) == {
        'StepDefinition',
        'StepMatcher',
        'StepRegistry',
        'step',
        'given',
        'when',
        'then',
        'but',
    }
