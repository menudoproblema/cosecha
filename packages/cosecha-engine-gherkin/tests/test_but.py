from __future__ import annotations

import asyncio

from cosecha.core.location import Location
from cosecha.engine.gherkin.collector import GherkinCollector
from cosecha.engine.gherkin.context import Context, ContextRegistry
from cosecha.engine.gherkin.steps import StepRegistry, but
from cosecha_internal.testkit import build_config


EXPECTED_STEP_COUNT = 2


def test_gherkin_but_keyword_support(tmp_path) -> None:
    collector = GherkinCollector()
    collector.initialize(build_config(tmp_path))

    feature_file = tmp_path / 'test_but.feature'
    feature_file.write_text(
        '\n'.join(
            (
                'Feature: But Test',
                '  Scenario: test but',
                '    Given a step',
                '    But another step',
            ),
        ),
        encoding='utf-8',
    )

    test_items = asyncio.run(collector.load_tests_from_file(feature_file))

    assert test_items is not None
    assert len(test_items) == 1
    scenario = test_items[0].scenario
    assert len(scenario.steps) == EXPECTED_STEP_COUNT
    assert scenario.steps[0].step_type == 'given'
    assert scenario.steps[1].step_type == 'but'


def test_gherkin_but_step_matching() -> None:
    registry = StepRegistry()
    executed = False

    @but('another step')
    async def _but_step(context):
        del context
        nonlocal executed
        executed = True

    registry.add_step_definition(_but_step.__step_definition__)

    match = registry.find_match('but', 'another step')

    assert match is not None

    context = Context(ContextRegistry(), registry, {})
    asyncio.run(match.step_definition.func(context))

    assert executed is True
    assert match.step_definition.step_type == 'but'
    assert match.step_definition.location == Location(
        match.step_definition.location.filename,
        match.step_definition.location.line,
        name=match.step_definition.location.name,
    )
