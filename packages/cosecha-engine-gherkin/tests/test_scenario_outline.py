from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

from cosecha.engine.gherkin.collector import GherkinCollector
from cosecha_internal.testkit import build_config


if TYPE_CHECKING:
    from pathlib import Path


EXAMPLE_COUNT = 2


def test_scenario_outline_collection(tmp_path: Path) -> None:
    root_path = tmp_path
    collector = GherkinCollector()
    collector.initialize(build_config(root_path))

    feature_file = root_path / 'test.feature'
    feature_file.write_text(
        '\n'.join(
            (
                'Feature: Outline Test',
                '  Scenario Outline: test',
                '    Given <val>',
                '    Examples:',
                '      | val |',
                '      | 1   |',
                '      | 2   |',
            ),
        ),
        encoding='utf-8',
    )

    test_items = asyncio.run(collector.load_tests_from_file(feature_file))

    assert test_items is not None
    assert len(test_items) == EXAMPLE_COUNT
    assert test_items[0].scenario.name == 'test [Example #1]'
    assert test_items[1].scenario.name == 'test [Example #2]'
    assert test_items[0].scenario.steps[0].text == '1'
    assert test_items[1].scenario.steps[0].text == '2'
