from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

from cosecha.engine.gherkin.collector import GherkinCollector
from cosecha.engine.gherkin.steps.registry import StepRegistry
from cosecha_internal.testkit import build_config


if TYPE_CHECKING:
    from pathlib import Path


def test_find_step_impl_directories_includes_root_and_local_steps(
    tmp_path: Path,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir()
    (root_path / 'steps').mkdir()
    feature_directory = root_path / 'payments'
    feature_directory.mkdir()
    (feature_directory / 'steps').mkdir()

    collector = GherkinCollector()
    collector.initialize(build_config(root_path), root_path)

    asyncio.run(
        collector.find_step_impl_directories(
            feature_directory / 'payment.feature',
        ),
    )

    assert collector.steps_directories == {
        root_path / 'steps',
        feature_directory / 'steps',
    }


def test_load_step_impl_registers_steps_from_discovered_directories(
    tmp_path: Path,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir()
    steps_directory = root_path / 'steps'
    steps_directory.mkdir()
    (steps_directory / 'payment_steps.py').write_text(
        '\n'.join(
            (
                'from cosecha.engine.gherkin.steps import given',
                '',
                '@given("the payment exists")',
                'async def payment_exists(context):',
                '    del context',
            ),
        ),
        encoding='utf-8',
    )

    collector = GherkinCollector()
    collector.initialize(build_config(root_path), root_path)
    collector.steps_directories = {steps_directory}
    step_registry = StepRegistry()

    asyncio.run(collector.load_step_impl(step_registry))

    assert step_registry.find_match('given', 'the payment exists') is not None
