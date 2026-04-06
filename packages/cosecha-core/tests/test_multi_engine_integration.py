from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

import pytest

from cosecha.core.knowledge_base import (
    DefinitionKnowledgeQuery,
    TestKnowledgeQuery,
)
from cosecha.core.operations import (
    QueryDefinitionsOperation,
    QueryDefinitionsOperationResult,
    QueryEngineDependenciesOperation,
    QueryEngineDependenciesOperationResult,
    QueryTestsOperation,
    QueryTestsOperationResult,
)
from cosecha.core.runner import Runner
from cosecha.engine.gherkin.engine import GherkinEngine
from cosecha.engine.pytest.engine import PytestEngine
from cosecha_internal.testkit import DummyReporter, build_config


if TYPE_CHECKING:
    from pathlib import Path


pytestmark = pytest.mark.filterwarnings('ignore:Unknown pytest.mark.api')


def _build_multi_engine_runner(root_path: Path) -> Runner:
    config = build_config(root_path)
    config.capture_log = False
    return Runner(
        config,
        {
            '': GherkinEngine('gherkin', reporter=DummyReporter()),
            '.': PytestEngine('pytest', reporter=DummyReporter()),
        },
    )


def _write_multi_engine_workspace(
    root_path: Path,
) -> tuple[Path, Path, Path]:
    features_path = root_path / 'features'
    steps_path = features_path / 'steps'
    steps_path.mkdir(parents=True)
    feature_path = features_path / 'mixed.feature'
    pytest_path = root_path / 'test_mixed.py'
    state_file = root_path / 'state.txt'
    (root_path / 'pytest.ini').write_text(
        '\n'.join(
            (
                '[pytest]',
                'markers =',
                '    api: API selection label',
            ),
        ),
        encoding='utf-8',
    )

    feature_path.write_text(
        '\n'.join(
            (
                'Feature: Mixed session',
                '  Scenario: Run Gherkin scenario',
                '    Given the mixed gherkin step runs',
            ),
        ),
        encoding='utf-8',
    )
    (steps_path / 'mixed_steps.py').write_text(
        '\n'.join(
            (
                'from pathlib import Path',
                'from cosecha.engine.gherkin.steps import given',
                '',
                'STATE = Path(__file__).resolve().parents[2] / "state.txt"',
                '',
                '@given("the mixed gherkin step runs")',
                'async def mixed_step(context):',
                '    """Mixed Gherkin definition."""',
                '    del context',
                (
                    '    existing = STATE.read_text(encoding="utf-8") '
                    'if STATE.exists() else ""'
                ),
                (
                    '    STATE.write_text('
                    'existing + "gherkin\\n", encoding="utf-8")'
                ),
            ),
        ),
        encoding='utf-8',
    )
    pytest_path.write_text(
        '\n'.join(
            (
                'from pathlib import Path',
                'import pytest',
                '',
                'STATE = Path(__file__).with_name("state.txt")',
                '',
                '@pytest.fixture',
                'def sample_fixture():',
                '    """Mixed Pytest fixture."""',
                '    return "fixture"',
                '',
                '@pytest.mark.api',
                'def test_pytest_case(sample_fixture):',
                '    """Mixed Pytest test."""',
                '    del sample_fixture',
                (
                    '    existing = STATE.read_text(encoding="utf-8") '
                    'if STATE.exists() else ""'
                ),
                (
                    '    STATE.write_text('
                    'existing + "pytest\\n", encoding="utf-8")'
                ),
            ),
        ),
        encoding='utf-8',
    )

    return feature_path, pytest_path, state_file


def _write_failing_multi_engine_workspace(
    root_path: Path,
) -> tuple[Path, Path]:
    feature_path, pytest_path, _state_file = _write_multi_engine_workspace(
        root_path,
    )
    pytest_path.write_text(
        '\n'.join(
            (
                'import pytest',
                '',
                '@pytest.mark.api',
                'def test_pytest_case():',
                '    raise AssertionError("boom")',
            ),
        ),
        encoding='utf-8',
    )
    return feature_path, pytest_path


def test_runner_executes_mixed_gherkin_and_pytest_session(
    tmp_path: Path,
) -> None:
    feature_path, pytest_path, state_file = _write_multi_engine_workspace(
        tmp_path,
    )
    runner = _build_multi_engine_runner(tmp_path)

    has_failures = asyncio.run(runner.run(paths=[feature_path, pytest_path]))

    assert has_failures is False
    assert state_file.read_text(encoding='utf-8') == 'gherkin\npytest\n'


def test_runner_persists_cross_engine_test_knowledge(
    tmp_path: Path,
) -> None:
    feature_path, pytest_path, _state_file = _write_multi_engine_workspace(
        tmp_path,
    )
    runner = _build_multi_engine_runner(tmp_path)

    has_failures = asyncio.run(runner.run(paths=[feature_path, pytest_path]))
    assert has_failures is False

    restored_runner = _build_multi_engine_runner(tmp_path)
    result = asyncio.run(
        restored_runner.execute_operation(
            QueryTestsOperation(
                query=TestKnowledgeQuery(limit=10),
            ),
        ),
    )
    query_result = QueryTestsOperationResult.from_dict(result.to_dict())

    assert query_result.context.source == 'persistent_knowledge_base'
    assert {test.engine_name for test in query_result.tests} == {
        'gherkin',
        'pytest',
    }
    assert {test.trace_id for test in query_result.tests} == {
        query_result.tests[0].trace_id,
    }
    assert {test.test_name for test in query_result.tests} == {
        'Scenario: Run Gherkin scenario',
        'test_pytest_case',
    }


def test_runner_persists_cross_engine_definition_knowledge(
    tmp_path: Path,
) -> None:
    feature_path, pytest_path, _state_file = _write_multi_engine_workspace(
        tmp_path,
    )
    runner = _build_multi_engine_runner(tmp_path)

    has_failures = asyncio.run(runner.run(paths=[feature_path, pytest_path]))
    assert has_failures is False

    restored_runner = _build_multi_engine_runner(tmp_path)
    result = asyncio.run(
        restored_runner.execute_operation(
            QueryDefinitionsOperation(
                query=DefinitionKnowledgeQuery(
                    include_invalidated=False,
                    limit=10,
                ),
            ),
        ),
    )
    query_result = QueryDefinitionsOperationResult.from_dict(result.to_dict())

    assert query_result.context.source == 'persistent_knowledge_base'
    assert {
        definition.engine_name for definition in query_result.definitions
    } == {'gherkin', 'pytest'}
    assert {
        descriptor.documentation
        for definition in query_result.definitions
        for descriptor in definition.descriptors
    } >= {
        'Mixed Gherkin definition.',
        'Mixed Pytest fixture.',
    }


def test_runner_exposes_active_multi_engine_dependency_rules(
    tmp_path: Path,
) -> None:
    feature_path, pytest_path, _state_file = _write_multi_engine_workspace(
        tmp_path,
    )
    runner = _build_multi_engine_runner(tmp_path)

    has_failures = asyncio.run(runner.run(paths=[feature_path, pytest_path]))
    assert has_failures is False

    restored_runner = _build_multi_engine_runner(tmp_path)
    result = asyncio.run(
        restored_runner.execute_operation(
            QueryEngineDependenciesOperation(),
        ),
    )
    query_result = QueryEngineDependenciesOperationResult.from_dict(
        result.to_dict(),
    )

    assert {
        (rule.source_engine_name, rule.target_engine_name)
        for rule in query_result.rules
    } == {
        ('gherkin', 'pytest'),
        ('pytest', 'gherkin'),
    }
    assert {rule.projection_policy for rule in query_result.rules} == {
        'degrade_to_explain',
        'diagnostic_only',
    }


def test_runner_projects_cross_engine_failures_from_latest_plan(
    tmp_path: Path,
) -> None:
    feature_path, pytest_path = _write_failing_multi_engine_workspace(
        tmp_path,
    )
    runner = _build_multi_engine_runner(tmp_path)

    has_failures = asyncio.run(runner.run(paths=[feature_path, pytest_path]))
    assert has_failures is True

    restored_runner = _build_multi_engine_runner(tmp_path)
    result = asyncio.run(
        restored_runner.execute_operation(
            QueryEngineDependenciesOperation(),
        ),
    )
    query_result = QueryEngineDependenciesOperationResult.from_dict(
        result.to_dict(),
    )

    assert len(query_result.projected_issues) == 1
    projected_issue = query_result.projected_issues[0]
    assert projected_issue.source_engine_name == 'pytest'
    assert projected_issue.target_engine_name == 'gherkin'
    assert projected_issue.projection_policy == 'degrade_to_explain'
    assert projected_issue.severity == 'warning'
    assert projected_issue.source_status in {'failed', 'error'}
