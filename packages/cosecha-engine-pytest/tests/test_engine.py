from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

import pytest

from cosecha.core.capabilities import (
    CAPABILITY_DRAFT_VALIDATION,
    CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE,
    CAPABILITY_PLAN_EXPLANATION,
    CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
    CAPABILITY_SELECTION_LABELS,
    CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY,
    build_capability_map,
)
from cosecha.core.items import TestResultStatus
from cosecha.core.runner import Runner
from cosecha.engine.pytest import (
    PytestEngine,
    PytestTestDefinition,
    PytestTestItem,
)
from cosecha_internal.testkit import DummyReporter, build_config


if TYPE_CHECKING:
    from pathlib import Path


EXPECTED_PYTEST_DEFINITION_COUNT = 5


def test_pytest_engine_collects_top_level_functions_and_class_methods(
    tmp_path: Path,
) -> None:
    tests_path = tmp_path / 'tests'
    tests_path.mkdir()
    test_file = tests_path / 'test_example.py'
    test_file.write_text(
        '\n'.join(
            (
                'import pytest',
                '',
                '@pytest.fixture',
                'def sample_fixture():',
                '    return "fixture"',
                '',
                '@pytest.mark.api',
                'def test_sync_case():',
                '    return None',
                '',
                '@pytest.mark.slow',
                'async def test_async_case():',
                '    return None',
                '',
                'def helper_function():',
                '    return None',
                '',
                'def test_with_fixture(sample_fixture):',
                '    return sample_fixture',
                '',
                '@pytest.mark.ui',
                'class TestAccountFlow:',
                '    @pytest.mark.api',
                '    def test_method_case(self):',
                '        return None',
                '',
                '    def test_method_with_fixture(self, sample_fixture):',
                '        del sample_fixture',
            ),
        ),
        encoding='utf-8',
    )

    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tests_path), '')

    asyncio.run(engine.collect(test_file))
    tests = engine.get_collected_tests()
    definition_index = engine.get_project_definition_index()

    assert [test.test_name for test in tests] == [
        'test_sync_case',
        'test_async_case',
        'test_with_fixture',
        'TestAccountFlow.test_method_case',
        'TestAccountFlow.test_method_with_fixture',
    ]
    assert tests[0].has_selection_label('api')
    assert tests[1].has_selection_label('slow')
    assert tests[3].has_selection_label('ui')
    assert tests[3].has_selection_label('api')
    assert tests[4].has_selection_label('ui')
    assert len(definition_index.tests) == EXPECTED_PYTEST_DEFINITION_COUNT


def test_pytest_engine_marks_collection_issues_with_failure_kind(
    tmp_path: Path,
) -> None:
    test_file = tmp_path / 'test_example.py'
    test_file.write_text(
        'def test_case():\n    return None\n',
        encoding='utf-8',
    )
    engine = PytestEngine('pytest', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')
    test = PytestTestItem(
        test_file,
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            skip_issue='unsupported skip condition',
        ),
        tmp_path,
    )

    with pytest.raises(RuntimeError, match='unsupported skip condition'):
        asyncio.run(engine.start_test(test))

    assert test.status == TestResultStatus.ERROR
    assert test.failure_kind == 'collection'
    assert test.error_code == 'pytest_case_skip_issue'


@pytest.mark.filterwarnings('ignore:Unknown pytest.mark.api')
@pytest.mark.filterwarnings('ignore:Unknown pytest.mark.slow')
def test_pytest_engine_runner_filters_selection_labels(tmp_path: Path) -> None:
    tests_path = tmp_path / 'tests'
    tests_path.mkdir()
    (tests_path / 'pytest.ini').write_text(
        '\n'.join(
            (
                '[pytest]',
                'markers =',
                '    api: API selection label',
                '    slow: Slow selection label',
            ),
        ),
        encoding='utf-8',
    )
    state_file = tests_path / 'state.txt'
    test_file = tests_path / 'test_markers.py'
    test_file.write_text(
        '\n'.join(
            (
                'import pytest',
                'from pathlib import Path',
                '',
                'STATE = Path(__file__).with_name("state.txt")',
                '',
                '@pytest.mark.api',
                'def test_api_case():',
                '    STATE.write_text("api\\n", encoding="utf-8")',
                '',
                '@pytest.mark.slow',
                'def test_slow_case():',
                '    existing = (',
                '        STATE.read_text(encoding="utf-8")',
                '        if STATE.exists() else ""',
                '    )',
                '    STATE.write_text(existing + "slow\\n", encoding="utf-8")',
            ),
        ),
        encoding='utf-8',
    )

    runner = Runner(
        build_config(tests_path),
        {'': PytestEngine('pytest', reporter=DummyReporter())},
    )

    has_failures = asyncio.run(
        runner.run(paths=[test_file], selection_labels=['api']),
    )

    assert has_failures is False
    assert state_file.read_text(encoding='utf-8') == 'api\n'


def test_pytest_engine_describes_supported_capabilities() -> None:
    engine = PytestEngine('pytest', reporter=DummyReporter())

    capability_map = build_capability_map(engine.describe_capabilities())

    assert CAPABILITY_SELECTION_LABELS in capability_map
    assert CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY in capability_map
    assert CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE in capability_map
    assert CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE in capability_map
    assert CAPABILITY_DRAFT_VALIDATION in capability_map
    assert CAPABILITY_PLAN_EXPLANATION in capability_map
