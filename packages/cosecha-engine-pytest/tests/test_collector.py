from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from cosecha.engine.pytest import collector as pytest_collector_module


if TYPE_CHECKING:
    import pytest


def test_discover_pytest_tests_collects_class_marks_and_parametrize_cases(
) -> None:
    definitions = pytest_collector_module.discover_pytest_tests_from_content(
        '\n'.join(
            (
                'import pytest',
                '',
                '@pytest.mark.api',
                'class TestFlow:',
                '    @pytest.mark.slow',
                '    def test_case(self):',
                '        return None',
                '',
                '@pytest.mark.parametrize(',
                '    ("value",),',
                '    [',
                '        (1,),',
                '        pytest.param(2, id="two", marks=pytest.mark.db),',
                '    ],',
                ')',
                'def test_param(value):',
                '    return value',
            ),
        ),
    )

    assert [
        (
            definition.function_name,
            definition.class_name,
            definition.parameter_case_id,
            definition.selection_labels,
        )
        for definition in definitions
    ] == [
        ('test_case', 'TestFlow', None, ('api', 'slow')),
        ('test_param', None, '1', ()),
        ('test_param', None, 'two', ('db',)),
    ]


def test_fixture_knowledge_by_source_reuses_conftest_parse_per_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tests_path = tmp_path / 'tests'
    package_path = tests_path / 'unit' / 'auth'
    package_path.mkdir(parents=True)
    conftest_path = tests_path / 'conftest.py'
    conftest_path.write_text(
        '\n'.join(
            (
                'import pytest',
                '',
                '@pytest.fixture',
                'def shared_fixture():',
                '    return "shared"',
            ),
        ),
        encoding='utf-8',
    )
    first_test = package_path / 'test_first.py'
    second_test = package_path / 'test_second.py'
    for test_file in (first_test, second_test):
        test_file.write_text(
            'def test_case(shared_fixture):\n    assert shared_fixture\n',
            encoding='utf-8',
        )

    discover_calls: list[Path] = []
    original_discover_records = (
        pytest_collector_module._discover_fixture_knowledge_records
    )

    def _track_discover_records(source_path: Path, **kwargs):
        discover_calls.append(source_path.resolve())
        return original_discover_records(source_path, **kwargs)

    monkeypatch.setattr(
        pytest_collector_module,
        '_discover_fixture_knowledge_records',
        _track_discover_records,
    )

    records = pytest_collector_module._discover_fixture_knowledge_by_source(
        (first_test, second_test),
        root_path=tests_path,
    )

    assert conftest_path.resolve() in records
    assert discover_calls.count(conftest_path.resolve()) == 1


def test_pytest_test_file_detection_matches_supported_naming() -> None:
    assert pytest_collector_module._is_pytest_test_file(Path('test_demo.py'))
    assert pytest_collector_module._is_pytest_test_file(Path('demo_test.py'))
    assert not pytest_collector_module._is_pytest_test_file(Path('demo.py'))
