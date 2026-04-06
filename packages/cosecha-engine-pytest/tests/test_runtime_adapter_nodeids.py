from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from cosecha.engine.pytest.runtime_adapter import (
    _build_nodeid_aliases,
    _resolve_report_nodeid,
    run_pytest_runtime_batch_in_process,
)


def test_runtime_adapter_resolves_nodeid_relative_to_package_root() -> None:
    requested_nodeid = (
        'packages/cosecha-core/tests/test_demo.py::test_case'
    )
    aliases = _build_nodeid_aliases((requested_nodeid,))

    assert (
        _resolve_report_nodeid(
            'tests/test_demo.py::test_case',
            frozenset((requested_nodeid,)),
            aliases,
        )
        == requested_nodeid
    )


def test_runtime_adapter_keeps_exact_requested_nodeid() -> None:
    requested_nodeid = (
        'packages/cosecha-core/tests/test_demo.py::test_case[param]'
    )
    aliases = _build_nodeid_aliases((requested_nodeid,))

    assert (
        _resolve_report_nodeid(
            requested_nodeid,
            frozenset((requested_nodeid,)),
            aliases,
        )
        == requested_nodeid
    )


def test_runtime_adapter_discards_ambiguous_relative_nodeid_aliases() -> None:
    requested_nodeids = (
        'packages/cosecha-core/tests/test_demo.py::test_case',
        'packages/cosecha-mcp/tests/test_demo.py::test_case',
    )
    aliases = _build_nodeid_aliases(requested_nodeids)

    assert (
        _resolve_report_nodeid(
            'tests/test_demo.py::test_case',
            frozenset(requested_nodeids),
            aliases,
        )
        is None
    )


def test_runtime_adapter_exposes_resources_through_request_bridge() -> None:
    fixture_request_type = pytest.FixtureRequest
    had_get_resource = hasattr(fixture_request_type, 'get_resource')
    previous_get_resource = getattr(
        fixture_request_type,
        'get_resource',
        None,
    )

    with TemporaryDirectory() as tmp_dir:
        root_path = Path(tmp_dir)
        tests_path = root_path / 'tests'
        tests_path.mkdir()
        (tests_path / 'conftest.py').write_text(
            '\n'.join(
                (
                    'from __future__ import annotations',
                    '',
                    'import pytest',
                    '',
                    '@pytest.fixture',
                    'def workspace_name(request):',
                    "    return request.get_resource('workspace')",
                ),
            ),
            encoding='utf-8',
        )
        (tests_path / 'test_demo.py').write_text(
            '\n'.join(
                (
                    'from __future__ import annotations',
                    '',
                    'def test_workspace_name(workspace_name):',
                    "    assert workspace_name == 'demo-workspace'",
                ),
            ),
            encoding='utf-8',
        )

        payload = run_pytest_runtime_batch_in_process(
            root_path=root_path,
            nodeids=('tests/test_demo.py::test_workspace_name',),
            resources={'workspace': 'demo-workspace'},
        )

    result = payload['tests/test_demo.py::test_workspace_name']
    assert result['status'] == 'passed'
    assert result['message'] is None
    assert result['failure_kind'] is None
    assert result['error_code'] is None
    assert isinstance(result['duration'], float)
    assert result['duration'] >= 0.0
    if had_get_resource:
        assert fixture_request_type.get_resource is previous_get_resource
    else:
        assert not hasattr(fixture_request_type, 'get_resource')
