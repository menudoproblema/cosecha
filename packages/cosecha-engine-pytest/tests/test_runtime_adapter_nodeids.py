from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest

from cosecha.core.manifest_types import ResourceBindingSpec
from cosecha.engine.pytest.resource_bridge_plugin import (
    _discover_manifest_path,
)
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


def test_resource_bridge_plugin_skips_manifest_fixture_without_resources(
) -> None:
    class CapturePlugin:
        def __init__(self) -> None:
            self.reports: dict[tuple[str, str], object] = {}

        def pytest_runtest_logreport(self, report) -> None:
            self.reports[(str(report.nodeid), report.when)] = report

    with TemporaryDirectory() as tmp_dir:
        root_path = Path(tmp_dir)
        tests_path = root_path / 'tests'
        tests_path.mkdir()
        (root_path / 'cosecha.toml').write_text(
            '\n'.join(
                (
                    '[manifest]',
                    'schema_version = 1',
                    '',
                    '[[engines]]',
                    'id = "pytest"',
                    'type = "pytest"',
                    'name = "pytest"',
                    'path = "tests"',
                    '',
                    '[[resources]]',
                    'name = "workspace"',
                    'provider = "demo:provider"',
                    'scope = "test"',
                    'mode = "ephemeral"',
                    '',
                    '[[resource_bindings]]',
                    'engine_type = "pytest"',
                    'resource_name = "workspace"',
                    'fixture_name = "cosecha_workspace"',
                ),
            ),
            encoding='utf-8',
        )
        (tests_path / 'test_demo_skip.py').write_text(
            '\n'.join(
                (
                    'from __future__ import annotations',
                    '',
                    'def test_workspace(cosecha_workspace):',
                    '    del cosecha_workspace',
                ),
            ),
            encoding='utf-8',
        )

        capture_plugin = CapturePlugin()
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.chdir(root_path)
            exit_code = pytest.main(
                ['-q', 'tests/test_demo_skip.py::test_workspace'],
                plugins=[capture_plugin],
            )

    assert exit_code == 0
    report = capture_plugin.reports[
        ('tests/test_demo_skip.py::test_workspace', 'setup')
    ]
    assert report.skipped
    assert "requires Cosecha resource 'workspace'" in str(report.longrepr)


def test_runtime_adapter_resolves_manifest_bound_resource_fixture() -> None:
    with TemporaryDirectory() as tmp_dir:
        root_path = Path(tmp_dir)
        tests_path = root_path / 'tests'
        tests_path.mkdir()
        (root_path / 'cosecha.toml').write_text(
            '\n'.join(
                (
                    '[manifest]',
                    'schema_version = 1',
                    '',
                    '[[engines]]',
                    'id = "pytest"',
                    'type = "pytest"',
                    'name = "pytest"',
                    'path = "tests"',
                    '',
                    '[[resources]]',
                    'name = "workspace"',
                    'provider = "demo:provider"',
                    'scope = "test"',
                    'mode = "ephemeral"',
                    '',
                    '[[resource_bindings]]',
                    'engine_type = "pytest"',
                    'resource_name = "workspace"',
                    'fixture_name = "cosecha_workspace"',
                ),
            ),
            encoding='utf-8',
        )
        (tests_path / 'test_demo_bound.py').write_text(
            '\n'.join(
                (
                    'from __future__ import annotations',
                    '',
                    'def test_workspace(cosecha_workspace):',
                    "    assert cosecha_workspace == 'demo-workspace'",
                ),
            ),
            encoding='utf-8',
        )

        payload = run_pytest_runtime_batch_in_process(
            root_path=root_path,
            nodeids=('tests/test_demo_bound.py::test_workspace',),
            resources={'workspace': 'demo-workspace'},
        )

    result = payload['tests/test_demo_bound.py::test_workspace']
    assert result['status'] == 'passed'
    assert result['message'] is None


def test_runtime_adapter_honors_explicit_resource_bindings() -> None:
    with TemporaryDirectory() as tmp_dir:
        root_path = Path(tmp_dir)
        tests_path = root_path / 'tests'
        tests_path.mkdir()
        (tests_path / 'test_demo_explicit.py').write_text(
            '\n'.join(
                (
                    'from __future__ import annotations',
                    '',
                    'def test_workspace(cosecha_workspace):',
                    "    assert cosecha_workspace == 'demo-workspace'",
                ),
            ),
            encoding='utf-8',
        )

        payload = run_pytest_runtime_batch_in_process(
            root_path=root_path,
            nodeids=('tests/test_demo_explicit.py::test_workspace',),
            resource_bindings=(
                ResourceBindingSpec(
                    engine_type='pytest',
                    resource_name='workspace',
                    fixture_name='cosecha_workspace',
                ),
            ),
            resources={'workspace': 'demo-workspace'},
        )

    result = payload['tests/test_demo_explicit.py::test_workspace']
    assert result['status'] == 'passed'
    assert result['message'] is None


def test_resource_bridge_plugin_discovers_manifest_in_parent_directory(
) -> None:
    class CapturePlugin:
        def __init__(self) -> None:
            self.reports: dict[tuple[str, str], object] = {}

        def pytest_runtest_logreport(self, report) -> None:
            self.reports[(str(report.nodeid), report.when)] = report

    with TemporaryDirectory() as tmp_dir:
        root_path = Path(tmp_dir)
        package_path = root_path / 'packages' / 'demo'
        tests_path = package_path / 'tests'
        tests_path.mkdir(parents=True)
        (root_path / 'cosecha.toml').write_text(
            '\n'.join(
                (
                    '[manifest]',
                    'schema_version = 1',
                    '',
                    '[[engines]]',
                    'id = "pytest"',
                    'type = "pytest"',
                    'name = "pytest"',
                    'path = "packages"',
                    '',
                    '[[resources]]',
                    'name = "workspace"',
                    'provider = "demo:provider"',
                    'scope = "test"',
                    'mode = "ephemeral"',
                    '',
                    '[[resource_bindings]]',
                    'engine_type = "pytest"',
                    'resource_name = "workspace"',
                    'fixture_name = "cosecha_workspace"',
                ),
            ),
            encoding='utf-8',
        )
        (tests_path / 'test_demo_nested.py').write_text(
            '\n'.join(
                (
                    'from __future__ import annotations',
                    '',
                    'def test_workspace(cosecha_workspace):',
                    '    del cosecha_workspace',
                ),
            ),
            encoding='utf-8',
        )

        capture_plugin = CapturePlugin()
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.chdir(package_path)
            exit_code = pytest.main(
                ['-q', 'tests/test_demo_nested.py::test_workspace'],
                plugins=[capture_plugin],
            )

    assert exit_code == 0
    report = capture_plugin.reports[
        ('tests/test_demo_nested.py::test_workspace', 'setup')
    ]
    assert report.skipped
    assert "requires Cosecha resource 'workspace'" in str(report.longrepr)


def test_resource_bridge_plugin_prefers_rootpath_manifest_over_cwd(
) -> None:
    with TemporaryDirectory() as root_dir, TemporaryDirectory() as cwd_dir:
        root_path = Path(root_dir)
        cwd_path = Path(cwd_dir)
        root_manifest = root_path / 'cosecha.toml'
        cwd_manifest = cwd_path / 'cosecha.toml'
        root_manifest.write_text('', encoding='utf-8')
        cwd_manifest.write_text('', encoding='utf-8')

        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.chdir(cwd_path)
            manifest_path = _discover_manifest_path(
                SimpleNamespace(rootpath=root_path),
            )

    assert manifest_path == root_manifest.resolve()
