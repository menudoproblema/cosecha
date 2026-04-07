from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from cosecha.engine.pytest.runtime_adapter import (
    _PytestRuntimeCapturePlugin,
    _build_call_result,
    _build_nodeid_aliases,
    _build_parser,
    _build_setup_result,
    _build_teardown_result,
    _build_unknown_result,
    _extract_skip_message,
    _format_longrepr,
    _iter_nodeid_aliases,
    _resolve_report_nodeid,
    _temporary_request_resource_bridge,
    _temporary_runtime_root,
    main,
    run_pytest_runtime_batch_in_process,
)


def _mock_report(
    nodeid: str = 'tests/test_example.py::test_case',
    when: str = 'call',
    *,
    failed: bool = False,
    skipped: bool = False,
    passed: bool = True,
    duration: float = 0.1,
    longrepr: Any = None,
    longreprtext: str | None = None,
    wasxfail: str | None = None,
):
    report = SimpleNamespace()
    report.nodeid = nodeid
    report.when = when
    report.failed = failed
    report.skipped = skipped
    report.passed = passed
    report.duration = duration
    report.longrepr = longrepr
    report.longreprtext = longreprtext
    if wasxfail is not None:
        report.wasxfail = wasxfail
    return report


def test_build_parser_accepts_required_args() -> None:
    args = _build_parser().parse_args(
        [
            '--root-path',
            '/tmp/root',
            '--test-path',
            '/tmp/test.py',
            '--nodeid',
            'tests/test_example.py::test_case',
            '--result-path',
            '/tmp/out.json',
        ],
    )

    assert args.root_path == '/tmp/root'
    assert args.test_path == '/tmp/test.py'
    assert args.nodeid == ['tests/test_example.py::test_case']
    assert args.result_path == '/tmp/out.json'


def test_build_nodeid_aliases_keeps_unique_aliases() -> None:
    aliases = _build_nodeid_aliases(
        (
            'pkg/tests/test_a.py::test_case',
            'other/tests/test_a.py::test_case',
            'pkg/tests/extra.py::test_case',
        ),
    )

    assert aliases['pkg/tests/test_a.py::test_case'] == (
        'pkg/tests/test_a.py::test_case'
    )
    assert aliases['other/tests/test_a.py::test_case'] == (
        'other/tests/test_a.py::test_case'
    )
    assert aliases['pkg/tests/extra.py::test_case'] == (
        'pkg/tests/extra.py::test_case'
    )
    assert aliases['extra.py::test_case'] == 'pkg/tests/extra.py::test_case'
    assert aliases['tests/extra.py::test_case'] == (
        'pkg/tests/extra.py::test_case'
    )
    assert 'tests/test_a.py::test_case' not in aliases


def test_resolve_report_nodeid_uses_requested_or_alias() -> None:
    nodeids = frozenset({'a::b', 'c::d'})
    aliases = {'alias::node': 'c::d'}

    assert _resolve_report_nodeid('a::b', nodeids, aliases) == 'a::b'
    assert _resolve_report_nodeid('alias::node', nodeids, aliases) == 'c::d'
    assert _resolve_report_nodeid('missing', nodeids, aliases) is None


def test_build_teardown_result_marks_bootstrap_errors() -> None:
    assert _build_teardown_result(None, 0.0) is None
    teardown_payload = _build_teardown_result(_mock_report(failed=True), 0.5)
    assert teardown_payload is not None
    assert teardown_payload.status == 'error'


def test_iter_nodeid_aliases_is_suffix_based() -> None:
    assert _iter_nodeid_aliases('a/b/c/test_demo.py::test_case') == (
        'a/b/c/test_demo.py::test_case',
        'b/c/test_demo.py::test_case',
        'c/test_demo.py::test_case',
        'test_demo.py::test_case',
    )


def test_runtime_capture_plugin_marks_collection_failed() -> None:
    plugin = _PytestRuntimeCapturePlugin(('tests/test.py::test_case',))
    plugin.collection_failed = True

    assert plugin.build_results()['tests/test.py::test_case'].status == 'error'
    assert (
        plugin.build_results()['tests/test.py::test_case'].failure_kind == 'collection'
    )


def test_build_setup_result_branches() -> None:
    assert _build_setup_result(None, 0.0) is None
    assert _build_setup_result(_mock_report(failed=True), 0.0).status == 'error'
    assert _build_setup_result(
        _mock_report(skipped=True, longrepr='skip reason'),
        0.1,
    ).status == 'skipped'


def test_build_call_result_handles_skipped_and_xfail() -> None:
    xfail_report = _mock_report(
        failed=True,
        when='call',
        wasxfail='expected fail',
        longrepr='boom',
    )
    assert _build_call_result(xfail_report, 0.2).status == 'failed'
    assert (
        _build_call_result(xfail_report, 0.2).message
        == 'Unexpected pass for xfail: expected fail'
    )

    xpass_report = _mock_report(
        skipped=True,
        when='call',
        wasxfail='X',
        longrepr='skip tuple',
    )
    assert _build_call_result(xpass_report, 0.2).status == 'skipped'
    assert (
        _build_call_result(xpass_report, 0.2).message
        == 'Expected failure: X'
    )

    passed_report = _mock_report(when='call', failed=False, skipped=False, passed=True)
    assert _build_call_result(passed_report, 0.3).status == 'passed'


def test_build_unknown_result_has_runtime_error_code() -> None:
    payload = _build_unknown_result(0.1)
    assert payload.failure_kind == 'runtime'
    assert payload.error_code == 'pytest_runtime_unknown_terminal_status'


def test_extract_skip_message_supports_tuple_and_fallback() -> None:
    assert (
        _extract_skip_message(
            SimpleNamespace(longrepr=('a', 'b', 'skip reason')),
        )
        == 'skip reason'
    )

    assert (
        _extract_skip_message(
            SimpleNamespace(
                longrepr=(1, 2),
                longreprtext='formatted message',
            ),
        )
        == 'formatted message'
    )

    assert (
        _extract_skip_message(SimpleNamespace(longrepr='fallback'))
        == 'fallback'
    )


def test_format_longrepr_prefers_longreprtext() -> None:
    assert (
        _format_longrepr(SimpleNamespace(longreprtext='direct', longrepr='x'))
        == 'direct'
    )

    assert _format_longrepr(SimpleNamespace(longrepr='legacy')) == 'legacy'
    assert _format_longrepr(SimpleNamespace(longrepr=None)) is None


def test_temporary_runtime_root_inserts_and_restores_root_path(tmp_path) -> None:
    original_sys_path = sys.path.copy()

    with _temporary_runtime_root(tmp_path):
        assert str(tmp_path) in sys.path
        assert str(sys.path[0]) == str(tmp_path)

    assert sys.path == original_sys_path


def test_temporary_request_resource_bridge_restores_fixture_request() -> None:
    class DummyRequest:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get_resource(self, resource_name: str) -> object:
            self.calls.append(resource_name)
            raise LookupError(resource_name)

    import cosecha.engine.pytest.runtime_adapter as runtime_adapter

    original_pytest = runtime_adapter.pytest
    runtime_adapter.pytest = SimpleNamespace(FixtureRequest=DummyRequest)

    try:
        with _temporary_request_resource_bridge({'workspace': 'alpha'}):
            request = DummyRequest()
            assert request.get_resource('workspace') == 'alpha'

        with _temporary_request_resource_bridge({'workspace': 'alpha'}):
            request = DummyRequest()
            with pytest.raises(LookupError):
                request.get_resource('other')

    finally:
        runtime_adapter.pytest = original_pytest


def test_temporary_request_resource_bridge_noop_without_pytest_or_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import cosecha.engine.pytest.runtime_adapter as runtime_adapter

    monkeypatch.setattr(runtime_adapter, 'pytest', SimpleNamespace())

    with _temporary_request_resource_bridge({}):
        pass


def test_temporary_request_resource_bridge_noop_without_fixture_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import cosecha.engine.pytest.runtime_adapter as runtime_adapter

    monkeypatch.setattr(
        runtime_adapter,
        'pytest',
        SimpleNamespace(FixtureRequest=None),
    )
    with _temporary_request_resource_bridge({'workspace': 'x'}):
        pass


def test_temporary_request_resource_bridge_installs_and_removes_new_attribute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import cosecha.engine.pytest.runtime_adapter as runtime_adapter

    class DummyRequest:
        pass

    monkeypatch.setattr(
        runtime_adapter,
        'pytest',
        SimpleNamespace(FixtureRequest=DummyRequest),
    )
    assert not hasattr(DummyRequest, 'get_resource')
    with _temporary_request_resource_bridge({'workspace': 'x'}):
        assert DummyRequest().get_resource('workspace') == 'x'
        with pytest.raises(LookupError):
            DummyRequest().get_resource('missing')
    assert not hasattr(DummyRequest, 'get_resource')


def test_runtime_capture_plugin_ignores_unknown_report_nodeid() -> None:
    plugin = _PytestRuntimeCapturePlugin(('tests/test_demo.py::test_case',))
    plugin.pytest_runtest_logreport(
        _mock_report(nodeid='tests/test_other.py::test_case'),
    )
    assert plugin.reports_by_nodeid == {}


def test_runtime_capture_plugin_collectreport_sets_collection_failed() -> None:
    plugin = _PytestRuntimeCapturePlugin(('tests/test_demo.py::test_case',))
    plugin.pytest_collectreport(SimpleNamespace(failed=True))
    assert plugin.collection_failed is True


def test_runtime_capture_plugin_builds_unknown_result_without_terminal_report(
) -> None:
    plugin = _PytestRuntimeCapturePlugin(('tests/test_demo.py::test_case',))
    payload = plugin.build_results()['tests/test_demo.py::test_case']
    assert payload.status == 'error'
    assert payload.failure_kind == 'runtime'


def test_build_call_result_none_and_inconclusive_branches() -> None:
    assert _build_call_result(None, 0.0) is None
    assert _build_call_result(_mock_report(passed=False), 0.0) is None
    skipped_payload = _build_call_result(
        _mock_report(
            skipped=True,
            passed=False,
            longrepr=('f', 'l', 'skip reason'),
        ),
        0.1,
    )
    assert skipped_payload is not None
    assert skipped_payload.status == 'skipped'
    assert skipped_payload.message == 'skip reason'


def test_build_nodeid_aliases_handles_collision_set_fast_path() -> None:
    aliases = _build_nodeid_aliases(
        (
            'a/tests/test_demo.py::test_case',
            'b/tests/test_demo.py::test_case',
            'c/tests/test_demo.py::test_case',
        ),
    )
    assert 'tests/test_demo.py::test_case' not in aliases
    assert aliases['c/tests/test_demo.py::test_case'] == (
        'c/tests/test_demo.py::test_case'
    )


def test_run_pytest_runtime_batch_in_process_import_error_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    import cosecha.engine.pytest.runtime_adapter as runtime_adapter

    monkeypatch.setattr(runtime_adapter, 'pytest', None)
    payloads = run_pytest_runtime_batch_in_process(
        root_path=tmp_path,
        nodeids=('tests/test.py::test_case',),
    )

    assert payloads == {
        'tests/test.py::test_case': {
            'duration': 0.0,
            'error_code': 'pytest_runtime_import_failed',
            'failure_kind': 'infrastructure',
            'message': 'Unable to import pytest in runtime adapter',
            'status': 'error',
        },
    }


def test_main_writes_error_payload_when_pytest_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    import cosecha.engine.pytest.runtime_adapter as runtime_adapter

    result_path = tmp_path / 'runtime-result.json'
    monkeypatch.setattr(runtime_adapter, 'pytest', None)
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'runtime_adapter',
            '--root-path',
            str(tmp_path),
            '--test-path',
            str(tmp_path / 'test.py'),
            '--nodeid',
            'tests/test.py::test_case',
            '--result-path',
            str(result_path),
        ],
    )
    assert main() == 1
    assert json.loads(result_path.read_text(encoding='utf-8')) == {
        'tests/test.py::test_case': {
            'duration': 0.0,
            'error_code': 'pytest_runtime_import_failed',
            'failure_kind': 'infrastructure',
            'message': 'Unable to import pytest in runtime adapter',
            'status': 'error',
        },
    }


def test_main_writes_success_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    import cosecha.engine.pytest.runtime_adapter as runtime_adapter

    result_path = tmp_path / 'runtime-result.json'
    monkeypatch.setattr(runtime_adapter, 'pytest', object())
    monkeypatch.setattr(
        runtime_adapter,
        'run_pytest_runtime_batch_in_process',
        lambda **_: {
            'tests/test.py::test_case': {
                'status': 'passed',
                'message': None,
                'duration': 0.2,
                'failure_kind': None,
                'error_code': None,
            },
        },
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'runtime_adapter',
            '--root-path',
            str(tmp_path),
            '--test-path',
            str(tmp_path / 'test.py'),
            '--nodeid',
            'tests/test.py::test_case',
            '--result-path',
            str(result_path),
        ],
    )
    assert main() == 0
    assert json.loads(result_path.read_text(encoding='utf-8')) == {
        'tests/test.py::test_case': {
            'duration': 0.2,
            'error_code': None,
            'failure_kind': None,
            'message': None,
            'status': 'passed',
        },
    }
