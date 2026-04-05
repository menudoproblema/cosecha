from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cosecha.core.runtime_profiles import build_runtime_canonical_binding_name


try:  # pragma: no cover
    import pytest
except Exception:  # pragma: no cover
    pytest = None


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.cosecha_manifest import ResourceBindingSpec


SKIP_LONGREPR_PARTS = 3


@dataclass(slots=True, frozen=True)
class PytestRuntimeResult:
    status: str
    message: str | None = None
    duration: float = 0.0
    failure_kind: str | None = None
    error_code: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'duration': self.duration,
            'error_code': self.error_code,
            'failure_kind': self.failure_kind,
            'message': self.message,
            'status': self.status,
        }


class _PytestRuntimeCapturePlugin:
    __slots__ = ('collection_failed', 'nodeids', 'reports_by_nodeid')

    def __init__(self, nodeids: tuple[str, ...]) -> None:
        self.nodeids = frozenset(nodeids)
        self.reports_by_nodeid: dict[str, dict[str, Any]] = {}
        self.collection_failed = False

    def pytest_runtest_logreport(self, report: Any) -> None:
        if report.nodeid not in self.nodeids:
            return

        self.reports_by_nodeid.setdefault(report.nodeid, {})[report.when] = (
            report
        )

    def pytest_collectreport(self, report: Any) -> None:
        if report.failed:
            self.collection_failed = True

    def build_results(self) -> dict[str, PytestRuntimeResult]:
        return {
            nodeid: self._build_result_for_nodeid(nodeid)
            for nodeid in self.nodeids
        }

    def _build_result_for_nodeid(self, nodeid: str) -> PytestRuntimeResult:
        if self.collection_failed:
            return PytestRuntimeResult(
                status='error',
                message='Pytest collection failed in runtime adapter',
                failure_kind='collection',
                error_code='pytest_runtime_collection_failed',
            )

        node_reports = self.reports_by_nodeid.get(nodeid, {})
        setup_report = node_reports.get('setup')
        call_report = node_reports.get('call')
        teardown_report = node_reports.get('teardown')

        duration = sum(
            report.duration
            for report in node_reports.values()
            if hasattr(report, 'duration')
        )

        for candidate in (
            _build_setup_result(setup_report, duration),
            _build_call_result(call_report, duration),
            _build_teardown_result(teardown_report, duration),
        ):
            if candidate is not None:
                return candidate

        return _build_unknown_result(duration)


def _build_setup_result(
    report: Any,
    duration: float,
) -> PytestRuntimeResult | None:
    if report is None:
        return None

    if report.failed:
        return PytestRuntimeResult(
            status='error',
            message=_format_longrepr(report),
            duration=duration,
            failure_kind='bootstrap',
        )

    if not report.skipped:
        return None

    return PytestRuntimeResult(
        status='skipped',
        message=_extract_skip_message(report),
        duration=duration,
    )


def _build_call_result(
    report: Any,
    duration: float,
) -> PytestRuntimeResult | None:
    if report is None:
        return None

    was_xfail = _extract_was_xfail(report)
    if report.failed:
        message = _format_longrepr(report)
        if was_xfail is not None:
            message = f'Unexpected pass for xfail: {was_xfail}'

        return PytestRuntimeResult(
            status='failed',
            message=message,
            duration=duration,
            failure_kind='test',
        )

    if report.skipped:
        if was_xfail is not None:
            message = f'Expected failure: {was_xfail}'
        else:
            message = _extract_skip_message(report)

        return PytestRuntimeResult(
            status='skipped',
            message=message,
            duration=duration,
        )

    if not report.passed:
        return None

    return PytestRuntimeResult(
        status='passed',
        message=(
            None
            if was_xfail is None
            else f'Unexpected pass for xfail: {was_xfail}'
        ),
        duration=duration,
    )


def _build_teardown_result(
    report: Any,
    duration: float,
) -> PytestRuntimeResult | None:
    if report is None or not report.failed:
        return None

    return PytestRuntimeResult(
        status='error',
        message=_format_longrepr(report),
        duration=duration,
        failure_kind='bootstrap',
    )


def _build_unknown_result(duration: float) -> PytestRuntimeResult:
    return PytestRuntimeResult(
        status='error',
        message=(
            'Pytest runtime adapter could not determine a terminal '
            'status for the selected node'
        ),
        duration=duration,
        failure_kind='runtime',
        error_code='pytest_runtime_unknown_terminal_status',
    )


def _extract_was_xfail(report: Any) -> str | None:
    was_xfail = getattr(report, 'wasxfail', None)
    return None if was_xfail is None else str(was_xfail)


def _extract_skip_message(report: Any) -> str | None:
    longrepr = getattr(report, 'longrepr', None)
    if isinstance(longrepr, tuple) and len(longrepr) == SKIP_LONGREPR_PARTS:
        return str(longrepr[2])

    return _format_longrepr(report)


def _format_longrepr(report: Any) -> str | None:
    longrepr_text = getattr(report, 'longreprtext', None)
    if isinstance(longrepr_text, str) and longrepr_text:
        return longrepr_text

    longrepr = getattr(report, 'longrepr', None)
    return None if longrepr is None else str(longrepr)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--root-path', required=True)
    parser.add_argument('--test-path', required=True)
    parser.add_argument(
        '--nodeid',
        action='append',
        required=True,
    )
    parser.add_argument('--result-path', required=True)
    return parser


@contextlib.contextmanager
def _temporary_runtime_root(root_path: Path):
    resolved_root_path = root_path.resolve()
    inserted = False
    original_cwd = Path.cwd()
    if str(resolved_root_path) not in sys.path:
        sys.path.insert(0, str(resolved_root_path))
        inserted = True

    os.chdir(resolved_root_path)
    try:
        yield
    finally:
        os.chdir(original_cwd)
        if inserted:
            with contextlib.suppress(ValueError):
                sys.path.remove(str(resolved_root_path))


def _build_resource_bridge_plugin(
    resource_bindings: tuple[ResourceBindingSpec, ...],
    resources: dict[str, object],
) -> object | None:
    if pytest is None:
        return None

    fixture_bindings = tuple(
        binding
        for binding in resource_bindings
        if binding.fixture_name is not None
        and binding.resource_name in resources
    )
    canonical_bindings = tuple(
        (
            build_runtime_canonical_binding_name(resource_name),
            resource_name,
        )
        for resource_name in resources
    )
    if not fixture_bindings and not canonical_bindings:
        return None

    plugin_attributes: dict[str, object] = {}

    for index, binding in enumerate(fixture_bindings):
        fixture_name = binding.fixture_name
        if fixture_name is None:
            continue

        @pytest.fixture(name=fixture_name)
        def _resource_fixture(
            _request=None,
            *,
            _resources=resources,
            _resource_name=binding.resource_name,
        ):
            del _request
            return _resources[_resource_name]

        plugin_attributes[f'_resource_fixture_{index}'] = _resource_fixture

    offset = len(plugin_attributes)
    for index, (fixture_name, resource_name) in enumerate(canonical_bindings):

        @pytest.fixture(name=fixture_name)
        def _canonical_resource_fixture(
            _request=None,
            *,
            _resources=resources,
            _resource_name=resource_name,
        ):
            del _request
            return _resources[_resource_name]

        plugin_attributes[f'_canonical_resource_fixture_{offset + index}'] = (
            _canonical_resource_fixture
        )

    plugin_type = type(
        '_PytestRuntimeResourceBridgePlugin',
        (),
        plugin_attributes,
    )
    return plugin_type()


def run_pytest_runtime_batch_in_process(
    *,
    root_path: Path,
    nodeids: tuple[str, ...],
    resource_bindings: tuple[ResourceBindingSpec, ...] = (),
    resources: dict[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    if pytest is None:
        return {
            nodeid: PytestRuntimeResult(
                status='error',
                message='Unable to import pytest in runtime adapter',
                failure_kind='infrastructure',
                error_code='pytest_runtime_import_failed',
            ).to_dict()
            for nodeid in nodeids
        }

    plugin = _PytestRuntimeCapturePlugin(nodeids)
    plugins: list[object] = [plugin]
    resource_bridge_plugin = _build_resource_bridge_plugin(
        resource_bindings,
        resources or {},
    )
    if resource_bridge_plugin is not None:
        plugins.append(resource_bridge_plugin)

    with _temporary_runtime_root(root_path):
        pytest.main(
            ['-q', '--disable-warnings', *nodeids],
            plugins=plugins,
        )

    return {
        nodeid: payload.to_dict()
        for nodeid, payload in plugin.build_results().items()
    }


def main() -> int:
    args = _build_parser().parse_args()
    result_path = Path(args.result_path).resolve()
    nodeids = tuple(str(nodeid) for nodeid in args.nodeid)

    if pytest is None:
        result_path.write_text(
            json.dumps(
                {
                    nodeid: PytestRuntimeResult(
                        status='error',
                        message='Unable to import pytest in runtime adapter',
                        failure_kind='infrastructure',
                        error_code='pytest_runtime_import_failed',
                    ).to_dict()
                    for nodeid in nodeids
                },
                ensure_ascii=False,
            ),
            encoding='utf-8',
        )
        return 1

    result = run_pytest_runtime_batch_in_process(
        root_path=Path(args.root_path).resolve(),
        nodeids=nodeids,
    )
    result_path.write_text(
        json.dumps(result, ensure_ascii=False),
        encoding='utf-8',
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
