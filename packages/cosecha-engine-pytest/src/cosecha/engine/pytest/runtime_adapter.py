from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from cosecha.engine.pytest import resource_bridge_plugin


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
    __slots__ = (
        'collection_failed',
        'nodeid_aliases',
        'nodeids',
        'reports_by_nodeid',
    )

    def __init__(self, nodeids: tuple[str, ...]) -> None:
        self.nodeids = frozenset(nodeids)
        self.nodeid_aliases = _build_nodeid_aliases(nodeids)
        self.reports_by_nodeid: dict[str, dict[str, Any]] = {}
        self.collection_failed = False

    def pytest_runtest_logreport(self, report: Any) -> None:
        resolved_nodeid = _resolve_report_nodeid(
            str(report.nodeid),
            self.nodeids,
            self.nodeid_aliases,
        )
        if resolved_nodeid is None:
            return

        self.reports_by_nodeid.setdefault(resolved_nodeid, {})[
            report.when
        ] = report

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


def _build_nodeid_aliases(
    nodeids: tuple[str, ...],
) -> dict[str, str]:
    aliases: dict[str, str] = {}
    collisions: set[str] = set()
    for nodeid in nodeids:
        for alias in _iter_nodeid_aliases(nodeid):
            if alias in collisions:
                continue
            current = aliases.get(alias)
            if current is None:
                aliases[alias] = nodeid
                continue
            if current != nodeid:
                aliases.pop(alias, None)
                collisions.add(alias)
    return aliases


def _iter_nodeid_aliases(nodeid: str) -> tuple[str, ...]:
    path_part, *tail_parts = nodeid.split('::')
    path = PurePosixPath(path_part)
    aliases = []
    for index in range(len(path.parts)):
        alias_path = PurePosixPath(*path.parts[index:]).as_posix()
        aliases.append('::'.join((alias_path, *tail_parts)))
    return tuple(aliases)


def _resolve_report_nodeid(
    report_nodeid: str,
    requested_nodeids: frozenset[str],
    nodeid_aliases: dict[str, str],
) -> str | None:
    if report_nodeid in requested_nodeids:
        return report_nodeid
    return nodeid_aliases.get(report_nodeid)


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


@contextlib.contextmanager
def _temporary_request_resource_bridge(
    resources: dict[str, object],
):
    if pytest is None or not resources:
        yield
        return

    fixture_request_type = getattr(pytest, 'FixtureRequest', None)
    if fixture_request_type is None:
        yield
        return

    had_existing = hasattr(fixture_request_type, 'get_resource')
    previous_value = getattr(
        fixture_request_type,
        'get_resource',
        None,
    )

    def _get_resource(self, resource_name: str) -> object:
        del self
        if resource_name not in resources:
            msg = (
                'Pytest request does not expose resource '
                f'{resource_name!r}'
            )
            raise LookupError(msg)
        return resources[resource_name]

    fixture_request_type.get_resource = _get_resource
    try:
        yield
    finally:
        if had_existing:
            fixture_request_type.get_resource = previous_value
        else:
            delattr(fixture_request_type, 'get_resource')


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
    plugins: list[object] = [
        plugin,
        resource_bridge_plugin,
    ]

    with _temporary_runtime_root(
        root_path,
    ), resource_bridge_plugin.temporary_resource_bindings(
        resource_bindings,
    ), resource_bridge_plugin.temporary_active_resource_bridge(
        resources or {},
    ), _temporary_request_resource_bridge(
        resources or {},
    ):
        pytest.main(
            [
                '-p',
                'no:cosecha_resource_bridge',
                '-o',
                'asyncio_default_fixture_loop_scope=function',
                '-q',
                '--disable-warnings',
                *nodeids,
            ],
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


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
