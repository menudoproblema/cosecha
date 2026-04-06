from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest

from cosecha.shell.runner_cli import (
    CliSelection,
    GherkinFormatCliRequest,
    GherkinPreCommitCliRequest,
    ManifestShowCliRequest,
    ManifestValidateCliRequest,
    PytestValidateCliRequest,
    QueryRenderOptions,
    _build_config,
    _build_gherkin_request,
    _build_manifest_request,
    _build_reports,
    _build_runtime_provider,
    _build_knowledge_query_request,
    _build_pytest_request,
    _build_query_render_options,
    _build_selection,
    _build_session_query_request,
    _collect_gherkin_feature_paths,
    _collect_python_validation_paths,
    _delete_knowledge_base_files,
    _format_query_payload,
    _iter_knowledge_base_file_paths,
    _normalize_cli_path_selector,
    _serialize_session_summary_artifact,
    _should_include_manifest_hook_arguments,
)
from cosecha.core.output import OutputDetail
from cosecha.core.runtime import LocalRuntimeProvider, ProcessRuntimeProvider
from cosecha.core.session_artifacts import (
    EngineReportSummary,
    InstrumentationSummary,
    LiveEngineSnapshotSummary,
    SessionArtifact,
    SessionReportSummary,
)
from cosecha_internal.testkit import build_config


def test_cli_selection_builds_requested_paths_and_labels() -> None:
    selection = CliSelection(
        engines=('pytest',),
        include_paths=('tests/unit',),
        exclude_paths=('tests/integration',),
        include_labels=('api',),
        exclude_labels=('slow',),
        test_limit=5,
    )

    assert selection.requested_paths() == ('tests/unit', '~tests/integration')
    assert selection.selection_labels() == ('api', '~slow')
    assert selection.selected_engine_names() == {'pytest'}


def test_cli_selection_without_engines_returns_none() -> None:
    assert CliSelection().selected_engine_names() is None


def test_normalize_cli_path_selector_accepts_root_relative_path(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_path = tmp_path / 'tests'
    unit_path = root_path / 'unit'
    unit_path.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)

    normalized = _normalize_cli_path_selector(
        'tests/unit',
        root_path=root_path,
    )

    assert normalized == 'unit'


def test_normalize_cli_path_selector_rejects_ambiguous_path(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_path = tmp_path / 'tests'
    (root_path / 'nested').mkdir(parents=True)
    (root_path / 'unit' / 'nested').mkdir(parents=True)
    monkeypatch.chdir(root_path / 'unit')

    with pytest.raises(ValueError, match='Ambiguous --path value'):
        _normalize_cli_path_selector('nested', root_path=root_path)


def test_build_selection_normalizes_paths_and_preserves_labels(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_path = tmp_path / 'tests'
    (root_path / 'unit').mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        'cosecha.shell.runner_cli._resolve_root_path',
        lambda: root_path,
    )
    args = Namespace(
        selected_engines=['pytest'],
        include_paths=['tests/unit'],
        exclude_paths=[],
        include_labels=['api'],
        exclude_labels=['slow'],
        test_limit=7,
    )

    selection = _build_selection(args)

    assert selection == CliSelection(
        engines=('pytest',),
        include_paths=('unit',),
        exclude_paths=(),
        include_labels=('api',),
        exclude_labels=('slow',),
        test_limit=7,
    )


def test_should_include_manifest_hook_arguments_for_bootstrap_commands() -> None:
    assert _should_include_manifest_hook_arguments(
        Namespace(command_group='manifest', manifest_command='show'),
    ) is False
    assert _should_include_manifest_hook_arguments(
        Namespace(command_group='pytest', pytest_command='validate'),
    ) is False
    assert _should_include_manifest_hook_arguments(
        Namespace(command_group='run'),
    ) is True


def test_build_query_render_options_splits_fields() -> None:
    options = _build_query_render_options(
        Namespace(
            test_limit=5,
            query_offset=2,
            query_sort_by='metadata.sequence_number',
            query_sort_order='desc',
            query_fields='event_type, status ,metadata.sequence_number',
            query_view='compact',
            query_preset='latest',
        ),
    )

    assert options == QueryRenderOptions(
        page_size=5,
        offset=2,
        sort_by='metadata.sequence_number',
        sort_order='desc',
        fields=('event_type', 'status', 'metadata.sequence_number'),
        view='compact',
        preset='latest',
    )


def test_format_query_payload_applies_latest_preset_to_events() -> None:
    payload = {
        'result_type': 'knowledge.events',
        'events': [
            {
                'event_type': 'test.finished',
                'timestamp': 1.0,
                'status': 'passed',
                'metadata': {
                    'sequence_number': 1,
                    'session_id': 'session-1',
                },
            },
            {
                'event_type': 'test.finished',
                'timestamp': 2.0,
                'status': 'failed',
                'metadata': {
                    'sequence_number': 2,
                    'session_id': 'session-1',
                },
            },
        ],
    }

    formatted = _format_query_payload(
        payload,
        render_options=QueryRenderOptions(preset='latest', view='compact'),
    )

    assert formatted['query']['sort_by'] == 'timestamp'
    assert formatted['query']['sort_order'] == 'desc'
    assert formatted['page']['returned'] == 2
    assert formatted['events'][0]['timestamp'] == 2.0
    assert formatted['events'][1]['timestamp'] == 1.0


def test_format_query_payload_applies_failures_preset_to_tests() -> None:
    payload = {
        'result_type': 'knowledge.tests',
        'tests': [
            {'node_stable_id': 'stable-1', 'status': 'passed'},
            {'node_stable_id': 'stable-2', 'status': 'failed'},
            {'node_stable_id': 'stable-3', 'status': 'error'},
        ],
    }

    formatted = _format_query_payload(
        payload,
        render_options=QueryRenderOptions(preset='failures'),
    )

    assert formatted['page']['total'] == 2
    assert [item['node_stable_id'] for item in formatted['tests']] == [
        'stable-2',
        'stable-3',
    ]


def test_build_knowledge_query_request_normalizes_test_path(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_path = tmp_path / 'tests'
    (root_path / 'unit').mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        'cosecha.shell.runner_cli._resolve_root_path',
        lambda: root_path,
    )
    args = Namespace(
        knowledge_query_target='tests',
        query_engine_name='pytest',
        query_test_path='tests/unit/test_demo.py',
        query_status='passed',
        query_failure_kind=None,
        query_node_stable_id=None,
        query_plan_id=None,
        test_limit=3,
        query_offset=0,
        query_sort_by=None,
        query_sort_order='asc',
        query_fields=None,
        query_view='full',
        query_preset=None,
    )

    request = _build_knowledge_query_request(args)

    assert request.operation.query.test_path == 'unit/test_demo.py'
    assert request.render_options.page_size == 3


def test_build_session_query_request_uses_summary_wrapper(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir(parents=True)
    monkeypatch.setattr(
        'cosecha.shell.runner_cli._resolve_root_path',
        lambda: root_path,
    )
    args = Namespace(
        session_command='summary',
        query_session_id='session-1',
        query_trace_id='trace-1',
        test_limit=2,
        query_offset=0,
        query_sort_by=None,
        query_sort_order='asc',
        query_fields=None,
        query_view='compact',
        query_preset='latest',
    )

    request = _build_session_query_request(args)

    assert request.operation.query.session_id == 'session-1'
    assert request.operation.query.trace_id == 'trace-1'
    assert request.render_options.preset == 'latest'


def test_build_reports_rejects_invalid_and_duplicate_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'cosecha.shell.runner_cli.Runner.available_reporter_types',
        classmethod(lambda cls: {'junit': object, 'json': object}),
    )

    with pytest.raises(ValueError, match='Expected name:path'):
        _build_reports(Namespace(extra_reports=['broken']))

    with pytest.raises(ValueError, match='Unsupported report type'):
        _build_reports(Namespace(extra_reports=['xml:report.xml']))

    with pytest.raises(ValueError, match='Duplicate report type'):
        _build_reports(
            Namespace(extra_reports=['junit:one.xml', 'junit:two.xml']),
        )


def test_build_config_forces_full_failures_in_debug_mode(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'cosecha.shell.runner_cli._resolve_root_path',
        lambda: tmp_path,
    )
    monkeypatch.setattr(
        'cosecha.shell.runner_cli.Runner.available_reporter_types',
        classmethod(lambda cls: {'junit': object}),
    )
    args = Namespace(
        output='debug',
        detail='standard',
        stop_on_error=False,
        no_capture_log=False,
        concurrency=2,
        strict_step_ambiguity=True,
        persist_live_engine_snapshots=True,
        extra_reports=['junit:report.xml'],
        definition_paths=['shared/defs.py'],
    )

    config = _build_config(args)

    assert config.root_path == tmp_path
    assert config.output_detail == OutputDetail.FULL_FAILURES
    assert config.reports == {'junit': Path('report.xml')}
    assert config.definition_paths == (
        (tmp_path / 'shared/defs.py').resolve(),
    )


def test_build_runtime_provider_switches_between_local_and_process() -> None:
    assert isinstance(
        _build_runtime_provider(Namespace(runtime='local')),
        LocalRuntimeProvider,
    )
    assert isinstance(
        _build_runtime_provider(Namespace(runtime='process')),
        ProcessRuntimeProvider,
    )


def test_collect_gherkin_feature_paths_discovers_directories_and_deduplicates(
    tmp_path,
) -> None:
    root_path = tmp_path / 'tests'
    feature_dir = root_path / 'features'
    feature_dir.mkdir(parents=True)
    login = feature_dir / 'login.feature'
    login.write_text('Feature: Login\n', encoding='utf-8')
    logout = feature_dir / 'logout.feature'
    logout.write_text('Feature: Logout\n', encoding='utf-8')

    paths = _collect_gherkin_feature_paths(
        ('features', 'features/login.feature'),
        root_path=root_path,
    )

    assert paths == (login, logout)


def test_collect_gherkin_feature_paths_can_ignore_non_feature_inputs(
    tmp_path,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir(parents=True)
    readme = root_path / 'README.md'
    readme.write_text('# docs\n', encoding='utf-8')

    assert _collect_gherkin_feature_paths(
        ('README.md',),
        root_path=root_path,
        ignore_non_feature_inputs=True,
    ) == ()


def test_collect_python_validation_paths_filters_python_files(
    tmp_path,
) -> None:
    root_path = tmp_path / 'tests'
    module_dir = root_path / 'unit'
    module_dir.mkdir(parents=True)
    test_file = module_dir / 'test_demo.py'
    test_file.write_text('def test_demo():\n    pass\n', encoding='utf-8')
    helper_file = module_dir / 'helper.py'
    helper_file.write_text('VALUE = 1\n', encoding='utf-8')

    paths = _collect_python_validation_paths(
        ('unit', 'unit/test_demo.py'),
        root_path=root_path,
    )

    assert paths == (helper_file, test_file)


def test_iter_and_delete_knowledge_base_files_include_sidecars(
    tmp_path,
) -> None:
    db_paths = _iter_knowledge_base_file_paths(tmp_path)
    for path in db_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('x', encoding='utf-8')

    removed = _delete_knowledge_base_files(tmp_path)

    assert removed == db_paths
    assert all(not path.exists() for path in db_paths)


def test_build_manifest_request_selects_expected_request_type() -> None:
    assert isinstance(
        _build_manifest_request(
            Namespace(manifest_command='show', manifest_file=None),
        ),
        ManifestShowCliRequest,
    )
    assert isinstance(
        _build_manifest_request(
            Namespace(manifest_command='validate', manifest_file=None),
        ),
        ManifestValidateCliRequest,
    )


def test_build_gherkin_and_pytest_requests_wrap_runtime_context(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_context = object()
    monkeypatch.setattr(
        'cosecha.shell.runner_cli._build_gherkin_runtime_context',
        lambda args, *, available_plugins: runtime_context,
    )
    monkeypatch.setattr(
        'cosecha.shell.runner_cli._resolve_root_path',
        lambda: tmp_path,
    )

    gherkin_request = _build_gherkin_request(
        Namespace(
            gherkin_command='fmt',
            paths=['tests/unit/login.feature'],
            check_only=True,
        ),
        available_plugins=[],
    )
    pre_commit_request = _build_gherkin_request(
        Namespace(
            gherkin_command='pre-commit',
            paths=['tests/unit/login.feature'],
            write_changes=False,
        ),
        available_plugins=[],
    )
    pytest_request = _build_pytest_request(
        Namespace(pytest_command='validate', paths=['tests/unit/test_demo.py']),
        available_plugins=[],
    )

    assert isinstance(gherkin_request, GherkinFormatCliRequest)
    assert gherkin_request.check is True
    assert isinstance(pre_commit_request, GherkinPreCommitCliRequest)
    assert pre_commit_request.context is runtime_context
    assert pre_commit_request.write is False
    assert isinstance(pytest_request, PytestValidateCliRequest)
    assert pytest_request.context is runtime_context


def test_serialize_session_summary_artifact_exposes_breakdown(
    tmp_path,
) -> None:
    artifact = SessionArtifact(
        session_id='session-1',
        root_path=str(tmp_path),
        config_snapshot=build_config(tmp_path).snapshot(),
        capability_snapshots=(),
        recorded_at=123.0,
        trace_id='trace-1',
        plan_id='plan-1',
        has_failures=True,
        report_summary=SessionReportSummary(
            total_tests=3,
            status_counts=(('passed', 2), ('failed', 1)),
            failure_kind_counts=(('test', 1),),
            engine_summaries=(
                EngineReportSummary(
                    engine_name='gherkin',
                    total_tests=3,
                    status_counts=(('passed', 2), ('failed', 1)),
                    failed_examples=('Scenario: auth',),
                    failed_files=('features/auth.feature',),
                ),
            ),
            live_engine_snapshots=(
                LiveEngineSnapshotSummary(
                    engine_name='gherkin',
                    snapshot_kind='catalog',
                    node_stable_id='stable-1',
                    payload={'steps': 3},
                ),
                LiveEngineSnapshotSummary(
                    engine_name='gherkin',
                    snapshot_kind='catalog',
                    node_stable_id='stable-2',
                    payload={'steps': 4},
                ),
            ),
            failed_examples=('Scenario: auth',),
            failed_files=('features/auth.feature',),
            instrumentation_summaries={
                'coverage': InstrumentationSummary(
                    instrumentation_name='coverage',
                    summary_kind='coverage.py',
                    payload={
                        'total_coverage': 87.5,
                        'report_type': 'coverage.py',
                        'engine_names': ['gherkin'],
                        'source_targets': ['src/app.py'],
                    },
                ),
            },
        ),
    )

    payload = _serialize_session_summary_artifact(artifact)

    assert payload['session_id'] == 'session-1'
    assert payload['coverage_total'] == 87.5
    assert payload['engine_count'] == 1
    assert payload['live_snapshot_count'] == 2
    assert payload['live_snapshot_breakdown'] == {'gherkin:catalog': 2}
    assert payload['failed_example_count'] == 1
    assert payload['failed_file_count'] == 1


def test_format_query_payload_sorts_summaries_by_latest_recorded_at() -> None:
    payload = {
        'result_type': 'session.summary',
        'summaries': [
            {'session_id': 'session-1', 'recorded_at': 10.0},
            {'session_id': 'session-2', 'recorded_at': 20.0},
        ],
    }

    formatted = _format_query_payload(
        payload,
        render_options=QueryRenderOptions(preset='latest', view='compact'),
    )

    assert formatted['query']['sort_by'] == 'recorded_at'
    assert formatted['query']['sort_order'] == 'desc'
    assert [item['session_id'] for item in formatted['summaries']] == [
        'session-2',
        'session-1',
    ]
