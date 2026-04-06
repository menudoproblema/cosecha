from __future__ import annotations

from pathlib import Path

from cosecha.core.items import TestResultStatus
from cosecha.core.output import OutputDetail, OutputMode
from cosecha.core.session_artifacts import SessionReportState
from cosecha_internal.testkit import (
    CapturingConsole,
    NullTelemetryStream,
    build_config,
    build_generic_report,
    build_gherkin_report,
    build_plugin_context,
    write_text_tree,
)


SCENARIO_LINE = 7


def test_build_config_uses_capturing_console(tmp_path: Path) -> None:
    config = build_config(
        tmp_path,
        output_mode=OutputMode.DEBUG,
        output_detail=OutputDetail.FULL_FAILURES,
    )

    assert isinstance(config.console, CapturingConsole)
    assert config.console.is_debug_mode()
    assert config.console.should_render_full_failures()


def test_build_plugin_context_keeps_report_state_and_telemetry(
    tmp_path: Path,
) -> None:
    config = build_config(tmp_path)
    report_state = SessionReportState()
    telemetry = NullTelemetryStream()

    context = build_plugin_context(
        config,
        telemetry_stream=telemetry,
        engine_names=('pytest',),
        session_report_state=report_state,
    )

    assert context.config is config
    assert context.telemetry_stream is telemetry
    assert context.session_report_state is report_state
    assert context.engine_names == ('pytest',)


def test_report_builders_return_expected_payloads() -> None:
    generic = build_generic_report(
        path='tests/test_demo.py',
        status=TestResultStatus.PASSED,
        engine_name='pytest',
    )
    gherkin = build_gherkin_report(
        path='features/demo.feature',
        status=TestResultStatus.FAILED,
        scenario_name='Escenario demo',
        scenario_line=SCENARIO_LINE,
    )

    assert generic.engine_name == 'pytest'
    assert gherkin.engine_name == 'gherkin'
    assert gherkin.engine_payload['scenario']['name'] == 'Escenario demo'
    assert (
        gherkin.engine_payload['scenario']['location']['line']
        == SCENARIO_LINE
    )


def test_write_text_tree_materializes_nested_files(tmp_path: Path) -> None:
    written_paths = write_text_tree(
        tmp_path,
        {
            'src/demo.py': 'VALUE = 1\n',
            'tests/test_demo.py': 'def test_demo():\n    assert True\n',
        },
    )

    assert tuple(path.relative_to(tmp_path) for path in written_paths) == (
        Path('src/demo.py'),
        Path('tests/test_demo.py'),
    )
