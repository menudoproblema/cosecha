from __future__ import annotations

from typing import TYPE_CHECKING

from cosecha.core.output import OutputDetail, OutputMode
from cosecha_internal.testkit import CapturingConsole, build_config


if TYPE_CHECKING:
    from pathlib import Path


def test_config_resolves_relative_definition_paths_and_roundtrips_snapshot(
    tmp_path: Path,
) -> None:
    config = build_config(tmp_path, output_mode=OutputMode.DEBUG)
    config.definition_paths = (
        (tmp_path / 'definitions').resolve(),
        (tmp_path / 'more-definitions').resolve(),
    )
    config.reports = {'json': tmp_path / 'reports' / 'report.json'}

    snapshot = config.snapshot()
    restored = type(config).from_snapshot(
        snapshot,
        console_cls=CapturingConsole,
    )

    assert snapshot.root_path == str(tmp_path.resolve())
    assert snapshot.output_mode == OutputMode.DEBUG.value
    assert snapshot.output_detail == OutputDetail.STANDARD.value
    assert snapshot.fingerprint == config.snapshot().fingerprint
    assert restored.root_path == tmp_path.resolve()
    assert restored.definition_paths == config.definition_paths
    assert restored.reports == config.reports
    assert isinstance(restored.console, CapturingConsole)
