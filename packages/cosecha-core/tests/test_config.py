from __future__ import annotations

from typing import TYPE_CHECKING

from cosecha.core.config import Config, ConfigSnapshot
from cosecha.core.output import OutputDetail, OutputMode
from cosecha.workspace import EffectiveWorkspace, ExecutionContext, ImportEnvironment
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


def test_config_builds_console_directly_from_snapshot(
    tmp_path: Path,
) -> None:
    config = build_config(tmp_path, output_mode=OutputMode.DEBUG)
    snapshot = config.snapshot()

    console = type(config).console_from_snapshot(
        snapshot,
        console_cls=CapturingConsole,
    )

    assert isinstance(console, CapturingConsole)
    assert console.output_mode == OutputMode.DEBUG
    assert console.output_detail == OutputDetail.STANDARD


def test_config_snapshot_dict_roundtrip_ignores_external_fingerprint(
    tmp_path: Path,
) -> None:
    snapshot = ConfigSnapshot(
        root_path=str(tmp_path),
        output_mode=OutputMode.SUMMARY.value,
        output_detail=OutputDetail.STANDARD.value,
        capture_log=True,
        stop_on_error=False,
        concurrency=2,
        strict_step_ambiguity=True,
        persist_live_engine_snapshots=True,
        reports=(('json', str(tmp_path / 'report.json')),),
        definition_paths=(str(tmp_path / 'defs'),),
    )
    payload = snapshot.to_dict()
    payload['fingerprint'] = 'tampered'

    restored = ConfigSnapshot.from_dict(payload)

    assert restored == snapshot
    assert snapshot.fingerprint != 'tampered'


def test_config_workspace_and_execution_context_properties(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    knowledge_anchor = workspace_root / 'tests'
    workspace = EffectiveWorkspace(
        manifest_path=None,
        workspace_root=workspace_root,
        knowledge_anchor=knowledge_anchor,
        import_environment=ImportEnvironment(),
    )
    execution_context = ExecutionContext(
        execution_root=workspace_root / 'exec-root',
        knowledge_storage_root=workspace_root / '.kb',
        shadow_root=workspace_root / 'shadow',
        invocation_id='run-1',
        workspace_fingerprint='workspace-fingerprint',
    )
    config = Config(
        root_path=tmp_path / 'ignored',
        workspace=workspace,
        execution_context=execution_context,
        console_cls=CapturingConsole,
    )

    assert config.root_path == knowledge_anchor.resolve()
    assert config.workspace_root_path == workspace_root
    assert config.execution_root_path == execution_context.execution_root
    assert (
        config.knowledge_storage_root_path
        == execution_context.knowledge_storage_root
    )
    assert config.shadow_root_path == execution_context.shadow_root

    restored = type(config).from_snapshot(
        config.snapshot(),
        console_cls=CapturingConsole,
    )
    assert restored.workspace == workspace
    assert restored.execution_context == execution_context


def test_config_defaults_to_root_when_execution_context_is_missing(
    tmp_path: Path,
) -> None:
    config = build_config(tmp_path)

    assert config.workspace_root_path == tmp_path.resolve()
    assert config.execution_root_path == tmp_path.resolve()
    assert config.knowledge_storage_root_path == (
        tmp_path.resolve() / '.cosecha'
    )
    assert config.shadow_root_path is None
