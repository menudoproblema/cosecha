from __future__ import annotations

import pytest

from cosecha.core.runtime_profiles import RuntimeProfileSpec
from cosecha.workspace import (
    WorkspaceResolutionError,
    WorkspaceResolutionPolicy,
    build_execution_context,
    discover_cosecha_manifest,
    resolve_workspace,
    using_policy,
    validate_workspace_root,
)


def test_discover_manifest_uses_active_policy(tmp_path) -> None:
    custom_manifest = tmp_path / 'custom' / 'cosecha.toml'
    custom_manifest.parent.mkdir(parents=True)
    custom_manifest.write_text(
        '[manifest]\nschema_version = 1\n',
        encoding='utf-8',
    )

    policy = WorkspaceResolutionPolicy(
        manifest_candidate_paths=('custom/cosecha.toml',),
        layout_adapters=(),
    )
    with using_policy(policy):
        assert discover_cosecha_manifest(start_path=tmp_path) == (
            custom_manifest.resolve()
        )


def test_resolve_workspace_for_tests_layout_uses_project_root_and_tests_anchor(
    tmp_path,
) -> None:
    manifest_path = tmp_path / 'tests' / 'cosecha.toml'
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        '[manifest]\nschema_version = 1\n',
        encoding='utf-8',
    )
    (tmp_path / 'src').mkdir()

    workspace = resolve_workspace(start_path=tmp_path)

    assert workspace.workspace_root == tmp_path.resolve()
    assert workspace.knowledge_anchor == (tmp_path / 'tests').resolve()
    assert workspace.import_environment.locations[0].path == (
        tmp_path / 'tests'
    ).resolve()


def test_workspace_fingerprint_is_portable_between_clones(tmp_path) -> None:
    first_root = tmp_path / 'clone-a'
    second_root = tmp_path / 'clone-b'
    for root in (first_root, second_root):
        manifest_path = root / 'tests' / 'cosecha.toml'
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text(
            '[manifest]\nschema_version = 1\n',
            encoding='utf-8',
        )
        (root / 'src').mkdir()

    first_workspace = resolve_workspace(start_path=first_root)
    second_workspace = resolve_workspace(start_path=second_root)

    assert first_workspace.fingerprint == second_workspace.fingerprint


def test_validate_workspace_root_rejects_roots_beyond_max_distance(
    tmp_path,
) -> None:
    manifest_path = tmp_path / 'nested' / 'tests' / 'cosecha.toml'
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        '[manifest]\nschema_version = 1\n',
        encoding='utf-8',
    )

    with pytest.raises(WorkspaceResolutionError):
        validate_workspace_root(
            tmp_path,
            manifest_path,
            max_distance=1,
        )


def test_build_execution_context_uses_runtime_profile_overrides(
    tmp_path,
) -> None:
    manifest_path = tmp_path / 'tests' / 'cosecha.toml'
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        '[manifest]\nschema_version = 1\n',
        encoding='utf-8',
    )
    workspace = resolve_workspace(start_path=tmp_path)

    execution_context = build_execution_context(
        workspace,
        RuntimeProfileSpec(
            id='shadow',
            execution_root='.shadow/run',
            knowledge_storage_root='.shadow/kb',
        ),
    )

    assert execution_context.execution_root == (
        tmp_path / '.shadow' / 'run'
    ).resolve()
    assert execution_context.knowledge_storage_root == (
        tmp_path / '.shadow' / 'kb'
    ).resolve()
