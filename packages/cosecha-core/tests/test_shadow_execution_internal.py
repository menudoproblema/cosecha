from __future__ import annotations

from types import SimpleNamespace

import pytest

from pathlib import Path

from cosecha.core.shadow_execution import (
    ShadowExecutionContext,
    resolve_shadow_execution_context,
)
from cosecha.workspace import ExecutionContext


def test_shadow_execution_context_roundtrip_and_for_session(tmp_path: Path) -> None:
    context = ShadowExecutionContext(
        root_path=tmp_path / 'shadow' / 'session-1',
        knowledge_storage_root=tmp_path / '.cosecha',
    )
    restored = ShadowExecutionContext.from_dict(context.to_dict())
    assert restored == context

    for_session = ShadowExecutionContext.for_session(
        knowledge_storage_root=tmp_path / '.cosecha',
        session_id='session-2',
    )
    assert for_session.root_path == (
        tmp_path / '.cosecha' / 'shadow' / 'session-2'
    ).resolve()


def test_shadow_execution_context_requires_knowledge_root_for_persistent_paths(
    tmp_path: Path,
) -> None:
    context = ShadowExecutionContext(root_path=tmp_path / 'shadow').materialize()
    with pytest.raises(RuntimeError, match='explicit knowledge_storage_root'):
        context.persistent_component_dir('cosecha.provider.ssl')


def test_shadow_execution_context_materializes_alias_as_directory_when_symlink_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(Path, 'symlink_to', lambda *args, **kwargs: (_ for _ in ()).throw(OSError('no symlink')))
    context = ShadowExecutionContext(root_path=tmp_path / 'shadow').materialize()
    assert context.coverage_legacy_alias_dir.exists()
    assert context.coverage_legacy_alias_dir.is_dir()


def test_shadow_cleanup_policy_handles_edge_cases(tmp_path: Path) -> None:
    context = ShadowExecutionContext(
        root_path=tmp_path / 'shadow' / 'session-1',
        knowledge_storage_root=tmp_path / '.cosecha',
    ).materialize()

    keep_capability = SimpleNamespace(
        component_id='cosecha.keep',
        ephemeral_domain='runtime',
        cleanup_on_success=True,
    )
    move_capability = SimpleNamespace(
        component_id='cosecha.move',
        ephemeral_domain='runtime',
        cleanup_on_success=False,
    )
    kept_namespace = context.runtime_component_dir(keep_capability.component_id)
    kept_namespace.mkdir(parents=True, exist_ok=True)
    moved_namespace = context.runtime_component_dir(move_capability.component_id)
    moved_namespace.mkdir(parents=True, exist_ok=True)
    (moved_namespace / 'artifact.txt').write_text('payload', encoding='utf-8')

    preexisting_target = context.preserved_artifacts_component_dir(
        move_capability.component_id,
    )
    preexisting_target.mkdir(parents=True, exist_ok=True)
    (preexisting_target / 'stale.txt').write_text('stale', encoding='utf-8')

    context.cleanup(
        preserve=True,
        session_succeeded=True,
        capabilities={
            'invalid_component': SimpleNamespace(
                component_id=123,
                ephemeral_domain='runtime',
            ),
            'invalid_domain': SimpleNamespace(
                component_id='cosecha.invalid',
                ephemeral_domain=object(),
            ),
            'missing_namespace': SimpleNamespace(
                component_id='cosecha.missing',
                ephemeral_domain='runtime',
                cleanup_on_success=False,
            ),
            keep_capability.component_id: keep_capability,
            move_capability.component_id: move_capability,
        },
    )

    assert kept_namespace.exists()
    assert moved_namespace.exists() is False
    assert (preexisting_target / 'artifact.txt').exists()
    assert (preexisting_target / 'stale.txt').exists() is False


def test_shadow_execution_context_validates_domain_and_resolves_from_env(
    tmp_path: Path,
) -> None:
    context = ShadowExecutionContext(root_path=tmp_path / 'shadow')
    with pytest.raises(ValueError, match='safe path segment'):
        context.ephemeral_domain_dir('INVALID/DOMAIN')

    execution_context = ExecutionContext(
        execution_root=tmp_path / 'exec',
        knowledge_storage_root=tmp_path / '.cosecha',
        workspace_fingerprint='workspace-1',
    )
    updated, shadow_context = resolve_shadow_execution_context(
        execution_context,
        session_id='session-3',
        env={
            'COSECHA_SHADOW_ROOT': str(tmp_path / 'external-shadow'),
            'COSECHA_KNOWLEDGE_STORAGE_ROOT': str(tmp_path / '.external-knowledge'),
        },
    )
    assert updated.shadow_root == (tmp_path / 'external-shadow').resolve()
    assert shadow_context.root_path == (tmp_path / 'external-shadow').resolve()
