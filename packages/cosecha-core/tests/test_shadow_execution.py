from __future__ import annotations

from cosecha.core.shadow_execution import (
    ShadowExecutionContext,
    bind_shadow_execution_context,
    resolve_shadow_execution_context,
    shadow_execution_context_from_env,
)
from cosecha.workspace import ExecutionContext


def test_shadow_execution_context_derives_paths_and_env(tmp_path) -> None:
    shadow = ShadowExecutionContext(
        root_path=tmp_path / 'shadow' / 'session-1',
        knowledge_storage_root=tmp_path / '.cosecha',
    )

    shadow.materialize()

    assert shadow.runtime_state_dir == shadow.root_path / 'runtime'
    assert shadow.coverage_dir == (
        shadow.root_path
        / 'instrumentation'
        / 'cosecha.instrumentation.coverage'
    )
    assert shadow.metadata_file == (
        shadow.root_path / 'instrumentation' / 'run-metadata.json'
    )
    assert shadow.instrumentation_component_dir('cosecha.instrumentation.coverage') == (
        shadow.coverage_dir
    )
    assert shadow.runtime_component_dir('cosecha.provider.ssl') == (
        shadow.root_path / 'runtime' / 'cosecha.provider.ssl'
    )
    assert shadow.component_ephemeral_dir('cosecha.engine.cache', 'cache') == (
        shadow.root_path / 'cache' / 'cosecha.engine.cache'
    )
    assert shadow.env()['COSECHA_SHADOW_ROOT'] == str(shadow.root_path)
    assert shadow.env()['COSECHA_KNOWLEDGE_STORAGE_ROOT'] == str(
        tmp_path / '.cosecha',
    )
    assert shadow.runtime_state_dir.exists()
    assert shadow.coverage_dir.exists()
    assert shadow.coverage_legacy_alias_dir.exists()
    assert shadow.coverage_legacy_alias_dir.resolve() == shadow.coverage_dir


def test_shadow_execution_context_routes_persistent_storage_to_knowledge_root(
    tmp_path,
) -> None:
    shadow = ShadowExecutionContext(
        root_path=tmp_path / 'shadow' / 'session-1',
        knowledge_storage_root=tmp_path / '.cosecha',
    ).materialize()

    assert shadow.persistent_component_dir('cosecha.instrumentation.coverage') == (
        (tmp_path / '.cosecha' / 'components' / 'cosecha.instrumentation.coverage')
        .resolve()
    )


def test_shadow_execution_context_from_env_restores_knowledge_storage_root(
    tmp_path,
) -> None:
    env = {
        'COSECHA_SHADOW_ROOT': str(tmp_path / 'shadow' / 'session-1'),
        'COSECHA_KNOWLEDGE_STORAGE_ROOT': str(tmp_path / '.cosecha'),
    }

    shadow = shadow_execution_context_from_env(env)

    assert shadow is not None
    assert shadow.root_path == (tmp_path / 'shadow' / 'session-1').resolve()
    assert shadow.knowledge_storage_root == (tmp_path / '.cosecha').resolve()


def test_resolve_shadow_execution_context_binds_root_to_execution_context(
    tmp_path,
) -> None:
    execution_context = ExecutionContext(
        execution_root=tmp_path,
        knowledge_storage_root=tmp_path / '.cosecha',
        workspace_fingerprint='ws-1',
    )

    updated_execution_context, shadow = resolve_shadow_execution_context(
        execution_context,
        session_id='session-1',
    )

    assert updated_execution_context.shadow_root == shadow.root_path
    assert shadow.root_path == (
        (tmp_path / '.cosecha' / 'shadow' / 'session-1').resolve()
    )


def test_bind_shadow_execution_context_preserves_execution_fields(
    tmp_path,
) -> None:
    execution_context = ExecutionContext(
        execution_root=tmp_path / 'exec',
        knowledge_storage_root=tmp_path / '.cosecha',
        invocation_id='inv-1',
        workspace_fingerprint='ws-1',
    )
    shadow = ShadowExecutionContext(
        root_path=tmp_path / 'shadow' / 'session-1',
    )

    bound = bind_shadow_execution_context(execution_context, shadow)

    assert bound.execution_root == execution_context.execution_root
    assert (
        bound.knowledge_storage_root
        == execution_context.knowledge_storage_root
    )
    assert bound.invocation_id == 'inv-1'
    assert bound.workspace_fingerprint == 'ws-1'
    assert bound.shadow_root == shadow.root_path
