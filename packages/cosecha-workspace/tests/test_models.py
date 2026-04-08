from __future__ import annotations

import importlib

from cosecha.workspace import models as workspace_models


FINGERPRINT_HEX_LENGTH = 64


def test_canonical_json_is_stable_and_ascii() -> None:
    importlib.reload(workspace_models)
    payload = {'b': 'ñ', 'a': 1}
    assert workspace_models._canonical_json(payload) == (
        '{"a":1,"b":"\\u00f1"}'
    )


def test_code_location_workspace_declaration_and_import_environment_roundtrip(
    tmp_path,
) -> None:
    location = workspace_models.CodeLocation(
        path=tmp_path / 'src',
        role='source',
        importable=True,
    )
    location_dict = location.to_dict()
    restored_location = workspace_models.CodeLocation.from_dict(location_dict)
    assert restored_location.path == location.path
    assert restored_location.role == 'source'
    assert restored_location.importable is True

    declaration = workspace_models.WorkspaceDeclaration(
        root='.',
        knowledge_anchor='tests',
        locations=(location,),
    )
    declaration_dict = declaration.to_dict()
    restored_declaration = workspace_models.WorkspaceDeclaration.from_dict(
        declaration_dict,
    )
    assert restored_declaration == declaration

    filtered_declaration = workspace_models.WorkspaceDeclaration.from_dict(
        {
            'root': None,
            'knowledge_anchor': None,
            'locations': [location_dict, 'invalid'],
        },
    )
    assert filtered_declaration.locations == (location,)

    import_environment = workspace_models.ImportEnvironment(
        locations=(location,),
    )
    import_environment_dict = import_environment.to_dict()
    restored_environment = workspace_models.ImportEnvironment.from_dict(
        import_environment_dict,
    )
    assert restored_environment == import_environment

    filtered_environment = workspace_models.ImportEnvironment.from_dict(
        {'locations': [location_dict, 1]},
    )
    assert filtered_environment.locations == (location,)


def test_workspace_provenance_and_execution_context_roundtrip(
    tmp_path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    workspace_root.mkdir()
    source_path = workspace_root / 'src'
    source_path.mkdir()
    tests_path = workspace_root / 'tests'
    tests_path.mkdir()
    manifest_path = tests_path / 'cosecha.toml'
    manifest_path.write_text(
        '[manifest]\nschema_version = 1\n', encoding='utf-8',
    )

    code_location = workspace_models.CodeLocation(
        path=source_path,
        role='source',
        importable=True,
    )
    adaptation = workspace_models.LayoutAdaptation(
        workspace_root=workspace_root,
        knowledge_anchor=tests_path,
        code_locations=(code_location,),
    )
    match = workspace_models.LayoutMatch(
        adapter_name='adapter',
        priority=100,
        adaptation=adaptation,
    )
    shadowed = workspace_models.ShadowedCodeLocation(
        path=source_path,
        kept_by_adapter='adapter',
        shadowed_by_adapter='other',
    )
    ignored = workspace_models.IgnoredRootContribution(
        adapter_name='other',
        priority=50,
        workspace_root=workspace_root / 'other',
        knowledge_anchor=None,
    )
    provenance = workspace_models.WorkspaceProvenance(
        manifest_discovered_from=manifest_path,
        root_winner_adapter='adapter',
        adapter_matches=(match,),
        shadowed_locations=(shadowed,),
        ignored_root_contributions=(ignored,),
    )
    workspace = workspace_models.EffectiveWorkspace(
        manifest_path=manifest_path,
        workspace_root=workspace_root,
        knowledge_anchor=tests_path,
        import_environment=workspace_models.ImportEnvironment(
            locations=(code_location,),
        ),
        declaration=workspace_models.WorkspaceDeclaration(
            root='.',
            knowledge_anchor='tests',
            locations=(code_location,),
        ),
        provenance=provenance,
    )

    workspace_dict = workspace.to_dict()
    restored_workspace = workspace_models.EffectiveWorkspace.from_dict(
        workspace_dict,
    )

    assert restored_workspace.workspace_root == workspace.workspace_root
    assert restored_workspace.knowledge_anchor == workspace.knowledge_anchor
    assert (
        restored_workspace.import_environment.locations[0].path == source_path
    )
    assert restored_workspace.provenance.root_winner_adapter == 'adapter'
    assert (
        restored_workspace.provenance.adapter_matches[
            0
        ].adaptation.workspace_root
        == workspace_root
    )
    assert (
        restored_workspace.provenance.shadowed_locations[0].kept_by_adapter
        == 'adapter'
    )
    assert (
        restored_workspace.provenance.ignored_root_contributions[0].reason
        == 'ignored_by_higher_priority'
    )
    assert len(workspace.fingerprint) == FINGERPRINT_HEX_LENGTH
    assert workspace.fingerprint == restored_workspace.fingerprint

    execution_context = workspace_models.ExecutionContext(
        execution_root=workspace_root / '.shadow' / 'run',
        knowledge_storage_root=workspace_root / '.shadow' / 'kb',
        shadow_root=workspace_root / '.shadow',
        invocation_id='inv-1',
        workspace_fingerprint=workspace.fingerprint,
    )
    execution_context_dict = execution_context.to_dict()
    restored_execution_context = workspace_models.ExecutionContext.from_dict(
        execution_context_dict,
    )
    assert restored_execution_context == execution_context

    minimal_execution_context = workspace_models.ExecutionContext.from_dict(
        {
            'execution_root': str(workspace_root),
            'knowledge_storage_root': str(tests_path),
        },
    )
    assert minimal_execution_context.shadow_root is None
    assert minimal_execution_context.invocation_id is None
