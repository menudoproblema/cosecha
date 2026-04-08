from __future__ import annotations

import importlib

from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.workspace import discovery
from cosecha.workspace.models import (
    CodeLocation,
    EffectiveWorkspace,
    ImportEnvironment,
    LayoutAdaptation,
    LayoutMatch,
    WorkspaceDeclaration,
)


EXPECTED_TWO_ITEMS = 2


class _StaticAdapter:
    def __init__(
        self,
        *,
        adapter_name: str,
        priority: int,
        adaptation: LayoutAdaptation | None,
    ) -> None:
        self.adapter_name = adapter_name
        self.priority = priority
        self._adaptation = adaptation

    def match(
        self,
        *,
        manifest_path: Path | None,
        declaration: WorkspaceDeclaration,
        candidate_root: Path,
        evidence_path: Path | None,
    ) -> LayoutMatch | None:
        del manifest_path, declaration, candidate_root, evidence_path
        if self._adaptation is None:
            return None
        return LayoutMatch(
            adapter_name=self.adapter_name,
            priority=self.priority,
            adaptation=self._adaptation,
        )


def test_discover_manifest_with_explicit_manifest_file_and_search_policy(
    tmp_path: Path,
) -> None:
    importlib.reload(discovery)
    manifest_path = tmp_path / 'tests' / 'cosecha.toml'
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        '[manifest]\nschema_version = 1\n', encoding='utf-8',
    )

    assert discovery.discover_cosecha_manifest(
        manifest_file=manifest_path,
    ) == (manifest_path.resolve())
    assert (
        discovery.discover_cosecha_manifest(
            manifest_file=tmp_path / 'missing.toml',
        )
        is None
    )

    nested = tmp_path / 'nested'
    nested.mkdir()
    file_path = nested / 'runner.py'
    file_path.write_text('print(1)\n', encoding='utf-8')
    policy = discovery.WorkspaceResolutionPolicy(
        manifest_candidate_paths=('tests/cosecha.toml',),
        layout_adapters=(),
    )
    with discovery.using_policy(policy):
        assert discovery.discover_cosecha_manifest(start_path=file_path) == (
            manifest_path.resolve()
        )
    assert discovery.get_active_policy() is discovery.DEFAULT_POLICY


def test_discover_workspace_evidence_and_manifest_validation_branches(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / 'project'
    project_root.mkdir()
    tests_root = project_root / 'tests'
    tests_root.mkdir()
    evidence_path = tests_root / '.cosecha' / 'kb.db'
    evidence_path.parent.mkdir(parents=True)
    evidence_path.write_text('', encoding='utf-8')

    disabled_policy = discovery.WorkspaceResolutionPolicy(
        allow_knowledge_base_fallback=False,
        layout_adapters=(),
    )
    assert (
        discovery._discover_workspace_evidence(
            project_root,
            policy=disabled_policy,
        )
        is None
    )

    enabled_policy = discovery.WorkspaceResolutionPolicy(layout_adapters=())
    assert (
        discovery._discover_workspace_evidence(
            project_root,
            policy=enabled_policy,
        )
        == evidence_path.resolve()
    )

    manifest_path = tests_root / 'cosecha.toml'
    manifest_path.write_text(
        '[manifest]\nschema_version = 1\n', encoding='utf-8',
    )
    discovery.validate_workspace_root(
        project_root, manifest_path, max_distance=1,
    )

    with pytest.raises(discovery.WorkspaceResolutionError):
        discovery.validate_workspace_root(
            project_root / 'outside',
            manifest_path,
            max_distance=1,
        )
    with pytest.raises(discovery.WorkspaceResolutionError):
        discovery.validate_workspace_root(
            project_root, manifest_path, max_distance=0,
        )
    discovery.validate_workspace_root(project_root, None, max_distance=0)


def test_load_workspace_declaration_and_build_explicit_adaptation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = tmp_path / 'tests' / 'cosecha.toml'
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        '\n'.join(
            (
                '[manifest]',
                'schema_version = 1',
                '',
                '[workspace]',
                'root = ".."',
                'knowledge_anchor = "tests"',
                '',
                '[[workspace.locations]]',
                'path = "src"',
                'role = "source"',
                'importable = true',
                '',
                '[[workspace.locations]]',
                'path = 123',
            ),
        )
        + '\n',
        encoding='utf-8',
    )

    declaration = discovery._load_workspace_declaration(manifest_path)
    assert declaration.root == '..'
    assert declaration.knowledge_anchor == 'tests'
    assert declaration.locations == (
        CodeLocation(path=Path('src'), role='source', importable=True),
    )

    assert (
        discovery._load_workspace_declaration(None) == WorkspaceDeclaration()
    )
    assert (
        discovery._load_workspace_declaration(tmp_path / 'missing.toml')
        == WorkspaceDeclaration()
    )

    malformed_manifest = tmp_path / 'malformed.toml'
    malformed_manifest.write_text(
        '[workspace]\nlocations = "bad"\n', encoding='utf-8',
    )
    assert (
        discovery._load_workspace_declaration(
            malformed_manifest,
        )
        == WorkspaceDeclaration()
    )
    non_dict_locations_manifest = tmp_path / 'non-dict-locations.toml'
    non_dict_locations_manifest.write_text(
        '[workspace]\nlocations = ["bad"]\n',
        encoding='utf-8',
    )
    assert (
        discovery._load_workspace_declaration(
            non_dict_locations_manifest,
        )
        == WorkspaceDeclaration()
    )

    explicit = discovery._build_explicit_adaptation(
        declaration,
        manifest_path=manifest_path,
    )
    assert explicit is not None
    assert explicit.workspace_root == tmp_path.resolve()
    assert explicit.knowledge_anchor == (tmp_path / 'tests').resolve()
    assert explicit.code_locations[0].path == (tmp_path / 'src').resolve()

    monkeypatch.chdir(tmp_path)
    fallback_adaptation = discovery._build_explicit_adaptation(
        WorkspaceDeclaration(
            root='.', locations=(CodeLocation(path=Path('src')),),
        ),
        manifest_path=None,
    )
    assert fallback_adaptation is not None
    assert fallback_adaptation.workspace_root == tmp_path.resolve()

    assert (
        discovery._build_explicit_adaptation(
            WorkspaceDeclaration(),
            manifest_path=manifest_path,
        )
        is None
    )


def test_merge_adaptations_select_winner_and_conflicts(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    workspace_root.mkdir()
    src_a = workspace_root / 'src_a'
    src_b = workspace_root / 'src_b'
    src_a.mkdir()
    src_b.mkdir()

    top_match = LayoutMatch(
        adapter_name='top',
        priority=100,
        adaptation=LayoutAdaptation(
            workspace_root=workspace_root,
            knowledge_anchor=workspace_root,
            code_locations=(
                CodeLocation(path=src_a, role='source'),
                CodeLocation(path=src_b, role='source'),
            ),
        ),
    )
    lower_match = LayoutMatch(
        adapter_name='lower',
        priority=50,
        adaptation=LayoutAdaptation(
            workspace_root=workspace_root / 'other',
            knowledge_anchor=None,
            code_locations=(CodeLocation(path=src_b, role='source'),),
        ),
    )
    merged, provenance = discovery._merge_adaptations(
        (lower_match, top_match),
        manifest_path=workspace_root / 'tests' / 'cosecha.toml',
    )

    assert merged.workspace_root == workspace_root
    assert len(merged.code_locations) == EXPECTED_TWO_ITEMS
    assert provenance.root_winner_adapter == 'top'
    assert len(provenance.shadowed_locations) == 1
    assert provenance.shadowed_locations[0].kept_by_adapter == 'top'
    assert len(provenance.ignored_root_contributions) == 1
    assert provenance.ignored_root_contributions[0].adapter_name == 'lower'

    with pytest.raises(discovery.WorkspaceResolutionError):
        discovery._merge_adaptations((), manifest_path=None)

    no_root = LayoutMatch(
        adapter_name='code-only',
        priority=1,
        adaptation=LayoutAdaptation(
            workspace_root=None,
            knowledge_anchor=None,
            code_locations=(CodeLocation(path=src_a),),
        ),
    )
    assert discovery._select_root_winner((no_root,)) is None

    conflict_a = LayoutMatch(
        adapter_name='a',
        priority=10,
        adaptation=LayoutAdaptation(
            workspace_root=workspace_root / 'a',
            knowledge_anchor=workspace_root / 'a',
        ),
    )
    conflict_b = LayoutMatch(
        adapter_name='b',
        priority=10,
        adaptation=LayoutAdaptation(
            workspace_root=workspace_root / 'b',
            knowledge_anchor=workspace_root / 'b',
        ),
    )
    with pytest.raises(discovery.WorkspaceResolutionError):
        discovery._select_root_winner((conflict_a, conflict_b))


def test_load_layout_adapters_materialize_policy_and_context_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded_calls: list[str] = []

    class _EntryPoint:
        def __init__(self, label: str, returned) -> None:
            self._label = label
            self._returned = returned

        def load(self):
            loaded_calls.append(self._label)
            return self._returned

    class _ClassAdapter:
        adapter_name = 'class'
        priority = 1

        def match(self, **_kwargs):
            return None

    instance_adapter = _StaticAdapter(
        adapter_name='instance',
        priority=1,
        adaptation=None,
    )
    monkeypatch.setattr(
        discovery,
        'entry_points',
        lambda *, group: (
            (
                _EntryPoint('class', _ClassAdapter),
                _EntryPoint('instance', instance_adapter),
            )
            if group == discovery.LAYOUT_ADAPTER_ENTRYPOINT_GROUP
            else ()
        ),
    )
    monkeypatch.setitem(discovery._LAYOUT_ADAPTER_CACHE, 'value', None)

    adapters = discovery._load_layout_adapters()
    assert len(adapters) == EXPECTED_TWO_ITEMS
    assert loaded_calls == ['class', 'instance']

    cached_adapters = discovery._load_layout_adapters()
    assert cached_adapters == adapters
    assert loaded_calls == ['class', 'instance']

    materialized = discovery._materialize_policy(
        discovery.WorkspaceResolutionPolicy(layout_adapters=()),
    )
    assert materialized.layout_adapters == adapters

    explicit_policy = discovery.WorkspaceResolutionPolicy(
        layout_adapters=(instance_adapter,),
    )
    assert discovery._materialize_policy(explicit_policy) is explicit_policy

    absolute_base = tmp_path.resolve()
    absolute = discovery._resolve_context_path(tmp_path, absolute_base)
    relative = discovery._resolve_context_path(tmp_path, Path('shadow'))
    assert absolute == absolute_base
    assert relative == (tmp_path / 'shadow').resolve()


def test_build_execution_context_and_resolve_workspace_paths(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    workspace_root.mkdir()
    workspace = EffectiveWorkspace(
        manifest_path=None,
        workspace_root=workspace_root,
        knowledge_anchor=workspace_root / 'tests',
        import_environment=ImportEnvironment(),
    )

    runtime_profile = SimpleNamespace(
        execution_root='run/profile',
        knowledge_storage_root='kb/profile',
    )
    cli_overrides = SimpleNamespace(
        execution_root=workspace_root / '.shadow' / 'run',
        knowledge_storage_root=workspace_root / '.shadow' / 'kb',
    )
    context = discovery.build_execution_context(
        workspace,
        runtime_profile,
        cli_overrides=cli_overrides,
        invocation_id='inv-42',
    )
    assert (
        context.execution_root
        == (workspace_root / '.shadow' / 'run').resolve()
    )
    assert (
        context.knowledge_storage_root
        == (workspace_root / '.shadow' / 'kb').resolve()
    )
    assert context.invocation_id == 'inv-42'
    assert context.workspace_fingerprint == workspace.fingerprint

    default_context = discovery.build_execution_context(workspace)
    assert default_context.execution_root == workspace_root.resolve()
    assert (
        default_context.knowledge_storage_root
        == (workspace_root / '.cosecha').resolve()
    )

    empty_policy = discovery.WorkspaceResolutionPolicy(
        allow_knowledge_base_fallback=False,
        layout_adapters=(),
    )
    with discovery.using_policy(
        empty_policy,
    ), pytest.raises(FileNotFoundError):
        discovery.resolve_workspace(start_path=workspace_root)

    manifest_path = workspace_root / 'tests' / 'cosecha.toml'
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        '[manifest]\nschema_version = 1\n', encoding='utf-8',
    )

    no_root_policy = discovery.WorkspaceResolutionPolicy(
        max_ancestor_distance=5,
        layout_adapters=(
            _StaticAdapter(
                adapter_name='code-only',
                priority=1,
                adaptation=LayoutAdaptation(
                    workspace_root=None,
                    knowledge_anchor=None,
                    code_locations=(
                        CodeLocation(path=workspace_root / 'src'),
                    ),
                ),
            ),
        ),
    )
    with discovery.using_policy(
        no_root_policy,
    ), pytest.raises(discovery.WorkspaceResolutionError):
        discovery.resolve_workspace(start_path=workspace_root)

    resolved_policy = discovery.WorkspaceResolutionPolicy(
        max_ancestor_distance=5,
        layout_adapters=(
            _StaticAdapter(
                adapter_name='adapter',
                priority=100,
                adaptation=LayoutAdaptation(
                    workspace_root=workspace_root,
                    knowledge_anchor=workspace_root / 'tests',
                    code_locations=(
                        CodeLocation(path=workspace_root / 'src'),
                    ),
                ),
            ),
        ),
    )
    with discovery.using_policy(resolved_policy):
        workspace_result = discovery.resolve_workspace(
            start_path=workspace_root,
        )
    assert workspace_result.workspace_root == workspace_root.resolve()
    assert workspace_result.provenance.root_winner_adapter == 'adapter'


def test_resolve_workspace_prioritizes_explicit_workspace_declaration(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / 'project'
    tests_root = workspace_root / 'tests'
    tests_root.mkdir(parents=True)
    manifest_path = tests_root / 'cosecha.toml'
    manifest_path.write_text(
        '\n'.join(
            (
                '[manifest]',
                'schema_version = 1',
                '',
                '[workspace]',
                'root = ".."',
                'knowledge_anchor = "tests"',
                '',
                '[[workspace.locations]]',
                'path = "src"',
                'role = "source"',
                'importable = true',
            ),
        )
        + '\n',
        encoding='utf-8',
    )
    (workspace_root / 'src').mkdir()

    policy = discovery.WorkspaceResolutionPolicy(
        max_ancestor_distance=5,
        layout_adapters=(),
    )
    with discovery.using_policy(policy):
        workspace = discovery.resolve_workspace(start_path=workspace_root)

    assert workspace.workspace_root == workspace_root.resolve()
    assert workspace.knowledge_anchor == tests_root.resolve()
    assert workspace.provenance.root_winner_adapter == 'workspace_declaration'
