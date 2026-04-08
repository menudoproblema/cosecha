from __future__ import annotations

import sys

from cosecha.workspace import LayoutAdaptation, WorkspaceDeclaration
from cosecha_workspace_python import (
    PythonConventionalLayoutAdapter,
    __version__,
    _build_python_code_locations,
    _discover_workspace_site_packages,
)


ADAPTER_PRIORITY = 100


def test_version_and_adapter_contract() -> None:
    adapter = PythonConventionalLayoutAdapter()
    assert __version__ == '0.1.0'
    assert adapter.adapter_name == 'python_conventional'
    assert adapter.priority == ADAPTER_PRIORITY


def test_match_returns_none_without_manifest_or_evidence(
    tmp_path,
) -> None:
    adapter = PythonConventionalLayoutAdapter()
    assert (
        adapter.match(
            manifest_path=None,
            declaration=WorkspaceDeclaration(),
            candidate_root=tmp_path,
            evidence_path=None,
        )
        is None
    )


def test_match_handles_tests_layout_and_cosecha_anchor(tmp_path) -> None:
    adapter = PythonConventionalLayoutAdapter()
    workspace_root = tmp_path / 'project'
    tests_root = workspace_root / 'tests'
    tests_root.mkdir(parents=True)
    manifest_path = tests_root / 'cosecha.toml'
    manifest_path.write_text(
        '[manifest]\nschema_version = 1\n', encoding='utf-8',
    )
    (workspace_root / 'src').mkdir()

    from_manifest = adapter.match(
        manifest_path=manifest_path,
        declaration=WorkspaceDeclaration(),
        candidate_root=workspace_root,
        evidence_path=None,
    )
    assert from_manifest is not None
    assert from_manifest.adaptation.workspace_root == workspace_root.resolve()
    assert from_manifest.adaptation.knowledge_anchor == tests_root.resolve()

    kb_path = tests_root / '.cosecha' / 'kb.db'
    kb_path.parent.mkdir(parents=True)
    kb_path.write_text('', encoding='utf-8')

    from_evidence = adapter.match(
        manifest_path=None,
        declaration=WorkspaceDeclaration(),
        candidate_root=workspace_root,
        evidence_path=kb_path,
    )
    assert from_evidence is not None
    assert from_evidence.adaptation.workspace_root == workspace_root.resolve()
    assert from_evidence.adaptation.knowledge_anchor == tests_root.resolve()


def test_build_python_code_locations_collects_candidates_and_siblings(
    tmp_path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    workspace_root.mkdir()
    knowledge_anchor = workspace_root / 'tests'
    knowledge_anchor.mkdir()
    (workspace_root / 'src').mkdir()
    (workspace_root / 'pkg_a' / 'src').mkdir(parents=True)
    (workspace_root / 'pkg_a' / 'tests').mkdir(parents=True)
    (workspace_root / 'pkg_b').mkdir()
    (workspace_root / 'README.md').write_text('readme\n', encoding='utf-8')

    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    site_packages = (
        workspace_root / '.venv' / 'lib' / version_name / 'site-packages'
    )
    site_packages.mkdir(parents=True)

    locations = _build_python_code_locations(
        workspace_root=workspace_root,
        knowledge_anchor=knowledge_anchor,
    )
    location_pairs = {(location.path, location.role) for location in locations}

    assert (knowledge_anchor.resolve(), 'tests') in location_pairs
    assert ((workspace_root / 'src').resolve(), 'source') in location_pairs
    assert (workspace_root.resolve(), 'source') in location_pairs
    assert (
        (workspace_root / 'pkg_a' / 'src').resolve(),
        'source',
    ) in location_pairs
    assert (
        (workspace_root / 'pkg_a' / 'tests').resolve(),
        'tests',
    ) in location_pairs
    assert (site_packages.resolve(), 'vendored') in location_pairs


def test_discover_workspace_site_packages_primary_and_fallback(
    tmp_path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    workspace_root.mkdir()
    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'

    primary = workspace_root / 'venv' / 'lib' / version_name / 'site-packages'
    primary.mkdir(parents=True)
    assert _discover_workspace_site_packages(workspace_root) == (primary,)

    empty_root = tmp_path / 'fallback'
    empty_root.mkdir()
    fallback = (
        empty_root / 'venv-custom' / 'lib' / version_name / 'site-packages'
    )
    fallback.mkdir(parents=True)
    wrong_version = (
        empty_root / 'venv-custom' / 'lib' / 'python0.0' / 'site-packages'
    )
    wrong_version.mkdir(parents=True)

    assert _discover_workspace_site_packages(empty_root) == (fallback,)

    no_virtualenv_root = tmp_path / 'none'
    no_virtualenv_root.mkdir()
    assert _discover_workspace_site_packages(no_virtualenv_root) == ()


def test_discover_workspace_site_packages_skips_non_existing_candidates(
    tmp_path,
) -> None:
    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    existing = (
        tmp_path / 'venv-custom' / 'lib' / version_name / 'site-packages'
    )
    existing.mkdir(parents=True)
    non_existing = tmp_path / 'missing' / 'site-packages'

    class _FakeRoot:
        def __init__(self, mapping: dict[str, tuple]) -> None:
            self._mapping = mapping

        def glob(self, pattern: str):
            return self._mapping.get(pattern, ())

    fake_root = _FakeRoot(
        {
            '.venv/lib/python*/site-packages': (),
            'venv/lib/python*/site-packages': (),
            'venv*/lib/python*/site-packages': (non_existing, existing),
        },
    )
    assert _discover_workspace_site_packages(fake_root) == (existing,)


def test_match_uses_non_tests_root_when_manifest_in_root(
    tmp_path,
) -> None:
    adapter = PythonConventionalLayoutAdapter()
    workspace_root = tmp_path / 'project'
    workspace_root.mkdir()
    manifest_path = workspace_root / 'cosecha.toml'
    manifest_path.write_text(
        '[manifest]\nschema_version = 1\n', encoding='utf-8',
    )

    match = adapter.match(
        manifest_path=manifest_path,
        declaration=WorkspaceDeclaration(),
        candidate_root=workspace_root,
        evidence_path=None,
    )
    assert match is not None
    adaptation: LayoutAdaptation = match.adaptation
    assert adaptation.workspace_root == workspace_root.resolve()
    assert adaptation.knowledge_anchor == workspace_root.resolve()
