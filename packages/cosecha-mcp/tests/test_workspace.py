from __future__ import annotations

import sys

from pathlib import Path


PACKAGES_ROOT = Path(__file__).resolve().parents[2]
for package_dir in sorted(PACKAGES_ROOT.iterdir()):
    if not package_dir.is_dir():
        continue
    for child_name in ('src', 'tests'):
        candidate = package_dir / child_name
        if not candidate.exists():
            continue
        rendered_path = str(candidate)
        if rendered_path in sys.path:
            continue
        sys.path.insert(0, rendered_path)

from cosecha_mcp.workspace import (
    # Imported after mutating sys.path for local package discovery in tests.
    normalize_workspace_relative_paths,
    resolve_cosecha_workspace,
)


def test_resolve_cosecha_workspace_prefers_tests_root(
    cosecha_workspace,
) -> None:
    tests_root = cosecha_workspace.project_path / 'tests'
    cosecha_workspace.write_project_file('tests/cosecha.toml', '')
    cosecha_workspace.write_project_file('tests/.cosecha/kb.db', '')

    workspace = resolve_cosecha_workspace(cosecha_workspace.project_path)

    assert workspace.project_path == cosecha_workspace.project_path.resolve()
    assert workspace.root_path == tests_root.resolve()
    assert workspace.manifest_path == (tests_root / 'cosecha.toml').resolve()
    assert workspace.knowledge_base_path == (
        tests_root / '.cosecha' / 'kb.db'
    ).resolve()


def test_resolve_cosecha_workspace_supports_root_layout(
    cosecha_workspace,
) -> None:
    cosecha_workspace.write_project_file('cosecha.toml', '')
    cosecha_workspace.write_project_file('.cosecha/kb.db', '')

    workspace = resolve_cosecha_workspace(cosecha_workspace.project_path)

    assert workspace.project_path == cosecha_workspace.project_path.resolve()
    assert workspace.root_path == cosecha_workspace.project_path.resolve()
    assert workspace.manifest_path == (
        cosecha_workspace.project_path / 'cosecha.toml'
    ).resolve()
    assert workspace.knowledge_base_path == (
        cosecha_workspace.project_path / '.cosecha' / 'kb.db'
    ).resolve()


def test_normalize_workspace_relative_paths_strips_root_name() -> None:
    root_path = Path('/tmp/project/tests')

    normalized_paths = normalize_workspace_relative_paths(
        root_path=root_path,
        raw_paths=['tests/unit/sample.feature', 'unit/example.feature'],
    )

    assert normalized_paths == ('unit/sample.feature', 'unit/example.feature')
