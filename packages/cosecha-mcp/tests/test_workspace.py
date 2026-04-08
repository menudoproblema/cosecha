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
    tmp_path: Path,
) -> None:
    tests_root = tmp_path / 'tests'
    tests_root.mkdir(parents=True)
    (tests_root / 'cosecha.toml').write_text('', encoding='utf-8')
    (tests_root / '.cosecha').mkdir(parents=True)
    (tests_root / '.cosecha' / 'kb.db').write_text('', encoding='utf-8')

    workspace = resolve_cosecha_workspace(tmp_path)

    assert workspace.project_path == tmp_path.resolve()
    assert workspace.root_path == tests_root.resolve()
    assert workspace.manifest_path == (tests_root / 'cosecha.toml').resolve()
    assert workspace.knowledge_base_path == (
        tmp_path / '.cosecha' / 'kb.db'
    ).resolve()


def test_resolve_cosecha_workspace_supports_root_layout(
    tmp_path: Path,
) -> None:
    (tmp_path / 'cosecha.toml').write_text('', encoding='utf-8')
    (tmp_path / '.cosecha').mkdir(parents=True)
    (tmp_path / '.cosecha' / 'kb.db').write_text('', encoding='utf-8')

    workspace = resolve_cosecha_workspace(tmp_path)

    assert workspace.project_path == tmp_path.resolve()
    assert workspace.root_path == tmp_path.resolve()
    assert workspace.manifest_path == (
        tmp_path / 'cosecha.toml'
    ).resolve()
    assert workspace.knowledge_base_path == (
        tmp_path / '.cosecha' / 'kb.db'
    ).resolve()


def test_normalize_workspace_relative_paths_strips_root_name() -> None:
    root_path = Path('/tmp/project/tests')

    normalized_paths = normalize_workspace_relative_paths(
        root_path=root_path,
        raw_paths=['tests/unit/sample.feature', 'unit/example.feature'],
    )

    assert normalized_paths == ('unit/sample.feature', 'unit/example.feature')


def test_normalize_workspace_relative_paths_returns_empty_for_none() -> None:
    assert normalize_workspace_relative_paths(
        root_path=Path('/tmp/project/tests'),
        raw_paths=None,
    ) == ()


def test_normalize_workspace_relative_paths_supports_absolute_paths(
    tmp_path: Path,
) -> None:
    root_path = (tmp_path / 'project' / 'tests').resolve()
    target = (root_path / 'unit' / 'sample.feature').resolve()
    target.parent.mkdir(parents=True)
    target.write_text('Feature: Demo\n', encoding='utf-8')

    normalized = normalize_workspace_relative_paths(
        root_path=root_path,
        raw_paths=[str(target)],
    )

    assert normalized == ('unit/sample.feature',)


def test_normalize_workspace_relative_paths_rejects_absolute_paths_outside_root(
    tmp_path: Path,
) -> None:
    root_path = (tmp_path / 'project' / 'tests').resolve()
    root_path.mkdir(parents=True)
    outside = (tmp_path / 'outside.feature').resolve()
    outside.write_text('Feature: Outside\n', encoding='utf-8')

    try:
        normalize_workspace_relative_paths(
            root_path=root_path,
            raw_paths=[str(outside)],
        )
    except ValueError as error:
        assert 'must point inside the active Cosecha root' in str(error)
    else:  # pragma: no cover - defensive assertion for clearer failure output
        raise AssertionError('expected ValueError for path outside root')
