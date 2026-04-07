from __future__ import annotations

import sys

from typing import TYPE_CHECKING

from cosecha.workspace import CodeLocation, LayoutAdaptation, LayoutMatch


__version__ = '0.1.0'


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path


class PythonConventionalLayoutAdapter:
    adapter_name = 'python_conventional'
    priority = 100

    def match(
        self,
        *,
        manifest_path: Path | None,
        declaration,
        candidate_root: Path,
        evidence_path: Path | None,
    ) -> LayoutMatch | None:
        del declaration, candidate_root
        anchor = manifest_path
        if anchor is None and evidence_path is not None:
            anchor = evidence_path
        if anchor is None:
            return None

        root_container = anchor.parent.resolve()
        if root_container.name == '.cosecha':
            root_container = root_container.parent
        if (
            anchor.name == 'kb.db' and root_container.name == 'tests'
        ) or root_container.name == 'tests':
            workspace_root = root_container.parent
            knowledge_anchor = root_container
        else:
            workspace_root = root_container
            knowledge_anchor = root_container

        return LayoutMatch(
            adapter_name=self.adapter_name,
            priority=self.priority,
            adaptation=LayoutAdaptation(
                workspace_root=workspace_root,
                knowledge_anchor=knowledge_anchor,
                code_locations=_build_python_code_locations(
                    workspace_root=workspace_root,
                    knowledge_anchor=knowledge_anchor,
                ),
            ),
        )


def _build_python_code_locations(
    *,
    workspace_root: Path,
    knowledge_anchor: Path,
) -> tuple[CodeLocation, ...]:
    locations: list[CodeLocation] = []
    anchor_role = 'tests' if knowledge_anchor.name == 'tests' else 'source'
    for candidate, role in (
        (knowledge_anchor, anchor_role),
        (workspace_root / 'src', 'source'),
        (workspace_root, 'source'),
    ):
        if candidate.exists():
            locations.append(
                CodeLocation(path=candidate.resolve(), role=role),  # type: ignore[arg-type]
            )

    if workspace_root.exists():
        for sibling in sorted(workspace_root.iterdir()):
            if not sibling.is_dir() or sibling == knowledge_anchor:
                continue
            for child_name, role in (('src', 'source'), ('tests', 'tests')):
                candidate = sibling / child_name
                if candidate.exists():
                    locations.append(
                        CodeLocation(
                            path=candidate.resolve(),
                            role=role,  # type: ignore[arg-type]
                        ),
                    )

    locations.extend(
        CodeLocation(
            path=site_packages_path.resolve(),
            role='vendored',
        )
        for site_packages_path in _discover_workspace_site_packages(
            workspace_root,
        )
    )

    return tuple(locations)


def _discover_workspace_site_packages(
    workspace_root: Path,
) -> tuple[Path, ...]:
    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    search_patterns = (
        f'.venv/lib/{version_name}/site-packages',
        f'venv/lib/{version_name}/site-packages',
        f'venv{sys.version_info.major}.{sys.version_info.minor}/lib/{version_name}/site-packages',
        f'venv{sys.version_info.major}{sys.version_info.minor}/lib/{version_name}/site-packages',
    )
    discovered_paths: list[Path] = []
    for pattern in search_patterns:
        discovered_paths.extend(
            candidate
            for candidate in sorted(workspace_root.glob(pattern))
            if candidate.exists()
        )

    if discovered_paths:
        return tuple(discovered_paths)

    fallback_patterns = (
        '.venv/lib/python*/site-packages',
        'venv/lib/python*/site-packages',
        'venv*/lib/python*/site-packages',
    )
    for pattern in fallback_patterns:
        for candidate in sorted(workspace_root.glob(pattern)):
            if not candidate.exists():
                continue
            if candidate.parent.name != version_name:
                continue
            discovered_paths.append(candidate)

    return tuple(discovered_paths)
