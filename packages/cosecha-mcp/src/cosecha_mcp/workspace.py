from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from cosecha.workspace import build_execution_context, resolve_workspace

from cosecha.core.knowledge_base import resolve_knowledge_base_path


@dataclass(slots=True, frozen=True)
class CosechaWorkspacePaths:
    project_path: Path
    root_path: Path
    manifest_path: Path | None
    knowledge_base_path: Path
    workspace_root: Path | None = None
    knowledge_anchor: Path | None = None
    execution_root: Path | None = None
    workspace_fingerprint: str | None = None

    def __post_init__(self) -> None:
        workspace_root = (
            self.root_path.parent
            if self.workspace_root is None and self.root_path.name == 'tests'
            else self.root_path
            if self.workspace_root is None
            else self.workspace_root
        )
        knowledge_anchor = (
            self.root_path
            if self.knowledge_anchor is None
            else self.knowledge_anchor
        )
        execution_root = (
            workspace_root
            if self.execution_root is None
            else self.execution_root
        )

        object.__setattr__(self, 'workspace_root', workspace_root)
        object.__setattr__(self, 'knowledge_anchor', knowledge_anchor)
        object.__setattr__(self, 'execution_root', execution_root)

    def to_dict(self) -> dict[str, object]:
        return {
            'execution_root': str(self.execution_root),
            'knowledge_anchor': str(self.knowledge_anchor),
            'knowledge_base_path': str(self.knowledge_base_path),
            'manifest_path': (
                None if self.manifest_path is None else str(self.manifest_path)
            ),
            'project_path': str(self.project_path),
            'root_path': str(self.root_path),
            'workspace_fingerprint': self.workspace_fingerprint,
            'workspace_root': str(self.workspace_root),
        }


def resolve_cosecha_workspace(
    start_path: str | Path | None = None,
) -> CosechaWorkspacePaths:
    workspace = resolve_workspace(start_path=start_path)
    execution_context = build_execution_context(workspace)
    return CosechaWorkspacePaths(
        project_path=workspace.workspace_root,
        root_path=workspace.knowledge_anchor,
        workspace_root=workspace.workspace_root,
        knowledge_anchor=workspace.knowledge_anchor,
        execution_root=execution_context.execution_root,
        manifest_path=workspace.manifest_path,
        knowledge_base_path=resolve_knowledge_base_path(
            workspace.workspace_root,
            knowledge_storage_root=execution_context.knowledge_storage_root,
        ),
        workspace_fingerprint=workspace.fingerprint,
    )


def normalize_workspace_relative_paths(
    *,
    root_path: Path,
    raw_paths: list[str] | tuple[str, ...] | None,
) -> tuple[str, ...]:
    if not raw_paths:
        return ()

    normalized_paths: list[str] = []
    root_path_abs = root_path.resolve()
    for raw_path in raw_paths:
        input_path = Path(raw_path)
        if input_path.is_absolute():
            resolved_path = input_path.resolve()
            try:
                relative_path = resolved_path.relative_to(root_path_abs)
            except ValueError as error:
                msg = (
                    'Path selector must point inside the active Cosecha root '
                    f'{root_path}: {raw_path!r}'
                )
                raise ValueError(msg) from error
            normalized_paths.append(relative_path.as_posix())
            continue

        if input_path.parts and input_path.parts[0] == root_path.name:
            normalized_paths.append(Path(*input_path.parts[1:]).as_posix())
            continue

        normalized_paths.append(input_path.as_posix())

    return tuple(normalized_paths)
