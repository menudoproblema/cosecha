from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from cosecha.core.knowledge_base import resolve_knowledge_base_path


@dataclass(slots=True, frozen=True)
class CosechaWorkspacePaths:
    project_path: Path
    root_path: Path
    manifest_path: Path | None
    knowledge_base_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            'knowledge_base_path': str(self.knowledge_base_path),
            'manifest_path': (
                None if self.manifest_path is None else str(self.manifest_path)
            ),
            'project_path': str(self.project_path),
            'root_path': str(self.root_path),
        }


def resolve_cosecha_workspace(
    start_path: str | Path | None = None,
) -> CosechaWorkspacePaths:
    origin = Path.cwd() if start_path is None else Path(start_path)
    candidate = origin if origin.is_dir() else origin.parent
    candidate = candidate.resolve()

    for current in (candidate, *candidate.parents):
        tests_root = current / 'tests'
        tests_manifest = tests_root / 'cosecha.toml'
        tests_kb = tests_root / '.cosecha' / 'kb.db'
        if tests_manifest.exists() or tests_kb.exists():
            return CosechaWorkspacePaths(
                project_path=current,
                root_path=tests_root,
                manifest_path=(
                    tests_manifest if tests_manifest.exists() else None
                ),
                knowledge_base_path=resolve_knowledge_base_path(tests_root),
            )

        root_manifest = current / 'cosecha.toml'
        root_kb = current / '.cosecha' / 'kb.db'
        if root_manifest.exists() or root_kb.exists():
            return CosechaWorkspacePaths(
                project_path=current,
                root_path=current,
                manifest_path=(
                    root_manifest if root_manifest.exists() else None
                ),
                knowledge_base_path=resolve_knowledge_base_path(current),
            )

    msg = (
        'No Cosecha workspace found from '
        f'{candidate}. Expected tests/cosecha.toml, cosecha.toml, '
        'tests/.cosecha/kb.db or .cosecha/kb.db in this directory or parents.'
    )
    raise FileNotFoundError(msg)


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
