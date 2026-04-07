from __future__ import annotations

import tomllib

from contextlib import contextmanager
from contextvars import ContextVar
from importlib.metadata import entry_points
from pathlib import Path
from threading import Lock
from typing import Protocol

from cosecha.workspace.models import (
    CodeLocation,
    EffectiveWorkspace,
    ExecutionContext,
    IgnoredRootContribution,
    ImportEnvironment,
    LayoutAdaptation,
    LayoutMatch,
    ShadowedCodeLocation,
    WorkspaceDeclaration,
    WorkspaceProvenance,
)


LAYOUT_ADAPTER_ENTRYPOINT_GROUP = 'cosecha.workspace.layouts'
_LAYOUT_ADAPTER_CACHE_LOCK = Lock()
_LAYOUT_ADAPTER_CACHE: dict[str, tuple[LayoutAdapter, ...] | None] = {
    'value': None,
}


class WorkspaceResolutionError(RuntimeError): ...


class LayoutAdapter(Protocol):
    adapter_name: str
    priority: int

    def match(
        self,
        *,
        manifest_path: Path | None,
        declaration: WorkspaceDeclaration,
        candidate_root: Path,
        evidence_path: Path | None,
    ) -> LayoutMatch | None: ...


class WorkspaceResolutionPolicy:
    __slots__ = (
        'allow_knowledge_base_fallback',
        'knowledge_base_candidate_paths',
        'layout_adapters',
        'manifest_candidate_paths',
        'max_ancestor_distance',
    )

    def __init__(
        self,
        *,
        manifest_candidate_paths: tuple[str, ...] = (
            'tests/cosecha.toml',
            'cosecha.toml',
        ),
        knowledge_base_candidate_paths: tuple[str, ...] = (
            'tests/.cosecha/kb.db',
            '.cosecha/kb.db',
        ),
        max_ancestor_distance: int = 1,
        layout_adapters: tuple[LayoutAdapter, ...] | None = None,
        allow_knowledge_base_fallback: bool = True,
    ) -> None:
        self.manifest_candidate_paths = manifest_candidate_paths
        self.knowledge_base_candidate_paths = knowledge_base_candidate_paths
        self.max_ancestor_distance = max_ancestor_distance
        self.layout_adapters = (
            () if layout_adapters is None else layout_adapters
        )
        self.allow_knowledge_base_fallback = allow_knowledge_base_fallback


def discover_cosecha_manifest(
    *,
    manifest_file: Path | None = None,
    start_path: str | Path | None = None,
) -> Path | None:
    if manifest_file is not None:
        return manifest_file.resolve() if manifest_file.exists() else None

    policy = get_active_policy()
    origin = Path.cwd() if start_path is None else Path(start_path)
    candidate = origin if origin.is_dir() else origin.parent
    candidate = candidate.resolve()

    for current in (candidate, *candidate.parents):
        for relative_path in policy.manifest_candidate_paths:
            manifest_path = current / relative_path
            if manifest_path.exists():
                return manifest_path.resolve()

    return None


def resolve_workspace(
    *,
    start_path: str | Path | None = None,
    manifest_file: Path | None = None,
) -> EffectiveWorkspace:
    policy = _materialize_policy(get_active_policy())
    origin = Path.cwd() if start_path is None else Path(start_path)
    candidate_root = origin if origin.is_dir() else origin.parent
    candidate_root = candidate_root.resolve()
    manifest_path = discover_cosecha_manifest(
        manifest_file=manifest_file,
        start_path=candidate_root,
    )
    evidence_path = _discover_workspace_evidence(
        candidate_root,
        policy=policy,
    )

    if manifest_path is None and evidence_path is None:
        msg = (
            'No Cosecha workspace found from '
            f'{candidate_root}. Expected manifest or knowledge base markers.'
        )
        raise FileNotFoundError(msg)

    declaration = _load_workspace_declaration(manifest_path)
    matches = tuple(
        match
        for adapter in policy.layout_adapters
        if (
            match := adapter.match(
                manifest_path=manifest_path,
                declaration=declaration,
                candidate_root=candidate_root,
                evidence_path=evidence_path,
            )
        )
        is not None
    )

    explicit_adaptation = _build_explicit_adaptation(
        declaration,
        manifest_path=manifest_path,
    )
    if explicit_adaptation is not None:
        matches = (
            LayoutMatch(
                adapter_name='workspace_declaration',
                priority=10_000,
                adaptation=explicit_adaptation,
            ),
            *matches,
        )

    merged_adaptation, provenance = _merge_adaptations(
        matches,
        manifest_path=manifest_path,
    )
    workspace_root = merged_adaptation.workspace_root
    if workspace_root is None:
        msg = 'Workspace resolution did not produce workspace_root'
        raise WorkspaceResolutionError(msg)
    knowledge_anchor = (
        merged_adaptation.knowledge_anchor
        if merged_adaptation.knowledge_anchor is not None
        else workspace_root
    )

    validate_workspace_root(
        workspace_root,
        manifest_path,
        max_distance=policy.max_ancestor_distance,
    )
    return EffectiveWorkspace(
        manifest_path=manifest_path,
        workspace_root=workspace_root.resolve(),
        knowledge_anchor=knowledge_anchor.resolve(),
        import_environment=ImportEnvironment(
            locations=tuple(
                CodeLocation(
                    path=location.path.resolve(),
                    role=location.role,
                    importable=location.importable,
                )
                for location in merged_adaptation.code_locations
            ),
        ),
        declaration=declaration,
        provenance=provenance,
    )


def build_execution_context(
    workspace: EffectiveWorkspace,
    runtime_profile=None,
    *,
    cli_overrides=None,
    invocation_id: str | None = None,
) -> ExecutionContext:
    execution_root = workspace.workspace_root
    knowledge_storage_root = workspace.workspace_root / '.cosecha'

    if runtime_profile is not None:
        raw_execution_root = getattr(runtime_profile, 'execution_root', None)
        raw_storage_root = getattr(
            runtime_profile,
            'knowledge_storage_root',
            None,
        )
        if isinstance(raw_execution_root, str) and raw_execution_root:
            execution_root = _resolve_context_path(
                workspace.workspace_root,
                Path(raw_execution_root),
            )
        if isinstance(raw_storage_root, str) and raw_storage_root:
            knowledge_storage_root = _resolve_context_path(
                workspace.workspace_root,
                Path(raw_storage_root),
            )

    if cli_overrides is not None:
        raw_execution_root = getattr(cli_overrides, 'execution_root', None)
        raw_storage_root = getattr(
            cli_overrides,
            'knowledge_storage_root',
            None,
        )
        if isinstance(raw_execution_root, Path):
            execution_root = raw_execution_root.resolve()
        if isinstance(raw_storage_root, Path):
            knowledge_storage_root = raw_storage_root.resolve()

    return ExecutionContext(
        execution_root=execution_root.resolve(),
        knowledge_storage_root=knowledge_storage_root.resolve(),
        invocation_id=invocation_id,
        workspace_fingerprint=workspace.fingerprint,
    )


def validate_workspace_root(
    workspace_root: Path,
    manifest_path: Path | None,
    *,
    max_distance: int,
) -> None:
    if manifest_path is None:
        return

    workspace_root = workspace_root.resolve()
    manifest_parent = manifest_path.parent.resolve()
    try:
        relative_path = manifest_parent.relative_to(workspace_root)
    except ValueError as error:
        msg = (
            'workspace_root must be an ancestor of manifest_path: '
            f'{workspace_root} !<= {manifest_path}'
        )
        raise WorkspaceResolutionError(msg) from error
    if len(relative_path.parts) > max_distance:
        msg = (
            'workspace_root exceeds max_ancestor_distance: '
            f'{workspace_root} -> {manifest_path}'
        )
        raise WorkspaceResolutionError(msg)


def _resolve_context_path(workspace_root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (workspace_root / path).resolve()


def _load_layout_adapters() -> tuple[LayoutAdapter, ...]:
    with _LAYOUT_ADAPTER_CACHE_LOCK:
        cached_adapters = _LAYOUT_ADAPTER_CACHE['value']
        if cached_adapters is not None:
            return cached_adapters

        loaded_adapters: list[LayoutAdapter] = []
        for entry_point in entry_points(group=LAYOUT_ADAPTER_ENTRYPOINT_GROUP):
            loaded = entry_point.load()
            adapter = loaded() if isinstance(loaded, type) else loaded
            loaded_adapters.append(adapter)
        cached_adapters = tuple(loaded_adapters)
        _LAYOUT_ADAPTER_CACHE['value'] = cached_adapters
        return cached_adapters


def _materialize_policy(
    policy: WorkspaceResolutionPolicy,
) -> WorkspaceResolutionPolicy:
    if policy.layout_adapters:
        return policy
    return WorkspaceResolutionPolicy(
        manifest_candidate_paths=policy.manifest_candidate_paths,
        knowledge_base_candidate_paths=policy.knowledge_base_candidate_paths,
        max_ancestor_distance=policy.max_ancestor_distance,
        layout_adapters=_load_layout_adapters(),
        allow_knowledge_base_fallback=policy.allow_knowledge_base_fallback,
    )


DEFAULT_POLICY = WorkspaceResolutionPolicy()
_active_policy: ContextVar[WorkspaceResolutionPolicy] = ContextVar(
    'cosecha_workspace_resolution_policy',
    default=DEFAULT_POLICY,
)


def get_active_policy() -> WorkspaceResolutionPolicy:
    return _active_policy.get()


@contextmanager
def using_policy(policy: WorkspaceResolutionPolicy):
    token = _active_policy.set(policy)
    try:
        yield
    finally:
        _active_policy.reset(token)


def _discover_workspace_evidence(
    candidate_root: Path,
    *,
    policy: WorkspaceResolutionPolicy,
) -> Path | None:
    if not policy.allow_knowledge_base_fallback:
        return None

    for current in (candidate_root, *candidate_root.parents):
        for relative_path in policy.knowledge_base_candidate_paths:
            evidence_path = current / relative_path
            if evidence_path.exists():
                return evidence_path.resolve()
    return None


def _load_workspace_declaration(
    manifest_path: Path | None,
) -> WorkspaceDeclaration:
    if manifest_path is None or not manifest_path.exists():
        return WorkspaceDeclaration()

    payload = tomllib.loads(manifest_path.read_text(encoding='utf-8'))
    workspace_payload = payload.get('workspace')
    if not isinstance(workspace_payload, dict):
        return WorkspaceDeclaration()

    raw_locations = workspace_payload.get('locations', ())
    locations: list[CodeLocation] = []
    if isinstance(raw_locations, list):
        for location in raw_locations:
            if not isinstance(location, dict):
                continue
            raw_path = location.get('path')
            if not isinstance(raw_path, str):
                continue
            locations.append(
                CodeLocation(
                    path=Path(raw_path),
                    role=str(location.get('role', 'source')),  # type: ignore[arg-type]
                    importable=bool(location.get('importable', True)),
                ),
            )

    return WorkspaceDeclaration(
        root=(
            None
            if workspace_payload.get('root') is None
            else str(workspace_payload['root'])
        ),
        knowledge_anchor=(
            None
            if workspace_payload.get('knowledge_anchor') is None
            else str(workspace_payload['knowledge_anchor'])
        ),
        locations=tuple(locations),
    )


def _build_explicit_adaptation(
    declaration: WorkspaceDeclaration,
    *,
    manifest_path: Path | None,
) -> LayoutAdaptation | None:
    if (
        declaration.root is None
        and declaration.knowledge_anchor is None
        and not declaration.locations
    ):
        return None

    manifest_dir = (
        manifest_path.parent.resolve()
        if manifest_path is not None
        else Path.cwd().resolve()
    )
    workspace_root = (
        (manifest_dir / declaration.root).resolve()
        if declaration.root is not None
        else None
    )
    effective_workspace_root = workspace_root or manifest_dir
    knowledge_anchor = (
        (effective_workspace_root / declaration.knowledge_anchor).resolve()
        if declaration.knowledge_anchor is not None
        else None
    )
    code_locations = tuple(
        CodeLocation(
            path=(effective_workspace_root / location.path).resolve(),
            role=location.role,
            importable=location.importable,
        )
        for location in declaration.locations
    )
    return LayoutAdaptation(
        workspace_root=workspace_root,
        knowledge_anchor=knowledge_anchor,
        code_locations=code_locations,
    )


def _merge_adaptations(
    matches: tuple[LayoutMatch, ...],
    *,
    manifest_path: Path | None,
) -> tuple[LayoutAdaptation, WorkspaceProvenance]:
    if not matches:
        msg = f'No layout adapters matched workspace for {manifest_path}'
        raise WorkspaceResolutionError(msg)

    sorted_matches = tuple(
        sorted(
            matches,
            key=lambda match: (-match.priority, match.adapter_name),
        ),
    )
    root_winner = _select_root_winner(sorted_matches)
    shadowed_locations: list[ShadowedCodeLocation] = []
    merged_locations: list[CodeLocation] = []
    seen_paths: dict[Path, str] = {}

    for match in sorted_matches:
        for location in match.adaptation.code_locations:
            resolved_path = location.path.resolve()
            if resolved_path in seen_paths:
                shadowed_locations.append(
                    ShadowedCodeLocation(
                        path=resolved_path,
                        kept_by_adapter=seen_paths[resolved_path],
                        shadowed_by_adapter=match.adapter_name,
                    ),
                )
                continue
            seen_paths[resolved_path] = match.adapter_name
            merged_locations.append(
                CodeLocation(
                    path=resolved_path,
                    role=location.role,
                    importable=location.importable,
                ),
            )

    ignored_root_contributions = tuple(
        IgnoredRootContribution(
            adapter_name=match.adapter_name,
            priority=match.priority,
            workspace_root=match.adaptation.workspace_root,
            knowledge_anchor=match.adaptation.knowledge_anchor,
        )
        for match in sorted_matches
        if root_winner is not None
        and match.adapter_name != root_winner.adapter_name
        and (
            match.adaptation.workspace_root is not None
            or match.adaptation.knowledge_anchor is not None
        )
    )

    provenance = WorkspaceProvenance(
        manifest_discovered_from=manifest_path,
        root_winner_adapter=(
            None if root_winner is None else root_winner.adapter_name
        ),
        adapter_matches=sorted_matches,
        shadowed_locations=tuple(shadowed_locations),
        ignored_root_contributions=ignored_root_contributions,
    )
    return (
        LayoutAdaptation(
            workspace_root=(
                None
                if root_winner is None
                else root_winner.adaptation.workspace_root
            ),
            knowledge_anchor=(
                None
                if root_winner is None
                else root_winner.adaptation.knowledge_anchor
            ),
            code_locations=tuple(merged_locations),
        ),
        provenance,
    )


def _select_root_winner(
    matches: tuple[LayoutMatch, ...],
) -> LayoutMatch | None:
    root_matches = tuple(
        match
        for match in matches
        if (
            match.adaptation.workspace_root is not None
            or match.adaptation.knowledge_anchor is not None
        )
    )
    if not root_matches:
        return None

    highest_priority = root_matches[0].priority
    highest_matches = tuple(
        match for match in root_matches if match.priority == highest_priority
    )
    first_match = highest_matches[0]
    for current_match in highest_matches[1:]:
        if (
            current_match.adaptation.workspace_root
            != first_match.adaptation.workspace_root
            or current_match.adaptation.knowledge_anchor
            != first_match.adaptation.knowledge_anchor
        ):
            msg = (
                'Multiple layout adapters proposed incompatible roots at the '
                f'same priority {highest_priority}: '
                f'{first_match.adapter_name}, {current_match.adapter_name}'
            )
            raise WorkspaceResolutionError(msg)
    return first_match
