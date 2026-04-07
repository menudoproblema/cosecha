from __future__ import annotations

import json

from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Literal


type CodeLocationRole = Literal['source', 'tests', 'vendored', 'generated']


def _canonical_json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(',', ':'),
        sort_keys=True,
    )


@dataclass(slots=True, frozen=True)
class CodeLocation:
    path: Path
    role: CodeLocationRole = 'source'
    importable: bool = True

    def to_dict(self) -> dict[str, object]:
        return {
            'path': str(self.path),
            'role': self.role,
            'importable': self.importable,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> CodeLocation:
        return cls(
            path=Path(str(data['path'])),
            role=str(data.get('role', 'source')),  # type: ignore[arg-type]
            importable=bool(data.get('importable', True)),
        )


@dataclass(slots=True, frozen=True)
class WorkspaceDeclaration:
    root: str | None = None
    knowledge_anchor: str | None = None
    locations: tuple[CodeLocation, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            'root': self.root,
            'knowledge_anchor': self.knowledge_anchor,
            'locations': [location.to_dict() for location in self.locations],
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> WorkspaceDeclaration:
        return cls(
            root=(
                None if data.get('root') is None else str(data.get('root'))
            ),
            knowledge_anchor=(
                None
                if data.get('knowledge_anchor') is None
                else str(data.get('knowledge_anchor'))
            ),
            locations=tuple(
                CodeLocation.from_dict(location)
                for location in data.get('locations', ())
                if isinstance(location, dict)
            ),
        )


@dataclass(slots=True, frozen=True)
class ImportEnvironment:
    locations: tuple[CodeLocation, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            'locations': [location.to_dict() for location in self.locations],
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ImportEnvironment:
        return cls(
            locations=tuple(
                CodeLocation.from_dict(location)
                for location in data.get('locations', ())
                if isinstance(location, dict)
            ),
        )


@dataclass(slots=True, frozen=True)
class LayoutAdaptation:
    workspace_root: Path | None = None
    knowledge_anchor: Path | None = None
    code_locations: tuple[CodeLocation, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            'workspace_root': (
                None
                if self.workspace_root is None
                else str(self.workspace_root)
            ),
            'knowledge_anchor': (
                None
                if self.knowledge_anchor is None
                else str(self.knowledge_anchor)
            ),
            'code_locations': [
                location.to_dict() for location in self.code_locations
            ],
        }


@dataclass(slots=True, frozen=True)
class LayoutMatch:
    adapter_name: str
    priority: int
    adaptation: LayoutAdaptation

    def to_dict(self) -> dict[str, object]:
        return {
            'adapter_name': self.adapter_name,
            'priority': self.priority,
            'adaptation': self.adaptation.to_dict(),
        }


@dataclass(slots=True, frozen=True)
class ShadowedCodeLocation:
    path: Path
    kept_by_adapter: str
    shadowed_by_adapter: str

    def to_dict(self) -> dict[str, object]:
        return {
            'path': str(self.path),
            'kept_by_adapter': self.kept_by_adapter,
            'shadowed_by_adapter': self.shadowed_by_adapter,
        }


@dataclass(slots=True, frozen=True)
class IgnoredRootContribution:
    adapter_name: str
    priority: int
    workspace_root: Path | None = None
    knowledge_anchor: Path | None = None
    reason: str = 'ignored_by_higher_priority'

    def to_dict(self) -> dict[str, object]:
        return {
            'adapter_name': self.adapter_name,
            'priority': self.priority,
            'workspace_root': (
                None
                if self.workspace_root is None
                else str(self.workspace_root)
            ),
            'knowledge_anchor': (
                None
                if self.knowledge_anchor is None
                else str(self.knowledge_anchor)
            ),
            'reason': self.reason,
        }


@dataclass(slots=True, frozen=True)
class WorkspaceProvenance:
    manifest_discovered_from: Path | None = None
    root_winner_adapter: str | None = None
    adapter_matches: tuple[LayoutMatch, ...] = ()
    shadowed_locations: tuple[ShadowedCodeLocation, ...] = ()
    ignored_root_contributions: tuple[IgnoredRootContribution, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            'manifest_discovered_from': (
                None
                if self.manifest_discovered_from is None
                else str(self.manifest_discovered_from)
            ),
            'root_winner_adapter': self.root_winner_adapter,
            'adapter_matches': [
                match.to_dict() for match in self.adapter_matches
            ],
            'shadowed_locations': [
                location.to_dict() for location in self.shadowed_locations
            ],
            'ignored_root_contributions': [
                contribution.to_dict()
                for contribution in self.ignored_root_contributions
            ],
        }


@dataclass(slots=True, frozen=True)
class EffectiveWorkspace:
    manifest_path: Path | None
    workspace_root: Path
    knowledge_anchor: Path
    import_environment: ImportEnvironment
    declaration: WorkspaceDeclaration = field(
        default_factory=WorkspaceDeclaration,
    )
    provenance: WorkspaceProvenance = field(
        default_factory=WorkspaceProvenance,
    )

    @property
    def fingerprint(self) -> str:
        payload = {
            'knowledge_anchor': str(
                self.knowledge_anchor.relative_to(self.workspace_root),
            ),
            'import_environment': [
                {
                    'path': str(
                        location.path.relative_to(self.workspace_root),
                    ),
                    'role': location.role,
                    'importable': location.importable,
                }
                for location in self.import_environment.locations
            ],
        }
        return sha256(_canonical_json(payload).encode('utf-8')).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            'manifest_path': (
                None if self.manifest_path is None else str(self.manifest_path)
            ),
            'workspace_root': str(self.workspace_root),
            'knowledge_anchor': str(self.knowledge_anchor),
            'import_environment': self.import_environment.to_dict(),
            'declaration': self.declaration.to_dict(),
            'provenance': self.provenance.to_dict(),
            'fingerprint': self.fingerprint,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> EffectiveWorkspace:
        return cls(
            manifest_path=(
                None
                if data.get('manifest_path') is None
                else Path(str(data['manifest_path']))
            ),
            workspace_root=Path(str(data['workspace_root'])),
            knowledge_anchor=Path(str(data['knowledge_anchor'])),
            import_environment=ImportEnvironment.from_dict(
                dict(data.get('import_environment', {})),
            ),
            declaration=WorkspaceDeclaration.from_dict(
                dict(data.get('declaration', {})),
            ),
            provenance=WorkspaceProvenance(
                manifest_discovered_from=(
                    None
                    if dict(data.get('provenance', {})).get(
                        'manifest_discovered_from',
                    )
                    is None
                    else Path(
                        str(
                            dict(data.get('provenance', {}))[
                                'manifest_discovered_from'
                            ],
                        ),
                    )
                ),
                root_winner_adapter=dict(data.get('provenance', {})).get(
                    'root_winner_adapter',
                ),
                adapter_matches=tuple(
                    LayoutMatch(
                        adapter_name=str(match['adapter_name']),
                        priority=int(match['priority']),
                        adaptation=LayoutAdaptation(
                            workspace_root=(
                                None
                                if match['adaptation'].get(
                                    'workspace_root',
                                )
                                is None
                                else Path(
                                    str(
                                        match['adaptation'][
                                            'workspace_root'
                                        ],
                                    ),
                                )
                            ),
                            knowledge_anchor=(
                                None
                                if match['adaptation'].get(
                                    'knowledge_anchor',
                                )
                                is None
                                else Path(
                                    str(
                                        match['adaptation'][
                                            'knowledge_anchor'
                                        ],
                                    ),
                                )
                            ),
                            code_locations=tuple(
                                CodeLocation.from_dict(location)
                                for location in match['adaptation'].get(
                                    'code_locations',
                                    (),
                                )
                                if isinstance(location, dict)
                            ),
                        ),
                    )
                    for match in dict(data.get('provenance', {})).get(
                        'adapter_matches',
                        (),
                    )
                    if isinstance(match, dict)
                ),
                shadowed_locations=tuple(
                    ShadowedCodeLocation(
                        path=Path(str(location['path'])),
                        kept_by_adapter=str(location['kept_by_adapter']),
                        shadowed_by_adapter=str(
                            location['shadowed_by_adapter'],
                        ),
                    )
                    for location in dict(data.get('provenance', {})).get(
                        'shadowed_locations',
                        (),
                    )
                    if isinstance(location, dict)
                ),
                ignored_root_contributions=tuple(
                    IgnoredRootContribution(
                        adapter_name=str(contribution['adapter_name']),
                        priority=int(contribution['priority']),
                        workspace_root=(
                            None
                            if contribution.get('workspace_root') is None
                            else Path(str(contribution['workspace_root']))
                        ),
                        knowledge_anchor=(
                            None
                            if contribution.get('knowledge_anchor') is None
                            else Path(str(contribution['knowledge_anchor']))
                        ),
                        reason=str(
                            contribution.get(
                                'reason',
                                'ignored_by_higher_priority',
                            ),
                        ),
                    )
                    for contribution in dict(data.get('provenance', {})).get(
                        'ignored_root_contributions',
                        (),
                    )
                    if isinstance(contribution, dict)
                ),
            ),
        )


@dataclass(slots=True, frozen=True)
class ExecutionContext:
    execution_root: Path
    knowledge_storage_root: Path
    shadow_root: Path | None = None
    invocation_id: str | None = None
    workspace_fingerprint: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            'execution_root': str(self.execution_root),
            'knowledge_storage_root': str(self.knowledge_storage_root),
            'shadow_root': (
                None if self.shadow_root is None else str(self.shadow_root)
            ),
            'invocation_id': self.invocation_id,
            'workspace_fingerprint': self.workspace_fingerprint,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ExecutionContext:
        return cls(
            execution_root=Path(str(data['execution_root'])),
            knowledge_storage_root=Path(
                str(data['knowledge_storage_root']),
            ),
            shadow_root=(
                None
                if data.get('shadow_root') is None
                else Path(str(data['shadow_root']))
            ),
            invocation_id=(
                None
                if data.get('invocation_id') is None
                else str(data['invocation_id'])
            ),
            workspace_fingerprint=(
                None
                if data.get('workspace_fingerprint') is None
                else str(data['workspace_fingerprint'])
            ),
        )
