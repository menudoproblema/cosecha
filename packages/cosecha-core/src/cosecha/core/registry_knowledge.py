from __future__ import annotations

from dataclasses import dataclass, field

from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


REGISTRY_KNOWLEDGE_LOADER_SCHEMA_VERSION = (
    'gherkin_registry_loader_snapshot:v3'
)


@dataclass(slots=True, frozen=True)
class RegistryKnowledgeEntry:
    layout_name: str
    module_import_path: str
    qualname: str
    class_name: str

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RegistryKnowledgeEntry:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RegistryKnowledgeSnapshot:
    engine_name: str
    module_spec: str
    package_hash: str
    layout_key: str
    loader_schema_version: str
    entries: tuple[RegistryKnowledgeEntry, ...] = field(
        default_factory=tuple,
    )
    source_count: int = 0
    created_at: float | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> RegistryKnowledgeSnapshot:
        return from_builtins_dict(data, target_type=cls)


@dataclass(slots=True, frozen=True)
class RegistryKnowledgeQuery:
    engine_name: str | None = None
    module_spec: str | None = None
    package_hash: str | None = None
    layout_key: str | None = None
    loader_schema_version: str | None = None
    limit: int | None = None

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> RegistryKnowledgeQuery:
        return from_builtins_dict(data, target_type=cls)
