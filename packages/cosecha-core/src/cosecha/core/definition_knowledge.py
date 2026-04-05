from __future__ import annotations

from dataclasses import dataclass, field

from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


@dataclass(slots=True, frozen=True)
class DefinitionKnowledgeRecord:
    source_line: int
    function_name: str
    category: str | None = None
    provider_kind: str | None = None
    provider_name: str | None = None
    runtime_required: bool = False
    runtime_reason: str | None = None
    declaration_origin: str | None = None
    discovery_mode: str = 'ast'
    documentation: str | None = None
    payload_kind: str = 'generic'
    payload_version: int = 1
    payload: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.payload_version < 1:
            msg = (
                'Definition knowledge payload_version must be a positive '
                f'integer: {self.payload_version}'
            )
            raise ValueError(msg)

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, object],
    ) -> DefinitionKnowledgeRecord:
        return from_builtins_dict(data, target_type=cls)
