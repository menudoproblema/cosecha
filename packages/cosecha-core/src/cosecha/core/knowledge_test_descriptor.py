from __future__ import annotations

from dataclasses import dataclass

from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


@dataclass(slots=True, frozen=True)
class TestDescriptorKnowledge:
    stable_id: str
    test_name: str
    file_path: str
    source_line: int
    selection_labels: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> TestDescriptorKnowledge:
        return from_builtins_dict(data, target_type=cls)
