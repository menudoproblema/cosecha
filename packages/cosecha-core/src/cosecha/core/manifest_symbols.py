from __future__ import annotations

import importlib

from dataclasses import dataclass
from pathlib import Path

from cosecha.core.module_loading import import_module_from_path
from cosecha.core.serialization import from_builtins_dict, to_builtins_dict


class ManifestValidationError(ValueError):
    __slots__ = ()


@dataclass(slots=True, frozen=True)
class SymbolRef:
    module: str
    qualname: str

    @classmethod
    def parse(cls, raw: str) -> SymbolRef:
        module, separator, qualname = raw.partition(':')
        if not separator or not module or not qualname:
            msg = f'Invalid symbol reference: {raw!r}'
            raise ManifestValidationError(msg)

        return cls(module=module, qualname=qualname)

    def resolve(self, *, root_path: Path) -> object:
        module_ref = self.module
        if module_ref.endswith('.py') or '/' in module_ref:
            module_path = Path(module_ref)
            if not module_path.is_absolute():
                module_path = (root_path / module_path).resolve()
            module = import_module_from_path(module_path)
        else:
            module = importlib.import_module(module_ref)

        resolved: object = module
        for part in self.qualname.split('.'):
            resolved = getattr(resolved, part, None)
            if resolved is None:
                msg = (
                    'Could not resolve symbol reference '
                    f'{self.module}:{self.qualname}'
                )
                raise ManifestValidationError(msg)

        return resolved

    def to_dict(self) -> dict[str, object]:
        return to_builtins_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> SymbolRef:
        return from_builtins_dict(data, target_type=cls)
