from __future__ import annotations

import hashlib
import importlib.util
import sys

from pathlib import Path
from typing import TYPE_CHECKING


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from types import ModuleType


def build_isolated_module_name(module_path: str | Path) -> str:
    resolved_path = Path(module_path).resolve()
    digest = hashlib.sha256(
        str(resolved_path).encode('utf-8'),
    ).hexdigest()[:16]
    return f'cosecha.dynamic.{resolved_path.stem}_{digest}'


def import_module_from_path(
    module_path: str | Path,
    *,
    prepare_import_paths: Callable | None = None,
) -> ModuleType:
    resolved_path = Path(module_path).resolve()
    module_name = build_isolated_module_name(resolved_path)
    cached_module = sys.modules.get(module_name)
    if cached_module is not None:
        return cached_module

    spec = importlib.util.spec_from_file_location(module_name, resolved_path)
    if not spec:
        msg = f'Could not find the module specification for {module_path}'
        raise ImportError(msg)

    module = importlib.util.module_from_spec(spec)
    if not spec.loader:
        msg = f'No loader found for module {module_name} at {module_path}'
        raise ImportError(msg)

    sys.modules[module_name] = module
    try:
        if prepare_import_paths is None:
            spec.loader.exec_module(module)
        else:
            with prepare_import_paths(resolved_path):
                spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise

    return module
