from __future__ import annotations

import builtins
import sys

import pytest

from cosecha.core.module_loading import (
    build_isolated_module_name,
    import_module_from_path,
)


def test_import_module_from_path_reuses_cached_module(tmp_path) -> None:
    module_path = tmp_path / 'sample_module.py'
    module_path.write_text(
        '\n'.join(
            (
                'import builtins',
                'builtins.__cosecha_counter__ = '
                'getattr(builtins, "__cosecha_counter__", 0) + 1',
            ),
        ),
        encoding='utf-8',
    )

    module_name = build_isolated_module_name(module_path)
    previous_module = sys.modules.pop(module_name, None)
    previous_counter = getattr(builtins, '__cosecha_counter__', None)

    try:
        first_module = import_module_from_path(module_path)
        second_module = import_module_from_path(module_path)

        assert first_module is second_module
        assert getattr(builtins, '__cosecha_counter__', None) == 1
    finally:
        sys.modules.pop(module_name, None)
        if previous_module is not None:
            sys.modules[module_name] = previous_module

        if previous_counter is None:
            if hasattr(builtins, '__cosecha_counter__'):
                delattr(builtins, '__cosecha_counter__')
        else:
            builtins.__cosecha_counter__ = previous_counter


def test_import_module_from_path_cleans_failed_imports(tmp_path) -> None:
    module_path = tmp_path / 'broken_module.py'
    module_path.write_text('raise RuntimeError("boom")\n', encoding='utf-8')

    module_name = build_isolated_module_name(module_path)
    previous_module = sys.modules.pop(module_name, None)

    try:
        with pytest.raises(RuntimeError, match='boom'):
            import_module_from_path(module_path)

        assert module_name not in sys.modules
    finally:
        sys.modules.pop(module_name, None)
        if previous_module is not None:
            sys.modules[module_name] = previous_module
