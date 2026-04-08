from __future__ import annotations

import builtins
import contextlib
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


def test_build_isolated_module_name_is_stable_and_path_based(tmp_path) -> None:
    module_path = tmp_path / 'stable_module.py'
    module_path.write_text('VALUE = 1\n', encoding='utf-8')

    first = build_isolated_module_name(module_path)
    second = build_isolated_module_name(str(module_path))

    assert first == second
    assert first.startswith('cosecha.dynamic.stable_module_')


def test_import_module_from_path_uses_prepare_import_paths(tmp_path) -> None:
    module_path = tmp_path / 'prepared_module.py'
    module_path.write_text('VALUE = 42\n', encoding='utf-8')
    entered: list[str] = []

    @contextlib.contextmanager
    def _prepare_import_paths(resolved_path):
        entered.append(str(resolved_path))
        yield

    module_name = build_isolated_module_name(module_path)
    previous_module = sys.modules.pop(module_name, None)
    try:
        module = import_module_from_path(
            module_path,
            prepare_import_paths=_prepare_import_paths,
        )
    finally:
        sys.modules.pop(module_name, None)
        if previous_module is not None:
            sys.modules[module_name] = previous_module

    assert module.VALUE == 42
    assert entered == [str(module_path.resolve())]


def test_import_module_from_path_rejects_missing_spec(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_path = tmp_path / 'no_spec_module.py'
    module_path.write_text('VALUE = 1\n', encoding='utf-8')
    monkeypatch.setattr(
        'cosecha.core.module_loading.importlib.util.spec_from_file_location',
        lambda name, path: None,
    )

    with pytest.raises(
        ImportError,
        match='Could not find the module specification',
    ):
        import_module_from_path(module_path)


def test_import_module_from_path_rejects_missing_loader(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_path = tmp_path / 'no_loader_module.py'
    module_path.write_text('VALUE = 1\n', encoding='utf-8')

    class _SpecWithoutLoader:
        loader = None

    monkeypatch.setattr(
        'cosecha.core.module_loading.importlib.util.spec_from_file_location',
        lambda name, path: _SpecWithoutLoader(),
    )
    monkeypatch.setattr(
        'cosecha.core.module_loading.importlib.util.module_from_spec',
        lambda spec: object(),
    )

    with pytest.raises(ImportError, match='No loader found for module'):
        import_module_from_path(module_path)
