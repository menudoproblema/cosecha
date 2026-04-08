from __future__ import annotations

from functools import partial
from pathlib import Path

import pytest

from cosecha.core.location import FunctionLocation, Location


def _wrapped_target() -> str:
    return 'ok'


def _decorator(fn):
    def _inner(*args, **kwargs):
        return fn(*args, **kwargs)

    _inner.__wrapped__ = fn
    return _inner


def test_location_equality_hash_and_string_representation(tmp_path: Path) -> None:
    first = Location(tmp_path / 'a.py', line=10, column=2, name='func')
    same_line = Location(tmp_path / 'a.py', line=10, column=99, name='other')
    other = Location(tmp_path / 'a.py', line=11)

    assert first == same_line
    assert first != other
    assert first != object()
    assert hash(first) == hash((first.filename, 10, 2, 'func'))
    assert str(first) == f'{tmp_path / "a.py"}:10::[func]'


def test_location_relative_and_with_name(tmp_path: Path) -> None:
    root = tmp_path / 'workspace'
    file_path = root / 'tests' / 'demo.py'
    file_path.parent.mkdir(parents=True)
    file_path.write_text('pass\n', encoding='utf-8')

    location = Location(file_path, line=4, column=1)
    relative = location.relative_to(root)
    renamed = location.with_name('my-test')
    outside = location.relative_to(tmp_path / 'other-root')

    assert relative.filename == Path('tests/demo.py')
    assert renamed.name == 'my-test'
    assert outside.filename == file_path


def test_function_location_unwraps_partial_and_wrapped_functions() -> None:
    wrapped = _decorator(_wrapped_target)

    partial_target = partial(wrapped)
    location = FunctionLocation(partial_target)

    assert location.func is _wrapped_target
    assert location.line >= 1
    assert location.filename.exists()
    assert '_wrapped_target' in location.name
    assert 'FunctionLocation' in repr(location)


def test_function_location_raises_when_source_file_cannot_be_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr('cosecha.core.location.inspect.getsourcefile', lambda _f: None)

    with pytest.raises(ValueError, match='Cannot determine source file'):
        FunctionLocation(_wrapped_target)
