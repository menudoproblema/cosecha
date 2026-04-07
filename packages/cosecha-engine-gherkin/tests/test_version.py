from __future__ import annotations

from cosecha.engine.gherkin._version import __version__, version_info


def test_version_constants_are_consistent() -> None:
    assert version_info == (0, 0, 1)
    assert __version__ == '0.0.1'
    assert __version__ == '.'.join(map(str, version_info))

