from cosecha.provider.ssl._version import __version__, version_info


def test_version_contract() -> None:
    assert __version__ == '.'.join(map(str, version_info))
    assert isinstance(version_info, tuple)
