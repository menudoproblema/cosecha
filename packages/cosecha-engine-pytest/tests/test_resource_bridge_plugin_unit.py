from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.manifest_types import ResourceBindingSpec
from cosecha.core.runtime_interop import build_runtime_canonical_binding_name
from cosecha.engine.pytest import resource_bridge_plugin


def test_build_dynamic_fixture_plugin_returns_none_for_missing_pytest() -> None:
    original_pytest = resource_bridge_plugin.pytest
    try:
        resource_bridge_plugin.pytest = None
        assert (
            resource_bridge_plugin._build_dynamic_fixture_plugin(
                (
                    ResourceBindingSpec(
                        engine_type='pytest',
                        resource_name='workspace',
                        fixture_name='cosecha_workspace',
                    ),
                )
            )
            is None
        )
    finally:
        resource_bridge_plugin.pytest = original_pytest


def test_iter_fixture_bindings_includes_fixture_and_canonical_name() -> None:
    bindings = (
        ResourceBindingSpec(
            engine_type='pytest',
            resource_name='database/main',
            fixture_name='db',
        ),
        ResourceBindingSpec(
            engine_type='pytest',
            resource_name='workspace',
        ),
    )
    fixture_bindings = tuple(resource_bridge_plugin._iter_fixture_bindings(bindings))

    assert fixture_bindings == (
        ('db', 'database/main'),
        (
            build_runtime_canonical_binding_name('database/main'),
            'database/main',
        ),
        (
            build_runtime_canonical_binding_name('workspace'),
            'workspace',
        ),
    )


def test_merge_pytest_resource_bindings_deduplicates_and_filters() -> None:
    first = ResourceBindingSpec(
        engine_type='pytest',
        resource_name='workspace',
        fixture_name='cosecha_workspace',
    )
    duplicate = ResourceBindingSpec(
        engine_type='pytest',
        resource_name='workspace',
        fixture_name='cosecha_workspace',
    )
    other_engine = ResourceBindingSpec(
        engine_type='other',
        resource_name='workspace',
    )
    assert resource_bridge_plugin._merge_pytest_resource_bindings(
        (other_engine,),
        (first, duplicate),
    ) == (first,)


def test_discover_manifest_path_falls_back_to_cwd_when_root_has_no_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_path = tmp_path / 'repo'
    cwd_path = tmp_path / 'cwd'
    root_path.mkdir()
    cwd_path.mkdir()
    cwd_manifest = cwd_path / 'cosecha.toml'

    def _fake_discover_cosecha_manifest(*, start_path: Path):
        resolved_start = Path(start_path).resolve()
        if resolved_start == root_path.resolve():
            return None
        if resolved_start == cwd_path.resolve():
            return cwd_manifest
        return None

    monkeypatch.setattr(
        resource_bridge_plugin,
        'discover_cosecha_manifest',
        _fake_discover_cosecha_manifest,
    )
    monkeypatch.chdir(cwd_path)

    manifest_path = resource_bridge_plugin._discover_manifest_path(
        SimpleNamespace(rootpath=root_path),
    )

    assert manifest_path == cwd_manifest


def test_dynamic_fixture_plugin_returns_resource_from_bridge():
    bindings = (
        ResourceBindingSpec(
            engine_type='pytest',
            resource_name='workspace',
            fixture_name='cosecha_workspace',
        ),
    )
    plugin = resource_bridge_plugin._build_dynamic_fixture_plugin(bindings)
    assert plugin is not None

    fixture = getattr(plugin, '_resource_fixture_cosecha_workspace').__wrapped__
    request = SimpleNamespace(
        get_resource=lambda _: (_ for _ in ()).throw(
            LookupError('missing'),
        ),
    )

    with resource_bridge_plugin.temporary_active_resource_bridge(
        {'workspace': 'demo-workspace'},
    ):
        assert fixture(request) == 'demo-workspace'


def test_dynamic_fixture_plugin_skips_when_resource_missing() -> None:
    bindings = (
        ResourceBindingSpec(
            engine_type='pytest',
            resource_name='workspace',
            fixture_name='cosecha_workspace',
        ),
    )
    plugin = resource_bridge_plugin._build_dynamic_fixture_plugin(bindings)
    assert plugin is not None

    fixture = getattr(plugin, '_resource_fixture_cosecha_workspace').__wrapped__
    request = SimpleNamespace(get_resource=lambda _: (_ for _ in ()).throw(
        LookupError('missing'),
    ))

    with pytest.raises(pytest.skip.Exception):
        fixture(request)


def test_request_bridge_install_and_restore_with_existing_get_resource():
    resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 0
    resource_bridge_plugin._REQUEST_BRIDGE['state'] = None

    class DummyRequest:
        def get_resource(self, resource_name: str) -> object:
            raise LookupError(resource_name)

    original_pytest = resource_bridge_plugin.pytest
    try:
        resource_bridge_plugin.pytest = SimpleNamespace(FixtureRequest=DummyRequest)
        resource_bridge_plugin._install_request_bridge()
        with resource_bridge_plugin.temporary_active_resource_bridge(
            {'workspace': 'demo'},
        ):
            assert DummyRequest().get_resource('workspace') == 'demo'

        resource_bridge_plugin._restore_request_bridge()
        restored = DummyRequest()
        with pytest.raises(LookupError):
            restored.get_resource('workspace')
    finally:
        resource_bridge_plugin.pytest = original_pytest
        resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 0
        resource_bridge_plugin._REQUEST_BRIDGE['state'] = None


def test_build_dynamic_fixture_plugin_deduplicates_fixture_names() -> None:
    bindings = (
        ResourceBindingSpec(
            engine_type='pytest',
            resource_name='workspace-a',
            fixture_name='cosecha_workspace',
        ),
        ResourceBindingSpec(
            engine_type='pytest',
            resource_name='workspace-b',
            fixture_name='cosecha_workspace',
        ),
    )
    plugin = resource_bridge_plugin._build_dynamic_fixture_plugin(bindings)
    assert plugin is not None
    fixture_attributes = [
        name
        for name in dir(plugin)
        if name.startswith('_resource_fixture_')
    ]
    assert fixture_attributes.count('_resource_fixture_cosecha_workspace') == 1


def test_build_dynamic_fixture_plugin_handles_empty_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        resource_bridge_plugin,
        '_iter_fixture_bindings',
        lambda *_args, **_kwargs: (),
    )
    assert (
        resource_bridge_plugin._build_dynamic_fixture_plugin(
            (
                ResourceBindingSpec(
                    engine_type='pytest',
                    resource_name='workspace',
                    fixture_name='cosecha_workspace',
                ),
            ),
        )
        is None
    )


def test_dynamic_fixture_plugin_can_return_none_when_skip_is_monkeypatched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bindings = (
        ResourceBindingSpec(
            engine_type='pytest',
            resource_name='workspace',
            fixture_name='cosecha_workspace',
        ),
    )
    plugin = resource_bridge_plugin._build_dynamic_fixture_plugin(bindings)
    assert plugin is not None
    fixture = getattr(plugin, '_resource_fixture_cosecha_workspace').__wrapped__
    request = SimpleNamespace(
        get_resource=lambda _name: (_ for _ in ()).throw(LookupError('x')),
    )
    monkeypatch.setattr(resource_bridge_plugin.pytest, 'skip', lambda *_args, **_kwargs: None)
    assert fixture(request) is None


def test_install_request_bridge_noop_when_pytest_missing() -> None:
    original_pytest = resource_bridge_plugin.pytest
    try:
        resource_bridge_plugin.pytest = None
        resource_bridge_plugin._install_request_bridge()
        assert resource_bridge_plugin._REQUEST_BRIDGE['state'] is None
    finally:
        resource_bridge_plugin.pytest = original_pytest
        resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 0
        resource_bridge_plugin._REQUEST_BRIDGE['state'] = None


def test_install_request_bridge_noop_without_fixture_request() -> None:
    original_pytest = resource_bridge_plugin.pytest
    try:
        resource_bridge_plugin.pytest = SimpleNamespace(FixtureRequest=None)
        resource_bridge_plugin._install_request_bridge()
        assert resource_bridge_plugin._REQUEST_BRIDGE['state'] is None
    finally:
        resource_bridge_plugin.pytest = original_pytest
        resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 0
        resource_bridge_plugin._REQUEST_BRIDGE['state'] = None


def test_request_bridge_lookup_errors_for_missing_active_resources() -> None:
    resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 0
    resource_bridge_plugin._REQUEST_BRIDGE['state'] = None

    class DummyRequest:
        pass

    original_pytest = resource_bridge_plugin.pytest
    try:
        resource_bridge_plugin.pytest = SimpleNamespace(FixtureRequest=DummyRequest)
        resource_bridge_plugin._install_request_bridge()
        request = DummyRequest()
        with pytest.raises(LookupError, match="does not expose resource 'workspace'"):
            request.get_resource('workspace')
        with resource_bridge_plugin.temporary_active_resource_bridge({'other': 'x'}):
            with pytest.raises(LookupError, match="does not expose resource 'workspace'"):
                request.get_resource('workspace')
    finally:
        resource_bridge_plugin._restore_request_bridge()
        resource_bridge_plugin.pytest = original_pytest
        resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 0
        resource_bridge_plugin._REQUEST_BRIDGE['state'] = None


def test_restore_request_bridge_handles_refcount_and_missing_state() -> None:
    resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 2
    resource_bridge_plugin._REQUEST_BRIDGE['state'] = ('dummy', True, object())
    resource_bridge_plugin._restore_request_bridge()
    assert resource_bridge_plugin._REQUEST_BRIDGE['refcount'] == 1
    assert resource_bridge_plugin._REQUEST_BRIDGE['state'] is not None

    resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 1
    resource_bridge_plugin._REQUEST_BRIDGE['state'] = None
    resource_bridge_plugin._restore_request_bridge()
    assert resource_bridge_plugin._REQUEST_BRIDGE['refcount'] == 0
    assert resource_bridge_plugin._REQUEST_BRIDGE['state'] is None


def test_request_bridge_restores_by_removing_injected_attribute() -> None:
    resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 0
    resource_bridge_plugin._REQUEST_BRIDGE['state'] = None

    class DummyRequest:
        pass

    original_pytest = resource_bridge_plugin.pytest
    try:
        resource_bridge_plugin.pytest = SimpleNamespace(FixtureRequest=DummyRequest)
        resource_bridge_plugin._install_request_bridge()
        assert hasattr(DummyRequest, 'get_resource')
        resource_bridge_plugin._restore_request_bridge()
        assert not hasattr(DummyRequest, 'get_resource')
    finally:
        resource_bridge_plugin.pytest = original_pytest
        resource_bridge_plugin._REQUEST_BRIDGE['refcount'] = 0
        resource_bridge_plugin._REQUEST_BRIDGE['state'] = None
