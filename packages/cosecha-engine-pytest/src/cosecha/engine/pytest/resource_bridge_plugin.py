from __future__ import annotations

import contextlib

from pathlib import Path
from typing import TYPE_CHECKING

from cosecha.core.cosecha_manifest import load_cosecha_manifest
from cosecha.core.runtime_profiles import build_runtime_canonical_binding_name
from cosecha.workspace import discover_cosecha_manifest


try:  # pragma: no cover
    import pytest
except Exception:  # pragma: no cover
    pytest = None


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.manifest_types import ResourceBindingSpec


_DYNAMIC_PLUGIN_NAME = 'cosecha-pytest-resource-bridge'
_ACTIVE_RESOURCES: list[dict[str, object]] = []
_ACTIVE_RESOURCE_BINDINGS: list[tuple[ResourceBindingSpec, ...]] = []
_REQUEST_BRIDGE = {
    'refcount': 0,
    'state': None,
}


def pytest_configure(config) -> None:
    _install_request_bridge()
    if config.pluginmanager.hasplugin(_DYNAMIC_PLUGIN_NAME):
        return

    dynamic_plugin = _build_dynamic_fixture_plugin(
        _load_pytest_resource_bindings(config),
    )
    if dynamic_plugin is None:
        return

    config.pluginmanager.register(
        dynamic_plugin,
        name=_DYNAMIC_PLUGIN_NAME,
    )


def pytest_unconfigure(config) -> None:
    del config
    _restore_request_bridge()


@contextlib.contextmanager
def temporary_active_resource_bridge(
    resources: dict[str, object],
):
    _ACTIVE_RESOURCES.append(resources.copy())
    try:
        yield
    finally:
        _ACTIVE_RESOURCES.pop()


@contextlib.contextmanager
def temporary_resource_bindings(
    resource_bindings: tuple[ResourceBindingSpec, ...],
):
    _ACTIVE_RESOURCE_BINDINGS.append(resource_bindings)
    try:
        yield
    finally:
        _ACTIVE_RESOURCE_BINDINGS.pop()


def _build_dynamic_fixture_plugin(
    resource_bindings,
) -> object | None:
    if pytest is None or not resource_bindings:
        return None

    plugin_attributes: dict[str, object] = {}
    registered_names: set[str] = set()

    for fixture_name, resource_name in _iter_fixture_bindings(
        resource_bindings,
    ):
        if fixture_name in registered_names:
            continue
        registered_names.add(fixture_name)

        @pytest.fixture(name=fixture_name)
        def _resource_fixture(
            request,
            *,
            _fixture_name=fixture_name,
            _resource_name=resource_name,
        ):
            if _ACTIVE_RESOURCES:
                active_resources = _ACTIVE_RESOURCES[-1]
                if _resource_name in active_resources:
                    return active_resources[_resource_name]

            get_resource = getattr(request, 'get_resource', None)
            if callable(get_resource):
                try:
                    return get_resource(_resource_name)
                except LookupError:
                    pass

            pytest.skip(
                'requires Cosecha resource '
                f'{_resource_name!r} via fixture {_fixture_name!r}',
            )
            return None

        plugin_attributes[f'_resource_fixture_{fixture_name}'] = (
            _resource_fixture
        )

    if not plugin_attributes:
        return None

    plugin_type = type(
        '_PytestResourceBridgePlugin',
        (),
        plugin_attributes,
    )
    return plugin_type()


def _iter_fixture_bindings(resource_bindings):
    for binding in resource_bindings:
        fixture_name = binding.fixture_name
        if fixture_name is not None:
            yield (fixture_name, binding.resource_name)

        yield (
            build_runtime_canonical_binding_name(binding.resource_name),
            binding.resource_name,
        )


def _load_pytest_resource_bindings(config):
    active_bindings = (
        _ACTIVE_RESOURCE_BINDINGS[-1]
        if _ACTIVE_RESOURCE_BINDINGS
        else ()
    )
    manifest_path = _discover_manifest_path(config)
    manifest_bindings = ()
    if manifest_path is not None:
        manifest = load_cosecha_manifest(manifest_file=manifest_path)
        if manifest is not None:
            manifest_bindings = tuple(
                binding
                for binding in manifest.resource_bindings
                if binding.engine_type == 'pytest'
            )

    return _merge_pytest_resource_bindings(
        active_bindings,
        manifest_bindings,
    )


def _discover_manifest_path(config) -> Path | None:
    root_path = Path(str(config.rootpath)).resolve()
    cwd_path = Path.cwd().resolve()
    manifest_path = discover_cosecha_manifest(start_path=root_path)
    if manifest_path is not None:
        return manifest_path
    if cwd_path != root_path:
        return discover_cosecha_manifest(start_path=cwd_path)
    return None


def _merge_pytest_resource_bindings(
    *binding_groups: tuple[ResourceBindingSpec, ...],
) -> tuple[ResourceBindingSpec, ...]:
    merged: list[ResourceBindingSpec] = []
    seen: set[ResourceBindingSpec] = set()
    for binding_group in binding_groups:
        for binding in binding_group:
            if binding.engine_type != 'pytest' or binding in seen:
                continue
            seen.add(binding)
            merged.append(binding)
    return tuple(merged)


def _install_request_bridge() -> None:
    if pytest is None:
        return

    _REQUEST_BRIDGE['refcount'] += 1
    if _REQUEST_BRIDGE['refcount'] > 1:
        return

    fixture_request_type = getattr(pytest, 'FixtureRequest', None)
    if fixture_request_type is None:
        return

    had_existing = hasattr(fixture_request_type, 'get_resource')
    previous_value = getattr(
        fixture_request_type,
        'get_resource',
        None,
    )

    def _get_resource(self, resource_name: str) -> object:
        if had_existing and callable(previous_value):
            try:
                return previous_value(self, resource_name)
            except LookupError:
                pass

        if not _ACTIVE_RESOURCES:
            msg = (
                'Pytest request does not expose resource '
                f'{resource_name!r}'
            )
            raise LookupError(msg)

        resources = _ACTIVE_RESOURCES[-1]
        if resource_name not in resources:
            msg = (
                'Pytest request does not expose resource '
                f'{resource_name!r}'
            )
            raise LookupError(msg)
        return resources[resource_name]

    fixture_request_type.get_resource = _get_resource
    _REQUEST_BRIDGE['state'] = (
        fixture_request_type,
        had_existing,
        previous_value,
    )


def _restore_request_bridge() -> None:
    if _REQUEST_BRIDGE['refcount'] == 0:
        return

    _REQUEST_BRIDGE['refcount'] -= 1
    if _REQUEST_BRIDGE['refcount'] > 0:
        return

    state = _REQUEST_BRIDGE['state']
    if state is None:
        return

    fixture_request_type, had_existing, previous_value = state
    if had_existing:
        fixture_request_type.get_resource = previous_value
    else:
        delattr(fixture_request_type, 'get_resource')
    _REQUEST_BRIDGE['state'] = None
