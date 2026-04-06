from __future__ import annotations

import asyncio
import importlib.util
import textwrap

from typing import TYPE_CHECKING

import pytest

from cosecha.core.manifest_symbols import SymbolRef
from cosecha.core.resources import (
    ResourceError,
    ResourceManager,
    ResourceRequirement,
    validate_resource_requirements,
)
from cosecha.provider.http import (
    HttpApplicationHandle,
    HttpApplicationProvider,
    HttpTransportHandle,
    HttpTransportProvider,
    provider as http_provider_module,
)
from cosecha.provider.ssl import SslMaterialProvider
from cosecha_internal.testkit import write_text_tree


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path


HTTP_OK = 200


class _SlowHttpApplicationProvider(HttpApplicationProvider):
    async def acquire(self, requirement, *, mode: str):
        await asyncio.sleep(0.05)
        return super().acquire(requirement, mode=mode)


def _build_application_requirement(
    *,
    name: str,
    config: dict[str, object],
    scope: str = 'run',
    mode: str = 'ephemeral',
) -> ResourceRequirement:
    return ResourceRequirement(
        name=name,
        provider=HttpApplicationProvider(),
        scope=scope,  # type: ignore[arg-type]
        mode=mode,  # type: ignore[arg-type]
        config=config,
    )


def _build_transport_requirement(
    *,
    name: str,
    config: dict[str, object],
    depends_on: tuple[str, ...] = (),
    scope: str = 'run',
    mode: str = 'ephemeral',
) -> ResourceRequirement:
    return ResourceRequirement(
        name=name,
        provider=HttpTransportProvider(),
        scope=scope,  # type: ignore[arg-type]
        mode=mode,  # type: ignore[arg-type]
        config=config,
        depends_on=depends_on,
    )


def _build_ssl_requirement(
    *,
    name: str,
    config: dict[str, object],
    scope: str = 'test',
    mode: str = 'ephemeral',
) -> ResourceRequirement:
    return ResourceRequirement(
        name=name,
        provider=SslMaterialProvider(),
        scope=scope,  # type: ignore[arg-type]
        mode=mode,  # type: ignore[arg-type]
        config=config,
    )


def _write_demo_apps(tmp_path: Path) -> Path:
    app_path = tmp_path / 'apps.py'
    write_text_tree(
        tmp_path,
        {
            'apps.py': textwrap.dedent(
                """
                import json

                startup_calls = 0
                shutdown_calls = 0


                async def asgi_app(scope, receive, send):
                    global startup_calls, shutdown_calls
                    if scope['type'] == 'lifespan':
                        while True:
                            message = await receive()
                            if message['type'] == 'lifespan.startup':
                                startup_calls += 1
                                await send(
                                    {
                                        'type': (
                                            'lifespan.startup.complete'
                                        ),
                                    },
                                )
                            elif message['type'] == 'lifespan.shutdown':
                                shutdown_calls += 1
                                await send(
                                    {
                                        'type': (
                                            'lifespan.shutdown.complete'
                                        ),
                                    },
                                )
                                return

                    body = json.dumps(
                        {
                            'path': scope['path'],
                            'query': scope['query_string'].decode('utf-8'),
                            'type': 'asgi',
                        }
                    ).encode('utf-8')
                    await send(
                        {
                            'type': 'http.response.start',
                            'status': 200,
                            'headers': [
                                (b'content-type', b'application/json'),
                            ],
                        }
                    )
                    await send({'type': 'http.response.body', 'body': body})


                def create_asgi():
                    return asgi_app


                def wsgi_app(environ, start_response):
                    payload = json.dumps(
                        {
                            'path': environ['PATH_INFO'],
                            'query': environ.get('QUERY_STRING', ''),
                            'type': 'wsgi',
                        }
                    ).encode('utf-8')
                    start_response(
                        '200 OK',
                        [
                            ('Content-Length', str(len(payload))),
                            ('Content-Type', 'application/json'),
                        ],
                    )
                    return [payload]


                def create_wsgi():
                    return wsgi_app


                def bad_app(one):
                    return one
                """,
            ).strip()
            + '\n',
        },
    )
    return app_path


def _resolve_symbol(root_path: Path, raw: str) -> object:
    return SymbolRef.parse(raw).resolve(root_path=root_path)


@pytest.mark.parametrize(
    ['backend', 'symbol_name', 'variant'],
    [
        ('asgi', 'asgi_app', 'direct'),
        ('asgi', 'create_asgi', 'factory'),
        ('wsgi', 'wsgi_app', 'direct'),
        ('wsgi', 'create_wsgi', 'factory'),
    ],
)
def test_application_provider_loads_direct_symbols_and_factories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
    symbol_name: str,
    variant: str,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    provider = HttpApplicationProvider()
    requirement = _build_application_requirement(
        name=f'{backend}-app',
        config={
            'backend': backend,
            'app': f'{app_path.name}:{symbol_name}',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')

    assert isinstance(resource, HttpApplicationHandle)
    assert resource.backend == backend
    assert resource.is_factory is (variant == 'factory')
    assert resource.source_ref == f'{app_path.name}:{symbol_name}'

    provider.release(resource, requirement, mode='ephemeral')


def test_application_provider_uses_explicit_resource_name_and_env_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('COSECHA_HTTP_RESOURCE_NAME', 'env-http-app')
    provider = HttpApplicationProvider()
    requirement = _build_application_requirement(
        name='api',
        config={
            'backend': 'asgi',
            'app': f'{app_path.name}:create_asgi',
            'resource_name': 'manifest-http-app',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')

    assert resource.resource_name == 'env-http-app'
    assert resource.generated_resource_name is False

    provider.release(resource, requirement, mode='ephemeral')


def test_application_provider_generates_distinct_names_for_multiapp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    provider = HttpApplicationProvider()
    first_requirement = _build_application_requirement(
        name='api-a',
        config={
            'backend': 'asgi',
            'app': f'{app_path.name}:create_asgi',
            'resource_prefix': 'suite',
        },
    )
    second_requirement = _build_application_requirement(
        name='api-b',
        config={
            'backend': 'wsgi',
            'app': f'{app_path.name}:create_wsgi',
            'resource_prefix': 'suite',
        },
    )

    first = provider.acquire(first_requirement, mode='ephemeral')
    second = provider.acquire(second_requirement, mode='ephemeral')

    assert first.resource_name != second.resource_name
    assert first.resource_name.startswith('suite_api_a_')
    assert second.resource_name.startswith('suite_api_b_')

    provider.release(first, first_requirement, mode='ephemeral')
    provider.release(second, second_requirement, mode='ephemeral')


def test_application_provider_rejects_invalid_backend_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    provider = HttpApplicationProvider()
    requirement = _build_application_requirement(
        name='bad-app',
        config={
            'backend': 'asgi',
            'app': f'{app_path.name}:bad_app',
        },
    )

    with pytest.raises(ResourceError) as excinfo:
        provider.acquire(requirement, mode='ephemeral')

    assert excinfo.value.code == 'http_application_invalid_source'


@pytest.mark.asyncio
async def test_inprocess_transport_supports_asgi_and_lifespan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    app_provider = HttpApplicationProvider()
    transport_provider = HttpTransportProvider()
    app_requirement = _build_application_requirement(
        name='api',
        config={
            'backend': 'asgi',
            'app': f'{app_path.name}:create_asgi',
        },
    )
    transport_requirement = _build_transport_requirement(
        name='api-http',
        config={'backend': 'inprocess'},
        depends_on=('api',),
    )

    app_resource = app_provider.acquire(app_requirement, mode='ephemeral')
    transport = transport_provider.acquire(
        transport_requirement,
        mode='ephemeral',
    )
    response = await transport.request(
        'GET',
        '/health',
        query={'page': '1'},
    )

    assert response == {
        'status_code': HTTP_OK,
        'headers': {'content-type': 'application/json'},
        'body': {
            'path': '/health',
            'query': 'page=1',
            'type': 'asgi',
        },
    }
    assert _resolve_symbol(tmp_path, f'{app_path.name}:startup_calls') == 1

    transport_provider.release(
        transport,
        transport_requirement,
        mode='ephemeral',
    )
    app_provider.release(app_resource, app_requirement, mode='ephemeral')
    await asyncio.sleep(0.05)

    assert _resolve_symbol(tmp_path, f'{app_path.name}:shutdown_calls') == 1


@pytest.mark.asyncio
async def test_inprocess_transport_supports_wsgi(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    app_provider = HttpApplicationProvider()
    transport_provider = HttpTransportProvider()
    app_requirement = _build_application_requirement(
        name='api',
        config={
            'backend': 'wsgi',
            'app': f'{app_path.name}:create_wsgi',
        },
    )
    transport_requirement = _build_transport_requirement(
        name='api-http',
        config={'backend': 'inprocess'},
        depends_on=('api',),
    )

    app_resource = app_provider.acquire(app_requirement, mode='ephemeral')
    transport = transport_provider.acquire(
        transport_requirement,
        mode='ephemeral',
    )
    response = await transport.request(
        'GET',
        '/items',
        query={'kind': 'demo'},
    )

    assert response['status_code'] == HTTP_OK
    assert response['body'] == {
        'path': '/items',
        'query': 'kind=demo',
        'type': 'wsgi',
    }

    transport_provider.release(
        transport,
        transport_requirement,
        mode='ephemeral',
    )
    app_provider.release(app_resource, app_requirement, mode='ephemeral')


@pytest.mark.asyncio
async def test_inprocess_transport_ignores_ssl_resource(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    app_provider = HttpApplicationProvider()
    transport_provider = HttpTransportProvider()
    app_requirement = _build_application_requirement(
        name='api',
        config={
            'backend': 'asgi',
            'app': f'{app_path.name}:create_asgi',
        },
    )
    transport_requirement = _build_transport_requirement(
        name='api-http',
        config={
            'backend': 'inprocess',
            'ssl_resource': 'tls',
        },
        depends_on=('api',),
    )

    validated = validate_resource_requirements(
        (
            app_requirement,
            transport_requirement,
        ),
    )
    assert validated == (
        app_requirement,
        transport_requirement,
    )

    app_resource = app_provider.acquire(app_requirement, mode='ephemeral')
    transport = transport_provider.acquire(
        transport_requirement,
        mode='ephemeral',
    )
    response = await transport.request('GET', '/health')

    assert response['status_code'] == HTTP_OK
    assert transport.base_url == 'http://inprocess.local'

    transport_provider.release(
        transport,
        transport_requirement,
        mode='ephemeral',
    )
    app_provider.release(app_resource, app_requirement, mode='ephemeral')


@pytest.mark.asyncio
async def test_standalone_wsgi_transport_chooses_free_port_and_serves_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    app_provider = HttpApplicationProvider()
    transport_provider = HttpTransportProvider()
    app_requirement = _build_application_requirement(
        name='api',
        config={
            'backend': 'wsgi',
            'app': f'{app_path.name}:create_wsgi',
        },
    )
    transport_requirement = _build_transport_requirement(
        name='api-http',
        config={'backend': 'standalone'},
        depends_on=('api',),
    )

    app_resource = app_provider.acquire(app_requirement, mode='ephemeral')
    transport = transport_provider.acquire(
        transport_requirement,
        mode='ephemeral',
    )

    assert isinstance(transport, HttpTransportHandle)
    assert transport.port is not None
    response = await transport.request('GET', '/demo')

    assert response['status_code'] == HTTP_OK
    assert response['body']['type'] == 'wsgi'

    transport_provider.release(
        transport,
        transport_requirement,
        mode='ephemeral',
    )
    app_provider.release(app_resource, app_requirement, mode='ephemeral')


@pytest.mark.asyncio
async def test_standalone_transports_choose_distinct_free_ports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    app_provider = HttpApplicationProvider()
    transport_provider = HttpTransportProvider()
    app_a_requirement = _build_application_requirement(
        name='api-a',
        config={
            'backend': 'wsgi',
            'app': f'{app_path.name}:create_wsgi',
        },
    )
    app_b_requirement = _build_application_requirement(
        name='api-b',
        config={
            'backend': 'wsgi',
            'app': f'{app_path.name}:create_wsgi',
        },
    )
    transport_a_requirement = _build_transport_requirement(
        name='api-a-http',
        config={'backend': 'standalone'},
        depends_on=('api-a',),
    )
    transport_b_requirement = _build_transport_requirement(
        name='api-b-http',
        config={'backend': 'standalone'},
        depends_on=('api-b',),
    )

    app_a = app_provider.acquire(app_a_requirement, mode='ephemeral')
    app_b = app_provider.acquire(app_b_requirement, mode='ephemeral')
    transport_a = transport_provider.acquire(
        transport_a_requirement,
        mode='ephemeral',
    )
    transport_b = transport_provider.acquire(
        transport_b_requirement,
        mode='ephemeral',
    )

    assert transport_a.port != transport_b.port

    transport_provider.release(
        transport_a,
        transport_a_requirement,
        mode='ephemeral',
    )
    transport_provider.release(
        transport_b,
        transport_b_requirement,
        mode='ephemeral',
    )
    app_provider.release(app_a, app_a_requirement, mode='ephemeral')
    app_provider.release(app_b, app_b_requirement, mode='ephemeral')


@pytest.mark.skipif(
    importlib.util.find_spec('uvicorn') is None
    and importlib.util.find_spec('hypercorn') is None,
    reason='ASGI standalone backend requires uvicorn or hypercorn',
)
@pytest.mark.asyncio
async def test_standalone_transport_supports_asgi(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    app_provider = HttpApplicationProvider()
    transport_provider = HttpTransportProvider()
    app_requirement = _build_application_requirement(
        name='api',
        config={
            'backend': 'asgi',
            'app': f'{app_path.name}:create_asgi',
        },
    )
    transport_requirement = _build_transport_requirement(
        name='api-http',
        config={'backend': 'standalone'},
        depends_on=('api',),
    )

    app_resource = app_provider.acquire(app_requirement, mode='ephemeral')
    transport = transport_provider.acquire(
        transport_requirement,
        mode='ephemeral',
    )
    response = await transport.request('GET', '/demo')

    assert response['status_code'] == HTTP_OK
    assert response['body']['type'] == 'asgi'

    transport_provider.release(
        transport,
        transport_requirement,
        mode='ephemeral',
    )
    app_provider.release(app_resource, app_requirement, mode='ephemeral')


@pytest.mark.asyncio
async def test_transport_live_uses_existing_server() -> None:
    transport_provider = HttpTransportProvider()
    live_requirement = _build_transport_requirement(
        name='api-live',
        config={
            'backend': 'live',
            'base_url': 'http://127.0.0.1:9000',
        },
        mode='live',
    )

    transport = transport_provider.acquire(live_requirement, mode='live')

    assert transport.backend == 'live'
    assert transport.base_url == 'http://127.0.0.1:9000'


@pytest.mark.skipif(
    importlib.util.find_spec('uvicorn') is None
    and importlib.util.find_spec('hypercorn') is None,
    reason='ASGI standalone backend requires uvicorn or hypercorn',
)
@pytest.mark.asyncio
async def test_transport_can_compose_http_app_and_ssl_dependencies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    manager = ResourceManager()
    app_requirement = _build_application_requirement(
        name='api',
        scope='test',
        config={
            'backend': 'asgi',
            'app': f'{app_path.name}:create_asgi',
        },
    )
    app_requirement = ResourceRequirement(
        name=app_requirement.name,
        provider=_SlowHttpApplicationProvider(),
        scope=app_requirement.scope,
        mode=app_requirement.mode,
        config=app_requirement.config,
    )
    ssl_requirement = _build_ssl_requirement(
        name='tls',
        config={},
    )
    transport_requirement = _build_transport_requirement(
        name='api-http',
        scope='test',
        config={
            'backend': 'standalone',
            'ssl_resource': 'tls',
        },
        depends_on=('api', 'tls'),
    )

    def _unexpected_registry_lookup(*args, **kwargs):
        del args, kwargs
        msg = 'registry fallback should not be used'
        raise AssertionError(msg)

    monkeypatch.setattr(
        http_provider_module,
        '_resolve_registered_application_handle',
        _unexpected_registry_lookup,
    )

    acquired = await manager.acquire_for_test(
        'test-1',
        (app_requirement, ssl_requirement, transport_requirement),
    )
    transport = acquired['api-http']
    response = await transport.request('GET', '/secure')

    assert transport.base_url.startswith('https://')
    assert response['status_code'] == HTTP_OK
    assert response['body']['type'] == 'asgi'

    await manager.release_for_test('test-1')


@pytest.mark.asyncio
@pytest.mark.skipif(
    importlib.util.find_spec('uvicorn') is None
    and importlib.util.find_spec('hypercorn') is None,
    reason='ASGI standalone backend requires uvicorn or hypercorn',
)
async def test_transport_accepts_application_resource_alias_with_tls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    manager = ResourceManager()
    app_requirement = _build_application_requirement(
        name='api',
        scope='test',
        config={
            'backend': 'asgi',
            'app': f'{app_path.name}:create_asgi',
        },
    )
    ssl_requirement = _build_ssl_requirement(
        name='tls',
        config={},
    )
    transport_requirement = _build_transport_requirement(
        name='api-http',
        scope='test',
        config={
            'backend': 'standalone',
            'application_resource': 'api',
            'ssl_resource': 'tls',
        },
        depends_on=('tls',),
    )

    acquired = await manager.acquire_for_test(
        'test-1',
        (app_requirement, ssl_requirement, transport_requirement),
    )
    transport = acquired['api-http']
    response = await transport.request('GET', '/alias')

    assert transport.base_url.startswith('https://')
    assert response['status_code'] == HTTP_OK
    assert response['body']['type'] == 'asgi'

    await manager.release_for_test('test-1')


@pytest.mark.asyncio
async def test_transport_rejects_wsgi_ssl_dependency_combination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_path = _write_demo_apps(tmp_path)
    monkeypatch.chdir(tmp_path)
    manager = ResourceManager()
    app_requirement = _build_application_requirement(
        name='api',
        scope='test',
        config={
            'backend': 'wsgi',
            'app': f'{app_path.name}:create_wsgi',
        },
    )
    ssl_requirement = _build_ssl_requirement(
        name='tls',
        config={},
    )
    transport_requirement = _build_transport_requirement(
        name='api-http',
        scope='test',
        config={
            'backend': 'standalone',
            'ssl_resource': 'tls',
        },
        depends_on=('api', 'tls'),
    )

    with pytest.raises(ResourceError) as excinfo:
        await manager.acquire_for_test(
            'test-1',
            (app_requirement, ssl_requirement, transport_requirement),
        )

    assert excinfo.value.code == 'http_transport_tls_requires_asgi'


@pytest.mark.parametrize('depends_on', [(), ('api-a', 'api-b')])
def test_transport_requires_single_application_dependency(
    depends_on: tuple[str, ...],
) -> None:
    provider = HttpTransportProvider()
    requirement = _build_transport_requirement(
        name='api-http',
        config={'backend': 'inprocess'},
        depends_on=depends_on,
    )

    with pytest.raises(ResourceError) as excinfo:
        provider.acquire(requirement, mode='ephemeral')

    assert (
        excinfo.value.code == 'http_transport_application_dependency_missing'
    )


def test_live_transport_requires_base_url() -> None:
    provider = HttpTransportProvider()
    requirement = _build_transport_requirement(
        name='api-live',
        config={'backend': 'live'},
        mode='live',
    )

    with pytest.raises(ResourceError) as excinfo:
        provider.acquire(requirement, mode='live')

    assert excinfo.value.code == 'http_transport_base_url_missing'


def test_live_transport_ignores_optional_resource_aliases_in_validation(
) -> None:
    requirement = _build_transport_requirement(
        name='api-live',
        config={
            'backend': 'live',
            'base_url': 'http://127.0.0.1:9000',
            'application_resource': 'api',
            'ssl_resource': 'tls',
        },
        mode='live',
    )

    validated = validate_resource_requirements((requirement,))

    assert validated == (requirement,)


def test_auto_standalone_server_prefers_uvicorn() -> None:
    original_checker = http_provider_module._is_optional_server_available
    http_provider_module._is_optional_server_available = (
        lambda server_name: server_name == 'uvicorn'
    )
    try:
        result = http_provider_module._resolve_standalone_server(
            'auto',
            requirement_name='api-http',
        )
    finally:
        http_provider_module._is_optional_server_available = original_checker

    assert result == 'uvicorn'


def test_auto_standalone_server_requires_installed_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        http_provider_module,
        '_is_optional_server_available',
        lambda server_name: False,
    )

    with pytest.raises(ResourceError) as excinfo:
        http_provider_module._resolve_standalone_server(
            'auto',
            requirement_name='api-http',
        )

    assert excinfo.value.code == 'http_transport_server_unavailable'


def test_explicit_hypercorn_requires_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        http_provider_module,
        '_is_optional_server_available',
        lambda server_name: False,
    )

    with pytest.raises(ResourceError) as excinfo:
        http_provider_module._resolve_standalone_server(
            'hypercorn',
            requirement_name='api-http',
        )

    assert excinfo.value.code == 'http_transport_server_unavailable'


def test_auto_standalone_server_falls_back_to_hypercorn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        http_provider_module,
        '_is_optional_server_available',
        lambda server_name: server_name == 'hypercorn',
    )

    result = http_provider_module._resolve_standalone_server(
        'auto',
        requirement_name='api-http',
    )

    assert result == 'hypercorn'


def test_standalone_tls_requires_cert_and_key() -> None:
    requirement = _build_transport_requirement(
        name='api-http',
        config={
            'backend': 'standalone',
            'ssl_certfile': 'server.crt',
        },
    )
    config = http_provider_module.HttpTransportConfig.from_requirement(
        requirement,
    )

    with pytest.raises(ResourceError) as excinfo:
        config.validate_standalone_tls(requirement_name='api-http')

    assert excinfo.value.code == 'http_transport_ssl_material_incomplete'
