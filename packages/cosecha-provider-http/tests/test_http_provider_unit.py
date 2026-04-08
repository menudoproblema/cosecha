from __future__ import annotations

import asyncio
import json
import runpy
import sys

from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.resources import ResourceError, ResourceRequirement
from cosecha.provider.http import (
    HttpApplicationHandle,
    HttpApplicationProvider,
    HttpTransportHandle,
    HttpTransportProvider,
    provider as http_provider_module,
)


HTTP_OK_STATUS = 200
HTTP_SERVER_ERROR_STATUS = 500
WAIT_BROKEN_MESSAGE = 'broken wait'
ASGI_FAILURE_MESSAGE = 'fail'


def _build_application_requirement(
    *,
    config: dict[str, object],
    name: str = 'api',
) -> ResourceRequirement:
    return ResourceRequirement(
        name=name,
        provider=HttpApplicationProvider(),
        scope='test',
        mode='ephemeral',
        config=config,
    )


def _build_transport_requirement(
    *,
    config: dict[str, object],
    depends_on: tuple[str, ...] = (),
    name: str = 'api-http',
) -> ResourceRequirement:
    return ResourceRequirement(
        name=name,
        provider=HttpTransportProvider(),
        scope='test',
        mode='ephemeral',
        config=config,
        depends_on=depends_on,
    )


def _build_asgi_handle(
    *,
    requirement_name: str = 'api',
    app=None,
) -> HttpApplicationHandle:
    async def _default_app(_scope, _receive, send):
        await send(
            {'type': 'http.response.start', 'status': 200, 'headers': []},
        )
        await send({'type': 'http.response.body', 'body': b'ok'})

    return HttpApplicationHandle(
        requirement_name=requirement_name,
        resource_name='api-resource',
        backend='asgi',
        app=app or _default_app,
        source_ref='apps.py:asgi_app',
        root_path=str(Path.cwd()),
        is_factory=False,
        cleanup_policy='auto',
        generated_resource_name=False,
    )


def _build_wsgi_handle(
    *,
    requirement_name: str = 'api',
    app=None,
) -> HttpApplicationHandle:
    def _default_app(_environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b'ok']

    return HttpApplicationHandle(
        requirement_name=requirement_name,
        resource_name='api-resource',
        backend='wsgi',
        app=app or _default_app,
        source_ref='apps.py:wsgi_app',
        root_path=str(Path.cwd()),
        is_factory=False,
        cleanup_policy='auto',
        generated_resource_name=False,
    )


class _DependencyContext:
    def __init__(
        self,
        *,
        dependencies: dict[str, object],
        capabilities: dict[str, dict[str, object]] | None = None,
    ) -> None:
        self._dependencies = dependencies
        self._capabilities = capabilities or {}

    def get_dependency(self, name: str):
        resource = self._dependencies.get(name)
        if resource is None:
            return None
        return SimpleNamespace(resource=resource)

    def get_capabilities(self, name: str) -> dict[str, object]:
        return dict(self._capabilities.get(name, {}))


@pytest.mark.asyncio
async def test_application_handle_lifespan_edge_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    non_asgi = _build_wsgi_handle()
    await non_asgi.ensure_started()
    await non_asgi.shutdown()

    handle = _build_asgi_handle()
    handle._lifespan_supported = False
    await handle.ensure_started()

    handle._lifespan_supported = None
    handle._lifespan_task = asyncio.create_task(asyncio.sleep(0))
    await handle.ensure_started()
    await asyncio.sleep(0)

    async def _pending_app(_scope, _receive, _send):
        await asyncio.sleep(0)

    failing_handle = _build_asgi_handle(app=_pending_app)

    async def _raise_runtime_error(awaitable, *_args, **_kwargs):
        awaitable.close()
        raise RuntimeError(WAIT_BROKEN_MESSAGE)

    original_wait_for = http_provider_module.asyncio.wait_for
    monkeypatch.setattr(
        http_provider_module.asyncio, 'wait_for', _raise_runtime_error,
    )
    await failing_handle.ensure_started()
    assert failing_handle._lifespan_supported is False

    startup_failed_handle = _build_asgi_handle(
        app=_build_startup_failed_asgi_app(),
    )
    monkeypatch.setattr(
        http_provider_module.asyncio,
        'wait_for',
        original_wait_for,
    )
    with pytest.raises(
        ResourceError, match='failed during ASGI lifespan startup',
    ):
        await startup_failed_handle.ensure_started()

    unexpected_startup_handle = _build_asgi_handle(
        app=_build_unexpected_startup_asgi_app(),
    )
    await unexpected_startup_handle.ensure_started()
    assert unexpected_startup_handle._lifespan_supported is False


def _build_startup_failed_asgi_app():
    async def _app(_scope, receive, send):
        message = await receive()
        if message.get('type') == 'lifespan.startup':
            await send({'type': 'lifespan.startup.failed'})

    return _app


def _build_unexpected_startup_asgi_app():
    async def _app(_scope, receive, send):
        message = await receive()
        if message.get('type') == 'lifespan.startup':
            await send({'type': 'lifespan.startup.unknown'})

    return _app


@pytest.mark.asyncio
async def test_shutdown_resets_on_missing_queues_and_unexpected_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = _build_asgi_handle()
    handle._lifespan_task = asyncio.create_task(asyncio.sleep(0))
    await handle.shutdown()

    handle = _build_asgi_handle()
    handle._lifespan_task = asyncio.create_task(asyncio.sleep(0))
    handle._lifespan_receive_queue = asyncio.Queue()
    handle._lifespan_response_queue = asyncio.Queue()
    await handle._lifespan_response_queue.put(
        {'type': 'lifespan.shutdown.failed'},
    )
    await handle.shutdown()
    assert handle._lifespan_task is None

    timeout_handle = _build_asgi_handle()
    timeout_handle._lifespan_task = asyncio.create_task(asyncio.sleep(0))
    timeout_handle._lifespan_receive_queue = asyncio.Queue()
    timeout_handle._lifespan_response_queue = asyncio.Queue()

    async def _raise_timeout(awaitable, *_args, **_kwargs):
        awaitable.close()
        raise TimeoutError

    monkeypatch.setattr(
        http_provider_module.asyncio, 'wait_for', _raise_timeout,
    )
    await timeout_handle.shutdown()
    assert timeout_handle._lifespan_task is None


def test_application_and_transport_provider_guard_methods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_provider = HttpApplicationProvider()
    app_requirement = _build_application_requirement(
        config={'backend': 'asgi', 'app': 'apps.py:asgi_app'},
    )
    app_provider.release(object(), app_requirement, mode='ephemeral')
    assert (
        app_provider.describe_external_handle(
            object(), app_requirement, mode='ephemeral',
        )
        is None
    )
    assert (
        app_provider.describe_capabilities(
            object(), app_requirement, mode='ephemeral',
        )
        == {}
    )
    app_provider.reap_orphan('h', app_requirement, mode='ephemeral')
    app_provider.revoke_orphan_access('h', app_requirement, mode='ephemeral')

    transport_provider = HttpTransportProvider()
    transport_requirement = _build_transport_requirement(
        config={
            'backend': 'standalone',
            'application_resource': 'api',
            'ssl_resource': 'tls',
        },
        depends_on=('api',),
    )
    assert transport_provider.resolve_dependency_names(
        transport_requirement, mode='ephemeral',
    ) == ('tls',)
    transport_provider.validate_dependency_capabilities(
        transport_requirement,
        mode='ephemeral',
        dependency_context=None,
    )
    transport_provider.release(
        object(), transport_requirement, mode='ephemeral',
    )
    assert (
        transport_provider.health_check(
            object(), transport_requirement, mode='ephemeral',
        )
        is False
    )
    assert (
        transport_provider.verify_integrity(
            object(), transport_requirement, mode='ephemeral',
        )
        is False
    )
    assert (
        transport_provider.describe_external_handle(
            object(), transport_requirement, mode='ephemeral',
        )
        is None
    )
    transport_provider.revoke_orphan_access(
        'h', transport_requirement, mode='ephemeral',
    )

    context = _DependencyContext(
        dependencies={'api': _build_asgi_handle()},
        capabilities={'tls': {}},
    )
    with pytest.raises(ResourceError, match='to expose TLS materials'):
        transport_provider.validate_dependency_capabilities(
            transport_requirement,
            mode='ephemeral',
            dependency_context=context,
        )

    tls_without_resource_requirement = _build_transport_requirement(
        config={
            'backend': 'standalone',
            'application_resource': 'api',
            'ssl_certfile': 'cert.pem',
            'ssl_keyfile': 'key.pem',
        },
        depends_on=('api',),
    )
    transport_provider.validate_dependency_capabilities(
        tls_without_resource_requirement,
        mode='ephemeral',
        dependency_context=context,
    )
    transport_provider.validate_dependency_capabilities(
        _build_transport_requirement(config={'backend': 'inprocess'}),
        mode='ephemeral',
        dependency_context=context,
    )
    transport_provider.validate_dependency_capabilities(
        _build_transport_requirement(
            config={
                'backend': 'standalone',
                'application_resource': 'api',
            },
            depends_on=('api',),
        ),
        mode='ephemeral',
        dependency_context=context,
    )

    preserve_handle = HttpTransportHandle(
        requirement_name='api-http',
        backend='standalone',
        base_url='http://127.0.0.1:8000',
        cleanup_policy='preserve',
        application_requirement_name='api',
        application_backend='asgi',
        process=SimpleNamespace(),
    )
    transport_provider.release(
        preserve_handle, transport_requirement, mode='ephemeral',
    )


@pytest.mark.asyncio
async def test_transport_handle_request_fallback_to_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = HttpTransportHandle(
        requirement_name='api-http',
        backend='inprocess',
        base_url='http://inprocess.local',
        cleanup_policy='auto',
        application_requirement_name='api',
        application_backend='asgi',
        application_handle=None,
    )
    registry_handle = _build_asgi_handle()
    monkeypatch.setitem(
        http_provider_module._APPLICATION_REGISTRY, 'api', registry_handle,
    )

    async def _fake_request(*_args, **_kwargs):
        return {'status_code': 200, 'headers': {}, 'body': 'ok'}

    monkeypatch.setattr(
        http_provider_module, '_request_inprocess_asgi', _fake_request,
    )
    response = await handle.request('GET', '/health')
    assert response['status_code'] == HTTP_OK_STATUS
    http_provider_module._APPLICATION_REGISTRY.clear()


def test_transport_provider_health_and_integrity_additional_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = HttpTransportProvider()
    requirement = _build_transport_requirement(
        config={'backend': 'standalone', 'application_resource': 'api'},
    )
    app_handle = _build_asgi_handle()
    inprocess = HttpTransportHandle(
        requirement_name='api-http',
        backend='inprocess',
        base_url='http://inprocess.local',
        cleanup_policy='auto',
        application_requirement_name='api',
        application_backend='wsgi',
        application_handle=app_handle,
    )
    assert (
        provider.health_check(inprocess, requirement, mode='ephemeral')
        is False
    )
    assert (
        provider.verify_integrity(inprocess, requirement, mode='ephemeral')
        is False
    )

    standalone_local = HttpTransportHandle(
        requirement_name='api-http',
        backend='standalone',
        base_url='http://127.0.0.1:8000',
        cleanup_policy='auto',
        application_requirement_name='api',
        application_backend='asgi',
        process=None,
        application_handle=app_handle,
    )
    assert (
        provider.health_check(standalone_local, requirement, mode='ephemeral')
        is True
    )

    mismatch_requirement = _build_transport_requirement(
        config={'backend': 'standalone', 'application_resource': 'api'},
        name='other-http',
    )
    assert (
        provider.verify_integrity(
            standalone_local, mismatch_requirement, mode='ephemeral',
        )
        is False
    )

    terminated: list[int] = []
    def _record_terminated_pid(pid: int) -> None:
        terminated.append(pid)

    monkeypatch.setattr(
        http_provider_module,
        '_terminate_pid',
        _record_terminated_pid,
    )
    provider.reap_orphan(
        json.dumps({'cleanup_policy': 'drop', 'pid': 777}),
        requirement,
        mode='ephemeral',
    )
    assert terminated == [777]
    provider.reap_orphan(
        json.dumps({'cleanup_policy': 'preserve'}),
        requirement,
        mode='ephemeral',
    )


@pytest.mark.asyncio
async def test_request_helpers_cover_wsgi_and_asgi_error_paths() -> None:
    class _ClosableIterable:
        def __init__(self) -> None:
            self.closed = False

        def __iter__(self):
            yield b'hello'

        def close(self) -> None:
            self.closed = True

    closable = _ClosableIterable()

    def _wsgi_app(_environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return closable

    wsgi_handle = _build_wsgi_handle(app=_wsgi_app)
    wsgi_response = await http_provider_module._request_inprocess_wsgi(
        wsgi_handle,
        'POST',
        '/demo',
        headers={'Content-Type': 'text/plain', 'X-Trace': '1'},
        query=None,
        body='hello',
    )
    assert wsgi_response['status_code'] == HTTP_OK_STATUS
    assert closable.closed is True

    error_wsgi_handle = _build_wsgi_handle(
        app=lambda _environ, _start_response: (_ for _ in ()).throw(
            RuntimeError('boom'),
        ),
    )
    assert (
        await http_provider_module._request_inprocess_wsgi(
            error_wsgi_handle,
            'GET',
            '/boom',
            headers=None,
            query=None,
            body=None,
        )
    )['status_code'] == HTTP_SERVER_ERROR_STATUS

    async def _asgi_reads_disconnect(scope, receive, send):
        del scope
        await receive()
        disconnect = await receive()
        assert disconnect['type'] == 'http.disconnect'
        await send(
            {'type': 'http.response.start', 'status': 200, 'headers': []},
        )
        await send({'type': 'http.response.body', 'body': b'ok'})

    asgi_handle = _build_asgi_handle(app=_asgi_reads_disconnect)
    asgi_response = await http_provider_module._request_inprocess_asgi(
        asgi_handle,
        'GET',
        '/demo',
        headers=None,
        query=None,
        body=b'payload',
    )
    assert asgi_response['status_code'] == HTTP_OK_STATUS

    async def _failing_asgi(_scope, _receive, _send):
        raise RuntimeError(ASGI_FAILURE_MESSAGE)

    failing_asgi = _build_asgi_handle(app=_failing_asgi)
    failing_asgi._lifespan_supported = False
    error_response = await http_provider_module._request_inprocess_asgi(
        failing_asgi,
        'GET',
        '/boom',
        headers=None,
        query=None,
        body=None,
    )
    assert error_response['status_code'] == HTTP_SERVER_ERROR_STATUS


def test_transport_and_config_special_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = http_provider_module.HttpTransportConfig(
        backend='standalone',
        application_resource='api',
        ssl_resource='tls',
        ssl_certfile=None,
        ssl_keyfile=None,
    )
    config.validate_standalone_tls(requirement_name='api-http')
    assert (
        config.resolve_application_requirement_name(
            _build_transport_requirement(
                config={'backend': 'standalone', 'ssl_resource': 'tls'},
                depends_on=('api', 'tls'),
            ),
        )
        == 'api'
    )

    provider = HttpTransportProvider()
    app_handle = _build_wsgi_handle()
    context = _DependencyContext(dependencies={'api': app_handle})
    requirement = _build_transport_requirement(
        config={
            'backend': 'standalone',
            'application_resource': 'api',
            'ssl_certfile': 'cert.pem',
            'ssl_keyfile': 'key.pem',
        },
        depends_on=('api',),
    )
    with pytest.raises(
        ResourceError, match='WSGI standalone does not support TLS',
    ):
        provider.acquire(
            requirement,
            mode='ephemeral',
            dependency_context=context,
        )

    asgi_context = _DependencyContext(
        dependencies={'api': _build_asgi_handle()},
    )
    requirement_asgi = _build_transport_requirement(
        config={'backend': 'standalone', 'application_resource': 'api'},
        depends_on=('api',),
    )
    process = SimpleNamespace(poll=lambda: None)
    monkeypatch.setattr(
        http_provider_module,
        '_spawn_standalone_transport_process',
        lambda *args, **kwargs: process,
    )
    monkeypatch.setattr(
        http_provider_module,
        '_resolve_standalone_server',
        lambda *_args, **_kwargs: 'uvicorn',
    )
    monkeypatch.setattr(
        http_provider_module,
        '_wait_for_http_server',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError('server down'),
        ),
    )
    terminated: list[object] = []
    def _record_terminated_process(proc: object) -> None:
        terminated.append(proc)

    monkeypatch.setattr(
        http_provider_module,
        '_terminate_process',
        _record_terminated_process,
    )
    with pytest.raises(RuntimeError, match='server down'):
        provider.acquire(
            requirement_asgi,
            mode='ephemeral',
            dependency_context=asgi_context,
        )
    assert terminated == [process]


@pytest.mark.asyncio
async def test_coroutine_runner_helpers() -> None:
    observed: list[str] = []

    async def _mark(label: str) -> None:
        observed.append(label)

    http_provider_module._run_coroutine_now_or_background(object())
    http_provider_module._run_coroutine_now_or_background(_mark('bg'))
    await asyncio.sleep(0)
    assert observed == ['bg']

    async def _inside_loop() -> None:
        http_provider_module._run_coroutine_now_or_background(_mark('loop'))
        await asyncio.sleep(0)

    await _inside_loop()
    assert 'loop' in observed

    task = asyncio.create_task(asyncio.sleep(10))
    await http_provider_module._cancel_task(task)
    await http_provider_module._await_task(
        asyncio.create_task(asyncio.sleep(0)),
    )


def test_run_coroutine_helper_uses_asyncio_run_outside_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[str] = []

    async def _mark() -> None:
        observed.append('sync')

    monkeypatch.setattr(
        http_provider_module.asyncio,
        'run',
        lambda coroutine: asyncio.new_event_loop().run_until_complete(
            coroutine,
        ),
    )
    http_provider_module._run_coroutine_now_or_background(_mark())
    assert observed == ['sync']


def test_main_guards_in_runners_via_run_module(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_provider_module = SimpleNamespace(
        load_application_from_source_ref=lambda *_args, **_kwargs: (
            object(),
            False,
        ),
    )
    monkeypatch.setitem(
        sys.modules, 'cosecha.provider.http.provider', fake_provider_module,
    )
    monkeypatch.setitem(
        sys.modules,
        'uvicorn',
        SimpleNamespace(run=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setitem(
        sys.modules,
        'hypercorn.asyncio',
        SimpleNamespace(serve=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setitem(
        sys.modules,
        'hypercorn.config',
        SimpleNamespace(Config=type('Config', (), {})),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'asgi_runner',
            '--app',
            'apps.py:asgi_app',
            '--host',
            '127.0.0.1',
            '--port',
            '8000',
            '--root-path',
            str(tmp_path),
            '--server',
            'uvicorn',
        ],
    )
    with pytest.warns(RuntimeWarning, match='found in sys.modules'):
        runpy.run_module(
            'cosecha.provider.http.asgi_runner',
            run_name='__main__',
        )

    class _Server:
        def __init__(self, *_args, **_kwargs):
            return

        def serve_forever(self, poll_interval: float = 0.5) -> None:
            del poll_interval

        def server_close(self) -> None:
            return

    monkeypatch.setattr(
        sys,
        'argv',
        [
            'wsgi_runner',
            '--app',
            'apps.py:wsgi_app',
            '--host',
            '127.0.0.1',
            '--port',
            '8080',
            '--root-path',
            str(tmp_path),
        ],
    )
    monkeypatch.setitem(
        sys.modules,
        'http.server',
        SimpleNamespace(
            BaseHTTPRequestHandler=object,
            ThreadingHTTPServer=_Server,
        ),
    )
    with pytest.warns(RuntimeWarning, match='found in sys.modules'):
        runpy.run_module(
            'cosecha.provider.http.wsgi_runner',
            run_name='__main__',
        )
