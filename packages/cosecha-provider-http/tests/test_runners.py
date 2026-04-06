from __future__ import annotations

from argparse import Namespace
from io import BytesIO
from types import SimpleNamespace

import pytest

from cosecha.provider.http import asgi_runner, wsgi_runner


def test_asgi_runner_uses_uvicorn_with_loaded_application(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}
    app = object()

    monkeypatch.setattr(
        asgi_runner,
        'load_application_from_source_ref',
        lambda source_ref, *, backend, root_path: (app, False),
    )
    monkeypatch.setattr(
        asgi_runner,
        '_build_parser',
        lambda: SimpleNamespace(
            parse_args=lambda: Namespace(
                app='apps.py:create_asgi',
                host='127.0.0.1',
                port=8000,
                root_path='/workspace',
                server='uvicorn',
                ssl_certfile='cert.pem',
                ssl_keyfile='key.pem',
                ssl_ca_certs='ca.pem',
            ),
        ),
    )
    monkeypatch.setattr(
        asgi_runner,
        'uvicorn',
        SimpleNamespace(
            run=lambda current_app, **kwargs: calls.update(
                app=current_app,
                kwargs=kwargs,
            ),
        ),
    )

    asgi_runner.main()

    assert calls['app'] is app
    assert calls['kwargs'] == {
        'host': '127.0.0.1',
        'port': 8000,
        'ssl_certfile': 'cert.pem',
        'ssl_keyfile': 'key.pem',
        'ssl_ca_certs': 'ca.pem',
        'log_level': 'warning',
        'access_log': False,
    }


def test_asgi_runner_rejects_missing_uvicorn_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        asgi_runner,
        'load_application_from_source_ref',
        lambda source_ref, *, backend, root_path: (object(), False),
    )
    monkeypatch.setattr(
        asgi_runner,
        '_build_parser',
        lambda: SimpleNamespace(
            parse_args=lambda: Namespace(
                app='apps.py:create_asgi',
                host='127.0.0.1',
                port=8000,
                root_path='/workspace',
                server='uvicorn',
                ssl_certfile=None,
                ssl_keyfile=None,
                ssl_ca_certs=None,
            ),
        ),
    )
    monkeypatch.setattr(asgi_runner, 'uvicorn', None)

    with pytest.raises(
        ModuleNotFoundError,
        match='ASGI standalone runtime requires uvicorn',
    ):
        asgi_runner.main()


def test_wsgi_runner_handler_translates_environ_and_response() -> None:
    captured: dict[str, object] = {}

    def app(environ, start_response):
        captured['environ'] = environ
        start_response('201 Created', [('X-Test', 'ok')])
        return [b'hello']

    handler_type = wsgi_runner._build_wsgi_handler(app)
    handler = object.__new__(handler_type)
    handler.path = '/demo?name=uve'
    handler.command = 'POST'
    handler.request_version = 'HTTP/1.1'
    handler.client_address = ('127.0.0.1', 12345)
    handler.server = SimpleNamespace(
        server_name='localhost',
        server_port=8080,
    )
    handler.headers = {
        'Content-Length': '5',
        'Content-Type': 'text/plain',
        'X-Trace': 'trace-1',
    }
    handler.rfile = BytesIO(b'hello')
    handler.wfile = BytesIO()
    sent: list[tuple[str, object]] = []
    handler.send_response = lambda status_code: sent.append(
        ('status', status_code),
    )
    handler.send_header = lambda name, value: sent.append((name, value))
    handler.end_headers = lambda: sent.append(('end', None))

    handler._handle_wsgi_request()

    environ = captured['environ']
    assert environ['PATH_INFO'] == '/demo'
    assert environ['QUERY_STRING'] == 'name=uve'
    assert environ['REQUEST_METHOD'] == 'POST'
    assert environ['CONTENT_TYPE'] == 'text/plain'
    assert environ['HTTP_X_TRACE'] == 'trace-1'
    assert handler.wfile.getvalue() == b'hello'
    assert ('status', 201) in sent
    assert ('X-Test', 'ok') in sent
    assert ('Content-Length', '5') in sent
    assert ('Connection', 'close') in sent


def test_wsgi_runner_main_creates_threading_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = object()
    events: list[tuple[str, object]] = []

    monkeypatch.setattr(
        wsgi_runner,
        'load_application_from_source_ref',
        lambda source_ref, *, backend, root_path: (app, False),
    )
    monkeypatch.setattr(
        wsgi_runner,
        '_build_parser',
        lambda: SimpleNamespace(
            parse_args=lambda: Namespace(
                app='apps.py:create_wsgi',
                host='127.0.0.1',
                port=8080,
                root_path='/workspace',
            ),
        ),
    )

    class _Server:
        def __init__(self, address, handler):
            events.append(('address', address))
            events.append(('handler_name', handler.__name__))

        def serve_forever(self, poll_interval: float = 0.5) -> None:
            events.append(('serve_forever', poll_interval))

        def server_close(self) -> None:
            events.append(('server_close', True))

    monkeypatch.setattr(wsgi_runner, 'ThreadingHTTPServer', _Server)

    wsgi_runner.main()

    assert events[0] == ('address', ('127.0.0.1', 8080))
    assert events[1][0] == 'handler_name'
    assert ('serve_forever', 0.5) in events
    assert ('server_close', True) in events
