# ruff: noqa: PLR0915
from __future__ import annotations

import json
import subprocess

from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from cosecha.core.resources import ResourceError
from cosecha.provider.http import (
    HttpApplicationHandle,
    HttpTransportConfig,
    provider as http_provider_module,
)


HTTP_TEAPOT_STATUS = 418
HTTP_SERVER_ERROR_STATUS = 500
OPTIONAL_PORT_VALUE = 12
TLS_PORT_VALUE = 443
TIMEOUT_SECONDS_VALUE = 2.5
TERMINATE_ASSERTION_MESSAGE = 'should not terminate'


def _build_asgi_handle() -> HttpApplicationHandle:
    async def _default_app(_scope, _receive, send):
        await send(
            {'type': 'http.response.start', 'status': 200, 'headers': []},
        )
        await send({'type': 'http.response.body', 'body': b'ok'})

    return HttpApplicationHandle(
        requirement_name='api',
        resource_name='api-resource',
        backend='asgi',
        app=_default_app,
        source_ref='apps.py:asgi_app',
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


def test_http_helpers_cover_uncovered_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module_path = tmp_path / 'apps.py'
    module_path.write_text('value = 1\n', encoding='utf-8')
    with pytest.raises(ResourceError, match='could not be resolved'):
        http_provider_module.load_application_from_source_ref(
            'apps.py:missing_symbol',
            backend='asgi',
            root_path=tmp_path,
        )

    assert (
        http_provider_module._matches_application_contract(object(), 'asgi')
        is False
    )
    assert http_provider_module._callable_supports_arity(object(), 1) is False

    def _callable():
        return None

    original_signature = http_provider_module.inspect.signature
    monkeypatch.setattr(
        http_provider_module.inspect,
        'signature',
        lambda _candidate: (_ for _ in ()).throw(ValueError('bad sig')),
    )
    assert http_provider_module._callable_supports_arity(_callable, 0) is False
    monkeypatch.setattr(
        http_provider_module.inspect,
        'signature',
        original_signature,
    )
    assert (
        http_provider_module._callable_supports_arity(lambda *args: None, 3)
        is True
    )
    assert (
        http_provider_module._callable_supports_arity(
            lambda *, value=None: value,
            0,
        )
        is True
    )

    class _HTTPErrorHeaders(dict):
        def items(self):
            return super().items()

    error = HTTPError(
        url='http://127.0.0.1:8000',
        code=418,
        msg='teapot',
        hdrs=_HTTPErrorHeaders({'Content-Type': 'application/json'}),
        fp=BytesIO(b'{"ok": false}'),
    )
    monkeypatch.setattr(
        http_provider_module,
        'urlopen',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(error),
    )
    error_response = http_provider_module._perform_http_request(
        'http://127.0.0.1:8000',
        'GET',
        {},
        b'',
        None,
    )
    assert error_response['status_code'] == HTTP_TEAPOT_STATUS

    assert http_provider_module._probe_http_server('http://localhost') is False
    monkeypatch.setattr(
        http_provider_module,
        '_is_optional_server_available',
        lambda name: name == 'uvicorn',
    )
    assert (
        http_provider_module._resolve_standalone_server(
            'uvicorn',
            requirement_name='api-http',
        )
        == 'uvicorn'
    )
    monkeypatch.setattr(
        http_provider_module,
        '_is_optional_server_available',
        lambda name: name == 'hypercorn',
    )
    assert (
        http_provider_module._resolve_standalone_server(
            'hypercorn',
            requirement_name='api-http',
        )
        == 'hypercorn'
    )
    monkeypatch.setattr(
        http_provider_module,
        '_is_optional_server_available',
        lambda _name: False,
    )
    with pytest.raises(ResourceError, match='requires `uvicorn`'):
        http_provider_module._resolve_standalone_server(
            'uvicorn',
            requirement_name='api-http',
        )

    assert (
        http_provider_module._prepare_request_body({}, b'bytes')[0] == b'bytes'
    )
    assert http_provider_module._prepare_request_body({}, 'text')[0] == b'text'
    json_body, headers = http_provider_module._prepare_request_body(
        {},
        {'a': 1},
    )
    assert json.loads(json_body.decode('utf-8')) == {'a': 1}
    assert headers['Content-Type'] == 'application/json'
    assert http_provider_module._deserialize_response_body(b'', {}) == ''
    assert (
        http_provider_module._deserialize_response_body(
            b'\xff',
            {'Content-Type': 'application/json'},
        )
        == '\ufffd'
    )
    assert (
        http_provider_module._server_error_response()['status_code']
        == HTTP_SERVER_ERROR_STATUS
    )

    asgi_handle = _build_asgi_handle()
    popen_calls: list[list[str]] = []
    monkeypatch.setattr(
        http_provider_module.subprocess,
        'Popen',
        lambda command, **_kwargs: (
            popen_calls.append(command) or SimpleNamespace()
        ),
    )
    http_provider_module._spawn_standalone_transport_process(
        asgi_handle,
        standalone_server='uvicorn',
        host='127.0.0.1',
        port=9000,
        ssl_certfile='cert.pem',
        ssl_keyfile='key.pem',
        ssl_ca_certs='ca.pem',
    )
    assert '--ssl-certfile' in popen_calls[0]
    assert '--ssl-keyfile' in popen_calls[0]
    assert '--ssl-ca-certs' in popen_calls[0]

    class _ExitedProcess:
        def poll(self):
            return 1

    with pytest.raises(ResourceError, match='exited during startup'):
        http_provider_module._wait_for_http_server(
            'http://127.0.0.1:9000',
            timeout_seconds=1.0,
            process=_ExitedProcess(),
            requirement_name='api-http',
        )

    class _RunningProcess:
        def poll(self):
            return None

    monotonic_values = iter((0.0, 0.05, 0.11))
    monkeypatch.setattr(
        http_provider_module.time,
        'monotonic',
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        http_provider_module.time,
        'sleep',
        lambda _seconds: None,
    )
    monkeypatch.setattr(
        http_provider_module,
        '_probe_http_server',
        lambda _url: False,
    )
    with pytest.raises(ResourceError, match='did not become ready'):
        http_provider_module._wait_for_http_server(
            'http://127.0.0.1:9000',
            timeout_seconds=0.1,
            process=_RunningProcess(),
            requirement_name='api-http',
        )

    invalid_context = _DependencyContext(dependencies={'api': object()})
    with pytest.raises(ResourceError, match='invalid application dependency'):
        http_provider_module._resolve_application_dependency_handle(
            'api',
            dependency_context=invalid_context,
            requirement_name='api-http',
        )

    config = HttpTransportConfig(
        backend='standalone',
        application_resource='api',
        ssl_resource='tls',
    )
    with pytest.raises(ResourceError, match='requires SSL dependency context'):
        http_provider_module._resolve_transport_ssl_materials(
            config,
            dependency_context=None,
            requirement_name='api-http',
        )

    missing_dep_context = _DependencyContext(
        dependencies={'api': _build_asgi_handle()},
    )
    with pytest.raises(
        ResourceError, match='could not resolve its SSL dependency',
    ):
        http_provider_module._resolve_transport_ssl_materials(
            config,
            dependency_context=missing_dep_context,
            requirement_name='api-http',
        )

    invalid_tls_context = _DependencyContext(
        dependencies={'tls': SimpleNamespace(cert_path=None, key_path='')},
    )
    with pytest.raises(
        ResourceError,
        match='to expose `cert_path` and `key_path`',
    ):
        http_provider_module._resolve_transport_ssl_materials(
            config,
            dependency_context=invalid_tls_context,
            requirement_name='api-http',
        )

    assert (
        http_provider_module._read_optional_dependency_path(
            SimpleNamespace(path='x'),
            'path',
        )
        == 'x'
    )
    assert (
        http_provider_module._read_optional_dependency_path(
            SimpleNamespace(path=''),
            'path',
        )
        is None
    )
    with pytest.raises(
        ResourceError,
        match='must provide both `ssl_certfile` and `ssl_keyfile`',
    ):
        http_provider_module._validate_transport_tls(
            requirement_name='api-http',
            ssl_certfile='cert.pem',
            ssl_keyfile=None,
        )

    with pytest.raises(
        ResourceError,
        match='missing an application resource dependency',
    ):
        http_provider_module._resolve_registered_application_handle(
            None,
            requirement_name='api-http',
        )
    with pytest.raises(
        ResourceError,
        match='could not resolve its application dependency',
    ):
        http_provider_module._resolve_registered_application_handle(
            'missing',
            requirement_name='api-http',
        )

    with pytest.raises(ValueError, match="one of 'asgi' or 'wsgi'"):
        http_provider_module._normalize_application_backend('x')
    with pytest.raises(ValueError, match='backend must be one of'):
        http_provider_module._normalize_transport_backend('x')
    with pytest.raises(ValueError, match="one of 'auto' or 'preserve'"):
        http_provider_module._normalize_cleanup_policy('x')
    with pytest.raises(
        ValueError,
        match="one of 'auto', 'uvicorn' or 'hypercorn'",
    ):
        http_provider_module._normalize_standalone_server('x')

    assert (
        http_provider_module._read_config_value(
            {'legacy': 1},
            'new',
            aliases=('legacy',),
        )
        == 1
    )
    with pytest.raises(ValueError, match='optional non-empty string'):
        http_provider_module._read_optional_str('')
    with pytest.raises(ValueError, match='must be a non-empty string'):
        http_provider_module._read_non_empty_str('', field_name='host')
    with pytest.raises(ValueError, match='must be an integer'):
        http_provider_module._read_optional_int([], field_name='port')
    assert (
        http_provider_module._read_optional_int(
            str(OPTIONAL_PORT_VALUE),
            field_name='port',
        )
        == OPTIONAL_PORT_VALUE
    )
    assert (
        http_provider_module._read_optional_port(
            TLS_PORT_VALUE,
            field_name='port',
        )
        == TLS_PORT_VALUE
    )
    assert (
        http_provider_module._read_non_negative_float(
            str(TIMEOUT_SECONDS_VALUE),
            field_name='timeout',
        )
        == TIMEOUT_SECONDS_VALUE
    )
    with pytest.raises(ValueError, match='must be a valid TCP port'):
        http_provider_module._read_optional_port(70000, field_name='port')
    with pytest.raises(ValueError, match='must be non-negative'):
        http_provider_module._read_non_negative_float(-1, field_name='timeout')
    with pytest.raises(ValueError, match='must be numeric'):
        http_provider_module._read_non_negative_float([], field_name='timeout')

    class _Exited:
        def poll(self):
            return 0

        def terminate(self):
            raise AssertionError(TERMINATE_ASSERTION_MESSAGE)

    http_provider_module._terminate_process(_Exited())

    class _TimeoutProcess:
        def __init__(self) -> None:
            self.killed = False

        def poll(self):
            return None

        def terminate(self):
            return None

        def wait(self, *, timeout: int):
            if not self.killed:
                raise subprocess.TimeoutExpired(cmd='python', timeout=timeout)

        def kill(self):
            self.killed = True

    http_provider_module._terminate_process(_TimeoutProcess())

    kill_calls: list[int] = []
    monkeypatch.setattr(
        http_provider_module.os,
        'kill',
        lambda pid, _sig: kill_calls.append(pid),
    )
    http_provider_module._terminate_pid(123)
    assert kill_calls == [123]

    with pytest.raises(ValueError, match='Invalid HTTP external handle'):
        http_provider_module._decode_external_handle('{bad')
    with pytest.raises(
        ValueError, match='Invalid HTTP external handle payload',
    ):
        http_provider_module._decode_external_handle('[]')
