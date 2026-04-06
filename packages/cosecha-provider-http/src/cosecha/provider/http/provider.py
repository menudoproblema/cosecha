from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import os
import signal
import socket
import ssl
import subprocess
import sys
import time

from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

from cosecha.core.manifest_symbols import (
    ManifestValidationError,
    SymbolRef,
)
from cosecha.core.resources import ResourceDependencyContext, ResourceError


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Mapping
    from types import TracebackType


type HttpApplicationBackendKind = Literal['asgi', 'wsgi']
type HttpTransportBackendKind = Literal['inprocess', 'standalone', 'live']
type HttpCleanupPolicy = Literal['auto', 'preserve']
type HttpStandaloneServerKind = Literal['auto', 'uvicorn', 'hypercorn']

APPLICATION_INTERFACE_NAME = 'application/http'
DEFAULT_HTTP_TIMEOUT_SECONDS = 5
HTTP_INTERNAL_SERVER_ERROR = 500
MAX_TCP_PORT = 65_535
_APPLICATION_REGISTRY: dict[str, HttpApplicationHandle] = {}
_BACKGROUND_TASKS: set[asyncio.Task[Any]] = set()


@dataclass(slots=True, frozen=True)
class HttpApplicationConfig:
    backend: HttpApplicationBackendKind
    app: str
    resource_name: str | None = None
    resource_prefix: str = 'cosecha'
    cleanup_policy: HttpCleanupPolicy = 'auto'

    @classmethod
    def from_requirement(cls, requirement) -> HttpApplicationConfig:
        config = requirement.config
        return cls(
            backend=_normalize_application_backend(
                _read_config_value(
                    config,
                    'backend',
                    env_names=('COSECHA_HTTP_BACKEND',),
                ),
            ),
            app=_read_non_empty_str(
                _read_config_value(
                    config,
                    'app',
                    env_names=('COSECHA_HTTP_APP',),
                ),
                field_name='app',
            ),
            resource_name=_read_optional_str(
                _read_config_value(
                    config,
                    'resource_name',
                    aliases=('application_name',),
                    env_names=(
                        'COSECHA_HTTP_RESOURCE_NAME',
                        'COSECHA_HTTP_APPLICATION_NAME',
                    ),
                ),
            ),
            resource_prefix=_read_non_empty_str(
                _read_config_value(
                    config,
                    'resource_prefix',
                    aliases=('application_prefix',),
                    env_names=(
                        'COSECHA_HTTP_RESOURCE_PREFIX',
                        'COSECHA_HTTP_APPLICATION_PREFIX',
                    ),
                    default='cosecha',
                ),
                field_name='resource_prefix',
            ),
            cleanup_policy=_normalize_cleanup_policy(
                _read_config_value(
                    config,
                    'cleanup_policy',
                    env_names=('COSECHA_HTTP_CLEANUP_POLICY',),
                    default='auto',
                ),
            ),
        )

    def resolve_resource_name(
        self,
        *,
        requirement_name: str,
    ) -> tuple[str, bool]:
        if self.resource_name is not None:
            return (self.resource_name, False)

        suffix = uuid4().hex[:12]
        normalized_name = requirement_name.replace('/', '_').replace('-', '_')
        return (f'{self.resource_prefix}_{normalized_name}_{suffix}', True)


@dataclass(slots=True)
class HttpApplicationHandle:
    requirement_name: str
    resource_name: str
    backend: HttpApplicationBackendKind
    app: Any
    source_ref: str
    root_path: str
    is_factory: bool
    cleanup_policy: HttpCleanupPolicy
    generated_resource_name: bool
    _lifespan_supported: bool | None = None
    _lifespan_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _lifespan_receive_queue: asyncio.Queue[dict[str, object]] | None = None
    _lifespan_response_queue: asyncio.Queue[dict[str, object]] | None = None
    _lifespan_task: asyncio.Task[None] | None = None

    def build_external_handle(self) -> str:
        return json.dumps(
            {
                'backend': self.backend,
                'cleanup_policy': self.cleanup_policy,
                'generated_resource_name': self.generated_resource_name,
                'is_factory': self.is_factory,
                'requirement_name': self.requirement_name,
                'resource_name': self.resource_name,
                'root_path': self.root_path,
                'source_ref': self.source_ref,
            },
            sort_keys=True,
        )

    async def ensure_started(self) -> None:
        if self.backend != 'asgi':
            return

        async with self._lifespan_lock:
            if self._lifespan_supported is False:
                return
            if self._lifespan_task is not None:
                return

            receive_queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()
            response_queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()

            async def receive() -> dict[str, object]:
                return await receive_queue.get()

            async def send(message: dict[str, object]) -> None:
                await response_queue.put(message)

            task = asyncio.create_task(
                self.app(
                    {
                        'type': 'lifespan',
                        'asgi': {'version': '3.0', 'spec_version': '2.3'},
                    },
                    receive,
                    send,
                ),
            )
            await receive_queue.put({'type': 'lifespan.startup'})

            try:
                message = await asyncio.wait_for(response_queue.get(), 1.0)
            except TimeoutError:
                await _cancel_task(task)
                self._lifespan_supported = False
                return
            except Exception:
                await _cancel_task(task)
                self._lifespan_supported = False
                return

            message_type = str(message.get('type', ''))
            if message_type == 'lifespan.startup.complete':
                self._lifespan_supported = True
                self._lifespan_receive_queue = receive_queue
                self._lifespan_response_queue = response_queue
                self._lifespan_task = task
                return

            await _cancel_task(task)
            if message_type == 'lifespan.startup.failed':
                msg = (
                    f'HTTP application {self.requirement_name!r} failed '
                    'during ASGI lifespan startup'
                )
                raise ResourceError(
                    self.requirement_name,
                    msg,
                    code='http_application_startup_failed',
                )

            self._lifespan_supported = False

    async def shutdown(self) -> None:
        if self.backend != 'asgi':
            return

        async with self._lifespan_lock:
            if self._lifespan_task is None:
                return
            receive_queue = self._lifespan_receive_queue
            response_queue = self._lifespan_response_queue
            if receive_queue is None or response_queue is None:
                self._reset_lifespan_state()
                return

            await receive_queue.put(
                {'type': 'lifespan.shutdown'},
            )
            try:
                message = await asyncio.wait_for(
                    response_queue.get(),
                    1.0,
                )
            except TimeoutError:
                await _cancel_task(self._lifespan_task)
                self._reset_lifespan_state()
                return

            message_type = str(message.get('type', ''))
            if message_type != 'lifespan.shutdown.complete':
                await _cancel_task(self._lifespan_task)
                self._reset_lifespan_state()
                return

            await _await_task(self._lifespan_task)
            self._reset_lifespan_state()

    def _reset_lifespan_state(self) -> None:
        self._lifespan_receive_queue = None
        self._lifespan_response_queue = None
        self._lifespan_task = None


@dataclass(slots=True, frozen=True)
class HttpTransportConfig:
    backend: HttpTransportBackendKind
    application_resource: str | None = None
    ssl_resource: str | None = None
    base_url: str | None = None
    host: str = '127.0.0.1'
    port: int | None = None
    standalone_server: HttpStandaloneServerKind = 'auto'
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_ca_certs: str | None = None
    startup_timeout_seconds: float = 10.0
    cleanup_policy: HttpCleanupPolicy = 'auto'

    @classmethod
    def from_requirement(cls, requirement) -> HttpTransportConfig:
        config = requirement.config
        return cls(
            backend=_normalize_transport_backend(
                _read_config_value(
                    config,
                    'backend',
                    env_names=('COSECHA_HTTP_BACKEND',),
                ),
            ),
            application_resource=_read_optional_str(
                _read_config_value(
                    config,
                    'application_resource',
                ),
            ),
            ssl_resource=_read_optional_str(
                _read_config_value(
                    config,
                    'ssl_resource',
                ),
            ),
            base_url=_read_optional_str(
                _read_config_value(
                    config,
                    'base_url',
                    env_names=('COSECHA_HTTP_BASE_URL',),
                ),
            ),
            host=_read_non_empty_str(
                _read_config_value(
                    config,
                    'host',
                    env_names=('COSECHA_HTTP_HOST',),
                    default='127.0.0.1',
                ),
                field_name='host',
            ),
            port=_read_optional_port(
                _read_config_value(
                    config,
                    'port',
                    env_names=('COSECHA_HTTP_PORT',),
                ),
                field_name='port',
            ),
            standalone_server=_normalize_standalone_server(
                _read_config_value(
                    config,
                    'standalone_server',
                    env_names=('COSECHA_HTTP_STANDALONE_SERVER',),
                    default='auto',
                ),
            ),
            ssl_certfile=_read_optional_str(
                _read_config_value(
                    config,
                    'ssl_certfile',
                    env_names=('COSECHA_HTTP_SSL_CERTFILE',),
                ),
            ),
            ssl_keyfile=_read_optional_str(
                _read_config_value(
                    config,
                    'ssl_keyfile',
                    env_names=('COSECHA_HTTP_SSL_KEYFILE',),
                ),
            ),
            ssl_ca_certs=_read_optional_str(
                _read_config_value(
                    config,
                    'ssl_ca_certs',
                    env_names=('COSECHA_HTTP_SSL_CA_CERTS',),
                ),
            ),
            startup_timeout_seconds=_read_non_negative_float(
                _read_config_value(
                    config,
                    'startup_timeout_seconds',
                    env_names=('COSECHA_HTTP_STARTUP_TIMEOUT_SECONDS',),
                    default=10.0,
                ),
                field_name='startup_timeout_seconds',
            ),
            cleanup_policy=_normalize_cleanup_policy(
                _read_config_value(
                    config,
                    'cleanup_policy',
                    env_names=('COSECHA_HTTP_CLEANUP_POLICY',),
                    default='auto',
                ),
            ),
        )

    def validate_standalone_tls(self, *, requirement_name: str) -> None:
        if self.ssl_certfile is None and self.ssl_keyfile is None:
            return
        if self.ssl_certfile is None or self.ssl_keyfile is None:
            msg = (
                f'HTTP transport resource {requirement_name!r} must provide '
                'both `ssl_certfile` and `ssl_keyfile`'
            )
            raise ResourceError(
                requirement_name,
                msg,
                code='http_transport_ssl_material_incomplete',
                unhealthy=False,
            )

    def uses_tls(self) -> bool:
        return self.ssl_certfile is not None and self.ssl_keyfile is not None

    def resolve_application_requirement_name(self, requirement) -> str:
        if self.application_resource is not None:
            return self.application_resource

        dependency_names = tuple(
            dependency_name
            for dependency_name in requirement.depends_on
            if dependency_name != self.ssl_resource
        )
        if len(requirement.depends_on) != 1:
            if len(dependency_names) == 1 and self.ssl_resource is not None:
                return dependency_names[0]
            msg = (
                f'HTTP transport resource {requirement.name!r} requires '
                'exactly one application dependency'
            )
            raise ResourceError(
                requirement.name,
                msg,
                code='http_transport_application_dependency_missing',
                unhealthy=False,
            )

        return requirement.depends_on[0]


@dataclass(slots=True)
class HttpTransportHandle:
    requirement_name: str
    backend: HttpTransportBackendKind
    base_url: str
    cleanup_policy: HttpCleanupPolicy
    application_requirement_name: str | None
    application_backend: HttpApplicationBackendKind | None
    host: str | None = None
    port: int | None = None
    standalone_server: HttpStandaloneServerKind | None = None
    ssl_ca_certs: str | None = None
    process: subprocess.Popen[str] | None = None
    application_handle: HttpApplicationHandle | None = None

    def build_external_handle(self) -> str:
        return json.dumps(
            {
                'application_backend': self.application_backend,
                'application_requirement_name': (
                    self.application_requirement_name
                ),
                'backend': self.backend,
                'base_url': self.base_url,
                'cleanup_policy': self.cleanup_policy,
                'host': self.host,
                'pid': self.process.pid if self.process is not None else None,
                'port': self.port,
                'requirement_name': self.requirement_name,
                'ssl_ca_certs': self.ssl_ca_certs,
                'standalone_server': self.standalone_server,
            },
            sort_keys=True,
        )

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        query: Mapping[str, object] | None = None,
        body: object | None = None,
    ) -> dict[str, object]:
        if self.backend == 'inprocess' or (
            self.backend == 'standalone'
            and self.application_backend == 'wsgi'
            and self.process is None
        ):
            application_handle = self.application_handle
            if application_handle is None:
                application_handle = _resolve_registered_application_handle(
                    self.application_requirement_name,
                    requirement_name=self.requirement_name,
                )
            if application_handle.backend == 'asgi':
                return await _request_inprocess_asgi(
                    application_handle,
                    method,
                    path,
                    headers=headers,
                    query=query,
                    body=body,
                )
            return await _request_inprocess_wsgi(
                application_handle,
                method,
                path,
                headers=headers,
                query=query,
                body=body,
            )

        return await _request_over_http(
            self.base_url,
            method,
            path,
            headers=headers,
            query=query,
            body=body,
            ssl_ca_certs=self.ssl_ca_certs,
        )


class HttpApplicationProvider:
    def supports_mode(self, mode: str) -> bool:
        return mode in {'live', 'ephemeral'}

    def acquire(self, requirement, *, mode: str) -> HttpApplicationHandle:
        del mode
        config = HttpApplicationConfig.from_requirement(requirement)
        resource_name, generated_resource_name = config.resolve_resource_name(
            requirement_name=requirement.name,
        )
        app, is_factory = load_application_from_source_ref(
            config.app,
            backend=config.backend,
            root_path=Path.cwd(),
        )
        handle = HttpApplicationHandle(
            requirement_name=requirement.name,
            resource_name=resource_name,
            backend=config.backend,
            app=app,
            source_ref=config.app,
            root_path=str(Path.cwd()),
            is_factory=is_factory,
            cleanup_policy=config.cleanup_policy,
            generated_resource_name=generated_resource_name,
        )
        _APPLICATION_REGISTRY[requirement.name] = handle
        return handle

    def release(self, resource, requirement, *, mode: str) -> None:
        del mode
        if not isinstance(resource, HttpApplicationHandle):
            return
        current = _APPLICATION_REGISTRY.get(requirement.name)
        if current is resource:
            _APPLICATION_REGISTRY.pop(requirement.name, None)
        _run_coroutine_now_or_background(resource.shutdown())

    def health_check(self, resource, requirement, *, mode: str) -> bool:
        del requirement, mode
        return isinstance(resource, HttpApplicationHandle)

    def verify_integrity(self, resource, requirement, *, mode: str) -> bool:
        del mode
        return (
            isinstance(resource, HttpApplicationHandle)
            and resource.requirement_name == requirement.name
        )

    def describe_external_handle(
        self,
        resource,
        requirement,
        *,
        mode: str,
    ) -> str | None:
        del requirement, mode
        if not isinstance(resource, HttpApplicationHandle):
            return None
        return resource.build_external_handle()

    def describe_capabilities(
        self,
        resource,
        requirement,
        *,
        mode: str,
    ) -> dict[str, object]:
        del requirement, mode
        if not isinstance(resource, HttpApplicationHandle):
            return {}
        return {
            'http.application_backend': resource.backend,
        }

    def reap_orphan(
        self,
        external_handle: str,
        requirement,
        *,
        mode: str,
    ) -> None:
        del external_handle, requirement, mode

    def revoke_orphan_access(
        self,
        external_handle: str,
        requirement,
        *,
        mode: str,
    ) -> None:
        del external_handle, requirement, mode


class HttpTransportProvider:
    def supports_mode(self, mode: str) -> bool:
        return mode in {'live', 'ephemeral'}

    def resolve_dependency_names(
        self,
        requirement,
        *,
        mode: str,
    ) -> tuple[str, ...]:
        del mode
        config = HttpTransportConfig.from_requirement(requirement)
        if config.backend == 'live':
            return ()
        dependency_names: list[str] = []
        if (
            config.application_resource is not None
            and config.application_resource not in requirement.depends_on
        ):
            dependency_names.append(config.application_resource)
        if (
            config.backend == 'standalone'
            and config.ssl_resource is not None
            and config.ssl_resource not in requirement.depends_on
        ):
            dependency_names.append(config.ssl_resource)
        return tuple(dependency_names)

    def validate_dependency_capabilities(
        self,
        requirement,
        *,
        mode: str,
        dependency_context: ResourceDependencyContext | None = None,
    ) -> None:
        del mode
        if dependency_context is None:
            return

        config = HttpTransportConfig.from_requirement(requirement)
        if config.backend != 'standalone':
            return

        tls_enabled = config.uses_tls() or config.ssl_resource is not None
        if not tls_enabled:
            return

        application_requirement_name = (
            config.resolve_application_requirement_name(requirement)
        )
        application_handle = _resolve_application_dependency_handle(
            application_requirement_name,
            dependency_context=dependency_context,
            requirement_name=requirement.name,
        )
        if application_handle.backend != 'asgi':
            msg = (
                f'HTTP transport resource {requirement.name!r} requires an '
                'ASGI application when TLS is enabled'
            )
            raise ResourceError(
                requirement.name,
                msg,
                code='http_transport_tls_requires_asgi',
                unhealthy=False,
            )

        if config.ssl_resource is None:
            return

        ssl_capabilities = dependency_context.get_capabilities(
            config.ssl_resource,
        )
        if ssl_capabilities.get('tls.materials') is True:
            return

        msg = (
            f'HTTP transport resource {requirement.name!r} requires SSL '
            f'dependency {config.ssl_resource!r} to expose TLS materials'
        )
        raise ResourceError(
            requirement.name,
            msg,
            code='http_transport_ssl_dependency_invalid',
            unhealthy=False,
        )

    def acquire(
        self,
        requirement,
        *,
        mode: str,
        dependency_context: ResourceDependencyContext | None = None,
    ) -> HttpTransportHandle:
        del mode
        config = HttpTransportConfig.from_requirement(requirement)
        if config.backend == 'live':
            if config.base_url is None:
                msg = (
                    f'HTTP transport resource {requirement.name!r} requires '
                    '`base_url` when backend is "live"'
                )
                raise ResourceError(
                    requirement.name,
                    msg,
                    code='http_transport_base_url_missing',
                    unhealthy=False,
                )
            return HttpTransportHandle(
                requirement_name=requirement.name,
                backend='live',
                base_url=config.base_url,
                cleanup_policy=config.cleanup_policy,
                application_requirement_name=None,
                application_backend=None,
            )

        application_requirement_name = (
            config.resolve_application_requirement_name(requirement)
        )
        application_handle = _resolve_application_dependency_handle(
            application_requirement_name,
            dependency_context=dependency_context,
            requirement_name=requirement.name,
        )

        if config.backend == 'inprocess':
            return HttpTransportHandle(
                requirement_name=requirement.name,
                backend='inprocess',
                base_url='http://inprocess.local',
                cleanup_policy=config.cleanup_policy,
                application_requirement_name=application_requirement_name,
                application_backend=application_handle.backend,
                application_handle=application_handle,
            )

        ssl_certfile, ssl_keyfile, ssl_ca_certs = (
            _resolve_transport_ssl_materials(
                config,
                dependency_context=dependency_context,
                requirement_name=requirement.name,
            )
        )

        host = config.host
        port = config.port or _find_free_tcp_port(host)
        _validate_transport_tls(
            requirement_name=requirement.name,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
        )
        if application_handle.backend == 'wsgi':
            if ssl_certfile is not None and ssl_keyfile is not None:
                msg = (
                    'HTTP WSGI standalone does not support TLS in the current '
                    'provider implementation'
                )
                raise ResourceError(
                    requirement.name,
                    msg,
                    code='http_transport_wsgi_tls_unsupported',
                    unhealthy=False,
                )
            return HttpTransportHandle(
                requirement_name=requirement.name,
                backend='standalone',
                base_url=f'http://{host}:{port}',
                cleanup_policy=config.cleanup_policy,
                application_requirement_name=application_requirement_name,
                application_backend=application_handle.backend,
                host=host,
                port=port,
                application_handle=application_handle,
            )
        standalone_server = _resolve_standalone_server(
            config.standalone_server,
            requirement_name=requirement.name,
        )
        process = _spawn_standalone_transport_process(
            application_handle,
            standalone_server=standalone_server,
            host=host,
            port=port,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_ca_certs=ssl_ca_certs,
        )
        scheme = (
            'https'
            if ssl_certfile is not None and ssl_keyfile is not None
            else 'http'
        )
        base_url = f'{scheme}://{host}:{port}'
        try:
            _wait_for_http_server(
                base_url,
                timeout_seconds=config.startup_timeout_seconds,
                process=process,
                requirement_name=requirement.name,
            )
        except Exception:
            _terminate_process(process)
            raise

        return HttpTransportHandle(
            requirement_name=requirement.name,
            backend='standalone',
            base_url=base_url,
            cleanup_policy=config.cleanup_policy,
            application_requirement_name=application_requirement_name,
            application_backend=application_handle.backend,
            host=host,
            port=port,
            standalone_server=standalone_server,
            ssl_ca_certs=ssl_ca_certs,
            process=process,
            application_handle=application_handle,
        )

    def release(self, resource, requirement, *, mode: str) -> None:
        del requirement, mode
        if not isinstance(resource, HttpTransportHandle):
            return
        if resource.process is None:
            return
        if resource.cleanup_policy == 'preserve':
            return
        _terminate_process(resource.process)

    def health_check(self, resource, requirement, *, mode: str) -> bool:
        del requirement, mode
        if not isinstance(resource, HttpTransportHandle):
            return False
        if resource.backend == 'inprocess':
            return (
                resource.application_handle is not None
                and resource.application_handle.backend
                == resource.application_backend
            )
        if (
            resource.process is None
            and resource.application_handle is not None
        ):
            return (
                resource.application_handle.backend
                == resource.application_backend
            )
        return _probe_http_server(resource.base_url)

    def verify_integrity(self, resource, requirement, *, mode: str) -> bool:
        del mode
        if not isinstance(resource, HttpTransportHandle):
            return False
        if resource.requirement_name != requirement.name:
            return False
        if resource.backend == 'inprocess':
            return (
                resource.application_handle is not None
                and resource.application_handle.backend
                == resource.application_backend
            )
        return bool(resource.base_url)

    def describe_external_handle(
        self,
        resource,
        requirement,
        *,
        mode: str,
    ) -> str | None:
        del requirement, mode
        if not isinstance(resource, HttpTransportHandle):
            return None
        return resource.build_external_handle()

    def reap_orphan(
        self,
        external_handle: str,
        requirement,
        *,
        mode: str,
    ) -> None:
        del requirement, mode
        payload = _decode_external_handle(external_handle)
        if payload.get('cleanup_policy') == 'preserve':
            return
        pid = _read_optional_int(payload.get('pid'), field_name='pid')
        if pid is not None:
            _terminate_pid(pid)

    def revoke_orphan_access(
        self,
        external_handle: str,
        requirement,
        *,
        mode: str,
    ) -> None:
        del external_handle, requirement, mode


def load_application_from_source_ref(
    source_ref: str,
    *,
    backend: HttpApplicationBackendKind,
    root_path: Path,
) -> tuple[object, bool]:
    try:
        symbol = SymbolRef.parse(source_ref).resolve(root_path=root_path)
    except (ImportError, AttributeError, ManifestValidationError) as error:
        msg = (
            f'HTTP source {source_ref!r} could not be resolved as an '
            'application symbol'
        )
        resource_name = APPLICATION_INTERFACE_NAME
        raise ResourceError(
            resource_name,
            msg,
            code='http_application_source_unresolvable',
            unhealthy=False,
        ) from error

    if _matches_application_contract(symbol, backend):
        return (symbol, False)

    if callable(symbol) and _callable_supports_arity(symbol, 0):
        resolved = symbol()
        if _matches_application_contract(resolved, backend):
            return (resolved, True)

    msg = (
        f'HTTP source {source_ref!r} does not resolve to a valid '
        f'{backend.upper()} application or zero-argument factory'
    )
    resource_name = APPLICATION_INTERFACE_NAME
    raise ResourceError(
        resource_name,
        msg,
        code='http_application_invalid_source',
        unhealthy=False,
    )


def _matches_application_contract(
    candidate: object,
    backend: HttpApplicationBackendKind,
) -> bool:
    if not callable(candidate):
        return False
    return _callable_supports_arity(
        candidate,
        3 if backend == 'asgi' else 2,
    )


def _callable_supports_arity(candidate: object, arity: int) -> bool:
    if not callable(candidate):
        return False

    try:
        signature = inspect.signature(candidate)
    except (TypeError, ValueError):
        return False

    minimum = 0
    maximum = 0
    has_varargs = False
    for parameter in signature.parameters.values():
        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            has_varargs = True
            continue
        if parameter.kind not in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }:
            continue
        maximum += 1
        if parameter.default is inspect.Signature.empty:
            minimum += 1

    if has_varargs:
        return minimum <= arity
    return minimum <= arity <= maximum


async def _request_inprocess_wsgi(  # noqa: PLR0913
    application_handle: HttpApplicationHandle,
    method: str,
    path: str,
    *,
    headers: Mapping[str, str] | None,
    query: Mapping[str, object] | None,
    body: object | None,
) -> dict[str, object]:
    serialized_body, request_headers = _prepare_request_body(headers, body)

    def _run_request() -> dict[str, object]:
        request_path = _build_request_path(path, query)
        parsed = urlsplit(request_path)
        environ = {
            'CONTENT_LENGTH': str(len(serialized_body)),
            'PATH_INFO': parsed.path or '/',
            'QUERY_STRING': parsed.query,
            'REQUEST_METHOD': method.upper(),
            'SERVER_NAME': 'inprocess.local',
            'SERVER_PORT': '80',
            'SERVER_PROTOCOL': 'HTTP/1.1',
            'wsgi.errors': sys.stderr,
            'wsgi.input': BytesIO(serialized_body),
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        content_type = _lookup_header(request_headers, 'content-type')
        if content_type:
            environ['CONTENT_TYPE'] = content_type
        for header_name, header_value in request_headers.items():
            lowered = header_name.lower()
            if lowered in {'content-length', 'content-type'}:
                continue
            environ['HTTP_' + header_name.upper().replace('-', '_')] = (
                header_value
            )

        captured_status: list[str] = []
        captured_headers: list[tuple[str, str]] = []

        def start_response(
            status: str,
            response_headers: list[tuple[str, str]],
            exc_info: tuple[
                type[BaseException],
                BaseException,
                TracebackType | None,
            ]
            | None = None,
        ) -> None:
            del exc_info
            captured_status[:] = [status]
            captured_headers[:] = response_headers

        try:
            response_iterable = application_handle.app(environ, start_response)
            try:
                chunks = [
                    chunk.encode('utf-8') if isinstance(chunk, str) else chunk
                    for chunk in response_iterable
                ]
            finally:
                close = getattr(response_iterable, 'close', None)
                if callable(close):
                    close()
        except Exception:
            return _server_error_response()

        response_headers = dict(captured_headers)
        status_code = (
            int(captured_status[0].split()[0]) if captured_status else 200
        )
        body_bytes = b''.join(chunks)
        return {
            'status_code': status_code,
            'headers': response_headers,
            'body': _deserialize_response_body(body_bytes, response_headers),
        }

    return await asyncio.to_thread(_run_request)


async def _request_inprocess_asgi(  # noqa: PLR0913
    application_handle: HttpApplicationHandle,
    method: str,
    path: str,
    *,
    headers: Mapping[str, str] | None,
    query: Mapping[str, object] | None,
    body: object | None,
) -> dict[str, object]:
    await application_handle.ensure_started()
    serialized_body, request_headers = _prepare_request_body(headers, body)
    sent_messages: list[dict[str, object]] = []
    body_sent = False
    request_path = _build_request_path(path, query)
    parsed = urlsplit(request_path)

    scope = {
        'type': 'http',
        'asgi': {'version': '3.0', 'spec_version': '2.3'},
        'http_version': '1.1',
        'method': method.upper(),
        'scheme': 'http',
        'path': parsed.path or '/',
        'raw_path': (parsed.path or '/').encode('utf-8'),
        'query_string': parsed.query.encode('utf-8'),
        'headers': [
            (
                header_name.lower().encode('latin1'),
                header_value.encode('latin1'),
            )
            for header_name, header_value in request_headers.items()
        ],
        'client': ('127.0.0.1', 0),
        'server': ('inprocess.local', 80),
        'root_path': '',
        'state': {},
        'extensions': {},
    }

    async def receive() -> dict[str, object]:
        nonlocal body_sent
        if body_sent:
            return {'type': 'http.disconnect'}
        body_sent = True
        return {
            'type': 'http.request',
            'body': serialized_body,
            'more_body': False,
        }

    async def send(message: dict[str, object]) -> None:
        sent_messages.append(message)

    try:
        await application_handle.app(scope, receive, send)
    except Exception:
        return _server_error_response()

    status_code = 200
    response_headers: dict[str, str] = {}
    response_body = b''
    for message in sent_messages:
        message_type = str(message.get('type', ''))
        if message_type == 'http.response.start':
            status_code = int(message.get('status', 200))
            raw_headers = message.get('headers', ())
            if isinstance(raw_headers, Iterable):
                response_headers = {
                    key.decode('latin1'): value.decode('latin1')
                    for key, value in raw_headers
                }
        elif message_type == 'http.response.body':
            response_body += bytes(message.get('body', b''))

    return {
        'status_code': status_code,
        'headers': response_headers,
        'body': _deserialize_response_body(response_body, response_headers),
    }


async def _request_over_http(  # noqa: PLR0913
    base_url: str,
    method: str,
    path: str,
    *,
    headers: Mapping[str, str] | None,
    query: Mapping[str, object] | None,
    body: object | None,
    ssl_ca_certs: str | None = None,
) -> dict[str, object]:
    serialized_body, request_headers = _prepare_request_body(headers, body)
    request_url = urljoin(
        base_url.rstrip('/') + '/',
        _build_request_path(path, query),
    )
    return await asyncio.to_thread(
        _perform_http_request,
        request_url,
        method.upper(),
        request_headers,
        serialized_body,
        ssl_ca_certs,
    )


def _perform_http_request(
    request_url: str,
    method: str,
    headers: Mapping[str, str],
    body: bytes,
    ssl_ca_certs: str | None,
) -> dict[str, object]:
    request = Request(  # noqa: S310
        request_url,
        method=method,
        headers=dict(headers),
        data=body or None,
    )
    ssl_context = None
    if request_url.startswith('https://'):
        ssl_context = ssl.create_default_context(cafile=ssl_ca_certs)
    try:
        with urlopen(  # noqa: S310
            request,
            timeout=DEFAULT_HTTP_TIMEOUT_SECONDS,
            context=ssl_context,
        ) as response:
            response_body = response.read()
            response_headers = dict(response.headers.items())
            return {
                'status_code': response.status,
                'headers': response_headers,
                'body': _deserialize_response_body(
                    response_body,
                    response_headers,
                ),
            }
    except HTTPError as error:
        response_body = error.read()
        response_headers = dict(error.headers.items())
        return {
            'status_code': error.code,
            'headers': response_headers,
            'body': _deserialize_response_body(
                response_body,
                response_headers,
            ),
        }


def _probe_http_server(base_url: str) -> bool:
    try:
        parsed = urlsplit(base_url)
        host = parsed.hostname
        port = parsed.port
        if host is None or port is None:
            return False
        with socket.create_connection(
            (host, port),
            timeout=DEFAULT_HTTP_TIMEOUT_SECONDS,
        ):
            return True
    except (OSError, URLError, ConnectionError, TimeoutError, ValueError):
        return False


def _resolve_standalone_server(
    preference: HttpStandaloneServerKind,
    *,
    requirement_name: str,
) -> Literal['uvicorn', 'hypercorn']:
    if preference == 'uvicorn':
        if _is_optional_server_available('uvicorn'):
            return 'uvicorn'
        msg = (
            f'HTTP transport resource {requirement_name!r} requires '
            '`uvicorn`, but it is not installed'
        )
        raise ResourceError(
            requirement_name,
            msg,
            code='http_transport_server_unavailable',
            unhealthy=False,
        )

    if preference == 'hypercorn':
        if _is_optional_server_available('hypercorn'):
            return 'hypercorn'
        msg = (
            f'HTTP transport resource {requirement_name!r} requires '
            '`hypercorn`, but it is not installed'
        )
        raise ResourceError(
            requirement_name,
            msg,
            code='http_transport_server_unavailable',
            unhealthy=False,
        )

    if _is_optional_server_available('uvicorn'):
        return 'uvicorn'
    if _is_optional_server_available('hypercorn'):
        return 'hypercorn'
    msg = (
        f'HTTP transport resource {requirement_name!r} cannot run '
        '`standalone` because neither `uvicorn` nor `hypercorn` is installed'
    )
    raise ResourceError(
        requirement_name,
        msg,
        code='http_transport_server_unavailable',
        unhealthy=False,
    )


def _is_optional_server_available(
    server_name: Literal['uvicorn', 'hypercorn'],
) -> bool:
    return importlib.util.find_spec(server_name) is not None


def _prepare_request_body(
    headers: Mapping[str, str] | None,
    body: object | None,
) -> tuple[bytes, dict[str, str]]:
    request_headers = dict(headers or {})
    if not _has_header(request_headers, 'connection'):
        request_headers['Connection'] = 'close'
    if body is None:
        return (b'', request_headers)
    if isinstance(body, bytes):
        return (body, request_headers)
    if isinstance(body, str):
        return (body.encode('utf-8'), request_headers)
    if not _has_header(request_headers, 'content-type'):
        request_headers['Content-Type'] = 'application/json'
    return (json.dumps(body).encode('utf-8'), request_headers)


def _deserialize_response_body(
    body: bytes,
    headers: Mapping[str, str],
) -> object:
    if not body:
        return ''
    content_type = _lookup_header(headers, 'content-type').lower()
    if content_type.startswith('application/json'):
        try:
            return json.loads(body.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return body.decode('utf-8', errors='replace')
    return body.decode('utf-8', errors='replace')


def _build_request_path(
    path: str,
    query: Mapping[str, object] | None,
) -> str:
    normalized_path = path if path.startswith('/') else f'/{path}'
    if not query:
        return normalized_path
    return f'{normalized_path}?{urlencode(query, doseq=True)}'


def _spawn_standalone_transport_process(  # noqa: PLR0913
    application_handle: HttpApplicationHandle,
    *,
    standalone_server: HttpStandaloneServerKind,
    host: str,
    port: int,
    ssl_certfile: str | None,
    ssl_keyfile: str | None,
    ssl_ca_certs: str | None,
) -> subprocess.Popen[str]:
    runner_module = (
        'cosecha.provider.http.asgi_runner'
        if application_handle.backend == 'asgi'
        else 'cosecha.provider.http.wsgi_runner'
    )
    command = [
        sys.executable,
        '-m',
        runner_module,
        '--app',
        application_handle.source_ref,
        '--host',
        host,
        '--port',
        str(port),
        '--root-path',
        application_handle.root_path,
    ]
    if application_handle.backend == 'asgi':
        command.extend(['--server', standalone_server])
        if ssl_certfile is not None:
            command.extend(['--ssl-certfile', ssl_certfile])
        if ssl_keyfile is not None:
            command.extend(['--ssl-keyfile', ssl_keyfile])
        if ssl_ca_certs is not None:
            command.extend(['--ssl-ca-certs', ssl_ca_certs])
    return subprocess.Popen(  # noqa: S603
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def _wait_for_http_server(
    base_url: str,
    *,
    timeout_seconds: float,
    process: subprocess.Popen[str],
    requirement_name: str,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            msg = 'HTTP standalone process exited during startup'
            raise ResourceError(
                requirement_name,
                msg,
                code='http_transport_standalone_exited',
                unhealthy=False,
            )
        if _probe_http_server(base_url):
            return
        time.sleep(0.1)

    msg = f'HTTP standalone server did not become ready at {base_url!r}'
    raise ResourceError(
        requirement_name,
        msg,
        code='http_transport_standalone_timeout',
        unhealthy=False,
    )


def _resolve_application_dependency_handle(
    application_requirement_name: str | None,
    *,
    dependency_context: ResourceDependencyContext | None,
    requirement_name: str,
) -> HttpApplicationHandle:
    if (
        dependency_context is not None
        and application_requirement_name is not None
    ):
        dependency = dependency_context.get_dependency(
            application_requirement_name,
        )
        if dependency is not None:
            resource = dependency.resource
            if isinstance(resource, HttpApplicationHandle):
                return resource
            msg = (
                f'HTTP transport resource {requirement_name!r} resolved '
                f'invalid application dependency '
                f'{application_requirement_name!r}'
            )
            raise ResourceError(
                requirement_name,
                msg,
                code='http_transport_application_dependency_invalid',
                unhealthy=False,
            )

    return _resolve_registered_application_handle(
        application_requirement_name,
        requirement_name=requirement_name,
    )


def _resolve_transport_ssl_materials(
    config: HttpTransportConfig,
    *,
    dependency_context: ResourceDependencyContext | None,
    requirement_name: str,
) -> tuple[str | None, str | None, str | None]:
    ssl_certfile = config.ssl_certfile
    ssl_keyfile = config.ssl_keyfile
    ssl_ca_certs = config.ssl_ca_certs

    if config.ssl_resource is None:
        return (ssl_certfile, ssl_keyfile, ssl_ca_certs)

    if dependency_context is None:
        msg = (
            f'HTTP transport resource {requirement_name!r} requires SSL '
            'dependency context to resolve TLS materials'
        )
        raise ResourceError(
            requirement_name,
            msg,
            code='http_transport_ssl_dependency_unavailable',
            unhealthy=False,
        )

    dependency = dependency_context.get_dependency(config.ssl_resource)
    if dependency is None:
        msg = (
            f'HTTP transport resource {requirement_name!r} could not resolve '
            f'its SSL dependency {config.ssl_resource!r}'
        )
        raise ResourceError(
            requirement_name,
            msg,
            code='http_transport_ssl_dependency_unavailable',
            unhealthy=False,
        )

    resource = dependency.resource
    ssl_certfile = ssl_certfile or _read_optional_dependency_path(
        resource,
        'cert_path',
    )
    ssl_keyfile = ssl_keyfile or _read_optional_dependency_path(
        resource,
        'key_path',
    )
    ssl_ca_certs = ssl_ca_certs or _read_optional_dependency_path(
        resource,
        'ca_cert_path',
    )
    if ssl_certfile is None or ssl_keyfile is None:
        msg = (
            f'HTTP transport resource {requirement_name!r} requires SSL '
            f'dependency {config.ssl_resource!r} to expose `cert_path` and '
            '`key_path`'
        )
        raise ResourceError(
            requirement_name,
            msg,
            code='http_transport_ssl_dependency_invalid',
            unhealthy=False,
        )

    return (ssl_certfile, ssl_keyfile, ssl_ca_certs)


def _read_optional_dependency_path(
    resource: object,
    attribute_name: str,
) -> str | None:
    value = getattr(resource, attribute_name, None)
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value
    return None


def _validate_transport_tls(
    *,
    requirement_name: str,
    ssl_certfile: str | None,
    ssl_keyfile: str | None,
) -> None:
    if ssl_certfile is None and ssl_keyfile is None:
        return
    if ssl_certfile is None or ssl_keyfile is None:
        msg = (
            f'HTTP transport resource {requirement_name!r} must provide '
            'both `ssl_certfile` and `ssl_keyfile`'
        )
        raise ResourceError(
            requirement_name,
            msg,
            code='http_transport_ssl_material_incomplete',
            unhealthy=False,
        )


def _resolve_registered_application_handle(
    application_requirement_name: str | None,
    *,
    requirement_name: str,
) -> HttpApplicationHandle:
    if application_requirement_name is None:
        msg = (
            f'HTTP transport resource {requirement_name!r} is missing an '
            'application resource dependency'
        )
        raise ResourceError(
            requirement_name,
            msg,
            code='http_transport_application_dependency_missing',
            unhealthy=False,
        )
    handle = _APPLICATION_REGISTRY.get(application_requirement_name)
    if handle is None:
        msg = (
            f'HTTP transport resource {requirement_name!r} could not resolve '
            f'its application dependency {application_requirement_name!r}'
        )
        raise ResourceError(
            requirement_name,
            msg,
            code='http_transport_application_dependency_unavailable',
            unhealthy=False,
        )
    return handle


def _normalize_application_backend(
    value: object,
) -> HttpApplicationBackendKind:
    if value in {'asgi', 'wsgi'}:
        return value
    msg = "HTTP application backend must be one of 'asgi' or 'wsgi'"
    raise ValueError(msg)


def _normalize_transport_backend(
    value: object,
) -> HttpTransportBackendKind:
    if value in {'inprocess', 'standalone', 'live'}:
        return value
    msg = (
        'HTTP transport backend must be one of '
        "'inprocess', 'standalone' or 'live'"
    )
    raise ValueError(msg)


def _normalize_cleanup_policy(value: object) -> HttpCleanupPolicy:
    if value in {'auto', 'preserve'}:
        return value
    msg = "HTTP cleanup_policy must be one of 'auto' or 'preserve'"
    raise ValueError(msg)


def _normalize_standalone_server(value: object) -> HttpStandaloneServerKind:
    if value in {'auto', 'uvicorn', 'hypercorn'}:
        return value
    msg = (
        'HTTP standalone_server must be one of '
        "'auto', 'uvicorn' or 'hypercorn'"
    )
    raise ValueError(msg)


def _read_config_value(
    config: Mapping[str, object],
    key: str,
    *,
    aliases: tuple[str, ...] = (),
    env_names: tuple[str, ...] = (),
    default: object | None = None,
) -> object | None:
    for env_name in env_names:
        if env_name in os.environ:
            return os.environ[env_name]
    if key in config:
        return config[key]
    for alias in aliases:
        if alias in config:
            return config[alias]
    return default


def _read_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value
    msg = 'Expected optional non-empty string'
    raise ValueError(msg)


def _read_non_empty_str(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and value:
        return value
    msg = f'HTTP config field {field_name!r} must be a non-empty string'
    raise ValueError(msg)


def _read_optional_int(value: object, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value:
        return int(value)
    msg = f'HTTP config field {field_name!r} must be an integer'
    raise ValueError(msg)


def _read_optional_port(value: object, *, field_name: str) -> int | None:
    port = _read_optional_int(value, field_name=field_name)
    if port is None:
        return None
    if 1 <= port <= MAX_TCP_PORT:
        return port
    msg = f'HTTP config field {field_name!r} must be a valid TCP port'
    raise ValueError(msg)


def _read_non_negative_float(value: object, *, field_name: str) -> float:
    if isinstance(value, int | float) and not isinstance(value, bool):
        if value < 0:
            msg = f'HTTP config field {field_name!r} must be non-negative'
            raise ValueError(msg)
        return float(value)
    if isinstance(value, str) and value:
        return _read_non_negative_float(float(value), field_name=field_name)
    msg = f'HTTP config field {field_name!r} must be numeric'
    raise ValueError(msg)


def _find_free_tcp_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _terminate_pid(pid: int) -> None:
    with suppress(OSError):
        os.kill(pid, signal.SIGTERM)


def _decode_external_handle(external_handle: str) -> dict[str, object]:
    try:
        decoded = json.loads(external_handle)
    except json.JSONDecodeError as error:
        msg = 'Invalid HTTP external handle'
        raise ValueError(msg) from error
    if isinstance(decoded, dict):
        return {str(key): value for key, value in decoded.items()}
    msg = 'Invalid HTTP external handle payload'
    raise ValueError(msg)


def _lookup_header(headers: Mapping[str, str], key: str) -> str:
    lowered_key = key.lower()
    for header_name, header_value in headers.items():
        if header_name.lower() == lowered_key:
            return header_value
    return ''


def _has_header(headers: Mapping[str, str], key: str) -> bool:
    return bool(_lookup_header(headers, key))


def _server_error_response() -> dict[str, object]:
    return {
        'status_code': HTTP_INTERNAL_SERVER_ERROR,
        'headers': {'Content-Type': 'text/plain; charset=utf-8'},
        'body': 'Internal Server Error',
    }


async def _cancel_task(task: asyncio.Task[None]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def _await_task(task: asyncio.Task[None]) -> None:
    with suppress(asyncio.CancelledError):
        await task


def _run_coroutine_now_or_background(coroutine: Any) -> None:
    if not hasattr(coroutine, '__await__'):
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coroutine)
        return
    task = loop.create_task(coroutine)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
