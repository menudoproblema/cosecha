from __future__ import annotations

import argparse

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from urllib.parse import urlsplit

from cosecha.provider.http.provider import load_application_from_source_ref


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--app', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--root-path', required=True)
    return parser


def _build_wsgi_handler(app):
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_GET(self) -> None:
            self._handle_wsgi_request()

        def do_POST(self) -> None:
            self._handle_wsgi_request()

        def do_PUT(self) -> None:
            self._handle_wsgi_request()

        def do_PATCH(self) -> None:
            self._handle_wsgi_request()

        def do_DELETE(self) -> None:
            self._handle_wsgi_request()

        def do_OPTIONS(self) -> None:
            self._handle_wsgi_request()

        def do_HEAD(self) -> None:
            self._handle_wsgi_request(send_body=False)

        def log_message(self, message_format: str, *args) -> None:
            del message_format, args

        def _handle_wsgi_request(self, *, send_body: bool = True) -> None:
            parsed = urlsplit(self.path)
            content_length = int(
                self.headers.get('Content-Length', '0') or '0',
            )
            request_body = (
                self.rfile.read(content_length) if content_length else b''
            )
            environ = {
                'CONTENT_LENGTH': str(content_length),
                'CONTENT_TYPE': self.headers.get('Content-Type', ''),
                'PATH_INFO': parsed.path or '/',
                'QUERY_STRING': parsed.query,
                'REMOTE_ADDR': self.client_address[0],
                'REQUEST_METHOD': self.command,
                'SCRIPT_NAME': '',
                'SERVER_NAME': self.server.server_name,
                'SERVER_PORT': str(self.server.server_port),
                'SERVER_PROTOCOL': self.request_version,
                'wsgi.errors': BytesIO(),
                'wsgi.input': BytesIO(request_body),
                'wsgi.multiprocess': False,
                'wsgi.multithread': True,
                'wsgi.run_once': False,
                'wsgi.url_scheme': 'http',
                'wsgi.version': (1, 0),
            }
            for header_name, header_value in self.headers.items():
                normalized_name = header_name.upper().replace('-', '_')
                if normalized_name in {'CONTENT_LENGTH', 'CONTENT_TYPE'}:
                    continue
                environ[f'HTTP_{normalized_name}'] = header_value

            response_status: list[str] = []
            response_headers: list[tuple[str, str]] = []

            def start_response(
                status: str,
                headers: list[tuple[str, str]],
                exc_info=None,
            ) -> None:
                del exc_info
                response_status[:] = [status]
                response_headers[:] = headers

            response_iterable = app(environ, start_response)
            try:
                response_chunks = [
                    chunk.encode('utf-8') if isinstance(chunk, str) else chunk
                    for chunk in response_iterable
                ]
            finally:
                close = getattr(response_iterable, 'close', None)
                if callable(close):
                    close()

            body = b''.join(response_chunks)
            status_line = response_status[0] if response_status else '200 OK'
            status_code = int(status_line.split()[0])
            headers = dict(response_headers)
            if 'Content-Length' not in headers:
                headers['Content-Length'] = str(len(body))
            if 'Connection' not in headers:
                headers['Connection'] = 'close'

            self.send_response(status_code)
            for header_name, header_value in headers.items():
                self.send_header(header_name, header_value)
            self.end_headers()
            if send_body:
                self.wfile.write(body)
            self.close_connection = True

    return _Handler


def main() -> None:
    args = _build_parser().parse_args()
    app, _is_factory = load_application_from_source_ref(
        args.app,
        backend='wsgi',
        root_path=Path(args.root_path),
    )
    server = ThreadingHTTPServer(
        (args.host, args.port),
        _build_wsgi_handler(app),
    )
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
