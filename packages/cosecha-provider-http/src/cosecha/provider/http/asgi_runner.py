from __future__ import annotations

import argparse
import asyncio

from pathlib import Path

from cosecha.provider.http.provider import load_application_from_source_ref


try:  # pragma: no cover - optional runtime dependency
    import uvicorn
except ModuleNotFoundError:  # pragma: no cover
    uvicorn = None

try:  # pragma: no cover - optional runtime dependency
    from hypercorn.asyncio import serve as hypercorn_serve
    from hypercorn.config import Config as HypercornConfig
except ModuleNotFoundError:  # pragma: no cover
    HypercornConfig = None
    hypercorn_serve = None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--app', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--root-path', required=True)
    parser.add_argument(
        '--server',
        required=True,
        choices=('uvicorn', 'hypercorn'),
    )
    parser.add_argument('--ssl-certfile')
    parser.add_argument('--ssl-keyfile')
    parser.add_argument('--ssl-ca-certs')
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    app, _is_factory = load_application_from_source_ref(
        args.app,
        backend='asgi',
        root_path=Path(args.root_path),
    )

    if args.server == 'hypercorn':
        if hypercorn_serve is None or HypercornConfig is None:
            msg = 'ASGI standalone runtime requires hypercorn'
            raise ModuleNotFoundError(msg)
        config = HypercornConfig()
        config.bind = [f'{args.host}:{args.port}']
        config.accesslog = None
        config.errorlog = None
        config.certfile = args.ssl_certfile
        config.keyfile = args.ssl_keyfile
        config.ca_certs = args.ssl_ca_certs
        asyncio.run(hypercorn_serve(app, config))
        return

    if uvicorn is None:
        msg = 'ASGI standalone runtime requires uvicorn'
        raise ModuleNotFoundError(msg)

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        ssl_certfile=args.ssl_certfile,
        ssl_keyfile=args.ssl_keyfile,
        ssl_ca_certs=args.ssl_ca_certs,
        log_level='warning',
        access_log=False,
    )


if __name__ == '__main__':
    main()
