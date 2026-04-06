from __future__ import annotations

import sys
import tomllib

from pathlib import Path


ROOT = Path(__file__).resolve().parent
WORKSPACE_CONFIG = tomllib.loads(
    (ROOT / 'pyproject.toml').read_text(encoding='utf-8'),
)

for member in WORKSPACE_CONFIG['tool']['uv']['workspace']['members']:
    candidate = ROOT / member / 'src'
    if not candidate.exists():
        continue

    rendered_path = str(candidate)
    if rendered_path in sys.path:
        continue
    sys.path.insert(0, rendered_path)

from cosecha.engine.pytest import (  # noqa: E402
    resource_bridge_plugin as _resource_bridge_plugin,
)


def pytest_configure(config) -> None:
    _resource_bridge_plugin.pytest_configure(config)


def pytest_unconfigure(config) -> None:
    _resource_bridge_plugin.pytest_unconfigure(config)
