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
