from __future__ import annotations

import sys

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT / 'src'
PACKAGES_ROOT = ROOT.parents[0]

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

for package_dir in sorted(PACKAGES_ROOT.iterdir()):
    if not package_dir.is_dir():
        continue
    for child_name in ('src', 'tests'):
        candidate = package_dir / child_name
        if not candidate.exists():
            continue
        rendered_path = str(candidate)
        if rendered_path in sys.path:
            continue
        sys.path.insert(0, rendered_path)
