from __future__ import annotations

import importlib

import cosecha.workspace._version as version_module

from cosecha import workspace


def test_workspace_api_exports_and_version_contract() -> None:
    importlib.reload(workspace)
    importlib.reload(version_module)
    exported = set(workspace.__all__)
    assert 'resolve_workspace' in exported
    assert 'WorkspaceResolutionPolicy' in exported
    assert 'ExecutionContext' in exported
    assert version_module.__version__ == '0.1.0'
