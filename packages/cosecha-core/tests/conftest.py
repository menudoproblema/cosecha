from __future__ import annotations

import pytest


@pytest.fixture
def cosecha_workspace(request):
    get_resource = getattr(request, 'get_resource', None)
    if not callable(get_resource):
        pytest.skip("requires Cosecha resource 'workspace'")
    try:
        return get_resource('workspace')
    except LookupError:
        pytest.skip("requires Cosecha resource 'workspace'")
