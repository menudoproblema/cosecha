from __future__ import annotations

import inspect

from cosecha.core.runner import Runner


def test_runner_uses_engine_snapshot_hook_without_pytest_specific_helper(
) -> None:
    source = inspect.getsource(Runner)

    assert 'build_live_snapshot_payload' in source
    assert '_build_pytest_engine_snapshot_payload' not in source
