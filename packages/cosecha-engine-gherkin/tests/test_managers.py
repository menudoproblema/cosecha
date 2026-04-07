from __future__ import annotations

from pathlib import Path

import pytest

from cosecha.engine.gherkin.managers import PatchContextManager, TempPathManager


def test_patch_context_manager_add_stop_and_cleanup() -> None:
    manager = PatchContextManager()

    first_mock = manager.add_patch(
        'cosecha.engine.gherkin.managers._patch_target_a',
        create=True,
    )
    assert '_patch_target_a' in repr(first_mock)

    with pytest.raises(ValueError, match='Duplicated patch target'):
        manager.add_patch(
            'cosecha.engine.gherkin.managers._patch_target_a',
            create=True,
        )

    manager.add_patch(
        'cosecha.engine.gherkin.managers._patch_target_b',
        create=True,
    )
    manager.stop_patch('cosecha.engine.gherkin.managers._patch_target_b')

    with pytest.raises(ValueError, match='Patch target .* not found'):
        manager.stop_patch('cosecha.engine.gherkin.managers._missing_patch')

    manager.cleanup()
    assert manager._patches == {}  # type: ignore[attr-defined]


def test_temp_path_manager_lifecycle() -> None:
    manager = TempPathManager()
    first_path = manager.get_path()
    second_path = manager.get_path()

    assert isinstance(first_path, Path)
    assert first_path == second_path
    assert first_path.exists()

    manager.cleanup()
    assert manager._temp_dir is None  # type: ignore[attr-defined]
    assert manager._path is None  # type: ignore[attr-defined]
