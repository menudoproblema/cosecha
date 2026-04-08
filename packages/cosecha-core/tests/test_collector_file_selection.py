from __future__ import annotations

import asyncio

from types import SimpleNamespace
from typing import TYPE_CHECKING

from cosecha.core.collector import Collector


if TYPE_CHECKING:
    from pathlib import Path


class DummyCollector(Collector):
    def __init__(self) -> None:
        super().__init__('feature')

    async def load_tests_from_file(self, test_path: Path):
        del test_path
        return []


def test_find_test_files_ignores_direct_file_with_wrong_extension(
    tmp_path: Path,
) -> None:
    other_file = tmp_path / 'notes.txt'
    other_file.write_text('not a test', encoding='utf-8')
    collector = DummyCollector()

    assert asyncio.run(collector.find_test_files(other_file)) == []


def test_find_test_files_accepts_direct_file_with_expected_extension(
    tmp_path: Path,
) -> None:
    feature_file = tmp_path / 'payment.feature'
    feature_file.write_text('Feature: payment', encoding='utf-8')
    collector = DummyCollector()

    assert asyncio.run(collector.find_test_files(feature_file)) == [
        feature_file,
    ]


def test_collect_accepts_multiple_paths_and_exclusions(tmp_path: Path) -> None:
    included_dir = tmp_path / 'courses'
    excluded_dir = included_dir / 'archived'
    shared_dir = tmp_path / 'shared'
    included_dir.mkdir()
    excluded_dir.mkdir()
    shared_dir.mkdir()

    first_feature = included_dir / 'payment.feature'
    ignored_feature = excluded_dir / 'legacy.feature'
    shared_feature = shared_dir / 'catalog.feature'
    first_feature.write_text('Feature: payment', encoding='utf-8')
    ignored_feature.write_text('Feature: legacy', encoding='utf-8')
    shared_feature.write_text('Feature: catalog', encoding='utf-8')

    collector = DummyCollector()
    collector.initialize(SimpleNamespace(root_path=tmp_path), tmp_path)

    asyncio.run(
        collector.collect(
            (included_dir, shared_dir, tmp_path),
            excluded_paths=(excluded_dir,),
        ),
    )

    assert collector.collected_files == {
        first_feature.relative_to(tmp_path),
        shared_feature.relative_to(tmp_path),
    }
    assert (
        ignored_feature.relative_to(tmp_path)
        not in collector.collected_files
    )


def test_find_test_files_returns_empty_for_missing_path(tmp_path: Path) -> None:
    collector = DummyCollector()

    assert asyncio.run(collector.find_test_files(tmp_path / 'missing.feature')) == []


def test_collect_ignores_paths_outside_base_path(tmp_path: Path) -> None:
    outside_root = tmp_path.parent / 'outside'
    outside_root.mkdir(exist_ok=True)
    outside_feature = outside_root / 'outside.feature'
    outside_feature.write_text('Feature: outside', encoding='utf-8')

    collector = DummyCollector()
    collector.initialize(SimpleNamespace(root_path=tmp_path), tmp_path)

    asyncio.run(collector.collect(outside_root))

    assert collector.collected_files == set()
    assert collector.collected_tests == ()


class _NoneReturningCollector(DummyCollector):
    async def load_tests_from_file(self, test_path: Path):
        del test_path
        return None


def test_collect_tracks_failed_files_when_loader_returns_none(
    tmp_path: Path,
) -> None:
    feature_file = tmp_path / 'broken.feature'
    feature_file.write_text('Feature: broken', encoding='utf-8')
    collector = _NoneReturningCollector()
    collector.initialize(SimpleNamespace(root_path=tmp_path), tmp_path)

    asyncio.run(collector.collect(tmp_path))

    assert collector.failed_files == {feature_file.relative_to(tmp_path)}
