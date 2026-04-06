from __future__ import annotations

import asyncio

from types import SimpleNamespace
from typing import TYPE_CHECKING

from cosecha.core.collector import Collector


if TYPE_CHECKING:
    from pathlib import Path


class ParallelDummyCollector(Collector):
    def __init__(self, test_paths: tuple[Path, ...]) -> None:
        super().__init__('feature')
        self.test_paths = test_paths
        self.started_paths: list[Path] = []
        self.all_started = asyncio.Event()

    async def find_test_files(self, base_path: Path) -> list[Path]:
        del base_path
        return list(self.test_paths)

    async def load_tests_from_file(self, test_path: Path):
        self.started_paths.append(test_path)
        if len(self.started_paths) == len(self.test_paths):
            self.all_started.set()
        await self.all_started.wait()
        return []


def test_collect_loads_files_in_parallel(tmp_path: Path) -> None:
    first_path = tmp_path / 'first.feature'
    second_path = tmp_path / 'second.feature'
    first_path.write_text('Feature: first\n', encoding='utf-8')
    second_path.write_text('Feature: second\n', encoding='utf-8')

    collector = ParallelDummyCollector((first_path, second_path))
    collector.initialize(SimpleNamespace(root_path=tmp_path), tmp_path)

    asyncio.run(asyncio.wait_for(collector.collect(tmp_path), timeout=0.2))

    assert collector.started_paths == [first_path, second_path]
    assert collector.collected_files == {
        first_path.relative_to(tmp_path),
        second_path.relative_to(tmp_path),
    }
    assert collector.failed_files == set()
    assert collector.collected_tests == ()
