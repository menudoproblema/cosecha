from __future__ import annotations

import asyncio
import time

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from cosecha.core.utils import is_subpath


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.config import Config
    from cosecha.core.domain_event_stream import DomainEventStream
    from cosecha.core.items import TestItem
    from cosecha.core.session_timing import SessionTiming


class Collector(ABC):
    __slots__ = (
        '_domain_event_stream',
        '_engine_name',
        '_session_timing',
        'base_path',
        'collected_files',
        'collected_tests',
        'config',
        'failed_files',
        'file_type',
    )

    def __init__(self, file_type: str):
        self.file_type = file_type
        self.collected_files: set[Path] = set()
        self.failed_files: set[Path] = set()
        self.collected_tests: tuple[TestItem, ...] = ()

    def initialize(
        self,
        config: Config,
        base_path: str | Path | None = None,
    ) -> None:
        self.config = config
        self.base_path = Path(base_path or config.root_path)
        self._session_timing: SessionTiming | None = None
        self._engine_name: str = ''
        self._domain_event_stream: DomainEventStream | None = None
        self.failed_files = set()
        self.collected_files = set()
        self.collected_tests = ()

    def bind_session_timing(
        self,
        session_timing: SessionTiming,
        engine_name: str,
    ) -> None:
        self._session_timing = session_timing
        self._engine_name = engine_name

    def bind_domain_event_stream(
        self,
        domain_event_stream: DomainEventStream,
    ) -> None:
        self._domain_event_stream = domain_event_stream

    def _record_phase(self, phase: str, duration: float) -> None:
        if self._session_timing is not None:
            self._session_timing.record_collect_phase(
                self._engine_name,
                phase,
                duration,
            )

    def _normalize_collect_paths(
        self,
        path: Path | tuple[Path, ...] | None,
    ) -> tuple[Path, ...]:
        if path is None:
            return (self.base_path,)

        raw_paths = (path,) if isinstance(path, Path) else tuple(path)

        deduped_paths: dict[Path, Path] = {}
        for candidate_path in raw_paths:
            deduped_paths.setdefault(
                candidate_path.resolve(),
                candidate_path,
            )

        return tuple(deduped_paths.values())

    def _should_ignore_path(
        self,
        test_path: Path,
        excluded_paths: tuple[Path, ...],
    ) -> bool:
        return any(
            is_subpath(excluded_path, test_path)
            for excluded_path in excluded_paths
        )

    async def collect(
        self,
        path: Path | tuple[Path, ...] | None,
        excluded_paths: tuple[Path, ...] = (),
    ):
        self.failed_files: set[Path] = set()
        self.collected_files: set[Path] = set()
        self.collected_tests: tuple[TestItem, ...] = ()

        collect_paths = tuple(
            candidate_path
            for candidate_path in self._normalize_collect_paths(path)
            if is_subpath(self.base_path, candidate_path)
        )
        if not collect_paths:
            return

        normalized_excluded_paths = self._normalize_collect_paths(
            tuple(excluded_paths),
        )

        _t = time.perf_counter()
        unique_test_paths: dict[Path, Path] = {}
        for collect_path in collect_paths:
            for test_path in await self.find_test_files(collect_path):
                if self._should_ignore_path(
                    test_path,
                    normalized_excluded_paths,
                ):
                    continue

                unique_test_paths.setdefault(test_path.resolve(), test_path)
        test_paths = [
            unique_test_paths[resolved_path]
            for resolved_path in sorted(
                unique_test_paths,
                key=str,
            )
        ]
        self._record_phase('find_test_files', time.perf_counter() - _t)

        # Cargamos todos los ficheros en paralelo: cada load_tests_from_file
        # puede offloadear I/O+CPU al thread pool de forma independiente.
        _t = time.perf_counter()
        results = await asyncio.gather(
            *(self.load_tests_from_file(p) for p in test_paths),
        )
        self._record_phase('parse_and_build', time.perf_counter() - _t)

        collected_tests: list[TestItem] = []
        for test_path, collected_list in zip(
            test_paths,
            results,
            strict=False,
        ):
            rel = test_path.relative_to(self.config.root_path)
            if collected_list is not None:
                collected_tests.extend(collected_list)
                self.collected_files.add(rel)
            else:
                self.failed_files.add(rel)

        self.collected_tests = tuple(collected_tests)

    async def find_test_files(self, base_path: Path) -> list[Path]:
        exists, is_file = await asyncio.to_thread(
            lambda: (base_path.exists(), base_path.is_file()),
        )
        if not exists:
            return []

        if is_file:
            # Solo aceptamos el fichero directo si coincide con la extension
            # que este collector sabe procesar.
            if base_path.suffix != f'.{self.file_type}':
                return []
            return [await asyncio.to_thread(base_path.resolve)]

        # rglob es una operacion sincrona que puede bloquear el event loop
        # en arboles de directorios grandes.
        paths = await asyncio.to_thread(
            lambda: list(base_path.rglob(f'*.{self.file_type}')),
        )
        return sorted(paths)

    @abstractmethod
    async def load_tests_from_file(
        self,
        test_path: Path,
    ) -> list[TestItem] | None: ...
