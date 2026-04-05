from __future__ import annotations

import time

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from cosecha.core.capabilities import CapabilityDescriptor
    from cosecha.core.collector import Collector
    from cosecha.core.config import Config
    from cosecha.core.console import Justify, Overflow
    from cosecha.core.domain_event_stream import DomainEventStream
    from cosecha.core.engine_dependencies import EngineDependencyRule
    from cosecha.core.hooks import EngineHook
    from cosecha.core.items import TestItem, TestPreflightDecision
    from cosecha.core.reporter import Reporter
    from cosecha.core.reporting_ir import TestReport
    from cosecha.core.runtime_profiles import (
        RuntimeProfileSpec,
        RuntimeServiceOffering,
    )
    from cosecha.core.session_timing import SessionTiming


class BaseContext(ABC):
    @abstractmethod
    async def cleanup(self) -> None: ...

    def set_resources(self, resources: dict[str, object]) -> None:
        del resources

    def set_execution_metadata(
        self,
        metadata: ExecutionContextMetadata,
    ) -> None:
        del metadata


@dataclass(slots=True, frozen=True)
class ExecutionContextMetadata:
    node_id: str
    node_stable_id: str
    session_id: str | None = None
    plan_id: str | None = None
    trace_id: str | None = None
    worker_id: int | None = None


class Engine(ABC):
    __slots__ = (
        '_domain_event_stream',
        '_session_timing',
        'base_path',
        'collector',
        'config',
        'hooks',
        'name',
        'reporter',
        'runtime_profile_ids',
        'runtime_profiles',
        'runtime_service_offerings',
    )

    def __init__(
        self,
        name: str,
        collector: Collector,
        reporter: Reporter,
        hooks: Iterable[EngineHook] = (),
    ) -> None:
        self.name = name
        self.collector = collector
        self.reporter = reporter
        self.hooks = hooks
        self._session_timing: SessionTiming | None = None
        self._domain_event_stream: DomainEventStream | None = None
        self.runtime_profile_ids: tuple[str, ...] = ()
        self.runtime_profiles: tuple[RuntimeProfileSpec, ...] = ()
        self.runtime_service_offerings: tuple[RuntimeServiceOffering, ...] = ()

    @classmethod
    def engine_api_version(cls) -> int:
        return 1

    @classmethod
    def engine_stability(cls) -> str:
        return 'stable'

    @abstractmethod
    async def generate_new_context(self, test: TestItem) -> BaseContext: ...

    def initialize(self, config: Config, path: str) -> None:
        self.config = config
        self.base_path = self.config.root_path / path
        self.collector.initialize(self.config, self.base_path)

    def bind_session_timing(self, session_timing: SessionTiming) -> None:
        self._session_timing = session_timing
        self.collector.bind_session_timing(session_timing, self.name)

    def bind_domain_event_stream(
        self,
        domain_event_stream: DomainEventStream,
    ) -> None:
        self._domain_event_stream = domain_event_stream
        self.collector.bind_domain_event_stream(domain_event_stream)

    async def collect(
        self,
        path: Path | tuple[Path, ...] | None = None,
        excluded_paths: tuple[Path, ...] = (),
    ):
        for hook in self.hooks:
            await hook.before_collect(self.base_path, self.collector, self)

        await self.collector.collect(path, excluded_paths)

        _t = time.perf_counter()
        for hook in self.hooks:
            await hook.after_collect(self.base_path, self.collector, self)
        if self._session_timing is not None:
            self._session_timing.record_collect_phase(
                self.name,
                'after_collect_hooks',
                time.perf_counter() - _t,
            )

    def is_file_collected(self, test_file: str | Path):
        try:
            path = Path(test_file).resolve().relative_to(self.config.root_path)
        except Exception:
            return False

        return path in self.collector.collected_files

    def is_file_failed(self, test_file: str | Path):
        try:
            path = Path(test_file).resolve().relative_to(self.config.root_path)
        except Exception:
            return False

        return path in self.collector.failed_files

    def get_collected_tests(self) -> tuple[TestItem, ...]:
        return self.collector.collected_tests

    async def start_session(self) -> None:
        for hook in self.hooks:
            await hook.before_session_start(self)

    async def finish_session(self):
        for hook in self.hooks:
            await hook.after_session_finish(self)

    async def start_test(self, test: TestItem):
        for hook in self.hooks:
            await hook.before_test_run(test, self)

    async def finish_test(
        self,
        test: TestItem,
        report: TestReport | None = None,
    ):
        del report
        for hook in self.hooks:
            await hook.after_test_run(test, self)

    def preflight_test(
        self,
        test: TestItem,
    ) -> TestPreflightDecision | None:
        del test
        return None

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return ()

    def describe_engine_dependencies(
        self,
    ) -> tuple[EngineDependencyRule, ...]:
        return ()

    def log(  # noqa: PLR0913
        self,
        *objects: Any,
        sep: str = ' ',
        end: str = '\n',
        style: str | None = None,
        justify: Justify | None = None,
        overflow: Overflow | None = None,
        no_wrap: bool | None = None,
        emoji: bool | None = None,
        markup: bool | None = None,
        highlight: bool | None = None,
        width: int | None = None,
        height: int | None = None,
        crop: bool = True,
        soft_wrap: bool | None = None,
        new_line_start: bool = False,
    ) -> None:
        self.config.diagnostics.trace(
            *objects,
            sep=sep,
            end=end,
            style=style,
            justify=justify,
            overflow=overflow,
            no_wrap=no_wrap,
            emoji=emoji,
            markup=markup,
            highlight=highlight,
            width=width,
            height=height,
            crop=crop,
            soft_wrap=soft_wrap,
            new_line_start=new_line_start,
        )
