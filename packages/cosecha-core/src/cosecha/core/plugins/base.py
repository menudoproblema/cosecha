from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Self


if TYPE_CHECKING:  # pragma: no cover
    from argparse import ArgumentParser, Namespace

    from cosecha.core.capabilities import CapabilityDescriptor
    from cosecha.core.config import Config
    from cosecha.core.console import Justify, Overflow
    from cosecha.core.domain_event_stream import DomainEventStream
    from cosecha.core.execution_ir import PlanningAnalysis
    from cosecha.core.knowledge_base import KnowledgeBase
    from cosecha.core.resources import ResourceManager
    from cosecha.core.session_artifacts import SessionReportState
    from cosecha.core.session_timing import SessionTiming
    from cosecha.core.telemetry import TelemetryStream


type PluginStability = Literal['stable', 'experimental']
type PluginSurface = Literal[
    'plan_middleware',
    'runtime',
    'reporter',
    'capability_publisher',
]

PLUGIN_API_VERSION = 1


@dataclass(slots=True, frozen=True)
class PluginContext:
    config: Config
    session_timing: SessionTiming
    telemetry_stream: TelemetryStream
    domain_event_stream: DomainEventStream
    knowledge_base: KnowledgeBase
    resource_manager: ResourceManager
    engine_names: tuple[str, ...] = ()
    runtime_worker_model: str = 'single_process'
    session_report_state: SessionReportState | None = None


class Plugin(ABC):
    __slots__ = ('config', 'context')

    @classmethod
    def plugin_api_version(cls) -> int:
        return PLUGIN_API_VERSION

    @classmethod
    def plugin_name(cls) -> str:
        return cls.__name__

    @classmethod
    def plugin_stability(cls) -> PluginStability:
        return 'stable'

    @classmethod
    def required_capabilities(cls) -> tuple[str, ...]:
        return ()

    @classmethod
    def provided_surfaces(cls) -> tuple[PluginSurface, ...]:
        surfaces: list[PluginSurface] = []
        if issubclass(cls, PlanMiddleware):
            surfaces.append('plan_middleware')
        if issubclass(cls, RuntimePlugin):
            surfaces.append('runtime')
        if issubclass(cls, ReporterPlugin):
            surfaces.append('reporter')
        if issubclass(cls, CapabilityPublisher):
            surfaces.append('capability_publisher')
        return tuple(surfaces)

    @classmethod
    def start_priority(cls) -> int:
        return 0

    @classmethod
    def finish_priority(cls) -> int:
        return 0

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

    async def initialize(self, context: PluginContext) -> None:
        self.context = context
        self.config = context.config

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return ()

    @classmethod
    @abstractmethod
    def register_arguments(cls, parser: ArgumentParser) -> None: ...

    @classmethod
    @abstractmethod
    def parse_args(cls, args: Namespace) -> Self | None: ...

    @abstractmethod
    async def start(self): ...

    @abstractmethod
    async def finish(self) -> None: ...

    async def after_session_closed(self) -> None:
        return None


class PlanMiddleware(Plugin):
    async def transform_planning_analysis(
        self,
        analysis: PlanningAnalysis,
    ) -> PlanningAnalysis:
        return analysis


class RuntimePlugin(Plugin):
    pass


class ReporterPlugin(Plugin):
    pass


class CapabilityPublisher(Plugin):
    pass
