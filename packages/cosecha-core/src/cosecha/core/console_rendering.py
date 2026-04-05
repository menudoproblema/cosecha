from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol


type ConsoleComponentKind = Literal[
    'section',
    'line',
    'table',
    'code_block',
    'status_badge',
    'extension',
]
type ConsoleTone = Literal[
    'default',
    'muted',
    'success',
    'warning',
    'error',
    'accent',
]


@dataclass(slots=True, frozen=True)
class TextSpan:
    text: str
    tone: ConsoleTone = 'default'
    emphatic: bool = False


@dataclass(slots=True, frozen=True)
class StatusBadge:
    label: str
    tone: ConsoleTone = 'default'
    kind: ConsoleComponentKind = 'status_badge'


@dataclass(slots=True, frozen=True)
class LineComponent:
    spans: tuple[TextSpan, ...] = ()
    badge: StatusBadge | None = None
    indent: int = 0
    kind: ConsoleComponentKind = 'line'


@dataclass(slots=True, frozen=True)
class TableComponent:
    columns: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...] = ()
    title: str | None = None
    kind: ConsoleComponentKind = 'table'


@dataclass(slots=True, frozen=True)
class CodeBlockComponent:
    code: str
    language: str | None = None
    title: str | None = None
    kind: ConsoleComponentKind = 'code_block'


@dataclass(slots=True, frozen=True)
class SectionComponent:
    title: str
    children: tuple[ConsoleRenderComponent, ...] = field(default_factory=tuple)
    kind: ConsoleComponentKind = 'section'


@dataclass(slots=True, frozen=True)
class ExtensionComponent:
    component_type: str
    payload: dict[str, object] = field(default_factory=dict)
    fallback: tuple[ConsoleRenderComponent, ...] = field(default_factory=tuple)
    kind: ConsoleComponentKind = 'extension'


type ConsoleRenderComponent = (
    LineComponent
    | TableComponent
    | CodeBlockComponent
    | SectionComponent
    | StatusBadge
    | ExtensionComponent
)


class EngineHumanRenderContribution(Protocol):
    contribution_name: str

    @classmethod
    def build_case_title(
        cls,
        report,
        *,
        config,
    ) -> tuple[TextSpan, ...] | None: ...

    @classmethod
    def build_console_components(
        cls,
        report,
        *,
        config,
    ) -> tuple[ConsoleRenderComponent, ...]: ...
