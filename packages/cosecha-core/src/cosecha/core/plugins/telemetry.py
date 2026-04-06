from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Self, override

from cosecha.core.plugins.base import Plugin
from cosecha.core.telemetry import JsonlTelemetrySink


if TYPE_CHECKING:  # pragma: no cover
    from argparse import ArgumentParser, Namespace

    from cosecha.core.plugins.base import PluginContext


class TelemetryPlugin(Plugin):
    __slots__ = ('_sink',)

    def __init__(self, path: Path) -> None:
        self._sink = JsonlTelemetrySink(path)

    @override
    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None:
        parser.add_argument(
            '--telemetry-jsonl',
            type=Path,
            default=None,
            help='Exporta spans de telemetria a un fichero JSONL',
        )

    @override
    @classmethod
    def parse_args(cls, args: Namespace) -> Self | None:
        if args.telemetry_jsonl is None:
            return None

        return cls(args.telemetry_jsonl)

    @override
    async def initialize(self, context: PluginContext) -> None:
        await super().initialize(context)
        context.telemetry_stream.add_sink(self._sink)

    @override
    async def start(self) -> None:
        async with self.context.telemetry_stream.span(
            'plugin.telemetry.sink.start',
            attributes={'cosecha.plugin.name': self.plugin_name()},
        ):
            await self._sink.start()

    @override
    async def finish(self) -> None:
        return None
