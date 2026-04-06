from __future__ import annotations

import asyncio

import pytest

from cosecha.core.capabilities import CapabilityDescriptor
from cosecha.core.collector import Collector
from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.plugins.base import (
    PLUGIN_API_VERSION,
    CapabilityPublisher,
    PlanMiddleware,
    Plugin,
    PluginContext,
    ReporterPlugin,
    RuntimePlugin,
)
from cosecha.core.runner import OperationCapabilityError, Runner
from cosecha.core.utils import validate_plugin_class
from cosecha_internal.testkit import DummyReporter, build_config


class _EmptyCollector(Collector):
    def __init__(self) -> None:
        super().__init__('feature')

    async def find_test_files(self, base_path):
        del base_path
        return []

    async def load_tests_from_file(self, test_path):
        del test_path
        return []


class _DummyContext(BaseContext):
    async def cleanup(self) -> None:
        return None


class _DummyEngine(Engine):
    async def generate_new_context(self, test) -> BaseContext:
        del test
        return _DummyContext()


class _MissingCapabilityPlugin(Plugin):
    @classmethod
    def register_arguments(cls, parser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args):
        del args
        return cls()

    @classmethod
    def required_capabilities(cls) -> tuple[str, ...]:
        return ('missing_capability_for_test',)

    async def initialize(self, context: PluginContext) -> None:
        await super().initialize(context)

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


class _UnsupportedApiVersionPlugin(Plugin):
    @classmethod
    def plugin_api_version(cls) -> int:
        return PLUGIN_API_VERSION + 1

    @classmethod
    def register_arguments(cls, parser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args):
        del args
        return cls()

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


class _ReporterOnlyPlugin(ReporterPlugin):
    @classmethod
    def register_arguments(cls, parser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args):
        del args
        return cls()

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


class _RuntimeOnlyPlugin(RuntimePlugin):
    @classmethod
    def register_arguments(cls, parser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args):
        del args
        return cls()

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


class _PublisherOnlyPlugin(CapabilityPublisher):
    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name='plugin.capability',
                level='supported',
                stability='experimental',
            ),
        )

    @classmethod
    def register_arguments(cls, parser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args):
        del args
        return cls()

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


class _MiddlewareOnlyPlugin(PlanMiddleware):
    @classmethod
    def register_arguments(cls, parser) -> None:
        del parser

    @classmethod
    def parse_args(cls, args):
        del args
        return cls()

    async def start(self):
        return None

    async def finish(self) -> None:
        return None


def _build_runner(tmp_path, plugins):
    engine = _DummyEngine('dummy', _EmptyCollector(), DummyReporter())
    config = build_config(tmp_path)
    config.capture_log = False
    return Runner(
        config,
        {'': engine},
        plugins=plugins,
    )


def test_validate_plugin_class_rejects_unsupported_api_version() -> None:
    with pytest.raises(ValueError, match='Unsupported plugin API version'):
        validate_plugin_class(_UnsupportedApiVersionPlugin)


def test_plugin_surfaces_are_reported_from_specialized_bases() -> None:
    assert _MiddlewareOnlyPlugin.provided_surfaces() == ('plan_middleware',)
    assert _RuntimeOnlyPlugin.provided_surfaces() == ('runtime',)
    assert _ReporterOnlyPlugin.provided_surfaces() == ('reporter',)
    assert _PublisherOnlyPlugin.provided_surfaces() == (
        'capability_publisher',
    )


def test_runner_rejects_plugin_when_required_capability_is_missing(
    tmp_path,
) -> None:
    runner = _build_runner(tmp_path, [_MissingCapabilityPlugin()])

    with pytest.raises(
        OperationCapabilityError,
        match='Plugin requires unsupported capabilities',
    ):
        asyncio.run(runner.start_session())


def test_runner_exposes_plugin_capability_snapshots(tmp_path) -> None:
    runner = _build_runner(tmp_path, [_PublisherOnlyPlugin()])

    snapshots = runner.describe_system_capabilities()

    plugin_snapshot = next(
        snapshot
        for snapshot in snapshots
        if snapshot.component_kind == 'plugin'
    )
    assert plugin_snapshot.component_name == '_PublisherOnlyPlugin'
    assert plugin_snapshot.capabilities[0].name == 'plugin.capability'
