from __future__ import annotations

import asyncio

from cosecha.core.capabilities import (
    CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
    CapabilityAttribute,
    CapabilityDescriptor,
)
from cosecha.core.collector import Collector
from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.runner import Runner
from cosecha.core.runtime import LocalRuntimeProvider
from cosecha.core.shadow import acquire_shadow_handle
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


class _InvariantRuntimeProvider(LocalRuntimeProvider):
    COSECHA_COMPONENT_ID = 'cosecha.runtime.invariant-test'

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return super().describe_capabilities() + (
            CapabilityDescriptor(
                name=CAPABILITY_PRODUCES_EPHEMERAL_ARTIFACTS,
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='component_id',
                        value=self.COSECHA_COMPONENT_ID,
                    ),
                    CapabilityAttribute(
                        name='ephemeral_domain',
                        value='runtime',
                    ),
                    CapabilityAttribute(
                        name='produces_persistent',
                        value=False,
                    ),
                    CapabilityAttribute(
                        name='cleanup_on_success',
                        value=False,
                    ),
                    CapabilityAttribute(
                        name='preserve_on_failure',
                        value=True,
                    ),
                ),
            ),
        )

    async def start(self) -> None:
        marker = acquire_shadow_handle(
            self.COSECHA_COMPONENT_ID,
        ).ephemeral_file('runtime.marker')
        marker.write_text('ok', encoding='utf-8')


def test_runner_only_writes_inside_declared_shadow_destinations(
    tmp_path,
) -> None:
    before = {
        path.relative_to(tmp_path)
        for path in tmp_path.rglob('*')
    }
    runner = Runner(
        build_config(tmp_path),
        {'': _DummyEngine('gherkin', _EmptyCollector(), DummyReporter())},
        runtime_provider=_InvariantRuntimeProvider(),
    )

    asyncio.run(runner.run())

    after = {
        path.relative_to(tmp_path)
        for path in tmp_path.rglob('*')
    }
    created = after - before
    session_id = runner._domain_event_session_id
    expected_created = {
        tmp_path.joinpath('.cosecha').relative_to(tmp_path),
        tmp_path.joinpath('.cosecha', 'kb.db').relative_to(tmp_path),
        tmp_path.joinpath(
            '.cosecha',
            'preserved_artifacts',
        ).relative_to(tmp_path),
        tmp_path.joinpath(
            '.cosecha',
            'preserved_artifacts',
            session_id,
        ).relative_to(tmp_path),
        tmp_path.joinpath(
            '.cosecha',
            'preserved_artifacts',
            session_id,
            _InvariantRuntimeProvider.COSECHA_COMPONENT_ID,
        ).relative_to(tmp_path),
        tmp_path.joinpath(
            '.cosecha',
            'preserved_artifacts',
            session_id,
            _InvariantRuntimeProvider.COSECHA_COMPONENT_ID,
            'runtime.marker',
        ).relative_to(tmp_path),
        tmp_path.joinpath('.cosecha', 'shadow').relative_to(tmp_path),
    }
    preserved_root = tmp_path.joinpath(
        '.cosecha',
        'preserved_artifacts',
        session_id,
    ).relative_to(tmp_path)

    assert created == expected_created
    assert all(
        path == preserved_root
        or not str(path).startswith(f'{preserved_root}/')
        or str(path).startswith(
            f'{preserved_root}/{_InvariantRuntimeProvider.COSECHA_COMPONENT_ID}',
        )
        for path in created
    )
