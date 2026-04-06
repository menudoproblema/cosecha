from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

from cosecha.core.config import Config
from cosecha.core.cosecha_manifest import PythonEngineDescriptor
from cosecha.core.discovery import (
    create_loaded_discovery_registry,
    using_discovery_registry,
)
from cosecha.core.items import TestResultStatus
from cosecha.core.runner import Runner
from cosecha.core.runtime import (
    ProcessRuntimeProvider,
    _resolve_request_cwd,
)
from cosecha.core.utils import setup_engines


if TYPE_CHECKING:
    from pathlib import Path

    import pytest


PYTHON_ENGINE_SOURCE = """
from pathlib import Path
import os

from cosecha.core.collector import Collector
from cosecha.core.engines.base import BaseContext, Engine
from cosecha.core.items import TestItem, TestResultStatus
from cosecha.core.reporter import Reporter
from cosecha.core.resources import ResourceRequirement


class DummyContext(BaseContext):
    def __init__(self) -> None:
        self.resources = {}
        self.config_flags = {}

    async def cleanup(self) -> None:
        return None

    def set_resources(self, resources) -> None:
        self.resources = resources.copy()


class DummyReporter(Reporter):
    async def add_test(self, test):
        del test

    async def add_test_result(self, test):
        del test

    async def print_report(self):
        return None


class DummyCollector(Collector):
    def __init__(self, tests):
        super().__init__('feature')
        self.tests = tests

    async def find_test_files(self, base_path):
        del base_path
        return [test.path for test in self.tests]

    async def load_tests_from_file(self, test_path):
        return [test for test in self.tests if test.path == test_path]

    async def load_tests_from_content(self, feature_content, test_path):
        del feature_content
        return [ProcessTest(test_path)]


class DummyEngine(Engine):
    async def generate_new_context(self, test):
        del test
        context = DummyContext()
        context.config_flags = {
            'capture_log': self.config.capture_log,
            'concurrency': self.config.concurrency,
            'strict_step_ambiguity': self.config.strict_step_ambiguity,
        }
        return context


class ProcessTest(TestItem):
    def __init__(self, path):
        super().__init__(path)

    async def run(self, context) -> None:
        assert context.resources['session_db'] == 'session-db'
        self.message = (
            f"{os.getpid()}:"
            f"{int(context.config_flags['strict_step_ambiguity'])}:"
            f"{int(context.config_flags['capture_log'])}:"
            f"{context.config_flags['concurrency']}"
        )
        self.status = TestResultStatus.PASSED

    def has_selection_label(self, name: str) -> bool:
        del name
        return False

    def get_resource_requirements(self):
        return (
            ResourceRequirement(
                name='session_db',
                scope='session',
                setup=lambda: 'session-db',
            ),
        )


TESTS = [
    ProcessTest(Path(__file__).parent / 'process.feature'),
    ProcessTest(Path(__file__).parent / 'process_second.feature'),
]


def build_engine():
    return DummyEngine(
        'dummy',
        collector=DummyCollector(TESTS),
        reporter=DummyReporter(),
    )
"""


PYTHON_ENGINE_MANIFEST = """
[manifest]
schema_version = 1

[[engines]]
id = "dummy"
type = "python"
name = "dummy"
path = ""
factory = "process_support.py:build_engine"
"""


PARALLEL_TEST_WORKER_COUNT = 3
PARALLEL_START_MIN_CONCURRENCY = 2


def _write_manifest_project(
    tests_path: Path,
    *,
    support_source: str,
) -> Path:
    manifest_path = tests_path / 'cosecha.toml'
    (tests_path / 'process_support.py').write_text(
        support_source,
        encoding='utf-8',
    )
    manifest_path.write_text(
        PYTHON_ENGINE_MANIFEST,
        encoding='utf-8',
    )
    return manifest_path


def test_resolve_request_cwd_prefers_project_root_when_tests_has_manifest(
    tmp_path: Path,
) -> None:
    tests_path = tmp_path / 'tests'
    tests_path.mkdir()
    (tests_path / 'cosecha.toml').write_text(
        '[manifest]\nschema_version = 1\n',
        encoding='utf-8',
    )

    assert _resolve_request_cwd(tests_path) == tmp_path
    assert _resolve_request_cwd(tmp_path) == tmp_path


def test_process_runtime_provider_starts_workers_in_parallel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_provider = ProcessRuntimeProvider(
        worker_count=PARALLEL_TEST_WORKER_COUNT,
    )
    runtime_provider.initialize(
        Config(
            root_path=tmp_path,
            capture_log=False,
            concurrency=PARALLEL_TEST_WORKER_COUNT,
        ),
    )
    active_starts = 0
    max_active_starts = 0

    class _FakeWorker:
        def __init__(self, worker_id: int) -> None:
            self.worker_id = worker_id

    async def _fake_start(
        worker_id: int,
        *,
        python_executable: str,
        cwd,
        root_path,
        session_id: str,
    ) -> _FakeWorker:
        nonlocal active_starts, max_active_starts
        del python_executable, cwd, root_path, session_id
        active_starts += 1
        max_active_starts = max(max_active_starts, active_starts)
        await asyncio.sleep(0)
        active_starts -= 1
        return _FakeWorker(worker_id)

    monkeypatch.setattr(
        'cosecha.core.runtime._PersistentWorker.start',
        _fake_start,
    )

    asyncio.run(runtime_provider.start())

    assert len(runtime_provider._workers) == PARALLEL_TEST_WORKER_COUNT
    assert [worker.worker_id for worker in runtime_provider._workers] == [
        0,
        1,
        2,
    ]
    assert max_active_starts >= PARALLEL_START_MIN_CONCURRENCY


def test_process_runtime_provider_executes_test_body_in_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_path = tmp_path
    tests_path = project_path / 'tests'
    tests_path.mkdir()
    (tests_path / 'process.feature').write_text(
        'Feature: process runtime\n',
        encoding='utf-8',
    )
    (tests_path / 'process_second.feature').write_text(
        'Feature: process runtime second\n',
        encoding='utf-8',
    )
    manifest_path = _write_manifest_project(
        tests_path,
        support_source=PYTHON_ENGINE_SOURCE,
    )

    monkeypatch.chdir(project_path)
    registry = create_loaded_discovery_registry()
    registry.register_engine_descriptor(PythonEngineDescriptor)
    with using_discovery_registry(registry):
        hooks, engines = setup_engines(
            Config(root_path=tests_path),
            manifest_file=manifest_path,
        )
    runtime_provider = ProcessRuntimeProvider(worker_count=1)
    runner = Runner(
        Config(
            root_path=tests_path,
            capture_log=False,
            stop_on_error=False,
            concurrency=1,
        ),
        engines,
        hooks,
        runtime_provider=runtime_provider,
    )

    has_failures = asyncio.run(runner.run())

    assert has_failures is False
    assert runtime_provider.executed_nodes == [
        'dummy:process.feature:0',
        'dummy:process_second.feature:1',
    ]
    engine = runner.engines[0]
    assert [test.status for test in engine.get_collected_tests()] == [
        TestResultStatus.PASSED,
        TestResultStatus.PASSED,
    ]
    assert len({test.message for test in engine.get_collected_tests()}) == 1
