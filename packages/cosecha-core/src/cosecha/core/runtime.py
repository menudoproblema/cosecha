from __future__ import annotations

import asyncio
import contextlib
import os
import sys

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING

from cosecha.core.capabilities import (
    CAPABILITY_LIVE_EXECUTION_OBSERVABILITY,
    CapabilityAttribute,
    CapabilityDescriptor,
    CapabilityOperationBinding,
)
from cosecha.core.domain_events import (
    DomainEventMetadata,
    ResourceLifecycleEvent,
    WorkerDegradedEvent,
    WorkerHeartbeatEvent,
    WorkerRecoveredEvent,
)
from cosecha.core.execution_ir import ExecutionBootstrap, ExecutionRequest
from cosecha.core.knowledge_base import (
    LIVE_EXECUTION_EVENT_TAIL_LIMIT,
    LIVE_EXECUTION_RESOURCE_LIMIT,
    LIVE_EXECUTION_RUNNING_TEST_LIMIT,
    LIVE_EXECUTION_WORKER_LIMIT,
)
from cosecha.core.reporting_ir import deserialize_test_report
from cosecha.core.resources import (
    ResourceManager,
    normalize_resource_scope,
    reap_orphaned_resource,
)
from cosecha.core.runtime_protocol import (
    RuntimeBootstrapCommand,
    RuntimeBootstrapResponse,
    RuntimeEnvelopeMetadata,
    RuntimeErrorResponse,
    RuntimeEventResponse,
    RuntimeExecuteCommand,
    RuntimeExecuteResponse,
    RuntimeReadyResponse,
    RuntimeResponse,
    RuntimeShutdownCommand,
    RuntimeShutdownResponse,
    RuntimeSnapshotResourceTimingsCommand,
    RuntimeSnapshotResourceTimingsResponse,
    build_runtime_message_id,
    deserialize_runtime_response,
)
from cosecha.core.scheduler import (
    RoundRobinWorkerSelectionPolicy,
    WorkerSelectionPolicy,
    assign_group_slots,
)
from cosecha.core.serialization import decode_json_dict, encode_json_bytes


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Awaitable, Callable

    from cosecha.core.config import Config
    from cosecha.core.domain_events import DomainEvent
    from cosecha.core.execution_ir import TestExecutionNode
    from cosecha.core.reporting_ir import TestReport
    from cosecha.core.resources import (
        EffectiveResourceScope,
        ResourceMaterializationSnapshot,
        ResourceRequirement,
        ResourceTiming,
    )
    from cosecha.core.scheduler import SchedulingPlan


@dataclass(slots=True, frozen=True)
class ExecutionBodyResult:
    report: TestReport
    phase_durations: dict[str, float] = field(default_factory=dict)
    resource_timings: tuple[ResourceTiming, ...] = field(default_factory=tuple)
    domain_events: tuple[DomainEvent, ...] = field(default_factory=tuple)


class RuntimeProvider(ABC):
    @classmethod
    def runtime_api_version(cls) -> int:
        return 1

    @classmethod
    def runtime_stability(cls) -> str:
        return 'stable'

    @classmethod
    def runtime_name(cls) -> str:
        return cls.__name__.removesuffix('RuntimeProvider').lower()

    @classmethod
    def runtime_worker_model(cls) -> str:
        return 'single_process'

    def initialize(self, config: Config) -> None:
        del config

    async def start(self) -> None: ...  # noqa: B027

    async def finish(self) -> None: ...  # noqa: B027

    async def prepare(
        self,
        plan,
        *,
        scheduling_plan: SchedulingPlan | None = None,
    ) -> None:
        del plan, scheduling_plan

    def take_resource_timings(self):
        return ()

    def take_domain_events(self):
        return ()

    def take_log_events(self):
        return ()

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return ()

    def legacy_session_scope(self) -> EffectiveResourceScope:
        return 'run'

    def scheduler_worker_count(self, config: Config) -> int:
        return max(1, config.concurrency)

    def scheduler_worker_selection_policy(
        self,
    ) -> WorkerSelectionPolicy | None:
        return None

    def live_execution_delivery_mode(self) -> str:
        return 'poll_by_cursor'

    def live_execution_granularity(self) -> str:
        return 'streaming'

    def bind_execution_slot(
        self,
        node: TestExecutionNode,
        worker_slot: int,
    ) -> None:
        del node, worker_slot

    async def wait_for_live_observability(
        self,
        timeout_seconds: float | None,
    ) -> bool:
        del timeout_seconds
        return False

    @abstractmethod
    async def execute(
        self,
        node: TestExecutionNode,
        executor: Callable[
            [TestExecutionNode],
            Awaitable[ExecutionBodyResult],
        ],
    ) -> ExecutionBodyResult: ...


class RuntimeInfrastructureError(RuntimeError):
    __slots__ = ('code', 'fatal', 'recoverable')

    def __init__(
        self,
        *,
        code: str,
        message: str,
        recoverable: bool = False,
        fatal: bool = True,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.recoverable = recoverable
        self.fatal = fatal


class LocalRuntimeProvider(RuntimeProvider):
    __slots__ = ('executed_nodes',)

    def __init__(self) -> None:
        self.executed_nodes: list[str] = []

    @classmethod
    def runtime_name(cls) -> str:
        return 'local'

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name='isolated_processes',
                level='unsupported',
            ),
            CapabilityDescriptor(
                name='persistent_workers',
                level='unsupported',
            ),
            CapabilityDescriptor(
                name='injected_execution_plans',
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='run',
                        result_type='run.result',
                        freshness='fresh',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name='run_scoped_resources',
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='supported_scopes',
                        value=('run', 'worker', 'test'),
                    ),
                ),
            ),
            CapabilityDescriptor(
                name='worker_scoped_resources',
                level='accepted_noop',
                summary=(
                    'Worker scope collapses into the local runtime process'
                ),
                attributes=(
                    CapabilityAttribute(
                        name='effective_scope',
                        value='run',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_LIVE_EXECUTION_OBSERVABILITY,
                level='supported',
                delivery_mode=self.live_execution_delivery_mode(),
                granularity=self.live_execution_granularity(),
                summary=(
                    'The local runtime exposes a volatile read-only live '
                    'projection for the active session'
                ),
                attributes=(
                    CapabilityAttribute(name='read_only', value=True),
                    CapabilityAttribute(
                        name='live_source',
                        value='live_projection',
                    ),
                    CapabilityAttribute(
                        name='delivery_mode',
                        value=self.live_execution_delivery_mode(),
                    ),
                    CapabilityAttribute(
                        name='granularity',
                        value=self.live_execution_granularity(),
                    ),
                    CapabilityAttribute(
                        name='live_channels',
                        value=('events', 'logs'),
                    ),
                    CapabilityAttribute(
                        name='running_test_limit',
                        value=LIVE_EXECUTION_RUNNING_TEST_LIMIT,
                    ),
                    CapabilityAttribute(
                        name='worker_limit',
                        value=LIVE_EXECUTION_WORKER_LIMIT,
                    ),
                    CapabilityAttribute(
                        name='resource_limit',
                        value=LIVE_EXECUTION_RESOURCE_LIMIT,
                    ),
                    CapabilityAttribute(
                        name='event_tail_limit',
                        value=LIVE_EXECUTION_EVENT_TAIL_LIMIT,
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='execution.subscribe',
                        result_type='execution.subscribe',
                        freshness='volatile',
                    ),
                    CapabilityOperationBinding(
                        operation_type='execution.live_status',
                        result_type='execution.live_status',
                        freshness='volatile',
                    ),
                    CapabilityOperationBinding(
                        operation_type='execution.live_tail',
                        result_type='execution.live_tail',
                        freshness='volatile',
                    ),
                ),
            ),
        )

    def legacy_session_scope(self) -> EffectiveResourceScope:
        return 'run'

    def scheduler_worker_count(self, config: Config) -> int:
        return max(1, config.concurrency)

    def live_execution_granularity(self) -> str:
        return 'streaming'

    async def execute(
        self,
        node: TestExecutionNode,
        executor: Callable[
            [TestExecutionNode],
            Awaitable[ExecutionBodyResult],
        ],
    ) -> ExecutionBodyResult:
        self.executed_nodes.append(node.id)
        result = await executor(node)
        return ExecutionBodyResult(
            report=result.report,
            phase_durations=result.phase_durations,
            resource_timings=result.resource_timings,
            domain_events=result.domain_events,
        )


@dataclass(slots=True, frozen=True)
class _TrackedOrphanResource:
    name: str
    scope: str
    external_handle: str


class _OrphanedResourceManager:
    __slots__ = ('_active_resources_by_worker', '_requirements_by_key')

    def __init__(self) -> None:
        self._requirements_by_key: dict[
            tuple[str, str],
            ResourceRequirement,
        ] = {}
        self._active_resources_by_worker: dict[
            int,
            dict[tuple[str, str, str], _TrackedOrphanResource],
        ] = {}

    def register_plan(
        self,
        plan: tuple[TestExecutionNode, ...],
        *,
        legacy_session_scope: EffectiveResourceScope = 'worker',
    ) -> None:
        self._requirements_by_key.clear()
        for node in plan:
            for requirement in node.resource_requirements:
                normalized_scope = normalize_resource_scope(
                    requirement.scope,
                    legacy_session_scope=legacy_session_scope,
                )
                self._requirements_by_key.setdefault(
                    (requirement.name, normalized_scope),
                    requirement,
                )

    def observe_resource_event(
        self,
        event: ResourceLifecycleEvent,
    ) -> None:
        worker_id = event.metadata.worker_id
        external_handle = event.external_handle
        if worker_id is None or external_handle is None:
            return

        worker_resources = self._active_resources_by_worker.setdefault(
            worker_id,
            {},
        )
        key = (event.name, event.scope, external_handle)
        if event.action == 'acquired':
            worker_resources[key] = _TrackedOrphanResource(
                name=event.name,
                scope=event.scope,
                external_handle=external_handle,
            )
            return

        worker_resources.pop(key, None)
        if not worker_resources:
            self._active_resources_by_worker.pop(worker_id, None)

    async def reap_worker(
        self,
        worker_id: int,
        *,
        state_path: Path | None = None,
    ) -> tuple[_TrackedOrphanResource, ...]:
        tracked_resources = dict(
            self._active_resources_by_worker.get(worker_id, {}),
        )
        if state_path is not None:
            tracked_resources.update(
                self._load_worker_state_resources(state_path),
            )

        reaped_resources: list[_TrackedOrphanResource] = []
        ordered_resources = self._order_resources_for_reap(
            tuple(tracked_resources.values()),
        )
        for tracked_resource in ordered_resources:
            requirement = self._requirements_by_key.get(
                (tracked_resource.name, tracked_resource.scope),
            )
            if requirement is None:
                continue

            provider = requirement.resolve_provider()
            await reap_orphaned_resource(
                provider,
                tracked_resource.external_handle,
                requirement,
            )
            reaped_resources.append(tracked_resource)

        self._active_resources_by_worker.pop(worker_id, None)
        return tuple(reaped_resources)

    def _order_resources_for_reap(
        self,
        tracked_resources: tuple[_TrackedOrphanResource, ...],
    ) -> tuple[_TrackedOrphanResource, ...]:
        requirements_by_name = {
            requirement.name: requirement
            for requirement in self._requirements_by_key.values()
        }
        depth_by_name: dict[str, int] = {}

        def _resolve_depth(name: str) -> int:
            cached_depth = depth_by_name.get(name)
            if cached_depth is not None:
                return cached_depth

            requirement = requirements_by_name.get(name)
            if requirement is None or not requirement.depends_on:
                depth_by_name[name] = 0
                return 0

            depth = 1 + max(
                _resolve_depth(dependency)
                for dependency in requirement.depends_on
                if dependency in requirements_by_name
            )
            depth_by_name[name] = depth
            return depth

        return tuple(
            sorted(
                tracked_resources,
                key=lambda resource: (
                    -_resolve_depth(resource.name),
                    resource.scope,
                    resource.name,
                    resource.external_handle,
                ),
            ),
        )

    def _load_worker_state_resources(
        self,
        state_path: Path,
    ) -> dict[tuple[str, str, str], _TrackedOrphanResource]:
        if not state_path.exists():
            return {}

        payload = decode_json_dict(state_path.read_bytes())
        records: dict[tuple[str, str, str], _TrackedOrphanResource] = {}
        for resource in payload.get('active_resources', []):
            if not isinstance(resource, dict):
                continue
            name = resource.get('name')
            scope = resource.get('scope')
            external_handle = resource.get('external_handle')
            if not isinstance(name, str):
                continue
            if not isinstance(scope, str):
                continue
            if not isinstance(external_handle, str):
                continue
            key = (name, scope, external_handle)
            records[key] = _TrackedOrphanResource(
                name=name,
                scope=scope,
                external_handle=external_handle,
            )

        for resource in payload.get('pending_resources', []):
            if not isinstance(resource, dict):
                continue
            name = resource.get('name')
            scope = resource.get('scope')
            external_handle = resource.get('external_handle')
            if not isinstance(name, str):
                continue
            if not isinstance(scope, str):
                continue
            if not isinstance(external_handle, str):
                continue
            key = (name, scope, external_handle)
            records.setdefault(
                key,
                _TrackedOrphanResource(
                    name=name,
                    scope=scope,
                    external_handle=external_handle,
                ),
            )

        return records


class _PersistentWorker:
    __slots__ = (
        '_command_futures',
        '_live_activity_event',
        '_live_domain_events',
        '_live_log_events',
        '_ready_future',
        '_stderr_task',
        '_stderr_text',
        '_stdout_lock',
        '_stdout_reader_task',
        'process',
        'session_id',
        'stdin',
        'stdout',
        'worker_id',
    )

    def __init__(
        self,
        worker_id: int,
        process: asyncio.subprocess.Process,
        *,
        session_id: str,
    ) -> None:
        self.worker_id = worker_id
        self.session_id = session_id
        self.process = process
        self.stdin = process.stdin
        self.stdout = process.stdout
        self._stdout_lock = asyncio.Lock()
        self._ready_future: asyncio.Future[RuntimeReadyResponse] = (
            asyncio.get_running_loop().create_future()
        )
        self._command_futures: dict[str, asyncio.Future[RuntimeResponse]] = {}
        self._live_activity_event = asyncio.Event()
        self._live_domain_events: list[DomainEvent] = []
        self._live_log_events: list[DomainEvent] = []
        self._stderr_text: list[str] = []
        self._stderr_task = asyncio.create_task(self._read_stderr())
        self._stdout_reader_task = asyncio.create_task(self._read_stdout())

    @classmethod
    async def start(
        cls,
        worker_id: int,
        *,
        python_executable: str,
        cwd: Path,
        root_path: Path,
        session_id: str,
    ) -> _PersistentWorker:
        process = await asyncio.create_subprocess_exec(
            python_executable,
            '-m',
            'cosecha.runtime_worker',
            '--persistent',
            '--worker-id',
            str(worker_id),
            '--cwd',
            str(cwd),
            '--root-path',
            str(root_path),
            '--session-id',
            session_id,
            cwd=cwd,
            env=_build_worker_env(),
            stderr=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
        )
        worker = cls(worker_id, process, session_id=session_id)
        await worker._wait_ready()
        return worker

    async def execute(self, request: ExecutionRequest) -> ExecutionBodyResult:
        response = await self._send_command(
            RuntimeExecuteCommand(
                request=request,
                metadata=self._build_command_metadata(
                    correlation_id=request.node.stable_id,
                    idempotency_key=request.node.stable_id,
                ),
            ),
        )
        if not isinstance(response, RuntimeExecuteResponse):
            msg = (
                'Persistent worker returned an invalid execute response '
                f'(worker={self.worker_id})'
            )
            raise RuntimeError(msg)

        return ExecutionBodyResult(
            report=deserialize_test_report(response.report),
            phase_durations=response.phase_durations,
            resource_timings=response.resource_timings,
            domain_events=(
                *(response.domain_events or ()),
                *(response.resource_events or ()),
            ),
        )

    async def bootstrap(
        self,
        nodes,
        *,
        resource_materialization_snapshots=(),
    ) -> None:
        response = await self._send_command(
            RuntimeBootstrapCommand(
                bootstrap=ExecutionBootstrap.from_nodes(
                    nodes,
                    resource_materialization_snapshots=(
                        resource_materialization_snapshots
                    ),
                ),
                metadata=self._build_command_metadata(
                    correlation_id=_build_bootstrap_correlation_id(nodes),
                ),
            ),
        )
        if not isinstance(response, RuntimeBootstrapResponse):
            msg = (
                'Persistent worker returned an invalid bootstrap response '
                f'(worker={self.worker_id})'
            )
            raise RuntimeError(msg)

    async def collect_resource_timings(self) -> tuple[ResourceTiming, ...]:
        response = await self._send_command(
            RuntimeSnapshotResourceTimingsCommand(
                metadata=self._build_command_metadata(),
            ),
        )
        if not isinstance(response, RuntimeSnapshotResourceTimingsResponse):
            msg = (
                'Persistent worker returned an invalid resource snapshot '
                f'response (worker={self.worker_id})'
            )
            raise RuntimeError(msg)

        return response.resource_timings

    async def shutdown(
        self,
    ) -> tuple[tuple[ResourceTiming, ...], tuple[DomainEvent, ...]]:
        response = await self._send_command(
            RuntimeShutdownCommand(
                metadata=self._build_command_metadata(),
            ),
        )
        await self.process.wait()
        await self._stdout_reader_task
        await self._stderr_task
        if not isinstance(response, RuntimeShutdownResponse):
            msg = (
                'Persistent worker returned an invalid shutdown response '
                f'(worker={self.worker_id})'
            )
            raise RuntimeError(msg)

        return (
            response.resource_timings,
            (
                *(response.domain_events or ()),
                *(response.resource_events or ()),
            ),
        )

    def is_alive(self) -> bool:
        return self.process.returncode is None

    async def _wait_ready(self) -> None:
        await self._ready_future

    async def _send_command(
        self,
        command: RuntimeBootstrapCommand
        | RuntimeExecuteCommand
        | RuntimeShutdownCommand
        | RuntimeSnapshotResourceTimingsCommand,
    ) -> RuntimeResponse:
        if self.stdin is None or self.stdout is None:
            msg = f'Persistent worker {self.worker_id} is not available'
            raise RuntimeError(msg)

        response_future: asyncio.Future[RuntimeResponse] = (
            asyncio.get_running_loop().create_future()
        )
        self._command_futures[command.metadata.message_id] = response_future
        async with self._stdout_lock:
            self.stdin.write(encode_json_bytes(command.to_dict()) + b'\n')
            await self.stdin.drain()

        try:
            response = await response_future
        finally:
            self._command_futures.pop(command.metadata.message_id, None)

        if isinstance(response, RuntimeErrorResponse):
            raise RuntimeInfrastructureError(
                code=response.error.code,
                message=(
                    'Persistent runtime worker failed'
                    f' (worker={self.worker_id}).\n'
                    f'error={response.error.code}: {response.error.message}\n'
                    f'stderr:\n{"".join(self._stderr_text)}'
                ),
                recoverable=response.error.recoverable,
                fatal=response.error.fatal,
            )

        if isinstance(
            response,
            (
                RuntimeBootstrapResponse,
                RuntimeExecuteResponse,
                RuntimeShutdownResponse,
                RuntimeSnapshotResourceTimingsResponse,
            ),
        ):
            return response

        msg = (
            'Persistent runtime worker failed'
            f' (worker={self.worker_id}).\n'
            f'response={response}\nstderr:\n{"".join(self._stderr_text)}'
        )
        raise RuntimeError(msg)

    def take_live_domain_events(self) -> tuple[DomainEvent, ...]:
        events = tuple(self._live_domain_events)
        self._live_domain_events.clear()
        self._reset_live_activity_if_idle()
        return events

    def take_live_log_events(self) -> tuple[DomainEvent, ...]:
        events = tuple(self._live_log_events)
        self._live_log_events.clear()
        self._reset_live_activity_if_idle()
        return events

    def has_live_activity(self) -> bool:
        return bool(self._live_domain_events or self._live_log_events)

    async def wait_for_live_activity(
        self,
        timeout_seconds: float | None,
    ) -> bool:
        if self._live_domain_events or self._live_log_events:
            return True

        try:
            if timeout_seconds is None:
                await self._live_activity_event.wait()
            else:
                await asyncio.wait_for(
                    self._live_activity_event.wait(),
                    timeout_seconds,
                )
        except TimeoutError:
            return False

        return bool(self._live_domain_events or self._live_log_events)

    async def _read_stdout(self) -> None:
        if self.stdout is None:
            msg = f'Persistent worker {self.worker_id} has no stdout pipe'
            raise RuntimeError(msg)

        while True:
            raw_line = await self.stdout.readline()
            if not raw_line:
                error = RuntimeError(
                    'Persistent runtime worker exited unexpectedly'
                    f' (worker={self.worker_id}).\nstderr:\n'
                    f'{"".join(self._stderr_text)}',
                )
                self._fail_pending_responses(error)
                return

            response = deserialize_runtime_response(
                decode_json_dict(raw_line),
            )
            if isinstance(response, RuntimeReadyResponse):
                if not self._ready_future.done():
                    self._ready_future.set_result(response)
                continue

            if isinstance(response, RuntimeEventResponse):
                if response.stream_kind == 'log':
                    self._live_log_events.append(response.event)
                else:
                    self._live_domain_events.append(response.event)
                self._live_activity_event.set()
                continue

            in_reply_to = response.metadata.in_reply_to
            if in_reply_to is None:
                continue

            future = self._command_futures.get(in_reply_to)
            if future is not None and not future.done():
                future.set_result(response)

    def _build_command_metadata(
        self,
        *,
        correlation_id: str | None = None,
        idempotency_key: str | None = None,
        trace_id: str | None = None,
    ) -> RuntimeEnvelopeMetadata:
        return RuntimeEnvelopeMetadata(
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
            session_id=self.session_id,
            trace_id=trace_id,
        )

    async def _read_stderr(self) -> None:
        if self.process.stderr is None:
            return

        while True:
            raw_line = await self.process.stderr.readline()
            if not raw_line:
                return

            self._stderr_text.append(
                raw_line.decode('utf-8', errors='replace'),
            )

    def _reset_live_activity_if_idle(self) -> None:
        if self._live_domain_events or self._live_log_events:
            return

        self._live_activity_event.clear()

    def _fail_pending_responses(self, error: Exception) -> None:
        if not self._ready_future.done():
            self._ready_future.set_exception(error)

        for future in self._command_futures.values():
            if not future.done():
                future.set_exception(error)


class ProcessRuntimeProvider(RuntimeProvider):
    __slots__ = (
        '_assigned_worker_by_node',
        '_bootstrap_plan',
        '_cwd',
        '_degraded_worker_ids',
        '_domain_events',
        '_log_events',
        '_orphaned_resources',
        '_resource_timings',
        '_root_path',
        '_run_materialization_snapshots',
        '_run_resource_manager',
        '_runtime_state_dir',
        '_session_id',
        '_worker_count',
        '_worker_selection_policy',
        '_workers',
        'executed_nodes',
        'python_executable',
    )

    def __init__(
        self,
        python_executable: str | None = None,
        worker_count: int | None = None,
        *,
        worker_selection_policy: WorkerSelectionPolicy | None = None,
    ) -> None:
        self.executed_nodes: list[str] = []
        self.python_executable = python_executable or sys.executable
        self._worker_count = worker_count
        self._worker_selection_policy = (
            worker_selection_policy or RoundRobinWorkerSelectionPolicy()
        )
        self._cwd: Path | None = None
        self._root_path: Path | None = None
        self._session_id: str | None = None
        self._workers: list[_PersistentWorker] = []
        self._assigned_worker_by_node: dict[str, _PersistentWorker] = {}
        self._degraded_worker_ids: set[int] = set()
        self._bootstrap_plan: tuple[TestExecutionNode, ...] = ()
        self._resource_timings: list[ResourceTiming] = []
        self._domain_events: list[DomainEvent] = []
        self._log_events: list[DomainEvent] = []
        self._run_materialization_snapshots: tuple[
            ResourceMaterializationSnapshot,
            ...,
        ] = ()
        self._runtime_state_dir: Path | None = None
        self._run_resource_manager = ResourceManager()
        self._orphaned_resources = _OrphanedResourceManager()

    @classmethod
    def runtime_name(cls) -> str:
        return 'process'

    @classmethod
    def runtime_worker_model(cls) -> str:
        return 'persistent_workers'

    def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        return (
            CapabilityDescriptor(
                name='isolated_processes',
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='isolation_unit',
                        value='worker_process',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name='persistent_workers',
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='worker_lifecycle',
                        value='session',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name='injected_execution_plans',
                level='supported',
                operations=(
                    CapabilityOperationBinding(
                        operation_type='run',
                        result_type='run.result',
                        freshness='fresh',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name='run_scoped_resources',
                level='supported',
                summary=(
                    'Run-scoped resources are materialized in the parent '
                    'runtime and rehydrated in workers from connection data'
                ),
                attributes=(
                    CapabilityAttribute(
                        name='supported_scopes',
                        value=('run', 'worker', 'test'),
                    ),
                    CapabilityAttribute(
                        name='materialization',
                        value='snapshot_rebind',
                    ),
                ),
            ),
            CapabilityDescriptor(
                name='worker_scoped_resources',
                level='supported',
                attributes=(
                    CapabilityAttribute(
                        name='supported_scopes',
                        value=('worker', 'test'),
                    ),
                ),
            ),
            CapabilityDescriptor(
                name=CAPABILITY_LIVE_EXECUTION_OBSERVABILITY,
                level='supported',
                delivery_mode=self.live_execution_delivery_mode(),
                granularity=self.live_execution_granularity(),
                summary=(
                    'The process runtime exposes a volatile read-only live '
                    'projection backed by observed domain events'
                ),
                attributes=(
                    CapabilityAttribute(name='read_only', value=True),
                    CapabilityAttribute(
                        name='live_source',
                        value='live_projection',
                    ),
                    CapabilityAttribute(
                        name='delivery_mode',
                        value=self.live_execution_delivery_mode(),
                    ),
                    CapabilityAttribute(
                        name='granularity',
                        value=self.live_execution_granularity(),
                    ),
                    CapabilityAttribute(
                        name='live_channels',
                        value=('events', 'logs'),
                    ),
                    CapabilityAttribute(
                        name='running_test_limit',
                        value=LIVE_EXECUTION_RUNNING_TEST_LIMIT,
                    ),
                    CapabilityAttribute(
                        name='worker_limit',
                        value=LIVE_EXECUTION_WORKER_LIMIT,
                    ),
                    CapabilityAttribute(
                        name='resource_limit',
                        value=LIVE_EXECUTION_RESOURCE_LIMIT,
                    ),
                    CapabilityAttribute(
                        name='event_tail_limit',
                        value=LIVE_EXECUTION_EVENT_TAIL_LIMIT,
                    ),
                ),
                operations=(
                    CapabilityOperationBinding(
                        operation_type='execution.subscribe',
                        result_type='execution.subscribe',
                        freshness='volatile',
                    ),
                    CapabilityOperationBinding(
                        operation_type='execution.live_status',
                        result_type='execution.live_status',
                        freshness='volatile',
                    ),
                    CapabilityOperationBinding(
                        operation_type='execution.live_tail',
                        result_type='execution.live_tail',
                        freshness='volatile',
                    ),
                ),
            ),
        )

    def legacy_session_scope(self) -> EffectiveResourceScope:
        return 'worker'

    def scheduler_worker_count(self, config: Config) -> int:
        configured_worker_count = self._worker_count or config.concurrency
        return max(1, configured_worker_count)

    def live_execution_granularity(self) -> str:
        return 'consolidated_response'

    def scheduler_worker_selection_policy(
        self,
    ) -> WorkerSelectionPolicy | None:
        return self._worker_selection_policy

    def initialize(self, config: Config) -> None:
        self._cwd = _resolve_request_cwd(config.root_path)
        self._root_path = config.root_path
        self._runtime_state_dir = config.root_path / '.cosecha' / 'runtime'
        if self._worker_count is None:
            self._worker_count = max(1, config.concurrency)

    async def start(self) -> None:
        if self._cwd is None or self._root_path is None:
            msg = 'ProcessRuntimeProvider.initialize() must run before start()'
            raise RuntimeError(msg)

        if self._workers:
            return

        if self._session_id is None:
            self._session_id = build_runtime_message_id()

        worker_ids = tuple(range(max(1, self._worker_count or 1)))
        workers = await asyncio.gather(
            *(
                _PersistentWorker.start(
                    worker_id,
                    python_executable=self.python_executable,
                    cwd=self._cwd,
                    root_path=self._root_path,
                    session_id=self._session_id,
                )
                for worker_id in worker_ids
            ),
        )
        for worker_id, worker in zip(worker_ids, workers, strict=True):
            self._workers.append(worker)
            self._domain_events.append(
                self._build_worker_heartbeat_event(
                    worker_id,
                    status='ready',
                ),
            )

    async def prepare(
        self,
        plan,
        *,
        scheduling_plan: SchedulingPlan | None = None,
    ) -> None:
        await self.start()
        if not self._workers:
            return

        del scheduling_plan
        self._orphaned_resources.register_plan(
            plan,
            legacy_session_scope='worker',
        )
        self._assigned_worker_by_node.clear()
        run_scoped_requirements = _collect_run_scoped_requirements(plan)
        await self._run_resource_manager.close()
        self._run_resource_manager = ResourceManager()
        if run_scoped_requirements:
            await self._run_resource_manager.acquire_for_test(
                '__process_runtime_run_scope__',
                run_scoped_requirements,
            )
        materialization_snapshots = (
            await self._run_resource_manager.build_materialization_snapshot(
                scopes=('run',),
            )
        )
        self._run_materialization_snapshots = materialization_snapshots
        compiled_plan = _compile_bootstrap_plan(
            plan,
            root_path=self._root_path,
        )
        self._bootstrap_plan = compiled_plan

        await asyncio.gather(
            *(
                worker.bootstrap(
                    compiled_plan,
                    resource_materialization_snapshots=(
                        materialization_snapshots
                    ),
                )
                for worker in self._workers
                if compiled_plan
            ),
        )

    async def execute(
        self,
        node: TestExecutionNode,
        executor: Callable[
            [TestExecutionNode],
            Awaitable[ExecutionBodyResult],
        ],
    ) -> ExecutionBodyResult:
        del executor
        self.executed_nodes.append(node.id)
        worker = self._assigned_worker_by_node.get(
            node.id,
        ) or self._assigned_worker_by_node.get(node.stable_id)
        if worker is None:
            msg = (
                'ProcessRuntimeProvider.prepare() must assign a worker '
                f'before execute(): {node.id}'
            )
            raise RuntimeError(msg)

        request = ExecutionRequest.from_node(
            self._cwd or _resolve_request_cwd(node.engine.config.root_path),
            self._root_path or node.engine.config.root_path,
            node,
        )
        try:
            result = await worker.execute(request)
        except Exception as error:
            if self._should_mark_worker_as_degraded(worker, error):
                self._mark_worker_degraded(worker, error)
            await self._handle_worker_failure(worker, error)
            raise

        worker_state_error = self._build_worker_state_error(worker.worker_id)
        if worker_state_error is not None:
            self._mark_worker_degraded(worker, worker_state_error)

        if (
            worker_state_error is None
            and worker.worker_id in self._degraded_worker_ids
        ):
            self._degraded_worker_ids.remove(worker.worker_id)
            self._domain_events.append(
                WorkerRecoveredEvent(
                    worker_id=worker.worker_id,
                    metadata=DomainEventMetadata(
                        session_id=self._session_id,
                        worker_id=worker.worker_id,
                    ),
                ),
            )
            self._domain_events.append(
                self._build_worker_heartbeat_event(
                    worker.worker_id,
                    status='recovered',
                ),
            )

        self._observe_resource_events(result.domain_events)
        self._domain_events.append(
            self._build_worker_heartbeat_event(
                worker.worker_id,
                status='alive',
            ),
        )
        return result

    def bind_execution_slot(
        self,
        node: TestExecutionNode,
        worker_slot: int,
    ) -> None:
        if not self._workers:
            return

        worker = self._workers[worker_slot]
        self._assigned_worker_by_node[node.id] = worker
        self._assigned_worker_by_node[node.stable_id] = worker

    async def finish(self) -> None:
        if not self._workers:
            await self._run_resource_manager.close()
            self._resource_timings.extend(
                self._run_resource_manager.build_resource_timing_snapshot(),
            )
            self._run_resource_manager = ResourceManager()
            self._session_id = None
            return

        self._resource_timings.clear()
        self._domain_events.clear()
        for worker in self._workers:
            try:
                resource_timings, domain_events = await worker.shutdown()
            except Exception as error:
                await self._handle_worker_failure(worker, error)
                continue

            live_domain_events = worker.take_live_domain_events()
            live_log_events = worker.take_live_log_events()
            combined_domain_events = (
                *live_domain_events,
                *domain_events,
            )
            self._resource_timings.extend(resource_timings)
            self._observe_resource_events(combined_domain_events)
            self._domain_events.extend(combined_domain_events)
            self._log_events.extend(live_log_events)
            self._domain_events.append(
                self._build_worker_heartbeat_event(
                    worker.worker_id,
                    status='closed',
                ),
            )
            self._delete_worker_state(worker.worker_id)

        self._workers.clear()
        self._assigned_worker_by_node.clear()
        self._degraded_worker_ids.clear()
        await self._run_resource_manager.close()
        self._resource_timings.extend(
            self._run_resource_manager.build_resource_timing_snapshot(),
        )
        self._run_resource_manager = ResourceManager()
        self._run_materialization_snapshots = ()
        self._session_id = None

    def take_resource_timings(self):
        resource_timings = tuple(self._resource_timings)
        self._resource_timings = []
        return resource_timings

    def take_domain_events(self):
        for worker in self._workers:
            self._domain_events.extend(worker.take_live_domain_events())
        domain_events = tuple(self._domain_events)
        self._domain_events = []
        return domain_events

    def take_log_events(self):
        for worker in self._workers:
            self._log_events.extend(worker.take_live_log_events())
        log_events = tuple(self._log_events)
        self._log_events = []
        return log_events

    def _observe_resource_events(
        self,
        events: tuple[DomainEvent, ...],
    ) -> None:
        for event in events:
            if isinstance(event, ResourceLifecycleEvent):
                self._orphaned_resources.observe_resource_event(event)

    async def _handle_worker_failure(
        self,
        worker: _PersistentWorker,
        error: Exception,
    ) -> None:
        if worker.is_alive():
            return

        await self._orphaned_resources.reap_worker(
            worker.worker_id,
            state_path=self._worker_state_path(worker.worker_id),
        )
        self._domain_events.append(
            WorkerDegradedEvent(
                worker_id=worker.worker_id,
                reason=getattr(error, 'code', type(error).__name__),
                metadata=DomainEventMetadata(
                    session_id=self._session_id,
                    worker_id=worker.worker_id,
                ),
            ),
        )
        self._domain_events.append(
            self._build_worker_heartbeat_event(
                worker.worker_id,
                status='lost',
            ),
        )
        self._degraded_worker_ids.discard(worker.worker_id)
        self._delete_worker_state(worker.worker_id)
        if (
            self._cwd is None
            or self._root_path is None
            or self._session_id is None
        ):
            return

        recovered_worker = await _PersistentWorker.start(
            worker.worker_id,
            python_executable=self.python_executable,
            cwd=self._cwd,
            root_path=self._root_path,
            session_id=self._session_id,
        )
        if self._bootstrap_plan:
            await recovered_worker.bootstrap(
                self._bootstrap_plan,
                resource_materialization_snapshots=(
                    self._run_materialization_snapshots
                ),
            )
        self._workers[worker.worker_id] = recovered_worker
        self._domain_events.append(
            WorkerRecoveredEvent(
                worker_id=worker.worker_id,
                metadata=DomainEventMetadata(
                    session_id=self._session_id,
                    worker_id=worker.worker_id,
                ),
            ),
        )
        self._domain_events.append(
            self._build_worker_heartbeat_event(
                worker.worker_id,
                status='recovered',
            ),
        )

    def _should_mark_worker_as_degraded(
        self,
        worker: _PersistentWorker,
        error: Exception,
    ) -> bool:
        if not worker.is_alive():
            return False
        if not isinstance(error, RuntimeInfrastructureError):
            return False
        return error.code.startswith('resource_') or (
            error.code in {
                'worker_local_unhealthy',
                'worker_pending_resource_handle_missing',
            }
        )

    def _mark_worker_degraded(
        self,
        worker: _PersistentWorker,
        error: Exception,
    ) -> None:
        if worker.worker_id in self._degraded_worker_ids:
            return

        self._degraded_worker_ids.add(worker.worker_id)
        self._domain_events.append(
            WorkerDegradedEvent(
                worker_id=worker.worker_id,
                reason=getattr(error, 'code', type(error).__name__),
                metadata=DomainEventMetadata(
                    session_id=self._session_id,
                    worker_id=worker.worker_id,
                ),
            ),
        )
        self._domain_events.append(
            self._build_worker_heartbeat_event(
                worker.worker_id,
                status='degraded',
            ),
        )

    async def wait_for_live_observability(
        self,
        timeout_seconds: float | None,
    ) -> bool:
        if any(worker.has_live_activity() for worker in self._workers):
            return True

        if not self._workers:
            if timeout_seconds is not None:
                await asyncio.sleep(timeout_seconds)
            return False

        wait_tasks = [
            asyncio.create_task(
                worker.wait_for_live_activity(timeout_seconds),
            )
            for worker in self._workers
        ]
        done, pending = await asyncio.wait(
            wait_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in pending:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        return any(task.result() for task in done)

    def _build_worker_heartbeat_event(
        self,
        worker_id: int,
        *,
        status: str,
    ) -> WorkerHeartbeatEvent:
        return WorkerHeartbeatEvent(
            worker_id=worker_id,
            status=status,
            metadata=DomainEventMetadata(
                session_id=self._session_id,
                worker_id=worker_id,
            ),
        )

    def _build_worker_state_error(
        self,
        worker_id: int,
    ) -> RuntimeInfrastructureError | None:
        state_path = self._worker_state_path(worker_id)
        if state_path is None or not state_path.exists():
            return None

        payload = decode_json_dict(state_path.read_bytes())
        status = payload.get('status')
        unhealthy_resources = payload.get('unhealthy_resources')
        pending_resources = payload.get('pending_resources')
        if (
            status != 'degraded'
            and not unhealthy_resources
            and not pending_resources
        ):
            return None

        reason = 'worker_local_unhealthy'
        code = 'worker_local_unhealthy'
        if isinstance(unhealthy_resources, list) and unhealthy_resources:
            first_resource_name = unhealthy_resources[0]
            if isinstance(first_resource_name, str):
                reason = f'worker_local_unhealthy:{first_resource_name}'
        elif isinstance(pending_resources, list) and pending_resources:
            first_pending_resource = pending_resources[0]
            if isinstance(first_pending_resource, dict):
                first_pending_name = first_pending_resource.get('name')
                if isinstance(first_pending_name, str):
                    reason = (
                        'worker_pending_resource_handle_missing:'
                        f'{first_pending_name}'
                    )
            code = 'worker_pending_resource_handle_missing'

        return RuntimeInfrastructureError(
            code=code,
            message=reason,
            recoverable=True,
            fatal=False,
        )

    def _worker_state_path(
        self,
        worker_id: int,
    ) -> Path | None:
        if self._runtime_state_dir is None or self._session_id is None:
            return None

        return (
            self._runtime_state_dir
            / self._session_id
            / f'worker-{worker_id}.json'
        )

    def _delete_worker_state(
        self,
        worker_id: int,
    ) -> None:
        state_path = self._worker_state_path(worker_id)
        if state_path is None or not state_path.exists():
            return

        state_path.unlink()


def _resolve_request_cwd(root_path: Path) -> Path:
    if root_path.name == 'tests' and (root_path / 'cosecha.toml').exists():
        return root_path.parent

    return root_path


def _build_worker_env() -> dict[str, str]:
    env = os.environ.copy()
    src_path = Path(__file__).resolve().parents[1]
    current_pythonpath = env.get('PYTHONPATH')
    env['PYTHONPATH'] = (
        f'{src_path}{os.pathsep}{current_pythonpath}'
        if current_pythonpath
        else str(src_path)
    )
    return env


def _build_bootstrap_correlation_id(
    nodes: tuple[TestExecutionNode, ...],
) -> str:
    if not nodes:
        return RuntimeEnvelopeMetadata().message_id

    digest = sha256()
    for node in nodes:
        digest.update(node.stable_id.encode('utf-8'))

    return digest.hexdigest()


def _group_plan_nodes_for_workers(
    plan: tuple[TestExecutionNode, ...],
) -> dict[tuple[str, str, str], list[TestExecutionNode]]:
    grouped_nodes: dict[tuple[str, str, str], list[TestExecutionNode]] = (
        defaultdict(list)
    )
    for node in plan:
        grouped_nodes[_build_node_bootstrap_key(node)].append(node)

    return grouped_nodes


def _resolve_group_slots(
    grouped_nodes: dict[tuple[str, str, str], list[TestExecutionNode]],
    *,
    scheduling_plan: SchedulingPlan | None,
    worker_count: int,
    worker_selection_policy: WorkerSelectionPolicy,
) -> dict[tuple[str, str, str], int]:
    if scheduling_plan is None:
        return assign_group_slots(
            grouped_nodes.keys(),
            worker_count,
            worker_selection_policy=worker_selection_policy,
        )

    group_slots: dict[tuple[str, str, str], int] = {}
    for group_key, nodes in grouped_nodes.items():
        assigned_slots = {
            decision.worker_slot
            for node in nodes
            if (
                decision := scheduling_plan.decision_for_node(
                    node.id,
                    node.stable_id,
                )
            )
            is not None
        }
        if not assigned_slots:
            continue
        if len(assigned_slots) != 1:
            msg = (
                'Scheduling plan assigned the same bootstrap group to '
                f'multiple workers: {group_key!r}'
            )
            raise ValueError(msg)

        group_slots[group_key] = assigned_slots.pop() % max(1, worker_count)

    if len(group_slots) == len(grouped_nodes):
        return group_slots

    fallback_slots = assign_group_slots(
        tuple(
            group_key
            for group_key in grouped_nodes
            if group_key not in group_slots
        ),
        worker_count,
        worker_selection_policy=worker_selection_policy,
    )
    group_slots.update(fallback_slots)
    return group_slots


def _build_node_bootstrap_key(
    node: TestExecutionNode,
) -> tuple[str, str, str]:
    source_digest = (
        sha256(node.source_content.encode('utf-8')).hexdigest()
        if node.source_content is not None
        else ''
    )
    return (
        node.engine_name,
        node.test_path,
        source_digest,
    )


def _compile_bootstrap_plan(
    plan: tuple[TestExecutionNode, ...],
    *,
    root_path: Path | None,
) -> tuple[TestExecutionNode, ...]:
    del root_path
    return plan


def _collect_run_scoped_requirements(
    plan: tuple[TestExecutionNode, ...],
):
    requirements_by_name = {}
    for node in plan:
        for requirement in node.resource_requirements:
            if requirement.scope != 'run':
                continue

            previous = requirements_by_name.get(requirement.name)
            if previous is not None and not _run_requirements_are_compatible(
                previous,
                requirement,
            ):
                msg = (
                    'ProcessRuntimeProvider found conflicting run-scoped '
                    f'resource requirements for {requirement.name!r}'
                )
                raise ValueError(msg)
            requirements_by_name[requirement.name] = requirement

    return tuple(
        requirements_by_name[name] for name in sorted(requirements_by_name)
    )


def _run_requirements_are_compatible(
    left,
    right,
) -> bool:
    return (
        left.name == right.name
        and left.scope == right.scope
        and left.mode == right.mode
        and left.depends_on == right.depends_on
        and left.conflicts_with == right.conflicts_with
        and (left.provider is None) == (right.provider is None)
    )
