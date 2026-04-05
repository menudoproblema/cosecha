from __future__ import annotations

import asyncio
import builtins
import contextlib
import fnmatch
import inspect
import json
import os
import sys
import tempfile

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cosecha.core.items import (
    ExecutionPredicateEvaluation,
    TestItem,
    TestResultStatus,
)
from cosecha.core.runtime_profiles import (
    RuntimeCapabilityRequirement,
    RuntimeModeDisallowance,
    RuntimeModeRequirement,
    RuntimeRequirementSet,
    build_runtime_canonical_binding_name,
)
from cosecha.core.serialization import decode_json_dict
from cosecha.core.utils import import_module_from_path
from cosecha.engine.pytest.runtime_adapter import (
    run_pytest_runtime_batch_in_process,
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from types import ModuleType

    from cosecha.core.cosecha_manifest import ResourceBindingSpec
    from cosecha.core.resources import ResourceRequirement
    from cosecha.core.runtime_profiles import RuntimeProfileSpec


COSECHA_PYTEST_RUNTIME_PROFILES_ENV = 'COSECHA_PYTEST_RUNTIME_PROFILES'
COSECHA_PYTEST_RUNTIME_ENGINE_ENV = 'COSECHA_PYTEST_RUNTIME_ENGINE'


@dataclass(slots=True, frozen=True)
class PytestTestDefinition:
    function_name: str
    line: int
    class_name: str | None = None
    fixture_names: tuple[str, ...] = ()
    usefixture_names: tuple[str, ...] = ()
    conftest_paths: tuple[str, ...] = ()
    configured_definition_paths: tuple[str, ...] = ()
    imported_definition_paths: tuple[str, ...] = ()
    skip_reason: str | None = None
    skip_issue: str | None = None
    xfail_reason: str | None = None
    xfail_issue: str | None = None
    xfail_strict: bool = False
    xfail_run: bool = True
    xfail_raises_paths: tuple[str, ...] = ()
    requires_pytest_runtime: bool = False
    pytest_runtime_reason: str | None = None
    parameter_values: tuple[tuple[str, object], ...] = ()
    indirect_fixture_names: tuple[str, ...] = ()
    parameter_case_id: str | None = None
    selection_labels: tuple[str, ...] = ()
    required_runtime_interfaces: tuple[str, ...] = ()
    required_runtime_capabilities: tuple[tuple[str, str], ...] = ()
    required_runtime_modes: tuple[tuple[str, str], ...] = ()
    disallowed_runtime_modes: tuple[tuple[str, str], ...] = ()


@dataclass(slots=True, frozen=True)
class _PytestRequestProxy:
    param: object | None = None


@dataclass(slots=True)
class _FixtureResolutionState:
    context: Any
    indirect_parameter_values: dict[str, object]
    resolved_fixtures: dict[str, object]
    active_fixtures: tuple[str, ...] = ()


_PYTEST_RUNTIME_NODEIDS_BY_MODULE: dict[tuple[Path, Path], set[str]] = {}
_PYTEST_RUNTIME_RESULTS_BY_MODULE: dict[
    tuple[Path, Path],
    dict[str, dict[str, object]],
] = {}
_PYTEST_RUNTIME_TASKS_BY_MODULE: dict[
    tuple[Path, Path],
    asyncio.Task[dict[str, dict[str, object]]],
] = {}


def reset_pytest_runtime_batch_cache(
    *,
    root_path: Path | None = None,
    clear_registrations: bool = True,
) -> None:
    if root_path is None:
        if clear_registrations:
            _PYTEST_RUNTIME_NODEIDS_BY_MODULE.clear()
        _PYTEST_RUNTIME_RESULTS_BY_MODULE.clear()
        _PYTEST_RUNTIME_TASKS_BY_MODULE.clear()
        return

    resolved_root_path = root_path.resolve()
    stale_keys = tuple(
        key
        for key in _PYTEST_RUNTIME_RESULTS_BY_MODULE
        if key[0] == resolved_root_path
    )
    for key in stale_keys:
        _PYTEST_RUNTIME_RESULTS_BY_MODULE.pop(key, None)
        _PYTEST_RUNTIME_TASKS_BY_MODULE.pop(key, None)
    if not clear_registrations:
        return

    stale_registration_keys = tuple(
        key
        for key in _PYTEST_RUNTIME_NODEIDS_BY_MODULE
        if key[0] == resolved_root_path
    )
    for key in stale_registration_keys:
        _PYTEST_RUNTIME_NODEIDS_BY_MODULE.pop(key, None)


@contextlib.contextmanager
def _temporary_import_root(root_path: Path):
    resolved_root_path = str(root_path.resolve())
    inserted = False
    if resolved_root_path not in sys.path:
        sys.path.insert(0, resolved_root_path)
        inserted = True

    try:
        yield
    finally:
        if inserted:
            with contextlib.suppress(ValueError):
                sys.path.remove(resolved_root_path)


class PytestTestItem(TestItem):
    __slots__ = (
        '_manifest_resource_requirements',
        '_resource_bindings',
        '_runtime_adapter_engine_name',
        '_runtime_profile_specs',
        'definition',
        'module_path',
        'name',
        'root_path',
        'test_name',
    )

    def __init__(
        self,
        module_path: Path,
        definition: PytestTestDefinition,
        root_path: Path,
    ) -> None:
        super().__init__(module_path)
        self.module_path = module_path
        self.definition = definition
        self.root_path = root_path
        self._manifest_resource_requirements: tuple[
            ResourceRequirement,
            ...,
        ] = ()
        self._resource_bindings: tuple[ResourceBindingSpec, ...] = ()
        self._runtime_profile_specs: tuple[RuntimeProfileSpec, ...] = ()
        self._runtime_adapter_engine_name: str | None = None
        self.name = _build_pytest_test_name(definition)
        self.test_name = self.name
        if self.definition.requires_pytest_runtime:
            batch_key = self._pytest_runtime_batch_key()
            _PYTEST_RUNTIME_NODEIDS_BY_MODULE.setdefault(batch_key, set()).add(
                self._build_pytest_nodeid(),
            )

    async def run(self, context: Any) -> None:
        if self.definition.requires_pytest_runtime:
            await self._run_via_pytest_runtime(context)
            return

        await self._run_via_internal_fast_path(context)

    def uses_internal_fast_path(self) -> bool:
        return not self.definition.requires_pytest_runtime

    async def _run_via_internal_fast_path(self, context: Any) -> None:
        xfail_active = (
            self.definition.xfail_reason is not None
            and self.definition.xfail_run
        )
        try:
            module, test_fn = self._load_test_callable()
            fixture_modules = self._load_fixture_modules(module)
            indirect_parameter_values = {
                name: value
                for name, value in self.definition.parameter_values
                if name in self.definition.indirect_fixture_names
            }
            fixture_values = await self._build_fixture_values(
                fixture_modules,
                context,
                indirect_parameter_values=indirect_parameter_values,
            )
            parameter_values = {
                name: value
                for name, value in self.definition.parameter_values
                if name not in self.definition.indirect_fixture_names
            }
            result = test_fn(**fixture_values, **parameter_values)
            if inspect.isawaitable(result):
                await result
        except Exception as error:
            if not xfail_active:
                raise

            if not _matches_expected_xfail_exception(
                error,
                module,
                self.definition.xfail_raises_paths,
            ):
                raise

            self.status = TestResultStatus.SKIPPED
            self.message = _build_xfail_message(
                self.definition.xfail_reason,
            )
            return

        if xfail_active:
            self.message = _build_xpass_message(
                self.definition.xfail_reason,
            )
            if self.definition.xfail_strict:
                self.status = TestResultStatus.FAILED
                return

            self.status = TestResultStatus.PASSED
            return

        self.status = TestResultStatus.PASSED

    def has_selection_label(self, name: str) -> bool:
        return any(
            fnmatch.fnmatch(label, name)
            for label in self.definition.selection_labels
        )

    def describe_execution_predicate(self) -> ExecutionPredicateEvaluation:
        if self.definition.skip_reason is not None:
            return ExecutionPredicateEvaluation(
                state='statically_skipped',
                reason=self.definition.skip_reason,
            )

        if self.definition.requires_pytest_runtime:
            return ExecutionPredicateEvaluation(
                state='runtime_only',
                reason=(
                    self.definition.pytest_runtime_reason
                    or 'Requires pytest runtime adapter'
                ),
            )

        if self.definition.skip_issue is not None:
            return ExecutionPredicateEvaluation(
                state='runtime_only',
                reason=self.definition.skip_issue,
            )

        return ExecutionPredicateEvaluation()

    def get_runtime_requirement_set(self) -> RuntimeRequirementSet:
        return RuntimeRequirementSet(
            interfaces=self.definition.required_runtime_interfaces,
            capabilities=tuple(
                RuntimeCapabilityRequirement(
                    interface_name=interface_name,
                    capability_name=capability_name,
                )
                for interface_name, capability_name in (
                    self.definition.required_runtime_capabilities
                )
            ),
            required_modes=tuple(
                RuntimeModeRequirement(
                    interface_name=interface_name,
                    mode_name=mode_name,
                )
                for interface_name, mode_name in (
                    self.definition.required_runtime_modes
                )
            ),
            disallowed_modes=tuple(
                RuntimeModeDisallowance(
                    interface_name=interface_name,
                    mode_name=mode_name,
                )
                for interface_name, mode_name in (
                    self.definition.disallowed_runtime_modes
                )
            ),
        )

    def bind_manifest_resources(
        self,
        requirements: tuple[ResourceRequirement, ...],
    ) -> None:
        self._manifest_resource_requirements = requirements

    def bind_resource_bindings(
        self,
        bindings: tuple[ResourceBindingSpec, ...],
    ) -> None:
        self._resource_bindings = bindings

    def bind_runtime_adapter_profiles(
        self,
        engine_name: str,
        profiles: tuple[RuntimeProfileSpec, ...],
    ) -> None:
        self._runtime_adapter_engine_name = engine_name
        self._runtime_profile_specs = profiles

    def get_resource_requirements(self) -> tuple[ResourceRequirement, ...]:
        return self._manifest_resource_requirements

    def _load_test_callable(self) -> tuple[object, Callable[..., object]]:
        with _temporary_import_root(self.root_path):
            module = import_module_from_path(self.module_path)
        test_owner: object = module
        if self.definition.class_name is not None:
            owner_cls = getattr(module, self.definition.class_name, None)
            if owner_cls is None:
                msg = (
                    'Unable to resolve pytest test class '
                    f'{self.definition.class_name!r} in {self.module_path}'
                )
                raise RuntimeError(msg)

            test_owner = owner_cls()

        test_fn = getattr(test_owner, self.definition.function_name, None)
        if not callable(test_fn):
            msg = (
                'Unable to resolve pytest test callable '
                f'{self.test_name!r} in {self.module_path}'
            )
            raise RuntimeError(msg)

        return (module, test_fn)

    def _load_fixture_modules(self, module: object) -> tuple[ModuleType, ...]:
        fixture_modules = [module]
        with _temporary_import_root(self.root_path):
            fixture_modules.extend(
                import_module_from_path(Path(conftest_path))
                for conftest_path in reversed(self.definition.conftest_paths)
            )
            fixture_modules.extend(
                import_module_from_path(Path(source_path))
                for source_path in self.definition.imported_definition_paths
            )
            fixture_modules.extend(
                import_module_from_path(Path(source_path))
                for source_path in self.definition.configured_definition_paths
            )

        return tuple(fixture_modules)

    async def _run_via_pytest_runtime(
        self,
        context: Any | None = None,
    ) -> None:
        if self._should_use_active_runtime_bridge(context):
            payload = run_pytest_runtime_batch_in_process(
                root_path=self.root_path,
                nodeids=(self._build_pytest_nodeid(),),
                resource_bindings=self._resource_bindings,
                resources=getattr(context, 'resources', {}),
            ).get(self._build_pytest_nodeid())
            if payload is None:
                msg = (
                    'Pytest active-session bridge did not return a payload '
                    f'for {self.test_name!r}'
                )
                raise RuntimeError(msg)

            self.status = TestResultStatus(str(payload['status']))
            self.message = _cast_optional_str(payload.get('message'))
            self.failure_kind = _cast_optional_str(
                payload.get('failure_kind'),
            )
            self.error_code = _cast_optional_str(payload.get('error_code'))
            return

        batch_key = self._pytest_runtime_batch_key()
        result_payloads = _PYTEST_RUNTIME_RESULTS_BY_MODULE.get(batch_key)
        nodeid = self._build_pytest_nodeid()
        if result_payloads is None or nodeid not in result_payloads:
            result_payloads = await self._get_or_execute_pytest_runtime_batch()
            _PYTEST_RUNTIME_RESULTS_BY_MODULE[batch_key] = result_payloads

        payload = result_payloads.get(nodeid)
        if payload is None:
            msg = (
                'Pytest runtime adapter did not return a payload for '
                f'{self.test_name!r}'
            )
            raise RuntimeError(msg)

        self.status = TestResultStatus(str(payload['status']))
        self.message = _cast_optional_str(payload.get('message'))
        self.failure_kind = _cast_optional_str(
            payload.get('failure_kind'),
        )
        self.error_code = _cast_optional_str(payload.get('error_code'))
        result_payloads.pop(nodeid, None)
        if not result_payloads:
            _PYTEST_RUNTIME_RESULTS_BY_MODULE.pop(batch_key, None)

    async def _execute_pytest_runtime_batch(
        self,
    ) -> dict[str, dict[str, object]]:
        with tempfile.TemporaryDirectory() as tmp_dir:
            result_path = Path(tmp_dir) / 'pytest-runtime-result.json'
            batch_key = self._pytest_runtime_batch_key()
            nodeids = tuple(
                sorted(
                    _PYTEST_RUNTIME_NODEIDS_BY_MODULE.get(
                        batch_key,
                        {self._build_pytest_nodeid()},
                    ),
                ),
            )
            command = [
                sys.executable,
                '-m',
                'cosecha.engine.pytest.runtime_adapter',
                '--root-path',
                str(self.root_path),
                '--test-path',
                str(self.module_path),
                '--result-path',
                str(result_path),
            ]
            for nodeid in nodeids:
                command.extend(['--nodeid', nodeid])
            env = os.environ.copy()
            if self._runtime_adapter_engine_name is not None:
                env[COSECHA_PYTEST_RUNTIME_ENGINE_ENV] = (
                    self._runtime_adapter_engine_name
                )
            if self._runtime_profile_specs:
                env[COSECHA_PYTEST_RUNTIME_PROFILES_ENV] = json.dumps(
                    [
                        profile.to_dict()
                        for profile in self._runtime_profile_specs
                    ],
                    ensure_ascii=False,
                )
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=self.root_path,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if not result_path.exists():
                stdout_text = stdout.decode('utf-8', errors='replace').strip()
                stderr_text = stderr.decode('utf-8', errors='replace').strip()
                msg = (
                    'Pytest runtime adapter did not produce a result for '
                    f'{self.test_name!r}'
                )
                details = '\n'.join(
                    line for line in (stdout_text, stderr_text) if line
                )
                if details:
                    msg = f'{msg}\n{details}'
                raise RuntimeError(msg)

            payload = decode_json_dict(result_path.read_bytes())
            result_payloads = {
                str(nodeid): current_payload
                for nodeid, current_payload in payload.items()
                if isinstance(nodeid, str)
                and isinstance(current_payload, dict)
            }

            if process.returncode not in {0, 1}:
                msg = (
                    'Pytest runtime adapter returned an unsupported exit code '
                    f'for module {self.module_path}'
                )
                raise RuntimeError(msg)

            return result_payloads

    async def _get_or_execute_pytest_runtime_batch(
        self,
    ) -> dict[str, dict[str, object]]:
        batch_key = self._pytest_runtime_batch_key()
        task = _PYTEST_RUNTIME_TASKS_BY_MODULE.get(batch_key)
        if task is None:
            task = asyncio.create_task(self._execute_pytest_runtime_batch())
            _PYTEST_RUNTIME_TASKS_BY_MODULE[batch_key] = task
        try:
            return await task
        finally:
            if task.done():
                _PYTEST_RUNTIME_TASKS_BY_MODULE.pop(batch_key, None)

    def _should_use_active_runtime_bridge(self, context: Any | None) -> bool:
        if context is None:
            return False

        resource_bindings = getattr(context, 'resource_bindings', ())
        if isinstance(resource_bindings, tuple) and resource_bindings:
            return True

        resources = getattr(context, 'resources', None)
        return isinstance(resources, dict) and bool(resources)

    def _pytest_runtime_batch_key(self) -> tuple[Path, Path]:
        return (self.root_path.resolve(), self.module_path.resolve())

    async def _build_fixture_values(
        self,
        fixture_modules: tuple[ModuleType, ...],
        context: Any,
        *,
        indirect_parameter_values: dict[str, object],
    ) -> dict[str, object]:
        fixture_values: dict[str, object] = {}
        resolution_state = _FixtureResolutionState(
            context=context,
            indirect_parameter_values=indirect_parameter_values,
            resolved_fixtures={},
        )
        for fixture_name in self.definition.usefixture_names:
            await self._resolve_fixture_value(
                fixture_modules,
                fixture_name,
                resolution_state=resolution_state,
            )

        for fixture_name in self.definition.fixture_names:
            fixture_values[fixture_name] = await self._resolve_fixture_value(
                fixture_modules,
                fixture_name,
                resolution_state=resolution_state,
            )

        return fixture_values

    async def _resolve_fixture_value(
        self,
        fixture_modules: tuple[ModuleType, ...],
        fixture_name: str,
        *,
        resolution_state: _FixtureResolutionState,
    ) -> object:
        if fixture_name in resolution_state.resolved_fixtures:
            return resolution_state.resolved_fixtures[fixture_name]

        if fixture_name in resolution_state.active_fixtures:
            cycle_path = ' -> '.join(
                (*resolution_state.active_fixtures, fixture_name),
            )
            msg = (
                'PytestEngine v1 does not support cyclic fixture '
                f'dependencies: {cycle_path}'
            )
            raise RuntimeError(msg)

        fixture_fn = None
        for fixture_module in fixture_modules:
            fixture_candidate = getattr(fixture_module, fixture_name, None)
            if callable(fixture_candidate):
                fixture_fn = fixture_candidate
                break

        if not callable(fixture_fn):
            resource_fixture = self._resolve_bound_resource_fixture(
                fixture_name,
                resolution_state.context,
            )
            if resource_fixture is not None:
                resolution_state.resolved_fixtures[fixture_name] = (
                    resource_fixture
                )
                return resource_fixture
            msg = (
                'Unable to resolve pytest fixture '
                f'{fixture_name!r} in {self.module_path}'
            )
            raise RuntimeError(msg)

        fixture_factory = getattr(fixture_fn, '__wrapped__', fixture_fn)
        dependency_values: dict[str, object] = {}
        fixture_signature = inspect.signature(fixture_factory)
        for dependency_name in fixture_signature.parameters:
            if dependency_name == 'request':
                dependency_values[dependency_name] = (
                    self._build_fixture_request(
                        fixture_name,
                        indirect_parameter_values=(
                            resolution_state.indirect_parameter_values
                        ),
                    )
                )
                continue

            dependency_value = await self._resolve_fixture_value(
                fixture_modules,
                dependency_name,
                resolution_state=_FixtureResolutionState(
                    context=resolution_state.context,
                    indirect_parameter_values=(
                        resolution_state.indirect_parameter_values
                    ),
                    resolved_fixtures=resolution_state.resolved_fixtures,
                    active_fixtures=(
                        *resolution_state.active_fixtures,
                        fixture_name,
                    ),
                ),
            )
            dependency_values[dependency_name] = dependency_value

        fixture_value = fixture_factory(**dependency_values)
        if inspect.isawaitable(fixture_value):
            fixture_value = await fixture_value

        if inspect.isgenerator(fixture_value):
            fixture_value = self._materialize_generator_fixture(
                fixture_name,
                fixture_value,
                context=resolution_state.context,
            )
        elif inspect.isasyncgen(fixture_value):
            fixture_value = await self._materialize_async_generator_fixture(
                fixture_name,
                fixture_value,
                context=resolution_state.context,
            )

        resolution_state.resolved_fixtures[fixture_name] = fixture_value
        return fixture_value

    def _build_fixture_request(
        self,
        fixture_name: str,
        *,
        indirect_parameter_values: dict[str, object],
    ) -> _PytestRequestProxy:
        return _PytestRequestProxy(
            param=indirect_parameter_values.get(fixture_name),
        )

    def _materialize_generator_fixture(
        self,
        fixture_name: str,
        fixture_value,
        *,
        context: Any,
    ) -> object:
        try:
            yielded_value = next(fixture_value)
        except StopIteration as error:
            msg = (
                f'Pytest yield fixture did not yield a value: {fixture_name!r}'
            )
            raise RuntimeError(msg) from error

        add_finalizer = getattr(context, 'add_finalizer', None)
        if not callable(add_finalizer):
            msg = (
                'PytestContext does not support fixture finalizers for '
                f'{fixture_name!r}'
            )
            raise RuntimeError(msg)

        async def _finalize_fixture() -> None:
            try:
                next(fixture_value)
            except StopIteration:
                return
            finally:
                fixture_value.close()

            msg = (
                'Pytest yield fixture yielded more than once: '
                f'{fixture_name!r}'
            )
            raise RuntimeError(msg)

        add_finalizer(_finalize_fixture)
        return yielded_value

    async def _materialize_async_generator_fixture(
        self,
        fixture_name: str,
        fixture_value,
        *,
        context: Any,
    ) -> object:
        try:
            yielded_value = await builtins.anext(fixture_value)
        except StopAsyncIteration as error:
            msg = (
                'Pytest async yield fixture did not yield a value: '
                f'{fixture_name!r}'
            )
            raise RuntimeError(msg) from error

        add_finalizer = getattr(context, 'add_finalizer', None)
        if not callable(add_finalizer):
            msg = (
                'PytestContext does not support fixture finalizers for '
                f'{fixture_name!r}'
            )
            raise RuntimeError(msg)

        async def _finalize_fixture() -> None:
            try:
                await builtins.anext(fixture_value)
            except StopAsyncIteration:
                return
            finally:
                await fixture_value.aclose()

            msg = (
                'Pytest async yield fixture yielded more than once: '
                f'{fixture_name!r}'
            )
            raise RuntimeError(msg)

        add_finalizer(_finalize_fixture)
        return yielded_value

    def _resolve_bound_resource_fixture(
        self,
        fixture_name: str,
        context: Any,
    ) -> object | None:
        resources = getattr(context, 'resources', None)
        if not isinstance(resources, dict):
            return None

        for binding in self._resource_bindings:
            if binding.fixture_name != fixture_name:
                continue
            if binding.resource_name not in resources:
                return None
            return resources[binding.resource_name]

        for resource_name, resource in resources.items():
            if build_runtime_canonical_binding_name(resource_name) != (
                fixture_name
            ):
                continue
            return resource

        return None

    def _build_pytest_nodeid(self) -> str:
        relative_path = self.module_path.relative_to(self.root_path)
        node_parts = [relative_path.as_posix()]
        if self.definition.class_name is not None:
            node_parts.append(self.definition.class_name)

        function_name = self.definition.function_name
        if self.definition.parameter_case_id is not None:
            function_name = (
                f'{function_name}[{self.definition.parameter_case_id}]'
            )
        node_parts.append(function_name)
        return '::'.join(node_parts)

    def __repr__(self) -> str:
        return f'<PytestTestItem {self.module_path}:{self.test_name}>'


def _build_pytest_test_name(definition: PytestTestDefinition) -> str:
    if definition.class_name is None:
        base_name = definition.function_name
    else:
        base_name = f'{definition.class_name}.{definition.function_name}'

    if definition.parameter_case_id is None:
        return base_name

    return f'{base_name}[{definition.parameter_case_id}]'


def _build_xfail_message(reason: str | None) -> str:
    if reason is None:
        return 'Expected failure by pytest xfail mark'

    return f'Expected failure: {reason}'


def _build_xpass_message(reason: str | None) -> str:
    if reason is None:
        return 'Unexpected pass for pytest xfail mark'

    return f'Unexpected pass for xfail: {reason}'


def _matches_expected_xfail_exception(
    error: Exception,
    module: object,
    expected_paths: tuple[str, ...],
) -> bool:
    if not expected_paths:
        return True

    for path in expected_paths:
        exception_type = _resolve_exception_type(path, module)
        if exception_type is not None and isinstance(error, exception_type):
            return True

    return False


def _resolve_exception_type(
    path: str,
    module: object,
) -> type[BaseException] | None:
    path_parts = path.split('.')
    current: object | None = getattr(module, path_parts[0], None)
    if current is None:
        current = getattr(builtins, path_parts[0], None)

    if current is None:
        return None

    for part in path_parts[1:]:
        current = getattr(current, part, None)
        if current is None:
            return None

    if isinstance(current, type) and issubclass(current, BaseException):
        return current

    return None


def _cast_optional_str(value: object) -> str | None:
    return None if value is None else str(value)
