from __future__ import annotations

import asyncio
import types

from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.manifest_types import ResourceBindingSpec
from cosecha.core.runtime_interop import build_runtime_canonical_binding_name
from cosecha.engine.pytest import items as items_module
from cosecha.engine.pytest.items import PytestTestDefinition, PytestTestItem


def test_request_proxy_get_resource_success_and_error() -> None:
    proxy = items_module._PytestRequestProxy(resources={'workspace': 'demo'})
    assert proxy.get_resource('workspace') == 'demo'

    missing_proxy = items_module._PytestRequestProxy(resources=None)
    with pytest.raises(LookupError, match="Unknown Cosecha resource: 'workspace'"):
        missing_proxy.get_resource('workspace')


def test_reset_runtime_batch_cache_global_and_root_scoped(tmp_path: Path) -> None:
    root_a = (tmp_path / 'a').resolve()
    root_b = (tmp_path / 'b').resolve()
    module_path = (tmp_path / 'tests' / 'test_demo.py').resolve()
    items_module._PYTEST_RUNTIME_NODEIDS_BY_MODULE[(root_a, module_path)] = {'id-a'}
    items_module._PYTEST_RUNTIME_NODEIDS_BY_MODULE[(root_b, module_path)] = {'id-b'}
    items_module._PYTEST_RUNTIME_RESULTS_BY_MODULE[(root_a, module_path)] = {'id-a': {}}
    items_module._PYTEST_RUNTIME_RESULTS_BY_MODULE[(root_b, module_path)] = {'id-b': {}}
    items_module._PYTEST_RUNTIME_TASKS_BY_MODULE[(root_a, module_path)] = (
        SimpleNamespace(done=lambda: True)
    )

    items_module.reset_pytest_runtime_batch_cache(
        root_path=root_a,
        clear_registrations=False,
    )
    assert (root_a, module_path) not in items_module._PYTEST_RUNTIME_RESULTS_BY_MODULE
    assert (root_b, module_path) in items_module._PYTEST_RUNTIME_RESULTS_BY_MODULE
    assert (root_a, module_path) in items_module._PYTEST_RUNTIME_NODEIDS_BY_MODULE

    items_module.reset_pytest_runtime_batch_cache(root_path=root_a)
    assert (root_a, module_path) not in items_module._PYTEST_RUNTIME_NODEIDS_BY_MODULE
    items_module.reset_pytest_runtime_batch_cache()
    assert items_module._PYTEST_RUNTIME_NODEIDS_BY_MODULE == {}
    assert items_module._PYTEST_RUNTIME_RESULTS_BY_MODULE == {}
    assert items_module._PYTEST_RUNTIME_TASKS_BY_MODULE == {}


def test_temporary_import_root_adds_and_removes_path(tmp_path: Path) -> None:
    import sys

    before = sys.path.copy()
    with items_module._temporary_import_root(tmp_path):
        assert str(tmp_path.resolve()) in sys.path
    assert sys.path == before


def test_register_builtin_manifest_descriptors_registers_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []
    fake_manifest_module = SimpleNamespace(
        PythonEngineDescriptor='python-engine',
        PythonHookDescriptor='python-hook',
    )
    monkeypatch.setattr(
        items_module.importlib,
        'import_module',
        lambda *_args, **_kwargs: fake_manifest_module,
    )
    monkeypatch.setattr(
        items_module,
        'register_engine_descriptor',
        lambda descriptor: calls.append(('engine', descriptor)),
    )
    monkeypatch.setattr(
        items_module,
        'register_hook_descriptor',
        lambda descriptor: calls.append(('hook', descriptor)),
    )

    items_module._register_builtin_manifest_descriptors()

    assert calls == [('engine', 'python-engine'), ('hook', 'python-hook')]


def test_temporary_loaded_discovery_registry_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker_calls: list[str] = []
    current_registry = object()
    default_registry = object()

    monkeypatch.setattr(
        items_module,
        '_register_builtin_manifest_descriptors',
        lambda: marker_calls.append('register'),
    )
    monkeypatch.setattr(
        items_module,
        'get_current_discovery_registry',
        lambda: current_registry,
    )
    monkeypatch.setattr(
        items_module,
        'get_default_discovery_registry',
        lambda: default_registry,
    )
    with items_module._temporary_loaded_discovery_registry():
        marker_calls.append('inside-current')

    monkeypatch.setattr(
        items_module,
        'get_current_discovery_registry',
        lambda: default_registry,
    )

    class _Ctx:
        def __enter__(self):
            marker_calls.append('enter-ctx')
            return self

        def __exit__(self, *_args):
            marker_calls.append('exit-ctx')
            return False

    monkeypatch.setattr(
        items_module,
        'create_loaded_discovery_registry',
        lambda: object(),
    )
    monkeypatch.setattr(
        items_module,
        'using_discovery_registry',
        lambda _registry: _Ctx(),
    )
    with items_module._temporary_loaded_discovery_registry():
        marker_calls.append('inside-default')

    assert marker_calls == [
        'register',
        'inside-current',
        'enter-ctx',
        'register',
        'inside-default',
        'exit-ctx',
    ]


def test_pytest_item_registers_runtime_nodeid_and_run_routes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    items_module.reset_pytest_runtime_batch_cache()
    runtime_item = PytestTestItem(
        tmp_path / 'tests' / 'test_demo.py',
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            requires_pytest_runtime=True,
        ),
        tmp_path,
    )
    runtime_key = runtime_item._pytest_runtime_batch_key()
    assert runtime_key in items_module._PYTEST_RUNTIME_NODEIDS_BY_MODULE

    calls: list[str] = []

    async def _runtime(*_args, **_kwargs) -> None:
        calls.append('runtime')

    async def _internal(*_args, **_kwargs) -> None:
        calls.append('internal')

    monkeypatch.setattr(PytestTestItem, '_run_via_pytest_runtime', _runtime)
    monkeypatch.setattr(
        PytestTestItem,
        '_run_via_internal_fast_path',
        _internal,
    )
    asyncio.run(runtime_item.run(SimpleNamespace()))

    internal_item = PytestTestItem(
        tmp_path / 'tests' / 'test_other.py',
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            requires_pytest_runtime=False,
        ),
        tmp_path,
    )
    asyncio.run(internal_item.run(SimpleNamespace()))

    assert calls == ['runtime', 'internal']


def test_pytest_item_predicate_and_binding_helpers(tmp_path: Path) -> None:
    item = PytestTestItem(
        tmp_path / 'tests' / 'test_demo.py',
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            skip_reason='skip me',
        ),
        tmp_path,
    )
    assert item.describe_execution_predicate().state == 'statically_skipped'
    assert item.uses_internal_fast_path() is True

    runtime_item = PytestTestItem(
        tmp_path / 'tests' / 'test_runtime.py',
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            requires_pytest_runtime=True,
            pytest_runtime_reason='runtime reason',
        ),
        tmp_path,
    )
    assert runtime_item.describe_execution_predicate().state == 'runtime_only'

    skip_issue_item = PytestTestItem(
        tmp_path / 'tests' / 'test_skip_issue.py',
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            skip_issue='skip issue',
        ),
        tmp_path,
    )
    assert skip_issue_item.describe_execution_predicate().reason == 'skip issue'

    requirement = SimpleNamespace(name='workspace')
    runtime_item.bind_manifest_resources((requirement,))
    runtime_item.bind_resource_bindings(
        (
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='workspace',
                fixture_name='cosecha_workspace',
            ),
        ),
    )
    runtime_item.bind_runtime_adapter_profiles('pytest', (SimpleNamespace(),))
    assert runtime_item.get_resource_requirements() == (requirement,)


def test_load_test_callable_success_and_failures(tmp_path: Path) -> None:
    module_path = tmp_path / 'test_demo.py'
    module_path.write_text(
        '\n'.join(
            (
                'class TestSuite:',
                '    def test_case(self):',
                '        return 1',
                '',
                'def test_function():',
                '    return 2',
            ),
        ),
        encoding='utf-8',
    )
    item = PytestTestItem(
        module_path,
        PytestTestDefinition(
            function_name='test_function',
            line=1,
        ),
        tmp_path,
    )
    module, function = item._load_test_callable()
    assert callable(function)
    assert function() == 2
    assert isinstance(module, types.ModuleType)

    class_item = PytestTestItem(
        module_path,
        PytestTestDefinition(
            function_name='test_case',
            class_name='TestSuite',
            line=1,
        ),
        tmp_path,
    )
    _module, method = class_item._load_test_callable()
    assert method() == 1

    missing_class_item = PytestTestItem(
        module_path,
        PytestTestDefinition(
            function_name='test_case',
            class_name='MissingClass',
            line=1,
        ),
        tmp_path,
    )
    with pytest.raises(RuntimeError, match='Unable to resolve pytest test class'):
        missing_class_item._load_test_callable()

    missing_function_item = PytestTestItem(
        module_path,
        PytestTestDefinition(
            function_name='missing',
            line=1,
        ),
        tmp_path,
    )
    with pytest.raises(RuntimeError, match='Unable to resolve pytest test callable'):
        missing_function_item._load_test_callable()


def test_runtime_bridge_selection_and_bound_resource_resolution(tmp_path: Path) -> None:
    definition = PytestTestDefinition(function_name='test_case', line=1)
    item = PytestTestItem(tmp_path / 'tests' / 'test_demo.py', definition, tmp_path)

    assert item._should_use_active_runtime_bridge(None) is False
    assert item._should_use_active_runtime_bridge(SimpleNamespace(resource_bindings=())) is False
    assert item._should_use_active_runtime_bridge(
        SimpleNamespace(resource_bindings=(object(),)),
    ) is True
    assert item._should_use_active_runtime_bridge(
        SimpleNamespace(resource_bindings=(), resources={'workspace': 'x'}),
    ) is True

    item.bind_resource_bindings(
        (
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='workspace',
                fixture_name='cosecha_workspace',
            ),
        ),
    )
    context = SimpleNamespace(
        resources={
            'workspace': 'workspace-resource',
            'database/main': 'db-resource',
        },
    )
    assert item._resolve_bound_resource_fixture('cosecha_workspace', context) == (
        'workspace-resource'
    )
    assert item._resolve_bound_resource_fixture(
        build_runtime_canonical_binding_name('database/main'),
        context,
    ) == 'db-resource'
    assert item._resolve_bound_resource_fixture('unknown', context) is None


def test_build_fixture_request_and_nodeid_repr_helpers(tmp_path: Path) -> None:
    item = PytestTestItem(
        tmp_path / 'tests' / 'test_demo.py',
        PytestTestDefinition(
            function_name='test_case',
            line=1,
            class_name='TestSuite',
            parameter_case_id='p1',
            selection_labels=('api',),
        ),
        tmp_path,
    )
    request = item._build_fixture_request(
        'fixture_name',
        context=SimpleNamespace(resources={'workspace': 'demo'}),
        indirect_parameter_values={'fixture_name': 'fixture-param'},
    )
    assert request.param == 'fixture-param'
    assert request.get_resource('workspace') == 'demo'
    assert item._build_pytest_nodeid() == 'tests/test_demo.py::TestSuite::test_case[p1]'
    assert 'PytestTestItem' in repr(item)
    assert item.has_selection_label('a*')


def test_materialize_generator_fixture_paths(tmp_path: Path) -> None:
    item = PytestTestItem(
        tmp_path / 'tests' / 'test_demo.py',
        PytestTestDefinition(function_name='test_case', line=1),
        tmp_path,
    )

    def _empty_generator():
        if False:
            yield None

    with pytest.raises(RuntimeError, match='did not yield a value'):
        item._materialize_generator_fixture(
            'fixture',
            _empty_generator(),
            context=SimpleNamespace(add_finalizer=lambda *_: None),
        )

    def _single_generator():
        yield 'value'

    with pytest.raises(RuntimeError, match='does not support fixture finalizers'):
        item._materialize_generator_fixture(
            'fixture',
            _single_generator(),
            context=SimpleNamespace(),
        )

    finalizers: list[object] = []
    yielded = item._materialize_generator_fixture(
        'fixture',
        _single_generator(),
        context=SimpleNamespace(add_finalizer=lambda finalizer: finalizers.append(finalizer)),
    )
    assert yielded == 'value'
    asyncio.run(finalizers[0]())


def test_materialize_async_generator_fixture_paths(tmp_path: Path) -> None:
    item = PytestTestItem(
        tmp_path / 'tests' / 'test_demo.py',
        PytestTestDefinition(function_name='test_case', line=1),
        tmp_path,
    )

    async def _empty_async_generator():
        if False:
            yield None

    with pytest.raises(RuntimeError, match='did not yield a value'):
        asyncio.run(
            item._materialize_async_generator_fixture(
                'fixture',
                _empty_async_generator(),
                context=SimpleNamespace(add_finalizer=lambda *_: None),
            ),
        )

    async def _single_async_generator():
        yield 'value'

    with pytest.raises(RuntimeError, match='does not support fixture finalizers'):
        asyncio.run(
            item._materialize_async_generator_fixture(
                'fixture',
                _single_async_generator(),
                context=SimpleNamespace(),
            ),
        )

    finalizers: list[object] = []
    yielded = asyncio.run(
        item._materialize_async_generator_fixture(
            'fixture',
            _single_async_generator(),
            context=SimpleNamespace(add_finalizer=lambda finalizer: finalizers.append(finalizer)),
        ),
    )
    assert yielded == 'value'
    asyncio.run(finalizers[0]())


def test_xfail_helpers_and_exception_resolution() -> None:
    assert items_module._build_xfail_message(None) == 'Expected failure by pytest xfail mark'
    assert items_module._build_xfail_message('reason') == 'Expected failure: reason'
    assert items_module._build_xpass_message(None) == 'Unexpected pass for pytest xfail mark'
    assert items_module._build_xpass_message('reason') == 'Unexpected pass for xfail: reason'

    module = SimpleNamespace(CustomError=RuntimeError, value=123)
    assert items_module._resolve_exception_type('CustomError', module) is RuntimeError
    assert items_module._resolve_exception_type('ValueError', module) is ValueError
    assert items_module._resolve_exception_type('value', module) is None
    assert items_module._resolve_exception_type('missing.path', module) is None

    assert items_module._matches_expected_xfail_exception(
        RuntimeError('x'),
        module,
        ('CustomError',),
    ) is True
    assert items_module._matches_expected_xfail_exception(
        RuntimeError('x'),
        module,
        ('ValueError',),
    ) is False
    assert items_module._matches_expected_xfail_exception(
        RuntimeError('x'),
        module,
        (),
    ) is True
    assert items_module._cast_optional_str(None) is None
    assert items_module._cast_optional_str(123) == '123'
