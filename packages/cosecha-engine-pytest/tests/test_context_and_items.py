from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING

import pytest

from cosecha.core.manifest_types import ResourceBindingSpec
from cosecha.core.runtime_interop import build_runtime_canonical_binding_name
from cosecha.core.runtime_profiles import (
    RuntimeCapabilityRequirement,
    RuntimeModeDisallowance,
    RuntimeModeRequirement,
)
from cosecha.engine.pytest.context import PytestContext
from cosecha.engine.pytest.items import (
    PytestTestDefinition,
    PytestTestItem,
    _build_pytest_test_name,
)


if TYPE_CHECKING:
    from pathlib import Path


def test_pytest_context_runs_finalizers_in_reverse_order() -> None:
    context = PytestContext()
    calls: list[str] = []

    async def first() -> None:
        calls.append('first')

    async def second() -> None:
        calls.append('second')

    context.add_finalizer(first)
    context.add_finalizer(second)
    asyncio.run(context.cleanup())

    assert calls == ['second', 'first']


def test_pytest_context_aggregates_finalizer_errors() -> None:
    context = PytestContext()

    async def first() -> None:
        msg = 'boom-1'
        raise RuntimeError(msg)

    async def second() -> None:
        msg = 'boom-2'
        raise ValueError(msg)

    context.add_finalizer(first)
    context.add_finalizer(second)

    with pytest.raises(
        ExceptionGroup,
        match='Error cleaning up pytest fixtures',
    ):
        asyncio.run(context.cleanup())


def test_pytest_context_raises_single_finalizer_error_directly() -> None:
    context = PytestContext()

    async def fail_once() -> None:
        msg = 'boom-once'
        raise RuntimeError(msg)

    context.add_finalizer(fail_once)
    with pytest.raises(RuntimeError, match='boom-once'):
        asyncio.run(context.cleanup())


def test_pytest_context_keeps_explicit_resource_bindings() -> None:
    bindings = (
        ResourceBindingSpec(
            engine_type='pytest',
            resource_name='workspace',
            fixture_name='cosecha_workspace',
        ),
    )
    context = PytestContext(resource_bindings=bindings)

    assert context.resource_bindings == bindings


def test_pytest_test_item_describes_predicates_and_runtime_requirements(
    tmp_path: Path,
) -> None:
    definition = PytestTestDefinition(
        function_name='test_demo',
        line=5,
        skip_reason='skip for now',
        required_runtime_interfaces=('db',),
        required_runtime_capabilities=(('db', 'transactions'),),
        required_runtime_modes=(('db', 'shared'),),
        disallowed_runtime_modes=(('db', 'offline'),),
    )
    item = PytestTestItem(tmp_path / 'test_demo.py', definition, tmp_path)

    predicate = item.describe_execution_predicate()
    requirement_set = item.get_runtime_requirement_set()

    assert predicate.state == 'statically_skipped'
    assert predicate.reason == 'skip for now'
    assert requirement_set.interfaces == ('db',)
    assert requirement_set.capabilities == (
        RuntimeCapabilityRequirement('db', 'transactions'),
    )
    assert requirement_set.required_modes == (
        RuntimeModeRequirement('db', 'shared'),
    )
    assert requirement_set.disallowed_modes == (
        RuntimeModeDisallowance('db', 'offline'),
    )


def test_pytest_test_item_resolves_bound_and_canonical_resource_fixtures(
    tmp_path: Path,
) -> None:
    definition = PytestTestDefinition(
        function_name='test_demo',
        line=5,
    )
    item = PytestTestItem(tmp_path / 'test_demo.py', definition, tmp_path)
    item.bind_resource_bindings(
        (
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='workspace',
                fixture_name='cosecha_workspace',
            ),
        ),
    )
    context = PytestContext()
    context.set_resources(
        {
            'workspace': object(),
            'database/mongo': 'db-resource',
        },
    )

    bound_resource = item._resolve_bound_resource_fixture(
        'cosecha_workspace',
        context,
    )
    canonical_resource = item._resolve_bound_resource_fixture(
        build_runtime_canonical_binding_name('database/mongo'),
        context,
    )

    assert bound_resource is context.resources['workspace']
    assert canonical_resource == 'db-resource'


def test_pytest_test_item_builds_nodeids_and_names_with_parameter_cases(
    tmp_path: Path,
) -> None:
    definition = PytestTestDefinition(
        function_name='test_demo',
        line=5,
        class_name='TestSuite',
        parameter_case_id='api',
    )
    item = PytestTestItem(tmp_path / 'test_demo.py', definition, tmp_path)

    assert _build_pytest_test_name(definition) == 'TestSuite.test_demo[api]'
    assert (
        item._build_pytest_nodeid()
        == 'test_demo.py::TestSuite::test_demo[api]'
    )
