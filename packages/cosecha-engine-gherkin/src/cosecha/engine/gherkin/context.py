from __future__ import annotations

from collections import UserDict
from typing import TYPE_CHECKING, Any, override

from cosecha.core.engines.base import BaseContext, ExecutionContextMetadata
from cosecha.engine.gherkin.managers import (
    BaseContextManager,
    TempPathManager,
)


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    from cosecha.core.cosecha_manifest import ResourceBindingSpec
    from cosecha.engine.gherkin.models import (
        DataTable,
        Feature,
        Scenario,
        Step,
    )
    from cosecha.engine.gherkin.steps.registry import StepRegistry
    from cosecha.engine.gherkin.types import DatatableCoercions


class ContextRegistry:
    def __init__(self, items: dict[tuple[str, str], Any] | None = None):
        self._items: dict[tuple[str, str], Any] = dict(items or {})

    def add(self, layout: str, name: str, item: Any):
        key = (layout, name)

        if key in self._items:
            if self._items[key] is item:
                return

            msg = f'Duplicated name "{name}" in layout "{layout}"'
            raise KeyError(msg)

        self._items[key] = item

    def get(self, layout: str, name: str):
        key = (layout, name)
        return self._items.get(key)

    def get_items(self, layout: str):
        return [
            (key[1], item)
            for key, item in self._items.items()
            if key[0] == layout
        ]

    def copy(self) -> ContextRegistry:
        # Evitamos deepcopy() porque es costoso y los items del contexto suelen
        # ser objetos que no necesitan una copia recursiva total o que se
        # gestionan via managers. El coste observado de dict.copy() para
        # tamaños de registry realistas sigue en el orden de microsegundos, así
        # que un modelo copy-on-write no compensa todavía su complejidad.
        return ContextRegistry(self._items.copy())


class Context(UserDict[str, Any], BaseContext):
    __slots__ = (
        '_coercions',
        '_execution_metadata',
        '_feature',
        '_managers',
        '_registry',
        '_resource_bindings',
        '_scenario',
        '_step',
        '_step_event_callback',
        '_step_registry',
        '_table',
    )

    def __init__(
        self,
        context_registry: ContextRegistry,
        step_registry: StepRegistry,
        coercions: DatatableCoercions,
        resource_bindings: tuple[ResourceBindingSpec, ...] = (),
        step_event_callback: Callable[
            [str, Context, Step, str | None, str | None],
            Awaitable[None],
        ]
        | None = None,
    ):
        self._registry = context_registry
        self._step_registry = step_registry
        self._coercions = coercions
        self._resource_bindings = resource_bindings
        self._step_event_callback = step_event_callback
        self._execution_metadata: ExecutionContextMetadata | None = None
        self._table = None
        self._feature: Feature | None = None
        self._scenario: Scenario | None = None
        self._step: Step | None = None
        self._managers: dict[type[BaseContextManager], BaseContextManager] = {}
        super().__init__()

    def setup_manager[T: BaseContextManager](self, manager_cls: type[T]) -> T:
        if manager_cls not in self._managers:
            self._managers[manager_cls] = manager_cls()

        return self._managers[manager_cls]  # type: ignore

    @override
    async def cleanup(self) -> None:
        errors: list[Exception] = []
        for manager in self._managers.values():
            try:
                manager.cleanup()
            except Exception as e:
                errors.append(e)
        if errors:
            msg = 'Errors during context cleanup'
            raise ExceptionGroup(msg, errors)

    @override
    def set_resources(self, resources: dict[str, object]) -> None:
        for name, resource in resources.items():
            self._registry.add('resource', name, resource)
            self[name] = resource
            for binding in self._resource_bindings:
                if binding.resource_name != name:
                    continue
                if binding.layout is None or binding.alias is None:
                    continue
                self._registry.add(binding.layout, binding.alias, resource)
                self[binding.alias] = resource

    @override
    def set_execution_metadata(
        self,
        metadata: ExecutionContextMetadata,
    ) -> None:
        self._execution_metadata = metadata

    def set_step(
        self,
        feature: Feature,
        scenario: Scenario,
        step: Step,
    ):
        self._table = None
        self._feature = feature
        self._scenario = scenario
        self._step = step

    async def notify_step_started(self, step: Step) -> None:
        if self._step_event_callback is None:
            return

        await self._step_event_callback('started', self, step, None, None)

    async def notify_step_finished(
        self,
        step: Step,
        *,
        status: str,
        message: str | None = None,
    ) -> None:
        if self._step_event_callback is None:
            return

        await self._step_event_callback(
            'finished',
            self,
            step,
            status,
            message,
        )

    @property
    def execution_metadata(self) -> ExecutionContextMetadata | None:
        return self._execution_metadata

    @property
    def registry(self) -> ContextRegistry:
        return self._registry

    @property
    def step_registry(self) -> StepRegistry:
        return self._step_registry

    @property
    def coercions(self) -> DatatableCoercions:
        return self._coercions

    def _raise_no_step_context(self, prop: str) -> None:
        keys = list(self.keys())
        msg = f'Context.{prop} accessed before set_step() was called' + (
            f' (context keys: {keys})' if keys else ' (context is empty)'
        )
        raise RuntimeError(msg)

    @property
    def feature(self) -> Feature:
        if self._feature is None:
            self._raise_no_step_context('feature')
        return self._feature  # type: ignore[return-value]

    @property
    def scenario(self) -> Scenario:
        if self._scenario is None:
            self._raise_no_step_context('scenario')
        return self._scenario  # type: ignore[return-value]

    @property
    def step(self) -> Step:
        if self._step is None:
            self._raise_no_step_context('step')
        return self._step  # type: ignore[return-value]

    @property
    def table(self) -> DataTable | None:
        if self._table is None and self._step is not None:
            self._table = self._step.table
        return self._table

    @property
    def tmp_path(self) -> Path:
        """Provee un directorio temporal aislado para el test."""
        return self.setup_manager(TempPathManager).get_path()
