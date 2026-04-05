from __future__ import annotations

from typing import TYPE_CHECKING

from cosecha.core.engines.base import BaseContext


if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Awaitable, Callable

    from cosecha.core.cosecha_manifest import ResourceBindingSpec


class PytestContext(BaseContext):
    __slots__ = ('_finalizers', 'resource_bindings', 'resources')

    def __init__(
        self,
        resource_bindings: tuple[ResourceBindingSpec, ...] = (),
    ) -> None:
        self._finalizers: list[Callable[[], Awaitable[None]]] = []
        self.resources: dict[str, object] = {}
        self.resource_bindings = resource_bindings

    async def cleanup(self) -> None:
        errors: list[Exception] = []
        while self._finalizers:
            finalizer = self._finalizers.pop()
            try:
                await finalizer()
            except Exception as error:
                errors.append(error)

        if not errors:
            return
        if len(errors) == 1:
            raise errors[0]

        msg = 'Error cleaning up pytest fixtures'
        raise ExceptionGroup(msg, errors)

    def set_resources(self, resources: dict[str, object]) -> None:
        self.resources = resources.copy()

    def add_finalizer(
        self,
        finalizer: Callable[[], Awaitable[None]],
    ) -> None:
        self._finalizers.append(finalizer)
