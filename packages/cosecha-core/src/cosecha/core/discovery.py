from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from importlib.metadata import EntryPoint, entry_points
from threading import Lock
from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:  # pragma: no cover
    from argparse import ArgumentParser, Namespace
    from pathlib import Path

    from cosecha.core.config import Config
    from cosecha.core.cosecha_manifest import (
        CosechaManifest,
        EngineSpec,
        HookSpec,
        ResourceBindingSpec,
        RuntimeProfileSpec,
    )
    from cosecha.core.engines.base import Engine
    from cosecha.core.hooks import Hook
    from cosecha.core.instrumentation import InstrumentationComponent
    from cosecha.core.plugins.base import Plugin
    from cosecha.core.resources import ResourceRequirement


ENGINE_ENTRYPOINT_GROUP = 'cosecha.engines'
HOOK_ENTRYPOINT_GROUP = 'cosecha.hooks'
PLUGIN_ENTRYPOINT_GROUP = 'cosecha.plugins'
INSTRUMENTATION_ENTRYPOINT_GROUP = 'cosecha.instrumentation'
SHELL_LSP_ENTRYPOINT_GROUP = 'cosecha.shell.lsp'
SHELL_CLI_ENTRYPOINT_GROUP = 'cosecha.shell.cli'
SHELL_REPORTING_ENTRYPOINT_GROUP = 'cosecha.shell.reporting'
CONSOLE_PRESENTER_ENTRYPOINT_GROUP = 'cosecha.console.presenters'
DEFINITION_QUERY_ENTRYPOINT_GROUP = 'cosecha.knowledge.query'
_ENTRY_POINT_CACHE_LOCK = Lock()
_ENTRY_POINT_CACHE: dict[str, tuple[EntryPoint, ...]] = {}


class DiscoveryLoadError(RuntimeError):
    __slots__ = ('entry_point_name', 'group')

    def __init__(
        self,
        *,
        group: str,
        entry_point_name: str,
        message: str,
    ) -> None:
        super().__init__(message)
        self.group = group
        self.entry_point_name = entry_point_name


class EngineDescriptor(Protocol):
    engine_type: str

    @classmethod
    def validate_resource_binding(
        cls,
        binding: ResourceBindingSpec,
        *,
        manifest: CosechaManifest,
    ) -> None: ...

    @classmethod
    def materialize(
        cls,
        engine_spec: EngineSpec,
        *,
        manifest: CosechaManifest,
        config: Config,
        active_profiles: tuple[RuntimeProfileSpec, ...],
        shared_requirements: tuple[ResourceRequirement, ...],
    ) -> Engine: ...


class HookDescriptor(Protocol):
    hook_type: str

    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None: ...

    @classmethod
    def apply_cli_overrides(
        cls,
        spec: HookSpec,
        args: Namespace,
    ) -> HookSpec: ...

    @classmethod
    def materialize(
        cls,
        spec: HookSpec,
        *,
        manifest_dir: Path,
    ) -> Hook: ...

    @classmethod
    def build_runtime_profile_hook_specs(
        cls,
        profile: RuntimeProfileSpec,
        *,
        engine_ids: tuple[str, ...],
    ) -> tuple[HookSpec, ...]: ...


class ShellContribution(Protocol):
    contribution_name: str


class ConsolePresenterContribution(Protocol):
    contribution_name: str


class DefinitionKnowledgeQueryProvider(Protocol):
    engine_name: str

    @classmethod
    def matching_descriptors(
        cls,
        descriptors,
        *,
        step_type: str | None = None,
        step_text: str | None = None,
    ): ...


class DiscoveryRegistry:
    __slots__ = (
        '_console_presenter_contributions',
        '_definition_query_providers',
        '_engine_descriptors',
        '_hook_descriptors',
        '_instrumentation_types',
        '_loaded',
        '_lock',
        '_plugin_types',
        '_shell_cli_contributions',
        '_shell_lsp_contributions',
        '_shell_reporting_contributions',
    )

    def __init__(self) -> None:
        self._engine_descriptors: dict[str, type[EngineDescriptor]] = {}
        self._definition_query_providers: dict[
            str,
            type[DefinitionKnowledgeQueryProvider],
        ] = {}
        self._hook_descriptors: dict[str, type[HookDescriptor]] = {}
        self._instrumentation_types: dict[
            str,
            type[InstrumentationComponent],
        ] = {}
        self._plugin_types: dict[str, type[Plugin]] = {}
        self._shell_lsp_contributions: dict[
            str,
            type[ShellContribution],
        ] = {}
        self._shell_cli_contributions: dict[
            str,
            type[ShellContribution],
        ] = {}
        self._shell_reporting_contributions: dict[
            str,
            type[ShellContribution],
        ] = {}
        self._console_presenter_contributions: dict[
            str,
            type[ConsolePresenterContribution],
        ] = {}
        self._loaded = False
        self._lock = Lock()

    def reset(self) -> None:
        with self._lock:
            self._engine_descriptors.clear()
            self._definition_query_providers.clear()
            self._hook_descriptors.clear()
            self._instrumentation_types.clear()
            self._plugin_types.clear()
            self._shell_lsp_contributions.clear()
            self._shell_cli_contributions.clear()
            self._shell_reporting_contributions.clear()
            self._console_presenter_contributions.clear()
            self._loaded = False

    def register_engine_descriptor(
        self,
        descriptor: type[EngineDescriptor],
    ) -> None:
        self._engine_descriptors[descriptor.engine_type] = descriptor

    def register_definition_query_provider(
        self,
        provider: type[DefinitionKnowledgeQueryProvider],
    ) -> None:
        self._definition_query_providers[provider.engine_name] = provider

    def register_hook_descriptor(
        self,
        descriptor: type[HookDescriptor],
    ) -> None:
        self._hook_descriptors[descriptor.hook_type] = descriptor

    def register_plugin_type(
        self,
        plugin_name: str,
        plugin_type: type[Plugin],
    ) -> None:
        self._plugin_types[plugin_name] = plugin_type

    def register_instrumentation_type(
        self,
        instrumentation_name: str,
        instrumentation_type: type[InstrumentationComponent],
    ) -> None:
        self._instrumentation_types[instrumentation_name] = (
            instrumentation_type
        )

    def register_shell_lsp_contribution(
        self,
        contribution: type[ShellContribution],
    ) -> None:
        self._shell_lsp_contributions[contribution.contribution_name] = (
            contribution
        )

    def register_shell_cli_contribution(
        self,
        contribution: type[ShellContribution],
    ) -> None:
        self._shell_cli_contributions[contribution.contribution_name] = (
            contribution
        )

    def register_shell_reporting_contribution(
        self,
        contribution: type[ShellContribution],
    ) -> None:
        self._shell_reporting_contributions[
            contribution.contribution_name
        ] = contribution

    def register_console_presenter_contribution(
        self,
        contribution: type[ConsolePresenterContribution],
    ) -> None:
        self._console_presenter_contributions[
            contribution.contribution_name
        ] = contribution

    def ensure_loaded(self) -> None:
        if self._loaded:
            return
        with self._lock:
            if self._loaded:
                return
            self._load_group(
                ENGINE_ENTRYPOINT_GROUP,
                self.register_engine_descriptor,
            )
            self._load_group(
                DEFINITION_QUERY_ENTRYPOINT_GROUP,
                self.register_definition_query_provider,
            )
            self._load_group(
                HOOK_ENTRYPOINT_GROUP,
                self.register_hook_descriptor,
            )
            self._load_plugin_group()
            self._load_instrumentation_group()
            self._load_group(
                SHELL_LSP_ENTRYPOINT_GROUP,
                self.register_shell_lsp_contribution,
            )
            self._load_group(
                SHELL_CLI_ENTRYPOINT_GROUP,
                self.register_shell_cli_contribution,
            )
            self._load_group(
                SHELL_REPORTING_ENTRYPOINT_GROUP,
                self.register_shell_reporting_contribution,
            )
            self._load_group(
                CONSOLE_PRESENTER_ENTRYPOINT_GROUP,
                self.register_console_presenter_contribution,
            )
            self._loaded = True

    def _load_group(self, group: str, register) -> None:
        for entry_point in _iter_group_entry_points(group):
            try:
                candidate = entry_point.load()
            except Exception as error:
                msg = (
                    'Failed to load discovery entry point '
                    f'{entry_point.name!r} from group {group!r}'
                )
                raise DiscoveryLoadError(
                    group=group,
                    entry_point_name=entry_point.name,
                    message=msg,
                ) from error
            if candidate is not None:
                register(candidate)

    def _load_plugin_group(self) -> None:
        for entry_point in _iter_group_entry_points(PLUGIN_ENTRYPOINT_GROUP):
            try:
                plugin_type = entry_point.load()
            except Exception as error:
                msg = (
                    'Failed to load plugin entry point '
                    f'{entry_point.name!r} from group '
                    f'{PLUGIN_ENTRYPOINT_GROUP!r}'
                )
                raise DiscoveryLoadError(
                    group=PLUGIN_ENTRYPOINT_GROUP,
                    entry_point_name=entry_point.name,
                    message=msg,
                ) from error
            if plugin_type is not None:
                self.register_plugin_type(entry_point.name, plugin_type)

    def _load_instrumentation_group(self) -> None:
        for entry_point in _iter_group_entry_points(
            INSTRUMENTATION_ENTRYPOINT_GROUP,
        ):
            try:
                instrumentation_type = entry_point.load()
            except Exception as error:
                msg = (
                    'Failed to load instrumentation entry point '
                    f'{entry_point.name!r} from group '
                    f'{INSTRUMENTATION_ENTRYPOINT_GROUP!r}'
                )
                raise DiscoveryLoadError(
                    group=INSTRUMENTATION_ENTRYPOINT_GROUP,
                    entry_point_name=entry_point.name,
                    message=msg,
                ) from error
            if instrumentation_type is not None:
                self.register_instrumentation_type(
                    entry_point.name,
                    instrumentation_type,
                )

    def iter_engine_descriptors(self) -> tuple[type[EngineDescriptor], ...]:
        self.ensure_loaded()
        return tuple(self._engine_descriptors.values())

    def get_engine_descriptor(
        self,
        engine_type: str,
    ) -> type[EngineDescriptor] | None:
        self.ensure_loaded()
        return self._engine_descriptors.get(engine_type)

    def get_definition_query_provider(
        self,
        engine_name: str,
    ) -> type[DefinitionKnowledgeQueryProvider] | None:
        self.ensure_loaded()
        return self._definition_query_providers.get(engine_name)

    def iter_hook_descriptors(self) -> tuple[type[HookDescriptor], ...]:
        self.ensure_loaded()
        return tuple(self._hook_descriptors.values())

    def get_hook_descriptor(
        self,
        hook_type: str,
    ) -> type[HookDescriptor] | None:
        self.ensure_loaded()
        return self._hook_descriptors.get(hook_type)

    def iter_plugin_types(self) -> tuple[type[Plugin], ...]:
        self.ensure_loaded()
        return tuple(self._plugin_types.values())

    def iter_instrumentation_types(
        self,
    ) -> tuple[type[InstrumentationComponent], ...]:
        self.ensure_loaded()
        return tuple(self._instrumentation_types.values())

    def iter_shell_lsp_contributions(
        self,
    ) -> tuple[type[ShellContribution], ...]:
        self.ensure_loaded()
        return tuple(self._shell_lsp_contributions.values())

    def iter_shell_cli_contributions(
        self,
    ) -> tuple[type[ShellContribution], ...]:
        self.ensure_loaded()
        return tuple(self._shell_cli_contributions.values())

    def iter_shell_reporting_contributions(
        self,
    ) -> tuple[type[ShellContribution], ...]:
        self.ensure_loaded()
        return tuple(self._shell_reporting_contributions.values())

    def iter_console_presenter_contributions(
        self,
    ) -> tuple[type[ConsolePresenterContribution], ...]:
        self.ensure_loaded()
        return tuple(self._console_presenter_contributions.values())


_default_registry = DiscoveryRegistry()
_current_registry: ContextVar[DiscoveryRegistry | None] = ContextVar(
    'cosecha_current_discovery_registry',
    default=None,
)


def get_default_discovery_registry() -> DiscoveryRegistry:
    return _default_registry


def get_current_discovery_registry() -> DiscoveryRegistry:
    current = _current_registry.get()
    return _default_registry if current is None else current


@contextmanager
def using_discovery_registry(registry: DiscoveryRegistry):
    token = _current_registry.set(registry)
    try:
        yield registry
    finally:
        _current_registry.reset(token)


def create_discovery_registry() -> DiscoveryRegistry:
    return DiscoveryRegistry()


def create_loaded_discovery_registry() -> DiscoveryRegistry:
    registry = create_discovery_registry()
    registry.ensure_loaded()
    return registry


def clear_discovery_registry() -> None:
    get_current_discovery_registry().reset()
    with _ENTRY_POINT_CACHE_LOCK:
        _ENTRY_POINT_CACHE.clear()


def register_engine_descriptor(descriptor: type[EngineDescriptor]) -> None:
    get_current_discovery_registry().register_engine_descriptor(descriptor)


def register_definition_query_provider(
    provider: type[DefinitionKnowledgeQueryProvider],
) -> None:
    get_current_discovery_registry().register_definition_query_provider(
        provider,
    )


def register_hook_descriptor(descriptor: type[HookDescriptor]) -> None:
    get_current_discovery_registry().register_hook_descriptor(descriptor)


def register_plugin_type(
    plugin_name: str,
    plugin_type: type[Plugin],
) -> None:
    get_current_discovery_registry().register_plugin_type(
        plugin_name,
        plugin_type,
    )


def register_instrumentation_type(
    instrumentation_name: str,
    instrumentation_type: type[InstrumentationComponent],
) -> None:
    get_current_discovery_registry().register_instrumentation_type(
        instrumentation_name,
        instrumentation_type,
    )


def register_shell_lsp_contribution(
    contribution: type[ShellContribution],
) -> None:
    get_current_discovery_registry().register_shell_lsp_contribution(
        contribution,
    )


def register_shell_cli_contribution(
    contribution: type[ShellContribution],
) -> None:
    get_current_discovery_registry().register_shell_cli_contribution(
        contribution,
    )


def register_shell_reporting_contribution(
    contribution: type[ShellContribution],
) -> None:
    get_current_discovery_registry().register_shell_reporting_contribution(
        contribution,
    )


def register_console_presenter_contribution(
    contribution: type[ConsolePresenterContribution],
) -> None:
    get_current_discovery_registry().register_console_presenter_contribution(
        contribution,
    )


def iter_engine_descriptors() -> tuple[type[EngineDescriptor], ...]:
    return get_current_discovery_registry().iter_engine_descriptors()


def get_engine_descriptor(
    engine_type: str,
) -> type[EngineDescriptor] | None:
    return get_current_discovery_registry().get_engine_descriptor(engine_type)


def get_definition_query_provider(
    engine_name: str,
) -> type[DefinitionKnowledgeQueryProvider] | None:
    return get_current_discovery_registry().get_definition_query_provider(
        engine_name,
    )


def iter_hook_descriptors() -> tuple[type[HookDescriptor], ...]:
    return get_current_discovery_registry().iter_hook_descriptors()


def get_hook_descriptor(
    hook_type: str,
) -> type[HookDescriptor] | None:
    return get_current_discovery_registry().get_hook_descriptor(hook_type)


def iter_plugin_types() -> tuple[type[Plugin], ...]:
    return get_current_discovery_registry().iter_plugin_types()


def iter_instrumentation_types(
) -> tuple[type[InstrumentationComponent], ...]:
    return get_current_discovery_registry().iter_instrumentation_types()


def iter_shell_lsp_contributions() -> tuple[type[ShellContribution], ...]:
    return get_current_discovery_registry().iter_shell_lsp_contributions()


def iter_shell_cli_contributions() -> tuple[type[ShellContribution], ...]:
    return get_current_discovery_registry().iter_shell_cli_contributions()


def iter_shell_reporting_contributions(
) -> tuple[type[ShellContribution], ...]:
    return (
        get_current_discovery_registry()
        .iter_shell_reporting_contributions()
    )


def iter_console_presenter_contributions(
) -> tuple[type[ConsolePresenterContribution], ...]:
    return (
        get_current_discovery_registry()
        .iter_console_presenter_contributions()
    )


def _iter_group_entry_points(group: str) -> tuple[EntryPoint, ...]:
    with _ENTRY_POINT_CACHE_LOCK:
        cached = _ENTRY_POINT_CACHE.get(group)
        if cached is not None:
            return cached

    discovered = tuple(entry_points(group=group))
    with _ENTRY_POINT_CACHE_LOCK:
        return _ENTRY_POINT_CACHE.setdefault(group, discovered)
