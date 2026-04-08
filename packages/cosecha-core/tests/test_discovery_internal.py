from __future__ import annotations

from cosecha.core import discovery


class _EngineDescriptor:
    engine_type = 'internal-engine'


class _HookDescriptor:
    hook_type = 'internal-hook'


class _DefinitionProvider:
    engine_name = 'internal-engine'


class _PluginType:
    pass


class _InstrumentationType:
    pass


class _ShellLspContribution:
    contribution_name = 'lsp'


class _ShellCliContribution:
    contribution_name = 'cli'


class _ShellReportingContribution:
    contribution_name = 'reporting'


class _ConsolePresenterContribution:
    contribution_name = 'presenter'


class _BrokenEntryPoint:
    name = 'broken'

    def load(self):
        raise RuntimeError('boom')


def test_discovery_registry_wrapper_functions_register_and_iterate() -> None:
    registry = discovery.create_discovery_registry()
    registry._loaded = True

    with discovery.using_discovery_registry(registry):
        discovery.register_engine_descriptor(_EngineDescriptor)
        discovery.register_definition_query_provider(_DefinitionProvider)
        discovery.register_hook_descriptor(_HookDescriptor)
        discovery.register_plugin_type('plugin', _PluginType)
        discovery.register_instrumentation_type('instrumentation', _InstrumentationType)
        discovery.register_shell_lsp_contribution(_ShellLspContribution)
        discovery.register_shell_cli_contribution(_ShellCliContribution)
        discovery.register_shell_reporting_contribution(
            _ShellReportingContribution,
        )
        discovery.register_console_presenter_contribution(
            _ConsolePresenterContribution,
        )

        assert discovery.get_engine_descriptor('internal-engine') is _EngineDescriptor
        assert discovery.get_definition_query_provider(
            'internal-engine',
        ) is _DefinitionProvider
        assert discovery.get_hook_descriptor('internal-hook') is _HookDescriptor
        assert discovery.iter_hook_descriptors() == (_HookDescriptor,)
        assert discovery.iter_shell_lsp_contributions() == (
            _ShellLspContribution,
        )
        assert discovery.iter_shell_cli_contributions() == (
            _ShellCliContribution,
        )
        assert discovery.iter_shell_reporting_contributions() == (
            _ShellReportingContribution,
        )
        assert discovery.iter_console_presenter_contributions() == (
            _ConsolePresenterContribution,
        )

    assert registry.iter_engine_descriptors() == (_EngineDescriptor,)
    assert registry.iter_hook_descriptors() == (_HookDescriptor,)
    assert registry.iter_shell_lsp_contributions() == (_ShellLspContribution,)
    assert registry.iter_shell_cli_contributions() == (_ShellCliContribution,)


def test_discovery_registry_load_errors_for_plugin_and_instrumentation(
    monkeypatch,
) -> None:
    registry = discovery.create_discovery_registry()

    monkeypatch.setattr(
        discovery,
        '_iter_group_entry_points',
        lambda group: (_BrokenEntryPoint(),)
        if group in {
            discovery.PLUGIN_ENTRYPOINT_GROUP,
            discovery.INSTRUMENTATION_ENTRYPOINT_GROUP,
        }
        else (),
    )

    try:
        registry._load_plugin_group()
    except discovery.DiscoveryLoadError as error:
        assert error.group == discovery.PLUGIN_ENTRYPOINT_GROUP
        assert error.entry_point_name == 'broken'
    else:
        raise AssertionError('expected DiscoveryLoadError for plugin group')

    try:
        registry._load_instrumentation_group()
    except discovery.DiscoveryLoadError as error:
        assert error.group == discovery.INSTRUMENTATION_ENTRYPOINT_GROUP
        assert error.entry_point_name == 'broken'
    else:
        raise AssertionError(
            'expected DiscoveryLoadError for instrumentation group',
        )


def test_discovery_registry_ensure_loaded_short_circuits_inside_lock() -> None:
    registry = discovery.create_discovery_registry()

    class _FlipLoadedLock:
        def __enter__(self):
            registry._loaded = True

        def __exit__(self, exc_type, exc, tb) -> bool:
            del exc_type, exc, tb
            return False

    registry._lock = _FlipLoadedLock()
    registry.ensure_loaded()

    assert registry._loaded is True
