from __future__ import annotations

import importlib
import sys

from contextlib import suppress
from pathlib import Path

from cosecha.core.console_rendering import (
    LineComponent,
    StatusBadge,
    TextSpan,
)
from cosecha.core.cosecha_manifest import ManifestValidationError
from cosecha.core.discovery import (
    register_console_presenter_contribution,
    register_definition_query_provider,
    register_engine_descriptor,
    register_shell_lsp_contribution,
)
from cosecha.core.items import TestResultStatus
from cosecha.engine.gherkin.definition_knowledge import matching_descriptors
from cosecha.engine.gherkin.engine import GherkinEngine
from cosecha.engine.gherkin.hooks import (
    GherkinLibraryHook,
    GherkinRegistryLoader,
)
from cosecha.engine.gherkin.reporting import (
    feature_location_text,
    gherkin_feature_name,
    gherkin_scenario_name,
    gherkin_step_result_reports,
)
from cosecha.engine.gherkin.utils import (
    get_step_definitions_from_module,
    import_step_modules,
)


class GherkinEngineDescriptor:
    engine_type = 'gherkin'

    @classmethod
    def validate_engine_spec(
        cls,
        engine_spec,
        *,
        manifest,
    ) -> None:
        del cls
        if engine_spec.registry_loaders or not engine_spec.step_library_modules:
            return

        modules_with_layout_steps = _discover_layout_step_modules(
            engine_spec.step_library_modules,
            root_path=manifest.manifest_dir,
        )
        if not modules_with_layout_steps:
            return

        modules_text = ', '.join(modules_with_layout_steps)
        msg = (
            f'Gherkin engine {engine_spec.id!r} defines steps with '
            f'layouts in step_library_modules ({modules_text}) but '
            'registry_layouts = []. Define at least one '
            '[[engines.registry_loaders]] entry with layouts.'
        )
        raise ManifestValidationError(msg)

    @classmethod
    def validate_resource_binding(
        cls,
        binding,
        *,
        manifest,
    ) -> None:
        del manifest
        if binding.layout is None or binding.alias is None:
            msg = (
                'Gherkin resource bindings require layout and alias for '
                f'{binding.resource_name!r}'
            )
            raise ManifestValidationError(msg)

    @classmethod
    def materialize(
        cls,
        engine_spec,
        *,
        manifest,
        config,
        active_profiles,
        shared_requirements,
    ):
        del active_profiles
        coercions = {
            name: spec.resolve(root_path=manifest.manifest_dir)
            for name, spec in engine_spec.coercions
        }
        registry_loaders = tuple(
            GherkinRegistryLoader(layouts=loader.layouts)
            for loader in engine_spec.registry_loaders
        )
        resource_bindings = tuple(
            binding
            for binding in manifest.resource_bindings
            if binding.engine_type == cls.engine_type
        )
        definition_paths = tuple(
            (
                config.root_path / definition_path
                if not Path(definition_path).is_absolute()
                else Path(definition_path)
            ).resolve()
            for definition_path in engine_spec.definition_paths
        )
        return GherkinEngine(
            name=engine_spec.name,
            hooks=(
                GherkinLibraryHook(
                    step_library_modules=engine_spec.step_library_modules,
                    registry_loaders=registry_loaders,
                ),
            ),
            coercions=coercions,
            shared_resource_requirements=shared_requirements,
            resource_bindings=resource_bindings,
            definition_paths=definition_paths,
        )


class GherkinLspContribution:
    contribution_name = 'gherkin'

    @classmethod
    def templates(cls) -> tuple[dict[str, object], ...]:
        return _import_gherkin_lsp().GHERKIN_TEMPLATES

    @classmethod
    def build_step_completion_suggestions_from_knowledge(
        cls,
        *,
        definitions,
        step_type,
        initial_text,
    ):
        completion = _import_gherkin_completion()
        return completion.build_step_completion_suggestions_from_knowledge(
            definitions=definitions,
            step_type=step_type,
            initial_text=initial_text,
        )

    @classmethod
    def generate_data_table(cls, rows: int, columns: int) -> str:
        completion = _import_gherkin_completion()
        return completion.generate_gherkin_data_table(rows, columns)


class GherkinDefinitionKnowledgeQueryProvider:
    engine_name = 'gherkin'

    @classmethod
    def matching_descriptors(
        cls,
        descriptors,
        *,
        step_type=None,
        step_text=None,
    ):
        del cls
        return matching_descriptors(
            descriptors,
            step_type=step_type,
            step_text=step_text,
        )


class GherkinConsolePresenter:
    contribution_name = 'gherkin'

    @classmethod
    def build_case_title(
        cls,
        report,
        *,
        config,
    ):
        del cls, config
        scenario_name = gherkin_scenario_name(report) or 'unknown'
        spans = [
            TextSpan('Scenario: ', tone='accent', emphatic=True),
            TextSpan(scenario_name),
        ]
        if report.message:
            spans.append(
                TextSpan(
                    f' [message={report.message}]',
                    tone='muted',
                ),
            )
        if report.failure_kind:
            spans.append(
                TextSpan(
                    f' [failure_kind={report.failure_kind}]',
                    tone='muted',
                ),
            )
        return tuple(spans)

    @classmethod
    def build_console_components(
        cls,
        report,
        *,
        config,
    ):
        del cls, config
        components: list[LineComponent] = []
        feature_name = gherkin_feature_name(report)
        if feature_name:
            components.append(
                LineComponent(
                    spans=(
                        TextSpan('Feature: ', tone='accent', emphatic=True),
                        TextSpan(feature_name),
                    ),
                ),
            )
        if report.failure_kind:
            components.append(
                LineComponent(
                    spans=(
                        TextSpan('Failure kind: ', tone='muted'),
                        TextSpan(report.failure_kind, tone='error'),
                    ),
                ),
            )
        for step_result in gherkin_step_result_reports(report):
            badge_tone = _status_to_tone(step_result.status)
            components.append(
                LineComponent(
                    badge=StatusBadge(
                        step_result.status.value,
                        tone=badge_tone,
                    ),
                    spans=(
                        TextSpan(
                            f'{step_result.step.keyword.title()} ',
                            tone='accent',
                            emphatic=True,
                        ),
                        TextSpan(step_result.step.text),
                    ),
                ),
            )
            if step_result.message:
                components.append(
                    LineComponent(
                        spans=(TextSpan(step_result.message, tone='muted'),),
                        indent=2,
                    ),
            )
        return tuple(components)

    @classmethod
    def accumulate_engine_summary(
        cls,
        report,
        *,
        detail_counts,
        feature_keys,
        failed_feature_keys,
    ) -> None:
        del cls
        feature_location = feature_location_text(report)
        if feature_location is not None:
            feature_keys.add(feature_location)
        if report.status in {
            TestResultStatus.FAILED,
            TestResultStatus.ERROR,
        } and feature_location is not None:
            failed_feature_keys.add(feature_location)
        detail_counts['scenarios.total'] = (
            detail_counts.get('scenarios.total', 0) + 1
        )
        detail_counts[f'scenarios.{report.status.value}'] = (
            detail_counts.get(f'scenarios.{report.status.value}', 0) + 1
        )
        for step_result in gherkin_step_result_reports(report):
            detail_counts['steps.total'] = (
                detail_counts.get('steps.total', 0) + 1
            )
            detail_counts[f'steps.{step_result.status.value}'] = (
                detail_counts.get(f'steps.{step_result.status.value}', 0)
                + 1
            )


def _discover_layout_step_modules(
    step_library_modules: tuple[str, ...],
    *,
    root_path: Path,
) -> tuple[str, ...]:
    modules_with_layout_steps: set[str] = set()
    resolved_root = str(root_path.resolve())
    path_inserted = False
    if resolved_root not in sys.path:
        sys.path.insert(0, resolved_root)
        path_inserted = True
    try:
        for module_spec in step_library_modules:
            try:
                modules = import_step_modules(module_spec)
            except Exception as exc:  # pragma: no cover
                msg = (
                    'Unable to inspect gherkin step_library_module '
                    f'{module_spec!r}: {exc}'
                )
                raise ManifestValidationError(msg) from exc

            for module in modules:
                if _module_uses_layout_steps(module):
                    modules_with_layout_steps.add(module.__name__)
    finally:
        if path_inserted:
            if sys.path and sys.path[0] == resolved_root:
                sys.path.pop(0)
            else:
                with suppress(ValueError):
                    sys.path.remove(resolved_root)

    return tuple(sorted(modules_with_layout_steps))


def _module_uses_layout_steps(module) -> bool:
    for step_definition in get_step_definitions_from_module(module):
        for step_text in step_definition.step_text_list:
            if step_text.layouts:
                return True
    return False


def _import_gherkin_completion():
    return importlib.import_module('cosecha.engine.gherkin.completion')


def _import_gherkin_lsp():
    return importlib.import_module('cosecha.engine.gherkin.lsp')


def _status_to_tone(status: TestResultStatus) -> str:
    if status == TestResultStatus.PASSED:
        return 'success'
    if status == TestResultStatus.SKIPPED:
        return 'warning'
    if status in {TestResultStatus.FAILED, TestResultStatus.ERROR}:
        return 'error'
    return 'muted'


register_engine_descriptor(GherkinEngineDescriptor)
register_console_presenter_contribution(GherkinConsolePresenter)
register_definition_query_provider(GherkinDefinitionKnowledgeQueryProvider)
register_shell_lsp_contribution(GherkinLspContribution)
