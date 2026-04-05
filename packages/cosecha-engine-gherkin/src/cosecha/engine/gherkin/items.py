import fnmatch
import itertools
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cosecha.core.items import TestItem, TestResultStatus
from cosecha.core.resources import ResourceRequirement
from cosecha.core.runtime_profiles import (
    RuntimeCapabilityRequirement,
    RuntimeModeDisallowance,
    RuntimeModeRequirement,
    RuntimeRequirementSet,
)
from cosecha.engine.gherkin.context import Context
from cosecha.engine.gherkin.models import Example, Feature, Scenario, Step
from cosecha.engine.gherkin.reporting import build_gherkin_engine_payload
from cosecha.engine.gherkin.steps.definition import Match


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.core.types import ExcInfo


@dataclass(slots=True)
class _StaticFeature:
    name: str
    location: Any
    tags: Any


@dataclass(slots=True)
class _StaticScenario:
    name: str
    location: Any
    tags: Any


@dataclass(slots=True)
class _StaticExample:
    location: Any


@dataclass(slots=True)
class _StaticStep:
    text: str
    keyword: str
    location: Any


class StepResult:
    __slots__ = ('exc_info', 'match', 'message', 'status', 'step')

    def __init__(
        self,
        step: Step,
        match: Match | None,
        status: TestResultStatus,
    ) -> None:
        self.step = step
        self.match = match
        self.status = status
        self.exc_info: ExcInfo | None = None
        self.message: str | None = None

    def clear_model(self) -> None:
        """Libera la referencia al objeto Step del modelo y al Match."""
        # Convertimos el Step real en un objeto ligero con solo los datos
        # necesarios para el reporte final si se vuelve a consultar.
        if hasattr(self.step, 'text'):
            self.step = _StaticStep(
                text=self.step.text,
                keyword=self.step.keyword,
                location=self.step.location,
            )  # type: ignore[assignment]

        self.match = None

    def __repr__(self) -> str:
        return f'<StepResult {self.status}>'


class GherkinTestItem(TestItem):
    engine_name = 'gherkin'
    __slots__ = (
        '_cached_tags',
        '_manifest_resource_requirements',
        'example',
        'feature',
        'name',
        'scenario',
        'step_result_list',
        'test_name',
    )

    def __init__(
        self,
        feature: Feature,
        scenario: Scenario,
        example: Example | None,
        path: Path,
    ) -> None:
        super().__init__(path)
        self.feature = feature
        self.scenario = scenario
        self.example = example
        self.name = f'Scenario: {scenario.name}'
        self.test_name = self.name
        self.step_result_list: list[StepResult] = []
        self._manifest_resource_requirements: tuple[
            ResourceRequirement,
            ...,
        ] = ()
        # Cacheamos los tags para poder liberar Feature/Scenario después.
        self._cached_tags: tuple[str, ...] = tuple(
            tag.name for tag in itertools.chain(feature.tags, scenario.tags)
        )

    async def run_step(
        self,
        step_result: StepResult,
        context: Context,
    ) -> None:
        match = step_result.match
        if not match:
            msg = (
                'run_step() called with an unmatched '
                f'StepResult: "{step_result.step}"'
            )
            raise RuntimeError(msg)
        step_definition = match.step_definition
        try:
            if match.step_text.min_table_rows:
                if (
                    not context.table
                    or len(context.table.rows) < match.step_text.min_table_rows
                ):
                    msg = (
                        'This step requires at least '
                        f'{match.step_text.min_table_rows} rows in the '
                        'data table'
                    )
                    raise ValueError(msg)
            elif match.step_text.required_table_rows:
                if (
                    not context.table
                    or len(context.table.rows)
                    != match.step_text.required_table_rows
                ):
                    msg = (
                        'This step requires exactly '
                        f'{match.step_text.required_table_rows} rows in the '
                        'data table'
                    )
                    raise ValueError(msg)
            elif context.table and not match.step_text.can_use_table:
                msg = 'No data table expected in this step'
                raise ValueError(msg)

            # Establecemos todos los parámetros extra de StepText
            kwargs: dict[str, Any] = {**match.step_text.params}
            # Por cada argumento encontrado, actualizamos su valor
            for argument in match.arguments:
                kwargs[argument.name] = argument.value

            step_fn = step_definition.func

            await step_fn(context, **kwargs)

            step_result.status = TestResultStatus.PASSED

        except AssertionError:
            step_result.status = TestResultStatus.FAILED
            step_result.message = 'Step failed'
            step_result.exc_info = sys.exc_info()

        except Exception:
            step_result.status = TestResultStatus.ERROR
            step_result.message = 'Error executing step'
            step_result.exc_info = sys.exc_info()

    async def run(self, context: Context) -> None:
        all_steps = self.scenario.all_steps
        self.failure_kind = None
        self.error_code = None

        if not all_steps:
            self.status = TestResultStatus.SKIPPED
            self.message = 'No steps to run'
            return

        # Limpiamos resultados previos para esta ejecución específica.
        # Importante en reintentos o paralelismo si el objeto se reusa.
        self.step_result_list = []

        # 1. Pre-resolución de matching para todos los steps.
        # Esto es eficiente porque find_match tiene cache interna.
        # Se hace antes de ejecutar para fallar rápido si falta algo.
        matches: list[Match | None] = [
            context.step_registry.find_match(step.step_type, step.text)
            for step in all_steps
        ]

        # 2. Ejecución secuencial de los steps resueltos.
        blocked = False
        last_step_result: StepResult | None = None

        for step, match in zip(all_steps, matches, strict=True):
            if not match:
                step_result = StepResult(step, None, TestResultStatus.SKIPPED)
                step_result.message = 'Missing step impl'
                self.step_result_list.append(step_result)
                if not blocked:
                    last_step_result = step_result
                    blocked = True
                continue

            step_result = StepResult(step, match, TestResultStatus.PENDING)
            self.step_result_list.append(step_result)

            if blocked:
                continue

            last_step_result = step_result
            context.set_step(self.feature, self.scenario, step_result.step)
            notify_step_started = getattr(
                context,
                'notify_step_started',
                None,
            )
            if notify_step_started is not None:
                await notify_step_started(step_result.step)
            await self.run_step(step_result, context)
            notify_step_finished = getattr(
                context,
                'notify_step_finished',
                None,
            )
            if notify_step_finished is not None:
                await notify_step_finished(
                    step_result.step,
                    status=step_result.status.value,
                    message=step_result.message,
                )

            if step_result.status != TestResultStatus.PASSED:
                blocked = True

        # El resultado del último paso relevante determina el estado del test.
        if last_step_result:
            self.status = last_step_result.status
            self.message = last_step_result.message
            if last_step_result.status == TestResultStatus.FAILED:
                self.failure_kind = 'test'
            elif last_step_result.status == TestResultStatus.ERROR:
                self.failure_kind = 'runtime'
                if last_step_result.exc_info is not None:
                    self.error_code = getattr(
                        last_step_result.exc_info[1],
                        'code',
                        None,
                    )

    def has_selection_label(self, name: str) -> bool:
        """Check if the test has a given selection label."""
        # Usamos la caché de tags para no depender de Feature/Scenario.
        return any(fnmatch.fnmatch(tag, name) for tag in self._cached_tags)

    def get_runtime_requirement_set(self) -> RuntimeRequirementSet:
        interfaces: list[str] = []
        capabilities: list[RuntimeCapabilityRequirement] = []
        required_modes: list[RuntimeModeRequirement] = []
        disallowed_modes: list[RuntimeModeDisallowance] = []
        for tag in self._cached_tags:
            if tag.startswith('@requires_capability:'):
                _, raw_interface, raw_capability = tag.split(':', maxsplit=2)
                capabilities.append(
                    RuntimeCapabilityRequirement(
                        interface_name=raw_interface,
                        capability_name=raw_capability,
                    ),
                )
                continue
            if tag.startswith('@requires_mode:'):
                _, raw_interface, raw_mode = tag.split(':', maxsplit=2)
                required_modes.append(
                    RuntimeModeRequirement(
                        interface_name=raw_interface,
                        mode_name=raw_mode,
                    ),
                )
                continue
            if tag.startswith('@disallow_mode:'):
                _, raw_interface, raw_mode = tag.split(':', maxsplit=2)
                disallowed_modes.append(
                    RuntimeModeDisallowance(
                        interface_name=raw_interface,
                        mode_name=raw_mode,
                    ),
                )
                continue
            if tag.startswith('@requires:'):
                interfaces.append(tag.removeprefix('@requires:'))
                continue

        return RuntimeRequirementSet(
            interfaces=tuple(dict.fromkeys(interfaces)),
            capabilities=tuple(dict.fromkeys(capabilities)),
            required_modes=tuple(dict.fromkeys(required_modes)),
            disallowed_modes=tuple(dict.fromkeys(disallowed_modes)),
        )

    def bind_manifest_resources(
        self,
        requirements: tuple[ResourceRequirement, ...],
    ) -> None:
        self._manifest_resource_requirements = requirements

    def get_resource_requirements(self) -> tuple[ResourceRequirement, ...]:
        return self._manifest_resource_requirements

    def clear_model(self) -> None:
        """Libera las referencias pesadas a Feature, Scenario y Steps."""
        # Convertimos los objetos ricos en stubs ligeros para el reporte.
        if hasattr(self.feature, 'name'):
            self.feature = _StaticFeature(
                name=self.feature.name,
                location=self.feature.location,
                tags=self.feature.tags,
            )  # type: ignore[assignment]

        if hasattr(self.scenario, 'name'):
            self.scenario = _StaticScenario(
                name=self.scenario.name,
                location=self.scenario.location,
                tags=self.scenario.tags,
            )  # type: ignore[assignment]

        if self.example and hasattr(self.example, 'location'):
            self.example = _StaticExample(
                location=self.example.location,
            )  # type: ignore[assignment]

        for step_result in self.step_result_list:
            step_result.clear_model()

    def get_required_step_texts(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (step.step_type, step.text) for step in self.scenario.all_steps
        )

    def build_engine_report_payload(
        self,
        *,
        root_path=None,
    ) -> dict[str, object]:
        return build_gherkin_engine_payload(
            self,
            root_path=root_path,
        )

    def __repr__(self) -> str:
        return f'<GherkinTest {self.feature.location}>'
