from __future__ import annotations

from typing import TYPE_CHECKING

from cosecha.core.location import Location
from cosecha.core.runtime_profiles import (
    RuntimeModeDisallowance,
    RuntimeModeRequirement,
)
from cosecha.engine.gherkin.items import GherkinTestItem
from cosecha.engine.gherkin.models import Feature, Scenario, Tag


if TYPE_CHECKING:
    from pathlib import Path


def test_gherkin_runtime_requirements_support_requires_mode(
    tmp_path: Path,
) -> None:
    feature_path = tmp_path / 'demo.feature'
    location = Location(feature_path, 1)
    feature = Feature(
        location=location,
        language='es',
        keyword='Caracteristica',
        name='Demo',
        description='',
        tags=(
            Tag(
                id='feature-tag',
                location=location,
                name='@requires_mode:application/http:asgi',
            ),
        ),
    )
    scenario = Scenario(
        id='scenario-1',
        location=location,
        keyword='Escenario',
        name='Demo',
        description='',
        tags=(
            Tag(
                id='scenario-tag',
                location=location,
                name='@disallow_mode:application/http:wsgi',
            ),
        ),
    )

    item = GherkinTestItem(feature, scenario, None, feature_path)
    requirements = item.get_runtime_requirement_set()

    assert requirements.required_modes == (
        RuntimeModeRequirement(
            interface_name='application/http',
            mode_name='asgi',
        ),
    )
    assert requirements.disallowed_modes == (
        RuntimeModeDisallowance(
            interface_name='application/http',
            mode_name='wsgi',
        ),
    )
