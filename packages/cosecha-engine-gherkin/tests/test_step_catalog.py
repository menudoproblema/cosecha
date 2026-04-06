from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING

from cosecha.engine.gherkin.step_ast_discovery import StepDiscoveryService
from cosecha.engine.gherkin.step_catalog import StepCatalog, StepQuery


if TYPE_CHECKING:
    from pathlib import Path


def _write_step_file(
    root_path: Path,
    relative_path: str,
    content: str,
):
    file_path = root_path / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(dedent(content), encoding='utf-8')
    return file_path


def test_step_discovery_service_extracts_literal_step_descriptors(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / 'tests'
    project_path.mkdir()
    (project_path / '__init__.py').write_text('', encoding='utf-8')
    file_path = _write_step_file(
        project_path,
        'payments/steps/auth_steps.py',
        """
        from cosecha.engine.gherkin.steps import given

        @given("the user is authenticated")
        async def user_is_authenticated(context):
            \"\"\"The user is already authenticated.\"\"\"
            del context
        """,
    )

    service = StepDiscoveryService(project_path)
    discovered_file = service.discover_step_file(file_path)

    assert discovered_file.discovery_mode == 'ast'
    assert discovered_file.requires_fallback_import is False
    assert len(discovered_file.descriptors) == 1
    descriptor = discovered_file.descriptors[0]
    assert descriptor.step_type == 'given'
    assert descriptor.patterns == ('the user is authenticated',)
    assert descriptor.literal_prefixes == ('the user is authenticated',)
    assert descriptor.function_name == 'user_is_authenticated'
    assert descriptor.documentation == 'The user is already authenticated.'
    assert descriptor.source_file == file_path.resolve()


def test_step_catalog_returns_candidate_files_by_prefix_and_dynamic_bucket(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / 'tests'
    project_path.mkdir()
    (project_path / '__init__.py').write_text('', encoding='utf-8')
    auth_file = _write_step_file(
        project_path,
        'steps/auth_steps.py',
        """
        from cosecha.engine.gherkin.steps import given

        @given("the user logs in")
        async def login(context):
            del context
        """,
    )
    dynamic_file = _write_step_file(
        project_path,
        'steps/dynamic_steps.py',
        """
        from cosecha.engine.gherkin.steps import step

        @step("{name} performs an action")
        async def any_action(context):
            del context
        """,
    )

    service = StepDiscoveryService(project_path)
    discovered_files = (
        service.discover_step_file(auth_file),
        service.discover_step_file(dynamic_file),
    )
    catalog = StepCatalog()
    catalog.update(discovered_files)

    assert catalog.find_candidate_files(
        'given',
        'the user logs in',
    ) == (
        auth_file.resolve(),
        dynamic_file.resolve(),
    )
    assert catalog.find_candidate_files_for_steps(
        (
            StepQuery('given', 'the user logs in'),
            StepQuery('step', 'alice performs an action'),
        ),
    ) == (
        auth_file.resolve(),
        dynamic_file.resolve(),
    )
