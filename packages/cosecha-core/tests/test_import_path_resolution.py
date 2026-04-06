from __future__ import annotations

import sys

from cosecha.core.utils import import_module_from_path


REFERENCE_OTHER_MINOR = 13


def test_import_module_from_path_adds_workspace_src_paths_for_test_files(
    cosecha_workspace,
) -> None:
    step_path = (
        cosecha_workspace.project_path
        / 'academo-app'
        / 'tests'
        / 'unit'
        / 'commands'
        / 'steps'
        / 'sample_step.py'
    )
    cosecha_workspace.write_project_file(
        step_path.relative_to(cosecha_workspace.project_path),
        '\n'.join(
            (
                'from mochuelo.models.instances import ModelInstance',
                'from mochuelo_testing.sample import VALUE',
                '',
                'RESULT = (ModelInstance.__name__, VALUE)',
            ),
        ),
    )
    cosecha_workspace.write_project_file(
        'mochuelo-framework/src/mochuelo/models/instances.py',
        'class ModelInstance:\n    pass\n',
    )
    cosecha_workspace.write_project_file(
        'mochuelo-framework/src/mochuelo/__init__.py',
        '',
    )
    cosecha_workspace.write_project_file(
        'mochuelo-framework/src/mochuelo/models/__init__.py',
        '',
    )
    cosecha_workspace.write_project_file(
        'mochuelo-testing/src/mochuelo_testing/sample.py',
        "VALUE = 'ok'\n",
    )
    cosecha_workspace.write_project_file(
        'mochuelo-testing/src/mochuelo_testing/__init__.py',
        '',
    )

    module = import_module_from_path(step_path)

    assert module.RESULT == ('ModelInstance', 'ok')


def test_import_module_from_path_adds_workspace_site_packages(
    cosecha_workspace,
) -> None:
    step_path = (
        cosecha_workspace.project_path
        / 'academo-app'
        / 'tests'
        / 'unit'
        / 'commands'
        / 'steps'
        / 'sample_step.py'
    )
    cosecha_workspace.write_project_file(
        step_path.relative_to(cosecha_workspace.project_path),
        '\n'.join(
            (
                'from external_dependency import VALUE',
                '',
                'RESULT = VALUE',
            ),
        ),
    )
    cosecha_workspace.write_site_package(
        'external_dependency.py',
        "VALUE = 'ok'\n",
    )

    module = import_module_from_path(step_path)

    assert module.RESULT == 'ok'


def test_import_module_from_path_ignores_other_python_site_packages(
    cosecha_workspace,
) -> None:
    other_minor = (
        REFERENCE_OTHER_MINOR
        if sys.version_info.minor != REFERENCE_OTHER_MINOR
        else 12
    )
    step_path = (
        cosecha_workspace.project_path
        / 'academo-app'
        / 'tests'
        / 'unit'
        / 'commands'
        / 'steps'
        / 'sample_step.py'
    )
    cosecha_workspace.write_project_file(
        step_path.relative_to(cosecha_workspace.project_path),
        '\n'.join(
            (
                'from external_dependency_current import VALUE',
                '',
                'RESULT = VALUE',
            ),
        ),
    )
    cosecha_workspace.write_site_package(
        'external_dependency_current.py',
        "VALUE = 'current'\n",
    )
    cosecha_workspace.write_project_file(
        (
            f'venv3.{other_minor}/lib/python3.{other_minor}/site-packages/'
            'external_dependency_current.py'
        ),
        "VALUE = 'other'\n",
    )

    module = import_module_from_path(step_path)

    assert module.RESULT == 'current'
