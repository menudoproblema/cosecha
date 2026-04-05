from __future__ import annotations

import sys

from typing import TYPE_CHECKING

from cosecha.core.utils import import_module_from_path


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path


REFERENCE_OTHER_MINOR = 13


def test_import_module_from_path_adds_workspace_src_paths_for_test_files(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    app_root = workspace_root / 'academo-app'
    support_root = workspace_root / 'mochuelo-framework'
    testing_root = workspace_root / 'mochuelo-testing'

    step_path = (
        app_root
        / 'tests'
        / 'unit'
        / 'commands'
        / 'steps'
        / 'sample_step.py'
    )
    step_path.parent.mkdir(parents=True)
    step_path.write_text(
        '\n'.join(
            (
                'from mochuelo.models.instances import ModelInstance',
                'from mochuelo_testing.sample import VALUE',
                '',
                'RESULT = (ModelInstance.__name__, VALUE)',
            ),
        ),
        encoding='utf-8',
    )

    support_module_path = (
        support_root / 'src' / 'mochuelo' / 'models' / 'instances.py'
    )
    support_module_path.parent.mkdir(parents=True)
    support_module_path.write_text(
        'class ModelInstance:\n    pass\n',
        encoding='utf-8',
    )

    testing_module_path = (
        testing_root / 'src' / 'mochuelo_testing' / 'sample.py'
    )
    testing_module_path.parent.mkdir(parents=True)
    testing_module_path.write_text(
        "VALUE = 'ok'\n",
        encoding='utf-8',
    )

    module = import_module_from_path(step_path)

    assert module.RESULT == ('ModelInstance', 'ok')


def test_import_module_from_path_adds_workspace_site_packages(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    app_root = workspace_root / 'academo-app'
    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    site_packages = (
        workspace_root
        / (
            'venv'
            f'{sys.version_info.major}.{sys.version_info.minor}'
            f'/lib/{version_name}/site-packages'
        )
    )

    step_path = (
        app_root
        / 'tests'
        / 'unit'
        / 'commands'
        / 'steps'
        / 'sample_step.py'
    )
    step_path.parent.mkdir(parents=True)
    step_path.write_text(
        '\n'.join(
            (
                'from external_dependency import VALUE',
                '',
                'RESULT = VALUE',
            ),
        ),
        encoding='utf-8',
    )

    site_packages.mkdir(parents=True)
    (site_packages / 'external_dependency.py').write_text(
        "VALUE = 'ok'\n",
        encoding='utf-8',
    )

    module = import_module_from_path(step_path)

    assert module.RESULT == 'ok'


def test_import_module_from_path_ignores_other_python_site_packages(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / 'workspace'
    app_root = workspace_root / 'academo-app'
    version_name = f'python{sys.version_info.major}.{sys.version_info.minor}'
    current_site_packages = (
        workspace_root
        / (
            'venv'
            f'{sys.version_info.major}.{sys.version_info.minor}'
            f'/lib/{version_name}/site-packages'
        )
    )

    other_minor = (
        REFERENCE_OTHER_MINOR
        if sys.version_info.minor != REFERENCE_OTHER_MINOR
        else 12
    )
    other_site_packages = (
        workspace_root
        / f'venv3.{other_minor}/lib/python3.{other_minor}/site-packages'
    )

    step_path = (
        app_root
        / 'tests'
        / 'unit'
        / 'commands'
        / 'steps'
        / 'sample_step.py'
    )
    step_path.parent.mkdir(parents=True)
    step_path.write_text(
        '\n'.join(
            (
                'from external_dependency_current import VALUE',
                '',
                'RESULT = VALUE',
            ),
        ),
        encoding='utf-8',
    )

    current_site_packages.mkdir(parents=True)
    (current_site_packages / 'external_dependency_current.py').write_text(
        "VALUE = 'current'\n",
        encoding='utf-8',
    )

    other_site_packages.mkdir(parents=True)
    (other_site_packages / 'external_dependency_current.py').write_text(
        "VALUE = 'other'\n",
        encoding='utf-8',
    )

    module = import_module_from_path(step_path)

    assert module.RESULT == 'current'
