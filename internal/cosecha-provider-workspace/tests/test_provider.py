from __future__ import annotations

import sys

from cosecha.core.resources import ResourceRequirement
from cosecha_internal.provider.workspace import (
    CosechaWorkspaceBuilder,
    CosechaWorkspaceProvider,
)


def test_builder_creates_workspace_layout_and_seed_files(
    tmp_path,
) -> None:
    python_version = f'{sys.version_info.major}.{sys.version_info.minor}'
    workspace = CosechaWorkspaceBuilder(
        tmp_path,
        project_name='workspace',
        layout='tests-root',
        with_manifest=True,
        manifest_text='[manifest]\nschema_version = 1\n',
        with_knowledge_base=True,
        python_version=python_version,
        venv_name=f'venv{python_version}',
        project_files={
            'demo/tests/unit/sample_test.py': 'def test_demo():\n    pass\n',
        },
        root_files={'features/example.feature': 'Feature: Demo\n'},
        site_packages={'external_dependency.py': "VALUE = 'ok'\n"},
        python_executables={'python': '#!/usr/bin/env python\n'},
        sibling_files={
            'shared-lib/src/shared_lib/sample.py': "VALUE = 'ok'\n",
        },
    ).build()

    assert workspace.project_path == tmp_path / 'workspace'
    assert workspace.root_path == workspace.project_path / 'tests'
    assert workspace.manifest_path == workspace.root_path / 'cosecha.toml'
    assert workspace.manifest_path.exists()
    assert workspace.knowledge_base_path.exists()
    assert workspace.site_packages_path == (
        workspace.project_path
        / f'venv{python_version}'
        / 'lib'
        / f'python{python_version}'
        / 'site-packages'
    )
    assert (
        workspace.project_path / 'demo' / 'tests' / 'unit' / 'sample_test.py'
    ).exists()
    assert (workspace.root_path / 'features' / 'example.feature').exists()
    assert (workspace.site_packages_path / 'external_dependency.py').exists()
    assert workspace.python_executable_path.exists()
    assert (
        workspace.project_path.parent
        / 'shared-lib'
        / 'src'
        / 'shared_lib'
        / 'sample.py'
    ).exists()


def test_provider_creates_and_releases_owned_workspace() -> None:
    provider = CosechaWorkspaceProvider()
    requirement = ResourceRequirement(
        name='workspace',
        provider=provider,
        scope='test',
        mode='ephemeral',
        config={
            'with_manifest': True,
            'with_knowledge_base': True,
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')

    assert provider.health_check(resource, requirement, mode='ephemeral')
    assert provider.verify_integrity(resource, requirement, mode='ephemeral')
    assert resource.owned_path is not None
    assert resource.owned_path.exists()

    provider.release(resource, requirement, mode='ephemeral')

    assert resource.owned_path is not None
    assert not resource.owned_path.exists()
