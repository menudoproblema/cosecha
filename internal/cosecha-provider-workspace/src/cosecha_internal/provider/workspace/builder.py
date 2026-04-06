from __future__ import annotations

import shutil
import sys

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from cosecha.core.knowledge_base import resolve_knowledge_base_path


type WorkspaceLayout = Literal['tests-root', 'root']
type WorkspaceCleanupPolicy = Literal['auto', 'preserve']


def _default_python_version() -> str:
    return f'{sys.version_info.major}.{sys.version_info.minor}'


@dataclass(slots=True, frozen=True)
class CosechaWorkspaceConfig:
    layout: WorkspaceLayout = 'tests-root'
    project_name: str | None = 'project'
    with_manifest: bool = False
    manifest_text: str = ''
    with_knowledge_base: bool = False
    python_version: str = field(default_factory=_default_python_version)
    venv_name: str = '.venv'
    cleanup_policy: WorkspaceCleanupPolicy = 'auto'
    project_files: dict[str, str] = field(default_factory=dict)
    root_files: dict[str, str] = field(default_factory=dict)
    site_packages: dict[str, str] = field(default_factory=dict)
    python_executables: dict[str, str] = field(default_factory=dict)
    sibling_files: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_requirement(cls, requirement) -> CosechaWorkspaceConfig:
        config = requirement.config
        return cls(
            layout=_normalize_layout(
                _read_optional_str(config.get('layout')) or 'tests-root',
            ),
            project_name=_read_project_name(config.get('project_name')),
            with_manifest=_read_bool(
                config.get('with_manifest'),
                default=False,
            ),
            manifest_text=(
                _read_optional_str(config.get('manifest_text')) or ''
            ),
            with_knowledge_base=_read_bool(
                config.get('with_knowledge_base'),
                default=False,
            ),
            python_version=_read_non_empty_str(
                config.get('python_version'),
                field_name='python_version',
                default=_default_python_version(),
            ),
            venv_name=_read_non_empty_str(
                config.get('venv_name'),
                field_name='venv_name',
                default='.venv',
            ),
            cleanup_policy=_normalize_cleanup_policy(
                _read_optional_str(config.get('cleanup_policy')) or 'auto',
            ),
            project_files=_read_text_mapping(
                config.get('project_files'),
                field_name='project_files',
            ),
            root_files=_read_text_mapping(
                config.get('root_files'),
                field_name='root_files',
            ),
            site_packages=_read_text_mapping(
                config.get('site_packages'),
                field_name='site_packages',
            ),
            python_executables=_read_text_mapping(
                config.get('python_executables'),
                field_name='python_executables',
            ),
            sibling_files=_read_text_mapping(
                config.get('sibling_files'),
                field_name='sibling_files',
            ),
        )


@dataclass(slots=True)
class CosechaWorkspaceHandle:
    project_path: Path
    root_path: Path
    manifest_path: Path | None
    knowledge_base_path: Path
    site_packages_path: Path
    python_executable_path: Path
    cleanup_policy: WorkspaceCleanupPolicy
    owned_path: Path | None = None

    def write_project_file(
        self,
        relative_path: str | Path,
        content: str,
    ) -> Path:
        return _write_text_file(self.project_path / relative_path, content)

    def write_root_file(
        self,
        relative_path: str | Path,
        content: str,
    ) -> Path:
        return _write_text_file(self.root_path / relative_path, content)

    def write_site_package(
        self,
        relative_path: str | Path,
        content: str,
    ) -> Path:
        return _write_text_file(
            self.site_packages_path / relative_path,
            content,
        )

    def write_sibling_file(
        self,
        relative_path: str | Path,
        content: str,
    ) -> Path:
        return _write_text_file(
            self.project_path.parent / relative_path,
            content,
        )

    def write_python_executable(
        self,
        executable_name: str,
        content: str = '',
    ) -> Path:
        executable_path = self.python_executable_path.parent / executable_name
        executable_path = _write_text_file(executable_path, content)
        executable_path.chmod(0o755)
        return executable_path

    def cleanup(self) -> None:
        if self.cleanup_policy != 'auto':
            return
        if self.owned_path is None or not self.owned_path.exists():
            return
        shutil.rmtree(self.owned_path)


class CosechaWorkspaceBuilder:
    __slots__ = ('base_path', 'config')

    def __init__(
        self,
        base_path: Path,
        *,
        config: CosechaWorkspaceConfig | None = None,
        **config_kwargs,
    ) -> None:
        self.base_path = Path(base_path)
        self.config = (
            config
            if config is not None
            else CosechaWorkspaceConfig(**config_kwargs)
        )

    def build(
        self,
        *,
        owned_path: Path | None = None,
    ) -> CosechaWorkspaceHandle:
        project_path = self.base_path
        if self.config.project_name is not None:
            project_path = project_path / self.config.project_name
        root_path = (
            project_path / 'tests'
            if self.config.layout == 'tests-root'
            else project_path
        )
        root_path.mkdir(parents=True, exist_ok=True)

        manifest_path: Path | None = None
        if self.config.with_manifest:
            manifest_path = _write_text_file(
                root_path / 'cosecha.toml',
                self.config.manifest_text,
            )

        knowledge_base_path = resolve_knowledge_base_path(root_path)
        if self.config.with_knowledge_base:
            _write_text_file(knowledge_base_path, '')

        site_packages_path = (
            project_path
            / self.config.venv_name
            / 'lib'
            / f'python{self.config.python_version}'
            / 'site-packages'
        )
        python_executable_path = (
            project_path
            / self.config.venv_name
            / 'bin'
            / 'python'
        )
        handle = CosechaWorkspaceHandle(
            project_path=project_path,
            root_path=root_path,
            manifest_path=manifest_path,
            knowledge_base_path=knowledge_base_path,
            site_packages_path=site_packages_path,
            python_executable_path=python_executable_path,
            cleanup_policy=self.config.cleanup_policy,
            owned_path=owned_path,
        )

        for relative_path, content in self.config.project_files.items():
            handle.write_project_file(relative_path, content)
        for relative_path, content in self.config.root_files.items():
            handle.write_root_file(relative_path, content)
        for relative_path, content in self.config.site_packages.items():
            handle.write_site_package(relative_path, content)
        for executable_name, content in self.config.python_executables.items():
            handle.write_python_executable(executable_name, content)
        for relative_path, content in self.config.sibling_files.items():
            handle.write_sibling_file(relative_path, content)

        return handle


def _write_text_file(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')
    return path


def _normalize_layout(value: str) -> WorkspaceLayout:
    if value in {'tests-root', 'root'}:
        return value
    msg = f'Unsupported workspace layout: {value!r}'
    raise ValueError(msg)


def _normalize_cleanup_policy(value: str) -> WorkspaceCleanupPolicy:
    if value in {'auto', 'preserve'}:
        return value
    msg = f'Unsupported workspace cleanup policy: {value!r}'
    raise ValueError(msg)


def _read_project_name(value: object) -> str | None:
    if value is None:
        return 'project'
    if value == '':
        return None
    return _read_non_empty_str(
        value,
        field_name='project_name',
        default='project',
    )


def _read_non_empty_str(
    value: object,
    *,
    field_name: str,
    default: str,
) -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        msg = f'Workspace field {field_name!r} must be a string'
        raise ValueError(msg)
    if not value.strip():
        msg = f'Workspace field {field_name!r} must not be empty'
        raise ValueError(msg)
    return value


def _read_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        msg = 'Workspace provider only accepts string values'
        raise ValueError(msg)
    return value


def _read_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        msg = 'Workspace provider boolean fields must use true/false'
        raise ValueError(msg)
    return value


def _read_text_mapping(
    value: object,
    *,
    field_name: str,
) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        msg = f'Workspace field {field_name!r} must be a table'
        raise ValueError(msg)
    normalized: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            msg = (
                f'Workspace field {field_name!r} must map string paths '
                'to string contents'
            )
            raise ValueError(msg)
        normalized[key] = item
    return normalized
