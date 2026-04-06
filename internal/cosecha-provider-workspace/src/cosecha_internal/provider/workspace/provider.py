from __future__ import annotations

import json
import shutil
import tempfile

from pathlib import Path

from cosecha.core.resources import ResourceError
from cosecha_internal.provider.workspace.builder import (
    CosechaWorkspaceBuilder,
    CosechaWorkspaceConfig,
    CosechaWorkspaceHandle,
)


class CosechaWorkspaceProvider:
    def supports_mode(self, mode) -> bool:
        return mode == 'ephemeral'

    def acquire(
        self,
        requirement,
        *,
        mode,
        dependency_context=None,
    ) -> CosechaWorkspaceHandle:
        del dependency_context
        if not self.supports_mode(mode):
            msg = (
                'CosechaWorkspaceProvider only supports '
                "mode='ephemeral'"
            )
            raise ResourceError(
                requirement.name,
                msg,
                code='workspace_mode_unsupported',
                unhealthy=False,
            )

        config = CosechaWorkspaceConfig.from_requirement(requirement)
        owned_path = Path(tempfile.mkdtemp(prefix='cosecha-workspace-'))
        builder = CosechaWorkspaceBuilder(owned_path, config=config)
        return builder.build(owned_path=owned_path)

    def reserve_external_handle(self, requirement, *, mode):
        del requirement, mode

    def discard_reserved_external_handle(
        self,
        external_handle,
        requirement,
        *,
        mode,
    ) -> None:
        del external_handle, requirement, mode

    def release(self, resource, requirement, *, mode) -> None:
        del requirement, mode
        resource.cleanup()

    def health_check(self, resource, requirement, *, mode) -> bool:
        del requirement, mode
        return resource.project_path.exists() and resource.root_path.exists()

    def verify_integrity(self, resource, requirement, *, mode) -> bool:
        return self.health_check(
            resource,
            requirement,
            mode=mode,
        ) and (
            resource.manifest_path is None
            or resource.manifest_path.exists()
        )

    def describe_external_handle(
        self,
        resource,
        requirement,
        *,
        mode,
    ) -> str | None:
        del requirement, mode
        return json.dumps(
            {
                'cleanup_policy': resource.cleanup_policy,
                'knowledge_base_path': str(resource.knowledge_base_path),
                'manifest_path': (
                    None
                    if resource.manifest_path is None
                    else str(resource.manifest_path)
                ),
                'owned_path': (
                    None
                    if resource.owned_path is None
                    else str(resource.owned_path)
                ),
                'project_path': str(resource.project_path),
                'root_path': str(resource.root_path),
            },
            sort_keys=True,
        )

    def reap_orphan(self, external_handle, requirement, *, mode) -> None:
        del requirement, mode
        payload = json.loads(external_handle)
        owned_path = payload.get('owned_path')
        if isinstance(owned_path, str):
            target = Path(owned_path)
        else:
            target = Path(str(payload['project_path']))
        if target.exists():
            shutil.rmtree(target)

    def revoke_orphan_access(
        self,
        external_handle,
        requirement,
        *,
        mode,
    ) -> None:
        del external_handle, requirement, mode
