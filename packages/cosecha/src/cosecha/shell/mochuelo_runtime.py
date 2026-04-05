from __future__ import annotations

from pathlib import Path

from cosecha.core.cosecha_manifest import (
    HookSpec,
    PythonHookDescriptor,
    SymbolRef,
)
from cosecha.core.discovery import register_hook_descriptor


class MochueloRuntimeServiceHookDescriptor(PythonHookDescriptor):
    hook_type = 'mochuelo_runtime_service'

    @classmethod
    def build_runtime_profile_hook_specs(
        cls,
        profile,
        *,
        engine_ids,
    ):
        resolver = SymbolRef.parse(
            'mochuelo_testing.cosecha.services:'
            'resolve_runtime_service_interfaces',
        ).resolve(root_path=Path.cwd())
        if not callable(resolver):
            return ()
        resolved_interfaces = resolver(profile)
        if not isinstance(resolved_interfaces, tuple):
            resolved_interfaces = tuple(resolved_interfaces)
        return tuple(
            HookSpec(
                id=f'runtime_profile:{profile.id}:{interface_name}',
                type=cls.hook_type,
                engine_ids=engine_ids,
                config={
                    'interface': str(interface_name),
                    'profile': profile.to_dict(),
                },
            )
            for interface_name in resolved_interfaces
        )

    @classmethod
    def materialize(
        cls,
        spec,
        *,
        manifest_dir,
    ):
        descriptor = SymbolRef.parse(
            'mochuelo_testing.cosecha.runtime_profile:'
            'MochueloRuntimeServiceHookDescriptor',
        ).resolve(root_path=manifest_dir)
        if not isinstance(descriptor, type):
            msg = 'Mochuelo runtime service hook descriptor is not a class'
            raise TypeError(msg)
        return descriptor.materialize(spec, manifest_dir=manifest_dir)


register_hook_descriptor(MochueloRuntimeServiceHookDescriptor)
