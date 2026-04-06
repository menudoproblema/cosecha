from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from cosecha.core.cosecha_manifest import HookSpec
from cosecha.shell.mochuelo_runtime import MochueloRuntimeServiceHookDescriptor


@dataclass
class _ProfileStub:
    id: str

    def to_dict(self) -> dict[str, object]:
        return {'id': self.id, 'kind': 'runtime-profile'}


def test_runtime_profile_hook_specs_materialize_interfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _ProfileStub(id='api')

    class _ResolvedSymbol:
        def resolve(self, *, root_path: Path):
            del root_path
            return lambda current_profile: (
                f'{current_profile.id}.db',
                f'{current_profile.id}.cache',
            )

    monkeypatch.setattr(
        'cosecha.shell.mochuelo_runtime.SymbolRef.parse',
        lambda raw: _ResolvedSymbol(),
    )

    specs = MochueloRuntimeServiceHookDescriptor.build_runtime_profile_hook_specs(
        profile,
        engine_ids=('pytest', 'gherkin'),
    )

    assert specs == (
        HookSpec(
            id='runtime_profile:api:api.db',
            type='mochuelo_runtime_service',
            engine_ids=('pytest', 'gherkin'),
            config={
                'interface': 'api.db',
                'profile': {'id': 'api', 'kind': 'runtime-profile'},
            },
        ),
        HookSpec(
            id='runtime_profile:api:api.cache',
            type='mochuelo_runtime_service',
            engine_ids=('pytest', 'gherkin'),
            config={
                'interface': 'api.cache',
                'profile': {'id': 'api', 'kind': 'runtime-profile'},
            },
        ),
    )


def test_runtime_profile_hook_specs_return_empty_when_symbol_is_not_callable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _ProfileStub(id='api')

    class _ResolvedSymbol:
        def resolve(self, *, root_path: Path):
            del root_path
            return 'not-callable'

    monkeypatch.setattr(
        'cosecha.shell.mochuelo_runtime.SymbolRef.parse',
        lambda raw: _ResolvedSymbol(),
    )

    specs = MochueloRuntimeServiceHookDescriptor.build_runtime_profile_hook_specs(
        profile,
        engine_ids=('pytest',),
    )

    assert specs == ()


def test_materialize_delegates_to_external_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = HookSpec(
        id='runtime_profile:api:api.db',
        type='mochuelo_runtime_service',
        config={'interface': 'api.db'},
    )
    materialized = object()

    class _ExternalDescriptor:
        @classmethod
        def materialize(cls, current_spec, *, manifest_dir: Path):
            assert current_spec == spec
            assert manifest_dir == tmp_path
            return materialized

    class _ResolvedSymbol:
        def resolve(self, *, root_path: Path):
            assert root_path == tmp_path
            return _ExternalDescriptor

    monkeypatch.setattr(
        'cosecha.shell.mochuelo_runtime.SymbolRef.parse',
        lambda raw: _ResolvedSymbol(),
    )

    assert (
        MochueloRuntimeServiceHookDescriptor.materialize(
            spec,
            manifest_dir=tmp_path,
        )
        is materialized
    )


def test_materialize_rejects_non_class_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ResolvedSymbol:
        def resolve(self, *, root_path: Path):
            del root_path
            return object()

    monkeypatch.setattr(
        'cosecha.shell.mochuelo_runtime.SymbolRef.parse',
        lambda raw: _ResolvedSymbol(),
    )

    with pytest.raises(
        TypeError,
        match='Mochuelo runtime service hook descriptor is not a class',
    ):
        MochueloRuntimeServiceHookDescriptor.materialize(
            HookSpec(id='x', type='mochuelo_runtime_service'),
            manifest_dir=tmp_path,
        )
