from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.manifest_selection import (
    engine_path_matches_request,
    evaluate_engine_selection,
    evaluate_resource_selection,
    evaluate_runtime_profile_selection,
    normalize_requested_path,
    select_engine_specs,
)
from cosecha.core.manifest_symbols import ManifestValidationError


def _engine(
    *,
    engine_id: str,
    name: str,
    engine_type: str,
    path: str,
    runtime_profile_ids: tuple[str, ...] = (),
):
    return SimpleNamespace(
        id=engine_id,
        name=name,
        type=engine_type,
        path=path,
        runtime_profile_ids=runtime_profile_ids,
    )


def _manifest():
    return SimpleNamespace(
        engines=(
            _engine(
                engine_id='engine-a',
                name='alpha',
                engine_type='pytest',
                path='tests',
                runtime_profile_ids=('profile-active',),
            ),
            _engine(
                engine_id='engine-b',
                name='beta',
                engine_type='gherkin',
                path='features',
                runtime_profile_ids=('profile-inactive',),
            ),
        ),
        runtime_profiles=(
            SimpleNamespace(id='profile-active'),
            SimpleNamespace(id='profile-inactive'),
            SimpleNamespace(id='profile-unused'),
        ),
        resources=(
            SimpleNamespace(name='db-active', scope='test', mode='live'),
            SimpleNamespace(name='db-inactive', scope='test', mode='live'),
            SimpleNamespace(name='db-unbound', scope='test', mode='live'),
        ),
        resource_bindings=(
            SimpleNamespace(
                engine_type='pytest',
                resource_name='db-active',
                fixture_name='db',
                layout='steps',
                alias='database',
            ),
            SimpleNamespace(
                engine_type='gherkin',
                resource_name='db-inactive',
                fixture_name=None,
                layout=None,
                alias=None,
            ),
        ),
    )


def test_select_engine_specs_rejects_unknown_engine_filters() -> None:
    manifest = _manifest()

    with pytest.raises(ManifestValidationError, match='Invalid engines selected'):
        select_engine_specs(
            manifest,
            config=SimpleNamespace(root_path=Path.cwd()),
            selected_engine_names={'missing'},
            requested_paths=(),
        )


def test_evaluate_engine_selection_covers_selected_default_and_fallback() -> None:
    manifest = _manifest()
    config = SimpleNamespace(root_path=Path.cwd())

    selected_decisions = evaluate_engine_selection(
        manifest,
        config=config,
        selected_engine_names={'alpha'},
        requested_paths=(),
    )
    assert selected_decisions[0].active is True
    assert selected_decisions[0].reasons == ('selected_by_engine_filter',)
    assert selected_decisions[1].active is False
    assert selected_decisions[1].reasons == ('not_selected_by_engine_filter',)

    default_decisions = evaluate_engine_selection(
        manifest,
        config=config,
        selected_engine_names=None,
        requested_paths=(),
    )
    assert all(decision.reasons == ('default_selection',) for decision in default_decisions)

    fallback_decisions = evaluate_engine_selection(
        manifest,
        config=config,
        selected_engine_names=None,
        requested_paths=('docs/readme.md',),
    )
    assert all(decision.active is True for decision in fallback_decisions)
    assert all(
        decision.reasons == ('no_engine_matched_requested_paths_fallback',)
        for decision in fallback_decisions
    )


def test_runtime_profile_and_resource_selection_reason_branches() -> None:
    manifest = _manifest()
    active_specs = (manifest.engines[0],)

    profile_decisions = evaluate_runtime_profile_selection(
        manifest,
        active_specs=active_specs,
    )
    decisions_by_id = {decision.id: decision for decision in profile_decisions}
    assert decisions_by_id['profile-active'].reasons == (
        'referenced_by_active_engines',
    )
    assert decisions_by_id['profile-inactive'].reasons == (
        'only_referenced_by_inactive_engines',
    )
    assert decisions_by_id['profile-unused'].reasons == (
        'unreferenced_runtime_profile',
    )

    resource_decisions = evaluate_resource_selection(
        manifest,
        active_specs=active_specs,
    )
    resource_by_name = {decision.name: decision for decision in resource_decisions}
    assert resource_by_name['db-active'].reasons == (
        'bound_to_active_engine_type',
    )
    assert resource_by_name['db-inactive'].reasons == (
        'only_bound_to_inactive_engine_type',
    )
    assert resource_by_name['db-unbound'].reasons == ('declared_without_bindings',)


def test_requested_path_normalization_and_engine_path_matching() -> None:
    root = Path('/tmp/root')
    absolute_child = root / 'tests' / 'test_example.py'
    normalized = normalize_requested_path(root, str(absolute_child))
    assert normalized == 'tests/test_example.py'

    assert engine_path_matches_request('.', 'any/path.py') is True
