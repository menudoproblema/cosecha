from __future__ import annotations

from pathlib import Path

from cosecha.core.manifest_symbols import ManifestValidationError
from cosecha.core.manifest_types import (
    ManifestEngineDecision,
    ManifestResourceBindingDecision,
    ManifestResourceDecision,
    RuntimeProfileDecision,
)


def select_engine_specs(
    manifest,
    *,
    config,
    selected_engine_names: set[str] | None,
    requested_paths: tuple[str, ...],
):
    if selected_engine_names:
        selected = tuple(
            engine
            for engine in manifest.engines
            if engine.name in selected_engine_names
        )
        if not selected:
            msg = (
                'Invalid engines selected. Available engines: '
                f'{", ".join(engine.name for engine in manifest.engines)}'
            )
            raise ManifestValidationError(msg)
        return selected

    include_paths = tuple(
        path
        for path in requested_paths
        if path and not str(path).startswith('~')
    )
    if not include_paths:
        return manifest.engines

    normalized_requested_paths = tuple(
        normalize_requested_path(config.root_path, raw_path)
        for raw_path in include_paths
    )

    selected = tuple(
        engine
        for engine in manifest.engines
        if any(
            engine_path_matches_request(engine.path, requested_path)
            for requested_path in normalized_requested_paths
        )
    )
    return selected or manifest.engines


def evaluate_engine_selection(
    manifest,
    *,
    config,
    selected_engine_names: set[str] | None,
    requested_paths: tuple[str, ...],
) -> tuple[ManifestEngineDecision, ...]:
    include_paths = tuple(
        path
        for path in requested_paths
        if path and not str(path).startswith('~')
    )
    normalized_requested_paths = tuple(
        normalize_requested_path(config.root_path, raw_path)
        for raw_path in include_paths
    )

    if selected_engine_names:
        return tuple(
            ManifestEngineDecision(
                id=engine.id,
                name=engine.name,
                type=engine.type,
                path=engine.path,
                active=engine.name in selected_engine_names,
                reasons=(
                    ('selected_by_engine_filter',)
                    if engine.name in selected_engine_names
                    else ('not_selected_by_engine_filter',)
                ),
            )
            for engine in manifest.engines
        )

    if not normalized_requested_paths:
        return tuple(
            ManifestEngineDecision(
                id=engine.id,
                name=engine.name,
                type=engine.type,
                path=engine.path,
                active=True,
                reasons=('default_selection',),
            )
            for engine in manifest.engines
        )

    matched_engine_ids = {
        engine.id
        for engine in manifest.engines
        if any(
            engine_path_matches_request(engine.path, requested_path)
            for requested_path in normalized_requested_paths
        )
    }
    fallback_to_all = not matched_engine_ids

    decisions: list[ManifestEngineDecision] = []
    for engine in manifest.engines:
        matched_requested_paths = tuple(
            requested_path
            for requested_path in normalized_requested_paths
            if engine_path_matches_request(engine.path, requested_path)
        )
        if fallback_to_all:
            reasons = (
                ('no_engine_matched_requested_paths_fallback',)
                if not matched_requested_paths
                else ('matched_requested_path',)
            )
            active = True
        else:
            active = bool(matched_requested_paths)
            reasons = (
                ('matched_requested_path',)
                if active
                else ('path_filter_mismatch',)
            )

        decisions.append(
            ManifestEngineDecision(
                id=engine.id,
                name=engine.name,
                type=engine.type,
                path=engine.path,
                active=active,
                reasons=reasons,
                matched_requested_paths=matched_requested_paths,
            ),
        )

    return tuple(decisions)


def evaluate_runtime_profile_selection(
    manifest,
    *,
    active_specs,
) -> tuple[RuntimeProfileDecision, ...]:
    active_engine_ids = {engine.id for engine in active_specs}
    decisions: list[RuntimeProfileDecision] = []
    for profile in manifest.runtime_profiles:
        referenced_engine_ids = tuple(
            engine.id
            for engine in manifest.engines
            if profile.id in engine.runtime_profile_ids
        )
        active_referenced_engine_ids = tuple(
            engine_id
            for engine_id in referenced_engine_ids
            if engine_id in active_engine_ids
        )
        reasons: list[str] = []
        if active_referenced_engine_ids:
            reasons.append('referenced_by_active_engines')
        elif referenced_engine_ids:
            reasons.append('only_referenced_by_inactive_engines')
        else:
            reasons.append('unreferenced_runtime_profile')
        decisions.append(
            RuntimeProfileDecision(
                id=profile.id,
                active=bool(active_referenced_engine_ids),
                referenced_engine_ids=referenced_engine_ids,
                active_engine_ids=active_referenced_engine_ids,
                reasons=tuple(reasons),
            ),
        )
    return tuple(decisions)


def evaluate_resource_selection(
    manifest,
    *,
    active_specs,
) -> tuple[ManifestResourceDecision, ...]:
    active_engine_types = {engine.type for engine in active_specs}
    decisions: list[ManifestResourceDecision] = []
    for resource in manifest.resources:
        bindings = tuple(
            binding
            for binding in manifest.resource_bindings
            if binding.resource_name == resource.name
        )
        binding_engine_types = tuple(
            sorted({binding.engine_type for binding in bindings}),
        )
        active_binding_engine_types = tuple(
            engine_type
            for engine_type in binding_engine_types
            if engine_type in active_engine_types
        )
        reasons: list[str] = []
        if active_binding_engine_types:
            reasons.append('bound_to_active_engine_type')
        elif binding_engine_types:
            reasons.append('only_bound_to_inactive_engine_type')
        else:
            reasons.append('declared_without_bindings')
        decisions.append(
            ManifestResourceDecision(
                name=resource.name,
                scope=resource.scope,
                mode=resource.mode,
                active=bool(active_binding_engine_types),
                binding_engine_types=binding_engine_types,
                active_binding_engine_types=active_binding_engine_types,
                reasons=tuple(reasons),
                bindings=tuple(
                    ManifestResourceBindingDecision(
                        engine_type=binding.engine_type,
                        active=binding.engine_type in active_engine_types,
                        fixture_name=binding.fixture_name,
                        layout=binding.layout,
                        alias=binding.alias,
                    )
                    for binding in bindings
                ),
            ),
        )
    return tuple(decisions)


def normalize_requested_path(root_path: Path, raw_path: str) -> str:
    requested_path = Path(raw_path)
    if requested_path.is_absolute():
        with_path = requested_path
    else:
        with_path = (Path.cwd() / requested_path).resolve()

    try:
        return str(with_path.relative_to(root_path))
    except ValueError:
        return str(requested_path).lstrip('./')


def engine_path_matches_request(
    engine_path: str,
    requested_path: str,
) -> bool:
    normalized_engine_path = engine_path.strip('/')
    normalized_requested_path = requested_path.strip('/')
    if normalized_engine_path in {'', '.'}:
        return True

    return (
        normalized_requested_path == normalized_engine_path
        or normalized_requested_path.startswith(
            f'{normalized_engine_path}/',
        )
        or normalized_engine_path.startswith(
            f'{normalized_requested_path}/',
        )
    )
