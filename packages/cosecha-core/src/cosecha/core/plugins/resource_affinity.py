from __future__ import annotations

import json
import time

from dataclasses import dataclass
from typing import TYPE_CHECKING, Self, override

from cosecha.core.execution_ir import (
    PlanningAnalysis,
    analyze_execution_plan,
    reorder_execution_plan_by_resource_affinity,
)
from cosecha.core.plugins.base import PlanMiddleware, PluginContext
from cosecha.core.resources import (
    RESOURCE_TIMING_PATH,
    ResourceTiming,
    normalize_resource_scope,
)


if TYPE_CHECKING:  # pragma: no cover
    from argparse import ArgumentParser, Namespace
    from pathlib import Path


RESOURCE_COST_ALPHA = 0.35


@dataclass(slots=True, frozen=True)
class ResourceCostRecord:
    name: str
    scope: str
    score: float
    sample_count: int = 0
    last_seen: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            'last_seen': self.last_seen,
            'name': self.name,
            'sample_count': self.sample_count,
            'scope': self.scope,
            'score': self.score,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> ResourceCostRecord:
        return cls(
            name=str(data['name']),
            scope=str(data['scope']),
            score=float(data.get('score', 0.0)),
            sample_count=int(data.get('sample_count', 0)),
            last_seen=float(data.get('last_seen', 0.0)),
        )


class ResourceAffinityPlugin(PlanMiddleware):
    __slots__ = (
        '_resource_costs',
        '_storage_path',
    )

    def __init__(self) -> None:
        self._resource_costs: tuple[ResourceCostRecord, ...] = ()
        self._storage_path: Path | None = None

    @override
    @classmethod
    def register_arguments(cls, parser: ArgumentParser) -> None:
        parser.add_argument(
            '--resource-affinity',
            action='store_true',
            default=False,
            help='Agrupa el plan por afinidad de recursos compartidos',
        )

    @override
    @classmethod
    def parse_args(cls, args: Namespace) -> Self | None:
        if not args.resource_affinity:
            return None

        return cls()

    @override
    async def initialize(self, context: PluginContext) -> None:
        await super().initialize(context)

    @override
    async def start(self):
        self._storage_path = self.config.root_path / RESOURCE_TIMING_PATH
        if self._storage_path.exists():
            payload = json.loads(
                self._storage_path.read_text(encoding='utf-8'),
            )
            self._resource_costs = _load_resource_cost_records(payload)

    @override
    async def finish(self) -> None:
        if self._storage_path is None:
            return

        self._storage_path.parent.mkdir(parents=True, exist_ok=True)
        merged_costs = _merge_resource_cost_records(
            self._resource_costs,
            self.context.resource_manager.build_resource_timing_snapshot(),
        )
        self._storage_path.write_text(
            json.dumps(
                {
                    'resource_costs': [
                        resource_cost.to_dict()
                        for resource_cost in merged_costs
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding='utf-8',
        )

    @override
    async def transform_planning_analysis(
        self,
        analysis: PlanningAnalysis,
    ) -> PlanningAnalysis:
        reordered_plan = reorder_execution_plan_by_resource_affinity(
            analysis.plan,
            resource_costs={
                (
                    resource_cost.name,
                    normalize_resource_scope(resource_cost.scope),
                ): resource_cost.score
                for resource_cost in self._resource_costs
            },
        )
        return analyze_execution_plan(
            reordered_plan,
            mode=analysis.mode,
        )


def _load_resource_cost_records(
    payload: dict[str, object],
) -> tuple[ResourceCostRecord, ...]:
    if 'resource_costs' in payload:
        return tuple(
            _normalize_resource_cost_record(
                ResourceCostRecord.from_dict(resource_cost),
            )
            for resource_cost in payload.get('resource_costs', [])
        )

    legacy_timings = tuple(
        ResourceTiming.from_dict(resource_timing)
        for resource_timing in payload.get('resource_timings', [])
    )
    if not legacy_timings:
        return ()

    observed_at = time.time()
    return tuple(
        ResourceCostRecord(
            name=resource_timing.name,
            scope=normalize_resource_scope(resource_timing.scope),
            score=_build_observed_resource_cost(resource_timing),
            sample_count=1,
            last_seen=observed_at,
        )
        for resource_timing in legacy_timings
    )


def _merge_resource_cost_records(
    current: tuple[ResourceCostRecord, ...],
    observed: tuple[ResourceTiming, ...],
) -> tuple[ResourceCostRecord, ...]:
    merged: dict[tuple[str, str], ResourceCostRecord] = {
        (resource_cost.name, resource_cost.scope): resource_cost
        for resource_cost in current
    }
    observed_at = time.time()

    for resource_timing in observed:
        normalized_scope = normalize_resource_scope(resource_timing.scope)
        key = (resource_timing.name, normalized_scope)
        observed_score = _build_observed_resource_cost(resource_timing)
        if observed_score <= 0.0:
            continue

        previous = merged.get(key)
        if previous is None:
            merged[key] = ResourceCostRecord(
                name=resource_timing.name,
                scope=normalized_scope,
                score=observed_score,
                sample_count=1,
                last_seen=observed_at,
            )
            continue

        merged[key] = ResourceCostRecord(
            name=resource_timing.name,
            scope=normalized_scope,
            score=(
                (1 - RESOURCE_COST_ALPHA) * previous.score
                + RESOURCE_COST_ALPHA * observed_score
            ),
            sample_count=previous.sample_count + 1,
            last_seen=observed_at,
        )

    return tuple(
        merged[key]
        for key in sorted(merged, key=lambda item: (item[1], item[0]))
    )


def _build_observed_resource_cost(resource_timing: ResourceTiming) -> float:
    observation_count = (
        resource_timing.acquire_count + resource_timing.release_count
    )
    if observation_count <= 0:
        return resource_timing.total_duration

    return resource_timing.total_duration / observation_count


def _normalize_resource_cost_record(
    record: ResourceCostRecord,
) -> ResourceCostRecord:
    normalized_scope = normalize_resource_scope(record.scope)
    if normalized_scope == record.scope:
        return record

    return ResourceCostRecord(
        name=record.name,
        scope=normalized_scope,
        score=record.score,
        sample_count=record.sample_count,
        last_seen=record.last_seen,
    )
