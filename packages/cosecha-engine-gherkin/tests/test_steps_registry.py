from __future__ import annotations

import pytest

from cosecha.engine.gherkin.steps.definition import StepDefinition, StepText
from cosecha.engine.gherkin.steps.registry import (
    AmbiguousStepError,
    StepRegistry,
    _checked_pairs,
)


@pytest.fixture(autouse=True)
def _clear_checked_pairs() -> None:
    _checked_pairs.clear()
    yield
    _checked_pairs.clear()


async def _noop_step(_context) -> None:
    del _context


def _definition(step_type: str, pattern: str) -> StepDefinition:
    return StepDefinition(step_type, (StepText(pattern),), _noop_step)


def test_registry_conflicting_types_and_conflict_heuristics() -> None:
    registry = StepRegistry()
    assert registry._get_conflicting_types('step') == (  # noqa: SLF001
        'step',
        'given',
        'when',
        'then',
        'but',
    )
    assert registry._get_conflicting_types('given') == ('given', 'step')  # noqa: SLF001
    assert registry._step_texts_may_conflict(  # noqa: SLF001
        (_definition('given', '{value}').step_text_list[0],),
        (_definition('given', '{other}').step_text_list[0],),
    )
    assert registry._step_texts_may_conflict(  # noqa: SLF001
        (_definition('given', '{value} user').step_text_list[0],),
        (_definition('given', '{name} users').step_text_list[0],),
    )
    assert registry._step_texts_may_conflict(  # noqa: SLF001
        (_definition('given', 'the user').step_text_list[0],),
        (_definition('given', 'the user logs in').step_text_list[0],),
    )
    assert not registry._step_texts_may_conflict(  # noqa: SLF001
        (_definition('given', 'alpha').step_text_list[0],),
        (_definition('given', 'beta').step_text_list[0],),
    )


def test_add_step_definition_bulk_load_and_cache_invalidation() -> None:
    registry = StepRegistry()
    step_definition = _definition('given', 'alpha')
    registry.add_step_definition(step_definition)
    registry.add_step_definition(step_definition)
    assert registry.find_match('given', 'alpha') is not None

    registry._match_cache[('given', 'alpha')] = None  # noqa: SLF001
    with registry.bulk_load():
        registry._invalidate_match_cache()  # noqa: SLF001
        assert registry._match_cache_dirty is True  # noqa: SLF001
    assert registry._match_cache == {}  # noqa: SLF001

    with registry.bulk_load():
        registry.add_step_definition(_definition('given', 'beta'))
    assert registry.find_match('given', 'beta') is not None


def test_strict_ambiguity_detection_and_checked_pairs_short_circuit() -> None:
    registry = StepRegistry(strict_ambiguity=True)
    first = _definition('given', 'the user exists')
    second = _definition('given', 'the user {name}')
    registry.add_step_definition(first)

    with pytest.raises(AmbiguousStepError, match='is ambiguous'):
        registry.add_step_definition(second)

    pair = frozenset((first, second))
    _checked_pairs.add(pair)
    registry = StepRegistry(strict_ambiguity=True)
    registry.add_step_definition(first)
    registry.add_step_definition(second)
    assert len(registry.iter_completion_definitions('given')) == 2


def test_dynamic_indexes_candidate_conflicts_and_prefix_helpers() -> None:
    registry = StepRegistry()
    dynamic_definition = _definition('given', '{name} logs in')
    fully_dynamic_definition = _definition('given', '{name}')
    prefixed_definition = _definition('given', 'the user logs in')
    registry.add_step_definition(dynamic_definition)
    registry.add_step_definition(fully_dynamic_definition)
    registry.add_step_definition(prefixed_definition)

    dynamic_candidates = registry._iter_dynamic_candidate_definitions(  # noqa: SLF001
        'given',
        step_text='alice logs in',
    )
    assert dynamic_definition in dynamic_candidates
    assert fully_dynamic_definition in dynamic_candidates

    no_fragment_candidates = registry._iter_dynamic_candidate_definitions(  # noqa: SLF001
        'given',
        step_text='zzz',
    )
    assert fully_dynamic_definition in no_fragment_candidates

    all_candidates = registry._iter_dynamic_candidate_definitions('given')  # noqa: SLF001
    assert fully_dynamic_definition in all_candidates

    conflicting = registry._get_candidate_conflicting_definitions(dynamic_definition)  # noqa: SLF001
    assert prefixed_definition in conflicting

    assert registry._prefix_matches_query(  # noqa: SLF001
        'the user',
        prefix='the',
        fragments=(),
    )
    assert registry._prefix_matches_query(  # noqa: SLF001
        'the user',
        prefix='',
        fragments=('user',),
    )


def test_validate_pending_bulk_step_definitions_raises_for_ambiguity() -> None:
    registry = StepRegistry(strict_ambiguity=True)
    first = _definition('given', 'the user exists')
    second = _definition('given', 'the user {name}')

    with pytest.raises(AmbiguousStepError, match='is ambiguous'):
        with registry.bulk_load():
            registry.add_step_definition(first)
            registry.add_step_definition(second)


def test_completion_iteration_and_lazy_resolver_and_match_cache() -> None:
    materialize_calls: list[tuple[str, str]] = []

    class _FakeLazyResolver:
        def materialize_for_query(self, step_type, step_text, _registry):
            materialize_calls.append((step_type, step_text))
            return False

    registry = StepRegistry(lazy_step_resolver=_FakeLazyResolver())
    first = _definition('given', 'the user logs in')
    second = _definition('step', '{name} performs action')
    registry.add_step_definition(first)
    registry.add_step_definition(second)
    registry.set_lazy_step_resolver(_FakeLazyResolver())

    completion_definitions = registry.iter_completion_definitions('given')
    assert first in completion_definitions
    assert second in completion_definitions

    match = registry.find_match('given', 'the user logs in')
    assert match is not None
    assert materialize_calls == [('given', 'the user logs in')]
    assert registry.find_match('given', 'the user logs in') is match


def test_runtime_ambiguity_and_find_helpers() -> None:
    registry = StepRegistry()
    first = _definition('given', 'the user {name}')
    second = _definition('step', 'the user {value}')
    registry.add_step_definition(first)
    registry.add_step_definition(second)

    with pytest.raises(AmbiguousStepError, match='is ambiguous'):
        registry.find_match('given', 'the user alice')

    unique_registry = StepRegistry()
    unique_registry.add_step_definition(_definition('given', 'the user logs in'))
    unique_registry.add_step_definition(_definition('given', '{name} logs in'))

    matches = unique_registry._find_matches_in_type('given', 'the user logs in')  # noqa: SLF001
    assert matches
    assert unique_registry._find_in_type('given', 'the user logs in') is not None  # noqa: SLF001
    assert unique_registry._find_in_type('given', 'missing') is None  # noqa: SLF001

    error = unique_registry._build_ambiguous_runtime_match_error(  # noqa: SLF001
        'given',
        'the user logs in',
        [matches[0], matches[0]],
    )
    assert isinstance(error, AmbiguousStepError)


def test_registry_internal_continuation_and_pair_tracking_branches(monkeypatch) -> None:
    registry = StepRegistry(strict_ambiguity=True)
    left = StepText('{left} alpha')
    right = StepText('{right} beta')
    assert not registry._step_texts_may_conflict((left,), (right,))  # noqa: SLF001

    first = StepDefinition(
        'given',
        (StepText('foo {id} bar'),),
        _noop_step,
    )
    second = StepDefinition(
        'given',
        (StepText('foo {name} baz'),),
        _noop_step,
    )
    registry.add_step_definition(first)
    registry.add_step_definition(second)
    assert frozenset((first, second)) in _checked_pairs

    registry = StepRegistry(strict_ambiguity=True)
    first = _definition('given', 'alpha')
    second = _definition('given', 'beta')
    registry._pending_bulk_step_definitions = [first, second]  # noqa: SLF001
    registry._steps_set.update({first, second})  # noqa: SLF001
    registry._index_step_definition(first)  # noqa: SLF001
    registry._index_step_definition(second)  # noqa: SLF001
    registry._validate_pending_bulk_step_definitions()  # noqa: SLF001

    pair = frozenset((first, second))
    _checked_pairs.add(pair)
    registry._validate_pending_bulk_step_definitions()  # noqa: SLF001

    registry = StepRegistry()
    dynamic = StepDefinition(
        'step',
        (StepText('alpha'), StepText('{value}')),
        _noop_step,
    )
    registry.add_step_definition(dynamic)
    completion_definitions = registry.iter_completion_definitions('step')
    assert completion_definitions == (dynamic,)


def test_registry_deduplicates_candidates_and_matches(monkeypatch) -> None:
    registry = StepRegistry()
    repeated = StepDefinition(
        'given',
        (StepText('alpha'), StepText('alpha suffix')),
        _noop_step,
    )
    registry.add_step_definition(repeated)
    matches = registry._find_matches_in_type('given', 'alpha suffix')  # noqa: SLF001
    assert len(matches) == 1

    dynamic = StepDefinition('given', (StepText('{name}'),), _noop_step)
    registry.add_step_definition(dynamic)

    monkeypatch.setattr(
        StepRegistry,
        '_iter_dynamic_candidate_definitions',
        lambda *_args, **_kwargs: (dynamic, dynamic),
    )
    dynamic_matches = registry._find_matches_in_type('given', 'alice')  # noqa: SLF001
    assert len(dynamic_matches) == 1

    base_match = dynamic_matches[0]
    monkeypatch.setattr(
        StepRegistry,
        '_find_matches_in_type',
        lambda *_args, **_kwargs: (base_match, base_match),
    )
    assert registry._find_match_across_conflicting_types('given', 'alice') is not None  # noqa: SLF001


def test_registry_find_in_type_returns_dynamic_match_path() -> None:
    registry = StepRegistry()
    registry.add_step_definition(StepDefinition('given', (StepText('{name}'),), _noop_step))

    assert registry._find_in_type('given', 'alice') is not None  # noqa: SLF001


def test_registry_add_step_definitions_and_non_conflicting_strict_paths(
    monkeypatch,
) -> None:
    registry = StepRegistry(strict_ambiguity=True)
    first = _definition('given', 'alpha')
    second = _definition('given', 'beta')

    registry.add_step_definitions((first, second))
    assert registry.find_match('given', 'alpha') is not None

    third = _definition('given', 'gamma')
    monkeypatch.setattr(
        StepRegistry,
        '_get_candidate_conflicting_definitions',
        lambda *_args, **_kwargs: (first,),
    )
    monkeypatch.setattr(
        StepRegistry,
        '_step_texts_may_conflict',
        lambda *_args, **_kwargs: False,
    )
    registry.add_step_definition(third)
    assert registry.find_match('given', 'gamma') is not None


def test_registry_remaining_internal_branches(monkeypatch) -> None:
    registry = StepRegistry()
    step_definition = _definition('step', '{name} foo')
    registry._fully_dynamic_steps['step'].append(step_definition)  # noqa: SLF001
    registry._dynamic_steps['step'].append(step_definition)  # noqa: SLF001
    registry._dynamic_steps_by_fragment['step']['foo'].extend(  # noqa: SLF001
        (step_definition, step_definition),
    )
    registry._dynamic_steps_by_fragment['step']['bar'].append(step_definition)  # noqa: SLF001
    candidates = registry._iter_dynamic_candidate_definitions(  # noqa: SLF001
        'step',
        step_text='foo query',
    )
    assert candidates

    definitions: list[StepDefinition] = []
    seen_definitions: set[StepDefinition] = {step_definition}
    registry._extend_prefixed_candidates(  # noqa: SLF001
        definitions,
        seen_definitions,
        {'alpha': [step_definition], 'beta': [step_definition]},
        prefix='',
        fragments=(),
    )
    assert definitions == []

    monkeypatch.setattr(
        StepRegistry,
        '_iter_dynamic_candidate_definitions',
        lambda *_args, **_kwargs: (step_definition,),
    )
    conflicting = registry._get_candidate_conflicting_definitions(step_definition)  # noqa: SLF001
    assert conflicting

    strict_registry = StepRegistry(strict_ambiguity=True)
    left = _definition('given', 'foo {id} bar')
    right = _definition('given', 'foo {name} baz')
    strict_registry._pending_bulk_step_definitions = [left]  # noqa: SLF001

    monkeypatch.setattr(
        StepRegistry,
        '_get_candidate_conflicting_definitions',
        lambda *_args, **_kwargs: (right,),
    )
    strict_registry._validate_pending_bulk_step_definitions()  # noqa: SLF001
    pair = frozenset((left, right))
    assert pair in _checked_pairs

    monkeypatch.setattr(
        StepRegistry,
        '_step_texts_may_conflict',
        lambda *_args, **_kwargs: False,
    )
    strict_registry._validate_pending_bulk_step_definitions()  # noqa: SLF001

    _checked_pairs.add(pair)
    monkeypatch.setattr(
        StepRegistry,
        '_step_texts_may_conflict',
        lambda *_args, **_kwargs: True,
    )
    strict_registry._validate_pending_bulk_step_definitions()  # noqa: SLF001

    completion_registry = StepRegistry()
    repeated_prefixed = StepDefinition(
        'given',
        (StepText('alpha'), StepText('beta')),
        _noop_step,
    )
    completion_registry.add_step_definition(repeated_prefixed)
    assert completion_registry.iter_completion_definitions('given') == (
        repeated_prefixed,
    )
