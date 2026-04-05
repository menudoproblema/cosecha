from collections import defaultdict
from contextlib import contextmanager

from cosecha.engine.gherkin.step_materialization import LazyStepResolver
from cosecha.engine.gherkin.steps.definition import Match, StepDefinition
from cosecha.engine.gherkin.types import StepType


class AmbiguousStepError(ValueError): ...


# Cache de pares de definiciones ya verificados como no ambiguos.
# Persiste entre sesiones del mismo proceso para evitar re-ejecutar
# match() sobre los mismos pares cuando se recargan las mismas librerias.
_checked_pairs: set[frozenset[StepDefinition]] = set()


class StepRegistry:
    __slots__ = (
        '_bulk_load_depth',
        '_dynamic_steps',
        '_dynamic_steps_by_fragment',
        '_fully_dynamic_steps',
        '_lazy_step_resolver',
        '_match_cache',
        '_match_cache_dirty',
        '_pending_bulk_step_definitions',
        '_steps_set',
        '_strict_ambiguity',
        'steps',
    )

    def __init__(
        self,
        *,
        lazy_step_resolver: LazyStepResolver | None = None,
        strict_ambiguity: bool = False,
    ):
        # Indexamos las definiciones por tipo y luego por su prefijo literal
        # para reducir el espacio de búsqueda de O(N) a O(1) o O(K).
        self.steps: dict[StepType, dict[str, list[StepDefinition]]] = {
            'given': defaultdict(list),
            'when': defaultdict(list),
            'then': defaultdict(list),
            'but': defaultdict(list),
            'step': defaultdict(list),
        }
        # Los steps que empiezan directamente por un parámetro '{}' no tienen
        # un prefijo útil para el índice. Van a esta cubeta especial.
        self._dynamic_steps: dict[StepType, list[StepDefinition]] = {
            'given': [],
            'when': [],
            'then': [],
            'but': [],
            'step': [],
        }
        self._dynamic_steps_by_fragment: dict[
            StepType,
            dict[str, list[StepDefinition]],
        ] = {
            'given': defaultdict(list),
            'when': defaultdict(list),
            'then': defaultdict(list),
            'but': defaultdict(list),
            'step': defaultdict(list),
        }
        self._fully_dynamic_steps: dict[StepType, list[StepDefinition]] = {
            'given': [],
            'when': [],
            'then': [],
            'but': [],
            'step': [],
        }
        # Set para deteccion O(1) de duplicados exactos en add_step_definition.
        self._steps_set: set[StepDefinition] = set()
        # Cacheamos resultados de find_match por (step_type, step_text).
        self._match_cache: dict[tuple[StepType, str], Match | None] = {}
        self._bulk_load_depth = 0
        self._pending_bulk_step_definitions: list[StepDefinition] = []
        self._match_cache_dirty = False
        self._strict_ambiguity = strict_ambiguity
        self._lazy_step_resolver = lazy_step_resolver

    def _get_conflicting_types(
        self,
        step_type: StepType,
    ) -> tuple[StepType, ...]:
        if step_type == 'step':
            return ('step', 'given', 'when', 'then', 'but')

        return (step_type, 'step')

    def _step_texts_may_conflict(
        self,
        left_step_texts: tuple,
        right_step_texts: tuple,
    ) -> bool:
        for left_step_text in left_step_texts:
            left_prefix = left_step_text.literal_prefix
            for right_step_text in right_step_texts:
                right_prefix = right_step_text.literal_prefix
                if not left_prefix or not right_prefix:
                    left_fragments = left_step_text.literal_fragments
                    right_fragments = right_step_text.literal_fragments
                    if not left_fragments or not right_fragments:
                        return True

                    if any(
                        left_fragment in right_fragment
                        or right_fragment in left_fragment
                        for left_fragment in left_fragments
                        for right_fragment in right_fragments
                    ):
                        return True

                    continue

                if left_prefix.startswith(
                    right_prefix,
                ) or right_prefix.startswith(left_prefix):
                    return True

        return False

    def add_step_definition(self, step_definition: StepDefinition) -> None:
        # Deteccion O(1) de duplicados exactos.
        if step_definition in self._steps_set:
            return

        if self._bulk_load_depth > 0:
            self._index_step_definition(step_definition)
            self._steps_set.add(step_definition)
            self._pending_bulk_step_definitions.append(step_definition)
            self._invalidate_match_cache()
            return

        if not self._strict_ambiguity:
            self._index_step_definition(step_definition)
            self._steps_set.add(step_definition)
            self._invalidate_match_cache()
            return

        for current_definition in self._get_candidate_conflicting_definitions(
            step_definition,
        ):
            if not self._step_texts_may_conflict(
                step_definition.step_text_list,
                current_definition.step_text_list,
            ):
                continue

            pair = frozenset((current_definition, step_definition))
            if pair in _checked_pairs:
                continue

            for step_text in step_definition.step_text_list:
                current_matches_new = current_definition.match(step_text.text)
                new_matches_current = any(
                    step_definition.match(current_step_text.text)
                    for current_step_text in current_definition.step_text_list
                )

                if current_matches_new or new_matches_current:
                    new_step = (
                        f"@{step_definition.step_type}('{step_text.text}')"
                    )

                    message = (
                        f'{new_step} is ambiguous:\n'
                        f'  new:      {step_definition.location}\n'
                        f'  existing: {current_definition.location}'
                    )

                    raise AmbiguousStepError(message)

            _checked_pairs.add(pair)

        self._index_step_definition(step_definition)
        self._steps_set.add(step_definition)
        self._invalidate_match_cache()

    def add_step_definitions(
        self,
        step_definitions,
    ) -> None:
        with self.bulk_load():
            for step_definition in step_definitions:
                self.add_step_definition(step_definition)

    @contextmanager
    def bulk_load(self):
        self._bulk_load_depth += 1
        try:
            yield
            if self._bulk_load_depth == 1 and self._strict_ambiguity:
                self._validate_pending_bulk_step_definitions()
        finally:
            self._bulk_load_depth -= 1
            if self._bulk_load_depth == 0:
                self._pending_bulk_step_definitions.clear()
                if self._match_cache_dirty:
                    self._match_cache.clear()
                    self._match_cache_dirty = False

    def _invalidate_match_cache(self) -> None:
        # Durante cargas masivas aplazamos el clear para no repetirlo
        # centenares de veces mientras el registro aun no esta utilizable.
        if self._bulk_load_depth > 0:
            self._match_cache_dirty = True
            return

        self._match_cache.clear()

    def _index_step_definition(self, step_definition: StepDefinition) -> None:
        # Si alguno de sus patrones no tiene prefijo, va a la cubeta dinámica.
        has_dynamic = False
        dynamic_fragments: set[str] = set()
        for step_text in step_definition.step_text_list:
            prefix = step_text.literal_prefix
            if not prefix:
                has_dynamic = True
                dynamic_fragments.update(step_text.literal_fragments)
            else:
                self.steps[step_definition.step_type][prefix].append(
                    step_definition,
                )

        if has_dynamic:
            self._dynamic_steps[step_definition.step_type].append(
                step_definition,
            )
            if dynamic_fragments:
                for fragment in dynamic_fragments:
                    self._dynamic_steps_by_fragment[step_definition.step_type][
                        fragment
                    ].append(step_definition)
            else:
                self._fully_dynamic_steps[step_definition.step_type].append(
                    step_definition,
                )

    def _iter_dynamic_candidate_definitions(
        self,
        step_type: StepType,
        *,
        step_text: str | None = None,
        step_definition: StepDefinition | None = None,
    ) -> tuple[StepDefinition, ...]:
        candidates: list[StepDefinition] = list(
            self._fully_dynamic_steps[step_type],
        )
        seen_definitions = set(candidates)
        fragment_index = self._dynamic_steps_by_fragment[step_type]

        query_fragments: set[str] = set()
        if step_text is not None:
            for fragment in fragment_index:
                if fragment in step_text:
                    query_fragments.add(fragment)
        elif step_definition is not None:
            for current_step_text in step_definition.step_text_list:
                query_fragments.update(current_step_text.literal_fragments)
        else:
            return tuple(candidates)

        if not query_fragments:
            return tuple(self._dynamic_steps[step_type])

        for indexed_fragment, definitions in fragment_index.items():
            if not any(
                indexed_fragment in query_fragment
                or query_fragment in indexed_fragment
                for query_fragment in query_fragments
            ):
                continue

            for current_definition in definitions:
                if current_definition in seen_definitions:
                    continue

                seen_definitions.add(current_definition)
                candidates.append(current_definition)

        return tuple(candidates)

    def _get_candidate_conflicting_definitions(
        self,
        step_definition: StepDefinition,
    ) -> tuple[StepDefinition, ...]:
        seen_definitions: set[StepDefinition] = set()
        definitions: list[StepDefinition] = []

        for conflicting_type in self._get_conflicting_types(
            step_definition.step_type,
        ):
            for current_definition in self._iter_dynamic_candidate_definitions(
                conflicting_type,
                step_definition=step_definition,
            ):
                if current_definition in seen_definitions:
                    continue

                seen_definitions.add(current_definition)
                definitions.append(current_definition)

            type_index = self.steps[conflicting_type]
            for step_text in step_definition.step_text_list:
                prefix = step_text.literal_prefix
                self._extend_prefixed_candidates(
                    definitions,
                    seen_definitions,
                    type_index,
                    prefix=prefix,
                    fragments=step_text.literal_fragments,
                )

        return tuple(definitions)

    def _extend_prefixed_candidates(
        self,
        definitions: list[StepDefinition],
        seen_definitions: set[StepDefinition],
        type_index: dict[str, list[StepDefinition]],
        *,
        prefix: str,
        fragments: tuple[str, ...],
    ) -> None:
        if not prefix and not fragments:
            candidate_buckets = tuple(type_index.values())
        else:
            candidate_buckets = tuple(
                prefixed_definitions
                for current_prefix, prefixed_definitions in type_index.items()
                if self._prefix_matches_query(
                    current_prefix,
                    prefix=prefix,
                    fragments=fragments,
                )
            )

        for prefixed_definitions in candidate_buckets:
            for current_definition in prefixed_definitions:
                if current_definition in seen_definitions:
                    continue

                seen_definitions.add(current_definition)
                definitions.append(current_definition)

    def _prefix_matches_query(
        self,
        current_prefix: str,
        *,
        prefix: str,
        fragments: tuple[str, ...],
    ) -> bool:
        if prefix:
            return current_prefix.startswith(prefix) or prefix.startswith(
                current_prefix,
            )

        return any(
            fragment in current_prefix or current_prefix in fragment
            for fragment in fragments
        )

    def _validate_pending_bulk_step_definitions(self) -> None:
        for step_definition in self._pending_bulk_step_definitions:
            for (
                current_definition
            ) in self._get_candidate_conflicting_definitions(step_definition):
                if current_definition is step_definition:
                    continue

                if not self._step_texts_may_conflict(
                    step_definition.step_text_list,
                    current_definition.step_text_list,
                ):
                    continue

                pair = frozenset((current_definition, step_definition))
                if pair in _checked_pairs:
                    continue

                for step_text in step_definition.step_text_list:
                    current_matches_new = current_definition.match(
                        step_text.text,
                    )
                    current_step_text_list = current_definition.step_text_list
                    new_matches_current = any(
                        step_definition.match(current_step_text.text)
                        for current_step_text in current_step_text_list
                    )

                    if current_matches_new or new_matches_current:
                        new_step = (
                            f"@{step_definition.step_type}('{step_text.text}')"
                        )

                        message = (
                            f'{new_step} is ambiguous:\n'
                            f'  new:      {step_definition.location}\n'
                            f'  existing: {current_definition.location}'
                        )

                        raise AmbiguousStepError(message)

                _checked_pairs.add(pair)

    def set_lazy_step_resolver(
        self,
        lazy_step_resolver: LazyStepResolver | None,
    ) -> None:
        self._lazy_step_resolver = lazy_step_resolver

    def iter_completion_definitions(
        self,
        step_type: StepType,
    ) -> tuple[StepDefinition, ...]:
        definitions: list[StepDefinition] = []
        seen_definitions: set[StepDefinition] = set()
        for current_type in self._get_conflicting_types(step_type):
            for prefixed_definitions in self.steps[current_type].values():
                for current_definition in prefixed_definitions:
                    if current_definition in seen_definitions:
                        continue
                    seen_definitions.add(current_definition)
                    definitions.append(current_definition)

            for current_definition in self._dynamic_steps[current_type]:
                if current_definition in seen_definitions:
                    continue
                seen_definitions.add(current_definition)
                definitions.append(current_definition)

        return tuple(definitions)

    def find_match(self, step_type: StepType, step_text: str):
        if self._lazy_step_resolver is not None:
            self._lazy_step_resolver.materialize_for_query(
                step_type,
                step_text,
                self,
            )

        key = (step_type, step_text)
        if key in self._match_cache:
            return self._match_cache[key]

        result = self._find_match_across_conflicting_types(
            step_type,
            step_text,
        )

        self._match_cache[key] = result
        return result

    def _find_match_across_conflicting_types(
        self,
        step_type: StepType,
        step_text: str,
    ) -> Match | None:
        matches: list[Match] = []
        seen_definitions: set[StepDefinition] = set()

        for current_type in self._get_conflicting_types(step_type):
            for match_result in self._find_matches_in_type(
                current_type,
                step_text,
            ):
                step_definition = match_result.step_definition
                if step_definition in seen_definitions:
                    continue

                seen_definitions.add(step_definition)
                matches.append(match_result)

        if len(matches) > 1:
            raise self._build_ambiguous_runtime_match_error(
                step_type,
                step_text,
                matches,
            )

        return matches[0] if matches else None

    def _find_matches_in_type(
        self,
        step_type: StepType,
        step_text: str,
    ) -> tuple[Match, ...]:
        matches: list[Match] = []
        seen_definitions: set[StepDefinition] = set()
        type_index = self.steps[step_type]

        for prefix, definitions in type_index.items():
            if not step_text.startswith(prefix):
                continue

            for step_definition in definitions:
                if step_definition in seen_definitions:
                    continue

                if match_result := step_definition.match(step_text):
                    seen_definitions.add(step_definition)
                    matches.append(match_result)

        for step_definition in self._iter_dynamic_candidate_definitions(
            step_type,
            step_text=step_text,
        ):
            if step_definition in seen_definitions:
                continue

            if match_result := step_definition.match(step_text):
                seen_definitions.add(step_definition)
                matches.append(match_result)

        return tuple(matches)

    def _build_ambiguous_runtime_match_error(
        self,
        step_type: StepType,
        step_text: str,
        matches: list[Match],
    ) -> AmbiguousStepError:
        current_match = matches[0]
        conflicting_match = matches[1]
        message = (
            f'@{step_type}({step_text!r}) is ambiguous:\n'
            f'  first:    {current_match.step_definition.location}\n'
            f'  second:   {conflicting_match.step_definition.location}'
        )
        return AmbiguousStepError(message)

    def _find_in_type(
        self,
        step_type: StepType,
        step_text: str,
    ) -> Match | None:
        # 1. Buscamos primero en el índice de prefijos literales.
        # Probamos si el texto empieza por alguna de las claves indexadas.

        type_index = self.steps[step_type]

        for prefix, definitions in type_index.items():
            if step_text.startswith(prefix):
                for step_definition in definitions:
                    if match_result := step_definition.match(step_text):
                        return match_result

        # 2. Si no hay match en el índice, probamos la cubeta dinámica.
        for step_definition in self._iter_dynamic_candidate_definitions(
            step_type,
            step_text=step_text,
        ):
            if match_result := step_definition.match(step_text):
                return match_result

        return None
