from cosecha.engine.gherkin.steps.definition import (
    StepDefinition,
    StepMatcher,
    StepText,
)
from cosecha.engine.gherkin.steps.registry import StepRegistry
from cosecha.engine.gherkin.types import StepFn


def step(
    *step_text: str | StepText,
    parser_cls: type[StepMatcher] | None = None,
    category: str | None = None,
):
    def wrapper(func: StepFn):
        if hasattr(func, '__step_definition__'):
            msg = (
                '@step is being applied more than once to the same '
                f'function {func.__name__!r}'
            )
            raise ValueError(msg)

        # Nos aseguramos de tener una lista
        step_text_list: list[str | StepText] = (
            [step_text]
            if isinstance(step_text, str | StepText)
            else list(step_text)
        )

        step_definition = StepDefinition(
            'step',
            # Nos aseguramos que cada elemento de la lista sea StepText
            [
                step_text
                if isinstance(step_text, StepText)
                else StepText(step_text)
                for step_text in step_text_list
            ],
            func,
            parser_cls=parser_cls,
            category=category,
        )

        func.__step_definition__ = step_definition
        return func

    return wrapper


def given(
    *step_text: str | StepText,
    parser_cls: type[StepMatcher] | None = None,
    category: str | None = None,
):
    def wrapper(func: StepFn):
        if hasattr(func, '__step_definition__'):
            msg = (
                '@given is being applied more than once to the '
                f'same function {func.__name__!r}'
            )
            raise ValueError(msg)

        # Nos aseguramos de tener una lista
        step_text_list: list[str | StepText] = (
            [step_text]
            if isinstance(step_text, str | StepText)
            else list(step_text)
        )

        step_definition = StepDefinition(
            'given',
            # Nos aseguramos que cada elemento de la lista sea StepText
            [
                step_text
                if isinstance(step_text, StepText)
                else StepText(step_text)
                for step_text in step_text_list
            ],
            func,
            parser_cls=parser_cls,
            category=category,
        )
        func.__step_definition__ = step_definition
        return func

    return wrapper


def when(
    *step_text: str | StepText,
    parser_cls: type[StepMatcher] | None = None,
    category: str | None = None,
):
    def wrapper(func: StepFn):
        if hasattr(func, '__step_definition__'):
            msg = (
                '@when is being applied more than once to the same '
                f'function {func.__name__!r}'
            )
            raise ValueError(msg)

        # Nos aseguramos de tener una lista
        step_text_list: list[str | StepText] = (
            [step_text]
            if isinstance(step_text, str | StepText)
            else list(step_text)
        )

        step_definition = StepDefinition(
            'when',
            # Nos aseguramos que cada elemento de la lista sea StepText
            [
                step_text
                if isinstance(step_text, StepText)
                else StepText(step_text)
                for step_text in step_text_list
            ],
            func,
            parser_cls=parser_cls,
            category=category,
        )
        func.__step_definition__ = step_definition

        return func

    return wrapper


def then(
    *step_text: str | StepText,
    parser_cls: type[StepMatcher] | None = None,
    category: str | None = None,
):
    def wrapper(func: StepFn):
        if hasattr(func, '__step_definition__'):
            msg = (
                '@then is being applied more than once to the same '
                f'function {func.__name__!r}'
            )
            raise ValueError(msg)

        # Nos aseguramos de tener una lista
        step_text_list: list[str | StepText] = (
            [step_text]
            if isinstance(step_text, str | StepText)
            else list(step_text)
        )

        step_definition = StepDefinition(
            'then',
            # Nos aseguramos que cada elemento de la lista sea StepText
            [
                step_text
                if isinstance(step_text, StepText)
                else StepText(step_text)
                for step_text in step_text_list
            ],
            func,
            parser_cls=parser_cls,
            category=category,
        )
        func.__step_definition__ = step_definition

        return func

    return wrapper


def but(
    *step_text: str | StepText,
    parser_cls: type[StepMatcher] | None = None,
    category: str | None = None,
):
    def wrapper(func: StepFn):
        if hasattr(func, '__step_definition__'):
            msg = (
                '@but is being applied more than once to the same '
                f'function {func.__name__!r}'
            )
            raise ValueError(msg)

        # Nos aseguramos de tener una lista
        step_text_list: list[str | StepText] = (
            [step_text]
            if isinstance(step_text, str | StepText)
            else list(step_text)
        )

        step_definition = StepDefinition(
            'but',
            # Nos aseguramos que cada elemento de la lista sea StepText
            [
                step_text
                if isinstance(step_text, StepText)
                else StepText(step_text)
                for step_text in step_text_list
            ],
            func,
            parser_cls=parser_cls,
            category=category,
        )
        func.__step_definition__ = step_definition

        return func

    return wrapper


__all__ = (
    'StepDefinition',
    'StepMatcher',
    'StepRegistry',
    'but',
    'given',
    'step',
    'then',
    'when',
)
