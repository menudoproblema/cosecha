from __future__ import annotations

import ast

from pathlib import Path
from textwrap import dedent

import pytest

from cosecha.engine.gherkin.step_ast_discovery import (
    StepDiscoveryService,
    _FileDiscoveryContext,
    _build_content_digest,
    _build_import_table,
    _count_dynamic_fragments,
    _extract_anchor_tokens,
    _extract_category,
    _extract_descriptor,
    _extract_literal_fragments,
    _extract_literal_suffixes,
    _extract_parser_cls_name,
    _extract_patterns,
    _literal_prefix,
    _literal_suffix,
    _references_unsupported_step_decorator,
    _resolve_decorator_name,
    _resolve_module_import_path,
)


def _parse_function_and_decorator(source: str):
    module = ast.parse(dedent(source))
    function = module.body[0]
    assert isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef)
    return module, function, function.decorator_list[0]


def test_ast_discovery_literal_extractors_and_dynamic_fragments() -> None:
    patterns = ('a user named {name}', '{action} immediately')
    assert _literal_prefix(patterns[0]) == 'a user named '
    assert _literal_suffix(patterns[0]) == ''
    assert _extract_literal_suffixes(patterns) == ('', ' immediately')
    assert _extract_literal_fragments(patterns) == (
        'a user named',
        'immediately',
    )
    assert _extract_anchor_tokens(patterns) == (
        'user',
        'named',
        'immediately',
    )
    assert _count_dynamic_fragments(patterns) == 1
    assert _count_dynamic_fragments(()) == 0


def test_module_import_resolution_and_content_digest(tmp_path: Path) -> None:
    root = tmp_path / 'tests'
    root.mkdir()
    file_path = root / 'steps' / 'demo.py'
    file_path.parent.mkdir()
    file_path.write_text('', encoding='utf-8')

    assert _resolve_module_import_path(root, file_path) == 'steps.demo'
    assert _resolve_module_import_path(root, tmp_path / 'outside.py') is None
    assert _resolve_module_import_path(root, root) is None

    class _DummyRelative:
        def with_suffix(self, _suffix: str):
            return self

        @property
        def parts(self) -> tuple[str, ...]:
            return ()

    class _DummyPath:
        def relative_to(self, _root):
            return _DummyRelative()

    assert _resolve_module_import_path(root, _DummyPath()) is None  # type: ignore[arg-type]
    assert _build_content_digest('hello') == _build_content_digest('hello')


def test_import_table_and_decorator_resolution_helpers() -> None:
    module = ast.parse(
        dedent(
            """
            from cosecha.engine.gherkin.steps import given as aliased_given
            from cosecha.engine.gherkin.steps import then
            from cosecha.engine.gherkin.steps import helper
            from another.module import given
            import cosecha.engine.gherkin.steps as steps_alias
            import cosecha.engine.gherkin as gherkin_alias
            import some.other.module as other_alias
            """
        ),
    )
    import_table = _build_import_table(module)

    assert import_table.symbol_aliases['aliased_given'] == 'given'
    assert import_table.symbol_aliases['then'] == 'then'
    assert 'aliased_given' in import_table.unsupported_symbol_aliases
    assert import_table.module_aliases['steps_alias'] == 'cosecha.engine.gherkin.steps'
    assert import_table.module_aliases['gherkin_alias'] == 'cosecha.engine.gherkin'
    assert 'steps_alias' in import_table.unsupported_module_aliases

    _, _, direct_decorator = _parse_function_and_decorator(
        """
        @then("ok")
        async def f():
            pass
        """,
    )
    assert _resolve_decorator_name(direct_decorator, import_table) == 'then'

    _, _, alias_decorator = _parse_function_and_decorator(
        """
        @aliased_given("x")
        async def f():
            pass
        """,
    )
    assert _resolve_decorator_name(alias_decorator, import_table) == 'given'
    assert _references_unsupported_step_decorator(alias_decorator, import_table)

    _, _, attribute_decorator = _parse_function_and_decorator(
        """
        @steps_alias.when("x")
        async def f():
            pass
        """,
    )
    assert _resolve_decorator_name(attribute_decorator, import_table) == 'when'
    assert _references_unsupported_step_decorator(
        attribute_decorator,
        import_table,
    )
    _, _, unsupported_attribute_decorator = _parse_function_and_decorator(
        """
        @other_alias.when("x")
        async def f():
            pass
        """,
    )
    assert (
        _resolve_decorator_name(unsupported_attribute_decorator, import_table)
        is None
    )

    unknown_table = _build_import_table(ast.parse('import x as y'))
    _, _, unknown_decorator = _parse_function_and_decorator(
        """
        @custom("ok")
        async def f():
            pass
        """,
    )
    assert _resolve_decorator_name(unknown_decorator, unknown_table) is None


def test_parser_category_and_pattern_extractors_validate_expressions() -> None:
    parser_attr = ast.parse('pkg.mod.Parser', mode='eval').body
    parser_name = ast.parse('Parser', mode='eval').body
    parser_none = ast.parse('None', mode='eval').body
    invalid_parser = ast.parse('1 + 2', mode='eval').body
    category_text = ast.parse('"billing"', mode='eval').body
    category_none = ast.parse('None', mode='eval').body
    invalid_category = ast.parse('42', mode='eval').body

    assert _extract_parser_cls_name(parser_none) is None
    assert _extract_parser_cls_name(parser_name) == 'Parser'
    assert _extract_parser_cls_name(parser_attr) == 'pkg.mod.Parser'
    with pytest.raises(ValueError, match='Unsupported parser_cls expression'):
        _extract_parser_cls_name(invalid_parser)

    assert _extract_category(category_text) == 'billing'
    assert _extract_category(category_none) is None
    with pytest.raises(ValueError, match='Unsupported category expression'):
        _extract_category(invalid_category)

    valid_call = ast.parse('given("a", "b")', mode='eval').body
    assert isinstance(valid_call, ast.Call)
    assert _extract_patterns(valid_call) == ('a', 'b')

    invalid_arg_call = ast.parse('given(1)', mode='eval').body
    assert isinstance(invalid_arg_call, ast.Call)
    with pytest.raises(ValueError, match='Unsupported step pattern expression'):
        _extract_patterns(invalid_arg_call)

    no_pattern_call = ast.parse('given()', mode='eval').body
    assert isinstance(no_pattern_call, ast.Call)
    with pytest.raises(
        ValueError,
        match='Step decorator requires at least one literal pattern',
    ):
        _extract_patterns(no_pattern_call)


def test_extract_descriptor_handles_supported_and_unsupported_cases(
    tmp_path: Path,
) -> None:
    file_context = _FileDiscoveryContext(
        file_path=(tmp_path / 'steps.py').resolve(),
        module_import_path='steps',
        mtime_ns=1,
        file_size=2,
    )
    import_table = _build_import_table(
        ast.parse('from cosecha.engine.gherkin.steps import given as aliased'),
    )
    _, function, decorator = _parse_function_and_decorator(
        """
        @aliased("a user", category="billing", parser_cls=CustomParser)
        async def step_fn():
            \"\"\"Doc\"\"\"
            pass
        """,
    )
    with pytest.raises(ValueError, match='Unsupported aliased step decorator'):
        _extract_descriptor(function, decorator, import_table, file_context)

    bare_table = _build_import_table(
        ast.parse('from cosecha.engine.gherkin.steps import given'),
    )
    _, function, bare_decorator = _parse_function_and_decorator(
        """
        @given
        def step_fn():
            pass
        """,
    )
    with pytest.raises(ValueError, match='Unsupported bare step decorator'):
        _extract_descriptor(function, bare_decorator, bare_table, file_context)

    _, function, invalid_kw_decorator = _parse_function_and_decorator(
        """
        @given("x", timeout=10)
        def step_fn():
            pass
        """,
    )
    with pytest.raises(ValueError, match='Unsupported keyword argument'):
        _extract_descriptor(
            function,
            invalid_kw_decorator,
            bare_table,
            file_context,
        )

    module = ast.parse('def step_fn():\n    pass')
    fn = module.body[0]
    assert isinstance(fn, ast.FunctionDef)
    undecorated = ast.Name(id='not_a_step')
    assert _extract_descriptor(fn, undecorated, bare_table, file_context) is None

    _, function, valid_decorator = _parse_function_and_decorator(
        """
        @given("a user {name}", category="billing", parser_cls=CustomParser)
        async def step_fn():
            \"\"\"Doc\"\"\"
            pass
        """,
    )
    descriptor = _extract_descriptor(
        function,
        valid_decorator,
        bare_table,
        file_context,
    )
    assert descriptor is not None
    assert descriptor.step_type == 'given'
    assert descriptor.function_name == 'step_fn'
    assert descriptor.category == 'billing'
    assert descriptor.parser_cls_name == 'CustomParser'
    assert descriptor.documentation == 'Doc'


def test_step_discovery_service_handles_ast_and_fallback_modes(
    tmp_path: Path,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir()
    valid_file = root_path / 'steps' / 'valid_steps.py'
    valid_file.parent.mkdir()
    valid_file.write_text(
        dedent(
            """
            from cosecha.engine.gherkin.steps import given

            @given("a valid step")
            async def valid_step():
                pass
            """
        ),
        encoding='utf-8',
    )
    invalid_syntax_file = root_path / 'steps' / 'invalid.py'
    invalid_syntax_file.write_text('def oops(:\n', encoding='utf-8')
    unsupported_alias_file = root_path / 'steps' / 'alias_steps.py'
    unsupported_alias_file.write_text(
        dedent(
            """
            from cosecha.engine.gherkin.steps import given as g

            @g("aliased")
            async def aliased_step():
                pass
            """
        ),
        encoding='utf-8',
    )
    unsupported_kw_file = root_path / 'steps' / 'unsupported_kw.py'
    unsupported_kw_file.write_text(
        dedent(
            """
            from cosecha.engine.gherkin.steps import given

            @given("x", timeout=10)
            async def broken():
                pass
            """
        ),
        encoding='utf-8',
    )
    ignored_error_file = root_path / 'steps' / 'ignored_error.py'
    ignored_error_file.write_text(
        dedent(
            """
            @not_a_step()
            async def not_a_step():
                pass
            """
        ),
        encoding='utf-8',
    )

    service = StepDiscoveryService(root_path)
    valid_discovery = service.discover_step_file(valid_file)
    syntax_discovery = service.discover_step_file(invalid_syntax_file)
    alias_discovery = service.discover_step_file(unsupported_alias_file)
    keyword_discovery = service.discover_step_file(unsupported_kw_file)
    ignored_discovery = service.discover_step_file(ignored_error_file)
    many = service.discover_step_files((valid_file, unsupported_kw_file))

    assert valid_discovery.discovery_mode == 'ast'
    assert len(valid_discovery.descriptors) == 1
    assert syntax_discovery.discovery_mode == 'fallback_import'
    assert syntax_discovery.requires_fallback_import is True
    assert alias_discovery.requires_fallback_import is True
    assert keyword_discovery.requires_fallback_import is True
    assert ignored_discovery.requires_fallback_import is False
    assert len(many) == 2


def test_step_discovery_service_marks_fallback_when_value_error_with_unsupported_alias_and_unknown_decorator(  # noqa: E501
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_path = tmp_path / 'tests'
    root_path.mkdir()
    file_path = root_path / 'steps.py'
    file_path.write_text(
        dedent(
            """
            @x
            def not_a_step():
                pass
            """
        ),
        encoding='utf-8',
    )
    service = StepDiscoveryService(root_path)
    monkeypatch.setattr(
        'cosecha.engine.gherkin.step_ast_discovery._extract_descriptor',
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError('x')),
    )
    monkeypatch.setattr(
        'cosecha.engine.gherkin.step_ast_discovery._resolve_decorator_name',
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        'cosecha.engine.gherkin.step_ast_discovery._references_unsupported_step_decorator',
        lambda *_args, **_kwargs: True,
    )

    discovered = service.discover_step_file(file_path)

    assert discovered.requires_fallback_import is True
