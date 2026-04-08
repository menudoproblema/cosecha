from __future__ import annotations

import asyncio

from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.knowledge_base import (
    DefinitionKnowledge,
    resolve_knowledge_base_path,
)
from cosecha.engine.gherkin.definition_knowledge import (
    build_gherkin_definition_record,
)
from cosecha_internal.testkit import build_config
from cosecha_lsp import lsp_server


def _run_async(coro):
    return asyncio.run(coro)


EXPECTED_PAIR_COUNT = 2
EXPECTED_ZERO_BASED_COLUMN = 2
EXPECTED_ZERO_BASED_LINE = 7


def test_start_handles_broken_pipe_and_closes_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = lsp_server.CosechaLanguageServer('cosecha-lsp-test', '0.1')
    config = build_config(tmp_path)
    close_calls: list[bool] = []
    shutdown_calls: list[bool] = []

    class _FakeRunner:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

    async def _broken_pipe(*_args, **_kwargs):
        raise BrokenPipeError

    monkeypatch.setattr(
        server,
        '_open_read_only_knowledge_base',
        lambda: SimpleNamespace(close=lambda: close_calls.append(True)),
    )
    monkeypatch.setattr(lsp_server, 'Runner', _FakeRunner)
    monkeypatch.setattr(lsp_server, 'aio_readline', _broken_pipe)
    monkeypatch.setattr(
        lsp_server,
        'StdOutTransportAdapter',
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(server.lsp, 'connection_made', lambda _transport: None)
    monkeypatch.setattr(
        server,
        'shutdown',
        lambda: shutdown_calls.append(True),
    )

    _run_async(
        server.start(
            config,
            [],
            {'gherkin': SimpleNamespace(name='gherkin')},
            stdin=BytesIO(),
            stdout=BytesIO(),
        ),
    )

    assert close_calls == [True]
    assert shutdown_calls == [True]


def test_start_handles_keyboard_interrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = lsp_server.CosechaLanguageServer('cosecha-lsp-test', '0.1')
    config = build_config(tmp_path)
    shutdown_calls: list[bool] = []

    class _FakeRunner:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

    async def _keyboard_interrupt(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(server, '_open_read_only_knowledge_base', lambda: None)
    monkeypatch.setattr(lsp_server, 'Runner', _FakeRunner)
    monkeypatch.setattr(lsp_server, 'aio_readline', _keyboard_interrupt)
    monkeypatch.setattr(
        lsp_server,
        'StdOutTransportAdapter',
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(server.lsp, 'connection_made', lambda _transport: None)
    monkeypatch.setattr(
        server,
        'shutdown',
        lambda: shutdown_calls.append(True),
    )

    _run_async(
        server.start(
            config,
            [],
            {'gherkin': SimpleNamespace(name='gherkin')},
            stdin=BytesIO(),
            stdout=BytesIO(),
        ),
    )

    assert shutdown_calls == [True]


def test_open_read_only_knowledge_base_handles_missing_file_and_open_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = lsp_server.CosechaLanguageServer('cosecha-lsp-test', '0.1')
    server.config = build_config(tmp_path)

    assert server._open_read_only_knowledge_base() is None

    db_path = resolve_knowledge_base_path(
        server.config.workspace_root_path,
        knowledge_storage_root=server.config.knowledge_storage_root_path,
    )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_text('', encoding='utf-8')
    monkeypatch.setattr(
        lsp_server,
        'ReadOnlyPersistentKnowledgeBase',
        lambda _db_path: (_ for _ in ()).throw(RuntimeError('boom')),
    )

    assert server._open_read_only_knowledge_base() is None


def test_find_gherkin_engine_rejects_non_gherkin_engines(
    tmp_path: Path,
) -> None:
    server = lsp_server.CosechaLanguageServer('cosecha-lsp-test', '0.1')
    server.config = build_config(tmp_path)
    server.runner = SimpleNamespace(find_engine=lambda _path: object())
    server.engines = {}
    server.read_only_knowledge_base = None

    with pytest.raises(TypeError, match='Invalid engine'):
        server.find_gherkin_engine((tmp_path / 'demo.feature').as_uri())


def test_find_gherkin_engine_from_knowledge_base_handles_no_matches(
    tmp_path: Path,
) -> None:
    server = lsp_server.CosechaLanguageServer('cosecha-lsp-test', '0.1')
    server.config = build_config(tmp_path)
    full_path = (tmp_path / 'demo.feature').resolve()

    server.read_only_knowledge_base = SimpleNamespace(
        query_tests=lambda _query: (),
    )
    assert server._find_gherkin_engine_from_knowledge_base(full_path) is None

    server.read_only_knowledge_base = SimpleNamespace(
        query_tests=lambda _query: (SimpleNamespace(),),
    )
    server.engines = {'pytest': SimpleNamespace(name='pytest')}
    assert server._find_gherkin_engine_from_knowledge_base(full_path) is None


def test_validate_resolve_and_suggest_return_empty_without_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = lsp_server.CosechaLanguageServer('cosecha-lsp-test', '0.1')
    document = SimpleNamespace(
        uri='file:///workspace/demo.feature',
        source='Feature: Demo\n',
        lines=['Given demo'],
    )
    monkeypatch.setattr(server, 'find_gherkin_engine', lambda _uri: None)

    assert _run_async(server.validate_gherkin_document(document)) == []
    assert _run_async(
        server.resolve_gherkin_step_definitions(
            document=document,
            step_type='given',
            step_text='demo',
        ),
    ) == ()
    assert _run_async(
        server.suggest_gherkin_step_completions(
            document=document,
            step_type='given',
            initial_text='dem',
            cursor_column=3,
            start_step_text_column=1,
        ),
    ) == ()


def test_suggest_completions_returns_empty_when_contribution_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = lsp_server.CosechaLanguageServer('cosecha-lsp-test', '0.1')
    document = SimpleNamespace(
        uri='file:///workspace/demo.feature',
        source='Given demo',
        lines=['Given demo'],
    )
    async def _execute_operation(_operation):
        return SimpleNamespace(definitions=())

    server.runner = SimpleNamespace(execute_operation=_execute_operation)
    monkeypatch.setattr(
        server,
        'find_gherkin_engine',
        lambda _uri: SimpleNamespace(name='gherkin'),
    )
    monkeypatch.setattr(
        lsp_server,
        '_get_gherkin_lsp_contribution',
        lambda: None,
    )

    assert _run_async(
        server.suggest_gherkin_step_completions(
            document=document,
            step_type='given',
            initial_text='demo',
            cursor_column=4,
            start_step_text_column=1,
        ),
    ) == ()


def test_build_resolved_definitions_skips_descriptors_without_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    definition = DefinitionKnowledge(
        engine_name='gherkin',
        file_path='steps/demo.py',
        definition_count=1,
        discovery_mode='ast',
        descriptors=(
            build_gherkin_definition_record(
                source_line=3,
                function_name='step_demo',
                step_type='given',
                patterns=('a demo',),
            ),
        ),
    )
    monkeypatch.setattr(
        lsp_server,
        'get_gherkin_payload',
        lambda _descriptor: None,
    )

    resolved = lsp_server.build_resolved_definitions_from_knowledge(
        definitions=(definition,),
        engine_name='gherkin',
        step_type='given',
        step_text='a demo',
    )

    assert resolved == ()


def test_feature_formatting_returns_empty_for_non_feature_documents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = SimpleNamespace(
        uri='file:///workspace/demo.py',
        source='print("x")',
        lines=['print("x")'],
        version=1,
    )
    monkeypatch.setattr(
        lsp_server,
        'server',
        SimpleNamespace(
            workspace=SimpleNamespace(get_text_document=lambda _uri: document),
            gherkin_edit_provider=SimpleNamespace(
                provide_document_formatting_edits=lambda _document: (),
            ),
        ),
    )

    result = lsp_server.feature_document_formatting(
        SimpleNamespace(text_document=SimpleNamespace(uri=document.uri)),
    )
    assert result == []


def test_feature_definitions_and_hover_cover_fallback_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    feature_document = SimpleNamespace(
        uri='file:///workspace/demo.feature',
        source='Given missing step',
        lines=['Given missing step'],
        version=1,
    )

    async def _resolve_many(**_kwargs):
        return (
            SimpleNamespace(
                file_path='/workspace/steps.py',
                line=2,
                column=None,
                step_type='given',
                patterns=('step one',),
            ),
            SimpleNamespace(
                file_path='/workspace/steps.py',
                line=3,
                column=None,
                step_type='given',
                patterns=('step two',),
            ),
        )

    async def _resolve_empty(**_kwargs):
        return ()

    monkeypatch.setattr(
        lsp_server,
        'server',
        SimpleNamespace(
            workspace=SimpleNamespace(
                get_text_document=lambda _uri: feature_document,
            ),
            resolve_gherkin_step_definitions=_resolve_many,
        ),
    )

    definitions = _run_async(
        lsp_server.feature_open_gherkin_step_definitions(
            SimpleNamespace(
                text_document=SimpleNamespace(uri=feature_document.uri),
                position=SimpleNamespace(line=0, character=1),
            ),
        ),
    )
    assert isinstance(definitions, list)
    assert len(definitions) == EXPECTED_PAIR_COUNT

    no_step_document = SimpleNamespace(
        uri='file:///workspace/demo.feature',
        source='Feature: Demo',
        lines=['Feature: Demo'],
        version=1,
    )
    monkeypatch.setattr(
        lsp_server.server.workspace,
        'get_text_document',
        lambda _uri: no_step_document,
    )
    assert _run_async(
        lsp_server.feature_open_gherkin_step_definitions(
            SimpleNamespace(
                text_document=SimpleNamespace(uri=no_step_document.uri),
                position=SimpleNamespace(line=0, character=0),
            ),
        ),
    ) is None

    non_feature_document = SimpleNamespace(
        uri='file:///workspace/demo.py',
        source='print("x")',
        lines=['print("x")'],
        version=1,
    )
    monkeypatch.setattr(
        lsp_server.server.workspace,
        'get_text_document',
        lambda _uri: non_feature_document,
    )
    assert _run_async(
        lsp_server.feature_hover(
            SimpleNamespace(
                text_document=SimpleNamespace(uri=non_feature_document.uri),
                position=SimpleNamespace(line=0, character=0),
            ),
        ),
    ) is None

    monkeypatch.setattr(
        lsp_server.server.workspace,
        'get_text_document',
        lambda _uri: no_step_document,
    )
    assert _run_async(
        lsp_server.feature_hover(
            SimpleNamespace(
                text_document=SimpleNamespace(uri=no_step_document.uri),
                position=SimpleNamespace(line=0, character=0),
            ),
        ),
    ) is None

    monkeypatch.setattr(
        lsp_server.server.workspace,
        'get_text_document',
        lambda _uri: feature_document,
    )
    monkeypatch.setattr(
        lsp_server.server,
        'resolve_gherkin_step_definitions',
        _resolve_empty,
    )
    assert _run_async(
        lsp_server.feature_hover(
            SimpleNamespace(
                text_document=SimpleNamespace(uri=feature_document.uri),
                position=SimpleNamespace(line=0, character=0),
            ),
        ),
    ) is None


def test_command_fallbacks_return_empty_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        lsp_server,
        '_get_gherkin_lsp_contribution',
        lambda: None,
    )

    templates = _run_async(
        lsp_server.command_get_templates(SimpleNamespace()),
    )
    assert templates == []
    assert _run_async(lsp_server.command_create_gherkin_table([2, 2])) == ''


def test_build_locations_uses_zero_based_columns() -> None:
    definitions = (
        SimpleNamespace(
            file_path='/workspace/steps.py',
            line=8,
            column=3,
        ),
    )

    locations = lsp_server.build_locations_from_resolved_definitions(
        definitions,
    )

    assert locations[0].range.start.character == EXPECTED_ZERO_BASED_COLUMN
    assert locations[0].range.start.line == EXPECTED_ZERO_BASED_LINE


def test_get_gherkin_step_info_covers_keyword_branches() -> None:
    assert lsp_server.get_gherkin_step_info(0, ['When action']) is not None
    assert lsp_server.get_gherkin_step_info(0, ['Then assertion']) is not None
    assert lsp_server.get_gherkin_step_info(0, ['But exception']) is not None
    assert lsp_server.get_gherkin_step_info(0, ['   ']) is None

    assert (
        lsp_server.get_gherkin_step_info(
            1,
            ['When action', 'And continuation'],
        ).step_type
        == 'when'
    )
    assert (
        lsp_server.get_gherkin_step_info(
            1,
            ['Then assertion', 'And continuation'],
        ).step_type
        == 'then'
    )
    assert (
        lsp_server.get_gherkin_step_info(
            1,
            ['But exception', 'And continuation'],
        ).step_type
        == 'but'
    )
    assert (
        lsp_server.get_gherkin_step_info(
            1,
            ['Feature: Demo', 'And step'],
        )
        is None
    )


def test_main_bootstraps_registry_workspace_and_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    registry = object()
    workspace = SimpleNamespace(knowledge_anchor=Path('/workspace/tests'))

    @contextmanager
    def _using_discovery_registry(current_registry):
        captured['registry'] = current_registry
        yield

    class _FakeConfig:
        def __init__(
            self,
            root_path,
            *,
            capture_log,
            workspace,
            execution_context,
        ) -> None:
            del capture_log, workspace, execution_context
            self.root_path = root_path

    async def _fake_start(config, hooks, engines):
        captured['start'] = (config, hooks, engines)

    monkeypatch.setattr(
        lsp_server,
        'create_loaded_discovery_registry',
        lambda: registry,
    )
    monkeypatch.setattr(
        lsp_server,
        'using_discovery_registry',
        _using_discovery_registry,
    )
    monkeypatch.setattr(lsp_server, 'resolve_workspace', lambda: workspace)
    monkeypatch.setattr(
        lsp_server,
        'build_execution_context',
        lambda _workspace: SimpleNamespace(execution_root=Path('/workspace')),
    )
    monkeypatch.setattr(lsp_server, 'Config', _FakeConfig)
    monkeypatch.setattr(
        lsp_server,
        'setup_engines',
        lambda _config: (
            ['hook-a'],
            {'gherkin': SimpleNamespace(name='gherkin')},
        ),
    )
    monkeypatch.setattr(lsp_server.server, 'start', _fake_start)

    def _run_until_complete(coro):
        return asyncio.run(coro)

    monkeypatch.setattr(
        lsp_server.server.loop,
        'run_until_complete',
        _run_until_complete,
    )

    lsp_server.main()

    assert captured['registry'] is registry
    assert captured['start'][1] == ['hook-a']
    assert list(captured['start'][2]) == ['gherkin']
