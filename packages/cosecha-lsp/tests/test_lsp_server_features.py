from __future__ import annotations

import asyncio

from pathlib import Path
from types import SimpleNamespace

from lsprotocol.types import Position, TextEdit

from cosecha.core.capabilities import DraftValidationIssue
from cosecha.core.operations import ResolvedDefinition
from cosecha.engine.gherkin.completion import CompletionSuggestion
from cosecha.engine.gherkin.formatter import (
    DocumentPosition,
    DocumentRange,
    DocumentTextEdit,
)
from cosecha_lsp import lsp_server


EXPECTED_ERROR_LINE = 3


def _run_async(coro):
    return asyncio.run(coro)


def _build_document(
    *,
    uri: str,
    source: str,
) -> SimpleNamespace:
    return SimpleNamespace(
        uri=uri,
        source=source,
        lines=source.splitlines(),
        version=1,
    )


def test_uri_to_path_decodes_percent_encoded_segments() -> None:
    path = lsp_server.uri_to_path('file:///workspace/demo%20file.feature')
    assert str(path).endswith('/workspace/demo file.feature')


def test_build_diagnostics_maps_error_and_warning() -> None:
    issues = (
        DraftValidationIssue(
            code='error',
            message='error message',
            severity='error',
            line=3,
            column=2,
        ),
        DraftValidationIssue(
            code='warn',
            message='warn message',
            severity='warning',
            line=None,
            column=None,
        ),
    )

    diagnostics = lsp_server.build_diagnostics_from_draft_validation(issues)

    assert diagnostics[0].message == 'error message'
    assert diagnostics[0].range.start.line == EXPECTED_ERROR_LINE
    assert diagnostics[1].range.start.line == 0


def test_build_hover_uses_default_documentation_when_missing() -> None:
    hover = lsp_server.build_hover_from_resolved_definition(
        ResolvedDefinition(
            engine_name='gherkin',
            file_path='steps/demo.py',
            line=5,
            step_type='given',
            patterns=('a demo',),
            function_name='step_demo',
            documentation=None,
            resolution_source='static_catalog',
        ),
    )

    assert 'No documentation available.' in hover.contents.value
    assert '**Patterns:**' in hover.contents.value


def test_get_gherkin_step_info_handles_keywords_and_and_chain() -> None:
    lines = [
        'Feature: Demo',
        '  Scenario: Example',
        '    Given precondition',
        '    And secondary',
        '    # comment',
        '    | table | row |',
        '    And final',
    ]

    given = lsp_server.get_gherkin_step_info(2, lines)
    and_after_given = lsp_server.get_gherkin_step_info(3, lines)
    and_after_comment = lsp_server.get_gherkin_step_info(6, lines)

    assert given is not None
    assert given.step_type == 'given'
    assert and_after_given is not None
    assert and_after_given.step_type == 'given'
    assert and_after_comment is not None
    assert and_after_comment.step_type == 'given'


def test_get_gherkin_step_info_returns_none_for_unknown_and_orphan_and(
) -> None:
    assert lsp_server.get_gherkin_step_info(0, ['Unknown text']) is None
    assert lsp_server.get_gherkin_step_info(0, ['And orphan']) is None


def test_get_gherkin_lsp_contribution_returns_gherkin_or_none(
    monkeypatch,
) -> None:
    gherkin = SimpleNamespace(contribution_name='gherkin')
    monkeypatch.setattr(
        lsp_server,
        'iter_shell_lsp_contributions',
        lambda: (
            SimpleNamespace(contribution_name='other'),
            gherkin,
        ),
    )
    assert lsp_server._get_gherkin_lsp_contribution() is gherkin

    monkeypatch.setattr(
        lsp_server,
        'iter_shell_lsp_contributions',
        lambda: (SimpleNamespace(contribution_name='other'),),
    )
    assert lsp_server._get_gherkin_lsp_contribution() is None


def test_feature_did_open_and_save_publish_diagnostics_for_feature(
    monkeypatch,
) -> None:
    document = _build_document(
        uri='file:///workspace/demo.feature',
        source='Feature: Demo\n',
    )
    published: list[tuple[str, list[str]]] = []
    async def _validate(_document):
        return ['diagnostic']

    monkeypatch.setattr(
        lsp_server,
        'server',
        SimpleNamespace(
            workspace=SimpleNamespace(
                get_text_document=lambda _uri: document,
            ),
            validate_gherkin_document=_validate,
            publish_diagnostics=lambda uri, diagnostics: published.append(
                (uri, diagnostics),
            ),
        ),
    )

    params = SimpleNamespace(text_document=SimpleNamespace(uri=document.uri))
    _run_async(lsp_server.feature_did_open(params))
    _run_async(lsp_server.feature_did_save(params))

    assert published == [
        (document.uri, ['diagnostic']),
        (document.uri, ['diagnostic']),
    ]


def test_feature_completions_and_formatting(monkeypatch) -> None:
    feature_document = _build_document(
        uri='file:///workspace/demo.feature',
        source='Given missing step',
    )
    async def _suggest(**_kwargs):
        return (
            CompletionSuggestion(
                label='Given missing step',
                insert_text='missing step',
                kind='text',
            ),
        )

    monkeypatch.setattr(
        lsp_server,
        'server',
        SimpleNamespace(
            workspace=SimpleNamespace(
                get_text_document=lambda _uri: feature_document,
            ),
            find_gherkin_engine=lambda _uri: SimpleNamespace(name='gherkin'),
            suggest_gherkin_step_completions=_suggest,
            gherkin_edit_provider=SimpleNamespace(
                provide_document_formatting_edits=lambda _document: (),
            ),
        ),
    )
    completion_params = SimpleNamespace(
        text_document=SimpleNamespace(uri=feature_document.uri),
        position=SimpleNamespace(line=0, character=8),
    )
    completion_result = _run_async(
        lsp_server.feature_completions(completion_params),
    )
    assert completion_result.items[0].label == 'Given missing step'

    non_feature_document = _build_document(
        uri='file:///workspace/demo.py',
        source='print("x")',
    )
    monkeypatch.setattr(
        lsp_server.server.workspace,
        'get_text_document',
        lambda _uri: non_feature_document,
    )
    empty_result = _run_async(
        lsp_server.feature_completions(completion_params),
    )
    assert empty_result.items == []

    monkeypatch.setattr(
        lsp_server.server.workspace,
        'get_text_document',
        lambda _uri: feature_document,
    )
    monkeypatch.setattr(
        lsp_server.server.gherkin_edit_provider,
        'provide_document_formatting_edits',
        lambda _document: (
            DocumentTextEdit(
                range=DocumentRange(
                    start=DocumentPosition(line=0, character=0),
                    end=DocumentPosition(line=0, character=5),
                ),
                new_text='Given ',
            ),
        ),
    )
    formatting_result = lsp_server.feature_document_formatting(
        SimpleNamespace(text_document=SimpleNamespace(uri=feature_document.uri)),
    )
    assert isinstance(formatting_result[0], TextEdit)
    assert formatting_result[0].new_text == 'Given '


def test_definition_hover_and_commands(monkeypatch) -> None:
    document = _build_document(
        uri='file:///workspace/demo.feature',
        source='Given missing step',
    )
    async def _resolve_defs(**_kwargs):
        return (
            ResolvedDefinition(
                engine_name='gherkin',
                file_path=str(Path('/workspace/steps.py')),
                line=10,
                step_type='given',
                patterns=('missing step',),
                function_name='step_impl',
                documentation='doc',
                resolution_source='static_catalog',
            ),
        )

    monkeypatch.setattr(
        lsp_server,
        'server',
        SimpleNamespace(
            workspace=SimpleNamespace(
                get_text_document=lambda _uri: document,
            ),
            resolve_gherkin_step_definitions=_resolve_defs,
        ),
    )

    definition_result = _run_async(
        lsp_server.feature_open_gherkin_step_definitions(
            SimpleNamespace(
                text_document=SimpleNamespace(uri=document.uri),
                position=Position(line=0, character=1),
            ),
        ),
    )
    assert definition_result is not None

    hover_result = _run_async(
        lsp_server.feature_hover(
            SimpleNamespace(
                text_document=SimpleNamespace(uri=document.uri),
                position=Position(line=0, character=1),
            ),
        ),
    )
    assert hover_result is not None
    assert 'missing step' in hover_result.contents.value

    monkeypatch.setattr(
        lsp_server,
        '_get_gherkin_lsp_contribution',
        lambda: SimpleNamespace(
            templates=lambda: ('template-a', 'template-b'),
            generate_data_table=lambda rows, columns: (
                f'rows={rows},columns={columns}'
            ),
        ),
    )
    assert _run_async(lsp_server.command_get_templates(SimpleNamespace())) == [
        'template-a',
        'template-b',
    ]
    assert _run_async(lsp_server.command_create_gherkin_table([2, 3])) == (
        'rows=2,columns=3'
    )
    assert _run_async(lsp_server.command_create_gherkin_table(['x'])) == (
        'Invalid arguments. Please provide rows and columns as integers.'
    )
