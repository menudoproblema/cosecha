from __future__ import annotations

from cosecha.engine.gherkin.formatter import (
    DocumentPosition,
    DocumentRange,
    DocumentTextEdit,
    GherkinDocumentFormattingEditProvider,
    PlainTextDocument,
)


def test_document_models_and_plain_text_document_lines() -> None:
    position = DocumentPosition(line=1, character=2)
    assert position.line == 1
    assert position.character == 2

    range_ = DocumentRange(start=position, end=DocumentPosition(3, 4))
    edit = DocumentTextEdit(range=range_, new_text='updated')
    assert edit.range.start == position
    assert edit.new_text == 'updated'

    document = PlainTextDocument(uri='file:///demo.feature', source='a\nb\n')
    assert document.version == 1
    assert document.lines == ['a\n', 'b\n']


def test_formatting_provider_low_level_helpers() -> None:
    provider = GherkinDocumentFormattingEditProvider()

    assert provider._parse_table_line('   | a | bb |') == ['a', 'bb']  # noqa: SLF001
    assert provider._format_table_line(2, ['a', 'bb'], [2, 3]) == '  | a  | bb  |'  # noqa: SLF001

    assert provider.create_indentation_edit('  Given x\n', 4, 1) is None
    changed = provider.create_indentation_edit(' Given x\n', 4, 2)
    assert changed is not None
    assert changed.new_text == '    Given x'

    inserted = provider.add_initial_content_line_edit('  Scenario: x\n', 2)
    assert inserted.new_text == '\n  Scenario: x'

    document = PlainTextDocument(
        uri='file:///demo.feature',
        source='      | a | bb |\n      | x | y |\n',
    )
    table_edits = provider.format_table(
        document,
        [['a', 'bb'], ['x', 'y']],
        [1, 2],
        0,
    )
    assert [edit.new_text for edit in table_edits] == [
        '      | a | bb |',
        '      | x | y  |',
    ]


def test_provide_document_formatting_edits_covers_keywords_tables_and_spacing() -> None:
    provider = GherkinDocumentFormattingEditProvider()
    document = PlainTextDocument(
        uri='file:///demo.feature',
        source=(
            'Feature:Demo\n'
            'Background:Init\n'
            ' Given setup\n'
            'Scenario:Smoke\n'
            ' Given run\n'
            'Examples:\n'
            ' |a|bb|\n'
            ' |x|y|\n'
            '  \n'
            '@tag\n'
            ' Scenario Outline:Outline\n'
            '  Given value <id>\n'
            'Then done\n'
        ),
    )

    edits = provider.provide_document_formatting_edits(document)
    edit_texts = [edit.new_text for edit in edits]
    edited_lines = {edit.range.start.line: edit.new_text for edit in edits}

    assert edited_lines[0] == 'Feature: Demo'
    assert '\nBackground: Init' in edit_texts
    assert '\nScenario: Smoke' in edit_texts
    assert '\nExamples:' in edit_texts
    assert edited_lines[1] == '  Background: Init'
    assert edited_lines[3] == '  Scenario: Smoke'
    assert edited_lines[5] == '    Examples:'
    assert edited_lines[6] == '      | a | bb |'
    assert edited_lines[7] == '      | x | y  |'
    assert edited_lines[8] == ''
    assert edited_lines[9] == '  @tag'
    assert edited_lines[10] == '  Scenario Outline:Outline'
    assert edited_lines[11] == '    Given value <id>'
    assert edited_lines[12] == '    Then done'


def test_provide_document_formatting_edits_flushes_trailing_table_and_outline_spacing() -> None:
    provider = GherkinDocumentFormattingEditProvider()
    document = PlainTextDocument(
        uri='file:///outline.feature',
        source=(
            'Feature: X\n'
            'Scenario Outline:Y\n'
            'Given step\n'
            'Examples:\n'
            '|a|b|\n'
            '|1|2|\n'
        ),
    )

    edits = provider.provide_document_formatting_edits(document)
    edit_texts = [edit.new_text for edit in edits]

    assert '\nScenario Outline:Y' in edit_texts
    assert '      | a | b |' in edit_texts
    assert '      | 1 | 2 |' in edit_texts


def test_provide_document_formatting_edits_keeps_feature_tags_at_root_level() -> None:
    provider = GherkinDocumentFormattingEditProvider()
    document = PlainTextDocument(
        uri='file:///tags.feature',
        source=(
            '  @system @mongodb\n'
            ' @requires:core/system\n'
            'Feature: Demo\n'
        ),
    )

    edits = provider.provide_document_formatting_edits(document)
    edited_lines = {edit.range.start.line: edit.new_text for edit in edits}

    assert edited_lines[0] == '@system @mongodb'
    assert edited_lines[1] == '@requires:core/system'
    assert 2 not in edited_lines
