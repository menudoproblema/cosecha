from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class DocumentPosition:
    line: int
    character: int


@dataclass(slots=True, frozen=True)
class DocumentRange:
    start: DocumentPosition
    end: DocumentPosition


@dataclass(slots=True, frozen=True)
class DocumentTextEdit:
    range: DocumentRange
    new_text: str


@dataclass(slots=True, frozen=True)
class PlainTextDocument:
    uri: str
    source: str
    version: int = 1

    @property
    def lines(self) -> list[str]:
        return self.source.splitlines(keepends=True)


class GherkinDocumentFormattingEditProvider:
    @staticmethod
    def _parse_table_line(line_text: str) -> list[str]:
        stripped_line = line_text.lstrip()
        return [cell.strip() for cell in stripped_line.split('|')[1:-1]]

    @staticmethod
    def _format_table_line(
        indent: int,
        cells: list[str],
        max_widths: list[int],
    ) -> str:
        formatted_cells = [
            cell.ljust(max_widths[i]) for i, cell in enumerate(cells)
        ]
        return ' ' * indent + '| ' + ' | '.join(formatted_cells) + ' |'

    def create_indentation_edit(
        self,
        line: str,
        line_number: int,
        expected_indents: int,
        spaces_per_indent: int = 2,
    ) -> DocumentTextEdit | None:
        expected_indentation = expected_indents * spaces_per_indent
        stripped_line = line.lstrip(' ')
        content_without_indentation = stripped_line.rstrip('\n')
        current_indentation = len(line) - len(stripped_line)
        if current_indentation == expected_indentation:
            return None

        new_line_content = (
            ' ' * expected_indentation
        ) + content_without_indentation
        if line.endswith('\n'):
            new_line_content += '\n'

        start_position = DocumentPosition(line=line_number, character=0)
        end_position = DocumentPosition(
            line=line_number,
            character=len(line),
        )
        return DocumentTextEdit(
            range=DocumentRange(start=start_position, end=end_position),
            new_text=new_line_content.rstrip('\n'),
        )

    def add_initial_content_line_edit(
        self,
        line: str,
        line_number: int,
        char: str = '\n',
    ) -> DocumentTextEdit:
        start_position = DocumentPosition(line=line_number, character=0)
        end_position = DocumentPosition(
            line=line_number,
            character=len(line),
        )
        return DocumentTextEdit(
            range=DocumentRange(start=start_position, end=end_position),
            new_text=char + line.rstrip('\n'),
        )

    def format_table(
        self,
        document: PlainTextDocument,
        table_lines: list[list[str]],
        max_widths: list[int],
        start_index: int,
    ) -> list[DocumentTextEdit]:
        edits: list[DocumentTextEdit] = []
        table_padding = 6
        lines = document.lines
        for line_index, cells in enumerate(table_lines):
            formatted_line = self._format_table_line(
                table_padding,
                cells,
                max_widths,
            )
            start_position = DocumentPosition(
                line=start_index + line_index,
                character=0,
            )
            end_position = DocumentPosition(
                line=start_index + line_index,
                character=len(lines[start_index + line_index]),
            )
            edits.append(
                DocumentTextEdit(
                    range=DocumentRange(
                        start=start_position,
                        end=end_position,
                    ),
                    new_text=formatted_line,
                ),
            )
        return edits

    def provide_document_formatting_edits(  # noqa: PLR0912, PLR0915
        self,
        document: PlainTextDocument,
    ) -> list[DocumentTextEdit]:
        edits: list[DocumentTextEdit] = []
        in_table = False
        table_lines: list[list[str]] = []
        max_widths: list[int] = []
        table_start_index = 0
        prev_line: str | None = None

        for line_number, original_line in enumerate(document.lines):
            line_text = original_line
            clean_line = line_text.strip(' \t')
            if not clean_line.strip('\n'):
                if line_text:
                    start_position = DocumentPosition(
                        line=line_number,
                        character=0,
                    )
                    end_position = DocumentPosition(
                        line=line_number,
                        character=len(line_text),
                    )
                    edits.append(
                        DocumentTextEdit(
                            range=DocumentRange(
                                start=start_position,
                                end=end_position,
                            ),
                            new_text=clean_line.rstrip('\n'),
                        ),
                    )
            elif clean_line.startswith('|'):
                if not in_table:
                    table_start_index = line_number
                    in_table = True
                cells = self._parse_table_line(line_text)
                table_lines.append(cells)
                if len(max_widths) < len(cells):
                    max_widths.extend([0] * (len(cells) - len(max_widths)))
                for index, cell in enumerate(cells):
                    max_widths[index] = max(max_widths[index], len(cell))
            else:
                expected_indents = None
                add_extra_line = False
                needs_edit = False

                if clean_line.startswith('Feature:'):
                    expected_indents = 0
                    keyword = 'Feature:'
                    keyword_len = len(keyword)
                    if (
                        len(clean_line.strip()) > keyword_len
                        and clean_line[keyword_len] != ' '
                    ):
                        needs_edit = True
                        pos = line_text.find(keyword)
                        line_text = (
                            line_text[:pos]
                            + keyword
                            + ' '
                            + line_text[pos + keyword_len :]
                        )
                elif clean_line.startswith('Background:'):
                    expected_indents = 1
                    if prev_line and prev_line.strip('\n'):
                        add_extra_line = True
                    keyword = 'Background:'
                    keyword_len = len(keyword)
                    if (
                        len(clean_line.strip()) > keyword_len
                        and clean_line[keyword_len] != ' '
                    ):
                        needs_edit = True
                        pos = line_text.find(keyword)
                        line_text = (
                            line_text[:pos]
                            + keyword
                            + ' '
                            + line_text[pos + keyword_len :]
                        )
                elif clean_line.startswith('Scenario:'):
                    expected_indents = 1
                    if prev_line and (
                        prev_line.strip('\n')
                        and not prev_line.strip(' \t').startswith('@')
                    ):
                        add_extra_line = True
                    keyword = 'Scenario:'
                    keyword_len = len(keyword)
                    if (
                        len(clean_line.strip()) > keyword_len
                        and clean_line[keyword_len] != ' '
                    ):
                        needs_edit = True
                        pos = line_text.find(keyword)
                        line_text = (
                            line_text[:pos]
                            + keyword
                            + ' '
                            + line_text[pos + keyword_len :]
                        )
                elif clean_line.startswith('Scenario Outline:'):
                    expected_indents = 1
                    if prev_line and (
                        prev_line.strip('\n')
                        and not prev_line.strip(' \t').startswith('@')
                    ):
                        add_extra_line = True
                elif clean_line.startswith('Examples:'):
                    expected_indents = 2
                    if prev_line and prev_line.strip('\n'):
                        add_extra_line = True
                elif clean_line.startswith(
                    ('Given ', 'When ', 'Then ', 'And ', 'But '),
                ):
                    expected_indents = 2
                elif clean_line.startswith('@'):
                    expected_indents = 1

                if in_table:
                    edits.extend(
                        self.format_table(
                            document,
                            table_lines,
                            max_widths,
                            table_start_index,
                        ),
                    )
                    in_table = False
                    table_lines = []
                    max_widths = []

                if add_extra_line:
                    edits.append(
                        self.add_initial_content_line_edit(
                            line_text,
                            line_number,
                        ),
                    )

                if expected_indents is not None:
                    edit = self.create_indentation_edit(
                        line_text,
                        line_number,
                        expected_indents,
                    )
                    if edit is not None:
                        edits.append(edit)
                    elif needs_edit:
                        start_position = DocumentPosition(
                            line=line_number,
                            character=0,
                        )
                        end_position = DocumentPosition(
                            line=line_number,
                            character=len(document.lines[line_number]),
                        )
                        edits.append(
                            DocumentTextEdit(
                                range=DocumentRange(
                                    start=start_position,
                                    end=end_position,
                                ),
                                new_text=line_text.rstrip('\n'),
                            ),
                        )
            prev_line = line_text

        if in_table:
            edits.extend(
                self.format_table(
                    document,
                    table_lines,
                    max_widths,
                    table_start_index,
                ),
            )

        return edits


__all__ = (
    'DocumentPosition',
    'DocumentRange',
    'DocumentTextEdit',
    'GherkinDocumentFormattingEditProvider',
    'PlainTextDocument',
)
