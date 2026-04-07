from __future__ import annotations

from cosecha.engine.gherkin.lsp import GHERKIN_TEMPLATES


def test_gherkin_templates_shape_and_snippets() -> None:
    assert len(GHERKIN_TEMPLATES) == 2
    first, second = GHERKIN_TEMPLATES

    assert first['insertTextFormat'] == 2
    assert '[Commands][F]' in first['label']
    assert 'Feature: Successful execution' in first['insertText']
    assert 'Then the "${2:my_sweet}" command should trigger an event' in first[
        'insertText'
    ]

    assert second['insertTextFormat'] == 2
    assert '[ViewModels][F]' in second['label']
    assert 'Feature: Verify "${1:MySweetViewModel}" execution outcome' in second[
        'insertText'
    ]
    assert 'Then the "${2:my_sweet}" viewmodel result should have' in second[
        'insertText'
    ]

