from __future__ import annotations

from cosecha.core.discovery import iter_shell_lsp_contributions


def test_gherkin_lsp_templates_come_from_engine_contribution() -> None:
    contributions = {
        contribution.contribution_name: contribution
        for contribution in iter_shell_lsp_contributions()
    }
    gherkin = contributions['gherkin']

    templates = gherkin.templates()
    assert templates
    assert templates[0]['label']
