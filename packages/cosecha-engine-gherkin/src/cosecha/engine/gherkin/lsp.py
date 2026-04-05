from __future__ import annotations


GHERKIN_TEMPLATES = (
    {
        'label': '[Commands][F] Successfully execution',
        'detail': (
            'Creates a feature for the successful execution of a command'
        ),
        'insertText': '\n'.join(
            (
                '@requires:core/system @requires:database/mongodb',
                'Feature: Successful execution of "${1:MySweetCommand}"',
                '',
                '  Scenario: Executes successfully',
                (
                    '    Given the command "${1:MySweetCommand}" named as '
                    '"${2:my_sweet}" is prepared with the following '
                    'parameters'
                ),
                '      | name          | description |',
                '      |               |             |',
                '    When the "${2:my_sweet}" command is executed',
                '    Then the "${2:my_sweet}" command should trigger an event',
                '',
            ),
        ),
        'insertTextFormat': 2,
    },
    {
        'label': '[ViewModels][F] ViewModel execution without expand',
        'detail': (
            'Creates a feature for the successful execution of a viewmodel'
        ),
        'insertText': '\n'.join(
            (
                '@requires:database/mongodb',
                'Feature: Verify "${1:MySweetViewModel}" execution outcome',
                '',
                '  Scenario: Returns expected result without input',
                (
                    '    Given the viewmodel "${1:MySweetViewModel}" named '
                    'as "${2:my_sweet}" is prepared without expands'
                ),
                '    When the "${2:my_sweet}" viewmodel is executed',
                '    Then the "${2:my_sweet}" viewmodel result should have',
                '      | name          | description |',
                '      |               |             |',
                '',
            ),
        ),
        'insertTextFormat': 2,
    },
)
