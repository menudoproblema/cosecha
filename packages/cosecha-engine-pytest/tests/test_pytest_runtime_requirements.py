from __future__ import annotations

import ast

from pathlib import Path

from cosecha.core.runtime_profiles import RuntimeModeRequirement
from cosecha.engine.pytest.collector import _extract_runtime_requirements
from cosecha.engine.pytest.items import PytestTestDefinition, PytestTestItem


def test_extract_runtime_requirements_collects_required_modes_with_inheritance(
) -> None:
    module = ast.parse(
        '\n'.join(
            (
                'import pytest',
                '',
                '@pytest.mark.requires_mode("application/http", "asgi")',
                'class TestDemo:',
                '    @pytest.mark.requires_mode(',
                '        "application/http",',
                '        "asgi",',
                '    )',
                '    @pytest.mark.disallow_mode(',
                '        "application/http",',
                '        "wsgi",',
                '    )',
                '    def test_case(self):',
                '        pass',
            ),
        ),
    )
    class_node = module.body[1]
    assert isinstance(class_node, ast.ClassDef)
    test_node = class_node.body[0]
    assert isinstance(test_node, ast.FunctionDef)

    (
        _class_interfaces,
        _class_capabilities,
        class_required_modes,
        _class_disallowed_modes,
    ) = _extract_runtime_requirements(
        class_node.decorator_list,
        marker_aliases=('pytest',),
    )
    (
        interfaces,
        capabilities,
        required_modes,
        disallowed_modes,
    ) = _extract_runtime_requirements(
        test_node.decorator_list,
        marker_aliases=('pytest',),
        inherited_required_modes=class_required_modes,
    )

    assert interfaces == ()
    assert capabilities == ()
    assert required_modes == (('application/http', 'asgi'),)
    assert disallowed_modes == (('application/http', 'wsgi'),)


def test_pytest_item_maps_required_modes_to_runtime_requirement_set() -> None:
    definition = PytestTestDefinition(
        function_name='test_case',
        line=10,
        required_runtime_modes=(('application/http', 'asgi'),),
    )
    item = PytestTestItem(
        module_path=Path('tests/test_demo.py'),
        definition=definition,
        root_path=Path(),
    )

    requirements = item.get_runtime_requirement_set()

    assert requirements.required_modes == (
        RuntimeModeRequirement(
            interface_name='application/http',
            mode_name='asgi',
        ),
    )
