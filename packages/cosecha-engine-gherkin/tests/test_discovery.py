from __future__ import annotations

from types import SimpleNamespace

import pytest

from cosecha.core.cosecha_manifest import ManifestValidationError
from cosecha.core.items import TestResultStatus
from cosecha.core.manifest_symbols import SymbolRef
from cosecha.core.manifest_types import (
    EngineSpec,
    RegistryLayoutSpec,
    RegistryLoaderSpec,
    ResourceBindingSpec,
)
from cosecha.core.reporting_ir import TestReport
from cosecha.engine.gherkin import GherkinEngine
from cosecha.engine.gherkin.definition_knowledge import (
    build_gherkin_definition_record,
)
from cosecha.engine.gherkin.discovery import (
    GherkinConsolePresenter,
    GherkinDefinitionKnowledgeQueryProvider,
    GherkinEngineDescriptor,
    GherkinLspContribution,
    _import_gherkin_completion,
    _import_gherkin_lsp,
    _status_to_tone,
)


def test_validate_resource_binding_requires_layout_and_alias() -> None:
    with pytest.raises(
        ManifestValidationError,
        match='Gherkin resource bindings require layout and alias',
    ):
        GherkinEngineDescriptor.validate_resource_binding(
            SimpleNamespace(
                layout=None,
                alias='db',
                resource_name='database',
            ),
            manifest=None,
        )

    with pytest.raises(
        ManifestValidationError,
        match='Gherkin resource bindings require layout and alias',
    ):
        GherkinEngineDescriptor.validate_resource_binding(
            SimpleNamespace(
                layout='resource',
                alias=None,
                resource_name='database',
            ),
            manifest=None,
        )

    assert (
        GherkinEngineDescriptor.validate_resource_binding(
            SimpleNamespace(
                layout='resource',
                alias='db',
                resource_name='database',
            ),
            manifest=None,
        )
        is None
    )


def test_materialize_builds_gherkin_engine_with_resolved_bindings_and_paths(
    tmp_path,
    monkeypatch,
) -> None:
    module_path = tmp_path / 'demo_coercions.py'
    module_path.write_text(
        '\n'.join(
            (
                'class BaseHelper:',
                '    pass',
                '',
                'def parse_upper(value, _location):',
                '    return value.upper()',
            ),
        ),
        encoding='utf-8',
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    engine_spec = EngineSpec(
        id='gherkin',
        type='gherkin',
        name='gherkin',
        path='features',
        step_library_modules=('demo_steps',),
        definition_paths=('steps', str((tmp_path / 'abs_steps').resolve())),
        coercions=(
            (
                'upper',
                SymbolRef.parse('demo_coercions:parse_upper'),
            ),
        ),
        registry_loaders=(
            RegistryLoaderSpec(
                layouts=(
                    RegistryLayoutSpec(
                        name='helper',
                        base=SymbolRef.parse('demo_coercions:BaseHelper'),
                        module_globs=('demo_coercions',),
                        match='subclass',
                    ),
                ),
            ),
        ),
    )
    config = SimpleNamespace(root_path=tmp_path)
    manifest = SimpleNamespace(
        manifest_dir=tmp_path,
        resource_bindings=(
            ResourceBindingSpec(
                engine_type='gherkin',
                resource_name='database',
                layout='resource',
                alias='db',
            ),
            ResourceBindingSpec(
                engine_type='pytest',
                resource_name='cache',
                layout='resource',
                alias='cache',
            ),
        ),
    )
    shared_requirements = (SimpleNamespace(interface_name='database'),)

    engine = GherkinEngineDescriptor.materialize(
        engine_spec,
        manifest=manifest,
        config=config,
        active_profiles=(),
        shared_requirements=shared_requirements,
    )

    assert isinstance(engine, GherkinEngine)
    assert engine.coercions['upper']('demo', None) == 'DEMO'
    assert engine.shared_resource_requirements == shared_requirements
    assert engine.resource_bindings == (manifest.resource_bindings[0],)
    assert engine.definition_path_overrides[0] == (tmp_path / 'steps').resolve()
    assert engine.definition_path_overrides[1] == (
        tmp_path / 'abs_steps'
    ).resolve()
    assert len(engine.hooks) == 1
    assert engine.hooks[0].step_library_modules == ('demo_steps',)
    assert len(engine.hooks[0].registry_loaders) == 1


def test_lsp_contribution_and_definition_query_provider_forward_calls(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        'cosecha.engine.gherkin.discovery._import_gherkin_lsp',
        lambda: SimpleNamespace(GHERKIN_TEMPLATES=({'name': 'template'},)),
    )
    monkeypatch.setattr(
        'cosecha.engine.gherkin.discovery._import_gherkin_completion',
        lambda: SimpleNamespace(
            build_step_completion_suggestions_from_knowledge=lambda **kwargs: (
                kwargs['step_type'],
                kwargs['initial_text'],
            ),
            generate_gherkin_data_table=lambda rows, columns: (
                f'{rows}x{columns}'
            ),
        ),
    )

    assert GherkinLspContribution.templates() == ({'name': 'template'},)
    assert (
        GherkinLspContribution.build_step_completion_suggestions_from_knowledge(
            definitions=(),
            step_type='given',
            initial_text='a',
        )
        == ('given', 'a')
    )
    assert GherkinLspContribution.generate_data_table(2, 3) == '2x3'

    given_descriptor = build_gherkin_definition_record(
        source_line=1,
        function_name='step_given',
        step_type='given',
        patterns=('a user',),
    )
    then_descriptor = build_gherkin_definition_record(
        source_line=2,
        function_name='step_then',
        step_type='then',
        patterns=('result is ok',),
    )

    matches = GherkinDefinitionKnowledgeQueryProvider.matching_descriptors(
        (given_descriptor, then_descriptor),
        step_type='given',
        step_text='a user',
    )
    assert matches == (given_descriptor,)


def test_console_presenter_builds_components_and_accumulates_summary() -> None:
    report = TestReport(
        path='features/payments.feature',
        status=TestResultStatus.FAILED,
        message='assertion failed',
        duration=0.1,
        failure_kind='assertion',
        engine_name='gherkin',
        engine_payload={
            'feature': {
                'name': 'Payments',
                'location': {'line': 1, 'text': 'features/payments.feature:1'},
            },
            'scenario': {
                'name': 'Pay card',
                'location': {'line': 5, 'text': 'features/payments.feature:5'},
            },
            'step_result_list': [
                {
                    'status': 'passed',
                    'message': None,
                    'exception_text': None,
                    'step': {
                        'keyword': 'Given ',
                        'text': 'a user',
                        'location': {'line': 6, 'text': 'features/payments.feature:6'},
                        'implementation_location': None,
                    },
                },
                {
                    'status': 'failed',
                    'message': 'boom',
                    'exception_text': 'trace',
                    'step': {
                        'keyword': 'When ',
                        'text': 'the card fails',
                        'location': {'line': 7, 'text': 'features/payments.feature:7'},
                        'implementation_location': {
                            'line': 70,
                            'text': 'steps/payment.py:70',
                        },
                    },
                },
            ],
        },
    )

    title = GherkinConsolePresenter.build_case_title(report, config=None)
    components = GherkinConsolePresenter.build_console_components(
        report,
        config=None,
    )
    detail_counts: dict[str, int] = {}
    feature_keys: set[str] = set()
    failed_feature_keys: set[str] = set()
    GherkinConsolePresenter.accumulate_engine_summary(
        report,
        detail_counts=detail_counts,
        feature_keys=feature_keys,
        failed_feature_keys=failed_feature_keys,
    )

    assert ''.join(span.text for span in title).startswith('Scenario: Pay card')
    assert len(components) == 5
    assert components[0].spans[0].text == 'Feature: '
    assert components[1].spans[0].text == 'Failure kind: '
    assert components[2].badge is not None
    assert components[2].badge.tone == 'success'
    assert components[3].badge is not None
    assert components[3].badge.tone == 'error'
    assert components[4].indent == 2
    assert detail_counts == {
        'scenarios.total': 1,
        'scenarios.failed': 1,
        'steps.total': 2,
        'steps.passed': 1,
        'steps.failed': 1,
    }
    assert feature_keys == {'features/payments.feature:1'}
    assert failed_feature_keys == {'features/payments.feature:1'}


def test_import_helpers_and_status_tone_mapping(monkeypatch) -> None:
    imported_modules: list[str] = []
    monkeypatch.setattr(
        'cosecha.engine.gherkin.discovery.importlib.import_module',
        lambda name: imported_modules.append(name) or SimpleNamespace(name=name),
    )

    assert _import_gherkin_completion().name == 'cosecha.engine.gherkin.completion'
    assert _import_gherkin_lsp().name == 'cosecha.engine.gherkin.lsp'
    assert imported_modules == [
        'cosecha.engine.gherkin.completion',
        'cosecha.engine.gherkin.lsp',
    ]
    assert _status_to_tone(TestResultStatus.PASSED) == 'success'
    assert _status_to_tone(TestResultStatus.SKIPPED) == 'warning'
    assert _status_to_tone(TestResultStatus.FAILED) == 'error'
    assert _status_to_tone(TestResultStatus.ERROR) == 'error'
    assert _status_to_tone(TestResultStatus.PENDING) == 'muted'
