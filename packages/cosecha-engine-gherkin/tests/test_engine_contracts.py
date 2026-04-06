from __future__ import annotations

from typing import TYPE_CHECKING

from cosecha.core.capabilities import (
    CAPABILITY_DRAFT_VALIDATION,
    CAPABILITY_LAZY_PROJECT_DEFINITION_LOADING,
    CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE,
    CAPABILITY_PLAN_EXPLANATION,
    CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE,
    CAPABILITY_PROJECT_REGISTRY_KNOWLEDGE,
    CAPABILITY_SELECTION_LABELS,
    CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY,
    build_capability_map,
)
from cosecha.engine.gherkin import GherkinEngine
from cosecha.engine.pytest import PytestEngine
from cosecha_internal.testkit import DummyReporter, build_config


if TYPE_CHECKING:
    from pathlib import Path


def test_gherkin_engine_describes_supported_capabilities(
    tmp_path: Path,
) -> None:
    engine = GherkinEngine('gherkin', reporter=DummyReporter())
    engine.initialize(build_config(tmp_path), '')

    capability_map = build_capability_map(engine.describe_capabilities())
    selection_labels = capability_map[CAPABILITY_SELECTION_LABELS]

    assert CAPABILITY_DRAFT_VALIDATION in capability_map
    assert CAPABILITY_SELECTION_LABELS in capability_map
    assert CAPABILITY_PROJECT_DEFINITION_KNOWLEDGE in capability_map
    assert CAPABILITY_PROJECT_REGISTRY_KNOWLEDGE in capability_map
    assert CAPABILITY_PLAN_EXPLANATION in capability_map
    assert CAPABILITY_LAZY_PROJECT_DEFINITION_LOADING in capability_map
    assert CAPABILITY_STATIC_PROJECT_DEFINITION_DISCOVERY in capability_map
    assert CAPABILITY_LIBRARY_DEFINITION_KNOWLEDGE in capability_map
    assert selection_labels.level == 'supported'
    assert {
        attribute.name: attribute.value
        for attribute in selection_labels.attributes
    } == {
        'label_sources': ('feature_tag', 'scenario_tag'),
        'supports_glob_matching': True,
    }
    assert {
        operation.operation_type
        for operation in selection_labels.operations
    } == {
        'run',
        'plan.analyze',
        'plan.explain',
        'plan.simulate',
    }


def test_gherkin_and_pytest_engines_publish_cross_engine_dependency_rules(
    tmp_path: Path,
) -> None:
    gherkin_engine = GherkinEngine('gherkin', reporter=DummyReporter())
    pytest_engine = PytestEngine('pytest', reporter=DummyReporter())
    config = build_config(tmp_path)
    gherkin_engine.initialize(config, '')
    pytest_engine.initialize(config, '')

    gherkin_rules = gherkin_engine.describe_engine_dependencies()
    pytest_rules = pytest_engine.describe_engine_dependencies()

    assert {rule.target_engine_name for rule in gherkin_rules} == {'pytest'}
    assert {rule.dependency_kind for rule in gherkin_rules} == {
        'execution',
        'knowledge',
    }
    assert {rule.projection_policy for rule in gherkin_rules} == {
        'degrade_to_explain',
        'diagnostic_only',
    }
    assert any(
        'knowledge.query_tests' in rule.operation_types
        for rule in gherkin_rules
    )

    assert {rule.target_engine_name for rule in pytest_rules} == {'gherkin'}
    assert {rule.dependency_kind for rule in pytest_rules} == {
        'execution',
        'knowledge',
    }
    assert {rule.projection_policy for rule in pytest_rules} == {
        'degrade_to_explain',
        'diagnostic_only',
    }
    assert any(
        'plan.explain' in rule.operation_types for rule in pytest_rules
    )
