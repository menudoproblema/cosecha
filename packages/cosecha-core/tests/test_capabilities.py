from __future__ import annotations

from pathlib import Path

from cosecha.core.capabilities import (
    CAPABILITY_API_VERSION,
    CapabilityAttribute,
    CapabilityComponentSnapshot,
    CapabilityDescriptor,
    CapabilityOperationBinding,
    CapabilityDescribingComponent,
    DefinitionResolvingEngine,
    DraftValidatingEngine,
    DraftValidationIssue,
    DraftValidationResult,
    ExplainablePlanner,
    ProjectDefinitionKnowledgeEngine,
    build_capability_map,
    build_component_capability_snapshot,
)


def test_capability_dataclasses_roundtrip_and_validation_status() -> None:
    issue_warning = DraftValidationIssue(
        code='warn-1',
        message='warning',
        severity='warning',
        line=3,
        column=4,
    )
    issue_error = DraftValidationIssue(
        code='err-1',
        message='error',
        severity='error',
    )
    result_warning_only = DraftValidationResult(
        test_count=1,
        required_step_texts=(('given', 'a step'),),
        step_candidate_files=('steps.py',),
        issues=(issue_warning,),
    )
    result_with_error = DraftValidationResult(
        test_count=2,
        issues=(issue_warning, issue_error),
    )

    assert DraftValidationIssue.from_dict(issue_warning.to_dict()) == issue_warning
    assert (
        DraftValidationResult.from_dict(result_warning_only.to_dict())
        == result_warning_only
    )
    assert result_warning_only.is_valid is True
    assert result_with_error.is_valid is False


def test_capability_descriptor_and_snapshot_builders_roundtrip() -> None:
    descriptor = CapabilityDescriptor(
        name='live_execution_observability',
        level='supported',
        summary='Expose runtime events and logs',
        attributes=(
            CapabilityAttribute(
                name='granularity',
                value='streaming',
            ),
            CapabilityAttribute(
                name='live_channels',
                value=('events', 'logs'),
            ),
        ),
        operations=(
            CapabilityOperationBinding(
                operation_type='execution.live_status',
                result_type='execution.live_status',
                freshness='fresh',
            ),
        ),
        delivery_mode='poll_by_cursor',
        granularity='streaming',
    )
    duplicate = CapabilityDescriptor(
        name='live_execution_observability',
        level='accepted_noop',
    )
    snapshot = build_component_capability_snapshot(
        component_name='runtime.process',
        component_kind='runtime',
        descriptors=(descriptor, duplicate),
    )

    assert CapabilityAttribute.from_dict(
        descriptor.attributes[0].to_dict(),
    ) == descriptor.attributes[0]
    assert CapabilityOperationBinding.from_dict(
        descriptor.operations[0].to_dict(),
    ) == descriptor.operations[0]
    assert CapabilityDescriptor.from_dict(descriptor.to_dict()) == descriptor
    assert (
        CapabilityComponentSnapshot.from_dict(snapshot.to_dict())
        == snapshot
    )
    assert snapshot.api_version == CAPABILITY_API_VERSION
    assert build_capability_map((descriptor, duplicate)) == {
        'live_execution_observability': duplicate,
    }


def test_runtime_checkable_capability_protocols() -> None:
    class _DraftEngine:
        async def validate_draft(
            self,
            source_content: str,
            test_path: Path,
        ) -> DraftValidationResult:
            del source_content, test_path
            return DraftValidationResult(test_count=0)

    class _DefinitionEngine:
        async def resolve_definition(
            self,
            *,
            test_path: Path,
            step_type: str,
            step_text: str,
        ):
            del test_path, step_type, step_text
            return ()

    class _KnowledgeEngine:
        def get_project_definition_index(self) -> object:
            return object()

    class _Planner:
        def build_execution_plan_analysis(
            self,
            *paths: Path,
            mode: str = 'strict',
        ) -> object:
            del paths, mode
            return object()

        def explain_execution_plan(
            self,
            *paths: Path,
            mode: str = 'relaxed',
        ) -> object:
            del paths, mode
            return object()

    class _Component:
        def describe_capabilities(self) -> tuple[CapabilityDescriptor, ...]:
            return ()

    assert isinstance(_DraftEngine(), DraftValidatingEngine)
    assert isinstance(_DefinitionEngine(), DefinitionResolvingEngine)
    assert isinstance(_KnowledgeEngine(), ProjectDefinitionKnowledgeEngine)
    assert isinstance(_Planner(), ExplainablePlanner)
    assert isinstance(_Component(), CapabilityDescribingComponent)
