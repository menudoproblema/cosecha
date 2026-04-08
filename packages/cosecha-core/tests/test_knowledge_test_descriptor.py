from __future__ import annotations

from cosecha.core.knowledge_test_descriptor import (
    TestDescriptorKnowledge as DescriptorKnowledge,
)


def test_test_descriptor_knowledge_roundtrip() -> None:
    descriptor = DescriptorKnowledge(
        stable_id='stable-1',
        test_name='Scenario: auth',
        file_path='features/auth.feature',
        source_line=3,
        selection_labels=('api', 'smoke'),
    )

    assert DescriptorKnowledge.from_dict(descriptor.to_dict()) == descriptor
