from __future__ import annotations

from pathlib import Path

import pytest

from cosecha.core.knowledge_test_descriptor import (
    TestDescriptorKnowledge as DescriptorKnowledge,
)
from cosecha.core.serialization import (
    decode_json,
    decode_json_dict,
    decode_json_list,
    encode_json_bytes,
    encode_json_text,
    encode_json_text_lossy,
    from_builtins,
    from_builtins_dict,
    to_builtins_dict,
    to_builtins_list,
)


def test_msgspec_json_roundtrips_dict_payload() -> None:
    payload = {
        'engine': 'gherkin',
        'labels': ['api', 'slow'],
        'message': 'ambito',
    }

    encoded = encode_json_bytes(payload)

    assert decode_json_dict(encoded) == payload


def test_msgspec_json_roundtrips_list_payload() -> None:
    payload = ['one', 2, True]

    encoded = encode_json_text(payload)

    assert decode_json_list(encoded) == payload


def test_lossy_encoder_converts_unknown_values_to_str() -> None:
    payload = {'path': Path('/workspace/report.txt')}

    decoded = decode_json(encode_json_text_lossy(payload))

    assert decoded == {'path': '/workspace/report.txt'}


def test_to_and_from_builtins_roundtrip_dataclass_payload() -> None:
    descriptor = DescriptorKnowledge(
        stable_id='stable-1',
        test_name='Scenario: auth',
        file_path='features/auth.feature',
        source_line=3,
        selection_labels=('api', 'slow'),
    )

    serialized = to_builtins_dict(descriptor)
    restored = from_builtins_dict(
        serialized,
        target_type=DescriptorKnowledge,
    )

    assert restored == descriptor


def test_decode_json_dict_rejects_non_dict_payload() -> None:
    with pytest.raises(ValueError, match='Expected JSON object payload'):
        decode_json_dict('[\"not\", \"an\", \"object\"]')


def test_decode_json_list_rejects_non_list_payload() -> None:
    with pytest.raises(ValueError, match='Expected JSON array payload'):
        decode_json_list('{\"not\": \"a list\"}')


def test_decode_json_dict_and_list_reject_invalid_json() -> None:
    with pytest.raises(ValueError, match='Expected JSON object payload'):
        decode_json_dict('{broken')
    with pytest.raises(ValueError, match='Expected JSON array payload'):
        decode_json_list('{broken')


def test_from_builtins_dict_rejects_invalid_payload() -> None:
    with pytest.raises(ValueError, match='Expected `int`, got `str`'):
        from_builtins_dict(
            {
                'stable_id': 'stable-1',
                'test_name': 'Scenario: auth',
                'file_path': 'features/auth.feature',
                'source_line': 'not-an-int',
            },
            target_type=DescriptorKnowledge,
        )


def test_to_builtins_helpers_reject_unexpected_shapes() -> None:
    with pytest.raises(ValueError, match='Expected builtin dict payload'):
        to_builtins_dict(['not-a-dict'])
    with pytest.raises(ValueError, match='Expected builtin list payload'):
        to_builtins_list({'not': 'a-list'})


def test_from_builtins_rejects_invalid_payload() -> None:
    with pytest.raises(ValueError, match='Expected `int`, got `str`'):
        from_builtins(['one', 'two'], target_type=tuple[int, ...])
