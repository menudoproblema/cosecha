from __future__ import annotations

from typing import Any

import msgspec


_JSON_DECODER = msgspec.json.Decoder()
_JSON_ENCODER = msgspec.json.Encoder()
_LOSSY_JSON_ENCODER = msgspec.json.Encoder(enc_hook=str)


def decode_json(data: str | bytes | bytearray | memoryview) -> object:
    return _JSON_DECODER.decode(data)


def decode_json_dict(
    data: str | bytes | bytearray | memoryview,
) -> dict[str, object]:
    try:
        decoded = decode_json(data)
    except msgspec.DecodeError as error:
        msg = 'Expected JSON object payload'
        raise ValueError(msg) from error
    if isinstance(decoded, dict):
        return {str(key): value for key, value in decoded.items()}

    msg = f'Expected JSON object payload, got {type(decoded).__name__}'
    raise ValueError(msg)


def decode_json_list(
    data: str | bytes | bytearray | memoryview,
) -> list[object]:
    try:
        decoded = decode_json(data)
    except msgspec.DecodeError as error:
        msg = 'Expected JSON array payload'
        raise ValueError(msg) from error
    if isinstance(decoded, list):
        return list(decoded)

    msg = f'Expected JSON array payload, got {type(decoded).__name__}'
    raise ValueError(msg)


def encode_json_bytes(value: Any) -> bytes:
    return _JSON_ENCODER.encode(value)


def encode_json_text(value: Any) -> str:
    return encode_json_bytes(value).decode('utf-8')


def encode_json_text_lossy(value: Any) -> str:
    return _LOSSY_JSON_ENCODER.encode(value).decode('utf-8')


def to_builtins_dict(value: Any) -> dict[str, object]:
    builtins = msgspec.to_builtins(value)
    if isinstance(builtins, dict):
        return {str(key): item for key, item in builtins.items()}

    msg = f'Expected builtin dict payload, got {type(builtins).__name__}'
    raise ValueError(msg)


def to_builtins_list(value: Any) -> list[object]:
    builtins = msgspec.to_builtins(value)
    if isinstance(builtins, list):
        return list(builtins)

    msg = f'Expected builtin list payload, got {type(builtins).__name__}'
    raise ValueError(msg)


def from_builtins_dict[T](
    data: dict[str, object],
    *,
    target_type: type[T],
) -> T:
    try:
        return msgspec.convert(data, type=target_type)
    except msgspec.ValidationError as error:
        msg = str(error) or f'Invalid payload for {target_type.__name__}'
        raise ValueError(msg) from error


def from_builtins[T](
    data: object,
    *,
    target_type: type[T],
) -> T:
    try:
        return msgspec.convert(data, type=target_type)
    except msgspec.ValidationError as error:
        msg = str(error) or f'Invalid payload for {target_type.__name__}'
        raise ValueError(msg) from error
