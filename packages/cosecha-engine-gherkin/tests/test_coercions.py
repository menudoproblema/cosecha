from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from cosecha.core.location import Location
from cosecha.engine.gherkin.coercions import (
    month_timedelta,
    parse_bool,
    parse_decimal,
    parse_json,
    parse_relative_date,
    parse_relative_datetime,
    parse_uuid,
)


LOCATION = Location(Path('demo.feature'), 3)


def test_parse_bool_and_json_and_decimal_roundtrip() -> None:
    assert parse_bool(' true ', LOCATION) is True
    assert parse_json('{"count": 2}', LOCATION) == {'count': 2}
    assert parse_decimal('10.50', LOCATION) == Decimal('10.50')


def test_parse_uuid_returns_uuid_instance() -> None:
    value = '550e8400-e29b-41d4-a716-446655440000'

    parsed = parse_uuid(value, LOCATION)

    assert parsed == UUID(value)


def test_parse_bool_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match='Cannot convert to bool'):
        parse_bool('maybe', LOCATION)


def test_month_timedelta_handles_month_overflow_and_leap_year() -> None:
    source = datetime(2024, 1, 31, 10, 30, tzinfo=UTC)

    delta = month_timedelta(source, 1)

    assert source + delta == datetime(2024, 2, 29, 10, 30, tzinfo=UTC)


def test_parse_relative_date_uses_get_today_for_today_based_offsets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_today = datetime(2026, 4, 6, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(
        'cosecha.engine.gherkin.coercions.get_today',
        lambda: fake_today,
    )

    parsed = parse_relative_date('today + 2 days', LOCATION)

    assert parsed == fake_today.replace(tzinfo=None) + timedelta(days=2)


def test_parse_relative_datetime_supports_today_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_today = datetime(2026, 4, 6, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(
        'cosecha.engine.gherkin.coercions.get_today',
        lambda: fake_today,
    )

    parsed = parse_relative_datetime('today + 3 hours', LOCATION)

    assert parsed == fake_today + timedelta(hours=3)


def test_parse_relative_date_rejects_invalid_expression() -> None:
    with pytest.raises(ValueError, match='Invalid date'):
        parse_relative_date('next century', LOCATION)
