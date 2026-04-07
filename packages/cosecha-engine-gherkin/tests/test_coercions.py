from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from cosecha.core.location import Location
from cosecha.engine.gherkin import coercions as coercions_module
from cosecha.engine.gherkin.coercions import (
    month_timedelta,
    parse_bool,
    parse_datetime,
    parse_decimal,
    parse_float,
    parse_int,
    parse_json,
    parse_relative_date,
    parse_relative_datetime,
    parse_string,
    parse_uuid,
)


LOCATION = Location(Path('demo.feature'), 3)


def test_parse_bool_and_json_and_decimal_roundtrip() -> None:
    assert parse_bool(' true ', LOCATION) is True
    assert parse_bool('0', LOCATION) is False
    assert parse_json('{"count": 2}', LOCATION) == {'count': 2}
    assert parse_decimal('10.50', LOCATION) == Decimal('10.50')


def test_parse_uuid_returns_uuid_instance() -> None:
    value = '550e8400-e29b-41d4-a716-446655440000'

    parsed = parse_uuid(value, LOCATION)

    assert parsed == UUID(value)


def test_parse_bool_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match='Cannot convert to bool'):
        parse_bool('maybe', LOCATION)


@pytest.mark.parametrize(
    ('value', 'parser', 'expected'),
    (
        ('42', parse_int, 42),
        ('10.5', parse_float, 10.5),
    ),
)
def test_numeric_parsers_parse_valid_values(
    value: str,
    parser,
    expected,
) -> None:
    assert parser(value, LOCATION) == expected


@pytest.mark.parametrize(
    ('value', 'parser', 'error_message'),
    (
        ('abc', parse_int, 'int parse exception'),
        ('abc', parse_float, 'float parse exception'),
        ('{invalid}', parse_json, 'json parse exception'),
        ('not-a-decimal', parse_decimal, 'decimal parse exception'),
        ('not-a-uuid', parse_uuid, 'uuid parse exception'),
    ),
)
def test_parsers_raise_value_error_on_invalid_input(
    value: str,
    parser,
    error_message: str,
) -> None:
    with pytest.raises(ValueError, match=error_message):
        parser(value, LOCATION)


def test_parse_string_returns_value_and_wraps_stringification_errors() -> None:
    assert parse_string('hello', LOCATION) == 'hello'

    class _BrokenString:
        def __str__(self) -> str:  # pragma: no cover - executed in test
            raise RuntimeError('boom')

    with pytest.raises(ValueError, match='str parse exception'):
        parse_string(_BrokenString(), LOCATION)  # type: ignore[arg-type]


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


def test_parse_relative_datetime_supports_keyword_and_delta_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = datetime(2026, 4, 7, 9, 0, tzinfo=UTC)
    fake_today = datetime(2026, 4, 6, 0, 0, tzinfo=UTC)

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            del tz
            return fixed_now

    monkeypatch.setattr(
        'cosecha.engine.gherkin.coercions.datetime',
        _FixedDateTime,
    )
    monkeypatch.setattr(
        'cosecha.engine.gherkin.coercions.get_today',
        lambda: fake_today,
    )

    assert parse_relative_datetime('now', LOCATION) == fixed_now
    assert parse_relative_datetime('today', LOCATION) == fake_today
    assert parse_relative_datetime('tomorrow', LOCATION) == (
        fixed_now + timedelta(days=1)
    )
    assert parse_relative_datetime('yesterday', LOCATION) == (
        fixed_now - timedelta(days=1)
    )
    assert parse_relative_datetime('last week', LOCATION) == (
        fixed_now - timedelta(weeks=1)
    )
    assert parse_relative_datetime('next week', LOCATION) == (
        fixed_now + timedelta(weeks=1)
    )
    assert parse_relative_datetime('last month', LOCATION) == (
        fixed_now - month_timedelta(fixed_now, 1)
    )
    assert parse_relative_datetime('next month', LOCATION) == (
        fixed_now + month_timedelta(fixed_now, 1)
    )
    assert parse_relative_datetime('next monday', LOCATION) == datetime(
        2026,
        4,
        13,
        9,
        0,
        tzinfo=UTC,
    )
    assert parse_relative_datetime('now + 5 minutes', LOCATION) == (
        fixed_now + timedelta(minutes=5)
    )
    assert parse_relative_datetime('today + 2 days', LOCATION) == (
        fake_today + timedelta(days=2)
    )
    assert parse_relative_datetime('next monday - 1 week', LOCATION) == (
        datetime(2026, 4, 13, 9, 0, tzinfo=UTC) - timedelta(weeks=1)
    )
    assert parse_relative_datetime('now + 1 month', LOCATION) == (
        fixed_now + month_timedelta(fixed_now, 1)
    )


def test_parse_relative_datetime_unknown_delta_unit_reaches_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeMatch:
        def groups(self) -> tuple[str, str, str, str]:
            return ('now', '+', '1', 'fortnight')

    monkeypatch.setattr(
        'cosecha.engine.gherkin.coercions.re.match',
        lambda *_args, **_kwargs: _FakeMatch(),
    )

    with pytest.raises(ValueError, match='Invalid date'):
        parse_relative_datetime('now + 1 fortnight', LOCATION)


def test_parse_relative_date_rejects_invalid_expression() -> None:
    with pytest.raises(ValueError, match='Invalid date'):
        parse_relative_date('next century', LOCATION)


def test_parse_relative_date_supports_keyword_and_delta_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_today = datetime(2026, 4, 6, 0, 0, tzinfo=UTC)
    naive_today = fake_today.replace(tzinfo=None)
    monkeypatch.setattr(
        'cosecha.engine.gherkin.coercions.get_today',
        lambda: fake_today,
    )

    assert parse_relative_date('today', LOCATION) == naive_today
    assert parse_relative_date('tomorrow', LOCATION) == (
        naive_today + timedelta(days=1)
    )
    assert parse_relative_date('yesterday', LOCATION) == (
        naive_today - timedelta(days=1)
    )
    assert parse_relative_date('last week', LOCATION) == (
        naive_today - timedelta(weeks=1)
    )
    assert parse_relative_date('next week', LOCATION) == (
        naive_today + timedelta(weeks=1)
    )
    assert parse_relative_date('last month', LOCATION) == (
        naive_today - month_timedelta(naive_today, 1)
    )
    assert parse_relative_date('next month', LOCATION) == (
        naive_today + month_timedelta(naive_today, 1)
    )
    assert parse_relative_date('next monday', LOCATION) == datetime(
        2026,
        4,
        6,
        0,
        0,
    )
    assert parse_relative_date('today + 2 days', LOCATION) == (
        naive_today + timedelta(days=2)
    )
    assert parse_relative_date('next monday - 1 week', LOCATION) == (
        naive_today - timedelta(weeks=1)
    )
    assert parse_relative_date('today + 1 month', LOCATION) == (
        naive_today + month_timedelta(naive_today, 1)
    )


def test_parse_relative_date_unknown_delta_unit_reaches_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeMatch:
        def groups(self) -> tuple[str, str, str, str]:
            return ('today', '+', '1', 'fortnight')

    monkeypatch.setattr(
        'cosecha.engine.gherkin.coercions.re.match',
        lambda *_args, **_kwargs: _FakeMatch(),
    )

    with pytest.raises(ValueError, match='Invalid date'):
        parse_relative_date('today + 1 fortnight', LOCATION)


def test_parse_relative_date_next_monday_paths_run_when_today_is_not_monday(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_today = datetime(2026, 4, 7, 0, 0, tzinfo=UTC)
    naive_today = fake_today.replace(tzinfo=None)
    monkeypatch.setattr(
        'cosecha.engine.gherkin.coercions.get_today',
        lambda: fake_today,
    )

    assert parse_relative_date('next monday', LOCATION) == datetime(
        2026,
        4,
        13,
        0,
        0,
    )
    assert parse_relative_date('next monday + 1 days', LOCATION) == (
        datetime(2026, 4, 14, 0, 0)
    )


@pytest.mark.parametrize(
    'value',
    (
        '2026-04-06 10:20:30',
        '2026/04/06 10:20:30',
        '2026-04-06T10:20:30Z',
        '2026-04-06T10:20:30.123456Z',
        '2026-04-06T10:20:30.123456',
        '2026-04-06T10:20:30',
        '2026-04-06 10:20:30.123456',
    ),
)
def test_parse_datetime_supports_all_documented_formats(value: str) -> None:
    parsed = parse_datetime(value, LOCATION)
    assert parsed.tzinfo == UTC


def test_parse_datetime_rejects_invalid_format() -> None:
    with pytest.raises(ValueError, match='Invalid date'):
        parse_datetime('2026-04-06Tinvalid', LOCATION)


def test_default_coercions_map_exposes_expected_parsers() -> None:
    assert coercions_module.DEFAULT_COERCIONS['json'] is parse_json
    assert coercions_module.DEFAULT_COERCIONS['relative_date'] is parse_relative_date
