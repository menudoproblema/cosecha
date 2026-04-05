from __future__ import annotations

import contextlib
import operator
import re
import uuid

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from json import loads
from typing import TYPE_CHECKING, Any, Final

from cosecha.core.utils import get_today


if TYPE_CHECKING:  # pragma: no cover
    from cosecha.engine.gherkin.models import Location
    from cosecha.engine.gherkin.types import DatatableCoercions


def parse_bool(value: str, location: Location) -> bool:
    value = value.strip().lower()

    if value in ['true', '1']:
        return True

    if value in ['false', '0']:
        return False

    msg = f'Cannot convert to bool "{value}" at: {location}'
    raise ValueError(msg)


def parse_json(value: str, location: Location) -> Any:
    try:
        return loads(value)
    except Exception as e:
        msg = f'json parse exception "{e}" at: {location}'
        raise ValueError(msg) from e


def parse_int(value: str, location: Location) -> int:
    try:
        return int(value)
    except Exception as e:
        msg = f'int parse exception "{e}" at: {location}'
        raise ValueError(msg) from e


def parse_float(value: str, location: Location) -> float:
    try:
        return float(value)
    except Exception as e:
        msg = f'float parse exception "{e}" at: {location}'
        raise ValueError(msg) from e


def parse_decimal(value: str, location: Location) -> Decimal:
    try:
        return Decimal(value)
    except Exception as e:
        msg = f'decimal parse exception "{e}" at: {location}'
        raise ValueError(msg) from e


def parse_string(value: str, location: Location) -> str:
    try:
        return str(value)
    except Exception as e:
        msg = f'str parse exception "{e}" at: {location}'
        raise ValueError(msg) from e


def parse_uuid(value: str, location: Location) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except Exception as e:
        msg = f'uuid parse exception "{e}" at: {location}'
        raise ValueError(msg) from e


def month_timedelta(src_date: datetime, months: int) -> timedelta:
    # Add months to the date, taking care of year overflow
    month = src_date.month - 1 + months
    year = src_date.year + month // 12
    month = month % 12 + 1
    day = min(
        src_date.day,
        [
            31,
            29
            if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0
            else 28,
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ][month - 1],
    )
    new_date = datetime(
        year=year,
        month=month,
        day=day,
        hour=src_date.hour,
        minute=src_date.minute,
        second=src_date.second,
        tzinfo=src_date.tzinfo,
    )

    delta_seconds = (new_date - src_date).total_seconds()
    return timedelta(seconds=delta_seconds)


def parse_relative_datetime(value: str, location: Location) -> datetime:  # noqa: PLR0911, PLR0912
    now = datetime.now(tz=UTC)

    if value == 'now':
        return now

    if value == 'today':
        return get_today()

    if value == 'tomorrow':
        return now + timedelta(days=1)

    if value == 'yesterday':
        return now - timedelta(days=1)

    if value == 'last week':
        return now - timedelta(weeks=1)

    if value == 'next week':
        return now + timedelta(weeks=1)

    if value == 'last month':
        return now - month_timedelta(now, 1)

    if value == 'next month':
        return now + month_timedelta(now, 1)

    if value == 'next monday':
        while now.weekday() != 0:
            now += timedelta(days=1)

        return now

    match = re.match(
        r'(now|today|next monday)\s*([+-])\s*(\d+)\s*(minute|minutes|day|days|month|months|hour|hours|week|weeks)',  # noqa: E501
        value,
    )
    if match:
        when, operator_str, amount, unit = match.groups()
        amount = int(amount)

        if when == 'today':
            now = get_today()
        if when == 'next monday':
            while now.weekday() != 0:
                now += timedelta(days=1)

        op = operator.add if operator_str == '+' else operator.sub

        match unit:
            case 'minute' | 'minutes':
                return op(now, timedelta(minutes=amount))
            case 'day' | 'days':
                return op(now, timedelta(days=amount))
            case 'hour' | 'hours':
                return op(now, timedelta(hours=amount))
            case 'week' | 'weeks':
                return op(now, timedelta(weeks=amount))
            case 'month' | 'months':
                # Handle month addition manually
                return op(now, month_timedelta(now, amount))
            case _:
                ...

    msg = f'Invalid date "{value}" at: {location}'
    raise ValueError(msg)


def parse_relative_date(value: str, location: Location) -> date:  # noqa: PLR0911, PLR0912
    today = datetime.combine(get_today(), time.min)  # 00:00:00

    if value == 'today':
        return today

    if value == 'tomorrow':
        return today + timedelta(days=1)

    if value == 'yesterday':
        return today - timedelta(days=1)

    if value == 'last week':
        return today - timedelta(weeks=1)

    if value == 'next week':
        return today + timedelta(weeks=1)

    if value == 'last month':
        return today - month_timedelta(today, 1)

    if value == 'next month':
        return today + month_timedelta(today, 1)

    if value == 'next monday':
        d = today
        while d.weekday() != 0:  # Monday == 0
            d += timedelta(days=1)
        return d

    match = re.match(
        r'(today|next monday)\s*([+-])\s*(\d+)\s*(day|days|week|weeks|month|months)',  # noqa: E501
        value,
    )
    if match:
        when, operator_str, amount_str, unit = match.groups()
        amount = int(amount_str)

        base = today
        if when == 'next monday':
            while base.weekday() != 0:
                base += timedelta(days=1)

        op = operator.add if operator_str == '+' else operator.sub

        match unit:
            case 'day' | 'days':
                return op(base, timedelta(days=amount))
            case 'week' | 'weeks':
                return op(base, timedelta(weeks=amount))
            case 'month' | 'months':
                return op(base, month_timedelta(base, amount))
            case _:
                ...

    msg = f'Invalid date "{value}" at: {location}'
    raise ValueError(msg)


def parse_datetime(value: str, location: Location) -> datetime:
    format_list = [
        '%Y-%m-%d %H:%M:%S',
        '%Y/%m/%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%SZ',  # ISO 8601 con Z para UTC
        '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO 8601 con fracción de segundos y Z
        '%Y-%m-%dT%H:%M:%S.%f',  # ISO 8601 con fracción de segundos sin Z
        '%Y-%m-%dT%H:%M:%S',  # ISO 8601 sin fracción de segundos
        '%Y-%m-%d %H:%M:%S.%f',  # JSON (con fracción de segundos)
        '%Y-%m-%d %H:%M:%S',  # JSON (sin fracción de segundos)
    ]

    for datetime_format in format_list:
        with contextlib.suppress(ValueError):
            return datetime.strptime(value, datetime_format).astimezone(UTC)

    msg = f'Invalid date "{value}" at: {location}'
    raise ValueError(msg)


DEFAULT_COERCIONS: Final[DatatableCoercions] = {
    'json': parse_json,
    'int': parse_int,
    'float': parse_float,
    'decimal': parse_decimal,
    'bool': parse_bool,
    'str': parse_string,
    'uuid': parse_uuid,
    'datetime': parse_datetime,
    'relative_datetime': parse_relative_datetime,
    'relative_date': parse_relative_date,
}
