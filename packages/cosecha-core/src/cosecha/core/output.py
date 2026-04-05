from __future__ import annotations

from enum import StrEnum


class OutputMode(StrEnum):
    SUMMARY = 'summary'
    LIVE = 'live'
    DEBUG = 'debug'
    TRACE = 'trace'


class OutputDetail(StrEnum):
    STANDARD = 'standard'
    FULL_FAILURES = 'full-failures'


__all__ = ('OutputDetail', 'OutputMode')
