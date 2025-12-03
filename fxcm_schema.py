"""Спільні TypedDict-схеми та константи для FXCM конектора та інгестора."""

from __future__ import annotations

import datetime as dt
from typing import Dict, List

from typing_extensions import TypedDict

MIN_ALLOWED_BAR_TIMESTAMP_MS = int(dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
MAX_FUTURE_DRIFT_SECONDS = 86_400  # дозволяємо бари з майбутнім часом до 1 доби
DEFAULT_MAX_BARS_PER_PAYLOAD = 5_000  # розумний дефолт для обмеження розміру пакета


class RedisBar(TypedDict):
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class SessionSymbolStatsPayload(TypedDict, total=False):
    symbol: str
    tf: str
    bars: int
    range: float
    avg: float
    high: float
    low: float


class SessionStatsEntryPayload(TypedDict, total=False):
    tag: str
    timezone: str
    session_open_utc: str
    session_close_utc: str
    symbols: List[SessionSymbolStatsPayload]


class SessionContextPayload(TypedDict, total=False):
    tag: str
    timezone: str
    weekly_open: str
    weekly_close: str
    daily_breaks: List[dict]
    holidays: List[str]
    next_open_utc: str
    next_open_ms: int
    next_open_seconds: float
    session_open_utc: str
    session_close_utc: str
    stats: Dict[str, SessionStatsEntryPayload]


class MarketStatusPayload(TypedDict, total=False):
    type: str
    state: str
    ts: str
    next_open_utc: str
    next_open_ms: int
    next_open_in_seconds: float
    session: SessionContextPayload
