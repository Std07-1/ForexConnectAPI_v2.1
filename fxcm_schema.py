"""Спільні TypedDict-схеми та константи для FXCM конектора та інгестора."""

from __future__ import annotations

import datetime as dt

try:  # Python 3.7 fallback для TypedDict
    from typing import TypedDict
except ImportError:  # pragma: no cover - typing_extensions для старих інтерпретаторів
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


class MarketStatusPayload(TypedDict, total=False):
    type: str
    state: str
    ts: str
    next_open_utc: str
