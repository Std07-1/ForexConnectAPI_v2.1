"""Спільні TypedDict-схеми та константи для FXCM конектора та інгестора.

Цей модуль описує «контракти» JSON-повідомлень у Redis-каналах (payload schemas).
TypedDict-и тут не впливають на runtime, але фіксують очікувані поля/типи.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

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


class OhlcvBarQualityPayload(TypedDict, total=False):
    tick_count: int
    bar_range: float
    body_size: float
    upper_wick: float
    lower_wick: float
    avg_spread: float
    max_spread: float


class OhlcvBarFlagsPayload(TypedDict, total=False):
    complete: bool
    synthetic: bool
    source: str
    tf: str


class OhlcvBarPayload(RedisBar, OhlcvBarQualityPayload, OhlcvBarFlagsPayload):
    """Один бар у payload каналу `fxcm:ohlcv`.

    Базовий контракт: поля з `RedisBar`.
    Розширення (опційні поля) можуть додаватися без ламання споживачів.
    """


class OhlcvPayload(TypedDict, total=False):
    """Повідомлення каналу `fxcm:ohlcv` (batch барів)."""

    symbol: str
    tf: str
    bars: List[OhlcvBarPayload]
    # Опційні поля:
    source: str
    sig: str


class PriceTickSnapPayload(TypedDict):
    """Повідомлення каналу `fxcm:price_tik` (один символ = один JSON)."""

    symbol: str
    bid: float
    ask: float
    mid: float
    tick_ts: float
    snap_ts: float


class PublicStatusSessionPayload(TypedDict, total=False):
    """Блок `session` для `fxcm:status` (людський, обрізаний зріз)."""

    name: str
    tag: str
    state: str
    current_open_utc: str
    current_close_utc: str
    next_open_utc: str
    seconds_to_close: float
    seconds_to_next_open: float
    # Додається лише коли `state == "closed"`.
    state_detail: str


class PublicStatusPayload(TypedDict, total=False):
    """Повідомлення каналу `fxcm:status`.

    Це публічний SPI для сторонніх систем; не містить внутрішньої діагностики.
    """

    ts: float
    process: str
    market: str
    price: str
    ohlcv: str
    note: str
    session: PublicStatusSessionPayload


class HeartbeatContextPayload(TypedDict, total=False):
    """Блок `context` у `fxcm:heartbeat`.

    Поля залежать від режиму (`mode=stream|idle|warmup_sample`) і можуть
    розширюватись новими ключами.
    """

    channel: Optional[str]
    mode: str
    redis_connected: bool
    redis_required: bool
    redis_channel: str
    poll_seconds: int
    lookback_minutes: int
    publish_interval_seconds: int
    cycle_seconds: float
    idle_reason: str
    next_open_seconds: float
    lag_seconds: float
    published_bars: int
    cache_enabled: bool
    calendar_open: bool
    ticks_alive: bool
    effective_market_open: bool
    tick_silence_seconds: float
    stream_targets: List[Dict[str, Any]]
    session: SessionContextPayload
    # Блоки розширеної діагностики (структура визначається реалізацією):
    price_stream: Dict[str, Any]
    tick_cadence: Dict[str, Any]
    history: Dict[str, Any]
    diag: Dict[str, Any]
    supervisor: Dict[str, Any]
    async_supervisor: Dict[str, Any]
    history_backoff_seconds: float
    redis_backoff_seconds: float


class HeartbeatPayload(TypedDict, total=False):
    """Повідомлення каналу heartbeat (технічний стан процесу)."""

    type: str
    state: str
    ts: str
    last_bar_close_ms: int
    next_open_utc: str
    sleep_seconds: float
    context: HeartbeatContextPayload


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
