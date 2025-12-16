"""Спільні TypedDict-схеми та константи для FXCM конектора та інгестора.

Цей модуль описує «контракти» JSON-повідомлень у Redis-каналах (payload schemas).
TypedDict-и тут не впливають на runtime, але фіксують очікувані поля/типи.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Mapping, Optional, Sequence

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


class PublicStatusSessionSymbolStatsPayload(TypedDict, total=False):
    """Рядок session-таблиці для `fxcm:status` (пер-серія: Symbol+TF)."""

    symbol: str
    tf: str
    high: float
    low: float
    avg: float


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
    # Мінімальна табличка per (symbol, tf): High/Low/Avg.
    symbols: List[PublicStatusSessionSymbolStatsPayload]


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


OHLCV_ROOT_REQUIRED_KEYS = frozenset({"symbol", "tf", "bars"})
OHLCV_ROOT_OPTIONAL_KEYS = frozenset({"source", "sig"})
OHLCV_ROOT_ALLOWED_KEYS = OHLCV_ROOT_REQUIRED_KEYS | OHLCV_ROOT_OPTIONAL_KEYS

OHLCV_BAR_REQUIRED_KEYS = frozenset(
    {"open_time", "close_time", "open", "high", "low", "close", "volume"}
)
OHLCV_BAR_OPTIONAL_KEYS = frozenset(
    {
        "complete",
        "synthetic",
        "source",
        "tf",
        "tick_count",
        "bar_range",
        "body_size",
        "upper_wick",
        "lower_wick",
        "avg_spread",
        "max_spread",
    }
)
OHLCV_BAR_ALLOWED_KEYS = OHLCV_BAR_REQUIRED_KEYS | OHLCV_BAR_OPTIONAL_KEYS

PRICE_TIK_REQUIRED_KEYS = frozenset({"symbol", "bid", "ask", "mid", "tick_ts", "snap_ts"})
PRICE_TIK_ALLOWED_KEYS = PRICE_TIK_REQUIRED_KEYS

STATUS_ROOT_REQUIRED_KEYS = frozenset({"ts", "process", "market", "price", "ohlcv", "note"})
STATUS_ROOT_OPTIONAL_KEYS = frozenset({"session"})
STATUS_ROOT_ALLOWED_KEYS = STATUS_ROOT_REQUIRED_KEYS | STATUS_ROOT_OPTIONAL_KEYS

STATUS_SESSION_ALLOWED_KEYS = frozenset(
    {
        "name",
        "tag",
        "state",
        "current_open_utc",
        "current_close_utc",
        "next_open_utc",
        "seconds_to_close",
        "seconds_to_next_open",
        "state_detail",
        "symbols",
    }
)

STATUS_SESSION_SYMBOL_REQUIRED_KEYS = frozenset({"symbol", "tf", "high", "low", "avg"})
STATUS_SESSION_SYMBOL_ALLOWED_KEYS = STATUS_SESSION_SYMBOL_REQUIRED_KEYS


def validate_ohlcv_payload_contract(payload: Mapping[str, Any]) -> None:
    """Runtime-валідація контракту `fxcm:ohlcv`.

    Мета: fail-fast при випадкових змінах схеми під час оновлень.
    - Не дозволяє зайві поля на root/bar рівні (їх потрібно явно додати у fxcm_schema.py).
    - Перевіряє наявність базових полів та базові типи.
    """

    if not isinstance(payload, Mapping):
        raise ValueError("OHLCV payload має бути mapping (dict)")

    root_keys = set(payload.keys())
    missing = OHLCV_ROOT_REQUIRED_KEYS - root_keys
    if missing:
        raise ValueError(f"OHLCV payload: бракує root-полів: {sorted(missing)}")

    extra = root_keys - OHLCV_ROOT_ALLOWED_KEYS
    if extra:
        raise ValueError(f"OHLCV payload: зайві root-поля (не в контракті): {sorted(extra)}")

    symbol = payload.get("symbol")
    tf = payload.get("tf")
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("OHLCV payload: 'symbol' має бути непорожнім рядком")
    if not isinstance(tf, str) or not tf:
        raise ValueError("OHLCV payload: 'tf' має бути непорожнім рядком")

    bars = payload.get("bars")
    if not isinstance(bars, Sequence) or isinstance(bars, (str, bytes)):
        raise ValueError("OHLCV payload: 'bars' має бути масивом об'єктів")

    for idx, bar in enumerate(bars):
        if not isinstance(bar, Mapping):
            raise ValueError(f"OHLCV payload: bars[{idx}] має бути mapping (dict)")
        bar_keys = set(bar.keys())
        missing_bar = OHLCV_BAR_REQUIRED_KEYS - bar_keys
        if missing_bar:
            raise ValueError(
                f"OHLCV payload: bars[{idx}] бракує полів: {sorted(missing_bar)}"
            )
        extra_bar = bar_keys - OHLCV_BAR_ALLOWED_KEYS
        if extra_bar:
            raise ValueError(
                f"OHLCV payload: bars[{idx}] зайві поля (не в контракті): {sorted(extra_bar)}"
            )

        open_time = bar.get("open_time")
        close_time = bar.get("close_time")
        if not isinstance(open_time, int):
            raise ValueError(f"OHLCV payload: bars[{idx}].open_time має бути int (ms)")
        if not isinstance(close_time, int):
            raise ValueError(f"OHLCV payload: bars[{idx}].close_time має бути int (ms)")
        if close_time < open_time:
            raise ValueError(
                f"OHLCV payload: bars[{idx}] close_time < open_time ({close_time} < {open_time})"
            )

        for key in ("open", "high", "low", "close", "volume"):
            value = bar.get(key)
            if not isinstance(value, (int, float)):
                raise ValueError(f"OHLCV payload: bars[{idx}].{key} має бути числом")

        if "tick_count" in bar and not isinstance(bar.get("tick_count"), int):
            raise ValueError(f"OHLCV payload: bars[{idx}].tick_count має бути int")

        for key in (
            "bar_range",
            "body_size",
            "upper_wick",
            "lower_wick",
            "avg_spread",
            "max_spread",
        ):
            if key in bar and not isinstance(bar.get(key), (int, float)):
                raise ValueError(f"OHLCV payload: bars[{idx}].{key} має бути числом")

        for key in ("complete", "synthetic"):
            if key in bar and not isinstance(bar.get(key), bool):
                raise ValueError(f"OHLCV payload: bars[{idx}].{key} має бути bool")

        for key in ("source", "tf"):
            if key in bar and not isinstance(bar.get(key), str):
                raise ValueError(f"OHLCV payload: bars[{idx}].{key} має бути рядком")


def validate_price_tik_payload_contract(payload: Mapping[str, Any]) -> None:
    """Runtime-валідація контракту `fxcm:price_tik` (один символ = один JSON)."""

    if not isinstance(payload, Mapping):
        raise ValueError("PriceTick payload має бути mapping (dict)")

    keys = set(payload.keys())
    missing = PRICE_TIK_REQUIRED_KEYS - keys
    if missing:
        raise ValueError(f"PriceTick payload: бракує полів: {sorted(missing)}")
    extra = keys - PRICE_TIK_ALLOWED_KEYS
    if extra:
        raise ValueError(f"PriceTick payload: зайві поля (не в контракті): {sorted(extra)}")

    symbol = payload.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("PriceTick payload: 'symbol' має бути непорожнім рядком")

    for key in ("bid", "ask", "mid", "tick_ts", "snap_ts"):
        value = payload.get(key)
        if not isinstance(value, (int, float)):
            raise ValueError(f"PriceTick payload: '{key}' має бути числом")


def validate_status_payload_contract(payload: Mapping[str, Any]) -> None:
    """Runtime-валідація контракту `fxcm:status`.

    Мета: зафіксувати публічний SPI та не допустити випадкових змін схеми.
    """

    if not isinstance(payload, Mapping):
        raise ValueError("Status payload має бути mapping (dict)")

    keys = set(payload.keys())
    missing = STATUS_ROOT_REQUIRED_KEYS - keys
    if missing:
        raise ValueError(f"Status payload: бракує полів: {sorted(missing)}")
    extra = keys - STATUS_ROOT_ALLOWED_KEYS
    if extra:
        raise ValueError(f"Status payload: зайві поля (не в контракті): {sorted(extra)}")

    ts = payload.get("ts")
    if not isinstance(ts, (int, float)):
        raise ValueError("Status payload: 'ts' має бути числом")

    for key in ("process", "market", "price", "ohlcv", "note"):
        value = payload.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError(f"Status payload: '{key}' має бути непорожнім рядком")

    session = payload.get("session")
    if session is None:
        return
    if not isinstance(session, Mapping):
        raise ValueError("Status payload: 'session' має бути mapping (dict)")

    session_keys = set(session.keys())
    extra_session = session_keys - STATUS_SESSION_ALLOWED_KEYS
    if extra_session:
        raise ValueError(
            f"Status payload: session зайві поля (не в контракті): {sorted(extra_session)}"
        )

    for key in ("name", "tag", "state"):
        if key in session and (not isinstance(session.get(key), str) or not session.get(key)):
            raise ValueError(f"Status payload: session.{key} має бути непорожнім рядком")

    for key in ("current_open_utc", "current_close_utc", "next_open_utc", "state_detail"):
        if key in session and not isinstance(session.get(key), str):
            raise ValueError(f"Status payload: session.{key} має бути рядком")

    for key in ("seconds_to_close", "seconds_to_next_open"):
        if key in session and not isinstance(session.get(key), (int, float)):
            raise ValueError(f"Status payload: session.{key} має бути числом")

    symbols = session.get("symbols")
    if symbols is None:
        return
    if not isinstance(symbols, Sequence) or isinstance(symbols, (str, bytes)):
        raise ValueError("Status payload: session.symbols має бути масивом")

    for idx, row in enumerate(symbols):
        if not isinstance(row, Mapping):
            raise ValueError(f"Status payload: session.symbols[{idx}] має бути mapping (dict)")
        row_keys = set(row.keys())
        missing_row = STATUS_SESSION_SYMBOL_REQUIRED_KEYS - row_keys
        if missing_row:
            raise ValueError(
                f"Status payload: session.symbols[{idx}] бракує полів: {sorted(missing_row)}"
            )
        extra_row = row_keys - STATUS_SESSION_SYMBOL_ALLOWED_KEYS
        if extra_row:
            raise ValueError(
                f"Status payload: session.symbols[{idx}] зайві поля (не в контракті): {sorted(extra_row)}"
            )
        if not isinstance(row.get("symbol"), str) or not row.get("symbol"):
            raise ValueError(f"Status payload: session.symbols[{idx}].symbol має бути непорожнім рядком")
        if not isinstance(row.get("tf"), str) or not row.get("tf"):
            raise ValueError(f"Status payload: session.symbols[{idx}].tf має бути непорожнім рядком")
        for key in ("high", "low", "avg"):
            if not isinstance(row.get(key), (int, float)):
                raise ValueError(
                    f"Status payload: session.symbols[{idx}].{key} має бути числом"
                )
