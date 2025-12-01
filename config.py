"""Конфігураційні структури та завантаження ENV для FXCM конектора."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

STREAM_DEFAULT_CONFIG = "XAU/USD:m1,XAU/USD:m5"  # Цільові інструменти та таймфрейми для стріму
CACHE_DEFAULT_DIR = "cache"  # Каталог для збереження кешу
CACHE_DEFAULT_MAX_BARS = 3_000  # Максимальна кількість барів у кеші
CACHE_DEFAULT_WARMUP_BARS = 1_000  # Кількість барів для "прогріву" кешу
METRICS_DEFAULT_PORT = 9200
HEARTBEAT_DEFAULT_CHANNEL = "fxcm:heartbeat"
CALENDAR_OVERRIDES_FILE = Path("config/calendar_overrides.json")
RUNTIME_SETTINGS_FILE = Path("config/runtime_settings.json")

def _load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - конфіг краще падати одразу
        raise ValueError(f"Некоректний JSON у {path}: {exc}") from exc



def _get_int_env(name: str, default: int, *, min_value: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(min_value, value)


def _get_bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _coerce_int(value: Any, default: int, *, min_value: int = 1) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min_value, parsed)


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _parse_stream_targets(raw_config: str) -> List[Tuple[str, str]]:
    targets: List[Tuple[str, str]] = []
    for chunk in raw_config.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if ":" in chunk:
            symbol_part, tf_part = chunk.split(":", 1)
        else:
            symbol_part, tf_part = chunk, "m1"
        targets.append((symbol_part.strip(), tf_part.strip()))
    return targets or [("XAU/USD", "m1")]


def _parse_stream_targets_config(value: Any) -> List[Tuple[str, str]]:
    if value is None:
        return _parse_stream_targets(STREAM_DEFAULT_CONFIG)
    if isinstance(value, str):
        return _parse_stream_targets(value)

    targets: List[Tuple[str, str]] = []
    if isinstance(value, Sequence):
        for entry in value:
            if isinstance(entry, str):
                targets.extend(_parse_stream_targets(entry))
                continue
            if isinstance(entry, Mapping):
                symbol_raw = str(entry.get("symbol") or entry.get("pair") or "").strip()
                tf_raw = str(entry.get("tf") or entry.get("timeframe") or "m1").strip()
                if symbol_raw:
                    targets.append((symbol_raw, tf_raw or "m1"))
    return targets or _parse_stream_targets(STREAM_DEFAULT_CONFIG)


def _coerce_float(value: Any, default: float, *, min_value: float = 0.1) -> float:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return max(min_value, parsed)


@dataclass(frozen=True)
class RedisSettings:
    host: str
    port: int


@dataclass(frozen=True)
class CacheSettings:
    enabled: bool
    root: Path
    max_bars: int
    warmup_bars: int


@dataclass(frozen=True)
class SampleRequestSettings:
    symbol: str
    timeframe: str
    hours: int


@dataclass(frozen=True)
class BackoffPolicy:
    base_delay: float
    factor: float
    max_delay: float


@dataclass(frozen=True)
class ObservabilitySettings:
    metrics_enabled: bool
    metrics_port: int
    heartbeat_channel: str


@dataclass(frozen=True)
class CalendarSettings:
    holidays: List[str]
    daily_breaks: List[Any]
    weekly_open: Optional[str]
    weekly_close: Optional[str]
    session_windows: List[Any]


@dataclass(frozen=True)
class BackoffSettings:
    fxcm_login: BackoffPolicy
    fxcm_stream: BackoffPolicy
    redis_stream: BackoffPolicy


def _parse_backoff_policy(value: Any, fallback: BackoffPolicy) -> BackoffPolicy:
    if not isinstance(value, Mapping):
        return fallback
    base_delay = _coerce_float(value.get("base_delay"), fallback.base_delay, min_value=0.1)
    factor = _coerce_float(value.get("factor"), fallback.factor, min_value=1.0)
    max_delay = _coerce_float(value.get("max_delay"), fallback.max_delay, min_value=base_delay)
    return BackoffPolicy(base_delay=base_delay, factor=factor, max_delay=max_delay)


def _load_calendar_settings() -> CalendarSettings:
    payload = _load_json_file(CALENDAR_OVERRIDES_FILE)

    def _collect_holidays(raw_list: Sequence[Any]) -> List[str]:
        values: List[str] = []
        for item in raw_list:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                values.append(text)
        return values

    holidays_raw = payload.get("holidays")
    holidays: List[str] = _collect_holidays(holidays_raw) if isinstance(holidays_raw, list) else []

    breaks_raw = payload.get("daily_breaks")
    daily_breaks: List[Any] = list(breaks_raw) if isinstance(breaks_raw, list) else []

    weekly_open = payload.get("weekly_open_utc")
    weekly_close = payload.get("weekly_close_utc")

    weekly_open_norm = str(weekly_open).strip() if isinstance(weekly_open, str) else None
    weekly_close_norm = str(weekly_close).strip() if isinstance(weekly_close, str) else None

    session_windows_raw = payload.get("session_windows")
    session_windows: List[Any] = (
        list(session_windows_raw) if isinstance(session_windows_raw, list) else []
    )

    return CalendarSettings(
        holidays=holidays,
        daily_breaks=daily_breaks,
        weekly_open=weekly_open_norm or None,
        weekly_close=weekly_close_norm or None,
        session_windows=session_windows,
    )


def _load_runtime_settings() -> Dict[str, Any]:
    return _load_json_file(RUNTIME_SETTINGS_FILE)


def load_stream_targets_from_settings() -> List[Tuple[str, str]]:
    """Повертає stream config (symbol, timeframe) з runtime_settings.json."""

    runtime_settings = _load_runtime_settings()
    stream_cfg_raw = runtime_settings.get("stream")
    stream_cfg = stream_cfg_raw if isinstance(stream_cfg_raw, Mapping) else {}
    return _parse_stream_targets_config(stream_cfg.get("config"))


@dataclass(frozen=True)
class FXCMConfig:
    username: str
    password: str
    connection: str
    host_url: str
    redis: RedisSettings
    cache: CacheSettings
    stream_mode: bool
    poll_seconds: int
    publish_interval_seconds: int
    lookback_minutes: int
    stream_targets: List[Tuple[str, str]]
    sample_request: SampleRequestSettings
    observability: ObservabilitySettings
    calendar: CalendarSettings
    backoff: BackoffSettings
    hmac_secret: Optional[str]
    hmac_algo: str
    redis_required: bool


def load_config() -> FXCMConfig:
    """Зчитує налаштування з ENV та повертає агреговану конфігурацію."""

    runtime_settings = _load_runtime_settings()

    username = os.environ.get("FXCM_USERNAME", "").strip()
    password = os.environ.get("FXCM_PASSWORD", "").strip()
    if not username or not password:
        raise ValueError("Потрібно задати FXCM_USERNAME та FXCM_PASSWORD у .env.")

    connection = os.environ.get("FXCM_CONNECTION", "Demo").strip() or "Demo"
    host_url = os.environ.get(
        "FXCM_HOST_URL",
        "http://www.fxcorporate.com/Hosts.jsp",
    ).strip()

    redis_settings = RedisSettings(
        host=os.environ.get("FXCM_REDIS_HOST", "127.0.0.1").strip() or "127.0.0.1",
        port=_get_int_env("FXCM_REDIS_PORT", 6379, min_value=1),
    )

    cache_cfg_raw = runtime_settings.get("cache")
    cache_cfg = cache_cfg_raw if isinstance(cache_cfg_raw, dict) else {}
    cache_enabled = _get_bool_env("FXCM_CACHE_ENABLED", True)
    cache_dir = Path(cache_cfg.get("dir", CACHE_DEFAULT_DIR))
    cache_max_bars = _coerce_int(cache_cfg.get("max_bars"), CACHE_DEFAULT_MAX_BARS, min_value=500)
    cache_warmup_bars = min(
        cache_max_bars,
        _coerce_int(cache_cfg.get("warmup_bars"), CACHE_DEFAULT_WARMUP_BARS, min_value=100),
    )
    cache_settings = CacheSettings(
        enabled=cache_enabled,
        root=cache_dir,
        max_bars=cache_max_bars,
        warmup_bars=cache_warmup_bars,
    )

    stream_cfg_raw = runtime_settings.get("stream")
    stream_cfg = stream_cfg_raw if isinstance(stream_cfg_raw, dict) else {}
    stream_mode = _coerce_bool(stream_cfg.get("mode"), False)
    base_poll_seconds = _coerce_int(stream_cfg.get("poll_seconds"), 5, min_value=1)
    if "fetch_interval_seconds" in stream_cfg:
        poll_seconds = _coerce_int(stream_cfg.get("fetch_interval_seconds"), base_poll_seconds, min_value=1)
    else:
        poll_seconds = base_poll_seconds
    publish_interval_seconds = _coerce_int(
        stream_cfg.get("publish_interval_seconds"),
        poll_seconds,
        min_value=1,
    )
    lookback_minutes = _coerce_int(stream_cfg.get("lookback_minutes"), 5, min_value=1)
    stream_targets = _parse_stream_targets_config(stream_cfg.get("config"))

    sample_cfg_raw = runtime_settings.get("sample_request")
    sample_cfg = sample_cfg_raw if isinstance(sample_cfg_raw, dict) else {}
    sample_request = SampleRequestSettings(
        symbol=str(sample_cfg.get("symbol", "EUR/USD")).strip() or "EUR/USD",
        timeframe=str(sample_cfg.get("timeframe", "m1")).strip() or "m1",
        hours=_coerce_int(sample_cfg.get("hours"), 24, min_value=1),
    )

    observability = ObservabilitySettings(
        metrics_enabled=_get_bool_env("FXCM_METRICS_ENABLED", True),
        metrics_port=_get_int_env(
            "FXCM_METRICS_PORT",
            METRICS_DEFAULT_PORT,
            min_value=1024,
        ),
        heartbeat_channel=os.environ.get(
            "FXCM_HEARTBEAT_CHANNEL",
            HEARTBEAT_DEFAULT_CHANNEL,
        ).strip(),
    )

    calendar_settings = _load_calendar_settings()

    backoff_cfg_raw = runtime_settings.get("backoff")
    backoff_cfg = backoff_cfg_raw if isinstance(backoff_cfg_raw, Mapping) else {}
    backoff_settings = BackoffSettings(
        fxcm_login=_parse_backoff_policy(
            backoff_cfg.get("fxcm_login"),
            BackoffPolicy(base_delay=2.0, factor=2.0, max_delay=60.0),
        ),
        fxcm_stream=_parse_backoff_policy(
            backoff_cfg.get("fxcm_stream"),
            BackoffPolicy(base_delay=5.0, factor=2.0, max_delay=300.0),
        ),
        redis_stream=_parse_backoff_policy(
            backoff_cfg.get("redis_stream"),
            BackoffPolicy(base_delay=1.0, factor=2.0, max_delay=60.0),
        ),
    )

    hmac_secret_raw = os.environ.get("FXCM_HMAC_SECRET")
    hmac_secret = hmac_secret_raw.strip() if isinstance(hmac_secret_raw, str) else ""
    if not hmac_secret:
        hmac_secret = None

    hmac_algo = (
        os.environ.get("FXCM_HMAC_ALGO", "sha256").strip().lower() or "sha256"
    )

    redis_required = _get_bool_env("FXCM_REDIS_REQUIRED", True)

    return FXCMConfig(
        username=username,
        password=password,
        connection=connection,
        host_url=host_url,
        redis=redis_settings,
        cache=cache_settings,
        stream_mode=stream_mode,
        poll_seconds=poll_seconds,
        publish_interval_seconds=publish_interval_seconds,
        lookback_minutes=lookback_minutes,
        stream_targets=stream_targets,
        sample_request=sample_request,
        observability=observability,
        calendar=calendar_settings,
        backoff=backoff_settings,
        hmac_secret=hmac_secret,
        hmac_algo=hmac_algo,
        redis_required=redis_required,
    )
