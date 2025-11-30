"""Мінімальний конектор FXCM через ForexConnectAPI.

POC:
- логін до FXCM через ForexConnect;
- запит історичних свічок через get_history;
- нормалізація у OHLCV-формат, сумісний із AiOne_t;
- вивід кількості барів і перших рядків.

Приклад запуску:
    $ python connector.py
"""
import datetime as dt
import json
import logging
import time
from logging import Logger
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, cast

import pandas as pd
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, start_http_server
try:
    from forexconnect import ForexConnect  # type: ignore[import]
except ImportError:  # pragma: no cover - SDK може бути не встановлено під час тестів
    class ForexConnect:  # type: ignore[override]
        """Заглушка, щоб тести могли імпортувати модуль без SDK."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401 - простий плейсхолдер
            raise RuntimeError(
                "ForexConnect SDK не встановлено. Використовуйте офіційний клієнт, "
                "щоб запускати конектор проти реального FXCM."
            )

        def login(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError(
                "ForexConnect SDK не встановлено. Використовуйте офіційний клієнт, "
                "щоб запускати конектор проти реального FXCM."
            )

        def logout(self) -> None:
            raise RuntimeError(
                "ForexConnect SDK не встановлено. Використовуйте офіційний клієнт, "
                "щоб запускати конектор проти реального FXCM."
            )

        def get_history(
            self,
            symbol: str,
            timeframe: str,
            start: dt.datetime,
            end: dt.datetime,
        ) -> List[Dict[str, Any]]:
            raise RuntimeError(
                "ForexConnect SDK не встановлено. Використовуйте офіційний клієнт, "
                "щоб запускати конектор проти реального FXCM."
            )
        
from rich.logging import RichHandler

from cache_utils import (
    CacheRecord,
    ensure_schema,
    load_cache,
    merge_and_trim,
    save_cache_data,
    save_cache_meta,
)
from config import (
    BackoffPolicy,
    CalendarSettings,
    FXCMConfig,
    RedisSettings,
    SampleRequestSettings,
    load_config,
)
from sessions import (
    generate_request_windows,
    is_trading_time,
    next_trading_open,
    override_calendar,
)
from fxcm_security import compute_payload_hmac
from fxcm_schema import (
    MAX_FUTURE_DRIFT_SECONDS,
    MIN_ALLOWED_BAR_TIMESTAMP_MS,
    MarketStatusPayload,
    RedisBar,
)

try:
    import redis
except Exception:  # noqa: BLE001
    redis = None


# Налаштування логування
log: Logger = logging.getLogger("fxcm_connector")

REDIS_CHANNEL = "fxcm:ohlcv"  # канал публікації OHLCV-барів
REDIS_STATUS_CHANNEL = "fxcm:market_status"  # канал публікації статусу ринку
BAR_INTERVAL_MS = 60_000  # 1 хвилина у мілісекундах
_LAST_MARKET_STATUS: Optional[Tuple[str, Optional[int]]] = None  # останній статус ринку для приглушення спаму
_METRICS_SERVER_STARTED = False
IDLE_LOG_INTERVAL_SECONDS = 300.0  # мінімальний інтервал між повідомленнями про паузу торгів

PROM_BARS_PUBLISHED = Counter(
    "fxcm_ohlcv_bars_total",
    "Загальна кількість опублікованих OHLCV-барів",
    ["symbol", "tf"],
)
PROM_STREAM_LAG_SECONDS = Gauge(
    "fxcm_stream_lag_seconds",
    "Лаг між поточним часом та close_time останнього бару",
    ["symbol", "tf"],
)
PROM_STREAM_STALENESS_SECONDS = Gauge(
    "fxcm_stream_staleness_seconds",
    "Час у секундах від останнього опублікованого close_time",
    ["symbol", "tf"],
)
PROM_ERROR_COUNTER = Counter(
    "fxcm_connector_errors_total",
    "Кількість помилок FXCM/Redis",
    ["type"],
)
PROM_MARKET_STATUS = Gauge(
    "fxcm_market_status",
    "Статус ринку: 1=open, 0=closed",
)
PROM_HEARTBEAT_TS = Gauge(
    "fxcm_connector_heartbeat_timestamp",
    "UNIX-час останнього heartbeat",
)
PROM_NEXT_OPEN_SECONDS = Gauge(
    "fxcm_next_open_seconds",
    "Скільки секунд залишилось до наступного торгового вікна",
)
PROM_DROPPED_BARS = Counter(
    "fxcm_dropped_bars_total",
    "Кількість відкинутих OHLCV-барів через аномальні timestamp",
    ["symbol", "tf"],
)
PROM_PRICE_HISTORY_NOT_READY = Counter(
    "fxcm_pricehistory_not_ready_total",
    "Скільки разів FXCM повернув 'PriceHistoryCommunicator is not ready'",
)


class BackoffController:
    """Простий експоненційний backoff з логуванням."""

    def __init__(self, policy: BackoffPolicy) -> None:
        self.policy = policy
        self._current = policy.base_delay

    def wait(self, reason: str) -> None:
        log.warning("%s. Наступна спроба через %.1f с.", reason, self._current)
        time.sleep(self._current)
        self._current = min(self.policy.max_delay, self._current * self.policy.factor)

    def reset(self) -> None:
        self._current = self.policy.base_delay


class FXCMRetryableError(RuntimeError):
    """Сигналізує, що необхідно перепідключити сесію FXCM."""


class RedisRetryableError(RuntimeError):
    """Сигналізує, що Redis потрібно перепідключити."""


class MarketTemporarilyClosed(RuntimeError):
    """Ринок закритий або FXCM тимчасово не готовий відповідати."""


def _login_fxcm_once(config: FXCMConfig) -> ForexConnect:
    fx = ForexConnect()
    fx.login(
        config.username,
        config.password,
        config.host_url,
        config.connection,
        "",  # session_id
        "",  # pin
    )
    log.info("Успішний логін до FXCM через ForexConnect.")
    return fx


def _close_fxcm_session(fx: Optional[ForexConnect], *, announce: bool = True) -> None:
    if fx is None:
        return
    try:
        fx.logout()
        if announce:
            log.info("Сесію FXCM коректно завершено.")
    except Exception as exc:  # noqa: BLE001
        log.exception("Помилка під час логауту: %s", exc)


def _obtain_fxcm_session(config: FXCMConfig, backoff: BackoffController) -> ForexConnect:
    while True:
        try:
            fx = _login_fxcm_once(config)
            backoff.reset()
            return fx
        except Exception as exc:  # noqa: BLE001
            backoff.wait(f"FXCM логін неуспішний: {exc}")


def _obtain_redis_client_blocking(
    settings: RedisSettings,
    backoff: BackoffController,
) -> Any:
    if redis is None:
        raise RuntimeError("Пакет redis недоступний — неможливо встановити з'єднання.")

    while True:
        client = _create_redis_client(settings)
        if client is not None:
            backoff.reset()
            return client
        backoff.wait("Redis недоступний або відмовив у з'єднанні")


def _apply_calendar_overrides(settings: CalendarSettings) -> None:
    override_calendar(
        holidays=settings.holidays or None,
        daily_breaks=settings.daily_breaks or None,
        weekly_open=settings.weekly_open,
        weekly_close=settings.weekly_close,
        replace_holidays=bool(settings.holidays),
    )


def _ensure_metrics_server(port: int) -> None:
    global _METRICS_SERVER_STARTED
    if _METRICS_SERVER_STARTED:
        return
    start_http_server(port)
    log.info("Prometheus-метрики доступні на порту %s.", port)
    _METRICS_SERVER_STARTED = True


def setup_logging() -> None:
    """Налаштовуємо логування з RichHandler.

    Робимо простий формат, щоб було читабельно у консолі під час відладки.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True)],
    )

def _tf_to_minutes(tf_label: str) -> int:
    label = tf_label.strip().lower()
    if label.endswith("m"):
        return max(1, int(label[:-1] or "1"))
    if label.endswith("h"):
        return max(1, int(label[:-1] or "1")) * 60
    if label.endswith("d"):
        return max(1, int(label[:-1] or "1")) * 1_440
    raise ValueError(f"Невідомий таймфрейм: {tf_label}")


def _ms_to_dt(value_ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(value_ms / 1000.0, tz=dt.timezone.utc)


def _download_history_range(
    fx: ForexConnect,
    *,
    symbol: str,
    timeframe_raw: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
) -> pd.DataFrame:
    if start_dt >= end_dt:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for win_start, win_end in generate_request_windows(start_dt, end_dt):
        if win_start >= win_end:
            continue
        try:
            history = fx.get_history(symbol, timeframe_raw, win_start, win_end)
        except Exception as exc:  # noqa: BLE001
            if _is_price_history_not_ready(exc):
                PROM_PRICE_HISTORY_NOT_READY.inc()
                _log_market_closed_once(_now_utc())
                log.debug(
                    "History cache: PriceHistoryCommunicator не готовий для %s (%s).",
                    symbol,
                    timeframe_raw,
                )
            else:
                log.exception(
                    "Помилка get_history(%s, %s, %s → %s): %s",
                    symbol,
                    timeframe_raw,
                    win_start,
                    win_end,
                    exc,
                )
            continue

        chunk = pd.DataFrame(history)
        if chunk.empty:
            continue
        frames.append(chunk)

    if not frames:
        return pd.DataFrame()

    df_raw = pd.concat(frames, ignore_index=True)
    df_raw = df_raw.drop_duplicates(subset=["Date"], keep="last")
    tf_norm = _map_timeframe_label(timeframe_raw)
    return _normalize_history_to_ohlcv(df_raw, symbol, tf_norm)


class HistoryCache:
    """Простий файловий кеш OHLCV з обрізанням за останніми барами."""

    def __init__(self, root: Path, max_bars: int, warmup_bars: int) -> None:
        self.root = root
        self.max_bars = max_bars
        self.warmup_bars = warmup_bars
        self.records: Dict[Tuple[str, str], CacheRecord] = {}
        self._disabled = False
        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            self._disable_cache(exc)

    def _key(self, symbol_norm: str, tf_norm: str) -> Tuple[str, str]:
        return symbol_norm, tf_norm

    def _empty_record(self) -> CacheRecord:
        return CacheRecord(ensure_schema(pd.DataFrame()), {})

    def _disable_cache(self, exc: Exception) -> None:
        if self._disabled:
            return
        self._disabled = True
        PROM_ERROR_COUNTER.labels("cache_io").inc()
        log.exception("Файловий кеш вимкнено через помилку IO: %s", exc)

    def _persist_data(self, symbol_norm: str, tf_norm: str, df: pd.DataFrame) -> None:
        if self._disabled:
            return
        try:
            save_cache_data(self.root, symbol_norm, tf_norm, df)
        except Exception as exc:  # noqa: BLE001
            self._disable_cache(exc)

    def _persist_meta(self, symbol_norm: str, tf_norm: str, meta: Dict[str, Any]) -> None:
        if self._disabled:
            return
        try:
            save_cache_meta(self.root, symbol_norm, tf_norm, meta)
        except Exception as exc:  # noqa: BLE001
            self._disable_cache(exc)

    def _persist_record(self, symbol_norm: str, tf_norm: str, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
        self._persist_data(symbol_norm, tf_norm, df)
        self._persist_meta(symbol_norm, tf_norm, meta)

    def _load(self, symbol_norm: str, tf_norm: str) -> CacheRecord:
        key = self._key(symbol_norm, tf_norm)
        if key not in self.records:
            if self._disabled:
                self.records[key] = self._empty_record()
            else:
                try:
                    self.records[key] = load_cache(self.root, symbol_norm, tf_norm)
                except Exception as exc:  # noqa: BLE001
                    self._disable_cache(exc)
                    self.records[key] = self._empty_record()
        return self.records[key]

    def ensure_ready(
        self,
        fx: ForexConnect,
        *,
        symbol_raw: str,
        timeframe_raw: str,
    ) -> pd.DataFrame:
        symbol_norm = _normalize_symbol(symbol_raw)
        tf_norm = _map_timeframe_label(timeframe_raw)
        record = self._load(symbol_norm, tf_norm)
        df_cache = record.data
        tf_minutes = _tf_to_minutes(tf_norm)
        now = _now_utc()
        desired_start = now - dt.timedelta(minutes=self.warmup_bars * tf_minutes)
        updated = False

        def fetch_and_merge(start_dt: dt.datetime, end_dt: dt.datetime) -> None:
            nonlocal df_cache, updated
            if start_dt >= end_dt:
                return
            chunk = _download_history_range(
                fx,
                symbol=symbol_raw,
                timeframe_raw=timeframe_raw,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            if chunk.empty:
                return
            df_cache = merge_and_trim(df_cache, chunk, max_rows=self.max_bars)
            updated = True

        if df_cache.empty:
            fetch_and_merge(desired_start, now)
        else:
            first_open_ms = int(df_cache["open_time"].min())
            first_open_dt = _ms_to_dt(first_open_ms)
            if first_open_dt > desired_start:
                fetch_and_merge(desired_start, first_open_dt)

            last_close_ms = int(df_cache["close_time"].max())
            last_close_dt = _ms_to_dt(last_close_ms)
            if is_trading_time(now) and last_close_dt < now - dt.timedelta(minutes=tf_minutes):
                fetch_and_merge(last_close_dt + dt.timedelta(milliseconds=1), now)

            if len(df_cache) < self.warmup_bars and first_open_dt > desired_start:
                need_minutes = (self.warmup_bars - len(df_cache)) * tf_minutes
                fetch_and_merge(first_open_dt - dt.timedelta(minutes=need_minutes), first_open_dt)

        if updated:
            meta = dict(record.meta)
            if not df_cache.empty:
                meta["last_close_time"] = int(df_cache["close_time"].max())
                meta["rows"] = len(df_cache)
            meta["last_refresh_utc"] = now.isoformat()
            self._persist_record(symbol_norm, tf_norm, df_cache, meta)
            self.records[self._key(symbol_norm, tf_norm)] = CacheRecord(df_cache, meta)
        else:
            self.records[self._key(symbol_norm, tf_norm)] = CacheRecord(df_cache, record.meta)

        return df_cache

    def append_stream_bars(
        self,
        *,
        symbol_raw: str,
        timeframe_raw: str,
        df_new: pd.DataFrame,
    ) -> None:
        if df_new is None or df_new.empty:
            return
        symbol_norm = _normalize_symbol(symbol_raw)
        tf_norm = _map_timeframe_label(timeframe_raw)
        record = self._load(symbol_norm, tf_norm)
        merged = merge_and_trim(record.data, df_new, max_rows=self.max_bars)
        meta = dict(record.meta)
        meta["last_close_time"] = int(merged["close_time"].max())
        meta["rows"] = len(merged)
        meta["last_stream_heartbeat"] = _now_utc().isoformat()
        meta["last_published_open_time"] = int(merged["open_time"].max())
        self._persist_record(symbol_norm, tf_norm, merged, meta)
        self.records[self._key(symbol_norm, tf_norm)] = CacheRecord(merged, meta)

    def get_last_open_time(self, symbol_raw: str, timeframe_raw: str) -> Optional[int]:
        symbol_norm = _normalize_symbol(symbol_raw)
        tf_norm = _map_timeframe_label(timeframe_raw)
        record = self._load(symbol_norm, tf_norm)
        if record.data.empty:
            return None
        return int(record.data["open_time"].max())

    def get_last_close_time(self, symbol_raw: str, timeframe_raw: str) -> Optional[int]:
        symbol_norm = _normalize_symbol(symbol_raw)
        tf_norm = _map_timeframe_label(timeframe_raw)
        record = self._load(symbol_norm, tf_norm)
        if not record.data.empty:
            return int(record.data["close_time"].max())
        meta_close = record.meta.get("last_close_time")
        if meta_close is None:
            return None
        try:
            return int(meta_close)
        except (TypeError, ValueError):
            return None

    def get_bars_to_publish(
        self,
        *,
        symbol_raw: str,
        timeframe_raw: str,
        limit: int,
        force: bool = False,
    ) -> pd.DataFrame:
        symbol_norm = _normalize_symbol(symbol_raw)
        tf_norm = _map_timeframe_label(timeframe_raw)
        record = self._load(symbol_norm, tf_norm)
        df_cache = record.data
        if df_cache.empty:
            return df_cache
        if force:
            subset = df_cache.tail(limit)
        else:
            last_published = record.meta.get("last_published_open_time")
            if last_published is None:
                subset = df_cache.tail(limit)
            else:
                subset = df_cache.loc[df_cache["open_time"] > int(last_published)]
                subset = subset.tail(limit)
        return cast(pd.DataFrame, subset.reset_index(drop=True))

    def mark_published(
        self,
        *,
        symbol_raw: str,
        timeframe_raw: str,
        last_open_time: int,
    ) -> None:
        symbol_norm = _normalize_symbol(symbol_raw)
        tf_norm = _map_timeframe_label(timeframe_raw)
        record = self._load(symbol_norm, tf_norm)
        meta = dict(record.meta)
        meta["last_published_open_time"] = last_open_time
        self._persist_meta(symbol_norm, tf_norm, meta)
        self.records[self._key(symbol_norm, tf_norm)] = CacheRecord(record.data, meta)


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _log_market_closed_once(now: dt.datetime, *, next_open: Optional[dt.datetime] = None) -> dt.datetime:
    """Лог «ринок закритий» з приглушенням спаму."""

    attr = "_next_notice_utc"
    next_notice = getattr(_log_market_closed_once, attr, None)
    if isinstance(next_notice, dt.datetime) and now < next_notice:
        return next_notice

    next_open_dt = next_open or next_trading_open(now)
    log.info(
        "FXCM: ринок закритий (%s UTC). Наступне торгове вікно ≥ %s UTC.",
        now.replace(microsecond=0).isoformat(),
        next_open_dt.replace(microsecond=0).isoformat(),
    )
    setattr(_log_market_closed_once, attr, next_open_dt)
    return next_open_dt


def _notify_market_closed(now: dt.datetime, redis_client: Optional[Any]) -> dt.datetime:
    next_open = _log_market_closed_once(now)
    _publish_market_status(redis_client, "closed", next_open=next_open)
    return next_open


def _is_price_history_not_ready(exc: Exception) -> bool:
    return "PriceHistoryCommunicator is not ready" in str(exc)


def _publish_market_status(
    redis_client: Optional[Any],
    state: str,
    *,
    next_open: Optional[dt.datetime] = None,
) -> None:
    global _LAST_MARKET_STATUS

    if redis_client is None:
        return

    next_open_key = int(next_open.timestamp() * 1000) if next_open else None
    status_key = (state, next_open_key)
    if _LAST_MARKET_STATUS == status_key:
        return

    payload: MarketStatusPayload = {
        "type": "market_status",
        "state": state,
        "ts": _now_utc().replace(microsecond=0).isoformat(),
    }
    if state == "closed" and next_open is not None:
        payload["next_open_utc"] = next_open.replace(microsecond=0).isoformat()

    message = json.dumps(payload, separators=(",", ":"))
    try:
        redis_client.publish(REDIS_STATUS_CHANNEL, message)
        _LAST_MARKET_STATUS = status_key
        log.info("Статус ринку → %s", state)
        PROM_MARKET_STATUS.set(1 if state == "open" else 0)
    except Exception as exc:  # noqa: BLE001
        log.exception("Не вдалося надіслати market_status у Redis: %s", exc)


def _publish_heartbeat(
    redis_client: Optional[Any],
    channel: Optional[str],
    *,
    state: str,
    last_bar_close_ms: Optional[int],
    next_open: Optional[dt.datetime] = None,
    sleep_seconds: Optional[float] = None,
) -> None:
    if redis_client is None or not channel:
        return

    payload: Dict[str, Any] = {
        "type": "heartbeat",
        "state": state,
        "ts": _now_utc().replace(microsecond=0).isoformat(),
    }
    if last_bar_close_ms is not None:
        payload["last_bar_close_ms"] = last_bar_close_ms
    if next_open is not None:
        payload["next_open_utc"] = next_open.replace(microsecond=0).isoformat()
    if sleep_seconds is not None:
        payload["sleep_seconds"] = sleep_seconds

    message = json.dumps(payload, separators=(",", ":"))
    try:
        redis_client.publish(channel, message)
        PROM_HEARTBEAT_TS.set(_now_utc().timestamp())
    except Exception as exc:  # noqa: BLE001
        PROM_ERROR_COUNTER.labels(type="redis").inc()
        log.debug("Heartbeat publish неуспішний: %s", exc)


def _normalize_symbol(raw_symbol: str) -> str:
    """Нормалізуємо символ FXCM до формату AiOne_t.

    Зараз проста стратегія: прибираємо '/', робимо upper.
    Приклад: 'EUR/USD' -> 'EURUSD'.
    """
    return raw_symbol.replace("/", "").upper()


class PublishDataGate:
    """Відкидає дублікати барів та трекає останні timestamp-и."""

    def __init__(self) -> None:
        self._last_open: Dict[Tuple[str, str], int] = {}
        self._last_close: Dict[Tuple[str, str], int] = {}

    def _key(self, symbol: str, timeframe: str) -> Tuple[str, str]:
        return (_normalize_symbol(symbol), _map_timeframe_label(timeframe))

    def seed(self, *, symbol: str, timeframe: str, last_open: Optional[int] = None, last_close: Optional[int] = None) -> None:
        key = self._key(symbol, timeframe)
        if last_open is not None:
            self._last_open[key] = int(last_open)
        if last_close is not None:
            self._last_close[key] = int(last_close)

    def filter_new_bars(self, df: pd.DataFrame, *, symbol: str, timeframe: str) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        key = self._key(symbol, timeframe)
        cutoff = self._last_open.get(key)
        if cutoff is None:
            return df
        filtered = cast(pd.DataFrame, df.loc[df["open_time"] > cutoff])
        dropped = len(df) - len(filtered)
        if dropped > 0:
            log.debug(
                "Data gate: відкинуто %d барів для %s %s (cutoff=%d).",
                dropped,
                key[0],
                key[1],
                cutoff,
            )
        return filtered

    def record_publish(self, df: pd.DataFrame, *, symbol: str, timeframe: str) -> None:
        if df is None or df.empty:
            return
        key = self._key(symbol, timeframe)
        newest_open = int(df["open_time"].max())
        newest_close = int(df["close_time"].max())
        self._last_open[key] = newest_open
        self._last_close[key] = newest_close
        self._update_staleness_metric_for_key(key)

    def staleness_seconds(self, *, symbol: str, timeframe: str) -> Optional[float]:
        key = self._key(symbol, timeframe)
        last_close = self._last_close.get(key)
        if last_close is None:
            return None
        now = _now_utc().timestamp()
        return max(0.0, now - last_close / 1000.0)

    def update_staleness_metrics(self) -> None:
        for key in list(self._last_close.keys()):
            self._update_staleness_metric_for_key(key)

    def _update_staleness_metric_for_key(self, key: Tuple[str, str]) -> None:
        last_close = self._last_close.get(key)
        if last_close is None:
            return
        now = _now_utc().timestamp()
        age = max(0.0, now - last_close / 1000.0)
        PROM_STREAM_STALENESS_SECONDS.labels(symbol=key[0], tf=key[1]).set(age)


def _normalize_history_to_ohlcv(
    df: pd.DataFrame,
    symbol: str,
    timeframe: str,
) -> pd.DataFrame:
    """Перетворює історію FXCM (Bid*/Ask*) у OHLCV формату AiOne_t.

    Припущення:
    - DataFrame має колонки: Date, BidOpen, BidHigh, BidLow, BidClose,
      AskOpen, AskHigh, AskLow, AskClose, Volume.
    - Таймфрейм зараз m1 (1 хвилина). Для інших tf поки що використовуємо
      той самий код, але з фіксованим кроком 60 сек як POC.

    Навіщо mid-ціна:
    - Для Stage1/SMC нам важливі відносні рухи, ATR тощо;
    - mid=(Bid+Ask)/2 симетричний, не прив'язуємося до суто bid чи ask.
    """
    missing = {
        "Date",
        "BidOpen",
        "BidHigh",
        "BidLow",
        "BidClose",
        "AskOpen",
        "AskHigh",
        "AskLow",
        "AskClose",
        "Volume",
    } - set(df.columns)
    if missing:
        log.warning("В історії відсутні очікувані колонки: %s", sorted(missing))

    # Обережно конвертуємо Date у datetime з tz=UTC та в msec.
    ts = cast(pd.Series, pd.to_datetime(df["Date"], utc=True, errors="coerce"))
    ts_valid_mask = cast(pd.Series, ts.notna())
    if not ts_valid_mask.all():
        log.warning("Є некоректні значення у колонці Date, вони будуть відкинуті.")
    df = cast(pd.DataFrame, df.loc[ts_valid_mask].copy())
    ts = ts.loc[ts_valid_mask]

    open_time_ms = (ts.astype("int64", copy=False) // 10**6).astype("int64")

    # Визначаємо тривалість бара на основі таймфрейму (мінімум 1 хвилина).
    tf_minutes = max(1, _tf_to_minutes(timeframe))
    bar_ms = tf_minutes * BAR_INTERVAL_MS

    # Обчислюємо mid-ціни.
    mid_open = (df["BidOpen"].astype(float) + df["AskOpen"].astype(float)) / 2.0
    mid_high = (df["BidHigh"].astype(float) + df["AskHigh"].astype(float)) / 2.0
    mid_low = (df["BidLow"].astype(float) + df["AskLow"].astype(float)) / 2.0
    mid_close = (df["BidClose"].astype(float) + df["AskClose"].astype(float)) / 2.0

    symbol_norm = _normalize_symbol(symbol)

    ohlcv = pd.DataFrame(
        {
            "symbol": symbol_norm,
            "tf": timeframe,
            "open_time": open_time_ms,
            "close_time": open_time_ms + bar_ms - 1,
            "open": mid_open,
            "high": mid_high,
            "low": mid_low,
            "close": mid_close,
            "volume": df["Volume"].astype(float),
        }
    )

    # Відкидаємо бари поза дозволеним вікном (анти-1970 та майбутнє).
    now_ms = int(_now_utc().timestamp() * 1000)
    max_allowed = now_ms + MAX_FUTURE_DRIFT_SECONDS * 1000
    valid_mask = (ohlcv["open_time"] >= MIN_ALLOWED_BAR_TIMESTAMP_MS) & (
        ohlcv["open_time"] <= max_allowed
    )
    if not valid_mask.all():
        dropped = int(len(ohlcv) - int(valid_mask.sum()))
        log.warning(
            "Відкинуто %d барів з аномальним timestamp для %s %s (вікно %s → %s).",
            dropped,
            symbol,
            timeframe,
            MIN_ALLOWED_BAR_TIMESTAMP_MS,
            max_allowed,
        )
        PROM_DROPPED_BARS.labels(symbol=symbol_norm, tf=timeframe).inc(dropped)
        ohlcv = ohlcv.loc[valid_mask]

    ohlcv = ohlcv.sort_values("open_time").reset_index(drop=True)

    return ohlcv


def _map_timeframe_label(raw_tf: str) -> str:
    """Маппінг FXCM tf (m1, H1) до тегів UnifiedStore (1m, 1h)."""

    raw = (raw_tf or "").strip()
    if not raw:
        return "1m"

    raw_lower = raw.lower()
    explicit = {
        "m1": "1m",
        "m5": "5m",
        "m15": "15m",
        "m30": "30m",
        "m60": "1h",
        "h1": "1h",
        "h4": "4h",
        "d1": "1d",
    }
    if raw_lower in explicit:
        return explicit[raw_lower]

    if raw_lower.startswith("m") and raw_lower[1:].isdigit():
        return f"{int(raw_lower[1:])}m"
    if raw_lower.startswith("h") and raw_lower[1:].isdigit():
        return f"{int(raw_lower[1:])}h"

    return raw


def _create_redis_client(settings: RedisSettings) -> Optional[Any]:
    """Створює Redis‑клієнт або повертає None, якщо бібліотека недоступна."""

    if redis is None:
        log.warning("Пакет redis недоступний. Публікацію буде пропущено.")
        return None

    try:
        client = redis.Redis(
            host=settings.host,
            port=settings.port,
            decode_responses=True,
        )
        client.ping()
        log.info(
            "Підключено до Redis %s:%s для каналу %s.",
            settings.host,
            settings.port,
            REDIS_CHANNEL,
        )
        return client
    except Exception as exc:  # noqa: BLE001
        log.exception("Не вдалося підключитись до Redis: %s", exc)
        return None


def publish_ohlcv_to_redis(
    df_ohlcv: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    redis_client: Optional[Any],
    data_gate: Optional[PublishDataGate] = None,
    min_publish_interval: float = 0.0,
    publish_rate_limit: Optional[Dict[Tuple[str, str], float]] = None,
    hmac_secret: Optional[str] = None,
    hmac_algo: str = "sha256",
) -> bool:
    """Серіалізує OHLCV у JSON і публікує до Redis."""

    if redis_client is None:
        log.info("Redis-клієнт не ініціалізовано — пропускаю публікацію.")
        return False

    if df_ohlcv.empty:
        log.info("Немає OHLCV-барів для публікації.")
        return False

    df_to_publish = df_ohlcv
    if data_gate is not None:
        df_to_publish = data_gate.filter_new_bars(df_ohlcv, symbol=symbol, timeframe=timeframe)
        if df_to_publish.empty:
            log.info("Data gate: немає нових барів для %s %s.", symbol, timeframe)
            return False

    bar_cols = [
        "open_time",
        "close_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    bars_raw = df_to_publish[bar_cols].to_dict("records")
    bars: List[RedisBar] = [
        {
            "open_time": int(row["open_time"]),
            "close_time": int(row["close_time"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
        }
        for row in bars_raw
    ]

    symbol_norm = _normalize_symbol(symbol)
    rate_limit_key = (symbol_norm, timeframe)
    if min_publish_interval > 0 and publish_rate_limit is not None:
        now = time.monotonic()
        last_ts = publish_rate_limit.get(rate_limit_key)
        if last_ts is not None and now - last_ts < min_publish_interval:
            time.sleep(min_publish_interval - (now - last_ts))
    base_payload: Dict[str, Any] = {
        "symbol": symbol_norm,
        "tf": timeframe,
        "bars": bars,
    }
    payload = dict(base_payload)
    if hmac_secret:
        payload["sig"] = compute_payload_hmac(
            base_payload,
            hmac_secret,
            hmac_algo,
        )

    message = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    try:
        redis_client.publish(REDIS_CHANNEL, message)
        if publish_rate_limit is not None:
            publish_rate_limit[rate_limit_key] = time.monotonic()
        log.info(
            "Опубліковано %d барів у Redis канал %s (%s %s).",
            len(bars),
            REDIS_CHANNEL,
            symbol_norm,
            timeframe,
        )
        PROM_BARS_PUBLISHED.labels(symbol=symbol_norm, tf=timeframe).inc(len(bars))
        if bars:
            last_close_sec = bars[-1]["close_time"] / 1000.0
            lag = max(0.0, _now_utc().timestamp() - last_close_sec)
            PROM_STREAM_LAG_SECONDS.labels(symbol=symbol_norm, tf=timeframe).set(lag)
            if data_gate is not None:
                data_gate.record_publish(df_to_publish, symbol=symbol, timeframe=timeframe)
        return True
    except Exception as exc:  # noqa: BLE001
        raise RedisRetryableError("Не вдалося надіслати OHLCV у Redis") from exc


def _fetch_and_publish_recent(
    fx: ForexConnect,
    *,
    symbol: str,
    timeframe_raw: str,
    redis_client: Optional[Any],
    lookback_minutes: int,
    last_open_time_ms: Dict[Tuple[str, str], int],
    data_gate: Optional[PublishDataGate] = None,
    min_publish_interval: float = 0.0,
    publish_rate_limit: Optional[Dict[Tuple[str, str], float]] = None,
    hmac_secret: Optional[str] = None,
    hmac_algo: str = "sha256",
) -> pd.DataFrame:
    """Витягує останні `lookback_minutes` і публікує лише нові бари.

    Якщо ринок закритий або FXCM повертає технічну помилку, повертає
    порожній DataFrame без генерації noisу у логах.
    """

    end_dt = _now_utc()
    if not is_trading_time(end_dt):
        _notify_market_closed(end_dt, redis_client)
        raise MarketTemporarilyClosed("Ринок закритий")

    start_dt = end_dt - dt.timedelta(minutes=max(lookback_minutes, 1))
    timeframe_norm = _map_timeframe_label(timeframe_raw)

    try:
        history = fx.get_history(symbol, timeframe_raw, start_dt, end_dt)
    except Exception as exc:  # noqa: BLE001
        if _is_price_history_not_ready(exc):
            PROM_PRICE_HISTORY_NOT_READY.inc()
            _notify_market_closed(end_dt, redis_client)
            log.debug(
                "Стрім: FXCM PriceHistoryCommunicator не готовий для %s (%s).",
                symbol,
                timeframe_raw,
            )
            raise MarketTemporarilyClosed("PriceHistoryCommunicator is not ready")
        raise FXCMRetryableError(
            f"FXCM get_history помилився для {symbol} ({timeframe_raw})"
        ) from exc

    df_raw = pd.DataFrame(history)
    if df_raw.empty:
        log.debug(
            "Стрім: FXCM повернув порожні дані для %s (%s) у вікні %s хв.",
            symbol,
            timeframe_raw,
            lookback_minutes,
        )
        return pd.DataFrame()

    df_ohlcv = _normalize_history_to_ohlcv(df_raw, symbol, timeframe_norm)
    if df_ohlcv.empty:
        return pd.DataFrame()

    key = (_normalize_symbol(symbol), timeframe_norm)
    cutoff = last_open_time_ms.get(key)
    if cutoff is not None:
        df_ohlcv = cast(pd.DataFrame, df_ohlcv.loc[df_ohlcv["open_time"] > cutoff])

    if df_ohlcv.empty:
        return pd.DataFrame()

    df_to_publish = df_ohlcv

    if redis_client is None:
        if data_gate is not None:
            data_gate.record_publish(df_to_publish, symbol=symbol, timeframe=timeframe_norm)
        newest = int(df_to_publish["open_time"].max())
        last_open_time_ms[key] = newest
        return df_to_publish

    publish_ok = publish_ohlcv_to_redis(
        df_to_publish,
        symbol=symbol,
        timeframe=timeframe_norm,
        redis_client=redis_client,
        data_gate=data_gate,
        min_publish_interval=min_publish_interval,
        publish_rate_limit=publish_rate_limit,
        hmac_secret=hmac_secret,
        hmac_algo=hmac_algo,
    )
    if not publish_ok:
        return pd.DataFrame()
    newest = int(df_to_publish["open_time"].max())
    last_open_time_ms[key] = newest
    _publish_market_status(redis_client, "open")
    return df_to_publish


def run_redis_healthcheck(redis_client: Optional[Any]) -> bool:
    """Перевіряє доступність Redis і здатність приймати повідомлення."""

    if redis_client is None:
        log.error("Redis-клієнт відсутній — health-check провалено.")
        return False

    ping_ok = False
    publish_ok = False

    try:
        redis_client.ping()
        ping_ok = True
    except Exception as exc:  # noqa: BLE001
        log.exception("Redis health-check: ping неуспішний: %s", exc)

    probe_payload = json.dumps(
        {
            "type": "healthcheck",
            "ts": int(dt.datetime.utcnow().timestamp() * 1000),
        },
        separators=(",", ":"),
    )
    try:
        redis_client.publish(REDIS_CHANNEL, probe_payload)
        publish_ok = True
    except Exception as exc:  # noqa: BLE001
        log.exception("Redis health-check: publish неуспішний: %s", exc)

    if ping_ok and publish_ok:
        log.info("Redis health-check: OK (ping + publish).")
    else:
        log.error("Redis health-check: FAILED (ping=%s, publish=%s).", ping_ok, publish_ok)

    return ping_ok and publish_ok


def fetch_history_sample(
    fx: ForexConnect,
    *,
    redis_client: Optional[Any],
    sample_settings: SampleRequestSettings,
    heartbeat_channel: Optional[str] = None,
    data_gate: Optional[PublishDataGate] = None,
    hmac_secret: Optional[str] = None,
    hmac_algo: str = "sha256",
) -> bool:
    """POC: витягуємо шматок історії через ForexConnect.get_history.

    Усі параметри визначаємо через централізований конфіг (`SampleRequestSettings`).
    """
    symbol = sample_settings.symbol
    timeframe_raw = sample_settings.timeframe
    timeframe = _map_timeframe_label(timeframe_raw)
    hours = max(1, int(sample_settings.hours))

    end_dt = _now_utc()
    if not is_trading_time(end_dt):
        _notify_market_closed(end_dt, redis_client)
        return False
    start_dt = end_dt - dt.timedelta(hours=hours)

    log.info(
        "Запит історії: %s, tf=%s (FXCM=%s), період %s → %s",
        symbol,
        timeframe,
        timeframe_raw,
        start_dt,
        end_dt,
    )

    try:
        history = fx.get_history(symbol, timeframe_raw, start_dt, end_dt)
    except Exception as exc:  # noqa: BLE001
        if _is_price_history_not_ready(exc):
            PROM_PRICE_HISTORY_NOT_READY.inc()
            _notify_market_closed(end_dt, redis_client)
            log.debug(
                "Warmup: FXCM PriceHistoryCommunicator не готовий для %s (%s).",
                symbol,
                timeframe_raw,
            )
        else:
            log.exception("Помилка під час запиту історії через get_history: %s", exc)
        return False

    df_raw = pd.DataFrame(history)

    if df_raw.empty:
        log.warning("FXCM повернув порожню історію для %s (%s).", symbol, timeframe)
        return False

    log.info("Отримано сирих барів: %d", len(df_raw))

    df_ohlcv = _normalize_history_to_ohlcv(df_raw, symbol, timeframe)

    if df_ohlcv.empty:
        log.warning("Після нормалізації OHLCV немає даних.")
        return False

    log.info("OHLCV-барів після нормалізації: %d", len(df_ohlcv))

    head_str = df_ohlcv.head().to_string()
    log.info("Перші 5 рядків OHLCV:\n%s", head_str)
    log.info("Останні 5 рядків OHLCV:\n%s", df_ohlcv.tail().to_string())

    try:
        publish_ok = publish_ohlcv_to_redis(
            df_ohlcv,
            symbol=symbol,
            timeframe=timeframe,
            redis_client=redis_client,
            data_gate=data_gate,
            hmac_secret=hmac_secret,
            hmac_algo=hmac_algo,
        )
    except RedisRetryableError as exc:
        PROM_ERROR_COUNTER.labels(type="redis").inc()
        log.error("Warmup-публікація OHLCV у Redis неуспішна: %s", exc)
        return False

    if publish_ok:
        log.info("Warmup-пакет успішно опубліковано у Redis.")
        _publish_market_status(redis_client, "open")
        _publish_heartbeat(
            redis_client,
            heartbeat_channel,
            state="warmup",  # sample-mode heartbeat
            last_bar_close_ms=int(df_ohlcv["close_time"].max()),
        )
    else:
        log.error("Warmup-публікація OHLCV у Redis не підтверджена.")

    return publish_ok


def stream_fx_data(
    fx: ForexConnect,
    *,
    redis_client: Optional[Any],
    require_redis: bool = True,
    poll_seconds: int,
    publish_interval_seconds: int,
    lookback_minutes: int,
    config: List[Tuple[str, str]],
    cache_manager: Optional[HistoryCache] = None,
    max_cycles: Optional[int] = None,
    fx_reconnector: Optional[Callable[[], ForexConnect]] = None,
    redis_reconnector: Optional[Callable[[], Any]] = None,
    heartbeat_channel: Optional[str] = None,
    data_gate: Optional[PublishDataGate] = None,
    hmac_secret: Optional[str] = None,
    hmac_algo: str = "sha256",
) -> None:
    """Безкінечний цикл стрімінгу OHLCV у Redis.

    `poll_seconds` контролює частоту звернення до FXCM, а
    `publish_interval_seconds` — мінімальний інтервал між публікаціями свічок у Redis.
    """

    if require_redis and redis_client is None:
        log.error("Стрім неможливий без Redis-клієнта.")
        return

    gate = data_gate or PublishDataGate()

    current_fx = fx
    current_redis = redis_client

    last_published_close_ms: Optional[int] = None
    last_open_time_ms: Dict[Tuple[str, str], int] = {}
    if cache_manager is not None:
        for symbol, tf_raw in config:
            cached = cache_manager.get_last_open_time(symbol, tf_raw)
            if cached is not None:
                key = (_normalize_symbol(symbol), _map_timeframe_label(tf_raw))
                last_open_time_ms[key] = cached
                gate.seed(symbol=symbol, timeframe=tf_raw, last_open=cached)
            cached_close = cache_manager.get_last_close_time(symbol, tf_raw)
            if cached_close is not None:
                if last_published_close_ms is None or cached_close > last_published_close_ms:
                    last_published_close_ms = cached_close
    publish_rate_limit: Dict[Tuple[str, str], float] = {}
    log.info(
        "Старт стрімінгу FXCM: %s, інтервал опитування %ss, вікно %s хв.",
        ", ".join(f"{sym} {tf}" for sym, tf in config),
        poll_seconds,
        lookback_minutes,
    )

    cycles = 0
    last_idle_log_ts: Optional[float] = None
    last_idle_next_open: Optional[dt.datetime] = None

    try:
        while True:
            cycle_start = time.monotonic()
            restart_cycle = False
            market_pause = False
            closed_reference: Optional[dt.datetime] = None
            now = _now_utc()
            if not is_trading_time(now):
                market_pause = True
                closed_reference = now
            else:
                for symbol, tf_raw in config:
                    try:
                        df_new = _fetch_and_publish_recent(
                            current_fx,
                            symbol=symbol,
                            timeframe_raw=tf_raw,
                            redis_client=current_redis,
                            lookback_minutes=lookback_minutes,
                            last_open_time_ms=last_open_time_ms,
                            data_gate=gate,
                            min_publish_interval=publish_interval_seconds,
                            publish_rate_limit=publish_rate_limit,
                            hmac_secret=hmac_secret,
                            hmac_algo=hmac_algo,
                        )
                    except MarketTemporarilyClosed as exc:
                        log.info("Стрім: ринок недоступний (%s)", exc)
                        market_pause = True
                        closed_reference = _now_utc()
                        break
                    except FXCMRetryableError as exc:
                        log.warning("Втрачено з'єднання з FXCM: %s", exc)
                        PROM_ERROR_COUNTER.labels(type="fxcm").inc()
                        if fx_reconnector is None:
                            raise
                        _close_fxcm_session(current_fx, announce=False)
                        current_fx = fx_reconnector()
                        restart_cycle = True
                        break
                    except RedisRetryableError as exc:
                        log.warning("Помилка Redis під час публікації: %s", exc)
                        PROM_ERROR_COUNTER.labels(type="redis").inc()
                        if redis_reconnector is None:
                            raise
                        current_redis = redis_reconnector()
                        restart_cycle = True
                        break

                    if df_new.empty:
                        continue

                    log.info(
                        "Стрім: %s %s → %d нових барів",
                        symbol,
                        tf_raw,
                        len(df_new),
                    )
                    last_published_close_ms = int(df_new["close_time"].max())
                    if cache_manager is not None:
                        cache_manager.append_stream_bars(
                            symbol_raw=symbol,
                            timeframe_raw=tf_raw,
                            df_new=df_new,
                        )

            if restart_cycle:
                gate.update_staleness_metrics()
                continue

            if market_pause:
                reference_time = closed_reference or _now_utc()
                next_open = _notify_market_closed(reference_time, current_redis)
                seconds_to_open = (
                    max(0.0, (next_open - _now_utc()).total_seconds()) if next_open else 0.0
                )
                should_log_idle_state = False
                if heartbeat_channel:
                    now_monotonic = time.monotonic()
                    if (
                        last_idle_log_ts is None
                        or now_monotonic - last_idle_log_ts >= IDLE_LOG_INTERVAL_SECONDS
                        or next_open != last_idle_next_open
                    ):
                        should_log_idle_state = True
                        last_idle_log_ts = now_monotonic
                        last_idle_next_open = next_open

                if should_log_idle_state and heartbeat_channel:
                    log.debug(
                        "Стрім призупинено: очікуємо %.0f с до наступної спроби (next_open ≥ %s).",
                        IDLE_LOG_INTERVAL_SECONDS,
                        next_open.isoformat() if next_open else "невідомо",
                    )
                PROM_NEXT_OPEN_SECONDS.set(seconds_to_open)
                _publish_heartbeat(
                    current_redis,
                    heartbeat_channel,
                    state="idle",
                    last_bar_close_ms=last_published_close_ms,
                    next_open=next_open,
                    sleep_seconds=poll_seconds,
                )
                gate.update_staleness_metrics()
                sleep_for = max(0.0, float(poll_seconds))
            else:
                _publish_heartbeat(
                    current_redis,
                    heartbeat_channel,
                    state="stream",
                    last_bar_close_ms=last_published_close_ms,
                )
                last_idle_log_ts = None
                last_idle_next_open = None
                gate.update_staleness_metrics()
                elapsed = time.monotonic() - cycle_start
                sleep_for = max(0.0, poll_seconds - elapsed)

            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                break
            if sleep_for and max_cycles is None:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        log.info("Стрім зупинено користувачем.")


def main() -> None:
    """Мінімальний тест підключення до FXCM та запиту історії.

    Логіка:
    - читаємо креденшали з .env (через python-dotenv);
    - логін через ForexConnect;
    - POC-запит історії свічок та нормалізація у OHLCV;
    - коректний логаут.
    """
    setup_logging()
    load_dotenv()

    log.info("Зчитуємо конфігурацію FXCM з ENV...")
    try:
        config: FXCMConfig = load_config()
    except ValueError as exc:
        log.error("%s", exc)
        return

    log.info(
        "Креденшали зчитано. Спроба логіну через %s (%s).",
        config.connection,
        config.host_url,
    )

    _apply_calendar_overrides(config.calendar)

    if config.observability.metrics_enabled:
        _ensure_metrics_server(config.observability.metrics_port)

    redis_client = _create_redis_client(config.redis)
    if redis_client is not None and config.redis_required:
        redis_ok = run_redis_healthcheck(redis_client)
        if not redis_ok:
            log.error(
                "Redis health-check не пройдено. Публікацію буде вимкнено на цю сесію."
            )
            redis_client = None

    if redis_client is None:
        if config.redis_required:
            log.error(
                "Redis не налаштований, а FXCM_REDIS_REQUIRED=1 — завершую роботу."
            )
            return
        log.warning(
            "Redis недоступний — публікація вимкнена (file-only режим)."
        )

    cache_manager: Optional[HistoryCache] = None
    if config.cache.enabled:
        cache_manager = HistoryCache(
            config.cache.root,
            config.cache.max_bars,
            config.cache.warmup_bars,
        )

    publish_gate = PublishDataGate()

    stream_mode = config.stream_mode
    poll_seconds = config.poll_seconds
    lookback_minutes = config.lookback_minutes
    stream_config = config.stream_targets

    primary_backoff = BackoffController(config.backoff.fxcm_login)
    fx_holder: Dict[str, Optional[ForexConnect]] = {
        "client": _obtain_fxcm_session(config, primary_backoff)
    }
    fx_stream_backoff = BackoffController(config.backoff.fxcm_stream)

    def reconnect_fxcm() -> ForexConnect:
        new_fx = _obtain_fxcm_session(config, fx_stream_backoff)
        fx_holder["client"] = new_fx
        return new_fx

    redis_holder: Dict[str, Optional[Any]] = {"client": redis_client}
    redis_reconnector: Optional[Callable[[], Any]] = None
    if config.redis_required and redis is not None:
        redis_stream_backoff = BackoffController(config.backoff.redis_stream)

        def _redis_reconnect() -> Any:
            client = _obtain_redis_client_blocking(config.redis, redis_stream_backoff)
            redis_holder["client"] = client
            return client

        redis_reconnector = _redis_reconnect

    try:
        fx_active = fx_holder["client"]
        if fx_active is None:
            log.error("FXCM клієнт відсутній — завершую роботу.")
            return

        if cache_manager is not None:
            for symbol, tf_raw in stream_config:
                cache_manager.ensure_ready(
                    fx_active,
                    symbol_raw=symbol,
                    timeframe_raw=tf_raw,
                )
                redis_conn = redis_holder["client"]
                if redis_conn is None:
                    continue
                tf_norm = _map_timeframe_label(tf_raw)
                force_warmup = _tf_to_minutes(tf_norm) == 1
                warmup_slice = cache_manager.get_bars_to_publish(
                    symbol_raw=symbol,
                    timeframe_raw=tf_raw,
                    limit=cache_manager.warmup_bars,
                    force=force_warmup,
                )
                if warmup_slice.empty:
                    continue
                try:
                    publish_ohlcv_to_redis(
                        warmup_slice,
                        symbol=symbol,
                        timeframe=tf_norm,
                        redis_client=redis_conn,
                        data_gate=publish_gate,
                        hmac_secret=config.hmac_secret,
                        hmac_algo=config.hmac_algo,
                    )
                except RedisRetryableError as exc:
                    PROM_ERROR_COUNTER.labels(type="redis").inc()
                    log.warning("Warmup із кешу: Redis недоступний: %s", exc)
                    if stream_mode and redis_reconnector is not None:
                        redis_conn = redis_reconnector()
                        try:
                            publish_ohlcv_to_redis(
                                warmup_slice,
                                symbol=symbol,
                                timeframe=tf_norm,
                                redis_client=redis_conn,
                                data_gate=publish_gate,
                                hmac_secret=config.hmac_secret,
                                hmac_algo=config.hmac_algo,
                            )
                        except RedisRetryableError as inner_exc:
                            log.error(
                                "Warmup із кешу: повторна помилка Redis: %s",
                                inner_exc,
                            )
                            redis_holder["client"] = None
                            continue
                    else:
                        redis_holder["client"] = None
                        continue

                last_published = int(warmup_slice["open_time"].max())
                cache_manager.mark_published(
                    symbol_raw=symbol,
                    timeframe_raw=tf_raw,
                    last_open_time=last_published,
                )
                if is_trading_time(_now_utc()):
                    _publish_market_status(redis_conn, "open")
                _publish_heartbeat(
                    redis_conn,
                    config.observability.heartbeat_channel,
                    state="warmup_cache",
                    last_bar_close_ms=int(warmup_slice["close_time"].max()),
                )
                log.info(
                    "Warmup із кешу: %s %s → %d барів",
                    symbol,
                    tf_raw,
                    len(warmup_slice),
                )
        if stream_mode:
            if config.redis_required:
                if redis is None:
                    log.error("Пакет redis не встановлено — режим стріму неможливий.")
                    return
                if redis_holder["client"] is None:
                    if redis_reconnector is None:
                        log.error("Redis недоступний — режим стріму неможливий.")
                        return
                    redis_holder["client"] = redis_reconnector()
            else:
                if redis is None:
                    log.info(
                        "Пакет redis не встановлено — продовжуємо лише з файловим кешем."
                    )
                elif redis_holder["client"] is None:
                    log.info(
                        "Redis недоступний — працюємо у file-only режимі (без публікацій)."
                    )
            stream_fx_data(
                fx_active,
                redis_client=redis_holder["client"],
                require_redis=config.redis_required,
                poll_seconds=poll_seconds,
                publish_interval_seconds=config.publish_interval_seconds,
                lookback_minutes=lookback_minutes,
                config=stream_config,
                cache_manager=cache_manager,
                fx_reconnector=reconnect_fxcm,
                redis_reconnector=redis_reconnector,
                heartbeat_channel=config.observability.heartbeat_channel,
                data_gate=publish_gate,
                hmac_secret=config.hmac_secret,
                hmac_algo=config.hmac_algo,
            )
        else:
            published = fetch_history_sample(
                fx_active,
                redis_client=redis_holder["client"],
                sample_settings=config.sample_request,
                heartbeat_channel=config.observability.heartbeat_channel,
                data_gate=publish_gate,
                hmac_secret=config.hmac_secret,
                hmac_algo=config.hmac_algo,
            )
            if not published:
                # На вихідних / святах це норма: ринок закритий, sample не йде.
                if is_trading_time(_now_utc()):
                    log.error(
                        "Під час запуску не вдалося підтвердити публікацію жодного "
                        "OHLCV-повідомлення (ринок відкритий — перевір FXCM/Redis)."
                    )
                else:
                    log.info(
                        "Startup-sample не виконувався, оскільки ринок закритий "
                        "на момент запуску."
                    )
    finally:
        _close_fxcm_session(fx_holder.get("client"))


if __name__ == "__main__":
    main()
