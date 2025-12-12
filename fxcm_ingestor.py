"""Валідація payload-ів з каналу fxcm:ohlcv перед записом у UnifiedStore."""

from __future__ import annotations

import datetime as dt
import hmac
import json
import os
from dataclasses import dataclass
from typing import Any, List, Mapping, MutableMapping, Tuple

from config import load_stream_targets_from_settings
from fxcm_schema import (
    DEFAULT_MAX_BARS_PER_PAYLOAD,
    MAX_FUTURE_DRIFT_SECONDS,
    MIN_ALLOWED_BAR_TIMESTAMP_MS,
    RedisBar,
)
from fxcm_security import compute_payload_hmac


def _normalize_timeframe(raw_tf: str) -> str:
    raw = (raw_tf or "").strip().lower()
    if not raw:
        return raw
    mapping = {
        "m1": "1m",
        "m5": "5m",
        "m15": "15m",
        "m30": "30m",
        "m60": "1h",
        "h1": "1h",
        "h4": "4h",
        "d1": "1d",
    }
    if raw in mapping:
        return mapping[raw]
    if raw.startswith("m") and raw[1:].isdigit():
        return f"{int(raw[1:])}m"
    if raw.startswith("h") and raw[1:].isdigit():
        return f"{int(raw[1:])}h"
    return raw


def _parse_target_pairs(raw_config: str) -> List[Tuple[str, str]]:
    targets: List[Tuple[str, str]] = []
    for chunk in raw_config.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if ":" in chunk:
            symbol_part, tf_part = chunk.split(":", 1)
        else:
            symbol_part, tf_part = chunk, "m1"
        symbol_part = symbol_part.strip()
        tf_part = (tf_part or "m1").strip()
        if not symbol_part:
            continue
        targets.append((symbol_part, tf_part or "m1"))
    return targets


def _parse_stream_config() -> tuple[tuple[str, ...], tuple[str, ...]]:
    env_override = os.environ.get("FXCM_STREAM_CONFIG")
    if env_override:
        targets = _parse_target_pairs(env_override)
    else:
        try:
            targets = load_stream_targets_from_settings()
        except Exception:  # pragma: no cover - файл конфігурації відсутній
            targets = []
    if not targets:
        return tuple(), tuple()
    symbols: List[str] = []
    timeframes: List[str] = []
    for symbol, tf in targets:
        if symbol and symbol not in symbols:
            symbols.append(symbol)
        tf_norm = _normalize_timeframe(tf)
        if tf_norm and tf_norm not in timeframes:
            timeframes.append(tf_norm)
    return tuple(symbols), tuple(timeframes)


_STREAM_SYMBOLS, _STREAM_TIMEFRAMES = _parse_stream_config()
DEFAULT_ALLOWED_SYMBOLS: tuple[str, ...] = _STREAM_SYMBOLS or ("XAU/USD", "EUR/USD")
DEFAULT_ALLOWED_TIMEFRAMES: tuple[str, ...] = _STREAM_TIMEFRAMES or ("1m", "5m", "15m", "30m", "1h", "4h", "1d")


class IngestValidationError(ValueError):
    """Порушення контракту даних при інгесті."""


@dataclass(frozen=True)
class IngestorConfig:
    """Налаштування переліку дозволених символів/таймфреймів та ліміту пакета."""

    allowed_symbols: tuple[str, ...] = DEFAULT_ALLOWED_SYMBOLS
    allowed_timeframes: tuple[str, ...] = DEFAULT_ALLOWED_TIMEFRAMES
    max_bars_per_payload: int = DEFAULT_MAX_BARS_PER_PAYLOAD
    hmac_secret: str | None = None
    hmac_algo: str = "sha256"

    @classmethod
    def from_env(cls) -> "IngestorConfig":
        return cls(
            allowed_symbols=_resolve_allowed_symbols(),
            allowed_timeframes=_resolve_allowed_timeframes(),
            max_bars_per_payload=_resolve_max_batch_size(),
            hmac_secret=_resolve_hmac_secret(),
            hmac_algo=_resolve_hmac_algo(),
        )


class FXCMIngestor:
    """Санітизує payload з Redis та гарантує базову схему."""

    def __init__(self, config: IngestorConfig | None = None) -> None:
        self.config = config or IngestorConfig.from_env()
        if not self.config.allowed_symbols:
            raise ValueError("Список дозволених символів не може бути порожнім.")
        if not self.config.allowed_timeframes:
            raise ValueError("Список дозволених таймфреймів не може бути порожнім.")
        self._allowed_symbols = {_normalize_symbol(symbol) for symbol in self.config.allowed_symbols}
        self._allowed_timeframes = {_normalize_timeframe(tf) for tf in self.config.allowed_timeframes}
        self._max_bars = max(1, int(self.config.max_bars_per_payload))
        self._hmac_secret = self.config.hmac_secret
        self._hmac_algo = self.config.hmac_algo

    def validate_json_message(self, message: str) -> Mapping[str, Any]:
        """Декодує JSON-повідомлення та запускає повну валідацію."""

        try:
            payload = json.loads(message)
        except json.JSONDecodeError as exc:  # pragma: no cover - захищаємось від «смiття» в логах
            raise IngestValidationError("Інгестор не зміг розпарсити JSON-повідомлення") from exc
        if not isinstance(payload, MutableMapping):
            raise IngestValidationError("Очікувався JSON-об'єкт із полями symbol/tf/bars.")
        return self.validate_payload(payload)

    def validate_payload(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        if self._hmac_secret:
            self._verify_signature(payload)
        symbol_norm = self._validate_symbol(payload.get("symbol"))
        timeframe_norm = self._validate_timeframe(payload.get("tf"))
        bars = self._validate_bars(payload.get("bars"))
        return {"symbol": symbol_norm, "tf": timeframe_norm, "bars": bars}

    def _verify_signature(self, payload: Mapping[str, Any]) -> None:
        sig = payload.get("sig")
        if not isinstance(sig, str) or not sig:
            raise IngestValidationError("HMAC-підпис відсутній або порожній.")
        base_payload = {
            "symbol": payload.get("symbol"),
            "tf": payload.get("tf"),
            "bars": payload.get("bars"),
        }
        expected = compute_payload_hmac(base_payload, self._hmac_secret or "", self._hmac_algo)
        if not hmac.compare_digest(sig, expected):
            raise IngestValidationError("HMAC-підпис некоректний.")

    def _validate_symbol(self, symbol_raw: Any) -> str:
        if not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise IngestValidationError("Поле symbol відсутнє або не є рядком.")
        normalized = _normalize_symbol(symbol_raw)
        if normalized not in self._allowed_symbols:
            raise IngestValidationError(f"Символ {symbol_raw!r} не дозволено до інгесту.")
        return normalized

    def _validate_timeframe(self, timeframe_raw: Any) -> str:
        if not isinstance(timeframe_raw, str) or not timeframe_raw.strip():
            raise IngestValidationError("Поле tf відсутнє або не є рядком.")
        normalized = _normalize_timeframe(timeframe_raw)
        if normalized not in self._allowed_timeframes:
            raise IngestValidationError(f"Таймфрейм {timeframe_raw!r} не входить до дозволеного переліку.")
        return normalized

    def _validate_bars(self, bars_raw: Any) -> List[RedisBar]:
        if not isinstance(bars_raw, list):
            raise IngestValidationError("Поле bars має містити список барів.")
        if len(bars_raw) > self._max_bars:
            raise IngestValidationError(
                f"Payload містить {len(bars_raw)} барів, що перевищує ліміт {self._max_bars}."
            )
        sanitized: List[RedisBar] = []
        previous_open: int | None = None
        now_ms = _now_ms()
        max_allowed = now_ms + MAX_FUTURE_DRIFT_SECONDS * 1000
        for idx, bar in enumerate(bars_raw):
            if isinstance(bar, Mapping):
                # Phase C: live-незакриті бари не пишемо/не передаємо далі.
                # Якщо complete відсутній — вважаємо, що бар complete=True.
                complete = bar.get("complete", True)
                if complete is False:
                    continue
            sanitized_bar = self._sanitize_bar(bar, idx)
            if sanitized_bar["open_time"] < MIN_ALLOWED_BAR_TIMESTAMP_MS:
                raise IngestValidationError(
                    f"Бар #{idx} має open_time < {MIN_ALLOWED_BAR_TIMESTAMP_MS} (зсув у минуле)."
                )
            if sanitized_bar["open_time"] > max_allowed:
                raise IngestValidationError(
                    f"Бар #{idx} має open_time у майбутньому (>{max_allowed})."
                )
            if previous_open is not None and sanitized_bar["open_time"] <= previous_open:
                raise IngestValidationError("open_time повинен суворо зростати в межах одного payload.")
            sanitized.append(sanitized_bar)
            previous_open = sanitized_bar["open_time"]
        return sanitized

    def _sanitize_bar(self, bar_raw: Any, idx: int) -> RedisBar:
        if not isinstance(bar_raw, Mapping):
            raise IngestValidationError(f"Бар #{idx} має бути JSON-об'єктом.")
        try:
            open_time = int(bar_raw["open_time"])
            close_time = int(bar_raw["close_time"])
            open_price = float(bar_raw["open"])
            high_price = float(bar_raw["high"])
            low_price = float(bar_raw["low"])
            close_price = float(bar_raw["close"])
            volume = float(bar_raw["volume"])
        except KeyError as key:
            raise IngestValidationError(f"Бар #{idx} не містить обов'язкового поля {key.args[0]!r}.") from None
        except (TypeError, ValueError):
            raise IngestValidationError(f"Бар #{idx} має некоректні числові значення.") from None

        if close_time < open_time:
            raise IngestValidationError(f"Бар #{idx} має close_time < open_time.")
        if low_price > high_price:
            raise IngestValidationError(f"Бар #{idx} має low > high, payload зіпсовано.")

        return RedisBar(
            open_time=open_time,
            close_time=close_time,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
        )


def _now_ms() -> int:
    return int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)


def _normalize_symbol(symbol: str) -> str:
    return symbol.replace("/", "").upper()


def _split_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return tuple()
    items = [chunk.strip() for chunk in value.split(",") if chunk.strip()]
    return tuple(items)


def _resolve_allowed_symbols() -> tuple[str, ...]:
    env_symbols = _split_csv(os.environ.get("FXCM_INGEST_ALLOWED_SYMBOLS"))
    if env_symbols:
        return env_symbols
    stream_symbols, _ = _parse_stream_config()
    if stream_symbols:
        return stream_symbols
    return DEFAULT_ALLOWED_SYMBOLS


def _resolve_allowed_timeframes() -> tuple[str, ...]:
    env_tf = _split_csv(os.environ.get("FXCM_INGEST_ALLOWED_TIMEFRAMES"))
    if env_tf:
        return tuple(_normalize_timeframe(tf) for tf in env_tf)
    _, stream_tf = _parse_stream_config()
    if stream_tf:
        return stream_tf
    return DEFAULT_ALLOWED_TIMEFRAMES


def _resolve_max_batch_size() -> int:
    raw = os.environ.get("FXCM_INGEST_MAX_BATCH")
    if raw is None:
        return DEFAULT_MAX_BARS_PER_PAYLOAD
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_MAX_BARS_PER_PAYLOAD
    return max(1, value)


def _resolve_hmac_secret() -> str | None:
    candidates = (
        os.environ.get("FXCM_INGEST_HMAC_SECRET"),
        os.environ.get("FXCM_HMAC_SECRET"),
    )
    for candidate in candidates:
        if candidate and candidate.strip():
            return candidate.strip()
    return None


def _resolve_hmac_algo() -> str:
    raw = (
        os.environ.get("FXCM_INGEST_HMAC_ALGO")
        or os.environ.get("FXCM_HMAC_ALGO")
        or "sha256"
    )
    return raw.strip().lower() or "sha256"
