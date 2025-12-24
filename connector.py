"""Мінімальний конектор FXCM через ForexConnectAPI.

POC:
- логін до FXCM через ForexConnect;
- запит історичних свічок через get_history;
- нормалізація у OHLCV-формат, сумісний із AiOne_t;
- вивід кількості барів і перших рядків.

Приклад запуску:
    $ python connector.py
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import math
import os
import queue
import random
import sys
import threading
import time
import statistics
from collections import defaultdict, deque
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field
from functools import partial
from logging import Logger
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    cast,
)

import pandas as pd
from env_profile import load_env_profile
from prometheus_client import Counter, Gauge, start_http_server

try:
    from forexconnect import Common, ForexConnect  # type: ignore[import]
except ImportError:  # pragma: no cover - SDK може бути не встановлено під час тестів

    class ForexConnect:  # type: ignore[override, no-redef]
        """Заглушка, щоб тести могли імпортувати модуль без SDK."""

        def __init__(
            self, *args: Any, **kwargs: Any
        ) -> None:  # noqa: D401 - простий плейсхолдер
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

    Common = None  # type: ignore[assignment]

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
    BackoffTuning,
    CalendarSettings,
    FXCMConfig,
    RedisSettings,
    SampleRequestSettings,
    TickCadenceTuning,
    load_config,
)
from fxcm_schema import (
    MAX_FUTURE_DRIFT_SECONDS,
    MIN_ALLOWED_BAR_TIMESTAMP_MS,
    MarketStatusPayload,
    SessionContextPayload,
    validate_ohlcv_payload_contract,
    validate_price_tik_payload_contract,
)
from fxcm_security import compute_payload_hmac
from rich.console import Console
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text
from sessions import (
    calendar_snapshot,
    generate_request_windows,
    is_trading_time,
    next_trading_open,
    override_calendar,
    resolve_session,
)
from tick_ohlcv import (
    FxcmTick,
    OhlcvFromLowerTfAggregator,
    TickOhlcvAggregator,
)
from tick_ohlcv import (
    OhlcvBar as TickOhlcvBar,
)

# Налаштування логування
log: Logger = logging.getLogger("fxcm_connector")
_LOGGING_CONFIGURED = False
_RICH_CONSOLE: Optional[Console] = None

# Рейт-ліміт для шумних повідомлень про відкидання барів.
# Найчастіший нормальний сценарій: FXCM history повертає поточний (ще не закритий)
# бар, а ми синтетично рахуємо close_time як open_time + tf - 1 → close_time у майбутньому.
_DROP_TS_LOG_STATE: Dict[Tuple[str, str, str], float] = {}
DROP_TS_LOG_INTERVAL_SECONDS = 60.0


def _should_log_drop_ts(symbol_norm: str, tf: str, reason: str) -> bool:
    """Неблокуючий rate-limit для логів про відкидання барів."""

    now = time.monotonic()
    key = (str(symbol_norm), str(tf), str(reason))
    last = _DROP_TS_LOG_STATE.get(key)
    if last is not None and now - last < DROP_TS_LOG_INTERVAL_SECONDS:
        return False
    _DROP_TS_LOG_STATE[key] = now
    return True


class VolumeCalibrator:
    """Калібрує tick-agg preview volume під FXCM history volume.

    Мета:
    - `complete=true` бари публікуються з FXCM history (Volume максимально «реальний»);
    - `complete=false` preview з tick-agg має виглядати реалістично (масштаб/динаміка),
      тому оцінюємо `volume ≈ tick_count * k`.

    Обмеження:
    - Це НЕ біржовий volume (для spot FX його немає в OfferTable).
    - k оцінюється з пар (history_volume, tick_count) по однаковому open_time.
    """

    def __init__(self, *, max_samples: int = 50) -> None:
        self._max_samples = max(5, int(max_samples))
        self._ratios: Dict[Tuple[str, str], Deque[float]] = {}
        self._samples: Dict[Tuple[str, str], Deque[Dict[str, Any]]] = {}
        self._best: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._last_debug: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _safe_median(values: Sequence[float]) -> Optional[float]:
        if not values:
            return None
        try:
            return float(statistics.median(list(values)))
        except Exception:
            return None

    @staticmethod
    def _compute_k_l2(samples: Sequence[Mapping[str, Any]]) -> Optional[float]:
        num = 0.0
        den = 0.0
        for s in samples:
            tc = s.get("tick_count")
            hv = s.get("history_volume")
            if not isinstance(tc, int) or tc <= 0:
                continue
            if not isinstance(hv, (int, float)) or hv < 0:
                continue
            num += float(tc) * float(hv)
            den += float(tc) * float(tc)
        if den <= 0.0:
            return None
        return num / den

    @staticmethod
    def _compute_mape_pct(samples: Sequence[Mapping[str, Any]], k: float) -> Optional[float]:
        errors: List[float] = []
        for s in samples:
            tc = s.get("tick_count")
            hv = s.get("history_volume")
            if not isinstance(tc, int) or tc <= 0:
                continue
            if not isinstance(hv, (int, float)):
                continue
            hv_f = float(hv)
            if hv_f <= 0.0:
                continue
            pred = float(tc) * float(k)
            errors.append(abs(pred - hv_f) / hv_f * 100.0)
        if not errors:
            return None
        try:
            return float(statistics.median(errors))
        except Exception:
            return None

    def update_from_history_bar(
        self,
        *,
        symbol_norm: str,
        tf_norm: str,
        open_time_ms: int,
        history_volume: float,
        tick_count: int,
    ) -> None:
        if tick_count <= 0:
            return
        if history_volume < 0:
            return
        ratio = float(history_volume) / float(tick_count)
        key = (str(symbol_norm), str(tf_norm))
        with self._lock:
            buf = self._ratios.get(key)
            if buf is None:
                buf = deque(maxlen=self._max_samples)
                self._ratios[key] = buf
            buf.append(ratio)

            sbuf = self._samples.get(key)
            if sbuf is None:
                sbuf = deque(maxlen=self._max_samples)
                self._samples[key] = sbuf
            sample = {
                "open_time": int(open_time_ms),
                "history_volume": float(history_volume),
                "tick_count": int(tick_count),
                "ratio": float(ratio),
            }
            sbuf.append(sample)

            ratios: List[float] = []
            for s in sbuf:
                ratio_value = s.get("ratio")
                if isinstance(ratio_value, float):
                    ratios.append(ratio_value)
                elif isinstance(ratio_value, int):
                    ratios.append(float(ratio_value))
            k_median = self._safe_median(ratios)
            k_l2 = self._compute_k_l2(sbuf)

            best_k: Optional[float] = None
            best_method: Optional[str] = None
            mape_median = self._compute_mape_pct(sbuf, k_median) if k_median is not None else None
            mape_l2 = self._compute_mape_pct(sbuf, k_l2) if k_l2 is not None else None

            if (
                k_median is not None
                and mape_median is not None
                and (mape_l2 is None or mape_median <= mape_l2)
            ):
                best_k = float(k_median)
                best_method = "median"
            elif k_l2 is not None and mape_l2 is not None:
                best_k = float(k_l2)
                best_method = "l2"
            elif k_median is not None:
                best_k = float(k_median)
                best_method = "median"
            elif k_l2 is not None:
                best_k = float(k_l2)
                best_method = "l2"

            predicted_volume: Optional[float] = None
            err_pct: Optional[float] = None
            if best_k is not None and history_volume > 0:
                predicted_volume = float(tick_count) * float(best_k)
                err_pct = (predicted_volume - float(history_volume)) / float(history_volume) * 100.0

            self._best[key] = {
                "k": best_k,
                "method": best_method,
                "mape_pct": mape_median if best_method == "median" else mape_l2,
                "k_median": k_median,
                "k_l2": k_l2,
                "mape_median_pct": mape_median,
                "mape_l2_pct": mape_l2,
            }

            self._last_debug[key] = {
                "open_time": int(open_time_ms),
                "history_volume": float(history_volume),
                "tick_count": int(tick_count),
                "ratio": float(ratio),
                "samples": int(len(buf)),
                "predicted_volume": predicted_volume,
                "err_pct": err_pct,
            }

    def estimate_preview_volume(
        self,
        *,
        symbol_norm: str,
        tf_norm: str,
        tick_count: int,
        fallback: float,
    ) -> float:
        if tick_count <= 0:
            return 0.0
        key = (str(symbol_norm), str(tf_norm))
        base_key = (str(symbol_norm), str(BASE_HISTORY_TF_NORM))
        with self._lock:
            best = self._best.get(key)
            if best is None and key != base_key:
                # MTF масштабування: ми калібруємо k лише на базовому TF (1m),
                # але tick_count лінійно агрерується для 5m/15m/1h/4h.
                # Тому, якщо для конкретного TF ще немає семплів, беремо k з 1m.
                best = self._best.get(base_key)
            k = None
            if isinstance(best, Mapping):
                k = best.get("k")
            if not isinstance(k, (int, float)):
                buf = self._ratios.get(key)
                if (not buf) and key != base_key:
                    buf = self._ratios.get(base_key)
                if not buf:
                    return float(fallback)
                try:
                    k = float(statistics.median(list(buf)))
                except Exception:
                    return float(fallback)
        return float(tick_count) * float(k)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            ratios = {key: list(values) for key, values in self._ratios.items()}
            last_debug = dict(self._last_debug)
            best = dict(self._best)
        summary: Dict[str, Any] = {}
        for (symbol, tf), values in ratios.items():
            if not values:
                continue
            best_entry = best.get((symbol, tf)) if isinstance(best, dict) else None
            k_value: Optional[float] = None
            if isinstance(best_entry, Mapping) and isinstance(best_entry.get("k"), (int, float)):
                k_value = float(best_entry["k"])
            if k_value is None:
                try:
                    k_value = float(statistics.median(values))
                except Exception:
                    continue
            sym_map = summary.setdefault(symbol, {})
            sym_map[tf] = {
                "k": float(k_value),
                "samples": int(len(values)),
                "last": last_debug.get((symbol, tf)),
                "method": (best_entry.get("method") if isinstance(best_entry, Mapping) else None),
                "mape_pct": (best_entry.get("mape_pct") if isinstance(best_entry, Mapping) else None),
                "k_median": (best_entry.get("k_median") if isinstance(best_entry, Mapping) else None),
                "k_l2": (best_entry.get("k_l2") if isinstance(best_entry, Mapping) else None),
                "mape_median_pct": (best_entry.get("mape_median_pct") if isinstance(best_entry, Mapping) else None),
                "mape_l2_pct": (best_entry.get("mape_l2_pct") if isinstance(best_entry, Mapping) else None),
            }
        return summary

    def reset(self) -> None:
        """Очищає накопичені семпли (переважно для юніт-тестів)."""

        with self._lock:
            self._ratios.clear()
            self._samples.clear()
            self._best.clear()
            self._last_debug.clear()

        with self._lock:
            self._ratios.clear()
            self._last_debug.clear()


_VOLUME_CALIBRATOR = VolumeCalibrator(max_samples=50)


def _resolve_connector_version() -> Tuple[str, str]:
    """Повертає офіційну версію конектора та джерело.

    Порядок пріоритетів:
    1) FXCM_CONNECTOR_VERSION_OVERRIDE (ENV) — для CI/деплою;
    2) файл VERSION у корені репозиторію;
    3) fallback "0.0.0".
    """

    override = os.getenv("FXCM_CONNECTOR_VERSION_OVERRIDE")
    if override and override.strip():
        return override.strip(), "env"

    version_path = Path(__file__).resolve().parent / "VERSION"
    try:
        if version_path.exists():
            text = version_path.read_text(encoding="utf-8").strip()
            if text:
                return text, "VERSION"
    except Exception:
        # Версія не має ламати роботу конектора.
        pass

    return "0.0.0", "fallback"


FXCM_CONNECTOR_VERSION, _FXCM_CONNECTOR_VERSION_SOURCE = _resolve_connector_version()


def _enforce_required_connector_version() -> None:
    """Опційно перевіряє версію конектора (для деплою/контролю сумісності).

    Якщо задано FXCM_CONNECTOR_REQUIRED_VERSION і вона не збігається з поточною,
    конектор завершується з кодом 2.
    """

    required = os.getenv("FXCM_CONNECTOR_REQUIRED_VERSION")
    if not required or not required.strip():
        return
    required = required.strip()
    if required == FXCM_CONNECTOR_VERSION:
        return
    log.error(
        "Невідповідність версії конектора: очікується %s, поточна %s (source=%s).",
        required,
        FXCM_CONNECTOR_VERSION,
        _FXCM_CONNECTOR_VERSION_SOURCE,
    )
    raise SystemExit(2)


class ConsoleStatusBar:
    """Живий status bar у консолі через Rich Live.

    На відміну від `\r` у STDOUT, Rich Live коректно співіснує з
    `RichHandler` у PowerShell/VS Code: статус оновлюється в одному рядку,
    а логи друкуються над ним.
    """

    def __init__(
        self, *, enabled: bool = True, refresh_per_second: float = 4.0
    ) -> None:
        global _RICH_CONSOLE
        console = _RICH_CONSOLE
        # Вимикаємо bar, якщо stdout не TTY або console ще не готовий.
        # Логи йдуть у stderr через RichHandler, тому орієнтуємось на stderr.
        self._enabled = bool(enabled) and sys.stderr.isatty() and console is not None
        self._console = console
        self._live: Optional[Live] = None
        self._refresh_per_second = max(1.0, float(refresh_per_second))
        self._spinner: Optional[Spinner] = None

    def start(self) -> None:
        if not self._enabled or self._console is None:
            return
        if self._live is not None:
            return
        self._spinner = Spinner("dots")
        self._live = Live(
            self._render_panel({}),
            console=self._console,
            refresh_per_second=self._refresh_per_second,
            transient=True,
        )
        self._live.__enter__()

    def stop(self) -> None:
        if self._live is None:
            return
        try:
            self._live.__exit__(None, None, None)
        finally:
            self._live = None
            self._spinner = None

    def update(self, snapshot: Mapping[str, Any]) -> None:
        if self._live is None:
            return
        self._live.update(self._render_panel(snapshot))

    @staticmethod
    def _format_next_open(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, dt.datetime):
            dt_value = value
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                dt_value = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return value
        if isinstance(value, str):
            # unreachable (handled above), лишаємо для ясності
            return value

        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=dt.timezone.utc)
        dt_utc = dt_value.astimezone(dt.timezone.utc).replace(microsecond=0)
        return dt_utc.strftime("%Y-%m-%d %H:%M UTC")

    @staticmethod
    def _format_short_duration(seconds: Optional[float]) -> Optional[str]:
        """Компактний формат тривалості: 59s, 3m07s, 2h05, 3d12."""

        if seconds is None:
            return None
        try:
            seconds_value = float(seconds)
        except (TypeError, ValueError):
            return None
        seconds_value = max(0.0, seconds_value)
        total = int(seconds_value)
        if total < 60:
            return f"{total}s"
        minutes, secs = divmod(total, 60)
        if minutes < 60:
            return f"{minutes}m{secs:02d}s"
        hours, minutes = divmod(minutes, 60)
        if hours < 24:
            return f"{hours}h{minutes:02d}"
        days, hours = divmod(hours, 24)
        return f"{days}d{hours:02d}"

    def _render_panel(self, snapshot: Mapping[str, Any]) -> Panel:
        mode = str(snapshot.get("mode") or "?")
        market_open = bool(snapshot.get("market_open"))
        calendar_open = bool(snapshot.get("calendar_open"))
        ticks_alive = bool(snapshot.get("ticks_alive"))
        redis_ok = bool(snapshot.get("redis_connected"))

        market_style = "green" if market_open else "yellow"
        calendar_style = "green" if calendar_open else "yellow"
        redis_style = "green" if redis_ok else "red"
        ticks_style = "green" if ticks_alive else "red"

        tick_silence = snapshot.get("tick_silence_seconds")
        tick_age_value: Optional[float] = None
        if tick_silence is not None:
            try:
                tick_age_value = float(tick_silence)
            except (TypeError, ValueError):
                tick_age_value = None
        if tick_age_value is not None and tick_age_value >= 0:
            if tick_age_value >= STATUS_PRICE_DOWN_SECONDS:
                ticks_style = "red"
            elif tick_age_value >= STATUS_PRICE_STALE_SECONDS:
                ticks_style = "yellow"

        headline = Text.assemble(
            ("mode=", "dim"),
            (mode, "bold"),
            ("  |  market=", "dim"),
            ("open" if market_open else "closed", market_style),
            ("  |  cal=", "dim"),
            ("open" if calendar_open else "closed", calendar_style),
            ("  |  ticks=", "dim"),
            ("alive" if ticks_alive else "down", ticks_style),
            ("  |  redis=", "dim"),
            ("ok" if redis_ok else "down", redis_style),
        )

        rows: List[Tuple[Text, Text]] = []

        if tick_age_value is not None and tick_age_value >= 0:
            rows.append(
                (
                    Text("tick_age", style="dim"),
                    Text(f"{tick_age_value:.1f}s", style=ticks_style),
                )
            )

        lag = snapshot.get("lag_seconds")
        if lag is not None:
            try:
                lag_value = float(lag)
            except (TypeError, ValueError):
                lag_value = None
            if lag_value is not None and lag_value >= 0:
                lag_label = (
                    self._format_short_duration(lag_value) or f"{lag_value:.1f}s"
                )
                rows.append((Text("lag", style="dim"), Text(lag_label, style="cyan")))

        ohlcv_live_age = snapshot.get("ohlcv_live_age_seconds")
        if ohlcv_live_age is not None:
            try:
                live_age_value = float(ohlcv_live_age)
            except (TypeError, ValueError):
                live_age_value = None
            if live_age_value is not None and live_age_value >= 0:
                label = self._format_short_duration(live_age_value) or f"{live_age_value:.1f}s"
                rows.append(
                    (Text("ohlcv_live_age", style="dim"), Text(label, style="cyan"))
                )

        supervisor_q = snapshot.get("supervisor_ohlcv_queue")
        supervisor_drop = snapshot.get("supervisor_ohlcv_dropped")
        supervisor_bp = snapshot.get("supervisor_backpressure")
        if supervisor_q is not None or supervisor_drop is not None or supervisor_bp is not None:
            try:
                q_value = int(supervisor_q) if supervisor_q is not None else None
            except (TypeError, ValueError):
                q_value = None
            try:
                d_value = int(supervisor_drop) if supervisor_drop is not None else None
            except (TypeError, ValueError):
                d_value = None
            bp_value = bool(supervisor_bp) if supervisor_bp is not None else None
            bp_style = "yellow" if bp_value else "green"
            parts: List[str] = []
            if q_value is not None:
                parts.append(f"q={q_value}")
            if d_value is not None:
                parts.append(f"drop={d_value}")
            if bp_value is not None:
                parts.append("bp=1" if bp_value else "bp=0")
            if parts:
                rows.append(
                    (Text("ohlcv_pipe", style="dim"), Text(" ".join(parts), style=bp_style))
                )

        next_open = self._format_next_open(snapshot.get("next_open"))
        if next_open:
            rows.append(
                (Text("next_open", style="dim"), Text(f"≥ {next_open}", style="cyan"))
            )

        idle_reason = snapshot.get("idle_reason")
        if idle_reason:
            rows.append(
                (Text("reason", style="dim"), Text(str(idle_reason), style="yellow"))
            )

        sleep_for = snapshot.get("sleep_for")
        if sleep_for is not None:
            try:
                sleep_value = float(sleep_for)
            except (TypeError, ValueError):
                sleep_value = None
            if sleep_value is not None and sleep_value >= 0:
                # У звичайному режимі poll часто = 5s, щоб не забивати панель — ховаємо.
                if abs(sleep_value - 5.0) > 0.05:
                    rows.append(
                        (
                            Text("sleep", style="dim"),
                            Text(f"{int(round(sleep_value))}s", style="dim"),
                        )
                    )

        bars = snapshot.get("cycle_published_bars")
        if bars is not None:
            try:
                bars_value = int(bars)
            except (TypeError, ValueError):
                bars_value = None
            if bars_value is not None and bars_value >= 0:
                rows.append(
                    (
                        Text("published_bars", style="dim"),
                        Text(str(bars_value), style="green"),
                    )
                )

        fx_bo = snapshot.get("history_backoff_seconds")
        if fx_bo is not None:
            try:
                fx_bo_value = float(fx_bo)
            except (TypeError, ValueError):
                fx_bo_value = None
            if fx_bo_value is not None and fx_bo_value > 0:
                rows.append(
                    (
                        Text("fxcm_backoff", style="dim"),
                        Text(f"{fx_bo_value:.1f}s", style="yellow"),
                    )
                )

        r_bo = snapshot.get("redis_backoff_seconds")
        if r_bo is not None:
            try:
                r_bo_value = float(r_bo)
            except (TypeError, ValueError):
                r_bo_value = None
            if r_bo_value is not None and r_bo_value > 0:
                rows.append(
                    (
                        Text("redis_backoff", style="dim"),
                        Text(f"{r_bo_value:.1f}s", style="yellow"),
                    )
                )

        if not rows:
            rows.append(
                (
                    Text("status", style="dim"),
                    Text("очікуємо оновлення стану…", style="dim"),
                )
            )

        table = Table.grid(expand=True)
        table.add_column(width=2)
        table.add_column(width=16)
        table.add_column(ratio=1)
        spinner = self._spinner or Text(" ")
        table.add_row(
            spinner,
            Text("FXCM connector", style="bold cyan"),
            Text("працюємо", style="bold"),
        )
        table.add_row(Text(""), Text(""), headline)
        for key, value in rows[:6]:
            table.add_row(Text(""), key, value)

        border_style = "green" if market_open else "yellow"
        return Panel.fit(
            table,
            title="Стан",
            title_align="center",
            padding=(0, 1),
            border_style=border_style,
        )


def setup_logging() -> None:
    """Налаштовуємо логування з RichHandler.

    Робимо простий формат, щоб було читабельно у консолі під час відладки.
    """
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    global _RICH_CONSOLE
    if _RICH_CONSOLE is None:
        # Вивід логів у stderr — стандартний та сумісний з більшістю середовищ.
        # Force terminal, щоб Rich не вимикав кольори у VS Code/PowerShell.
        force_terminal = (
            bool(sys.stderr.isatty()) or os.getenv("FXCM_RICH_FORCE_TERMINAL") == "1"
        )
        _RICH_CONSOLE = Console(
            stderr=True,
            force_terminal=force_terminal,
            color_system="standard" if force_terminal else None,
        )

    # Важливо: у Python 3.7 `logging.basicConfig(...)` НЕ перезаписує існуючі
    # root-handlers (і може бути no-op, якщо середовище/IDE вже налаштували логування).
    # Тому конфігуруємо саме логер `fxcm_connector`, щоб мати стабільний формат.
    target_logger = logging.getLogger("fxcm_connector")
    for handler in target_logger.handlers:
        if isinstance(handler, RichHandler):
            _LOGGING_CONFIGURED = True
            return

    handler = RichHandler(
        console=_RICH_CONSOLE,
        rich_tracebacks=True,
        show_path=False,
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))

    target_logger.setLevel(logging.INFO)
    target_logger.addHandler(handler)
    target_logger.propagate = False
    _LOGGING_CONFIGURED = True


# Конфігуруємо логування ще під час імпорту, щоб ранні log.debug не зникали.
setup_logging()

# Ініціалізація змінних Redis
try:
    import redis

    log.debug("redis версія %s", redis.__version__)  # type: ignore[union-attr]
except Exception:  # noqa: BLE001
    redis = None  # type: ignore[assignment]
    log.warning("redis не встановлено; деякі функції можуть бути недоступні.")

# ── FXCM / ForexConnect imports ──

try:
    from forexconnect import fxcorepy  # type: ignore[import]
except Exception as exc:
    fxcorepy = None
    log.warning(
        "fxcorepy/ForexConnect недоступні: %s; деякі функції "
        "(тик-стрім, частина FXCM-логіки) будуть вимкнені.",
        exc,
    )
else:
    # Не завалюємося, якщо конкретного методу немає.
    version = None

    # 1) Якщо у майбутніх версіях зʼявиться getVersion — пробуємо його використати.
    get_version = getattr(fxcorepy, "getVersion", None)
    if callable(get_version):
        try:
            version = get_version()  # type: ignore[misc]
        except Exception:
            version = None

    # 2) Інакше пробуємо взяти __version__ або просто лог без версії.
    if version is None:
        version = getattr(fxcorepy, "__version__", None)

    if version:
        log.debug("fxcorepy імпортовано успішно, версія %s", version)
    else:
        log.debug("fxcorepy імпортовано успішно, версію визначити не вдалося")


# Константи конектора
REDIS_CHANNEL = "fxcm:ohlcv"  # legacy дефолт; реальний канал беремо з config.ohlcv_channel
REDIS_STATUS_CHANNEL = "fxcm:market_status"  # канал публікації статусу ринку
STATUS_CHANNEL_DEFAULT = "fxcm:status"
STATUS_PRICE_STALE_SECONDS = 15.0
STATUS_PRICE_DOWN_SECONDS = 60.0
STATUS_OHLCV_DELAYED_SECONDS = 60.0
STATUS_OHLCV_DOWN_SECONDS = 180.0
BAR_INTERVAL_MS = 60_000  # 1 хвилина у мілісекундах
PRICE_SNAPSHOT_QUEUE_MAXSIZE = 5_000
_LAST_MARKET_STATUS: Optional[Tuple[str, Optional[int]]] = (
    None  # останній статус ринку для приглушення спаму
)
_LAST_MARKET_STATUS_TS: Optional[float] = None  # час останньої трансляції статусу
_STATUS_CHANNEL = STATUS_CHANNEL_DEFAULT
_OHLCV_CHANNEL = REDIS_CHANNEL
_LAST_STATUS_PUBLISHED_TS: Optional[float] = None
_LAST_STATUS_PUBLISHED_KEY: Optional[Tuple[Any, ...]] = None
_LAST_TELEMETRY_PUBLISHED_TS_BY_CHANNEL: Dict[str, float] = {}

# Мінімальний інтервал між публікаціями fxcm:status (anti-flood).
# Важливо: це публічний канал для зовнішніх консюмерів, не дебаг-лог.
STATUS_PUBLISH_MIN_INTERVAL_SECONDS = 1.0


def _telemetry_rate_limited(channel: str, *, now_ts: float) -> bool:
    """Повертає True, якщо публікацію в цей канал треба приглушити (anti-flood)."""

    interval = float(STATUS_PUBLISH_MIN_INTERVAL_SECONDS)
    if interval <= 0.0:
        return False
    last_ts = _LAST_TELEMETRY_PUBLISHED_TS_BY_CHANNEL.get(channel)
    return last_ts is not None and now_ts - last_ts < interval


def _telemetry_mark_published(channel: str, *, now_ts: float) -> None:
    _LAST_TELEMETRY_PUBLISHED_TS_BY_CHANNEL[channel] = now_ts


def _status_publish_key(snapshot: Mapping[str, Any]) -> Tuple[Any, ...]:
    """Будує ключ стану без полів, що часто змінюються (лічильники/ts).

    Використовується тільки для rate-limit fxcm:status, щоб не спамити Redis.
    """

    session = snapshot.get("session")
    if isinstance(session, Mapping):
        session_key = (
            session.get("name"),
            session.get("tag"),
            session.get("state"),
            session.get("current_open_utc"),
            session.get("current_close_utc"),
            session.get("next_open_utc"),
        )
    else:
        session_key = None
    return (
        snapshot.get("process"),
        snapshot.get("market"),
        snapshot.get("price"),
        snapshot.get("ohlcv"),
        snapshot.get("note"),
        session_key,
    )


_LAST_STATUS_SNAPSHOT: Optional[Dict[str, Any]] = None
_LAST_SESSION_CONTEXT: Optional[Dict[str, Any]] = None
_CURRENT_MARKET_STATE = "unknown"
_METRICS_SERVER_STARTED = False
IDLE_LOG_INTERVAL_SECONDS = (
    300.0  # мінімальний інтервал між повідомленнями про паузу торгів
)
MARKET_STATUS_REFRESH_SECONDS = 30.0  # як часто повторно транслювати незмінний статус
_SESSION_STATS_TRACKER: Optional[SessionStatsTracker] = None
_SESSION_TAG_SETTING = os.environ.get("FXCM_SESSION_TAG", "AUTO").strip() or "AUTO"
_AUTO_SESSION_TAG = _SESSION_TAG_SETTING.upper() == "AUTO"
_DEFAULT_SESSION_TAG = "NY_METALS"
SESSION_WEEKEND_THRESHOLD_SECONDS = 36 * 3600  # 36 годин — явно weekend-gap
SESSION_OVERNIGHT_THRESHOLD_SECONDS = (
    6 * 3600
)  # все, що довше 6 годин, не intraday break

# Черги для взаємодії між потоками та асинхронними тасками
OHLCV_QUEUE_MAXSIZE = 200
PRICE_QUEUE_MAXSIZE = 500
HEARTBEAT_QUEUE_MAXSIZE = 200
MARKET_STATUS_QUEUE_MAXSIZE = 50
SUPERVISOR_SUBMIT_TIMEOUT_SECONDS = 2.0
SUPERVISOR_RETRY_DELAY_SECONDS = 1.0
SUPERVISOR_JOIN_TIMEOUT_SECONDS = 5.0
THROTTLE_LOG_INTERVAL_SECONDS = 12.0
_INFO_THROTTLE_REASONS = {"min_interval"}


# Prometheus-метрики
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

PROM_TICK_AGG_TICKS_TOTAL = Counter(
    "fxcm_tick_agg_ticks_total",
    "Сумарна кількість тиків, оброблених TickOhlcvWorker",
    ["symbol"],
)
PROM_TICK_AGG_BARS_TOTAL = Counter(
    "fxcm_tick_agg_bars_total",
    "Кількість згенерованих барів TickOhlcvWorker (complete bars)",
    ["symbol", "tf", "synthetic"],
)
PROM_TICK_AGG_AVG_SPREAD = Gauge(
    "fxcm_tick_agg_avg_spread",
    "Оцінка avg spread (ask-bid) для закритих барів tick-agg",
    ["symbol", "tf"],
)
PROM_STREAM_STALENESS_SECONDS = Gauge(
    "fxcm_stream_staleness_seconds",
    "Час у секундах від останнього опублікованого close_time",
    ["symbol", "tf"],
)
PROM_STREAM_LAST_CLOSE_MS = Gauge(
    "fxcm_stream_last_close_ms",
    "Unix epoch (ms) close_time останнього опублікованого бару",
    ["symbol", "tf"],
)

PROM_OHLCV_COMPLETE_LAST_TS_SECONDS = Gauge(
    "fxcm_ohlcv_complete_last_ts_seconds",
    "UNIX-час (seconds) останньої публікації complete=true бару",
    ["symbol", "tf"],
)
PROM_OHLCV_LIVE_LAST_TS_SECONDS = Gauge(
    "fxcm_ohlcv_live_last_ts_seconds",
    "UNIX-час (seconds) останньої публікації live complete=false бару",
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
PROM_CALENDAR_OPEN = Gauge(
    "fxcm_calendar_open",
    "Статус ринку за календарем: 1=open, 0=closed",
)
PROM_TICK_STREAM_ALIVE = Gauge(
    "fxcm_tick_stream_alive",
    "Чи надходять тики (tick_silence нижче порогу): 1=alive, 0=down",
)
PROM_EFFECTIVE_MARKET_OPEN = Gauge(
    "fxcm_effective_market_open",
    "Ефективний стан стріму: 1=працюємо, 0=idle",
)
PROM_CALENDAR_CLOSED_TICKS_ALIVE = Counter(
    "fxcm_calendar_closed_ticks_alive_total",
    "Скільки разів календар CLOSED, але тики ще надходили",
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

PROM_COMMANDS_TOTAL = Counter(
    "fxcm_commands_total",
    "Скільки команд з `fxcm:commands` було оброблено",
    ["type", "result"],
)
PROM_COMMANDS_LAST_TS_SECONDS = Gauge(
    "fxcm_commands_last_ts_seconds",
    "UNIX-час (seconds) останньої обробленої команди з `fxcm:commands`",
)

PROM_UNIVERSE_APPLY_TOTAL = Counter(
    "fxcm_universe_apply_total",
    "Скільки разів конектор застосував dynamic universe оновлення",
    ["result"],
)
PROM_UNIVERSE_ACTIVE_SYMBOLS = Gauge(
    "fxcm_universe_active_symbols",
    "Кількість активних символів у universe (symbols)",
)
PROM_UNIVERSE_LAST_APPLY_TS_SECONDS = Gauge(
    "fxcm_universe_last_apply_ts_seconds",
    "UNIX-час (seconds) останнього успішного apply universe",
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


class HistoryQuota:
    """Неблокуючий rate-limiter для get_history з ковзними вікнами та пріоритетами."""

    _MIN_INTERVAL_THRESHOLD = (
        0.45  # значення за замовчуванням для мінімальних інтервалів
    )
    _WARN_THRESHOLD = 0.7  # дефолт для попереджувального стану
    _RESERVE_THRESHOLD = 0.7  # дефолт для пріоритетного резерву
    _CRITICAL_THRESHOLD = 0.9  # дефолт для критичного стану

    def __init__(
        self,
        max_calls_per_min: int,
        max_calls_per_hour: int,
        *,
        min_interval_by_tf: Optional[Dict[str, float]] = None,
        priority_targets: Optional[Sequence[Tuple[str, str]]] = None,
        priority_reserve_per_min: int = 0,
        priority_reserve_per_hour: int = 0,
        load_thresholds: Optional[Mapping[str, float]] = None,
    ) -> None:
        self.max_calls_per_min = max(1, int(max_calls_per_min))
        self.max_calls_per_hour = max(1, int(max_calls_per_hour))
        normalized_intervals: Dict[str, float] = {}
        if min_interval_by_tf:
            for raw_tf, value in min_interval_by_tf.items():
                if value is None:
                    continue
                tf_key = self._tf_key(raw_tf)
                if not tf_key:
                    continue
                normalized_intervals[tf_key] = max(0.0, float(value))
        self.min_interval_by_tf = normalized_intervals
        self.priority_reserve_per_min = max(0, int(priority_reserve_per_min))
        self.priority_reserve_per_hour = max(0, int(priority_reserve_per_hour))
        self._priority_targets: Set[Tuple[str, str]] = {
            self._priority_key(symbol, timeframe)
            for symbol, timeframe in (priority_targets or [])
        }
        thresholds = load_thresholds or {}
        self._min_interval_threshold = self._clamp_ratio(
            thresholds.get("min_interval"),
            self._MIN_INTERVAL_THRESHOLD,
        )
        self._warn_threshold = self._clamp_ratio(
            thresholds.get("warn"),
            self._WARN_THRESHOLD,
        )
        self._reserve_threshold = self._clamp_ratio(
            thresholds.get("reserve"),
            self._RESERVE_THRESHOLD,
        )
        self._critical_threshold = self._clamp_ratio(
            thresholds.get("critical"),
            self._CRITICAL_THRESHOLD,
        )
        self._calls_60s: Deque[float] = deque()
        self._calls_3600s: Deque[float] = deque()
        self._priority_calls_60s: Deque[float] = deque()
        self._priority_calls_3600s: Deque[float] = deque()
        self._non_priority_calls_60s: Deque[float] = deque()
        self._non_priority_calls_3600s: Deque[float] = deque()
        self._last_call_ts_by_tf: Dict[str, float] = {}
        self._skipped_polls: Dict[Tuple[str, str], int] = defaultdict(int)
        self._last_next_slot = 0.0
        self._last_denied_ts: Optional[float] = None
        self._last_denied_reason: Optional[str] = None
        self._state_decay_seconds = 15.0

    @staticmethod
    def _tf_key(label: str) -> str:
        return (label or "").strip().lower()

    @staticmethod
    def _priority_key(symbol: str, timeframe: str) -> Tuple[str, str]:
        return (_normalize_symbol(symbol), _map_timeframe_label(timeframe).lower())

    @staticmethod
    def _clamp_ratio(value: Optional[float], default: float) -> float:
        if value is None:
            return default
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        if math.isnan(parsed):
            return default
        return min(1.0, max(0.0, parsed))

    def _prune(self, now: float) -> None:
        cutoff_60 = now - 60.0
        for window in (
            self._calls_60s,
            self._priority_calls_60s,
            self._non_priority_calls_60s,
        ):
            while window and window[0] <= cutoff_60:
                window.popleft()
        cutoff_3600 = now - 3_600.0
        for window in (
            self._calls_3600s,
            self._priority_calls_3600s,
            self._non_priority_calls_3600s,
        ):
            while window and window[0] <= cutoff_3600:
                window.popleft()

    @staticmethod
    def _window_wait(
        window: Deque[float], limit: int, window_seconds: float, now: float
    ) -> float:
        if limit <= 0 or len(window) < limit:
            return 0.0
        oldest = window[0]
        remaining = window_seconds - (now - oldest)
        return max(0.0, remaining)

    def _interval_wait(self, tf_label: str, now: float) -> float:
        tf_key = self._tf_key(tf_label)
        min_interval = self.min_interval_by_tf.get(tf_key)
        if not min_interval:
            return 0.0
        last_call = self._last_call_ts_by_tf.get(tf_key)
        if last_call is None:
            return 0.0
        return max(0.0, min_interval - (now - last_call))

    def allow_call(
        self, symbol: str, timeframe: str, now: float
    ) -> Tuple[bool, float, str]:
        """Перевіряє бюджет get_history без блокування."""

        self._prune(now)
        tf_norm = _map_timeframe_label(timeframe)
        is_priority = self._priority_key(symbol, timeframe) in self._priority_targets
        wait_60 = self._window_wait(self._calls_60s, self.max_calls_per_min, 60.0, now)
        wait_3600 = self._window_wait(
            self._calls_3600s, self.max_calls_per_hour, 3_600.0, now
        )
        wait_interval = self._interval_wait(tf_norm, now)
        wait_reserve = 0.0
        if not is_priority:
            wait_reserve = max(
                self._reserve_wait(
                    self._non_priority_calls_60s,
                    self.max_calls_per_min - self.priority_reserve_per_min,
                    60.0,
                    now,
                ),
                self._reserve_wait(
                    self._non_priority_calls_3600s,
                    self.max_calls_per_hour - self.priority_reserve_per_hour,
                    3_600.0,
                    now,
                ),
            )
        ratio_60 = (
            len(self._calls_60s) / self.max_calls_per_min
            if self.max_calls_per_min
            else 0.0
        )
        ratio_3600 = (
            len(self._calls_3600s) / self.max_calls_per_hour
            if self.max_calls_per_hour
            else 0.0
        )
        load_ratio = max(ratio_60, ratio_3600)
        apply_min_interval = (
            load_ratio >= self._min_interval_threshold and not is_priority
        )
        apply_priority_reserve = (
            load_ratio >= self._reserve_threshold and not is_priority
        )

        candidates: List[Tuple[str, float]] = []

        def _register_wait(label: str, value: float) -> None:
            if value > 0.0:
                candidates.append((label, value))

        _register_wait("minute_quota", wait_60)
        _register_wait("hour_quota", wait_3600)
        if apply_min_interval:
            _register_wait("min_interval", wait_interval)
        if apply_priority_reserve:
            _register_wait("priority_reserve", wait_reserve)

        if not candidates:
            self._last_next_slot = 0.0
            self._last_denied_reason = None
            return True, 0.0, "ok"

        deny_reason, wait_seconds = max(candidates, key=lambda item: item[1])

        self._last_next_slot = wait_seconds
        self._last_denied_ts = now
        self._last_denied_reason = deny_reason
        return False, wait_seconds, deny_reason

    @staticmethod
    def _reserve_wait(
        window: Deque[float], limit: int, window_seconds: float, now: float
    ) -> float:
        if limit <= 0:
            return window_seconds
        if len(window) < max(0, limit):
            return 0.0
        oldest = window[0]
        remaining = window_seconds - (now - oldest)
        return max(0.0, remaining)

    def record_call(self, symbol: str, timeframe: str, now: float) -> None:
        """Фіксує успішний виклик get_history."""

        tf_norm = _map_timeframe_label(timeframe)
        is_priority = self._priority_key(symbol, timeframe) in self._priority_targets
        self._calls_60s.append(now)
        self._calls_3600s.append(now)
        target_60 = (
            self._priority_calls_60s if is_priority else self._non_priority_calls_60s
        )
        target_3600 = (
            self._priority_calls_3600s
            if is_priority
            else self._non_priority_calls_3600s
        )
        target_60.append(now)
        target_3600.append(now)
        self._prune(now)
        self._last_call_ts_by_tf[self._tf_key(tf_norm)] = now
        self._last_next_slot = 0.0

    def register_skip(self, symbol: str, timeframe: str) -> None:
        key = (_normalize_symbol(symbol), _map_timeframe_label(timeframe))
        self._skipped_polls[key] += 1

    def snapshot(self) -> Dict[str, Any]:
        now = time.monotonic()
        state = self._resolve_state(now)
        skipped = [
            {"symbol": symbol, "tf": tf, "count": count}
            for (symbol, tf), count in sorted(
                self._skipped_polls.items(),
                key=lambda item: item[1],
                reverse=True,
            )
        ][:12]
        priority_targets = [
            f"{symbol}:{tf}" for symbol, tf in sorted(self._priority_targets)
        ]
        return {
            "calls_60s": len(self._calls_60s),
            "limit_60s": self.max_calls_per_min,
            "calls_3600s": len(self._calls_3600s),
            "limit_3600s": self.max_calls_per_hour,
            "priority": {
                "targets": priority_targets,
                "reserve_per_min": self.priority_reserve_per_min,
                "reserve_per_hour": self.priority_reserve_per_hour,
                "calls_60s": len(self._priority_calls_60s),
                "calls_3600s": len(self._priority_calls_3600s),
            },
            "throttle_state": state,
            "next_slot_seconds": round(self._last_next_slot, 3),
            "last_denied_reason": self._last_denied_reason,
            "skipped_polls": skipped,
        }

    def _resolve_state(self, now: float) -> str:
        ratio_60 = (
            len(self._calls_60s) / self.max_calls_per_min
            if self.max_calls_per_min
            else 0.0
        )
        ratio_3600 = (
            len(self._calls_3600s) / self.max_calls_per_hour
            if self.max_calls_per_hour
            else 0.0
        )
        max_ratio = max(ratio_60, ratio_3600)
        recent_quota_deny = (
            self._last_denied_ts is not None
            and now - self._last_denied_ts <= self._state_decay_seconds
            and (self._last_denied_reason in {"minute_quota", "hour_quota"})
        )
        if max_ratio >= self._critical_threshold and recent_quota_deny:
            return "critical"
        if max_ratio >= self._warn_threshold or recent_quota_deny:
            return "warn"
        return "ok"


class TickCadenceController:
    """Керує адаптивною частотою опитування history на основі тикової активності."""

    MIN_CADENCE_SECONDS = 2.0
    MAX_CADENCE_SECONDS = 20.0

    def __init__(
        self,
        *,
        tuning: TickCadenceTuning,
        base_poll_seconds: float,
        timeframes: Sequence[str],
    ) -> None:
        self._tuning = tuning
        self._base_poll = max(0.5, float(base_poll_seconds))
        tf_keys = {_map_timeframe_label(tf) for tf in timeframes if tf}
        if not tf_keys:
            tf_keys = {"1m"}
        self._tf_bases: Dict[str, float] = {}
        for tf in tf_keys:
            base = self._base_poll * max(1.0, _tf_to_minutes(tf) / 1.0)
            self._tf_bases[tf] = self._clamp(base)
        self._cadence_by_tf: Dict[str, float] = dict(self._tf_bases)
        self._next_due_by_tf: Dict[str, float] = {}
        self._prices_state = "ok"
        self._history_active = True
        self._tick_silence: Optional[float] = None
        self._last_reason = "live_ticks"
        self._current_multiplier = tuning.live_multiplier
        self._last_update_monotonic = time.monotonic()

    def update_state(
        self,
        *,
        tick_metadata: Optional[Dict[str, Any]],
        market_open: bool,
        now_monotonic: Optional[float] = None,
    ) -> None:
        if now_monotonic is None:
            now_monotonic = time.monotonic()
        silence = self._extract_silence(tick_metadata)
        self._tick_silence = silence
        previous_history_state = self._history_active

        if not market_open:
            prices_state = "stale"
            history_active = False
            reason = "calendar_closed"
        elif silence is None:
            prices_state = "lag"
            history_active = True
            reason = "no_ticks"
        elif silence <= self._tuning.live_threshold_seconds:
            prices_state = "ok"
            history_active = True
            reason = "live_ticks"
        elif silence >= self._tuning.idle_threshold_seconds:
            prices_state = "stale"
            history_active = False
            reason = "tick_idle"
        else:
            prices_state = "lag"
            history_active = True
            reason = "tick_lag"

        self._prices_state = prices_state
        self._history_active = history_active
        self._last_reason = reason
        self._current_multiplier = self._resolve_multiplier(prices_state)
        self._last_update_monotonic = now_monotonic
        self._recompute_cadence(now_monotonic)

        if history_active and not previous_history_state:
            for tf in self._tf_bases:
                self._next_due_by_tf[tf] = now_monotonic

    def history_state_label(self) -> str:
        return "active" if self._history_active else "paused"

    def should_poll(
        self, timeframe: str, now: Optional[float] = None
    ) -> Tuple[bool, float]:
        tf = _map_timeframe_label(timeframe)
        cadence = self._cadence_by_tf.get(tf, self._clamp(self._base_poll))
        if now is None:
            now = time.monotonic()
        if not self._history_active:
            self._next_due_by_tf[tf] = now + cadence
            return False, cadence
        next_due = self._next_due_by_tf.get(tf)
        if next_due is None or now >= next_due:
            self._next_due_by_tf[tf] = now + cadence
            return True, 0.0
        return False, max(0.0, next_due - now)

    def next_wakeup_in_seconds(
        self, now_monotonic: Optional[float] = None
    ) -> Optional[float]:
        if now_monotonic is None:
            now_monotonic = time.monotonic()
        if not self._next_due_by_tf:
            return None
        waits = [max(0.0, due - now_monotonic) for due in self._next_due_by_tf.values()]
        if not waits:
            return None
        return min(waits)

    def snapshot(self) -> Dict[str, Any]:
        now = time.monotonic()
        next_poll = {
            tf: max(0.0, due - now) for tf, due in self._next_due_by_tf.items()
        }
        return {
            "state": self._prices_state,
            "history_state": self.history_state_label(),
            "tick_silence_seconds": self._tick_silence,
            "cadence_seconds": {
                tf: round(value, 3) for tf, value in sorted(self._cadence_by_tf.items())
            },
            "next_poll_in_seconds": {
                tf: round(value, 3) for tf, value in sorted(next_poll.items())
            },
            "multiplier": round(self._current_multiplier, 3),
            "reason": self._last_reason,
        }

    def _recompute_cadence(self, now: float) -> None:
        for tf, base in self._tf_bases.items():
            new_value = self._clamp(base * self._current_multiplier)
            previous = self._cadence_by_tf.get(tf)
            self._cadence_by_tf[tf] = new_value
            if previous is None:
                continue
            next_due = self._next_due_by_tf.get(tf)
            if next_due is None or next_due <= now:
                self._next_due_by_tf[tf] = now
                continue
            if new_value < previous and next_due - now > new_value:
                self._next_due_by_tf[tf] = now + new_value

    def _resolve_multiplier(self, prices_state: str) -> float:
        if prices_state == "ok":
            return self._tuning.live_multiplier
        if prices_state == "lag":
            return (self._tuning.live_multiplier + self._tuning.idle_multiplier) / 2.0
        return self._tuning.idle_multiplier

    def _extract_silence(self, metadata: Optional[Dict[str, Any]]) -> Optional[float]:
        if not metadata:
            return None
        silence = metadata.get("tick_silence_seconds")
        if silence is None:
            return None
        try:
            value = float(silence)
        except (TypeError, ValueError):
            return None
        if value < 0:
            return None
        return value

    @classmethod
    def _clamp(cls, value: float) -> float:
        return min(cls.MAX_CADENCE_SECONDS, max(cls.MIN_CADENCE_SECONDS, value))


class FxcmBackoffController:
    """Потокобезпечний backoff з джитером для FXCM history та Redis."""

    def __init__(
        self,
        *,
        base_seconds: float,
        max_seconds: float,
        multiplier: float,
        jitter: float,
    ) -> None:
        self._base_seconds = max(0.1, float(base_seconds))
        self._max_seconds = max(self._base_seconds, float(max_seconds))
        self._multiplier = max(1.0, float(multiplier))
        self._jitter = max(0.0, float(jitter))
        self._lock = threading.Lock()
        self._current_sleep = self._base_seconds
        self._fail_count = 0
        self._active = False
        self._last_error: Optional[str] = None
        self._last_error_ts: Optional[dt.datetime] = None
        self._resume_wall: Optional[dt.datetime] = None
        self._resume_monotonic = 0.0

    @classmethod
    def from_tuning(cls, tuning: BackoffTuning) -> "FxcmBackoffController":
        return cls(
            base_seconds=tuning.base_seconds,
            max_seconds=tuning.max_seconds,
            multiplier=tuning.multiplier,
            jitter=tuning.jitter,
        )

    def fail(self, reason: str) -> float:
        with self._lock:
            target = self._current_sleep if self._active else self._base_seconds
            next_sleep = min(
                self._max_seconds, target * (self._multiplier if self._active else 1.0)
            )
            jitter_factor = 1.0
            if self._jitter > 0.0:
                jitter_factor = random.uniform(
                    max(0.0, 1.0 - self._jitter), 1.0 + self._jitter
                )
            sleep_seconds = min(
                self._max_seconds, max(self._base_seconds, next_sleep * jitter_factor)
            )
            now_mon = time.monotonic()
            now_wall = dt.datetime.now(dt.timezone.utc)
            self._current_sleep = sleep_seconds
            self._fail_count += 1
            self._active = True
            self._last_error = reason
            self._last_error_ts = now_wall
            self._resume_monotonic = now_mon + sleep_seconds
            self._resume_wall = now_wall + dt.timedelta(seconds=sleep_seconds)
            return sleep_seconds

    def success(self) -> None:
        with self._lock:
            if not self._active and self._fail_count == 0:
                return
            self._active = False
            self._fail_count = 0
            self._current_sleep = self._base_seconds
            self._resume_monotonic = 0.0
            self._resume_wall = None

    def remaining(self) -> float:
        with self._lock:
            return self._remaining_locked()

    def is_active(self) -> bool:
        with self._lock:
            self._expire_if_needed_locked()
            return self._active

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            remaining = self._remaining_locked()
            return {
                "active": self._active and remaining > 0.0,
                "sleep_seconds": round(self._current_sleep, 3),
                "remaining_seconds": round(max(0.0, remaining), 3),
                "fail_count": self._fail_count,
                "last_error": self._last_error,
                "last_error_ts": self._format_ts(self._last_error_ts),
                "resume_at": self._format_ts(self._resume_wall),
            }

    def _remaining_locked(self) -> float:
        if not self._active:
            return 0.0
        remaining = self._resume_monotonic - time.monotonic()
        if remaining <= 0:
            self._expire_if_needed_locked()
            return 0.0
        return remaining

    def _expire_if_needed_locked(self) -> None:
        if not self._active:
            return
        if self._resume_monotonic <= 0.0:
            return
        if self._resume_monotonic - time.monotonic() <= 0:
            self._active = False
            self._resume_monotonic = 0.0
            self._resume_wall = None

    @staticmethod
    def _format_ts(value: Optional[dt.datetime]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()


class FXCMRetryableError(RuntimeError):
    """Сигналізує, що необхідно перепідключити сесію FXCM."""


class RedisRetryableError(RuntimeError):
    """Сигналізує, що Redis потрібно перепідключити."""


class MarketTemporarilyClosed(RuntimeError):
    """Ринок закритий або FXCM тимчасово не готовий відповідати."""


class SinkBackpressure(RuntimeError):
    """Черга supervisor переповнена або подію не вдалося доставити."""


class SinkSubmissionError(RuntimeError):
    """Будь-яка інша помилка під час передачі події в supervisor."""


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
    log.debug("Успішний логін до FXCM через ForexConnect.")
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


def _obtain_fxcm_session(
    config: FXCMConfig, backoff: BackoffController
) -> ForexConnect:
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
        session_windows=settings.session_windows or None,
        replace_holidays=bool(settings.holidays),
    )


def _ensure_metrics_server(port: int) -> None:
    global _METRICS_SERVER_STARTED
    if _METRICS_SERVER_STARTED:
        return
    start_http_server(port)
    log.info("Prometheus-метрики доступні на порту %s.", port)
    _METRICS_SERVER_STARTED = True


def _tf_to_minutes(tf_label: str) -> int:
    label = tf_label.strip().lower()
    if label.endswith("m"):
        return max(1, int(label[:-1] or "1"))
    if label.endswith("h"):
        return max(1, int(label[:-1] or "1")) * 60
    if label.endswith("d"):
        return max(1, int(label[:-1] or "1")) * 1_440
    raise ValueError(f"Невідомий таймфрейм: {tf_label}")


# Базовий таймфрейм FXCM history для масштабування MTF.
# Важливо: для багатьох символів не тягнемо history по кожному TF, а беремо лише 1m,
# а старші TF для `complete=true` будуємо локально.
BASE_HISTORY_TF_NORM = "1m"
BASE_HISTORY_TF_RAW = "m1"
HISTORY_AGG_SOURCE = "history_agg"


class HistoryMtfCompleteAggregator:
    """Інкрементально агрегує `complete=true` 1m history-бари у старші TF.

    Правила:
    - Публікуємо `complete=true` для старших TF лише коли маємо повне вікно
      кратних 1m барів з рівною дискретизацією (без пропусків у хвилинах).
    - Якщо старт стріму всередині вікна (наприклад, посеред години) — перший
      неповний bucket просто пропускаємо.
    """

    def __init__(self, *, symbol_norm: str, target_tf: str) -> None:
        self._symbol = str(symbol_norm)
        self._tf = str(target_tf)
        target_minutes = _tf_to_minutes(self._tf)
        self._target_ms = int(target_minutes) * BAR_INTERVAL_MS
        self._lower_ms = int(BAR_INTERVAL_MS)
        self._bars_per_window = max(1, int(self._target_ms // self._lower_ms))

        self._bucket_start: Optional[int] = None
        self._expected_next_open: Optional[int] = None
        self._window: List[Dict[str, Any]] = []

    @property
    def tf(self) -> str:
        return self._tf

    def ingest_df_1m(self, df_1m: pd.DataFrame) -> pd.DataFrame:
        if df_1m is None or df_1m.empty:
            return pd.DataFrame()
        if "open_time" not in df_1m.columns:
            return pd.DataFrame()

        rows_out: List[Dict[str, Any]] = []
        # Працюємо у порядку часу.
        df_sorted = cast(pd.DataFrame, df_1m.sort_values("open_time"))
        for row in df_sorted.itertuples(index=False):
            try:
                open_time = int(getattr(row, "open_time"))
            except Exception:  # noqa: BLE001
                continue

            bucket_start = (open_time // self._target_ms) * self._target_ms

            # Якщо ми ще не в bucket-і або bucket змінився — стартуємо заново.
            if self._bucket_start is None or bucket_start != self._bucket_start:
                self._bucket_start = int(bucket_start)
                self._window.clear()
                self._expected_next_open = int(bucket_start)

            # Публікуємо лише повні bucket-и: якщо почали не з початку bucket-а —
            # пропускаємо, поки не дійдемо до рівно bucket_start.
            if self._expected_next_open is None:
                self._expected_next_open = int(bucket_start)
            if open_time != int(self._expected_next_open):
                # Пропуск/дірка у хвилинах або старт з середини bucket-а.
                # Скидаємо вікно і чекаємо наступного валідного bucket_start.
                self._window.clear()
                self._expected_next_open = int(bucket_start)
                if open_time != int(self._expected_next_open):
                    continue

            bar_open = float(getattr(row, "open"))
            bar_high = float(getattr(row, "high"))
            bar_low = float(getattr(row, "low"))
            bar_close = float(getattr(row, "close"))
            bar_volume = float(getattr(row, "volume"))

            self._window.append(
                {
                    "open": bar_open,
                    "high": bar_high,
                    "low": bar_low,
                    "close": bar_close,
                    "volume": bar_volume,
                }
            )
            self._expected_next_open = int(self._expected_next_open) + self._lower_ms

            if len(self._window) < self._bars_per_window:
                continue

            # Закриваємо bucket.
            window_start = int(self._bucket_start)
            rows_out.append(
                {
                    "symbol": self._symbol,
                    "tf": self._tf,
                    "open_time": window_start,
                    "close_time": window_start + self._target_ms - 1,
                    "open": float(self._window[0]["open"]),
                    "high": float(max(item["high"] for item in self._window)),
                    "low": float(min(item["low"] for item in self._window)),
                    "close": float(self._window[-1]["close"]),
                    "volume": float(sum(item["volume"] for item in self._window)),
                    "source": HISTORY_AGG_SOURCE,
                }
            )
            self._window.clear()
            self._bucket_start = None
            self._expected_next_open = None

        if not rows_out:
            return pd.DataFrame()
        return cast(pd.DataFrame, pd.DataFrame(rows_out))


class MtfCrosscheckController:
    """Періодично звіряє history_agg (1m→TF) з прямим FXCM history по TF.

    Це діагностика/контроль якості:
    - не змінює основний polling (він лишається лише 1m);
    - не публікує FXCM H1/H4 назовні (щоб не змішувати джерела);
    - лише логірує `WARNING` при розбіжності понад поріг.
    """

    def __init__(self, settings: Any) -> None:
        self._settings = settings
        self._last_check_monotonic: Dict[Tuple[str, str], float] = {}
        self._last_checked_open_ms: Dict[Tuple[str, str], int] = {}
        self._checks_in_cycle = 0

    def begin_cycle(self) -> None:
        self._checks_in_cycle = 0

    def maybe_check_from_agg_df(
        self,
        fx: ForexConnect,
        *,
        symbol_raw: str,
        target_tf: str,
        df_agg: pd.DataFrame,
    ) -> None:
        if not bool(getattr(self._settings, "enabled", False)):
            return
        if df_agg is None or df_agg.empty:
            return
        max_per_cycle = int(getattr(self._settings, "max_checks_per_cycle", 0) or 0)
        if self._checks_in_cycle >= max_per_cycle:
            return

        tf_norm = str(target_tf)
        timeframes = getattr(self._settings, "timeframes", None)
        if isinstance(timeframes, list) and timeframes and tf_norm not in set(timeframes):
            return

        symbol_norm = _normalize_symbol(symbol_raw)
        key = (symbol_norm, tf_norm)

        now_monotonic = time.monotonic()
        last = self._last_check_monotonic.get(key)
        min_interval = float(getattr(self._settings, "min_interval_seconds", 0.0) or 0.0)
        if last is not None and now_monotonic - last < min_interval:
            return

        try:
            # Перевіряємо лише найсвіжіший агрегований complete бар.
            last_row = df_agg.iloc[-1]
            open_ms = int(last_row.get("open_time"))
            close_ms = int(last_row.get("close_time"))
        except Exception:  # noqa: BLE001
            return

        if self._last_checked_open_ms.get(key) == open_ms:
            return

        try:
            ok = self._check_one_bar(
                fx,
                symbol_raw=symbol_raw,
                tf_norm=tf_norm,
                open_ms=open_ms,
                close_ms=close_ms,
                agg_row=last_row,
            )
        except Exception:  # noqa: BLE001
            return

        self._last_check_monotonic[key] = now_monotonic
        self._last_checked_open_ms[key] = open_ms
        self._checks_in_cycle += 1
        _ = ok

    def _check_one_bar(
        self,
        fx: ForexConnect,
        *,
        symbol_raw: str,
        tf_norm: str,
        open_ms: int,
        close_ms: int,
        agg_row: Any,
    ) -> bool:
        tf_minutes = _tf_to_minutes(tf_norm)
        tf_ms = int(tf_minutes) * 60_000

        start_dt = _ms_to_dt(int(open_ms) - 2 * tf_ms)
        end_dt = _ms_to_dt(int(close_ms) + 2 * tf_ms)

        tf_fxcm = _to_fxcm_timeframe(tf_norm)
        try:
            history = fx.get_history(symbol_raw, tf_fxcm, start_dt, end_dt)
        except Exception as exc:  # noqa: BLE001
            if _is_price_history_not_ready(exc):
                return True
            log.debug(
                "MTF cross-check: get_history не вдався для %s (%s): %s",
                symbol_raw,
                tf_fxcm,
                exc,
            )
            return True

        df_raw = pd.DataFrame(history)
        if df_raw.empty:
            return True
        df_direct = _normalize_history_to_ohlcv(df_raw, symbol_raw, tf_norm)
        if df_direct.empty:
            return True

        try:
            df_match = cast(
                pd.DataFrame,
                df_direct.loc[df_direct["open_time"] == int(open_ms)],
            )
        except Exception:  # noqa: BLE001
            return True

        if df_match.empty:
            log.warning(
                "MTF cross-check: FXCM не повернув бар open_time=%s для %s %s (FXCM=%s).",
                _ms_to_dt(int(open_ms)).isoformat(),
                _normalize_symbol(symbol_raw),
                tf_norm,
                tf_fxcm,
            )
            return False

        direct_row = df_match.iloc[-1]
        issues: List[str] = []

        try:
            direct_close_ms = int(direct_row.get("close_time"))
            if int(direct_close_ms) != int(close_ms):
                issues.append(
                    f"close_time_ms: agg={int(close_ms)} fxcm={int(direct_close_ms)}"
                )
        except Exception:  # noqa: BLE001
            pass

        price_abs_tol = float(getattr(self._settings, "price_abs_tol", 0.0) or 0.0)
        price_rel_tol = float(getattr(self._settings, "price_rel_tol", 0.0) or 0.0)
        volume_abs_tol = float(getattr(self._settings, "volume_abs_tol", 0.0) or 0.0)
        volume_rel_tol = float(getattr(self._settings, "volume_rel_tol", 0.0) or 0.0)

        def _cmp(field: str, *, abs_tol: float, rel_tol: float) -> None:
            try:
                a = float(agg_row.get(field))
                b = float(direct_row.get(field))
            except Exception:  # noqa: BLE001
                return
            if not math.isclose(a, b, abs_tol=abs_tol, rel_tol=rel_tol):
                issues.append(f"{field}: agg={a} fxcm={b} diff={a - b}")

        for price_field in ("open", "high", "low", "close"):
            _cmp(price_field, abs_tol=price_abs_tol, rel_tol=price_rel_tol)
        _cmp("volume", abs_tol=volume_abs_tol, rel_tol=volume_rel_tol)

        if issues:
            log.warning(
                "MTF cross-check: розбіжність > порогу для %s %s open=%s: %s",
                _normalize_symbol(symbol_raw),
                tf_norm,
                _ms_to_dt(int(open_ms)).isoformat(),
                "; ".join(issues),
            )
            return False
        return True


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
    tf_norm = _map_timeframe_label(timeframe_raw)
    tf_fxcm = _to_fxcm_timeframe(tf_norm)
    for win_start, win_end in generate_request_windows(start_dt, end_dt):
        if win_start >= win_end:
            continue
        try:
            history = fx.get_history(symbol, tf_fxcm, win_start, win_end)
        except Exception as exc:  # noqa: BLE001
            if _is_price_history_not_ready(exc):
                PROM_PRICE_HISTORY_NOT_READY.inc()
                _log_market_closed_once(_now_utc())
                log.debug(
                    "History cache: PriceHistoryCommunicator не готовий для %s (%s).",
                    symbol,
                    tf_fxcm,
                )
            else:
                log.exception(
                    "Помилка get_history(%s, %s, %s → %s): %s",
                    symbol,
                    tf_fxcm,
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

    def _persist_meta(
        self, symbol_norm: str, tf_norm: str, meta: Dict[str, Any]
    ) -> None:
        if self._disabled:
            return
        try:
            save_cache_meta(self.root, symbol_norm, tf_norm, meta)
        except Exception as exc:  # noqa: BLE001
            self._disable_cache(exc)

    def _persist_record(
        self, symbol_norm: str, tf_norm: str, df: pd.DataFrame, meta: Dict[str, Any]
    ) -> None:
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

    def get_cached_df(self, symbol_raw: str, timeframe_raw: str) -> pd.DataFrame:
        """Повертає поточний кешований DataFrame для `(symbol, tf)`.

        Використовується для внутрішніх операцій (warmup/агрегації) без
        дублювання читання з диска.
        """

        symbol_norm = _normalize_symbol(symbol_raw)
        tf_norm = _map_timeframe_label(timeframe_raw)
        record = self._load(symbol_norm, tf_norm)
        return record.data

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
            if is_trading_time(now) and last_close_dt < now - dt.timedelta(
                minutes=tf_minutes
            ):
                fetch_and_merge(last_close_dt + dt.timedelta(milliseconds=1), now)

            if len(df_cache) < self.warmup_bars and first_open_dt > desired_start:
                need_minutes = (self.warmup_bars - len(df_cache)) * tf_minutes
                fetch_and_merge(
                    first_open_dt - dt.timedelta(minutes=need_minutes), first_open_dt
                )

        if updated:
            meta = dict(record.meta)
            if not df_cache.empty:
                meta["last_close_time"] = int(df_cache["close_time"].max())
                meta["rows"] = len(df_cache)
            meta["last_refresh_utc"] = now.isoformat()
            self._persist_record(symbol_norm, tf_norm, df_cache, meta)
            self.records[self._key(symbol_norm, tf_norm)] = CacheRecord(df_cache, meta)
        else:
            self.records[self._key(symbol_norm, tf_norm)] = CacheRecord(
                df_cache, record.meta
            )

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


def _log_market_closed_once(
    now: dt.datetime, *, next_open: Optional[dt.datetime] = None
) -> dt.datetime:
    """Лог «ринок закритий» з приглушенням спаму."""

    attr = "_next_notice_utc"
    next_notice = getattr(_log_market_closed_once, attr, None)
    if isinstance(next_notice, dt.datetime) and now < next_notice:
        return next_notice

    next_open_dt = next_open or next_trading_open(now)
    log.debug(
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
) -> bool:
    global _LAST_MARKET_STATUS, _LAST_MARKET_STATUS_TS

    if redis_client is None:
        return False

    next_open_key = int(next_open.timestamp() * 1000) if next_open else None
    status_key = (state, next_open_key)
    is_state_change = _LAST_MARKET_STATUS != status_key
    now_monotonic = time.monotonic()
    if (
        _LAST_MARKET_STATUS == status_key
        and _LAST_MARKET_STATUS_TS is not None
        and now_monotonic - _LAST_MARKET_STATUS_TS < MARKET_STATUS_REFRESH_SECONDS
    ):
        return True

    payload = cast(
        MarketStatusPayload,
        {
            "type": "market_status",
            "state": state,
            "ts": _now_utc().replace(microsecond=0).isoformat(),
        },
    )
    if state == "closed" and next_open is not None:
        payload["next_open_utc"] = next_open.replace(microsecond=0).isoformat()
        payload["next_open_ms"] = int(next_open.timestamp() * 1000)
        payload["next_open_in_seconds"] = max(
            0.0,
            (next_open - _now_utc()).total_seconds(),
        )
    payload["session"] = _build_session_context(
        next_open, session_stats=_session_stats_snapshot()
    )

    message = json.dumps(payload, separators=(",", ":"))
    status_snapshot = _build_status_snapshot_from_market(payload)
    success = False
    try:
        now_ts = time.time()
        if _telemetry_rate_limited(REDIS_STATUS_CHANNEL, now_ts=now_ts):
            return True
        redis_client.publish(REDIS_STATUS_CHANNEL, message)
        _telemetry_mark_published(REDIS_STATUS_CHANNEL, now_ts=now_ts)
        _LAST_MARKET_STATUS = status_key
        _LAST_MARKET_STATUS_TS = now_monotonic
        if is_state_change:
            log.debug("Статус ринку → %s", state)
        else:
            log.debug("Статус ринку (refresh) → %s", state)
        PROM_MARKET_STATUS.set(1 if state == "open" else 0)
        success = True
    except Exception as exc:  # noqa: BLE001
        log.exception("Не вдалося надіслати market_status у Redis: %s", exc)
        PROM_ERROR_COUNTER.labels(type="redis").inc()
    finally:
        _publish_public_status(redis_client, status_snapshot)
    return success


def _sync_market_status_with_calendar(redis_client: Optional[Any]) -> None:
    """Публікує open/closed відповідно до календаря на момент виклику."""

    if redis_client is None:
        return
    now = _now_utc()
    if is_trading_time(now):
        _publish_market_status(redis_client, "open")
        return
    _notify_market_closed(now, redis_client)


def _float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _tick_stream_alive(
    tick_metadata: Optional[Dict[str, Any]],
    *,
    tick_cadence: Optional[TickCadenceController] = None,
) -> bool:
    """Повертає True, якщо FXCM ще реально шле тики.

    Важлива вимога S1: календар може показувати CLOSED, але FXCM інколи ще
    певний час продовжує штовхати котирування. У такому випадку ми не
    «засинаємо» тільки через календар — тримаємо стрім активним, доки тики
    не згаснуть.
    """

    if not tick_metadata:
        return False
    silence = _float_or_none(tick_metadata.get("tick_silence_seconds"))
    if silence is None:
        return False

    threshold = float(STATUS_PRICE_DOWN_SECONDS)
    if tick_cadence is not None:
        tuning = getattr(tick_cadence, "_tuning", None)
        idle_threshold = getattr(tuning, "idle_threshold_seconds", None)
        if idle_threshold is not None:
            try:
                threshold = float(idle_threshold)
            except (TypeError, ValueError):
                threshold = float(STATUS_PRICE_DOWN_SECONDS)

    return silence < threshold


def _humanize_session_name(tag: str) -> str:
    if not tag:
        return "Session"
    cleaned = tag.replace("_", " ").replace("-", " ").strip()
    return cleaned.title() or "Session"


def _parse_iso8601(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
        except (OverflowError, ValueError):
            return None
    if isinstance(value, str):
        candidate = value.replace("Z", "+00:00")
        try:
            parsed = dt.datetime.fromisoformat(candidate)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    return None


def _derive_session_state_detail(
    state_label: str,
    seconds_to_next_open: Optional[float],
    seconds_to_close: Optional[float],
) -> Optional[str]:
    state_norm = (state_label or "").lower()
    if state_norm != "closed":
        return None
    if seconds_to_next_open is None:
        return None
    if seconds_to_next_open >= SESSION_WEEKEND_THRESHOLD_SECONDS:
        return "weekend"
    if seconds_to_next_open >= SESSION_OVERNIGHT_THRESHOLD_SECONDS:
        return "overnight"
    return "intrabreak"


def _build_status_session_symbols_stats(
    stats_obj: Any,
    *,
    session_tag: str,
) -> Optional[List[Dict[str, Any]]]:
    """Будує мінімальний зріз session-статистики для публічного `fxcm:status`.

    Повертає список рядків під таблицю `Symbol | TF | High | Low | Avg`.
    """

    if not isinstance(stats_obj, Mapping):
        return None

    entry_obj: Any = stats_obj.get(session_tag)
    if not isinstance(entry_obj, Mapping):
        # Фолбек: якщо є рівно один запис, беремо його.
        if len(stats_obj) == 1:
            try:
                entry_obj = next(iter(stats_obj.values()))
            except StopIteration:
                return None
        if not isinstance(entry_obj, Mapping):
            return None

    symbols_obj = entry_obj.get("symbols")
    if not isinstance(symbols_obj, list):
        return None

    rows: List[Dict[str, Any]] = []
    for item in symbols_obj:
        if not isinstance(item, Mapping):
            continue
        symbol = item.get("symbol")
        tf = item.get("tf")
        if not isinstance(symbol, str) or not symbol:
            continue
        if not isinstance(tf, str) or not tf:
            continue
        high = _float_or_none(item.get("high"))
        low = _float_or_none(item.get("low"))
        avg = _float_or_none(item.get("avg"))
        if high is None or low is None or avg is None:
            continue
        rows.append(
            {
                "symbol": symbol,
                "tf": tf,
                "high": float(high),
                "low": float(low),
                "avg": float(avg),
            }
        )

    return rows or None


def _build_status_session_block(
    session_ctx: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    global _LAST_SESSION_CONTEXT
    source: Optional[Dict[str, Any]]
    if isinstance(session_ctx, Mapping):
        source = dict(session_ctx)
        _LAST_SESSION_CONTEXT = source
    else:
        source = (
            dict(_LAST_SESSION_CONTEXT) if _LAST_SESSION_CONTEXT is not None else None
        )
    if source is None:
        return None

    tag = str(source.get("tag") or "UNKNOWN")
    name = str(source.get("name") or _humanize_session_name(tag))
    open_dt = _parse_iso8601(
        source.get("session_open_utc") or source.get("current_open_utc")
    )
    close_dt = _parse_iso8601(
        source.get("session_close_utc") or source.get("current_close_utc")
    )
    next_open_dt: Optional[dt.datetime]
    next_open_dt = _parse_iso8601(source.get("next_open_utc"))
    if next_open_dt is None and source.get("next_open_ms") is not None:
        next_open_dt = _parse_iso8601(float(source["next_open_ms"]) / 1000.0)
    now = _now_utc()
    if open_dt is None and close_dt is not None:
        open_dt = close_dt - dt.timedelta(hours=1)
    if close_dt is None and open_dt is not None:
        close_dt = open_dt + dt.timedelta(hours=1)
    session_state = "unknown"
    if open_dt is not None and close_dt is not None:
        if open_dt <= now < close_dt:
            session_state = "open"
        elif now < open_dt:
            session_state = "preopen"
        else:
            session_state = "closed"
    seconds_to_close: Optional[float]
    seconds_to_close = max(0.0, (close_dt - now).total_seconds()) if close_dt else None
    seconds_to_next_open = source.get("next_open_seconds")
    if seconds_to_next_open is None and next_open_dt is not None:
        seconds_to_next_open = max(0.0, (next_open_dt - now).total_seconds())

    payload: Dict[str, Any] = {
        "name": name,
        "tag": tag,
        "state": session_state,
        "current_open_utc": (
            open_dt.replace(microsecond=0).isoformat() if open_dt else None
        ),
        "current_close_utc": (
            close_dt.replace(microsecond=0).isoformat() if close_dt else None
        ),
        "next_open_utc": (
            next_open_dt.replace(microsecond=0).isoformat() if next_open_dt else None
        ),
        "seconds_to_close": seconds_to_close,
        "seconds_to_next_open": seconds_to_next_open,
    }
    state_detail = _derive_session_state_detail(
        session_state, seconds_to_next_open, seconds_to_close
    )
    if state_detail:
        payload["state_detail"] = state_detail

    symbols_stats = _build_status_session_symbols_stats(
        source.get("stats"),
        session_tag=tag,
    )
    if symbols_stats is not None:
        payload["symbols"] = symbols_stats
    return {key: value for key, value in payload.items() if value is not None}


def _derive_process_status(raw_state: str, context: Mapping[str, Any]) -> str:
    normalized = (raw_state or "").lower()
    if normalized in {"stream", "warmup", "warmup_cache"}:
        return "stream"
    if normalized == "idle":
        idle_reason = str(context.get("idle_reason") or "").lower()
        if "sleep" in idle_reason:
            return "sleep"
        return "idle"
    if normalized == "error":
        return "error"
    return "stream"


def _derive_price_status(context: Mapping[str, Any]) -> str:
    meta = context.get("price_stream") if isinstance(context, Mapping) else None
    silence = None
    state_label = ""
    if isinstance(meta, Mapping):
        silence = _float_or_none(meta.get("tick_silence_seconds"))
        state_label = str(meta.get("state") or "").lower()
    if silence is None:
        silence = _float_or_none(context.get("tick_silence_seconds"))
    if state_label in {"stopped", "error"}:
        return "down"
    if state_label == "stale":
        return "stale"
    if silence is None:
        return "ok" if isinstance(meta, Mapping) else "down"
    if silence >= STATUS_PRICE_DOWN_SECONDS:
        return "down"
    if silence >= STATUS_PRICE_STALE_SECONDS:
        return "stale"
    return "ok"


def _derive_ohlcv_status(
    context: Mapping[str, Any],
    process_state: str,
    last_bar_close_ms: Optional[int],
) -> str:
    if process_state in {"idle", "sleep"}:
        return "down"
    lag_seconds = _float_or_none(context.get("lag_seconds"))
    if lag_seconds is None and last_bar_close_ms is not None:
        lag_seconds = _calc_lag_seconds(last_bar_close_ms)
    if lag_seconds is None:
        targets = context.get("stream_targets")
        if isinstance(targets, list):
            lag_candidates: List[float] = []
            for entry in targets:
                if isinstance(entry, Mapping):
                    value = _float_or_none(entry.get("staleness_seconds"))
                    if value is not None:
                        lag_candidates.append(value)
            if lag_candidates:
                lag_seconds = max(lag_candidates)
    live_age = _float_or_none(context.get("ohlcv_live_age_seconds"))
    if lag_seconds is None:
        return "ok"
    if lag_seconds >= STATUS_OHLCV_DOWN_SECONDS:
        # Якщо complete-бари не виходять, але live complete=false оновлення є —
        # це не повний down, а радше partial/delayed стан (downstream все одно
        # може стояти, але причина інша).
        if live_age is not None and live_age <= 5.0:
            return "delayed"
        return "down"
    if lag_seconds >= STATUS_OHLCV_DELAYED_SECONDS:
        return "delayed"
    return "ok"


def _derive_status_note(
    process_state: str,
    price_state: str,
    ohlcv_state: str,
    context: Mapping[str, Any],
    session_block: Optional[Mapping[str, Any]] = None,
) -> str:
    session_state_detail: Optional[str]
    if isinstance(session_block, Mapping):
        detail_value = session_block.get("state_detail")
        session_state_detail = (
            str(detail_value) if isinstance(detail_value, str) else detail_value
        )
    else:
        session_state_detail = None

    def _resolve_next_open_seconds() -> Optional[float]:
        value = _float_or_none(context.get("next_open_seconds"))
        if value is not None:
            return value
        if isinstance(session_block, Mapping):
            return _float_or_none(session_block.get("seconds_to_next_open"))
        return None

    live_age = _float_or_none(context.get("ohlcv_live_age_seconds"))
    if (
        process_state == "stream"
        and price_state == "ok"
        and ohlcv_state in {"delayed", "down"}
        and live_age is not None
        and live_age <= 5.0
    ):
        return "only_live_bars_no_complete"

    if process_state in {"idle", "sleep"} and session_state_detail == "weekend":
        next_open_seconds = _resolve_next_open_seconds()
        if next_open_seconds is not None and next_open_seconds >= 86_400:
            days = max(1, int(next_open_seconds // 86_400))
            return f"weekend ({days}d)"
        return "weekend"
    if process_state in {"idle", "sleep"} and session_state_detail == "overnight":
        return "overnight pause"
    if process_state == "idle":
        reason = str(context.get("idle_reason") or "market_closed")
        return f"idle: {reason}"
    if process_state == "sleep":
        reason = str(context.get("idle_reason") or "autosleep")
        return f"sleep: {reason}"
    history_backoff = _float_or_none(context.get("history_backoff_seconds"))
    if history_backoff:
        return f"backoff FXCM {int(math.ceil(history_backoff))}s"
    redis_backoff = _float_or_none(context.get("redis_backoff_seconds"))
    if redis_backoff:
        return f"backoff Redis {int(math.ceil(redis_backoff))}s"
    if ohlcv_state == "delayed":
        lag_value = _float_or_none(context.get("lag_seconds"))
        if lag_value is not None:
            return f"lag {int(math.ceil(lag_value))}s"
        return "ohlcv delayed"
    if ohlcv_state == "down":
        return "ohlcv down"
    if price_state == "stale":
        return "price stale"
    if price_state == "down":
        return "price down"
    return "ok"


def _build_status_snapshot_from_heartbeat(
    payload: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    context_obj = payload.get("context")
    context = context_obj if isinstance(context_obj, Mapping) else {}
    process_state = _derive_process_status(payload.get("state", ""), context)
    price_state = _derive_price_status(context)
    ohlcv_state = _derive_ohlcv_status(
        context, process_state, payload.get("last_bar_close_ms")
    )
    session_block = _build_status_session_block(context.get("session"))
    note = _derive_status_note(
        process_state, price_state, ohlcv_state, context, session_block
    )
    status_payload: Dict[str, Any] = {
        "ts": time.time(),
        "process": process_state,
        "market": _CURRENT_MARKET_STATE,
        "price": price_state,
        "ohlcv": ohlcv_state,
        "note": note,
    }
    if session_block is not None:
        status_payload["session"] = session_block
    return status_payload


def _build_status_snapshot_from_market(
    payload: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    global _CURRENT_MARKET_STATE
    state = str(payload.get("state") or "unknown").lower() or "unknown"
    _CURRENT_MARKET_STATE = state
    session_block = _build_status_session_block(payload.get("session"))
    base = dict(_LAST_STATUS_SNAPSHOT) if _LAST_STATUS_SNAPSHOT else None
    now_ts = time.time()
    if base is None:
        snapshot: Dict[str, Any] = {
            "ts": now_ts,
            "process": "stream" if state == "open" else "idle",
            "market": state,
            "price": "ok" if state == "open" else "down",
            "ohlcv": "ok" if state == "open" else "down",
            "note": "ok" if state == "open" else "market closed",
        }
    else:
        snapshot = dict(base)
        snapshot["ts"] = now_ts
        snapshot["market"] = state
        if state != "open" and snapshot.get("process") == "stream":
            snapshot["process"] = "idle"
        if state != "open":
            snapshot["note"] = "market closed"
    if session_block is not None:
        snapshot["session"] = session_block
    elif "session" in snapshot:
        snapshot.pop("session")
    return snapshot


def _publish_public_status(
    redis_client: Optional[Any], snapshot: Optional[Dict[str, Any]]
) -> None:
    global _LAST_STATUS_SNAPSHOT
    if snapshot is None:
        return
    copy_payload = dict(snapshot)
    session_block = copy_payload.get("session")
    if isinstance(session_block, dict):
        copy_payload["session"] = dict(session_block)
    _LAST_STATUS_SNAPSHOT = copy_payload
    if redis_client is None or not _STATUS_CHANNEL:
        return

    global _LAST_STATUS_PUBLISHED_TS
    global _LAST_STATUS_PUBLISHED_KEY
    now_ts = time.time()
    publish_key = _status_publish_key(copy_payload)
    if _LAST_STATUS_PUBLISHED_TS is not None:
        # Захист від flood: не публікуємо частіше за мін-інтервал незалежно
        # від дрібних змін у snapshot (наприклад, лічильники або секунди до close).
        if now_ts - _LAST_STATUS_PUBLISHED_TS < STATUS_PUBLISH_MIN_INTERVAL_SECONDS:
            return
    message = json.dumps(copy_payload, separators=(",", ":"), ensure_ascii=False)
    try:
        redis_client.publish(_STATUS_CHANNEL, message)
        _LAST_STATUS_PUBLISHED_TS = now_ts
        _LAST_STATUS_PUBLISHED_KEY = publish_key
    except Exception as exc:  # noqa: BLE001
        log.debug("Не вдалося надіслати статус у Redis: %s", exc)


def _publish_heartbeat(
    redis_client: Optional[Any],
    channel: Optional[str],
    *,
    state: str,
    last_bar_close_ms: Optional[int],
    next_open: Optional[dt.datetime] = None,
    sleep_seconds: Optional[float] = None,
    context: Optional[Dict[str, Any]] = None,
) -> bool:
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
    if context:
        payload["context"] = context

    message = json.dumps(payload, separators=(",", ":"))
    status_snapshot = _build_status_snapshot_from_heartbeat(payload)
    if redis_client is None or not channel:
        _publish_public_status(redis_client, status_snapshot)
        return False
    now_ts = time.time()
    if _telemetry_rate_limited(channel, now_ts=now_ts):
        _publish_public_status(redis_client, status_snapshot)
        return True
    success = False
    try:
        redis_client.publish(channel, message)
        _telemetry_mark_published(channel, now_ts=now_ts)
        PROM_HEARTBEAT_TS.set(_now_utc().timestamp())
        success = True
    except Exception as exc:  # noqa: BLE001
        PROM_ERROR_COUNTER.labels(type="redis").inc()
        log.debug("Heartbeat publish неуспішний: %s", exc)
    finally:
        _publish_public_status(redis_client, status_snapshot)
    return success


def _calc_lag_seconds(last_close_ms: Optional[int]) -> Optional[float]:
    if last_close_ms is None:
        return None
    now = _now_utc().timestamp()
    return max(0.0, now - last_close_ms / 1000.0)


def _resolve_idle_sleep_seconds(
    poll_seconds: int,
    cadence_sleep_seconds: Optional[float],
) -> float:
    """Гарантує мінімальний idle-сон навіть при нульовому cadence."""

    base_sleep = max(1.0, float(poll_seconds))
    if cadence_sleep_seconds is None or cadence_sleep_seconds <= 0.0:
        return base_sleep
    return max(base_sleep, cadence_sleep_seconds)


def _session_stats_snapshot() -> Optional[Dict[str, Any]]:
    if _SESSION_STATS_TRACKER is None:
        return None
    snapshot = _SESSION_STATS_TRACKER.snapshot()
    return snapshot or None


def _build_session_context(
    next_open: Optional[dt.datetime] = None,
    *,
    session_stats: Optional[Dict[str, Any]] = None,
) -> SessionContextPayload:
    snapshot = calendar_snapshot()
    weekly_open = snapshot.get("weekly_open")
    weekly_close = snapshot.get("weekly_close")
    default_timezone = "UTC"
    for candidate in (weekly_open, weekly_close):
        if isinstance(candidate, str) and "@" in candidate:
            default_timezone = candidate.split("@", 1)[1] or default_timezone
            break
    reference_ts = _now_utc()
    target_next_open = next_open or next_trading_open(reference_ts)
    resolved_session = resolve_session(reference_ts) if _AUTO_SESSION_TAG else None
    if resolved_session is not None:
        tag = resolved_session.tag
        timezone = resolved_session.timezone
    else:
        tag = _SESSION_TAG_SETTING if not _AUTO_SESSION_TAG else _DEFAULT_SESSION_TAG
        timezone = default_timezone
    context = cast(
        SessionContextPayload,
        {
            "tag": tag,
            "timezone": timezone,
            "next_open_utc": target_next_open.replace(microsecond=0).isoformat(),
            "next_open_ms": int(target_next_open.timestamp() * 1000),
            "next_open_seconds": max(
                0.0, (target_next_open - _now_utc()).total_seconds()
            ),
        },
    )
    if resolved_session is not None:
        context["session_open_utc"] = resolved_session.session_open_utc.replace(
            microsecond=0
        ).isoformat()
        context["session_close_utc"] = resolved_session.session_close_utc.replace(
            microsecond=0
        ).isoformat()
    if isinstance(weekly_open, str):
        context["weekly_open"] = weekly_open
    if isinstance(weekly_close, str):
        context["weekly_close"] = weekly_close
    daily_breaks = snapshot.get("daily_breaks")
    if isinstance(daily_breaks, list):
        context["daily_breaks"] = daily_breaks
    holidays = snapshot.get("holidays")
    if isinstance(holidays, list):
        context["holidays"] = holidays
    if session_stats:
        context["stats"] = session_stats
    return context


def _normalize_symbol(raw_symbol: str) -> str:
    """Нормалізуємо символ FXCM до формату AiOne_t.

    Зараз проста стратегія: прибираємо '/', робимо upper.
    Приклад: 'EUR/USD' -> 'EURUSD'.
    """
    return raw_symbol.replace("/", "").upper()


def _denormalize_symbol(symbol_norm: str) -> str:
    """Перетворює нормалізований символ (без `/`) у FXCM-формат.

    Приклади:
    - `XAUUSD` -> `XAU/USD`
    - `EURUSD` -> `EUR/USD`

    Якщо у рядку вже є `/`, повертаємо як є.
    """

    raw = (symbol_norm or "").strip()
    if not raw:
        return ""
    if "/" in raw:
        return raw
    upper = raw.upper()
    if len(upper) <= 3:
        return upper
    # Базове правило для FX/металів: XXXYYY -> XXX/YYY.
    return f"{upper[:-3]}/{upper[-3:]}"


def _to_fxcm_timeframe(tf_norm: str) -> str:
    """Перетворює нормалізований timeframe (`1m`, `1h`) у FXCM tf.

    Важливо: у ForexConnect/FXCM годинні TF мають вигляд `H1`, `H4` (а не `m60`).
    """

    raw = (tf_norm or "").strip().lower()
    if not raw:
        return "m1"

    explicit = {
        "1m": "m1",
        "5m": "m5",
        "15m": "m15",
        "30m": "m30",
        "1h": "H1",
        "4h": "H4",
        "1d": "D1",
        # Сумісність із «сирими» FXCM-позначеннями, якщо сюди випадково передали raw.
        "m60": "H1",
        "h1": "H1",
        "h4": "H4",
        "d1": "D1",
    }
    if raw in explicit:
        return explicit[raw]

    if raw.endswith("m") and raw[:-1].isdigit():
        return f"m{int(raw[:-1])}"
    if raw.endswith("h") and raw[:-1].isdigit():
        hours = int(raw[:-1])
        return f"H{hours}"

    if raw.endswith("d") and raw[:-1].isdigit():
        days = int(raw[:-1])
        return f"D{days}"

    return tf_norm


@dataclass
class StreamUniverse:
    """Динамічний universe таргетів для стріму (v1).

    - base_targets: дозволений список `(symbol_raw, timeframe_raw)` з конфігу.
    - dynamic_enabled: якщо False — працюємо як раніше (весь base).
    - default_targets: мінімальний fallback, якщо SMC ще не задав universe.
    - max_targets: верхня межа активних таргетів у dynamic режимі.
    """

    base_targets: List[Tuple[str, str]]
    dynamic_enabled: bool
    default_targets: List[Tuple[str, str]]
    max_targets: int

    _active_from_smc: List[Tuple[str, str]] = field(default_factory=list)
    _dirty: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _base_norm_set: Set[Tuple[str, str]] = field(default_factory=set, init=False)

    def __post_init__(self) -> None:
        self.base_targets = [(sym, tf) for sym, tf in (self.base_targets or []) if sym and tf]
        self.default_targets = [(sym, tf) for sym, tf in (self.default_targets or []) if sym and tf]
        self.max_targets = max(1, int(self.max_targets or 1))

        self._base_norm_set = {
            (_normalize_symbol(sym), _map_timeframe_label(tf)) for sym, tf in self.base_targets
        }
        # Гарантуємо підмножину base_targets (вимога U1).
        filtered_default: List[Tuple[str, str]] = []
        seen: Set[Tuple[str, str]] = set()
        for sym, tf in self.default_targets:
            key = (_normalize_symbol(sym), _map_timeframe_label(tf))
            if key in seen:
                continue
            seen.add(key)
            if key not in self._base_norm_set:
                log.warning(
                    "Dynamic universe: default_targets містить пару поза base_targets, пропускаємо (%s %s)",
                    sym,
                    tf,
                )
                continue
            filtered_default.append((sym, tf))
        self.default_targets = filtered_default

    def has_dynamic_requests(self) -> bool:
        with self._lock:
            return bool(self._active_from_smc)

    def mark_dirty(self) -> None:
        with self._lock:
            self._dirty = True

    def consume_dirty(self) -> bool:
        with self._lock:
            if not self._dirty:
                return False
            self._dirty = False
            return True

    def set_from_smc(self, targets: List[Tuple[str, str]]) -> None:
        """Оновлює активний universe з команди SMC."""

        normalized: List[Tuple[str, str]] = []
        seen: Set[Tuple[str, str]] = set()
        ignored_outside_base = 0
        for sym, tf in (targets or []):
            if not sym or not tf:
                continue
            key = (_normalize_symbol(sym), _map_timeframe_label(tf))
            if key in seen:
                continue
            seen.add(key)
            if key not in self._base_norm_set:
                ignored_outside_base += 1
                continue
            normalized.append((sym, tf))
            if len(normalized) >= self.max_targets:
                break

        with self._lock:
            if normalized == self._active_from_smc:
                return
            self._active_from_smc = list(normalized)
            self._dirty = True

        if ignored_outside_base:
            log.info(
                "Dynamic universe: проігноровано %d таргет(ів), які не входять у base_targets",
                ignored_outside_base,
            )

    def get_effective_targets(self) -> List[Tuple[str, str]]:
        if not self.dynamic_enabled:
            return list(self.base_targets)

        with self._lock:
            active = list(self._active_from_smc)

        if active:
            return active
        if self.default_targets:
            return list(self.default_targets)
        # Фолбек: перші max_targets з base.
        return list(self.base_targets[: self.max_targets])


class FxcmCommandWorker:
    """Підписник на `fxcm:commands` для warmup/backfill від SMC (S3).

    Важливо: 1m/5m live OHLCV лишаються лише tick_agg.
    Команди для цих TF можуть оновлювати кеш (warmup), але не публікують history у `fxcm:ohlcv`.
    """

    _RATE_LIMIT_SECONDS = 2.0

    def __init__(
        self,
        *,
        redis_client: Optional[Any],
        fx_holder: Dict[str, Optional[ForexConnect]],
        cache_manager: Optional[HistoryCache],
        tick_aggregation_enabled: bool,
        tick_agg_timeframes: Sequence[str] = ("m1", "m5"),
        channel: str,
        ohlcv_channel: str,
        stream_targets: Sequence[Tuple[str, str]] = (),
        universe: Optional[StreamUniverse] = None,
        backfill_min_minutes: int = 0,
        backfill_max_minutes: int = 360,
        hmac_secret: Optional[str] = None,
        hmac_algo: str = "sha256",
    ) -> None:
        self._redis_client = redis_client
        self._fx_holder = fx_holder
        self._cache_manager = cache_manager
        self._tick_aggregation_enabled = bool(tick_aggregation_enabled)
        self._tick_tf_norm: Set[str] = {
            _map_timeframe_label(tf) for tf in (tick_agg_timeframes or ())
        }
        self._channel = str(channel or "").strip()
        self._ohlcv_channel = str(ohlcv_channel or "").strip() or REDIS_CHANNEL
        self._universe = universe
        self._backfill_min_minutes = max(0, int(backfill_min_minutes))
        self._backfill_max_minutes = max(self._backfill_min_minutes, int(backfill_max_minutes) or 0)
        self._hmac_secret = hmac_secret
        self._hmac_algo = (hmac_algo or "sha256").strip().lower() or "sha256"
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._pubsub: Optional[Any] = None
        self._last_cmd_ts: Dict[Tuple[str, str, str], float] = {}
        self._targets: Set[Tuple[str, str]] = {
            (_normalize_symbol(symbol), _map_timeframe_label(tf_raw))
            for symbol, tf_raw in (stream_targets or ())
        }
        self._symbols_in_targets: Set[str] = {sym for sym, _ in self._targets}

    @staticmethod
    def _is_supported_mtf_warmup_tf(tf_norm: str) -> bool:
        # Мінімальний набір, який нам реально потрібен для SMC warmup.
        return str(tf_norm) in {"15m", "30m", "1h", "4h", "1d"}

    def _prefetch_history_into_cache(
        self,
        fx: ForexConnect,
        *,
        symbol_fxcm: str,
        timeframe_raw: str,
        lookback_minutes: int,
        min_history_bars: int,
    ) -> None:
        cache_manager = self._cache_manager
        # У тестах `cache_manager` часто є mock. Prefetch не має
        # робити реальні FXCM/pandas операції — лише для реального кеша.
        if cache_manager is None or not isinstance(cache_manager, HistoryCache):
            return
        tf_norm = _map_timeframe_label(timeframe_raw)
        tf_minutes = _tf_to_minutes(tf_norm)
        desired_minutes = 0
        if lookback_minutes > 0:
            desired_minutes = int(lookback_minutes)
        elif min_history_bars > 0:
            desired_minutes = int(min_history_bars) * int(tf_minutes)
        if desired_minutes <= 0:
            return

        end_dt = _now_utc()
        start_dt = end_dt - dt.timedelta(minutes=int(desired_minutes))
        df_range = _download_history_range(
            fx,
            symbol=symbol_fxcm,
            timeframe_raw=timeframe_raw,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        if df_range is None or df_range.empty:
            return
        cache_manager.append_stream_bars(
            symbol_raw=symbol_fxcm,
            timeframe_raw=timeframe_raw,
            df_new=df_range,
        )

    def start(self) -> None:
        if self._redis_client is None:
            log.info("S3: канал команд не запущено — Redis недоступний.")
            return
        if not self._channel:
            log.info("S3: канал команд не запущено — channel порожній.")
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="FxcmCommandWorker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        pubsub = self._pubsub
        if pubsub is not None and hasattr(pubsub, "close"):
            try:
                pubsub.close()
            except Exception:  # noqa: BLE001
                pass
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._thread = None
        self._pubsub = None

    def _run(self) -> None:
        assert self._redis_client is not None
        try:
            pubsub = self._redis_client.pubsub(ignore_subscribe_messages=True)
            self._pubsub = pubsub
            pubsub.subscribe(self._channel)
            log.info("S3: підписано канал команд: %s", self._channel)
        except Exception as exc:  # noqa: BLE001
            PROM_COMMANDS_TOTAL.labels(type="subscribe", result="error").inc()
            log.exception("S3: не вдалося підписатися на канал команд %s: %s", self._channel, exc)
            return

        last_poll_error_ts = 0.0

        def _should_reconnect(exc: Exception) -> bool:
            text = str(exc).lower()
            needles = (
                "i/o operation on closed file",
                "closed file",
                "connection",
                "broken pipe",
                "connection reset",
                "connection error",
                "timeout",
            )
            return any(needle in text for needle in needles)

        def _reconnect() -> Optional[Any]:
            if self._stop_event.is_set():
                return None
            client = self._redis_client
            if client is None:
                log.warning(
                    "S3: Redis-клієнт відсутній, перепідписання каналу %s неможливе.",
                    self._channel,
                )
                return None
            try:
                if pubsub is not None and hasattr(pubsub, "close"):
                    try:
                        pubsub.close()
                    except Exception:  # noqa: BLE001
                        pass
                new_pubsub = client.pubsub(ignore_subscribe_messages=True)
                self._pubsub = new_pubsub
                new_pubsub.subscribe(self._channel)
                log.info("S3: перепідписано канал команд: %s", self._channel)
                return new_pubsub
            except Exception as reconnect_exc:  # noqa: BLE001
                now = time.time()
                if now - last_poll_error_ts >= self._RATE_LIMIT_SECONDS:
                    log.warning(
                        "S3: не вдалося перепідписатися на канал команд %s: %s",
                        self._channel,
                        reconnect_exc,
                    )
                return None

        while not self._stop_event.is_set():
            try:
                message = pubsub.get_message(timeout=1.0)
            except Exception as exc:  # noqa: BLE001
                if self._stop_event.is_set():
                    break
                PROM_COMMANDS_TOTAL.labels(type="poll", result="error").inc()
                now = time.time()
                if now - last_poll_error_ts >= self._RATE_LIMIT_SECONDS:
                    log.warning("S3: помилка читання pubsub (%s): %s", self._channel, exc)
                    last_poll_error_ts = now
                if _should_reconnect(exc):
                    new_pubsub = _reconnect()
                    if new_pubsub is not None:
                        pubsub = new_pubsub
                time.sleep(1.0)
                continue
            if not message:
                continue
            raw = message.get("data")
            try:
                payload = json.loads(raw)
            except Exception as exc:  # noqa: BLE001
                PROM_COMMANDS_TOTAL.labels(type="parse", result="error").inc()
                log.warning("S3: некоректний JSON у команді: %s", exc)
                continue
            try:
                self._handle_command(payload)
            except Exception as exc:  # noqa: BLE001
                cmd_type = str(payload.get("type") or "unknown")
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
                log.exception("S3: помилка обробки команди %s: %s", cmd_type, exc)

    def _handle_command(self, payload: Mapping[str, Any]) -> None:
        cmd_type = str(payload.get("type") or "").strip()
        if cmd_type == "fxcm_set_universe":
            universe = self._universe
            if universe is None or not universe.dynamic_enabled:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ignored").inc()
                log.info("S3: fxcm_set_universe — dynamic_universe вимкнено, ігноруємо")
                return

            targets_raw = payload.get("targets")
            if not isinstance(targets_raw, list):
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
                log.warning("S3: fxcm_set_universe: поле targets має бути списком")
                return

            parsed: List[Tuple[str, str]] = []
            for entry in targets_raw:
                if not isinstance(entry, Mapping):
                    continue
                sym_in = entry.get("symbol")
                tf_in = entry.get("tf")
                if not isinstance(sym_in, str) or not isinstance(tf_in, str):
                    continue
                sym_raw = sym_in.strip()
                tf_raw = tf_in.strip()
                if not sym_raw or not tf_raw:
                    continue
                # Приймаємо `XAUUSD` і `XAU/USD`, а також `1m` і `m1`.
                sym_fxcm = _denormalize_symbol(sym_raw)
                tf_fxcm = _to_fxcm_timeframe(_map_timeframe_label(tf_raw))
                parsed.append((sym_fxcm, tf_fxcm))

            universe.set_from_smc(parsed)
            effective = universe.get_effective_targets()
            PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
            log.info(
                "S3: fxcm_set_universe застосовано, активних таргетів: %d, список: %s",
                len(effective),
                ", ".join(f"{sym}:{tf}" for sym, tf in effective) or "<empty>",
            )
            return

        if cmd_type not in {"fxcm_warmup", "fxcm_backfill"}:
            if cmd_type:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ignored").inc()
            log.info("S3: невідома команда type=%s — ігноруємо", cmd_type or "<empty>")
            return

        symbol_in = payload.get("symbol")
        tf_in = payload.get("tf")
        if not isinstance(symbol_in, str) or not isinstance(tf_in, str):
            PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
            log.warning("S3: %s: відсутні або некоректні поля symbol/tf", cmd_type)
            return

        symbol_raw = symbol_in.strip()
        tf_raw = tf_in.strip()
        tf_norm = _map_timeframe_label(tf_raw)

        symbol_fxcm = _denormalize_symbol(symbol_raw)
        symbol_norm = _normalize_symbol(symbol_fxcm)

        if self._targets:
            if symbol_norm not in self._symbols_in_targets:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ignored").inc()
                log.info(
                    "S3: команда для нецільового інструменту, ігноруємо (%s %s)",
                    symbol_norm,
                    tf_norm,
                )
                return
            if (symbol_norm, tf_norm) not in self._targets:
                # Дозволяємо S3 warmup для MTF навіть якщо TF не прописаний у таргетах.
                # Це потрібно, щоб SMC міг запросити прогрів 1h/4h, а ми віддали
                # `history_agg` (похідний від 1m history) без прямого polling H1/H4.
                if not (
                    cmd_type == "fxcm_warmup" and self._is_supported_mtf_warmup_tf(tf_norm)
                ):
                    PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ignored").inc()
                    log.info(
                        "S3: команда для нецільового інструменту, ігноруємо (%s %s)",
                        symbol_norm,
                        tf_norm,
                    )
                    return

        key = (symbol_norm, tf_norm, cmd_type)
        now = time.monotonic()
        last_ts = self._last_cmd_ts.get(key)
        if last_ts is not None and now - last_ts < self._RATE_LIMIT_SECONDS:
            PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ignored").inc()
            return
        self._last_cmd_ts[key] = now

        PROM_COMMANDS_LAST_TS_SECONDS.set(time.time())

        min_history_bars = 0
        lookback_minutes = 0
        try:
            min_history_bars = int(payload.get("min_history_bars") or 0)
        except Exception:  # noqa: BLE001
            min_history_bars = 0
        try:
            lookback_minutes = int(payload.get("lookback_minutes") or 0)
        except Exception:  # noqa: BLE001
            lookback_minutes = 0

        tick_tf = self._tick_aggregation_enabled and tf_norm in self._tick_tf_norm

        if cmd_type == "fxcm_warmup":
            fx = self._fx_holder.get("client")
            if fx is None or self._cache_manager is None:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
                log.warning(
                    "S3: fxcm_warmup %s %s — FXCM або кеш недоступні",
                    symbol_norm,
                    tf_norm,
                )
                return

            # Для tick TF (1m/5m): прогріваємо 1m history у кеш та публікуємо історичні бари,
            # щоб SMC міг зробити повний warmup. Live tick-preview як і раніше лишається
            # `complete=false`, тож правило "не змішувати" зберігається.
            if tick_tf:
                try:
                    # Працюємо лише з базовим TF (1m) — без FXCM polling для m5.
                    self._cache_manager.ensure_ready(
                        fx,
                        symbol_raw=symbol_fxcm,
                        timeframe_raw=BASE_HISTORY_TF_RAW,
                    )

                    tf_minutes = _tf_to_minutes(tf_norm)
                    bars_1m = (
                        int(min_history_bars) * max(1, int(tf_minutes // _tf_to_minutes(BASE_HISTORY_TF_NORM)))
                        if min_history_bars > 0
                        else 0
                    )
                    # Опційний prefetch (лише для реального HistoryCache).
                    self._prefetch_history_into_cache(
                        fx,
                        symbol_fxcm=symbol_fxcm,
                        timeframe_raw=BASE_HISTORY_TF_RAW,
                        lookback_minutes=lookback_minutes,
                        min_history_bars=bars_1m,
                    )
                except Exception as exc:  # noqa: BLE001
                    PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
                    log.exception(
                        "S3: fxcm_warmup помилка warmup-cache(%s, %s): %s",
                        symbol_fxcm,
                        tf_norm,
                        exc,
                    )
                    return

                # Для 1m публікуємо історію напряму з кешу (complete=true за замовчуванням).
                if tf_norm == BASE_HISTORY_TF_NORM:
                    if self._redis_client is None:
                        PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ignored").inc()
                        log.info(
                            "S3: fxcm_warmup %s %s — Redis недоступний, оновлено лише кеш",
                            symbol_norm,
                            tf_norm,
                        )
                        return

                    if min_history_bars > 0:
                        limit = int(min_history_bars)
                    elif lookback_minutes > 0:
                        limit = int(max(1, math.ceil(float(lookback_minutes) / float(_tf_to_minutes(tf_norm)))))
                    else:
                        limit = int(self._cache_manager.warmup_bars)

                    warmup_slice = self._cache_manager.get_bars_to_publish(
                        symbol_raw=symbol_fxcm,
                        timeframe_raw=BASE_HISTORY_TF_RAW,
                        limit=limit,
                        force=True,
                    )
                    if warmup_slice.empty:
                        PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                        log.info(
                            "S3: fxcm_warmup %s %s — warmup_slice порожній",
                            symbol_norm,
                            tf_norm,
                        )
                        return

                    publish_ohlcv_to_redis(
                        warmup_slice,
                        symbol=symbol_fxcm,
                        timeframe=tf_norm,
                        redis_client=self._redis_client,
                        channel=self._ohlcv_channel,
                        data_gate=None,
                        hmac_secret=self._hmac_secret,
                        hmac_algo=self._hmac_algo,
                        source="history_s3",
                    )
                    last_published = int(warmup_slice["open_time"].max())
                    self._cache_manager.mark_published(
                        symbol_raw=symbol_fxcm,
                        timeframe_raw=BASE_HISTORY_TF_RAW,
                        last_open_time=last_published,
                    )
                    PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                    log.info(
                        "S3: fxcm_warmup опубліковано %d барів для %s %s",
                        int(len(warmup_slice)),
                        symbol_norm,
                        tf_norm,
                    )
                    return

            # Для MTF warmup (1h/4h/..): підтягуємо 1m history у кеш на потрібний lookback,
            # локально будуємо `history_agg` і публікуємо саме його (без polling H1/H4).
            if tf_norm != BASE_HISTORY_TF_NORM:
                try:
                    self._prefetch_history_into_cache(
                        fx,
                        symbol_fxcm=symbol_fxcm,
                        timeframe_raw=BASE_HISTORY_TF_RAW,
                        lookback_minutes=lookback_minutes,
                        min_history_bars=(
                            int(min_history_bars)
                            * max(1, int(_tf_to_minutes(tf_norm) // _tf_to_minutes(BASE_HISTORY_TF_NORM)))
                            if min_history_bars > 0
                            else 0
                        ),
                    )
                    df_1m = self._cache_manager.get_cached_df(
                        symbol_fxcm,
                        BASE_HISTORY_TF_RAW,
                    )
                except Exception as exc:  # noqa: BLE001
                    PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
                    log.exception(
                        "S3: fxcm_warmup помилка під час prefetch 1m для %s %s: %s",
                        symbol_norm,
                        tf_norm,
                        exc,
                    )
                    return

                if df_1m is None or df_1m.empty:
                    PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                    log.info(
                        "S3: fxcm_warmup %s %s — 1m кеш порожній після prefetch",
                        symbol_norm,
                        tf_norm,
                    )
                    return

                agg = HistoryMtfCompleteAggregator(
                    symbol_norm=symbol_norm,
                    target_tf=str(tf_norm),
                )
                df_agg = agg.ingest_df_1m(df_1m)
                if df_agg is None or df_agg.empty:
                    PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                    log.info(
                        "S3: fxcm_warmup %s %s — history_agg не дав повних барів",
                        symbol_norm,
                        tf_norm,
                    )
                    return

                if self._redis_client is None:
                    PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ignored").inc()
                    log.info(
                        "S3: fxcm_warmup %s %s — Redis недоступний, агреговано без публікації",
                        symbol_norm,
                        tf_norm,
                    )
                    return

                if min_history_bars > 0:
                    limit = int(min_history_bars)
                elif lookback_minutes > 0:
                    limit = int(max(1, math.ceil(float(lookback_minutes) / float(_tf_to_minutes(tf_norm)))))
                else:
                    limit = 0
                if limit > 0:
                    df_agg = cast(pd.DataFrame, df_agg.tail(limit).reset_index(drop=True))

                publish_ohlcv_to_redis(
                    df_agg,
                    symbol=symbol_fxcm,
                    timeframe=str(tf_norm),
                    redis_client=self._redis_client,
                    channel=self._ohlcv_channel,
                    data_gate=None,
                    hmac_secret=self._hmac_secret,
                    hmac_algo=self._hmac_algo,
                    source="history_s3",
                )
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                log.info(
                    "S3: fxcm_warmup опубліковано %d history_agg барів для %s %s",
                    int(len(df_agg)),
                    symbol_norm,
                    tf_norm,
                )
                return

            fx_tf_raw = _to_fxcm_timeframe(tf_norm)
            try:
                self._prefetch_history_into_cache(
                    fx,
                    symbol_fxcm=symbol_fxcm,
                    timeframe_raw=fx_tf_raw,
                    lookback_minutes=lookback_minutes,
                    min_history_bars=min_history_bars,
                )
                self._cache_manager.ensure_ready(
                    fx,
                    symbol_raw=symbol_fxcm,
                    timeframe_raw=fx_tf_raw,
                )
            except Exception as exc:  # noqa: BLE001
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
                log.exception(
                    "S3: fxcm_warmup помилка ensure_ready(%s, %s): %s",
                    symbol_fxcm,
                    fx_tf_raw,
                    exc,
                )
                return

            if self._redis_client is None:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ignored").inc()
                log.info(
                    "S3: fxcm_warmup %s %s — Redis недоступний, оновлено лише кеш",
                    symbol_norm,
                    tf_norm,
                )
                return

            limit = min_history_bars or self._cache_manager.warmup_bars
            warmup_slice = self._cache_manager.get_bars_to_publish(
                symbol_raw=symbol_fxcm,
                timeframe_raw=fx_tf_raw,
                limit=limit,
                force=False,
            )
            if warmup_slice.empty:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                log.info(
                    "S3: fxcm_warmup %s %s — warmup_slice порожній",
                    symbol_norm,
                    tf_norm,
                )
                return

            publish_ohlcv_to_redis(
                warmup_slice,
                symbol=symbol_fxcm,
                timeframe=tf_norm,
                redis_client=self._redis_client,
                channel=self._ohlcv_channel,
                data_gate=None,
                hmac_secret=self._hmac_secret,
                hmac_algo=self._hmac_algo,
                source="history_s3",
            )
            PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
            log.info(
                "S3: fxcm_warmup опубліковано %d барів для %s %s",
                int(len(warmup_slice)),
                symbol_norm,
                tf_norm,
            )
            return

        # fxcm_backfill
        if tick_tf:
            fx = self._fx_holder.get("client")
            if fx is None or self._cache_manager is None:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
                log.warning(
                    "S3: fxcm_backfill %s %s — FXCM або кеш недоступні",
                    symbol_norm,
                    tf_norm,
                )
                return

            try:
                # Працюємо лише з 1m history, без прямого backfill по m5.
                self._cache_manager.ensure_ready(
                    fx,
                    symbol_raw=symbol_fxcm,
                    timeframe_raw=BASE_HISTORY_TF_RAW,
                )
                tf_minutes = _tf_to_minutes(tf_norm)
                bars_1m = int(max(1, int(tf_minutes // _tf_to_minutes(BASE_HISTORY_TF_NORM))))
                if lookback_minutes > 0:
                    bars_1m = int(max(1, lookback_minutes))
                if min_history_bars > 0:
                    bars_1m = int(min_history_bars) * int(max(1, tf_minutes // _tf_to_minutes(BASE_HISTORY_TF_NORM)))
                self._prefetch_history_into_cache(
                    fx,
                    symbol_fxcm=symbol_fxcm,
                    timeframe_raw=BASE_HISTORY_TF_RAW,
                    lookback_minutes=lookback_minutes,
                    min_history_bars=bars_1m,
                )
            except Exception as exc:  # noqa: BLE001
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
                log.exception(
                    "S3: fxcm_backfill помилка під час prefetch 1m для %s %s: %s",
                    symbol_norm,
                    tf_norm,
                    exc,
                )
                return

            if self._redis_client is None:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ignored").inc()
                log.info(
                    "S3: fxcm_backfill %s %s — Redis недоступний, оновлено лише кеш",
                    symbol_norm,
                    tf_norm,
                )
                return

            # 1m: публікуємо історію з кешу; 5m: агрегація з 1m кешу.
            if tf_norm == BASE_HISTORY_TF_NORM:
                if lookback_minutes > 0:
                    limit = int(max(1, lookback_minutes))
                elif min_history_bars > 0:
                    limit = int(min_history_bars)
                else:
                    limit = int(self._cache_manager.warmup_bars)

                backfill_slice = self._cache_manager.get_bars_to_publish(
                    symbol_raw=symbol_fxcm,
                    timeframe_raw=BASE_HISTORY_TF_RAW,
                    limit=limit,
                    force=True,
                )
                if backfill_slice.empty:
                    PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                    log.info(
                        "S3: fxcm_backfill %s %s — backfill_slice порожній",
                        symbol_norm,
                        tf_norm,
                    )
                    return

                publish_ohlcv_to_redis(
                    backfill_slice,
                    symbol=symbol_fxcm,
                    timeframe=tf_norm,
                    redis_client=self._redis_client,
                    channel=self._ohlcv_channel,
                    data_gate=None,
                    hmac_secret=self._hmac_secret,
                    hmac_algo=self._hmac_algo,
                    source="history_s3",
                )
                last_published = int(backfill_slice["open_time"].max())
                self._cache_manager.mark_published(
                    symbol_raw=symbol_fxcm,
                    timeframe_raw=BASE_HISTORY_TF_RAW,
                    last_open_time=last_published,
                )
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                log.info(
                    "S3: fxcm_backfill опубліковано %d барів для %s %s (lookback=%d)",
                    int(len(backfill_slice)),
                    symbol_norm,
                    tf_norm,
                    int(lookback_minutes or 0),
                )
                return

            # Для 5m/..: використовуємо існуючу MTF агрегацію з 1m кешу.
            try:
                df_1m = self._cache_manager.get_cached_df(
                    symbol_fxcm,
                    BASE_HISTORY_TF_RAW,
                )
            except Exception as exc:  # noqa: BLE001
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
                log.exception(
                    "S3: fxcm_backfill %s %s — не вдалося прочитати 1m кеш: %s",
                    symbol_norm,
                    tf_norm,
                    exc,
                )
                return

            if df_1m is None or df_1m.empty:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                log.info(
                    "S3: fxcm_backfill %s %s — 1m кеш порожній",
                    symbol_norm,
                    tf_norm,
                )
                return

            agg = HistoryMtfCompleteAggregator(
                symbol_norm=symbol_norm,
                target_tf=str(tf_norm),
            )
            df_agg = agg.ingest_df_1m(df_1m)
            if df_agg is None or df_agg.empty:
                PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
                log.info(
                    "S3: fxcm_backfill %s %s — history_agg не дав повних барів",
                    symbol_norm,
                    tf_norm,
                )
                return

            limit = 0
            if lookback_minutes > 0:
                limit = int(max(1, math.ceil(float(lookback_minutes) / float(_tf_to_minutes(tf_norm)))))
            elif min_history_bars > 0:
                limit = int(min_history_bars)
            if limit > 0:
                df_agg = cast(pd.DataFrame, df_agg.tail(limit).reset_index(drop=True))

            publish_ohlcv_to_redis(
                df_agg,
                symbol=symbol_fxcm,
                timeframe=str(tf_norm),
                redis_client=self._redis_client,
                channel=self._ohlcv_channel,
                data_gate=None,
                hmac_secret=self._hmac_secret,
                hmac_algo=self._hmac_algo,
                source="history_s3",
            )
            PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
            log.info(
                "S3: fxcm_backfill опубліковано %d history_agg барів для %s %s (lookback=%d)",
                int(len(df_agg)),
                symbol_norm,
                tf_norm,
                int(lookback_minutes or 0),
            )
            return

        fx = self._fx_holder.get("client")
        if fx is None or self._redis_client is None:
            PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
            log.warning(
                "S3: fxcm_backfill %s %s — FXCM або Redis недоступні",
                symbol_norm,
                tf_norm,
            )
            return

        lookback_raw = int(lookback_minutes or 60)
        lookback = lookback_raw
        if self._backfill_max_minutes > 0:
            lookback = min(lookback, self._backfill_max_minutes)
        if self._backfill_min_minutes > 0:
            lookback = max(lookback, self._backfill_min_minutes)
        if lookback != lookback_raw:
            log.info(
                "S3: fxcm_backfill lookback скориговано з %d до %d хв",
                lookback_raw,
                lookback,
            )
        fx_tf_raw = _to_fxcm_timeframe(tf_norm)
        try:
            _fetch_and_publish_recent(
                fx,
                symbol=symbol_fxcm,
                timeframe_raw=fx_tf_raw,
                redis_client=self._redis_client,
                ohlcv_channel=self._ohlcv_channel,
                lookback_minutes=lookback,
                last_open_time_ms={},
                data_gate=None,
                min_publish_interval=0.0,
                publish_rate_limit=None,
                hmac_secret=self._hmac_secret,
                hmac_algo=self._hmac_algo,
                session_stats=None,
                ohlcv_sink=None,
                market_status_sink=None,
                allow_calendar_closed=True,
            )
            PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="ok").inc()
            log.info(
                "S3: fxcm_backfill виконано для %s %s (lookback=%d)",
                symbol_norm,
                tf_norm,
                lookback,
            )
        except Exception as exc:  # noqa: BLE001
            PROM_COMMANDS_TOTAL.labels(type=cmd_type, result="error").inc()
            log.exception(
                "S3: fxcm_backfill помилка для %s %s: %s",
                symbol_fxcm,
                fx_tf_raw,
                exc,
            )


class PriceTick(NamedTuple):
    symbol: str
    bid: float
    ask: float
    mid: float
    tick_ts: float


@dataclass
class PriceTickSnap:
    symbol: str
    bid: float
    ask: float
    mid: float
    tick_ts: float
    snap_ts: float


@dataclass
class OhlcvBatch:
    symbol: str
    timeframe: str
    data: pd.DataFrame
    fetched_at: float
    source: str = "stream"


@dataclass
class HeartbeatEvent:
    state: str
    last_bar_close_ms: Optional[int]
    next_open: Optional[dt.datetime]
    sleep_seconds: Optional[float]
    context: Dict[str, Any]


@dataclass
class MarketStatusEvent:
    state: str
    next_open: Optional[dt.datetime]


class AsyncStreamSupervisor:
    """Асинхронно публікує OHLCV/heartbeat/diagnostics через окремий event loop."""

    _SENTINEL = object()

    def __init__(
        self,
        *,
        redis_supplier: Callable[[], Optional[Any]],
        redis_reconnector: Optional[Callable[[], Any]] = None,
        data_gate: Optional[PublishDataGate] = None,
        ohlcv_channel: Optional[str] = None,
        heartbeat_channel: Optional[str] = None,
        price_channel: Optional[str] = None,
        hmac_secret: Optional[str] = None,
        hmac_algo: str = "sha256",
        redis_backoff: Optional[FxcmBackoffController] = None,
    ) -> None:
        self._redis_supplier = redis_supplier
        self._redis_reconnector = redis_reconnector
        self._redis_backoff = redis_backoff
        self._data_gate = data_gate
        self._ohlcv_channel = (ohlcv_channel or "").strip() or None
        self._heartbeat_channel = heartbeat_channel
        self._price_channel = price_channel
        self._hmac_secret = hmac_secret
        self._hmac_algo = hmac_algo

        self._loop = asyncio.new_event_loop()
        self._loop_ready = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="FXCMAsyncSupervisor",
            daemon=True,
        )
        self._started = False
        self._stopping = False
        self._tasks: List[asyncio.Task[Any]] = []
        self._task_handles: Dict[str, Optional[asyncio.Task[Any]]] = {}
        self._queue_stats: Dict[str, Dict[str, Any]] = {}
        self._task_stats: Dict[str, Dict[str, Any]] = {}
        self._last_error: Optional[str] = None
        self._last_publish_ts: Dict[str, float] = {}
        self._publish_counts: Dict[str, int] = {}
        self._started_ts: Optional[float] = None
        self._backpressure_flag = False
        self._backpressure_events = 0
        self._ohlcv_queue: Optional[asyncio.Queue[Any]] = None
        self._heartbeat_queue: Optional[asyncio.Queue[Any]] = None
        self._market_status_queue: Optional[asyncio.Queue[Any]] = None
        self._price_queue: Optional[asyncio.Queue[Any]] = None

        self._register_task("history_consumer")
        self._register_task("heartbeat_consumer")
        self._register_task("market_status_consumer")
        if price_channel:
            self._register_task("price_snapshot_consumer")

    def start(self) -> None:
        if self._started:
            return
        self._thread.start()
        self._loop_ready.wait()
        future = asyncio.run_coroutine_threadsafe(self._start_consumers(), self._loop)
        future.result(timeout=SUPERVISOR_SUBMIT_TIMEOUT_SECONDS)
        self._started = True
        self._started_ts = time.time()

    def stop(self) -> None:
        if not self._started:
            return
        self._stopping = True
        future = asyncio.run_coroutine_threadsafe(self._shutdown_async(), self._loop)
        try:
            future.result(timeout=SUPERVISOR_SUBMIT_TIMEOUT_SECONDS)
        except FutureTimeout:
            log.warning("Supervisor: завершення зайняло надто довго.")
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=SUPERVISOR_JOIN_TIMEOUT_SECONDS)
        self._tasks.clear()
        self._started = False

    # ── Публічні sink-и ──

    def submit_ohlcv_batch(self, batch: OhlcvBatch) -> None:
        self._ensure_running()
        if self._ohlcv_queue is None:
            raise SinkSubmissionError("Supervisor queue ще ініціалізується")
        # Для `fxcm:ohlcv` важливо не «зависати» через backpressure.
        # Якщо Redis/консьюмер тимчасово повільні, краще дропнути найстаріший батч,
        # ніж повністю зупинити доставку live-preview (`complete=false`) для UI.
        self._submit_coroutine(
            self._enqueue(self._ohlcv_queue, batch, "ohlcv", drop_oldest=True)
        )

    def submit_heartbeat(self, event: HeartbeatEvent) -> None:
        self._ensure_running()
        if self._heartbeat_queue is None:
            raise SinkSubmissionError("Supervisor queue ще ініціалізується")
        self._submit_coroutine(
            self._enqueue(self._heartbeat_queue, event, "heartbeat", drop_oldest=True)
        )

    def submit_market_status(self, event: MarketStatusEvent) -> None:
        self._ensure_running()
        if self._market_status_queue is None:
            raise SinkSubmissionError("Supervisor queue ще ініціалізується")
        self._submit_coroutine(
            self._enqueue(self._market_status_queue, event, "market_status")
        )

    def submit_price_snapshot(self, snap: PriceTickSnap) -> None:
        if self._price_queue is None:
            raise SinkSubmissionError("Price snapshot sink не увімкнено")
        self._ensure_running()
        self._submit_coroutine(self._enqueue(self._price_queue, snap, "price"))

    # ── Внутрішня логіка ──

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)

        async def _bootstrap() -> None:
            try:
                await self._initialize_queues()
            finally:
                self._loop_ready.set()

        self._loop.create_task(_bootstrap())
        self._loop.run_forever()

    async def _initialize_queues(self) -> None:
        self._ohlcv_queue = asyncio.Queue(maxsize=OHLCV_QUEUE_MAXSIZE)
        self._heartbeat_queue = asyncio.Queue(maxsize=HEARTBEAT_QUEUE_MAXSIZE)
        self._market_status_queue = asyncio.Queue(maxsize=MARKET_STATUS_QUEUE_MAXSIZE)
        if self._price_channel:
            self._price_queue = asyncio.Queue(maxsize=PRICE_QUEUE_MAXSIZE)
        else:
            self._price_queue = None
        self._register_queue("ohlcv", self._ohlcv_queue)
        self._register_queue("heartbeat", self._heartbeat_queue)
        self._register_queue("market_status", self._market_status_queue)
        if self._price_queue is not None:
            self._register_queue("price", self._price_queue)

    async def _start_consumers(self) -> None:
        history_task = asyncio.ensure_future(self._history_consumer())
        heartbeat_task = asyncio.ensure_future(self._heartbeat_consumer())
        market_task = asyncio.ensure_future(self._market_status_consumer())
        tasks = [history_task, heartbeat_task, market_task]
        task_map: Dict[str, Optional[asyncio.Task[Any]]] = {
            "history_consumer": history_task,
            "heartbeat_consumer": heartbeat_task,
            "market_status_consumer": market_task,
        }
        if self._price_queue is not None:
            price_task = asyncio.ensure_future(self._price_snapshot_consumer())
            tasks.append(price_task)
            task_map["price_snapshot_consumer"] = price_task
        self._tasks = tasks
        self._task_handles = task_map

    async def _shutdown_async(self) -> None:
        queue_entries = self._iter_queues()
        for _name, queue_obj in queue_entries:
            await queue_obj.put(self._SENTINEL)
        joins = [queue_obj.join() for _name, queue_obj in queue_entries]
        await asyncio.gather(*joins, return_exceptions=True)
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def _ensure_running(self) -> None:
        if not self._started or self._stopping:
            raise SinkSubmissionError("Supervisor неактивний")

    def _submit_coroutine(self, coro: Awaitable[None]) -> None:
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)  # type: ignore
        try:
            future.result(timeout=SUPERVISOR_SUBMIT_TIMEOUT_SECONDS)
            self._backpressure_flag = False
        except asyncio.QueueFull as exc:
            self._backpressure_flag = True
            self._backpressure_events += 1
            raise SinkBackpressure("Supervisor: черга переповнена") from exc
        except FutureTimeout as exc:
            raise SinkSubmissionError("Supervisor: таймаут enqueue") from exc
        except SinkBackpressure:
            self._backpressure_flag = True
            self._backpressure_events += 1
            raise
        except Exception as exc:  # noqa: BLE001
            raise SinkSubmissionError("Supervisor: enqueue помилка") from exc

    async def _enqueue(
        self,
        queue_obj: asyncio.Queue[Any],
        item: Any,
        queue_name: str,
        *,
        drop_oldest: bool = False,
    ) -> None:
        inserted = False
        try:
            queue_obj.put_nowait(item)
            inserted = True
        except asyncio.QueueFull:
            if drop_oldest:
                dropped = False
                try:
                    queue_obj.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                else:
                    queue_obj.task_done()
                    dropped = True
                    self._note_queue_drop(queue_name)
                if dropped:
                    try:
                        queue_obj.put_nowait(item)
                        inserted = True
                    except asyncio.QueueFull:
                        self._note_queue_drop(queue_name)
                        raise
                else:
                    self._note_queue_drop(queue_name)
                    raise
            else:
                self._note_queue_drop(queue_name)
                raise
        if inserted:
            self._note_queue_enqueue(queue_name)

    async def _history_consumer(self) -> None:
        assert self._ohlcv_queue is not None
        while True:
            batch = await self._ohlcv_queue.get()
            sentinel = batch is self._SENTINEL
            try:
                if not sentinel:
                    await self._publish_ohlcv_batch(cast(OhlcvBatch, batch))
                    self._note_queue_processed("ohlcv")
                    self._note_task_activity("history_consumer")
            finally:
                self._ohlcv_queue.task_done()
            if sentinel:
                break

    async def _heartbeat_consumer(self) -> None:
        assert self._heartbeat_queue is not None
        while True:
            event = await self._heartbeat_queue.get()
            sentinel = event is self._SENTINEL
            try:
                if not sentinel:
                    await self._publish_heartbeat_event(cast(HeartbeatEvent, event))
                    self._note_queue_processed("heartbeat")
                    self._note_task_activity("heartbeat_consumer")
            finally:
                self._heartbeat_queue.task_done()
            if sentinel:
                break

    async def _market_status_consumer(self) -> None:
        assert self._market_status_queue is not None
        while True:
            event = await self._market_status_queue.get()
            sentinel = event is self._SENTINEL
            try:
                if not sentinel:
                    await self._publish_market_status_event(
                        cast(MarketStatusEvent, event)
                    )
                    self._note_queue_processed("market_status")
                    self._note_task_activity("market_status_consumer")
            finally:
                self._market_status_queue.task_done()
            if sentinel:
                break

    async def _price_snapshot_consumer(self) -> None:
        assert self._price_queue is not None  # для mypy/аналізу
        while True:
            snap = await self._price_queue.get()
            sentinel = snap is self._SENTINEL
            try:
                if not sentinel:
                    await self._publish_price_snapshot_event(cast(PriceTickSnap, snap))
                    self._note_queue_processed("price")
                    self._note_task_activity("price_snapshot_consumer")
            finally:
                self._price_queue.task_done()
            if sentinel:
                break

    async def _publish_ohlcv_batch(self, batch: OhlcvBatch) -> None:
        while not self._stopping:
            client = await self._ensure_redis_client()
            if client is None:
                await asyncio.sleep(SUPERVISOR_RETRY_DELAY_SECONDS)
                continue
            try:
                await self._run_blocking(
                    publish_ohlcv_to_redis,
                    batch.data,
                    symbol=batch.symbol,
                    timeframe=batch.timeframe,
                    redis_client=client,
                    channel=self._ohlcv_channel,
                    data_gate=self._data_gate,
                    hmac_secret=self._hmac_secret,
                    hmac_algo=self._hmac_algo,
                    source=batch.source,
                )
                self._mark_publish("ohlcv")
                return
            except RedisRetryableError:
                PROM_ERROR_COUNTER.labels(type="redis").inc()
                await self._handle_redis_error()
            except Exception as exc:  # noqa: BLE001
                self._last_error = f"ohlcv_publish: {exc}"
                log.exception("Supervisor: публікація OHLCV завершилась помилкою.")
                return

    async def _publish_heartbeat_event(self, event: HeartbeatEvent) -> None:
        client = await self._ensure_redis_client()
        if client is None:
            await asyncio.sleep(SUPERVISOR_RETRY_DELAY_SECONDS)
            return
        try:
            ok = await self._run_blocking(
                _publish_heartbeat,
                client,
                self._heartbeat_channel,
                state=event.state,
                last_bar_close_ms=event.last_bar_close_ms,
                next_open=event.next_open,
                sleep_seconds=event.sleep_seconds,
                context=event.context,
            )
            if not ok:
                raise RedisRetryableError("heartbeat publish failed")
            self._mark_publish("heartbeat")
        except RedisRetryableError:
            PROM_ERROR_COUNTER.labels(type="redis").inc()
            await self._handle_redis_error()
        except Exception as exc:  # noqa: BLE001
            self._last_error = f"heartbeat: {exc}"
            log.exception("Supervisor: публікація heartbeat завершилась помилкою.")

    async def _publish_market_status_event(self, event: MarketStatusEvent) -> None:
        client = await self._ensure_redis_client()
        if client is None:
            await asyncio.sleep(SUPERVISOR_RETRY_DELAY_SECONDS)
            return
        try:
            ok = await self._run_blocking(
                _publish_market_status,
                client,
                event.state,
                next_open=event.next_open,
            )
            if not ok:
                raise RedisRetryableError("market status publish failed")
            self._mark_publish("market_status")
        except RedisRetryableError:
            PROM_ERROR_COUNTER.labels(type="redis").inc()
            await self._handle_redis_error()
        except Exception as exc:  # noqa: BLE001
            self._last_error = f"market_status: {exc}"
            log.exception("Supervisor: публікація market_status завершилась помилкою.")

    async def _publish_price_snapshot_event(self, snap: PriceTickSnap) -> None:
        if self._price_channel is None:
            return
        while not self._stopping:
            client = await self._ensure_redis_client()
            if client is None:
                await asyncio.sleep(SUPERVISOR_RETRY_DELAY_SECONDS)
                continue
            try:
                await self._run_blocking(
                    _publish_price_snapshot,
                    client,
                    channel=self._price_channel,
                    snapshot=snap,
                )
                self._mark_publish("price")
                return
            except RedisRetryableError:
                PROM_ERROR_COUNTER.labels(type="redis").inc()
                await self._handle_redis_error()
            except Exception as exc:  # noqa: BLE001
                self._last_error = f"price_snapshot: {exc}"
                log.exception("Supervisor: публікація price snapshot неуспішна.")
                return

    async def _ensure_redis_client(
        self,
    ) -> Optional[Any]:  # noqa: PLR6301 - метод класу
        if self._redis_backoff is not None and self._redis_backoff.remaining() > 0.0:
            return None
        client = self._redis_supplier()
        if client is not None:
            return client
        if self._redis_reconnector is None:
            return None
        try:
            return await self._run_blocking(self._redis_reconnector)
        except Exception:  # noqa: BLE001
            log.exception("Supervisor: перепідключення Redis неуспішне.")
            return None

    async def _handle_redis_error(self) -> None:
        self._note_redis_failure("redis_error")
        if self._redis_reconnector is None:
            await asyncio.sleep(SUPERVISOR_RETRY_DELAY_SECONDS)
            return
        try:
            await self._run_blocking(self._redis_reconnector)
        except Exception:  # noqa: BLE001
            log.exception("Supervisor: redis_reconnector знову впав.")
            await asyncio.sleep(SUPERVISOR_RETRY_DELAY_SECONDS)

    async def _run_blocking(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

    def _register_queue(self, name: str, queue_obj: asyncio.Queue[Any]) -> None:
        self._queue_stats[name] = {
            "name": name,
            "maxsize": queue_obj.maxsize,
            "enqueued": 0,
            "processed": 0,
            "dropped": 0,
            "last_enqueue_ts": None,
            "last_processed_ts": None,
        }

    def _register_task(self, name: str) -> None:
        self._task_stats[name] = {
            "name": name,
            "processed": 0,
            "errors": 0,
            "last_activity_ts": None,
            "last_error": None,
        }

    def _note_queue_enqueue(self, name: str) -> None:
        stats = self._queue_stats.get(name)
        if not stats:
            return
        stats["enqueued"] += 1
        stats["last_enqueue_ts"] = time.time()

    def _note_queue_processed(self, name: str) -> None:
        stats = self._queue_stats.get(name)
        if not stats:
            return
        stats["processed"] += 1
        stats["last_processed_ts"] = time.time()

    def _note_queue_drop(self, name: str) -> None:
        stats = self._queue_stats.get(name)
        if not stats:
            return
        stats["dropped"] += 1
        stats["last_processed_ts"] = time.time()

    def _note_task_activity(self, name: str, *, error: Optional[str] = None) -> None:
        stats = self._task_stats.get(name)
        if not stats:
            return
        now = time.time()
        stats["last_activity_ts"] = now
        if error:
            stats["errors"] += 1
            stats["last_error"] = error
            self._last_error = error
        else:
            stats["processed"] += 1

    def _iter_queues(self) -> List[Tuple[str, asyncio.Queue[Any]]]:
        entries: List[Tuple[str, asyncio.Queue[Any]]] = []
        if self._ohlcv_queue is not None:
            entries.append(("ohlcv", self._ohlcv_queue))
        if self._heartbeat_queue is not None:
            entries.append(("heartbeat", self._heartbeat_queue))
        if self._market_status_queue is not None:
            entries.append(("market_status", self._market_status_queue))
        if self._price_queue is not None:
            entries.append(("price", self._price_queue))
        return entries

    def _note_redis_failure(self, reason: str) -> None:
        if self._redis_backoff is None:
            return
        self._redis_backoff.fail(reason)

    def _note_redis_success(self) -> None:
        if self._redis_backoff is None:
            return
        self._redis_backoff.success()

    def _task_state_label(self, task: Optional[asyncio.Task[Any]]) -> str:
        if task is None:
            return "disabled"
        if task.cancelled():
            return "cancelled"
        if task.done():
            return "error" if task.exception() else "done"
        return "running"

    def _mark_publish(self, channel: str) -> None:
        now = time.time()
        self._last_publish_ts[channel] = now
        self._publish_counts[channel] = self._publish_counts.get(channel, 0) + 1
        self._note_redis_success()

    async def _gather_diag_snapshot(self) -> Dict[str, Any]:
        diag: Dict[str, Any] = {
            "state": "stopping" if self._stopping else "running",
            "loop_alive": self._loop.is_running(),
            "started_at": self._started_ts,
            "backpressure": self._backpressure_flag,
            "last_error": self._last_error,
            "queues": [],
            "tasks": [],
            "last_publish_ts": self._last_publish_ts.copy(),
            "publish_counts": self._publish_counts.copy(),
            "backpressure_events": self._backpressure_events,
        }
        now = time.time()
        if self._started_ts is not None:
            diag["uptime_seconds"] = max(0.0, now - self._started_ts)
        total_enqueued = 0
        total_processed = 0
        total_dropped = 0
        total_depth = 0
        for name, queue_obj in self._iter_queues():
            stats = self._queue_stats.get(name, {})
            size = queue_obj.qsize()
            total_depth += size
            enqueued = int(stats.get("enqueued") or 0)
            processed = int(stats.get("processed") or 0)
            dropped = int(stats.get("dropped") or 0)
            total_enqueued += enqueued
            total_processed += processed
            total_dropped += dropped
            diag["queues"].append(
                {
                    "name": name,
                    "size": size,
                    "maxsize": queue_obj.maxsize,
                    "enqueued": enqueued,
                    "processed": processed,
                    "dropped": dropped,
                    "last_enqueue_ts": stats.get("last_enqueue_ts"),
                    "last_processed_ts": stats.get("last_processed_ts"),
                }
            )
        active_tasks = 0
        total_errors = 0
        for name, task in self._task_handles.items():
            stats = self._task_stats.get(name, {})
            last_activity = stats.get("last_activity_ts")
            idle_seconds = None
            if isinstance(last_activity, (int, float)):
                idle_seconds = max(0.0, now - float(last_activity))
            if task is not None and not task.done():
                active_tasks += 1
            total_errors += int(stats.get("errors") or 0)
            diag["tasks"].append(
                {
                    "name": name,
                    "state": self._task_state_label(task),
                    "processed": stats.get("processed"),
                    "errors": stats.get("errors"),
                    "idle_seconds": idle_seconds,
                    "last_error": stats.get("last_error"),
                    "last_event_ts": last_activity,
                }
            )
        diag["metrics"] = {
            "total_enqueued": total_enqueued,
            "total_processed": total_processed,
            "total_dropped": total_dropped,
            "queue_depth_total": total_depth,
            "publishes_total": sum(self._publish_counts.values()),
            "active_tasks": active_tasks,
            "task_errors": total_errors,
        }
        return diag

    def diagnostics_snapshot(self) -> Dict[str, Any]:
        if not self._started:
            return {}
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._gather_diag_snapshot(), self._loop
            )
            return future.result(timeout=SUPERVISOR_SUBMIT_TIMEOUT_SECONDS)
        except Exception:  # noqa: BLE001
            return {"state": "error"}


class PriceSnapshotWorker:
    """Агрегує FXCM-тикі та публікує снепшоти у Redis канал."""

    def __init__(
        self,
        redis_supplier: Callable[[], Optional[Any]],
        *,
        channel: str,
        interval_seconds: float,
        symbols: Sequence[str],
        snapshot_callback: Optional[Callable[[PriceTickSnap], None]] = None,
    ) -> None:
        self._redis_supplier = redis_supplier
        self._channel = channel
        self._interval = max(0.5, float(interval_seconds))
        self._queue: "queue.Queue[PriceTick]" = queue.Queue(
            maxsize=PRICE_SNAPSHOT_QUEUE_MAXSIZE
        )
        self._lock = threading.Lock()
        self._last_ticks: Dict[str, PriceTick] = {}
        self._last_snapshot_ts: Optional[float] = None
        self._last_tick_ts: Optional[float] = None
        self._symbols: Set[str] = {_normalize_symbol(sym) for sym in symbols if sym}
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._snapshot_callback = snapshot_callback

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="price-snapshot-worker", daemon=True
        )
        self._thread.start()

    def set_symbols(self, symbols: Sequence[str]) -> None:
        """Оновлює фільтр символів без перезапуску воркера (dynamic universe v1)."""

        new_set = {_normalize_symbol(sym) for sym in symbols if sym}
        with self._lock:
            self._symbols = new_set

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def enqueue_tick(self, symbol: str, bid: float, ask: float, tick_ts: float) -> None:
        symbol_norm = _normalize_symbol(symbol)
        if self._symbols and symbol_norm not in self._symbols:
            return
        mid = (float(bid) + float(ask)) / 2.0
        tick = PriceTick(
            symbol=symbol_norm,
            bid=float(bid),
            ask=float(ask),
            mid=mid,
            tick_ts=float(tick_ts),
        )
        try:
            self._queue.put_nowait(tick)
        except queue.Full:
            try:
                self._queue.get_nowait()
            except queue.Empty:  # pragma: no cover - гонка між потоками
                pass
            try:
                self._queue.put_nowait(tick)
            except queue.Full:
                log.debug(
                    "Черга price snapshot переповнена — тик %s відкинуто.", symbol_norm
                )

    def flush(self) -> None:
        """Примусово публікує останні відомі тики (використовується у тестах)."""

        self._drain_queue()
        self._publish_snapshot()

    def snapshot_metadata(self) -> Dict[str, Any]:
        with self._lock:
            last_snap = self._last_snapshot_ts
            last_tick = self._last_tick_ts
            last_ticks = list(self._last_ticks.values())
            thread_alive = self._thread is not None and self._thread.is_alive()
        now = time.time()
        silence = max(0.0, now - last_tick) if last_tick is not None else None
        symbol_states: List[Dict[str, Any]] = []
        max_age = silence
        for tick in sorted(last_ticks, key=lambda item: item.symbol):
            age = max(0.0, now - tick.tick_ts)
            symbol_states.append(
                {
                    "symbol": tick.symbol,
                    "bid": tick.bid,
                    "ask": tick.ask,
                    "mid": tick.mid,
                    "tick_ts": tick.tick_ts,
                    "tick_age_seconds": age,
                }
            )
            if max_age is None or age > max_age:
                max_age = age
        silence = max_age
        state = "ok"
        if not thread_alive:
            state = "stopped"
        elif not last_ticks:
            state = "waiting"
        elif silence is not None and silence > self._interval * 3:
            state = "stale"
        metadata: Dict[str, Any] = {
            "channel": self._channel,
            "interval_seconds": self._interval,
            "symbols": [tick["symbol"] for tick in symbol_states],
            "symbols_state": symbol_states,
            "last_snap_ts": last_snap,
            "last_tick_ts": last_tick,
            "tick_silence_seconds": silence,
            "queue_depth": self._queue.qsize(),
            "thread_alive": thread_alive,
            "state": state,
        }
        return metadata

    def _run(self) -> None:
        next_publish = time.monotonic() + self._interval
        while not self._stop.is_set():
            self._drain_queue()
            now = time.monotonic()
            if now >= next_publish:
                self._publish_snapshot()
                next_publish = now + self._interval
            self._stop.wait(0.2)
        self._drain_queue()
        self._publish_snapshot()

    def _drain_queue(self) -> None:
        while True:
            try:
                tick = self._queue.get_nowait()
            except queue.Empty:
                break
            with self._lock:
                self._last_ticks[tick.symbol] = tick
                self._last_tick_ts = tick.tick_ts

    def _publish_snapshot(self) -> None:
        with self._lock:
            ticks = list(self._last_ticks.values())
        if not ticks:
            return
        snap_ts = time.time()
        callback = self._snapshot_callback
        if callback is not None:
            for tick in ticks:
                callback(
                    PriceTickSnap(
                        symbol=tick.symbol,
                        bid=tick.bid,
                        ask=tick.ask,
                        mid=tick.mid,
                        tick_ts=tick.tick_ts,
                        snap_ts=snap_ts,
                    )
                )
            with self._lock:
                self._last_snapshot_ts = snap_ts
            return

        redis_client = self._redis_supplier()
        if redis_client is None:
            return
        try:
            for tick in ticks:
                _publish_price_snapshot(
                    redis_client,
                    channel=self._channel,
                    snapshot=PriceTickSnap(
                        symbol=tick.symbol,
                        bid=tick.bid,
                        ask=tick.ask,
                        mid=tick.mid,
                        tick_ts=tick.tick_ts,
                        snap_ts=snap_ts,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            PROM_ERROR_COUNTER.labels(type="redis").inc()
            log.debug("Публікація price snapshot неуспішна: %s", exc)
            return
        with self._lock:
            self._last_snapshot_ts = snap_ts


class TickOhlcvWorker:
    """Агрегує live тики у 1m/5m OHLCV та віддає live-бар у ohlcv sink.

    Важливо:
    - Вхідні тики приходять з FXCM OfferTable callback у довільному потоці.
    - Агрегатори НЕ thread-safe, тому обробка іде у власному worker thread через чергу.
        - Tick-agg використовується як live preview (`complete=false`).
        - Фінальні бари (`complete=true`) мають приходити з FXCM history, щоб `volume`
            був максимально звіряємим/«реальним» для трейдера.
    """

    def __init__(
        self,
        *,
        enabled: bool,
        symbols: Sequence[str],
        max_synth_gap_minutes: int,
        live_publish_min_interval_seconds: float = 0.25,
        ohlcv_sink: Callable[[OhlcvBatch], None],
    ) -> None:
        self._enabled = bool(enabled)
        self._symbols: Set[str] = {_normalize_symbol(sym) for sym in symbols if sym}
        self._max_synth_gap_minutes = max(0, int(max_synth_gap_minutes))
        self._ohlcv_sink = ohlcv_sink
        self._queue: "queue.Queue[PriceTick]" = queue.Queue(
            maxsize=PRICE_SNAPSHOT_QUEUE_MAXSIZE
        )
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._agg_1m: Dict[str, TickOhlcvAggregator] = {}
        self._agg_5m: Dict[str, OhlcvFromLowerTfAggregator] = {}
        self._agg_15m: Dict[str, OhlcvFromLowerTfAggregator] = {}
        self._agg_1h: Dict[str, OhlcvFromLowerTfAggregator] = {}
        self._agg_4h: Dict[str, OhlcvFromLowerTfAggregator] = {}

        # Якщо tick_ts приходить “у майбутньому” відносно локального time.time(),
        # tick-agg може будувати bucket-и з end_ms > now_ms, і clock-flush не
        # зможе закривати бари (симптом: only_live_bars_no_complete).
        self._max_future_tick_skew_seconds = 5.0
        self._future_tick_clamps_total = 0
        self._future_tick_clamps_by_symbol: Dict[str, int] = {}
        self._last_tick_skew_seconds_by_symbol: Dict[str, float] = {}
        self._last_complete_close_ms: Dict[Tuple[str, str], int] = {}

        # Для калібрування preview volume: зберігаємо tick_count для закритих барів,
        # щоб зіставити їх із FXCM history Volume по open_time.
        self._closed_tick_counts: Dict[Tuple[str, str], Dict[int, int]] = {}
        self._closed_tick_counts_order: Dict[Tuple[str, str], Deque[int]] = {}
        self._closed_tick_counts_max = 500

        # Live-оновлення (complete=false) у `fxcm:ohlcv` мають бути частими,
        # але не кожен тик. Тримаємо тротлінг на рівні worker-а.
        self._live_publish_min_interval_seconds = max(
            0.05, float(live_publish_min_interval_seconds)
        )
        self._last_live_publish_ts: Dict[Tuple[str, str], float] = {}

        # Clock-flush: закриваємо бари по часу, навіть якщо немає тика у наступному bucket.
        # Інтервал робимо помірним (200–1000мс), щоб не забивати CPU.
        self._clock_flush_interval_seconds = min(
            1.0, max(0.2, float(self._live_publish_min_interval_seconds))
        )
        self._last_clock_flush_monotonic: float = 0.0
        self._last_tick_monotonic: Optional[float] = None

    def snapshot_metadata(self) -> Dict[str, Any]:
        """Легка діагностика tick-agg для heartbeat/status.

        Мета: швидко зрозуміти, чи tick_ts зсунуті у майбутнє та коли востаннє
        реально виходили `complete=true` бари.
        """

        thread_alive = bool(self._thread and self._thread.is_alive())
        now_monotonic = time.monotonic()
        tick_age = (
            None
            if self._last_tick_monotonic is None
            else max(0.0, now_monotonic - float(self._last_tick_monotonic))
        )
        flush_age = (
            None
            if self._last_clock_flush_monotonic <= 0.0
            else max(0.0, now_monotonic - float(self._last_clock_flush_monotonic))
        )

        last_complete: Dict[str, Dict[str, int]] = {}
        for (symbol, tf), close_ms in self._last_complete_close_ms.items():
            sym_map = last_complete.setdefault(symbol, {})
            sym_map[str(tf)] = int(close_ms)

        volume_calibration = _VOLUME_CALIBRATOR.snapshot()

        return {
            "enabled": bool(self._enabled),
            "thread_alive": thread_alive,
            "queue_depth": self._queue.qsize(),
            "last_tick_age_seconds": tick_age,
            "last_clock_flush_age_seconds": flush_age,
            "max_future_tick_skew_seconds": float(self._max_future_tick_skew_seconds),
            "future_tick_clamps_total": int(self._future_tick_clamps_total),
            "future_tick_clamps_by_symbol": dict(self._future_tick_clamps_by_symbol),
            "last_tick_skew_seconds_by_symbol": dict(self._last_tick_skew_seconds_by_symbol),
            "last_complete_close_ms": last_complete,
            "volume_calibration": volume_calibration,
        }

    def get_closed_tick_count(
        self, *, symbol_norm: str, tf_norm: str, open_time_ms: int
    ) -> Optional[int]:
        key = (str(symbol_norm), str(tf_norm))
        mapping = self._closed_tick_counts.get(key)
        if not mapping:
            return None
        value = mapping.get(int(open_time_ms))
        return int(value) if value is not None else None

    def start(self) -> None:
        if not self._enabled:
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="tick-ohlcv-worker", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def set_symbols(self, symbols: Sequence[str]) -> None:
        """Оновлює список символів, які агрегуються (dynamic universe v1)."""

        self._symbols = {_normalize_symbol(sym) for sym in symbols if sym}
        # Легка зачистка агрегаторів, щоб не накопичувати стан по неактивних.
        active = set(self._symbols)
        for sym in list(self._agg_1m.keys()):
            if sym not in active:
                self._agg_1m.pop(sym, None)
        for sym in list(self._agg_5m.keys()):
            if sym not in active:
                self._agg_5m.pop(sym, None)
        for sym in list(self._agg_15m.keys()):
            if sym not in active:
                self._agg_15m.pop(sym, None)
        for sym in list(self._agg_1h.keys()):
            if sym not in active:
                self._agg_1h.pop(sym, None)
        for sym in list(self._agg_4h.keys()):
            if sym not in active:
                self._agg_4h.pop(sym, None)

    def _ingest_into_higher_preview_aggs(
        self, bar: TickOhlcvBar, *, now_monotonic: Optional[float] = None
    ) -> None:
        """Пробиває 1m бар (live або complete) у 5m/15m/1h/4h preview-агрегатори."""

        symbol = str(bar.symbol)
        agg_map: List[Tuple[str, Dict[str, OhlcvFromLowerTfAggregator], int]] = [
            ("5m", self._agg_5m, 300),
            ("15m", self._agg_15m, 900),
            ("1h", self._agg_1h, 3600),
            ("4h", self._agg_4h, 14400),
        ]
        for _label, mapping, target_seconds in agg_map:
            agg = mapping.get(symbol)
            if agg is None:
                agg = OhlcvFromLowerTfAggregator(
                    symbol,
                    target_tf_seconds=int(target_seconds),
                    lower_tf_seconds=60,
                    source="tick_agg",
                )
                mapping[symbol] = agg
            try:
                res = agg.ingest_bar(bar)
            except Exception:  # noqa: BLE001
                continue
            if res.live_bar is not None:
                self._publish_tick_bar(res.live_bar, now_monotonic=now_monotonic)
            for closed in res.closed_bars:
                self._publish_tick_bar(closed)

    def enqueue_tick(self, symbol: str, bid: float, ask: float, tick_ts: float) -> None:
        if not self._enabled:
            return
        symbol_norm = _normalize_symbol(symbol)
        if self._symbols and symbol_norm not in self._symbols:
            return
        mid = (float(bid) + float(ask)) / 2.0
        tick = PriceTick(
            symbol=symbol_norm,
            bid=float(bid),
            ask=float(ask),
            mid=mid,
            tick_ts=float(tick_ts),
        )
        try:
            self._queue.put_nowait(tick)
        except queue.Full:
            try:
                self._queue.get_nowait()
            except queue.Empty:  # pragma: no cover
                pass
            try:
                self._queue.put_nowait(tick)
            except queue.Full:
                log.debug(
                    "Черга tick_ohlcv переповнена — тик %s відкинуто.", symbol_norm
                )

    def _run(self) -> None:
        while not self._stop.is_set():
            drained = False
            while True:
                try:
                    tick = self._queue.get_nowait()
                except queue.Empty:
                    break
                drained = True
                try:
                    self._process_tick(tick)
                finally:
                    self._queue.task_done()
            self._maybe_clock_flush()
            if not drained:
                self._stop.wait(0.1)

    def _maybe_clock_flush(self) -> None:
        if not self._enabled:
            return
        if not self._agg_1m:
            return
        now_monotonic = time.monotonic()
        if (
            self._last_clock_flush_monotonic > 0.0
            and now_monotonic - self._last_clock_flush_monotonic
            < self._clock_flush_interval_seconds
        ):
            return

        calendar_open = is_trading_time(_now_utc())
        ticks_recent = (
            self._last_tick_monotonic is not None
            and now_monotonic - self._last_tick_monotonic <= 10.0
        )
        if not calendar_open and not ticks_recent:
            return

        self._last_clock_flush_monotonic = now_monotonic
        now_ms = int(time.time() * 1000)
        for symbol, agg_1m in list(self._agg_1m.items()):
            try:
                result = agg_1m.flush_until(now_ms)
            except Exception:  # noqa: BLE001 - flush не має валити worker
                continue
            for bar in result.closed_bars:
                # Закриті (complete=True) бари від tick-agg не публікуємо назовні.
                # Вони використовуються лише як внутрішній тригер/агрегаційна база,
                # а «авторитетний» complete бар (із реальним Volume) має прийти з FXCM history.
                self._publish_tick_bar(bar)
                self._ingest_into_higher_preview_aggs(bar)

    def _process_tick(self, tick: PriceTick) -> None:
        symbol = tick.symbol
        PROM_TICK_AGG_TICKS_TOTAL.labels(symbol=symbol).inc()
        now_monotonic = time.monotonic()
        self._last_tick_monotonic = now_monotonic

        now_sec = time.time()
        skew_sec = float(tick.tick_ts) - float(now_sec)
        self._last_tick_skew_seconds_by_symbol[symbol] = float(skew_sec)
        agg_1m = self._agg_1m.get(symbol)
        if agg_1m is None:
            agg_1m = TickOhlcvAggregator(
                symbol,
                tf_seconds=60,
                fill_gaps=True,
                max_synthetic_gap_minutes=self._max_synth_gap_minutes,
                source="tick_agg",
            )
            self._agg_1m[symbol] = agg_1m

        tick_ms = int(float(tick.tick_ts) * 1000)
        now_ms = int(float(now_sec) * 1000)
        if skew_sec > self._max_future_tick_skew_seconds:
            tick_ms = now_ms
            self._future_tick_clamps_total += 1
            self._future_tick_clamps_by_symbol[symbol] = (
                self._future_tick_clamps_by_symbol.get(symbol, 0) + 1
            )

        fx_tick = FxcmTick(
            symbol=symbol,
            ts_ms=int(tick_ms),
            price=float(tick.mid),
            bid=float(tick.bid),
            ask=float(tick.ask),
            # Для live stream у нас немає "біржового" обсягу з OfferTable.
            # Натомість використовуємо tick volume: +1 за кожен прийнятий тик.
            # Це відповідає інтуїції TradingView/CFD-потоків, де volume часто є "tick volume".
            volume=1.0,
        )

        result_1m = agg_1m.ingest_tick(fx_tick)

        if result_1m.live_bar is not None:
            self._publish_tick_bar(result_1m.live_bar, now_monotonic=now_monotonic)
            self._ingest_into_higher_preview_aggs(
                result_1m.live_bar, now_monotonic=now_monotonic
            )

        for bar in result_1m.closed_bars:
            self._publish_tick_bar(bar)
            self._ingest_into_higher_preview_aggs(bar, now_monotonic=now_monotonic)

    def _publish_tick_bar(
        self, bar: TickOhlcvBar, *, now_monotonic: Optional[float] = None
    ) -> None:
        if not bar.complete:
            key = (str(bar.symbol), str(bar.tf))
            now = now_monotonic if now_monotonic is not None else time.monotonic()
            last = self._last_live_publish_ts.get(key)
            if (
                last is not None
                and now - last < self._live_publish_min_interval_seconds
            ):
                return
            self._last_live_publish_ts[key] = now
        else:
            self._last_complete_close_ms[(str(bar.symbol), str(bar.tf))] = int(bar.end_ms)

            key = (str(bar.symbol), str(bar.tf))
            open_time = int(getattr(bar, "start_ms", 0) or 0)
            tick_count = int(getattr(bar, "tick_count", 0) or 0)
            if open_time > 0 and tick_count >= 0:
                mapping = self._closed_tick_counts.setdefault(key, {})
                order = self._closed_tick_counts_order.setdefault(
                    key, deque(maxlen=self._closed_tick_counts_max)
                )
                if open_time not in mapping:
                    order.append(open_time)
                mapping[open_time] = tick_count
                # Підчищаємо найстаріше, щоб dict не ріс.
                while len(mapping) > self._closed_tick_counts_max and order:
                    old_open = order.popleft()
                    mapping.pop(int(old_open), None)

            PROM_TICK_AGG_BARS_TOTAL.labels(
                symbol=str(bar.symbol),
                tf=str(bar.tf),
                synthetic="1" if bool(bar.synthetic) else "0",
            ).inc()
            try:
                PROM_TICK_AGG_AVG_SPREAD.labels(
                    symbol=str(bar.symbol),
                    tf=str(bar.tf),
                ).set(float(getattr(bar, "avg_spread", 0.0) or 0.0))
            except Exception:  # noqa: BLE001 - метрика не критична
                pass

            # Важливо: complete бари з tick-agg НЕ публікуємо в Redis як фінальні.
            # Це дозволяє тримати фінальний volume «реальним» (із FXCM history),
            # а tick-agg використовувати лише як live preview (complete=false).
            return
        df = pd.DataFrame(
            [
                {
                    "open_time": int(bar.start_ms),
                    "close_time": int(bar.end_ms),
                    "open": float(bar.open),
                    "high": float(bar.high),
                    "low": float(bar.low),
                    "close": float(bar.close),
                    "tick_count": int(getattr(bar, "tick_count", 0) or 0),
                    "volume": float(
                        _VOLUME_CALIBRATOR.estimate_preview_volume(
                            symbol_norm=str(bar.symbol),
                            tf_norm=str(bar.tf),
                            tick_count=int(getattr(bar, "tick_count", 0) or 0),
                            fallback=float(bar.volume),
                        )
                    ),
                    "bar_range": float(getattr(bar, "bar_range", 0.0) or 0.0),
                    "body_size": float(getattr(bar, "body_size", 0.0) or 0.0),
                    "upper_wick": float(getattr(bar, "upper_wick", 0.0) or 0.0),
                    "lower_wick": float(getattr(bar, "lower_wick", 0.0) or 0.0),
                    "avg_spread": float(getattr(bar, "avg_spread", 0.0) or 0.0),
                    "max_spread": float(getattr(bar, "max_spread", 0.0) or 0.0),
                    "complete": bool(bar.complete),
                    "synthetic": bool(bar.synthetic),
                    "source": str(bar.source),
                    "tf": str(bar.tf),
                }
            ]
        )
        batch = OhlcvBatch(
            symbol=bar.symbol,
            timeframe=str(bar.tf),
            data=df,
            fetched_at=time.time(),
            source=str(bar.source),
        )
        try:
            self._ohlcv_sink(batch)
        except Exception as exc:  # noqa: BLE001
            # Важливо: sink може тимчасово відмовити (backpressure/Redis проблеми).
            # Tick worker не має падати, інакше UI перестає отримувати live preview.
            PROM_ERROR_COUNTER.labels(type="tick_agg_sink").inc()
            log.debug(
                "Tick-agg: не вдалося передати OhlcvBatch у sink (%s %s complete=%s): %s",
                str(bar.symbol),
                str(bar.tf),
                bool(bar.complete),
                exc,
            )
            return


class FXCMOfferSubscription:
    """Підписка на FXCM OfferTable для отримання живих котирувань."""

    def __init__(
        self,
        fx: ForexConnect,
        *,
        symbols: Sequence[str],
        on_tick: Callable[[str, float, float, float], None],
    ) -> None:
        self._fx = fx
        self._symbols = {_normalize_symbol(sym) for sym in symbols if sym}
        self._on_tick = on_tick
        self._table: Optional[Any] = None
        self._listener: Optional[Any] = None
        self._fallback_listener: Optional[Any] = None
        self._fallback_updates: List[Any] = []
        self._active = False
        if fxcorepy is None:
            log.info("fxcorepy недоступний — tick-підписка пропущена.")
            return
        offers_table = self._obtain_offers_table(fx)
        if offers_table is None:
            log.error(
                "Не вдалося отримати OFFERS table (table manager вимкнений або таблиці ще не завантажені)."
            )
            return
        subscribed = self._subscribe_via_common(offers_table)
        if not subscribed:
            subscribed = self._subscribe_via_fxcorepy(offers_table)
        if not subscribed:
            log.error(
                "FXCM OfferTable підписка недоступна (Common/fxcorepy listener відсутні)."
            )
            return
        self._table = offers_table
        self._active = True
        log.debug(
            "FXCM OfferTable підписка активована для %d символів.",
            len(self._symbols) or -1,
        )

    def close(self) -> None:
        if not self._active:
            return
        try:
            if self._table is not None:
                if self._listener is not None:
                    unsubscribe = (
                        getattr(Common, "unsubscribe_table_updates", None)
                        if Common is not None
                        else None
                    )
                    if callable(unsubscribe):
                        # type: ignore[misc]
                        unsubscribe(self._table, self._listener)  # type: ignore[misc]
                    else:
                        listener_unsubscribe = getattr(
                            self._listener, "unsubscribe", None
                        )
                        if callable(listener_unsubscribe):
                            try:
                                listener_unsubscribe()
                                log.debug(
                                    "FXCM OfferTable listener відписано "
                                    "fallback-методом listener.unsubscribe().",
                                )
                            except Exception as exc:  # noqa: BLE001
                                log.debug(
                                    "Fallback listener.unsubscribe() неуспішний: %s",
                                    exc,
                                )
                        else:
                            log.debug(
                                "Common.unsubscribe_table_updates недоступний, "
                                "а listener.unsubscribe() відсутній — відписка пропущена.",
                            )
                if self._fallback_listener is not None and self._fallback_updates:
                    for update_type in list(self._fallback_updates):
                        try:
                            self._table.unsubscribe_update(
                                update_type, self._fallback_listener
                            )
                        except Exception as exc:  # noqa: BLE001
                            log.debug(
                                "Не вдалося відписатися від OFFERS (%s): %s",
                                update_type,
                                exc,
                            )
        except Exception as exc:  # noqa: BLE001
            log.debug("Помилка під час відписки OfferTable: %s", exc)
        finally:
            self._active = False
            self._table = None
            self._listener = None
            self._fallback_listener = None
            self._fallback_updates.clear()

    def _subscribe_via_common(self, offers_table: Any) -> bool:
        if Common is None:
            return False
        subscribe = getattr(Common, "subscribe_table_updates", None)
        if not callable(subscribe):
            return False
        try:
            self._listener = subscribe(
                offers_table,
                on_add_callback=self._handle_table_callback,
                on_change_callback=self._handle_table_callback,
            )
            return True
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Common.subscribe_table_updates недоступний, пробуємо fxcorepy fallback: %s",
                exc,
            )
            self._listener = None
            return False

    def _subscribe_via_fxcorepy(self, offers_table: Any) -> bool:
        base_listener = getattr(fxcorepy, "AO2GTableListener", None)
        if base_listener is None:
            return False

        class _OfferListener(base_listener):  # type: ignore[misc, valid-type]
            def __init__(self, handler: Callable[[Any], None]) -> None:
                super().__init__()
                self._handler = handler

            def on_added(
                self, _listener: Any, _row_id: Any, row_data: Any
            ) -> None:  # noqa: D401
                self._handler(row_data)

            def on_changed(self, _listener: Any, _row_id: Any, row_data: Any) -> None:
                self._handler(row_data)

        try:
            listener = _OfferListener(self._handle_row)
        except Exception as exc:  # noqa: BLE001
            log.warning("Не вдалося створити fxcorepy AO2GTableListener: %s", exc)
            return False
        update_type = getattr(fxcorepy, "O2GTableUpdateType", None)
        if update_type is None:
            log.warning("fxcorepy.O2GTableUpdateType відсутній — fallback неможливий.")
            return False
        types_to_subscribe = []
        for attr in ("INSERT", "UPDATE"):
            update_value = getattr(update_type, attr, None)
            if update_value is not None:
                types_to_subscribe.append(update_value)
        if not types_to_subscribe:
            log.warning(
                "fxcorepy.O2GTableUpdateType не містить INSERT/UPDATE — fallback неможливий."
            )
            return False
        try:
            for update_value in types_to_subscribe:
                offers_table.subscribe_update(update_value, listener)
        except Exception as exc:  # noqa: BLE001
            log.warning("OFFERS subscribe через fxcorepy неуспішний: %s", exc)
            return False
        self._fallback_listener = listener
        self._fallback_updates = list(types_to_subscribe)
        log.info(
            "FXCM OfferTable підписано через fxcorepy fallback (Common недоступний)."
        )
        return True

    def _handle_table_callback(self, *args: Any, **kwargs: Any) -> None:
        row_data: Any = None
        if "row_data" in kwargs:
            row_data = kwargs["row_data"]
        elif args:
            # Common може передавати (row_id, row_data) або лише row_data.
            row_data = args[-1]
        self._handle_row(row_data)

    def _obtain_offers_table(self, fx: ForexConnect) -> Optional[Any]:
        table_id = getattr(ForexConnect, "OFFERS", None)
        get_table = getattr(fx, "get_table", None)
        if callable(get_table) and table_id is not None:
            try:
                table = get_table(table_id)
                if table is not None:
                    return table
            except Exception as exc:  # noqa: BLE001
                log.debug("ForexConnect.get_table OFFERS повернув помилку: %s", exc)
        manager = getattr(fx, "table_manager", None)
        if manager is None:
            return None
        try:
            # type: ignore - `manager` може бути `None` або не мати методу в середовищі без SDK;
            # головне пам’ятати, що таким чином ми свідомо вимикаємо mypy-перевірку на цьому виклику.
            return manager.get_table(fxcorepy.O2GTableType.OFFERS)  # type: ignore
        except Exception as exc:  # noqa: BLE001
            log.debug("TableManager.get_table OFFERS неуспішний: %s", exc)
            return None

    def _handle_row(self, row_data: Any) -> None:
        if row_data is None:
            return
        symbol_raw = self._read_field(
            row_data, ["Instrument", "Symbol", "instrument", "symbol", "OfferSymbol"]
        )
        symbol = _normalize_symbol(symbol_raw) if symbol_raw else None
        if not symbol:
            return
        if self._symbols and symbol not in self._symbols:
            return
        bid = self._read_float(row_data, ["Bid", "bid", "BestBid"], ["getBid"])
        ask = self._read_float(row_data, ["Ask", "ask", "BestAsk"], ["getAsk"])
        if bid is None or ask is None:
            return
        tick_dt = self._read_time(row_data)
        tick_ts = tick_dt.timestamp() if tick_dt else time.time()
        try:
            self._on_tick(symbol, bid, ask, tick_ts)
        except Exception as exc:  # noqa: BLE001
            log.debug("Tick callback викликав помилку для %s: %s", symbol, exc)

    @staticmethod
    def _read_field(row_data: Any, candidates: Sequence[str]) -> Optional[str]:
        for name in candidates:
            value = FXCMOfferSubscription._value_from_row(row_data, name)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        getter = getattr(row_data, "getInstrument", None)
        if callable(getter):
            try:
                value = getter()
                if value:
                    text = str(value).strip()
                    if text:
                        return text
            except Exception:  # noqa: BLE001
                return None
        return None

    @staticmethod
    def _read_float(
        row_data: Any, fields: Sequence[str], methods: Sequence[str]
    ) -> Optional[float]:
        for name in fields:
            value = FXCMOfferSubscription._value_from_row(row_data, name)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        for method in methods:
            getter = getattr(row_data, method, None)
            if callable(getter):
                try:
                    value = getter()
                    if value is not None:
                        return float(value)  # type: ignore
                except Exception:  # noqa: BLE001
                    continue
        return None

    @staticmethod
    def _read_time(row_data: Any) -> Optional[dt.datetime]:
        candidates = ["Time", "time", "QuoteTime", "LastUpdate", "lastUpdate"]
        for name in candidates:
            value = FXCMOfferSubscription._value_from_row(row_data, name)
            dt_value = FXCMOfferSubscription._coerce_datetime(value)
            if dt_value is not None:
                return dt_value
        getter = getattr(row_data, "getTime", None)
        if callable(getter):
            try:
                return FXCMOfferSubscription._coerce_datetime(getter())
            except Exception:  # noqa: BLE001
                return None
        return None

    @staticmethod
    def _coerce_datetime(value: Any) -> Optional[dt.datetime]:
        if isinstance(value, dt.datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=dt.timezone.utc)
            return value
        if isinstance(value, (int, float)):
            ts = float(value)
            # У різних обгортках FXCM час може приходити як epoch-seconds або epoch-milliseconds.
            # Якщо помилково інтерпретувати ms як seconds, tick_ts стане “далеким майбутнім”,
            # і tick-agg ніколи не зможе закрити бари по clock-flush.
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                return dt.datetime.fromisoformat(text)
            except ValueError:
                return None
        return None

    @staticmethod
    def _value_from_row(row_data: Any, name: str) -> Any:
        if hasattr(row_data, name):
            value = getattr(row_data, name)
            if callable(value):
                try:
                    return value()
                except Exception:  # noqa: BLE001
                    return None
            return value
        return None


class PublishDataGate:
    """Відкидає дублікати барів та трекає останні timestamp-и."""

    def __init__(self) -> None:
        self._last_open: Dict[Tuple[str, str], int] = {}
        self._last_close: Dict[Tuple[str, str], int] = {}
        self._last_live_publish_ts: Dict[Tuple[str, str], float] = {}
        self._lock = threading.Lock()

        # Захист від “майбутніх” timestamp-ів: якщо один бар/тік прийшов з
        # некоректним часом, data gate може запам'ятати cutoff у майбутньому і
        # почати відкидати всі реальні complete=true бари, залишаючи лише live
        # complete=false оновлення (симптом: only_live_bars_no_complete).
        self._max_future_ms = 120_000

    def _key(self, symbol: str, timeframe: str) -> Tuple[str, str]:
        return (_normalize_symbol(symbol), _map_timeframe_label(timeframe))

    def seed(
        self,
        *,
        symbol: str,
        timeframe: str,
        last_open: Optional[int] = None,
        last_close: Optional[int] = None,
    ) -> None:
        key = self._key(symbol, timeframe)
        with self._lock:
            if last_open is not None:
                self._last_open[key] = int(last_open)
            if last_close is not None:
                self._last_close[key] = int(last_close)

    def filter_new_bars(
        self, df: pd.DataFrame, *, symbol: str, timeframe: str
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        key = self._key(symbol, timeframe)
        with self._lock:
            cutoff = self._last_open.get(key)
        if cutoff is None:
            return df

        now_ms = int(_now_utc().timestamp() * 1000.0)
        if cutoff > now_ms + self._max_future_ms:
            log.warning(
                "Data gate: cutoff у майбутньому для %s %s (cutoff=%d, now=%d). Скидаю gate.",
                key[0],
                key[1],
                cutoff,
                now_ms,
            )
            with self._lock:
                self._last_open.pop(key, None)
                self._last_close.pop(key, None)
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

        now_ms = int(_now_utc().timestamp() * 1000.0)
        if newest_open > now_ms + self._max_future_ms or newest_close > now_ms + self._max_future_ms:
            log.warning(
                "Data gate: ігнорую оновлення last_open/last_close у майбутньому для %s %s (open=%d close=%d now=%d).",
                key[0],
                key[1],
                newest_open,
                newest_close,
                now_ms,
            )
            return
        with self._lock:
            self._last_open[key] = newest_open
            self._last_close[key] = newest_close
        self._update_staleness_metric_for_key(key)

    def record_live_publish(
        self,
        *,
        symbol: str,
        timeframe: str,
        published_ts_seconds: float,
    ) -> None:
        key = self._key(symbol, timeframe)
        with self._lock:
            self._last_live_publish_ts[key] = float(published_ts_seconds)

    def live_age_seconds(self, *, symbol: str, timeframe: str) -> Optional[float]:
        key = self._key(symbol, timeframe)
        with self._lock:
            last_ts = self._last_live_publish_ts.get(key)
        if last_ts is None:
            return None
        return max(0.0, time.time() - float(last_ts))

    def last_close_ms(self, *, symbol: str, timeframe: str) -> Optional[int]:
        """Останній close_time (ms) для complete=true барів по ключу."""

        key = self._key(symbol, timeframe)
        with self._lock:
            value = self._last_close.get(key)
        return int(value) if value is not None else None

    def max_last_close_ms(self, targets: Sequence[Tuple[str, str]]) -> Optional[int]:
        """Максимальний close_time (ms) серед заданих stream targets."""

        candidates: List[int] = []
        for symbol, timeframe in targets:
            value = self.last_close_ms(symbol=symbol, timeframe=timeframe)
            if value is not None:
                candidates.append(int(value))
        return max(candidates) if candidates else None

    def staleness_seconds(self, *, symbol: str, timeframe: str) -> Optional[float]:
        key = self._key(symbol, timeframe)
        with self._lock:
            last_close = self._last_close.get(key)
        if last_close is None:
            return None
        now = _now_utc().timestamp()
        return max(0.0, now - last_close / 1000.0)

    def update_staleness_metrics(self) -> None:
        with self._lock:
            keys = list(self._last_close.keys())
        for key in keys:
            self._update_staleness_metric_for_key(key)

    def _update_staleness_metric_for_key(self, key: Tuple[str, str]) -> None:
        with self._lock:
            last_close = self._last_close.get(key)
        if last_close is None:
            return
        now = _now_utc().timestamp()
        age = max(0.0, now - last_close / 1000.0)
        PROM_STREAM_STALENESS_SECONDS.labels(symbol=key[0], tf=key[1]).set(age)
        PROM_STREAM_LAG_SECONDS.labels(symbol=key[0], tf=key[1]).set(age)
        PROM_STREAM_LAST_CLOSE_MS.labels(symbol=key[0], tf=key[1]).set(last_close)


class _SessionSymbolStats:
    def __init__(
        self,
        *,
        tag: str,
        timezone: str,
        session_open: dt.datetime,
        session_close: dt.datetime,
        symbol: str,
        timeframe: str,
    ) -> None:
        self.tag = tag
        self.timezone = timezone
        self.session_open_utc = session_open
        self.session_close_utc = session_close
        self.symbol = symbol
        self.timeframe = timeframe
        self.high: Optional[float] = None
        self.low: Optional[float] = None
        self.sum_close = 0.0
        self.bars = 0

    def update(self, *, high: float, low: float, close: float) -> None:
        if self.high is None or high > self.high:
            self.high = high
        if self.low is None or low < self.low:
            self.low = low
        self.sum_close += close
        self.bars += 1

    def to_symbol_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": self.symbol,
            "tf": self.timeframe,
            "bars": self.bars,
        }
        if self.high is not None and self.low is not None:
            payload["range"] = self.high - self.low
            payload["high"] = self.high
            payload["low"] = self.low
        if self.bars > 0:
            payload["avg"] = self.sum_close / self.bars
        return payload


class SessionStatsTracker:
    RETENTION_HOURS = 24

    def __init__(self) -> None:
        self._entries: Dict[Tuple[str, str], _SessionSymbolStats] = {}

    @staticmethod
    def _session_key(tag: str, session_open: dt.datetime) -> str:
        return f"{tag}:{int(session_open.timestamp())}"

    @staticmethod
    def _symbol_key(symbol: str, timeframe: str) -> str:
        return f"{_normalize_symbol(symbol)}:{timeframe}"

    def reset(self) -> None:
        self._entries.clear()

    def ingest(self, symbol: str, timeframe: str, df_bars: pd.DataFrame) -> None:
        if df_bars.empty:
            return
        records = cast(Sequence[Dict[str, Any]], df_bars.to_dict("records"))
        for record in records:
            close_time_raw = record.get("close_time")
            if close_time_raw is None:
                continue
            try:
                close_ts = _ms_to_dt(int(close_time_raw))
            except Exception:  # noqa: BLE001
                continue
            session_info = resolve_session(close_ts)
            if session_info is None:
                continue
            session_key = self._session_key(
                session_info.tag, session_info.session_open_utc
            )
            symbol_key = self._symbol_key(symbol, timeframe)
            entry_key = (session_key, symbol_key)
            entry = self._entries.get(entry_key)
            if entry is None or close_ts >= entry.session_close_utc:
                entry = _SessionSymbolStats(
                    tag=session_info.tag,
                    timezone=session_info.timezone,
                    session_open=session_info.session_open_utc,
                    session_close=session_info.session_close_utc,
                    symbol=_normalize_symbol(symbol),
                    timeframe=timeframe,
                )
                self._entries[entry_key] = entry
            high_raw = record.get("high")
            low_raw = record.get("low")
            close_raw = record.get("close")
            if high_raw is None or low_raw is None or close_raw is None:
                continue
            try:
                high_val = float(high_raw)
                low_val = float(low_raw)
                close_val = float(close_raw)
            except Exception:  # noqa: BLE001
                continue
            entry.update(high=high_val, low=low_val, close=close_val)
        self._prune()

    def snapshot(self) -> Dict[str, Any]:
        self._prune()
        result: Dict[str, Any] = {}
        for entry in self._entries.values():
            if entry.bars == 0:
                continue
            bucket = result.setdefault(
                entry.tag,
                {
                    "tag": entry.tag,
                    "timezone": entry.timezone,
                    "session_open_utc": entry.session_open_utc.replace(
                        microsecond=0
                    ).isoformat(),
                    "session_close_utc": entry.session_close_utc.replace(
                        microsecond=0
                    ).isoformat(),
                    "symbols": [],
                },
            )
            bucket.setdefault("symbols", []).append(entry.to_symbol_payload())
        return result

    def _prune(self) -> None:
        cutoff = _now_utc() - dt.timedelta(hours=self.RETENTION_HOURS)
        for key, entry in list(self._entries.items()):
            if entry.session_close_utc < cutoff:
                del self._entries[key]


def _stream_targets_summary(
    targets: Sequence[Tuple[str, str]],
    gate: Optional[PublishDataGate] = None,
) -> List[Dict[str, Any]]:
    summary: List[Dict[str, Any]] = []
    for symbol, tf_raw in targets:
        entry: Dict[str, Any] = {
            "symbol": symbol,
            "tf": tf_raw,
        }
        if gate is not None:
            staleness = gate.staleness_seconds(symbol=symbol, timeframe=tf_raw)
            if staleness is not None:
                entry["staleness_seconds"] = staleness
            live_age = gate.live_age_seconds(symbol=symbol, timeframe=tf_raw)
            if live_age is not None:
                entry["live_age_seconds"] = live_age
        summary.append(entry)
    return summary


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
    # Важливо: FXCM history може повернути поточний (ще не закритий) бар.
    # Оскільки ми синтетично обчислюємо close_time як open_time + tf - 1,
    # такий бар матиме close_time у майбутньому. Для warmup/cache це шкідливо
    # (SMC все одно потребує лише complete бари), тож відкидаємо його.
    now_ms = int(_now_utc().timestamp() * 1000)
    max_allowed = now_ms + MAX_FUTURE_DRIFT_SECONDS * 1000
    # Для close_time робимо значно жорсткіше вікно: інакше поточний (ще не
    # закритий) бар пройде валідацію, бо його close_time може бути на хвилини
    # попереду `now_ms` (особливо для 5m/15m і т.д.).
    close_max_allowed = now_ms + 10_000
    invalid_open = (ohlcv["open_time"] < MIN_ALLOWED_BAR_TIMESTAMP_MS) | (
        ohlcv["open_time"] > max_allowed
    )
    invalid_close = ohlcv["close_time"] > close_max_allowed
    invalid_mask = invalid_open | invalid_close
    if invalid_mask.any():
        dropped = int(invalid_mask.sum())
        dropped_open = int(invalid_open.sum())
        dropped_close_only = int((invalid_close & ~invalid_open).sum())

        if dropped_open > 0:
            if _should_log_drop_ts(symbol_norm, timeframe, "open_time"):
                log.warning(
                    "Відкинуто %d барів з аномальним timestamp для %s %s (open_time вікно %s → %s; close_time max=%s).",
                    dropped,
                    symbol,
                    timeframe,
                    MIN_ALLOWED_BAR_TIMESTAMP_MS,
                    max_allowed,
                    close_max_allowed,
                )
        elif dropped_close_only > 0:
            if _should_log_drop_ts(symbol_norm, timeframe, "close_time_future"):
                log.debug(
                    "Відкинуто %d незакритих history-барів для %s %s (close_time max=%s).",
                    dropped_close_only,
                    symbol,
                    timeframe,
                    close_max_allowed,
                )

        PROM_DROPPED_BARS.labels(symbol=symbol_norm, tf=timeframe).inc(dropped)
        ohlcv = ohlcv.loc[~invalid_mask]

    ohlcv = cast(pd.DataFrame, ohlcv.sort_values(by="open_time"))
    ohlcv = cast(pd.DataFrame, ohlcv.reset_index(drop=True))

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
            "Redis-клієнт версія: %s, %s:%s.",
            redis.__version__,
            settings.host,
            settings.port,
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
    channel: Optional[str] = None,
    data_gate: Optional[PublishDataGate] = None,
    min_publish_interval: float = 0.0,
    publish_rate_limit: Optional[Dict[Tuple[str, str], float]] = None,
    hmac_secret: Optional[str] = None,
    hmac_algo: str = "sha256",
    source: Optional[str] = None,
) -> bool:
    """Серіалізує OHLCV у JSON і публікує до Redis."""

    if redis_client is None:
        log.debug("Redis-клієнт не ініціалізовано — пропускаю публікацію.")
        return False

    if df_ohlcv.empty:
        log.debug("Немає OHLCV-барів для публікації.")
        return False

    df_to_publish = df_ohlcv
    if data_gate is not None:
        # Важливо для live complete=false:
        # - data gate трекає `open_time` і відкидає повтори;
        # - live-бар багато разів оновлюється з тим самим open_time;
        # - downstream (UDS/SMC) все одно ігнорує complete=false, але ці бари
        #   критичні для діагностики та UI.
        # Тому gate застосовуємо лише до complete-блоків.
        if "complete" in df_to_publish.columns:
            mask_complete = df_to_publish["complete"].fillna(False).astype(bool)
            df_complete = cast(pd.DataFrame, df_to_publish.loc[mask_complete])
            df_live = cast(pd.DataFrame, df_to_publish.loc[~mask_complete])
            df_complete_filtered = data_gate.filter_new_bars(
                df_complete, symbol=symbol, timeframe=timeframe
            )
            if df_complete_filtered.empty and df_live.empty:
                log.debug("Data gate: немає нових барів для %s %s.", symbol, timeframe)
                return False
            df_to_publish = pd.concat([df_complete_filtered, df_live], ignore_index=True)
        else:
            df_to_publish = data_gate.filter_new_bars(
                df_to_publish, symbol=symbol, timeframe=timeframe
            )
            if df_to_publish.empty:
                log.debug("Data gate: немає нових барів для %s %s.", symbol, timeframe)
                return False

    base_cols = [
        "open_time",
        "close_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    quality_cols = [
        "tick_count",
        "bar_range",
        "body_size",
        "upper_wick",
        "lower_wick",
        "avg_spread",
        "max_spread",
    ]
    include_quality = all(col in df_to_publish.columns for col in quality_cols)
    include_complete = "complete" in df_to_publish.columns
    include_synthetic = "synthetic" in df_to_publish.columns
    include_source = "source" in df_to_publish.columns
    include_tf = "tf" in df_to_publish.columns

    cols = list(base_cols)
    if include_quality:
        cols.extend(quality_cols)
    if include_complete:
        cols.append("complete")
    if include_synthetic:
        cols.append("synthetic")
    if include_source:
        cols.append("source")
    if include_tf:
        cols.append("tf")

    bars: List[Dict[str, Any]] = []
    for row in df_to_publish[cols].itertuples(index=False, name=None):
        # Порядок row відповідає `cols`.
        open_time = int(row[0])
        close_time = int(row[1])
        bar_payload: Dict[str, Any] = {
            "open_time": open_time,
            "close_time": close_time,
            "open": float(row[2]),
            "high": float(row[3]),
            "low": float(row[4]),
            "close": float(row[5]),
            "volume": float(row[6]),
        }
        idx = len(base_cols)
        if include_quality:
            bar_payload["tick_count"] = int(row[idx])
            idx += 1
            bar_payload["bar_range"] = float(row[idx])
            idx += 1
            bar_payload["body_size"] = float(row[idx])
            idx += 1
            bar_payload["upper_wick"] = float(row[idx])
            idx += 1
            bar_payload["lower_wick"] = float(row[idx])
            idx += 1
            bar_payload["avg_spread"] = float(row[idx])
            idx += 1
            bar_payload["max_spread"] = float(row[idx])
            idx += 1
        if include_complete:
            bar_payload["complete"] = bool(row[idx])
            idx += 1
        if include_synthetic:
            bar_payload["synthetic"] = bool(row[idx])
            idx += 1
        if include_source:
            bar_payload["source"] = str(row[idx])
            idx += 1
        if include_tf:
            bar_payload["tf"] = str(row[idx])
        bars.append(bar_payload)

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
    if source:
        payload["source"] = str(source)
    if hmac_secret:
        payload["sig"] = compute_payload_hmac(
            base_payload,
            hmac_secret,
            hmac_algo,
        )

    try:
        validate_ohlcv_payload_contract(payload)
    except ValueError as exc:
        PROM_ERROR_COUNTER.labels(type="contract").inc()
        log.error("Порушено контракт fxcm:ohlcv — публікацію скасовано: %s", exc)
        return False

    message = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    target_channel = (channel or "").strip() or REDIS_CHANNEL

    try:
        redis_client.publish(target_channel, message)
        if publish_rate_limit is not None:
            publish_rate_limit[rate_limit_key] = time.monotonic()
        log.debug(
            "Опубліковано %d барів у Redis канал %s (%s %s).",
            len(bars),
            target_channel,
            symbol_norm,
            timeframe,
        )
        PROM_BARS_PUBLISHED.labels(symbol=symbol_norm, tf=timeframe).inc(len(bars))
        if bars:
            now_publish_ts = time.time()
            # Метрики/стан мають відображати реальність:
            # - staleness/last_close_ms/lag — тільки по complete=true барах;
            # - live complete=false — окремо, для діагностики.
            if "complete" in df_to_publish.columns:
                mask_complete = df_to_publish["complete"].fillna(False).astype(bool)
                df_complete_only = cast(pd.DataFrame, df_to_publish.loc[mask_complete])
                df_live_only = cast(pd.DataFrame, df_to_publish.loc[~mask_complete])
                if not df_live_only.empty:
                    PROM_OHLCV_LIVE_LAST_TS_SECONDS.labels(
                        symbol=symbol_norm, tf=timeframe
                    ).set(now_publish_ts)
                    if data_gate is not None:
                        data_gate.record_live_publish(
                            symbol=symbol,
                            timeframe=timeframe,
                            published_ts_seconds=now_publish_ts,
                        )
                if not df_complete_only.empty:
                    last_close_sec = float(df_complete_only["close_time"].max()) / 1000.0
                    lag = max(0.0, _now_utc().timestamp() - last_close_sec)
                    PROM_STREAM_LAG_SECONDS.labels(symbol=symbol_norm, tf=timeframe).set(lag)
                    PROM_OHLCV_COMPLETE_LAST_TS_SECONDS.labels(
                        symbol=symbol_norm, tf=timeframe
                    ).set(now_publish_ts)
                    if data_gate is not None:
                        data_gate.record_publish(
                            df_complete_only, symbol=symbol, timeframe=timeframe
                        )
            else:
                last_close_sec = bars[-1]["close_time"] / 1000.0
                lag = max(0.0, _now_utc().timestamp() - last_close_sec)
                PROM_STREAM_LAG_SECONDS.labels(symbol=symbol_norm, tf=timeframe).set(lag)
                PROM_OHLCV_COMPLETE_LAST_TS_SECONDS.labels(
                    symbol=symbol_norm, tf=timeframe
                ).set(now_publish_ts)
                if data_gate is not None:
                    data_gate.record_publish(
                        df_to_publish, symbol=symbol, timeframe=timeframe
                    )
        return True
    except Exception as exc:  # noqa: BLE001
        raise RedisRetryableError("Не вдалося надіслати OHLCV у Redis") from exc


def _publish_price_snapshot(
    redis_client: Optional[Any],
    *,
    channel: str,
    snapshot: PriceTickSnap,
) -> None:
    if redis_client is None:
        raise RedisRetryableError("Redis недоступний для price snapshot")

    payload = {
        "symbol": snapshot.symbol,
        "bid": snapshot.bid,
        "ask": snapshot.ask,
        "mid": snapshot.mid,
        "tick_ts": snapshot.tick_ts,
        "snap_ts": snapshot.snap_ts,
    }

    validate_price_tik_payload_contract(payload)

    message = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    try:
        redis_client.publish(channel, message)
    except Exception as exc:  # noqa: BLE001
        raise RedisRetryableError(
            "Не вдалося надіслати price snapshot у Redis"
        ) from exc


def _fetch_and_publish_recent(
    fx: ForexConnect,
    *,
    symbol: str,
    timeframe_raw: str,
    redis_client: Optional[Any],
    ohlcv_channel: Optional[str] = None,
    lookback_minutes: int,
    last_open_time_ms: Dict[Tuple[str, str], int],
    data_gate: Optional[PublishDataGate] = None,
    min_publish_interval: float = 0.0,
    publish_rate_limit: Optional[Dict[Tuple[str, str], float]] = None,
    hmac_secret: Optional[str] = None,
    hmac_algo: str = "sha256",
    session_stats: Optional[SessionStatsTracker] = None,
    ohlcv_sink: Optional[Callable[[OhlcvBatch], None]] = None,
    market_status_sink: Optional[Callable[[MarketStatusEvent], None]] = None,
    allow_calendar_closed: bool = False,
) -> pd.DataFrame:
    """Витягує останні `lookback_minutes` і публікує лише нові бари.

    Якщо ринок закритий або FXCM повертає технічну помилку, повертає
    порожній DataFrame без генерації noisу у логах.
    """

    end_dt = _now_utc()
    if not allow_calendar_closed and not is_trading_time(end_dt):
        next_open = _notify_market_closed(
            end_dt, redis_client if market_status_sink is None else None
        )
        if market_status_sink is not None:
            try:
                market_status_sink(
                    MarketStatusEvent(state="closed", next_open=next_open)
                )
            except Exception:  # noqa: BLE001
                log.exception("Не вдалося передати market_status sink (closed).")
        raise MarketTemporarilyClosed("Ринок закритий")

    start_dt = end_dt - dt.timedelta(minutes=max(lookback_minutes, 1))
    timeframe_norm = _map_timeframe_label(timeframe_raw)
    timeframe_fxcm = _to_fxcm_timeframe(timeframe_norm)

    try:
        history = fx.get_history(symbol, timeframe_fxcm, start_dt, end_dt)
    except Exception as exc:  # noqa: BLE001
        if _is_price_history_not_ready(exc):
            PROM_PRICE_HISTORY_NOT_READY.inc()
            next_open = _notify_market_closed(
                end_dt, redis_client if market_status_sink is None else None
            )
            if market_status_sink is not None:
                try:
                    market_status_sink(
                        MarketStatusEvent(state="closed", next_open=next_open)
                    )
                except Exception:  # noqa: BLE001
                    log.exception(
                        "Не вдалося передати market_status sink (price history not ready)."
                    )
            log.debug(
                "Стрім: FXCM PriceHistoryCommunicator не готовий для %s (%s).",
                symbol,
                timeframe_fxcm,
            )
            raise MarketTemporarilyClosed("PriceHistoryCommunicator is not ready")
        raise FXCMRetryableError(
            f"FXCM get_history помилився для {symbol} ({timeframe_fxcm})"
        ) from exc

    df_raw = pd.DataFrame(history)
    if df_raw.empty:
        log.debug(
            "Стрім: FXCM повернув порожні дані для %s (%s) у вікні %s хв.",
            symbol,
            timeframe_fxcm,
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

    if session_stats is not None:
        session_stats.ingest(symbol, timeframe_norm, df_to_publish)

    if redis_client is None and ohlcv_sink is None:
        if data_gate is not None:
            data_gate.record_publish(
                df_to_publish, symbol=symbol, timeframe=timeframe_norm
            )
        newest = int(df_to_publish["open_time"].max())
        last_open_time_ms[key] = newest
        return df_to_publish

    publish_ok = True
    if ohlcv_sink is not None:
        batch = OhlcvBatch(
            symbol=_normalize_symbol(symbol),
            timeframe=timeframe_norm,
            data=df_to_publish.copy(deep=False),
            fetched_at=time.time(),
            source="stream",
        )
        try:
            ohlcv_sink(batch)
        except Exception:  # noqa: BLE001
            publish_ok = False
            log.exception("Не вдалося передати OhlcvBatch у sink.")
    else:
        publish_ok = publish_ohlcv_to_redis(
            df_to_publish,
            symbol=symbol,
            timeframe=timeframe_norm,
            redis_client=redis_client,
            channel=ohlcv_channel,
            data_gate=data_gate,
            min_publish_interval=min_publish_interval,
            publish_rate_limit=publish_rate_limit,
            hmac_secret=hmac_secret,
            hmac_algo=hmac_algo,
        )
        if not publish_ok:
            return pd.DataFrame()
    if not publish_ok:
        return pd.DataFrame()

    newest = int(df_to_publish["open_time"].max())
    last_open_time_ms[key] = newest
    if market_status_sink is not None:
        try:
            market_status_sink(MarketStatusEvent(state="open", next_open=None))
        except Exception:  # noqa: BLE001
            log.exception("Не вдалося передати market_status sink (open).")
    else:
        if allow_calendar_closed:
            _publish_market_status(redis_client, "open")
        else:
            _sync_market_status_with_calendar(redis_client)
    return df_to_publish


def run_redis_healthcheck(redis_client: Optional[Any], *, ohlcv_channel: Optional[str] = None) -> bool:
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
        target_channel = (ohlcv_channel or "").strip() or REDIS_CHANNEL
        redis_client.publish(target_channel, probe_payload)
        publish_ok = True
    except Exception as exc:  # noqa: BLE001
        log.exception("Redis health-check: publish неуспішний: %s", exc)

    if ping_ok and publish_ok:
        log.debug("Redis health-check: OK (ping + publish).")
    else:
        log.error(
            "Redis health-check: FAILED (ping=%s, publish=%s).", ping_ok, publish_ok
        )

    return ping_ok and publish_ok


def fetch_history_sample(
    fx: ForexConnect,
    *,
    redis_client: Optional[Any],
    sample_settings: SampleRequestSettings,
    heartbeat_channel: Optional[str] = None,
    ohlcv_channel: Optional[str] = None,
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
    timeframe_fxcm = _to_fxcm_timeframe(timeframe)
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
        timeframe_fxcm,
        start_dt,
        end_dt,
    )

    try:
        history = fx.get_history(symbol, timeframe_fxcm, start_dt, end_dt)
    except Exception as exc:  # noqa: BLE001
        if _is_price_history_not_ready(exc):
            PROM_PRICE_HISTORY_NOT_READY.inc()
            _notify_market_closed(end_dt, redis_client)
            log.debug(
                "Warmup: FXCM PriceHistoryCommunicator не готовий для %s (%s).",
                symbol,
                timeframe_fxcm,
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
            channel=ohlcv_channel,
            data_gate=data_gate,
            hmac_secret=hmac_secret,
            hmac_algo=hmac_algo,
            source="stream",
        )
    except RedisRetryableError as exc:
        PROM_ERROR_COUNTER.labels(type="redis").inc()
        log.error("Warmup-публікація OHLCV у Redis неуспішна: %s", exc)
        return False

    if publish_ok:
        log.info("Warmup-пакет успішно опубліковано у Redis.")
        _sync_market_status_with_calendar(redis_client)
        last_close_ms = int(df_ohlcv["close_time"].max())
        lag_seconds = _calc_lag_seconds(last_close_ms)
        heartbeat_context: Dict[str, Any] = {
            "channel": heartbeat_channel,
            "mode": "warmup_sample",
            "redis_connected": redis_client is not None,
            "redis_channel": (ohlcv_channel or "").strip() or REDIS_CHANNEL,
            "stream_targets": _stream_targets_summary([(symbol, timeframe)], None),
            "published_bars": len(df_ohlcv),
        }
        if lag_seconds is not None:
            heartbeat_context["lag_seconds"] = lag_seconds
        heartbeat_context["session"] = _build_session_context(next_open=None)
        _publish_heartbeat(
            redis_client,
            heartbeat_channel,
            state="warmup",  # sample-mode heartbeat
            last_bar_close_ms=last_close_ms,
            context=heartbeat_context,
        )
    else:
        log.error("Warmup-публікація OHLCV у Redis не підтверджена.")

    return publish_ok


def _attach_price_stream_context(
    context: Dict[str, Any],
    price_stream: Optional[PriceSnapshotWorker],
    *,
    metadata: Optional[Dict[str, Any]] = None,
    cadence_snapshot: Optional[Dict[str, Any]] = None,
) -> None:
    if price_stream is None:
        return
    if metadata is None:
        metadata = price_stream.snapshot_metadata()
    else:
        metadata = dict(metadata)
    if not metadata:
        return
    if cadence_snapshot:
        metadata = dict(metadata)
        metadata["adaptive_cadence"] = cadence_snapshot
    context["price_stream"] = metadata
    silence = metadata.get("tick_silence_seconds")
    if silence is not None:
        context.setdefault("tick_silence_seconds", silence)


def _attach_tick_agg_context(
    context: Dict[str, Any],
    tick_ohlcv_worker: Optional["TickOhlcvWorker"],
) -> None:
    if tick_ohlcv_worker is None:
        return
    try:
        snapshot = tick_ohlcv_worker.snapshot_metadata()
    except Exception:  # noqa: BLE001 - діагностика не критична
        return
    if snapshot:
        context["tick_agg"] = snapshot


def _attach_supervisor_context(
    context: Dict[str, Any],
    supervisor: Optional["AsyncStreamSupervisor"],
) -> None:
    if supervisor is None:
        return
    try:
        snapshot = supervisor.diagnostics_snapshot()
    except Exception:  # noqa: BLE001 - діагностика не критична
        return
    if snapshot:
        context["supervisor"] = snapshot
        context["async_supervisor"] = snapshot


def _attach_history_quota_context(
    context: Dict[str, Any], quota: Optional[HistoryQuota]
) -> None:
    if quota is None:
        return
    context["history"] = quota.snapshot()


def _attach_backoff_context(
    context: Dict[str, Any],
    *,
    history_backoff: Optional[FxcmBackoffController],
    redis_backoff: Optional[FxcmBackoffController],
) -> None:
    payload: Dict[str, Any] = {}
    if history_backoff is not None:
        payload["fxcm_history"] = history_backoff.snapshot()
    if redis_backoff is not None:
        payload["redis"] = redis_backoff.snapshot()
    if not payload:
        return
    diag = context.setdefault("diag", {})
    diag["backoff"] = payload


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
    price_stream: Optional[PriceSnapshotWorker] = None,
    ohlcv_sink: Optional[Callable[[OhlcvBatch], None]] = None,
    heartbeat_sink: Optional[Callable[[HeartbeatEvent], None]] = None,
    market_status_sink: Optional[Callable[[MarketStatusEvent], None]] = None,
    async_supervisor: Optional["AsyncStreamSupervisor"] = None,
    history_quota: Optional[HistoryQuota] = None,
    fx_session_observer: Optional[Callable[[ForexConnect], None]] = None,
    history_backoff: Optional[FxcmBackoffController] = None,
    redis_backoff: Optional[FxcmBackoffController] = None,
    tick_cadence: Optional[TickCadenceController] = None,
    tick_aggregation_enabled: bool = False,
    tick_aggregation_timeframes: Optional[Sequence[str]] = None,
    mtf_crosscheck: Optional[Any] = None,
    status_bar: Optional[ConsoleStatusBar] = None,
    universe: Optional[StreamUniverse] = None,
    universe_update_callback: Optional[Callable[[List[Tuple[str, str]]], None]] = None,
    tick_ohlcv_worker: Optional["TickOhlcvWorker"] = None,
) -> None:
    """Безкінечний цикл стрімінгу OHLCV у Redis.

    `poll_seconds` контролює частоту звернення до FXCM, а
    `publish_interval_seconds` — мінімальний інтервал між публікаціями свічок у Redis.
    Якщо `price_stream` задано, heartbeat збагачується метаданими про снепшоти тика.
    """

    if require_redis and redis_client is None:
        log.error("Стрім неможливий без Redis-клієнта.")
        return

    gate = data_gate or PublishDataGate()
    session_stats_tracker = SessionStatsTracker()
    global _SESSION_STATS_TRACKER
    _SESSION_STATS_TRACKER = session_stats_tracker

    current_fx = fx
    current_redis = redis_client

    def _notify_fx_session_observer(target_fx: ForexConnect) -> None:
        if fx_session_observer is None:
            return
        try:
            fx_session_observer(target_fx)
        except Exception:  # noqa: BLE001
            log.exception("Не вдалося оновити FXCM session observer після reconnect.")

    _notify_fx_session_observer(current_fx)

    def _emit_market_status_event(state: str, next_open: Optional[dt.datetime]) -> None:
        event = MarketStatusEvent(state=state, next_open=next_open)
        if market_status_sink is not None:
            try:
                market_status_sink(event)
            except Exception:  # noqa: BLE001
                log.exception("Не вдалося передати market_status sink.")
        else:
            ok = _publish_market_status(current_redis, state, next_open=next_open)
            if not ok:
                if current_redis is None:
                    return
                if redis_backoff is not None:
                    redis_backoff.fail("redis_market_status")
                raise RedisRetryableError("Публікація market_status неуспішна")
            if redis_backoff is not None:
                redis_backoff.success()

    def _emit_heartbeat_event(
        *,
        state: str,
        last_bar_close_ms: Optional[int],
        context: Dict[str, Any],
        next_open: Optional[dt.datetime] = None,
        sleep_seconds: Optional[float] = None,
    ) -> None:
        event = HeartbeatEvent(
            state=state,
            last_bar_close_ms=last_bar_close_ms,
            next_open=next_open,
            sleep_seconds=sleep_seconds,
            context=context,
        )
        if heartbeat_sink is not None:
            try:
                heartbeat_sink(event)
            except Exception:  # noqa: BLE001
                log.exception("Не вдалося передати heartbeat у sink.")
        else:
            ok = _publish_heartbeat(
                current_redis,
                heartbeat_channel,
                state=state,
                last_bar_close_ms=last_bar_close_ms,
                next_open=next_open,
                sleep_seconds=sleep_seconds,
                context=context,
            )
            if not ok:
                if current_redis is None or not heartbeat_channel:
                    return
                if redis_backoff is not None:
                    redis_backoff.fail("redis_heartbeat")
                raise RedisRetryableError("Публікація heartbeat неуспішна")
            if redis_backoff is not None:
                redis_backoff.success()

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
                if (
                    last_published_close_ms is None
                    or cached_close > last_published_close_ms
                ):
                    last_published_close_ms = cached_close
    publish_rate_limit: Dict[Tuple[str, str], float] = {}
    throttle_log_state: Dict[Tuple[str, str, str], float] = {}

    # MTF: локальні агрегатори для `complete=true` барів (history_agg).
    mtf_aggregators: Dict[Tuple[str, str], HistoryMtfCompleteAggregator] = {}
    mtf_crosscheck_ctrl: Optional[MtfCrosscheckController] = (
        MtfCrosscheckController(mtf_crosscheck)
        if mtf_crosscheck is not None and bool(getattr(mtf_crosscheck, "enabled", False))
        else None
    )

    universe_active_symbols_count: Optional[int] = None
    universe_last_apply_ts: Optional[float] = None

    def _should_log_throttle(
        symbol_norm: str, timeframe_norm: str, reason: str
    ) -> bool:
        now_log = time.monotonic()
        key = (symbol_norm, timeframe_norm, reason)
        last_logged = throttle_log_state.get(key)
        if (
            last_logged is not None
            and now_log - last_logged < THROTTLE_LOG_INTERVAL_SECONDS
        ):
            return False
        throttle_log_state[key] = now_log
        return True

    ohlcv_sink_effective = ohlcv_sink
    if ohlcv_sink is not None and publish_interval_seconds > 0:
        min_interval = float(publish_interval_seconds)

        def _rate_limited_sink(
            batch: OhlcvBatch, *, _sink: Callable[[OhlcvBatch], None] = ohlcv_sink
        ) -> None:
            key = (batch.symbol, batch.timeframe)
            last_ts = publish_rate_limit.get(key)
            now_ts = time.monotonic()
            if last_ts is not None and now_ts - last_ts < min_interval:
                time.sleep(min_interval - (now_ts - last_ts))
            publish_rate_limit[key] = time.monotonic()
            _sink(batch)

        ohlcv_sink_effective = _rate_limited_sink
    log.debug(
        "Запуск циклу конектора FXCM: %s, інтервал опитування %ss, вікно %s хв.",
        ", ".join(f"{sym} {tf}" for sym, tf in config),
        poll_seconds,
        lookback_minutes,
    )

    cycles = 0
    last_idle_log_ts: Optional[float] = None
    last_idle_next_open: Optional[dt.datetime] = None
    last_ticks_alive: Optional[bool] = None

    try:
        while True:
            if universe is not None and universe.consume_dirty():
                try:
                    config = universe.get_effective_targets()
                    if universe_update_callback is not None:
                        universe_update_callback(list(config))
                    universe_active_symbols_count = len(
                        {str(sym) for sym, _ in config if sym}
                    )
                    universe_last_apply_ts = time.time()
                    PROM_UNIVERSE_APPLY_TOTAL.labels(result="ok").inc()
                    PROM_UNIVERSE_ACTIVE_SYMBOLS.set(
                        float(universe_active_symbols_count)
                    )
                    PROM_UNIVERSE_LAST_APPLY_TS_SECONDS.set(
                        float(universe_last_apply_ts)
                    )
                except Exception:  # noqa: BLE001
                    log.exception("Dynamic universe: не вдалося застосувати оновлення")
                    PROM_UNIVERSE_APPLY_TOTAL.labels(result="error").inc()

            cycle_start = time.monotonic()
            if mtf_crosscheck_ctrl is not None:
                mtf_crosscheck_ctrl.begin_cycle()
            restart_cycle = False
            market_pause = False
            closed_reference: Optional[dt.datetime] = None
            idle_reason: Optional[str] = None
            cycle_published_bars = 0
            now = _now_utc()
            calendar_open = is_trading_time(now)
            price_stream_metadata = (
                price_stream.snapshot_metadata() if price_stream is not None else None
            )
            ticks_alive = _tick_stream_alive(
                price_stream_metadata, tick_cadence=tick_cadence
            )
            market_open = calendar_open or ticks_alive

            tick_silence_seconds = None
            if isinstance(price_stream_metadata, Mapping):
                tick_silence_seconds = _float_or_none(
                    price_stream_metadata.get("tick_silence_seconds")
                )

            PROM_CALENDAR_OPEN.set(1 if calendar_open else 0)
            PROM_TICK_STREAM_ALIVE.set(1 if ticks_alive else 0)
            PROM_EFFECTIVE_MARKET_OPEN.set(1 if market_open else 0)
            if ticks_alive and not calendar_open:
                PROM_CALENDAR_CLOSED_TICKS_ALIVE.inc()
            cadence_snapshot: Optional[Dict[str, Any]] = None
            if tick_cadence is not None:
                tick_cadence.update_state(
                    tick_metadata=price_stream_metadata,
                    market_open=market_open,
                    now_monotonic=cycle_start,
                )
                cadence_snapshot = tick_cadence.snapshot()
            history_backoff_remaining = (
                history_backoff.remaining() if history_backoff is not None else 0.0
            )
            history_paused_by_backoff = history_backoff_remaining > 0.0
            redis_backoff_remaining = (
                redis_backoff.remaining() if redis_backoff is not None else 0.0
            )
            if not market_open:
                market_pause = True
                closed_reference = now
                if last_ticks_alive:
                    idle_reason = "ticks_stopped_after_calendar_close"
                else:
                    idle_reason = "calendar_closed"
            else:
                history_call_attempted = False
                cadence_allowances: Dict[str, bool] = {}
                # MTF: з 1m history будуємо старші TF локально.
                # Тримаємо агрегатори по (symbol_norm, target_tf).
                mtf_targets_by_symbol: Dict[str, Set[str]] = {}
                for symbol_raw, tf_raw in config:
                    sym_norm = _normalize_symbol(symbol_raw)
                    tf_norm = _map_timeframe_label(tf_raw)
                    mtf_targets_by_symbol.setdefault(sym_norm, set()).add(tf_norm)
                # Якщо tick-agg внутрішньо вже закрив бар, але history ще не
                # опублікував complete=true (з Volume), форсуємо history poll
                # якнайшвидше для відповідних таргетів.
                forced_history_targets: Set[Tuple[str, str]] = set()
                if tick_ohlcv_worker is not None and tick_aggregation_enabled:
                    try:
                        tick_meta = tick_ohlcv_worker.snapshot_metadata()
                        last_complete = tick_meta.get("last_complete_close_ms")
                        if isinstance(last_complete, Mapping):
                            for symbol_raw, _tf_raw in config:
                                # History підтягуємо лише для 1m.
                                tf_norm = BASE_HISTORY_TF_NORM
                                symbol_norm = _normalize_symbol(symbol_raw)
                                sym_map = last_complete.get(symbol_norm)
                                if not isinstance(sym_map, Mapping):
                                    continue
                                tick_close_ms = sym_map.get(tf_norm)
                                if tick_close_ms is None:
                                    continue
                                published_close_ms = gate.last_close_ms(
                                    symbol=symbol_raw, timeframe=BASE_HISTORY_TF_RAW
                                )
                                if (
                                    published_close_ms is None
                                    or int(tick_close_ms) > int(published_close_ms)
                                ):
                                    forced_history_targets.add((symbol_raw, BASE_HISTORY_TF_RAW))
                    except Exception:  # noqa: BLE001
                        # Діагностика не має ламати стрім.
                        pass

                if history_paused_by_backoff:
                    idle_reason = "fxcm_backoff"
                else:
                    # Спочатку пробуємо таргети, де tick-agg уже закрив бар,
                    # щоб швидше підтягнути complete=true з history.
                    # Poll history лише по одному разу на символ (1m).
                    ordered_targets: List[Tuple[str, str]] = []
                    seen_symbols: Set[str] = set()
                    for symbol_raw, _tf_raw in config:
                        if symbol_raw in seen_symbols:
                            continue
                        seen_symbols.add(symbol_raw)
                        ordered_targets.append((symbol_raw, BASE_HISTORY_TF_RAW))

                    if forced_history_targets:
                        ordered_targets.sort(
                            key=lambda item: 0 if item in forced_history_targets else 1
                        )

                    for symbol, tf_raw in ordered_targets:
                        tf_norm = BASE_HISTORY_TF_NORM
                        if tick_cadence is not None:
                            allow_cadence = cadence_allowances.get(tf_norm)
                            if allow_cadence is None:
                                allow_cadence, _ = tick_cadence.should_poll(tf_norm)
                                cadence_allowances[tf_norm] = allow_cadence
                            # Якщо tick-agg вже закрив бар — cadence не має
                            # затримувати підтягування history для complete=true.
                            if not allow_cadence and (symbol, tf_raw) not in forced_history_targets:
                                continue
                        call_started_ts: Optional[float] = None
                        deny_reason = ""
                        if history_quota is not None:
                            call_started_ts = time.monotonic()
                            allowed, next_slot, deny_reason = history_quota.allow_call(
                                symbol,
                                tf_norm,
                                call_started_ts,
                            )
                            if not allowed:
                                history_quota.register_skip(symbol, tf_norm)
                                symbol_norm = _normalize_symbol(symbol)
                                if _should_log_throttle(
                                    symbol_norm, tf_norm, deny_reason
                                ):
                                    logger = (
                                        log.info
                                        if deny_reason in _INFO_THROTTLE_REASONS
                                        else log.warning
                                    )
                                    logger(
                                        "FXCM history throttle (%s): %s %s, наступний слот через %.1f с.",
                                        deny_reason,
                                        symbol,
                                        tf_raw,
                                        next_slot,
                                    )
                                continue
                        history_call_attempted = True
                        try:
                            # Для «форсованих» таргетів не чекаємо publish_interval:
                            # якщо history вже має complete бар — публікуємо відразу.
                            min_publish_interval_effective = (
                                0.0
                                if (symbol, tf_raw) in forced_history_targets
                                else publish_interval_seconds
                            )
                            df_new = _fetch_and_publish_recent(
                                current_fx,
                                symbol=symbol,
                                timeframe_raw=tf_raw,
                                redis_client=current_redis,
                                ohlcv_channel=_OHLCV_CHANNEL,
                                lookback_minutes=lookback_minutes,
                                last_open_time_ms=last_open_time_ms,
                                data_gate=gate,
                                min_publish_interval=min_publish_interval_effective,
                                publish_rate_limit=publish_rate_limit,
                                hmac_secret=hmac_secret,
                                hmac_algo=hmac_algo,
                                session_stats=session_stats_tracker,
                                ohlcv_sink=ohlcv_sink_effective,
                                market_status_sink=market_status_sink,
                                allow_calendar_closed=ticks_alive,
                            )
                        except MarketTemporarilyClosed as exc:
                            log.info("Стрім: ринок недоступний (%s)", exc)
                            market_pause = True
                            closed_reference = _now_utc()
                            idle_reason = "fxcm_temporarily_unavailable"
                            break
                        except FXCMRetryableError as exc:
                            log.warning("Втрачено з'єднання з FXCM: %s", exc)
                            PROM_ERROR_COUNTER.labels(type="fxcm").inc()
                            if history_backoff is not None:
                                history_backoff.fail("fxcm_history")
                            if fx_reconnector is None:
                                raise
                            _close_fxcm_session(current_fx, announce=False)
                            current_fx = fx_reconnector()
                            _notify_fx_session_observer(current_fx)
                            restart_cycle = True
                            break
                        except RedisRetryableError as exc:
                            log.warning("Помилка Redis під час публікації: %s", exc)
                            PROM_ERROR_COUNTER.labels(type="redis").inc()
                            if redis_backoff is not None:
                                redis_backoff.fail("redis_publish")
                            if redis_reconnector is None:
                                raise
                            current_redis = redis_reconnector()
                            restart_cycle = True
                            break
                        finally:
                            if (
                                history_quota is not None
                                and call_started_ts is not None
                            ):
                                history_quota.record_call(
                                    symbol, tf_norm, call_started_ts
                                )

                        if df_new.empty:
                            continue

                        # Після оновлення 1m history — агрегуємо complete бари у старші TF.
                        try:
                            symbol_norm = _normalize_symbol(symbol)
                            desired_tfs = mtf_targets_by_symbol.get(symbol_norm, set())
                            for target_tf in sorted(desired_tfs):
                                if target_tf == BASE_HISTORY_TF_NORM:
                                    continue
                                key = (symbol_norm, str(target_tf))
                                agg = mtf_aggregators.get(key)
                                if agg is None:
                                    agg = HistoryMtfCompleteAggregator(
                                        symbol_norm=symbol_norm,
                                        target_tf=str(target_tf),
                                    )
                                    mtf_aggregators[key] = agg
                                df_agg = agg.ingest_df_1m(df_new)
                                if df_agg is None or df_agg.empty:
                                    continue
                                if session_stats_tracker is not None:
                                    session_stats_tracker.ingest(
                                        symbol, str(target_tf), df_agg
                                    )
                                if ohlcv_sink_effective is not None:
                                    batch = OhlcvBatch(
                                        symbol=_normalize_symbol(symbol),
                                        timeframe=str(target_tf),
                                        data=df_agg.copy(deep=False),
                                        fetched_at=time.time(),
                                        source=HISTORY_AGG_SOURCE,
                                    )
                                    ohlcv_sink_effective(batch)
                                else:
                                    publish_ohlcv_to_redis(
                                        df_agg,
                                        symbol=symbol,
                                        timeframe=str(target_tf),
                                        redis_client=current_redis,
                                        channel=_OHLCV_CHANNEL,
                                        data_gate=gate,
                                        min_publish_interval=publish_interval_seconds,
                                        publish_rate_limit=publish_rate_limit,
                                        hmac_secret=hmac_secret,
                                        hmac_algo=hmac_algo,
                                        source=HISTORY_AGG_SOURCE,
                                    )

                                if mtf_crosscheck_ctrl is not None:
                                    mtf_crosscheck_ctrl.maybe_check_from_agg_df(
                                        current_fx,
                                        symbol_raw=symbol,
                                        target_tf=str(target_tf),
                                        df_agg=df_agg,
                                    )
                        except Exception:  # noqa: BLE001
                            # MTF-агрегація не повинна ламати базовий стрім.
                            pass

                        # Калібруємо preview volume під FXCM history: зіставляємо
                        # history Volume із tick_count з tick-agg (по open_time).
                        if tick_ohlcv_worker is not None and tick_aggregation_enabled:
                            try:
                                symbol_norm = _normalize_symbol(symbol)
                                tf_norm = _map_timeframe_label(tf_raw)
                                for row in df_new.itertuples(index=False):
                                    open_time = int(getattr(row, "open_time"))
                                    history_volume = float(getattr(row, "volume"))
                                    tick_count = tick_ohlcv_worker.get_closed_tick_count(
                                        symbol_norm=symbol_norm,
                                        tf_norm=tf_norm,
                                        open_time_ms=open_time,
                                    )
                                    if tick_count is None:
                                        continue
                                    _VOLUME_CALIBRATOR.update_from_history_bar(
                                        symbol_norm=symbol_norm,
                                        tf_norm=tf_norm,
                                        open_time_ms=open_time,
                                        history_volume=history_volume,
                                        tick_count=int(tick_count),
                                    )
                            except Exception:  # noqa: BLE001
                                pass

                        log.info(
                            "Стрім: %s %s → %d нових барів",
                            symbol,
                            tf_raw,
                            len(df_new),
                        )
                        last_published_close_ms = int(df_new["close_time"].max())
                        cycle_published_bars += len(df_new)
                        if cache_manager is not None:
                            cache_manager.append_stream_bars(
                                symbol_raw=symbol,
                                timeframe_raw=tf_raw,
                                df_new=df_new,
                            )

                if (
                    history_backoff is not None
                    and history_call_attempted
                    and not restart_cycle
                    and not market_pause
                ):
                    history_backoff.success()
                    history_backoff_remaining = history_backoff.remaining()

            if (
                not restart_cycle
                and redis_backoff is not None
                and async_supervisor is None
            ):
                redis_backoff.success()

            if history_backoff is not None:
                history_backoff_remaining = history_backoff.remaining()
                history_paused_by_backoff = history_backoff_remaining > 0.0
            if redis_backoff is not None:
                redis_backoff_remaining = redis_backoff.remaining()

            if restart_cycle:
                gate.update_staleness_metrics()
                continue

            cadence_sleep_seconds: Optional[float] = None
            if tick_cadence is not None:
                cadence_sleep_seconds = tick_cadence.next_wakeup_in_seconds()
            elapsed = time.monotonic() - cycle_start
            if market_pause:
                reference_time = closed_reference or _now_utc()
                next_open = _notify_market_closed(
                    reference_time,
                    current_redis if market_status_sink is None else None,
                )
                if market_status_sink is not None:
                    _emit_market_status_event("closed", next_open)
                seconds_to_open = (
                    max(0.0, (next_open - _now_utc()).total_seconds())
                    if next_open
                    else 0.0
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
                gate_last_close_ms = gate.max_last_close_ms(config)
                effective_last_close_ms = (
                    gate_last_close_ms
                    if gate_last_close_ms is not None
                    else last_published_close_ms
                )
                lag_seconds = _calc_lag_seconds(effective_last_close_ms)
                idle_context: Dict[str, Any] = {
                    "channel": heartbeat_channel,
                    "mode": "idle",
                    "calendar_open": calendar_open,
                    "ticks_alive": ticks_alive,
                    "effective_market_open": market_open,
                    "redis_connected": current_redis is not None,
                    "redis_required": require_redis,
                    "redis_channel": (getattr(config, "ohlcv_channel", "") or "").strip()
                    or REDIS_CHANNEL,
                    "poll_seconds": poll_seconds,
                    "lookback_minutes": lookback_minutes,
                    "publish_interval_seconds": publish_interval_seconds,
                    "cycle_seconds": elapsed,
                    "idle_reason": idle_reason or "market_closed",
                    "next_open_seconds": seconds_to_open,
                    "stream_targets": _stream_targets_summary(config, gate),
                    "published_bars": cycle_published_bars,
                    "cache_enabled": cache_manager is not None,
                }
                targets_view = idle_context.get("stream_targets")
                if isinstance(targets_view, list):
                    live_ages: List[float] = []
                    for entry in targets_view:
                        if not isinstance(entry, Mapping):
                            continue
                        value = _float_or_none(entry.get("live_age_seconds"))
                        if value is None:
                            continue
                        live_ages.append(float(value))
                    if live_ages:
                        idle_context["ohlcv_live_age_seconds"] = float(min(live_ages))
                if universe_active_symbols_count is not None:
                    idle_context["universe"] = {
                        "active_symbols_count": universe_active_symbols_count,
                        "last_apply_ts": universe_last_apply_ts,
                    }
                if cadence_snapshot:
                    idle_context["tick_cadence"] = cadence_snapshot
                    idle_context["history_state"] = cadence_snapshot.get(
                        "history_state"
                    )
                _attach_price_stream_context(
                    idle_context,
                    price_stream,
                    metadata=price_stream_metadata,
                    cadence_snapshot=cadence_snapshot,
                )
                _attach_tick_agg_context(idle_context, tick_ohlcv_worker)
                _attach_supervisor_context(idle_context, async_supervisor)
                _attach_history_quota_context(idle_context, history_quota)
                if history_paused_by_backoff and history_backoff_remaining > 0.0:
                    idle_context["history_backoff_seconds"] = round(
                        history_backoff_remaining, 3
                    )
                if redis_backoff_remaining > 0.0:
                    idle_context["redis_backoff_seconds"] = round(
                        redis_backoff_remaining, 3
                    )
                _attach_backoff_context(
                    idle_context,
                    history_backoff=history_backoff,
                    redis_backoff=redis_backoff,
                )
                if lag_seconds is not None:
                    idle_context["lag_seconds"] = lag_seconds
                stats_snapshot = session_stats_tracker.snapshot()
                idle_context["session"] = _build_session_context(
                    next_open,
                    session_stats=stats_snapshot,
                )
                planned_sleep = _resolve_idle_sleep_seconds(
                    poll_seconds, cadence_sleep_seconds
                )
                _emit_heartbeat_event(
                    state="idle",
                    last_bar_close_ms=effective_last_close_ms,
                    next_open=next_open,
                    sleep_seconds=planned_sleep,
                    context=idle_context,
                )
                gate.update_staleness_metrics()
                sleep_for = max(0.0, planned_sleep)
            else:
                gate_last_close_ms = gate.max_last_close_ms(config)
                effective_last_close_ms = (
                    gate_last_close_ms
                    if gate_last_close_ms is not None
                    else last_published_close_ms
                )
                lag_seconds = _calc_lag_seconds(effective_last_close_ms)
                stream_context: Dict[str, Any] = {
                    "channel": heartbeat_channel,
                    "mode": "stream",
                    "calendar_open": calendar_open,
                    "ticks_alive": ticks_alive,
                    "effective_market_open": market_open,
                    "redis_connected": current_redis is not None,
                    "redis_required": require_redis,
                    "redis_channel": (getattr(config, "ohlcv_channel", "") or "").strip()
                    or REDIS_CHANNEL,
                    "poll_seconds": poll_seconds,
                    "lookback_minutes": lookback_minutes,
                    "publish_interval_seconds": publish_interval_seconds,
                    "cycle_seconds": elapsed,
                    "stream_targets": _stream_targets_summary(config, gate),
                    "published_bars": cycle_published_bars,
                    "cache_enabled": cache_manager is not None,
                }
                targets_view = stream_context.get("stream_targets")
                if isinstance(targets_view, list):
                    live_ages: List[float] = []
                    for entry in targets_view:
                        if not isinstance(entry, Mapping):
                            continue
                        value = _float_or_none(entry.get("live_age_seconds"))
                        if value is None:
                            continue
                        live_ages.append(float(value))
                    if live_ages:
                        stream_context["ohlcv_live_age_seconds"] = float(min(live_ages))
                if universe_active_symbols_count is not None:
                    stream_context["universe"] = {
                        "active_symbols_count": universe_active_symbols_count,
                        "last_apply_ts": universe_last_apply_ts,
                    }
                if cadence_snapshot:
                    stream_context["tick_cadence"] = cadence_snapshot
                    stream_context["history_state"] = cadence_snapshot.get(
                        "history_state"
                    )
                _attach_price_stream_context(
                    stream_context,
                    price_stream,
                    metadata=price_stream_metadata,
                    cadence_snapshot=cadence_snapshot,
                )
                _attach_tick_agg_context(stream_context, tick_ohlcv_worker)
                _attach_supervisor_context(stream_context, async_supervisor)
                _attach_history_quota_context(stream_context, history_quota)
                if history_paused_by_backoff and history_backoff_remaining > 0.0:
                    stream_context["history_backoff_seconds"] = round(
                        history_backoff_remaining, 3
                    )
                if redis_backoff_remaining > 0.0:
                    stream_context["redis_backoff_seconds"] = round(
                        redis_backoff_remaining, 3
                    )
                _attach_backoff_context(
                    stream_context,
                    history_backoff=history_backoff,
                    redis_backoff=redis_backoff,
                )
                if lag_seconds is not None:
                    stream_context["lag_seconds"] = lag_seconds
                stats_snapshot = session_stats_tracker.snapshot()
                stream_context["session"] = _build_session_context(
                    None,
                    session_stats=stats_snapshot,
                )
                base_sleep = max(0.0, poll_seconds - elapsed)
                if cadence_sleep_seconds is None or cadence_sleep_seconds <= 0.0:
                    sleep_for = base_sleep
                else:
                    sleep_for = cadence_sleep_seconds
                sleep_for = max(0.0, sleep_for)
                # Захист від busy-loop: інколи cadence може повернути 0,
                # тоді цикл починає спамити heartbeat/status без пауз.
                if max_cycles is None and sleep_for <= 0.0:
                    sleep_for = 0.2
                _emit_heartbeat_event(
                    state="stream",
                    last_bar_close_ms=effective_last_close_ms,
                    sleep_seconds=sleep_for,
                    context=stream_context,
                )
                last_idle_log_ts = None
                last_idle_next_open = None
                gate.update_staleness_metrics()

            if not market_pause:
                if history_backoff_remaining > 0.0:
                    sleep_for = max(sleep_for, history_backoff_remaining)
                if redis_backoff_remaining > 0.0:
                    sleep_for = max(sleep_for, redis_backoff_remaining)

            if status_bar is not None:
                ohlcv_live_age_seconds: Optional[float] = None
                try:
                    live_ages: List[float] = []
                    for sym, tf_raw in config:
                        value = gate.live_age_seconds(symbol=sym, timeframe=tf_raw)
                        if value is not None:
                            live_ages.append(float(value))
                    if live_ages:
                        ohlcv_live_age_seconds = float(min(live_ages))
                except Exception:  # noqa: BLE001
                    ohlcv_live_age_seconds = None

                supervisor_backpressure: Optional[bool] = None
                supervisor_ohlcv_queue: Optional[int] = None
                supervisor_ohlcv_dropped: Optional[int] = None
                if async_supervisor is not None:
                    try:
                        diag = async_supervisor.diagnostics_snapshot()
                        if isinstance(diag, Mapping):
                            supervisor_backpressure = bool(diag.get("backpressure"))
                            queues = diag.get("queues")
                            if isinstance(queues, list):
                                for entry in queues:
                                    if not isinstance(entry, Mapping):
                                        continue
                                    if str(entry.get("name")) != "ohlcv":
                                        continue
                                    try:
                                        raw_size = entry.get("size")
                                        supervisor_ohlcv_queue = (
                                            int(raw_size) if raw_size is not None else None
                                        )
                                    except (TypeError, ValueError):
                                        supervisor_ohlcv_queue = None
                                    try:
                                        raw_dropped = entry.get("dropped")
                                        supervisor_ohlcv_dropped = (
                                            int(raw_dropped)
                                            if raw_dropped is not None
                                            else None
                                        )
                                    except (TypeError, ValueError):
                                        supervisor_ohlcv_dropped = None
                                    break
                    except Exception:  # noqa: BLE001
                        pass

                status_bar.update(
                    {
                        "mode": "idle" if market_pause else "stream",
                        "market_open": market_open,
                        "calendar_open": calendar_open,
                        "ticks_alive": ticks_alive,
                        "tick_silence_seconds": tick_silence_seconds,
                        "next_open": last_idle_next_open,
                        "idle_reason": idle_reason,
                        "lag_seconds": lag_seconds,
                        "ohlcv_live_age_seconds": ohlcv_live_age_seconds,
                        "redis_connected": current_redis is not None,
                        "sleep_for": sleep_for,
                        "cycle_published_bars": cycle_published_bars,
                        "history_backoff_seconds": history_backoff_remaining,
                        "redis_backoff_seconds": redis_backoff_remaining,
                        "supervisor_backpressure": supervisor_backpressure,
                        "supervisor_ohlcv_queue": supervisor_ohlcv_queue,
                        "supervisor_ohlcv_dropped": supervisor_ohlcv_dropped,
                    }
                )

            last_ticks_alive = ticks_alive

            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                break
            if sleep_for and max_cycles is None:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        log.info("Стрім зупинено користувачем.")
    finally:
        if _SESSION_STATS_TRACKER is session_stats_tracker:
            _SESSION_STATS_TRACKER = None


def main() -> None:
    """Мінімальний тест підключення до FXCM та запиту історії.

    Логіка:
    - читаємо креденшали з `.env` dispatcher та профілю (через python-dotenv);
    - логін через ForexConnect;
    - POC-запит історії свічок та нормалізація у OHLCV;
    - коректний логаут.
    """
    _enforce_required_connector_version()
    log.info("Запуск FXCM Connector %s", FXCM_CONNECTOR_VERSION)
    setup_logging()
    load_env_profile()

    log.debug("Зчитуємо конфігурацію FXCM з ENV...")
    try:
        config: FXCMConfig = load_config()
    except ValueError as exc:
        log.error("%s", exc)
        return

    global _STATUS_CHANNEL
    _STATUS_CHANNEL = config.observability.status_channel or STATUS_CHANNEL_DEFAULT

    global _OHLCV_CHANNEL
    _OHLCV_CHANNEL = (getattr(config, "ohlcv_channel", "") or "").strip() or REDIS_CHANNEL

    global STATUS_PUBLISH_MIN_INTERVAL_SECONDS
    STATUS_PUBLISH_MIN_INTERVAL_SECONDS = float(
        config.observability.telemetry_min_publish_interval_seconds
    )

    log.debug(
        "Креденшали зчитано. Спроба логіну через %s (%s).",
        config.connection,
        config.host_url,
    )

    _apply_calendar_overrides(config.calendar)

    if config.observability.metrics_enabled:
        _ensure_metrics_server(config.observability.metrics_port)

    redis_client = _create_redis_client(config.redis)
    if redis_client is not None and config.redis_required:
        redis_ok = run_redis_healthcheck(redis_client, ohlcv_channel=_OHLCV_CHANNEL)
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
        log.warning("Redis недоступний — публікація вимкнена (file-only режим).")

    cache_manager: Optional[HistoryCache] = None
    if config.cache.enabled:
        cache_manager = HistoryCache(
            config.cache.root,
            config.cache.max_bars,
            config.cache.warmup_bars,
        )

    publish_gate = PublishDataGate()
    price_worker: Optional[PriceSnapshotWorker] = None
    offer_subscription: Optional[FXCMOfferSubscription] = None
    supervisor: Optional[AsyncStreamSupervisor] = None
    tick_cadence_controller: Optional[TickCadenceController] = None
    commands_worker: Optional[FxcmCommandWorker] = None

    stream_mode = config.stream_mode
    poll_seconds = config.poll_seconds
    lookback_minutes = config.lookback_minutes
    universe = StreamUniverse(
        base_targets=config.stream_targets,
        dynamic_enabled=bool(config.dynamic_universe_enabled),
        default_targets=list(config.dynamic_universe_default_targets),
        max_targets=int(config.dynamic_universe_max_targets),
    )
    stream_config = universe.get_effective_targets()

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
            targets_by_symbol: Dict[str, Set[str]] = {}
            for symbol, tf_raw in stream_config:
                tf_norm = _map_timeframe_label(tf_raw)
                targets_by_symbol.setdefault(symbol, set()).add(tf_norm)

            for symbol, tf_set in targets_by_symbol.items():
                # Кеш/FXCM history прогріваємо лише для 1m.
                cache_manager.ensure_ready(
                    fx_active,
                    symbol_raw=symbol,
                    timeframe_raw=BASE_HISTORY_TF_RAW,
                )
                redis_conn = redis_holder["client"]
                if redis_conn is None:
                    continue

                # 1m warmup з кешу.
                warmup_1m = cache_manager.get_bars_to_publish(
                    symbol_raw=symbol,
                    timeframe_raw=BASE_HISTORY_TF_RAW,
                    limit=cache_manager.warmup_bars,
                    force=True,
                )
                if not warmup_1m.empty:
                    try:
                        publish_ohlcv_to_redis(
                            warmup_1m,
                            symbol=symbol,
                            timeframe=BASE_HISTORY_TF_NORM,
                            redis_client=redis_conn,
                            channel=config.ohlcv_channel,
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
                                    warmup_1m,
                                    symbol=symbol,
                                    timeframe=BASE_HISTORY_TF_NORM,
                                    redis_client=redis_conn,
                                    channel=config.ohlcv_channel,
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

                    last_published = int(warmup_1m["open_time"].max())
                    cache_manager.mark_published(
                        symbol_raw=symbol,
                        timeframe_raw=BASE_HISTORY_TF_RAW,
                        last_open_time=last_published,
                    )
                    _sync_market_status_with_calendar(redis_conn)
                    last_close_ms = int(warmup_1m["close_time"].max())
                    lag_seconds = _calc_lag_seconds(last_close_ms)
                    warmup_context: Dict[str, Any] = {
                        "channel": config.observability.heartbeat_channel,
                        "mode": "warmup_cache",
                        "redis_connected": redis_conn is not None,
                        "redis_required": config.redis_required,
                        "redis_channel": REDIS_CHANNEL,
                        "stream_targets": _stream_targets_summary(
                            [(symbol, BASE_HISTORY_TF_RAW)], publish_gate
                        ),
                        "published_bars": len(warmup_1m),
                        "cache_source": "history_cache",
                    }
                    if lag_seconds is not None:
                        warmup_context["lag_seconds"] = lag_seconds
                    warmup_context["session"] = _build_session_context(next_open=None)
                    _publish_heartbeat(
                        redis_conn,
                        config.observability.heartbeat_channel,
                        state="warmup_cache",
                        last_bar_close_ms=last_close_ms,
                        context=warmup_context,
                    )
                    log.debug(
                        "Warmup із локального кешу: %s %s → %d барів",
                        symbol,
                        BASE_HISTORY_TF_RAW,
                        len(warmup_1m),
                    )

                # Старші TF warmup: агрегація з 1m кешу.
                try:
                    symbol_norm = _normalize_symbol(symbol)
                    record = cache_manager._load(symbol_norm, BASE_HISTORY_TF_NORM)
                    df_cache_1m = record.data
                except Exception:  # noqa: BLE001
                    df_cache_1m = pd.DataFrame()

                if df_cache_1m is None or df_cache_1m.empty:
                    continue

                for tf_norm in sorted({tf for tf in tf_set if tf != BASE_HISTORY_TF_NORM}):
                    agg = HistoryMtfCompleteAggregator(
                        symbol_norm=_normalize_symbol(symbol),
                        target_tf=tf_norm,
                    )
                    df_agg_all = agg.ingest_df_1m(df_cache_1m)
                    if df_agg_all is None or df_agg_all.empty:
                        continue
                    warmup_limit = max(1, int(cache_manager.warmup_bars // max(1, _tf_to_minutes(tf_norm))))
                    df_agg = cast(pd.DataFrame, df_agg_all.tail(int(warmup_limit)).reset_index(drop=True))
                    if df_agg.empty:
                        continue
                    try:
                        publish_ohlcv_to_redis(
                            df_agg,
                            symbol=symbol,
                            timeframe=tf_norm,
                            redis_client=redis_conn,
                            channel=config.ohlcv_channel,
                            data_gate=publish_gate,
                            hmac_secret=config.hmac_secret,
                            hmac_algo=config.hmac_algo,
                            source=HISTORY_AGG_SOURCE,
                        )
                    except RedisRetryableError:
                        # Warmup для старших TF не критичний: 1m все одно прогрівається.
                        continue
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
            subscription_holder: Dict[str, Optional[FXCMOfferSubscription]] = {
                "instance": None
            }

            price_worker = PriceSnapshotWorker(
                lambda: redis_holder["client"],
                channel=config.price_stream.channel,
                interval_seconds=config.price_stream.interval_seconds,
                symbols=[symbol for symbol, _ in stream_config],
                snapshot_callback=(
                    supervisor.submit_price_snapshot if supervisor is not None else None
                ),
            )
            price_worker.start()
            log.debug(
                "Tick-снепшоти: канал %s, інтервал %.1f с.",
                config.price_stream.channel,
                config.price_stream.interval_seconds,
            )

            tick_cadence_controller = TickCadenceController(
                tuning=config.tick_cadence,
                base_poll_seconds=poll_seconds,
                # Беремо базові TF, щоб cadence мав ключі для будь-яких dynamic підмножин.
                timeframes=[tf for _, tf in config.stream_targets],
            )

            min_interval_map: Dict[str, float] = {}
            if config.history_min_interval_seconds_m1 is not None:
                min_interval_map["1m"] = float(config.history_min_interval_seconds_m1)
            if config.history_min_interval_seconds_m5 is not None:
                min_interval_map["5m"] = float(config.history_min_interval_seconds_m5)
            history_quota = HistoryQuota(
                max_calls_per_min=config.history_max_calls_per_min,
                max_calls_per_hour=config.history_max_calls_per_hour,
                min_interval_by_tf=min_interval_map,
                priority_targets=config.history_priority_targets,
                priority_reserve_per_min=config.history_priority_reserve_per_min,
                priority_reserve_per_hour=config.history_priority_reserve_per_hour,
                load_thresholds={
                    "min_interval": config.history_load_thresholds.min_interval,
                    "warn": config.history_load_thresholds.warn,
                    "reserve": config.history_load_thresholds.reserve,
                    "critical": config.history_load_thresholds.critical,
                },
            )
            history_backoff_ctrl = FxcmBackoffController.from_tuning(
                config.history_backoff
            )
            redis_backoff_ctrl = FxcmBackoffController.from_tuning(config.redis_backoff)

            if config.async_supervisor:
                supervisor = AsyncStreamSupervisor(
                    redis_supplier=lambda: redis_holder["client"],
                    redis_reconnector=redis_reconnector,
                    data_gate=publish_gate,
                    ohlcv_channel=config.ohlcv_channel,
                    heartbeat_channel=config.observability.heartbeat_channel,
                    price_channel=config.price_stream.channel,
                    hmac_secret=config.hmac_secret,
                    hmac_algo=config.hmac_algo,
                    redis_backoff=redis_backoff_ctrl,
                )
                supervisor.start()

            def _direct_ohlcv_sink(batch: OhlcvBatch) -> None:
                publish_ohlcv_to_redis(
                    batch.data,
                    symbol=batch.symbol,
                    timeframe=batch.timeframe,
                    redis_client=redis_holder["client"],
                    channel=config.ohlcv_channel,
                    data_gate=publish_gate,
                    hmac_secret=config.hmac_secret,
                    hmac_algo=config.hmac_algo,
                    source=batch.source,
                )

            ohlcv_sink = (
                supervisor.submit_ohlcv_batch
                if supervisor is not None
                else _direct_ohlcv_sink
            )

            tick_ohlcv_worker = TickOhlcvWorker(
                enabled=config.tick_aggregation_enabled,
                symbols=[symbol for symbol, _ in stream_config],
                max_synth_gap_minutes=config.tick_aggregation_max_synth_gap_minutes,
                live_publish_min_interval_seconds=config.tick_aggregation_live_publish_interval_seconds,
                ohlcv_sink=ohlcv_sink,
            )
            tick_ohlcv_worker.start()

            def _on_universe_update(new_targets: List[Tuple[str, str]]) -> None:
                nonlocal stream_config
                stream_config = list(new_targets)
                new_symbols = [symbol for symbol, _ in stream_config]
                if price_worker is not None:
                    price_worker.set_symbols(new_symbols)
                tick_ohlcv_worker.set_symbols(new_symbols)
                fx_active_inner = fx_holder.get("client")
                if isinstance(fx_active_inner, ForexConnect):
                    _refresh_price_subscription(fx_active_inner)

            if config.commands_channel and redis_holder["client"] is not None:
                commands_worker = FxcmCommandWorker(
                    redis_client=redis_holder["client"],
                    fx_holder=fx_holder,
                    cache_manager=cache_manager,
                    tick_aggregation_enabled=config.tick_aggregation_enabled,
                    tick_agg_timeframes=("m1", "m5"),
                    channel=config.commands_channel,
                    ohlcv_channel=config.ohlcv_channel,
                    stream_targets=config.stream_targets,
                    universe=universe,
                    backfill_min_minutes=int(config.backfill_min_minutes),
                    backfill_max_minutes=int(config.backfill_max_minutes),
                    hmac_secret=config.hmac_secret,
                    hmac_algo=config.hmac_algo,
                )
                commands_worker.start()
                log.info("S3: FxcmCommandWorker запущено, канал команд: %s", config.commands_channel)

            status_bar = ConsoleStatusBar(enabled=True, refresh_per_second=4.0)
            status_bar.start()

            def _refresh_price_subscription(target_fx: ForexConnect) -> None:
                if price_worker is None:
                    return

                def _on_tick(
                    symbol: str, bid: float, ask: float, tick_ts: float
                ) -> None:
                    price_worker.enqueue_tick(symbol, bid, ask, tick_ts)
                    tick_ohlcv_worker.enqueue_tick(symbol, bid, ask, tick_ts)

                existing = subscription_holder["instance"]
                if existing is not None:
                    existing.close()
                try:
                    subscription_holder["instance"] = FXCMOfferSubscription(
                        target_fx,
                        symbols=[symbol for symbol, _ in stream_config],
                        on_tick=_on_tick,
                    )
                except Exception:
                    subscription_holder["instance"] = None
                    log.exception("Не вдалося підписатися на FXCM OfferTable.")

            try:
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
                    price_stream=price_worker,
                    ohlcv_sink=(
                        supervisor.submit_ohlcv_batch
                        if supervisor is not None
                        else None
                    ),
                    heartbeat_sink=(
                        supervisor.submit_heartbeat if supervisor is not None else None
                    ),
                    market_status_sink=(
                        supervisor.submit_market_status
                        if supervisor is not None
                        else None
                    ),
                    async_supervisor=supervisor,
                    history_quota=history_quota,
                    fx_session_observer=_refresh_price_subscription,
                    history_backoff=history_backoff_ctrl,
                    redis_backoff=redis_backoff_ctrl,
                    tick_cadence=tick_cadence_controller,
                    tick_aggregation_enabled=config.tick_aggregation_enabled,
                    tick_aggregation_timeframes=("1m", "5m"),
                    mtf_crosscheck=config.mtf_crosscheck,
                    status_bar=status_bar,
                    universe=universe,
                    universe_update_callback=_on_universe_update,
                    tick_ohlcv_worker=tick_ohlcv_worker,
                )
            finally:
                status_bar.stop()
                offer_subscription = subscription_holder.get("instance")
                if offer_subscription is not None:
                    offer_subscription.close()
                if price_worker is not None:
                    price_worker.stop()
                tick_ohlcv_worker.stop()
                if commands_worker is not None:
                    commands_worker.stop()
                if supervisor is not None:
                    supervisor.stop()
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
