"""Консольний viewer для FXCM-конектора.

Формує live-dashboard у терміналі з такими панелями:
- heartbeat/market_status діагностика (стан, лаг, цикл, next_open, Redis-зʼєднання);
- інцидент-стрічка та алерти (fxcm_pause, calendar_pause, redis_disconnect, ohlcv_idle/lag/empty);
- таймлайн подій HB/MS із підсумками та легендою;
- таблиці stream targets та OHLCV (staleness, lag, msg age, sparkline-тренди);
- сесійний блок із тегами, timezone, range/baseline та статистикою symbols;
- Redis health, issue counters і меню режимів.
- режим supervisor для моніторингу async supervisor (черги sink-ів, таски та останні помилки).

Запуск:
    python tools/debug_viewer.py --redis-host 127.0.0.1 --redis-port 6379

Канали/пороги задаються секцією `viewer` у `config/runtime_settings.json`;
redis host/port можна переозначити CLI або через `.env`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import select
import signal
import statistics
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Sequence, Set, Tuple, Union

from dotenv import load_dotenv
from rich import box
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

try:
    import redis  # type: ignore[import]
except ImportError:  # pragma: no cover - viewer потребує redis-client
    redis = None

try:
    from prometheus_client import Counter as PromCounter, Gauge as PromGauge  # type: ignore[import]
except ImportError:  # pragma: no cover - метрики не обов'язкові
    PromCounter = None  # type: ignore[assignment]
    PromGauge = None  # type: ignore[assignment]

try:  # pragma: no cover - доступно лише на Windows
    import msvcrt  # type: ignore[import]
except ImportError:  # noqa: SIM105
    msvcrt = None

try:  # pragma: no cover - недоступно на Windows
    import termios
    import tty
except ImportError:  # noqa: SIM105
    termios = None  # type: ignore[assignment]
    tty = None  # type: ignore[assignment]

try:
    from config import RUNTIME_SETTINGS_FILE as _RUNTIME_SETTINGS_PATH
except Exception:  # pragma: no cover - fallback якщо модуль config недоступний
    _RUNTIME_SETTINGS_PATH = Path("config/runtime_settings.json")


def _load_viewer_settings() -> Dict[str, Any]:
    try:
        payload = json.loads(_RUNTIME_SETTINGS_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except OSError:
        return {}
    except json.JSONDecodeError:
        return {}
    viewer_cfg = payload.get("viewer")
    return viewer_cfg if isinstance(viewer_cfg, dict) else {}


_VIEWER_SETTINGS = _load_viewer_settings()


def _cfg_str(key: str, default: str) -> str:
    value = _VIEWER_SETTINGS.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return default


def _cfg_float(key: str, default: float, *, min_value: Optional[float] = None) -> float:
    value = _VIEWER_SETTINGS.get(key)
    if value is None:
        return default
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            parsed = float(text)
        except ValueError:
            return default
    else:
        return default
    if min_value is not None:
        return max(min_value, parsed)
    return parsed


def _cfg_int(key: str, default: int, *, min_value: int = 1) -> int:
    value = _VIEWER_SETTINGS.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        parsed = int(value)
    elif isinstance(value, (int, float)):
        parsed = int(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            parsed = int(float(text))
        except ValueError:
            return default
    else:
        return default
    return max(min_value, parsed)


def _cfg_list(key: str, default: Sequence[str]) -> List[str]:
    value = _VIEWER_SETTINGS.get(key)
    items: List[str] = []
    if isinstance(value, list):
        raw_items = value
    elif isinstance(value, str):
        raw_items = [part.strip() for part in value.split(",")]
    else:
        raw_items = list(default)
    for item in raw_items:
        if not item:
            continue
        text = str(item).strip()
        if text:
            items.append(text)
    if not items:
        return list(default)
    return items


def _cfg_dict_str(key: str) -> Dict[str, str]:
    value = _VIEWER_SETTINGS.get(key)
    if not isinstance(value, dict):
        return {}
    result: Dict[str, str] = {}
    for raw_key, raw_val in value.items():
        key_text = str(raw_key).strip()
        val_text = str(raw_val).strip()
        if key_text and val_text:
            result[key_text] = val_text
    return result

HEARTBEAT_CHANNEL_DEFAULT = _cfg_str("heartbeat_channel", "fxcm:heartbeat")
MARKET_STATUS_CHANNEL_DEFAULT = _cfg_str("market_status_channel", "fxcm:market_status")
OHLCV_CHANNEL_DEFAULT = _cfg_str("ohlcv_channel", "fxcm:ohlcv")
REDIS_HEALTH_INTERVAL = _cfg_float("redis_health_interval", 8.0, min_value=1.0)
LAG_SPIKE_THRESHOLD = _cfg_float("lag_spike_threshold", 180.0, min_value=1.0)
FXCM_PAUSE_REASONS = {"fxcm_temporarily_unavailable", "fxcm_unavailable"}
CALENDAR_PAUSE_REASONS = {"calendar", "calendar_closed", "market_closed"}
ISSUE_LABELS = {
    "fxcm_pause": "FXCM паузи",
    "calendar_pause": "Календарні паузи",
    "redis_disconnect": "Розриви Redis",
    "lag_spike": f"Лаг > {int(LAG_SPIKE_THRESHOLD)} с",
    "ohlcv_msg_idle": "OHLCV idle",
    "ohlcv_lag": "OHLCV лаги",
    "ohlcv_empty": "OHLCV порожні payload",
}
HEARTBEAT_ALERT_SECONDS = _cfg_float("heartbeat_alert_seconds", 45.0, min_value=5.0)
OHLCV_MSG_IDLE_WARN_SECONDS = _cfg_float("ohlcv_msg_idle_warn_seconds", 90.0, min_value=5.0)
OHLCV_MSG_IDLE_ERROR_SECONDS = _cfg_float("ohlcv_msg_idle_error_seconds", 180.0, min_value=10.0)
OHLCV_LAG_WARN_SECONDS = _cfg_float("ohlcv_lag_warn_seconds", 45.0, min_value=5.0)
OHLCV_LAG_ERROR_SECONDS = _cfg_float("ohlcv_lag_error_seconds", 120.0, min_value=10.0)
_IDLE_THRESHOLD_OVERRIDES: Dict[str, Tuple[float, float]] = {
    "1m": (
        _cfg_float("tf_1m_idle_warn_seconds", 70.0, min_value=5.0),
        _cfg_float("tf_1m_idle_error_seconds", 90.0, min_value=10.0),
    ),
    "5m": (
        _cfg_float("tf_5m_idle_warn_seconds", 450.0, min_value=5.0),
        _cfg_float("tf_5m_idle_error_seconds", 900.0, min_value=10.0),
    ),
}
ALERT_SEVERITY_ORDER = {"danger": 0, "warning": 1, "info": 2}
ALERT_SEVERITY_COLORS = {"danger": "red", "warning": "yellow", "info": "cyan"}
MENU_TEXT = "[P] пауза  [C] очистити  [Q] вихід  [0] TL  [1] SUM  [2] SES  [3] ALERT  [4] STREAM  [5] PRICE  [6] SUPR"
TIMELINE_MATRIX_ROWS = _cfg_int("timeline_matrix_rows", 10, min_value=2)
TIMELINE_MAX_COLUMNS = _cfg_int("timeline_max_columns", 120, min_value=10)
_TIMELINE_HISTORY_DEFAULT = max(240, TIMELINE_MATRIX_ROWS * TIMELINE_MAX_COLUMNS * 2)
TIMELINE_HISTORY_MAX = _cfg_int("timeline_history_max", _TIMELINE_HISTORY_DEFAULT, min_value=TIMELINE_MATRIX_ROWS)
TIMELINE_FOCUS_MINUTES = _cfg_float("timeline_focus_minutes", 30.0, min_value=1.0)
SUPERVISOR_CHANNELS_DEFAULT = ("ohlcv", "heartbeat", "market_status", "price")
SUPERVISOR_CHANNELS = tuple(_cfg_list("supervisor_channels", SUPERVISOR_CHANNELS_DEFAULT))
_SUPERVISOR_CHANNEL_HINT_DEFAULTS = {
    "ohlcv": "Основний sink для OHLCV повідомлень (fxcm:ohlcv).",
    "heartbeat": "Публікація heartbeat із контекстом stream (fxcm:heartbeat).",
    "market_status": "Оновлення стану ринку (fxcm:market_status).",
    "price": "Снепшоти цін із PriceSnapshotWorker (fxcm:price_tik).",
}
SUPERVISOR_CHANNEL_HINT_OVERRIDES = _cfg_dict_str("supervisor_channel_hints")


def _resolve_channel_hint(name: str) -> str:
    return SUPERVISOR_CHANNEL_HINT_OVERRIDES.get(name) or _SUPERVISOR_CHANNEL_HINT_DEFAULTS.get(name, "")


SUPERVISOR_CHANNEL_HINTS = {name: _resolve_channel_hint(name) for name in SUPERVISOR_CHANNELS}
SUPERVISOR_METRIC_HINTS = {
    "total_enqueued": "Скільки payload покладено в черги (включно з необробленими).",
    "total_processed": "Скільки payload знято воркерами (успішно/помилково).",
    "total_dropped": "Кількість дропів через переповнення черги.",
    "queue_depth_total": "Сумарна глибина черг (допомагає бачити беклог).",
    "publishes_total": "Загальна кількість publish викликів із моменту старту supervisor.",
    "active_tasks": "Скільки тасків зараз у стані running.",
    "task_errors": "Сумарні помилки, які повідомили таски.",
    "backpressure_events": "Скільки разів enqueue повертав QueueFull (backpressure).",
}


def _tf_label_to_minutes(tf_label: str) -> Optional[float]:
    tf_norm = (tf_label or "").strip().lower()
    if not tf_norm:
        return None
    if tf_norm.endswith("m") and tf_norm[:-1].isdigit():
        return float(max(1, int(tf_norm[:-1] or "1")))
    if tf_norm.endswith("h") and tf_norm[:-1].isdigit():
        return float(max(1, int(tf_norm[:-1] or "1")) * 60)
    if tf_norm.endswith("d") and tf_norm[:-1].isdigit():
        return float(max(1, int(tf_norm[:-1] or "1")) * 1_440)
    return None


def resolve_idle_thresholds(tf_label: str) -> Tuple[float, float]:
    tf_key = (tf_label or "").strip().lower()
    override = _IDLE_THRESHOLD_OVERRIDES.get(tf_key)
    if override:
        return override
    tf_minutes = _tf_label_to_minutes(tf_key)
    if tf_minutes is not None:
        tf_seconds = tf_minutes * 60.0
        warn = max(OHLCV_MSG_IDLE_WARN_SECONDS, tf_seconds * 1.5)
        error = max(OHLCV_MSG_IDLE_ERROR_SECONDS, tf_seconds * 3.0)
        return warn, error
    return OHLCV_MSG_IDLE_WARN_SECONDS, OHLCV_MSG_IDLE_ERROR_SECONDS

if PromGauge is not None:
    PROM_VIEWER_OHLCV_LAG_SECONDS = PromGauge(
        "ai_one_fxcm_ohlcv_lag_seconds",
        "Lag between last close time and now (seconds) per symbol/tf as observed by debug viewer",
        ["symbol", "tf"],
    )
    PROM_VIEWER_OHLCV_MSG_AGE_SECONDS = PromGauge(
        "ai_one_fxcm_ohlcv_msg_age_seconds",
        "Age of the latest OHLCV message per symbol/tf as observed by debug viewer",
        ["symbol", "tf"],
    )
else:  # pragma: no cover - prom клієнт опціональний
    PROM_VIEWER_OHLCV_LAG_SECONDS = None
    PROM_VIEWER_OHLCV_MSG_AGE_SECONDS = None

if PromCounter is not None:
    PROM_VIEWER_OHLCV_GAPS_TOTAL = PromCounter(
        "ai_one_fxcm_ohlcv_gaps_total",
        "Number of OHLCV idle/lag gap events detected by debug viewer",
        ["symbol", "tf"],
    )
else:  # pragma: no cover
    PROM_VIEWER_OHLCV_GAPS_TOTAL = None


class DashboardMode(Enum):
    TIMELINE = 0
    SUMMARY = 1
    SESSION = 2
    ALERTS = 3
    STREAMS = 4
    PRICE = 5
    SUPERVISOR = 6


@dataclass
class TimelineEvent:
    source: str
    state: str
    ts_iso: str
    ts_epoch: float


@dataclass
class ViewerAlert:
    key: str
    message: str
    severity: str
    since_ts: float


@dataclass
class ViewerState:
    last_heartbeat: Optional[Dict[str, Any]] = None
    last_market_status: Optional[Dict[str, Any]] = None
    last_message_ts: float = field(default_factory=time.time)
    last_heartbeat_ts: Optional[float] = None
    timeline_events: Deque[TimelineEvent] = field(default_factory=lambda: deque(maxlen=TIMELINE_HISTORY_MAX))
    staleness_history: Dict[Tuple[str, str], Deque[float]] = field(default_factory=dict)
    stream_target_updated: Dict[Tuple[str, str], float] = field(default_factory=dict)
    session_range_history: Dict[Tuple[str, str, str], Deque[float]] = field(default_factory=dict)
    session_range_snapshot: Dict[Tuple[str, str, str], Tuple[Optional[float], Optional[float]]] = field(default_factory=dict)
    redis_health: Dict[str, Any] = field(default_factory=dict)
    ohlcv_targets: Dict[Tuple[str, str], Dict[str, Any]] = field(default_factory=dict)
    ohlcv_lag_history: Dict[Tuple[str, str], Deque[float]] = field(default_factory=dict)
    ohlcv_updated: Dict[Tuple[str, str], float] = field(default_factory=dict)
    ohlcv_zero_bars_since: Dict[Tuple[str, str], float] = field(default_factory=dict)
    ohlcv_idle_flags: Dict[Tuple[str, str], bool] = field(default_factory=dict)
    ohlcv_lag_flags: Dict[Tuple[str, str], bool] = field(default_factory=dict)
    issue_counters: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    issue_state_flags: Dict[str, bool] = field(default_factory=dict)
    paused: bool = False
    last_action: Optional[str] = None
    alerts: Dict[str, ViewerAlert] = field(default_factory=dict)
    active_mode: DashboardMode = DashboardMode.SUMMARY
    last_ohlcv_ts: Optional[float] = None
    incident_feed: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=24))
    price_stream: Dict[str, Any] = field(default_factory=dict)
    price_stream_updated: Optional[float] = None
    tick_silence_seconds: Optional[float] = None
    supervisor_diag: Dict[str, Any] = field(default_factory=dict)
    supervisor_updated: Optional[float] = None

    def note_heartbeat(self, payload: Dict[str, Any]) -> None:
        self.last_heartbeat = payload
        now_ts = time.time()
        self.last_message_ts = now_ts
        self.last_heartbeat_ts = now_ts
        context = payload.get("context") or {}
        state = str(payload.get("state", "?") or "?")
        self._update_staleness_history(context)
        session_context = context.get("session") or {}
        self._update_session_ranges(session_context)
        self._update_issue_counters(state, context)
        self._add_timeline_event(source="HB", state=state, ts=payload.get("ts"))
        self.tick_silence_seconds = _coerce_float(context.get("tick_silence_seconds"))
        price_ctx = context.get("price_stream")
        if isinstance(price_ctx, dict):
            self.price_stream = price_ctx
            self.price_stream_updated = now_ts
        else:
            self.price_stream = {}
        supervisor_ctx = None
        for key in ("supervisor", "async_supervisor"):
            candidate = context.get(key)
            if isinstance(candidate, dict):
                supervisor_ctx = candidate
                break
        if supervisor_ctx is not None:
            self.supervisor_diag = supervisor_ctx
            self.supervisor_updated = now_ts
        else:
            self.supervisor_diag = {}
            self.supervisor_updated = None

    def note_market_status(self, payload: Dict[str, Any]) -> None:
        self.last_market_status = payload
        self.last_message_ts = time.time()
        self._add_timeline_event(source="MS", state=str(payload.get("state", "?")), ts=payload.get("ts"))

    def note_ohlcv(self, payload: Dict[str, Any]) -> None:
        """Фіксує OHLCV-повідомлення та відокремлює Lag vs Msg age.

        Lag (s)   = now - last_close_time (differentia між ринком і останньою свічкою).
        Msg age   = now - last_receive_time (як давно приходили будь-які дані).
        """

        symbol_raw = payload.get("symbol")
        tf_raw = payload.get("tf")
        bars = payload.get("bars")
        if not symbol_raw or not tf_raw or not isinstance(bars, list):
            return

        symbol = str(symbol_raw)
        tf = str(tf_raw)
        key = (symbol, tf)
        bars_per_msg = len(bars)
        now_ts = time.time()

        last_close_ms: Optional[int] = None
        for entry in bars:
            if not isinstance(entry, dict):
                continue
            close_ms = _coerce_int(entry.get("close_time"))
            if close_ms is None:
                continue
            if last_close_ms is None or close_ms > last_close_ms:
                last_close_ms = close_ms
        if last_close_ms is None and key in self.ohlcv_targets:
            last_close_ms = _coerce_int(self.ohlcv_targets[key].get("last_close_ms"))

        lag_seconds = None
        if last_close_ms is not None:
            lag_seconds = max(0.0, now_ts - (last_close_ms / 1000.0))

        self.ohlcv_targets[key] = {
            "symbol": symbol,
            "tf": tf,
            "bars_per_msg": bars_per_msg,
            "last_close_ms": last_close_ms,
        }
        if lag_seconds is not None:
            history = self.ohlcv_lag_history.setdefault(key, deque(maxlen=40))
            history.append(lag_seconds)
        self.ohlcv_updated[key] = now_ts
        self.last_ohlcv_ts = now_ts

        if bars_per_msg == 0:
            self.ohlcv_zero_bars_since[key] = now_ts
        else:
            self.ohlcv_zero_bars_since.pop(key, None)

        _update_ohlcv_metrics(symbol, tf, lag_seconds, msg_age=0.0)

        if len(self.ohlcv_targets) > 200:
            self._prune_ohlcv(now_ts)

    def _prune_ohlcv(self, now_ts: float) -> None:
        cutoff = now_ts - 6 * 3600
        for key in list(self.ohlcv_targets.keys()):
            updated = self.ohlcv_updated.get(key)
            if updated is None or updated < cutoff:
                self.ohlcv_targets.pop(key, None)
                self.ohlcv_lag_history.pop(key, None)
                self.ohlcv_updated.pop(key, None)
                self.ohlcv_zero_bars_since.pop(key, None)
                self.ohlcv_idle_flags.pop(key, None)
                self.ohlcv_lag_flags.pop(key, None)

    def note_redis_health(self, payload: Dict[str, Any]) -> None:
        self.redis_health = payload
        status = str(payload.get("status", "")).lower()
        error_message = payload.get("error")
        latency = payload.get("latency_ms")
        if error_message:
            message = f"Redis health: {error_message}"
        elif latency is not None:
            message = f"Redis latency {_format_ms(latency)}"
        else:
            message = "Redis ping ok"
        self._set_alert("redis_health", status == "error", message, severity="danger")

    def _add_timeline_event(self, *, source: str, state: str, ts: Optional[str]) -> None:
        try:
            ts_epoch = _iso_to_epoch(ts)
            ts_iso = ts or dt.datetime.fromtimestamp(ts_epoch, tz=dt.timezone.utc).isoformat()
        except Exception:
            ts_epoch = time.time()
            ts_iso = dt.datetime.fromtimestamp(ts_epoch, tz=dt.timezone.utc).isoformat()
        symbol_state = state or "?"
        self.timeline_events.append(TimelineEvent(source=source, state=symbol_state, ts_iso=ts_iso, ts_epoch=ts_epoch))

    def _update_staleness_history(self, context: Dict[str, Any]) -> None:
        targets = context.get("stream_targets") or []
        seen_keys: Set[Tuple[str, str]] = set()
        now_ts = time.time()
        for target in targets:
            symbol = str(target.get("symbol", "?"))
            tf = str(target.get("tf", "?"))
            key = (symbol, tf)
            seen_keys.add(key)
            self.stream_target_updated[key] = now_ts
            staleness = _coerce_float(target.get("staleness_seconds"))
            if staleness is None:
                continue
            history = self.staleness_history.setdefault(key, deque(maxlen=20))
            history.append(staleness)
        # prune histories that are no longer present to avoid leaking memory
        for key in list(self.staleness_history.keys()):
            if key not in seen_keys and len(self.staleness_history) > 50:
                self.staleness_history.pop(key, None)
        for key in list(self.stream_target_updated.keys()):
            if key not in seen_keys:
                self.stream_target_updated.pop(key, None)

    def _update_session_ranges(self, session_context: Dict[str, Any]) -> None:
        stats = session_context.get("stats") if isinstance(session_context, dict) else None
        if not isinstance(stats, dict):
            return
        seen_keys: Set[Tuple[str, str, str]] = set()
        for tag, payload in stats.items():
            symbols = None
            if isinstance(payload, dict):
                symbols = payload.get("symbols")
            elif isinstance(payload, list):
                symbols = payload
            if not isinstance(symbols, list):
                continue
            for entry in symbols:
                if not isinstance(entry, dict):
                    continue
                symbol = str(entry.get("symbol", "?"))
                tf = str(entry.get("tf", "?"))
                key = (str(tag), symbol, tf)
                seen_keys.add(key)
                current_range = _coerce_float(entry.get("range"))
                if current_range is None:
                    high_val = _coerce_float(entry.get("high"))
                    low_val = _coerce_float(entry.get("low"))
                    if high_val is not None and low_val is not None:
                        current_range = high_val - low_val
                if current_range is None:
                    continue
                history = self.session_range_history.setdefault(key, deque(maxlen=30))
                baseline = statistics.mean(history) if history else None
                self.session_range_snapshot[key] = (current_range, baseline)
                history.append(current_range)
        if seen_keys:
            for key in list(self.session_range_snapshot.keys()):
                if key not in seen_keys:
                    self.session_range_snapshot.pop(key, None)

    def toggle_pause(self) -> None:
        self.paused = not self.paused
        self.last_action = "Пауза увімкнена" if self.paused else "Пауза вимкнена"

    def clear_timeline(self) -> None:
        self.timeline_events.clear()
        self.last_action = "Таймлайн очищено"

    def note_action(self, text: str) -> None:
        self.last_action = text

    def _append_incident(self, key: str, active: bool, detail: Optional[str]) -> None:
        entry = {
            "key": key,
            "label": ISSUE_LABELS.get(key, key),
            "active": active,
            "detail": detail or "—",
            "ts": time.time(),
        }
        self.incident_feed.appendleft(entry)

    def _set_alert(self, key: str, active: bool, message: Optional[str], severity: str = "warning") -> bool:
        changed = False
        if active:
            existing = self.alerts.get(key)
            if existing:
                new_message = message or existing.message
                if existing.message != new_message or existing.severity != severity:
                    existing.message = new_message
                    existing.severity = severity
                    changed = True
            else:
                self.alerts[key] = ViewerAlert(
                    key=key,
                    message=message or "—",
                    severity=severity,
                    since_ts=time.time(),
                )
                changed = True
        else:
            if key in self.alerts:
                self.alerts.pop(key, None)
                changed = True
        return changed

    def active_alerts(self) -> List[ViewerAlert]:
        return sorted(
            self.alerts.values(),
            key=lambda alert: (ALERT_SEVERITY_ORDER.get(alert.severity, 99), alert.since_ts),
        )

    def _record_issue(self, key: str, detail: Optional[str]) -> None:
        entry = self.issue_counters.setdefault(key, {"count": 0})
        entry["count"] += 1
        entry["last_ts"] = time.time()
        if detail:
            entry["detail"] = detail

    def _track_issue_state(self, key: str, active: bool, detail: Optional[str]) -> None:
        previous = self.issue_state_flags.get(key, False)
        self.issue_state_flags[key] = active
        if active and not previous:
            self._record_issue(key, detail)
        if active != previous:
            self._append_incident(key, active, detail)

    def _update_issue_counters(self, state: str, context: Dict[str, Any]) -> None:
        idle_reason_raw = context.get("idle_reason")
        pause_reason_raw = context.get("market_pause_reason")
        idle_reason = str(idle_reason_raw).strip().lower() if idle_reason_raw else ""
        pause_reason = str(pause_reason_raw).strip().lower() if pause_reason_raw else ""

        fxcm_active = state.lower() == "idle" and (
            idle_reason in FXCM_PAUSE_REASONS or pause_reason in FXCM_PAUSE_REASONS
        )
        detail_fxcm = idle_reason_raw or pause_reason_raw or "fxcm pause"
        self._track_issue_state("fxcm_pause", fxcm_active, str(detail_fxcm))
        self._set_alert("fxcm_pause", fxcm_active, str(detail_fxcm), severity="warning")

        calendar_active = state.lower() == "idle" and (
            idle_reason in CALENDAR_PAUSE_REASONS or pause_reason in CALENDAR_PAUSE_REASONS
        )
        detail_calendar = idle_reason_raw or pause_reason_raw or "calendar pause"
        self._track_issue_state("calendar_pause", calendar_active, str(detail_calendar))
        self._set_alert("calendar_pause", calendar_active, str(detail_calendar), severity="info")

        redis_required = context.get("redis_required")
        redis_connected = context.get("redis_connected")
        redis_active = bool((redis_required is None or redis_required) and redis_connected is False)
        self._track_issue_state("redis_disconnect", redis_active, "Redis disconnected")
        self._set_alert("redis_disconnect", redis_active, "Redis disconnected", severity="danger")

        lag_value = _coerce_float(context.get("lag_seconds"))
        lag_active = bool(lag_value is not None and lag_value >= LAG_SPIKE_THRESHOLD)
        detail_lag = f"Lag {lag_value:.1f}s" if lag_value is not None else None
        self._track_issue_state("lag_spike", lag_active, detail_lag)
        self._set_alert("lag_spike", lag_active, detail_lag, severity="warning")

    def heartbeat_age(self) -> Optional[float]:
        if self.last_heartbeat_ts is None:
            return None
        return max(0.0, time.time() - self.last_heartbeat_ts)


def _iso_to_epoch(value: Optional[str]) -> float:
    if not value:
        raise ValueError("missing timestamp")
    normalized = value.replace("Z", "+00:00")
    return dt.datetime.fromisoformat(normalized).timestamp()


def _coerce_float(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _coerce_int(value: Any) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _format_float(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "—"
    return f"{value:,.{digits}f}"


def _format_bool(value: Any) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        return "yes" if value else "no"
    if isinstance(value, str):
        return "yes" if value.lower() in {"1", "true", "yes"} else "no"
    return "—"


def _format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    if seconds < 1:
        return "<1s"
    total = int(seconds)
    if total < 60:
        return f"{total:02}s"
    minutes, secs = divmod(total, 60)
    if total < 3_600:
        return f"{minutes:02}:{secs:02}"
    hours, minutes = divmod(minutes, 60)
    if total < 86_400:
        return f"{hours:02}:{minutes:02}:{secs:02}"
    days, hours = divmod(hours, 24)
    return f"{days:02}d {hours:02}:{minutes:02}:{secs:02}"


def _format_utc_timestamp(ts: Optional[float]) -> str:
    if ts is None:
        return "—"
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _format_short_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    seconds = max(0.0, seconds)
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}"


def _format_diag_timestamp(value: Any) -> str:
    if value is None:
        return "—"
    epoch: Optional[float] = None
    if isinstance(value, (int, float)):
        epoch = float(value)
    elif isinstance(value, str) and value.strip():
        text = value.replace("Z", "+00:00")
        try:
            epoch = dt.datetime.fromisoformat(text).timestamp()
        except ValueError:
            return value
    if epoch is None:
        return "—"
    ts = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    ago = max(0.0, time.time() - epoch)
    return f"{ts.strftime('%H:%M:%S')}Z ({_format_short_duration(ago)} ago)"


def _format_diag_duration(value: Any) -> str:
    seconds = _coerce_float(value)
    if seconds is None:
        return "—"
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 60:
        return f"{seconds:.1f} s"
    return _format_short_duration(seconds)


def _normalize_epoch_seconds(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 10_000_000_000:  # мс → секунди
            numeric /= 1000.0
        return numeric
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            numeric = float(text)
        except ValueError:
            try:
                return _iso_to_epoch(text)
            except Exception:
                return None
        if numeric > 10_000_000_000:
            numeric /= 1000.0
        return numeric
    return None


def _format_age_label(age_seconds: Optional[float], ts_value: Any = None) -> str:
    if age_seconds is not None:
        return _format_short_duration(age_seconds)
    epoch = _normalize_epoch_seconds(ts_value)
    if epoch is None:
        return "—"
    return _format_short_duration(max(0.0, time.time() - epoch))


def _format_usage_text(ratio: Optional[float]) -> Text:
    if ratio is None:
        return Text("—", style="dim")
    ratio = max(0.0, min(1.0, ratio))
    percent = ratio * 100.0
    if percent >= 90:
        style = "bold red"
    elif percent >= 70:
        style = "yellow"
    else:
        style = "green"
    return Text(f"{percent:.0f}%", style=style)


def _format_int_value(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{int(round(value)):,}"


def _shorten_text(value: Any, limit: int = 60) -> str:
    if value is None:
        return "—"
    text = str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


SPARKLINE_CHARS = "▁▂▃▄▅▆▇█"
RANGE_BAR_WIDTH = 14


def _render_sparkline(values: Optional[Deque[float]]) -> str:
    if not values:
        return "—"
    if len(values) == 1:
        idx = 0 if values[0] == 0 else len(SPARKLINE_CHARS) - 1
        return SPARKLINE_CHARS[idx]
    v_min = min(values)
    v_max = max(values)
    if v_max - v_min < 1e-9:
        idx = min(len(SPARKLINE_CHARS) - 1, int(len(SPARKLINE_CHARS) * 0.5))
        return SPARKLINE_CHARS[idx] * len(values)
    normalized = [(v - v_min) / (v_max - v_min) for v in values]
    spark = "".join(
        SPARKLINE_CHARS[min(len(SPARKLINE_CHARS) - 1, int(val * (len(SPARKLINE_CHARS) - 1)))]
        for val in normalized
    )
    return spark[-12:]


def _format_lag_age(lag: Optional[float], age: Optional[float]) -> str:
    lag_str = f"{lag:.1f}s" if lag is not None else "—"
    age_str = _format_short_duration(age)
    return f"{lag_str} / {age_str}"


def _range_ratio(current: Optional[float], baseline: Optional[float]) -> Optional[float]:
    if current is None or baseline is None:
        return None
    if baseline <= 0:
        return None
    return current / baseline


def _range_color(ratio: Optional[float]) -> str:
    if ratio is None:
        return "white"
    if ratio >= 1.25:
        return "red"
    if ratio >= 0.9:
        return "yellow"
    return "green"


def _format_ratio(ratio: Optional[float]) -> str:
    if ratio is None:
        return "—"
    return f"{ratio * 100:.0f}%"


def _render_range_indicator(current: Optional[float], baseline: Optional[float], width: int = RANGE_BAR_WIDTH) -> Text:
    if current is None:
        return Text("—", style="dim")
    ratio = _range_ratio(current, baseline)
    cap = 2.0
    normalized = ratio if ratio is not None else 1.0
    normalized = max(0.0, min(normalized, cap)) / cap
    filled = int(round(normalized * width))
    filled = max(0, min(width, filled))
    remainder = width - filled
    bar = Text()
    if filled:
        bar.append("█" * filled, style=_range_color(ratio))
    if remainder:
        bar.append("·" * remainder, style="dim")
    if ratio is not None:
        bar.append(f"  {_format_ratio(ratio)}", style="white")
    else:
        bar.append("  —", style="dim")
    return bar


class KeyboardInput:
    """Платформо-залежний неблокуючий зчитувач клавіш."""

    def __init__(self) -> None:
        self.fd: Optional[int] = None
        self._old_settings: Any = None

    def __enter__(self) -> "KeyboardInput":
        if os.name != "nt" and sys.stdin.isatty() and termios and tty:
            self.fd = sys.stdin.fileno()
            self._old_settings = termios.tcgetattr(self.fd)
            tty.setcbreak(self.fd)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001, D401 - стандартний протокол
        if self.fd is not None and self._old_settings is not None and termios:
            tcsetattr = getattr(termios, "tcsetattr", None)
            tcsadrain = getattr(termios, "TCSADRAIN", None)
            if callable(tcsetattr) and tcsadrain is not None:
                tcsetattr(self.fd, tcsadrain, self._old_settings)
            self.fd = None
            self._old_settings = None

    def poll(self) -> Optional[str]:
        if msvcrt is not None:  # pragma: no cover - відсутній на Linux
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch == "\r":
                    return "\n"
                return ch
            return None
        if self.fd is None:
            return None
        try:
            ready, _, _ = select.select([sys.stdin], [], [], 0)
        except Exception:  # noqa: BLE001
            return None
        if not ready:
            return None
        try:
            data = os.read(self.fd, 1)
        except OSError:
            return None
        if not data:
            return None
        return data.decode("utf-8", "ignore")


def handle_keypress(key: str, state: ViewerState) -> bool:
    """Обробляє натискання клавіш. Повертає True, якщо треба завершити роботу."""

    normalized = key.lower()
    if normalized in {"q", "\u0003"}:  # q або Ctrl+C
        state.note_action("Завершення за запитом")
        return True
    if normalized in {"p", " "}:
        state.toggle_pause()
        return False
    if normalized == "c":
        state.clear_timeline()
        return False
    mode_map = {
        "0": DashboardMode.TIMELINE,
        "1": DashboardMode.SUMMARY,
        "2": DashboardMode.SESSION,
        "3": DashboardMode.ALERTS,
        "4": DashboardMode.STREAMS,
        "5": DashboardMode.PRICE,
        "6": DashboardMode.SUPERVISOR,
    }
    if normalized in mode_map:
        new_mode = mode_map[normalized]
        state.active_mode = new_mode
        state.note_action(f"Mode → {new_mode.name}")
        return False
    return False


def _redis_status_color(status: str) -> str:
    status_lower = status.lower()
    if status_lower == "ok":
        return "green"
    if status_lower == "error":
        return "red"
    return "yellow"


def _format_ms(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{value:.1f} ms"


def _format_bytes(value: Optional[int]) -> str:
    if value is None:
        return "—"
    size = float(value)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    idx = 0
    while size >= 1024.0 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    return f"{size:.1f} {units[idx]}"


def refresh_time_alerts(state: ViewerState) -> bool:
    changed = False
    if state.last_heartbeat is None:
        changed |= state._set_alert("heartbeat_missing", True, "Очікуємо перший heartbeat…", severity="info")
    else:
        changed |= state._set_alert("heartbeat_missing", False, None)
    hb_age = state.heartbeat_age()
    if hb_age is not None:
        changed |= state._set_alert(
            "heartbeat_stale",
            hb_age >= HEARTBEAT_ALERT_SECONDS,
            f"Heartbeat затримався на {hb_age:.0f} с",
            severity="danger",
        )
    else:
        changed |= state._set_alert("heartbeat_stale", False, None)
    return changed


def refresh_ohlcv_alerts(state: ViewerState) -> bool:
    changed = False
    now = time.time()
    if state.last_ohlcv_ts is None:
        changed |= state._set_alert("ohlcv_missing", True, "Очікуємо перший OHLCV payload…", severity="info")
        state._track_issue_state("ohlcv_msg_idle", False, None)
        state._track_issue_state("ohlcv_lag", False, None)
        state._track_issue_state("ohlcv_empty", False, None)
        return changed

    changed |= state._set_alert("ohlcv_missing", False, None)

    if not state.ohlcv_targets:
        state._track_issue_state("ohlcv_msg_idle", False, None)
        state._track_issue_state("ohlcv_lag", False, None)
        state._track_issue_state("ohlcv_empty", False, None)
        changed |= state._set_alert("ohlcv_msg_idle", False, None)
        changed |= state._set_alert("ohlcv_lag", False, None)
        changed |= state._set_alert("ohlcv_empty", False, None)
        return changed

    market_open = str((state.last_market_status or {}).get("state", "")).lower() == "open"
    idle_entries: List[Tuple[str, str, float, float]] = []
    lag_entries: List[Tuple[str, str, float]] = []

    for key, payload in state.ohlcv_targets.items():
        symbol = payload.get("symbol", "?")
        tf = payload.get("tf", "?")
        updated_ts = state.ohlcv_updated.get(key)
        msg_age = max(0.0, now - updated_ts) if updated_ts else None
        last_close_ms = _coerce_int(payload.get("last_close_ms"))
        lag_seconds = None
        if last_close_ms is not None:
            lag_seconds = max(0.0, now - (last_close_ms / 1000.0))
        _update_ohlcv_metrics(symbol, tf, lag_seconds, msg_age)

        warn_threshold, error_threshold = resolve_idle_thresholds(tf)
        idle_active = msg_age is not None and msg_age >= warn_threshold
        if idle_active and msg_age is not None:
            idle_entries.append((symbol, tf, msg_age, error_threshold))
        previous_idle = state.ohlcv_idle_flags.get(key, False)
        if idle_active:
            state.ohlcv_idle_flags[key] = True
        else:
            state.ohlcv_idle_flags.pop(key, None)
        if idle_active and not previous_idle:
            _increment_ohlcv_gap(symbol, tf)

        if lag_seconds is None:
            lag_active = False
        else:
            lag_active = market_open and lag_seconds >= OHLCV_LAG_WARN_SECONDS
            if lag_active:
                lag_entries.append((symbol, tf, lag_seconds))
        previous_lag = state.ohlcv_lag_flags.get(key, False)
        if lag_active:
            state.ohlcv_lag_flags[key] = True
        else:
            state.ohlcv_lag_flags.pop(key, None)
        if lag_active and not previous_lag:
            _increment_ohlcv_gap(symbol, tf)

    idle_entries.sort(key=lambda item: item[2], reverse=True)
    lag_entries.sort(key=lambda item: item[2], reverse=True)

    if idle_entries:
        worst_idle = idle_entries[0]
        detail_idle = f"{worst_idle[0]} {worst_idle[1]} msg age {worst_idle[2]:.0f}s"
        severity_idle = "danger" if worst_idle[2] >= worst_idle[3] else "warning"
        summary_idle = ", ".join(
            f"{sym} {tf} {age:.0f}s" for sym, tf, age, _ in idle_entries[:3]
        )
        state._track_issue_state("ohlcv_msg_idle", True, detail_idle)
        changed |= state._set_alert(
            "ohlcv_msg_idle",
            True,
            f"Немає нових OHLCV {summary_idle}",
            severity=severity_idle,
        )
    else:
        state._track_issue_state("ohlcv_msg_idle", False, None)
        changed |= state._set_alert("ohlcv_msg_idle", False, None)

    if lag_entries:
        worst_lag = lag_entries[0]
        detail_lag = f"{worst_lag[0]} {worst_lag[1]} lag {worst_lag[2]:.0f}s"
        severity_lag = "danger" if worst_lag[2] >= OHLCV_LAG_ERROR_SECONDS else "warning"
        summary_lag = ", ".join(
            f"{sym} {tf} {lag:.0f}s" for sym, tf, lag in lag_entries[:3]
        )
        state._track_issue_state("ohlcv_lag", True, detail_lag)
        changed |= state._set_alert(
            "ohlcv_lag",
            True,
            f"OHLCV лаг: {summary_lag}",
            severity=severity_lag,
        )
    else:
        state._track_issue_state("ohlcv_lag", False, None)
        changed |= state._set_alert("ohlcv_lag", False, None)

    zero_entries = list(state.ohlcv_zero_bars_since.items())
    zero_entries.sort(key=lambda item: item[1], reverse=True)
    if zero_entries:
        keys = [
            f"{sym} {tf} ({int(now - since)}s)"
            for (sym, tf), since in zero_entries[:3]
        ]
        detail_zero = ", ".join(keys)
        state._track_issue_state("ohlcv_empty", True, detail_zero)
        changed |= state._set_alert(
            "ohlcv_empty",
            True,
            f"Порожні OHLCV payload: {detail_zero}",
            severity="warning",
        )
    else:
        state._track_issue_state("ohlcv_empty", False, None)
        changed |= state._set_alert("ohlcv_empty", False, None)

    return changed


def refresh_redis_health(redis_client: "redis.Redis", state: ViewerState) -> None:  # type: ignore[name-defined]
    started = time.perf_counter()
    payload: Dict[str, Any]
    try:
        redis_client.ping()
        latency_ms = (time.perf_counter() - started) * 1000.0
        stats = redis_client.info()  # single round-trip for detailed metrics
        payload = {
            "status": "ok",
            "latency_ms": latency_ms,
            "checked_at": time.time(),
            "connected_clients": stats.get("connected_clients"),
            "blocked_clients": stats.get("blocked_clients"),
            "used_memory": stats.get("used_memory"),
            "used_memory_human": stats.get("used_memory_human"),
            "used_memory_rss_human": stats.get("used_memory_rss_human"),
            "redis_version": stats.get("redis_version"),
            "role": stats.get("role"),
            "uptime_in_seconds": stats.get("uptime_in_seconds"),
        }
    except Exception as exc:  # noqa: BLE001
        payload = {
            "status": "error",
            "latency_ms": None,
            "checked_at": time.time(),
            "error": str(exc),
        }
    state.note_redis_health(payload)


def _format_epoch_ms(ms_value: Any) -> str:
    ms = _coerce_int(ms_value)
    if ms is None:
        return "—"
    dt_value = dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc)
    return f"{ms} ({dt_value.isoformat()})"


def _format_epoch_ms_compact(ms_value: Any) -> str:
    ms = _coerce_int(ms_value)
    if ms is None:
        return "—"
    dt_value = dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc)
    return dt_value.strftime("%m-%d %H:%M:%S")


def _update_ohlcv_metrics(symbol: str, tf: str, lag_seconds: Optional[float], msg_age: Optional[float]) -> None:
    if PROM_VIEWER_OHLCV_LAG_SECONDS is not None and lag_seconds is not None:
        PROM_VIEWER_OHLCV_LAG_SECONDS.labels(symbol=symbol, tf=tf).set(lag_seconds)
    if PROM_VIEWER_OHLCV_MSG_AGE_SECONDS is not None and msg_age is not None:
        PROM_VIEWER_OHLCV_MSG_AGE_SECONDS.labels(symbol=symbol, tf=tf).set(max(0.0, msg_age))


def _increment_ohlcv_gap(symbol: str, tf: str) -> None:
    if PROM_VIEWER_OHLCV_GAPS_TOTAL is not None:
        PROM_VIEWER_OHLCV_GAPS_TOTAL.labels(symbol=symbol, tf=tf).inc()


def _compute_next_open_seconds(
    heartbeat: Dict[str, Any],
    session: Dict[str, Any],
    resolved_next_open: Optional[str],
) -> Optional[float]:
    candidate = _coerce_float(heartbeat.get("next_open_seconds"))
    if candidate and candidate > 0:
        return candidate
    candidate = _coerce_float(session.get("next_open_seconds"))
    if candidate and candidate > 0:
        return candidate
    next_open_iso = resolved_next_open or session.get("next_open_utc") or heartbeat.get("next_open_utc")
    if next_open_iso:
        try:
            target = _iso_to_epoch(next_open_iso)
            return max(0.0, target - time.time())
        except Exception:
            return None
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FXCM console viewer")
    parser.add_argument("--redis-host", default=os.environ.get("FXCM_REDIS_HOST", "127.0.0.1"))
    parser.add_argument("--redis-port", default=int(os.environ.get("FXCM_REDIS_PORT", "6379")), type=int)
    parser.add_argument("--redis-password", default=os.environ.get("FXCM_REDIS_PASSWORD"))
    parser.add_argument("--heartbeat-channel", default=HEARTBEAT_CHANNEL_DEFAULT)
    parser.add_argument("--market-status-channel", default=MARKET_STATUS_CHANNEL_DEFAULT)
    parser.add_argument("--ohlcv-channel", default=OHLCV_CHANNEL_DEFAULT, help="канал OHLCV (порожній рядок = вимкнено)")
    parser.add_argument("--refresh", default=2.0, type=float, help="частота оновлення UI (сек)")
    return parser.parse_args()


def build_summary_panel(state: ViewerState) -> Panel:
    table = Table.grid(padding=1)
    table.add_column(justify="right", style="bold cyan", overflow="fold")
    table.add_column(overflow="fold")

    heartbeat = state.last_heartbeat or {}
    context = heartbeat.get("context") or {}
    session = context.get("session") or {}
    market_status = state.last_market_status or {}

    table.add_row("Heartbeat state", heartbeat.get("state", "—"))
    table.add_row("Last close", _format_epoch_ms(heartbeat.get("last_bar_close_ms")))

    lag = context.get("lag_seconds")
    table.add_row("Lag seconds", _format_float(_coerce_float(lag)))

    idle_reason = context.get("idle_reason")
    if idle_reason:
        table.add_row("Idle reason", idle_reason)

    next_open = session.get("next_open_utc") or heartbeat.get("next_open_utc")
    if next_open:
        table.add_row("Next open", next_open)

    next_open_sec = _compute_next_open_seconds(heartbeat, session, next_open)
    if next_open_sec is not None:
        table.add_row("Next open (sec)", f"{next_open_sec:.0f}")

    market_state = market_status.get("state")
    if market_state:
        table.add_row("Market status", market_state)

    hb_age = state.heartbeat_age()
    if hb_age is not None:
        table.add_row("Heartbeat age", _format_duration(hb_age))

    table.add_row("Cycle seconds", _format_float(_coerce_float(context.get("cycle_seconds")), digits=2))
    table.add_row("Monitor mode", state.active_mode.name)
    table.add_row("Last action", state.last_action or "—")

    return Panel(table, title="FXCM summary", border_style="cyan", box=box.ROUNDED)


def build_fxcm_diag_panel(state: ViewerState) -> Panel:
    heartbeat = state.last_heartbeat or {}
    context = heartbeat.get("context") or {}
    if not context:
        return Panel(
            "Очікуємо контекст heartbeat…",
            title="FXCM diagnostics",
            border_style="yellow",
            box=box.ROUNDED,
        )

    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold cyan", justify="right", overflow="fold")
    table.add_column(overflow="fold")

    diag_rows = [
        ("Mode", context.get("mode", "—")),
        ("Heartbeat ts", _format_diag_timestamp(heartbeat.get("ts"))),
        ("Lag", _format_diag_duration(context.get("lag_seconds"))),
        ("Cycle", _format_diag_duration(context.get("cycle_seconds"))),
        ("Published bars", context.get("published_bars", "—")),
        ("Poll seconds", _format_diag_duration(context.get("poll_seconds"))),
        (
            "Publish interval",
            _format_diag_duration(context.get("publish_interval_seconds")),
        ),
        ("Lookback (min)", context.get("lookback_minutes", "—")),
        ("Redis connected", _format_bool(context.get("redis_connected"))),
        ("Redis required", _format_bool(context.get("redis_required"))),
        ("Redis channel", context.get("redis_channel", "—")),
        ("Cache enabled", _format_bool(context.get("cache_enabled"))),
    ]
    idle_reason = context.get("idle_reason")
    if idle_reason:
        diag_rows.append(("Idle reason", idle_reason))
    next_open_seconds = _coerce_float(context.get("next_open_seconds"))
    if next_open_seconds is not None:
        diag_rows.append(("Next open (s)", f"{next_open_seconds:.0f}"))
    tick_silence = _coerce_float(context.get("tick_silence_seconds"))
    if tick_silence is not None:
        diag_rows.append(("Tick silence (s)", f"{tick_silence:.1f}"))
    stream_targets = context.get("stream_targets")
    if isinstance(stream_targets, list):
        diag_rows.append(("Targets tracked", len(stream_targets)))

    for label, value in diag_rows:
        table.add_row(label, str(value))

    return Panel(table, title="FXCM diagnostics", border_style="cyan", box=box.ROUNDED)


def build_incidents_panel(state: ViewerState) -> Panel:
    incidents = list(state.incident_feed)
    if not incidents:
        return Panel(
            "Інцидентів поки немає",
            title="Recent incidents",
            border_style="green",
            box=box.ROUNDED,
        )

    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("When", justify="right", overflow="fold")
    table.add_column("Issue", style="bold", overflow="fold")
    table.add_column("State", justify="center", overflow="fold")
    table.add_column("Detail", overflow="fold")

    for entry in incidents[:8]:
        ts_label = _format_diag_timestamp(entry.get("ts"))
        label = entry.get("label") or entry.get("key") or "—"
        active = bool(entry.get("active"))
        state_text = Text(
            "ACTIVE" if active else "CLEAR",
            style=f"bold {'red' if active else 'green'}",
        )
        detail = entry.get("detail") or "—"
        table.add_row(ts_label, label, state_text, detail)

    border = "red" if incidents and incidents[0].get("active") else "cyan"
    return Panel(table, title="Recent incidents", border_style=border, box=box.ROUNDED)


def build_menu_bar() -> Panel:
    return Panel(Text(MENU_TEXT, justify="center", style="bold"), box=box.SIMPLE, border_style="dim")

def build_alerts_panel(state: ViewerState) -> Panel:
    alerts = state.active_alerts()
    if not alerts:
        return Panel("Активних алертів немає", title="Alerts", border_style="green", box=box.ROUNDED)

    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Severity", justify="left", style="bold", overflow="fold")
    table.add_column("Since", justify="right", overflow="fold")
    table.add_column("Message", overflow="fold")

    for alert in alerts:
        color = ALERT_SEVERITY_COLORS.get(alert.severity, "white")
        label = Text(alert.severity.upper(), style=f"bold {color}")
        table.add_row(label, _format_utc_timestamp(alert.since_ts), alert.message)

    border = ALERT_SEVERITY_COLORS.get(alerts[0].severity, "yellow")
    return Panel(table, title="Alerts", border_style=border, box=box.ROUNDED)

def build_issue_panel(state: ViewerState) -> Panel:
    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Issue", style="bold", overflow="fold")
    table.add_column("Count", justify="right", overflow="fold")
    table.add_column("Last seen", justify="right", overflow="fold")
    table.add_column("Details", overflow="fold")

    has_activity = False
    for key, label in ISSUE_LABELS.items():
        entry = state.issue_counters.get(key)
        count = entry.get("count", 0) if entry else 0
        if count:
            has_activity = True
        last_ts = _coerce_float(entry.get("last_ts")) if entry else None
        detail = entry.get("detail") if entry else None
        table.add_row(
            label,
            str(count),
            _format_utc_timestamp(last_ts),
            detail or "—",
        )

    border = "red" if has_activity else "green"
    return Panel(table, title="Issue counters", border_style=border, box=box.ROUNDED)


_QUEUE_DEPTH_KEYS = ("size", "depth", "pending", "queued", "count")
_QUEUE_MAX_KEYS = ("maxsize", "capacity", "limit")
_QUEUE_PROCESSED_KEYS = ("processed", "published", "sent", "ok", "success")
_QUEUE_DROPPED_KEYS = ("dropped", "failed", "errors", "rejected")
_QUEUE_AGE_KEYS = ("age_seconds", "idle_seconds", "oldest_age_seconds")
_QUEUE_TS_KEYS = ("last_enqueue_ts", "last_event_ts", "updated_ts")

_TASK_STATE_KEYS = ("state", "status")
_TASK_PROCESSED_KEYS = ("processed", "published", "ok", "success", "handled")
_TASK_ERROR_KEYS = ("errors", "failed", "exceptions")
_TASK_IDLE_KEYS = ("idle_seconds", "lag_seconds", "age_seconds")
_TASK_TS_KEYS = ("last_processed_ts", "last_success_ts", "last_event_ts")


def _extract_numeric(entry: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[float]:
    for key in keys:
        if key in entry:
            parsed = _coerce_float(entry.get(key))
            if parsed is not None:
                return parsed
    return None


def _extract_timestamp(entry: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[float]:
    for key in keys:
        if key in entry:
            ts_val = _normalize_epoch_seconds(entry.get(key))
            if ts_val is not None:
                return ts_val
    return None


def _resolve_entry_name(entry: Dict[str, Any], fallback: str) -> str:
    for key in ("name", "queue", "task", "key"):
        value = entry.get(key)
        if value:
            return str(value)
    return fallback


def _supervisor_queue_entries(diag: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = diag.get("queues") or diag.get("queue")
    entries: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        for name, info in payload.items():
            if isinstance(info, dict):
                entry = dict(info)
                entry.setdefault("name", str(name))
                entries.append(entry)
    elif isinstance(payload, list):
        for idx, info in enumerate(payload):
            if isinstance(info, dict):
                entry = dict(info)
                entry.setdefault("name", _resolve_entry_name(entry, f"queue_{idx}"))
                entries.append(entry)
    return entries


def _supervisor_task_entries(diag: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = diag.get("tasks") or diag.get("workers") or diag.get("consumers")
    entries: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        for name, info in payload.items():
            if isinstance(info, dict):
                entry = dict(info)
                entry.setdefault("name", str(name))
                entries.append(entry)
    elif isinstance(payload, list):
        for idx, info in enumerate(payload):
            if isinstance(info, dict):
                entry = dict(info)
                entry.setdefault("name", _resolve_entry_name(entry, f"task_{idx}"))
                entries.append(entry)
    return entries


def _task_state_text(value: Any) -> Text:
    label = str(value or "?").lower()
    style = "white"
    if label in {"running", "active", "ok"}:
        style = "green"
    elif label in {"idle", "waiting"}:
        style = "yellow"
    elif label in {"error", "failed", "stopped"}:
        style = "red"
    return Text(label or "?", style=f"bold {style}" if style != "white" else style)


def build_supervisor_summary_panel(
    state: ViewerState,
    diag: Dict[str, Any],
    queues: List[Dict[str, Any]],
    tasks: List[Dict[str, Any]],
) -> Panel:
    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold cyan", justify="right", overflow="fold")
    table.add_column(overflow="fold")

    rows: List[Tuple[str, str]] = []
    rows.append(("Стан", str(diag.get("state", "—"))))
    rows.append(("Loop", _format_bool(diag.get("loop_alive"))))
    backpressure = diag.get("backpressure")
    if backpressure is None:
        backpressure = diag.get("backpressure_active")
    rows.append(("Backpressure", _format_bool(backpressure)))
    uptime = _coerce_float(diag.get("uptime_seconds"))
    if uptime is None and diag.get("started_at") is not None:
        started = _normalize_epoch_seconds(diag.get("started_at"))
        if started is not None:
            uptime = max(0.0, time.time() - started)
    rows.append(("Uptime", _format_duration(uptime)))
    last_publish = diag.get("last_publish_ts") or diag.get("last_success_ts")
    if last_publish is not None:
        rows.append(("Остання публікація", _format_diag_timestamp(last_publish)))
    if state.supervisor_updated is not None:
        rows.append(("Оновлено", _format_short_duration(max(0.0, time.time() - state.supervisor_updated))))
    rows.append(("Черги", str(len(queues))))
    rows.append(("Таски", str(len(tasks))))
    last_error = diag.get("last_error") or diag.get("error")
    if last_error:
        rows.append(("Остання помилка", _shorten_text(last_error)))

    for label, value in rows:
        table.add_row(label, value)

    return Panel(table, title="Async supervisor", border_style="cyan", box=box.ROUNDED)


def build_supervisor_queues_panel(queues: List[Dict[str, Any]]) -> Panel:
    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Черга", style="bold", overflow="fold")
    table.add_column("Depth", justify="right", overflow="fold")
    table.add_column("Max", justify="right", overflow="fold")
    table.add_column("Заповнення", justify="right", overflow="fold")
    table.add_column("Вік", justify="right", overflow="fold")
    table.add_column("Віпрацьовано", justify="right", overflow="fold")
    table.add_column("Відкинуто", justify="right", overflow="fold")

    if not queues:
        table.add_row("—", "—", "—", Text("—", style="dim"), "—", "—", "—")
        return Panel(table, title="Supervisor: черги", border_style="yellow", box=box.ROUNDED)

    def _queue_depth(entry: Dict[str, Any]) -> Optional[float]:
        return _extract_numeric(entry, _QUEUE_DEPTH_KEYS)

    sorted_entries = sorted(queues, key=lambda item: _queue_depth(item) or 0.0, reverse=True)
    for entry in sorted_entries:
        name = _resolve_entry_name(entry, "queue")
        depth = _queue_depth(entry)
        capacity = _extract_numeric(entry, _QUEUE_MAX_KEYS)
        usage = None
        if depth is not None and capacity is not None and capacity > 0:
            usage = depth / capacity
        age_seconds = _extract_numeric(entry, _QUEUE_AGE_KEYS)
        age_label = _format_age_label(age_seconds, _extract_timestamp(entry, _QUEUE_TS_KEYS))
        processed = _extract_numeric(entry, _QUEUE_PROCESSED_KEYS)
        dropped = _extract_numeric(entry, _QUEUE_DROPPED_KEYS)
        table.add_row(
            name,
            _format_int_value(depth),
            _format_int_value(capacity),
            _format_usage_text(usage),
            age_label,
            _format_int_value(processed),
            _format_int_value(dropped),
        )

    return Panel(table, title="Supervisor: черги", border_style="magenta", box=box.ROUNDED)


def build_supervisor_tasks_panel(tasks: List[Dict[str, Any]]) -> Panel:
    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Таск", style="bold", overflow="fold")
    table.add_column("Стан", justify="left", overflow="fold")
    table.add_column("Віпрацьовано", justify="right", overflow="fold")
    table.add_column("Помилки", justify="right", overflow="fold")
    table.add_column("Простій", justify="right", overflow="fold")
    table.add_column("Остання помилка", overflow="fold")

    if not tasks:
        table.add_row("—", "—", "—", "—", "—", "—")
        return Panel(table, title="Supervisor: таски", border_style="yellow", box=box.ROUNDED)

    for entry in tasks:
        name = _resolve_entry_name(entry, "task")
        state_text = _task_state_text(entry.get("state") or entry.get("status"))
        processed = _extract_numeric(entry, _TASK_PROCESSED_KEYS)
        errors = _extract_numeric(entry, _TASK_ERROR_KEYS)
        idle_seconds = _extract_numeric(entry, _TASK_IDLE_KEYS)
        idle_label = _format_age_label(idle_seconds, _extract_timestamp(entry, _TASK_TS_KEYS))
        last_error = entry.get("last_error") or entry.get("error") or entry.get("error_message")
        table.add_row(
            name,
            state_text,
            _format_int_value(processed),
            _format_int_value(errors),
            idle_label,
            _shorten_text(last_error),
        )

    return Panel(table, title="Supervisor: таски", border_style="green", box=box.ROUNDED)


def build_supervisor_metrics_panel(diag: Dict[str, Any]) -> Panel:
    totals_table = Table.grid(padding=(0, 1))
    totals_table.add_column(style="bold cyan", justify="right", overflow="fold")
    totals_table.add_column(justify="right", overflow="fold")
    totals_table.add_column(style="dim", overflow="fold")

    metrics_block = diag.get("metrics") if isinstance(diag.get("metrics"), dict) else None
    metric_rows = (
        ("Поставлено", "total_enqueued"),
        ("Віпрацьовано", "total_processed"),
        ("Відкинуто", "total_dropped"),
        ("Сумарний depth", "queue_depth_total"),
        ("Публікацій загалом", "publishes_total"),
        ("Активні таски", "active_tasks"),
        ("Помилки тасків", "task_errors"),
    )
    if metrics_block:
        for label, key in metric_rows:
            value = _format_int_value(_coerce_float(metrics_block.get(key)))
            totals_table.add_row(label, value, SUPERVISOR_METRIC_HINTS.get(key, ""))
    else:
        totals_table.add_row("Агреговані лічильники", "—", "")

    backpressure_events = _coerce_float(diag.get("backpressure_events"))
    totals_table.add_row(
        "Backpressure подій",
        _format_int_value(backpressure_events),
        SUPERVISOR_METRIC_HINTS.get("backpressure_events", ""),
    )

    publish_table = _build_publish_counts_table(diag)
    special_panel = build_supervisor_special_channels_panel(diag)

    group_items: List[Any] = [totals_table, Text("")]
    if special_panel is not None:
        group_items.extend([special_panel, Text("")])
    group_items.append(publish_table)
    body = Group(*group_items)
    return Panel(body, title="Supervisor: метрики", border_style="cyan", box=box.ROUNDED)


def _build_publish_counts_table(diag: Dict[str, Any]) -> Table:
    table = Table(box=box.MINIMAL_DOUBLE_HEAD, expand=True)
    table.add_column("Канал", style="bold", overflow="fold")
    table.add_column("Публікацій", justify="right", overflow="fold")
    publish_counts = diag.get("publish_counts") if isinstance(diag.get("publish_counts"), dict) else None
    if publish_counts:
        sorted_counts = sorted(publish_counts.items(), key=lambda item: item[1], reverse=True)
        for name, count in sorted_counts[:8]:
            table.add_row(str(name), _format_int_value(_coerce_float(count)))
        if len(sorted_counts) > 8:
            remainder = sum(value for _, value in sorted_counts[8:])
            table.add_row(f"Інші ({len(sorted_counts) - 8})", _format_int_value(_coerce_float(remainder)))
    else:
        table.add_row("—", "—")
    return table


def build_supervisor_special_channels_panel(diag: Dict[str, Any]) -> Optional[Panel]:
    if not SUPERVISOR_CHANNELS:
        return None
    publish_counts = diag.get("publish_counts") if isinstance(diag.get("publish_counts"), dict) else None
    if not publish_counts:
        return None
    table = Table(box=box.SIMPLE_HEAD, expand=True)
    table.add_column("Спеціальний канал", style="bold", overflow="fold")
    table.add_column("Публікацій", justify="right", overflow="fold")
    table.add_column("Опис", style="dim", overflow="fold")
    has_rows = False
    for channel in SUPERVISOR_CHANNELS:
        count_value = _coerce_float(publish_counts.get(channel)) if publish_counts else None
        if count_value is None:
            count_value = 0.0
        table.add_row(
            str(channel),
            _format_int_value(count_value),
            SUPERVISOR_CHANNEL_HINTS.get(channel) or _resolve_channel_hint(channel),
        )
        has_rows = True
    if not has_rows:
        return None
    return Panel(table, title="Спеціальні канали", border_style="cyan", box=box.ROUNDED)


def build_supervisor_mode(state: ViewerState) -> Union[Layout, Panel]:
    diag = state.supervisor_diag
    if not diag:
        hint = "Heartbeat поки не містить supervisor-контекст. Перевір, чи увімкнено async_supervisor."
        return Panel(hint, title="Async supervisor", border_style="yellow", box=box.ROUNDED)

    queues = _supervisor_queue_entries(diag)
    tasks = _supervisor_task_entries(diag)

    layout = Layout(name="supervisor_mode")
    layout.split_column(
        Layout(name="sup_summary_row", size=11, minimum_size=8),
        Layout(name="sup_tables", ratio=1, minimum_size=8),
    )
    summary_row = Layout(name="sup_summary_layout")
    summary_row.split_row(
        Layout(name="sup_summary", ratio=1, minimum_size=8),
        Layout(name="sup_metrics", ratio=1, minimum_size=8),
    )
    summary_row["sup_summary"].update(build_supervisor_summary_panel(state, diag, queues, tasks))
    summary_row["sup_metrics"].update(build_supervisor_metrics_panel(diag))
    layout["sup_summary_row"].update(summary_row)

    tables = Layout(name="sup_tables_layout")
    tables.split_row(
        Layout(name="sup_queues", ratio=1, minimum_size=8),
        Layout(name="sup_tasks", ratio=1, minimum_size=8),
    )
    tables["sup_queues"].update(build_supervisor_queues_panel(queues))
    tables["sup_tasks"].update(build_supervisor_tasks_panel(tasks))
    layout["sup_tables"].update(tables)
    return layout


def build_redis_panel(state: ViewerState) -> Panel:
    health = state.redis_health
    if not health:
        return Panel("Очікуємо ping/info…", title="Redis health", border_style="yellow", box=box.ROUNDED)

    table = Table.grid(padding=(0, 1))
    table.add_column(style="bold cyan", justify="right", overflow="fold")
    table.add_column(overflow="fold")

    status = health.get("status", "unknown")
    table.add_row("Status", status)
    table.add_row("Latency", _format_ms(health.get("latency_ms")))
    table.add_row("Checked", _format_utc_timestamp(_coerce_float(health.get("checked_at"))))

    clients = health.get("connected_clients")
    blocked = health.get("blocked_clients")
    clients_label = "—"
    if clients is not None:
        clients_label = str(clients)
        if blocked:
            clients_label += f" (+{blocked} blocked)"
    table.add_row("Clients", clients_label)

    memory_label = health.get("used_memory_human") or _format_bytes(_coerce_int(health.get("used_memory")))
    if health.get("used_memory_rss_human"):
        memory_label = f"{memory_label} / RSS {health['used_memory_rss_human']}"
    table.add_row("Memory", memory_label)

    version_label = health.get("redis_version", "—")
    if health.get("role"):
        version_label = f"{version_label} ({health['role']})"
    table.add_row("Version", version_label)

    uptime = _coerce_float(health.get("uptime_in_seconds"))
    table.add_row("Uptime", _format_duration(uptime) if uptime is not None else "—")

    error = health.get("error")
    if error:
        table.add_row("Error", error)

    border = _redis_status_color(status)
    return Panel(table, title="Redis health", border_style=border, box=box.ROUNDED)


def build_stream_targets_panel(state: ViewerState) -> Panel:
    heartbeat = state.last_heartbeat or {}
    context = heartbeat.get("context") or {}
    targets = context.get("stream_targets") or []
    entries: List[Dict[str, Any]] = []
    now_ts = time.time()
    for target in targets:
        symbol = target.get("symbol", "?")
        tf = target.get("tf", "?")
        staleness = _coerce_float(target.get("staleness_seconds"))
        ms_value = staleness * 1000.0 if staleness is not None else None
        spark = _render_sparkline(state.staleness_history.get((symbol, tf)))
        updated_ts = state.stream_target_updated.get((symbol, tf))
        updated_age = max(0.0, now_ts - updated_ts) if updated_ts else None
        entries.append(
            {
                "symbol": symbol,
                "tf": tf,
                "staleness": staleness,
                "ms": ms_value,
                "spark": spark,
                "updated": updated_age,
            }
        )

    entries.sort(key=lambda item: item.get("staleness") or -1.0, reverse=True)

    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Symbol", style="bold", overflow="fold")
    table.add_column("TF", overflow="fold")
    table.add_column("Staleness (s)", justify="right", overflow="fold")
    table.add_column("Staleness (ms)", justify="right", overflow="fold")
    table.add_column("Trend", justify="left", overflow="fold")
    table.add_column("Updated", justify="right", overflow="fold")
    if not entries:
        table.add_row("—", "—", "—", "—", "—", "—")
    else:
        for item in entries:
            table.add_row(
                item["symbol"],
                item["tf"],
                f"{item['staleness']:.1f}" if item["staleness"] is not None else "—",
                _format_float(item.get("ms"), digits=0),
                item["spark"],
                _format_duration(item.get("updated")),
            )

    return Panel(table, title="Stream targets", border_style="magenta")


def build_price_stream_panel(state: ViewerState) -> Panel:
    data = state.price_stream or {}
    symbols_state = data.get("symbols_state") if isinstance(data, dict) else None
    updated_age = None
    if state.price_stream_updated is not None:
        updated_age = max(0.0, time.time() - state.price_stream_updated)
    if not data or not isinstance(symbols_state, list):
        hint = "Очікуємо price_stream у heartbeat…"
        if updated_age is not None:
            hint = f"Price stream не оновлювався {updated_age:.0f} с"
        return Panel(hint, title="Live FXCM price", border_style="yellow", box=box.ROUNDED)

    summary = Table.grid(padding=(0, 1))
    summary.add_column(style="bold cyan", justify="right", overflow="fold")
    summary.add_column(overflow="fold")

    state_label = str(data.get("state", "?")).lower()
    state_color = {
        "ok": "green",
        "stream": "green",
        "waiting": "yellow",
        "stale": "red",
        "stopped": "red",
    }.get(state_label, "cyan")
    summary.add_row("State", Text(state_label or "?", style=f"bold {state_color}"))
    silence = _coerce_float(data.get("tick_silence_seconds"))
    if silence is None:
        silence = state.tick_silence_seconds
    summary.add_row("Tick silence", _format_diag_duration(silence))
    summary.add_row("Channel", data.get("channel", "—"))
    summary.add_row("Interval", _format_diag_duration(data.get("interval_seconds")))
    summary.add_row("Queue depth", str(data.get("queue_depth", "—")))
    summary.add_row("Thread", _format_bool(data.get("thread_alive")))
    summary.add_row("Last snap", _format_diag_timestamp(data.get("last_snap_ts")))
    summary.add_row("Last tick", _format_diag_timestamp(data.get("last_tick_ts")))
    if updated_age is not None:
        summary.add_row("Updated", _format_diag_duration(updated_age))

    rows_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    rows_table.add_column("Symbol", style="bold", overflow="fold")
    rows_table.add_column("Mid", justify="right", overflow="fold")
    rows_table.add_column("Bid", justify="right", overflow="fold")
    rows_table.add_column("Ask", justify="right", overflow="fold")
    rows_table.add_column("Tick age", justify="right", overflow="fold")

    entries: List[Dict[str, Any]] = []
    for entry in symbols_state:
        if not isinstance(entry, dict):
            continue
        symbol = entry.get("symbol")
        if not symbol:
            continue
        entries.append(
            {
                "symbol": symbol,
                "mid": _coerce_float(entry.get("mid")),
                "bid": _coerce_float(entry.get("bid")),
                "ask": _coerce_float(entry.get("ask")),
                "age": _coerce_float(entry.get("tick_age_seconds")),
            }
        )
    entries.sort(key=lambda item: item.get("symbol") or "")

    if not entries:
        rows_table.add_row("—", "—", "—", "—", "—")
    else:
        for item in entries:
            rows_table.add_row(
                str(item["symbol"]),
                _format_float(item.get("mid"), digits=5),
                _format_float(item.get("bid"), digits=5),
                _format_float(item.get("ask"), digits=5),
                _format_diag_duration(item.get("age")),
            )

    extras: List[Any] = [summary, rows_table]
    special_panel = build_supervisor_special_channels_panel(state.supervisor_diag)
    if special_panel is not None:
        extras.append(special_panel)
    content = Group(*extras)
    return Panel(content, title="Live FXCM price", border_style="green", box=box.ROUNDED)


def build_ohlcv_panel(state: ViewerState) -> Panel:
    if not state.ohlcv_targets:
        return Panel("Очікуємо OHLCV payload…", title="OHLCV channel", border_style="yellow", box=box.ROUNDED)

    now_ts = time.time()
    entries: List[Dict[str, Any]] = []
    for key, payload in state.ohlcv_targets.items():
        last_close_ms = payload.get("last_close_ms")
        lag = None
        if last_close_ms is not None:
            lag = max(0.0, now_ts - (last_close_ms / 1000.0))
        updated = state.ohlcv_updated.get(key)
        msg_age = max(0.0, now_ts - updated) if updated else None
        entries.append(
            {
                "symbol": payload.get("symbol", "?"),
                "tf": payload.get("tf", "?"),
                "bars": payload.get("bars_per_msg", 0),
                "last_close_ms": last_close_ms,
                "lag": lag,
                "msg_age": msg_age,
                "spark": _render_sparkline(state.ohlcv_lag_history.get(key)),
            }
        )

    entries.sort(key=lambda item: item.get("lag") or -1.0, reverse=True)

    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Symbol", style="bold", overflow="fold")
    table.add_column("TF", overflow="fold")
    table.add_column("Bars/msg", justify="right", overflow="fold")
    table.add_column("Last close", justify="right", overflow="fold")
    table.add_column("Lag (s)", justify="right", overflow="fold")
    table.add_column("Msg age", justify="right", overflow="fold")
    table.add_column("Trend", overflow="fold")
    if not entries:
        table.add_row("—", "—", "0", "—", "—", "—", "—")
    else:
        for item in entries:
            table.add_row(
                item.get("symbol", "?"),
                item.get("tf", "?"),
                str(item.get("bars", 0)),
                _format_epoch_ms_compact(item.get("last_close_ms")),
                f"{item['lag']:.1f}" if item.get("lag") is not None else "—",
                _format_short_duration(item.get("msg_age")),
                item.get("spark", "—"),
            )

    return Panel(table, title="OHLCV channel", border_style="blue", box=box.ROUNDED)


def build_session_panel(state: ViewerState) -> Panel:
    heartbeat = state.last_heartbeat or {}
    context = heartbeat.get("context") or {}
    session = context.get("session") or {}
    if not session:
        return Panel("(session context відсутній)", title="Session", border_style="yellow")

    info = Table.grid(padding=(0, 1))
    info.add_column(style="bold green", overflow="fold")
    info.add_column(overflow="fold")
    info_rows = [
        ("Tag", session.get("tag", "—")),
        ("Timezone", session.get("timezone", "—")),
        ("Session open", session.get("session_open_utc", "—")),
        ("Session close", session.get("session_close_utc", "—")),
        ("Weekly open", session.get("weekly_open", "—")),
        ("Weekly close", session.get("weekly_close", "—")),
    ]
    for label, value in info_rows:
        info.add_row(label, value)

    stats = session.get("stats") or {}
    stats_table = Table(box=box.MINIMAL_DOUBLE_HEAD, expand=True)
    stats_table.add_column("Session", style="bold", overflow="fold")
    stats_table.add_column("Symbol", overflow="fold")
    stats_table.add_column("TF", overflow="fold")
    stats_table.add_column("Bars", justify="right", overflow="fold")
    stats_table.add_column("High", justify="right", overflow="fold")
    stats_table.add_column("Low", justify="right", overflow="fold")
    stats_table.add_column("Δ", justify="right", overflow="fold")
    stats_table.add_column("Δ x bars", justify="right", overflow="fold")
    stats_table.add_column("Avg", justify="right", overflow="fold")

    snapshot_items = sorted(
        state.session_range_snapshot.items(),
        key=lambda item: (_range_ratio(item[1][0], item[1][1]) or 0.0),
        reverse=True,
    )

    range_table = Table(box=box.SIMPLE_HEAVY, expand=True)
    range_table.add_column("Session", style="bold", overflow="fold")
    range_table.add_column("Symbol", overflow="fold")
    range_table.add_column("TF", overflow="fold")
    range_table.add_column("Δ now", justify="right", overflow="fold")
    range_table.add_column("Baseline", justify="right", overflow="fold")
    range_table.add_column("Ratio", justify="right", overflow="fold")
    range_table.add_column("Indicator", overflow="fold")

    if snapshot_items:
        for (tag, symbol, tf), (current, baseline) in snapshot_items:
            ratio = _range_ratio(current, baseline)
            range_table.add_row(
                tag,
                symbol,
                tf,
                _format_float(current),
                _format_float(baseline),
                _format_ratio(ratio),
                _render_range_indicator(current, baseline),
            )
    else:
        range_table.add_row("—", "—", "—", "—", "—", "—", Text("—", style="dim"))

    if stats:
        for tag, payload in stats.items():
            symbols = payload.get("symbols") or []
            if not symbols:
                stats_table.add_row(tag, "—", "—", "0", "—", "—")
                continue
            for entry in symbols:
                bars = _coerce_int(entry.get("bars")) or 0
                high_val = _coerce_float(entry.get("high"))
                low_val = _coerce_float(entry.get("low"))
                raw_range = _coerce_float(entry.get("range"))
                derived_range = None
                if high_val is not None and low_val is not None:
                    derived_range = high_val - low_val
                elif raw_range is not None:
                    derived_range = raw_range
                cumulative_range = derived_range * bars if (derived_range is not None and bars) else None
                stats_table.add_row(
                    tag,
                    entry.get("symbol", "?"),
                    entry.get("tf", "?"),
                    f"{bars}",
                    _format_float(high_val),
                    _format_float(low_val),
                    _format_float(derived_range),
                    _format_float(cumulative_range),
                    _format_float(_coerce_float(entry.get("avg"))),
                )
    elif not stats:
        stats_table.add_row("—", "—", "—", "0", "—", "—", "—", "—", "—")

    content: List[Any] = [info, Text(""), range_table, Text(""), stats_table]
    return Panel(Group(*content), title="Session context", border_style="green", box=box.ROUNDED)


def _symbol_for_event(event: TimelineEvent) -> tuple[str, str]:
    state_lower = event.state.lower()
    if event.source == "MS":
        if state_lower == "open":
            return "O", "green"
        if state_lower == "closed":
            return "C", "red"
        return state_lower[:1].upper() or "M", "white"
    # heartbeat states
    mapping = {
        "stream": ("S", "green"),
        "idle": ("I", "yellow"),
        "warmup": ("W", "cyan"),
        "warmup_cache": ("W", "cyan"),
        "warmup_sample": ("W", "cyan"),
    }
    symbol, color = mapping.get(state_lower, (state_lower[:1].upper() or "H", "white"))
    return symbol, color


def _timeline_dimensions() -> tuple[int, int]:
    try:
        width = Console().width
    except Exception:  # pragma: no cover - fallback для середовищ без TTY
        width = 80
    columns = max(10, min(TIMELINE_MAX_COLUMNS, width - 6))
    rows = max(2, TIMELINE_MATRIX_ROWS)
    return rows, columns


def _build_timeline_grid(events: List[TimelineEvent]) -> tuple[list[list[Optional[TimelineEvent]]], List[TimelineEvent]]:
    """
    Будує сітку таймлайна «як книжку»: зліва направо, зверху вниз.

    Старіші події йдуть у верхніх рядках, новіші – внизу праворуч.
    Кількість рядків адаптується до обсягу подій (але не більше
    TIMELINE_MATRIX_ROWS), щоб не тримати зайвий порожній простір
    і не витісняти підсумкові рядки з панелі.
    """
    rows, columns = _timeline_dimensions()
    max_cells = rows * columns
    if max_cells <= 0:
        return [], []
    visible_events = events[-max_cells:]
    grid: list[list[Optional[TimelineEvent]]] = [[None for _ in range(columns)] for _ in range(rows)]
    # Починаємо з (row=0, col=0), щоб таймлайн читався як текст:
    # зліва направо, зверху вниз; старі події поступово «виштовхуються»
    # з верхньої частини вікна, але останні завжди внизу/праворуч.
    for idx, event in enumerate(visible_events):
        cell_index = idx
        row = cell_index // columns
        col = cell_index % columns
        if 0 <= row < rows:
            grid[row][col] = event
    return grid, visible_events


def _timeline_summary_lines(events: List[TimelineEvent], state: ViewerState) -> List[Text]:
    if not events:
        return [Text("Подій поки немає", style="dim")]

    total = len(events)
    counts: Counter[str] = Counter({"S": 0, "I": 0, "O": 0, "C": 0})
    for event in events:
        symbol, _ = _symbol_for_event(event)
        if symbol in counts:
            counts[symbol] += 1

    summary_parts: List[str] = []
    for label in ("S", "I", "O", "C"):
        value = counts[label]
        if total and value:
            percent = int(round(100 * value / total))
            summary_parts.append(f"{label}: {value} ({percent}%)")
        else:
            summary_parts.append(f"{label}: {value}")
    summary_line = Text(" · ".join(summary_parts), style="bold")

    gaps: List[float] = []
    for idx in range(1, len(events)):
        gap = events[idx].ts_epoch - events[idx - 1].ts_epoch
        if gap >= 0:
            gaps.append(gap)
    max_gap = _format_short_duration(max(gaps)) if gaps else "—"
    avg_gap_seconds = (sum(gaps) / len(gaps)) if gaps else None
    if avg_gap_seconds is not None:
        if avg_gap_seconds < 60:
            avg_gap = f"{avg_gap_seconds:.1f}s"
        else:
            avg_gap = _format_short_duration(avg_gap_seconds)
    else:
        avg_gap = "—"
    window_span = events[-1].ts_epoch - events[0].ts_epoch
    window_label = _format_duration(window_span)
    stats_line = Text(f"window: {window_label} · max_gap: {max_gap} · avg_gap: {avg_gap}")

    last_ts = events[-1].ts_epoch
    market_state = (state.last_market_status or {}).get("state")
    if not market_state:
        market_state = (state.last_heartbeat or {}).get("state")
    tail_line = Text(
        f"last_event: {_format_utc_timestamp(last_ts)} · market: {market_state or '—'}",
        style="dim",
    )

    legend = Text("S=stream, I=idle, O=open, C=closed", style="dim")
    return [summary_line, stats_line, tail_line, legend]


def build_timeline_panel(state: ViewerState) -> Panel:
    events = list(state.timeline_events)
    if not events:
        return Panel("Очікуємо події…", title="Timeline", border_style="blue")

    grid, visible_events = _build_timeline_grid(events)
    if not grid:
        return Panel("Очікуємо події…", title="Timeline", border_style="blue")

    table = Table.grid(padding=0)
    columns = len(grid[0]) if grid else 0
    if columns <= 0:
        columns = 1
    for _ in range(columns):
        table.add_column(justify="center", width=1, no_wrap=True)

    focus_cutoff = time.time() - max(0.0, TIMELINE_FOCUS_MINUTES) * 60.0
    for row in grid:
        cells: List[Text] = []
        for event in row:
            if event is None:
                cells.append(Text(" ", style="dim"))
                continue
            symbol, color = _symbol_for_event(event)
            if event.ts_epoch >= focus_cutoff:
                style = f"bold {color}"
            else:
                style = f"{color} dim"
            cells.append(Text(symbol, style=style))
        table.add_row(*cells)

    summary_lines = _timeline_summary_lines(visible_events, state)
    # Спочатку summary у лівому верхньому кутку, потім грід подій.
    content_items: List[Any] = []
    content_items.extend(summary_lines)
    content_items.append(table)
    return Panel(Group(*content_items), title="Event timeline", border_style="blue", box=box.ROUNDED)


def _render_mode_body(state: ViewerState) -> Union[Layout, Panel]:
    mode = state.active_mode
    if mode == DashboardMode.SUMMARY:
        layout = Layout(name="summary_mode")
        layout.split_row(
            Layout(name="fxcm_diag", ratio=2, minimum_size=6),
            Layout(name="incidents", ratio=2, minimum_size=6),
        )
        layout["fxcm_diag"].update(build_fxcm_diag_panel(state))
        layout["incidents"].update(build_incidents_panel(state))
        return layout
    if mode == DashboardMode.SESSION:
        return build_session_panel(state)
    if mode == DashboardMode.TIMELINE:
        return build_timeline_panel(state)
    if mode == DashboardMode.STREAMS:
        layout = Layout(name="streams_mode")
        layout.split_column(
            Layout(name="redis", ratio=1, minimum_size=5),
            Layout(name="streams", ratio=2, minimum_size=6),
        )
        layout["redis"].update(build_redis_panel(state))
        layout["streams"].update(build_stream_targets_panel(state))
        return layout
    if mode == DashboardMode.PRICE:
        return build_price_stream_panel(state)
    if mode == DashboardMode.SUPERVISOR:
        return build_supervisor_mode(state)
    layout = Layout(name="alerts_mode")
    layout.split_column(
        Layout(name="alerts", ratio=1, minimum_size=5),
        Layout(name="issues", ratio=1, minimum_size=5),
        Layout(name="ohlcv", ratio=1, minimum_size=5),
    )
    layout["alerts"].update(build_alerts_panel(state))
    layout["issues"].update(build_issue_panel(state))
    layout["ohlcv"].update(build_ohlcv_panel(state))
    return layout


def render_dashboard(state: ViewerState) -> Layout:
    root = Layout(name="root")
    root.split_column(
        Layout(name="body", ratio=1),
        Layout(name="menu", size=3),
    )
    root["body"].update(_render_mode_body(state))
    root["menu"].update(build_menu_bar())
    return root


def run_viewer(args: argparse.Namespace) -> None:
    if redis is None:
        raise RuntimeError("Потрібно встановити пакет redis для використання viewer.")

    client = redis.Redis(host=args.redis_host, port=args.redis_port, password=args.redis_password, decode_responses=False)
    pubsub = client.pubsub(ignore_subscribe_messages=True)
    channels = [args.heartbeat_channel, args.market_status_channel]
    if args.ohlcv_channel:
        channels.append(args.ohlcv_channel)
    pubsub.subscribe(*channels)

    state = ViewerState()
    last_render = 0.0
    next_health_check = 0.0

    console = Console()
    current_mode = state.active_mode
    force_render = True
    with KeyboardInput() as keyboard:
        with Live(
            render_dashboard(state),
            refresh_per_second=max(1.0, 1.0 / args.refresh),
            screen=True,
            console=console,
        ) as live:
            try:
                while True:
                    message = pubsub.get_message(timeout=0.5)
                    if message and message.get("type") == "message":
                        channel = message.get("channel")
                        if isinstance(channel, bytes):
                            channel = channel.decode("utf-8", "ignore")
                        raw_data = message.get("data")
                        if raw_data is None:
                            continue
                        if isinstance(raw_data, bytes):
                            raw_data = raw_data.decode("utf-8", "ignore")
                        try:
                            payload = json.loads(raw_data)
                        except Exception:
                            continue
                        if channel == args.heartbeat_channel:
                            state.note_heartbeat(payload)
                        elif channel == args.market_status_channel:
                            state.note_market_status(payload)
                        elif args.ohlcv_channel and channel == args.ohlcv_channel:
                            state.note_ohlcv(payload)
                        force_render = True

                    key = keyboard.poll()
                    if key:
                        if handle_keypress(key, state):
                            raise KeyboardInterrupt
                        force_render = True

                    if refresh_time_alerts(state):
                        force_render = True
                    if refresh_ohlcv_alerts(state):
                        force_render = True

                    now = time.time()
                    if state.active_mode != current_mode:
                        current_mode = state.active_mode
                        force_render = True
                    if now >= next_health_check:
                        refresh_redis_health(client, state)
                        next_health_check = now + max(2.0, REDIS_HEALTH_INTERVAL)
                        force_render = True

                    should_render = False
                    if not state.paused and (now - last_render >= args.refresh):
                        should_render = True
                    if force_render:
                        should_render = True
                    if should_render:
                        live.update(render_dashboard(state))
                        last_render = now
                        force_render = False
            except KeyboardInterrupt:
                live.stop()
                print("\nViewer зупинено користувачем.")
            finally:
                pubsub.close()
                client.close()


def main() -> None:
    load_dotenv()
    args = parse_args()
    run_viewer(args)


if __name__ == "__main__":
    if os.name == "nt":
        signal.signal(signal.SIGINT, signal.SIG_DFL)
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"Viewer завершився з помилкою: {exc}", file=sys.stderr)
        sys.exit(1)
