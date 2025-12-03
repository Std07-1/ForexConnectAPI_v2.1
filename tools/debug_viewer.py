"""Консольний viewer для FXCM-конектора.

Запускає простий live-dashboard у терміналі та показує:
- останній heartbeat (стан, лаг, причину idle, таймінги);
- market_status з next_open;
- таблицю stream targets (staleness/lag);
- сесійну інформацію (tag, timezone, вікно, stats).

Використання:
    python tools/debug_viewer.py --redis-host 127.0.0.1 --redis-port 6379

Параметри можна не передавати, якщо у середовищі вже виставлені
FXCM_REDIS_HOST / FXCM_REDIS_PORT / FXCM_HEARTBEAT_CHANNEL.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import select
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
import statistics
from typing import Any, Deque, Dict, Optional, Tuple, Set, List, Union

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

HEARTBEAT_CHANNEL_DEFAULT = os.environ.get("FXCM_HEARTBEAT_CHANNEL", "fxcm:heartbeat")
MARKET_STATUS_CHANNEL_DEFAULT = os.environ.get("FXCM_MARKET_STATUS_CHANNEL", "fxcm:market_status")
OHLCV_CHANNEL_DEFAULT = os.environ.get("FXCM_OHLCV_CHANNEL", "fxcm:ohlcv")
REDIS_HEALTH_INTERVAL = float(os.environ.get("FXCM_VIEWER_REDIS_HEALTH_INTERVAL", 8))
LAG_SPIKE_THRESHOLD = float(os.environ.get("FXCM_VIEWER_LAG_SPIKE_THRESHOLD", 180))
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
HEARTBEAT_ALERT_SECONDS = float(os.environ.get("FXCM_VIEWER_HEARTBEAT_ALERT_SECONDS", 45))
OHLCV_MSG_IDLE_WARN_SECONDS = float(os.environ.get("FXCM_VIEWER_OHLCV_MSG_WARN_SECONDS", 90))
OHLCV_MSG_IDLE_ERROR_SECONDS = float(os.environ.get("FXCM_VIEWER_OHLCV_MSG_ERROR_SECONDS", 180))
OHLCV_LAG_WARN_SECONDS = float(os.environ.get("FXCM_VIEWER_OHLCV_LAG_WARN_SECONDS", 45))
OHLCV_LAG_ERROR_SECONDS = float(os.environ.get("FXCM_VIEWER_OHLCV_LAG_ERROR_SECONDS", 120))
ALERT_SEVERITY_ORDER = {"danger": 0, "warning": 1, "info": 2}
ALERT_SEVERITY_COLORS = {"danger": "red", "warning": "yellow", "info": "cyan"}
MENU_TEXT = "[P] пауза  [C] очистити  [Q] вихід  [0] TL  [1] SUM  [2] SES  [3] ALERT  [4] STREAM"

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
    timeline_events: Deque[TimelineEvent] = field(default_factory=lambda: deque(maxlen=480))
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
    idle_entries: List[Tuple[str, str, float]] = []
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

        idle_active = msg_age is not None and msg_age >= OHLCV_MSG_IDLE_WARN_SECONDS
        if idle_active and msg_age is not None:
            idle_entries.append((symbol, tf, msg_age))
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
        severity_idle = "danger" if worst_idle[2] >= OHLCV_MSG_IDLE_ERROR_SECONDS else "warning"
        summary_idle = ", ".join(
            f"{sym} {tf} {age:.0f}s" for sym, tf, age in idle_entries[:3]
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


def build_timeline_panel(state: ViewerState) -> Panel:
    events = list(state.timeline_events)
    if not events:
        return Panel("Очікуємо події…", title="Timeline", border_style="blue")

    symbol_line = Text()
    time_line = Text()
    for idx, event in enumerate(events):
        symbol, color = _symbol_for_event(event)
        symbol_line.append(f" {symbol} ", style=color)
        if idx % 3 == 0:
            label = dt.datetime.fromtimestamp(event.ts_epoch, tz=dt.timezone.utc).strftime("%H:%M")
            time_line.append(f" {label} ", style=color)
        else:
            time_line.append("    ")

    legend = Text("S=stream, I=idle, O=open, C=closed", style="dim")
    content = Group(symbol_line, time_line, Text(""), legend)
    return Panel(content, title="Event timeline", border_style="blue", box=box.ROUNDED)


def _render_mode_body(state: ViewerState) -> Union[Layout, Panel]:
    mode = state.active_mode
    if mode == DashboardMode.SUMMARY:
        return build_summary_panel(state)
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
