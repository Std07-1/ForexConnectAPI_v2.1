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
import statistics
from typing import Any, Deque, Dict, Optional, Tuple, Set

from dotenv import load_dotenv
from rich import box
from rich.console import Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

try:
    import redis  # type: ignore[import]
except ImportError:  # pragma: no cover - viewer потребує redis-client
    redis = None

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
REDIS_HEALTH_INTERVAL = float(os.environ.get("FXCM_VIEWER_REDIS_HEALTH_INTERVAL", 8))
LAG_SPIKE_THRESHOLD = float(os.environ.get("FXCM_VIEWER_LAG_SPIKE_THRESHOLD", 180))
FXCM_PAUSE_REASONS = {"fxcm_temporarily_unavailable", "fxcm_unavailable"}
CALENDAR_PAUSE_REASONS = {"calendar", "calendar_closed", "market_closed"}
ISSUE_LABELS = {
    "fxcm_pause": "FXCM паузи",
    "calendar_pause": "Календарні паузи",
    "redis_disconnect": "Розриви Redis",
    "lag_spike": f"Лаг > {int(LAG_SPIKE_THRESHOLD)} с",
}


@dataclass
class TimelineEvent:
    source: str
    state: str
    ts_iso: str
    ts_epoch: float


@dataclass
class ViewerState:
    last_heartbeat: Optional[Dict[str, Any]] = None
    last_market_status: Optional[Dict[str, Any]] = None
    last_message_ts: float = field(default_factory=time.time)
    timeline_events: Deque[TimelineEvent] = field(default_factory=lambda: deque(maxlen=60))
    staleness_history: Dict[Tuple[str, str], Deque[float]] = field(default_factory=dict)
    session_range_history: Dict[Tuple[str, str, str], Deque[float]] = field(default_factory=dict)
    session_range_snapshot: Dict[Tuple[str, str, str], Tuple[Optional[float], Optional[float]]] = field(default_factory=dict)
    redis_health: Dict[str, Any] = field(default_factory=dict)
    issue_counters: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    issue_state_flags: Dict[str, bool] = field(default_factory=dict)
    paused: bool = False
    last_action: Optional[str] = None

    def note_heartbeat(self, payload: Dict[str, Any]) -> None:
        self.last_heartbeat = payload
        self.last_message_ts = time.time()
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

    def note_redis_health(self, payload: Dict[str, Any]) -> None:
        self.redis_health = payload

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
        for target in targets:
            symbol = str(target.get("symbol", "?"))
            tf = str(target.get("tf", "?"))
            key = (symbol, tf)
            seen_keys.add(key)
            staleness = _coerce_float(target.get("staleness_seconds"))
            if staleness is None:
                continue
            history = self.staleness_history.setdefault(key, deque(maxlen=20))
            history.append(staleness)
        # prune histories that are no longer present to avoid leaking memory
        for key in list(self.staleness_history.keys()):
            if key not in seen_keys and len(self.staleness_history) > 50:
                self.staleness_history.pop(key, None)

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

        calendar_active = state.lower() == "idle" and (
            idle_reason in CALENDAR_PAUSE_REASONS or pause_reason in CALENDAR_PAUSE_REASONS
        )
        detail_calendar = idle_reason_raw or pause_reason_raw or "calendar pause"
        self._track_issue_state("calendar_pause", calendar_active, str(detail_calendar))

        redis_required = context.get("redis_required")
        redis_connected = context.get("redis_connected")
        redis_active = bool((redis_required is None or redis_required) and redis_connected is False)
        self._track_issue_state("redis_disconnect", redis_active, "Redis disconnected")

        lag_value = _coerce_float(context.get("lag_seconds"))
        lag_active = bool(lag_value is not None and lag_value >= LAG_SPIKE_THRESHOLD)
        detail_lag = f"Lag {lag_value:.1f}s" if lag_value is not None else None
        self._track_issue_state("lag_spike", lag_active, detail_lag)

    def heartbeat_age(self) -> Optional[float]:
        if not self.last_heartbeat:
            return None
        ts = self.last_heartbeat.get("ts")
        try:
            last_ts = _iso_to_epoch(ts)
            return max(0.0, time.time() - last_ts)
        except Exception:
            return None


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
    total = int(seconds)
    days, rem = divmod(total, 86_400)
    hours, rem = divmod(rem, 3_600)
    minutes, secs = divmod(rem, 60)
    return f"{days:02}d {hours:02}:{minutes:02}:{secs:02}"


def _format_utc_timestamp(ts: Optional[float]) -> str:
    if ts is None:
        return "—"
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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
            termios.tcsetattr(self.fd, termios.TCSADRAIN, self._old_settings)
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
    parser.add_argument("--refresh", default=2.0, type=float, help="частота оновлення UI (сек)")
    return parser.parse_args()


def build_summary_panel(state: ViewerState) -> Panel:
    table = Table.grid(padding=1)
    table.add_column(justify="right", style="bold cyan")
    table.add_column()

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
    table.add_row("Redis connected", _format_bool(context.get("redis_connected")))
    table.add_row("Redis required", _format_bool(context.get("redis_required")))
    table.add_row("Cache enabled", _format_bool(context.get("cache_enabled")))
    table.add_row("Last action", state.last_action or "—")

    controls_line = Text("Controls: [P] пауза  [C] очистити  [Q] вихід", style="dim")
    content = Group(table, Text(""), controls_line)
    return Panel(content, title="FXCM summary", border_style="cyan", box=box.ROUNDED)


def build_issue_panel(state: ViewerState) -> Panel:
    table = Table(box=box.SIMPLE_HEAVY)
    table.add_column("Issue", style="bold")
    table.add_column("Count", justify="right")
    table.add_column("Last seen", justify="right")
    table.add_column("Details")

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
    table.add_column(style="bold cyan", justify="right")
    table.add_column()

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

    table = Table(box=box.SIMPLE_HEAVY)
    table.add_column("Symbol", style="bold")
    table.add_column("TF")
    table.add_column("Staleness (s)", justify="right")
    table.add_column("Staleness (ms)", justify="right")
    table.add_column("Trend", justify="left")

    if not targets:
        table.add_row("—", "—", "—", "—", "—")
    else:
        for target in targets:
            symbol = target.get("symbol", "?")
            tf = target.get("tf", "?")
            staleness = _coerce_float(target.get("staleness_seconds"))
            ms_value = staleness * 1000.0 if staleness is not None else None
            spark = _render_sparkline(state.staleness_history.get((symbol, tf)))
            table.add_row(
                symbol,
                tf,
                f"{staleness:.1f}" if staleness is not None else "—",
                _format_float(ms_value, digits=0),
                spark,
            )

    return Panel(table, title="Stream targets", border_style="magenta")


def build_session_panel(state: ViewerState) -> Panel:
    heartbeat = state.last_heartbeat or {}
    context = heartbeat.get("context") or {}
    session = context.get("session") or {}
    if not session:
        return Panel("(session context відсутній)", title="Session", border_style="yellow")

    info = Table.grid(padding=(0, 1))
    info.add_column(style="bold green")
    info.add_column()
    info.add_row("Tag", session.get("tag", "—"))
    info.add_row("Timezone", session.get("timezone", "—"))
    info.add_row("Session open", session.get("session_open_utc", "—"))
    info.add_row("Session close", session.get("session_close_utc", "—"))
    info.add_row("Weekly open", session.get("weekly_open", "—"))
    info.add_row("Weekly close", session.get("weekly_close", "—"))

    stats = session.get("stats") or {}
    stats_table = Table(box=box.MINIMAL_DOUBLE_HEAD)
    stats_table.add_column("Session", style="bold")
    stats_table.add_column("Symbol")
    stats_table.add_column("TF")
    stats_table.add_column("Bars", justify="right")
    stats_table.add_column("High", justify="right")
    stats_table.add_column("Low", justify="right")
    stats_table.add_column("Δ", justify="right")
    stats_table.add_column("Δ x bars", justify="right")
    stats_table.add_column("Avg", justify="right")

    range_table = Table(box=box.SIMPLE_HEAVY)
    range_table.add_column("Session", style="bold")
    range_table.add_column("Symbol")
    range_table.add_column("TF")
    range_table.add_column("Δ now", justify="right")
    range_table.add_column("Baseline", justify="right")
    range_table.add_column("Ratio", justify="right")
    range_table.add_column("Indicator")

    snapshot_items = sorted(state.session_range_snapshot.items())
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
    else:
        stats_table.add_row("—", "—", "—", "0", "—", "—", "—", "—", "—")

    content = [info, Text("")]
    content.append(range_table)
    content.append(Text(""))
    content.append(stats_table)
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
            time_line.append(f" {label} ", style="dim")
        else:
            time_line.append("    ")

    legend = Text("S=stream, I=idle, O=open, C=closed", style="dim")
    content = Group(symbol_line, time_line, Text(""), legend)
    return Panel(content, title="Event timeline", border_style="blue", box=box.ROUNDED)


def render_dashboard(state: ViewerState) -> Layout:
    layout = Layout(name="root")
    layout.split_column(
        Layout(name="top", ratio=3),
        Layout(name="middle", size=9),
        Layout(name="bottom", ratio=2),
    )
    layout["top"].split_row(
        Layout(build_summary_panel(state), name="summary"),
        Layout(build_session_panel(state), name="session"),
    )
    layout["middle"].split_row(
        Layout(build_issue_panel(state), name="issues"),
        Layout(build_redis_panel(state), name="redis"),
        Layout(build_stream_targets_panel(state), name="streams"),
    )
    layout["bottom"].update(build_timeline_panel(state))
    return layout


def run_viewer(args: argparse.Namespace) -> None:
    if redis is None:
        raise RuntimeError("Потрібно встановити пакет redis для використання viewer.")

    client = redis.Redis(host=args.redis_host, port=args.redis_port, password=args.redis_password, decode_responses=False)
    pubsub = client.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(args.heartbeat_channel, args.market_status_channel)

    state = ViewerState()
    last_render = 0.0
    next_health_check = 0.0

    force_render = True
    with KeyboardInput() as keyboard:
        with Live(render_dashboard(state), refresh_per_second=max(1.0, 1.0 / args.refresh), screen=True) as live:
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
                        force_render = True

                    key = keyboard.poll()
                    if key:
                        if handle_keypress(key, state):
                            raise KeyboardInterrupt
                        force_render = True

                    now = time.time()
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
