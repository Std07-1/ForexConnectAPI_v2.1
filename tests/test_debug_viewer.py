"""Unit tests for tools.debug_viewer helpers."""

import time

import pytest
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel

from tools import debug_viewer as dv


def _mock_bar(close_ms: int) -> dict:
    return {
        "open_time": close_ms - 60_000,
        "close_time": close_ms,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 10.0,
    }


def test_note_ohlcv_tracks_latest_bar() -> None:
    state = dv.ViewerState()
    now_ms = int(time.time() * 1000)
    payload = {"symbol": "EUR/USD", "tf": "1m", "bars": [_mock_bar(now_ms)]}

    state.note_ohlcv(payload)

    key = ("EUR/USD", "1m")
    assert key in state.ohlcv_targets
    entry = state.ohlcv_targets[key]
    assert entry["last_close_ms"] == now_ms
    assert entry["bars_per_msg"] == 1
    assert state.last_ohlcv_ts is not None
    assert list(state.ohlcv_lag_history[key])  # history populated


def test_refresh_ohlcv_alerts_sets_missing_before_messages() -> None:
    state = dv.ViewerState()

    changed = dv.refresh_ohlcv_alerts(state)

    assert changed is True
    assert "ohlcv_missing" in state.alerts
    assert state.alerts["ohlcv_missing"].severity == "info"


@pytest.mark.parametrize("age_delta", [0.0, 5.0])
def test_refresh_ohlcv_alerts_detects_stale(age_delta: float) -> None:
    state = dv.ViewerState()
    key = ("XAU/USD", "1m")
    warn_threshold, _ = dv.resolve_idle_thresholds("1m")
    state.last_ohlcv_ts = time.time() - (warn_threshold + age_delta)
    state.ohlcv_targets[key] = {
        "symbol": "XAU/USD",
        "tf": "1m",
        "bars_per_msg": 1,
        "last_close_ms": int((time.time() - 30) * 1000),
    }
    state.ohlcv_updated[key] = state.last_ohlcv_ts
    state.last_market_status = {"state": "open"}

    dv.refresh_ohlcv_alerts(state)

    assert "ohlcv_msg_idle" in state.alerts
    alert = state.alerts["ohlcv_msg_idle"]
    assert "XAU/USD" in alert.message
    assert alert.severity in {"warning", "danger"}


def test_note_ohlcv_zero_bars_tracks_issue() -> None:
    state = dv.ViewerState()
    payload = {"symbol": "EUR/USD", "tf": "1m", "bars": []}

    state.note_ohlcv(payload)

    key = ("EUR/USD", "1m")
    assert key in state.ohlcv_zero_bars_since
    assert state.ohlcv_targets[key]["bars_per_msg"] == 0


def test_refresh_ohlcv_alerts_ignores_lag_when_market_closed() -> None:
    state = dv.ViewerState()
    key = ("XAU/USD", "1m")
    now = time.time()
    state.last_ohlcv_ts = now
    state.ohlcv_targets[key] = {
        "symbol": "XAU/USD",
        "tf": "1m",
        "bars_per_msg": 1,
        "last_close_ms": int((now - (dv.OHLCV_LAG_WARN_SECONDS + 10)) * 1000),
    }
    state.ohlcv_updated[key] = now
    state.last_market_status = {"state": "closed"}

    dv.refresh_ohlcv_alerts(state)

    assert "ohlcv_lag" not in state.alerts


def test_refresh_ohlcv_alerts_sets_idle_warning_per_symbol() -> None:
    state = dv.ViewerState()
    key = ("XAU/USD", "1m")
    warn_threshold, _ = dv.resolve_idle_thresholds("1m")
    last_update = time.time() - (warn_threshold + 5)
    state.last_ohlcv_ts = last_update
    state.ohlcv_targets[key] = {
        "symbol": "XAU/USD",
        "tf": "1m",
        "bars_per_msg": 2,
        "last_close_ms": int((time.time() - 30) * 1000),
    }
    state.ohlcv_updated[key] = last_update
    state.last_market_status = {"state": "open"}

    dv.refresh_ohlcv_alerts(state)

    alert = state.alerts.get("ohlcv_msg_idle")
    assert alert is not None
    assert "XAU/USD" in alert.message
    assert alert.severity in {"warning", "danger"}


def test_incident_feed_tracks_issue_transitions() -> None:
    state = dv.ViewerState()

    state._track_issue_state("fxcm_pause", True, "fxcm pause")

    assert state.incident_feed
    entry = state.incident_feed[0]
    assert entry["active"] is True
    assert entry["key"] == "fxcm_pause"

    state._track_issue_state("fxcm_pause", False, "recovered")

    assert len(state.incident_feed) >= 2
    assert state.incident_feed[0]["active"] is False


def test_note_heartbeat_tracks_supervisor_context() -> None:
    state = dv.ViewerState()
    diag = {
        "state": "running",
        "queues": [{"name": "ohlcv", "size": 2, "maxsize": 10, "processed": 5}],
    }
    heartbeat = {
        "state": "stream",
        "context": {"supervisor": diag},
    }

    state.note_heartbeat(heartbeat)

    assert state.supervisor_diag == diag
    assert state.supervisor_updated is not None


def test_build_supervisor_mode_without_context() -> None:
    state = dv.ViewerState()

    result = dv.build_supervisor_mode(state)

    assert isinstance(result, Panel)
    renderable = getattr(result, "renderable", "")
    assert "supervisor-контекст" in str(renderable)


def test_build_supervisor_mode_with_context_returns_layout() -> None:
    state = dv.ViewerState()
    state.supervisor_updated = time.time()
    state.supervisor_diag = {
        "state": "running",
        "loop_alive": True,
        "queues": [
            {
                "name": "ohlcv",
                "size": 3,
                "maxsize": 10,
                "processed": 120,
                "last_enqueue_ts": time.time() - 1,
            }
        ],
        "tasks": [
            {
                "name": "history_consumer",
                "state": "running",
                "processed": 200,
                "idle_seconds": 0.5,
            }
        ],
        "metrics": {
            "total_enqueued": 10,
            "total_processed": 9,
            "total_dropped": 1,
            "queue_depth_total": 3,
            "publishes_total": 4,
            "active_tasks": 1,
            "task_errors": 0,
        },
        "publish_counts": {"ohlcv": 4},
        "backpressure_events": 0,
    }

    result = dv.build_supervisor_mode(state)

    assert isinstance(result, Layout)


def test_build_supervisor_metrics_panel_renders_publish_totals(monkeypatch: pytest.MonkeyPatch) -> None:
    diag = {
        "metrics": {
            "total_enqueued": 12,
            "total_processed": 10,
            "total_dropped": 2,
            "queue_depth_total": 5,
            "publishes_total": 20,
            "active_tasks": 2,
            "task_errors": 1,
        },
        "publish_counts": {"ohlcv": 15, "heartbeat": 5, "market_status": 2, "custom_sink": 1},
        "backpressure_events": 3,
    }
    monkeypatch.setattr(dv, "SUPERVISOR_CHANNELS", ("ohlcv", "custom_sink"))
    monkeypatch.setattr(dv, "SUPERVISOR_CHANNEL_HINTS", {"ohlcv": "Головний sink", "custom_sink": "Кастом"})

    panel = dv.build_supervisor_metrics_panel(diag)

    assert isinstance(panel, Panel)
    console = Console(record=True, width=120)
    console.print(panel)
    render_str = console.export_text()
    assert "Публікацій загалом" in render_str
    assert "ohlcv" in render_str
    assert "custom_sink" in render_str
    assert "Кастом" in render_str


def test_build_price_stream_panel_embeds_special_channels(monkeypatch: pytest.MonkeyPatch) -> None:
    state = dv.ViewerState()
    now = time.time()
    state.price_stream_updated = now
    state.price_stream = {
        "state": "ok",
        "channel": "fxcm:price_tik",
        "interval_seconds": 3.0,
        "symbols_state": [
            {
                "symbol": "XAUUSD",
                "mid": 4209.3,
                "bid": 4209.1,
                "ask": 4209.5,
                "tick_age_seconds": 1.0,
            }
        ],
    }
    state.tick_cadence = {
        "state": "ok",
        "history_state": "active",
        "cadence_seconds": {"1m": 3.0},
        "next_poll_in_seconds": {"1m": 1.5},
        "multiplier": 1.0,
    }
    state.tick_cadence_updated = now
    diag = {
        "publish_counts": {"ohlcv": 12, "heartbeat": 4},
    }
    state.supervisor_diag = diag
    state.supervisor_updated = now
    monkeypatch.setattr(dv, "SUPERVISOR_CHANNELS", ("ohlcv",))
    monkeypatch.setattr(dv, "SUPERVISOR_CHANNEL_HINTS", {"ohlcv": "Головний sink"})

    panel = dv.build_price_stream_panel(state)

    console = Console(record=True, width=120)
    console.print(panel)
    render_str = console.export_text()
    assert "Спеціальні канали" in render_str
    assert "ohlcv" in render_str
    assert "Adaptive cadence" in render_str
    assert "Viewer snapshots" in render_str


def test_special_channels_panel_shows_price_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    diag = {"publish_counts": {}}
    monkeypatch.setattr(dv, "SUPERVISOR_CHANNELS", ("price",))
    monkeypatch.setattr(dv, "SUPERVISOR_CHANNEL_HINTS", {"price": "Снепшоти"})

    panel = dv.build_supervisor_special_channels_panel(diag)

    assert isinstance(panel, Panel)


def test_handle_keypress_switches_to_stats_mode() -> None:
    state = dv.ViewerState()
    assert state.active_mode != dv.DashboardMode.STATS

    should_exit = dv.handle_keypress("8", state)

    assert should_exit is False
    assert state.active_mode == dv.DashboardMode.STATS
    # smoke: рендер STATS не має падати
    dashboard = dv.render_dashboard(state)
    assert isinstance(dashboard, Layout)
    console = Console(record=True, width=120)
    console.print(dashboard)
    render_str = console.export_text()
    assert "S1 stats" in render_str


def test_note_ohlcv_detects_gap_between_chunks() -> None:
    state = dv.ViewerState()
    # Перший чанк: останній бар закінчився о 00:00:59
    state.note_ohlcv(
        {
            "symbol": "XAUUSD",
            "tf": "1m",
            "bars": [
                {
                    "open_time": 0,
                    "close_time": 59_999,
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 0.0,
                }
            ],
        }
    )

    # Другий чанк: стартує з 00:02:00 (пропущена 1 хвилина 00:01)
    state.note_ohlcv(
        {
            "symbol": "XAUUSD",
            "tf": "1m",
            "bars": [
                {
                    "open_time": 120_000,
                    "close_time": 179_999,
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 0.0,
                }
            ],
        }
    )

    assert state.ohlcv_gap_events
    gap = state.ohlcv_gap_events[0]
    assert gap.symbol == "XAUUSD"
    assert gap.tf == "1m"
    assert gap.missing_min == 1.0
    assert gap.filled is False

    panel = dv.build_ohlcv_gaps_panel(state)
    assert isinstance(panel, Panel)
    console = Console(record=True, width=160)
    console.print(panel)
    text = console.export_text()
    assert "Top 10 largest gaps" in text
    assert "XAUUSD" in text


def test_supervisor_special_channels_panel_uses_fallback_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    diag = {"publish_counts": {}}
    monkeypatch.setattr(dv, "SUPERVISOR_CHANNELS", ("price",))
    monkeypatch.setattr(dv, "SUPERVISOR_CHANNEL_HINTS", {"price": "Снепшоти"})

    panel = dv.build_supervisor_special_channels_panel(diag, fallback_counts={"price": 7})

    console = Console(record=True, width=120)
    console.print(panel)
    render_str = console.export_text()
    assert "7" in render_str


def test_note_heartbeat_tracks_tick_cadence() -> None:
    state = dv.ViewerState()
    cadence = {"state": "ok", "cadence_seconds": {"1m": 3.0}}
    heartbeat = {"state": "stream", "context": {"tick_cadence": cadence}}

    state.note_heartbeat(heartbeat)

    assert state.tick_cadence == cadence
    assert state.tick_cadence_updated is not None


def test_note_heartbeat_counts_price_snapshots() -> None:
    state = dv.ViewerState()
    first = {
        "state": "stream",
        "context": {
            "price_stream": {
                "last_snap_ts": 100.0,
                "symbols_state": [{"symbol": "XAUUSD"}]
            }
        },
    }
    second = {
        "state": "stream",
        "context": {
            "price_stream": {
                "last_snap_ts": 105.0,
                "symbols_state": [{"symbol": "XAUUSD"}, {"symbol": "EURUSD"}],
            }
        },
    }

    state.note_heartbeat(first)
    state.note_heartbeat(second)

    assert state.price_snapshot_total == 2


def test_build_tick_cadence_panel_renders_snapshot() -> None:
    state = dv.ViewerState()
    state.tick_cadence_updated = time.time()
    state.tick_cadence = {
        "state": "lag",
        "history_state": "paused",
        "reason": "tick_idle",
        "tick_silence_seconds": 45.0,
        "multiplier": 2.5,
        "cadence_seconds": {"1m": 6.0, "5m": 12.0},
        "next_poll_in_seconds": {"1m": 5.0},
    }

    panel = dv.build_tick_cadence_panel(state)

    assert isinstance(panel, Panel)
    console = Console(record=True, width=120)
    console.print(panel)
    render_str = console.export_text()
    assert "Adaptive cadence" in render_str
    assert "1m" in render_str
    assert "6.0" in render_str

