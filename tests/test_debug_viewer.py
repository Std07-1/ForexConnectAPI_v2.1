"""Unit tests for tools.debug_viewer helpers."""

import time

import pytest

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

