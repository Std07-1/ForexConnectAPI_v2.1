from __future__ import annotations

import asyncio
import datetime as dt
import json
import tempfile
import time
import types
import unittest
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, cast
from unittest import mock

import pandas as pd

import connector
import sessions

from config import SampleRequestSettings, TickCadenceTuning
from connector import (
    AsyncStreamSupervisor,
    HeartbeatEvent,
    MarketStatusEvent,
    HistoryCache,
    OhlcvBatch,
    PriceSnapshotWorker,
    PriceTickSnap,
    PublishDataGate,
    publish_ohlcv_to_redis,
    stream_fx_data,
)
from fxcm_security import compute_payload_hmac


def _make_session_match(now: dt.datetime, tag: str = "LDN_METALS") -> sessions.SessionMatch:
    base_date = now.date()
    session_open = dt.datetime.combine(base_date, dt.time(8, 30), tzinfo=dt.timezone.utc)
    session_close = dt.datetime.combine(base_date, dt.time(16, 30), tzinfo=dt.timezone.utc)
    return sessions.SessionMatch(tag, "Europe/London", session_open, session_close, "UTC")


class FakeForexConnect:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()

    def extend(self, df_new: pd.DataFrame) -> None:
        self.df = pd.concat([self.df, df_new], ignore_index=True)

    def get_history(self, symbol: str, timeframe: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Any:
        mask = (self.df["Date"] >= start_dt) & (self.df["Date"] < end_dt)
        subset = self.df.loc[mask]
        return [row.to_dict() for _, row in subset.iterrows()]

    # ForexConnect API сумісність
    def login(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - не використовується
        return

    def logout(self) -> None:  # pragma: no cover - не використовується
        return


class FakeRedis:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def publish(self, channel: str, message: str) -> int:
        self.messages.append((channel, message))
        return 1


class DummySupervisor:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self.payload = payload
        self.calls = 0

    def diagnostics_snapshot(self) -> Dict[str, Any]:
        self.calls += 1
        return self.payload


class StaticPriceStream:
    def __init__(self, metadata: Dict[str, Any]) -> None:
        self._metadata = metadata

    def snapshot_metadata(self) -> Dict[str, Any]:
        return dict(self._metadata)


class CycleAwareCadence:
    """Емулятор TickCadence, що дозволяє кожен таймфрейм лише раз за цикл."""

    def __init__(self) -> None:
        self._seen: Dict[str, int] = {}

    def update_state(
        self,
        *,
        tick_metadata: Optional[Dict[str, Any]],
        market_open: bool,
        now_monotonic: Optional[float] = None,
    ) -> None:
        self._seen.clear()

    def should_poll(self, timeframe: str, now: Optional[float] = None) -> Tuple[bool, float]:
        tf = timeframe
        count = self._seen.get(tf, 0)
        self._seen[tf] = count + 1
        if count == 0:
            return True, 0.0
        return False, 5.0

    def next_wakeup_in_seconds(self, now_monotonic: Optional[float] = None) -> Optional[float]:
        return None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "state": "ok",
            "history_state": "active",
        }


def _make_sample_df(start: dt.datetime, count: int) -> pd.DataFrame:
    rows: Dict[str, list[Any]] = {k: [] for k in [
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
    ]}
    price = 4100.0
    for i in range(count):
        current = start + dt.timedelta(minutes=i)
        rows["Date"].append(current)
        bid_open = price + i * 0.1
        rows["BidOpen"].append(bid_open)
        rows["BidHigh"].append(bid_open + 0.05)
        rows["BidLow"].append(bid_open - 0.05)
        rows["BidClose"].append(bid_open + 0.02)
        rows["AskOpen"].append(bid_open + 0.01)
        rows["AskHigh"].append(bid_open + 0.06)
        rows["AskLow"].append(bid_open - 0.04)
        rows["AskClose"].append(bid_open + 0.03)
        rows["Volume"].append(100 + i)
    return pd.DataFrame(rows)


class AlwaysFailForexConnect:
    def __init__(self) -> None:
        self.calls = 0

    def get_history(self, symbol: str, timeframe: str, start_dt: dt.datetime, end_dt: dt.datetime) -> Any:
        self.calls += 1
        raise RuntimeError("session lost")

    def login(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - не використовується
        return

    def logout(self) -> None:  # pragma: no cover - не використовується
        return


class FailingOnceRedis(FakeRedis):
    def __init__(self) -> None:
        super().__init__()
        self.fail = True

    def publish(self, channel: str, message: str) -> int:
        if self.fail:
            self.fail = False
            raise RuntimeError("redis down")
        return super().publish(channel, message)


class WarmupStreamTest(unittest.TestCase):
    def setUp(self) -> None:
        connector._LAST_MARKET_STATUS = None

    def _build_df(self, start: dt.datetime, count: int) -> pd.DataFrame:
        return _make_sample_df(start, count)

    def test_warmup_and_stream_with_cache(self) -> None:
        fixed_now = dt.datetime(2025, 5, 5, 12, 0, tzinfo=dt.timezone.utc)
        holder = {"now": fixed_now}

        def fake_now() -> dt.datetime:
            return holder["now"]

        base_df = self._build_df(fixed_now - dt.timedelta(minutes=10), 8)
        fx = FakeForexConnect(base_df)
        redis_client = FakeRedis()

        with tempfile.TemporaryDirectory() as tmpdir, mock.patch(
            "connector._now_utc", side_effect=fake_now
        ):
            cache = HistoryCache(Path(tmpdir), max_bars=50, warmup_bars=5)
            cache.ensure_ready(
                cast(connector.ForexConnect, fx),
                symbol_raw="XAU/USD",
                timeframe_raw="m1",
            )
            warmup_slice = cache.get_bars_to_publish(
                symbol_raw="XAU/USD",
                timeframe_raw="m1",
                limit=5,
            )
            self.assertFalse(warmup_slice.empty)
            publish_ohlcv_to_redis(
                warmup_slice,
                symbol="XAU/USD",
                timeframe="1m",
                redis_client=redis_client,
            )
            cache.mark_published(
                symbol_raw="XAU/USD",
                timeframe_raw="m1",
                last_open_time=int(warmup_slice["open_time"].max()),
            )
            self.assertGreaterEqual(len(redis_client.messages), 1)
            ohlcv_messages = [
                (channel, payload)
                for channel, payload in redis_client.messages
                if channel == connector.REDIS_CHANNEL
            ]
            channel, payload = ohlcv_messages[-1]
            data = json.loads(payload)
            self.assertEqual(len(data["bars"]), len(warmup_slice))

            # Додаємо нові дані для стріму
            extra_df = self._build_df(fixed_now - dt.timedelta(minutes=2), 3)
            fx.extend(extra_df)
            holder["now"] = fixed_now + dt.timedelta(minutes=2)

            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=redis_client,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=cache,
                max_cycles=1,
            )

        self.assertGreaterEqual(len(redis_client.messages), 2)
        ohlcv_messages = [
            payload for channel, payload in redis_client.messages if channel == connector.REDIS_CHANNEL
        ]
        latest_payload = ohlcv_messages[-1]
        latest = json.loads(latest_payload)
        self.assertGreater(len(latest["bars"]), 0)

        status_messages = [
            json.loads(payload)
            for channel, payload in redis_client.messages
            if channel == connector.REDIS_STATUS_CHANNEL
        ]
        self.assertTrue(status_messages)
        self.assertEqual(status_messages[-1]["state"], "open")

    def test_market_status_closed(self) -> None:
        saturday = dt.datetime(2025, 5, 3, 12, 0, tzinfo=dt.timezone.utc)
        holder = {"now": saturday}

        def fake_now() -> dt.datetime:
            return holder["now"]

        fx = FakeForexConnect(self._build_df(saturday - dt.timedelta(minutes=5), 2))
        redis_client = FakeRedis()
        heartbeat_channel = "fxcm:test:heartbeat"

        with mock.patch("connector._now_utc", side_effect=fake_now):
            connector.stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=redis_client,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=None,
                max_cycles=1,
                heartbeat_channel=heartbeat_channel,
            )

        status_messages = [
            json.loads(payload)
            for channel, payload in redis_client.messages
            if channel == connector.REDIS_STATUS_CHANNEL
        ]
        self.assertTrue(status_messages)
        last_status = status_messages[-1]
        self.assertEqual(last_status["state"], "closed")
        self.assertIn("next_open_ms", last_status)
        self.assertIn("next_open_in_seconds", last_status)
        self.assertIn("session", last_status)
        self.assertEqual(last_status["session"].get("tag"), "NY_METALS")
        self.assertIn("timezone", last_status["session"])

        heartbeat_messages = [
            json.loads(payload)
            for channel, payload in redis_client.messages
            if channel == heartbeat_channel
        ]
        self.assertTrue(heartbeat_messages)
        last_heartbeat = heartbeat_messages[-1]
        self.assertEqual(last_heartbeat["state"], "idle")
        self.assertIn("context", last_heartbeat)
        self.assertEqual(last_heartbeat["context"]["mode"], "idle")
        self.assertIn("idle_reason", last_heartbeat["context"])
        self.assertIn("session", last_heartbeat["context"])


class BackoffControllerTest(unittest.TestCase):
    def test_backoff_remaining_and_snapshot(self) -> None:
        controller = connector.FxcmBackoffController(
            base_seconds=2.0,
            max_seconds=8.0,
            multiplier=2.0,
            jitter=0.0,
        )
        fake_time = {"value": 100.0}

        def fake_monotonic() -> float:
            return fake_time["value"]

        with mock.patch("connector.time.monotonic", side_effect=fake_monotonic):
            with mock.patch("connector.random.uniform", return_value=1.0):
                delay = controller.fail("fxcm_history")
                self.assertAlmostEqual(delay, 2.0)
            fake_time["value"] = 101.0
            remaining = controller.remaining()
            self.assertAlmostEqual(remaining, 1.0)
            snapshot = controller.snapshot()
            self.assertTrue(snapshot["active"])
            self.assertGreater(snapshot["remaining_seconds"], 0)
            controller.success()
            self.assertFalse(controller.snapshot()["active"])
            self.assertEqual(controller.remaining(), 0.0)

    def test_backoff_multiplier_capped_by_max(self) -> None:
        controller = connector.FxcmBackoffController(
            base_seconds=3.0,
            max_seconds=5.0,
            multiplier=2.0,
            jitter=0.0,
        )
        with mock.patch("connector.random.uniform", return_value=1.0), mock.patch(
            "connector.time.monotonic", return_value=50.0
        ):
            first = controller.fail("fxcm_history")
        with mock.patch("connector.random.uniform", return_value=1.0), mock.patch(
            "connector.time.monotonic", return_value=60.0
        ):
            second = controller.fail("fxcm_history")
        self.assertAlmostEqual(first, 3.0)
        self.assertAlmostEqual(second, 5.0)


class FetchRecentSinkTest(unittest.TestCase):
    def test_fetch_recent_uses_sink_without_redis(self) -> None:
        fixed_now = dt.datetime(2025, 5, 6, 10, 0, tzinfo=dt.timezone.utc)
        fx = FakeForexConnect(_make_sample_df(fixed_now - dt.timedelta(minutes=4), 4))
        captured: list[OhlcvBatch] = []
        last_open: Dict[Tuple[str, str], int] = {}
        gate = PublishDataGate()

        with mock.patch("connector._now_utc", return_value=fixed_now), mock.patch(
            "connector.is_trading_time", return_value=True
        ):
            df_result = connector._fetch_and_publish_recent(  # type: ignore[attr-defined]
                cast(connector.ForexConnect, fx),
                symbol="XAU/USD",
                timeframe_raw="m1",
                redis_client=None,
                lookback_minutes=5,
                last_open_time_ms=last_open,
                data_gate=gate,
                min_publish_interval=0,
                publish_rate_limit={},
                hmac_secret=None,
                hmac_algo="sha256",
                session_stats=None,
                ohlcv_sink=captured.append,
            )

        self.assertTrue(captured)
        batch = captured[-1]
        self.assertEqual(batch.symbol, "XAUUSD")
        self.assertFalse(df_result.empty)
        pd.testing.assert_frame_equal(batch.data.reset_index(drop=True), df_result.reset_index(drop=True))
        self.assertTrue(last_open)


class PriceSnapshotWorkerTest(unittest.TestCase):
    def test_flush_publishes_latest_tick(self) -> None:
        redis_client = FakeRedis()
        worker = PriceSnapshotWorker(
            lambda: redis_client,
            channel="fxcm:test:ticks",
            interval_seconds=0.1,
            symbols=["XAU/USD"],
        )
        worker.enqueue_tick("XAU/USD", 2010.1, 2010.3, tick_ts=1_700_000_000.0)
        worker.enqueue_tick("XAU/USD", 2010.2, 2010.4, tick_ts=1_700_000_003.0)
        worker.flush()
        messages = [payload for channel, payload in redis_client.messages if channel == "fxcm:test:ticks"]
        self.assertTrue(messages)
        payload = json.loads(messages[-1])
        self.assertEqual(payload["symbol"], "XAUUSD")
        self.assertAlmostEqual(payload["bid"], 2010.2)
        self.assertIn("snap_ts", payload)
        metadata = worker.snapshot_metadata()
        self.assertEqual(metadata["channel"], "fxcm:test:ticks")
        self.assertTrue(metadata["symbols"])


class TickCadenceControllerTest(unittest.TestCase):
    def _build_controller(self) -> connector.TickCadenceController:
        tuning = TickCadenceTuning(
            live_threshold_seconds=10,
            idle_threshold_seconds=60,
            live_multiplier=1.0,
            idle_multiplier=3.0,
        )
        return connector.TickCadenceController(
            tuning=tuning,
            base_poll_seconds=3.0,
            timeframes=["m1", "m5"],
        )

    def test_live_state_allows_fast_poll(self) -> None:
        controller = self._build_controller()
        controller.update_state(
            tick_metadata={"tick_silence_seconds": 2.0},
            market_open=True,
            now_monotonic=0.0,
        )
        snapshot = controller.snapshot()
        self.assertEqual(snapshot["state"], "ok")
        allowed_first, _ = controller.should_poll("m1", now=0.0)
        self.assertTrue(allowed_first)
        allowed_second, wait_second = controller.should_poll("m1", now=1.0)
        self.assertFalse(allowed_second)
        self.assertGreater(wait_second, 0.0)

    def test_idle_state_pauses_history_until_ticks_return(self) -> None:
        controller = self._build_controller()
        controller.update_state(
            tick_metadata={"tick_silence_seconds": 120.0},
            market_open=True,
            now_monotonic=5.0,
        )
        snapshot = controller.snapshot()
        self.assertEqual(snapshot["history_state"], "paused")
        allowed_idle, _ = controller.should_poll("m1", now=5.0)
        self.assertFalse(allowed_idle)

        controller.update_state(
            tick_metadata={"tick_silence_seconds": 1.0},
            market_open=True,
            now_monotonic=6.0,
        )
        snapshot_live = controller.snapshot()
        self.assertEqual(snapshot_live["history_state"], "active")
        resumed_allowed, _ = controller.should_poll("m1", now=6.0)
        self.assertTrue(resumed_allowed)

    def test_cadence_respects_bounds(self) -> None:
        tuning = TickCadenceTuning(
            live_threshold_seconds=5,
            idle_threshold_seconds=10,
            live_multiplier=0.1,
            idle_multiplier=10.0,
        )
        controller = connector.TickCadenceController(
            tuning=tuning,
            base_poll_seconds=1.0,
            timeframes=["m1"],
        )
        controller.update_state(
            tick_metadata={"tick_silence_seconds": 1.0},
            market_open=True,
            now_monotonic=0.0,
        )
        snap_live = controller.snapshot()
        min_bound = connector.TickCadenceController.MIN_CADENCE_SECONDS
        self.assertGreaterEqual(snap_live["cadence_seconds"]["1m"], min_bound)

        controller.update_state(
            tick_metadata={"tick_silence_seconds": 60.0},
            market_open=True,
            now_monotonic=10.0,
        )
        snap_idle = controller.snapshot()
        max_bound = connector.TickCadenceController.MAX_CADENCE_SECONDS
        self.assertLessEqual(snap_idle["cadence_seconds"]["1m"], max_bound)


class TickCadenceAutosleepTest(unittest.TestCase):
    def setUp(self) -> None:
        connector._LAST_MARKET_STATUS = None
        self.fixed_now = dt.datetime(2025, 5, 6, 10, 0, tzinfo=dt.timezone.utc)
        self.stream_config = [("XAU/USD", "m1")]

    def _build_cadence(
        self,
        *,
        live_multiplier: float,
        idle_multiplier: float,
    ) -> connector.TickCadenceController:
        tuning = TickCadenceTuning(
            live_threshold_seconds=5.0,
            idle_threshold_seconds=30.0,
            live_multiplier=live_multiplier,
            idle_multiplier=idle_multiplier,
        )
        return connector.TickCadenceController(
            tuning=tuning,
            base_poll_seconds=3.0,
            timeframes=[tf for _, tf in self.stream_config],
        )

    def test_idle_ticks_extend_sleep_interval(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 5))
        heartbeat_events: list[HeartbeatEvent] = []
        tick_cadence = self._build_cadence(live_multiplier=1.0, idle_multiplier=4.0)
        price_stream = StaticPriceStream({"tick_silence_seconds": 300.0})

        with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
            "connector.is_trading_time",
            return_value=True,
        ), mock.patch("connector._fetch_and_publish_recent") as mock_fetch:
            mock_fetch.return_value = pd.DataFrame()
            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=None,
                require_redis=False,
                poll_seconds=3,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=self.stream_config,
                cache_manager=None,
                max_cycles=1,
                heartbeat_sink=heartbeat_events.append,
                price_stream=price_stream,
                tick_cadence=tick_cadence,
            )

        mock_fetch.assert_not_called()
        self.assertTrue(heartbeat_events)
        event = heartbeat_events[-1]
        self.assertEqual(event.state, "stream")
        self.assertIsNotNone(event.sleep_seconds)
        assert event.sleep_seconds is not None
        self.assertGreater(event.sleep_seconds, 0.0)
        self.assertEqual(event.context.get("history_state"), "paused")

    def test_live_ticks_reduce_sleep_interval(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 5))
        heartbeat_events: list[HeartbeatEvent] = []
        tick_cadence = self._build_cadence(live_multiplier=0.5, idle_multiplier=2.0)
        price_stream = StaticPriceStream({"tick_silence_seconds": 0.5})

        with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
            "connector.is_trading_time",
            return_value=True,
        ), mock.patch("connector._fetch_and_publish_recent") as mock_fetch:
            mock_fetch.return_value = pd.DataFrame()
            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=None,
                require_redis=False,
                poll_seconds=4,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=self.stream_config,
                cache_manager=None,
                max_cycles=1,
                heartbeat_sink=heartbeat_events.append,
                price_stream=price_stream,
                tick_cadence=tick_cadence,
            )

        mock_fetch.assert_called()
        self.assertTrue(heartbeat_events)
        event = heartbeat_events[-1]
        self.assertEqual(event.state, "stream")
        self.assertIsNotNone(event.sleep_seconds)
        assert event.sleep_seconds is not None
        self.assertLess(event.sleep_seconds, 4.0)
        self.assertEqual(event.context.get("history_state"), "active")


class TickCadenceFairnessTest(unittest.TestCase):
    def setUp(self) -> None:
        connector._LAST_MARKET_STATUS = None
        self.fixed_now = dt.datetime(2025, 5, 6, 10, 0, tzinfo=dt.timezone.utc)

    def test_cadence_slot_applies_to_all_symbols_with_same_tf(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 5))
        cadence = CycleAwareCadence()
        stub_df = pd.DataFrame(
            {
                "open_time": [1764958800000],
                "close_time": [1764958860000],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
            }
        )

        with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
            "connector.is_trading_time",
            return_value=True,
        ), mock.patch("connector._fetch_and_publish_recent") as mock_fetch:
            mock_fetch.return_value = stub_df
            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=None,
                require_redis=False,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1"), ("EUR/USD", "m1")],
                cache_manager=None,
                max_cycles=1,
                tick_cadence=cadence,
            )

        self.assertEqual(mock_fetch.call_count, 2)
        symbols = [kwargs["symbol"] for _args, kwargs in mock_fetch.call_args_list]
        self.assertEqual(symbols, ["XAU/USD", "EUR/USD"])


class HeartbeatContractTest(unittest.TestCase):
    def setUp(self) -> None:
        connector._LAST_MARKET_STATUS = None
        self.fixed_now = dt.datetime(2025, 5, 5, 12, 0, tzinfo=dt.timezone.utc)

    def _extract_channel_messages(self, redis_client: FakeRedis, channel: str) -> list[dict[str, Any]]:
        return [json.loads(payload) for published_channel, payload in redis_client.messages if published_channel == channel]

    def test_fetch_history_sample_emits_warmup_heartbeat(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=30), 15))
        redis_client = FakeRedis()
        heartbeat_channel = "fxcm:test:heartbeat"
        sample_settings = SampleRequestSettings(symbol="XAU/USD", timeframe="m1", hours=1)
        gate = PublishDataGate()

        with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
            "connector.is_trading_time", return_value=True
        ):
            published = connector.fetch_history_sample(
                cast(connector.ForexConnect, fx),
                redis_client=redis_client,
                sample_settings=sample_settings,
                heartbeat_channel=heartbeat_channel,
                data_gate=gate,
            )

        self.assertTrue(published)
        heartbeat_messages = self._extract_channel_messages(redis_client, heartbeat_channel)
        self.assertTrue(heartbeat_messages)
        last_msg = heartbeat_messages[-1]
        self.assertEqual(last_msg["state"], "warmup")
        self.assertIn("last_bar_close_ms", last_msg)
        self.assertIn("context", last_msg)
        self.assertEqual(last_msg["context"].get("mode"), "warmup_sample")
        self.assertIn("stream_targets", last_msg["context"])
        self.assertIn("session", last_msg["context"])

    def test_stream_publishes_stream_heartbeat(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=10), 10))
        redis_client = FakeRedis()
        heartbeat_channel = "fxcm:test:heartbeat"

        session_match = _make_session_match(self.fixed_now)

        with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
            "connector.is_trading_time", return_value=True
        ), mock.patch("connector.resolve_session", return_value=session_match):
            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=redis_client,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=None,
                max_cycles=1,
                heartbeat_channel=heartbeat_channel,
            )

        heartbeat_messages = self._extract_channel_messages(redis_client, heartbeat_channel)
        self.assertTrue(heartbeat_messages)
        last_msg = heartbeat_messages[-1]
        self.assertEqual(last_msg["state"], "stream")
        self.assertIn("last_bar_close_ms", last_msg)
        self.assertIn("context", last_msg)
        ctx = last_msg["context"]
        self.assertEqual(ctx.get("mode"), "stream")
        self.assertIn("stream_targets", ctx)


class HistoryQuotaTest(unittest.TestCase):
    def test_sliding_windows_limit_calls(self) -> None:
        quota = connector.HistoryQuota(max_calls_per_min=2, max_calls_per_hour=4)
        base_now = time.monotonic()
        allowed_first, wait_first, reason_first = quota.allow_call("XAU/USD", "1m", base_now)
        self.assertTrue(allowed_first)
        self.assertEqual(wait_first, 0.0)
        self.assertEqual(reason_first, "ok")
        quota.record_call("XAU/USD", "1m", base_now)

        allowed_second, _, _ = quota.allow_call("XAU/USD", "1m", base_now + 1.0)
        self.assertTrue(allowed_second)
        quota.record_call("XAU/USD", "1m", base_now + 1.0)

        allowed_third, wait_third, reason_third = quota.allow_call("XAU/USD", "1m", base_now + 2.0)
        self.assertFalse(allowed_third)
        self.assertGreater(wait_third, 40.0)
        self.assertEqual(reason_third, "minute_quota")
        snapshot = quota.snapshot()
        self.assertEqual(snapshot["calls_60s"], 2)
        self.assertEqual(snapshot["throttle_state"], "critical")

    def test_min_interval_and_skipped_snapshot(self) -> None:
        quota = connector.HistoryQuota(
            max_calls_per_min=10,
            max_calls_per_hour=20,
            min_interval_by_tf={"1m": 2.0},
        )
        base_now = time.monotonic()
        required_calls = int(quota.max_calls_per_min * connector.HistoryQuota._MIN_INTERVAL_THRESHOLD) + 1
        for i in range(required_calls):
            quota.record_call("EUR/USD", "1m", base_now - 10 + i * 0.1)
        allowed, _, _ = quota.allow_call("EUR/USD", "1m", base_now)
        self.assertTrue(allowed)
        quota.record_call("EUR/USD", "1m", base_now)
        allowed_fast, wait_fast, reason_fast = quota.allow_call("EUR/USD", "1m", base_now + 0.5)
        self.assertFalse(allowed_fast)
        self.assertGreater(wait_fast, 0.9)
        self.assertEqual(reason_fast, "min_interval")
        quota.register_skip("EUR/USD", "1m")
        snapshot = quota.snapshot()
        skipped = snapshot.get("skipped_polls") or []
        self.assertTrue(skipped)
        self.assertEqual(skipped[0]["symbol"], "EURUSD")
        self.assertEqual(skipped[0]["tf"], "1m")
        self.assertEqual(snapshot["throttle_state"], "ok")

    def test_min_interval_disabled_when_load_low(self) -> None:
        quota = connector.HistoryQuota(
            max_calls_per_min=120,
            max_calls_per_hour=2400,
            min_interval_by_tf={"1m": 2.0},
        )
        now = time.monotonic()
        allowed_first, _, _ = quota.allow_call("EUR/USD", "1m", now)
        self.assertTrue(allowed_first)
        quota.record_call("EUR/USD", "1m", now)
        allowed_second, wait_second, reason_second = quota.allow_call("EUR/USD", "1m", now + 0.5)
        self.assertTrue(allowed_second)
        self.assertEqual(wait_second, 0.0)
        self.assertEqual(reason_second, "ok")

    def test_custom_min_interval_threshold_triggers_early(self) -> None:
        quota = connector.HistoryQuota(
            max_calls_per_min=20,
            max_calls_per_hour=40,
            min_interval_by_tf={"1m": 2.0},
            load_thresholds={"min_interval": 0.1},
        )
        base_now = time.monotonic()
        for offset in (-3.0, -0.5, -0.2):
            quota.record_call("EUR/USD", "1m", base_now + offset)
        allowed_fast, wait_fast, reason_fast = quota.allow_call("EUR/USD", "1m", base_now)
        self.assertFalse(allowed_fast)
        self.assertGreater(wait_fast, 1.5)
        self.assertEqual(reason_fast, "min_interval")

    def test_warn_state_when_usage_high(self) -> None:
        quota = connector.HistoryQuota(max_calls_per_min=4, max_calls_per_hour=40)
        base_now = time.monotonic()
        for i in range(3):
            quota.record_call("XAU/USD", "1m", base_now + i * 0.1)
        snapshot = quota.snapshot()
        self.assertEqual(snapshot["throttle_state"], "warn")

    def test_warn_threshold_can_be_configured(self) -> None:
        quota = connector.HistoryQuota(
            max_calls_per_min=10,
            max_calls_per_hour=40,
            load_thresholds={"warn": 0.3},
        )
        base_now = time.monotonic()
        for i in range(3):
            quota.record_call("XAU/USD", "1m", base_now + i * 0.1)
        snapshot = quota.snapshot()
        self.assertEqual(snapshot["throttle_state"], "warn")

    def test_priority_skips_min_interval_under_load(self) -> None:
        quota = connector.HistoryQuota(
            max_calls_per_min=10,
            max_calls_per_hour=20,
            min_interval_by_tf={"1m": 2.0},
            priority_targets=[("XAU/USD", "m1")],
        )
        base_now = time.monotonic()
        required_calls = int(quota.max_calls_per_min * connector.HistoryQuota._MIN_INTERVAL_THRESHOLD) + 1
        for i in range(required_calls):
            quota.record_call("EUR/USD", "1m", base_now - 10 + i * 0.1)
        allowed_first, _, _ = quota.allow_call("XAU/USD", "1m", base_now)
        self.assertTrue(allowed_first)
        quota.record_call("XAU/USD", "1m", base_now)
        allowed_second, wait_second, reason_second = quota.allow_call("XAU/USD", "1m", base_now + 0.5)
        self.assertTrue(allowed_second)
        self.assertEqual(wait_second, 0.0)
        self.assertEqual(reason_second, "ok")

    def test_recent_quota_denial_promotes_state(self) -> None:
        quota = connector.HistoryQuota(max_calls_per_min=2, max_calls_per_hour=40)
        base_now = time.monotonic()
        for offset in (0.0, 0.5):
            allowed, _, _ = quota.allow_call("XAU/USD", "1m", base_now + offset)
            self.assertTrue(allowed)
            quota.record_call("XAU/USD", "1m", base_now + offset)
        allowed_blocked, _, reason = quota.allow_call("XAU/USD", "1m", base_now + 1.0)
        self.assertFalse(allowed_blocked)
        self.assertEqual(reason, "minute_quota")
        quota._calls_60s.clear()
        quota._calls_3600s.clear()
        snapshot = quota.snapshot()
        self.assertEqual(snapshot["throttle_state"], "warn")

    def test_priority_reserve_blocks_non_priority(self) -> None:
        quota = connector.HistoryQuota(
            max_calls_per_min=4,
            max_calls_per_hour=40,
            priority_targets=[("XAU/USD", "m1")],
            priority_reserve_per_min=2,
        )
        now = 0.0
        for _ in range(2):
            allowed, _, _ = quota.allow_call("XAU/USD", "m1", now)
            self.assertTrue(allowed)
            quota.record_call("XAU/USD", "m1", now)
            now += 1.0

        for _ in range(2):
            allowed_non_priority, _, _ = quota.allow_call("EUR/USD", "m1", now)
            self.assertTrue(allowed_non_priority)
            quota.record_call("EUR/USD", "m1", now)
            now += 1.0

        allowed_blocked, wait_blocked, reason_blocked = quota.allow_call("EUR/USD", "m1", now)
        self.assertFalse(allowed_blocked)
        self.assertGreater(wait_blocked, 0.0)
        self.assertEqual(reason_blocked, "priority_reserve")


class HistoryThrottleIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        connector._LAST_MARKET_STATUS = None
        self.fixed_now = dt.datetime(2025, 5, 5, 12, 0, tzinfo=dt.timezone.utc)

    def _extract_channel_messages(self, redis_client: FakeRedis, channel: str) -> list[dict[str, Any]]:
        return [json.loads(payload) for published_channel, payload in redis_client.messages if published_channel == channel]

    def test_stream_exposes_quota_in_heartbeat(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=10), 12))
        redis_client = FakeRedis()
        heartbeat_events: list[HeartbeatEvent] = []
        quota = connector.HistoryQuota(max_calls_per_min=2, max_calls_per_hour=10)
        session_match = _make_session_match(self.fixed_now)

        with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
            "connector.is_trading_time", return_value=True
        ), mock.patch("connector.resolve_session", return_value=session_match):
            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=redis_client,
                require_redis=False,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=None,
                max_cycles=3,
                heartbeat_sink=heartbeat_events.append,
                history_quota=quota,
            )

        self.assertTrue(heartbeat_events)
        last_event = heartbeat_events[-1]
        self.assertEqual(last_event.state, "stream")
        history_ctx = last_event.context.get("history") if last_event.context else None
        self.assertIsNotNone(history_ctx)
        assert isinstance(history_ctx, dict)
        self.assertEqual(history_ctx.get("calls_60s"), 2)
        skipped = history_ctx.get("skipped_polls") or []
        self.assertTrue(skipped)
        self.assertGreaterEqual(skipped[0].get("count", 0), 1)

    def test_stream_heartbeat_includes_supervisor_context(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=10), 10))
        redis_client = FakeRedis()
        heartbeat_channel = "fxcm:test:heartbeat"
        supervisor_payload = {"state": "running", "queues": [], "tasks": []}
        supervisor = DummySupervisor(supervisor_payload)

        with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
            "connector.is_trading_time", return_value=True
        ):
            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=redis_client,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=None,
                max_cycles=1,
                heartbeat_channel=heartbeat_channel,
                async_supervisor=cast(connector.AsyncStreamSupervisor, supervisor),
            )

        heartbeat_messages = self._extract_channel_messages(redis_client, heartbeat_channel)
        self.assertTrue(heartbeat_messages)
        ctx = heartbeat_messages[-1]["context"]
        self.assertIn("supervisor", ctx)
        self.assertEqual(ctx["supervisor"], supervisor_payload)
        self.assertEqual(ctx.get("async_supervisor"), supervisor_payload)
        self.assertGreaterEqual(supervisor.calls, 1)

    def test_idle_heartbeat_uses_cache_last_close(self) -> None:
        df = _make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 5)
        normalized = connector._normalize_history_to_ohlcv(df, symbol="XAU/USD", timeframe="1m")
        expected_close = int(normalized["close_time"].max())
        redis_client = FakeRedis()
        heartbeat_channel = "fxcm:test:heartbeat"

        with tempfile.TemporaryDirectory() as tmpdir:
            cache = HistoryCache(Path(tmpdir), max_bars=10, warmup_bars=3)
            cache.append_stream_bars(
                symbol_raw="XAU/USD",
                timeframe_raw="m1",
                df_new=normalized,
            )

            with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
                "connector.is_trading_time", return_value=False
            ):
                stream_fx_data(
                    cast(connector.ForexConnect, FakeForexConnect(df)),
                    redis_client=redis_client,
                    poll_seconds=0,
                    publish_interval_seconds=0,
                    lookback_minutes=5,
                    config=[("XAU/USD", "m1")],
                    cache_manager=cache,
                    max_cycles=1,
                    heartbeat_channel=heartbeat_channel,
                )

        heartbeat_messages = self._extract_channel_messages(redis_client, heartbeat_channel)
        self.assertTrue(heartbeat_messages)
        last_msg = heartbeat_messages[-1]
        self.assertEqual(last_msg["state"], "idle")
        self.assertEqual(last_msg.get("last_bar_close_ms"), expected_close)
        self.assertIn("context", last_msg)
        ctx = last_msg["context"]
        self.assertEqual(ctx.get("mode"), "idle")
        self.assertIn("lag_seconds", ctx)
        self.assertIn("session", ctx)

    def test_price_stream_metadata_present_in_stream_context(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 5))
        redis_client = FakeRedis()
        heartbeat_channel = "fxcm:test:heartbeat"

        class DummyPriceStream:
            def snapshot_metadata(self) -> dict[str, Any]:
                return {"channel": "fxcm:test:ticks", "symbols": ["XAUUSD"], "interval_seconds": 1.0}

        dummy_stream = DummyPriceStream()

        with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
            "connector.is_trading_time", return_value=True
        ), mock.patch("connector.resolve_session", return_value=_make_session_match(self.fixed_now)):
            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=redis_client,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=None,
                max_cycles=1,
                heartbeat_channel=heartbeat_channel,
                price_stream=dummy_stream,  # type: ignore[arg-type]
            )

        heartbeat_messages = [
            json.loads(payload)
            for channel, payload in redis_client.messages
            if channel == heartbeat_channel
        ]
        self.assertTrue(heartbeat_messages)
        last_msg = heartbeat_messages[-1]
        ctx = last_msg["context"]
        self.assertIn("price_stream", ctx)
        self.assertEqual(ctx["price_stream"]["channel"], "fxcm:test:ticks")


class ReconnectLogicTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixed_now = dt.datetime(2025, 5, 5, 12, 0, tzinfo=dt.timezone.utc)

    def test_stream_recovers_from_fxcm_failure(self) -> None:
        failing_fx = AlwaysFailForexConnect()
        replacement_fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 3))
        redis_client = FakeRedis()
        reconnect_calls = {"count": 0}

        def fx_reconnector() -> connector.ForexConnect:
            reconnect_calls["count"] += 1
            return cast(connector.ForexConnect, replacement_fx)

        holder = {"now": self.fixed_now}

        def fake_now() -> dt.datetime:
            return holder["now"]

        with mock.patch("connector._now_utc", side_effect=fake_now):
            stream_fx_data(
                cast(connector.ForexConnect, failing_fx),
                redis_client=redis_client,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=None,
                max_cycles=1,
                fx_reconnector=fx_reconnector,
            )

        self.assertGreater(reconnect_calls["count"], 0)
        self.assertGreater(len(redis_client.messages), 0)

    def test_stream_recovers_from_redis_failure(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 3))
        flaky_redis = FailingOnceRedis()
        replacement_redis = FakeRedis()
        reconnect_calls = {"count": 0}

        def redis_reconnector() -> FakeRedis:
            reconnect_calls["count"] += 1
            return replacement_redis

        def fake_now() -> dt.datetime:
            return self.fixed_now

        with mock.patch("connector._now_utc", side_effect=fake_now):
            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=flaky_redis,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=None,
                max_cycles=1,
                redis_reconnector=redis_reconnector,
            )

        self.assertEqual(reconnect_calls["count"], 1)
        self.assertFalse(flaky_redis.messages)
        self.assertGreater(len(replacement_redis.messages), 0)

    def test_fx_session_observer_invoked_on_reconnect(self) -> None:
        fx_initial = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 3))
        fx_replacement = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 3))
        redis_client = FakeRedis()
        observer_calls: list[connector.ForexConnect] = []

        def observer(client: connector.ForexConnect) -> None:
            observer_calls.append(client)

        fx_reconnector = mock.Mock(return_value=cast(connector.ForexConnect, fx_replacement))

        side_effects = [connector.FXCMRetryableError("fx down"), pd.DataFrame()]

        with mock.patch("connector._fetch_and_publish_recent", side_effect=side_effects), mock.patch(
            "connector._now_utc", return_value=self.fixed_now
        ), mock.patch("connector.is_trading_time", return_value=True):
            stream_fx_data(
                cast(connector.ForexConnect, fx_initial),
                redis_client=redis_client,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=None,
                max_cycles=1,
                fx_reconnector=fx_reconnector,
                fx_session_observer=observer,
            )

        self.assertEqual(fx_reconnector.call_count, 1)
        self.assertGreaterEqual(len(observer_calls), 2)
        self.assertIs(observer_calls[0], fx_initial)
        self.assertIs(observer_calls[-1], fx_replacement)


class AsyncSupervisorTest(unittest.TestCase):
    def setUp(self) -> None:
        connector._LAST_MARKET_STATUS = None
        connector._LAST_MARKET_STATUS_TS = None
        self.redis = FakeRedis()
        self.gate = PublishDataGate()
        self.supervisor = AsyncStreamSupervisor(
            redis_supplier=lambda: self.redis,
            data_gate=self.gate,
            heartbeat_channel="fxcm:test:heartbeat",
            price_channel="fxcm:test:ticks",
        )
        self.supervisor.start()

    def tearDown(self) -> None:
        self.supervisor.stop()

    @staticmethod
    def _wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return False

    def test_supervisor_publishes_ohlcv_batches(self) -> None:
        fixed_now = dt.datetime(2025, 5, 6, 12, 0, tzinfo=dt.timezone.utc)
        df = _make_sample_df(fixed_now - dt.timedelta(minutes=3), 3)
        normalized = connector._normalize_history_to_ohlcv(df, "XAU/USD", "1m")
        batch = OhlcvBatch(
            symbol="XAUUSD",
            timeframe="1m",
            data=normalized,
            fetched_at=time.time(),
            source="test",
        )

        self.supervisor.submit_ohlcv_batch(batch)

        self.assertTrue(
            self._wait_for(
                lambda: any(channel == connector.REDIS_CHANNEL for channel, _ in self.redis.messages)
            )
        )
        payloads = [
            json.loads(payload)
            for channel, payload in self.redis.messages
            if channel == connector.REDIS_CHANNEL
        ]
        self.assertTrue(payloads)
        last = payloads[-1]
        self.assertEqual(last["symbol"], "XAUUSD")
        self.assertEqual(len(last["bars"]), len(normalized))

    def test_supervisor_publishes_heartbeat_and_status(self) -> None:
        heartbeat = HeartbeatEvent(
            state="stream",
            last_bar_close_ms=111,
            next_open=None,
            sleep_seconds=None,
            context={"channel": "fxcm:test:heartbeat"},
        )
        self.supervisor.submit_heartbeat(heartbeat)
        status_event = MarketStatusEvent(state="open", next_open=None)
        self.supervisor.submit_market_status(status_event)

        self.assertTrue(
            self._wait_for(
                lambda: any(channel == "fxcm:test:heartbeat" for channel, _ in self.redis.messages)
            )
        )
        self.assertTrue(
            self._wait_for(
                lambda: any(channel == connector.REDIS_STATUS_CHANNEL for channel, _ in self.redis.messages)
            )
        )

    def test_heartbeat_queue_drops_when_full(self) -> None:
        self.supervisor.stop()
        self.redis = FakeRedis()
        self.gate = PublishDataGate()
        with mock.patch.object(connector, "HEARTBEAT_QUEUE_MAXSIZE", 1):
            supervisor = AsyncStreamSupervisor(
                redis_supplier=lambda: self.redis,
                data_gate=self.gate,
                heartbeat_channel="fxcm:test:heartbeat",
                price_channel="fxcm:test:ticks",
            )
            supervisor.start()
        self.supervisor = supervisor

        publish_counter = {"count": 0}

        async def slow_publish(self_obj: AsyncStreamSupervisor, event: HeartbeatEvent) -> None:
            publish_counter["count"] += 1
            await asyncio.sleep(0.2)

        heartbeat = HeartbeatEvent(
            state="idle",
            last_bar_close_ms=222,
            next_open=None,
            sleep_seconds=5.0,
            context={"channel": "fxcm:test:heartbeat"},
        )

        diag: Dict[str, Any] = {}
        with mock.patch.object(AsyncStreamSupervisor, "_publish_heartbeat_event", slow_publish):
            try:
                supervisor.submit_heartbeat(heartbeat)
                supervisor.submit_heartbeat(heartbeat)
                supervisor.submit_heartbeat(heartbeat)
                self.assertTrue(self._wait_for(lambda: publish_counter["count"] >= 1))
                diag = supervisor.diagnostics_snapshot()
            finally:
                supervisor.stop()

        heartbeat_diag = next((entry for entry in diag.get("queues", []) if entry["name"] == "heartbeat"), None)
        self.assertIsNotNone(heartbeat_diag)
        self.assertGreaterEqual(heartbeat_diag.get("dropped", 0), 1)
        self.assertEqual(diag.get("backpressure_events"), 0)

    def test_supervisor_publishes_price_snapshots(self) -> None:
        snap = PriceTickSnap(
            symbol="XAUUSD",
            bid=2010.1,
            ask=2010.3,
            mid=2010.2,
            tick_ts=1_700_000_000.0,
            snap_ts=1_700_000_003.0,
        )
        self.supervisor.submit_price_snapshot(snap)
        self.assertTrue(
            self._wait_for(
                lambda: any(channel == "fxcm:test:ticks" for channel, _ in self.redis.messages)
            )
        )

    def test_diagnostics_snapshot_exposes_metrics(self) -> None:
        diag = self.supervisor.diagnostics_snapshot()

        self.assertTrue(diag)
        self.assertIn("publish_counts", diag)
        self.assertEqual(diag.get("backpressure_events"), 0)
        metrics = diag.get("metrics")
        self.assertIsInstance(metrics, dict)
        self.assertIn("publishes_total", metrics)
        self.assertIn("queue_depth_total", metrics)


class OfferSubscriptionFallbackTest(unittest.TestCase):
    def test_fallback_listener_used_when_common_missing(self) -> None:
        class DummyListenerBase:
            def __init__(self) -> None:
                pass

        dummy_fxcorepy = types.SimpleNamespace(
            O2GTableType=types.SimpleNamespace(OFFERS="OFFERS"),
            O2GTableUpdateType=types.SimpleNamespace(INSERT="insert", UPDATE="update"),
            AO2GTableListener=DummyListenerBase,
        )

        class DummyOffersTable:
            def __init__(self) -> None:
                self.subscriptions: list[tuple[Any, Any]] = []

            def subscribe_update(self, update_type: Any, listener: Any) -> None:
                self.subscriptions.append((update_type, listener))

            def unsubscribe_update(self, update_type: Any, listener: Any) -> None:
                self.subscriptions.remove((update_type, listener))

        class DummyManager:
            def __init__(self, table: DummyOffersTable) -> None:
                self._table = table

            def get_table(self, _table_id: Any) -> DummyOffersTable:
                return self._table

        class DummyFx:
            def __init__(self, table: DummyOffersTable) -> None:
                self.table_manager = DummyManager(table)

        offers_table = DummyOffersTable()
        fx = DummyFx(offers_table)
        old_common = connector.Common
        old_fxcorepy = connector.fxcorepy
        had_offers_attr = hasattr(connector.ForexConnect, "OFFERS")
        old_offers_attr = getattr(connector.ForexConnect, "OFFERS", None)
        try:
            connector.Common = None
            connector.fxcorepy = dummy_fxcorepy
            connector.ForexConnect.OFFERS = "OFFERS"
            subscription = connector.FXCMOfferSubscription(  # type: ignore[arg-type]
                fx,
                symbols=["XAU/USD"],
                on_tick=lambda *_: None,
            )
            self.assertTrue(subscription._active)
            self.assertEqual(len(offers_table.subscriptions), 2)
            subscription.close()
            self.assertFalse(offers_table.subscriptions)
        finally:
            connector.Common = old_common
            connector.fxcorepy = old_fxcorepy
            if had_offers_attr:
                connector.ForexConnect.OFFERS = old_offers_attr
            else:
                try:
                    delattr(connector.ForexConnect, "OFFERS")
                except AttributeError:
                    pass


class FileOnlyModeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixed_now = dt.datetime(2025, 5, 5, 12, 0, tzinfo=dt.timezone.utc)

    def test_stream_runs_without_redis_when_not_required(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 5))
        gate = PublishDataGate()

        with mock.patch("connector._now_utc", return_value=self.fixed_now), mock.patch(
            "connector.is_trading_time", return_value=True
        ):
            stream_fx_data(
                cast(connector.ForexConnect, fx),
                redis_client=None,
                require_redis=False,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=5,
                config=[("XAU/USD", "m1")],
                cache_manager=None,
                max_cycles=1,
                data_gate=gate,
            )

        staleness = gate.staleness_seconds(symbol="XAU/USD", timeframe="m1")
        self.assertIsNotNone(staleness)


class DataQualityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixed_now = dt.datetime(2025, 5, 6, 12, 0, tzinfo=dt.timezone.utc)

    def test_normalize_history_respects_tf_and_filters_anomalies(self) -> None:
        df = _make_sample_df(self.fixed_now - dt.timedelta(minutes=10), 3)
        df.loc[df.index[0], "Date"] = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)

        with mock.patch("connector._now_utc", return_value=self.fixed_now):
            normalized = connector._normalize_history_to_ohlcv(
                df,
                symbol="XAU/USD",
                timeframe="5m",
            )

        self.assertEqual(len(normalized), 2)
        bar_duration = normalized.loc[0, "close_time"] - normalized.loc[0, "open_time"] + 1
        self.assertEqual(bar_duration, 5 * 60 * 1000)
        self.assertTrue((normalized["open_time"] >= connector.MIN_ALLOWED_BAR_TIMESTAMP_MS).all())

    def test_history_cache_append_and_publish(self) -> None:
        df = _make_sample_df(self.fixed_now - dt.timedelta(minutes=5), 5)
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch(
            "connector._now_utc", return_value=self.fixed_now
        ):
            cache = HistoryCache(Path(tmpdir), max_bars=10, warmup_bars=3)
            symbol = "XAU/USD"
            tf = "m1"
            normalized = connector._normalize_history_to_ohlcv(df, symbol, "1m")
            cache.append_stream_bars(
                symbol_raw=symbol,
                timeframe_raw=tf,
                df_new=normalized,
            )
            subset = cache.get_bars_to_publish(
                symbol_raw=symbol,
                timeframe_raw=tf,
                limit=2,
                force=True,
            )
            self.assertEqual(len(subset), 2)
            last_open = int(subset["open_time"].max())
            cache.mark_published(
                symbol_raw=symbol,
                timeframe_raw=tf,
                last_open_time=last_open,
            )
            subsequent = cache.get_bars_to_publish(
                symbol_raw=symbol,
                timeframe_raw=tf,
                limit=5,
            )
            self.assertEqual(len(subsequent), 0)

    def test_publish_data_gate_blocks_duplicates(self) -> None:
        redis_client = FakeRedis()
        gate = PublishDataGate()
        df = _make_sample_df(self.fixed_now - dt.timedelta(minutes=3), 3)
        with mock.patch("connector._now_utc", return_value=self.fixed_now):
            normalized = connector._normalize_history_to_ohlcv(df, "XAU/USD", "1m")
        first = publish_ohlcv_to_redis(
            normalized,
            symbol="XAU/USD",
            timeframe="1m",
            redis_client=redis_client,
            data_gate=gate,
        )
        self.assertTrue(first)
        published_before = len(redis_client.messages)
        second = publish_ohlcv_to_redis(
            normalized,
            symbol="XAU/USD",
            timeframe="1m",
            redis_client=redis_client,
            data_gate=gate,
        )
        self.assertFalse(second)
        self.assertEqual(len(redis_client.messages), published_before)


class HMACTest(unittest.TestCase):
    def test_publish_adds_signature_when_secret_set(self) -> None:
        fixed_now = dt.datetime(2025, 5, 5, 12, 0, tzinfo=dt.timezone.utc)
        df = _make_sample_df(fixed_now - dt.timedelta(minutes=2), 2)
        with mock.patch("connector._now_utc", return_value=fixed_now):
            normalized = connector._normalize_history_to_ohlcv(df, "XAU/USD", "1m")
        redis_client = FakeRedis()
        secret = "super_secret"
        publish_ohlcv_to_redis(
            normalized,
            symbol="XAU/USD",
            timeframe="1m",
            redis_client=redis_client,
            hmac_secret=secret,
            hmac_algo="sha256",
        )
        self.assertTrue(redis_client.messages)
        _, payload_raw = redis_client.messages[-1]
        payload = json.loads(payload_raw)
        sig = payload.pop("sig", None)
        self.assertIsNotNone(sig)
        expected = compute_payload_hmac(payload, secret, "sha256")
        self.assertEqual(sig, expected)

    def test_publish_skips_signature_when_secret_missing(self) -> None:
        fixed_now = dt.datetime(2025, 5, 5, 12, 0, tzinfo=dt.timezone.utc)
        df = _make_sample_df(fixed_now - dt.timedelta(minutes=2), 2)
        with mock.patch("connector._now_utc", return_value=fixed_now):
            normalized = connector._normalize_history_to_ohlcv(df, "EUR/USD", "1m")
        redis_client = FakeRedis()
        publish_ohlcv_to_redis(
            normalized,
            symbol="EUR/USD",
            timeframe="1m",
            redis_client=redis_client,
            hmac_secret=None,
        )
        _, payload_raw = redis_client.messages[-1]
        payload = json.loads(payload_raw)
        self.assertNotIn("sig", payload)


class CalendarOverrideTest(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshot = sessions.calendar_snapshot()

    def tearDown(self) -> None:
        sessions.override_calendar(
            holidays=self.snapshot["holidays"],
            daily_breaks=self.snapshot["daily_breaks"],
            weekly_open=self.snapshot["weekly_open"],
            weekly_close=self.snapshot["weekly_close"],
            replace_holidays=True,
        )

    def test_next_trading_open_respects_new_york_standard_time(self) -> None:
        ts = dt.datetime(2025, 11, 30, 21, 29, 43, tzinfo=dt.timezone.utc)
        expected = dt.datetime(2025, 11, 30, 23, 0, tzinfo=dt.timezone.utc)
        self.assertEqual(sessions.next_trading_open(ts), expected)

    def test_next_trading_open_respects_new_york_dst(self) -> None:
        ts = dt.datetime(2025, 7, 13, 20, 29, 0, tzinfo=dt.timezone.utc)
        expected = dt.datetime(2025, 7, 13, 22, 0, tzinfo=dt.timezone.utc)
        self.assertEqual(sessions.next_trading_open(ts), expected)

    def test_is_trading_time_false_during_new_york_maintenance(self) -> None:
        maintenance_dt = dt.datetime(2025, 7, 7, 21, 30, tzinfo=dt.timezone.utc)
        after_reopen = dt.datetime(2025, 7, 7, 22, 5, tzinfo=dt.timezone.utc)
        self.assertFalse(sessions.is_trading_time(maintenance_dt))
        self.assertTrue(sessions.is_trading_time(after_reopen))

    def test_is_trading_time_handles_ahead_timezone_weekend(self) -> None:
        sessions.override_calendar(
            weekly_open="00:00@Asia/Tokyo",
            weekly_close="21:55@America/New_York",
        )
        friday_evening = dt.datetime(2025, 12, 5, 15, 0, tzinfo=dt.timezone.utc)
        self.assertTrue(sessions.is_trading_time(friday_evening))


    def test_override_calendar_with_extra_holiday(self) -> None:
        sessions.override_calendar(holidays=["2030-01-02"], replace_holidays=True)
        ts = dt.datetime(2030, 1, 2, 12, 0, tzinfo=dt.timezone.utc)
        self.assertFalse(sessions.is_trading_time(ts))

    def test_load_calendar_from_json_file(self) -> None:
        payload = {
            "holidays": ["2030-07-04"],
            "daily_breaks": ["10:00-10:15"],
            "weekly_open_utc": "21:00",
            "weekly_close_utc": "21:30",
        }
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as tmp:
            json.dump(payload, tmp)
            tmp_path = Path(tmp.name)
        try:
            sessions.load_calendar_file(tmp_path)
            holiday_ts = dt.datetime(2030, 7, 4, 12, 0, tzinfo=dt.timezone.utc)
            break_ts = dt.datetime(2030, 7, 3, 10, 5, tzinfo=dt.timezone.utc)
            before_open = dt.datetime(2030, 7, 6, 20, 30, tzinfo=dt.timezone.utc)
            self.assertFalse(sessions.is_trading_time(holiday_ts))
            self.assertFalse(sessions.is_trading_time(break_ts))
            self.assertFalse(sessions.is_trading_time(before_open))
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


class StatusSnapshotTest(unittest.TestCase):
    def setUp(self) -> None:
        connector._LAST_STATUS_SNAPSHOT = None
        connector._LAST_SESSION_CONTEXT = None
        connector._CURRENT_MARKET_STATE = "open"

    def test_status_snapshot_from_heartbeat_reflects_ok_states(self) -> None:
        fake_now = dt.datetime(2025, 12, 5, 6, 0, tzinfo=dt.timezone.utc)
        payload = {
            "type": "heartbeat",
            "state": "stream",
            "ts": fake_now.isoformat(),
            "last_bar_close_ms": int(fake_now.timestamp() * 1000),
            "context": {
                "lag_seconds": 5,
                "price_stream": {"tick_silence_seconds": 1.0, "state": "ok"},
                "session": {
                    "tag": "TOKYO",
                    "session_open_utc": "2025-12-05T00:00:00+00:00",
                    "session_close_utc": "2025-12-05T09:00:00+00:00",
                    "next_open_utc": "2025-12-05T09:00:00+00:00",
                    "next_open_seconds": 5400,
                },
                "stream_targets": [
                    {"symbol": "XAU/USD", "tf": "m1", "staleness_seconds": 5},
                ],
            },
        }
        with mock.patch("connector._now_utc", return_value=fake_now):
            snapshot = connector._build_status_snapshot_from_heartbeat(payload)
        assert snapshot is not None
        self.assertEqual(snapshot["process"], "stream")
        self.assertEqual(snapshot["market"], "open")
        self.assertEqual(snapshot["price"], "ok")
        self.assertEqual(snapshot["ohlcv"], "ok")
        self.assertIn("session", snapshot)
        self.assertEqual(snapshot["note"], "ok")

    def test_status_snapshot_from_market_updates_existing_snapshot(self) -> None:
        connector._LAST_STATUS_SNAPSHOT = {
            "ts": 1.0,
            "process": "stream",
            "market": "open",
            "price": "ok",
            "ohlcv": "ok",
            "note": "ok",
        }
        fake_now = dt.datetime(2025, 12, 5, 20, 0, tzinfo=dt.timezone.utc)
        payload = {
            "state": "closed",
            "session": {
                "tag": "LDN_METALS",
                "session_open_utc": "2025-12-05T08:30:00+00:00",
                "session_close_utc": "2025-12-05T14:30:00+00:00",
                "next_open_utc": "2025-12-06T00:00:00+00:00",
                "next_open_seconds": 14400,
            },
        }
        with mock.patch("connector._now_utc", return_value=fake_now):
            snapshot = connector._build_status_snapshot_from_market(payload)
        assert snapshot is not None
        self.assertEqual(snapshot["market"], "closed")
        self.assertEqual(snapshot["process"], "idle")
        self.assertEqual(snapshot["note"], "market closed")
        self.assertIn("session", snapshot)
        session = snapshot["session"]
        self.assertIsInstance(session, dict)
        self.assertEqual(session.get("tag"), "LDN_METALS")
        self.assertEqual(session.get("state"), "closed")
        self.assertEqual(session.get("state_detail"), "intrabreak")

    def test_status_snapshot_marks_weekend(self) -> None:
        fake_now = dt.datetime(2025, 12, 6, 12, 0, tzinfo=dt.timezone.utc)
        payload = {
            "type": "heartbeat",
            "state": "idle",
            "ts": fake_now.isoformat(),
            "context": {
                "idle_reason": "calendar_closed",
                "next_open_seconds": 172800.0,
                "session": {
                    "tag": "NY_METALS",
                    "session_open_utc": "2025-12-05T14:30:00+00:00",
                    "session_close_utc": "2025-12-05T21:00:00+00:00",
                    "next_open_utc": "2025-12-08T00:00:00+00:00",
                    "next_open_seconds": 172800.0,
                },
            },
        }
        with mock.patch("connector._now_utc", return_value=fake_now):
            snapshot = connector._build_status_snapshot_from_heartbeat(payload)
        assert snapshot is not None
        self.assertEqual(snapshot["note"], "weekend (2d)")
        session = snapshot["session"]
        self.assertIsInstance(session, dict)
        self.assertEqual(session.get("state"), "closed")
        self.assertEqual(session.get("state_detail"), "weekend")


class IdleSleepHelperTest(unittest.TestCase):
    def test_idle_sleep_not_less_than_poll(self) -> None:
        self.assertEqual(connector._resolve_idle_sleep_seconds(5, None), 5.0)
        self.assertEqual(connector._resolve_idle_sleep_seconds(5, 0.0), 5.0)
        self.assertEqual(connector._resolve_idle_sleep_seconds(5, -2.0), 5.0)
        self.assertEqual(connector._resolve_idle_sleep_seconds(5, 7.5), 7.5)


if __name__ == "__main__":
    unittest.main()
