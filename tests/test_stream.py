from __future__ import annotations

import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, cast
from unittest import mock

import pandas as pd

import connector
import sessions

from config import SampleRequestSettings
from connector import HistoryCache, PublishDataGate, publish_ohlcv_to_redis, stream_fx_data
from fxcm_security import compute_payload_hmac


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
        self.assertEqual(status_messages[-1]["state"], "closed")

        heartbeat_messages = [
            json.loads(payload)
            for channel, payload in redis_client.messages
            if channel == heartbeat_channel
        ]
        self.assertTrue(heartbeat_messages)
        self.assertEqual(heartbeat_messages[-1]["state"], "idle")


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
        self.assertEqual(heartbeat_messages[-1]["state"], "warmup")
        self.assertIn("last_bar_close_ms", heartbeat_messages[-1])

    def test_stream_publishes_stream_heartbeat(self) -> None:
        fx = FakeForexConnect(_make_sample_df(self.fixed_now - dt.timedelta(minutes=10), 10))
        redis_client = FakeRedis()
        heartbeat_channel = "fxcm:test:heartbeat"

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
            )

        heartbeat_messages = self._extract_channel_messages(redis_client, heartbeat_channel)
        self.assertTrue(heartbeat_messages)
        self.assertEqual(heartbeat_messages[-1]["state"], "stream")
        self.assertIn("last_bar_close_ms", heartbeat_messages[-1])

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


if __name__ == "__main__":
    unittest.main()
