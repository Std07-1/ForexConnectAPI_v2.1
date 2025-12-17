from __future__ import annotations

import datetime as dt
import json
import unittest
from typing import Any
from unittest import mock

import pandas as pd

import connector
from connector import stream_fx_data


def _make_sample_df(start: dt.datetime, count: int) -> pd.DataFrame:
    rows: dict[str, list[Any]] = {k: [] for k in [
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


class RecordingForexConnect:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df.copy()
        self.calls: list[str] = []

    def get_history(
        self, symbol: str, timeframe: str, start_dt: dt.datetime, end_dt: dt.datetime
    ) -> Any:
        self.calls.append(str(timeframe))
        mask = (self._df["Date"] >= start_dt) & (self._df["Date"] < end_dt)
        subset = self._df.loc[mask]
        return [row.to_dict() for _, row in subset.iterrows()]

    def login(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        return

    def logout(self) -> None:  # pragma: no cover
        return


class FakeRedis:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def publish(self, channel: str, message: str) -> int:
        self.messages.append((channel, message))
        return 1


class MtfHistoryAggregationTest(unittest.TestCase):
    def setUp(self) -> None:
        connector._LAST_MARKET_STATUS = None
        connector._LAST_MARKET_STATUS_TS = None
        connector._LAST_STATUS_PUBLISHED_TS = None
        connector._LAST_STATUS_PUBLISHED_KEY = None
        connector._LAST_TELEMETRY_PUBLISHED_TS_BY_CHANNEL.clear()

    def test_stream_polls_fxcm_only_1m_and_emits_1h_complete_from_history_agg(self) -> None:
        fixed_now = dt.datetime(2025, 5, 5, 12, 0, tzinfo=dt.timezone.utc)
        start = fixed_now - dt.timedelta(hours=2)
        df = _make_sample_df(start, 120)
        fx = RecordingForexConnect(df)
        redis_client = FakeRedis()

        with mock.patch("connector._now_utc", return_value=fixed_now), mock.patch(
            "connector.is_trading_time", return_value=True
        ):
            stream_fx_data(
                fx,  # type: ignore[arg-type]
                redis_client=redis_client,
                require_redis=True,
                poll_seconds=0,
                publish_interval_seconds=0,
                lookback_minutes=200,
                config=[("XAU/USD", "1h")],
                cache_manager=None,
                max_cycles=1,
                tick_aggregation_enabled=False,
            )

        # FXCM history викликається лише для m1.
        self.assertTrue(fx.calls)
        self.assertEqual(set(fx.calls), {"m1"})

        ohlcv_msgs = [
            json.loads(payload)
            for channel, payload in redis_client.messages
            if channel == connector.REDIS_CHANNEL
        ]
        self.assertTrue(ohlcv_msgs)

        one_h_msgs = [m for m in ohlcv_msgs if m.get("tf") == "1h"]
        self.assertTrue(one_h_msgs)

        total_bars = sum(len(m.get("bars") or []) for m in one_h_msgs)
        self.assertEqual(total_bars, 2)

        bars = []
        for m in one_h_msgs:
            bars.extend(m.get("bars") or [])
        bars = sorted(bars, key=lambda b: int(b.get("open_time", 0)))

        # Перевіряємо, що це дві повні години (UTC) з close_time inclusive.
        first = bars[0]
        second = bars[1]
        self.assertEqual(int(first["close_time"]) - int(first["open_time"]) + 1, 60 * 60 * 1000)
        self.assertEqual(int(second["close_time"]) - int(second["open_time"]) + 1, 60 * 60 * 1000)

        # І що відмічено джерело.
        self.assertIn(first.get("source"), {"history_agg", None})
