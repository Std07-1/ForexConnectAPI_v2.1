from __future__ import annotations

import unittest

from tick_ohlcv import OhlcvBar

import connector


class TickAggResilienceTest(unittest.TestCase):
    def test_tick_agg_sink_failure_does_not_crash(self) -> None:
        def _bad_sink(_batch: connector.OhlcvBatch) -> None:
            raise RuntimeError("sink down")

        worker = connector.TickOhlcvWorker(
            enabled=True,
            symbols=["XAU/USD"],
            max_synth_gap_minutes=60,
            live_publish_min_interval_seconds=0.0,
            ohlcv_sink=_bad_sink,
        )

        bar = OhlcvBar(
            symbol="XAU/USD",
            tf="1m",
            start_ms=1_000,
            end_ms=60_000,
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            volume=1.0,
            complete=False,
            synthetic=False,
            source="tick_agg",
        )

        # Не має кидати виняток — worker продовжує працювати, навіть якщо sink тимчасово падає.
        worker._publish_tick_bar(bar)  # type: ignore[attr-defined]
