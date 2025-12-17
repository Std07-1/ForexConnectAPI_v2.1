from __future__ import annotations

import datetime as dt
import unittest
from unittest import mock

import pandas as pd

import connector
from config import MtfCrosscheckSettings


class FakeFxForCrosscheck:
    def __init__(self, *, bar_dt: dt.datetime, close_value: float) -> None:
        self._bar_dt = bar_dt
        self._close_value = float(close_value)

    def get_history(self, symbol: str, timeframe: str, start_dt: dt.datetime, end_dt: dt.datetime):
        # Повертаємо 1 H1 бар з навмисно "поганим" close.
        return [
            {
                "Date": self._bar_dt,
                "BidOpen": 1.0,
                "BidHigh": 1.0,
                "BidLow": 1.0,
                "BidClose": float(self._close_value),
                "AskOpen": 1.0,
                "AskHigh": 1.0,
                "AskLow": 1.0,
                "AskClose": float(self._close_value),
                "Volume": 10.0,
            }
        ]


class MtfCrosscheckTest(unittest.TestCase):
    def test_crosscheck_logs_warning_on_price_mismatch(self) -> None:
        bar_dt = dt.datetime(2025, 12, 17, 10, 0, tzinfo=dt.timezone.utc)
        open_ms = int(bar_dt.timestamp() * 1000)
        close_ms = open_ms + 60 * 60 * 1000 - 1

        df_agg = pd.DataFrame(
            [
                {
                    "open_time": open_ms,
                    "close_time": close_ms,
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 10.0,
                    "source": "history_agg",
                }
            ]
        )

        settings = MtfCrosscheckSettings(
            enabled=True,
            timeframes=["1h"],
            min_interval_seconds=0.0,
            max_checks_per_cycle=10,
            price_abs_tol=0.0001,
            price_rel_tol=0.0,
            volume_abs_tol=0.0,
            volume_rel_tol=0.0,
        )
        ctrl = connector.MtfCrosscheckController(settings)
        fx = FakeFxForCrosscheck(bar_dt=bar_dt, close_value=2.0)

        # Нормалізатор історії відкидає бари з close_time у майбутньому.
        # Для детермінованого тесту фіксуємо "поточний" час так, щоб bar_dt був у минулому.
        fake_now = dt.datetime(2025, 12, 18, 0, 0, tzinfo=dt.timezone.utc)
        with mock.patch.object(connector, "_now_utc", return_value=fake_now), mock.patch.object(
            connector.log, "warning"
        ) as warn:
            ctrl.begin_cycle()
            ctrl.maybe_check_from_agg_df(
                fx,  # type: ignore[arg-type]
                symbol_raw="XAU/USD",
                target_tf="1h",
                df_agg=df_agg,
            )
            self.assertTrue(warn.called)
