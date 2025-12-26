from __future__ import annotations

import datetime as dt
import unittest
from typing import Any
from unittest import mock

import pandas as pd

import connector


class NoDataForexConnect:
    def get_history(
        self, symbol: str, timeframe: str, start_dt: dt.datetime, end_dt: dt.datetime
    ) -> Any:
        raise RuntimeError(
            "QuotesManager error: Reason='unsupported scope', Description=No data found for "
            "symbolID=4001, interval=100, startTimeStamp=20251226-00:00:00.000, "
            "endTimeStamp=20251226-04:01:01.000! code: QuotesLoaderError subcode: 1"
        )

    def login(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        return

    def logout(self) -> None:  # pragma: no cover
        return


class HistoryNoDataHandlingTest(unittest.TestCase):
    def setUp(self) -> None:
        connector._LAST_MARKET_STATUS = None
        connector._LAST_MARKET_STATUS_TS = None
        connector._LAST_STATUS_PUBLISHED_TS = None
        connector._LAST_STATUS_PUBLISHED_KEY = None
        connector._LAST_TELEMETRY_PUBLISHED_TS_BY_CHANNEL.clear()

    def test_download_history_range_no_data_returns_empty_df(self) -> None:
        fx = NoDataForexConnect()
        start = dt.datetime(2025, 12, 26, 0, 0, tzinfo=dt.timezone.utc)
        end = start + dt.timedelta(hours=1)

        with mock.patch(
            "connector.generate_request_windows",
            return_value=iter([(start, end)]),
        ):
            df = connector._download_history_range(  # type: ignore[attr-defined]
                fx,  # type: ignore[arg-type]
                symbol="XAU/USD",
                timeframe_raw="m1",
                start_dt=start,
                end_dt=end,
            )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    def test_fetch_and_publish_recent_no_data_returns_empty_df(self) -> None:
        fx = NoDataForexConnect()
        fixed_now = dt.datetime(2025, 12, 26, 4, 0, tzinfo=dt.timezone.utc)

        with mock.patch("connector._now_utc", return_value=fixed_now), mock.patch(
            "connector.is_trading_time", return_value=True
        ):
            df = connector._fetch_and_publish_recent(  # type: ignore[attr-defined]
                fx,  # type: ignore[arg-type]
                symbol="XAU/USD",
                timeframe_raw="m1",
                redis_client=None,
                lookback_minutes=240,
                last_open_time_ms={},
            )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)
