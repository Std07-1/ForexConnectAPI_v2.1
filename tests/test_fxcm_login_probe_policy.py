from __future__ import annotations

import datetime as dt
import unittest

import connector


class FxcmLoginProbePolicyTest(unittest.TestCase):
    def test_probe_delay_far_from_open_is_long(self) -> None:
        now = dt.datetime(2025, 12, 27, 0, 0, tzinfo=dt.timezone.utc)
        next_open = now + dt.timedelta(hours=24)
        delay = connector._compute_fxcm_login_probe_sleep_seconds(now, next_open=next_open)  # type: ignore[attr-defined]
        self.assertGreaterEqual(delay, 15 * 60)

    def test_probe_delay_close_to_open_is_short(self) -> None:
        now = dt.datetime(2025, 12, 27, 0, 0, tzinfo=dt.timezone.utc)
        next_open = now + dt.timedelta(minutes=3)
        delay = connector._compute_fxcm_login_probe_sleep_seconds(now, next_open=next_open)  # type: ignore[attr-defined]
        self.assertLessEqual(delay, 10.0)
        self.assertGreaterEqual(delay, 1.0)

    def test_probe_delay_never_oversleeps_open(self) -> None:
        now = dt.datetime(2025, 12, 27, 0, 0, tzinfo=dt.timezone.utc)
        next_open = now + dt.timedelta(seconds=20)
        delay = connector._compute_fxcm_login_probe_sleep_seconds(now, next_open=next_open)  # type: ignore[attr-defined]
        self.assertLess(delay, 20.0)
        self.assertGreaterEqual(delay, 1.0)
