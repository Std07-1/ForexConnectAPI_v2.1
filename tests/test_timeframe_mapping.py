from __future__ import annotations

import unittest

import connector


class TimeframeMappingTest(unittest.TestCase):
    def test_map_timeframe_label_accepts_fxcm_aliases(self) -> None:
        self.assertEqual(connector._map_timeframe_label("m1"), "1m")
        self.assertEqual(connector._map_timeframe_label("m15"), "15m")
        self.assertEqual(connector._map_timeframe_label("m60"), "1h")
        self.assertEqual(connector._map_timeframe_label("H1"), "1h")
        self.assertEqual(connector._map_timeframe_label("h4"), "4h")
        self.assertEqual(connector._map_timeframe_label("H4"), "4h")

    def test_to_fxcm_timeframe_uses_hour_format(self) -> None:
        self.assertEqual(connector._to_fxcm_timeframe("1m"), "m1")
        self.assertEqual(connector._to_fxcm_timeframe("15m"), "m15")
        self.assertEqual(connector._to_fxcm_timeframe("1h"), "H1")
        self.assertEqual(connector._to_fxcm_timeframe("4h"), "H4")
        self.assertEqual(connector._to_fxcm_timeframe("1d"), "D1")

    def test_to_fxcm_timeframe_is_robust_to_raw_inputs(self) -> None:
        # Іноді сюди можуть передати "сирі" TF (наприклад, із конфігу/логів).
        self.assertEqual(connector._to_fxcm_timeframe("m60"), "H1")
        self.assertEqual(connector._to_fxcm_timeframe("h1"), "H1")
        self.assertEqual(connector._to_fxcm_timeframe("h4"), "H4")
        self.assertEqual(connector._to_fxcm_timeframe("d1"), "D1")


if __name__ == "__main__":
    unittest.main()
