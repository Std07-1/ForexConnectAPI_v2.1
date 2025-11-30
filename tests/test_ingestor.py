from __future__ import annotations

import unittest
from unittest import mock

from fxcm_ingestor import FXCMIngestor, IngestValidationError, IngestorConfig
from fxcm_security import compute_payload_hmac


class FXCMIngestorValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        config = IngestorConfig(
            allowed_symbols=("XAU/USD", "EUR/USD"),
            allowed_timeframes=("1m", "5m"),
            max_bars_per_payload=10,
        )
        self.ingestor = FXCMIngestor(config)
        self.now_ms = 1_746_000_000_000
        patcher = mock.patch("fxcm_ingestor._now_ms", return_value=self.now_ms)
        self.addCleanup(patcher.stop)
        patcher.start()

    def _sample_bar(self, offset_minutes: int) -> dict[str, float]:
        base_open = self.now_ms - offset_minutes * 60_000
        return {
            "open_time": base_open,
            "close_time": base_open + 59_000,
            "open": 2000.0 + offset_minutes,
            "high": 2000.5 + offset_minutes,
            "low": 1999.5 + offset_minutes,
            "close": 2000.2 + offset_minutes,
            "volume": 150 + offset_minutes,
        }

    def test_validate_payload_happy_path(self) -> None:
        payload = {
            "symbol": "XAUUSD",
            "tf": "m1",
            "bars": [self._sample_bar(2), self._sample_bar(1)],
        }
        result = self.ingestor.validate_payload(payload)
        self.assertEqual(result["symbol"], "XAUUSD")
        self.assertEqual(result["tf"], "1m")
        self.assertEqual(len(result["bars"]), 2)
        self.assertGreater(result["bars"][1]["open_time"], result["bars"][0]["open_time"])

    def test_rejects_unknown_symbol(self) -> None:
        payload = {"symbol": "GBPUSD", "tf": "1m", "bars": [self._sample_bar(1)]}
        with self.assertRaises(IngestValidationError):
            self.ingestor.validate_payload(payload)

    def test_rejects_unknown_timeframe(self) -> None:
        payload = {"symbol": "XAUUSD", "tf": "2h", "bars": [self._sample_bar(1)]}
        with self.assertRaises(IngestValidationError):
            self.ingestor.validate_payload(payload)

    def test_rejects_batch_larger_than_limit(self) -> None:
        strict_config = IngestorConfig(
            allowed_symbols=("XAU/USD",),
            allowed_timeframes=("1m",),
            max_bars_per_payload=1,
        )
        ingestor = FXCMIngestor(strict_config)
        with mock.patch("fxcm_ingestor._now_ms", return_value=self.now_ms):
            payload = {"symbol": "XAUUSD", "tf": "1m", "bars": [self._sample_bar(1), self._sample_bar(0)]}
            with self.assertRaises(IngestValidationError):
                ingestor.validate_payload(payload)

    def test_validate_json_message_requires_object(self) -> None:
        with self.assertRaises(IngestValidationError):
            self.ingestor.validate_json_message("[]")

    def test_hmac_signature_required_when_secret_enabled(self) -> None:
        secure_config = IngestorConfig(
            allowed_symbols=("XAU/USD",),
            allowed_timeframes=("1m",),
            max_bars_per_payload=10,
            hmac_secret="top_secret",
        )
        secure_ingestor = FXCMIngestor(secure_config)
        payload = {
            "symbol": "XAUUSD",
            "tf": "1m",
            "bars": [self._sample_bar(1)],
        }
        payload["sig"] = compute_payload_hmac(
            {"symbol": payload["symbol"], "tf": payload["tf"], "bars": payload["bars"]},
            "top_secret",
            "sha256",
        )
        result = secure_ingestor.validate_payload(payload)
        self.assertEqual(len(result["bars"]), 1)

    def test_hmac_signature_missing_raises(self) -> None:
        secure_config = IngestorConfig(
            allowed_symbols=("XAU/USD",),
            allowed_timeframes=("1m",),
            max_bars_per_payload=10,
            hmac_secret="top_secret",
        )
        secure_ingestor = FXCMIngestor(secure_config)
        payload = {
            "symbol": "XAUUSD",
            "tf": "1m",
            "bars": [self._sample_bar(1)],
        }
        with self.assertRaises(IngestValidationError):
            secure_ingestor.validate_payload(payload)

    def test_hmac_signature_invalid_raises(self) -> None:
        secure_config = IngestorConfig(
            allowed_symbols=("XAU/USD",),
            allowed_timeframes=("1m",),
            max_bars_per_payload=10,
            hmac_secret="top_secret",
        )
        secure_ingestor = FXCMIngestor(secure_config)
        payload = {
            "symbol": "XAUUSD",
            "tf": "1m",
            "bars": [self._sample_bar(1)],
            "sig": "deadbeef",
        }
        with self.assertRaises(IngestValidationError):
            secure_ingestor.validate_payload(payload)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
