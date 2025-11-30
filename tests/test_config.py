from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import config


class BackoffConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self._base_env = {
            "FXCM_USERNAME": "demo_user",
            "FXCM_PASSWORD": "demo_pass",
        }

    def _write_runtime_settings(self, payload: dict[str, object]) -> Path:
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as tmp:
            json.dump(payload, tmp)
            tmp_path = Path(tmp.name)
        return tmp_path

    def _load_with_payload(self, payload: dict[str, object]):
        runtime_path = self._write_runtime_settings(payload)
        try:
            with mock.patch.dict(os.environ, self._base_env, clear=False):
                with mock.patch.object(config, "RUNTIME_SETTINGS_FILE", runtime_path):
                    return config.load_config()
        finally:
            try:
                runtime_path.unlink()
            except FileNotFoundError:
                pass

    def test_load_config_reads_backoff_policies(self) -> None:
        payload = {
            "backoff": {
                "fxcm_login": {"base_delay": 3, "factor": 1.5, "max_delay": 45},
                "fxcm_stream": {"base_delay": 7, "factor": 3, "max_delay": 600},
                "redis_stream": {"base_delay": 2, "factor": 2.5, "max_delay": 120},
            }
        }
        cfg = self._load_with_payload(payload)
        self.assertEqual(cfg.backoff.fxcm_login.base_delay, 3.0)
        self.assertEqual(cfg.backoff.fxcm_login.factor, 1.5)
        self.assertEqual(cfg.backoff.fxcm_login.max_delay, 45.0)
        self.assertEqual(cfg.backoff.fxcm_stream.base_delay, 7.0)
        self.assertEqual(cfg.backoff.fxcm_stream.factor, 3.0)
        self.assertEqual(cfg.backoff.fxcm_stream.max_delay, 600.0)
        self.assertEqual(cfg.backoff.redis_stream.base_delay, 2.0)
        self.assertEqual(cfg.backoff.redis_stream.factor, 2.5)
        self.assertEqual(cfg.backoff.redis_stream.max_delay, 120.0)

    def test_backoff_policy_values_are_clamped(self) -> None:
        payload = {
            "backoff": {
                "fxcm_login": {"base_delay": -5, "factor": 0.5, "max_delay": 0},
            }
        }
        cfg = self._load_with_payload(payload)
        self.assertEqual(cfg.backoff.fxcm_login.base_delay, 0.1)
        self.assertEqual(cfg.backoff.fxcm_login.factor, 1.0)
        self.assertEqual(cfg.backoff.fxcm_login.max_delay, 0.1)
        self.assertEqual(cfg.backoff.fxcm_stream.base_delay, 5.0)
        self.assertEqual(cfg.backoff.fxcm_stream.factor, 2.0)
        self.assertEqual(cfg.backoff.fxcm_stream.max_delay, 300.0)
        self.assertEqual(cfg.backoff.redis_stream.base_delay, 1.0)
        self.assertEqual(cfg.backoff.redis_stream.factor, 2.0)
        self.assertEqual(cfg.backoff.redis_stream.max_delay, 60.0)

    def test_stream_intervals_default_and_override(self) -> None:
        runtime_payload = {
            "stream": {
                "poll_seconds": 7,
                "publish_interval_seconds": 9,
            }
        }
        cfg = self._load_with_payload(runtime_payload)
        self.assertEqual(cfg.poll_seconds, 7)
        self.assertEqual(cfg.publish_interval_seconds, 9)

        runtime_payload["stream"]["fetch_interval_seconds"] = 3
        cfg_override = self._load_with_payload(runtime_payload)
        self.assertEqual(cfg_override.poll_seconds, 3)
        self.assertEqual(cfg_override.publish_interval_seconds, 9)


if __name__ == "__main__":
    unittest.main()
