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

    def _load_with_payload(self, payload: dict[str, object], extra_env: dict[str, str] | None = None):
        runtime_path = self._write_runtime_settings(payload)
        try:
            env = dict(self._base_env)
            if extra_env:
                env.update(extra_env)
            with mock.patch.dict(os.environ, env, clear=False):
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

    def test_price_stream_settings_defaults_and_override(self) -> None:
        cfg_default = self._load_with_payload({})
        self.assertEqual(cfg_default.price_stream.channel, config.PRICE_SNAPSHOT_CHANNEL_DEFAULT)
        self.assertAlmostEqual(cfg_default.price_stream.interval_seconds, 3.0)

        runtime_payload = {
            "stream": {
                "price_snap_channel": "fxcm:test:price",
                "price_snap_interval_seconds": 1.25,
            }
        }
        cfg_override = self._load_with_payload(runtime_payload)
        self.assertEqual(cfg_override.price_stream.channel, "fxcm:test:price")
        self.assertAlmostEqual(cfg_override.price_stream.interval_seconds, 1.25)

    def test_status_channel_defaults_and_override(self) -> None:
        cfg_default = self._load_with_payload({})
        self.assertEqual(cfg_default.observability.status_channel, config.STATUS_CHANNEL_DEFAULT)

        runtime_payload = {"stream": {"status_channel": "fxcm:test:status"}}
        cfg_runtime = self._load_with_payload(runtime_payload)
        self.assertEqual(cfg_runtime.observability.status_channel, "fxcm:test:status")

        cfg_env = self._load_with_payload({}, {"FXCM_STATUS_CHANNEL": "fxcm:env:status"})
        self.assertEqual(cfg_env.observability.status_channel, "fxcm:env:status")

    def test_telemetry_min_publish_interval_defaults_and_override(self) -> None:
        cfg_default = self._load_with_payload({})
        self.assertAlmostEqual(cfg_default.observability.telemetry_min_publish_interval_seconds, 1.0)

        cfg_runtime = self._load_with_payload({"stream": {"telemetry_min_publish_interval_seconds": 5}})
        self.assertAlmostEqual(cfg_runtime.observability.telemetry_min_publish_interval_seconds, 5.0)

        cfg_alias = self._load_with_payload({"stream": {"status_publish_min_interval_seconds": 2}})
        self.assertAlmostEqual(cfg_alias.observability.telemetry_min_publish_interval_seconds, 2.0)

    def test_tick_cadence_defaults_and_override(self) -> None:
        cfg_default = self._load_with_payload({})
        self.assertAlmostEqual(cfg_default.tick_cadence.live_threshold_seconds, 30.0)
        self.assertAlmostEqual(cfg_default.tick_cadence.idle_threshold_seconds, 120.0)
        self.assertAlmostEqual(cfg_default.tick_cadence.live_multiplier, 1.0)
        self.assertAlmostEqual(cfg_default.tick_cadence.idle_multiplier, 2.5)

        runtime_payload = {
            "stream": {
                "tick_cadence": {
                    "live_threshold_seconds": 60,
                    "idle_threshold_seconds": 240,
                    "live_multiplier": 1.4,
                    "idle_multiplier": 3.2,
                }
            }
        }
        cfg_override = self._load_with_payload(runtime_payload)
        self.assertAlmostEqual(cfg_override.tick_cadence.live_threshold_seconds, 60.0)
        self.assertAlmostEqual(cfg_override.tick_cadence.idle_threshold_seconds, 240.0)
        self.assertAlmostEqual(cfg_override.tick_cadence.live_multiplier, 1.4)
        self.assertAlmostEqual(cfg_override.tick_cadence.idle_multiplier, 3.2)

    def test_history_quota_settings_defaults_and_override(self) -> None:
        cfg_default = self._load_with_payload({})
        self.assertEqual(cfg_default.history_max_calls_per_min, 120)
        self.assertEqual(cfg_default.history_max_calls_per_hour, 2400)
        self.assertIsNone(cfg_default.history_min_interval_seconds_m1)
        self.assertIsNone(cfg_default.history_min_interval_seconds_m5)
        self.assertEqual(cfg_default.history_priority_targets, [])
        self.assertEqual(cfg_default.history_priority_reserve_per_min, 0)
        self.assertEqual(cfg_default.history_priority_reserve_per_hour, 0)
        self.assertAlmostEqual(cfg_default.history_load_thresholds.min_interval, 0.45)
        self.assertAlmostEqual(cfg_default.history_load_thresholds.warn, 0.7)
        self.assertAlmostEqual(cfg_default.history_load_thresholds.reserve, 0.7)
        self.assertAlmostEqual(cfg_default.history_load_thresholds.critical, 0.9)

        runtime_payload = {
            "stream": {
                "history_max_calls_per_min": 12,
                "history_max_calls_per_hour": 120,
                "history_min_interval_seconds_m1": 1.5,
                "history_min_interval_seconds_m5": 7,
                "history_priority_targets": ["XAU/USD:m1", "XAU/USD:m5"],
                "history_priority_reserve_per_min": 5,
                "history_priority_reserve_per_hour": 50,
                "history_load_thresholds": {
                    "min_interval": 0.3,
                    "warn": 0.5,
                    "reserve": 0.6,
                    "critical": 0.8,
                },
            }
        }
        cfg_override = self._load_with_payload(runtime_payload)
        self.assertEqual(cfg_override.history_max_calls_per_min, 12)
        self.assertEqual(cfg_override.history_max_calls_per_hour, 120)
        self.assertAlmostEqual(cfg_override.history_min_interval_seconds_m1, 1.5)
        self.assertAlmostEqual(cfg_override.history_min_interval_seconds_m5, 7.0)
        self.assertEqual(cfg_override.history_priority_targets, [("XAU/USD", "m1"), ("XAU/USD", "m5")])
        self.assertEqual(cfg_override.history_priority_reserve_per_min, 5)
        self.assertEqual(cfg_override.history_priority_reserve_per_hour, 50)
        self.assertAlmostEqual(cfg_override.history_load_thresholds.min_interval, 0.3)
        self.assertAlmostEqual(cfg_override.history_load_thresholds.warn, 0.5)
        self.assertAlmostEqual(cfg_override.history_load_thresholds.reserve, 0.6)
        self.assertAlmostEqual(cfg_override.history_load_thresholds.critical, 0.8)

    def test_stream_backoff_tuning_defaults_and_override(self) -> None:
        cfg_default = self._load_with_payload({})
        self.assertAlmostEqual(cfg_default.history_backoff.base_seconds, 5.0)
        self.assertAlmostEqual(cfg_default.history_backoff.max_seconds, 180.0)
        self.assertAlmostEqual(cfg_default.history_backoff.multiplier, 2.0)
        self.assertAlmostEqual(cfg_default.history_backoff.jitter, 0.25)
        self.assertAlmostEqual(cfg_default.redis_backoff.base_seconds, 2.0)
        self.assertAlmostEqual(cfg_default.redis_backoff.max_seconds, 60.0)
        self.assertAlmostEqual(cfg_default.redis_backoff.multiplier, 2.0)
        self.assertAlmostEqual(cfg_default.redis_backoff.jitter, 0.2)

        runtime_payload = {
            "stream": {
                "history_backoff": {
                    "base_seconds": 3,
                    "max_seconds": 90,
                    "multiplier": 1.5,
                    "jitter": 0.1,
                },
                "redis_backoff": {
                    "base_seconds": 4,
                    "max_seconds": 30,
                    "multiplier": 3,
                    "jitter": 0.5,
                },
            }
        }
        cfg_override = self._load_with_payload(runtime_payload)
        self.assertAlmostEqual(cfg_override.history_backoff.base_seconds, 3.0)
        self.assertAlmostEqual(cfg_override.history_backoff.max_seconds, 90.0)
        self.assertAlmostEqual(cfg_override.history_backoff.multiplier, 1.5)
        self.assertAlmostEqual(cfg_override.history_backoff.jitter, 0.1)
        self.assertAlmostEqual(cfg_override.redis_backoff.base_seconds, 4.0)
        self.assertAlmostEqual(cfg_override.redis_backoff.max_seconds, 30.0)
        self.assertAlmostEqual(cfg_override.redis_backoff.multiplier, 3.0)
        self.assertAlmostEqual(cfg_override.redis_backoff.jitter, 0.5)

    def test_redis_required_flag_respects_env_override(self) -> None:
        cfg_default = self._load_with_payload({})
        self.assertTrue(cfg_default.redis_required)

        cfg_optional = self._load_with_payload({}, {"FXCM_REDIS_REQUIRED": "0"})
        self.assertFalse(cfg_optional.redis_required)


if __name__ == "__main__":
    unittest.main()
