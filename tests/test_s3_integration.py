from __future__ import annotations

import json
import threading
import time
import unittest
from contextlib import ExitStack
from typing import Any, List, Optional
from unittest import mock

import pandas as pd

import connector


class _FakePubSub:
    def __init__(self, messages: List[str], *, done: threading.Event) -> None:
        self._messages = list(messages)
        self._done = done
        self._closed = False

    def subscribe(self, _channel: str) -> None:
        return

    def get_message(self, timeout: float = 0.0) -> Optional[dict[str, Any]]:
        if self._closed:
            return None
        if self._messages:
            payload = self._messages.pop(0)
            if not self._messages:
                self._done.set()
            return {"data": payload}
        if timeout:
            time.sleep(min(0.01, float(timeout)))
        return None

    def close(self) -> None:
        self._closed = True


class S3IntegrationTest(unittest.TestCase):
    def test_command_worker_handles_warmup_backfill_and_set_universe(self) -> None:
        done = threading.Event()

        base_targets = [
            ("XAU/USD", "m1"),
            ("XAU/USD", "m5"),
            ("EUR/USD", "m15"),
        ]
        universe = connector.StreamUniverse(
            base_targets=base_targets,
            dynamic_enabled=True,
            default_targets=[("XAU/USD", "m1")],
            max_targets=2,
        )

        fx_holder: dict[str, connector.ForexConnect | None] = {"client": mock.Mock(spec=connector.ForexConnect)}
        cache = mock.Mock()
        cache.warmup_bars = 10
        cache.get_bars_to_publish.return_value = pd.DataFrame(
            [
                {
                    "open_time": 1000,
                    "close_time": 2000,
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1.0,
                }
            ]
        )

        messages = [
            json.dumps(
                {
                    "type": "fxcm_set_universe",
                    "targets": [
                        {"symbol": "XAUUSD", "tf": "1m"},
                        {"symbol": "XAUUSD", "tf": "5m"},
                        {"symbol": "USDJPY", "tf": "1m"},
                    ],
                    "reason": "smc_focus",
                }
            ),
            json.dumps({"type": "fxcm_warmup", "symbol": "XAUUSD", "tf": "1m", "min_history_bars": 250}),
            json.dumps({"type": "fxcm_backfill", "symbol": "XAUUSD", "tf": "1m", "lookback_minutes": 9999}),
            json.dumps({"type": "fxcm_backfill", "symbol": "EURUSD", "tf": "15m", "lookback_minutes": 9999}),
        ]

        redis_client = mock.Mock()
        redis_client.pubsub.return_value = _FakePubSub(messages, done=done)

        worker = connector.FxcmCommandWorker(
            redis_client=redis_client,
            fx_holder=fx_holder,
            cache_manager=cache,
            tick_aggregation_enabled=True,
            tick_agg_timeframes=("m1", "m5"),
            channel="fxcm:commands",
            ohlcv_channel="fxcm:ohlcv",
            stream_targets=base_targets,
            universe=universe,
            backfill_min_minutes=10,
            backfill_max_minutes=360,
        )

        with ExitStack() as stack:
            ensure_ready_mock = stack.enter_context(mock.patch.object(cache, "ensure_ready"))
            publish_mock = stack.enter_context(mock.patch.object(connector, "publish_ohlcv_to_redis"))
            backfill_mock = stack.enter_context(mock.patch.object(connector, "_fetch_and_publish_recent"))

            worker.start()
            self.assertTrue(done.wait(1.0), "Не всі повідомлення pubsub були прочитані")
            worker.stop()

            # fxcm_set_universe: max_targets=2, лише ті, що у base_targets.
            self.assertTrue(universe.has_dynamic_requests())
            self.assertEqual(
                universe.get_effective_targets(),
                [("XAU/USD", "m1"), ("XAU/USD", "m5")],
            )

            # fxcm_warmup для tick TF: ensure_ready викликається і публікується історичний slice.
            ensure_ready_mock.assert_called()
            self.assertTrue(publish_mock.called)

            # fxcm_backfill для 1m/5m: тепер теж публікує історичний slice (без прямого FXCM polling m5).
            # fxcm_backfill для 15m: викликається з clamp lookback.
            self.assertTrue(backfill_mock.called)
            _, kwargs = backfill_mock.call_args
            self.assertEqual(kwargs.get("lookback_minutes"), 360)


if __name__ == "__main__":
    unittest.main()
