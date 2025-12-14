from __future__ import annotations

import unittest
from unittest import mock

import connector


def _fx_holder_with_mock() -> dict[str, connector.ForexConnect | None]:
    fx = mock.Mock(spec=connector.ForexConnect)
    return {"client": fx}


class FxcmCommandWorkerTest(unittest.TestCase):
    def test_warmup_for_tick_tf_updates_cache_only(self) -> None:
        fx_holder = _fx_holder_with_mock()
        cache = mock.Mock()

        worker = connector.FxcmCommandWorker(
            redis_client=mock.Mock(),
            fx_holder=fx_holder,
            cache_manager=cache,
            tick_aggregation_enabled=True,
            tick_agg_timeframes=("m1", "m5"),
            channel="fxcm:commands",
            stream_targets=[("XAU/USD", "m1")],
            hmac_secret=None,
            hmac_algo="sha256",
        )

        payload = {
            "type": "fxcm_warmup",
            "symbol": "XAUUSD",
            "tf": "1m",
            "min_history_bars": 250,
            "lookback_minutes": 60,
        }

        with mock.patch.object(connector, "publish_ohlcv_to_redis") as publish_mock:
            worker._handle_command(payload)

        cache.ensure_ready.assert_called()
        publish_mock.assert_not_called()

    def test_backfill_for_tick_tf_is_ignored(self) -> None:
        fx_holder = _fx_holder_with_mock()

        worker = connector.FxcmCommandWorker(
            redis_client=mock.Mock(),
            fx_holder=fx_holder,
            cache_manager=mock.Mock(),
            tick_aggregation_enabled=True,
            tick_agg_timeframes=("m1", "m5"),
            channel="fxcm:commands",
            stream_targets=[("XAU/USD", "m5")],
        )

        payload = {
            "type": "fxcm_backfill",
            "symbol": "XAUUSD",
            "tf": "5m",
            "lookback_minutes": 90,
        }

        with mock.patch.object(connector, "_fetch_and_publish_recent") as backfill_mock:
            worker._handle_command(payload)

        backfill_mock.assert_not_called()

    def test_command_for_non_target_is_ignored(self) -> None:
        worker = connector.FxcmCommandWorker(
            redis_client=mock.Mock(),
            fx_holder=_fx_holder_with_mock(),
            cache_manager=mock.Mock(),
            tick_aggregation_enabled=False,
            channel="fxcm:commands",
            stream_targets=[("EUR/USD", "m15")],
        )

        payload = {
            "type": "fxcm_warmup",
            "symbol": "XAUUSD",
            "tf": "1m",
            "min_history_bars": 100,
        }

        with mock.patch.object(worker._cache_manager, "ensure_ready") as ensure_mock:
            worker._handle_command(payload)

        ensure_mock.assert_not_called()

    def test_pubsub_closed_file_on_stop_does_not_warn(self) -> None:
        stop_event = connector.threading.Event()

        class _PubSub:
            def subscribe(self, *_args, **_kwargs):
                return None

            def get_message(self, timeout: float = 0.0):
                stop_event.set()
                raise ValueError("I/O operation on closed file.")

            def close(self):
                return None

        redis_client = mock.Mock()
        redis_client.pubsub.return_value = _PubSub()

        worker = connector.FxcmCommandWorker(
            redis_client=redis_client,
            fx_holder=_fx_holder_with_mock(),
            cache_manager=mock.Mock(),
            tick_aggregation_enabled=False,
            channel="fxcm:commands",
            stream_targets=[("EUR/USD", "m15")],
        )
        worker._stop_event = stop_event

        with mock.patch.object(connector, "log") as log_mock:
            worker._run()

        # Очікуваний shutdown не має створювати WARNING-спам.
        log_mock.warning.assert_not_called()
