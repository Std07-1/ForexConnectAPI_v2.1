"""Тести runtime-валідації контрактів (продьюсерська сторона).

Мета: захистити інтеграцію від випадкових змін JSON-схем при рефакторингах.
"""

import json

import pandas as pd

from connector import PriceTickSnap, _publish_price_snapshot, publish_ohlcv_to_redis
from connector import TickOhlcvWorker
from fxcm_schema import validate_ohlcv_payload_contract, validate_price_tik_payload_contract
from tick_ohlcv import OhlcvBar


class FakeRedis:
    def __init__(self) -> None:
        self.published = []

    def publish(self, channel: str, message: str) -> None:
        self.published.append((channel, message))


def test_publish_ohlcv_payload_passes_contract_validation() -> None:
    fake = FakeRedis()

    df = pd.DataFrame(
        [
            {
                "open_time": 1765557600000,
                "close_time": 1765557659999,
                "open": 4278.0,
                "high": 4284.0,
                "low": 4277.0,
                "close": 4281.0,
                "volume": 123.0,
                "tick_count": 37,
                "bar_range": 7.0,
                "body_size": 3.0,
                "upper_wick": 1.0,
                "lower_wick": 3.0,
                "avg_spread": 0.05,
                "max_spread": 0.12,
                "complete": True,
                "synthetic": False,
                "source": "tick_agg",
                "tf": "1m",
            }
        ]
    )

    ok = publish_ohlcv_to_redis(
        df,
        symbol="XAU/USD",
        timeframe="1m",
        redis_client=fake,
        source="tick_agg",
    )

    assert ok is True
    assert fake.published
    _, message = fake.published[-1]
    payload = json.loads(message)
    validate_ohlcv_payload_contract(payload)


def test_publish_price_tik_payload_passes_contract_validation() -> None:
    fake = FakeRedis()

    _publish_price_snapshot(
        fake,
        channel="fxcm:price_tik",
        snapshot=PriceTickSnap(
            symbol="XAUUSD",
            bid=2045.1,
            ask=2045.3,
            mid=2045.2,
            tick_ts=1701600000.0,
            snap_ts=1701600003.0,
        ),
    )

    assert fake.published
    _, message = fake.published[-1]
    payload = json.loads(message)
    validate_price_tik_payload_contract(payload)


def test_tick_ohlcv_live_publish_is_throttled(monkeypatch) -> None:
    published = []

    def sink(_batch) -> None:
        published.append(_batch)

    worker = TickOhlcvWorker(
        enabled=True,
        symbols=["XAUUSD"],
        max_synth_gap_minutes=0,
        ohlcv_sink=sink,
    )
    worker._live_publish_min_interval_seconds = 0.25

    bar = OhlcvBar(
        symbol="XAUUSD",
        tf="1m",
        start_ms=1_700_000_000_000,
        end_ms=1_700_000_060_000,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=0.0,
        complete=False,
        synthetic=False,
        source="tick_agg",
    )

    t = {"v": 1000.0}

    def fake_monotonic() -> float:
        return t["v"]

    monkeypatch.setattr("connector.time.monotonic", fake_monotonic)

    worker._publish_tick_bar(bar)
    assert len(published) == 1

    # У межах 250мс повтор не має проходити.
    t["v"] += 0.10
    worker._publish_tick_bar(bar)
    assert len(published) == 1

    # Після 250мс — має проходити.
    t["v"] += 0.20
    worker._publish_tick_bar(bar)
    assert len(published) == 2
