"""Тести ядра tick→OHLCV.

Важливо: у кожному тесті є посилання на відповідне правило з docs/TIK_bucket.md,
щоб було видно відповідність «специфікація → тест».
"""

from tick_ohlcv import FxcmTick, OhlcvBar, OhlcvFromLowerTfAggregator, TickOhlcvAggregator


MS = 1_000
MINUTE_MS = 60_000


def make_tick_at(
    *, minute: int, second: int, price: float, volume: float = 0.0, symbol: str = "XAUUSD"
) -> FxcmTick:
    """Будує тик у ms відносно 12:00:00 (умовний нуль)."""

    ts_seconds = minute * 60 + second
    return FxcmTick(symbol=symbol, ts_ms=ts_seconds * MS, price=price, volume=volume)


def make_tick_with_spread(
    *, minute: int, second: int, bid: float, ask: float, volume: float = 0.0, symbol: str = "XAUUSD"
) -> FxcmTick:
    """Будує тик із bid/ask (mid=середина), щоб тестувати spread-метрики."""

    mid = (bid + ask) / 2.0
    ts_seconds = minute * 60 + second
    return FxcmTick(
        symbol=symbol,
        ts_ms=ts_seconds * MS,
        price=mid,
        bid=bid,
        ask=ask,
        volume=volume,
    )


class TestTickOhlcvAggregator1m:
    def test_single_minute_three_ticks_no_gap(self) -> None:
        # docs/TIK_bucket.md §1.1: bucket семантика `[start_ms, end_ms)`.
        aggregator = TickOhlcvAggregator("XAUUSD", tf_seconds=60)

        r1 = aggregator.ingest_tick(
            make_tick_at(minute=0, second=1, price=100.0, volume=1.0)
        )
        r2 = aggregator.ingest_tick(
            make_tick_at(minute=0, second=20, price=101.0, volume=2.0)
        )
        r3 = aggregator.ingest_tick(
            make_tick_at(minute=0, second=59, price=99.0, volume=3.0)
        )

        assert r1.closed_bars == []
        assert r2.closed_bars == []
        assert r3.closed_bars == []

        assert r3.live_bar is not None
        assert r3.live_bar.complete is False
        assert r3.live_bar.synthetic is False
        assert r3.live_bar.start_ms == 0
        assert r3.live_bar.end_ms == MINUTE_MS
        assert r3.live_bar.open == 100.0
        assert r3.live_bar.high == 101.0
        assert r3.live_bar.low == 99.0
        assert r3.live_bar.close == 99.0
        assert r3.live_bar.volume == 6.0

        # Тик рівно на межі `end_ms` має закрити попередній bucket.
        # docs/TIK_bucket.md §1.1: подія `ts_ms == end_ms` належить наступному bucket.
        r4 = aggregator.ingest_tick(
            make_tick_at(minute=1, second=0, price=100.5, volume=1.0)
        )
        assert len(r4.closed_bars) == 1

        closed = r4.closed_bars[0]
        assert closed.complete is True
        assert closed.synthetic is False
        assert closed.start_ms == 0
        assert closed.end_ms == MINUTE_MS
        assert closed.open == r3.live_bar.open
        assert closed.high == r3.live_bar.high
        assert closed.low == r3.live_bar.low
        assert closed.close == r3.live_bar.close
        assert closed.volume == r3.live_bar.volume

        assert r4.live_bar is not None
        assert r4.live_bar.complete is False
        assert r4.live_bar.start_ms == MINUTE_MS
        assert r4.live_bar.end_ms == 2 * MINUTE_MS
        assert r4.live_bar.open == 100.5
        assert r4.live_bar.high == 100.5
        assert r4.live_bar.low == 100.5
        assert r4.live_bar.close == 100.5
        assert r4.live_bar.volume == 1.0

    def test_single_tick_minute_zero_range(self) -> None:
        # docs/TIK_bucket.md §1.1: `ts_ms == end_ms` закриває попередню хвилину.
        aggregator = TickOhlcvAggregator("XAUUSD", tf_seconds=60)

        r1 = aggregator.ingest_tick(
            make_tick_at(minute=0, second=1, price=200.0, volume=4.0)
        )
        assert r1.closed_bars == []
        assert r1.live_bar is not None

        r2 = aggregator.ingest_tick(
            make_tick_at(minute=1, second=0, price=201.0, volume=1.0)
        )
        assert len(r2.closed_bars) == 1
        closed = r2.closed_bars[0]
        assert closed.complete is True
        assert closed.open == 200.0
        assert closed.high == 200.0
        assert closed.low == 200.0
        assert closed.close == 200.0
        assert closed.volume == 4.0

    def test_clock_flush_closes_bar_without_rollover_tick(self) -> None:
        # Якщо немає тика у наступному bucket, бар все одно має закриватися по часу.
        # Це критично для downstream, який ігнорує complete=false.
        aggregator = TickOhlcvAggregator("XAUUSD", tf_seconds=60)

        r1 = aggregator.ingest_tick(make_tick_at(minute=0, second=10, price=100.0))
        assert r1.live_bar is not None
        assert r1.live_bar.complete is False

        # Час перейшов межу 1-ї хвилини (60_000 ms), але нового тика немає.
        flushed = aggregator.flush_until(MINUTE_MS)
        assert len(flushed.closed_bars) == 1
        closed = flushed.closed_bars[0]
        assert closed.complete is True
        assert closed.start_ms == 0
        assert closed.end_ms == MINUTE_MS
        assert closed.open == 100.0
        assert closed.high == 100.0
        assert closed.low == 100.0
        assert closed.close == 100.0

    def test_microstructure_fields_tick_count_spread_and_geometry(self) -> None:
        aggregator = TickOhlcvAggregator("XAUUSD", tf_seconds=60)

        aggregator.ingest_tick(
            make_tick_with_spread(minute=0, second=1, bid=99.9, ask=100.1)
        )
        aggregator.ingest_tick(
            make_tick_with_spread(minute=0, second=10, bid=100.0, ask=100.3)
        )
        aggregator.ingest_tick(
            make_tick_with_spread(minute=0, second=59, bid=99.7, ask=100.0)
        )

        r_close = aggregator.ingest_tick(
            make_tick_with_spread(minute=1, second=0, bid=100.0, ask=100.2)
        )
        assert len(r_close.closed_bars) == 1
        closed = r_close.closed_bars[0]

        assert closed.complete is True
        assert closed.synthetic is False
        assert closed.tick_count == 3

        # Спреди: 0.2, 0.3, 0.3
        assert round(closed.avg_spread, 6) == round((0.2 + 0.3 + 0.3) / 3.0, 6)
        assert round(closed.max_spread, 6) == round(0.3, 6)

        # Геометрія: bar_range = high-low, body_size = |close-open|, wick-и.
        assert round(closed.bar_range, 6) == round(closed.high - closed.low, 6)
        assert round(closed.body_size, 6) == round(abs(closed.close - closed.open), 6)
        assert round(closed.upper_wick, 6) == round(closed.high - max(closed.open, closed.close), 6)
        assert round(closed.lower_wick, 6) == round(min(closed.open, closed.close) - closed.low, 6)


class TestTickOhlcvAggregatorGaps:
    def test_gap_shorter_than_limit_all_minutes_synthetic(self) -> None:
        # docs/TIK_bucket.md §1.2: короткі gap можна заповнювати synthetic.
        aggregator = TickOhlcvAggregator(
            "XAUUSD",
            tf_seconds=60,
            fill_gaps=True,
            max_synthetic_gap_minutes=3,
        )

        aggregator.ingest_tick(
            make_tick_at(minute=0, second=1, price=100.0, volume=1.0)
        )
        aggregator.ingest_tick(
            make_tick_at(minute=0, second=59, price=101.0, volume=2.0)
        )

        # Наступний тик у хвилині N+2 (пропущено хвилину N+1).
        r = aggregator.ingest_tick(
            make_tick_at(minute=2, second=0, price=102.0, volume=1.0)
        )

        assert len(r.closed_bars) == 2
        real_bar = r.closed_bars[0]
        synth_bar = r.closed_bars[1]

        assert real_bar.complete is True
        assert real_bar.synthetic is False
        assert real_bar.start_ms == 0
        assert real_bar.end_ms == MINUTE_MS

        assert synth_bar.complete is True
        assert synth_bar.synthetic is True
        assert synth_bar.start_ms == MINUTE_MS
        assert synth_bar.end_ms == 2 * MINUTE_MS
        assert synth_bar.open == real_bar.close
        assert synth_bar.high == real_bar.close
        assert synth_bar.low == real_bar.close
        assert synth_bar.close == real_bar.close
        assert synth_bar.volume == 0.0

        assert r.live_bar is not None
        assert r.live_bar.complete is False
        assert r.live_bar.start_ms == 2 * MINUTE_MS

    def test_gap_longer_than_limit_respects_max_synthetic(self) -> None:
        # docs/TIK_bucket.md §1.2: якщо gap > MAX_SYNTHETIC_GAP_MINUTES — synthetic не створюємо.
        aggregator = TickOhlcvAggregator(
            "XAUUSD",
            tf_seconds=60,
            fill_gaps=True,
            max_synthetic_gap_minutes=3,
        )

        aggregator.ingest_tick(
            make_tick_at(minute=0, second=1, price=100.0, volume=1.0)
        )
        aggregator.ingest_tick(
            make_tick_at(minute=0, second=59, price=101.0, volume=2.0)
        )

        r = aggregator.ingest_tick(
            make_tick_at(minute=10, second=0, price=110.0, volume=1.0)
        )

        assert len(r.closed_bars) == 1
        assert r.closed_bars[0].synthetic is False
        assert r.live_bar is not None
        assert r.live_bar.start_ms == 10 * MINUTE_MS

    def test_synthetic_bar_flags_and_zero_range(self) -> None:
        # docs/TIK_bucket.md §1.3: synthetic-бар з'являється при виявленні gap.
        # docs/TIK_bucket.md §1.2: synthetic має volume=0 та flat OHLC.
        aggregator = TickOhlcvAggregator(
            "XAUUSD",
            tf_seconds=60,
            fill_gaps=True,
            max_synthetic_gap_minutes=3,
        )

        aggregator.ingest_tick(
            make_tick_at(minute=0, second=1, price=100.0, volume=1.0)
        )
        r = aggregator.ingest_tick(
            make_tick_at(minute=2, second=0, price=102.0, volume=1.0)
        )
        assert len(r.closed_bars) == 2
        synth = r.closed_bars[1]
        assert synth.synthetic is True
        assert synth.complete is True
        assert synth.volume == 0.0
        assert synth.high == synth.low == synth.open == synth.close


class TestOhlcvFromLowerTfAggregator5m:
    def test_5m_aggregator_single_bucket_five_1m_bars(self) -> None:
        # docs/TIK_bucket.md §1.6: 5m будується з complete 1m-барів.
        higher = OhlcvFromLowerTfAggregator(
            "XAUUSD", target_tf_seconds=300, lower_tf_seconds=60
        )

        base_bars: list[OhlcvBar] = []
        for i in range(5):
            start_ms = i * MINUTE_MS
            base_bars.append(
                OhlcvBar(
                    symbol="XAUUSD",
                    tf="1m",
                    start_ms=start_ms,
                    end_ms=start_ms + MINUTE_MS,
                    open=100.0 + i,
                    high=101.0 + i,
                    low=99.0 + i,
                    close=100.5 + i,
                    volume=1.0 + i,
                    complete=True,
                    synthetic=False,
                    source="tick_agg",
                )
            )

        # До перших 4 барів має бути live 5m (complete=False).
        for i in range(4):
            r = higher.ingest_bar(base_bars[i])
            assert r.closed_bars == []
            assert r.live_bar is not None
            assert r.live_bar.complete is False
            assert r.live_bar.start_ms == 0
            assert r.live_bar.end_ms == 300_000

        # На 5-му барі — закриття 5m.
        r5 = higher.ingest_bar(base_bars[4])
        assert len(r5.closed_bars) == 1
        assert r5.live_bar is None

        agg_bar = r5.closed_bars[0]
        assert agg_bar.complete is True
        assert agg_bar.tf == "5m"
        assert agg_bar.start_ms % 300_000 == 0
        assert agg_bar.end_ms - agg_bar.start_ms == 300_000
        assert agg_bar.open == base_bars[0].open
        assert agg_bar.close == base_bars[-1].close
        assert agg_bar.high == max(b.high for b in base_bars)
        assert agg_bar.low == min(b.low for b in base_bars)
        assert agg_bar.volume == sum(b.volume for b in base_bars)

    def test_5m_aggregator_propagates_tick_count_and_spread(self) -> None:
        higher = OhlcvFromLowerTfAggregator(
            "XAUUSD", target_tf_seconds=300, lower_tf_seconds=60
        )

        base_bars: list[OhlcvBar] = []
        for i in range(5):
            start_ms = i * MINUTE_MS
            base_bars.append(
                OhlcvBar(
                    symbol="XAUUSD",
                    tf="1m",
                    start_ms=start_ms,
                    end_ms=start_ms + MINUTE_MS,
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.0,
                    volume=0.0,
                    complete=True,
                    synthetic=False,
                    source="tick_agg",
                    tick_count=10,
                    avg_spread=0.2,
                    max_spread=0.3,
                )
            )

        for bar in base_bars[:-1]:
            r = higher.ingest_bar(bar)
            assert r.closed_bars == []

        r5 = higher.ingest_bar(base_bars[-1])
        assert len(r5.closed_bars) == 1
        closed = r5.closed_bars[0]

        assert closed.tick_count == 50
        assert round(closed.avg_spread, 6) == round(0.2, 6)
        assert round(closed.max_spread, 6) == round(0.3, 6)

    def test_5m_aggregator_with_synthetic_1m(self) -> None:
        # docs/TIK_bucket.md §1.6: synthetic для 5m лише коли всі 1m synthetic.
        higher = OhlcvFromLowerTfAggregator(
            "XAUUSD", target_tf_seconds=300, lower_tf_seconds=60
        )

        bars: list[OhlcvBar] = []
        for i in range(5):
            start_ms = i * MINUTE_MS
            bars.append(
                OhlcvBar(
                    symbol="XAUUSD",
                    tf="1m",
                    start_ms=start_ms,
                    end_ms=start_ms + MINUTE_MS,
                    open=100.0,
                    high=100.0,
                    low=100.0,
                    close=100.0,
                    volume=0.0,
                    complete=True,
                    synthetic=(i == 2),
                    source="tick_agg",
                )
            )

        r = None
        for b in bars:
            r = higher.ingest_bar(b)
        assert r is not None
        assert len(r.closed_bars) == 1
        assert r.closed_bars[0].synthetic is False


class TestTickOhlcvEdgeCases:
    def test_out_of_order_tick_does_not_change_bar(self) -> None:
        # docs/TIK_bucket.md §1.5: out-of-order тик (старіший за current.start_ms) ігноруємо.
        aggregator = TickOhlcvAggregator("XAUUSD", tf_seconds=60)

        r_newer = aggregator.ingest_tick(
            make_tick_at(minute=1, second=10, price=100.0, volume=1.0)
        )
        assert r_newer.live_bar is not None
        snapshot = r_newer.live_bar

        r_older = aggregator.ingest_tick(
            make_tick_at(minute=0, second=59, price=999.0, volume=999.0)
        )

        assert r_older.out_of_order is True
        assert r_older.closed_bars == []
        assert r_older.live_bar is not None
        assert r_older.live_bar.start_ms == snapshot.start_ms
        assert r_older.live_bar.end_ms == snapshot.end_ms
        assert r_older.live_bar.open == snapshot.open
        assert r_older.live_bar.high == snapshot.high
        assert r_older.live_bar.low == snapshot.low
        assert r_older.live_bar.close == snapshot.close
        assert r_older.live_bar.volume == snapshot.volume
        assert aggregator.out_of_order_ticks == 1

    def test_tick_older_than_current_bucket_ignored(self) -> None:
        # docs/TIK_bucket.md §1.5: тик із `ts_ms < current.start_ms` не змінює стан бару.
        aggregator = TickOhlcvAggregator("XAUUSD", tf_seconds=60)

        r0 = aggregator.ingest_tick(
            make_tick_at(minute=2, second=30, price=50.0, volume=2.0)
        )
        assert r0.live_bar is not None
        snapshot = r0.live_bar

        r_old = aggregator.ingest_tick(
            make_tick_at(minute=1, second=15, price=1.0, volume=1.0)
        )
        assert r_old.out_of_order is True
        assert r_old.closed_bars == []
        assert r_old.live_bar is not None
        assert r_old.live_bar.open == snapshot.open
        assert r_old.live_bar.close == snapshot.close
        assert r_old.live_bar.volume == snapshot.volume

    def test_restart_with_last_close_only_starts_new_bar(self) -> None:
        # docs/TIK_bucket.md §1.4: після рестарту перший реальний тик відкриває новий live-бар.
        # Примітка: last_close з persistent-стану зберігається/використовується на рівні конектора;
        # цей агрегатор у поточній реалізації не приймає last_close, тож перевіряємо базову поведінку.
        aggregator = TickOhlcvAggregator("XAUUSD", tf_seconds=60)

        r = aggregator.ingest_tick(
            make_tick_at(minute=5, second=0, price=123.45, volume=1.0)
        )
        assert r.closed_bars == []
        assert r.live_bar is not None
        assert r.live_bar.complete is False
        assert r.live_bar.synthetic is False
        assert r.live_bar.open == 123.45
        assert r.live_bar.start_ms == 5 * MINUTE_MS
