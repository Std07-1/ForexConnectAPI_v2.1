"""Агрегація тикових подій у OHLCV-бари для FXCM стріму.

Інтервали трактуються як `[start_ms, end_ms)`: останній момент bar'у не входить
до нього, тому тик із `ts_ms == end_ms` автоматично відкриває наступний bucket.
"""

from __future__ import annotations

from dataclasses import dataclass

MILLISECONDS_IN_SECOND = 1_000
SECONDS_IN_MINUTE = 60
MILLISECONDS_IN_MINUTE = MILLISECONDS_IN_SECOND * SECONDS_IN_MINUTE
DEFAULT_SOURCE = "tick_agg"


def _format_tf_label(tf_seconds: int) -> str:
    """Перетворює тривалість у секундах на текстовий таймфрейм."""
    if tf_seconds % (SECONDS_IN_MINUTE * 60) == 0:
        hours = tf_seconds // (SECONDS_IN_MINUTE * 60)
        return f"{hours}h"
    if tf_seconds % SECONDS_IN_MINUTE == 0:
        minutes = tf_seconds // SECONDS_IN_MINUTE
        return f"{minutes}m"
    return f"{tf_seconds}s"


def _bucket_start(ts_ms: int, tf_ms: int) -> int:
    return (ts_ms // tf_ms) * tf_ms


@dataclass
class FxcmTick:
    """Сира подія тика, яку конвертуємо у OHLCV."""

    symbol: str
    ts_ms: int
    price: float
    volume: float = 0.0


@dataclass
class OhlcvBar:
    """Готовий OHLCV-бар, який можна відправляти у fxcm:ohlcv."""

    symbol: str
    tf: str
    start_ms: int
    end_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    complete: bool
    synthetic: bool
    source: str = DEFAULT_SOURCE


@dataclass
class CurrentBar:
    """Живий бар у межах поточного bucket `[start_ms, end_ms)` для символу."""

    symbol: str
    tf_ms: int
    start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    synthetic: bool = False

    @property
    def end_ms(self) -> int:
        return self.start_ms + self.tf_ms

    def update_with_tick(self, tick: FxcmTick) -> None:
        self.high = max(self.high, tick.price)
        self.low = min(self.low, tick.price)
        self.close = tick.price
        self.volume += tick.volume

    def to_ohlcv(self, tf_label: str, source: str, complete: bool) -> OhlcvBar:
        return OhlcvBar(
            symbol=self.symbol,
            tf=tf_label,
            start_ms=self.start_ms,
            end_ms=self.end_ms,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            complete=complete,
            synthetic=self.synthetic,
            source=source,
        )

    @classmethod
    def synthetic_bar(
        cls, symbol: str, tf_ms: int, start_ms: int, price: float
    ) -> CurrentBar:
        return cls(
            symbol=symbol,
            tf_ms=tf_ms,
            start_ms=start_ms,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=0.0,
            synthetic=True,
        )


@dataclass
class AggregationResult:
    """Результат одного виклику агрегатора."""

    closed_bars: list[OhlcvBar]
    live_bar: OhlcvBar | None
    out_of_order: bool = False


class TickOhlcvAggregator:
    """Агрегує FXCM тики у OHLCV-бари з підтримкою synthetic-gap логіки.

    Бар фіксується в момент переходу на інший bucket. Якщо gap між закритим
    bucket та наступним тиком перевищує `max_synthetic_gap_minutes`, ми не
    заповнюємо його flat-ланцюжком, щоби процес «засинав» на вихідних та під час
    довгих реконектів і прокидався лише після появи реальної ліквідності.
    """

    def __init__(
        self,
        symbol: str,
        tf_seconds: int,
        *,
        fill_gaps: bool = True,
        max_synthetic_gap_minutes: int = 60,
        source: str = DEFAULT_SOURCE,
    ) -> None:
        if tf_seconds <= 0:
            raise ValueError("tf_seconds має бути > 0")
        self.symbol = symbol
        self.tf_seconds = tf_seconds
        self.tf_ms = tf_seconds * MILLISECONDS_IN_SECOND
        self.tf_label = _format_tf_label(tf_seconds)
        self.fill_gaps = fill_gaps
        self.max_synthetic_gap_minutes = max_synthetic_gap_minutes
        self.source = source
        self.current_bar: CurrentBar | None = None
        self.ticks_ingested = 0
        self.synthetic_bars_emitted = 0
        self.closed_bars_emitted = 0
        self.out_of_order_ticks = 0

    def ingest_tick(self, tick: FxcmTick) -> AggregationResult:
        self.ticks_ingested += 1
        bucket_start = _bucket_start(tick.ts_ms, self.tf_ms)

        if self.current_bar is None:
            self.current_bar = self._start_new_bar(bucket_start, tick)
            return AggregationResult(
                [], self.current_bar.to_ohlcv(self.tf_label, self.source, False)
            )

        if bucket_start < self.current_bar.start_ms:
            self.out_of_order_ticks += 1
            return AggregationResult(
                [],
                self.current_bar.to_ohlcv(self.tf_label, self.source, False),
                out_of_order=True,
            )

        closed: list[OhlcvBar] = []

        if bucket_start > self.current_bar.start_ms:
            closed.append(self._close_current_bar())
            last_close = closed[-1].close
            last_end_ms = closed[-1].end_ms
            closed.extend(
                self._fill_synthetic_gap(
                    last_end_ms=last_end_ms,
                    next_bucket_start=bucket_start,
                    last_price=last_close,
                )
            )
            self.current_bar = self._start_new_bar(bucket_start, tick)
        else:
            self.current_bar.update_with_tick(tick)

        live_bar = self.current_bar.to_ohlcv(self.tf_label, self.source, False)
        return AggregationResult(closed, live_bar)

    def _start_new_bar(self, bucket_start: int, tick: FxcmTick) -> CurrentBar:
        bar = CurrentBar(
            symbol=self.symbol,
            tf_ms=self.tf_ms,
            start_ms=bucket_start,
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=tick.volume,
        )
        return bar

    def _close_current_bar(self) -> OhlcvBar:
        if self.current_bar is None:
            raise RuntimeError("Немає бару для закриття")
        bar = self.current_bar.to_ohlcv(self.tf_label, self.source, True)
        self.closed_bars_emitted += 1
        self.current_bar = None
        return bar

    def _fill_synthetic_gap(
        self, *, last_end_ms: int, next_bucket_start: int, last_price: float
    ) -> list[OhlcvBar]:
        if not self.fill_gaps or self.max_synthetic_gap_minutes <= 0:
            return []
        gap_ms = max(0, next_bucket_start - last_end_ms)
        if gap_ms == 0:
            return []
        missing_bars_total = gap_ms // self.tf_ms
        if missing_bars_total == 0:
            return []
        missing_minutes = gap_ms / MILLISECONDS_IN_MINUTE
        if missing_minutes > self.max_synthetic_gap_minutes:
            return []
        synthetic: list[OhlcvBar] = []
        for i in range(missing_bars_total):
            start_ms = last_end_ms + i * self.tf_ms
            bar = CurrentBar.synthetic_bar(
                self.symbol, self.tf_ms, start_ms, last_price
            )
            synthetic.append(bar.to_ohlcv(self.tf_label, self.source, True))
        self.synthetic_bars_emitted += len(synthetic)
        return synthetic


class OhlcvFromLowerTfAggregator:
    """Агрегує complete 1m-бари у старші таймфрейми без додаткових synthetic."""

    def __init__(
        self,
        symbol: str,
        target_tf_seconds: int,
        lower_tf_seconds: int,
        source: str = DEFAULT_SOURCE,
    ) -> None:
        if target_tf_seconds % lower_tf_seconds != 0:
            raise ValueError("target_tf_seconds має бути кратним lower_tf_seconds")
        self.symbol = symbol
        self.target_tf_seconds = target_tf_seconds
        self.lower_tf_seconds = lower_tf_seconds
        self.target_tf_ms = target_tf_seconds * MILLISECONDS_IN_SECOND
        self.lower_tf_ms = lower_tf_seconds * MILLISECONDS_IN_SECOND
        self.tf_label = _format_tf_label(target_tf_seconds)
        self.lower_tf_label = _format_tf_label(lower_tf_seconds)
        self.bars_per_window = target_tf_seconds // lower_tf_seconds
        self.source = source
        self.current_window_start: int | None = None
        self.window_bars: list[OhlcvBar] = []
        self.live_lower_bar: OhlcvBar | None = None

    def ingest_bar(self, bar: OhlcvBar) -> AggregationResult:
        if bar.symbol != self.symbol:
            raise ValueError("Бар іншого символу не підтримується цим агрегатором")
        if bar.tf != self.lower_tf_label:
            raise ValueError("Невірний таймфрейм джерела")

        if not bar.complete:
            self.live_lower_bar = bar
            live = self._build_live_bar()
            return AggregationResult([], live)
        self.live_lower_bar = None

        closed: list[OhlcvBar] = []
        window_start = _bucket_start(bar.start_ms, self.target_tf_ms)
        if self.current_window_start is None:
            self.current_window_start = window_start

        if window_start != self.current_window_start and self.window_bars:
            closed.append(self._close_window())
            self.current_window_start = window_start
            self.window_bars.clear()

        self.window_bars.append(bar)

        if len(self.window_bars) == self.bars_per_window:
            closed.append(self._close_window())
            self.current_window_start = None
            self.window_bars.clear()

        live_bar = self._build_live_bar()
        return AggregationResult(closed, live_bar)

    def _close_window(self) -> OhlcvBar:
        if not self.window_bars:
            raise RuntimeError("Немає барів для агрегації")
        start_ms = self.window_bars[0].start_ms
        end_ms = start_ms + self.target_tf_ms
        open_price = self.window_bars[0].open
        close_price = self.window_bars[-1].close
        high_price = max(bar.high for bar in self.window_bars)
        low_price = min(bar.low for bar in self.window_bars)
        volume = sum(bar.volume for bar in self.window_bars)
        synthetic = all(bar.synthetic for bar in self.window_bars)
        return OhlcvBar(
            symbol=self.symbol,
            tf=self.tf_label,
            start_ms=start_ms,
            end_ms=end_ms,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            complete=True,
            synthetic=synthetic,
            source=self.source,
        )

    def _build_live_bar(self) -> OhlcvBar | None:
        live_components: list[OhlcvBar] = []
        live_components.extend(self.window_bars)
        if self.live_lower_bar is not None:
            live_components.append(self.live_lower_bar)
        if not live_components:
            return None
        start_ms = self.current_window_start
        if start_ms is None:
            start_ms = _bucket_start(live_components[0].start_ms, self.target_tf_ms)
        end_ms = start_ms + self.target_tf_ms
        open_price = live_components[0].open
        close_price = live_components[-1].close
        high_price = max(bar.high for bar in live_components)
        low_price = min(bar.low for bar in live_components)
        volume = sum(bar.volume for bar in live_components)
        synthetic = all(bar.synthetic for bar in live_components)
        return OhlcvBar(
            symbol=self.symbol,
            tf=self.tf_label,
            start_ms=start_ms,
            end_ms=end_ms,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            complete=False,
            synthetic=synthetic,
            source=self.source,
        )
