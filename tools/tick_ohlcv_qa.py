"""QA-CLI для офлайнового аналізу tick→OHLCV: gap'и, synthetic та sleep-модель.

Вхід:
- JSONL або CSV з полями `symbol`, `ts_ms`, `bid`, `ask`, `volume`.
  (Якщо `price` присутній — використовуємо його; інакше ціна = (bid+ask)/2.)

Вихід:
- статистика real/synthetic барів;
- розподіл gap'ів (у хвилинах); 
- топ-N найбільших gap'ів із прапорами filled_by_synth / sleep_model.
"""

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

# Дозволяє запускати файл напряму як `python tools/tick_ohlcv_qa.py ...`
# і при цьому імпортувати модулі з кореня репозиторію.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tick_ohlcv import FxcmTick, TickOhlcvAggregator


@dataclass
class GapEvent:
    """Один gap між двома реальними (synthetic=False) барами."""

    symbol: str
    prev_end_ms: int
    next_start_ms: int
    missing_minutes: int
    synthetic_minutes: int
    max_synth_allowed: int
    filled_by_synth: bool
    sleep_model: bool


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="QA-аналізатор gap'ів і synthetic")
    parser.add_argument("--ticks-file", required=True, help="Шлях до файлу з тиками")
    parser.add_argument(
        "--format",
        default="jsonl",
        choices=["jsonl", "csv"],
        help="Формат файлу тиків",
    )
    parser.add_argument(
        "--symbol",
        help="Символ для аналізу (якщо порожньо, береться з першого рядка)",
    )
    parser.add_argument(
        "--tf-sec",
        type=int,
        default=60,
        help="Таймфрейм у секундах (наприклад, 60)",
    )
    parser.add_argument(
        "--max-synth-gap-minutes",
        type=int,
        default=60,
        help="MAX_SYNTHETIC_GAP_MINUTES для tick-агрегатора",
    )
    parser.add_argument(
        "--fill-gaps",
        type=int,
        default=1,
        choices=[0, 1],
        help="1 — дозволити synthetic, 0 — заборонити",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=20,
        help="Скільки найбільших gap'ів показати",
    )
    parser.add_argument(
        "--min-gap-minutes",
        type=int,
        default=2,
        help="Поріг, з якого gap вважаємо «великим» і додаємо в таблицю",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        help="Обмежити кількість рядків (тиків) для швидкого прогону",
    )
    return parser.parse_args()


def _coerce_price(raw: dict) -> float:
    if "price" in raw and raw["price"] is not None:
        return float(raw["price"])
    bid = raw.get("bid")
    ask = raw.get("ask")
    if bid is None or ask is None:
        raise ValueError("Потрібен або price, або bid+ask")
    return (float(bid) + float(ask)) / 2.0


def _iter_ticks_jsonl(path: Path, max_rows: Optional[int]) -> Iterator[FxcmTick]:
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if max_rows is not None and count >= max_rows:
                return
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
            yield FxcmTick(
                symbol=str(raw["symbol"]),
                ts_ms=int(raw["ts_ms"]),
                price=_coerce_price(raw),
                volume=float(raw.get("volume", 0.0)),
            )
            count += 1


def _iter_ticks_csv(path: Path, max_rows: Optional[int]) -> Iterator[FxcmTick]:
    count = 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            if max_rows is not None and count >= max_rows:
                return
            yield FxcmTick(
                symbol=str(raw["symbol"]),
                ts_ms=int(raw["ts_ms"]),
                price=_coerce_price(raw),
                volume=float(raw.get("volume", 0.0) or 0.0),
            )
            count += 1


def _iter_ticks(path: Path, fmt: str, max_rows: Optional[int]) -> Iterator[FxcmTick]:
    if fmt == "jsonl":
        return _iter_ticks_jsonl(path, max_rows)
    return _iter_ticks_csv(path, max_rows)


def _peek_first(it: Iterator[FxcmTick]) -> Tuple[FxcmTick, Iterator[FxcmTick]]:
    first = next(it)
    return first, chain([first], it)


def _fmt_utc_ms(ts_ms: int) -> str:
    return datetime.utcfromtimestamp(ts_ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S")


def _gap_bucket_label(missing_minutes: int) -> str:
    if missing_minutes <= 0:
        return "0"
    if missing_minutes == 1:
        return "1-1"
    if 2 <= missing_minutes <= 5:
        return "2-5"
    if 6 <= missing_minutes <= 15:
        return "6-15"
    return ">15"


def main() -> None:
    args = _parse_args()
    ticks_file = Path(args.ticks_file)
    ticks_iter = _iter_ticks(ticks_file, args.format, args.max_rows)

    try:
        first_tick, ticks_iter = _peek_first(ticks_iter)
    except StopIteration:
        raise SystemExit("Файл не містить тиків")

    symbol = args.symbol or first_tick.symbol
    tf_ms = int(args.tf_sec) * 1000
    max_synth_allowed = int(args.max_synth_gap_minutes)

    aggregator = TickOhlcvAggregator(
        symbol,
        tf_seconds=int(args.tf_sec),
        fill_gaps=bool(args.fill_gaps),
        max_synthetic_gap_minutes=max_synth_allowed,
    )

    total_ticks = 0
    total_bars_real = 0
    total_bars_synth = 0
    gap_events: List[GapEvent] = []
    gap_distribution: Dict[str, int] = {"1-1": 0, "2-5": 0, "6-15": 0, ">15": 0}

    last_real_end_ms: Optional[int] = None
    synth_since_last_real = 0
    sleep_gaps_count = 0
    sleep_total_minutes = 0

    for tick in ticks_iter:
        if tick.symbol != symbol:
            continue
        total_ticks += 1

        result = aggregator.ingest_tick(tick)
        for bar in result.closed_bars:
            if bar.synthetic:
                total_bars_synth += 1
                synth_since_last_real += 1
                continue

            total_bars_real += 1
            if last_real_end_ms is not None:
                gap_ms = bar.start_ms - last_real_end_ms
                if gap_ms > 0:
                    missing_minutes = int(gap_ms // tf_ms)
                    gap_distribution[_gap_bucket_label(missing_minutes)] += 1
                    filled_by_synth = (
                        synth_since_last_real == missing_minutes
                        and missing_minutes <= max_synth_allowed
                    )
                    sleep_model = missing_minutes > max_synth_allowed and synth_since_last_real == 0
                    if sleep_model:
                        sleep_gaps_count += 1
                        sleep_total_minutes += missing_minutes

                    if missing_minutes >= int(args.min_gap_minutes):
                        gap_events.append(
                            GapEvent(
                                symbol=symbol,
                                prev_end_ms=last_real_end_ms,
                                next_start_ms=bar.start_ms,
                                missing_minutes=missing_minutes,
                                synthetic_minutes=synth_since_last_real,
                                max_synth_allowed=max_synth_allowed,
                                filled_by_synth=filled_by_synth,
                                sleep_model=sleep_model,
                            )
                        )
            synth_since_last_real = 0
            last_real_end_ms = bar.end_ms

    total_bars = total_bars_real + total_bars_synth
    synth_ratio = (total_bars_synth / total_bars) if total_bars > 0 else 0.0

    print(f"symbol: {symbol}")
    print(f"tf_sec: {args.tf_sec}")
    print(f"ticks: {total_ticks}")
    print(f"bars_real: {total_bars_real}")
    print(f"bars_synth: {total_bars_synth}")
    print(f"bars_synth_ratio: {synth_ratio:.6f}")
    print(f"gap_events_total: {len(gap_events)} (min_gap_minutes={args.min_gap_minutes})")
    print(f"out_of_order_ticks: {aggregator.out_of_order_ticks}")
    print(f"sleep_gaps_count: {sleep_gaps_count}")
    print(f"sleep_total_minutes: {sleep_total_minutes}")

    print("\nGap distribution (by missing minutes):")
    for label in ["1-1", "2-5", "6-15", ">15"]:
        print(f"  {label:<4}: {gap_distribution[label]}")

    gap_events_sorted = sorted(gap_events, key=lambda e: e.missing_minutes, reverse=True)
    top = gap_events_sorted[: int(args.max_samples)]

    print(f"\nTop {len(top)} largest gaps:")
    # Таблиця у «режимі 8»: фіксовані колонки близько 8 символів, без зайвого шуму.
    header = (
        f"{'symbol':<8} {'miss_min':>8} {'synth_min':>9} {'filled?':>8} {'sleep?':>7} "
        f"{'prev_end_utc':<19} {'next_start_utc':<19}"
    )
    print(header)
    for ev in top:
        print(
            f"{ev.symbol:<8} {ev.missing_minutes:>8} {ev.synthetic_minutes:>9} "
            f"{str(ev.filled_by_synth):>8} {str(ev.sleep_model):>7} "
            f"{_fmt_utc_ms(ev.prev_end_ms):<19} {_fmt_utc_ms(ev.next_start_ms):<19}"
        )


if __name__ == "__main__":
    main()
