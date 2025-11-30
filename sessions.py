"""Утиліти для торгових сесій FXCM (спрощена модель 24/5 з технічними паузами).

Модуль отримав гнучкі хелпери для оверрайду календаря через ENV/JSON.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

log = logging.getLogger("fxcm_sessions")
if not log.handlers:
    log.addHandler(logging.NullHandler())

# FXCM зазвичай торгує з неділі 22:00 UTC до п'ятниці 22:00 UTC.
_WEEKLY_OPEN_TIME = dt.time(22, 0)
_WEEKLY_CLOSE_TIME = dt.time(22, 0)

# Щоденні вікна технічного обслуговування (UTC).
# За замовчуванням: коротка перерва близько 22:00 UTC та розширена пауза в п'ятницю.
_DAILY_BREAKS: Sequence[Tuple[dt.time, dt.time]] = (
    (dt.time(21, 58), dt.time(22, 5)),  # технічна перерва перед розкриттям сесії
    (dt.time(22, 0), dt.time(22, 15)),  # резервне вікно (може перекриватись з першим)
)

# Базові біржові свята (UTC дати). Список можна розширювати ззовні.
_FXCM_HOLIDAYS_UTC: List[str] = [
    "2025-01-01",  # New Year's Day
    "2025-01-20",  # Martin Luther King Jr. Day
    "2025-02-17",  # Presidents' Day
    "2025-04-18",  # Good Friday
    "2025-05-26",  # Memorial Day
    "2025-07-04",  # Independence Day
    "2025-09-01",  # Labor Day
    "2025-11-27",  # Thanksgiving
    "2025-12-25",  # Christmas Day
]
def _normalize_date_str(date_str: str) -> Optional[str]:
    raw = (date_str or "").strip()
    if not raw:
        return None
    try:
        value = dt.datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
        return value
    except ValueError:
        log.warning("Некоректний формат дати свята: %s", date_str)
        return None


def _parse_time_str(value: str) -> Optional[dt.time]:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        hours, minutes = raw.split(":", 1)
        return dt.time(int(hours), int(minutes))
    except Exception:  # noqa: BLE001
        log.warning("Некоректний формат часу: %s (очікується HH:MM)", value)
        return None


def _parse_break_entry(entry: Any) -> Optional[Tuple[dt.time, dt.time]]:
    start_raw: Optional[str]
    end_raw: Optional[str]

    if isinstance(entry, str):
        if "-" not in entry:
            return None
        start_raw, end_raw = entry.split("-", 1)
    elif isinstance(entry, Sequence) and len(entry) == 2:
        start_raw = str(entry[0])
        end_raw = str(entry[1])
    elif isinstance(entry, Mapping):
        start_raw = entry.get("start") or entry.get("from")
        end_raw = entry.get("end") or entry.get("to")
    else:
        return None

    start_time = _parse_time_str(start_raw or "")
    end_time = _parse_time_str(end_raw or "")
    if start_time is None or end_time is None:
        return None
    return start_time, end_time


def register_holiday(date_str: str) -> None:
    """Дозволяє динамічно додати нову дату свята (UTC)."""

    normalized = _normalize_date_str(date_str)
    if normalized and normalized not in _FXCM_HOLIDAYS_UTC:
        _FXCM_HOLIDAYS_UTC.append(normalized)


def register_holidays(dates: Sequence[str]) -> None:
    for item in dates:
        register_holiday(item)


def override_calendar(
    *,
    holidays: Optional[Sequence[str]] = None,
    daily_breaks: Optional[Sequence[Any]] = None,
    weekly_open: Optional[str] = None,
    weekly_close: Optional[str] = None,
    replace_holidays: bool = False,
) -> None:
    """Гнучко оновлює календар (використовується ENV/JSON).

    - holidays: список дат `YYYY-MM-DD`. Якщо replace_holidays=True — повністю
      замінює список; інакше додає нові значення.
    - daily_breaks: ітерація записів "HH:MM-HH:MM" / [start, end] / {start,end}.
      Якщо задано — замінює `_DAILY_BREAKS`.
    - weekly_open|weekly_close: часи відкриття/закриття тижневої сесії (HH:MM).
    """

    global _FXCM_HOLIDAYS_UTC, _DAILY_BREAKS, _WEEKLY_OPEN_TIME, _WEEKLY_CLOSE_TIME

    updated = False

    if holidays:
        normalized = [value for value in (_normalize_date_str(h) for h in holidays) if value]
        if normalized:
            if replace_holidays:
                _FXCM_HOLIDAYS_UTC = list(dict.fromkeys(normalized))
            else:
                for value in normalized:
                    if value not in _FXCM_HOLIDAYS_UTC:
                        _FXCM_HOLIDAYS_UTC.append(value)
            updated = True

    if daily_breaks:
        parsed_breaks = [value for value in (_parse_break_entry(entry) for entry in daily_breaks) if value]
        if parsed_breaks:
            _DAILY_BREAKS = tuple(parsed_breaks)
            updated = True

    if weekly_open:
        parsed = _parse_time_str(weekly_open)
        if parsed:
            _WEEKLY_OPEN_TIME = parsed
            updated = True

    if weekly_close:
        parsed = _parse_time_str(weekly_close)
        if parsed:
            _WEEKLY_CLOSE_TIME = parsed
            updated = True

    if updated:
        log.info(
            "Календар FXCM оновлено (holidays=%d, breaks=%d, open=%s, close=%s).",
            len(_FXCM_HOLIDAYS_UTC),
            len(_DAILY_BREAKS),
            _WEEKLY_OPEN_TIME,
            _WEEKLY_CLOSE_TIME,
        )


def load_calendar_file(path: Path) -> None:
    """Завантажує оверрайд календаря з JSON-файлу.

    Формат:
    {
      "holidays": ["YYYY-MM-DD", ...],
      "daily_breaks": ["HH:MM-HH:MM", ["HH:MM","HH:MM"], {"start":"HH:MM","end":"HH:MM"}],
      "weekly_open_utc": "22:00",
      "weekly_close_utc": "22:00"
    }
    """

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        log.warning("Файл календаря не знайдено: %s", path)
        return
    except json.JSONDecodeError as exc:
        log.error("Некоректний JSON у файлі %s: %s", path, exc)
        return

    holidays = payload.get("holidays") if isinstance(payload.get("holidays"), list) else None
    daily_breaks = (
        payload.get("daily_breaks") if isinstance(payload.get("daily_breaks"), list) else None
    )
    weekly_open = payload.get("weekly_open_utc")
    weekly_close = payload.get("weekly_close_utc")

    override_calendar(
        holidays=holidays,
        daily_breaks=daily_breaks,
        weekly_open=weekly_open,
        weekly_close=weekly_close,
        replace_holidays=True,
    )


def calendar_snapshot() -> Dict[str, Any]:
    """Повертає поточний стан календаря (для дебагу/тестів)."""

    return {
        "holidays": list(_FXCM_HOLIDAYS_UTC),
        "daily_breaks": [(start.strftime("%H:%M"), end.strftime("%H:%M")) for start, end in _DAILY_BREAKS],
        "weekly_open": _WEEKLY_OPEN_TIME.strftime("%H:%M"),
        "weekly_close": _WEEKLY_CLOSE_TIME.strftime("%H:%M"),
    }


def _is_holiday(ts: dt.datetime) -> bool:
    return ts.strftime("%Y-%m-%d") in _FXCM_HOLIDAYS_UTC


def _in_daily_break(ts: dt.datetime) -> bool:
    for start, end in _DAILY_BREAKS:
        if start <= end:
            if start <= ts.time() < end:
                return True
        else:  # інтервал перетинає північ
            if ts.time() >= start or ts.time() < end:
                return True
    return False


def is_trading_time(ts: dt.datetime) -> bool:
    """Перевіряє, чи входить момент у торгове вікно (UTC)."""

    weekday = ts.weekday()  # Monday=0 ... Sunday=6

    if _is_holiday(ts):
        return False

    if weekday == 5:  # субота
        return False
    if weekday == 6 and ts.time() < _WEEKLY_OPEN_TIME:
        return False
    if weekday == 4 and ts.time() >= _WEEKLY_CLOSE_TIME:
        return False

    if _in_daily_break(ts):
        return False

    return True


def next_trading_open(ts: dt.datetime) -> dt.datetime:
    """Повертає найближчий (>=ts) момент початку торгів."""

    candidate = ts.replace(second=0, microsecond=0)
    while not is_trading_time(candidate):
        candidate += dt.timedelta(minutes=1)
    return candidate


def next_trading_pause(ts: dt.datetime) -> dt.datetime:
    """Наступний момент завершення торгового вікна (включаючи вихідні та перерви)."""

    weekday = ts.weekday()
    today = ts.date()

    # Перевірка денної перерви
    for start, end in _DAILY_BREAKS:
        start_dt = dt.datetime.combine(today, start, tzinfo=ts.tzinfo)
        end_dt = dt.datetime.combine(today, end, tzinfo=ts.tzinfo)
        if start <= end:
            if start_dt <= ts < end_dt:
                return ts  # вже в паузі
            if ts < start_dt:
                return start_dt
        else:
            # перерва перетинає північ (наприклад 23:55-00:10)
            if ts >= start_dt:
                return start_dt
            if ts < end_dt:
                return dt.datetime.combine(today - dt.timedelta(days=1), start, tzinfo=ts.tzinfo)

    # Кінець тижневої сесії
    if weekday == 4:  # п'ятниця
        close_dt = dt.datetime.combine(today, _WEEKLY_CLOSE_TIME, tzinfo=ts.tzinfo)
        if ts < close_dt:
            return close_dt
    if weekday in (5,):  # субота (в нормі не зайдемо, але на всяк випадок)
        return next_trading_open(ts)
    if weekday == 6:  # неділя
        close_dt = dt.datetime.combine(today, dt.time(23, 59), tzinfo=ts.tzinfo)
        return close_dt

    # Дефолт: вважаємо, що найближча пауза через добу
    return ts + dt.timedelta(days=1)


def generate_request_windows(
    start: dt.datetime,
    end: dt.datetime,
    *,
    chunk_minutes: int = 240,
) -> Iterator[Tuple[dt.datetime, dt.datetime]]:
    """Генерує вікна для запитів історії лише в межах торгових періодів."""

    if start >= end:
        return

    cursor = start
    while cursor < end:
        cursor = next_trading_open(cursor)
        if cursor >= end:
            break
        window_end = min(end, cursor + dt.timedelta(minutes=chunk_minutes))
        yield cursor, window_end
        cursor = window_end
