"""Утиліти для торгових сесій FXCM (спрощена модель 24/5 з технічними паузами).

Модуль отримав гнучкі хелпери для оверрайду календаря через ENV/JSON.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, NamedTuple, Optional, Sequence, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.7 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore[assignment]

log = logging.getLogger("fxcm_sessions")
if not log.handlers:
    log.addHandler(logging.NullHandler())

_TZ_UTC = "UTC"
_TZ_NEW_YORK = "America/New_York"
_TZ_LONDON = "Europe/London"
_TZ_TOKYO = "Asia/Tokyo"


class BreakWindow(NamedTuple):
    start: dt.time
    end: dt.time
    tz: str = _TZ_UTC


class SessionWindow(NamedTuple):
    tag: str
    start: dt.time
    end: dt.time
    window_tz: str
    timezone: str


class SessionMatch(NamedTuple):
    tag: str
    timezone: str
    session_open_utc: dt.datetime
    session_close_utc: dt.datetime
    window_tz: str


_TZ_CACHE: Dict[str, ZoneInfo] = {}
_KNOWN_TZ_ALIASES = {
    "utc": _TZ_UTC,
    "z": _TZ_UTC,
    "gmt": _TZ_UTC,
    "america/new_york": _TZ_NEW_YORK,
    "america/new-york": _TZ_NEW_YORK,
    "new_york": _TZ_NEW_YORK,
    "new-york": _TZ_NEW_YORK,
    "ny": _TZ_NEW_YORK,
    "et": _TZ_NEW_YORK,
    "est": _TZ_NEW_YORK,
    "edt": _TZ_NEW_YORK,
    "europe/london": _TZ_LONDON,
    "europe/lon": _TZ_LONDON,
    "london": _TZ_LONDON,
    "uk": _TZ_LONDON,
    "bst": _TZ_LONDON,
    "asia/tokyo": _TZ_TOKYO,
    "tokyo": _TZ_TOKYO,
    "japan": _TZ_TOKYO,
    "jst": _TZ_TOKYO,
}

# FXCM metals: неділя 18:00 (New York)  п'ятниця 16:55 (New York).
_WEEKLY_OPEN_TIME = dt.time(18, 0)
_WEEKLY_OPEN_TZ = _TZ_NEW_YORK
_WEEKLY_CLOSE_TIME = dt.time(16, 55)
_WEEKLY_CLOSE_TZ = _TZ_NEW_YORK

# Щоденне вікно технічного обслуговування (17:00-18:00 New York).
_DAILY_BREAKS: Sequence[BreakWindow] = (
    BreakWindow(dt.time(17, 0), dt.time(18, 0), _TZ_NEW_YORK),
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

_SESSION_WINDOWS: Sequence[SessionWindow] = ()


def _get_zoneinfo(label: str) -> ZoneInfo:
    cached = _TZ_CACHE.get(label)
    if cached:
        return cached
    tz = ZoneInfo(label)
    _TZ_CACHE[label] = tz
    return tz


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


def _nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> dt.date:
    first_day = dt.date(year, month, 1)
    offset = (weekday - first_day.weekday()) % 7
    target = first_day + dt.timedelta(days=offset + 7 * (occurrence - 1))
    return target


def _ensure_utc(ts: dt.datetime) -> dt.datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc)


def _normalize_tz_label(raw: Optional[str]) -> str:
    if not raw:
        return _TZ_UTC
    text = raw.strip()
    alias_key = text.lower().replace("\\", "/")
    target = _KNOWN_TZ_ALIASES.get(alias_key, text)
    try:
        _get_zoneinfo(target)
        return target
    except Exception:
        log.warning("Невідомий timezone '%s', використовую UTC", raw)
        return _TZ_UTC


def _convert_utc_to_local(ts: dt.datetime, tz_label: str) -> dt.datetime:
    ts_utc = _ensure_utc(ts)
    try:
        tz = _get_zoneinfo(tz_label)
    except Exception:
        return ts_utc
    return ts_utc.astimezone(tz).replace(tzinfo=None)


def _convert_local_to_utc(local_dt: dt.datetime, tz_label: str, *, target_tzinfo: Optional[dt.tzinfo] = None) -> dt.datetime:
    try:
        tz = _get_zoneinfo(tz_label)
    except Exception:
        tz = dt.timezone.utc
    localized = local_dt.replace(tzinfo=tz)
    utc_dt = localized.astimezone(dt.timezone.utc)
    if target_tzinfo is None or target_tzinfo is dt.timezone.utc:
        return utc_dt
    return utc_dt.astimezone(target_tzinfo)


def _split_time_and_tz(value: str) -> Tuple[str, Optional[str]]:
    if "@" in value:
        time_part, tz_part = value.split("@", 1)
        return time_part, tz_part
    return value, None


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


def _parse_time_with_tz(value: str) -> Optional[Tuple[dt.time, str]]:
    time_part, tz_raw = _split_time_and_tz(value)
    parsed_time = _parse_time_str(time_part)
    if parsed_time is None:
        return None
    return parsed_time, _normalize_tz_label(tz_raw)


def _parse_break_entry(entry: Any) -> Optional[BreakWindow]:
    start_raw: Optional[str]
    end_raw: Optional[str]
    tz_raw: Optional[str] = None

    if isinstance(entry, str):
        entry_body, tz_raw = _split_time_and_tz(entry)
        if "-" not in entry_body:
            return None
        start_raw, end_raw = entry_body.split("-", 1)
    elif isinstance(entry, Sequence) and len(entry) in {2, 3}:
        start_raw = str(entry[0])
        end_raw = str(entry[1])
        if len(entry) == 3:
            tz_raw = str(entry[2])
    elif isinstance(entry, Mapping):
        start_raw = entry.get("start") or entry.get("from")
        end_raw = entry.get("end") or entry.get("to")
        tz_raw = entry.get("tz") or entry.get("timezone")
    else:
        return None

    start_time = _parse_time_str(start_raw or "")
    end_time = _parse_time_str(end_raw or "")
    if start_time is None or end_time is None:
        return None
    tz_label = _normalize_tz_label(tz_raw)
    return BreakWindow(start_time, end_time, tz_label)


def _parse_session_window(entry: Any) -> Optional[SessionWindow]:
    if not isinstance(entry, Mapping):
        return None
    tag = str(entry.get("tag") or entry.get("name") or "").strip()
    if not tag:
        return None
    start = _parse_time_str(str(entry.get("start") or ""))
    end = _parse_time_str(str(entry.get("end") or ""))
    if start is None or end is None:
        return None
    window_tz = _normalize_tz_label(entry.get("tz") or entry.get("window_tz"))
    timezone = _normalize_tz_label(entry.get("timezone")) if entry.get("timezone") else window_tz
    return SessionWindow(tag=tag, start=start, end=end, window_tz=window_tz, timezone=timezone)


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
    session_windows: Optional[Sequence[Any]] = None,
    replace_holidays: bool = False,
) -> None:
    """Гнучко оновлює календар (використовується ENV/JSON)."""

    global _FXCM_HOLIDAYS_UTC, _DAILY_BREAKS, _WEEKLY_OPEN_TIME, _WEEKLY_OPEN_TZ, _WEEKLY_CLOSE_TIME, _WEEKLY_CLOSE_TZ, _SESSION_WINDOWS

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
        parsed = _parse_time_with_tz(weekly_open)
        if parsed:
            _WEEKLY_OPEN_TIME, _WEEKLY_OPEN_TZ = parsed
            updated = True

    if weekly_close:
        parsed = _parse_time_with_tz(weekly_close)
        if parsed:
            _WEEKLY_CLOSE_TIME, _WEEKLY_CLOSE_TZ = parsed
            updated = True

    if session_windows is not None:
        parsed_windows = [value for value in (_parse_session_window(entry) for entry in session_windows) if value]
        _SESSION_WINDOWS = tuple(parsed_windows)
        updated = True

    if updated:
        log.info(
            "Календар FXCM оновлено (holidays=%d, breaks=%d, sessions=%d, open=%s@%s, close=%s@%s).",
            len(_FXCM_HOLIDAYS_UTC),
            len(_DAILY_BREAKS),
            len(_SESSION_WINDOWS),
            _WEEKLY_OPEN_TIME,
            _WEEKLY_OPEN_TZ,
            _WEEKLY_CLOSE_TIME,
            _WEEKLY_CLOSE_TZ,
        )


def load_calendar_file(path: Path) -> None:
    """Завантажує оверрайд календаря з JSON-файлу."""

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
    session_windows = (
        payload.get("session_windows") if isinstance(payload.get("session_windows"), list) else None
    )

    override_calendar(
        holidays=holidays,
        daily_breaks=daily_breaks,
        weekly_open=weekly_open,
        weekly_close=weekly_close,
        session_windows=session_windows,
        replace_holidays=True,
    )


def calendar_snapshot() -> Dict[str, Any]:
    """Повертає поточний стан календаря (для дебагу/тестів)."""

    return {
        "holidays": list(_FXCM_HOLIDAYS_UTC),
        "daily_breaks": [
            {"start": start.strftime("%H:%M"), "end": end.strftime("%H:%M"), "tz": tz}
            for start, end, tz in _DAILY_BREAKS
        ],
        "weekly_open": f"{_WEEKLY_OPEN_TIME.strftime('%H:%M')}@{_WEEKLY_OPEN_TZ}",
        "weekly_close": f"{_WEEKLY_CLOSE_TIME.strftime('%H:%M')}@{_WEEKLY_CLOSE_TZ}",
        "session_windows": [
            {
                "tag": window.tag,
                "start": window.start.strftime("%H:%M"),
                "end": window.end.strftime("%H:%M"),
                "tz": window.window_tz,
                "timezone": window.timezone,
            }
            for window in _SESSION_WINDOWS
        ],
    }


def _is_holiday(ts: dt.datetime) -> bool:
    return ts.strftime("%Y-%m-%d") in _FXCM_HOLIDAYS_UTC


def _in_daily_break(ts: dt.datetime) -> bool:
    ts_utc = _ensure_utc(ts)
    for start, end, tz_label in _DAILY_BREAKS:
        local_dt = _convert_utc_to_local(ts_utc, tz_label)
        local_time = local_dt.time()
        if start <= end:
            if start <= local_time < end:
                return True
        else:
            if local_time >= start or local_time < end:
                return True
    return False


def is_trading_time(ts: dt.datetime) -> bool:
    """Перевіряє, чи входить момент у торгове вікно (UTC)."""

    ts_utc = _ensure_utc(ts)
    if _is_holiday(ts_utc):
        return False

    local_open_dt = _convert_utc_to_local(ts_utc, _WEEKLY_OPEN_TZ)
    local_close_dt = _convert_utc_to_local(ts_utc, _WEEKLY_CLOSE_TZ)

    weekday_open = local_open_dt.weekday()
    time_open = local_open_dt.time()
    if weekday_open == 5:
        return False
    if weekday_open == 6 and time_open < _WEEKLY_OPEN_TIME:
        return False

    weekday_close = local_close_dt.weekday()
    time_close = local_close_dt.time()
    if weekday_close == 4 and time_close >= _WEEKLY_CLOSE_TIME:
        return False
    if weekday_close == 5:
        return False

    if _in_daily_break(ts_utc):
        return False

    return True


def next_trading_open(ts: dt.datetime) -> dt.datetime:
    """Повертає найближчий (>=ts) момент початку торгів."""

    candidate = _ensure_utc(ts).replace(second=0, microsecond=0)
    while not is_trading_time(candidate):
        candidate += dt.timedelta(minutes=1)
    return candidate


def _session_window_bounds(window: SessionWindow, ts_utc: dt.datetime) -> Tuple[dt.datetime, dt.datetime]:
    tz = _get_zoneinfo(window.window_tz)
    local_ts = _ensure_utc(ts_utc).astimezone(tz)
    local_date = local_ts.date()
    start_local = dt.datetime.combine(local_date, window.start)
    end_local = dt.datetime.combine(local_date, window.end)
    if window.start <= window.end:
        pass
    else:
        if local_ts.time() >= window.start:
            end_local += dt.timedelta(days=1)
        else:
            start_local -= dt.timedelta(days=1)
    start_utc = _convert_local_to_utc(start_local, window.window_tz)
    end_utc = _convert_local_to_utc(end_local, window.window_tz)
    return start_utc, end_utc


def resolve_session(ts: dt.datetime) -> Optional[SessionMatch]:
    ts_utc = _ensure_utc(ts)
    for window in _SESSION_WINDOWS:
        start_utc, end_utc = _session_window_bounds(window, ts_utc)
        if start_utc <= ts_utc < end_utc:
            return SessionMatch(
                tag=window.tag,
                timezone=window.timezone,
                session_open_utc=start_utc,
                session_close_utc=end_utc,
                window_tz=window.window_tz,
            )
    return None


def next_trading_pause(ts: dt.datetime) -> dt.datetime:
    """Наступний момент завершення торгового вікна (включаючи вихідні та перерви)."""

    ts_utc = _ensure_utc(ts)
    tzinfo = ts.tzinfo or dt.timezone.utc

    if _in_daily_break(ts_utc):
        return ts_utc.astimezone(tzinfo)

    candidates: List[dt.datetime] = []

    for start, _end, tz_label in _DAILY_BREAKS:
        local_dt = _convert_utc_to_local(ts_utc, tz_label)
        local_date = local_dt.date()
        start_local = dt.datetime.combine(local_date, start)
        start_utc = _convert_local_to_utc(start_local, tz_label)
        if start_utc <= ts_utc:
            start_local = dt.datetime.combine(local_date + dt.timedelta(days=1), start)
            start_utc = _convert_local_to_utc(start_local, tz_label)
        candidates.append(start_utc)

    local_close_dt = _convert_utc_to_local(ts_utc, _WEEKLY_CLOSE_TZ)
    close_local = dt.datetime.combine(local_close_dt.date(), _WEEKLY_CLOSE_TIME)
    close_utc = _convert_local_to_utc(close_local, _WEEKLY_CLOSE_TZ)
    if close_utc <= ts_utc:
        close_local = dt.datetime.combine(local_close_dt.date() + dt.timedelta(days=1), _WEEKLY_CLOSE_TIME)
        close_utc = _convert_local_to_utc(close_local, _WEEKLY_CLOSE_TZ)
    candidates.append(close_utc)

    return min(candidates).astimezone(tzinfo)


def generate_request_windows(
    start: dt.datetime,
    end: dt.datetime,
    *,
    chunk_minutes: int = 240,
) -> Iterator[Tuple[dt.datetime, dt.datetime]]:
    """Генерує вікна для запитів історії лише в межах торгових періодів."""

    if start >= end:
        return

    cursor = _ensure_utc(start)
    end_utc = _ensure_utc(end)
    while cursor < end_utc:
        cursor = next_trading_open(cursor)
        if cursor >= end_utc:
            break
        window_end = min(end_utc, cursor + dt.timedelta(minutes=chunk_minutes))
        yield cursor, window_end
        cursor = window_end
