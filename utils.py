"""Універсальні утиліти (форматування, нормалізація, допоміжні хелпери).

Призначення:
    • Форматування числових метрик (ціна, обсяг, OI)
    • Нормалізація TP/SL відповідно до напрямку угоди
    • Маппінг рекомендацій у сигнали UI
    • Уніфікація та очищення часових колонок DataFrame
    • Нормалізація назв тригерів до канонічних коротких ідентифікаторів

Принципи:
    • Відсутність побічних ефектів (чиста логіка)
    • Українська мова для коментарів / докстрінгів
    • Мінімальна залежність від решти системи (центральні константи з config)
"""

from __future__ import annotations

import logging

import pandas as pd

from rich.logging import RichHandler


# ── Локальний логер модуля ────────────────────────────────────────────────────
logger = logging.getLogger("utils")
if not logger.handlers:  # захист від повторної ініціалізації
    handler = RichHandler(rich_tracebacks=True)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s", datefmt="[%X]"))
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

# ── Timestamp / DataFrame ────────────────────────────────────────────────────
def ensure_timestamp_column(
    df: pd.DataFrame,
    *,
    as_index: bool = False,
    drop_duplicates: bool = True,
    sort: bool = True,
    logger_obj: logging.Logger | None = None,
    min_rows: int = 1,
    log_prefix: str = "",
) -> pd.DataFrame:
    """Уніфікує колонку/індекс `timestamp` у DataFrame.

    Можливості:
      - гарантує наявність `timestamp: datetime64[ns, UTC]`;
      - за потреби перетворює у колонку/індекс;
      - видаляє дублі та `NaT`;
      - стабільно сортує за часом;
      - повертає порожній DataFrame, якщо після обробки рядків < `min_rows`.

    Args:
        df: Вхідний DataFrame.
        as_index: Якщо True — встановити `timestamp` індексом.
        drop_duplicates: Видаляти дублі `timestamp`.
        sort: Сортувати за `timestamp`.
        logger_obj: Логер для детальніших повідомлень.
        min_rows: Мінімальна кількість рядків після обробки.
        log_prefix: Префікс до діагностичних повідомлень.

    Returns:
        pd.DataFrame: Очищена/уніфікована таблиця або порожній DataFrame.
    """

    def _log(msg: str) -> None:
        if logger_obj:
            logger_obj.debug("%s%s", log_prefix, msg)

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        _log("[ensure_timestamp_column] DataFrame порожній або невалідний.")
        return pd.DataFrame()

    # Якщо timestamp є індексом — переносимо у колонку
    if "timestamp" not in df.columns and df.index.name == "timestamp":
        df = df.reset_index()
        _log("[ensure_timestamp_column] Перенесено timestamp з індексу у колонку.")

    # Нормалізація колонки
    if "timestamp" in df.columns:

        ts = df["timestamp"]

        # 1) Якщо dtype не datetime → як і було:
        if not pd.api.types.is_datetime64_any_dtype(ts):
            if pd.api.types.is_integer_dtype(ts):
                v = ts.astype("int64")
                try:
                    med = float(v.median())
                except Exception:
                    med = float(v.iloc[0]) if len(v) else 0.0
                # Автовизначення одиниць часу за порядком величини
                if 1e11 <= med < 1e14:
                    unit = "ms"
                elif 1e9 <= med < 1e11:
                    unit = "s"
                elif 1e14 <= med < 1e17:
                    unit = "us"
                elif 1e17 <= med < 1e20:
                    unit = "ns"
                else:
                    unit = "ms"  # дефолт безпечний для Binance
                df["timestamp"] = pd.to_datetime(
                    v, unit=unit, errors="coerce", utc=True
                )
                _log(f"[ensure_timestamp_column] int→datetime(unit={unit}).")
            else:
                df["timestamp"] = pd.to_datetime(ts, errors="coerce", utc=True)
                _log("[ensure_timestamp_column] to_datetime(auto).")
        else:
            # 2) Уже datetime, але схоже на епоху → спробуємо відновити з сирих колонок
            try:
                years = ts.dt.year
                if years.max() <= 1971:
                    # спроба відновлення з альтернативних сирих полів
                    candidates = ["open_time", "openTime", "time", "t", "close_time"]
                    raw_col = None
                    for c in candidates:
                        if c in df.columns:
                            raw_col = c
                            break
                    if raw_col is not None:
                        raw = df[raw_col]
                        if pd.api.types.is_integer_dtype(raw):
                            v = raw.astype("int64")
                            med = float(v.median())
                            # Вибір одиниць:
                            # ~1e9..1e10 → секунди, ~1e11..1e13 → мілісекунди,
                            # ~1e14..1e16 → мікросекунди, ~1e17..1e19 → наносекунди
                            if 1e11 <= med < 1e14:
                                unit = "ms"
                            elif 1e9 <= med < 1e11:
                                unit = "s"
                            elif 1e14 <= med < 1e17:
                                unit = "us"
                            elif 1e17 <= med < 1e20:
                                unit = "ns"
                            else:
                                unit = None
                            if unit:
                                df["timestamp"] = pd.to_datetime(v, unit=unit, utc=True)
                                _log(
                                    f"[ensure_timestamp_column] Відновлено з '{raw_col}' (unit={unit})."
                                )
                            else:
                                _log(
                                    f"[ensure_timestamp_column] '{raw_col}' має нетипову шкалу (median={med:.3g})."
                                )
                        else:
                            _log(
                                f"[ensure_timestamp_column] '{raw_col}' не є цілочисельним."
                            )
                    else:
                        _log(
                            "[ensure_timestamp_column] Нема сирої колонки часу для відновлення."
                        )
            except Exception:
                pass
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            _log("[ensure_timestamp_column] Конвертовано 'timestamp' у datetime (UTC).")

        before = len(df)
        df = df.dropna(subset=["timestamp"]).copy()
        removed = before - len(df)
        if removed > 0:
            _log(
                f"[ensure_timestamp_column] Видалено {removed} рядків із NaT у 'timestamp'."
            )

        if drop_duplicates:
            before = len(df)
            df = df.drop_duplicates(subset=["timestamp"])
            dups = before - len(df)
            if dups > 0:
                _log(
                    f"[ensure_timestamp_column] Видалено {dups} дублікатів по 'timestamp'."
                )

        if sort:
            df = df.sort_values("timestamp", kind="stable")
            _log("[ensure_timestamp_column] Відсортовано за 'timestamp' (stable).")

        if as_index and df.index.name != "timestamp":
            df = df.set_index("timestamp")
            _log("[ensure_timestamp_column] Встановлено 'timestamp' як індекс.")
        elif not as_index and df.index.name == "timestamp":
            df = df.reset_index()
            _log(
                "[ensure_timestamp_column] Переведено 'timestamp' з індексу у колонку."
            )
    else:
        _log("[ensure_timestamp_column] Відсутня колонка 'timestamp' у DataFrame.")

    # Діагностика прикладу
    if len(df) > 0:
        if "timestamp" in df.columns:
            _log(f"[ensure_timestamp_column] Приклад: {df['timestamp'].iloc[0]!r}")
        elif df.index.name == "timestamp":
            _log(f"[ensure_timestamp_column] Приклад (індекс): {df.index[0]!r}")

    if len(df) < min_rows:
        _log(
            f"[ensure_timestamp_column] Після обробки залишилось {len(df)} рядків (<{min_rows}). "
            "Повертаю порожній DataFrame."
        )
        return pd.DataFrame()

    return df
