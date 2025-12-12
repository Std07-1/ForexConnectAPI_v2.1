"""Одноразове наповнення кешу XAU/USD для тестових сценаріїв.

Скрипт підʼєднується до FXCM через наявний конфіг, витягує історію
для заданих таймфреймів і зберігає дані до файлового кешу та
додаткових CSV-вікон (7/14/30 днів за замовчуванням).
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional, Sequence
import pandas as pd
from dotenv import load_dotenv

from config import load_config
from connector import (
    BackoffController,
    HistoryCache,
    _close_fxcm_session,
    _map_timeframe_label,
    _normalize_symbol,
    _obtain_fxcm_session,
)
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _forexconnect_available() -> bool:
    return importlib.util.find_spec("forexconnect") is not None


def _detect_repo_python() -> Optional[Path]:
    venv_dir = ROOT_DIR / ".venv_fxcm37"
    candidates: List[Path] = []
    if os.name == "nt":
        candidates.append(venv_dir / "Scripts" / "python.exe")
    else:
        candidates.append(venv_dir / "bin" / "python")
        candidates.append(venv_dir / "bin" / "python3")
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _ensure_forexconnect_backend() -> None:
    if _forexconnect_available():
        return
    reexec_flag = "FXCM_BOOTSTRAP_FORCE_PYTHON"
    if os.environ.get(reexec_flag) == "1":
        raise RuntimeError(
            "ForexConnect SDK не знайдено. Активуй .venv_fxcm37 або встанови forexconnect==1.6.43 перед запуском утиліти."
        )
    candidate = _detect_repo_python()
    if candidate is not None and Path(sys.executable).resolve() != candidate.resolve():
        os.environ[reexec_flag] = "1"
        os.execv(
            str(candidate),
            [
                str(candidate),
                str(Path(__file__).resolve()),
                *sys.argv[1:],
            ],
        )
    raise RuntimeError(
        "ForexConnect SDK не знайдено у поточному середовищі. Активуй .venv_fxcm37 або використай python із forexconnect."
    )


_ensure_forexconnect_backend()




logger = logging.getLogger("cache_bootstrap")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Заповнює локальний кеш історією та експортує додаткові вікна."
    )
    parser.add_argument(
        "--symbol",
        default="XAU/USD",
        help="Символ FXCM (дефолт XAU/USD)",
    )
    parser.add_argument(
        "--timeframes",
        nargs="+",
        default=("m1", "m5", "h1"),
        help="Перелік таймфреймів FXCM (приклад: m1 m5 h1)",
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        type=int,
        default=(7, 14, 30),
        help="Тривалість вікон у днях для додаткових CSV",
    )
    parser.add_argument(
        "--export-dir",
        default=None,
        help="Кастомний шлях для CSV; дефолт cache/exports",
    )
    return parser.parse_args()


def _normalize_timeframe_arg(raw: str) -> str:
    value = (raw or "").strip().lower()
    if not value:
        raise ValueError("Таймфрейм не може бути порожнім")
    if value.startswith("m") and value[1:].isdigit():
        return value
    if value.endswith("m") and value[:-1].isdigit():
        return f"m{value[:-1]}"
    if value.startswith("h") and value[1:].isdigit():
        return value
    if value.endswith("h") and value[:-1].isdigit():
        return f"h{value[:-1]}"
    raise ValueError(f"Непідтримуваний таймфрейм: {raw}")


def _validate_windows(raw_windows: Sequence[int]) -> List[int]:
    cleaned: List[int] = []
    for item in raw_windows:
        if item is None or item <= 0:
            continue
        cleaned.append(int(item))
    unique = sorted(set(cleaned))
    if not unique:
        raise ValueError("Потрібно хоча б одне додатне вікно")
    return unique


def _export_windows(
    df_cache: pd.DataFrame,
    *,
    export_root: Path,
    symbol_norm: str,
    tf_norm: str,
    windows_days: Sequence[int],
) -> None:
    if df_cache.empty:
        logger.warning("Немає даних для експорту (%s %s)", symbol_norm, tf_norm)
        return
    now = dt.datetime.now(dt.timezone.utc)
    export_root.mkdir(parents=True, exist_ok=True)
    for days in windows_days:
        cutoff = now - dt.timedelta(days=days)
        cutoff_ms = int(cutoff.timestamp() * 1000)
        window_df = df_cache.loc[df_cache["open_time"] >= cutoff_ms]
        if window_df.empty:
            logger.warning("Вікно %s днів для %s %s порожнє", days, symbol_norm, tf_norm)
            continue
        out_path = export_root / f"{symbol_norm}_{tf_norm}_{days}d.csv"
        window_df.reset_index(drop=True).to_csv(out_path, index=False)
        logger.info("Експортовано %s рядків → %s", len(window_df), out_path)


def main() -> None:
    from connector import setup_logging

    setup_logging()
    load_dotenv()

    args = _parse_args()
    try:
        timeframes = tuple(_normalize_timeframe_arg(tf) for tf in args.timeframes)
    except ValueError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    try:
        windows = _validate_windows(args.windows)
    except ValueError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    config = load_config()
    if not config.cache.enabled:
        logger.error("Файловий кеш вимкнено в конфізі — завершення")
        sys.exit(1)

    export_root = (
        Path(args.export_dir)
        if args.export_dir
        else config.cache.root / "exports"
    )
    cache_manager = HistoryCache(
        config.cache.root,
        config.cache.max_bars,
        config.cache.warmup_bars,
    )

    backoff = BackoffController(config.backoff.fxcm_login)
    fx_client = _obtain_fxcm_session(config, backoff)
    symbol_norm = _normalize_symbol(args.symbol)
    try:
        for tf_raw in timeframes:
            logger.info("Завантажуємо історію для %s %s", args.symbol, tf_raw)
            df_cache = cache_manager.ensure_ready(
                fx_client,
                symbol_raw=args.symbol,
                timeframe_raw=tf_raw,
            )
            tf_norm = _map_timeframe_label(tf_raw)
            if df_cache.empty:
                logger.warning("FXCM повернув порожню вибірку для %s %s", symbol_norm, tf_norm)
            else:
                logger.info(
                    "Кеш містить %s рядків (%s → %s)",
                    len(df_cache),
                    df_cache["open_time"].min(),
                    df_cache["open_time"].max(),
                )
            _export_windows(
                df_cache,
                export_root=export_root,
                symbol_norm=symbol_norm,
                tf_norm=tf_norm,
                windows_days=windows,
            )
    finally:
        _close_fxcm_session(fx_client)

    logger.info(
        "Кеш готовий: символ %s, TF %s, вікна %s днів",
        args.symbol,
        ",".join(timeframes),
        ",".join(str(v) for v in windows),
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Збій bootstrap кешу: %s", exc)
        sys.exit(1)
