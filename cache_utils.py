"""Примітивне файлове кешування OHLCV-барів."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple, cast

import pandas as pd

CACHE_VERSION = 1
CACHE_COLUMNS = [
    "symbol",
    "tf",
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


def _paths(root: Path, symbol: str, tf: str) -> Tuple[Path, Path]:
    csv_path = root / f"{symbol}_{tf}.csv"
    meta_path = root / f"{symbol}_{tf}.meta.json"
    return csv_path, meta_path


@dataclass
class CacheRecord:
    data: pd.DataFrame
    meta: Dict[str, Any]


def ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=CACHE_COLUMNS)
    missing = [col for col in CACHE_COLUMNS if col not in df.columns]
    if missing:
        df = df.reindex(columns=[*df.columns, *missing])
    return cast(pd.DataFrame, df[CACHE_COLUMNS].copy())


def load_cache(root: Path, symbol: str, tf: str) -> CacheRecord:
    root.mkdir(parents=True, exist_ok=True)
    csv_path, meta_path = _paths(root, symbol, tf)

    if not csv_path.exists():
        return CacheRecord(ensure_schema(pd.DataFrame()), {})

    try:
        df = pd.read_csv(csv_path)
    except Exception:
        return CacheRecord(ensure_schema(pd.DataFrame()), {})

    df = ensure_schema(df)
    try:
        with meta_path.open("r", encoding="utf-8") as fh:
            meta = json.load(fh)
    except Exception:
        meta = {}

    if meta.get("version") != CACHE_VERSION:
        return CacheRecord(ensure_schema(pd.DataFrame()), {})

    return CacheRecord(df, meta)


def save_cache_data(root: Path, symbol: str, tf: str, df: pd.DataFrame) -> None:
    root.mkdir(parents=True, exist_ok=True)
    csv_path, _ = _paths(root, symbol, tf)
    ensure_schema(df).to_csv(csv_path, index=False)


def save_cache_meta(root: Path, symbol: str, tf: str, meta: Dict[str, Any]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _, meta_path = _paths(root, symbol, tf)
    payload = dict(meta)
    payload["version"] = CACHE_VERSION
    with meta_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def save_cache(root: Path, symbol: str, tf: str, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
    """Залишено для сумісності: перезаписує і дані, і метадані."""

    save_cache_data(root, symbol, tf, df)
    save_cache_meta(root, symbol, tf, meta)


def merge_and_trim(
    current: pd.DataFrame,
    incoming: pd.DataFrame,
    *,
    max_rows: int,
) -> pd.DataFrame:
    if current is None or current.empty:
        merged = ensure_schema(incoming)
    elif incoming is None or incoming.empty:
        merged = ensure_schema(current)
    else:
        merged = pd.concat([current, incoming], ignore_index=True)
        merged = merged.drop_duplicates(subset=["open_time"], keep="last")
    if merged.empty:
        return ensure_schema(merged)
    merged = cast(pd.DataFrame, merged.sort_values("open_time").reset_index(drop=True))
    if len(merged) > max_rows:
        merged = merged.iloc[-max_rows:].reset_index(drop=True)
    return cast(pd.DataFrame, merged)