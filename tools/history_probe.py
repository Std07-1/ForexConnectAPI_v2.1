"""Утиліта для швидкої перевірки доступності FXCM history по таймфреймах.

Призначення: запустити на сервері та одразу побачити, чи реально FXCM
повертає історію для `m15`, `m60` (1h) та `h4`, використовуючи той самий
мапінг таймфреймів, що й продакшн-конектор.

Приклад:
  C:\Aione_projects\fxcm_connector\.venv_fxcm37\Scripts\python.exe -m tools.history_probe --symbol XAU/USD --hours 24 --tfs m15 m60 h4

Вимоги:
- Задати `FXCM_USERNAME`, `FXCM_PASSWORD` (та за потреби `FXCM_CONNECTION`, `FXCM_HOST_URL`) у середовищі.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from dotenv import dotenv_values, find_dotenv, load_dotenv

import connector
from config import load_config


def _parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Перевіряє FXCM get_history для заданих таймфреймів та показує коротку статистику."
        )
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help=(
            "Опційно: шлях до .env. Якщо задано — завантажується перед читанням конфігу. "
            "Якщо не задано — намагаємось прочитати .env у поточній директорії."
        ),
    )
    parser.add_argument(
        "--override-env",
        action="store_true",
        help=(
            "Якщо задано — значення з .env перекривають поточні змінні середовища. "
            "Корисно, якщо у сесії PowerShell лишилися старі/неправильні FXCM_* змінні."
        ),
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="Символ у форматі FXCM (напр. XAU/USD, EUR/USD).",
    )
    parser.add_argument(
        "--login-timeout-seconds",
        type=float,
        default=30.0,
        help="Ліміт часу на FXCM login (щоб утиліта не зависала назавжди). Default: 30s.",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Скільки годин історії запитати (default: 24).",
    )
    parser.add_argument(
        "--tfs",
        nargs="+",
        required=True,
        help="Список таймфреймів: напр. m15 m60 h4 або 15m 1h 4h.",
    )
    return parser.parse_args(argv)


def _probe_one(
    fx: connector.ForexConnect,
    *,
    symbol_raw: str,
    tf_raw: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
) -> Tuple[str, str, str, int, str]:
    tf_norm = connector._map_timeframe_label(tf_raw)
    tf_fxcm = connector._to_fxcm_timeframe(tf_norm)

    try:
        history = fx.get_history(symbol_raw, tf_fxcm, start_dt, end_dt)
    except Exception as exc:  # noqa: BLE001
        return (tf_raw, tf_norm, tf_fxcm, -1, f"EXC: {type(exc).__name__}: {exc}")

    df_raw = pd.DataFrame(history)
    if df_raw.empty:
        return (tf_raw, tf_norm, tf_fxcm, 0, "EMPTY")

    date_min = df_raw["Date"].min() if "Date" in df_raw.columns else None
    date_max = df_raw["Date"].max() if "Date" in df_raw.columns else None

    if isinstance(date_min, dt.datetime) and isinstance(date_max, dt.datetime):
        info = f"OK: {date_min.isoformat()} → {date_max.isoformat()}"
    else:
        info = "OK"

    return (tf_raw, tf_norm, tf_fxcm, int(len(df_raw)), info)


def main(argv: List[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    override_env = bool(getattr(args, "override_env", False))

    env_file_raw = args.env_file
    if isinstance(env_file_raw, str) and env_file_raw.strip():
        env_path = Path(env_file_raw.strip())
        if not env_path.exists():
            print(f"ERROR: env-file не знайдено: {env_path}")
            return 3
        loaded = load_dotenv(dotenv_path=str(env_path), override=override_env)
        print(f"Env: завантажено з {env_path} -> {bool(loaded)}")
    else:
        detected = find_dotenv(usecwd=True) or "(не знайдено)"
        loaded = load_dotenv(override=override_env)
        print(f"Env: autodetect={detected} -> {bool(loaded)}")

    # Показуємо лише наявність ключів, без значень.
    has_user = bool((os.environ.get("FXCM_USERNAME") or "").strip())
    has_pass = bool((os.environ.get("FXCM_PASSWORD") or "").strip())
    has_conn = bool((os.environ.get("FXCM_CONNECTION") or "").strip())
    print(f"Env keys: FXCM_USERNAME={has_user}, FXCM_PASSWORD={has_pass}, FXCM_CONNECTION={has_conn}")

    # Діагностика перекриттів: .env vs ENV (без значень/секретів).
    env_path_for_compare: str | None = None
    if isinstance(env_file_raw, str) and env_file_raw.strip():
        env_path_for_compare = str(Path(env_file_raw.strip()))
    else:
        detected_path = find_dotenv(usecwd=True)
        env_path_for_compare = detected_path or None

    if env_path_for_compare and Path(env_path_for_compare).exists():
        values = dotenv_values(env_path_for_compare)
        file_user_len = len((values.get("FXCM_USERNAME") or "").strip())
        env_user_len = len((os.environ.get("FXCM_USERNAME") or "").strip())
        file_conn = (values.get("FXCM_CONNECTION") or "").strip()
        env_conn = (os.environ.get("FXCM_CONNECTION") or "").strip()
        if file_user_len and env_user_len and file_user_len != env_user_len:
            print(
                "WARN: FXCM_USERNAME у ENV відрізняється від .env (len %d vs %d). "
                "Можеш запустити з --override-env або очистити Env:FXCM_USERNAME/FXCM_PASSWORD."
                % (env_user_len, file_user_len)
            )
        if file_conn and env_conn and file_conn != env_conn:
            print(
                "WARN: FXCM_CONNECTION у ENV відрізняється від .env (%s vs %s)."
                % (env_conn, file_conn)
            )

    try:
        cfg = load_config()
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: не вдалося завантажити конфіг/креденшали: {exc}")
        return 3

    # Не друкуємо креденшали, лише безпечний контекст для дебагу.
    user_len = len(getattr(cfg, "username", "") or "")
    conn = getattr(cfg, "connection", "")
    host_url = getattr(cfg, "host_url", "")
    print(
        "Config: connection=%s, host_url=%s, username_len=%d"
        % (str(conn), str(host_url), int(user_len))
    )
    fx = None
    try:
        login_deadline = time.monotonic() + max(1.0, float(args.login_timeout_seconds))
        attempt = 0
        while True:
            attempt += 1
            try:
                fx = connector._login_fxcm_once(cfg)
                break
            except KeyboardInterrupt:
                print("ABORT: перервано користувачем під час FXCM login.")
                return 130
            except Exception as exc:  # noqa: BLE001
                if time.monotonic() >= login_deadline:
                    print(f"ERROR: FXCM login неуспішний за відведений час: {exc}")
                    return 4

                # Простий backoff з клампом по deadline (щоб не спати довше ніж залишок часу).
                delay = min(10.0, float(2 ** (attempt - 1)))
                remaining = max(0.0, login_deadline - time.monotonic())
                sleep_for = min(delay, remaining)
                print(
                    f"WARN: FXCM login неуспішний (спроба {attempt}): {exc}. "
                    f"Пауза {sleep_for:.1f}с (залишок {remaining:.1f}с)."
                )
                if sleep_for <= 0:
                    print("ERROR: FXCM login неуспішний за відведений час.")
                    return 4
                try:
                    time.sleep(sleep_for)
                except KeyboardInterrupt:
                    print("ABORT: перервано користувачем під час очікування логіну.")
                    return 130

        end_dt = connector._now_utc()
        start_dt = end_dt - dt.timedelta(hours=max(1, int(args.hours)))

        symbol = str(args.symbol).strip()
        tfs = [str(tf).strip() for tf in args.tfs if str(tf).strip()]

        print(f"FXCM history probe: symbol={symbol}, період={start_dt.isoformat()} → {end_dt.isoformat()}")
        print("tf_raw\ttf_norm\ttf_fxcm\trows\tresult")

        any_ok = False
        for tf_raw in tfs:
            tf_raw_out, tf_norm, tf_fxcm, rows, info = _probe_one(
                fx,
                symbol_raw=symbol,
                tf_raw=tf_raw,
                start_dt=start_dt,
                end_dt=end_dt,
            )
            if rows > 0:
                any_ok = True
            print(f"{tf_raw_out}\t{tf_norm}\t{tf_fxcm}\t{rows}\t{info}")

        return 0 if any_ok else 2
    finally:
        connector._close_fxcm_session(fx)


if __name__ == "__main__":
    raise SystemExit(main())
