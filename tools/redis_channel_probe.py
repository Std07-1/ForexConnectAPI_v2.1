"""Швидка перевірка Redis Pub/Sub каналів FXCM-конектора.

Утиліта безпечна: лише підписується на канали і читає повідомлення (не публікує).
Працює з профілями `.env`/`AI_ONE_ENV_FILE` через `env_profile.load_env_profile()`.

Приклад:
  python -m tools.redis_channel_probe --seconds 8
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any, Dict, List, Optional

import redis

from config import FXCMConfig, load_config
from env_profile import load_env_profile


def _summarize_payload(raw: str) -> Dict[str, Any]:
    try:
        obj = json.loads(raw)
    except Exception:
        return {"raw_len": len(raw)}

    if not isinstance(obj, dict):
        return {"json_type": type(obj).__name__}

    summary: Dict[str, Any] = {}
    for key in ("type", "ts", "process", "market", "price", "ohlcv", "symbol", "tf"):
        if key in obj:
            summary[key] = obj.get(key)

    bars = obj.get("bars")
    if isinstance(bars, list):
        summary["bars_n"] = len(bars)

    if "source" in obj:
        summary["source"] = obj.get("source")

    return summary


def _collect_channels(cfg: FXCMConfig) -> List[str]:
    return [
        cfg.ohlcv_channel,
        cfg.price_stream.channel,
        cfg.observability.status_channel,
        cfg.observability.heartbeat_channel,
        cfg.commands_channel,
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Перевірка публікацій у Redis-каналах FXCM")
    parser.add_argument("--seconds", type=float, default=6.0, help="Тривалість прослуховування, сек")
    args = parser.parse_args()

    load_env_profile()
    cfg = load_config()

    r = redis.Redis(
        host=cfg.redis.host,
        port=cfg.redis.port,
        password=None,
        decode_responses=True,
        socket_connect_timeout=2.0,
        socket_timeout=2.0,
    )
    r.ping()

    channels = _collect_channels(cfg)

    print(f"Redis: {cfg.redis.host}:{cfg.redis.port}")
    print("Канали:")
    for ch in channels:
        print(f"  - {ch}")

    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(*channels)

    end_ts = time.time() + float(args.seconds)
    counts: Dict[str, int] = {ch: 0 for ch in channels}
    last: Dict[str, Optional[Dict[str, Any]]] = {ch: None for ch in channels}

    while time.time() < end_ts:
        msg = pubsub.get_message(timeout=1.0)
        if not msg:
            continue

        ch = msg.get("channel")
        data = msg.get("data")
        if not isinstance(ch, str) or ch not in counts:
            continue
        if not isinstance(data, str):
            continue

        counts[ch] += 1
        last[ch] = _summarize_payload(data)

    pubsub.close()

    print(f"\n--- counts ({args.seconds:.1f}s) ---")
    for ch in channels:
        print(f"{ch}: {counts[ch]}")

    print("\n--- last summaries ---")
    for ch in channels:
        print(f"{ch}: {last[ch]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
