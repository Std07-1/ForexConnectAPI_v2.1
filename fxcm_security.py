"""Спільні утиліти безпеки для FXCM конектора/інгестора."""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Mapping


def compute_payload_hmac(
    payload: Mapping[str, Any],
    secret: str,
    algo: str = "sha256",
) -> str:
    """Повертає hex HMAC для JSON payload з детермінованим серіалізатором."""

    if not secret:
        return ""

    normalized_algo = (algo or "sha256").strip().lower() or "sha256"
    digestmod = getattr(hashlib, normalized_algo, hashlib.sha256)
    raw = json.dumps(
        payload,
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=False,
    ).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), raw, digestmod).hexdigest()
