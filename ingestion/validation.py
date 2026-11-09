"""Validate raw events before they enter the warehouse.

Bad records are not dropped silently: the consumer writes them to a dead-letter
file with the reason, so they can be inspected and replayed.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

# field -> allowed Python types (None allowed only where listed)
SCHEMA: dict[str, tuple[type, ...]] = {
    "event_id": (str,),
    "timestamp": (str,),
    "team": (str,),
    "feature": (str,),
    "user_id": (str,),
    "model": (str,),
    "request_type": (str,),
    "input_tokens": (int,),
    "output_tokens": (int,),
    "latency_ms": (int,),
    "status": (str,),
    "cost_usd": (int, float),
    "agent_run_id": (str, type(None)),
    "injected_anomaly": (str, type(None)),
}
STATUSES = {"success", "error", "rate_limited"}
REQUEST_TYPES = {"chat", "embedding"}


class InvalidEvent(ValueError):
    pass


def parse_event(raw: bytes | str) -> dict[str, Any]:
    """Parse and validate one JSON event. Raises InvalidEvent with a clear reason."""
    try:
        event = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidEvent(f"not valid JSON: {exc}") from None
    if not isinstance(event, dict):
        raise InvalidEvent("event is not a JSON object")

    missing = SCHEMA.keys() - event.keys()
    if missing:
        raise InvalidEvent(f"missing fields: {sorted(missing)}")
    for field, types in SCHEMA.items():
        value = event[field]
        # bool is a subclass of int in Python; reject it explicitly
        if isinstance(value, bool) or not isinstance(value, types):
            raise InvalidEvent(f"field '{field}' has wrong type {type(value).__name__}")

    if event["status"] not in STATUSES:
        raise InvalidEvent(f"unknown status '{event['status']}'")
    if event["request_type"] not in REQUEST_TYPES:
        raise InvalidEvent(f"unknown request_type '{event['request_type']}'")
    for f in ("input_tokens", "output_tokens", "latency_ms", "cost_usd"):
        if event[f] < 0:
            raise InvalidEvent(f"negative {f}")
    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    except ValueError:
        raise InvalidEvent(f"bad timestamp '{event['timestamp']}'") from None

    return {k: event[k] for k in SCHEMA}  # drop unknown extra fields
