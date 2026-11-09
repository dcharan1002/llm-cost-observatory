"""Injected anomalies — the incidents the observatory should catch.

Four kinds, modelled on real AI cost incidents:

* runaway_agent  – an agent gets stuck in a loop and fires hundreds of calls per
                   minute in one run, with its context growing every step.
* model_switch   – a feature silently moves to the most expensive model.
* error_spike    – a burst of failed requests (bad deploy, provider outage).
* token_bloat    – prompts suddenly become several times larger (context stuffing).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from generator import config
from generator.config import FeatureSpec, TeamSpec
from generator.events import EventFactory, finalize, format_ts

ANOMALY_TYPES = ("runaway_agent", "model_switch", "error_spike", "token_bloat")

# (min, max) duration in minutes per anomaly type
_DURATION_MIN = {
    "runaway_agent": (15, 60),
    "model_switch": (120, 360),
    "error_spike": (15, 45),
    "token_bloat": (60, 180),
}


@dataclass
class Anomaly:
    type: str
    start: datetime
    end: datetime
    team: str
    feature: str
    params: dict[str, Any] = field(default_factory=dict)

    def active(self, ts: datetime) -> bool:
        return self.start <= ts < self.end

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["start"], d["end"] = format_ts(self.start), format_ts(self.end)
        return d


def _all_features() -> list[tuple[TeamSpec, FeatureSpec]]:
    return [(t, f) for t in config.TEAMS for f in t.features]


def create_anomaly(kind: str, start: datetime, rng: np.random.Generator) -> Anomaly:
    """Create one anomaly of ``kind`` starting at ``start`` on a suitable feature."""
    pairs = _all_features()
    if kind == "runaway_agent":
        pairs = [(t, f) for t, f in pairs if f.is_agent]
    elif kind in ("model_switch", "token_bloat"):
        # Only chat features that aren't already on the premium model
        pairs = [
            (t, f) for t, f in pairs
            if not config.MODELS[next(iter(f.models))].is_embedding and config.PREMIUM_MODEL not in f.models
        ]
    team, feature = pairs[rng.integers(len(pairs))]

    lo, hi = _DURATION_MIN[kind]
    end = start + timedelta(minutes=int(rng.integers(lo, hi + 1)))

    params: dict[str, Any] = {}
    if kind == "runaway_agent":
        params = {
            "calls_per_minute": int(rng.integers(80, 250)),
            "run_id": str(uuid.UUID(bytes=rng.bytes(16), version=4)),
            "model": max(feature.models, key=lambda m: config.MODELS[m].input_price_per_m),
        }
    elif kind == "model_switch":
        params = {"to_model": config.PREMIUM_MODEL}
    elif kind == "error_spike":
        params = {"error_rate": round(float(rng.uniform(0.35, 0.7)), 2)}
    elif kind == "token_bloat":
        params = {"multiplier": round(float(rng.uniform(4, 10)), 1)}

    return Anomaly(kind, start, end, team.name, feature.name, params)


def random_schedule(
    start: datetime, end: datetime, count: int, rng: np.random.Generator
) -> list[Anomaly]:
    """Spread ``count`` anomalies over the window, cycling through every type."""
    total_min = int((end - start).total_seconds() // 60)
    if count <= 0 or total_min < 60:
        return []
    anomalies = []
    for i in range(count):
        kind = ANOMALY_TYPES[i % len(ANOMALY_TYPES)]
        offset = int(rng.integers(0, max(1, total_min - 60)))
        anomalies.append(create_anomaly(kind, start + timedelta(minutes=offset), rng))
    return sorted(anomalies, key=lambda a: a.start)


class AnomalyEngine:
    """Applies active anomalies to normal events and emits the extra events they cause."""

    def __init__(self, anomalies: list[Anomaly], rng: np.random.Generator):
        self.anomalies = list(anomalies)
        self.rng = rng
        self._runaway_step: dict[str, int] = {}

    def add(self, anomaly: Anomaly) -> None:
        self.anomalies.append(anomaly)

    def active(self, ts: datetime) -> list[Anomaly]:
        return [a for a in self.anomalies if a.active(ts)]

    def apply(self, event: dict[str, Any], ts: datetime) -> dict[str, Any]:
        """Modify a normal event in place if an anomaly targets its team/feature."""
        for a in self.active(ts):
            if a.type == "runaway_agent" or (a.team, a.feature) != (event["team"], event["feature"]):
                continue
            if a.type == "model_switch":
                event["model"] = a.params["to_model"]
            elif a.type == "error_spike":
                if self.rng.random() < a.params["error_rate"]:
                    event["status"] = "error"
                else:
                    continue
            elif a.type == "token_bloat":
                event["input_tokens"] = int(event["input_tokens"] * a.params["multiplier"])
            event["injected_anomaly"] = a.type
            finalize(event, self.rng)
        return event

    def extra_events(
        self, factory: EventFactory, window_start: datetime, seconds: float
    ) -> list[dict[str, Any]]:
        """Extra calls fired by runaway agents during [window_start, window_start + seconds)."""
        events = []
        for a in self.active(window_start):
            if a.type != "runaway_agent":
                continue
            team = config.TEAMS_BY_NAME[a.team]
            feature = next(f for f in team.features if f.name == a.feature)
            n = self.rng.poisson(a.params["calls_per_minute"] * seconds / 60)
            for _ in range(n):
                ts = window_start + timedelta(seconds=float(self.rng.uniform(0, seconds)))
                e = factory.make_event(ts, team, feature)
                step = self._runaway_step.get(a.params["run_id"], 0) + 1
                self._runaway_step[a.params["run_id"]] = step
                e["model"] = a.params["model"]
                e["agent_run_id"] = a.params["run_id"]
                e["user_id"] = f"{a.team}_agent_bot"
                e["status"] = "success"
                # Context grows every step as the agent re-sends its history,
                # until the agent framework truncates it (every 50 steps).
                e["input_tokens"] = feature.input_tokens_median + (step % 50) * 300
                e["injected_anomaly"] = "runaway_agent"
                finalize(e, self.rng)
                events.append(e)
        return events


def save_ground_truth(anomalies: list[Anomaly], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump([a.to_dict() for a in anomalies], f, indent=2)
