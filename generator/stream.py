"""Event streams: historical backfill and real-time live mode."""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np

from generator.anomalies import ANOMALY_TYPES, Anomaly, AnomalyEngine, create_anomaly
from generator.events import EventFactory
from generator.traffic import traffic_multiplier

Event = dict[str, Any]


def _window_events(
    factory: EventFactory,
    engine: AnomalyEngine,
    rng: np.random.Generator,
    window_start: datetime,
    seconds: float,
    rate_per_sec: float,
    seasonality: bool,
) -> list[Event]:
    """All events (normal + anomalous) in one time window, sorted by timestamp."""
    mult = traffic_multiplier(window_start) if seasonality else 1.0
    n = rng.poisson(rate_per_sec * seconds * mult)
    events = []
    for _ in range(n):
        ts = window_start + timedelta(seconds=float(rng.uniform(0, seconds)))
        events.append(engine.apply(factory.make_event(ts), ts))
    events.extend(engine.extra_events(factory, window_start, seconds))
    events.sort(key=lambda e: e["timestamp"])
    return events


def backfill(
    start: datetime,
    end: datetime,
    rate_per_sec: float,
    anomalies: list[Anomaly],
    seed: int | None = None,
    seasonality: bool = True,
) -> Iterator[Event]:
    """Generate historical events between ``start`` and ``end``, minute by minute."""
    rng = np.random.default_rng(seed)
    factory = EventFactory(rng)
    engine = AnomalyEngine(anomalies, rng)
    t = start
    while t < end:
        yield from _window_events(factory, engine, rng, t, 60, rate_per_sec, seasonality)
        t += timedelta(minutes=1)


def live(
    rate_per_sec: float,
    anomaly_chance_per_min: float = 0.03,
    seed: int | None = None,
    seasonality: bool = False,
    duration_sec: float | None = None,
    on_anomaly=None,
) -> Iterator[Event]:
    """Emit events in real time, one 1-second window at a time.

    A new anomaly starts with probability ``anomaly_chance_per_min`` each minute
    (when none is active), so a live demo shows an incident every ~30 minutes.
    """
    rng = np.random.default_rng(seed)
    factory = EventFactory(rng)
    engine = AnomalyEngine([], rng)
    started = time.monotonic()
    next_anomaly_check = 0.0
    kinds_cycle = 0

    while duration_sec is None or time.monotonic() - started < duration_sec:
        tick = time.monotonic()
        now = datetime.now(UTC)

        if tick >= next_anomaly_check:
            next_anomaly_check = tick + 60
            if not engine.active(now) and rng.random() < anomaly_chance_per_min:
                a = create_anomaly(ANOMALY_TYPES[kinds_cycle % len(ANOMALY_TYPES)], now, rng)
                kinds_cycle += 1
                engine.add(a)
                if on_anomaly:
                    on_anomaly(a)

        yield from _window_events(factory, engine, rng, now, 1.0, rate_per_sec, seasonality)
        time.sleep(max(0.0, 1.0 - (time.monotonic() - tick)))
