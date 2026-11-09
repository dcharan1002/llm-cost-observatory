"""Command-line interface.

Examples
--------
Backfill 7 days of history (plus ground-truth anomalies file):
    python -m generator backfill --days 7 --out data/raw/events.jsonl

Stream live events to the terminal for 30 seconds:
    python -m generator live --rate 5 --duration 30

Stream live events to Kafka (settings from .env):
    python -m generator live --sink kafka
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np

from common.settings import get_settings
from generator.anomalies import random_schedule, save_ground_truth
from generator.sinks import JsonlSink, make_sink
from generator.stream import backfill, live


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


class Stats:
    def __init__(self) -> None:
        self.count = 0
        self.cost = 0.0
        self.anomalous = Counter()

    def add(self, e: dict) -> None:
        self.count += 1
        self.cost += e["cost_usd"]
        if e["injected_anomaly"]:
            self.anomalous[e["injected_anomaly"]] += 1

    def report(self) -> None:
        _log(f"✅ {self.count:,} events · total cost ${self.cost:,.2f}")
        for kind, n in self.anomalous.most_common():
            _log(f"   ↳ {n:,} events affected by {kind}")


def cmd_backfill(args: argparse.Namespace) -> None:
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    start = end - timedelta(days=args.days)
    rng = np.random.default_rng(args.seed)
    anomalies = random_schedule(start, end, args.anomalies, rng)

    if args.out != "-":
        truth_path = str(Path(args.out).with_name("anomalies_ground_truth.json"))
        save_ground_truth(anomalies, truth_path)
        _log(f"📝 {len(anomalies)} injected anomalies → {truth_path}")
    for a in anomalies:
        _log(f"   • {a.type:<14} {a.team}/{a.feature}  {a.start:%a %H:%M} → {a.end:%H:%M} UTC")

    sink, stats = JsonlSink(args.out), Stats()
    seed = None if args.seed is None else args.seed + 1
    try:
        for e in backfill(start, end, args.rate, anomalies, seed, not args.flat):
            sink.write(e)
            stats.add(e)
    finally:
        sink.close()
    stats.report()


def cmd_live(args: argparse.Namespace) -> None:
    def announce(a):
        _log(f"🚨 anomaly started: {a.type} on {a.team}/{a.feature} until {a.end:%H:%M:%S} UTC")

    settings = get_settings()
    kind = args.sink or settings.sink
    sink, stats = make_sink(kind, settings, args.out), Stats()
    show_progress = not (kind == "jsonl" and args.out == "-")
    last_report = time.monotonic()
    _log(f"▶ streaming ~{args.rate} events/sec to {kind} (Ctrl+C to stop)")
    try:
        for e in live(args.rate, args.anomaly_chance, args.seed, args.seasonality, args.duration, announce):
            sink.write(e)
            stats.add(e)
            if show_progress and time.monotonic() - last_report >= 10:
                last_report = time.monotonic()
                _log(f"   … {stats.count:,} events sent · ${stats.cost:,.2f}")
    except KeyboardInterrupt:
        pass
    finally:
        sink.close()
    stats.report()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="generator", description="Synthetic LLM usage event generator")
    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser("backfill", help="generate historical events")
    b.add_argument("--days", type=float, default=7, help="days of history (default 7)")
    b.add_argument("--rate", type=float, default=0.5, help="peak events/sec (default 0.5)")
    b.add_argument("--anomalies", type=int, default=8, help="number of injected anomalies (default 8)")
    b.add_argument("--out", default="data/raw/events.jsonl", help="output file, or - for stdout")
    b.add_argument("--seed", type=int, default=42, help="random seed for reproducible data")
    b.add_argument("--flat", action="store_true", help="disable daily/weekly seasonality")
    b.set_defaults(func=cmd_backfill)

    lv = sub.add_parser("live", help="stream events in real time")
    lv.add_argument("--rate", type=float, default=5, help="events/sec (default 5)")
    lv.add_argument("--anomaly-chance", type=float, default=0.03,
                    help="chance per minute an anomaly starts (default 0.03)")
    lv.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    lv.add_argument("--sink", choices=["jsonl", "kafka", "landing"], default=None,
                    help="where to send events (default: SINK in .env, else jsonl)")
    lv.add_argument("--out", default="-", help="jsonl sink: output file, or - for stdout (default)")
    lv.add_argument("--seed", type=int, default=None)
    lv.add_argument("--seasonality", action="store_true", help="apply time-of-day traffic pattern")
    lv.set_defaults(func=cmd_live)
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
