"""Where generated events go.

* JsonlSink   – one JSON line per event to a file or stdout
* KafkaSink   – publishes to a Kafka topic (Aiven, Redpanda, any Kafka)
* LandingSink – writes small micro-batch files into a landing folder (offline fallback)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from common.settings import Settings, kafka_config


def _dumps(event: dict[str, Any]) -> str:
    return json.dumps(event, separators=(",", ":"))


class Sink(Protocol):
    def write(self, event: dict[str, Any]) -> None: ...
    def close(self) -> None: ...


class JsonlSink:
    """Writes one JSON object per line to a file, or to stdout when path is '-'."""

    def __init__(self, path: str = "-"):
        if path == "-":
            self._f = sys.stdout
            self._owned = False
        else:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._f = open(path, "w")
            self._owned = True

    def write(self, event: dict[str, Any]) -> None:
        self._f.write(_dumps(event) + "\n")

    def close(self) -> None:
        self._f.flush()
        if self._owned:
            self._f.close()


class KafkaSink:
    """Publishes events to Kafka, keyed by team so each team's events stay ordered."""

    def __init__(self, settings: Settings, producer=None):
        if producer is None:
            from confluent_kafka import Producer

            conf = kafka_config(settings) | {
                "acks": "all",                 # wait for all replicas
                "enable.idempotence": True,    # no duplicates on retry
                "linger.ms": 100,              # small batches = fewer requests
                "compression.type": "lz4",     # ~3-4x smaller on the wire
            }
            producer = Producer(conf)
        self._producer = producer
        self._topic = settings.kafka_topic
        self.delivered = 0
        self.failed = 0
        self.last_error: str | None = None

    def _on_delivery(self, err, _msg) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            self.last_error = str(err)

    def write(self, event: dict[str, Any]) -> None:
        payload = _dumps(event).encode()
        while True:
            try:
                self._producer.produce(
                    self._topic, key=event["team"].encode(), value=payload, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                # Local queue is full: wait for deliveries, then retry.
                self._producer.poll(1.0)
        self._producer.poll(0)

    def close(self) -> None:
        remaining = self._producer.flush(30)
        print(
            f"📨 Kafka: {self.delivered:,} delivered, {self.failed:,} failed, {remaining:,} undelivered",
            file=sys.stderr,
        )
        if self.last_error:
            print(f"   last error: {self.last_error}", file=sys.stderr)


class LandingSink:
    """Buffers events and writes them as small JSONL files into a landing folder.

    Each file is written to a .tmp name first and then renamed, so the consumer
    never reads a half-written file.
    """

    def __init__(self, landing_dir: str, max_events: int = 500, max_seconds: float = 5.0):
        self._dir = Path(landing_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._max_events = max_events
        self._max_seconds = max_seconds
        self._buffer: list[str] = []
        self._opened = time.monotonic()
        self._seq = 0
        self.files_written = 0

    def write(self, event: dict[str, Any]) -> None:
        self._buffer.append(_dumps(event))
        if len(self._buffer) >= self._max_events or time.monotonic() - self._opened >= self._max_seconds:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        self._seq += 1
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
        final = self._dir / f"events_{stamp}_{os.getpid()}_{self._seq:06d}.jsonl"
        tmp = final.with_suffix(".tmp")
        tmp.write_text("\n".join(self._buffer) + "\n")
        os.replace(tmp, final)
        self.files_written += 1
        self._buffer.clear()
        self._opened = time.monotonic()

    def close(self) -> None:
        self.flush()
        print(f"📂 landing: {self.files_written:,} files written to {self._dir}", file=sys.stderr)


def make_sink(kind: str, settings: Settings, out: str = "-") -> Sink:
    if kind == "kafka":
        return KafkaSink(settings)
    if kind == "landing":
        return LandingSink(settings.landing_dir)
    if kind == "jsonl":
        return JsonlSink(out)
    raise ValueError(f"unknown sink '{kind}' (use jsonl, kafka or landing)")
