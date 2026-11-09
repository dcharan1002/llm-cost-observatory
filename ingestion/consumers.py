"""Streaming consumers that move events into DuckDB.

* KafkaIngestor   – reads the Kafka topic in micro-batches, commits offsets only
                    after the batch is safely in DuckDB (at-least-once).
* LandingIngestor – watches the landing folder for new JSONL files (offline mode).

Invalid records go to a dead-letter JSONL file instead of stopping the stream.
"""

from __future__ import annotations

import json
import shutil
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common.settings import Settings, kafka_config
from ingestion.validation import InvalidEvent, parse_event
from ingestion.warehouse import insert_events


def _log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", file=sys.stderr, flush=True)


class _StopFlag:
    """Lets Ctrl+C finish the current batch cleanly instead of killing it mid-write."""

    def __init__(self) -> None:
        self.stop = False
        try:
            signal.signal(signal.SIGINT, self._handle)
            signal.signal(signal.SIGTERM, self._handle)
        except ValueError:  # not in main thread (e.g. tests)
            pass

    def _handle(self, *_):
        if self.stop:  # second Ctrl+C: exit immediately
            raise KeyboardInterrupt
        self.stop = True
        _log("⏹  stopping after current batch…")


class DeadLetter:
    def __init__(self, directory: str):
        self.dir = Path(directory)
        self.count = 0

    def write(self, raw: bytes | str, reason: str, origin: str) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        record = {"failed_at": datetime.now(UTC).isoformat(), "origin": origin, "reason": reason, "raw": raw}
        with open(self.dir / f"dead_letter_{datetime.now(UTC):%Y%m%d}.jsonl", "a") as f:
            f.write(json.dumps(record) + "\n")
        self.count += 1


class _Progress:
    def __init__(self, every_sec: float = 10.0):
        self.every = every_sec
        self.last = time.monotonic()
        self.received = self.inserted = self.duplicates = 0

    def add(self, received: int, inserted: int) -> None:
        self.received += received
        self.inserted += inserted
        self.duplicates += received - inserted

    def maybe_report(self, extra: str = "", force: bool = False) -> None:
        if force or time.monotonic() - self.last >= self.every:
            self.last = time.monotonic()
            _log(f"📥 {self.inserted:,} inserted · {self.duplicates:,} duplicates skipped {extra}")


# ---------------------------------------------------------------- Kafka
class KafkaIngestor:
    def __init__(self, settings: Settings, batch_size: int = 1000, batch_timeout: float = 2.0, consumer=None):
        self.s = settings
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        if consumer is None:
            from confluent_kafka import Consumer

            consumer = Consumer(kafka_config(settings) | {
                "group.id": settings.kafka_group_id,
                "auto.offset.reset": "earliest",   # new consumer group starts from the beginning
                "enable.auto.commit": False,       # commit manually after writing to DuckDB
            })
        self.consumer = consumer
        self.dead = DeadLetter(settings.dead_letter_dir)
        self.progress = _Progress()

    def process_batch(self, messages: list[Any]) -> int:
        """Validate and store one batch. Returns rows inserted."""
        events, offsets = [], []
        for m in messages:
            if m.error():
                from confluent_kafka import KafkaError

                if m.error().code() != KafkaError._PARTITION_EOF:
                    _log(f"⚠️  Kafka error: {m.error()}")
                continue
            try:
                events.append(parse_event(m.value()))
                offsets.append((m.partition(), m.offset()))
            except InvalidEvent as exc:
                self.dead.write(m.value(), str(exc), f"kafka:{m.topic()}/{m.partition()}@{m.offset()}")
        inserted = insert_events(self.s.duckdb_path, events, "kafka", offsets)
        self.progress.add(len(events), inserted)
        return inserted

    def run(self, max_batches: int | None = None, idle_exit_sec: float | None = None) -> None:
        """Consume until Ctrl+C, ``max_batches`` batches, or ``idle_exit_sec`` with no messages."""
        flag = _StopFlag()
        self.consumer.subscribe([self.s.kafka_topic])
        _log(f"▶ consuming '{self.s.kafka_topic}' from {self.s.kafka_bootstrap} → {self.s.duckdb_path}")
        batches, idle_since = 0, time.monotonic()
        try:
            while not flag.stop:
                msgs = self.consumer.consume(num_messages=self.batch_size, timeout=self.batch_timeout)
                if msgs:
                    idle_since = time.monotonic()
                    self.process_batch(msgs)
                    # Commit only after the batch is safely stored.
                    self.consumer.commit(asynchronous=False)
                    batches += 1
                elif idle_exit_sec is not None and time.monotonic() - idle_since >= idle_exit_sec:
                    break
                self.progress.maybe_report(f"· {self.dead.count} dead-lettered")
                if max_batches is not None and batches >= max_batches:
                    break
        finally:
            self.consumer.close()
            self.progress.maybe_report(f"· {self.dead.count} dead-lettered", force=True)


# ---------------------------------------------------------------- Landing folder
class LandingIngestor:
    def __init__(self, settings: Settings, poll_interval: float = 2.0):
        self.s = settings
        self.poll_interval = poll_interval
        self.landing = Path(settings.landing_dir)
        self.archive = Path(settings.archive_dir)
        self.dead = DeadLetter(settings.dead_letter_dir)
        self.progress = _Progress()

    def process_file(self, path: Path) -> int:
        events = []
        with open(path, encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    events.append(parse_event(line))
                except InvalidEvent as exc:
                    self.dead.write(line.rstrip("\n"), str(exc), f"file:{path.name}:{i}")
        inserted = insert_events(self.s.duckdb_path, events, "landing")
        self.progress.add(len(events), inserted)
        # Archive by date so the landing folder only holds unprocessed files.
        dest = self.archive / datetime.now(UTC).strftime("%Y-%m-%d")
        dest.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), dest / path.name)
        return inserted

    def run_once(self) -> int:
        """Process every complete file currently in the landing folder."""
        self.landing.mkdir(parents=True, exist_ok=True)
        total = 0
        for path in sorted(self.landing.glob("*.jsonl")):  # .tmp files are still being written
            total += self.process_file(path)
        return total

    def run(self, once: bool = False) -> None:
        flag = _StopFlag()
        _log(f"▶ watching {self.landing} → {self.s.duckdb_path}")
        while True:
            self.run_once()
            if once or flag.stop:
                break
            time.sleep(self.poll_interval)
        self.progress.maybe_report(f"· {self.dead.count} dead-lettered", force=True)
