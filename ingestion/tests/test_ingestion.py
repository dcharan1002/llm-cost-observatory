import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from common.settings import get_settings
from generator.sinks import KafkaSink, LandingSink
from generator.stream import backfill
from ingestion.consumers import KafkaIngestor, LandingIngestor
from ingestion.validation import InvalidEvent, parse_event
from ingestion.warehouse import RAW_TABLE, insert_events, load_jsonl_file, stats

START = datetime(2026, 11, 2, 9, 0, tzinfo=UTC)


def _events(minutes=10, seed=1):
    return list(backfill(START, START + timedelta(minutes=minutes), 2.0, [], seed))


@pytest.fixture
def settings(tmp_path):
    return replace(
        get_settings(),
        duckdb_path=str(tmp_path / "wh.duckdb"),
        landing_dir=str(tmp_path / "landing"),
        archive_dir=str(tmp_path / "archive"),
        dead_letter_dir=str(tmp_path / "dead"),
        kafka_topic="llm_usage_test",
    )


def _count(path, where="true"):
    with duckdb.connect(path, read_only=True) as con:
        return con.execute(f"SELECT count(*) FROM {RAW_TABLE} WHERE {where}").fetchone()[0]


# ---------------------------------------------------------------- validation
def test_valid_event_passes():
    e = _events(1)[0]
    assert parse_event(json.dumps(e)) == e


@pytest.mark.parametrize(
    "mutate, reason",
    [
        (lambda e: e.pop("cost_usd"), "missing"),
        (lambda e: e.update(input_tokens="12"), "wrong type"),
        (lambda e: e.update(input_tokens=True), "wrong type"),
        (lambda e: e.update(status="exploded"), "unknown status"),
        (lambda e: e.update(output_tokens=-5), "negative"),
        (lambda e: e.update(timestamp="yesterday"), "bad timestamp"),
    ],
)
def test_invalid_events_rejected(mutate, reason):
    e = _events(1)[0]
    mutate(e)
    with pytest.raises(InvalidEvent, match=reason):
        parse_event(json.dumps(e))


def test_garbage_rejected():
    with pytest.raises(InvalidEvent):
        parse_event(b"\xff not json")
    with pytest.raises(InvalidEvent):
        parse_event("[1, 2, 3]")


# ---------------------------------------------------------------- warehouse
def test_insert_is_idempotent(settings):
    events = _events()
    db = settings.duckdb_path
    assert insert_events(db, events, "kafka") == len(events)
    assert insert_events(db, events, "kafka") == 0
    assert _count(db) == len(events)


def test_insert_keeps_kafka_lineage(settings):
    events = _events(2)
    insert_events(settings.duckdb_path, events, "kafka", [(1, i) for i in range(len(events))])
    with duckdb.connect(settings.duckdb_path, read_only=True) as con:
        part, max_off = con.execute(f"SELECT max(_kafka_partition), max(_kafka_offset) FROM {RAW_TABLE}").fetchone()
    assert (part, max_off) == (1, len(events) - 1)


def test_values_round_trip(settings):
    e = _events(1)[0]
    insert_events(settings.duckdb_path, [e], "kafka")
    with duckdb.connect(settings.duckdb_path, read_only=True) as con:
        row = con.execute(f"SELECT event_id, team, model, input_tokens, cost_usd, event_timestamp FROM {RAW_TABLE}")
        eid, team, model, tokens, cost, ts = row.fetchone()
    assert (eid, team, model, tokens) == (e["event_id"], e["team"], e["model"], e["input_tokens"])
    assert cost == pytest.approx(e["cost_usd"])
    assert ts.strftime("%Y-%m-%dT%H:%M:%S") == e["timestamp"][:19]


def test_bulk_load_file(settings, tmp_path):
    events = _events(30)
    f = tmp_path / "events.jsonl"
    f.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    assert load_jsonl_file(settings.duckdb_path, str(f)) == len(events)
    assert load_jsonl_file(settings.duckdb_path, str(f)) == 0
    st = stats(settings.duckdb_path)
    assert st["rows"] == len(events) and st["by_source"] == {"file": len(events)}


def test_stats_on_missing_db(tmp_path):
    assert stats(str(tmp_path / "nope.duckdb")) == {"rows": 0}


# ---------------------------------------------------------------- landing mode
def test_landing_end_to_end(settings):
    events = _events()
    sink = LandingSink(settings.landing_dir, max_events=50)
    for e in events:
        sink.write(e)
    sink.close()
    assert sink.files_written >= len(events) // 50

    ing = LandingIngestor(settings)
    assert ing.run_once() == len(events)
    assert not list(ing.landing.glob("*.jsonl"))          # landing emptied
    assert len(list(ing.archive.rglob("*.jsonl"))) == sink.files_written  # archived
    assert ing.run_once() == 0                            # nothing left to do


def test_landing_dead_letters_bad_lines(settings):
    good = _events(1)
    landing = LandingSink(settings.landing_dir)
    for e in good:
        landing.write(e)
    landing.close()
    (ing := LandingIngestor(settings)).landing.joinpath("bad.jsonl").write_text('{"oops": 1}\nnot json\n')
    ing.run_once()
    assert _count(settings.duckdb_path) == len(good)
    assert ing.dead.count == 2
    dead = next(ing.dead.dir.glob("*.jsonl")).read_text().splitlines()
    assert all("reason" in json.loads(line) for line in dead)


def test_landing_ignores_partial_files(settings):
    landing = LandingIngestor(settings)
    landing.landing.mkdir(parents=True)
    landing.landing.joinpath("events_x.tmp").write_text("{}")
    assert landing.run_once() == 0
    assert landing.landing.joinpath("events_x.tmp").exists()


# ---------------------------------------------------------------- Kafka (with fakes)
class FakeMessage:
    def __init__(self, value, offset, partition=0, topic="llm_usage_test"):
        self._v, self._o, self._p, self._t = value, offset, partition, topic

    def value(self):
        return self._v

    def offset(self):
        return self._o

    def partition(self):
        return self._p

    def topic(self):
        return self._t

    def error(self):
        return None


class FakeBroker:
    """Minimal in-memory stand-in for a Kafka producer + consumer."""

    def __init__(self):
        self.messages = []
        self.committed = 0
        self._pos = 0
        self.closed = False

    # producer API
    def produce(self, topic, key, value, on_delivery):
        self.messages.append(FakeMessage(value, len(self.messages)))
        on_delivery(None, None)

    def poll(self, _t):
        return 0

    def flush(self, _t):
        return 0

    # consumer API
    def subscribe(self, topics):
        self.topics = topics

    def consume(self, num_messages, timeout):
        batch = self.messages[self._pos:self._pos + num_messages]
        self._pos += len(batch)
        return batch

    def commit(self, asynchronous):
        self.committed = self._pos

    def close(self):
        self.closed = True


def test_kafka_sink_and_ingestor_with_fake_broker(settings):
    broker = FakeBroker()
    sink = KafkaSink(settings, producer=broker)
    events = _events()
    for e in events:
        sink.write(e)
    sink.close()
    assert sink.delivered == len(events)

    broker.messages.append(FakeMessage(b"garbage", len(broker.messages)))
    ing = KafkaIngestor(settings, batch_size=100, consumer=broker)
    ing.run(idle_exit_sec=0)
    assert _count(settings.duckdb_path, "_source = 'kafka'") == len(events)
    assert ing.dead.count == 1
    assert broker.committed == len(broker.messages)  # offsets committed after writes
    assert broker.closed


def test_kafka_replay_creates_no_duplicates(settings):
    broker = FakeBroker()
    sink = KafkaSink(settings, producer=broker)
    for e in _events():
        sink.write(e)
    KafkaIngestor(settings, consumer=broker).run(idle_exit_sec=0)
    first = _count(settings.duckdb_path)
    broker._pos = 0  # simulate a crash before commit → messages redelivered
    KafkaIngestor(settings, consumer=broker).run(idle_exit_sec=0)
    assert _count(settings.duckdb_path) == first
