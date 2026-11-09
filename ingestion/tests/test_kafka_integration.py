"""End-to-end test against a real Kafka broker.

Skipped unless KAFKA_INTEGRATION=1. GitHub Actions runs it against Redpanda;
locally you can run it against Aiven:
    $env:KAFKA_INTEGRATION=1; python -m pytest -m kafka      (PowerShell)
"""

import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from common.settings import get_settings, kafka_config
from generator.sinks import KafkaSink
from generator.stream import backfill
from ingestion.consumers import KafkaIngestor

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(os.getenv("KAFKA_INTEGRATION") != "1", reason="set KAFKA_INTEGRATION=1 to run"),
]


def test_produce_and_consume_round_trip(tmp_path):
    from confluent_kafka.admin import AdminClient, NewTopic

    s = replace(
        get_settings(),
        kafka_topic=f"it_{uuid.uuid4().hex[:8]}",
        kafka_group_id=f"it_{uuid.uuid4().hex[:8]}",
        duckdb_path=str(tmp_path / "wh.duckdb"),
        dead_letter_dir=str(tmp_path / "dead"),
    )
    admin = AdminClient(kafka_config(s))
    admin.create_topics([NewTopic(s.kafka_topic, num_partitions=2, replication_factor=-1)])[s.kafka_topic].result(30)
    try:
        start = datetime(2026, 11, 2, 9, tzinfo=UTC)
        events = list(backfill(start, start + timedelta(minutes=5), 2.0, [], seed=3))

        sink = KafkaSink(s)
        for e in events:
            sink.write(e)
        sink.close()
        assert sink.delivered == len(events) and sink.failed == 0

        KafkaIngestor(s, batch_size=200).run(idle_exit_sec=10)

        with duckdb.connect(s.duckdb_path, read_only=True) as con:
            rows, parts = con.execute(
                "SELECT count(*), count(DISTINCT _kafka_partition) FROM raw_llm_usage"
            ).fetchone()
        assert rows == len(events)
        assert parts == 2  # keyed by team → spread over both partitions
    finally:
        admin.delete_topics([s.kafka_topic])
