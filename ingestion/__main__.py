"""Ingestion command-line interface.

    python -m ingestion ping                      # test the Kafka connection
    python -m ingestion create-topic              # create the llm_usage topic
    python -m ingestion consume                   # Kafka → DuckDB (Ctrl+C to stop)
    python -m ingestion consume --source landing  # landing folder → DuckDB
    python -m ingestion load data/raw/events.jsonl  # bulk-load a backfill file
    python -m ingestion stats                     # what's in the warehouse
"""

from __future__ import annotations

import argparse
import sys

from common.settings import get_settings, kafka_config
from ingestion.consumers import KafkaIngestor, LandingIngestor
from ingestion.warehouse import load_jsonl_file, stats


def cmd_ping(_args) -> None:
    from confluent_kafka.admin import AdminClient

    s = get_settings()
    print(f"Connecting to {s.kafka_bootstrap} ({s.kafka_security_protocol})…")
    md = AdminClient(kafka_config(s)).list_topics(timeout=15)
    print(f"✅ connected · {len(md.brokers)} broker(s)")
    topics = sorted(t for t in md.topics if not t.startswith("_"))
    print(f"   topics: {', '.join(topics) or '(none yet)'}")
    if s.kafka_topic not in md.topics:
        print(f"   ⚠️  topic '{s.kafka_topic}' does not exist yet → run: python -m ingestion create-topic")


def cmd_create_topic(args) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    s = get_settings()
    admin = AdminClient(kafka_config(s))
    fut = admin.create_topics([NewTopic(s.kafka_topic, num_partitions=args.partitions, replication_factor=-1)])
    try:
        fut[s.kafka_topic].result(timeout=30)
        print(f"✅ created topic '{s.kafka_topic}' with {args.partitions} partitions")
    except Exception as exc:  # noqa: BLE001 - surface any broker error to the user
        if "TOPIC_ALREADY_EXISTS" in str(exc):
            print(f"ℹ️  topic '{s.kafka_topic}' already exists")
        else:
            print(f"❌ could not create topic: {exc}\n   You can also create it in the Aiven console → Topics.")
            sys.exit(1)


def cmd_consume(args) -> None:
    s = get_settings()
    if args.source == "kafka":
        KafkaIngestor(s, batch_size=args.batch_size).run(idle_exit_sec=args.idle_exit)
    else:
        LandingIngestor(s).run(once=args.once)


def cmd_load(args) -> None:
    s = get_settings()
    n = load_jsonl_file(s.duckdb_path, args.path)
    print(f"✅ loaded {n:,} new rows from {args.path} into {s.duckdb_path}")


def cmd_stats(_args) -> None:
    st = stats(get_settings().duckdb_path)
    if not st["rows"]:
        print("Warehouse is empty. Load data with: python -m ingestion load data/raw/events.jsonl")
        return
    print(f"rows:            {st['rows']:,}")
    print(f"events from:     {st['first_event']:%Y-%m-%d %H:%M} → {st['last_event']:%Y-%m-%d %H:%M} UTC")
    print(f"total cost:      ${st['total_cost_usd']:,.2f}")
    print(f"last ingested:   {st['last_ingested_at']:%Y-%m-%d %H:%M:%S} UTC")
    print("by source:       " + ", ".join(f"{k}={v:,}" for k, v in st["by_source"].items()))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="ingestion", description="Move LLM usage events into DuckDB")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("ping", help="test the Kafka connection").set_defaults(func=cmd_ping)

    t = sub.add_parser("create-topic", help="create the Kafka topic")
    t.add_argument("--partitions", type=int, default=2)
    t.set_defaults(func=cmd_create_topic)

    c = sub.add_parser("consume", help="stream events into DuckDB")
    c.add_argument("--source", choices=["kafka", "landing"], default="kafka")
    c.add_argument("--batch-size", type=int, default=1000)
    c.add_argument("--once", action="store_true", help="landing: process current files and exit")
    c.add_argument("--idle-exit", type=float, default=None, help="kafka: exit after N idle seconds")
    c.set_defaults(func=cmd_consume)

    ld = sub.add_parser("load", help="bulk-load a JSONL file (e.g. a backfill)")
    ld.add_argument("path")
    ld.set_defaults(func=cmd_load)

    sub.add_parser("stats", help="summarize the warehouse").set_defaults(func=cmd_stats)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
