# Ingestion — streaming events into DuckDB

Moves events from **Kafka** (or a landing folder) into the `raw_llm_usage` table in DuckDB.

```
generator ──▶ Kafka topic "llm_usage" ──▶ KafkaIngestor ──▶ DuckDB raw_llm_usage
          └─▶ data/landing/*.jsonl    ──▶ LandingIngestor ─┘
backfill file ───────────────────────────▶ bulk load ───────┘
```

Setup guide: [docs/KAFKA_SETUP.md](../docs/KAFKA_SETUP.md)

## Commands

| Command | What it does |
|---|---|
| `python -m ingestion ping` | Test the Kafka connection and list topics |
| `python -m ingestion create-topic` | Create the `llm_usage` topic (2 partitions) |
| `python -m ingestion consume` | Stream Kafka → DuckDB until Ctrl+C |
| `python -m ingestion consume --source landing` | Stream landing folder → DuckDB |
| `python -m ingestion load <file.jsonl>` | Bulk-load a backfill file |
| `python -m ingestion stats` | Rows, time range, total cost, rows per source |

## Design decisions

**At-least-once delivery + idempotent writes.** Offsets are committed only *after* a batch is stored in DuckDB. If the consumer crashes in between, Kafka redelivers the batch, and `INSERT OR IGNORE` on the `event_id` primary key skips rows already stored. The result is effectively exactly-once, without distributed transactions.

**Micro-batching.** The consumer pulls up to 1,000 messages or waits 2 seconds, whichever comes first, then loads the batch through DuckDB's bulk JSON reader. That's about 40× faster than row-by-row inserts (≈1,200 rows in 0.1 s).

**Partitioning by team.** The producer keys messages by `team`, so each team's events stay ordered within a partition and load spreads across partitions.

**Dead-letter queue.** Invalid records (bad JSON, missing fields, wrong types, negative tokens) are written to `data/dead_letter/` with the reason and origin, instead of crashing the stream or being silently dropped.

**Short write locks.** DuckDB allows one writing process at a time, so the consumer opens the database only while writing each batch, and retries briefly if another process holds the lock. That lets dbt and the API use the same file in later phases.

**Lineage columns.** Every row records `_source` (`kafka`, `landing`, `file`), `_kafka_partition`, `_kafka_offset` and `_ingested_at`, so any row can be traced back to its origin.

**Offline fallback.** The landing-folder mode writes atomic micro-batch files (`.tmp` → rename), the same pattern as cloud "auto loader" ingestion, so the pipeline still runs without a broker.

## Raw table

| Column | Type | Notes |
|---|---|---|
| event_id | VARCHAR (PK) | Deduplication key |
| event_timestamp | TIMESTAMP | UTC |
| team, feature, user_id, model, request_type, status | VARCHAR | |
| input_tokens, output_tokens, latency_ms | INTEGER | |
| cost_usd | DOUBLE | |
| agent_run_id | VARCHAR | Agent features only |
| injected_anomaly | VARCHAR | Ground-truth label, for evaluation only |
| _source, _kafka_partition, _kafka_offset, _ingested_at | — | Lineage |

## Tests

```bash
pytest -q                                        # unit tests with an in-memory fake broker
KAFKA_INTEGRATION=1 pytest -m kafka              # real broker (runs in GitHub Actions on Redpanda)
```
