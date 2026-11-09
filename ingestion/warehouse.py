"""DuckDB warehouse: the raw events table and idempotent loading.

The database file is opened only for the duration of each write, because DuckDB
allows one writing process at a time. This keeps it usable by dbt and the API.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

RAW_TABLE = "raw_llm_usage"

DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    event_id          VARCHAR PRIMARY KEY,
    event_timestamp   TIMESTAMP NOT NULL,      -- UTC
    team              VARCHAR NOT NULL,
    feature           VARCHAR NOT NULL,
    user_id           VARCHAR NOT NULL,
    model             VARCHAR NOT NULL,
    request_type      VARCHAR NOT NULL,
    input_tokens      INTEGER NOT NULL,
    output_tokens     INTEGER NOT NULL,
    latency_ms        INTEGER NOT NULL,
    status            VARCHAR NOT NULL,
    cost_usd          DOUBLE  NOT NULL,
    agent_run_id      VARCHAR,
    injected_anomaly  VARCHAR,                 -- ground-truth label, never used for detection
    _source           VARCHAR NOT NULL,        -- kafka | landing | file
    _kafka_partition  INTEGER,
    _kafka_offset     BIGINT,
    _ingested_at      TIMESTAMP NOT NULL       -- UTC
)
"""

_JSON_COLUMNS = """{
    'event_id': 'VARCHAR', 'timestamp': 'VARCHAR', 'team': 'VARCHAR',
    'feature': 'VARCHAR', 'user_id': 'VARCHAR', 'model': 'VARCHAR',
    'request_type': 'VARCHAR', 'input_tokens': 'INTEGER', 'output_tokens': 'INTEGER',
    'latency_ms': 'INTEGER', 'status': 'VARCHAR', 'cost_usd': 'DOUBLE',
    'agent_run_id': 'VARCHAR', 'injected_anomaly': 'VARCHAR',
    '_kafka_partition': 'INTEGER', '_kafka_offset': 'BIGINT'
}"""

_LOAD_SQL = f"""
INSERT OR IGNORE INTO {RAW_TABLE}
SELECT
    event_id,
    CAST(strptime(timestamp, ['%Y-%m-%dT%H:%M:%S.%gZ', '%Y-%m-%dT%H:%M:%SZ']) AS TIMESTAMP),
    team, feature, user_id, model, request_type,
    input_tokens, output_tokens, latency_ms, status, cost_usd,
    agent_run_id, injected_anomaly,
    ? AS _source, _kafka_partition, _kafka_offset, ? AS _ingested_at
FROM read_json(?, format = 'newline_delimited', columns = {_JSON_COLUMNS})
"""


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(UTC).replace(tzinfo=None)


@contextmanager
def connect(path: str, read_only: bool = False, retries: int = 20):
    """Open DuckDB, retrying briefly if another process holds the write lock."""
    if not read_only:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=read_only)
            break
        except duckdb.IOException:
            if attempt == retries - 1:
                raise
            time.sleep(0.25)
    try:
        if not read_only:
            con.execute(DDL)
        yield con
    finally:
        con.close()


def _load(path: str, jsonl_path: str, source: str) -> int:
    with connect(path) as con:
        before = con.execute(f"SELECT count(*) FROM {RAW_TABLE}").fetchone()[0]
        con.execute(_LOAD_SQL, [source, _utcnow(), str(jsonl_path)])
        after = con.execute(f"SELECT count(*) FROM {RAW_TABLE}").fetchone()[0]
    return after - before


def insert_events(
    path: str,
    events: Iterable[dict[str, Any]],
    source: str,
    offsets: Iterable[tuple[int | None, int | None]] | None = None,
) -> int:
    """Insert validated events. Duplicates (same event_id) are ignored.

    Returns the number of new rows. Because inserts are idempotent, replaying a
    Kafka batch after a crash never creates duplicates (at-least-once delivery
    + idempotent writes = effectively exactly-once).

    The batch is staged as a temporary JSONL file and loaded with DuckDB's bulk
    JSON reader, which is ~100x faster than row-by-row inserts.
    """
    events = list(events)
    if not events:
        return 0
    offsets = list(offsets) if offsets is not None else [(None, None)] * len(events)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(suffix=".jsonl", dir=Path(path).parent)
    try:
        with os.fdopen(fd, "w") as f:
            for e, (part, off) in zip(events, offsets, strict=True):
                f.write(json.dumps(e | {"_kafka_partition": part, "_kafka_offset": off}) + "\n")
        return _load(path, tmp, source)
    finally:
        os.remove(tmp)


def load_jsonl_file(path: str, jsonl_path: str, source: str = "file") -> int:
    """Bulk-load a JSONL file of events. Returns the number of new rows."""
    return _load(path, jsonl_path, source)


def stats(path: str) -> dict[str, Any]:
    """Quick health summary of the raw table."""
    if not Path(path).exists():
        return {"rows": 0}
    with connect(path, read_only=True) as con:
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [RAW_TABLE]
        ).fetchone()[0]
        if not exists:
            return {"rows": 0}
        rows, first, last, cost, last_ingest = con.execute(
            f"SELECT count(*), min(event_timestamp), max(event_timestamp), sum(cost_usd), max(_ingested_at) "
            f"FROM {RAW_TABLE}"
        ).fetchone()
        by_source = dict(con.execute(f"SELECT _source, count(*) FROM {RAW_TABLE} GROUP BY 1 ORDER BY 1").fetchall())
    return {
        "rows": rows, "first_event": first, "last_event": last,
        "total_cost_usd": round(cost or 0, 2), "last_ingested_at": last_ingest, "by_source": by_source,
    }
