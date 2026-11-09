# Generator — synthetic LLM usage events

Produces realistic LLM API usage events for a fictional company with 6 teams, 16 features and 5 models, including **injected incidents** for the observatory to detect.

## Usage

```bash
# 7 days of history → data/raw/events.jsonl (+ anomalies_ground_truth.json)
python -m generator backfill --days 7

# Live stream to the terminal, ~20 events/sec (Ctrl+C to stop)
python -m generator live

# Short live demo where an anomaly starts almost immediately
python -m generator live --rate 5 --duration 60 --anomaly-chance 1
```

| Option | Mode | Default | Meaning |
|---|---|---|---|
| `--days` | backfill | 7 | Days of history |
| `--rate` | both | 0.5 / 20 | Events per second (backfill rate is scaled by time-of-day) |
| `--anomalies` | backfill | 8 | Incidents to inject (cycles through all 4 types) |
| `--anomaly-chance` | live | 0.03 | Chance per minute that a new incident starts |
| `--seed` | both | 42 / random | Same seed → identical data |
| `--out` | both | file / stdout | Output path, or `-` for stdout |
| `--flat` / `--seasonality` | backfill / live | — | Toggle day/week traffic patterns |

Makefile shortcuts: `make generate` and `make stream`.

## Event schema

```json
{
  "event_id": "348deb11-9fab-417f-a74b-6c26b37ea501",
  "timestamp": "2026-11-06T14:10:00.189Z",
  "team": "engineering",
  "feature": "coding_agent",
  "user_id": "engineering_user_004",
  "model": "pro-large",
  "request_type": "chat",
  "input_tokens": 9120,
  "output_tokens": 870,
  "latency_ms": 11540,
  "status": "success",
  "cost_usd": 0.04041,
  "agent_run_id": "0b6f…",
  "injected_anomaly": null
}
```

| Field | Notes |
|---|---|
| `status` | `success` (~98%), `error`, `rate_limited`. Failed calls have 0 output tokens and cost 0 |
| `request_type` | `chat` or `embedding` (embeddings have no output tokens) |
| `agent_run_id` | Set only for agent features; groups the multi-step calls of one agent run |
| `injected_anomaly` | **Ground-truth label** for evaluating detection in Phase 4. Detection code must never use it |

## Models (fictional, realistic price tiers)

| Model | Input $/1M | Output $/1M |
|---|---|---|
| frontier-xl | 15.00 | 75.00 |
| pro-large | 3.00 | 15.00 |
| standard-medium | 0.80 | 4.00 |
| fast-small | 0.15 | 0.60 |
| embed-v2 | 0.02 | — |

## Realism

- **Seasonality:** traffic peaks during working hours (UTC) and drops ~65% at weekends.
- **Token counts:** lognormal around each feature's median, like real prompts.
- **Agents:** agent features group calls into runs of 3–15 steps.
- **Latency:** model overhead + time per output token + prompt size, with jitter.

## Injected anomalies

| Type | What happens | Real-world cause |
|---|---|---|
| `runaway_agent` | One agent run fires 80–250 calls/min for 15–60 min, context growing each step | Agent stuck in a tool-call loop |
| `model_switch` | A feature moves to `frontier-xl` for 2–6 hours | Config change or "just try the best model" |
| `error_spike` | 35–70% of a feature's calls fail for 15–45 min | Bad deploy or provider outage |
| `token_bloat` | Prompts become 4–10× larger for 1–3 hours | Context stuffing / RAG retrieving too much |

In backfill mode the schedule is saved to `anomalies_ground_truth.json` next to the events file.

## Tests

```bash
pytest -q
```
