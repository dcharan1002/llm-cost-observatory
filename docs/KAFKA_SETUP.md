# Phase 2 — Kafka Setup (Aiven free tier, no Docker)

The project streams events through a real **Apache Kafka** cluster hosted on Aiven's free tier. Nothing to install except Python packages.

> Commands are for **PowerShell** on Windows. On macOS/Linux/WSL use `source .venv/bin/activate` and `cp` instead of `copy`.

## 1. Create the Kafka service

1. Sign up at <https://aiven.io/free-kafka> (no credit card).
2. **Create service → Apache Kafka → Free plan.** Pick the cloud region closest to you.
3. Wait until the status shows **Running** (a few minutes).

## 2. Download the connection files

On the service's **Overview / Connection information** page (authentication method: **Client certificate**):

| What Aiven shows | Save it as |
|---|---|
| **Service URI** (e.g. `kafka-xxxx.aivencloud.com:12345`) | copy the text |
| **Access Key** | `certs/service.key` |
| **Access Certificate** | `certs/service.cert` |
| **CA Certificate** | `certs/ca.pem` |

Create a `certs` folder in the project root and put the three files there.

> 🔒 These files are like passwords. `certs/` is in `.gitignore`, so git will never commit them. Never paste them into chat, issues or screenshots.

## 3. Configure `.env`

```powershell
copy .env.example .env    # only if you don't have a .env yet
notepad .env
```

Set:

```ini
SINK=kafka
KAFKA_BOOTSTRAP_SERVERS=kafka-xxxx.aivencloud.com:12345   # your Service URI
KAFKA_SECURITY_PROTOCOL=SSL
```

The certificate paths already default to `certs/ca.pem`, `certs/service.cert` and `certs/service.key`.

## 4. Install packages and test the connection

```powershell
.venv\Scripts\activate
pip install -r requirements.txt
python -m ingestion ping
```

You should see `✅ connected`. Then create the topic:

```powershell
python -m ingestion create-topic
```

If that fails (some free plans restrict it), create it in the Aiven console: **Topics → Add topic → name `llm_usage`, 2 partitions**.

## 5. Run the pipeline

Open **two** PowerShell windows in the project folder (activate `.venv` in each):

**Window 1 — producer**
```powershell
python -m generator live --sink kafka
```

**Window 2 — consumer**
```powershell
python -m ingestion consume
```

Every 10 seconds you'll see progress in both windows. Stop either with **Ctrl+C** (the consumer finishes its current batch first).

Check what's in the warehouse at any time:

```powershell
python -m ingestion stats
```

## 6. Load history (recommended)

Seven days of history makes the dashboards interesting. Load it directly instead of pushing it through Kafka:

```powershell
python -m generator backfill --days 7
python -m ingestion load data/raw/events.jsonl
```

## Troubleshooting

| Problem | Fix |
|---|---|
| `KAFKA_SSL_CA ... not found` | The cert files aren't in `certs/` or have different names |
| `ping` hangs / times out | Service is powered off (free services sleep when idle). Power it on in the Aiven console |
| `SSL handshake failed` | Wrong file in the wrong slot, or authentication method isn't *Client certificate* |
| `UNKNOWN_TOPIC_OR_PART` | Topic not created yet: run `create-topic` or create it in the console |
| `Could not set lock on file` | Another program has the DuckDB file open for writing; the consumer retries automatically |

## Offline mode (no Kafka)

When the Aiven service is asleep or you're offline, use the landing folder instead. Same pipeline, no broker:

```powershell
python -m generator live --sink landing        # window 1
python -m ingestion consume --source landing   # window 2
```
