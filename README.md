# 💸 LLM Cost Observatory

> Real-time FinOps for AI: track LLM API spend by team, model and feature, catch runaway agents before they burn the budget, and ask questions about your spend in plain English.

[![CI](https://github.com/dcharan1002/llm-cost-observatory/actions/workflows/ci.yml/badge.svg)](https://github.com/dcharan1002/llm-cost-observatory/actions/workflows/ci.yml)
![status](https://img.shields.io/badge/status-in%20progress-yellow)
![python](https://img.shields.io/badge/python-3.12-blue)
![license](https://img.shields.io/badge/license-MIT-green)

---

## 🧩 The problem

Companies are adopting LLM APIs and AI coding agents fast, and their bills are growing even faster. Most teams can't answer simple questions like:

- Which team or feature is driving our AI spend?
- Did someone switch to an expensive model without telling anyone?
- Is an agent stuck in a loop, calling the API thousands of times?

**LLM Cost Observatory** is an end-to-end data platform that answers these questions in real time.

## ✨ Features (planned)

- [x] Synthetic LLM usage event generator with realistic patterns and injected anomalies
- [x] Streaming ingestion with Apache Kafka (Aiven) into DuckDB, with idempotent micro-batches and a dead-letter queue
- [ ] dbt transformations with data-quality tests
- [ ] Anomaly detection (rolling z-score + rule-based runaway-agent detection)
- [ ] Hourly orchestration with Dagster + Discord alerts
- [ ] FastAPI backend with an "ask your spend" text-to-SQL assistant
- [ ] Next.js dashboard
- [ ] CI/CD with GitHub Actions, deployed on Vercel + Render *(CI with a real-Kafka integration test is live)*

## 🏗️ Architecture

```
 ┌────────────┐    ┌──────────┐    ┌──────────┐    ┌─────────┐    ┌──────────┐
 │ Generator  │──▶ │  Kafka   │──▶ │ Consumer │──▶ │ DuckDB  │──▶ │   dbt    │
 │ (Python)   │    │ (Aiven)  │    │ (Python) │    │  (raw)  │    │ (marts)  │
 └────────────┘    └──────────┘    └──────────┘    └─────────┘    └────┬─────┘
                                                                       │
            ┌──────────────┐    ┌──────────┐    ┌──────────────────┐   │
            │ Next.js      │◀── │ FastAPI  │◀── │ Anomaly detection│◀──┘
            │ dashboard    │    │ + LLM    │    │ + Discord alerts │
            └──────────────┘    └──────────┘    └──────────────────┘
                         Orchestrated hourly by Dagster
```

## 🛠️ Tech stack

| Layer | Tools |
|---|---|
| Data generation | Python, NumPy |
| Streaming | Apache Kafka on Aiven (free tier), confluent-kafka; Redpanda in CI |
| Storage | DuckDB (Apache Iceberg planned) |
| Transformation | dbt Core + dbt-duckdb |
| Orchestration | Dagster |
| Backend | FastAPI, Pydantic |
| AI | Ollama (local) / free-tier LLM API (hosted) |
| Frontend | Next.js, Tailwind CSS, Recharts |
| DevOps | GitHub Actions, Vercel, Render |

## 📁 Project structure

```
llm-cost-observatory/
├── generator/        # synthetic LLM usage event producer
├── common/           # shared settings + Kafka config
├── ingestion/        # Kafka consumer → DuckDB
├── dbt_project/      # transformations + data-quality tests
├── orchestration/    # Dagster jobs, schedules, alerts
├── backend/          # FastAPI service
├── frontend/         # Next.js dashboard
├── docs/             # setup guide, design notes, screenshots
├── .github/workflows # CI/CD
├── docker-compose.yml
└── Makefile
```

## 🚀 Quick start

> Full setup instructions: [docs/SETUP.md](docs/SETUP.md)

```bash
git clone https://github.com/dcharan1002/llm-cost-observatory.git
cd llm-cost-observatory
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # Windows: copy .env.example .env

# 7 days of history straight into the warehouse
python -m generator backfill --days 7
python -m ingestion load data/raw/events.jsonl
python -m ingestion stats

# Live streaming through Kafka (two terminals) — see docs/KAFKA_SETUP.md
python -m generator live --sink kafka
python -m ingestion consume

pytest -q
```

> No Kafka handy? Use `--sink landing` and `consume --source landing` for the same pipeline through a local folder.

## 🗺️ Roadmap

| Phase | Description | Status |
|---|---|---|
| 0 | Project setup | ✅ |
| 1 | Synthetic data generator ([details](generator/README.md)) | ✅ |
| 2 | Streaming pipeline ([details](ingestion/README.md)) | ✅ |
| 3 | dbt models + tests | ⏳ |
| 4 | Anomaly detection | ⏳ |
| 5 | Orchestration + alerts | ⏳ |
| 6 | FastAPI backend + AI assistant | ⏳ |
| 7 | Next.js dashboard | ⏳ |
| 8 | CI/CD | ⏳ |
| 9 | Deployment | ⏳ |
| 10 | Demo + write-up | ⏳ |
