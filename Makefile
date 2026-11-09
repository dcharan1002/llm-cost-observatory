.PHONY: help setup up down logs clean test lint generate stream load consume produce stats ping

PYTHON := python3
VENV   := .venv
PIP    := $(VENV)/bin/pip

help:            ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

setup:           ## Create the virtual environment and install dependencies
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@[ -f .env ] || cp .env.example .env
	@mkdir -p data
	@echo "✅ Setup complete. Activate with: source $(VENV)/bin/activate"

up:              ## Start all services with Docker Compose
	docker compose up -d --build

down:            ## Stop all services
	docker compose down

logs:            ## Follow service logs
	docker compose logs -f

generate:        ## Backfill 7 days of synthetic events into data/raw/
	$(VENV)/bin/python -m generator backfill --days 7

stream:          ## Stream live events to the terminal (Ctrl+C to stop)
	$(VENV)/bin/python -m generator live --rate 5

load:            ## Bulk-load the backfill file into DuckDB
	$(VENV)/bin/python -m ingestion load data/raw/events.jsonl

produce:         ## Stream live events to Kafka
	$(VENV)/bin/python -m generator live --sink kafka

consume:         ## Consume Kafka into DuckDB
	$(VENV)/bin/python -m ingestion consume

ping:            ## Test the Kafka connection
	$(VENV)/bin/python -m ingestion ping

stats:           ## Summarize the warehouse
	$(VENV)/bin/python -m ingestion stats

test:            ## Run Python tests
	$(VENV)/bin/pytest -q

lint:            ## Lint Python code
	$(VENV)/bin/ruff check .

clean:           ## Remove generated data and caches (keeps .env)
	rm -rf data/*.duckdb .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
