from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from generator import config
from generator.__main__ import main
from generator.anomalies import ANOMALY_TYPES, create_anomaly, random_schedule
from generator.events import compute_cost
from generator.stream import backfill, live
from generator.traffic import traffic_multiplier

START = datetime(2026, 11, 2, 9, 0, tzinfo=UTC)  # a Monday morning
EXPECTED_KEYS = {
    "event_id", "timestamp", "team", "feature", "user_id", "model", "request_type",
    "input_tokens", "output_tokens", "latency_ms", "status", "cost_usd",
    "agent_run_id", "injected_anomaly",
}


def _events(hours=2, rate=2.0, anomalies=(), seed=1):
    return list(backfill(START, START + timedelta(hours=hours), rate, list(anomalies), seed))


# ---------- cost & schema ----------
def test_compute_cost():
    model = config.MODELS["pro-large"]  # $3 in / $15 out per 1M
    assert compute_cost(model, 1_000_000, 0) == pytest.approx(3.0)
    assert compute_cost(model, 1000, 1000) == pytest.approx(0.018)


def test_event_schema_and_values():
    events = _events()
    assert events
    teams = {t.name for t in config.TEAMS}
    for e in events:
        assert set(e) == EXPECTED_KEYS
        assert e["team"] in teams
        assert e["model"] in config.MODELS
        assert e["input_tokens"] > 0 and e["output_tokens"] >= 0 and e["cost_usd"] >= 0
        assert e["status"] in {"success", "error", "rate_limited"}
        assert e["timestamp"].endswith("Z")


def test_failed_requests_are_not_billed():
    failed = [e for e in _events(hours=6) if e["status"] != "success"]
    assert failed
    assert all(e["cost_usd"] == 0 and e["output_tokens"] == 0 for e in failed)


def test_embeddings_have_no_output_tokens():
    emb = [e for e in _events() if e["request_type"] == "embedding"]
    assert emb and all(e["output_tokens"] == 0 for e in emb)


def test_cost_matches_tokens():
    for e in _events()[:200]:
        if e["status"] == "success":
            expected = compute_cost(config.MODELS[e["model"]], e["input_tokens"], e["output_tokens"])
            assert e["cost_usd"] == pytest.approx(expected)


def test_agent_features_have_run_ids():
    events = _events()
    agents = [e for e in events if e["feature"] in {"support_agent", "coding_agent"}]
    others = [e for e in events if e["feature"] not in {"support_agent", "coding_agent"}]
    assert agents and all(e["agent_run_id"] for e in agents)
    assert all(e["agent_run_id"] is None for e in others)


# ---------- determinism & ordering ----------
def test_same_seed_same_data():
    assert _events(seed=7) == _events(seed=7)
    assert _events(seed=7) != _events(seed=8)


def test_backfill_is_time_ordered():
    ts = [e["timestamp"] for e in _events()]
    assert ts == sorted(ts)


def test_rate_roughly_matches():
    events = _events(hours=1, rate=5.0)  # 09:00 UTC multiplier = 0.9
    expected = 5.0 * 3600 * traffic_multiplier(START)
    assert 0.9 * expected < len(events) < 1.1 * expected


def test_weekends_are_quieter():
    sat = datetime(2026, 11, 7, 12, tzinfo=UTC)
    wed = datetime(2026, 11, 4, 12, tzinfo=UTC)
    assert traffic_multiplier(sat) < traffic_multiplier(wed)


# ---------- anomalies ----------
@pytest.mark.parametrize("kind", ANOMALY_TYPES)
def test_each_anomaly_type_leaves_a_trace(kind):
    rng = np.random.default_rng(3)
    a = create_anomaly(kind, START, rng)
    a.end = START + timedelta(hours=2)
    events = _events(hours=2, rate=3.0, anomalies=[a])
    hit = [e for e in events if e["injected_anomaly"] == kind]
    assert hit, f"{kind} produced no events"
    assert all((e["team"], e["feature"]) == (a.team, a.feature) for e in hit)

    if kind == "runaway_agent":
        assert len({e["agent_run_id"] for e in hit}) == 1
        assert len(hit) > 1000  # 80-250 calls/min for 2 hours
    elif kind == "model_switch":
        assert all(e["model"] == config.PREMIUM_MODEL for e in hit)
    elif kind == "error_spike":
        assert all(e["status"] == "error" for e in hit)
    elif kind == "token_bloat":
        normal = [e["input_tokens"] for e in events
                  if (e["team"], e["feature"]) == (a.team, a.feature) and not e["injected_anomaly"]]
        bloated = [e["input_tokens"] for e in hit]
        assert not normal or np.median(bloated) > 3 * np.median(normal)


def test_no_anomalies_outside_window():
    rng = np.random.default_rng(4)
    a = create_anomaly("model_switch", START + timedelta(hours=1), rng)
    a.end = START + timedelta(hours=2)
    for e in _events(hours=3, anomalies=[a]):
        if e["injected_anomaly"]:
            assert a.start.isoformat()[:13] <= e["timestamp"][:13] < a.end.isoformat()[:13]


def test_random_schedule_covers_all_types():
    rng = np.random.default_rng(0)
    sched = random_schedule(START, START + timedelta(days=3), 8, rng)
    assert len(sched) == 8
    assert {a.type for a in sched} == set(ANOMALY_TYPES)


def test_runaway_only_targets_agent_features():
    rng = np.random.default_rng(5)
    agent_features = {f.name for t in config.TEAMS for f in t.features if f.is_agent}
    for _ in range(20):
        assert create_anomaly("runaway_agent", START, rng).feature in agent_features


# ---------- live mode & CLI ----------
def test_live_mode_streams_events():
    events = list(live(rate_per_sec=20, duration_sec=1.5, seed=1))
    assert len(events) > 5
    assert set(events[0]) == EXPECTED_KEYS


def test_cli_backfill_writes_files(tmp_path):
    out = tmp_path / "raw" / "events.jsonl"
    main(["backfill", "--days", "0.1", "--rate", "1", "--anomalies", "2", "--out", str(out)])
    assert out.exists() and out.stat().st_size > 0
    assert (tmp_path / "raw" / "anomalies_ground_truth.json").exists()
