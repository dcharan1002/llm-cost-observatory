"""Build individual LLM usage events."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import numpy as np

from generator import config
from generator.config import FeatureSpec, ModelSpec, TeamSpec


def compute_cost(model: ModelSpec, input_tokens: int, output_tokens: int) -> float:
    """USD cost of one request, rounded to 8 decimals."""
    cost = (
        input_tokens / 1_000_000 * model.input_price_per_m
        + output_tokens / 1_000_000 * model.output_price_per_m
    )
    return round(cost, 8)


def format_ts(ts: datetime) -> str:
    """ISO-8601 UTC timestamp with millisecond precision, e.g. 2026-11-09T14:03:21.123Z."""
    ts = ts.astimezone(UTC)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


class EventFactory:
    """Creates realistic events. All randomness comes from ``rng`` so runs are reproducible."""

    def __init__(self, rng: np.random.Generator):
        self.rng = rng
        self._team_weights = np.array([t.weight for t in config.TEAMS])
        self._team_weights = self._team_weights / self._team_weights.sum()
        # Active agent run per (team, feature); calls reuse it until a new run starts.
        self._agent_runs: dict[tuple[str, str], tuple[str, int]] = {}

    # ---------- random helpers ----------
    def _uuid(self) -> str:
        return str(uuid.UUID(bytes=self.rng.bytes(16), version=4))

    def _tokens(self, median: int) -> int:
        if median <= 0:
            return 0
        return max(1, int(self.rng.lognormal(np.log(median), config.TOKEN_SIGMA)))

    def pick_team(self) -> TeamSpec:
        return config.TEAMS[self.rng.choice(len(config.TEAMS), p=self._team_weights)]

    def pick_feature(self, team: TeamSpec) -> FeatureSpec:
        w = np.array([f.weight for f in team.features], dtype=float)
        return team.features[self.rng.choice(len(team.features), p=w / w.sum())]

    def pick_model(self, feature: FeatureSpec) -> str:
        names = list(feature.models)
        w = np.array(list(feature.models.values()), dtype=float)
        return names[self.rng.choice(len(names), p=w / w.sum())]

    def _agent_run_id(self, team: str, feature: str) -> str:
        """Agent runs last ~3-15 calls before a new run starts."""
        key = (team, feature)
        run = self._agent_runs.get(key)
        if run is None or run[1] <= 0:
            run = (self._uuid(), int(self.rng.integers(3, 16)))
        self._agent_runs[key] = (run[0], run[1] - 1)
        return run[0]

    # ---------- main entry point ----------
    def make_event(
        self,
        ts: datetime,
        team: TeamSpec | None = None,
        feature: FeatureSpec | None = None,
    ) -> dict[str, Any]:
        team = team or self.pick_team()
        feature = feature or self.pick_feature(team)
        model_name = self.pick_model(feature)
        model = config.MODELS[model_name]

        input_tokens = self._tokens(feature.input_tokens_median)
        output_tokens = 0 if model.is_embedding else self._tokens(feature.output_tokens_median)

        r = self.rng.random()
        if r < config.P_RATE_LIMITED:
            status = "rate_limited"
        elif r < config.P_RATE_LIMITED + config.P_ERROR:
            status = "error"
        else:
            status = "success"

        event = {
            "event_id": self._uuid(),
            "timestamp": format_ts(ts),
            "team": team.name,
            "feature": feature.name,
            "user_id": f"{team.name}_user_{int(self.rng.integers(1, config.USERS_PER_TEAM + 1)):03d}",
            "model": model_name,
            "request_type": "embedding" if model.is_embedding else "chat",
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "latency_ms": 0,
            "status": status,
            "cost_usd": 0.0,
            "agent_run_id": self._agent_run_id(team.name, feature.name) if feature.is_agent else None,
            # Ground-truth label for evaluating anomaly detection (Phase 4).
            # Detection logic must never read this field.
            "injected_anomaly": None,
        }
        finalize(event, self.rng)
        return event


def finalize(event: dict[str, Any], rng: np.random.Generator) -> None:
    """(Re)compute status-dependent fields, latency and cost after any modification."""
    model = config.MODELS[event["model"]]
    if event["status"] != "success":
        # Failed requests return no output and are not billed.
        event["output_tokens"] = 0
        event["cost_usd"] = 0.0
        event["latency_ms"] = int(rng.integers(50, 400)) if event["status"] == "rate_limited" else int(
            model.base_latency_ms * rng.uniform(0.5, 3.0)
        )
        return
    latency = model.base_latency_ms + event["output_tokens"] * model.ms_per_output_token
    latency += event["input_tokens"] * 0.02  # longer prompts take a bit longer
    event["latency_ms"] = int(latency * rng.uniform(0.8, 1.3))
    event["cost_usd"] = compute_cost(model, event["input_tokens"], event["output_tokens"])
