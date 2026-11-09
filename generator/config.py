"""Static configuration: models, teams and features.

Model names and prices are fictional but follow realistic price tiers
(USD per 1M tokens) so cost patterns look like a real company's bill.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ModelSpec:
    name: str
    input_price_per_m: float   # USD per 1M input tokens
    output_price_per_m: float  # USD per 1M output tokens
    base_latency_ms: int       # time to first token / request overhead
    ms_per_output_token: float
    is_embedding: bool = False


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    models: dict[str, float]          # model name -> selection weight
    input_tokens_median: int
    output_tokens_median: int
    weight: float = 1.0               # share of the team's traffic
    is_agent: bool = False            # calls grouped into multi-step agent runs


@dataclass(frozen=True)
class TeamSpec:
    name: str
    weight: float                     # share of company traffic
    features: tuple[FeatureSpec, ...] = field(default_factory=tuple)


MODELS: dict[str, ModelSpec] = {
    m.name: m
    for m in (
        ModelSpec("frontier-xl", 15.00, 75.00, 1800, 22.0),
        ModelSpec("pro-large", 3.00, 15.00, 900, 12.0),
        ModelSpec("standard-medium", 0.80, 4.00, 500, 7.0),
        ModelSpec("fast-small", 0.15, 0.60, 200, 3.0),
        ModelSpec("embed-v2", 0.02, 0.00, 60, 0.0, is_embedding=True),
    )
}

# The most expensive chat model — used by the "model switch" anomaly.
PREMIUM_MODEL = "frontier-xl"

TEAMS: tuple[TeamSpec, ...] = (
    TeamSpec("search", 0.25, (
        FeatureSpec("query_rewrite", {"fast-small": 1.0}, 150, 40, weight=3),
        FeatureSpec("semantic_index", {"embed-v2": 1.0}, 600, 0, weight=4),
        FeatureSpec("answer_summary", {"standard-medium": 0.8, "pro-large": 0.2}, 2500, 300, weight=2),
    )),
    TeamSpec("support", 0.22, (
        FeatureSpec("ticket_triage", {"fast-small": 1.0}, 800, 30, weight=3),
        FeatureSpec("reply_draft", {"standard-medium": 0.6, "pro-large": 0.4}, 1800, 350, weight=2),
        FeatureSpec("support_agent", {"pro-large": 1.0}, 3500, 400, weight=2, is_agent=True),
    )),
    TeamSpec("engineering", 0.20, (
        FeatureSpec("code_review", {"pro-large": 0.8, "frontier-xl": 0.2}, 6000, 700, weight=2),
        FeatureSpec("coding_agent", {"pro-large": 0.7, "frontier-xl": 0.3}, 9000, 900, weight=2, is_agent=True),
        FeatureSpec("docs_qa", {"standard-medium": 1.0}, 3000, 250, weight=1),
    )),
    TeamSpec("marketing", 0.12, (
        FeatureSpec("copy_generation", {"standard-medium": 0.7, "pro-large": 0.3}, 700, 600, weight=2),
        FeatureSpec("image_caption", {"fast-small": 1.0}, 400, 60, weight=1),
    )),
    TeamSpec("data-science", 0.11, (
        FeatureSpec("batch_labeling", {"fast-small": 1.0}, 500, 20, weight=3),
        FeatureSpec("experiment_eval", {"pro-large": 1.0}, 4000, 500, weight=1),
    )),
    TeamSpec("sales", 0.10, (
        FeatureSpec("email_draft", {"standard-medium": 1.0}, 900, 350, weight=2),
        FeatureSpec("call_summary", {"standard-medium": 0.7, "pro-large": 0.3}, 7000, 400, weight=1),
    )),
)

TEAMS_BY_NAME: dict[str, TeamSpec] = {t.name: t for t in TEAMS}

# Baseline request outcomes
P_ERROR = 0.015
P_RATE_LIMITED = 0.005

# Spread of token counts around the median (lognormal sigma)
TOKEN_SIGMA = 0.6

# Number of distinct users per team (user_001 ... user_N)
USERS_PER_TEAM = 25
