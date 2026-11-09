"""Traffic seasonality: busier during working hours and on weekdays."""

from __future__ import annotations

from datetime import datetime

# Relative traffic per UTC hour (0-23). Peaks during US/EU working hours.
_HOURLY = (
    0.30, 0.25, 0.22, 0.20, 0.22, 0.30,  # 00-05
    0.45, 0.60, 0.75, 0.90, 1.00, 1.00,  # 06-11
    1.00, 1.00, 1.00, 1.00, 0.95, 0.90,  # 12-17
    0.85, 0.75, 0.65, 0.55, 0.45, 0.35,  # 18-23
)
_WEEKEND_FACTOR = 0.35


def traffic_multiplier(ts: datetime) -> float:
    """Return a 0-1 multiplier for the base event rate at time ``ts`` (UTC)."""
    m = _HOURLY[ts.hour]
    if ts.weekday() >= 5:  # Saturday / Sunday
        m *= _WEEKEND_FACTOR
    return m
