"""
Datadog metrics registry.

Provides centralized definitions and light metadata for Datadog summary
metrics to promote consistency across producers and consumers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class MetricDef:
    name: str
    unit: str
    description: str


REGISTRY: Dict[str, MetricDef] = {
    # Quality metrics
    "datadog.events.quality.overall_noise_score": MetricDef(
        name="datadog.events.quality.overall_noise_score",
        unit="score",
        description="Overall noise score (0-100, lower is cleaner).",
    ),
    "datadog.events.quality.self_healing_rate": MetricDef(
        name="datadog.events.quality.self_healing_rate",
        unit="percent",
        description="Rate of alerts resolved without human intervention.",
    ),
    "datadog.events.quality.actionable_alerts_percent": MetricDef(
        name="datadog.events.quality.actionable_alerts_percent",
        unit="percent",
        description="Percentage of alerts considered actionable (vs noise).",
    ),
    # Temporal metrics
    "datadog.events.temporal.avg_ttr_minutes": MetricDef(
        name="datadog.events.temporal.avg_ttr_minutes",
        unit="minutes",
        description="Average time-to-resolution for alerts in minutes.",
    ),
    "datadog.events.temporal.mtbf_hours": MetricDef(
        name="datadog.events.temporal.mtbf_hours",
        unit="hours",
        description="Mean time between failures in hours.",
    ),
    # Health metrics
    "datadog.events.health.average_score": MetricDef(
        name="datadog.events.health.average_score",
        unit="score",
        description="Average monitor health score (0-100).",
    ),
    "datadog.events.health.monitors_needing_attention": MetricDef(
        name="datadog.events.health.monitors_needing_attention",
        unit="monitors",
        description="Count of monitors with low health requiring attention.",
    ),
}
