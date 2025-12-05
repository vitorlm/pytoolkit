"""Enhanced AlertCycle data model with auto-healing classification and trend analysis support."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class CycleClassification(Enum):
    """Classification types for alert cycles based on auto-healing analysis."""

    FLAPPING = "FLAPPING"
    BENIGN_TRANSIENT = "BENIGN_TRANSIENT"
    ACTIONABLE = "ACTIONABLE"


@dataclass(frozen=True)
class EnhancedLifecycleEvent:
    """Enhanced lifecycle event with additional metadata for classification."""

    raw: dict[str, Any]
    timestamp: datetime | None
    timestamp_epoch: float | None
    monitor_id: str
    monitor_name: str | None
    alert_cycle_key: str
    source_state: str | None
    destination_state: str | None
    transition_type: str | None
    status: str | None
    team: str | None
    env: str | None
    duration_seconds: float | None
    priority: int | None

    # Enhanced metadata for classification
    was_paged: bool | None = None
    human_action_taken: bool | None = None
    correlation_id: str | None = None
    severity_level: str | None = None

    def state_label(self) -> str | None:
        return self.destination_state or self.status

    def is_business_hours(self) -> bool:
        """Check if event occurred during Brazilian business hours (9 AM - 5 PM)."""
        if not self.timestamp:
            return False

        try:
            # Convert to Brazilian timezone (UTC-3)
            from datetime import timedelta, timezone

            brazil_tz = timezone(timedelta(hours=-3))
            brazil_time = self.timestamp.astimezone(brazil_tz)
            return 9 <= brazil_time.hour <= 17
        except Exception:
            # Fallback to UTC if conversion fails
            return 9 <= self.timestamp.hour <= 17


@dataclass
class EnhancedAlertCycle:
    """Enhanced AlertCycle with auto-healing classification capabilities."""

    key: str
    monitor_id: str
    monitor_name: str | None
    events: list[EnhancedLifecycleEvent] = field(default_factory=list)

    # Enhanced classification fields
    cycle_classification: CycleClassification | None = None
    classification_confidence: float | None = None
    classification_reasons: list[str] = field(default_factory=list)

    # Additional metadata
    root_cause_hint: str | None = None
    correlation_group: str | None = None

    def __post_init__(self) -> None:
        self.events.sort(key=lambda evt: evt.timestamp or datetime.min.replace(tzinfo=UTC))

    @property
    def start(self) -> datetime | None:
        return self.events[0].timestamp if self.events else None

    @property
    def end(self) -> datetime | None:
        return self.events[-1].timestamp if self.events else None

    @property
    def duration_seconds(self) -> float | None:
        if self.start and self.end:
            return max((self.end - self.start).total_seconds(), 0.0)
        if self.events and self.events[0].duration_seconds is not None:
            return self.events[0].duration_seconds
        return None

    @property
    def duration_minutes(self) -> float | None:
        """Get cycle duration in minutes."""
        duration_s = self.duration_seconds
        return duration_s / 60.0 if duration_s is not None else None

    def state_sequence(self) -> list[str]:
        sequence: list[str] = []
        for event in self.events:
            if event.source_state and (not sequence or sequence[-1] != event.source_state):
                sequence.append(event.source_state)
            destination = event.destination_state or event.status
            if destination and (not sequence or sequence[-1] != destination):
                sequence.append(destination)
        return sequence

    def state_transition_count(self) -> int:
        """Count the number of state transitions in this cycle."""
        sequence = self.state_sequence()
        return max(0, len(sequence) - 1)

    def has_recovery(self) -> bool:
        for event in self.events:
            transition = (event.transition_type or "").lower()
            if "recovery" in transition:
                return True
            if (event.destination_state or "").upper() == "OK":
                return True
        return False

    def has_alert(self) -> bool:
        for event in self.events:
            if (event.destination_state or "").upper() == "ALERT":
                return True
        return False

    def recovered_quickly(self, threshold_seconds: float = 300) -> bool:
        """Check if cycle recovered within threshold (default 5 minutes)."""
        duration = self.duration_seconds
        return duration is not None and duration <= threshold_seconds

    def is_business_hours_cycle(self) -> bool:
        """Check if majority of events occurred during business hours."""
        if not self.events:
            return False

        business_events = sum(1 for evt in self.events if evt.is_business_hours())
        return business_events > len(self.events) / 2

    def teams(self) -> list[str]:
        return sorted({event.team for event in self.events if event.team})

    def environments(self) -> list[str]:
        return sorted({event.env for event in self.events if event.env})

    def time_to_resolution_seconds(self) -> float | None:
        if not self.start:
            return None
        for event in self.events:
            transition = (event.transition_type or "").lower()
            is_recovery_transition = "recovery" in transition
            destination_ok = (event.destination_state or "").upper() == "OK"
            if is_recovery_transition or destination_ok:
                if event.timestamp is None:
                    continue
                return max((event.timestamp - self.start).total_seconds(), 0.0)
        return None

    def was_human_action_involved(self) -> bool:
        """Heuristic to determine if human action was likely involved."""
        # Check explicit metadata first
        for event in self.events:
            if event.human_action_taken is True:
                return True
            if event.was_paged is True:
                return True

        # Heuristic: If TTR is significantly longer than cycle duration,
        # it suggests human intervention
        ttr = self.time_to_resolution_seconds()
        cycle_duration = self.duration_seconds

        if ttr and cycle_duration:
            # If time to resolution is much longer than cycle duration,
            # suggests manual investigation/action
            return ttr > cycle_duration * 1.5 and ttr > 600  # 10 minutes

        return False


@dataclass
class WeeklyMonitorSnapshot:
    """Weekly snapshot of monitor metrics for trend analysis."""

    monitor_id: str
    monitor_name: str | None
    iso_week: str  # Format: "2025-W42"
    year: int
    week: int

    # Core metrics
    noise_score: float
    self_heal_rate: float
    actionable_rate: float
    mttr_seconds: float | None
    mtbf_seconds: float | None
    cycles_count: int
    events_count: int

    # Enhanced metrics
    flapping_cycles: int
    benign_transient_cycles: int
    actionable_cycles: int
    business_hours_events_pct: float

    # Additional metadata
    teams: list[str] = field(default_factory=list)
    environments: list[str] = field(default_factory=list)
    avg_cycle_duration_minutes: float | None = None
    health_score: float | None = None

    @classmethod
    def from_monitor_metrics(
        cls,
        monitor_id: str,
        monitor_name: str | None,
        iso_week: str,
        year: int,
        week: int,
        metrics: dict[str, Any],
    ) -> WeeklyMonitorSnapshot:
        """Create snapshot from computed monitor metrics."""
        return cls(
            monitor_id=monitor_id,
            monitor_name=monitor_name,
            iso_week=iso_week,
            year=year,
            week=week,
            noise_score=float(metrics.get("noise_score", 0.0)),
            self_heal_rate=float(metrics.get("self_healing_rate", 0.0)),
            actionable_rate=1.0 - float(metrics.get("manual_rate", 0.0)),
            mttr_seconds=metrics.get("avg_time_to_resolution_seconds"),
            mtbf_seconds=metrics.get("mtbf_seconds"),
            cycles_count=int(metrics.get("cycle_count", 0)),
            events_count=int(metrics.get("event_count", 0)),
            flapping_cycles=int(metrics.get("flapping_cycles", 0)),
            benign_transient_cycles=int(metrics.get("benign_transient_cycles", 0)),
            actionable_cycles=int(metrics.get("actionable_cycles", 0)),
            business_hours_events_pct=float(metrics.get("business_hours_percentage", 0.0)),
            teams=metrics.get("teams", []),
            environments=metrics.get("environments", []),
            avg_cycle_duration_minutes=metrics.get("avg_cycle_duration_minutes"),
            health_score=metrics.get("health_score", {}).get("score")
            if isinstance(metrics.get("health_score"), dict)
            else None,
        )


@dataclass
class WeeklySummarySnapshot:
    """Weekly summary snapshot across all monitors."""

    iso_week: str
    year: int
    week: int

    # Aggregated metrics
    total_monitors: int
    total_cycles: int
    total_events: int

    avg_noise_score: float
    avg_self_heal_rate: float
    avg_actionable_rate: float
    avg_health_score: float | None

    # Classification totals
    total_flapping_cycles: int
    total_benign_transient_cycles: int
    total_actionable_cycles: int

    # Trend indicators
    monitors_improved: int = 0  # Compared to previous week
    monitors_degraded: int = 0
    monitors_stable: int = 0

    @classmethod
    def from_monitor_snapshots(
        cls,
        iso_week: str,
        year: int,
        week: int,
        monitor_snapshots: list[WeeklyMonitorSnapshot],
    ) -> WeeklySummarySnapshot:
        """Create summary from individual monitor snapshots."""
        if not monitor_snapshots:
            return cls(
                iso_week=iso_week,
                year=year,
                week=week,
                total_monitors=0,
                total_cycles=0,
                total_events=0,
                avg_noise_score=0.0,
                avg_self_heal_rate=0.0,
                avg_actionable_rate=0.0,
                avg_health_score=None,
                total_flapping_cycles=0,
                total_benign_transient_cycles=0,
                total_actionable_cycles=0,
            )

        total_monitors = len(monitor_snapshots)
        total_cycles = sum(s.cycles_count for s in monitor_snapshots)
        total_events = sum(s.events_count for s in monitor_snapshots)

        # Calculate averages
        avg_noise_score = sum(s.noise_score for s in monitor_snapshots) / total_monitors
        avg_self_heal_rate = sum(s.self_heal_rate for s in monitor_snapshots) / total_monitors
        avg_actionable_rate = sum(s.actionable_rate for s in monitor_snapshots) / total_monitors

        health_scores = [s.health_score for s in monitor_snapshots if s.health_score is not None]
        avg_health_score = sum(health_scores) / len(health_scores) if health_scores else None

        return cls(
            iso_week=iso_week,
            year=year,
            week=week,
            total_monitors=total_monitors,
            total_cycles=total_cycles,
            total_events=total_events,
            avg_noise_score=avg_noise_score,
            avg_self_heal_rate=avg_self_heal_rate,
            avg_actionable_rate=avg_actionable_rate,
            avg_health_score=avg_health_score,
            total_flapping_cycles=sum(s.flapping_cycles for s in monitor_snapshots),
            total_benign_transient_cycles=sum(s.benign_transient_cycles for s in monitor_snapshots),
            total_actionable_cycles=sum(s.actionable_cycles for s in monitor_snapshots),
        )
