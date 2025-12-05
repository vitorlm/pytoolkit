"""Auto-healing alert classifier for distinguishing flapping, benign transients, and actionable alerts."""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from utils.logging.logging_manager import LogManager

from .enhanced_alert_cycle import (
    CycleClassification,
    EnhancedAlertCycle,
)


@dataclass
class ClassificationConfig:
    """Configuration parameters for alert classification."""

    # Flapping detection thresholds
    flap_window_minutes: int = 60
    flap_min_cycles: int = 3
    flap_max_transitions_per_hour: int = 10
    flap_cv_threshold: float = 0.5  # Coefficient of variation threshold

    # Benign transient thresholds
    transient_max_duration_seconds: float = 300  # 5 minutes
    transient_window_seconds: float = 3600  # 1 hour for checking repeated flips
    transient_min_gap_seconds: float = 1800  # 30 minutes between transients

    # Actionable alert thresholds
    actionable_min_duration_seconds: float = 600  # 10 minutes
    actionable_min_ttl_seconds: float = 120  # 2 minutes minimum time-to-live

    # Hysteresis and debounce (configurable)
    enable_hysteresis: bool = False
    hysteresis_up_threshold_multiplier: float = 1.05
    hysteresis_down_threshold_multiplier: float = 0.95
    debounce_seconds: float = 60

    # Business hours window (Brazilian timezone)
    business_hours_start: int = 9  # 9 AM
    business_hours_end: int = 17  # 5 PM

    # Minimum data requirements for classification
    min_events_for_classification: int = 2
    min_weeks_for_trends: int = 3


@dataclass
class ClassificationResult:
    """Result of cycle classification with reasoning."""

    classification: CycleClassification
    confidence: float  # 0.0 - 1.0
    reasons: list[str] = field(default_factory=list)
    metadata: dict[str, any] = field(default_factory=dict)


class AlertCycleClassifier:
    """Classifier for distinguishing alert cycle types based on behavior patterns."""

    def __init__(self, config: ClassificationConfig | None = None):
        self.config = config or ClassificationConfig()
        self.logger = LogManager.get_instance().get_logger("AlertCycleClassifier")

    def classify_cycle(self, cycle: EnhancedAlertCycle) -> ClassificationResult:
        """Classify a single alert cycle."""
        if len(cycle.events) < self.config.min_events_for_classification:
            return ClassificationResult(
                classification=CycleClassification.ACTIONABLE,
                confidence=0.1,
                reasons=["Insufficient events for reliable classification"],
            )

        # Check for benign transient first (most restrictive)
        transient_result = self._check_benign_transient(cycle)
        if transient_result.classification == CycleClassification.BENIGN_TRANSIENT:
            return transient_result

        # Check for flapping (medium restrictive)
        flapping_result = self._check_flapping(cycle)
        if flapping_result.classification == CycleClassification.FLAPPING:
            return flapping_result

        # Default to actionable
        return self._classify_actionable(cycle)

    def classify_monitor_cycles(
        self,
        monitor_id: str,
        cycles: list[EnhancedAlertCycle],
        time_window_hours: int = 24,
    ) -> dict[str, ClassificationResult]:
        """Classify all cycles for a monitor, considering temporal patterns."""
        results = {}

        if not cycles:
            return results

        # Sort cycles by timestamp
        sorted_cycles = sorted(cycles, key=lambda c: c.start or datetime.min.replace(tzinfo=UTC))

        # Check for monitor-level flapping patterns
        monitor_flapping = self._detect_monitor_flapping(sorted_cycles, time_window_hours)

        for cycle in sorted_cycles:
            # Classify individual cycle
            result = self.classify_cycle(cycle)

            # Apply monitor-level context
            if monitor_flapping["is_flapping"]:
                if result.classification != CycleClassification.FLAPPING:
                    result.reasons.append("Part of monitor-level flapping pattern")
                    result.confidence = min(result.confidence + 0.2, 1.0)

                # Override classification if strong monitor-level flapping evidence
                if (
                    monitor_flapping["confidence"] > 0.8
                    and result.classification == CycleClassification.BENIGN_TRANSIENT
                ):
                    result = ClassificationResult(
                        classification=CycleClassification.FLAPPING,
                        confidence=min(
                            result.confidence + monitor_flapping["confidence"] * 0.3,
                            1.0,
                        ),
                        reasons=result.reasons
                        + [
                            f"Monitor shows strong flapping pattern ({monitor_flapping['cycles_in_window']} cycles in {time_window_hours}h)"
                        ],
                    )

            results[cycle.key] = result

        return results

    def _check_benign_transient(self, cycle: EnhancedAlertCycle) -> ClassificationResult:
        """Check if cycle qualifies as benign transient."""
        reasons = []
        confidence_factors = []

        # Check duration constraint
        duration = cycle.duration_seconds
        if duration is None or duration > self.config.transient_max_duration_seconds:
            return ClassificationResult(
                classification=CycleClassification.ACTIONABLE,
                confidence=0.8,
                reasons=["Duration exceeds benign transient threshold"],
            )

        reasons.append(f"Short duration: {duration:.1f}s (< {self.config.transient_max_duration_seconds}s)")
        confidence_factors.append(0.3)

        # Check state transitions (should be simple: alert -> recover)
        transitions = cycle.state_transition_count()
        if transitions != 1:
            return ClassificationResult(
                classification=CycleClassification.ACTIONABLE,
                confidence=0.7,
                reasons=["Complex state transitions suggest non-transient issue"],
            )

        reasons.append("Simple transition pattern (single alert → recovery)")
        confidence_factors.append(0.25)

        # Check for human action indicators
        if cycle.was_human_action_involved():
            return ClassificationResult(
                classification=CycleClassification.ACTIONABLE,
                confidence=0.9,
                reasons=["Evidence of human intervention"],
            )

        reasons.append("No evidence of manual intervention")
        confidence_factors.append(0.2)

        # Check recovery time vs cycle duration (should be similar for transients)
        ttr = cycle.time_to_resolution_seconds()
        if ttr and duration:
            ratio = abs(ttr - duration) / max(duration, 1)
            if ratio < 0.2:  # TTR ≈ cycle duration
                reasons.append("Resolution time matches cycle duration (automatic recovery)")
                confidence_factors.append(0.15)
            else:
                confidence_factors.append(-0.1)  # Penalty for mismatch

        # Business hours bonus (issues during business hours less likely to be transient)
        if not cycle.is_business_hours_cycle():
            reasons.append("Occurred outside business hours")
            confidence_factors.append(0.1)

        # Calculate final confidence
        base_confidence = 0.6  # Base confidence for benign transient
        final_confidence = min(1.0, base_confidence + sum(confidence_factors))

        if final_confidence >= 0.7:
            return ClassificationResult(
                classification=CycleClassification.BENIGN_TRANSIENT,
                confidence=final_confidence,
                reasons=reasons,
                metadata={
                    "duration_seconds": duration,
                    "transition_count": transitions,
                    "time_to_resolution": ttr,
                },
            )

        return ClassificationResult(
            classification=CycleClassification.ACTIONABLE,
            confidence=0.8 - final_confidence,
            reasons=["Does not meet benign transient criteria"] + reasons,
        )

    def _check_flapping(self, cycle: EnhancedAlertCycle) -> ClassificationResult:
        """Check if cycle is part of flapping pattern."""
        reasons = []
        confidence_factors = []

        # Individual cycle flapping indicators
        transitions = cycle.state_transition_count()
        if transitions >= 4:  # Multiple state changes
            reasons.append(f"High transition count: {transitions}")
            confidence_factors.append(0.3)

        # Duration-based flapping (very short cycles repeatedly)
        duration = cycle.duration_seconds
        if duration and duration < 60:  # Less than 1 minute
            reasons.append(f"Very short duration: {duration:.1f}s")
            confidence_factors.append(0.2)

        # Check for rapid state oscillation within the cycle
        if len(cycle.events) >= 4:
            state_sequence = cycle.state_sequence()
            oscillation_score = self._calculate_oscillation_score(state_sequence)
            if oscillation_score > 0.5:
                reasons.append(f"High state oscillation: {oscillation_score:.2f}")
                confidence_factors.append(0.25)

        # Base confidence calculation
        base_confidence = 0.4
        final_confidence = min(1.0, base_confidence + sum(confidence_factors))

        if final_confidence >= 0.7:
            return ClassificationResult(
                classification=CycleClassification.FLAPPING,
                confidence=final_confidence,
                reasons=reasons,
                metadata={
                    "transition_count": transitions,
                    "duration_seconds": duration,
                    "oscillation_detected": True,
                },
            )

        return ClassificationResult(
            classification=CycleClassification.ACTIONABLE,
            confidence=0.6,
            reasons=["No strong flapping indicators in individual cycle"],
        )

    def _classify_actionable(self, cycle: EnhancedAlertCycle) -> ClassificationResult:
        """Classify cycle as actionable with reasoning."""
        reasons = []
        confidence_factors = []

        # Duration indicates substantial issue
        duration = cycle.duration_seconds
        if duration and duration >= self.config.actionable_min_duration_seconds:
            reasons.append(f"Substantial duration: {duration:.1f}s (≥ {self.config.actionable_min_duration_seconds}s)")
            confidence_factors.append(0.3)

        # Human action involvement
        if cycle.was_human_action_involved():
            reasons.append("Evidence of manual intervention")
            confidence_factors.append(0.4)

        # Business hours occurrence (higher likelihood of actionable)
        if cycle.is_business_hours_cycle():
            reasons.append("Occurred during business hours")
            confidence_factors.append(0.15)

        # Complex state transitions
        transitions = cycle.state_transition_count()
        if transitions > 1:
            reasons.append(f"Complex state transitions: {transitions}")
            confidence_factors.append(0.15)

        # Calculate confidence
        base_confidence = 0.5
        final_confidence = min(1.0, base_confidence + sum(confidence_factors))

        return ClassificationResult(
            classification=CycleClassification.ACTIONABLE,
            confidence=final_confidence,
            reasons=reasons if reasons else ["Default actionable classification"],
            metadata={
                "duration_seconds": duration,
                "transition_count": transitions,
                "business_hours": cycle.is_business_hours_cycle(),
            },
        )

    def _detect_monitor_flapping(self, cycles: list[EnhancedAlertCycle], time_window_hours: int) -> dict[str, any]:
        """Detect flapping at monitor level across cycles."""
        if len(cycles) < self.config.flap_min_cycles:
            return {"is_flapping": False, "confidence": 0.0, "cycles_in_window": 0}

        # Create time windows and count cycles
        window_counts = defaultdict(int)
        window_delta = timedelta(hours=time_window_hours)

        for i, cycle in enumerate(cycles):
            if not cycle.start:
                continue

            # Count cycles in window starting from this cycle
            window_end = cycle.start + window_delta
            cycles_in_window = 1  # Include current cycle

            for j in range(i + 1, len(cycles)):
                other_cycle = cycles[j]
                if not other_cycle.start:
                    continue
                if other_cycle.start <= window_end:
                    cycles_in_window += 1
                else:
                    break

            window_counts[cycle.start] = cycles_in_window

        # Find maximum cycles in any window
        max_cycles_in_window = max(window_counts.values()) if window_counts else 0

        # Flapping criteria
        is_flapping = max_cycles_in_window >= self.config.flap_min_cycles

        # Calculate confidence based on cycle density
        if is_flapping:
            # Higher density = higher confidence
            confidence = min(1.0, (max_cycles_in_window - self.config.flap_min_cycles) / 10.0 + 0.6)

            # Additional confidence from duration patterns
            short_cycles = sum(
                1 for cycle in cycles[-max_cycles_in_window:] if cycle.duration_seconds and cycle.duration_seconds < 300
            )
            if short_cycles >= max_cycles_in_window * 0.7:
                confidence = min(1.0, confidence + 0.2)
        else:
            confidence = 0.0

        return {
            "is_flapping": is_flapping,
            "confidence": confidence,
            "cycles_in_window": max_cycles_in_window,
            "window_hours": time_window_hours,
        }

    def _calculate_oscillation_score(self, state_sequence: list[str]) -> float:
        """Calculate oscillation score based on state sequence pattern."""
        if len(state_sequence) < 3:
            return 0.0

        # Count state reversals (A->B->A patterns)
        reversals = 0
        for i in range(len(state_sequence) - 2):
            if state_sequence[i] == state_sequence[i + 2] and state_sequence[i] != state_sequence[i + 1]:
                reversals += 1

        # Normalize by sequence length
        max_possible_reversals = max(1, len(state_sequence) - 2)
        return reversals / max_possible_reversals

    def generate_classification_summary(
        self, classification_results: dict[str, ClassificationResult]
    ) -> dict[str, any]:
        """Generate summary statistics from classification results."""
        if not classification_results:
            return {
                "total_cycles": 0,
                "flapping_cycles": 0,
                "benign_transient_cycles": 0,
                "actionable_cycles": 0,
                "avg_confidence": 0.0,
            }

        total_cycles = len(classification_results)
        flapping_cycles = sum(
            1 for r in classification_results.values() if r.classification == CycleClassification.FLAPPING
        )
        benign_transient_cycles = sum(
            1 for r in classification_results.values() if r.classification == CycleClassification.BENIGN_TRANSIENT
        )
        actionable_cycles = sum(
            1 for r in classification_results.values() if r.classification == CycleClassification.ACTIONABLE
        )

        avg_confidence = statistics.mean([r.confidence for r in classification_results.values()])

        return {
            "total_cycles": total_cycles,
            "flapping_cycles": flapping_cycles,
            "benign_transient_cycles": benign_transient_cycles,
            "actionable_cycles": actionable_cycles,
            "flapping_rate": flapping_cycles / max(total_cycles, 1),
            "benign_transient_rate": benign_transient_cycles / max(total_cycles, 1),
            "actionable_rate": actionable_cycles / max(total_cycles, 1),
            "avg_confidence": avg_confidence,
        }

    def generate_recommendations(
        self,
        monitor_id: str,
        classification_results: dict[str, ClassificationResult],
        monitor_metadata: dict[str, any] | None = None,
    ) -> list[dict[str, any]]:
        """Generate actionable recommendations based on classification results."""
        recommendations = []
        summary = self.generate_classification_summary(classification_results)

        # High flapping rate recommendations
        if summary["flapping_rate"] > 0.5:
            recommendations.append(
                {
                    "type": "threshold_adjustment",
                    "priority": "high",
                    "action": "Increase debounce window or add hysteresis",
                    "reason": f"{summary['flapping_cycles']}/{summary['total_cycles']} cycles are flapping",
                    "details": {
                        "suggested_debounce_seconds": self.config.debounce_seconds * 2,
                        "suggested_hysteresis": True,
                    },
                }
            )

        # High benign transient rate recommendations
        if summary["benign_transient_rate"] > 0.7:
            recommendations.append(
                {
                    "type": "notification_policy",
                    "priority": "medium",
                    "action": "Route to dashboard instead of paging",
                    "reason": f"{summary['benign_transient_cycles']}/{summary['total_cycles']} cycles are benign transients",
                    "details": {
                        "suggested_notification_level": "info",
                        "consider_dashboard_only": True,
                    },
                }
            )

        # Low actionable rate with high confidence
        if summary["actionable_rate"] < 0.3 and summary["avg_confidence"] > 0.8:
            recommendations.append(
                {
                    "type": "monitor_review",
                    "priority": "medium",
                    "action": "Consider monitor removal or threshold adjustment",
                    "reason": f"Only {summary['actionable_cycles']}/{summary['total_cycles']} cycles require action",
                    "details": {
                        "removal_candidate": True,
                        "confidence_score": summary["avg_confidence"],
                    },
                }
            )

        return recommendations
