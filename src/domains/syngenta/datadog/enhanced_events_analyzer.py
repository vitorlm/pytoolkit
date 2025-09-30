"""Enhanced Datadog Events Analyzer with auto-healing classification and trend analysis."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from utils.logging.logging_manager import LogManager

from .alert_classifier import AlertCycleClassifier, ClassificationConfig
from .config import ObservabilityConfig, load_config
from .enhanced_alert_cycle import (
    EnhancedAlertCycle,
    EnhancedLifecycleEvent,
    WeeklyMonitorSnapshot,
    WeeklySummarySnapshot,
)
from .events_analyzer import DatadogEventsAnalyzer  # Import base analyzer
from .trend_analyzer import TrendAnalyzer, WeeklySnapshotManager, get_current_iso_week


class EnhancedDatadogEventsAnalyzer(DatadogEventsAnalyzer):
    """Enhanced analyzer with auto-healing classification and trend analysis."""

    def __init__(
        self,
        events_data: List[Dict[str, Any]],
        *,
        analysis_period_days: int = 30,
        deleted_monitors: List[str] = None,
        config: Optional[ObservabilityConfig] = None,
    ) -> None:
        # Initialize base analyzer
        super().__init__(
            events_data,
            analysis_period_days=analysis_period_days,
            deleted_monitors=deleted_monitors,
        )

        self.config = config or load_config()
        self.logger = LogManager.get_instance().get_logger(
            "EnhancedDatadogEventsAnalyzer"
        )

        # Initialize enhanced components
        classification_config = ClassificationConfig(
            flap_window_minutes=self.config.flapping.flap_window_minutes,
            flap_min_cycles=self.config.flapping.flap_min_cycles,
            flap_max_transitions_per_hour=self.config.flapping.flap_max_transitions_per_hour,
            transient_max_duration_seconds=self.config.transient.transient_max_duration_seconds,
            actionable_min_duration_seconds=self.config.actionable.actionable_min_duration_seconds,
            business_hours_start=self.config.business_hours.start_hour,
            business_hours_end=self.config.business_hours.end_hour,
        )

        self.classifier = AlertCycleClassifier(classification_config)

        # Initialize trend analysis components
        self.snapshot_manager = WeeklySnapshotManager(
            storage_dir=self.config.snapshots_dir
        )
        self.trend_analyzer = TrendAnalyzer(
            self.snapshot_manager,
            min_weeks=self.config.trend_analysis.min_weeks_for_trends,
        )

        # Enhanced cycle storage
        self.enhanced_cycles: Dict[str, EnhancedAlertCycle] = {}
        self.classification_results: Dict[str, Dict[str, Any]] = {}

        # Convert existing cycles to enhanced cycles
        self._convert_to_enhanced_cycles()

    def _convert_to_enhanced_cycles(self) -> None:
        """Convert existing alert cycles to enhanced format."""
        for cycle_key, cycle in self.alert_cycles.items():
            # Convert events
            enhanced_events = []
            for event in cycle.events:
                enhanced_event = EnhancedLifecycleEvent(
                    raw=event.raw,
                    timestamp=event.timestamp,
                    timestamp_epoch=event.timestamp_epoch,
                    monitor_id=event.monitor_id,
                    monitor_name=event.monitor_name,
                    alert_cycle_key=event.alert_cycle_key,
                    source_state=event.source_state,
                    destination_state=event.destination_state,
                    transition_type=event.transition_type,
                    status=event.status,
                    team=event.team,
                    env=event.env,
                    duration_seconds=event.duration_seconds,
                    priority=event.priority,
                    # Enhanced fields (inferred from existing data)
                    was_paged=self._infer_paged_status(event),
                    human_action_taken=None,  # Will be inferred during classification
                    correlation_id=None,
                    severity_level=self._infer_severity_level(event),
                )
                enhanced_events.append(enhanced_event)

            # Create enhanced cycle
            enhanced_cycle = EnhancedAlertCycle(
                key=cycle.key,
                monitor_id=cycle.monitor_id,
                monitor_name=cycle.monitor_name,
                events=enhanced_events,
            )

            self.enhanced_cycles[cycle_key] = enhanced_cycle

    def _infer_paged_status(self, event) -> Optional[bool]:
        """Infer if event resulted in paging based on metadata."""
        # Check priority level
        if hasattr(event, "priority") and event.priority and event.priority >= 3:
            return True

        # Check alert type
        alert_type = event.raw.get("alert_type") or ""
        if isinstance(alert_type, str) and (
            "error" in alert_type.lower() or "critical" in alert_type.lower()
        ):
            return True

        return None

    def _infer_severity_level(self, event) -> Optional[str]:
        """Infer severity level from event data."""
        # Check destination state
        if hasattr(event, "destination_state"):
            state = (event.destination_state or "").upper()
            if state == "ALERT":
                return "HIGH"
            elif state == "WARN":
                return "MEDIUM"
            elif state == "OK":
                return "LOW"

        return None

    def analyze_enhanced_alert_quality(self) -> Dict[str, Any]:
        """Analyze alert quality with enhanced classification."""
        per_monitor: Dict[str, Dict[str, Any]] = {}

        for monitor_id in self.monitor_cycles.keys():
            monitor_cycles = [
                self.enhanced_cycles[cycle.key]
                for cycle in self.monitor_cycles[monitor_id]
                if cycle.key in self.enhanced_cycles
            ]

            if not monitor_cycles:
                continue

            # Classify cycles for this monitor
            classifications = self.classifier.classify_monitor_cycles(
                monitor_id, monitor_cycles
            )

            # Store classification results
            self.classification_results[monitor_id] = classifications

            # Calculate enhanced metrics
            enhanced_metrics = self._compute_enhanced_monitor_quality(
                monitor_id, monitor_cycles, classifications
            )
            per_monitor[monitor_id] = enhanced_metrics

        # Update internal storage
        self._monitor_metrics = per_monitor

        # Calculate overall metrics
        overall = self._aggregate_enhanced_quality_metrics(per_monitor.values())

        return {
            "overall": overall,
            "per_monitor": per_monitor,
            "classification_summary": self._generate_classification_summary(),
        }

    def _compute_enhanced_monitor_quality(
        self,
        monitor_id: str,
        cycles: List[EnhancedAlertCycle],
        classifications: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Compute enhanced quality metrics including classification results."""
        # Use the original alert cycles from the base class
        original_cycles = self.monitor_cycles.get(monitor_id, [])
        base_metrics = self._compute_monitor_quality(monitor_id, original_cycles)

        # Add enhanced classification metrics
        classification_summary = self.classifier.generate_classification_summary(
            classifications
        )

        enhanced_metrics = base_metrics.copy()
        enhanced_metrics.update(
            {
                # Classification counts
                "flapping_cycles": classification_summary["flapping_cycles"],
                "benign_transient_cycles": classification_summary[
                    "benign_transient_cycles"
                ],
                "actionable_cycles": classification_summary["actionable_cycles"],
                # Classification rates
                "flapping_rate": classification_summary["flapping_rate"],
                "benign_transient_rate": classification_summary[
                    "benign_transient_rate"
                ],
                "actionable_rate": classification_summary["actionable_rate"],
                # Average classification confidence
                "classification_confidence": classification_summary["avg_confidence"],
                # Business hours analysis
                "business_hours_cycles": sum(
                    1 for cycle in cycles if cycle.is_business_hours_cycle()
                ),
                "business_hours_percentage": (
                    sum(1 for cycle in cycles if cycle.is_business_hours_cycle())
                    / max(len(cycles), 1)
                    * 100
                ),
            }
        )

        return enhanced_metrics

    def _aggregate_enhanced_quality_metrics(
        self, monitor_metrics: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Aggregate enhanced quality metrics across monitors."""
        base_overall = self._aggregate_quality_metrics(monitor_metrics)

        if not monitor_metrics:
            base_overall.update(
                {
                    "total_flapping_cycles": 0,
                    "total_benign_transient_cycles": 0,
                    "total_actionable_cycles": 0,
                    "avg_flapping_rate": 0.0,
                    "avg_benign_transient_rate": 0.0,
                    "avg_actionable_rate": 0.0,
                    "avg_classification_confidence": 0.0,
                }
            )
            return base_overall

        # Calculate enhanced aggregates
        flapping_cycles = [m.get("flapping_cycles", 0) for m in monitor_metrics]
        benign_cycles = [m.get("benign_transient_cycles", 0) for m in monitor_metrics]
        actionable_cycles = [m.get("actionable_cycles", 0) for m in monitor_metrics]

        flapping_rates = [m.get("flapping_rate", 0.0) for m in monitor_metrics]
        benign_rates = [m.get("benign_transient_rate", 0.0) for m in monitor_metrics]
        actionable_rates = [m.get("actionable_rate", 0.0) for m in monitor_metrics]

        confidences = [m.get("classification_confidence", 0.0) for m in monitor_metrics]

        enhanced_metrics = {
            "total_flapping_cycles": sum(flapping_cycles),
            "total_benign_transient_cycles": sum(benign_cycles),
            "total_actionable_cycles": sum(actionable_cycles),
            "avg_flapping_rate": sum(flapping_rates) / len(flapping_rates),
            "avg_benign_transient_rate": sum(benign_rates) / len(benign_rates),
            "avg_actionable_rate": sum(actionable_rates) / len(actionable_rates),
            "avg_classification_confidence": sum(confidences) / len(confidences),
        }

        base_overall.update(enhanced_metrics)
        return base_overall

    def _generate_classification_summary(self) -> Dict[str, Any]:
        """Generate overall classification summary across all monitors."""
        all_classifications = {}
        for monitor_classifications in self.classification_results.values():
            all_classifications.update(monitor_classifications)

        return self.classifier.generate_classification_summary(all_classifications)

    def generate_enhanced_recommendations(self) -> Dict[str, Any]:
        """Generate enhanced recommendations with classification insights."""
        recommendations = {
            "flapping_mitigation": [],
            "benign_transient_policies": [],
            "threshold_adjustments": [],
            "automation_opportunities": [],
        }

        for monitor_id, classifications in self.classification_results.items():
            monitor_recs = self.classifier.generate_recommendations(
                monitor_id, classifications, self._monitor_metrics.get(monitor_id, {})
            )

            # Categorize recommendations
            for rec in monitor_recs:
                rec_type = rec.get("type", "other")
                if rec_type == "threshold_adjustment":
                    recommendations["threshold_adjustments"].append(
                        {
                            "monitor_id": monitor_id,
                            "monitor_name": self._monitor_metrics.get(
                                monitor_id, {}
                            ).get("monitor_name"),
                            **rec,
                        }
                    )
                elif rec_type == "notification_policy":
                    recommendations["benign_transient_policies"].append(
                        {
                            "monitor_id": monitor_id,
                            "monitor_name": self._monitor_metrics.get(
                                monitor_id, {}
                            ).get("monitor_name"),
                            **rec,
                        }
                    )

        return recommendations

    def create_weekly_snapshot(
        self, iso_week: Optional[str] = None
    ) -> Tuple[List[WeeklyMonitorSnapshot], WeeklySummarySnapshot]:
        """Create weekly snapshots for trend analysis."""
        if iso_week is None:
            iso_week = get_current_iso_week()

        year, week = iso_week.split("-W")
        year, week = int(year), int(week)

        monitor_snapshots = []

        # Create snapshot for each monitor
        for monitor_id, metrics in self._monitor_metrics.items():
            snapshot = WeeklyMonitorSnapshot.from_monitor_metrics(
                monitor_id=monitor_id,
                monitor_name=metrics.get("monitor_name"),
                iso_week=iso_week,
                year=year,
                week=week,
                metrics=metrics,
            )
            monitor_snapshots.append(snapshot)

        # Create summary snapshot
        summary_snapshot = WeeklySummarySnapshot.from_monitor_snapshots(
            iso_week=iso_week, year=year, week=week, monitor_snapshots=monitor_snapshots
        )

        return monitor_snapshots, summary_snapshot

    def save_weekly_snapshots(self, iso_week: Optional[str] = None) -> None:
        """Save weekly snapshots for trend analysis."""
        monitor_snapshots, summary_snapshot = self.create_weekly_snapshot(iso_week)

        current_week = iso_week or get_current_iso_week()
        self.snapshot_manager.save_weekly_snapshots(
            current_week, monitor_snapshots, summary_snapshot
        )

        # Cleanup old snapshots if enabled
        if self.config.enable_snapshot_cleanup:
            self.snapshot_manager.cleanup_old_snapshots(
                keep_weeks=self.config.trend_analysis.max_retention_weeks
            )

    def analyze_trends(
        self, current_week: Optional[str] = None, lookback_weeks: Optional[int] = None
    ) -> Dict[str, Any]:
        """Analyze temporal trends across monitors."""
        current_week = current_week or get_current_iso_week()
        lookback_weeks = (
            lookback_weeks or self.config.trend_analysis.default_lookback_weeks
        )

        # Analyze overall trends
        trend_summary = self.trend_analyzer.analyze_overall_trends(
            current_week, lookback_weeks
        )

        # Analyze individual monitor trends
        monitor_trends = {}
        for monitor_id in self._monitor_metrics.keys():
            trend_analysis = self.trend_analyzer.analyze_monitor_trends(
                monitor_id, current_week, lookback_weeks
            )
            if trend_analysis:
                monitor_trends[monitor_id] = trend_analysis

        return {
            "summary": trend_summary,
            "per_monitor": monitor_trends,
            "analysis_period": {
                "current_week": current_week,
                "lookback_weeks": lookback_weeks,
                "total_weeks_analyzed": len(
                    self.snapshot_manager.get_available_weeks(lookback_weeks + 1)
                ),
            },
        }

    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """Generate comprehensive report with all enhanced features."""
        # Base analysis
        base_quality = self.analyze_alert_quality()

        # Enhanced analysis
        enhanced_quality = self.analyze_enhanced_alert_quality()

        # Recommendations
        recommendations = self.generate_enhanced_recommendations()

        # Temporal metrics
        temporal_metrics = self.calculate_temporal_metrics()

        # Trend analysis (if enabled)
        trends = None
        if self.config.enable_trend_analysis:
            try:
                trends = self.analyze_trends()
            except Exception as e:
                self.logger.warning(f"Trend analysis failed: {e}")

        # Detailed monitor statistics
        detailed_stats = self.generate_detailed_monitor_statistics()

        return {
            "base_quality": base_quality,
            "enhanced_quality": enhanced_quality,
            "recommendations": recommendations,
            "temporal_metrics": temporal_metrics,
            "trends": trends,
            "detailed_statistics": detailed_stats,
            "config_summary": {
                "analysis_period_days": self.analysis_period_days,
                "classification_enabled": True,
                "trend_analysis_enabled": self.config.enable_trend_analysis,
                "min_weeks_for_trends": self.config.trend_analysis.min_weeks_for_trends,
            },
        }
