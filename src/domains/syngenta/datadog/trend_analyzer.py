"""Temporal trend analysis system for tracking week-over-week changes in monitor health."""

from __future__ import annotations

import json
import os
import statistics
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from utils.logging.logging_manager import LogManager

from .enhanced_alert_cycle import WeeklyMonitorSnapshot, WeeklySummarySnapshot


class TrendDirection(Enum):
    """Trend direction indicators."""
    IMPROVING = "IMPROVING"
    DEGRADING = "DEGRADING"
    STABLE = "STABLE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@dataclass
class TrendMetric:
    """Individual metric trend analysis."""
    metric_name: str
    current_value: float
    previous_value: Optional[float]
    delta_absolute: Optional[float]
    delta_percentage: Optional[float]
    four_week_avg: Optional[float]
    direction: TrendDirection
    significance: float  # 0.0 - 1.0, statistical significance of change


@dataclass
class MonitorTrendAnalysis:
    """Trend analysis for a single monitor."""
    monitor_id: str
    monitor_name: Optional[str]
    current_week: str
    weeks_analyzed: int

    # Key trend metrics
    noise_score_trend: TrendMetric
    self_heal_rate_trend: TrendMetric
    health_score_trend: Optional[TrendMetric]
    cycles_per_week_trend: TrendMetric
    flapping_rate_trend: TrendMetric

    # Overall trend assessment
    overall_direction: TrendDirection
    trend_confidence: float
    notable_changes: List[str] = field(default_factory=list)


@dataclass
class TrendSummary:
    """Overall trend summary across all monitors."""
    analysis_week: str
    total_monitors: int
    weeks_available: int

    # Distribution of trend directions
    monitors_improving: int
    monitors_degrading: int
    monitors_stable: int
    monitors_insufficient_data: int

    # Key insights
    top_improvements: List[str] = field(default_factory=list)
    top_degradations: List[str] = field(default_factory=list)
    significant_changes: List[str] = field(default_factory=list)


class WeeklySnapshotManager:
    """Manages persistence and retrieval of weekly snapshots."""

    def __init__(self, storage_dir: str = "snapshots"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.logger = LogManager.get_instance().get_logger("WeeklySnapshotManager")

    def save_weekly_snapshots(
        self,
        iso_week: str,
        monitor_snapshots: List[WeeklyMonitorSnapshot],
        summary_snapshot: WeeklySummarySnapshot
    ) -> None:
        """Save weekly snapshots to persistent storage."""
        try:
            # Save monitor snapshots
            monitors_file = self.storage_dir / f"monitors_{iso_week}.json"
            monitors_data = [asdict(snapshot) for snapshot in monitor_snapshots]

            with open(monitors_file, 'w') as f:
                json.dump({
                    "metadata": {
                        "iso_week": iso_week,
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "snapshot_count": len(monitor_snapshots)
                    },
                    "monitors": monitors_data
                }, f, indent=2)

            # Save summary snapshot
            summary_file = self.storage_dir / f"summary_{iso_week}.json"
            with open(summary_file, 'w') as f:
                json.dump({
                    "metadata": {
                        "iso_week": iso_week,
                        "generated_at": datetime.now(timezone.utc).isoformat()
                    },
                    "summary": asdict(summary_snapshot)
                }, f, indent=2)

            self.logger.info(f"Saved weekly snapshots for {iso_week}: {len(monitor_snapshots)} monitors")

        except Exception as e:
            self.logger.error(f"Failed to save weekly snapshots for {iso_week}: {e}")
            raise

    def load_weekly_snapshots(
        self,
        iso_week: str
    ) -> Tuple[List[WeeklyMonitorSnapshot], Optional[WeeklySummarySnapshot]]:
        """Load weekly snapshots from persistent storage."""
        try:
            # Load monitor snapshots
            monitors_file = self.storage_dir / f"monitors_{iso_week}.json"
            if not monitors_file.exists():
                return [], None

            with open(monitors_file, 'r') as f:
                monitors_data = json.load(f)

            monitor_snapshots = [
                WeeklyMonitorSnapshot(**monitor_data)
                for monitor_data in monitors_data.get("monitors", [])
            ]

            # Load summary snapshot
            summary_file = self.storage_dir / f"summary_{iso_week}.json"
            summary_snapshot = None
            if summary_file.exists():
                with open(summary_file, 'r') as f:
                    summary_data = json.load(f)
                summary_snapshot = WeeklySummarySnapshot(**summary_data.get("summary", {}))

            return monitor_snapshots, summary_snapshot

        except Exception as e:
            self.logger.error(f"Failed to load weekly snapshots for {iso_week}: {e}")
            return [], None

    def get_available_weeks(self, limit: Optional[int] = None) -> List[str]:
        """Get list of available weeks, sorted by date (newest first)."""
        try:
            monitor_files = list(self.storage_dir.glob("monitors_*.json"))
            weeks = []

            for file in monitor_files:
                # Extract week from filename: monitors_2025-W42.json
                filename = file.stem  # monitors_2025-W42
                if filename.startswith("monitors_"):
                    week = filename[9:]  # 2025-W42
                    weeks.append(week)

            # Sort by year and week number
            weeks.sort(key=lambda w: (int(w.split('-W')[0]), int(w.split('-W')[1])), reverse=True)

            if limit:
                weeks = weeks[:limit]

            return weeks

        except Exception as e:
            self.logger.error(f"Failed to get available weeks: {e}")
            return []

    def cleanup_old_snapshots(self, keep_weeks: int = 12) -> None:
        """Remove old snapshot files beyond the retention period."""
        try:
            available_weeks = self.get_available_weeks()
            if len(available_weeks) <= keep_weeks:
                return

            weeks_to_delete = available_weeks[keep_weeks:]
            deleted_count = 0

            for week in weeks_to_delete:
                monitors_file = self.storage_dir / f"monitors_{week}.json"
                summary_file = self.storage_dir / f"summary_{week}.json"

                if monitors_file.exists():
                    monitors_file.unlink()
                    deleted_count += 1

                if summary_file.exists():
                    summary_file.unlink()

            self.logger.info(f"Cleaned up {deleted_count} old snapshot weeks")

        except Exception as e:
            self.logger.error(f"Failed to cleanup old snapshots: {e}")


class TrendAnalyzer:
    """Analyzes temporal trends in monitor performance metrics."""

    def __init__(self, snapshot_manager: WeeklySnapshotManager, min_weeks: int = 3):
        self.snapshot_manager = snapshot_manager
        self.min_weeks = min_weeks
        self.logger = LogManager.get_instance().get_logger("TrendAnalyzer")

    def analyze_monitor_trends(
        self,
        monitor_id: str,
        current_week: str,
        lookback_weeks: int = 8
    ) -> Optional[MonitorTrendAnalysis]:
        """Analyze trends for a specific monitor."""
        # Get historical data
        historical_data = self._get_monitor_history(monitor_id, current_week, lookback_weeks)

        if len(historical_data) < self.min_weeks:
            return None

        # Calculate trend metrics
        noise_trend = self._calculate_trend_metric("noise_score", historical_data)
        self_heal_trend = self._calculate_trend_metric("self_heal_rate", historical_data)
        health_trend = self._calculate_trend_metric("health_score", historical_data)
        cycles_trend = self._calculate_trend_metric("cycles_count", historical_data)

        # Calculate flapping rate trend
        flapping_data = []
        for week_data in historical_data:
            total_cycles = week_data.get("cycles_count", 1)
            flapping_cycles = week_data.get("flapping_cycles", 0)
            flapping_rate = flapping_cycles / max(total_cycles, 1)
            flapping_data.append({"week": week_data["iso_week"], "value": flapping_rate})

        flapping_trend = self._calculate_trend_metric_from_data("flapping_rate", flapping_data)

        # Determine overall trend direction
        overall_direction, trend_confidence = self._calculate_overall_trend(
            [noise_trend, self_heal_trend, health_trend, cycles_trend, flapping_trend]
        )

        # Generate notable changes
        notable_changes = self._identify_notable_changes([
            noise_trend, self_heal_trend, health_trend, cycles_trend, flapping_trend
        ])

        current_data = historical_data[-1]  # Most recent week
        return MonitorTrendAnalysis(
            monitor_id=monitor_id,
            monitor_name=current_data.get("monitor_name"),
            current_week=current_week,
            weeks_analyzed=len(historical_data),
            noise_score_trend=noise_trend,
            self_heal_rate_trend=self_heal_trend,
            health_score_trend=health_trend,
            cycles_per_week_trend=cycles_trend,
            flapping_rate_trend=flapping_trend,
            overall_direction=overall_direction,
            trend_confidence=trend_confidence,
            notable_changes=notable_changes
        )

    def analyze_overall_trends(
        self,
        current_week: str,
        lookback_weeks: int = 8
    ) -> TrendSummary:
        """Analyze trends across all monitors."""
        # Get available weeks
        available_weeks = self.snapshot_manager.get_available_weeks(lookback_weeks + 1)
        if not available_weeks or current_week not in available_weeks:
            return TrendSummary(
                analysis_week=current_week,
                total_monitors=0,
                weeks_available=0,
                monitors_improving=0,
                monitors_degrading=0,
                monitors_stable=0,
                monitors_insufficient_data=0
            )

        # Get all monitors from current week
        current_monitors, _ = self.snapshot_manager.load_weekly_snapshots(current_week)
        if not current_monitors:
            return TrendSummary(
                analysis_week=current_week,
                total_monitors=0,
                weeks_available=len(available_weeks),
                monitors_improving=0,
                monitors_degrading=0,
                monitors_stable=0,
                monitors_insufficient_data=0
            )

        # Analyze trends for each monitor
        trend_results = []
        for monitor in current_monitors:
            trend_analysis = self.analyze_monitor_trends(
                monitor.monitor_id,
                current_week,
                lookback_weeks
            )
            if trend_analysis:
                trend_results.append(trend_analysis)

        # Count trend directions
        monitors_improving = sum(1 for t in trend_results if t.overall_direction == TrendDirection.IMPROVING)
        monitors_degrading = sum(1 for t in trend_results if t.overall_direction == TrendDirection.DEGRADING)
        monitors_stable = sum(1 for t in trend_results if t.overall_direction == TrendDirection.STABLE)
        monitors_insufficient = len(current_monitors) - len(trend_results)

        # Identify top changes
        top_improvements = self._get_top_changes(trend_results, TrendDirection.IMPROVING)
        top_degradations = self._get_top_changes(trend_results, TrendDirection.DEGRADING)
        significant_changes = self._get_significant_changes(trend_results)

        return TrendSummary(
            analysis_week=current_week,
            total_monitors=len(current_monitors),
            weeks_available=len(available_weeks),
            monitors_improving=monitors_improving,
            monitors_degrading=monitors_degrading,
            monitors_stable=monitors_stable,
            monitors_insufficient_data=monitors_insufficient,
            top_improvements=top_improvements,
            top_degradations=top_degradations,
            significant_changes=significant_changes
        )

    def _get_monitor_history(
        self,
        monitor_id: str,
        current_week: str,
        lookback_weeks: int
    ) -> List[Dict[str, any]]:
        """Get historical data for a monitor."""
        available_weeks = self.snapshot_manager.get_available_weeks(lookback_weeks + 1)

        # Ensure we include current week and get the most recent weeks
        if current_week not in available_weeks:
            available_weeks = [current_week] + available_weeks

        historical_data = []
        for week in available_weeks[:lookback_weeks + 1]:
            monitor_snapshots, _ = self.snapshot_manager.load_weekly_snapshots(week)

            for snapshot in monitor_snapshots:
                if snapshot.monitor_id == monitor_id:
                    historical_data.append({
                        "iso_week": week,
                        "monitor_name": snapshot.monitor_name,
                        "noise_score": snapshot.noise_score,
                        "self_heal_rate": snapshot.self_heal_rate,
                        "health_score": snapshot.health_score,
                        "cycles_count": snapshot.cycles_count,
                        "flapping_cycles": snapshot.flapping_cycles,
                        "benign_transient_cycles": snapshot.benign_transient_cycles,
                        "actionable_cycles": snapshot.actionable_cycles
                    })
                    break

        # Sort by week (oldest first for trend calculation)
        historical_data.sort(key=lambda x: (
            int(x["iso_week"].split('-W')[0]),
            int(x["iso_week"].split('-W')[1])
        ))

        return historical_data

    def _calculate_trend_metric(
        self,
        metric_name: str,
        historical_data: List[Dict[str, any]]
    ) -> TrendMetric:
        """Calculate trend metric from historical data."""
        values = []
        weeks = []

        for data in historical_data:
            value = data.get(metric_name)
            if value is not None:
                values.append(float(value))
                weeks.append(data["iso_week"])

        return self._calculate_trend_metric_from_data(
            metric_name,
            [{"week": w, "value": v} for w, v in zip(weeks, values)]
        )

    def _calculate_trend_metric_from_data(
        self,
        metric_name: str,
        data_points: List[Dict[str, any]]
    ) -> TrendMetric:
        """Calculate trend metric from data points."""
        if len(data_points) < 2:
            return TrendMetric(
                metric_name=metric_name,
                current_value=data_points[0]["value"] if data_points else 0.0,
                previous_value=None,
                delta_absolute=None,
                delta_percentage=None,
                four_week_avg=None,
                direction=TrendDirection.INSUFFICIENT_DATA,
                significance=0.0
            )

        values = [dp["value"] for dp in data_points]
        current_value = values[-1]
        previous_value = values[-2] if len(values) >= 2 else None

        # Calculate deltas
        delta_absolute = None
        delta_percentage = None
        if previous_value is not None:
            delta_absolute = current_value - previous_value
            if previous_value != 0:
                delta_percentage = (delta_absolute / abs(previous_value)) * 100

        # Calculate 4-week average
        four_week_avg = None
        if len(values) >= 4:
            four_week_avg = statistics.mean(values[-4:])

        # Determine trend direction and significance
        direction, significance = self._calculate_trend_direction(values, metric_name)

        return TrendMetric(
            metric_name=metric_name,
            current_value=current_value,
            previous_value=previous_value,
            delta_absolute=delta_absolute,
            delta_percentage=delta_percentage,
            four_week_avg=four_week_avg,
            direction=direction,
            significance=significance
        )

    def _calculate_trend_direction(
        self,
        values: List[float],
        metric_name: str
    ) -> Tuple[TrendDirection, float]:
        """Calculate trend direction and statistical significance."""
        if len(values) < self.min_weeks:
            return TrendDirection.INSUFFICIENT_DATA, 0.0

        # Calculate linear regression slope
        n = len(values)
        x_values = list(range(n))
        x_mean = statistics.mean(x_values)
        y_mean = statistics.mean(values)

        # Calculate slope
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, values))
        denominator = sum((x - x_mean) ** 2 for x in x_values)

        if denominator == 0:
            return TrendDirection.STABLE, 0.0

        slope = numerator / denominator

        # Calculate correlation coefficient for significance
        y_variance = sum((y - y_mean) ** 2 for y in values)
        if y_variance == 0:
            return TrendDirection.STABLE, 1.0

        correlation = numerator / (denominator * y_variance) ** 0.5
        significance = abs(correlation)

        # Determine direction based on metric type and slope
        # For metrics where higher is better (health_score, self_heal_rate)
        # For metrics where lower is better (noise_score, flapping_rate)

        better_when_higher = metric_name in ["health_score", "self_heal_rate", "actionable_rate"]
        better_when_lower = metric_name in ["noise_score", "flapping_rate", "cycles_count"]

        # Threshold for considering change significant
        slope_threshold = 0.01  # Adjust based on metric scale

        if abs(slope) < slope_threshold or significance < 0.3:
            return TrendDirection.STABLE, significance

        if slope > 0:
            if better_when_higher:
                return TrendDirection.IMPROVING, significance
            elif better_when_lower:
                return TrendDirection.DEGRADING, significance
            else:
                return TrendDirection.STABLE, significance
        else:
            if better_when_higher:
                return TrendDirection.DEGRADING, significance
            elif better_when_lower:
                return TrendDirection.IMPROVING, significance
            else:
                return TrendDirection.STABLE, significance

    def _calculate_overall_trend(
        self,
        metrics: List[TrendMetric]
    ) -> Tuple[TrendDirection, float]:
        """Calculate overall trend direction from individual metrics."""
        # Filter out metrics with insufficient data
        valid_metrics = [m for m in metrics if m.direction != TrendDirection.INSUFFICIENT_DATA]

        if not valid_metrics:
            return TrendDirection.INSUFFICIENT_DATA, 0.0

        # Weight metrics by significance
        improving_score = sum(
            m.significance for m in valid_metrics
            if m.direction == TrendDirection.IMPROVING
        )
        degrading_score = sum(
            m.significance for m in valid_metrics
            if m.direction == TrendDirection.DEGRADING
        )
        stable_score = sum(
            m.significance for m in valid_metrics
            if m.direction == TrendDirection.STABLE
        )

        total_score = improving_score + degrading_score + stable_score
        if total_score == 0:
            return TrendDirection.STABLE, 0.0

        # Calculate confidence as the relative dominance of the winning direction
        if improving_score > degrading_score and improving_score > stable_score:
            confidence = improving_score / total_score
            return TrendDirection.IMPROVING, confidence
        elif degrading_score > stable_score:
            confidence = degrading_score / total_score
            return TrendDirection.DEGRADING, confidence
        else:
            confidence = stable_score / total_score
            return TrendDirection.STABLE, confidence

    def _identify_notable_changes(self, metrics: List[TrendMetric]) -> List[str]:
        """Identify notable changes in metrics."""
        notable = []

        for metric in metrics:
            if metric.direction == TrendDirection.INSUFFICIENT_DATA:
                continue

            # Large percentage changes
            if metric.delta_percentage and abs(metric.delta_percentage) > 20:
                direction_word = "increased" if metric.delta_percentage > 0 else "decreased"
                notable.append(
                    f"{metric.metric_name} {direction_word} by {abs(metric.delta_percentage):.1f}% week-over-week"
                )

            # High significance changes
            elif metric.significance > 0.7:
                if metric.direction == TrendDirection.IMPROVING:
                    notable.append(f"{metric.metric_name} showing strong improvement trend")
                elif metric.direction == TrendDirection.DEGRADING:
                    notable.append(f"{metric.metric_name} showing concerning degradation trend")

        return notable

    def _get_top_changes(
        self,
        trend_results: List[MonitorTrendAnalysis],
        direction: TrendDirection,
        limit: int = 5
    ) -> List[str]:
        """Get top monitors with specified trend direction."""
        filtered_monitors = [
            t for t in trend_results
            if t.overall_direction == direction and t.trend_confidence > 0.5
        ]

        # Sort by confidence
        filtered_monitors.sort(key=lambda t: t.trend_confidence, reverse=True)

        return [
            f"{t.monitor_name or t.monitor_id} (confidence: {t.trend_confidence:.2f})"
            for t in filtered_monitors[:limit]
        ]

    def _get_significant_changes(
        self,
        trend_results: List[MonitorTrendAnalysis],
        min_confidence: float = 0.8
    ) -> List[str]:
        """Get monitors with significant changes."""
        significant = []

        for trend in trend_results:
            if trend.trend_confidence < min_confidence:
                continue

            # Check for significant metric changes
            for metric in [
                trend.noise_score_trend,
                trend.health_score_trend,
                trend.flapping_rate_trend
            ]:
                if not metric or not metric.delta_percentage:
                    continue

                if abs(metric.delta_percentage) > 30:
                    change_type = "improvement" if metric.direction == TrendDirection.IMPROVING else "degradation"
                    significant.append(
                        f"{trend.monitor_name or trend.monitor_id}: "
                        f"{metric.metric_name} {change_type} of {abs(metric.delta_percentage):.1f}%"
                    )

        return significant[:10]  # Limit to top 10


def get_current_iso_week() -> str:
    """Get current ISO week string (YYYY-WXX format)."""
    now = datetime.now(timezone.utc)
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"