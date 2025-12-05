"""JIRA Domain Metrics Registry

This module provides a central registry for JIRA metric definitions, standardizing
metric names, units, descriptions, and validation schemas across all JIRA commands.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class MetricDefinition:
    """Definition of a JIRA metric with metadata."""

    name: str
    unit: str
    description: str
    category: str
    min_value: float | None = None
    max_value: float | None = None
    decimal_places: int = 1
    is_percentage: bool = False
    aggregation_method: str = "latest"  # latest, sum, avg, max, min


class JiraMetricsRegistry:
    """Central registry for JIRA domain metrics.

    Provides standardized metric definitions and validation for all JIRA commands
    including cycle time, issue adherence, and net flow metrics.
    """

    def __init__(self):
        self._metrics = self._initialize_metrics()

    def _initialize_metrics(self) -> dict[str, MetricDefinition]:
        """Initialize all JIRA metric definitions."""
        metrics = {}

        # Cycle Time Metrics
        metrics.update(self._get_cycle_time_metrics())

        # Issue Adherence Metrics
        metrics.update(self._get_adherence_metrics())

        # Net Flow Metrics
        metrics.update(self._get_net_flow_metrics())

        # Team Segmentation Metrics
        metrics.update(self._get_segmentation_metrics())

        # Trending and Statistical Metrics
        metrics.update(self._get_trending_metrics())

        return metrics

    def _get_cycle_time_metrics(self) -> dict[str, MetricDefinition]:
        """Get cycle time metric definitions."""
        return {
            "jira.cycle_time.average_hours": MetricDefinition(
                name="jira.cycle_time.average_hours",
                unit="hours",
                description="Average time from start to completion of issues",
                category="cycle_time",
                min_value=0.0,
                decimal_places=1,
            ),
            "jira.cycle_time.median_hours": MetricDefinition(
                name="jira.cycle_time.median_hours",
                unit="hours",
                description="Median time from start to completion of issues",
                category="cycle_time",
                min_value=0.0,
                decimal_places=1,
            ),
            "jira.cycle_time.p95_hours": MetricDefinition(
                name="jira.cycle_time.p95_hours",
                unit="hours",
                description="95th percentile cycle time",
                category="cycle_time",
                min_value=0.0,
                decimal_places=1,
            ),
            "jira.cycle_time.standard_deviation_hours": MetricDefinition(
                name="jira.cycle_time.standard_deviation_hours",
                unit="hours",
                description="Standard deviation of cycle times",
                category="cycle_time",
                min_value=0.0,
                decimal_places=1,
            ),
            "jira.cycle_time.throughput": MetricDefinition(
                name="jira.cycle_time.throughput",
                unit="issues",
                description="Number of issues completed in the period",
                category="cycle_time",
                min_value=0,
                decimal_places=0,
                aggregation_method="sum",
            ),
            "jira.cycle_time.sle_compliance_percent": MetricDefinition(
                name="jira.cycle_time.sle_compliance_percent",
                unit="percent",
                description="Percentage of issues meeting SLE targets",
                category="cycle_time",
                min_value=0.0,
                max_value=100.0,
                is_percentage=True,
            ),
        }

    def _get_adherence_metrics(self) -> dict[str, MetricDefinition]:
        """Get issue adherence metric definitions."""
        return {
            "jira.issue_adherence.adherence_rate": MetricDefinition(
                name="jira.issue_adherence.adherence_rate",
                unit="percent",
                description="Percentage of issues completed on or before due date",
                category="adherence",
                min_value=0.0,
                max_value=100.0,
                is_percentage=True,
            ),
            "jira.issue_adherence.weighted_adherence": MetricDefinition(
                name="jira.issue_adherence.weighted_adherence",
                unit="percent",
                description="Weighted adherence score with penalties for lateness",
                category="adherence",
                min_value=0.0,
                max_value=100.0,
                is_percentage=True,
            ),
            "jira.issue_adherence.due_date_coverage_percent": MetricDefinition(
                name="jira.issue_adherence.due_date_coverage_percent",
                unit="percent",
                description="Percentage of issues with due dates set",
                category="adherence",
                min_value=0.0,
                max_value=100.0,
                is_percentage=True,
            ),
            "jira.issue_adherence.on_time_completion_count": MetricDefinition(
                name="jira.issue_adherence.on_time_completion_count",
                unit="issues",
                description="Number of issues completed on time",
                category="adherence",
                min_value=0,
                decimal_places=0,
                aggregation_method="sum",
            ),
            "jira.issue_adherence.late_completion_count": MetricDefinition(
                name="jira.issue_adherence.late_completion_count",
                unit="issues",
                description="Number of issues completed late",
                category="adherence",
                min_value=0,
                decimal_places=0,
                aggregation_method="sum",
            ),
            "jira.issue_adherence.early_completion_count": MetricDefinition(
                name="jira.issue_adherence.early_completion_count",
                unit="issues",
                description="Number of issues completed early",
                category="adherence",
                min_value=0,
                decimal_places=0,
                aggregation_method="sum",
            ),
            "jira.issue_adherence.avg_days_late": MetricDefinition(
                name="jira.issue_adherence.avg_days_late",
                unit="days",
                description="Average days late for late completions",
                category="adherence",
                decimal_places=1,
            ),
        }

    def _get_net_flow_metrics(self) -> dict[str, MetricDefinition]:
        """Get net flow metric definitions."""
        return {
            "jira.net_flow.net_flow": MetricDefinition(
                name="jira.net_flow.net_flow",
                unit="issues",
                description="Difference between arrival rate and throughput",
                category="net_flow",
                decimal_places=0,
            ),
            "jira.net_flow.flow_ratio": MetricDefinition(
                name="jira.net_flow.flow_ratio",
                unit="percent",
                description="Ratio of throughput to arrival rate",
                category="net_flow",
                min_value=0.0,
                is_percentage=True,
            ),
            "jira.net_flow.arrival_rate": MetricDefinition(
                name="jira.net_flow.arrival_rate",
                unit="issues",
                description="Number of new issues created in the period",
                category="net_flow",
                min_value=0,
                decimal_places=0,
                aggregation_method="sum",
            ),
            "jira.net_flow.throughput": MetricDefinition(
                name="jira.net_flow.throughput",
                unit="issues",
                description="Number of issues completed in the period",
                category="net_flow",
                min_value=0,
                decimal_places=0,
                aggregation_method="sum",
            ),
            "jira.net_flow.flow_efficiency": MetricDefinition(
                name="jira.net_flow.flow_efficiency",
                unit="percent",
                description="Efficiency of work flow through the system",
                category="net_flow",
                min_value=0.0,
                max_value=100.0,
                is_percentage=True,
            ),
            "jira.net_flow.volatility_score": MetricDefinition(
                name="jira.net_flow.volatility_score",
                unit="score",
                description="Volatility score for flow patterns",
                category="net_flow",
                min_value=0.0,
            ),
            "jira.net_flow.flow_debt": MetricDefinition(
                name="jira.net_flow.flow_debt",
                unit="issues",
                description="Cumulative flow debt over time",
                category="net_flow",
                decimal_places=0,
            ),
        }

    def _get_segmentation_metrics(self) -> dict[str, MetricDefinition]:
        """Get team/type segmentation metric definitions."""
        return {
            "jira.segmentation.team_cycle_time": MetricDefinition(
                name="jira.segmentation.team_cycle_time",
                unit="hours",
                description="Average cycle time for specific team",
                category="segmentation",
                min_value=0.0,
                decimal_places=1,
            ),
            "jira.segmentation.team_throughput": MetricDefinition(
                name="jira.segmentation.team_throughput",
                unit="issues",
                description="Throughput for specific team",
                category="segmentation",
                min_value=0,
                decimal_places=0,
                aggregation_method="sum",
            ),
            "jira.segmentation.issue_type_adherence": MetricDefinition(
                name="jira.segmentation.issue_type_adherence",
                unit="percent",
                description="Adherence rate for specific issue type",
                category="segmentation",
                min_value=0.0,
                max_value=100.0,
                is_percentage=True,
            ),
        }

    def _get_trending_metrics(self) -> dict[str, MetricDefinition]:
        """Get trending and statistical metric definitions."""
        return {
            "jira.trending.baseline_comparison": MetricDefinition(
                name="jira.trending.baseline_comparison",
                unit="percent",
                description="Comparison to baseline performance",
                category="trending",
                is_percentage=True,
            ),
            "jira.trending.trend_direction": MetricDefinition(
                name="jira.trending.trend_direction",
                unit="direction",
                description="Direction of trend (improving/worsening/stable)",
                category="trending",
                decimal_places=0,
            ),
            "jira.trending.volatility": MetricDefinition(
                name="jira.trending.volatility",
                unit="score",
                description="Volatility score for trend analysis",
                category="trending",
                min_value=0.0,
            ),
            "jira.trending.alert_threshold_breached": MetricDefinition(
                name="jira.trending.alert_threshold_breached",
                unit="boolean",
                description="Whether alert thresholds were breached",
                category="trending",
                decimal_places=0,
            ),
            "jira.statistical.confidence_interval_low": MetricDefinition(
                name="jira.statistical.confidence_interval_low",
                unit="value",
                description="Lower bound of confidence interval",
                category="statistical",
            ),
            "jira.statistical.confidence_interval_high": MetricDefinition(
                name="jira.statistical.confidence_interval_high",
                unit="value",
                description="Upper bound of confidence interval",
                category="statistical",
            ),
        }

    def get_metric_definition(self, metric_name: str) -> MetricDefinition | None:
        """Get metric definition by name.

        Args:
            metric_name: Name of the metric

        Returns:
            MetricDefinition if found, None otherwise
        """
        return self._metrics.get(metric_name)

    def get_metrics_by_category(self, category: str) -> list[MetricDefinition]:
        """Get all metrics in a specific category.

        Args:
            category: Category name (cycle_time, adherence, net_flow, etc.)

        Returns:
            List of metric definitions in the category
        """
        return [metric for metric in self._metrics.values() if metric.category == category]

    def get_all_categories(self) -> list[str]:
        """Get all available metric categories.

        Returns:
            List of category names
        """
        return list(set(metric.category for metric in self._metrics.values()))

    def validate_metric_value(self, metric_name: str, value: Any) -> bool:
        """Validate a metric value against its definition.

        Args:
            metric_name: Name of the metric
            value: Value to validate

        Returns:
            True if valid, False otherwise
        """
        definition = self.get_metric_definition(metric_name)
        if not definition:
            return False

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return False

        # Check min/max bounds
        if definition.min_value is not None and numeric_value < definition.min_value:
            return False

        if definition.max_value is not None and numeric_value > definition.max_value:
            return False

        return True

    def format_metric_value(self, metric_name: str, value: Any) -> str:
        """Format a metric value according to its definition.

        Args:
            metric_name: Name of the metric
            value: Value to format

        Returns:
            Formatted value string
        """
        definition = self.get_metric_definition(metric_name)
        if not definition:
            return str(value)

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return str(value)

        # Format with appropriate decimal places
        if definition.decimal_places == 0:
            formatted = f"{numeric_value:.0f}"
        else:
            formatted = f"{numeric_value:.{definition.decimal_places}f}"

        # Add percentage symbol if needed
        if definition.is_percentage:
            formatted += "%"

        return formatted

    def get_metric_summary(self) -> dict[str, Any]:
        """Get summary information about all metrics.

        Returns:
            Dictionary with metric counts and categories
        """
        categories = {}
        for metric in self._metrics.values():
            if metric.category not in categories:
                categories[metric.category] = []
            categories[metric.category].append(metric.name)

        return {
            "total_metrics": len(self._metrics),
            "total_categories": len(categories),
            "metrics_by_category": categories,
            "version": "1.0.0",
        }


# Global registry instance
JIRA_METRICS_REGISTRY = JiraMetricsRegistry()


def get_jira_metric_definition(metric_name: str) -> MetricDefinition | None:
    """Convenience function to get metric definition.

    Args:
        metric_name: Name of the metric

    Returns:
        MetricDefinition if found, None otherwise
    """
    return JIRA_METRICS_REGISTRY.get_metric_definition(metric_name)


def validate_jira_metric(metric_name: str, value: Any) -> bool:
    """Convenience function to validate metric value.

    Args:
        metric_name: Name of the metric
        value: Value to validate

    Returns:
        True if valid, False otherwise
    """
    return JIRA_METRICS_REGISTRY.validate_metric_value(metric_name, value)


def format_jira_metric(metric_name: str, value: Any) -> str:
    """Convenience function to format metric value.

    Args:
        metric_name: Name of the metric
        value: Value to format

    Returns:
        Formatted value string
    """
    return JIRA_METRICS_REGISTRY.format_metric_value(metric_name, value)
