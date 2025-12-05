"""Visualization Service - Comprehensive chart generation for team assessments.

Created in Phase 5 to consolidate and enhance visualization capabilities.

This service provides:
- Temporal trend visualizations (multi-year comparisons)
- Productivity dashboards (team and member level)
- Interactive HTML reports
- Enhanced chart generation with consistent styling
"""

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from utils.logging.logging_manager import LogManager

from ..core.assessment_report import (
    AvailabilityMetrics,
    MemberAnnualAssessment,
    TeamProductivityMetrics,
)
from .chart_mixin import ChartMixin


class VisualizationService(ChartMixin):
    """Service for generating comprehensive visualizations for team assessments.

    Extends ChartMixin with domain-specific visualization capabilities:
    - Temporal trend analysis charts
    - Productivity dashboards
    - Team comparison visualizations
    - Availability impact charts
    """

    def __init__(self, output_path: str = "output"):
        """Initialize the service.

        Args:
            output_path: Path to save generated visualizations
        """
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self._logger = LogManager.get_instance().get_logger("VisualizationService")
        self.logger.info(f"VisualizationService initialized with output: {self.output_path}")

    def plot_temporal_trend(
        self,
        member_name: str,
        assessments: list[MemberAnnualAssessment],
        metric_name: str,
        title: str | None = None,
        filename: str | None = None,
    ) -> str:
        """Plot temporal trend for a specific metric across multiple years.

        Args:
            member_name: Member name
            assessments: List of annual assessments sorted by year
            metric_name: Name of metric to plot (e.g., "overall_score", "adherence_rate")
            title: Custom chart title
            filename: Custom filename (auto-generated if not provided)

        Returns:
            Path to saved chart

        Example:
            >>> service = VisualizationService()
            >>> chart_path = service.plot_temporal_trend(
            ...     "John Doe",
            ...     [assessment_2023, assessment_2024],
            ...     "overall_score"
            ... )
        """
        self.logger.info(f"Plotting temporal trend for {member_name}: {metric_name}")

        if not assessments:
            self.logger.warning("No assessments provided for temporal trend")
            return ""

        # Extract years and values
        years = []
        values = []

        for assessment in sorted(assessments, key=lambda a: a.evaluation_period.year):
            year = assessment.evaluation_period.year

            # Extract metric value based on name
            value = self._extract_metric_value(assessment, metric_name)

            if value is not None:
                years.append(year)
                values.append(value)

        if not years:
            self.logger.warning(f"No data found for metric: {metric_name}")
            return ""

        # Create line plot
        fig, ax = plt.subplots(figsize=(10, 6))

        ax.plot(years, values, marker="o", linewidth=2, markersize=8, color="#2E86AB")
        ax.fill_between(years, values, alpha=0.3, color="#2E86AB")

        # Add value labels on points
        for year, value in zip(years, values):
            ax.text(year, value, f"{value:.1f}", ha="center", va="bottom", fontsize=10)

        ax.set_xlabel("Year", fontsize=12, fontweight="bold")
        ax.set_ylabel(metric_name.replace("_", " ").title(), fontsize=12, fontweight="bold")
        ax.set_title(
            title or f"{member_name} - {metric_name.replace('_', ' ').title()} Trend",
            fontsize=14,
            fontweight="bold",
        )
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.set_xticks(years)

        # Save plot
        if not filename:
            filename = f"trend_{member_name.replace(' ', '_')}_{metric_name}.png"

        self._save_plot(plt, filename)

        chart_path = str(self.output_path / filename)
        self.logger.info(f"✓ Temporal trend saved: {chart_path}")
        return chart_path

    def plot_multi_member_comparison(
        self,
        assessments: dict[str, MemberAnnualAssessment],
        metric_name: str,
        title: str | None = None,
        filename: str | None = None,
    ) -> str:
        """Plot comparison of a metric across multiple members.

        Args:
            assessments: Dict mapping member names to their assessments
            metric_name: Metric to compare
            title: Custom chart title
            filename: Custom filename

        Returns:
            Path to saved chart
        """
        self.logger.info(f"Plotting multi-member comparison: {metric_name}")

        if not assessments:
            self.logger.warning("No assessments provided for comparison")
            return ""

        # Extract member names and values
        members = []
        values = []

        for member_name, assessment in sorted(assessments.items()):
            value = self._extract_metric_value(assessment, metric_name)

            if value is not None:
                members.append(member_name)
                values.append(value)

        if not members:
            self.logger.warning(f"No data found for metric: {metric_name}")
            return ""

        # Create horizontal bar chart
        df = pd.DataFrame({"Member": members, "Value": values})

        self.plot_horizontal_bar_chart(
            df=df,
            x_col="Value",
            y_col="Member",
            title=title or f"{metric_name.replace('_', ' ').title()} by Member",
            filename=filename or f"comparison_{metric_name}.png",
        )

        chart_path = str(self.output_path / (filename or f"comparison_{metric_name}.png"))
        self.logger.info(f"✓ Multi-member comparison saved: {chart_path}")
        return chart_path

    def plot_productivity_dashboard(
        self,
        member_name: str,
        productivity_metrics: dict[str, float],
        availability_metrics: AvailabilityMetrics | None = None,
        filename: str | None = None,
    ) -> str:
        """Create comprehensive productivity dashboard for a member.

        Args:
            member_name: Member name
            productivity_metrics: Dict with productivity scores
            availability_metrics: Optional availability metrics
            filename: Custom filename

        Returns:
            Path to saved dashboard
        """
        self.logger.info(f"Creating productivity dashboard for {member_name}")

        # Create figure with subplots
        fig = plt.figure(figsize=(16, 10))
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

        # 1. Overall Productivity Gauge
        ax1 = fig.add_subplot(gs[0, 0])
        self._plot_gauge(ax1, productivity_metrics.get("overall_score", 0), "Overall Productivity")

        # 2. Epic vs Bug Productivity
        ax2 = fig.add_subplot(gs[0, 1])
        epic_score = productivity_metrics.get("epic_score", 0)
        bug_score = productivity_metrics.get("bug_score", 0)
        ax2.bar(["Epic", "Bug"], [epic_score, bug_score], color=["#2E86AB", "#A23B72"])
        ax2.set_ylabel("Score", fontweight="bold")
        ax2.set_title("Epic vs Bug Productivity", fontweight="bold")
        ax2.grid(axis="y", linestyle="--", alpha=0.6)

        # 3. Adherence Metrics
        ax3 = fig.add_subplot(gs[1, 0])
        adherence_rate = productivity_metrics.get("adherence_rate", 0)
        spillover_rate = productivity_metrics.get("spillover_rate", 0)
        ax3.bar(
            ["Adherence", "Spillover"],
            [adherence_rate, spillover_rate],
            color=["#06A77D", "#F18F01"],
        )
        ax3.set_ylabel("Percentage (%)", fontweight="bold")
        ax3.set_title("Adherence vs Spillover", fontweight="bold")
        ax3.grid(axis="y", linestyle="--", alpha=0.6)

        # 4. Collaboration Metrics
        ax4 = fig.add_subplot(gs[1, 1])
        collab_score = productivity_metrics.get("collaboration_score", 0)
        comment_count = productivity_metrics.get("comment_count", 0)
        ax4.bar(
            ["Collaboration\nScore", "Comment\nCount"],
            [collab_score, min(comment_count / 10, 100)],  # Normalize comments
            color=["#C73E1D", "#6A4C93"],
        )
        ax4.set_ylabel("Score / Normalized Count", fontweight="bold")
        ax4.set_title("Collaboration Metrics", fontweight="bold")
        ax4.grid(axis="y", linestyle="--", alpha=0.6)

        # 5. Availability (if provided)
        if availability_metrics:
            ax5 = fig.add_subplot(gs[2, :])

            # Create stacked bar
            available_pct = availability_metrics.availability_percentage
            absent_pct = 100 - available_pct

            ax5.barh([0], [available_pct], color="#06A77D", label="Available")
            ax5.barh([0], [absent_pct], left=[available_pct], color="#F18F01", label="Absent")

            ax5.set_xlim(0, 100)
            ax5.set_yticks([])
            ax5.set_xlabel("Percentage (%)", fontweight="bold")
            ax5.set_title(
                f"Availability: {available_pct:.1f}% ({availability_metrics.days_absent} days absent)",
                fontweight="bold",
            )
            ax5.legend(loc="upper right")
            ax5.grid(axis="x", linestyle="--", alpha=0.6)
        else:
            # Show summary metrics instead
            ax5 = fig.add_subplot(gs[2, :])
            ax5.axis("off")

            summary_text = self._format_summary_text(productivity_metrics)
            ax5.text(
                0.5,
                0.5,
                summary_text,
                ha="center",
                va="center",
                fontsize=12,
                family="monospace",
                bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
            )

        # Overall title
        fig.suptitle(
            f"{member_name} - Productivity Dashboard",
            fontsize=16,
            fontweight="bold",
            y=0.98,
        )

        # Save dashboard
        if not filename:
            filename = f"dashboard_{member_name.replace(' ', '_')}.png"

        filepath = self.output_path / filename
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        self.logger.info(f"✓ Productivity dashboard saved: {filepath}")
        return str(filepath)

    def plot_team_comparison(
        self,
        team_metrics: dict[str, TeamProductivityMetrics],
        metric_name: str = "epic_adherence_rate",
        title: str | None = None,
        filename: str | None = None,
    ) -> str:
        """Plot comparison across multiple teams/squads.

        Args:
            team_metrics: Dict mapping team names to their metrics
            metric_name: Metric to compare
            title: Custom chart title
            filename: Custom filename

        Returns:
            Path to saved chart
        """
        self.logger.info(f"Plotting team comparison: {metric_name}")

        if not team_metrics:
            self.logger.warning("No team metrics provided")
            return ""

        # Extract team names and values
        teams = []
        values = []

        for team_name, metrics in sorted(team_metrics.items()):
            value = getattr(metrics, metric_name, None)

            if value is not None:
                teams.append(team_name)
                values.append(value)

        if not teams:
            self.logger.warning(f"No data found for metric: {metric_name}")
            return ""

        # Create grouped bar chart
        fig, ax = plt.subplots(figsize=(12, 6))

        colors = plt.cm.Set3(np.linspace(0, 1, len(teams)))
        bars = ax.bar(teams, values, color=colors, edgecolor="black", linewidth=1.5)

        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"{height:.1f}",
                ha="center",
                va="bottom",
                fontsize=10,
                fontweight="bold",
            )

        ax.set_ylabel(metric_name.replace("_", " ").title(), fontsize=12, fontweight="bold")
        ax.set_xlabel("Team/Squad", fontsize=12, fontweight="bold")
        ax.set_title(
            title or f"{metric_name.replace('_', ' ').title()} by Team",
            fontsize=14,
            fontweight="bold",
        )
        ax.grid(axis="y", linestyle="--", alpha=0.6)
        plt.xticks(rotation=45, ha="right")

        # Save chart
        if not filename:
            filename = f"team_comparison_{metric_name}.png"

        filepath = self.output_path / filename
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        self.logger.info(f"✓ Team comparison saved: {filepath}")
        return str(filepath)

    def plot_availability_impact(
        self,
        member_availability: dict[str, AvailabilityMetrics],
        filename: str | None = None,
    ) -> str:
        """Plot availability impact across team members.

        Args:
            member_availability: Dict mapping member names to availability metrics
            filename: Custom filename

        Returns:
            Path to saved chart
        """
        self.logger.info("Plotting availability impact")

        if not member_availability:
            self.logger.warning("No availability data provided")
            return ""

        # Prepare data
        members = []
        available_days = []
        absent_days = []

        for member, metrics in sorted(member_availability.items()):
            members.append(member)
            available = metrics.total_working_days - metrics.days_absent
            available_days.append(available)
            absent_days.append(metrics.days_absent)

        # Create stacked horizontal bar chart
        fig, ax = plt.subplots(figsize=(12, max(6, len(members) * 0.5)))

        y_pos = np.arange(len(members))

        ax.barh(y_pos, available_days, label="Available", color="#06A77D")
        ax.barh(y_pos, absent_days, left=available_days, label="Absent", color="#F18F01")

        ax.set_yticks(y_pos)
        ax.set_yticklabels(members)
        ax.set_xlabel("Days", fontsize=12, fontweight="bold")
        ax.set_title("Team Availability Impact", fontsize=14, fontweight="bold")
        ax.legend(loc="upper right")
        ax.grid(axis="x", linestyle="--", alpha=0.6)

        # Save chart
        if not filename:
            filename = "availability_impact.png"

        filepath = self.output_path / filename
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        plt.close()

        self.logger.info(f"✓ Availability impact saved: {filepath}")
        return str(filepath)

    def _plot_gauge(self, ax: Any, value: float, label: str, max_value: float = 100) -> None:
        """Plot a gauge/speedometer chart.

        Args:
            ax: Matplotlib axis
            value: Current value
            label: Label for the gauge
            max_value: Maximum value for the gauge
        """
        # Normalize value to 0-1
        norm_value = min(value / max_value, 1.0)

        # Create gauge
        theta = np.linspace(0, np.pi, 100)

        # Background arc
        ax.plot(np.cos(theta), np.sin(theta), "lightgray", linewidth=20)

        # Value arc with color gradient
        color = self._get_performance_color(norm_value)
        value_theta = theta[: int(norm_value * len(theta))]
        ax.plot(np.cos(value_theta), np.sin(value_theta), color=color, linewidth=20)

        # Needle
        needle_angle = np.pi * (1 - norm_value)
        ax.plot(
            [0, 0.7 * np.cos(needle_angle)],
            [0, 0.7 * np.sin(needle_angle)],
            "black",
            linewidth=3,
        )
        ax.plot(0, 0, "o", color="black", markersize=10)

        # Value text
        ax.text(
            0,
            -0.3,
            f"{value:.1f}",
            ha="center",
            va="center",
            fontsize=24,
            fontweight="bold",
        )
        ax.text(0, -0.5, label, ha="center", va="center", fontsize=12, fontweight="bold")

        ax.set_xlim(-1.2, 1.2)
        ax.set_ylim(-0.6, 1.2)
        ax.axis("off")

    def _get_performance_color(self, norm_value: float) -> str:
        """Get color based on performance value (0-1)."""
        if norm_value >= 0.8:
            return "#06A77D"  # Green
        elif norm_value >= 0.6:
            return "#2E86AB"  # Blue
        elif norm_value >= 0.4:
            return "#F18F01"  # Orange
        else:
            return "#C73E1D"  # Red

    def _extract_metric_value(self, assessment: MemberAnnualAssessment, metric_name: str) -> float | None:
        """Extract metric value from assessment object."""
        # Try direct attribute access
        if hasattr(assessment, metric_name):
            return getattr(assessment, metric_name)

        # Try nested attributes
        if "." in metric_name:
            parts = metric_name.split(".")
            obj = assessment
            for part in parts:
                if hasattr(obj, part):
                    obj = getattr(obj, part)
                else:
                    return None
            return obj if isinstance(obj, (int, float)) else None

        return None

    def _format_summary_text(self, metrics: dict[str, float]) -> str:
        """Format metrics as summary text."""
        lines = []
        for key, value in sorted(metrics.items()):
            label = key.replace("_", " ").title()
            if isinstance(value, float):
                lines.append(f"{label}: {value:.2f}")
            else:
                lines.append(f"{label}: {value}")

        return "\n".join(lines)
