"""
Issue Resolution Time Chart Service

This service provides charting capabilities for issue resolution time analysis,
extending the ChartMixin with domain-specific chart types for resolution metrics.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, List, Optional

from domains.syngenta.team_assessment.services.chart_mixin import ChartMixin
from utils.logging.logging_manager import LogManager


class IssueResolutionTimeChartService(ChartMixin):
    """
    Chart service for issue resolution time analysis.
    Provides specialized charts for resolution time metrics grouped by issue type and priority.
    """

    def __init__(self, output_path: str = ""):
        """
        Initialize the chart service.

        Args:
            output_path (str): Path where charts will be saved
        """
        self.output_path = output_path
        self._logger = LogManager.get_instance().get_logger("IssueResolutionTimeChartService")

        # Ensure output directory exists
        if self.output_path and not os.path.exists(self.output_path):
            os.makedirs(self.output_path, exist_ok=True)

    @property
    def logger(self):
        """Get the logger instance."""
        return self._logger

    def _prepare_chart_data(self, stats_by_type_priority: Dict) -> pd.DataFrame:
        """
        Convert the nested statistics dictionary to a DataFrame suitable for charting.

        Args:
            stats_by_type_priority (Dict): Nested dict with issue_type -> priority -> metrics

        Returns:
            pd.DataFrame: Flattened data with columns for issue_type, priority, and all metrics
        """
        rows = []
        for issue_type, priorities in stats_by_type_priority.items():
            for priority, metrics in priorities.items():
                row = {
                    "issue_type": issue_type,
                    "priority": priority,
                    "priority_order": self._get_priority_order(priority),
                    **metrics,
                }
                rows.append(row)

        df = pd.DataFrame(rows)
        # Sort by issue type and priority order for consistent display
        df = df.sort_values(["issue_type", "priority_order"])
        return df

    def _get_priority_order(self, priority: str) -> int:
        """
        Convert priority string to numeric order for sorting.

        Args:
            priority (str): Priority string like "Critical [P1]", "High [P2]", etc.

        Returns:
            int: Numeric order (1 for P1, 2 for P2, etc.)
        """
        priority_map = {"Critical [P1]": 1, "High [P2]": 2, "Medium [P3]": 3, "Low [P4]": 4}
        return priority_map.get(priority, 999)  # Unknown priorities go to end

    def _get_priority_colors(self) -> Dict[str, str]:
        """Get consistent colors for different priorities."""
        return {
            "Critical [P1]": "#FF4444",  # Red
            "High [P2]": "#FF8800",  # Orange
            "Medium [P3]": "#FFDD00",  # Yellow
            "Low [P4]": "#44AA44",  # Green
        }

    def _get_issue_type_colors(self) -> Dict[str, str]:
        """Get consistent colors for different issue types."""
        return {"Bug": "#E74C3C", "Support": "#3498DB"}  # Red-ish  # Blue

    def plot_resolution_metrics_comparison(
        self, stats_by_type_priority: Dict, filename: str = "resolution_metrics_comparison.png"
    ) -> None:
        """
        Create separate grouped bar charts for each issue type comparing median_days,
        p90_days, and suggested_sla_days for each priority.

        Args:
            stats_by_type_priority (Dict): Statistics data
            filename (str): Base output filename (will be modified per issue type)
        """
        self.logger.info("Generating resolution metrics comparison charts by issue type")

        df = self._prepare_chart_data(stats_by_type_priority)

        if df.empty:
            self.logger.warning("No data available for resolution metrics comparison chart")
            return

        # Create separate chart for each issue type
        for issue_type in df["issue_type"].unique():
            issue_df = df[df["issue_type"] == issue_type].copy()

            if issue_df.empty:
                continue

            # Sort by priority order
            issue_df = issue_df.sort_values("priority_order")

            # Generate filename for this issue type
            base_name = filename.replace(".png", "")
            type_filename = f"{base_name}_{issue_type.lower()}.png"

            # Plot using the grouped bar chart from ChartMixin
            self.plot_grouped_bar_chart(
                df=issue_df,
                x_col="priority",
                series=["median_days", "p90_days", "suggested_sla_days"],
                series_labels=["Median Days", "P90 Days", "Suggested SLA Days"],
                colors=["#3498DB", "#E67E22", "#27AE60"],  # Blue, Orange, Green
                title=f"{issue_type} Issues - Resolution Time Metrics by Priority",
                xlabel="Priority",
                ylabel="Days",
                filename=type_filename,
                bar_width=0.25,
            )

    def plot_issue_count_distribution(
        self, stats_by_type_priority: Dict, filename: str = "issue_count_distribution.png"
    ) -> None:
        """
        Create separate bar charts showing issue count distribution by priority for each type.

        Args:
            stats_by_type_priority (Dict): Statistics data
            filename (str): Base output filename (will be modified per issue type)
        """
        self.logger.info("Generating issue count distribution charts by issue type")

        df = self._prepare_chart_data(stats_by_type_priority)

        if df.empty:
            self.logger.warning("No data available for issue count distribution chart")
            return

        priority_colors = self._get_priority_colors()

        # Create separate chart for each issue type
        for issue_type in df["issue_type"].unique():
            issue_df = df[df["issue_type"] == issue_type].copy()

            if issue_df.empty:
                continue

            # Sort by priority order
            issue_df = issue_df.sort_values("priority_order")

            # Generate filename for this issue type
            base_name = filename.replace(".png", "")
            type_filename = f"{base_name}_{issue_type.lower()}.png"

            # Create bar chart
            fig, ax = plt.subplots(figsize=(8, 6))

            colors = [priority_colors.get(p, "#CCCCCC") for p in issue_df["priority"]]

            bars = ax.bar(issue_df["priority"], issue_df["count"], color=colors, width=0.6)

            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.annotate(
                    f"{int(height)}",
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha="center",
                    va="bottom",
                )

            ax.set_title(
                f"{issue_type} Issues - Count Distribution by Priority",
                fontsize=16,
                fontweight="bold",
            )
            ax.set_xlabel("Priority", fontsize=12)
            ax.set_ylabel("Number of Issues", fontsize=12)
            ax.grid(True, axis="y", linestyle="--", alpha=0.6)

            # Rotate x labels for better readability
            plt.xticks(rotation=45, ha="right")

            self._save_plot(plt, type_filename)

    def plot_variability_analysis(
        self,
        stats_by_type_priority: Dict,
        raw_issues_data: List[Dict],
        filename: str = "variability_analysis.png",
    ) -> None:
        """
        Create separate scatter plots for each issue type showing p90_days vs. IQR (Q3-Q1) to
        identify groups with high variability where SLAs might be riskier.
        IQR is calculated from raw resolution_time_days data for each issue type and priority.
        """
        import numpy as np

        self.logger.info(
            "Generating variability analysis scatter plots by issue type (IQR from raw data)"
        )

        df = self._prepare_chart_data(stats_by_type_priority)

        if df.empty:
            self.logger.warning("No data available for variability analysis chart")
            return

        priority_markers = {
            "Critical [P1]": "o",  # Circle
            "High [P2]": "s",  # Square
            "Medium [P3]": "^",  # Triangle
            "Low [P4]": "D",  # Diamond
        }
        priority_colors = self._get_priority_colors()

        # Calculate IQR from raw data for each issue type and priority
        def _calculate_iqr_from_raw_data(issue_type: str, priority: str) -> float:
            """
            Calculate IQR (Q3 - Q1) from raw resolution_time_days data for a specific
            issue type and priority combination.
            """
            # Filter raw data for this issue type and priority
            filtered_data = [
                issue
                for issue in raw_issues_data
                if issue.get("issue_type") == issue_type
                and issue.get("priority") == priority
                and issue.get("resolution_time_days") is not None
            ]

            if not filtered_data:
                return 0.0

            # Extract resolution times
            times = [issue["resolution_time_days"] for issue in filtered_data]

            if len(times) < 2:
                return 0.0

            # Calculate Q1 and Q3 using NumPy
            q1 = np.percentile(times, 25)
            q3 = np.percentile(times, 75)

            return float(q3 - q1)

        # Create separate chart for each issue type
        for issue_type in df["issue_type"].unique():
            issue_df = df[df["issue_type"] == issue_type].copy()

            if issue_df.empty:
                continue

            # Calculate IQR for each row using raw data
            iqr_values = []
            for _, row in issue_df.iterrows():
                iqr = _calculate_iqr_from_raw_data(issue_type, row["priority"])
                iqr_values.append(iqr)
            issue_df["iqr"] = iqr_values

            # Generate filename for this issue type
            base_name = filename.replace(".png", "")
            type_filename = f"{base_name}_{issue_type.lower()}.png"

            fig, ax = plt.subplots(figsize=(8, 6))

            # Plot each priority for this issue type
            for priority in issue_df["priority"].unique():
                subset = issue_df[issue_df["priority"] == priority]
                if not subset.empty:
                    ax.scatter(
                        subset["iqr"],
                        subset["p90_days"],
                        c=priority_colors.get(priority, "#CCCCCC"),
                        marker=priority_markers.get(priority, "o"),
                        s=subset["count"] * 15,  # Size based on issue count
                        alpha=0.7,
                        label=priority,
                        edgecolors="black",
                        linewidth=0.5,
                    )

            ax.set_xlabel("IQR (Days)", fontsize=12)
            ax.set_ylabel("P90 Resolution Time (Days)", fontsize=12)
            ax.set_title(
                f"{issue_type} Issues - Variability Analysis (IQR)\n(Bubble size = Issue count)",
                fontsize=14,
                fontweight="bold",
            )
            ax.grid(True, linestyle="--", alpha=0.6)
            ax.legend(title="Priority", bbox_to_anchor=(1.05, 1), loc="upper left")

            self._save_plot(plt, type_filename)

    def plot_resolution_time_histogram(
        self, raw_issues_data: List[Dict], filename: str = "resolution_time_histogram.png"
    ) -> None:
        """
        Create histograms of resolution times by issue type and priority.
        Bin ranges: <1d, 1-2d, 2-3d, 3-5d, 5-10d, 10-20d, >20d.
        Uses _get_priority_colors for color consistency.
        """
        self.logger.info("Generating resolution time histograms by issue type and priority")

        if not raw_issues_data:
            self.logger.warning("No raw issue data available for histogram")
            return

        import pandas as pd

        df = pd.DataFrame(raw_issues_data)
        df = df[df["resolution_time_days"].notna()]
        if df.empty:
            self.logger.warning("No valid resolution time data for histogram")
            return

        bins = [0, 1, 2, 3, 5, 10, 20, float("inf")]
        bin_labels = ["<1d", "1-2d", "2-3d", "3-5d", "5-10d", "10-20d", ">20d"]
        priority_colors = self._get_priority_colors()

        for issue_type in df["issue_type"].unique():
            issue_df = df[df["issue_type"] == issue_type].copy()
            if issue_df.empty:
                continue
            # Group by priority
            for priority in issue_df["priority"].unique():
                prio_df = issue_df[issue_df["priority"] == priority].copy()
                if prio_df.empty:
                    continue
                prio_df.loc[:, "bucket"] = pd.cut(
                    prio_df["resolution_time_days"], bins=bins, labels=bin_labels, right=False
                )
                counts = prio_df["bucket"].value_counts().reindex(bin_labels, fill_value=0)
                color = priority_colors.get(priority, "#CCCCCC")
                fig, ax = plt.subplots(figsize=(8, 5))
                bars = ax.bar(bin_labels, counts.values, color=color, alpha=0.8)
                for bar, count in zip(bars, counts.values):
                    height = bar.get_height()
                    ax.annotate(
                        f"{int(count)}",
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha="center",
                        va="bottom",
                    )
                ax.set_title(f"{issue_type} - {priority} Resolution Time Histogram")
                ax.set_xlabel("Resolution Time Bucket")
                ax.set_ylabel("Number of Issues")
                ax.grid(True, axis="y", linestyle="--", alpha=0.6)
                plt.tight_layout()
                base_name = filename.replace(".png", "")
                type_filename = (
                    f"{base_name}_{issue_type.lower()}_"
                    f"{priority.replace(' ', '').replace('[', '').replace(']', '').lower()}.png"
                )
                self._save_plot(plt, type_filename)

    def plot_resolution_time_boxplot(
        self, raw_issues_data: List[Dict], filename: str = "resolution_time_boxplot.png"
    ) -> None:
        """
        Create separate boxplots for each issue type showing resolution time
        distribution by priority.

        Args:
            raw_issues_data (List[Dict]): List of individual issue data
            filename (str): Base output filename (will be modified per issue type)
        """
        self.logger.info("Generating resolution time boxplots by issue type")

        if not raw_issues_data:
            self.logger.warning("No raw issue data available for boxplot")
            return

        # Convert to DataFrame
        df = pd.DataFrame(raw_issues_data)
        df = df[df["resolution_time_days"].notna()]  # Remove null values

        if df.empty:
            self.logger.warning("No valid resolution time data for boxplot")
            return

        # Create separate boxplot for each issue type
        for issue_type in df["issue_type"].unique():
            issue_df = df[df["issue_type"] == issue_type].copy()

            if issue_df.empty:
                continue

            # Generate filename for this issue type
            base_name = filename.replace(".png", "")
            type_filename = f"{base_name}_{issue_type.lower()}.png"

            # Group data by priority for this issue type
            groups = issue_df.groupby("priority")["resolution_time_days"].apply(list).to_dict()

            if not groups:
                continue

            # Use ChartMixin boxplot method
            self.plot_boxplot_chart(
                data=groups,
                title=f"{issue_type} Issues - Resolution Time Distribution by Priority",
                x_col="Priority",
                y_col="Resolution Time (Days)",
                filename=type_filename,
            )

    def plot_sla_comparison_chart(
        self, stats_by_type_priority: Dict, filename: str = "sla_comparison.png"
    ) -> None:
        """
        Create charts showing SLA vs. actual metrics and percentage of issues resolved under SLA.
        Uses the precise sla_compliance_percentage metric from the data.
        """
        self.logger.info("Generating SLA comparison charts by issue type (actual compliance)")

        df = self._prepare_chart_data(stats_by_type_priority)

        if df.empty:
            self.logger.warning("No data available for SLA comparison chart")
            return

        # Create separate chart for each issue type
        for issue_type in df["issue_type"].unique():
            issue_df = df[df["issue_type"] == issue_type].copy()

            if issue_df.empty:
                continue

            # Sort by priority order
            issue_df = issue_df.sort_values("priority_order")

            # Generate filename for this issue type
            base_name = filename.replace(".png", "")
            type_filename = f"{base_name}_{issue_type.lower()}.png"

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))

            # Top chart: SLA vs Metrics comparison
            x_pos = range(len(issue_df))
            width = 0.2

            ax1.bar(
                [x - width for x in x_pos],
                issue_df["median_days"],
                width,
                label="Median",
                color="#3498DB",
                alpha=0.8,
            )
            ax1.bar(x_pos, issue_df["p90_days"], width, label="P90", color="#E67E22", alpha=0.8)
            ax1.bar(
                [x + width for x in x_pos],
                issue_df["p95_days"],
                width,
                label="P95",
                color="#E74C3C",
                alpha=0.8,
            )
            ax1.bar(
                [x + 2 * width for x in x_pos],
                issue_df["suggested_sla_days"],
                width,
                label="Suggested SLA",
                color="#27AE60",
                alpha=0.8,
            )

            ax1.set_xlabel("Priority")
            ax1.set_ylabel("Days")
            ax1.set_title(f"{issue_type} Issues - SLA vs Resolution Metrics")
            ax1.set_xticks(x_pos)
            ax1.set_xticklabels(issue_df["priority"], rotation=45, ha="right")
            ax1.legend()
            ax1.grid(True, axis="y", linestyle="--", alpha=0.6)

            # Bottom chart: Issues resolved under SLA percentage (actual)
            sla_compliance = list(issue_df["sla_compliance_percentage"].fillna(0.0))
            colors = [self._get_priority_colors().get(p, "#CCCCCC") for p in issue_df["priority"]]
            bars = ax2.bar(issue_df["priority"], sla_compliance, color=colors, alpha=0.8)

            # Add percentage labels on bars
            for bar, pct in zip(bars, sla_compliance):
                height = bar.get_height()
                ax2.annotate(
                    f"{pct:.1f}%",
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha="center",
                    va="bottom",
                )

            ax2.set_xlabel("Priority")
            ax2.set_ylabel("% Issues Resolved Under SLA")
            ax2.set_title(f"{issue_type} Issues - SLA Compliance (Actual)")
            ax2.set_ylim(0, 100)
            ax2.grid(True, axis="y", linestyle="--", alpha=0.6)
            plt.xticks(rotation=45, ha="right")

            plt.tight_layout()
            self._save_plot(plt, type_filename)

    def plot_trends_over_time(
        self, raw_issues_data: List[Dict], filename: str = "resolution_trends.png"
    ) -> None:
        """
        Create line charts showing resolution time trends over time.

        Args:
            raw_issues_data (List[Dict]): List of individual issue data
            filename (str): Base output filename (will be modified per issue type)
        """
        self.logger.info("Generating resolution trends over time by issue type")

        if not raw_issues_data:
            self.logger.warning("No raw issue data available for trends chart")
            return

        # Convert to DataFrame and prepare data
        df = pd.DataFrame(raw_issues_data)
        df = df[df["resolution_time_days"].notna()]  # Remove null values

        if df.empty:
            self.logger.warning("No valid resolution time data for trends chart")
            return

        # Convert resolved_date to datetime and extract month-year
        df["resolved_date"] = pd.to_datetime(df["resolved_date"])
        df["month_year"] = df["resolved_date"].dt.to_period("M")

        # Create separate trends chart for each issue type
        for issue_type in df["issue_type"].unique():
            issue_df = df[df["issue_type"] == issue_type].copy()

            if issue_df.empty:
                continue

            # Generate filename for this issue type
            base_name = filename.replace(".png", "")
            type_filename = f"{base_name}_{issue_type.lower()}.png"

            # Group by month and calculate median resolution time
            monthly_stats = (
                issue_df.groupby("month_year")
                .agg({"resolution_time_days": ["median", "count"]})
                .reset_index()
            )

            # Flatten column names
            monthly_stats.columns = ["month_year", "median_days", "issue_count"]

            if len(monthly_stats) < 2:  # Need at least 2 points for a trend
                self.logger.warning(f"Insufficient data points for {issue_type} trends")
                continue

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

            # Top chart: Median resolution time trend
            ax1.plot(
                monthly_stats["month_year"].astype(str),
                monthly_stats["median_days"],
                marker="o",
                linewidth=2,
                markersize=6,
                color="#3498DB",
            )
            ax1.set_xlabel("Month")
            ax1.set_ylabel("Median Resolution Time (Days)")
            ax1.set_title(f"{issue_type} Issues - Resolution Time Trend")
            ax1.grid(True, linestyle="--", alpha=0.6)
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha="right")

            # Bottom chart: Issue count trend
            ax2.bar(
                monthly_stats["month_year"].astype(str),
                monthly_stats["issue_count"],
                alpha=0.7,
                color="#E67E22",
            )
            ax2.set_xlabel("Month")
            ax2.set_ylabel("Number of Issues Resolved")
            ax2.set_title(f"{issue_type} Issues - Volume Trend")
            ax2.grid(True, axis="y", linestyle="--", alpha=0.6)
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha="right")

            plt.tight_layout()
            self._save_plot(plt, type_filename)

    def plot_all_charts(
        self,
        stats_by_type_priority: Dict,
        raw_issues_data: Optional[List[Dict]] = None,
        chart_types: Optional[List[str]] = None,
    ) -> None:
        """
        Generate all available charts for issue resolution time analysis.
        Now includes 'histogram' chart type.
        """
        self.logger.info("Generating all issue resolution time charts")

        available_charts = {
            "metrics_comparison": lambda: self.plot_resolution_metrics_comparison(
                stats_by_type_priority
            ),
            "count_distribution": lambda: self.plot_issue_count_distribution(
                stats_by_type_priority
            ),
            "variability_analysis": lambda: self.plot_variability_analysis(
                stats_by_type_priority, raw_issues_data or []
            ),
            "boxplot": lambda: self.plot_resolution_time_boxplot(raw_issues_data or []),
            "sla_comparison": lambda: self.plot_sla_comparison_chart(stats_by_type_priority),
            "trends_over_time": lambda: self.plot_trends_over_time(raw_issues_data or []),
            "histogram": lambda: self.plot_resolution_time_histogram(raw_issues_data or []),
        }

        # Generate requested charts or all if none specified
        charts_to_generate = chart_types or list(available_charts.keys())

        for chart_type in charts_to_generate:
            if chart_type in available_charts:
                try:
                    available_charts[chart_type]()
                    self.logger.info(f"Successfully generated {chart_type} chart")
                except Exception as e:
                    self.logger.error(f"Failed to generate {chart_type} chart: {e}")
            else:
                self.logger.warning(f"Unknown chart type: {chart_type}")
