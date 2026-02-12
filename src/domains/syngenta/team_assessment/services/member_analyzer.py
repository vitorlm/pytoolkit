import os
import statistics as stats_module
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

from domains.syngenta.team_assessment.core.statistics import IndividualStatistics, TeamStatistics
from domains.syngenta.team_assessment.services.chart_mixin import ChartMixin
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager


class MemberAnalyzer(ChartMixin):
    """Analyzer para as estatísticas individuais de feedback de um membro.

    Transforma os dados individuais (do objeto IndividualStatistics) e, se disponíveis,
    os dados do time (TeamStatistics) em formatos genéricos para os métodos do ChartMixin.

    Como o objeto IndividualStatistics já contém as estatísticas agregadas (ex.:
    "criteria_stats"), os métodos que antes processavam feedback detalhado foram
    adaptados para utilizar essas informações.
    """

    def __init__(
        self,
        member_name: str,
        member_data: IndividualStatistics,
        team_data: TeamStatistics,
        output_path: str | None = None,
        current_period_label: str | None = None,
        raw_feedback: dict[str, Any] | None = None,
        productivity_metrics: dict[str, Any] | None = None,
    ):
        """Initializes the analyzer with individual member data and team data.

        Args:
            member_name: Full name of the member.
            member_data: Aggregated feedback statistics for the member.
            team_data: Aggregated team-level statistics including criteria_stats.
            output_path: Path to save generated charts.
            current_period_label: Label for the current period (e.g., "Nov/2025").
            raw_feedback: Raw evaluator-level feedback dict
                {evaluator_name: {criterion: [Indicator]}}.
            productivity_metrics: Productivity metrics dict from planning analysis.
        """
        self.team_data = team_data
        self.name = member_name.split()[0]
        self.individual_data = member_data
        self.current_period_label = current_period_label or "Current"
        self.raw_feedback = raw_feedback
        self.productivity_metrics = productivity_metrics

        self.output_path = os.path.join(output_path or "", "members", self.name)
        FileManager.create_folder(self.output_path)

        self._logger = LogManager.get_instance().get_logger("MemberAnalyzer")

    def _get_comparison_bar_data(self) -> pd.DataFrame:
        """Prepares the data for the comparison bar chart.

        For each criterion, creates a row with the following information:
            - Criterion name
            - Individual average
            - Team average
            - Team Q1
            - Team Q3

        Returns:
            pd.DataFrame: A DataFrame with columns "Criterion", "Individual", "Team Average",
            "Team Q1", and "Team Q3".
        """
        rows = []
        for criterion, ind_stats in self.individual_data.criteria_stats.items():
            team_stats = self.team_data.criteria_stats.get(criterion, {})
            row = {
                "Criterion": criterion,
                "Individual": ind_stats.get("average", 0),
                "Team Average": team_stats.get("average", 0),
                "Team Q1": team_stats.get("q1", 0),
                "Team Q3": team_stats.get("q3", 0),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    def plot_comparison_bar_chart(self, title: str = "Individual vs Team Comparison") -> None:
        """Generates a grouped bar chart comparing the individual's average with team statistics.

        Uses the generic grouped bar chart method for each criterion.

        Args:
            title (str): The title of the chart.
        """
        # Prepare the data.
        df = self._get_comparison_bar_data()

        # Check if DataFrame is empty or missing required columns
        if df.empty or "Criterion" not in df.columns:
            self._logger.warning("No data available for comparison bar chart. Skipping.")
            return

        # Define the series (columns) that will be plotted as grouped bars.
        series = ["Individual", "Team Average", "Team Q1", "Team Q3"]
        # Optionally, define labels for each series.
        series_labels = ["Individual", "Team Average", "Team Q1", "Team Q3"]
        # Define colors for each series.
        colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]

        # Use the generic grouped bar chart method from the ChartMixin.
        self.plot_grouped_bar_chart(
            df=df,
            x_col="Criterion",
            series=series,
            series_labels=series_labels,
            colors=colors,
            title=title,
            xlabel="Criterion",
            ylabel="Average Level",
            filename="member_team_comparison_bar_chart.png",
            bar_width=0.2,
        )

    def _get_comparison_radar_data(self) -> tuple[list[str], dict[str, list[float]]]:
        """Prepares the data for the comparison radar chart.

        Returns:
            Tuple of (labels, data) where:
            - labels: List of criterion names
            - data: Dictionary with 2-3 series:
                * Current period label (e.g., "Nov/2025"): Individual current values
                * "Team Average": Team current values
                * "Historical Avg": Historical averages (only if historical data exists)
        """
        labels = []
        individual_values = []
        team_values = []

        # Get historical averages
        historical_averages = self._get_historical_criteria_averages()

        # Process current period criteria
        for criterion, criterion_stats in self.individual_data.criteria_stats.items():
            labels.append(criterion)
            individual_values.append(criterion_stats.get("average", 0))
            team_values.append(self.team_data.criteria_stats.get(criterion, {}).get("average", 0))

        # Build data dictionary with current values
        data = {
            self.current_period_label: individual_values,
            "Team Average": team_values,
        }

        # Add historical series only if historical data exists
        if historical_averages:
            historical_values = []
            for criterion in labels:
                hist_value = historical_averages.get(criterion, 0)
                historical_values.append(hist_value)
            data["Historical Avg"] = historical_values

        return labels, data

    def _get_indicators_radar_data(self) -> tuple[list[str], dict[str, list[float]]]:
        """Prepares indicator-level data for the radar chart with team benchmark.

        Returns:
            Tuple of (labels, data) where labels are indicator names and data contains
            series for Individual (current period label), Team Average, and optionally
            Historical Avg.
        """
        labels: list[str] = []
        individual_values: list[float] = []
        team_values: list[float] = []

        for criterion, criterion_stats in self.individual_data.criteria_stats.items():
            team_criterion = self.team_data.criteria_stats.get(criterion, {})
            team_indicator_stats = team_criterion.get("indicator_stats", {})

            for indicator, indicator_stats in criterion_stats.get("indicator_stats", {}).items():
                labels.append(indicator)
                individual_values.append(indicator_stats.get("average", 0))
                team_values.append(team_indicator_stats.get(indicator, {}).get("average", 0))

        data: dict[str, list[float]] = {
            self.current_period_label: individual_values,
            "Team Average": team_values,
        }

        # Add historical indicator averages if available
        historical_indicator_avgs = self._get_historical_indicator_averages()
        if historical_indicator_avgs:
            hist_values = [historical_indicator_avgs.get(lbl, 0) for lbl in labels]
            data["Historical Avg"] = hist_values

        return labels, data

    def _get_historical_indicator_averages(self) -> dict[str, float]:
        """Calculates average of each indicator across all historical periods.

        Returns:
            Dictionary mapping indicator names to their historical average scores.
            Returns empty dict if no historical data exists.
        """
        if not self.individual_data.historical_evaluations:
            return {}

        indicator_scores: dict[str, list[float]] = {}

        for hist_entry in self.individual_data.historical_evaluations:
            period_data = hist_entry.get("data", {})

            for evaluator_data in period_data.values():
                if not isinstance(evaluator_data, dict):
                    continue
                for criterion, indicators in evaluator_data.items():
                    if criterion == "_period_metadata":
                        continue
                    if isinstance(indicators, list):
                        for ind in indicators:
                            ind_name = ind.get("name") if isinstance(ind, dict) else getattr(ind, "name", None)
                            level = ind.get("level") if isinstance(ind, dict) else getattr(ind, "level", None)
                            if ind_name and level is not None:
                                if ind_name not in indicator_scores:
                                    indicator_scores[ind_name] = []
                                indicator_scores[ind_name].append(level)

        return {name: sum(scores) / len(scores) for name, scores in indicator_scores.items() if scores}

    def _get_indicator_gap_data(
        self,
    ) -> tuple[list[str], list[float], list[str], list[str]]:
        """Prepares data for the indicator gap chart (diverging bar).

        Returns:
            Tuple of (labels, gaps, group_labels, annotations) where:
            - labels: indicator names
            - gaps: member_avg - team_avg for each indicator
            - group_labels: criterion name per indicator (for grouping)
            - annotations: formatted "member: X.XX | team: X.XX" strings
        """
        labels: list[str] = []
        gaps: list[float] = []
        group_labels: list[str] = []
        annotations: list[str] = []

        for criterion, criterion_stats in self.individual_data.criteria_stats.items():
            team_criterion = self.team_data.criteria_stats.get(criterion, {})
            team_indicator_stats = team_criterion.get("indicator_stats", {})

            for indicator, ind_stats in criterion_stats.get("indicator_stats", {}).items():
                member_avg = ind_stats.get("average", 0)
                team_avg = team_indicator_stats.get(indicator, {}).get("average", 0)
                gap = round(member_avg - team_avg, 2)

                labels.append(indicator)
                gaps.append(gap)
                group_labels.append(criterion)
                annotations.append(f"member: {member_avg:.2f} | team: {team_avg:.2f}")

        return labels, gaps, group_labels, annotations

    def plot_indicator_gap_chart(self, title: str = "Indicator Gap: Individual vs Team") -> None:
        """Generates a diverging horizontal bar chart showing indicator gaps.

        Shows the gap between member and team average for each indicator, grouped by criterion.

        Args:
            title: The title of the chart.
        """
        labels, gaps, group_labels, annotations = self._get_indicator_gap_data()

        if not labels:
            self._logger.info(f"No indicator data available for {self.name} - skipping indicator gap chart")
            return

        self.plot_diverging_bar_chart(
            labels=labels,
            values=gaps,
            title=title,
            filename="member_indicator_gap_chart.png",
            group_labels=group_labels,
            annotations=annotations,
            xlabel="Gap from Team Average",
        )

    def plot_criterion_comparison_radar_chart(self, title: str = "Individual vs Team Radar Comparison") -> None:
        """Generates a radar chart comparing the individual and team average levels.

        Compares levels for each criterion between individual and team.

        Args:
            title (str): The title of the radar chart.
        """
        labels, data = self._get_comparison_radar_data()

        # Check if we have data to plot
        if not labels or not data:
            self._logger.warning("No data available for radar chart. Skipping.")
            return

        super().plot_radar_chart(
            labels,
            data,
            title=title,
            filename="member_team_comparison_radar_chart.png",
        )

    def plot_member_strengths_weaknesses_radar_chart(
        self, title: str = "Individual vs Team: Indicator Comparison"
    ) -> None:
        """Generates a radar chart comparing individual and team indicator scores.

        Shows all indicators with Individual, Team Average, and optionally Historical Avg series.

        Args:
            title: The title of the radar chart.
        """
        labels, data = self._get_indicators_radar_data()

        if not labels or not data:
            self._logger.warning("No data available for indicator comparison radar chart. Skipping.")
            return

        super().plot_radar_chart(
            labels,
            data,
            title=title,
            filename="member_indicator_comparison_radar_chart.png",
        )

    def _calculate_period_statistics(self, period_data: dict) -> dict[str, float]:
        """Calculates criteria averages from raw period evaluation data.

        Args:
            period_data: Raw evaluation data for a period
                {
                  "evaluator_name": {
                    "criterion": [Indicator dicts with "level" key]
                  }
                }

        Returns:
            Dictionary mapping criterion names to average scores
            {"Technical Skills": 4.2, "Delivery Skills": 4.0, ...}
        """
        criteria_scores = {}

        # Aggregate all indicators across all evaluators for each criterion
        for evaluator_data in period_data.values():
            if not isinstance(evaluator_data, dict):
                continue

            for criterion, indicators in evaluator_data.items():
                if criterion not in criteria_scores:
                    criteria_scores[criterion] = []

                # Extract scores from indicator dicts or objects
                if isinstance(indicators, list):
                    for indicator in indicators:
                        # Handle both dict and object formats
                        if isinstance(indicator, dict):
                            level = indicator.get("level")
                            if level is not None:
                                criteria_scores[criterion].append(level)
                        elif hasattr(indicator, "score") and indicator.score is not None:
                            criteria_scores[criterion].append(indicator.score)
                        elif hasattr(indicator, "level") and indicator.level is not None:
                            criteria_scores[criterion].append(indicator.level)

        # Calculate mean score per criterion
        criteria_averages = {}
        for criterion, scores in criteria_scores.items():
            if scores:
                criteria_averages[criterion] = sum(scores) / len(scores)

        return criteria_averages

    def _get_historical_criteria_averages(self) -> dict[str, float]:
        """Calculates average of each criterion across all historical periods.

        Returns:
            Dictionary with criterion averages from historical periods only.
            Example: {
                "Delivery Skills": 3.11,  # average of 3.33 (Nov/24) + 2.89 (Jun/25)
                "Soft Skills": 3.47,       # average of 3.50 (Nov/24) + 3.44 (Jun/25)
                "Technical Skills": 3.78,
                "Values and Behaviors": 2.92
            }
            Returns empty dict if no historical data exists.
        """
        if not self.individual_data.historical_evaluations:
            return {}

        # Accumulate scores per criterion across all historical periods
        criteria_all_scores = {}

        for hist_entry in self.individual_data.historical_evaluations:
            period_data = hist_entry.get("data", {})
            period_averages = self._calculate_period_statistics(period_data)

            for criterion, avg in period_averages.items():
                if criterion not in criteria_all_scores:
                    criteria_all_scores[criterion] = []
                criteria_all_scores[criterion].append(avg)

        # Calculate mean for each criterion
        historical_averages = {}
        for criterion, scores in criteria_all_scores.items():
            if scores:
                historical_averages[criterion] = sum(scores) / len(scores)

        return historical_averages

    def _get_historical_criteria_data(self) -> tuple[list[str], dict[str, list[float]]]:
        """Extracts historical criteria averages from historical_evaluations.

        Returns:
            Tuple of (period_labels, criteria_data)
            - period_labels: ["Nov/2024", "Jun/2025", "Nov/2025"]
            - criteria_data: {
                "Technical Skills": [3.5, 3.8, 4.2],
                "Delivery Skills": [4.0, 4.1, 4.3],
                ...
              }
        """
        period_labels = []
        criteria_data = {}

        # Check if historical data exists
        if not self.individual_data.historical_evaluations:
            return period_labels, criteria_data

        # Sort historical periods by timestamp
        sorted_history = sorted(self.individual_data.historical_evaluations, key=lambda x: x["period"]["timestamp"])

        # Process each historical period
        for hist_entry in sorted_history:
            period_info = hist_entry["period"]
            period_label = f"{period_info['period_name']}/{period_info['year']}"
            period_labels.append(period_label)

            # Calculate criteria averages for this period
            period_data = hist_entry.get("data", {})
            period_averages = self._calculate_period_statistics(period_data)

            # Add to criteria_data
            for criterion, average in period_averages.items():
                if criterion not in criteria_data:
                    criteria_data[criterion] = []
                criteria_data[criterion].append(average)

        # Add current period data from criteria_stats
        if self.individual_data.criteria_stats:
            period_labels.append(self.current_period_label)

            for criterion, stats in self.individual_data.criteria_stats.items():
                if criterion not in criteria_data:
                    criteria_data[criterion] = [None] * len(period_labels[:-1])
                criteria_data[criterion].append(stats.get("average", 0))

        return period_labels, criteria_data

    def plot_criteria_evolution(self, title: str = "Criteria Evolution Over Time") -> None:
        """Generates a line chart showing evolution of each criterion's average score over time.

        Args:
            title (str): The title of the chart.
        """
        # Get historical data
        period_labels, criteria_data = self._get_historical_criteria_data()

        # If no historical data, log and return
        if not period_labels or not criteria_data:
            self._logger.info(f"No historical data available for {self.name} - skipping criteria evolution chart")
            return

        self._logger.info(f"Generating criteria evolution chart for {self.name}")

        # Create figure
        _fig, ax = plt.subplots(figsize=(12, 6))

        # Plot each criterion
        for criterion, values in criteria_data.items():
            # Filter out None values for plotting
            valid_indices = [i for i, v in enumerate(values) if v is not None]
            valid_labels = [period_labels[i] for i in valid_indices]
            valid_values = [values[i] for i in valid_indices]

            if valid_values:
                ax.plot(valid_labels, valid_values, marker="o", label=criterion, linewidth=2)

        # Configure chart
        ax.set_xlabel("Period", fontsize=12, fontweight="bold")
        ax.set_ylabel("Average Score", fontsize=12, fontweight="bold")
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.set_ylim(0, 5)
        ax.legend(loc="best", fontsize=9)
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Save chart
        filepath = f"{self.output_path}/member_criteria_evolution.png"
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        self._logger.info(f"Saved criteria evolution chart to {filepath}")
        plt.close()

    def plot_overall_evolution(self, title: str = "Overall Performance Evolution") -> None:
        """Generates a line chart showing member's overall average progression over time.

        Args:
            title (str): The title of the chart.
        """
        # Get historical data
        period_labels, criteria_data = self._get_historical_criteria_data()

        # If no historical data, log and return
        if not period_labels or not criteria_data:
            self._logger.info(f"No historical data available for {self.name} - skipping overall evolution chart")
            return

        self._logger.info(f"Generating overall evolution chart for {self.name}")

        # Calculate overall averages for each period
        overall_averages = []
        for period_idx in range(len(period_labels)):
            period_scores = []
            for criterion_values in criteria_data.values():
                if period_idx < len(criterion_values) and criterion_values[period_idx] is not None:
                    period_scores.append(criterion_values[period_idx])

            if period_scores:
                overall_averages.append(sum(period_scores) / len(period_scores))
            else:
                overall_averages.append(None)

        # Filter out None values
        valid_indices = [i for i, v in enumerate(overall_averages) if v is not None]
        valid_labels = [period_labels[i] for i in valid_indices]
        valid_values = [overall_averages[i] for i in valid_indices]

        if not valid_values:
            self._logger.warning(f"No valid overall averages for {self.name}")
            return

        # Calculate trend
        if len(valid_values) >= 2:
            start_value = valid_values[0]
            end_value = valid_values[-1]
            change = end_value - start_value
            pct_change = (change / start_value * 100) if start_value > 0 else 0

            if change > 0.1:
                trend = "↑ Improving"
                trend_color = "green"
            elif change < -0.1:
                trend = "↓ Declining"
                trend_color = "red"
            else:
                trend = "→ Stable"
                trend_color = "gray"
        else:
            pct_change = 0
            trend = "→ Stable"
            trend_color = "gray"

        # Create figure
        _fig, ax = plt.subplots(figsize=(12, 6))

        # Plot line
        ax.plot(valid_labels, valid_values, marker="o", linewidth=3, color=trend_color)

        # Annotate start and end points
        if len(valid_values) >= 2:
            ax.annotate(
                f"Start: {valid_values[0]:.2f}",
                xy=(0, valid_values[0]),
                xytext=(10, 10),
                textcoords="offset points",
                fontsize=10,
                fontweight="bold",
            )
            ax.annotate(
                f"Current: {valid_values[-1]:.2f}\n({pct_change:+.1f}%)",
                xy=(len(valid_values) - 1, valid_values[-1]),
                xytext=(10, -20),
                textcoords="offset points",
                fontsize=10,
                fontweight="bold",
            )

        # Configure chart
        ax.set_xlabel("Period", fontsize=12, fontweight="bold")
        ax.set_ylabel("Overall Average Score", fontsize=12, fontweight="bold")
        ax.set_title(f"{title}\n{trend}", fontsize=14, fontweight="bold", color=trend_color)
        ax.set_ylim(0, 5)
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Save chart
        filepath = f"{self.output_path}/member_overall_evolution.png"
        plt.savefig(filepath, dpi=300, bbox_inches="tight")
        self._logger.info(f"Saved overall evolution chart to {filepath}")
        plt.close()

    def _get_evaluator_consistency_data(
        self,
    ) -> tuple[list[str], list[float], list[float], list[int]] | None:
        """Extracts per-indicator standard deviation across evaluators from raw feedback.

        Returns:
            Tuple of (indicator_labels, std_devs, member_avgs, evaluator_counts) sorted by
            std_dev descending, or None if raw feedback is unavailable.
        """
        if not self.raw_feedback:
            return None

        # Collect per-indicator scores across all evaluators
        indicator_scores: dict[str, list[int]] = {}

        for evaluator_data in self.raw_feedback.values():
            if not isinstance(evaluator_data, dict):
                continue
            for criterion, indicators in evaluator_data.items():
                if criterion == "_period_metadata":
                    continue
                if isinstance(indicators, list):
                    for ind in indicators:
                        ind_name = ind.get("name") if isinstance(ind, dict) else getattr(ind, "name", None)
                        level = ind.get("level") if isinstance(ind, dict) else getattr(ind, "level", None)
                        if ind_name and level is not None:
                            if ind_name not in indicator_scores:
                                indicator_scores[ind_name] = []
                            indicator_scores[ind_name].append(level)

        if not indicator_scores:
            return None

        # Calculate stats per indicator
        labels: list[str] = []
        std_devs: list[float] = []
        member_avgs: list[float] = []
        evaluator_counts: list[int] = []

        for ind_name, scores in indicator_scores.items():
            labels.append(ind_name)
            avg = sum(scores) / len(scores)
            member_avgs.append(round(avg, 2))
            evaluator_counts.append(len(scores))
            if len(scores) > 1:
                std_devs.append(round(stats_module.stdev(scores), 2))
            else:
                std_devs.append(0.0)

        # Sort by std_dev descending (most inconsistent first)
        sorted_indices = sorted(range(len(std_devs)), key=lambda i: std_devs[i], reverse=True)
        labels = [labels[i] for i in sorted_indices]
        std_devs = [std_devs[i] for i in sorted_indices]
        member_avgs = [member_avgs[i] for i in sorted_indices]
        evaluator_counts = [evaluator_counts[i] for i in sorted_indices]

        return labels, std_devs, member_avgs, evaluator_counts

    def plot_evaluator_consistency_chart(self, title: str = "Evaluator Agreement by Indicator") -> None:
        """Generates a horizontal bar chart showing evaluator agreement per indicator.

        Shows per-indicator standard deviation across evaluators, highlighting where
        agreement is high vs. low.

        Args:
            title: The title of the chart.
        """
        data = self._get_evaluator_consistency_data()
        if not data:
            self._logger.info(f"No raw feedback available for {self.name} - skipping evaluator consistency chart")
            return

        labels, std_devs, member_avgs, evaluator_counts = data
        n = len(labels)

        # Color by agreement level
        colors = []
        for sd in std_devs:
            if sd < 0.5:
                colors.append("#2ecc71")  # green = high agreement
            elif sd < 1.0:
                colors.append("#f39c12")  # yellow = moderate
            else:
                colors.append("#e74c3c")  # red = low agreement

        _fig, ax = plt.subplots(figsize=(14, max(8, n * 0.55)))
        y_positions = np.arange(n)

        ax.barh(y_positions, std_devs, color=colors, edgecolor="white", height=0.6)

        # Annotate each bar with evaluator count and score range
        for i in range(n):
            ax.text(
                std_devs[i] + 0.03,
                i,
                f"avg: {member_avgs[i]:.2f} | n={evaluator_counts[i]}",
                va="center",
                ha="left",
                fontsize=8,
                color="#333333",
            )

        ax.set_yticks(y_positions)
        ax.set_yticklabels(labels, fontsize=10)
        ax.set_xlabel("Standard Deviation (lower = more agreement)", fontsize=12)
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.grid(axis="x", linestyle="--", alpha=0.4)
        ax.invert_yaxis()

        # Legend for color coding
        legend_elements = [
            Patch(facecolor="#2ecc71", label="High agreement (std < 0.5)"),
            Patch(facecolor="#f39c12", label="Moderate (0.5 <= std < 1.0)"),
            Patch(facecolor="#e74c3c", label="Low agreement (std >= 1.0)"),
        ]
        ax.legend(handles=legend_elements, loc="lower right", fontsize=9)

        plt.tight_layout()
        self._save_plot(plt, "member_evaluator_consistency.png")

    def _get_growth_delta_data(self) -> tuple[list[str], list[float], list[float]] | None:
        """Extracts previous period vs current period averages per criterion.

        Returns:
            Tuple of (labels, previous_values, current_values) or None if no historical data.
        """
        if not self.individual_data.historical_evaluations:
            return None

        # Get the most recent historical period
        sorted_history = sorted(self.individual_data.historical_evaluations, key=lambda x: x["period"]["timestamp"])
        latest_historical = sorted_history[-1]
        period_info = latest_historical["period"]
        self._previous_period_label = f"{period_info['period_name']}/{period_info['year']}"

        # Calculate criteria averages for the most recent historical period
        period_data = latest_historical.get("data", {})
        previous_averages = self._calculate_period_statistics(period_data)

        if not previous_averages:
            return None

        # Build parallel lists for criteria present in both periods
        labels: list[str] = []
        previous_values: list[float] = []
        current_values: list[float] = []

        for criterion, current_stats in self.individual_data.criteria_stats.items():
            if criterion in previous_averages:
                labels.append(criterion)
                previous_values.append(previous_averages[criterion])
                current_values.append(current_stats.get("average", 0))

        if not labels:
            return None

        return labels, previous_values, current_values

    def plot_growth_delta_chart(self, title: str = "Growth: Previous Period vs Current") -> None:
        """Generates a dumbbell chart showing score changes from last historical period to current.

        Args:
            title: The title of the chart.
        """
        data = self._get_growth_delta_data()
        if not data:
            self._logger.info(f"No historical growth data available for {self.name} - skipping growth delta chart")
            return

        labels, previous_values, current_values = data

        before_label = getattr(self, "_previous_period_label", "Previous")
        after_label = self.current_period_label

        self.plot_dumbbell_chart(
            labels=labels,
            before_values=previous_values,
            after_values=current_values,
            before_label=before_label,
            after_label=after_label,
            title=title,
            filename="member_growth_delta.png",
        )

    def _get_productivity_data(self) -> tuple[list[str], list[float], str, list[str]] | None:
        """Extracts work distribution and key metrics from productivity_metrics.

        Returns:
            Tuple of (labels, sizes, center_text, metric_annotations) or None if no data.
        """
        if not self.productivity_metrics:
            return None

        distribution = self.productivity_metrics.get("distribution")
        key_metrics = self.productivity_metrics.get("key_metrics")

        if not distribution:
            return None

        labels: list[str] = []
        sizes: list[float] = []
        for category, cat_data in distribution.items():
            pct = cat_data.get("percentage", 0) if isinstance(cat_data, dict) else 0
            labels.append(category)
            sizes.append(pct)

        # Center text from value_focus
        center_text = ""
        if key_metrics and "value_focus" in key_metrics:
            vf = key_metrics["value_focus"]
            vf_value = vf.get("value", 0) if isinstance(vf, dict) else 0
            center_text = f"{vf_value:.1f}%\nValue Focus"

        # Build metric annotations
        annotations: list[str] = []
        if key_metrics:
            for metric_name, metric_data in key_metrics.items():
                if isinstance(metric_data, dict):
                    value = metric_data.get("value", 0)
                    benchmark = metric_data.get("benchmark", "")
                    description = metric_data.get("description", metric_name)
                    annotations.append(f"{description}: {value:.1f}% [{benchmark}]")

        return labels, sizes, center_text, annotations

    def plot_productivity_profile(self, title: str = "Work Distribution Profile") -> None:
        """Generates a donut chart showing work type distribution with key metrics.

        Args:
            title: The title of the chart.
        """
        data = self._get_productivity_data()
        if not data:
            self._logger.info(f"No productivity data available for {self.name} - skipping productivity profile")
            return

        labels, sizes, center_text, annotations = data

        self.plot_donut_chart(
            labels=labels,
            sizes=sizes,
            title=title,
            filename="member_productivity_profile.png",
            center_text=center_text,
            annotations=annotations,
        )

    def plot_all_charts(self) -> None:
        """Generates all assessment charts for the individual vs team analysis."""
        # Core comparison charts
        self.plot_comparison_bar_chart()
        self.plot_indicator_gap_chart()
        self.plot_member_strengths_weaknesses_radar_chart()
        self.plot_evaluator_consistency_chart()

        # Temporal evolution charts (if historical data available)
        if self.individual_data.historical_evaluations:
            self._logger.info(f"Generating temporal evolution charts for {self.name}")
            self.plot_criteria_evolution()
            self.plot_overall_evolution()
            self.plot_growth_delta_chart()
        else:
            self._logger.info(f"No historical data available for {self.name} - skipping temporal charts")

        # Productivity charts (if data available)
        self.plot_productivity_profile()
