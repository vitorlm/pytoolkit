from typing import Any

import numpy as np
import pandas as pd

from domains.syngenta.team_assessment.core.statistics import TeamStatistics
from domains.syngenta.team_assessment.services.chart_mixin import ChartMixin
from utils.logging.logging_manager import LogManager


class TeamAnalyzer(ChartMixin):
    """Analyzer for team-level statistics.
    Transforms team-specific data (criteria_stats) into a generic format expected by ChartMixin
    and generates the charts.
    """

    def __init__(
        self,
        team_stats: TeamStatistics,
        output_path: str | None = None,
        historical_stats: list[dict[str, Any]] | None = None,
        current_period_label: str = "Current",
    ):
        """Initializes the team analyzer.

        Args:
            team_stats (TeamStatistics): The team's statistics data.
            output_path (Optional[str]): Path to save the generated charts.
            historical_stats (Optional[List[Dict]]): List of historical statistics with period info.
            current_period_label (str): Label for the current period (e.g., "Nov/2025").
        """
        self.team = team_stats
        self.criteria_stats: dict[str, Any] = self.team.criteria_stats
        self.output_path = output_path
        self.historical_stats = historical_stats or []
        self.current_period_label = current_period_label
        self._logger = LogManager.get_instance().get_logger("TeamAnalyzer")

    def _get_boxplot_data(self) -> dict[str, list[float]]:
        """Transforms the team's criteria stats into a dictionary suitable for the generic boxplot.
        Each key is a criterion name and the value is a list of simulated values.
        Here we generate 10 sample values per criterion using a normal distribution centered on
        the average, with an estimated standard deviation based on the interquartile range.

        Returns:
            Dict[str, List[float]]: The boxplot data.
        """
        box_data = {}
        for criterion, stats in self.team.criteria_stats.items():
            # Filter out empty levels to avoid boxplot errors
            levels = stats.get("levels", [])
            if levels:  # Only include criteria with data
                box_data[criterion] = levels

        return box_data

    def plot_boxplot(self, title: str = "Skills Distribution") -> None:
        """Generates a boxplot chart for the team's skills.
        Transforms the team criteria stats into generic boxplot data and calls the mixin method.

        Args:
            title (str): The title of the plot.
        """
        data = self._get_boxplot_data()

        # Skip if no data to plot
        if not data:
            self._logger.warning("No data available for boxplot chart")
            return

        colors = [
            "#" + "".join([np.random.choice(list("0123456789ABCDEF")) for _ in range(6)]) for _ in range(len(data))
        ]
        self.plot_boxplot_chart(
            data,
            title=title,
            x_col="Criterion",
            y_col="Skill Level",
            filename="team_skills_distribution_boxplot.png",
            box_colors=colors,
        )

    def _get_radar_data(self) -> tuple[list[str], dict[str, list[float]]]:
        """Transforms the team's criteria stats into the format expected for a radar chart.

        Returns:
            A tuple (labels, data) where:
              - labels is a list of criterion names.
              - data is a dictionary mapping series names ("Average", "Q1", "Q3") to lists of values
        """
        # Filter out criteria with incomplete data
        labels = [
            crit
            for crit in self.criteria_stats.keys()
            if isinstance(self.criteria_stats[crit], dict) and "average" in self.criteria_stats[crit]
        ]

        averages = [self.criteria_stats[crit]["average"] for crit in labels]
        q1_values = [self.criteria_stats[crit]["q1"] for crit in labels]
        q3_values = [self.criteria_stats[crit]["q3"] for crit in labels]

        data = {
            "Average": averages,
            "Q1": q1_values,
            "Q3": q3_values,
        }
        return labels, data

    def plot_radar_chart(self, title: str = "Team Skills Distribution Radar Chart") -> None:
        """Generates a radar chart comparing the team's average skills.
        Transforms the team data into generic radar data and calls the mixin method.

        Args:
            title (str): The title of the radar chart.
        """
        labels, data = self._get_radar_data()

        # Skip if no data to plot
        if not labels or not any(data.values()):
            self._logger.warning("No data available for radar chart")
            return

        # Call the mixin's generic radar chart method via super() to avoid name collision.
        super().plot_radar_chart(
            labels,
            data,
            title=title,
            filename="team_skills_radar_chart.png",
        )

    def _get_bar_chart_data(self) -> pd.DataFrame:
        """Transforms team criteria stats into a DataFrame suitable for a horizontal bar chart.
        Each row represents one indicator with its corresponding average value and group.

        Returns:
            pd.DataFrame: DataFrame with columns "Label", "Value", and "Group".
        """
        rows = []
        for criterion, stats in self.criteria_stats.items():
            indicator_stats = stats.get("indicator_stats", {})
            for indicator, ind_stats in indicator_stats.items():
                rows.append(
                    {
                        "Indicator": f"{criterion} - {indicator}",
                        "Level Average": ind_stats["average"],
                        "Criterion": criterion,
                    }
                )
        return pd.DataFrame(rows)

    def plot_bar_chart(self, title: str = "Average of Each Criterion by Indicator") -> None:
        """Generates a horizontal bar chart to display the average of indicators within each criterion.
        Transforms the team data into a DataFrame and calls the generic horizontal bar chart method.

        Args:
            title (str): The title of the bar chart.
        """
        df = self._get_bar_chart_data()

        # Skip if no data to plot
        if df.empty:
            self._logger.warning("No data available for bar chart")
            return

        super().plot_horizontal_bar_chart(
            df,
            x_col="Level Average",
            y_col="Indicator",
            title=title,
            filename="team_criteria_indicator_bars.png",
            group_col="Criterion",
        )

    def plot_temporal_evolution(self) -> None:
        """Generates line charts showing evolution of criteria averages over time.
        Only plotted if historical data is available.
        """
        if not self.historical_stats:
            self._logger.info("No historical data available for temporal evolution chart")
            return

        self._logger.info("Generating temporal evolution chart")

        import matplotlib.pyplot as plt

        # Prepare data: extract period labels and averages for each criterion
        periods = []
        criteria_evolution = {}

        # Add historical periods
        for hist_entry in sorted(self.historical_stats, key=lambda x: x["period"]["timestamp"]):
            period_label = hist_entry["period"]["label"]
            periods.append(period_label)

            hist_team_stats = hist_entry["team_stats"]
            for criterion, stats in hist_team_stats.criteria_stats.items():
                if criterion not in criteria_evolution:
                    criteria_evolution[criterion] = []
                criteria_evolution[criterion].append(stats.get("average", 0))

        # Add current period
        periods.append(self.current_period_label)
        for criterion, stats in self.criteria_stats.items():
            if criterion not in criteria_evolution:
                criteria_evolution[criterion] = [0] * len(periods[:-1])
            criteria_evolution[criterion].append(stats.get("average", 0))

        # Plot
        fig, ax = plt.subplots(figsize=(12, 6))

        for criterion, values in criteria_evolution.items():
            ax.plot(periods, values, marker="o", label=criterion, linewidth=2)

        ax.set_xlabel("Period", fontsize=12, fontweight="bold")
        ax.set_ylabel("Average Level", fontsize=12, fontweight="bold")
        ax.set_title("Team Skills Evolution Over Time", fontsize=14, fontweight="bold")
        ax.legend(loc="best", fontsize=9)
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        if self.output_path:
            filepath = f"{self.output_path}/team_temporal_evolution.png"
            plt.savefig(filepath, dpi=300, bbox_inches="tight")
            self._logger.info(f"Saved temporal evolution chart to {filepath}")
        plt.close()

    def plot_criteria_comparison_over_time(self) -> None:
        """Generates grouped bar chart comparing each criterion across periods.
        Only plotted if historical data is available.
        """
        if not self.historical_stats:
            self._logger.info("No historical data available for criteria comparison chart")
            return

        self._logger.info("Generating criteria comparison over time chart")

        import matplotlib.pyplot as plt

        # Prepare data
        periods = []
        criteria_data = {}

        # Add historical periods
        for hist_entry in sorted(self.historical_stats, key=lambda x: x["period"]["timestamp"]):
            period_label = hist_entry["period"]["label"]
            periods.append(period_label)

            hist_team_stats = hist_entry["team_stats"]
            for criterion, stats in hist_team_stats.criteria_stats.items():
                if criterion not in criteria_data:
                    criteria_data[criterion] = []
                criteria_data[criterion].append(stats.get("average", 0))

        # Add current period
        periods.append(self.current_period_label)
        for criterion, stats in self.criteria_stats.items():
            if criterion not in criteria_data:
                criteria_data[criterion] = [0] * len(periods[:-1])
            criteria_data[criterion].append(stats.get("average", 0))

        # Plot grouped bar chart
        fig, ax = plt.subplots(figsize=(12, 6))

        x = np.arange(len(periods))
        width = 0.8 / len(criteria_data)

        for i, (criterion, values) in enumerate(criteria_data.items()):
            offset = width * i - (width * len(criteria_data) / 2) + width / 2
            ax.bar(x + offset, values, width, label=criterion)

        ax.set_xlabel("Period", fontsize=12, fontweight="bold")
        ax.set_ylabel("Average Level", fontsize=12, fontweight="bold")
        ax.set_title("Criteria Comparison Across Periods", fontsize=14, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(periods, rotation=45, ha="right")
        ax.legend(loc="best", fontsize=9)
        ax.grid(True, alpha=0.3, axis="y")
        plt.tight_layout()

        if self.output_path:
            filepath = f"{self.output_path}/team_criteria_comparison.png"
            plt.savefig(filepath, dpi=300, bbox_inches="tight")
            self._logger.info(f"Saved criteria comparison chart to {filepath}")
        plt.close()

    def plot_all_charts(self) -> None:
        """Calls all plot methods to generate all charts."""
        self.plot_boxplot()
        self.plot_radar_chart()
        self.plot_bar_chart()

        # Add temporal charts if historical data is available
        if self.historical_stats:
            self._logger.info(f"Generating temporal charts with {len(self.historical_stats)} historical periods")
            self.plot_temporal_evolution()
            self.plot_criteria_comparison_over_time()
        else:
            self._logger.info("No historical data - skipping temporal charts")
