import numpy as np
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple

from utils.logging.logging_manager import LogManager
from domains.syngenta.team_assessment.core.statistics import TeamStatistics
from domains.syngenta.team_assessment.services.chart_mixin import ChartMixin


class TeamAnalyzer(ChartMixin):
    """
    Analyzer for team-level statistics.
    Transforms team-specific data (criteria_stats) into a generic format expected by ChartMixin
    and generates the charts.
    """

    def __init__(self, team_stats: TeamStatistics, output_path: Optional[str] = None):
        """
        Initializes the team analyzer.

        Args:
            team_stats (TeamStatistics): The team's statistics data.
            output_path (Optional[str]): Path to save the generated charts.
        """
        self.team = team_stats
        self.criteria_stats: Dict[str, Any] = self.team.criteria_stats
        self.output_path = output_path
        self._logger = LogManager.get_instance().get_logger("TeamAnalyzer")

    def _get_boxplot_data(self) -> Dict[str, List[float]]:
        """
        Transforms the team's criteria stats into a dictionary suitable for the generic boxplot.
        Each key is a criterion name and the value is a list of simulated values.
        Here we generate 10 sample values per criterion using a normal distribution centered on
        the average, with an estimated standard deviation based on the interquartile range.

        Returns:
            Dict[str, List[float]]: The boxplot data.
        """

        box_data = {}
        for criterion, stats in self.team.criteria_stats.items():
            box_data[criterion] = stats["levels"]

        return box_data

    def plot_boxplot(self, title: str = "Skills Distribution") -> None:
        """
        Generates a boxplot chart for the team's skills.
        Transforms the team criteria stats into generic boxplot data and calls the mixin method.

        Args:
            title (str): The title of the plot.
        """
        data = self._get_boxplot_data()
        colors = [
            "#"
            + "".join([np.random.choice(list("0123456789ABCDEF")) for _ in range(6)])
            for _ in range(len(data))
        ]
        self.plot_boxplot_chart(
            data,
            title=title,
            x_col="Criterion",
            y_col="Skill Level",
            filename="team_skills_distribution_boxplot.png",
            box_colors=colors,
        )

    def _get_radar_data(self) -> Tuple[List[str], Dict[str, List[float]]]:
        """
        Transforms the team's criteria stats into the format expected for a radar chart.
        Returns:
            A tuple (labels, data) where:
              - labels is a list of criterion names.
              - data is a dictionary mapping series names ("Average", "Q1", "Q3") to lists of values
        """
        labels = list(self.criteria_stats.keys())
        averages = [self.criteria_stats[crit]["average"] for crit in labels]
        q1_values = [self.criteria_stats[crit]["q1"] for crit in labels]
        q3_values = [self.criteria_stats[crit]["q3"] for crit in labels]

        data = {
            "Average": averages,
            "Q1": q1_values,
            "Q3": q3_values,
        }
        return labels, data

    def plot_radar_chart(
        self, title: str = "Team Skills Distribution Radar Chart"
    ) -> None:
        """
        Generates a radar chart comparing the team's average skills.
        Transforms the team data into generic radar data and calls the mixin method.

        Args:
            title (str): The title of the radar chart.
        """
        labels, data = self._get_radar_data()
        # Call the mixin's generic radar chart method via super() to avoid name collision.
        super().plot_radar_chart(
            labels,
            data,
            title=title,
            filename="team_skills_radar_chart.png",
        )

    def _get_bar_chart_data(self) -> pd.DataFrame:
        """
        Transforms team criteria stats into a DataFrame suitable for a horizontal bar chart.
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

    def plot_bar_chart(
        self, title: str = "Average of Each Criterion by Indicator"
    ) -> None:
        """
        Generates a horizontal bar chart to display the average of indicators within each criterion.
        Transforms the team data into a DataFrame and calls the generic horizontal bar chart method.

        Args:
            title (str): The title of the bar chart.
        """
        df = self._get_bar_chart_data()

        super().plot_horizontal_bar_chart(
            df,
            x_col="Level Average",
            y_col="Indicator",
            title=title,
            filename="team_criteria_indicator_bars.png",
            group_col="Criterion",
        )

    def plot_all_charts(self) -> None:
        """
        Calls all plot methods to generate all charts.
        """
        self.plot_boxplot()
        self.plot_radar_chart()
        self.plot_bar_chart()
