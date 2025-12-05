import os

import pandas as pd

from domains.syngenta.team_assessment.core.statistics import IndividualStatistics, TeamStatistics
from domains.syngenta.team_assessment.services.chart_mixin import ChartMixin
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager


class MemberAnalyzer(ChartMixin):
    """Analyzer para as estatísticas individuais de feedback de um membro, com dados
    opcionais do time. Transforma os dados individuais (do objeto
    IndividualStatistics) e, se disponíveis, os dados do time (TeamStatistics) em
    formatos genéricos para os métodos do ChartMixin.

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
    ):
        """Inicializa o analisador com os dados individuais do membro e os dados do time.

        Args:
            member_data (IndividualStatistics): Dados de feedback agregados do membro.
            team_data (TeamStatistics): Dados agregados do time, que devem incluir
                "criteria_stats".
            output_path (Optional[str]): Caminho para salvar os gráficos gerados.
        """
        self.team_data = team_data
        self.name = member_name.split()[0]
        self.individual_data = member_data

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
        """Generates a grouped bar chart comparing the individual's average with team statistics for
        each criterion by using the generic grouped bar chart method.

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
        Creates axes for each criterion and two series:
            - "Individual": the individual's average values.
            - "Team Average": the team's average values.

        Returns:
            Tuple[List[str], Dict[str, List[float]]]: A tuple containing a list of criterion labels
            and a dictionary mapping series names to their corresponding list of values.
        """
        labels = []
        individual_values = []
        team_values = []
        for criterion, criterion_stats in self.individual_data.criteria_stats.items():
            labels.append(criterion)
            individual_values.append(criterion_stats.get("average", 0))
            team_values.append(self.team_data.criteria_stats.get(criterion, {}).get("average", 0))

        data = {
            "Individual": individual_values,
            "Team Average": team_values,
        }
        return labels, data

    def _get_indicators_radar_data(self) -> dict[str, list[float]]:
        labels = []
        individual_values = []

        for _, criterion_stats in self.individual_data.criteria_stats.items():
            for indicator, indicator_stats in criterion_stats["indicator_stats"].items():
                labels.append(indicator)
                individual_values.append(indicator_stats.get("average", 0))

        data = {
            "Individual": individual_values,
        }
        return labels, data

    def plot_criterion_comparison_radar_chart(self, title: str = "Individual vs Team Radar Comparison") -> None:
        """Generates a radar chart comparing the individual's and team's average levels for
        each criterion.

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
        self, title: str = "Individual Indicator Strengths and Weaknesses"
    ) -> None:
        labels, data = self._get_indicators_radar_data()

        # Check if we have data to plot
        if not labels or not data:
            self._logger.warning("No data available for strengths/weaknesses radar chart. Skipping.")
            return

        super().plot_radar_chart(
            labels,
            data,
            title=title,
            filename="member_indicator_strengths_weaknesses_radar_chart.png",
        )

    def plot_all_charts(self) -> None:
        """Generates all comparison charts (bar and radar) for the individual vs team analysis."""
        self.plot_comparison_bar_chart()
        self.plot_criterion_comparison_radar_chart()
        self.plot_member_strengths_weaknesses_radar_chart()
