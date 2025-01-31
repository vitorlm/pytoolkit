import os
import numpy as np
from typing import Optional, Dict, Any
import matplotlib.pyplot as plt
import pandas as pd
from utils.logging.logging_manager import LogManager


class TeamAnalyzer:
    _output_path: Optional[str] = None
    _logger = LogManager.get_instance().get_logger("TeamAnalyzer")

    @property
    def output_path(self) -> Optional[str]:
        return self._output_path

    @output_path.setter
    def output_path(self, path: Optional[str]) -> None:
        self._output_path = path

    def __init__(self, team_stats: Any):
        self.team = team_stats

    def _save_plot(self, plt_instance, filename: str, adjust_params: Optional[dict] = None) -> None:
        """
        Saves the plot to the specified filename.

        Args:
            plt_instance: The Matplotlib plot instance to save.
            filename (str): The filename for the saved plot.
            adjust_params (dict, optional): Parameters for `subplots_adjust` to fine-tune the layout
        """
        file_path = os.path.join(self._output_path or "", filename)

        # Apply layout adjustments if provided
        if adjust_params:
            plt_instance.subplots_adjust(**adjust_params)

        # Save the plot
        plt_instance.savefig(file_path, dpi=600, bbox_inches="tight")
        plt_instance.close()
        self._logger.info(f"Plot saved to {file_path}")

    def _validate_criteria_stats(self, criteria_stats: Dict[str, Any]) -> None:
        """Validates the criteria statistics structure."""
        for criterion, values in criteria_stats.items():
            if not all(key in values for key in ["lowest", "q1", "q3", "highest", "average"]):
                raise KeyError(f"Missing required keys in '{criterion}' stats.")

    def plot_boxplot(self, title: str = "Skills Distribution") -> None:
        """
        Generates a boxplot for team skills based on the provided statistics.

        Args:
            title (str): The title of the plot.
        """
        self._logger.info("Starting to plot boxplot for team skills distribution.")

        try:
            self._validate_criteria_stats(self.team.criteria_stats)
        except KeyError as e:
            self._logger.error(f"Validation error: {e}")
            raise

        boxplot_data = []
        labels = []
        for criterion, values in self.team.criteria_stats.items():
            boxplot_data.append([values["lowest"], values["q1"], values["q3"], values["highest"]])
            labels.append(criterion)

        fig, ax = plt.subplots(figsize=(10, 6))
        box = ax.boxplot(
            boxplot_data,
            labels=labels,
            patch_artist=True,
            boxprops=dict(color="black"),
            medianprops=dict(color="orange", linewidth=2),
            whiskerprops=dict(color="black", linestyle="--"),
            capprops=dict(color="black"),
        )

        ax.set_title(title, fontsize=16, fontweight="bold")
        ax.set_ylabel("Skill Level", fontsize=12)
        ax.set_xlabel("Evaluated Criteria", fontsize=12)
        ax.grid(True, linestyle="--", alpha=0.6)

        colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]
        for patch, color in zip(box["boxes"], colors):
            patch.set_facecolor(color)

        self._save_plot(plt, "team_skills_distribution_boxplot.png")

    def plot_radar_chart(self, title: str = "Team Skills Distribution") -> None:
        """
        Generates a radar chart to compare the team's average skills.

        Args:
            title (str): The title of the radar chart.
        """
        self._logger.info("Generating radar chart for team skills distribution.")

        try:
            # Validate the data structure
            self._validate_criteria_stats(self.team.criteria_stats)
        except KeyError as e:
            self._logger.error(f"Validation error: {e}")
            raise

        labels = list(self.team.criteria_stats.keys())
        values = [self.team.criteria_stats[criterion]["average"] for criterion in labels]

        # Compute angles for the radar chart and close the loop
        num_vars = len(labels)
        angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
        values += values[:1]
        angles += angles[:1]

        # Create radar chart
        fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))
        ax.fill(angles, values, color="#1f77b4", alpha=0.25)
        ax.plot(angles, values, color="#1f77b4", linewidth=2)

        # Configure radial axis
        ax.set_yticks([1, 2, 3, 4, 5])
        ax.set_yticklabels([1, 2, 3, 4, 5], fontsize=10, color="gray")
        ax.set_ylim(0, 5)

        # Reduce the size of the polar frame
        ax.set_position([0.2, 0.2, 0.6, 0.6])

        # Configure angular axis
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(labels, fontsize=12, weight="bold")
        ax.spines["polar"].set_visible(True)

        # Title positioning and overall styling
        ax.set_title(title, fontsize=16, weight="bold", pad=40)
        ax.grid(color="gray", linestyle="--", linewidth=0.5, alpha=0.7)

        self._save_plot(
            plt_instance=plt,
            filename="team_skills_radar_chart.png",
            adjust_params={"left": 0.1, "right": 0.9, "top": 0.9, "bottom": 0.1},
        )

    def plot_indicator_bars(self) -> None:
        """Generates a bar chart to display the average of indicators within each criterion."""
        self._logger.info("Generating bar chart for indicators.")
        indicator_stats = []
        colors = {}
        color_palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
        color_index = 0

        for criterion, data in self.team.criteria_stats.items():
            if criterion not in colors:
                colors[criterion] = color_palette[color_index % len(color_palette)]
                color_index += 1
            for indicator, stats in data["indicator_stats"].items():
                indicator_stats.append(
                    {
                        "Indicator": f"{criterion} - {indicator}",
                        "Average": stats["average"],
                        "Criterion": criterion,
                    }
                )

        df_indicators = pd.DataFrame(indicator_stats)
        df_indicators.sort_values(by="Average", inplace=True, ascending=True)

        plt.figure(figsize=(10, 6))
        plt.barh(
            df_indicators["Indicator"],
            df_indicators["Average"],
            color=[colors[c] for c in df_indicators["Criterion"]],
        )
        plt.xlabel("Average Level")
        plt.title("Average of Each Criterion by Indicator")
        plt.grid(axis="x", linestyle="--", alpha=0.7)

        legend_patches = [
            plt.Line2D([0], [0], color=color, lw=4, label=crit) for crit, color in colors.items()
        ]
        plt.legend(handles=legend_patches, title="Criterion")

        self._save_plot(plt, "team_criteria_indicator_bars.png")
