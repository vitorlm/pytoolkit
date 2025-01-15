import os
from typing import Dict, Union, Tuple
from log_config import log_manager
from ..core.config import Config
from .feedback_specialist import FeedbackSpecialist
from ..core.statistics import TeamStatistics, IndividualStatistics, StatisticsHelper
from ..core.validations import ValidationHelper
from ..core.indicators import Indicator

# Type aliases for better readability
CriteriaDict = Dict[str, Dict]
StatisticsDict = Dict[str, Union[float, Dict]]


class CompetencyAnalyzer:
    """
    Handles analysis of competency matrices from Excel files.

    Attributes:
        logger: Configured logging instance for the module.
        feedback_specialist: Instance of FeedbackSpecialist for feedback analysis.
    """

    def __init__(self, feedback_specialist: FeedbackSpecialist):
        """
        Initializes the CompetencyAnalyzer with logging and feedback components.

        Args:
            feedback_specialist (FeedbackSpecialist): Instance of FeedbackSpecialist.
        """
        self.logger = log_manager.get_logger(
            module_name=os.path.splitext(os.path.basename(__file__))[0]
        )
        self.feedback_specialist = feedback_specialist

    def analyze(
        self, competency_matrix: CriteriaDict
    ) -> Tuple[TeamStatistics, Dict[str, IndividualStatistics]]:
        """
        Analyzes the competency matrix and computes team and individual statistics.

        Args:
            competency_matrix (CriteriaDict): Processed competency data.

        Returns:
            Dict: Dictionary containing team and individual statistics, including outliers.
        """
        self.logger.info("Starting analysis of the competency matrix.")
        ValidationHelper.validate_competency_matrix(competency_matrix)

        team_stats = self._calculate_team_statistics(competency_matrix)
        individual_stats = self._calculate_individual_statistics(competency_matrix, team_stats)
        team_stats.outliers = self._detect_outliers(individual_stats, Config.outlier_threshold)

        self.logger.info("Completed analysis of the competency matrix.")

        return team_stats, individual_stats

    def _calculate_team_statistics(self, competency_matrix: CriteriaDict) -> TeamStatistics:
        """
        Calculates team-level statistics from the competency matrix.

        Args:
            competency_matrix (CriteriaDict): Processed competency data.

        Returns:
            TeamStatistics: Computed team statistics.
        """
        self.logger.debug("Calculating team-level statistics.")
        team_stats = TeamStatistics()

        for evaluatee in competency_matrix.values():
            for evaluator_data in evaluatee.values():
                for criterion, indicators in evaluator_data.items():
                    if criterion not in team_stats.criteria_stats.items():
                        team_stats.criteria_stats[criterion] = {
                            "levels": [],
                            "indicator_stats": {},
                        }

                    for indicator in indicators:
                        if isinstance(indicator, Indicator) and (
                            hasattr(indicator, "level") and isinstance(indicator.level, int)
                        ):
                            level = indicator.level
                            ind_name = indicator.name

                            team_stats.overall_levels.append(level)
                            team_stats.criteria_stats[criterion]["levels"].append(level)

                            if (
                                ind_name
                                not in team_stats.criteria_stats[criterion]["indicator_stats"]
                            ):
                                team_stats.criteria_stats[criterion]["indicator_stats"][
                                    ind_name
                                ] = {"levels": []}
                            team_stats.criteria_stats[criterion]["indicator_stats"][ind_name][
                                "levels"
                            ].append(level)

        team_stats.finalize_statistics()
        self._finalize_statistics_for_criteria(team_stats.criteria_stats)
        return team_stats

    def _finalize_statistics_for_criteria(self, criteria_stats: Dict):
        """
        Finalizes statistical calculations for individual criteria and indicators.

        Args:
            criteria_stats (Dict): Dictionary containing criteria statistics.
        """
        for criterion_data in criteria_stats.values():
            levels = criterion_data.pop("levels", [])
            if levels:
                q1, q3 = StatisticsHelper.calculate_percentiles(levels)
                criterion_data["q1"] = q1
                criterion_data["q3"] = q3
                criterion_data["average"] = StatisticsHelper.calculate_mean(levels)
                criterion_data["highest"] = max(levels)
                criterion_data["lowest"] = min(levels)

            for ind_stats in criterion_data["indicator_stats"].values():
                ind_levels = ind_stats.pop("levels", [])
                if ind_levels:
                    q1, q3 = StatisticsHelper.calculate_percentiles(ind_levels)
                    ind_stats["q1"] = q1
                    ind_stats["q3"] = q3
                    ind_stats["average"] = StatisticsHelper.calculate_mean(ind_levels)
                    ind_stats["highest"] = max(ind_levels)
                    ind_stats["lowest"] = min(ind_levels)

    def _calculate_individual_statistics(
        self, competency_matrix: CriteriaDict, team_stats: TeamStatistics
    ) -> Dict[str, IndividualStatistics]:
        """
        Calculates individual-level statistics with comparisons to team statistics.

        Args:
            competency_matrix (CriteriaDict): Processed competency data.
            team_stats (TeamStatistics): Precomputed team-level statistics.

        Returns:
            Dict[str, IndividualStatistics]: Individual statistics mapped by evaluatee name.
        """
        self.logger.debug("Calculating individual-level statistics.")
        team_member_statistics = {}

        for evaluatee_name, evaluatee_data in competency_matrix.items():
            individual_stats = IndividualStatistics()

            for evaluator_data in evaluatee_data.values():
                for criterion, indicators in evaluator_data.items():
                    if criterion not in individual_stats.criteria_stats:
                        individual_stats.criteria_stats[criterion] = {}

                    criterion_stats = individual_stats.criteria_stats[criterion]

                    if "levels" not in criterion_stats:
                        criterion_stats["levels"] = []

                    if "indicator_stats" not in criterion_stats:
                        criterion_stats["indicator_stats"] = {}

                    for indicator in indicators:
                        if isinstance(indicator, Indicator) and (
                            hasattr(indicator, "level") and isinstance(indicator.level, int)
                        ):
                            level = indicator.level
                            ind_name = indicator.name

                            individual_stats.overall_levels.append(level)
                            criterion_stats["levels"].append(level)

                            ind_stats = criterion_stats["indicator_stats"].setdefault(ind_name, {})

                            if "levels" not in ind_stats:
                                ind_stats["levels"] = []

                            ind_stats["levels"].append(level)

            self._finalize_statistics_for_criteria(individual_stats.criteria_stats)
            individual_stats.finalize_statistics(Config.criteria_weights, team_stats)

            team_member_statistics[evaluatee_name] = individual_stats

        return team_member_statistics

    def _detect_outliers(
        self, individual_stats: Dict[str, IndividualStatistics], threshold: float
    ) -> Dict:
        """
        Detects statistical outliers in individual statistics.

        Args:
            individual_stats (Dict): Dictionary of individual statistics.
            threshold (float): Number of standard deviations for outlier detection.

        Returns:
            Dict: Dictionary of identified outliers and their statistics.
        """
        self.logger.debug("Detecting outliers in individual statistics.")
        averages = [stats.average_level for stats in individual_stats.values()]
        outlier_indices = StatisticsHelper.calculate_outliers(averages, threshold)

        return {
            name: stats
            for name, stats in individual_stats.items()
            if stats.average_level in outlier_indices
        }
