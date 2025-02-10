from typing import Dict, List, Union, Tuple
from domains.syngenta.team_assessment.processors.criteria_processor import Criterion
from utils.logging.logging_manager import LogManager
from ..core.config import Config
from .feedback_specialist import FeedbackSpecialist
from ..core.statistics import TeamStatistics, IndividualStatistics, StatisticsHelper
from ..core.indicators import Indicator

# Type aliases for better readability
CriteriaDict = Dict[str, Dict]
StatisticsDict = Dict[str, Union[float, Dict]]

logger = LogManager.get_instance().get_logger("CompetencyAnalyzer")


class FeedbackAnalyzer:
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
        self.feedback_specialist = feedback_specialist
        self._config = Config()

    def analyze(
        self, competency_matrix: List[Criterion], feedback: CriteriaDict
    ) -> Tuple[TeamStatistics, Dict[str, IndividualStatistics]]:
        """
        Analyzes the competency matrix and computes team and individual statistics.

        Args:
            competency_matrix (CriteriaDict): Processed competency data.

        Returns:
            Dict: Dictionary containing team and individual statistics, including outliers.
        """
        logger.info("Starting analysis of the competency matrix.")

        team_stats = self._calculate_team_statistics(feedback)
        individual_stats = self._calculate_individual_statistics(feedback, team_stats)
        team_stats.outliers = self._detect_outliers(individual_stats)

        logger.info("Completed analysis of the competency matrix.")

        return team_stats, individual_stats

    def _calculate_team_statistics(self, feedback: CriteriaDict) -> TeamStatistics:
        """
        Calculates team-level statistics from the competency matrix.

        Args:
            competency_matrix (CriteriaDict): Processed competency data.

        Returns:
            TeamStatistics: Computed team statistics.
        """
        logger.debug("Calculating team-level statistics.")
        team_stats = TeamStatistics()

        for evaluatee in feedback.values():
            for evaluator_data in evaluatee.values():
                for criterion, indicators in evaluator_data.items():
                    if criterion not in team_stats.criteria_stats.keys():
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
        del team_stats.overall_levels
        return team_stats

    def _finalize_statistics_for_criteria(self, criteria_stats: Dict):
        """
        Finalizes the statistics for each criterion and its indicators.
        This method processes the given criteria statistics dictionary to calculate
        and add statistical measures such as quartiles (Q1 and Q3), average, highest,
        and lowest values for each criterion and its associated indicators.
        Args:
            criteria_stats (Dict): A dictionary containing statistics for each criterion.
            The dictionary is expected to have the following structure:
            {
                "criterion_name": {
                "levels": List[int],
                "indicator_stats": {
                    "indicator_name": {
                    "levels": List[int]
                    }
                }
                }
            }
        Modifies:
            The input dictionary `criteria_stats` by adding the following keys to each
            criterion and indicator:
            - "q1": The first quartile (25th percentile) of the levels.
            - "q3": The third quartile (75th percentile) of the levels.
            - "average": The mean of the levels.
            - "highest": The maximum value of the levels.
            - "lowest": The minimum value of the levels.
        """

        for criterion_data in criteria_stats.values():
            levels = criterion_data.get("levels", [])
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
        self, feedback: CriteriaDict, team_stats: TeamStatistics
    ) -> Dict[str, IndividualStatistics]:
        """
        Calculate individual-level statistics for each team member based on the competency matrix
        and feedback.

        Args:
            competency_matrix (List[Criterion]): A list of criteria used for evaluation.
            feedback (CriteriaDict): A dictionary containing feedback data for each team member.
            team_stats (TeamStatistics): An object containing overall team statistics.

        Returns:
            Dict[str, IndividualStatistics]: A dictionary where the keys are team member names and
            the values are their individual statistics.
        """
        logger.debug("Calculating individual-level statistics.")
        team_member_statistics = {}

        for evaluatee_name, evaluatee_data in feedback.items():
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
            individual_stats.finalize_statistics(self._config.criteria_weights, team_stats)
            del individual_stats.overall_levels
            team_member_statistics[evaluatee_name] = individual_stats

        return team_member_statistics

    def _detect_outliers(self, individual_stats: Dict[str, IndividualStatistics]) -> Dict:
        """
        Detects statistical outliers in individual statistics.

        Args:
            individual_stats (Dict): Dictionary of individual statistics.
            threshold (float): Number of standard deviations for outlier detection.

        Returns:
            Dict: Dictionary of identified outliers and their statistics.
        """
        logger.debug("Detecting outliers in individual statistics.")
        averages = [stats.average_level for stats in individual_stats.values()]
        outlier_indices = StatisticsHelper.calculate_outliers(
            averages, self._config.outlier_threshold
        )

        min_outlier = min(outlier_indices)
        max_outlier = max(outlier_indices)
        return {
            name: stats
            for name, stats in individual_stats.items()
            if stats.average_level <= min_outlier or stats.average_level >= max_outlier
        }
