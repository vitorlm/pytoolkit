import os
from typing import Dict, List, Union, Tuple
import numpy as np
from log_config import log_manager
from .config import Config
from .feedback_specialist import FeedbackSpecialist

# Type aliases for better readability
CriteriaDict = Dict[str, Dict]
StatisticsDict = Dict[str, Union[float, Dict]]


class StatisticsHelper:
    """
    Helper class for statistical calculations.
    Contains methods to calculate mean, percentiles, and detect outliers.
    """

    @staticmethod
    def calculate_mean(data: List[float]) -> float:
        """Calculates the mean of a list of numbers."""
        return round(sum(data) / len(data), 2) if data else 0.0

    @staticmethod
    def calculate_percentiles(data: List[float]) -> Dict[int, float]:
        """Calculates specified percentiles (Q1 and Q3) for a list of numbers."""
        return np.percentile(data, Config.PERCENTILE_Q1), np.percentile(data, Config.PERCENTILE_Q3)

    @staticmethod
    def calculate_outliers(data: List[float], threshold: float) -> List[float]:
        """Detects outliers based on the standard deviation threshold."""
        mean = StatisticsHelper.calculate_mean(data)
        std_dev = (sum((x - mean) ** 2 for x in data) / len(data)) ** 0.5
        return [x for x in data if abs(x - mean) > threshold * std_dev]


class ValidationHelper:
    """
    Helper class for validating competency matrix structure.
    """

    @staticmethod
    def validate_competency_matrix(competency_matrix: CriteriaDict) -> None:
        """Validates the structure and contents of the competency matrix."""
        if not isinstance(competency_matrix, dict):
            raise ValueError("Competency matrix must be a dictionary.")

        for evaluatee, evaluator_data in competency_matrix.items():
            if not isinstance(evaluatee, str):
                raise ValueError("Each evaluatee name must be a string.")
            if not isinstance(evaluator_data, dict):
                raise ValueError(f"Evaluator data for {evaluatee} must be a dictionary.")

            for evaluator, criteria_data in evaluator_data.items():
                if not isinstance(evaluator, str):
                    raise ValueError(f"Evaluator name must be a string for {evaluatee}.")
                if not isinstance(criteria_data, dict):
                    raise ValueError(f"Criteria data for {evaluator} must be a dictionary.")

                for criterion, indicators in criteria_data.items():
                    if not isinstance(criterion, str):
                        raise ValueError(f"Criterion name must be a string for {evaluator}.")
                    if not isinstance(indicators, list):
                        raise ValueError(f"Indicators for {criterion} must be a list.")

                    for indicator in indicators:
                        if not isinstance(indicator, dict):
                            raise ValueError(f"Each indicator must be a dictionary in {criterion}.")


class BaseStatistics:
    """
    Base class to store shared statistical attributes and methods.
    """

    overall_levels: List[int] = []
    criteria_stats: Dict[str, Dict] = {}
    average_level: float = 0.0
    highest_level: int = 0
    lowest_level: int = 0
    q1: float = 0.0
    q3: float = 0.0

    def finalize_statistics(self) -> None:
        """
        Finalizes statistical calculations for base-level data.
        """
        if self.overall_levels:
            self.average_level = StatisticsHelper.calculate_mean(self.overall_levels)
            self.highest_level = max(self.overall_levels)
            self.lowest_level = min(self.overall_levels)
            self.q1, self.q3 = StatisticsHelper.calculate_percentiles(self.overall_levels)


class TeamStatistics(BaseStatistics):
    """
    Class to store and calculate team-level statistics.

    Attributes:
        outliers: Dictionary of identified outliers and their statistics.
    """

    outliers: Dict = {}


class IndividualStatistics(BaseStatistics):
    """
    Class to store and calculate individual-level statistics.

    Attributes:
        weighted_average: Weighted average competency level for the individual.
        insights: Strengths and opportunities for the individual.
    """

    weighted_average: float = 0.0
    insights = {"strengths": [], "opportunities": []}

    def _calculate_weighted_average(self, criteria_weights: Dict[str, float]) -> None:
        """
        Calculates the weighted average based on predefined weights.
        """
        weighted_sum = 0
        total_weight = sum(criteria_weights.values())

        for criterion, stats in self.criteria_stats.items():
            average = stats.get("average", 0)
            weight = criteria_weights.get(criterion, 0)
            weighted_sum += average * weight

        self.weighted_average = round(weighted_sum / total_weight, 2) if total_weight > 0 else 0.0

    def _strengths_opportunities(self, team_stats: TeamStatistics) -> None:
        """
        Identifies strengths and opportunities for a team member based on team comparisons.

        Args:
            team_stats (TeamStatistics): Team-level statistics for comparison.
        """

        for criterion, criterion_data in self.criteria_stats.items():

            if criterion_data["average"] > team_stats.q3:
                self.insights["strengths"].append(
                    {"criterion": criterion, "reason": "Above Q3 of team distribution"}
                )
            elif criterion_data["average"] < team_stats.q1:
                self.insights["opportunities"].append(
                    {"criterion": criterion, "reason": "Below Q1 of team distribution"}
                )

    def finalize_statistics(
        self, criteria_weights: Dict[str, float], team_stats: TeamStatistics
    ) -> None:
        """
        Finalizes statistical calculations for individual-level data.
        Extends the base implementation by adding weighted average and insights.
        """
        # Call the base method
        super().finalize_statistics()

        # Calculate weighted average
        self._calculate_weighted_average(criteria_weights)

        # Calculate strengths and opportunities
        self._strengths_opportunities(team_stats)


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
        team_stats.outliers = self._detect_outliers(individual_stats, Config.OUTLIER_THRESHOLD)

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
                        if isinstance(indicator, dict) and "level" in indicator:
                            level = indicator["level"]
                            ind_name = indicator["indicator"]

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
                        if isinstance(indicator, dict) and "level" in indicator:
                            level = indicator["level"]
                            ind_name = indicator["indicator"]

                            individual_stats.overall_levels.append(level)
                            criterion_stats["levels"].append(level)

                            ind_stats = criterion_stats["indicator_stats"].setdefault(ind_name, {})

                            if "levels" not in ind_stats:
                                ind_stats["levels"] = []

                            ind_stats["levels"].append(level)

            self._finalize_statistics_for_criteria(individual_stats.criteria_stats)
            individual_stats.finalize_statistics(Config.CRITERIA_WEIGHTS, team_stats)

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
