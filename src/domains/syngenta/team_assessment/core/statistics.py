from typing import List, Dict, Optional
from pydantic import BaseModel, Field
from log_config import LogManager

from .indicators import Indicator
import numpy as np
import statistics

# Configure logger
logger = LogManager.get_instance().get_logger("StatisticsHelper")


class BaseStatistics(BaseModel):
    """
    Base class to store shared statistical attributes and methods.
    """

    overall_levels: List[int] = Field(default_factory=list)
    criteria_stats: Dict[str, Dict] = Field(default_factory=dict)
    average_level: float = 0.0
    weighted_average: float = 0.0
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

    outliers: Dict = Field(default_factory=dict)


class IndividualStatistics(BaseStatistics):
    """
    Class to store and calculate individual-level statistics.

    Attributes:
        weighted_average: Weighted average competency level for the individual.
        insights: Strengths and opportunities for the individual.
    """

    weighted_average: float = 0.0
    insights: Dict[str, List[Dict[str, str]]] = Field(
        default_factory=lambda: {"strengths": [], "opportunities": []}
    )

    def _calculate_weighted_average(self, criteria_weights: Dict[str, float]) -> None:
        """
        Calculate the weighted average of criteria statistics.

        This method calculates the weighted average of the criteria statistics
        using the provided criteria weights. The result is stored in the
        `self.weighted_average` attribute.

        Args:
            criteria_weights (Dict[str, float]): A dictionary where the keys are
                the criteria names and the values are the corresponding weights.

        Returns:
            None
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
        differences = {"strengths": [], "opportunities": []}

        for criterion, criterion_data in self.criteria_stats.items():
            team_criterion_data = team_stats.criteria_stats.get(criterion, {})

            member_avg = criterion_data["average"]
            team_q1 = team_criterion_data.get("q1", float("-inf"))
            team_q3 = team_criterion_data.get("q3", float("inf"))

            # Criterion-level evaluation
            if member_avg >= team_q3:
                differences["strengths"].append(
                    {
                        "criterion": criterion,
                        "reason": "Above Q3 of team distribution",
                        "team_level": team_q3,
                        "member_level": member_avg,
                    }
                )
            elif member_avg <= team_q1:
                differences["opportunities"].append(
                    {
                        "criterion": criterion,
                        "reason": "Below Q1 of team distribution",
                        "team_level": team_q1,
                        "member_level": member_avg,
                    }
                )

            # Indicator-level evaluation
            for indicator, indicator_data in criterion_data.get("indicator_stats", {}).items():
                team_indicator_data = team_criterion_data.get("indicator_stats", {}).get(
                    indicator, {}
                )

                member_avg = indicator_data["average"]
                team_q1 = team_indicator_data.get("q1", float("-inf"))
                team_q3 = team_indicator_data.get("q3", float("inf"))

                if member_avg >= team_q3:
                    differences["strengths"].append(
                        {
                            "indicator": indicator,
                            "reason": "Above Q3 of team distribution",
                            "team_level": team_q3,
                            "member_level": member_avg,
                        }
                    )
                elif member_avg <= team_q1:
                    differences["opportunities"].append(
                        {
                            "indicator": indicator,
                            "reason": "Below Q1 of team distribution",
                            "team_level": team_q1,
                            "member_level": member_avg,
                        }
                    )

        # Sort by absolute difference to prioritize the most significant ones
        differences["strengths"].sort(
            key=lambda x: abs(x["member_level"] - x["team_level"]), reverse=True
        )
        differences["opportunities"].sort(
            key=lambda x: abs(x["member_level"] - x["team_level"]), reverse=True
        )

        # Store only the top two in each category
        self.insights["strengths"] = differences["strengths"][:2]
        self.insights["opportunities"] = differences["opportunities"][:2]

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


class StatisticsHelper:
    """
    General helper class for computing statistics.
    """

    @staticmethod
    def calculate_correlation(indicators: List[Indicator], reference: List[int]) -> Optional[float]:
        """
        Calculate the Pearson correlation coefficient between two lists of indicators and a
        reference.

        The Pearson correlation coefficient is a measure of the linear correlation between two sets
        of data. It ranges from -1 to 1, where 1 means total positive linear correlation, 0 means
        no linear correlation, and -1 means total negative linear correlation.

        Args:
            indicators (List[Indicator]): A list of Indicator objects to be compared.
            reference (List[int]): A list of integer reference values.

        Returns:
            Optional[float]: The rounded Pearson correlation coefficient if inputs are valid,
            otherwise None.

        Raises:
            Warning: Logs a warning if the input lists are empty, of different lengths, or if their
            standard deviation is zero.
        """

        if not indicators or len(indicators) != len(reference):
            logger.warning("Invalid input for correlation calculation.")
            return None
        if np.std(reference) == 0 or np.std(indicators) == 0:
            logger.warning("Standard deviation is zero for correlation calculation.")
            return None

        correlation = np.corrcoef(indicators, reference)[0, 1]
        logger.debug(f"Computed correlation: {correlation}")
        return round(correlation, 2)

    @staticmethod
    def calculate_mean(values: List[int]) -> Optional[float]:
        """
        Calculate the mean (average) of a list of integers.
        The mean is calculated by summing all the values in the list and then
        dividing by the number of values. The result is rounded to 2 decimal places.
        Args:
            values (List[int]): A list of integers to calculate the mean from.
        Returns:
            Optional[float]: The mean of the list of integers, rounded to 2 decimal
            places. Returns None if the input list is empty.
        """

        return round(statistics.mean(values), 2) if values else None

    @staticmethod
    def calculate_std_dev(values: List[int]) -> Optional[float]:
        """
        Calculate the standard deviation of a list of integers.
        The standard deviation is a measure of the amount of variation or dispersion
        in a set of values. A low standard deviation indicates that the values tend
        to be close to the mean of the set, while a high standard deviation indicates
        that the values are spread out over a wider range.
        Args:
            values (List[int]): A list of integers for which the standard deviation
                                is to be calculated.
        Returns:
            Optional[float]: The standard deviation of the list of integers, rounded
                             to 2 decimal places. Returns None if the list contains
                             fewer than 2 values.
        """

        return round(statistics.stdev(values), 2) if len(values) > 1 else None

    @staticmethod
    def calculate_percentiles(
        data: List[float], q1: Optional[float] = 25, q3: Optional[float] = 75
    ) -> Dict[int, float]:
        """
        Calculate the specified percentiles (quartiles) for a given dataset.

        Parameters:
        data (List[float]): A list of numerical values.
        q1 (Optional[float]): The first quartile (25th percentile) to calculate. Default is 25.
        q3 (Optional[float]): The third quartile (75th percentile) to calculate. Default is 75.

        Returns:
        Dict[int, float]: A dictionary containing the calculated percentiles.
        """
        return np.percentile(data, q1), np.percentile(data, q3)

    @staticmethod
    def calculate_outliers(data: List[float], threshold: float) -> List[float]:
        """
        Detects outliers based on the standard deviation threshold.

        An outlier is a data point that is significantly different from other data points in a
        dataset. In this method, a data point is considered an outlier if its distance from the
        mean is greater than a specified number of standard deviations (threshold).

        Mathematically, for a given dataset:
        - Mean (μ) is the average of the data points.
        - Standard Deviation (σ) measures the dispersion of the data points from the mean.

        A data point x is considered an outlier if:
        |x - μ| > threshold * σ

        Args:
            data (List[float]): A list of numerical values.
            threshold (float): The number of standard deviations from the mean to use as the cutoff
                               for outliers.

        Returns:
            List[float]: A list of outliers.
        """
        mean = StatisticsHelper.calculate_mean(data)
        std_dev = (sum((x - mean) ** 2 for x in data) / len(data)) ** 0.5
        return [x for x in data if abs(x - mean) > threshold * std_dev]
