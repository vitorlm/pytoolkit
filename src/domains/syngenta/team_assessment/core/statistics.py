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


class StatisticsHelper:
    """
    General helper class for computing statistics.
    """

    @staticmethod
    def calculate_correlation(indicators: List[Indicator], reference: List[int]) -> Optional[float]:
        """
        Calculates the correlation between indicator levels and a reference list.

        Args:
            indicators: List of Indicator objects.
            reference: Reference list of levels.

        Returns:
            Pearson correlation coefficient or None if invalid input.
        """
        if not indicators or len(indicators) != len(reference):
            logger.warning("Invalid input for correlation calculation.")
            return None

        correlation = np.corrcoef(indicators, reference)[0, 1]
        logger.info(f"Computed correlation: {correlation}")
        return round(correlation, 2)

    @staticmethod
    def calculate_mean(values: List[int]) -> Optional[float]:
        return round(statistics.mean(values), 2) if values else None

    @staticmethod
    def calculate_std_dev(values: List[int]) -> Optional[float]:
        return round(statistics.stdev(values), 2) if len(values) > 1 else None

    @staticmethod
    def calculate_percentiles(
        data: List[float], q1: Optional[float] = 25, q3: Optional[float] = 75
    ) -> Dict[int, float]:
        """Calculates specified percentiles (Q1 and Q3) for a list of numbers."""
        return np.percentile(data, q1), np.percentile(data, q3)

    @staticmethod
    def calculate_outliers(data: List[float], threshold: float) -> List[float]:
        """Detects outliers based on the standard deviation threshold."""
        mean = StatisticsHelper.calculate_mean(data)
        std_dev = (sum((x - mean) ** 2 for x in data) / len(data)) ** 0.5
        return [x for x in data if abs(x - mean) > threshold * std_dev]
