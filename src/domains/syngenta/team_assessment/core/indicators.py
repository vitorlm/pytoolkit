from typing import List, Dict, Optional
from pydantic import BaseModel
import numpy as np
from log_config import LogManager

# Configure logger
logger = LogManager.get_instance().get_logger("indicators_statistics")


class Indicator(BaseModel):
    """
    Model for an individual indicator in a competency evaluation.
    """

    name: str
    level: int
    evidence: Optional[str] = None


class IndicatorStatistics:
    """
    Class for computing statistics for a set of indicators.
    """

    indicators: List[Indicator]

    def __init__(self, indicators: List[Indicator]):
        if not indicators:
            raise ValueError("Indicator list cannot be empty.")
        self.indicators = indicators

    def compute_mean_level(self) -> float:
        """Computes the mean level of indicators."""
        levels = [indicator.level for indicator in self.indicators]
        mean = np.mean(levels)
        logger.info(f"Computed mean level: {mean}")
        return round(mean, 2)

    def compute_level_distribution(self) -> Dict[int, int]:
        """Computes the frequency distribution of levels."""
        distribution = {}
        for indicator in self.indicators:
            distribution[indicator.level] = distribution.get(indicator.level, 0) + 1
        logger.info(f"Computed level distribution: {distribution}")
        return distribution

    def find_outliers(self, threshold: float = 1.5) -> List[Indicator]:
        """
        Finds outliers in the indicator levels based on the IQR method.

        Args:
            threshold: Multiplier for the interquartile range to define outliers.

        Returns:
            List of indicators considered outliers.
        """
        levels = [indicator.level for indicator in self.indicators]
        q1 = np.percentile(levels, 25)
        q3 = np.percentile(levels, 75)
        iqr = q3 - q1
        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        outliers = [
            indicator
            for indicator in self.indicators
            if not (lower_bound <= indicator.level <= upper_bound)
        ]
        logger.info(f"Identified outliers: {outliers}")
        return outliers
