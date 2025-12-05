"""Annual Comparison Service - Multi-year comparative analysis for team assessments.

Created in Phase 2 to enable historical comparison and trend analysis across
multiple evaluation periods.
"""

from pathlib import Path

from utils.logging.logging_manager import LogManager

from ..core.assessment_report import (
    ComparativeMetrics,
    EvaluationPeriod,
    TrendAnalysis,
)
from ..core.statistics import IndividualStatistics
from ..processors.feedback_processor import FeedbackProcessor
from ..services.feedback_analyzer import FeedbackAnalyzer


class AnnualComparisonService:
    """Service for comparing member assessments across multiple years.

    Provides comprehensive multi-year analysis including:
    - Competency growth trends per criterion
    - Peer feedback consistency analysis
    - Self vs peer evaluation alignment
    - Areas of sustained strength
    - Areas needing continued development
    """

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("AnnualComparisonService")
        self.feedback_processor = FeedbackProcessor()
        self.feedback_analyzer = FeedbackAnalyzer()

    def compare_member_years(
        self,
        member_name: str,
        feedback_folders: dict[str, str],
        competency_matrix_file: str,
    ) -> dict:
        """Generate comparative report for member across multiple years.

        Args:
            member_name: Name of member to analyze
            feedback_folders: Dict mapping year label to folder path
                             e.g., {"2023": "/path/2023", "2024": "/path/2024"}
            competency_matrix_file: Path to competency matrix file

        Returns:
            Dict with comparative analysis including:
            - scores_by_period: Scores for each evaluation period
            - growth_trajectory: Growth trend over time
            - trend_analysis: Detailed trend analysis
            - period_stats: Statistics for each period

        Example:
            >>> service = AnnualComparisonService()
            >>> result = service.compare_member_years(
            ...     "John Doe",
            ...     {"2023": "/feedback/2023", "2024": "/feedback/2024"},
            ...     "/matrix/competency.xlsx"
            ... )
        """
        self.logger.info(f"Starting multi-year comparison for {member_name}")
        self.logger.info(f"Periods to compare: {list(feedback_folders.keys())}")

        # Load competency matrix (structure definition)
        from ..processors.criteria_processor import CriteriaProcessor

        criteria_processor = CriteriaProcessor()
        competency_matrix = criteria_processor.process_file(Path(competency_matrix_file))

        # Process feedback for each period
        period_stats: dict[str, IndividualStatistics] = {}
        period_feedback: dict[str, dict] = {}

        for period_label, folder_path in feedback_folders.items():
            try:
                self.logger.info(f"Processing feedback for period {period_label}")

                # Process feedback folder
                feedback_data = self.feedback_processor.process_folder(folder_path)

                # Analyze feedback to get member statistics
                team_stats, members_stats = self.feedback_analyzer.analyze(competency_matrix, feedback_data)

                # Extract member's statistics
                if member_name in members_stats:
                    period_stats[period_label] = members_stats[member_name]
                    period_feedback[period_label] = feedback_data.get(member_name, {})
                    self.logger.info(f"✓ Found statistics for {member_name} in {period_label}")
                else:
                    self.logger.warning(f"✗ No statistics found for {member_name} in {period_label}")

            except Exception as e:
                self.logger.error(f"Failed to process period {period_label}: {e}", exc_info=True)
                continue

        if not period_stats:
            self.logger.error(f"No statistics found for {member_name} in any period")
            return {
                "member_name": member_name,
                "periods": [],
                "error": "No data found for member",
            }

        # Calculate comparative metrics
        comparative_metrics = self._calculate_comparative_metrics(member_name, period_stats)

        # Perform trend analysis
        trend_analysis = self._analyze_trends(member_name, period_stats)

        # Calculate consistency metrics
        consistency_score = self._calculate_consistency_score(period_stats)

        result = {
            "member_name": member_name,
            "periods": list(period_stats.keys()),
            "period_stats": {period: stats.model_dump() for period, stats in period_stats.items()},
            "comparative_metrics": comparative_metrics,
            "trend_analysis": trend_analysis.model_dump(),
            "consistency_score": consistency_score,
            "areas_of_sustained_strength": self._identify_sustained_strengths(period_stats),
            "areas_for_continued_development": self._identify_continued_development(period_stats),
        }

        self.logger.info(f"Multi-year comparison complete for {member_name}")
        return result

    def _calculate_comparative_metrics(
        self, member_name: str, period_stats: dict[str, IndividualStatistics]
    ) -> ComparativeMetrics:
        """Calculate comparative metrics across periods.

        Args:
            member_name: Member name
            period_stats: Statistics for each period

        Returns:
            ComparativeMetrics instance
        """
        # Extract scores by period
        scores_by_period = {period: stats.overall_average for period, stats in period_stats.items()}

        # Sort periods chronologically
        sorted_periods = sorted(scores_by_period.keys())
        growth_trajectory = [scores_by_period[p] for p in sorted_periods]

        # Find best and worst periods
        best_period = max(scores_by_period, key=scores_by_period.get)
        worst_period = min(scores_by_period, key=scores_by_period.get)

        # Calculate average growth rate
        if len(growth_trajectory) > 1:
            growth_rates = [
                ((growth_trajectory[i + 1] - growth_trajectory[i]) / growth_trajectory[i]) * 100
                for i in range(len(growth_trajectory) - 1)
            ]
            average_growth_rate = sum(growth_rates) / len(growth_rates)
        else:
            average_growth_rate = 0.0

        return ComparativeMetrics(
            member_name=member_name,
            periods=[EvaluationPeriod.from_label(p) for p in sorted_periods],
            scores_by_period=scores_by_period,
            growth_trajectory=growth_trajectory,
            best_period=best_period,
            worst_period=worst_period,
            average_growth_rate=average_growth_rate,
        )

    def _analyze_trends(self, member_name: str, period_stats: dict[str, IndividualStatistics]) -> TrendAnalysis:
        """Analyze trends across evaluation periods.

        Args:
            member_name: Member name
            period_stats: Statistics for each period

        Returns:
            TrendAnalysis instance
        """
        sorted_periods = sorted(period_stats.keys())

        # Get criterion-level trends
        criterion_trends: dict[str, list[float]] = {}

        for period in sorted_periods:
            stats = period_stats[period]
            for criterion, criterion_stat in stats.criteria.items():
                if criterion not in criterion_trends:
                    criterion_trends[criterion] = []
                criterion_trends[criterion].append(criterion_stat.average)

        # Identify improving and declining criteria
        areas_of_improvement = []
        areas_of_decline = []

        for criterion, scores in criterion_trends.items():
            if len(scores) > 1:
                # Compare first and last
                if scores[-1] > scores[0] + 0.2:  # Threshold for improvement
                    areas_of_improvement.append(criterion)
                elif scores[-1] < scores[0] - 0.2:  # Threshold for decline
                    areas_of_decline.append(criterion)

        # Calculate overall trend direction
        overall_scores = [period_stats[p].overall_average for p in sorted_periods]
        if len(overall_scores) > 1:
            if overall_scores[-1] > overall_scores[0] + 0.2:
                trend_direction = "improving"
            elif overall_scores[-1] < overall_scores[0] - 0.2:
                trend_direction = "declining"
            else:
                trend_direction = "stable"

            growth_rate = ((overall_scores[-1] - overall_scores[0]) / overall_scores[0]) * 100
        else:
            trend_direction = "stable"
            growth_rate = 0.0

        # Calculate consistency score (inverse of coefficient of variation)
        if overall_scores:
            mean_score = sum(overall_scores) / len(overall_scores)
            if mean_score > 0:
                std_dev = (sum((x - mean_score) ** 2 for x in overall_scores) / len(overall_scores)) ** 0.5
                cv = std_dev / mean_score  # Coefficient of variation
                consistency_score = max(0, 1 - cv)  # Inverse for consistency
            else:
                consistency_score = 0.0
        else:
            consistency_score = 0.0

        return TrendAnalysis(
            trend_direction=trend_direction,
            growth_rate=growth_rate,
            areas_of_improvement=areas_of_improvement,
            areas_of_decline=areas_of_decline,
            consistency_score=consistency_score,
        )

    def _calculate_consistency_score(self, period_stats: dict[str, IndividualStatistics]) -> float:
        """Calculate performance consistency across periods.

        Args:
            period_stats: Statistics for each period

        Returns:
            Consistency score (0-1, where 1 is most consistent)
        """
        if len(period_stats) < 2:
            return 1.0  # Only one period, perfectly consistent

        overall_scores = [stats.overall_average for stats in period_stats.values()]
        mean_score = sum(overall_scores) / len(overall_scores)

        if mean_score == 0:
            return 0.0

        # Calculate coefficient of variation
        std_dev = (sum((x - mean_score) ** 2 for x in overall_scores) / len(overall_scores)) ** 0.5
        cv = std_dev / mean_score

        # Invert for consistency (low CV = high consistency)
        return max(0, 1 - cv)

    def _identify_sustained_strengths(self, period_stats: dict[str, IndividualStatistics]) -> list[str]:
        """Identify criteria that are consistently strong across all periods.

        Args:
            period_stats: Statistics for each period

        Returns:
            List of criterion names showing sustained strength
        """
        if not period_stats:
            return []

        # Get all criteria that appear in all periods
        all_criteria = set()
        for stats in period_stats.values():
            all_criteria.update(stats.criteria.keys())

        sustained_strengths = []
        STRENGTH_THRESHOLD = 4.0  # Level 4 or higher

        for criterion in all_criteria:
            # Check if criterion is strong in ALL periods
            is_sustained_strength = True
            for stats in period_stats.values():
                if criterion not in stats.criteria:
                    is_sustained_strength = False
                    break
                if stats.criteria[criterion].average < STRENGTH_THRESHOLD:
                    is_sustained_strength = False
                    break

            if is_sustained_strength:
                sustained_strengths.append(criterion)

        return sustained_strengths

    def _identify_continued_development(self, period_stats: dict[str, IndividualStatistics]) -> list[str]:
        """Identify criteria that consistently need development.

        Args:
            period_stats: Statistics for each period

        Returns:
            List of criterion names needing continued development
        """
        if not period_stats:
            return []

        # Get all criteria
        all_criteria = set()
        for stats in period_stats.values():
            all_criteria.update(stats.criteria.keys())

        continued_development = []
        DEVELOPMENT_THRESHOLD = 3.0  # Below level 3

        for criterion in all_criteria:
            # Check if criterion consistently needs development
            needs_development = True
            for stats in period_stats.values():
                if criterion not in stats.criteria:
                    needs_development = False
                    break
                if stats.criteria[criterion].average >= DEVELOPMENT_THRESHOLD:
                    needs_development = False
                    break

            if needs_development:
                continued_development.append(criterion)

        return continued_development

    def get_member_growth_summary(self, member_name: str, period_stats: dict[str, IndividualStatistics]) -> str:
        """Generate human-readable growth summary for member.

        Args:
            member_name: Member name
            period_stats: Statistics for each period

        Returns:
            Formatted growth summary text
        """
        trend = self._analyze_trends(member_name, period_stats)
        strengths = self._identify_sustained_strengths(period_stats)
        development = self._identify_continued_development(period_stats)

        summary_parts = [
            f"Growth Summary for {member_name}",
            "=" * 60,
            f"Overall Trend: {trend.trend_direction.upper()}",
            f"Growth Rate: {trend.growth_rate:.1f}%",
            f"Consistency Score: {trend.consistency_score:.2f}",
            "",
        ]

        if trend.areas_of_improvement:
            summary_parts.append("Areas of Improvement:")
            for area in trend.areas_of_improvement:
                summary_parts.append(f"  ✓ {area}")
            summary_parts.append("")

        if trend.areas_of_decline:
            summary_parts.append("Areas of Decline:")
            for area in trend.areas_of_decline:
                summary_parts.append(f"  ✗ {area}")
            summary_parts.append("")

        if strengths:
            summary_parts.append("Sustained Strengths:")
            for strength in strengths:
                summary_parts.append(f"  ★ {strength}")
            summary_parts.append("")

        if development:
            summary_parts.append("Areas for Continued Development:")
            for area in development:
                summary_parts.append(f"  → {area}")

        return "\n".join(summary_parts)
