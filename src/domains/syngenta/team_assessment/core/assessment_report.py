"""Assessment Report Models - Comprehensive data models for annual assessments.

Created in Phase 2 to support multi-year comparative analysis and complete
member assessment reporting.
"""

from datetime import date

from pydantic import BaseModel, Field

from .member_productivity_metrics import MemberProductivityMetrics
from .statistics import IndividualStatistics, TeamStatistics


class EvaluationPeriod(BaseModel):
    """Represents a single evaluation period (month/quarter/year).

    Attributes:
        year: Evaluation year
        month: Optional evaluation month (1-12)
        quarter: Optional quarter identifier (1-4)
        period_label: Human-readable label (e.g., "2024-Annual", "2024-Q3")
    """

    year: int = Field(..., description="Evaluation year")
    month: int | None = Field(None, ge=1, le=12, description="Evaluation month (1-12)")
    quarter: int | None = Field(None, ge=1, le=4, description="Quarter (1-4)")
    period_label: str = Field(..., description="Period label (e.g., '2024', '2024-Q3')")

    @classmethod
    def from_label(cls, label: str) -> "EvaluationPeriod":
        """Create EvaluationPeriod from label string.

        Args:
            label: Period label (e.g., "2024", "2024-Q3", "2024-11")

        Returns:
            EvaluationPeriod instance
        """
        parts = label.split("-")
        year = int(parts[0])

        if len(parts) == 1:
            # Just year
            return cls(year=year, period_label=label)
        elif parts[1].startswith("Q"):
            # Quarter format
            quarter = int(parts[1][1])
            return cls(year=year, quarter=quarter, period_label=label)
        else:
            # Month format
            month = int(parts[1])
            return cls(year=year, month=month, period_label=label)


class MemberAbsence(BaseModel):
    """Track member absences extracted from planning sheets.

    Attributes:
        member: Member name
        absence_type: Type of absence (vacation, sick, training, etc.)
        start_date: Absence start date
        end_date: Absence end date
        days_count: Total days absent
        notes: Optional notes about the absence
    """

    member: str = Field(..., description="Member name")
    absence_type: str = Field(..., description="Absence type (vacation, sick, training, etc.)")
    start_date: date = Field(..., description="Absence start date")
    end_date: date = Field(..., description="Absence end date")
    days_count: int = Field(..., description="Total days absent")
    notes: str | None = Field(None, description="Optional notes")


class AvailabilityMetrics(BaseModel):
    """Member availability metrics considering absences.

    Attributes:
        total_working_days: Total working days in period
        days_absent: Total days absent
        availability_percentage: Availability rate (0-100)
        absences_by_type: Breakdown of absences by type
    """

    total_working_days: int = Field(..., description="Total working days in period")
    days_absent: int = Field(..., description="Total days absent")
    availability_percentage: float = Field(..., ge=0, le=100, description="Availability rate (0-100)")
    absences_by_type: dict[str, int] = Field(default_factory=dict, description="Absences by type")


class TrendAnalysis(BaseModel):
    """Trend analysis for multi-year comparison.

    Attributes:
        trend_direction: Overall trend direction (improving, declining, stable)
        growth_rate: Year-over-year growth rate percentage
        areas_of_improvement: Criteria showing improvement
        areas_of_decline: Criteria showing decline
        consistency_score: Measure of performance consistency (0-1)
    """

    trend_direction: str = Field(..., description="Trend direction (improving, declining, stable)")
    growth_rate: float = Field(..., description="Year-over-year growth rate (%)")
    areas_of_improvement: list[str] = Field(default_factory=list, description="Criteria showing improvement")
    areas_of_decline: list[str] = Field(default_factory=list, description="Criteria showing decline")
    consistency_score: float = Field(..., ge=0, le=1, description="Performance consistency (0-1)")


class MemberAnnualAssessment(BaseModel):
    """Complete annual assessment for one member.

    Comprehensive assessment combining competency evaluations, planning data,
    and productivity metrics for a single evaluation period.

    Attributes:
        member_name: Member full name
        evaluation_period: Evaluation period information
        competency_stats: Competency evaluation statistics
        historical_comparison: Optional comparison with previous periods
        productivity_metrics: Optional JIRA productivity metrics
        absences: List of absences during the period
        availability_metrics: Availability metrics
        overall_score: Aggregated overall score
        performance_category: Performance category (e.g., "High Performer")
        year_over_year_growth: Optional growth rate compared to previous year
        areas_of_strength: List of strength areas
        areas_for_development: List of development areas
    """

    model_config = {"arbitrary_types_allowed": True}

    member_name: str = Field(..., description="Member full name")
    evaluation_period: EvaluationPeriod = Field(..., description="Evaluation period")
    competency_stats: IndividualStatistics = Field(..., description="Competency evaluation statistics")
    historical_comparison: dict[str, IndividualStatistics] | None = Field(
        None, description="Historical statistics by period"
    )
    productivity_metrics: MemberProductivityMetrics | None = Field(None, description="JIRA productivity metrics")
    absences: list[MemberAbsence] = Field(default_factory=list, description="Absences during period")
    availability_metrics: AvailabilityMetrics | None = Field(None, description="Availability metrics")
    overall_score: float = Field(..., description="Aggregated overall score")
    performance_category: str = Field(..., description="Performance category (e.g., 'High Performer')")
    year_over_year_growth: float | None = Field(None, description="Growth rate vs previous year (%)")
    areas_of_strength: list[str] = Field(default_factory=list, description="Strength areas")
    areas_for_development: list[str] = Field(default_factory=list, description="Development areas")
    trend_analysis: TrendAnalysis | None = Field(None, description="Temporal trend analysis")


class TeamProductivityMetrics(BaseModel):
    """Team-level productivity metrics from JIRA.

    Aggregated metrics for the entire team or squad during a period.

    Attributes:
        team_name: Team or squad name
        evaluation_period: Evaluation period
        total_epics_planned: Total epics planned for the period
        total_epics_delivered: Total epics delivered
        team_velocity: Average story points per cycle
        epic_adherence_rate: Percentage of epics delivered on time
        bug_resolution_rate: Average bug resolution time
        spillover_rate: Percentage of epics spilled over
        resource_utilization: Resource utilization percentage
    """

    team_name: str = Field(..., description="Team or squad name")
    evaluation_period: EvaluationPeriod = Field(..., description="Evaluation period")
    total_epics_planned: int = Field(..., description="Total epics planned")
    total_epics_delivered: int = Field(..., description="Total epics delivered")
    team_velocity: float = Field(..., description="Average story points per cycle")
    epic_adherence_rate: float = Field(..., ge=0, le=100, description="Epic adherence rate (%)")
    bug_resolution_rate: float = Field(..., description="Average bug resolution time (days)")
    spillover_rate: float = Field(..., ge=0, le=100, description="Spillover rate (%)")
    resource_utilization: float = Field(..., ge=0, le=100, description="Resource utilization (%)")


class TeamAnnualReport(BaseModel):
    """Complete team assessment report.

    Comprehensive annual report for the entire team including all member
    assessments and team-level metrics.

    Attributes:
        team_name: Team name
        evaluation_period: Evaluation period
        team_stats: Team-level competency statistics
        team_productivity: Optional team productivity metrics
        member_assessments: Dictionary of member assessments
        top_performers: List of top performer names
        areas_for_team_growth: Areas where team should improve
        team_velocity_trends: Team velocity by cycle/quarter
        generated_date: Report generation date
    """

    model_config = {"arbitrary_types_allowed": True}

    team_name: str = Field(..., description="Team name")
    evaluation_period: EvaluationPeriod = Field(..., description="Evaluation period")
    team_stats: TeamStatistics = Field(..., description="Team-level competency statistics")
    team_productivity: TeamProductivityMetrics | None = Field(None, description="Team productivity metrics")
    member_assessments: dict[str, MemberAnnualAssessment] = Field(
        default_factory=dict, description="Member assessments by name"
    )
    top_performers: list[str] = Field(default_factory=list, description="Top performer names")
    areas_for_team_growth: list[str] = Field(default_factory=list, description="Areas for team improvement")
    team_velocity_trends: dict[str, float] = Field(default_factory=dict, description="Velocity by cycle/quarter")
    generated_date: date = Field(default_factory=date.today, description="Report generation date")


class ComparativeMetrics(BaseModel):
    """Comparative metrics across multiple evaluation periods.

    Used for multi-year analysis and trend visualization.

    Attributes:
        member_name: Optional member name (if member-specific)
        periods: List of evaluation periods compared
        scores_by_period: Scores for each period
        growth_trajectory: Growth trend over time
        best_period: Period with highest performance
        worst_period: Period with lowest performance
        average_growth_rate: Average growth rate across periods
    """

    member_name: str | None = Field(None, description="Member name (if specific)")
    periods: list[EvaluationPeriod] = Field(..., description="Evaluation periods compared")
    scores_by_period: dict[str, float] = Field(..., description="Scores by period label")
    growth_trajectory: list[float] = Field(..., description="Growth trend values")
    best_period: str = Field(..., description="Best performing period")
    worst_period: str = Field(..., description="Lowest performing period")
    average_growth_rate: float = Field(..., description="Average growth rate (%)")
