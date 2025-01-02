from typing import Optional, Dict, List
from pydantic import BaseModel


class FeedbackAssessment(BaseModel):
    """
    Model for validating health check data.

    Attributes:
        date (date): Date of the feedback.
        feedback_by (str): Name of the person providing the feedback.
        effort (int): Effort score (1-5).
        effort_comment (Optional[str]): Comment about the effort score.
        impact (int): Impact score (1-5).
        impact_comment (Optional[str]): Comment about the impact score.
        morale (int): Morale score (1-5).
        morale_comment (Optional[str]): Comment about the morale score.
        retention (int): Retention risk score (1-5).
        retention_comment (Optional[str]): Comment about the retention risk score.
    """

    health_check_date: str
    feedback_by: str
    effort: Optional[int]
    effort_comment: Optional[str]
    impact: Optional[int]
    impact_comment: Optional[str]
    morale: Optional[int]
    morale_comment: Optional[str]
    retention: Optional[int]
    retention_comment: Optional[str]

    def __init__(
        self,
        health_check_date: str,
        feedback_by: str,
        effort: Optional[int],
        effort_comment: Optional[str],
        impact: Optional[int],
        impact_comment: Optional[str],
        morale: Optional[int],
        morale_comment: Optional[str],
        retention: Optional[int],
        retention_comment: Optional[str],
    ):
        self.health_check_date = health_check_date
        self.feedback_by = feedback_by
        self.effort = effort
        self.effort_comment = effort_comment
        self.impact = impact
        self.impact_comment = impact_comment
        self.morale = morale
        self.morale_comment = morale_comment
        self.retention = retention
        self.retention_comment = retention_comment


class CorrelationSummary(BaseModel):
    def __init__(
        self,
        effort_vs_impact: Optional[float] = None,
        effort_vs_morale: Optional[float] = None,
        effort_vs_retention: Optional[float] = None,
        impact_vs_morale: Optional[float] = None,
        impact_vs_retention: Optional[float] = None,
        morale_vs_retention: Optional[float] = None,
    ):
        self.effort_vs_impact = effort_vs_impact
        self.effort_vs_morale = effort_vs_morale
        self.effort_vs_retention = effort_vs_retention
        self.impact_vs_morale = impact_vs_morale
        self.impact_vs_retention = impact_vs_retention
        self.morale_vs_retention = morale_vs_retention


class HealthCheckStatistics(BaseModel):
    def __init__(
        self,
        sample_size: int,
        means: Dict[str, Optional[float]],
        std_devs: Dict[str, Optional[float]],
        correlations: CorrelationSummary,
    ):
        self.sample_size = sample_size
        self.means = {
            "effort": means.get("effort"),
            "impact": means.get("impact"),
            "morale": means.get("morale"),
            "retention": means.get("retention"),
        }
        self.std_devs = {
            "effort": std_devs.get("effort"),
            "impact": std_devs.get("impact"),
            "morale": std_devs.get("morale"),
            "retention": std_devs.get("retention"),
        }
        self.correlations = correlations


class MemberHealthCheck(BaseModel):
    """
    Model representing a member's health check data and statistics.
    """

    feedback_data: List[FeedbackAssessment]
    statistics: Optional[HealthCheckStatistics]
