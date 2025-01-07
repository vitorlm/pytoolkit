from typing import Optional, Dict, List
from pydantic import BaseModel, model_validator


class FeedbackAssessment(BaseModel):
    """
    Model for validating health check data.
    """

    health_check_date: str
    feedback_by: str
    effort: Optional[int] = None
    effort_comment: Optional[str] = None
    impact: Optional[int] = None
    impact_comment: Optional[str] = None
    morale: Optional[int] = None
    morale_comment: Optional[str] = None
    retention: Optional[int] = None
    retention_comment: Optional[str] = None


class CorrelationSummary(BaseModel):
    effort_vs_impact: Optional[float] = None
    effort_vs_morale: Optional[float] = None
    effort_vs_retention: Optional[float] = None
    impact_vs_morale: Optional[float] = None
    impact_vs_retention: Optional[float] = None
    morale_vs_retention: Optional[float] = None


class HealthCheckStatistics(BaseModel):
    sample_size: int
    means: Dict[str, Optional[float]]
    std_devs: Dict[str, Optional[float]]
    correlations: CorrelationSummary

    @model_validator(mode="before")
    @classmethod
    def validate_and_initialize(cls, values: Dict) -> Dict:
        """
        Validator to initialize means and std_devs with default values if not provided.
        """
        means = values.get("means", {})
        std_devs = values.get("std_devs", {})
        values["means"] = {
            "effort": means.get("effort"),
            "impact": means.get("impact"),
            "morale": means.get("morale"),
            "retention": means.get("retention"),
        }
        values["std_devs"] = {
            "effort": std_devs.get("effort"),
            "impact": std_devs.get("impact"),
            "morale": std_devs.get("morale"),
            "retention": std_devs.get("retention"),
        }
        return values


class MemberHealthCheck(BaseModel):
    """
    Model representing a member's health check data and statistics.
    """

    feedback_data: List[FeedbackAssessment]
    statistics: Optional[HealthCheckStatistics] = None
