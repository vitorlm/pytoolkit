from pydantic import BaseModel, model_validator


class FeedbackAssessment(BaseModel):
    """Model for validating health check data."""

    health_check_date: str
    feedback_by: str
    effort: int | None = None
    effort_comment: str | None = None
    impact: int | None = None
    impact_comment: str | None = None
    morale: int | None = None
    morale_comment: str | None = None
    retention: int | None = None
    retention_comment: str | None = None


class CorrelationSummary(BaseModel):
    effort_vs_impact: float | None = None
    effort_vs_morale: float | None = None
    effort_vs_retention: float | None = None
    impact_vs_morale: float | None = None
    impact_vs_retention: float | None = None
    morale_vs_retention: float | None = None


class HealthCheckStatistics(BaseModel):
    sample_size: int
    means: dict[str, float | None]
    std_devs: dict[str, float | None]
    correlations: CorrelationSummary

    @model_validator(mode="before")
    @classmethod
    def validate_and_initialize(cls, values: dict) -> dict:
        """Validator to initialize means and std_devs with default values if not provided."""
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
    """Model representing a member's health check data and statistics."""

    feedback_data: list[FeedbackAssessment]
    statistics: HealthCheckStatistics | None = None
