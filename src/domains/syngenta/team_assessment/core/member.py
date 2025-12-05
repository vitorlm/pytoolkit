from typing import Any

from pydantic import BaseModel, Field

from .health_check import MemberHealthCheck
from .indicators import Indicator
from .issue import Issue
from .statistics import IndividualStatistics


class Member(BaseModel):
    """Model for validating member-level competency data.

    Attributes:
        name (str): First name of the team member.
        last_name (Optional[str]): Last name of the team member.
        feedback (Optional[dict[str, dict[str, list[Indicator]]]]): Feedback data.
        feedback_stats (Optional[IndividualStatistics]): Feedback statistics.
        tasks (Optional[list[Task]]): list of tasks assigned to the member.
        health_check (Optional[MemberHealthCheck]): Health check data for the member.
        productivity_metrics (Optional[dict]): Productivity metrics from Planning + JIRA analysis.
        tasks_data (Optional[dict]): Raw task data separated from metrics (NEW).
    """

    name: str
    last_name: str | None = None
    feedback: dict[str, dict[str, list[Indicator]]] | None = Field(default_factory=lambda: {})
    feedback_stats: IndividualStatistics | None = None
    tasks: list[Issue] | None = Field(default_factory=lambda: [])
    health_check: MemberHealthCheck | None = None
    productivity_metrics: dict[str, Any] | None = None
    tasks_data: dict[str, Any] | None = None  # NEW: Separated task data

    @classmethod
    def create(
        cls,
        name: str,
        last_name: str | None = None,
        feedback: dict[str, dict[str, list[Indicator]]] | None = None,
        feedback_stats: IndividualStatistics | None = None,
        tasks: list[Issue] | None = None,
        health_check: MemberHealthCheck | None = None,
        productivity_metrics: dict[str, Any] | None = None,
        tasks_data: dict[str, Any] | None = None,
    ) -> "Member":
        """Factory method to create a Member instance with custom initialization logic.

        Args:
            name (str): First name of the team member.
            last_name (Optional[str]): Last name of the team member.
            feedback (Optional[dict[str, dict[str, list[Indicator]]]]): Feedback data.
            feedback_stats (Optional[IndividualStatistics]): Feedback statistics.
            tasks (Optional[list[Task]]): list of tasks assigned to the member.
            health_check (Optional[MemberHealthCheck]): Health check data.
            productivity_metrics (Optional[dict]): Productivity metrics.
            tasks_data (Optional[dict]): Raw task data separated from metrics (NEW).

        Returns:
            Member: A Member instance.
        """
        return cls(
            name=name,
            last_name=last_name,
            feedback=feedback or {},
            feedback_stats=feedback_stats,
            tasks=tasks or [],
            health_check=health_check,
            productivity_metrics=productivity_metrics,
            tasks_data=tasks_data,
        )
