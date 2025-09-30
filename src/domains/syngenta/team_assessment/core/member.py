from typing import Optional
from pydantic import BaseModel, Field
from .statistics import IndividualStatistics
from .health_check import MemberHealthCheck
from .indicators import Indicator
from .issue import Issue


class Member(BaseModel):
    """
    Model for validating member-level competency data.

    Attributes:
        name (str): First name of the team member.
        last_name (Optional[str]): Last name of the team member.
        feedback (Optional[dict[str, dict[str, list[Indicator]]]]): Feedback data.
        feedback_stats (Optional[IndividualStatistics]): Feedback statistics.
        tasks (Optional[list[Task]]): list of tasks assigned to the member.
        health_check (Optional[MemberHealthCheck]): Health check data for the member.
    """

    name: str
    last_name: Optional[str] = None
    feedback: Optional[dict[str, dict[str, list[Indicator]]]] = Field(
        default_factory=lambda: {}
    )
    feedback_stats: Optional[IndividualStatistics] = None  # Added this field
    tasks: Optional[list[Issue]] = Field(default_factory=lambda: [])
    health_check: Optional[MemberHealthCheck] = None

    @classmethod
    def create(
        cls,
        name: str,
        last_name: Optional[str] = None,
        feedback: Optional[dict[str, dict[str, list[Indicator]]]] = None,
        feedback_stats: Optional[IndividualStatistics] = None,
        tasks: Optional[list[Issue]] = None,
        health_check: Optional[MemberHealthCheck] = None,
    ) -> "Member":
        """
        Factory method to create a Member instance with custom initialization logic.

        Args:
            name (str): First name of the team member.
            last_name (Optional[str]): Last name of the team member.
            feedback (Optional[dict[str, dict[str, list[Indicator]]]]): Feedback data.
            feedback_stats (Optional[IndividualStatistics]): Feedback statistics.
            tasks (Optional[list[Task]]): list of tasks assigned to the member.
            health_check (Optional[MemberHealthCheck]): Health check data.

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
        )
