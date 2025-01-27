from typing import List, Dict, Optional
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
        feedback (Optional[Dict[str, Dict[str, List[Indicator]]]]): Feedback data.
        feedback_stats (Optional[IndividualStatistics]): Feedback statistics.
        tasks (Optional[List[Task]]): List of tasks assigned to the member.
        health_check (Optional[MemberHealthCheck]): Health check data for the member.
    """

    name: str
    last_name: Optional[str] = None
    feedback: Optional[Dict[str, Dict[str, List[Indicator]]]] = Field(default_factory=dict)
    feedback_stats: Optional[IndividualStatistics] = None  # Added this field
    tasks: Optional[List[Issue]] = Field(default_factory=list)
    health_check: Optional[MemberHealthCheck] = None

    @classmethod
    def create(
        cls,
        name: str,
        last_name: Optional[str] = None,
        feedback: Optional[Dict[str, Dict[str, List[Indicator]]]] = None,
        feedback_stats: Optional[IndividualStatistics] = None,
        tasks: Optional[List[Issue]] = None,
        health_check: Optional[MemberHealthCheck] = None,
    ) -> "Member":
        """
        Factory method to create a Member instance with custom initialization logic.

        Args:
            name (str): First name of the team member.
            last_name (Optional[str]): Last name of the team member.
            feedback (Optional[Dict[str, Dict[str, List[Indicator]]]]): Feedback data.
            feedback_stats (Optional[IndividualStatistics]): Feedback statistics.
            tasks (Optional[List[Task]]): List of tasks assigned to the member.
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
