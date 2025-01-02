from typing import List, Dict, Optional
from pydantic import BaseModel
from .statistics import IndividualStatistics
from .health_check import MemberHealthCheck
from .indicators import Indicator
from .task import Task


class Member(BaseModel):
    """
    Model for validating member-level competency data.

    Attributes:
        member (str): Name of the team member.
        indicators (List[Indicator]): List of competency indicators for this member.
    """

    name: str
    last_name: Optional[str]
    feedback: Optional[Dict[str, Dict[str, List[Indicator]]]] = {}
    tasks: Optional[List[Task]] = []
    health_check: Optional[MemberHealthCheck] = None

    def __init__(
        self,
        name: str,
        last_name: Optional[str],
        feedback: Optional[Dict[str, Dict[str, List[Indicator]]]] = None,
        feedback_stats: Optional[IndividualStatistics] = None,
        tasks: Optional[List[Task]] = None,
        health_check: Optional[MemberHealthCheck] = None,
    ):
        self.name = name
        self.last_name = last_name
        self.feedback = feedback or {}
        self.feedback_stats = feedback_stats
        self.tasks = tasks or []
        self.health_check = health_check
