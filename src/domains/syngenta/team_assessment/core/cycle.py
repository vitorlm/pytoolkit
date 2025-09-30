from datetime import date, timedelta
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from .issue import Issue
from .config import Config


class CycleSummary(BaseModel):
    """
    Represents the summary of a cycle.
    """

    total_cycle_work_days: int = Field(0, description="Total work days in the cycle.")
    total_cycle_duration: int = Field(0, description="Total duration of the cycle.")
    total_bug_days: int = Field(0, description="Total number of bug days.")
    total_spillover_days: int = Field(0, description="Total number of spillover days.")
    total_off_days: int = Field(0, description="Total number of off days.")
    net_available_cycle_work_days: int = Field(
        0, description="Net available work days."
    )
    effective_work_days: int = Field(0, description="Effective work days.")
    total_epics: int = Field(0, description="Total number of epics in the cycle.")
    total_non_epics: int = Field(0, description="Total number of non-epic issues.")
    total_bugs: int = Field(0, description="Total number of bugs.")
    total_closed_epics: int = Field(0, description="Total number of closed epics.")
    total_spillover_epics: int = Field(
        0, description="Total number of spillover epics."
    )
    total_members: int = Field(0, description="Total number of members in the cycle.")

    epics_completion_rate: float = Field(0.0, description="Epics completion rate.")
    planned_cycle_days: int = Field(0, description="Total planned days for the cycle.")
    executed_cycle_days: int = Field(
        0, description="Total executed days for the cycle."
    )
    efficiency: float = Field(0.0, description="Efficiency of the cycle.")
    bug_workload_percentage: float = Field(0.0, description="Bug workload percentage.")
    bugs_epic_ratio: float = Field(0.0, description="Bug epic ratio.")
    spillover_workload_percentage: float = Field(
        0.0, description="Spillover workload percentage."
    )
    days_off_impact_ratio: float = Field(0.0, description="Days off impact ratio.")

    average_adherence_to_dates: float = Field(
        0.0, description="Average adherence to planned dates."
    )
    adherence_categories: Dict[str, int] = Field(
        default_factory=lambda: {"Excellent": 0, "Good": 0, "Fair": 0, "Poor": 0},
        description="Distribution of epics across adherence categories.",
    )
    average_efficiency: float = Field(
        0.0, description="Average efficiency of the cycle."
    )
    average_resource_utilization: float = Field(
        0.0, description="Average resource utilization."
    )
    epics_by_type: Dict[str, int] = Field(
        default_factory=dict, description="Number of epics by type."
    )
    members_contribution: Dict[str, Dict[str, int]] = Field(
        default_factory=dict, description="Contributions of each member."
    )


class Cycle(BaseModel):
    """
    Represents a cycle with its associated data and methods.
    """

    name: str = Field(..., description="Name of the cycle.")
    start_date: Optional[date] = Field(None, description="Start date of the cycle.")
    end_date: Optional[date] = Field(None, description="End date of the cycle.")
    backlog: Dict[str, Issue] = Field(
        default_factory=dict, description="Backlog of issues in the cycle."
    )
    planned_issues: List[str] = Field(
        default_factory=list, description="List of planned issue codes for the cycle."
    )
    executed_issues: List[str] = Field(
        default_factory=list, description="List of executed issue codes in the cycle."
    )
    closed_epics: List[Dict] = Field(
        default_factory=list, description="Closed epics in the cycle."
    )
    spillover_epics: List[Dict] = Field(
        default_factory=list, description="Spillover epics in the cycle."
    )
    member_list: List[str] = Field(
        default_factory=list, description="List of members in the cycle."
    )
    bugs: List[Dict] = Field(default_factory=list, description="Bugs in the cycle.")
    summary: Optional[CycleSummary] = Field(None, description="Summary of the cycle.")

    def __init__(self, config: Config, **data):
        super().__init__(**data)
        self._config = config

    def _calculate_cycle_duration_and_work_days(self):
        if self.start_date and self.end_date and self.member_list:
            cycle_duration = (self.end_date - self.start_date).days + 1
            weekdays = sum(
                1
                for i in range(cycle_duration)
                if (self.start_date + timedelta(days=i)).weekday() < 5
            )
            total_cycle_duration = weekdays
            total_cycle_work_days = weekdays * len(self.member_list)
            return total_cycle_work_days, total_cycle_duration
        return 0, 0

    def _calculate_total_days_by_type(self):
        """Calculates the total days spent on each issue type."""

        issue_types = [
            self._config.issue_type_bug,
            self._config.issue_type_spillover,
            self._config.issue_type_off,
        ]

        total_days = {}

        for issue_type in issue_types:
            if issue_type in self.executed_issues:
                issue = self.backlog[issue_type]
                total_days[issue_type] = issue.executed.issue_total_days
            else:
                total_days[issue_type] = 0

        return total_days

    def summarize(self):
        total_cycle_work_days, total_cycle_duration = (
            self._calculate_cycle_duration_and_work_days()
        )

        epics = [
            issue
            for issue in self.backlog.values()
            if issue
            and issue.executed
            and issue.code not in self._config.issue_helper_codes
        ]
        total_epics = len(epics)

        total_non_epics = len(self.backlog) - total_epics

        total_closed_epics = len(self.closed_epics)

        total_spillover_epics = len(self.spillover_epics)

        totals = self._calculate_total_days_by_type()

        total_bug_days = totals[self._config.issue_type_bug]
        total_spillover_days = totals[self._config.issue_type_spillover]
        total_off_days = totals[self._config.issue_type_off]

        net_available_cycle_work_days = total_cycle_work_days - total_off_days
        effective_work_days = (
            total_cycle_work_days
            - total_spillover_days
            - total_bug_days
            - total_off_days
        )

        total_bugs = len(self.bugs) if self.bugs else 0

        epics_completion_rate = (
            round(total_closed_epics / total_epics * 100, 2)
            if total_epics or total_epics == 0
            else 0
        )

        planned_cycle_days = sum(
            issue.summary.planned_issue_days
            for issue in self.backlog.values()
            if issue.summary and issue.summary.planned_issue_days
        )

        executed_cycle_days = sum(
            issue.summary.executed_issue_days
            for issue in self.backlog.values()
            if issue.summary and issue.summary.executed_issue_days
        )

        efficiency = round(
            (
                (net_available_cycle_work_days / executed_cycle_days) * 100
                if executed_cycle_days
                else 0
            ),
            2,
        )

        bug_workload_percentage = round(
            (
                (total_bug_days / net_available_cycle_work_days) * 100
                if net_available_cycle_work_days
                else 0
            ),
            2,
        )

        bugs_epic_ratio = round(
            ((total_bugs / total_epics) * 100 if total_epics else 0), 2
        )

        spillover_workload_percentage = round(
            (
                (total_spillover_days / net_available_cycle_work_days) * 100
                if net_available_cycle_work_days
                else 0
            ),
            2,
        )

        days_off_impact_ratio = round(
            (
                (total_off_days / total_cycle_duration) * 100
                if total_cycle_duration
                else 0
            ),
            2,
        )

        average_adherence_to_dates = (
            round(
                sum(epic.summary.adherence_to_dates for epic in epics) / total_epics, 2
            )
            if total_epics > 0
            else 0
        )

        adherence_categories = {
            "Excellent": 0,
            "Good": 0,
            "Fair": 0,
            "Poor": 0,
        }
        for epic in epics:
            adherence_categories[epic.summary.categorize_adherence] += 1

        average_efficiency = round(
            (
                sum(epic.summary.efficiency for epic in epics) / total_epics
                if total_epics > 0
                else 0
            ),
            2,
        )

        average_resource_utilization = (
            round(
                sum(
                    epic.summary.resource_utilization
                    for epic in epics
                    if epic.summary.resource_utilization is not None
                )
                / total_epics,
                2,
            )
            if total_epics > 0
            else 0
        )

        epics_by_type = {}
        for epic in epics:
            epic_type = epic.type
            epics_by_type[epic_type] = epics_by_type.get(epic_type, 0) + 1

        members_contribution = {
            member: {"planned_item": 0, "executed_item": 0}
            for member in self.member_list
        }
        for member in self.member_list:
            for code in self.planned_issues:
                issue = self.backlog[code]
                if member in issue.planned.member_list:
                    members_contribution[member]["planned_item"] += 1
            for code in self.executed_issues:
                issue = self.backlog[code]
                if member in issue.executed.member_list:
                    members_contribution[member]["executed_item"] += 1

        summary = {
            "total_cycle_work_days": total_cycle_work_days,
            "total_cycle_duration": total_cycle_duration,
            "total_bug_days": total_bug_days,
            "total_spillover_days": total_spillover_days,
            "total_off_days": total_off_days,
            "net_available_cycle_work_days": net_available_cycle_work_days,
            "effective_work_days": effective_work_days,
            "total_epics": total_epics,
            "total_non_epics": total_non_epics,
            "total_bugs": total_bugs,
            "total_closed_epics": total_closed_epics,
            "total_spillover_epics": total_spillover_epics,
            "total_members": len(self.member_list),
            "epics_completion_rate": epics_completion_rate,
            "planned_cycle_days": planned_cycle_days,
            "executed_cycle_days": executed_cycle_days,
            "efficiency": efficiency,
            "bug_workload_percentage": bug_workload_percentage,
            "bugs_epic_ratio": bugs_epic_ratio,
            "spillover_workload_percentage": spillover_workload_percentage,
            "days_off_impact_ratio": days_off_impact_ratio,
            "average_adherence_to_dates": average_adherence_to_dates,
            "adherence_categories": adherence_categories,
            "average_efficiency": average_efficiency,
            "average_resource_utilization": average_resource_utilization,
            "epics_by_type": epics_by_type,
            "members_contribution": members_contribution,
        }

        self.summary = CycleSummary(**summary)

    def summarize_issues(self):
        for issue in self.backlog.values():
            issue.summarize()
            if any(closed_epic.key == issue.jira for closed_epic in self.closed_epics):
                issue.closed = True
