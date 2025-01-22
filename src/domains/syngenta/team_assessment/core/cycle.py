from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import date, timedelta
from .task import TaskSummary
from .config import Config


@dataclass
class Cycle:
    _name: str
    _start_date: Optional[date] = None
    _end_date: Optional[date] = None
    _cycle_work_days: Optional[int] = None
    _backlog: Dict[str, TaskSummary] = field(default_factory=dict)
    _planned_tasks: Dict[str, TaskSummary] = field(default_factory=dict)
    _executed_tasks: Dict[str, TaskSummary] = field(default_factory=dict)
    _member_list: List[str] = field(default_factory=list)
    _summary: Optional[dict] = None

    def __init__(self, name: str, config: Config):
        self._name = name
        self._config = config
        self._member_list = []
        self._backlog = {}
        self._planned_tasks = {}
        self._executed_tasks = {}
        self._summary = None
        self._cycle_work_days = 0

    def _recalculate_work_days(self):
        if self._start_date is not None and self._end_date is not None and self.member_list:
            cycle_duration = (self._end_date - self._start_date).days + 1
            weekdays = sum(
                1
                for i in range(cycle_duration)
                if (self._start_date + timedelta(days=i)).weekday() < 5
            )
            self._cycle_work_days = weekdays * len(self.member_list)

    @property
    def name(self) -> str:
        return self._name

    @property
    def start_date(self) -> Optional[date]:
        return self._start_date

    @start_date.setter
    def start_date(self, value: Optional[date]):
        self._start_date = value
        self._recalculate_work_days()

    @property
    def end_date(self) -> Optional[date]:
        return self._end_date

    @end_date.setter
    def end_date(self, value: Optional[date]):
        self._end_date = value
        self._recalculate_work_days()

    @property
    def backlog(self) -> Dict[str, TaskSummary]:
        return self._backlog

    @property
    def planned_tasks(self) -> Dict[str, TaskSummary]:
        return self._planned_tasks

    @property
    def executed_tasks(self) -> Dict[str, TaskSummary]:
        return self._executed_tasks

    @property
    def member_list(self) -> List[str]:
        return self._member_list

    @member_list.setter
    def member_list(self, value: List[str]):
        self._member_list = value

    @property
    def summary(self) -> Optional[dict]:
        return self._summary

    @summary.setter
    def summary(self, value: Optional[dict]):
        self._summary = value

    @property
    def cycle_work_days(self) -> Optional[int]:
        return self._cycle_work_days

    def summarize(self) -> dict:
        """
        Generates a consolidated summary of the cycle, including various metrics
        derived from the task summaries.
        """

        epics = [
            epic
            for epic in self.backlog.values()
            if epic and epic.code.lower() not in self._config.epics_helper_codes
        ]
        total_epics = len(epics)

        total_non_epics = len(self.backlog) - total_epics

        # Calculate time spent on bug, spillover, and out tasks
        total_bug_days = sum(
            task.executed_task_days
            for task in self.backlog.values()
            if task.code.lower() == self._config.epic_bug
        )

        total_spillover_days = sum(
            task.executed_task_days
            for task in self.backlog.values()
            if task.code.lower() == self._config.epic_spillover
        )

        total_off_days = sum(
            task.executed_task_days
            for task in self.backlog.values()
            if task.code.lower() == self._config.epic_out
        )

        # Duration metrics
        planned_cycle_days = sum(
            epic.planned_task_days for epic in self.backlog.values() if epic.planned_task_days
        )

        executed_cycle_days = sum(
            epic.executed_task_days for epic in self.backlog.values() if epic.executed_task_days
        )

        # Adherence metrics
        average_adherence_to_dates = (
            round(sum(epic.adherence_to_dates for epic in epics) / total_epics, 2)
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
            adherence_categories[epic.categorize_adherence] += 1

        # Efficiency metrics
        average_efficiency = (
            sum(task.efficiency for task in epics) / total_epics if total_epics > 0 else 0
        )

        # Resource utilization metrics
        average_resource_utilization = (
            round(
                sum(
                    task.resource_utilization
                    for task in self.backlog.values()
                    if task.resource_utilization is not None
                )
                / total_epics,
                2,
            )
            if total_epics > 0
            else 0
        )

        # Count tasks by type
        epics_by_type = {}
        for epic in self.backlog.values():
            if epic not in self._config.epics_helper_codes:
                epic_type = epic.type
                epics_by_type[epic_type] = epics_by_type.get(epic_type, 0) + 1

        # Contributions by members (using sets to avoid double-counting)
        members_contribution = {
            member: {"planned_tasks": 0, "executed_tasks": 0} for member in self.member_list
        }
        for member in self.member_list:
            planned_tasks = {
                task.code for task in self.planned_tasks.values() if member in task.member_list
            }
            members_contribution[member]["planned_tasks"] = len(planned_tasks)

            executed_tasks = {
                task.code for task in self.executed_tasks.values() if member in task.member_list
            }
            members_contribution[member]["executed_tasks"] = len(executed_tasks)

        summary = {
            "total_epics": total_epics,
            "total_non_epics": total_non_epics,
            "planned_cycle_days": planned_cycle_days,
            "executed_cycle_days": executed_cycle_days,
            "average_adherence_to_dates": average_adherence_to_dates,
            "adherence_categories": adherence_categories,
            "average_efficiency": average_efficiency,
            "average_resource_utilization": average_resource_utilization,
            "epics_by_type": epics_by_type,
            "members_contribution": members_contribution,
            "total_members": len(self.member_list),
            "total_bug_days": total_bug_days,
            "total_spillover_days": total_spillover_days,
            "total_off_days": total_off_days,
        }

        self.summary = summary

    def summarize_tasks(self):
        for task in self.backlog:
            task_summary = self.backlog[task]

            planned_task = self.planned_tasks.get(task)
            task_summary.planned_start_date = planned_task.start_date if planned_task else None
            task_summary.planned_end_date = planned_task.end_date if planned_task else None
            task_summary.planned_task_days = planned_task.task_total_days if planned_task else 0
            task_summary.planned_resources = len(planned_task.member_list) if planned_task else 0

            executed_task = self.executed_tasks.get(task)
            task_summary.actual_start_date = executed_task.start_date if executed_task else None
            task_summary.actual_end_date = executed_task.end_date if executed_task else None
            task_summary.executed_task_days = executed_task.task_total_days if executed_task else 0
            task_summary.actual_resources = len(executed_task.member_list) if executed_task else 0
