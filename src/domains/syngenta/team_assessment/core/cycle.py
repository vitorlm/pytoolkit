from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import date
from .task import TaskSummary
from .config import Config


@dataclass
class Cycle:
    _name: str
    _start_date: Optional[date] = None
    _end_date: Optional[date] = None
    _execution_duration: int = 0
    _backlog: Dict[str, TaskSummary] = field(default_factory=dict)
    _planned_tasks: Dict[str, TaskSummary] = field(default_factory=dict)
    _executed_tasks: Dict[str, TaskSummary] = field(default_factory=dict)
    _member_list: List[str] = field(default_factory=list)
    _summary: Optional[dict] = None
    _effective_duration: int = 0

    def __init__(self, name: str, config: Config):
        self._name = name
        self._config = config
        self._start_date = None
        self._end_date = None
        self._execution_duration = 0
        self._backlog = {}
        self._planned_tasks = {}
        self._executed_tasks = {}
        self._member_list = []
        self._summary = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def start_date(self) -> Optional[date]:
        return self._start_date

    @start_date.setter
    def start_date(self, value: Optional[date]):
        self._start_date = value

    @property
    def end_date(self) -> Optional[date]:
        return self._end_date

    @end_date.setter
    def end_date(self, value: Optional[date]):
        self._end_date = value

    @property
    def execution_duration(self) -> int:
        return self._execution_duration

    @execution_duration.setter
    def execution_duration(self, value: int):
        self._execution_duration = value

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
    def effective_duration(self) -> int:
        return self._effective_duration

    @summary.setter
    def effective_duration(self, value: int):
        self._effective_duration = value

    def summarize(self) -> dict:
        """
        Generates a consolidated summary of the cycle, including various metrics
        derived from the task summaries.
        """

        total_tasks = len(
            [task for task in self.backlog if task not in self._config.epics_helper_codes]
        )

        # Calculate time spent on bug, spillover, and out tasks
        time_spent_bug = sum(
            task.actual_duration
            for task in self.backlog.values()
            if task.code.lower() == self._config.epic_bug
        )

        time_spent_spillover = sum(
            task.actual_duration
            for task in self.backlog.values()
            if task.code.lower() == self._config.epic_spillover
        )

        time_spent_out = sum(
            task.actual_duration
            for task in self.backlog.values()
            if task.code.lower() == self._config.epic_out
        )

        # Duration metrics
        total_planned_duration = sum(
            task.planned_duration for task in self.backlog.values() if task.planned_duration
        )
        total_actual_duration = sum(
            task.actual_duration for task in self.backlog.values() if task.actual_duration
        )

        # Adherence metrics
        average_adherence_to_dates = (
            sum(task.adherence_to_dates for task in self.backlog.values()) / total_tasks
            if total_tasks > 0
            else 0
        )

        adherence_categories = {
            "Excellent": 0,
            "Good": 0,
            "Fair": 0,
            "Poor": 0,
        }
        for task in self.backlog.values():
            adherence_categories[task.categorize_adherence] += 1

        # Efficiency metrics
        average_efficiency = (
            sum(task.efficiency for task in self.backlog.values()) / total_tasks
            if total_tasks > 0
            else 0
        )

        # Resource utilization metrics
        average_resource_utilization = (
            sum(
                task.resource_utilization
                for task in self.backlog.values()
                if task.resource_utilization is not None
            )
            / total_tasks
            if total_tasks > 0
            else 0
        )

        # Count tasks by type
        tasks_by_type = {}
        for task in self.backlog.values():
            task_type = task.type
            tasks_by_type[task_type] = tasks_by_type.get(task_type, 0) + 1

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
            "total_tasks": total_tasks,
            "total_planned_duration": total_planned_duration,
            "total_actual_duration": total_actual_duration,
            "average_adherence_to_dates": average_adherence_to_dates,
            "adherence_categories": adherence_categories,
            "average_efficiency": average_efficiency,
            "average_resource_utilization": average_resource_utilization,
            "tasks_by_type": tasks_by_type,
            "members_contribution": members_contribution,
            "total_members": len(self.member_list),
            "time_spent_bug": time_spent_bug,
            "time_spent_spillover": time_spent_spillover,
            "time_spent_out": time_spent_out,
        }

        self.summary = summary

    def summarize_tasks(self):
        for task in self.backlog:
            task_summary = self.backlog[task]
            planned_task = self.planned_tasks.get(task)
            executed_task = self.executed_tasks.get(task)

            task_summary.planned_start_date = planned_task.start_date if planned_task else None
            task_summary.planned_end_date = planned_task.end_date if planned_task else None
            task_summary.actual_start_date = executed_task.start_date if executed_task else None
            task_summary.actual_end_date = executed_task.end_date if executed_task else None
            task_summary.planned_duration = planned_task.execution_duration if planned_task else 0
            task_summary.actual_duration = executed_task.execution_duration if executed_task else 0
            task_summary.planned_resources = len(planned_task.member_list) if planned_task else 0
            task_summary.actual_resources = len(executed_task.member_list) if executed_task else 0
            self.execution_duration += task_summary.actual_duration
