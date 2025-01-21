from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import date
from .cycle import Cycle


@dataclass
class TeamSummary:
    _cycles: Dict[str, "Cycle"] = field(default_factory=dict)
    _total_days: int = 0
    _start_date: Optional[date] = None
    _end_date: Optional[date] = None
    _member_list: List[str] = field(default_factory=list)
    _summary: Optional[dict] = None

    def _calculate_attributes(self) -> None:
        """
        Calculates total_days, start_date, end_date, and member_list based on the provided cycles.
        """
        all_dates = [
            date
            for cycle in self._cycles.values()
            for date in (cycle.start_date, cycle.end_date)
            if date
        ]
        all_members = {member for cycle in self._cycles.values() for member in cycle.member_list}

        self._start_date = min(all_dates) if all_dates else None
        self._end_date = max(all_dates) if all_dates else None
        self._total_days = (
            (self._end_date - self._start_date).days if self._start_date and self._end_date else 0
        )
        self._member_list = sorted(all_members)

    def summarize(self) -> None:
        """
        Generates a consolidated summary of the team's performance over a period, aggregating data
        from individual cycles.
        """
        self._calculate_attributes()
        total_cycles = len(self._cycles)

        if total_cycles == 0:
            self._summary = {}
            return

        summary_aggregations = {
            "total_tasks": 0,
            "total_planned_duration": 0,
            "total_actual_duration": 0,
            "average_adherence_to_dates": 0,
            "average_efficiency": 0,
            "average_resource_utilization": 0,
            "time_spent_bug": 0,
            "time_spent_spillover": 0,
            "time_spent_out": 0,
        }

        adherence_categories = {"Excellent": 0, "Good": 0, "Fair": 0, "Poor": 0}
        tasks_by_type = {}
        members_contribution = {}

        for cycle in self._cycles.values():
            summary = cycle.summary
            summary_aggregations["total_tasks"] += summary.get("total_tasks", 0)
            summary_aggregations["total_planned_duration"] += summary.get(
                "total_planned_duration", 0
            )
            summary_aggregations["total_actual_duration"] += summary.get("total_actual_duration", 0)
            summary_aggregations["average_adherence_to_dates"] += summary.get(
                "average_adherence_to_dates", 0
            )
            summary_aggregations["average_efficiency"] += summary.get("average_efficiency", 0)
            summary_aggregations["average_resource_utilization"] += summary.get(
                "average_resource_utilization", 0
            )

            summary_aggregations["time_spent_bug"] += summary.get("time_spent_bug", 0)
            summary_aggregations["time_spent_spillover"] += summary.get("time_spent_spillover", 0)
            summary_aggregations["time_spent_out"] += summary.get("time_spent_out", 0)

            for category, count in summary.get("adherence_categories", {}).items():
                adherence_categories[category] += count

            for task_type, count in summary.get("tasks_by_type", {}).items():
                tasks_by_type[task_type] = tasks_by_type.get(task_type, 0) + count

            for member, contribution in summary.get("members_contribution", {}).items():
                if member not in members_contribution:
                    members_contribution[member] = {"planned_tasks": 0, "executed_tasks": 0}
                members_contribution[member]["planned_tasks"] += contribution.get(
                    "planned_tasks", 0
                )
                members_contribution[member]["executed_tasks"] += contribution.get(
                    "executed_tasks", 0
                )

        average_tasks_per_cycle = (
            summary_aggregations["total_tasks"] / total_cycles if total_cycles else 0
        )

        average_time_spent_bugs_per_cycle = (
            summary_aggregations["time_spent_bug"] / total_cycles if total_cycles else 0
        )

        average_time_spent_spillover_per_cycle = (
            summary_aggregations["time_spent_spillover"] / total_cycles if total_cycles else 0
        )

        average_time_spent_out_per_cycle = (
            summary_aggregations["time_spent_out"] / total_cycles if total_cycles else 0
        )

        self._summary = {
            "total_cycles": total_cycles,
            "total_tasks": summary_aggregations["total_tasks"],
            "average_tasks_per_cycle": average_tasks_per_cycle,
            "total_planned_duration": summary_aggregations["total_planned_duration"],
            "total_actual_duration": summary_aggregations["total_actual_duration"],
            "average_adherence_to_dates": summary_aggregations["average_adherence_to_dates"]
            / total_cycles,
            "adherence_categories": adherence_categories,
            "average_efficiency": summary_aggregations["average_efficiency"] / total_cycles,
            "average_resource_utilization": summary_aggregations["average_resource_utilization"]
            / total_cycles,
            "tasks_by_type": tasks_by_type,
            "members_contribution": members_contribution,
            "total_members": len(self._member_list),
            "total_time_spent_bug": summary_aggregations["time_spent_bug"],
            "total_time_spent_spillover": summary_aggregations["time_spent_spillover"],
            "total_time_spent_out": summary_aggregations["time_spent_out"],
            "average_time_spent_bugs_per_cycle": average_time_spent_bugs_per_cycle,
            "average_time_spent_spillover_per_cycle": average_time_spent_spillover_per_cycle,
            "average_time_spent_out_per_cycle": average_time_spent_out_per_cycle,
        }

    @property
    def cycles(self) -> Dict[str, "Cycle"]:
        return self._cycles

    def add_cycle(self, name: str, cycle: "Cycle") -> None:
        """
        Adds or updates a cycle in the cycles dictionary.
        Recalculates attributes and updates the summary.
        """
        self._cycles[name] = cycle

    @property
    def total_days(self) -> int:
        return self._total_days

    @property
    def start_date(self) -> Optional[date]:
        return self._start_date

    @property
    def end_date(self) -> Optional[date]:
        return self._end_date

    @property
    def member_list(self) -> List[str]:
        return self._member_list

    @property
    def summary(self) -> Optional[dict]:
        return self._summary
