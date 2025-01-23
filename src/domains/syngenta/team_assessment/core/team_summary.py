from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional
from .cycle import Cycle


@dataclass
class TeamSummary:
    _cycles: Dict[str, "Cycle"] = field(default_factory=dict)
    _cycle_count: Optional[int] = None
    _total_cycle_work_days: Optional[int] = None
    _start_date: Optional[date] = None
    _end_date: Optional[date] = None
    _total_work_days: Optional[int] = None
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
        self._start_date = min(all_dates) if all_dates else None
        self._end_date = max(all_dates) if all_dates else None

        all_members = {member for cycle in self._cycles.values() for member in cycle.member_list}
        self._member_list = sorted(all_members)

    def summarize(self) -> None:
        """
        Generates a consolidated summary of the team's performance over a period, aggregating data
        from individual cycles.
        """
        self._calculate_attributes()

        if self._cycle_count == 0:
            self._summary = {}
            return

        summary_aggregations = {
            "total_work_days": 0,
            "total_epics": 0,
            "total_non_epics": 0,
            "total_planned_days": 0,
            "total_executed_days": 0,
            "average_adherence_to_dates": 0,
            "adherence_categories": {},
            "average_efficiency": 0,
            "average_resource_utilization": 0,
            "epics_by_type": {},
            "members_contribution": {},
            "total_members": 0,
            "total_bug_days": 0,
            "total_spillover_days": 0,
            "total_off_days": 0,
        }

        adherence_categories = {"Excellent": 0, "Good": 0, "Fair": 0, "Poor": 0}
        epics_by_type = {}
        members_contribution = {}

        unique_non_epics = set()
        for cycle in self._cycles.values():
            summary = cycle.summary

            summary_aggregations["total_work_days"] += cycle.cycle_work_days
            summary_aggregations["total_epics"] += summary.get("total_epics", 0)

            for epic in cycle.backlog.values():
                if (
                    epic
                    and epic.code.lower() in self._config.epics_helper_codes
                    and epic.code.lower() not in unique_non_epics
                ):
                    unique_non_epics.add(epic.code.lower())
                    summary_aggregations["total_non_epics"] += len(unique_non_epics)

            summary_aggregations["planned_cycle_days"] += summary.get("planned_cycle_days", 0)
            summary_aggregations["executed_cycle_days"] += summary.get("executed_cycle_days", 0)
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
                epics_by_type[task_type] = epics_by_type.get(task_type, 0) + count

            for member, contribution in summary.get("members_contribution", {}).items():
                if member not in members_contribution:
                    members_contribution[member] = {"planned_item": 0, "executed_item": 0}
                members_contribution[member]["planned_item"] += contribution.get("planned_item", 0)
                members_contribution[member]["executed_item"] += contribution.get(
                    "executed_item", 0
                )

        average_epics_per_cycle = (
            summary_aggregations["total_epics"] / self._cycle_count if self._cycle_count else 0
        )

        average_bugs_days_per_cycle = (
            summary_aggregations["total_bug_days"] / self._cycle_count if self._cycle_count else 0
        )

        average_spillover_per_cycle = (
            summary_aggregations["total_spillover_days"] / self._cycle_count
            if self._cycle_count
            else 0
        )

        average_off_days_per_cycle = (
            summary_aggregations["total_off_days"] / self._cycle_count if self._cycle_count else 0
        )

        effective_availability = (
            summary_aggregations["total_work_days"] - summary_aggregations["total_off_days"]
        )

        effective_work_days = (
            summary_aggregations["total_work_days"]
            - summary_aggregations["total_spillover_days"]
            - summary_aggregations["total_bug_days"]
            - summary_aggregations["total_off_days"]
        )

        efficiency = (
            (summary_aggregations["total_executed_days"] / effective_work_days) * 100
            if effective_work_days
            else 0
        )
        bug_ratio = (
            (summary_aggregations["time_spent_bug"] / summary_aggregations["total_cycle_days"])
            * 100
            if summary_aggregations["total_cycle_days"]
            else 0
        )
        spillover_ratio = (
            (
                summary_aggregations["time_spent_spillover"]
                / summary_aggregations["total_cycle_days"]
            )
            * 100
            if summary_aggregations["total_cycle_days"]
            else 0
        )
        effective_availability = (
            (total_working_days / summary_aggregations["total_cycle_days"]) * 100
            if summary_aggregations["total_cycle_days"]
            else 0
        )
        productivity_per_day = (
            summary_aggregations["total_actual_duration"] / total_working_days
            if total_working_days
            else 0
        )
        avg_time_per_task = (
            summary_aggregations["total_actual_duration"] / summary_aggregations["total_tasks"]
            if summary_aggregations["total_tasks"]
            else 0
        )

        balance_time = {
            "task_time_ratio": (
                (
                    summary_aggregations["total_actual_duration"]
                    / summary_aggregations["total_cycle_days"]
                )
                * 100
                if summary_aggregations["total_cycle_days"]
                else 0
            ),
            "bug_time_ratio": (
                (summary_aggregations["time_spent_bug"] / summary_aggregations["total_cycle_days"])
                * 100
                if summary_aggregations["total_cycle_days"]
                else 0
            ),
            "out_time_ratio": (
                (summary_aggregations["time_spent_out"] / summary_aggregations["total_cycle_days"])
                * 100
                if summary_aggregations["total_cycle_days"]
                else 0
            ),
        }

        impact_days_off = (
            (summary_aggregations["time_spent_out"] / summary_aggregations["total_cycle_days"])
            * 100
            if summary_aggregations["total_cycle_days"]
            else 0
        )

        self._summary = {
            "cycle_count": self._cycle_count,
            "total_cycle_days": summary_aggregations["total_cycle_days"],
            "total_tasks": summary_aggregations["total_tasks"],
            "average_tasks_per_cycle": average_tasks_per_cycle,
            "total_planned_duration": summary_aggregations["total_planned_duration"],
            "total_actual_duration": summary_aggregations["total_actual_duration"],
            "average_adherence_to_dates": summary_aggregations["average_adherence_to_dates"]
            / self._cycle_count,
            "adherence_categories": adherence_categories,
            "average_efficiency": summary_aggregations["average_efficiency"] / self._cycle_count,
            "average_resource_utilization": summary_aggregations["average_resource_utilization"]
            / self._cycle_count,
            "tasks_by_type": epics_by_type,
            "members_contribution": members_contribution,
            "total_members": len(self._member_list),
            "total_time_spent_bug": summary_aggregations["time_spent_bug"],
            "total_time_spent_spillover": summary_aggregations["time_spent_spillover"],
            "total_time_spent_out": summary_aggregations["time_spent_out"],
            "average_time_spent_bugs_per_cycle": average_time_spent_bugs_per_cycle,
            "average_time_spent_spillover_per_cycle": average_time_spent_spillover_per_cycle,
            "average_time_spent_out_per_cycle": average_time_spent_out_per_cycle,
            "efficiency": efficiency,
            "bug_ratio": bug_ratio,
            "spillover_ratio": spillover_ratio,
            "effective_availability": effective_availability,
            "productivity_per_day": productivity_per_day,
            "avg_time_per_task": avg_time_per_task,
            "balance_time": balance_time,
            "impact_days_off": impact_days_off,
        }

    @property
    def cycles(self) -> Dict[str, "Cycle"]:
        return self._cycles

    @property
    def cycle_count(self) -> int:
        return len(self._cycles)

    def add_cycle(self, name: str, cycle: "Cycle") -> None:
        """
        Adds or updates a cycle in the cycles dictionary.
        Recalculates attributes and updates the summary.
        """
        self._cycles[name] = cycle

    @property
    def cycleDurationInDays(self) -> int:
        return self._total_cycle_work_days

    @property
    def start_date(self) -> Optional[date]:
        return self._start_date

    @property
    def end_date(self) -> Optional[date]:
        return self._end_date

    @property
    def total_work_days(self) -> Optional[int]:
        return self._total_work_days

    @property
    def member_list(self) -> List[str]:
        return self._member_list

    @property
    def summary(self) -> Optional[dict]:
        return self._summary
