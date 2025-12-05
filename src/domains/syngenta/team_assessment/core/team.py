from datetime import date

from pydantic import BaseModel, Field

from .config import Config
from .cycle import Cycle


class TeamSummary(BaseModel):
    """Represents the summary of a team's performance over a period."""

    total_work_days: int = Field(0, description="Total work days across all cycles.")
    total_duration: int = Field(0, description="Total duration of all cycles.")
    total_epics: int = Field(0, description="Total number of epics across all cycles.")
    total_non_epics: int = Field(0, description="Total number of non-epics across all cycles.")
    total_closed_epics: int = Field(0, description="Total number of closed epics across all cycles.")
    total_bug_days: int = Field(0, description="Total bug days across all cycles.")
    total_bugs: int = Field(0, description="Total number of bugs across all cycles.")
    bugs_per_day: float = Field(0.0, description="Bugs per day across all cycles.")
    total_spillover_days: int = Field(0, description="Total spillover days across all cycles.")
    total_off_days: int = Field(0, description="Total off days across all cycles.")
    total_executed_days: int = Field(0, description="Total executed days across all cycles.")
    total_planned_days: int = Field(0, description="Total planned days across all cycles.")
    epics_completion_rate: float = Field(0.0, description="Epics completion rate across all cycles.")
    average_adherence_to_dates: float = Field(0.0, description="Average adherence to dates across all cycles.")
    adherence_categories: dict[str, int] = Field(
        default_factory=lambda: {"Excellent": 0, "Good": 0, "Fair": 0, "Poor": 0},
        description="Distribution of epics across adherence categories.",
    )
    average_efficiency: float = Field(0.0, description="Average efficiency across all cycles.")
    average_resource_utilization: float = Field(0.0, description="Average resource utilization across all cycles.")
    epics_by_type: dict[str, int] = Field(
        default_factory=dict, description="Number of epics by type across all cycles."
    )
    members_contribution: dict[str, dict[str, int]] = Field(
        default_factory=dict,
        description="Contributions of each member across all cycles.",
    )
    average_epics_per_cycle: float = Field(0.0, description="Average epics per cycle.")
    average_bug_days_per_cycle: float = Field(0.0, description="Average bug days per cycle.")
    average_bugs_per_cycle: float = Field(0.0, description="Average bugs per cycle.")
    average_spillover_days_per_cycle: float = Field(0.0, description="Average spillover days per cycle.")
    average_off_days_per_cycle: float = Field(0.0, description="Average off days per cycle.")
    efficiency: float = Field(0.0, description="Efficiency across all cycles.")
    bug_workload_percentage: float = Field(0.0, description="Bug workload percentage across all cycles.")
    bugs_epic_ratio: float = Field(0.0, description="Bugs epic ratio across all cycles.")
    spillover_workload_percentage: float = Field(0.0, description="Spillover workload percentage across all cycles.")
    net_available_work_days: int = Field(0, description="Net available work days across all cycles.")
    effective_work_days: int = Field(0, description="Effective work days across all cycles.")
    days_off_impact_ratio: float = Field(0.0, description="Days off impact ratio across all cycles.")


class Team(BaseModel):
    """Represents a team with its basic information."""

    name: str = Field(..., description="Name of the team.")
    member_list: list[str] = Field(default_factory=list, description="List of team members.")
    cycles: dict[str, Cycle] = Field(default_factory=dict, description="Cycles data for the team.")
    start_date: date | None = Field(None, description="Start date of the earliest cycle.")
    end_date: date | None = Field(None, description="End date of the latest cycle.")
    summary: TeamSummary | None = Field(None, description="Summary of the team's performance.")

    def __init__(self, name: str, config: Config, **data):
        data.update({"name": name})
        super().__init__(**data)
        self._config = config

    def _calculate_dates(self) -> None:
        """Calculates start_date and end_date based on the provided cycles."""
        all_dates = [date for cycle in self.cycles.values() for date in (cycle.start_date, cycle.end_date) if date]
        self.start_date = min(all_dates) if all_dates else None
        self.end_date = max(all_dates) if all_dates else None

    def summarize(self) -> None:
        """Generates a consolidated summary of the team's performance over a period, aggregating data
        from individual cycles.
        """
        self._calculate_dates()

        cycle_count = len(self.cycles)

        if cycle_count == 0:
            return

        # Initialize attributes with default values
        total_work_days = 0
        total_duration = 0
        total_epics = 0
        total_non_epics = 0
        total_closed_epics = 0
        total_bug_days = 0
        total_bugs = 0
        bugs_per_day = 0
        total_spillover_days = 0
        total_off_days = 0
        total_executed_days = 0
        total_planned_days = 0
        epics_completion_rate = 0
        bug_workload_percentage = 0
        average_adherence_to_dates = 0
        adherence_categories = {"Excellent": 0, "Good": 0, "Fair": 0, "Poor": 0}
        average_efficiency = 0
        average_resource_utilization = 0
        epics_by_type = {}
        members_contribution = {}

        unique_non_epics = set()
        for cycle in self.cycles.values():
            summary = cycle.summary

            total_duration += summary.total_cycle_duration
            total_work_days += summary.total_cycle_work_days
            total_bug_days += summary.total_bug_days
            total_spillover_days += summary.total_spillover_days
            total_off_days += summary.total_off_days

            total_bugs += summary.total_bugs
            total_epics += summary.total_epics
            total_closed_epics += summary.total_closed_epics

            for epic in cycle.backlog.values():
                if epic and epic.code in self._config.issue_helper_codes and epic.code not in unique_non_epics:
                    unique_non_epics.add(epic.code)

            total_planned_days += summary.planned_cycle_days
            total_executed_days += summary.executed_cycle_days
            average_adherence_to_dates += summary.average_adherence_to_dates
            for category, count in summary.adherence_categories.items():
                adherence_categories[category] += count

            average_efficiency += summary.average_efficiency

            average_resource_utilization += summary.average_resource_utilization

            for epic_type, count in summary.epics_by_type.items():
                epics_by_type[epic_type] = epics_by_type.get(epic_type, 0) + count

            for member, contribution in summary.members_contribution.items():
                if member not in members_contribution:
                    members_contribution[member] = {
                        "planned_item": 0,
                        "executed_item": 0,
                    }
                members_contribution[member]["planned_item"] += contribution.get("planned_item", 0)
                members_contribution[member]["executed_item"] += contribution.get("executed_item", 0)

        total_non_epics += len(unique_non_epics)
        epics_completion_rate = round(
            ((total_closed_epics / total_epics) * 100 if total_epics or total_epics == 0 else 0),
            2,
        )

        average_epics_per_cycle = round(total_epics / cycle_count, 2) if cycle_count else 0
        average_bugs_per_cycle = round(total_bugs / cycle_count, 2) if cycle_count else 0
        average_bug_days_per_cycle = round(total_bug_days / cycle_count, 2) if cycle_count else 0
        average_spillover_days_per_cycle = round(total_spillover_days / cycle_count, 2) if cycle_count else 0
        average_off_days_per_cycle = round(total_off_days / cycle_count, 2) if cycle_count else 0

        net_available_work_days = total_work_days - total_off_days

        effective_work_days = total_work_days - total_spillover_days - total_bug_days - total_off_days

        efficiency = round(
            ((net_available_work_days / total_executed_days) * 100 if total_executed_days else 0),
            2,
        )

        bugs_per_day = round(total_bugs / net_available_work_days, 2) if net_available_work_days else 0

        bug_workload_percentage = round(
            (
                (total_bug_days / net_available_work_days) * 100
                if net_available_work_days or net_available_work_days == 0
                else 0
            ),
            2,
        )

        bugs_epic_ratio = round(
            (total_bugs / total_epics if total_epics or total_epics == 0 else 0),
            2,
        )

        spillover_workload_percentage = round(
            (
                (total_spillover_days / net_available_work_days) * 100
                if net_available_work_days or net_available_work_days == 0
                else 0
            ),
            2,
        )

        days_off_impact_ratio = round(
            ((total_off_days / total_work_days) * 100 if total_work_days or total_work_days == 0 else 0),
            2,
        )

        summary = {
            "total_work_days": total_work_days,
            "total_duration": total_duration,
            "total_epics": total_epics,
            "total_non_epics": total_non_epics,
            "total_closed_epics": total_closed_epics,
            "total_bug_days": total_bug_days,
            "total_bugs": total_bugs,
            "bugs_per_day": bugs_per_day,
            "total_spillover_days": total_spillover_days,
            "total_off_days": total_off_days,
            "total_executed_days": total_executed_days,
            "total_planned_days": total_planned_days,
            "epics_completion_rate": epics_completion_rate,
            "average_adherence_to_dates": (round(average_adherence_to_dates / cycle_count, 2) if cycle_count else 0),
            "adherence_categories": adherence_categories,
            "average_efficiency": round(average_efficiency / cycle_count, 2) if cycle_count else 0,
            "average_resource_utilization": (
                round(average_resource_utilization / cycle_count, 2) if cycle_count else 0
            ),
            "epics_by_type": epics_by_type,
            "members_contribution": members_contribution,
            "average_epics_per_cycle": average_epics_per_cycle,
            "average_bug_days_per_cycle": average_bug_days_per_cycle,
            "average_bugs_per_cycle": average_bugs_per_cycle,
            "average_spillover_days_per_cycle": average_spillover_days_per_cycle,
            "average_off_days_per_cycle": average_off_days_per_cycle,
            "efficiency": efficiency,
            "bug_workload_percentage": bug_workload_percentage,
            "bugs_epic_ratio": bugs_epic_ratio,
            "spillover_workload_percentage": spillover_workload_percentage,
            "net_available_work_days": net_available_work_days,
            "effective_work_days": effective_work_days,
            "days_off_impact_ratio": days_off_impact_ratio,
        }

        self.summary = TeamSummary(**summary)

    def add_cycle(self, name: str, cycle: Cycle) -> None:
        """Adds or updates a cycle in the cycles dictionary.
        Recalculates dates and updates the summary.
        """
        self.cycles[name] = cycle
