"""Member Productivity Metrics Models

This module defines Pydantic models for comprehensive member productivity analysis,
combining planning data from Excel with JIRA execution metrics.

Models:
    - EpicAllocation: Epic data enriched with JIRA adherence metrics
    - TimeAllocation: Time distribution from planning Excel
    - AdherenceSummary: Aggregated adherence statistics
    - SpilloverSummary: Analysis of planned vs actual dates
    - BugMetrics: Bug work analysis with changelog tracking
    - EpicParticipation: Member's contribution to epics via child issues
    - CollaborationMetrics: Cross-team collaboration analysis
    - MemberProductivityMetrics: Main comprehensive productivity assessment

Follows PyToolkit patterns:
    - Pydantic v2.10 BaseModel with field descriptions
    - Type hints throughout
    - Validators for data consistency
    - Properties for calculated fields
"""

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field, computed_field, field_validator


class AssignmentPeriod(BaseModel):
    """Represents a time period when a member was assigned to an issue in WIP status.

    Used for tracking actual work time on bugs and child issues.
    """

    start: datetime = Field(..., description="Start timestamp of assignment period")
    end: datetime = Field(..., description="End timestamp of assignment period")
    status: str = Field(..., description="JIRA status during this period")

    @computed_field
    @property
    def duration_hours(self) -> float:
        """Calculate duration of assignment period in hours."""
        return (self.end - self.start).total_seconds() / 3600

    @computed_field
    @property
    def duration_days(self) -> float:
        """Calculate duration of assignment period in business days (8 hours)."""
        return self.duration_hours / 8


class EpicAllocation(BaseModel):
    """Represents an epic from planning with JIRA enrichment.

    Combines:
    - Planning data: allocated days per member
    - JIRA data: 4 custom dates (planned_start, planned_end, actual_start, actual_end)
    - Adherence metrics: planned_end vs actual_end
    - Due date changelog: track due_date changes
    """

    epic_key: str = Field(..., description="JIRA epic key (e.g., CWS-123)")
    epic_summary: str | None = Field(None, description="Epic summary/title")

    # Planning data
    allocated_days: float = Field(..., description="Days allocated to member in planning Excel")

    # JIRA custom fields (4 dates)
    planned_start_date: date | None = Field(None, description="Planned start date (customfield_10357)")
    planned_end_date: date | None = Field(None, description="Planned end date (customfield_10487)")
    actual_start_date: date | None = Field(None, description="Actual start date (customfield_10015)")
    actual_end_date: date | None = Field(None, description="Actual end date (customfield_10233)")

    # Due date tracking
    initial_due_date: date | None = Field(None, description="Initial due date from JIRA")
    current_due_date: date | None = Field(None, description="Current due date")
    due_date_changes: int = Field(0, description="Number of times due date changed")

    # Adherence metrics
    is_adherent: bool | None = Field(None, description="Whether planned_end >= actual_end")
    days_difference: int | None = Field(None, description="Difference between planned_end and actual_end")

    # JIRA metadata
    status: str | None = Field(None, description="Current JIRA status")
    assignee: str | None = Field(None, description="Current assignee")

    @field_validator("epic_key")
    @classmethod
    def validate_epic_key(cls, v: str) -> str:
        """Validate epic key format."""
        if not v or "-" not in v:
            raise ValueError(f"Invalid epic key format: {v}")
        return v.upper()

    @computed_field
    @property
    def planned_duration_days(self) -> int | None:
        """Calculate planned duration from custom fields."""
        if self.planned_start_date and self.planned_end_date:
            return (self.planned_end_date - self.planned_start_date).days + 1
        return None

    @computed_field
    @property
    def actual_duration_days(self) -> int | None:
        """Calculate actual duration from custom fields."""
        if self.actual_start_date and self.actual_end_date:
            return (self.actual_end_date - self.actual_start_date).days + 1
        return None


class TimeAllocation(BaseModel):
    """Time allocation from planning Excel.

    Represents how member's time is distributed across:
    - Epics (feature work)
    - Bugs (maintenance work)
    - Other activities (meetings, support, etc.)
    """

    total_allocated_days: float = Field(..., description="Total days allocated in cycle")

    # By type
    epic_days: float = Field(0.0, description="Days allocated to epics")
    bug_days: float = Field(0.0, description="Days allocated to bugs")
    other_days: float = Field(0.0, description="Days for other activities")

    # Metrics
    epic_count: int = Field(0, description="Number of epics allocated")
    bug_count: int = Field(0, description="Number of bugs allocated")

    @computed_field
    @property
    def utilization_rate(self) -> float:
        """Calculate utilization rate (% of time allocated)."""
        if self.total_allocated_days <= 0:
            return 0.0
        allocated = self.epic_days + self.bug_days + self.other_days
        return min((allocated / self.total_allocated_days) * 100, 100.0)

    @computed_field
    @property
    def epic_focus_percentage(self) -> float:
        """Calculate % of time on epics."""
        total = self.epic_days + self.bug_days + self.other_days
        if total <= 0:
            return 0.0
        return (self.epic_days / total) * 100

    @computed_field
    @property
    def bug_focus_percentage(self) -> float:
        """Calculate % of time on bugs."""
        total = self.epic_days + self.bug_days + self.other_days
        if total <= 0:
            return 0.0
        return (self.bug_days / total) * 100


class AdherenceSummary(BaseModel):
    """Summary of adherence metrics across all epics.

    Adherence = planned_end_date >= actual_end_date
    """

    total_epics: int = Field(0, description="Total epics worked on")
    adherent_epics: int = Field(0, description="Epics meeting planned_end date")
    non_adherent_epics: int = Field(0, description="Epics exceeding planned_end date")

    avg_days_difference: float = Field(0.0, description="Average days difference (planned vs actual)")
    max_delay_days: int = Field(0, description="Maximum delay in days")
    max_early_days: int = Field(0, description="Maximum early completion in days")

    @computed_field
    @property
    def adherence_rate(self) -> float:
        """Calculate adherence rate percentage."""
        if self.total_epics <= 0:
            return 0.0
        return (self.adherent_epics / self.total_epics) * 100


class SpilloverSummary(BaseModel):
    """Analysis of work spillover from one cycle to another.

    Spillover occurs when:
    - planned_end_date is in current cycle
    - actual_end_date is in future cycle
    """

    total_spillovers: int = Field(0, description="Number of epics spilling over")
    spillover_epics: list[str] = Field(default_factory=list, description="Epic keys that spilled over")
    avg_spillover_days: float = Field(0.0, description="Average spillover duration")

    @computed_field
    @property
    def spillover_rate(self) -> float:
        """Calculate spillover rate (requires total_epics from context)."""
        # Note: This would ideally reference total_epics, but kept simple for now
        return 0.0


class BugMetrics(BaseModel):
    """Bug work metrics with changelog tracking.

    Combines assignee changes + status changes to calculate:
    - Time spent on each bug (only when assigned AND in WIP status)
    - Handoff detection (multiple people working on same bug)
    - Average time per bug

    Pattern: See cycle_time_service.py lines 450-600
    """

    total_bugs_worked: int = Field(0, description="Total bugs worked on")
    total_time_hours: float = Field(0.0, description="Total time on bugs (hours)")
    total_time_days: float = Field(0.0, description="Total time on bugs (business days)")

    bugs_with_handoff: int = Field(0, description="Bugs with multiple assignees")
    avg_time_per_bug_hours: float = Field(0.0, description="Average time per bug (hours)")

    # Detailed bug data
    bug_details: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Detailed metrics per bug (key, time, assignment_periods)",
    )

    @computed_field
    @property
    def avg_time_per_bug_days(self) -> float:
        """Calculate average time per bug in business days."""
        if self.total_bugs_worked <= 0:
            return 0.0
        return self.total_time_days / self.total_bugs_worked


class EpicParticipation(BaseModel):
    """Member's participation in an epic via child issues.

    Analysis based on:
    - Child issues of epic (subtasks, stories linked via Parent Link)
    - Changelog: assignee + status changes
    - Time calculation: when assigned AND in WIP status
    """

    epic_key: str = Field(..., description="Epic key")

    # Child issue analysis
    total_child_issues: int = Field(0, description="Total child issues")
    child_issues_worked: int = Field(0, description="Child issues where member worked")

    # Time metrics
    total_days_in_epic: float = Field(0.0, description="Total days worked on epic's child issues")
    assignment_periods: list[AssignmentPeriod] = Field(
        default_factory=list, description="Assignment periods across all child issues"
    )

    # Planning comparison
    planned_days: float = Field(0.0, description="Days allocated in planning")

    @computed_field
    @property
    def execution_vs_planning_ratio(self) -> float:
        """Calculate ratio of actual vs planned days."""
        if self.planned_days <= 0:
            return 0.0
        return (self.total_days_in_epic / self.planned_days) * 100


class CollaborationMetrics(BaseModel):
    """Collaboration metrics across epics.

    Based on:
    - Changelog: assignees who worked on same issues
    - Comments: interactions on issues
    - Planning data: shared epic allocations
    """

    # Collaborators
    unique_collaborators: list[str] = Field(default_factory=list, description="Unique collaborators worked with")
    total_collaborators: int = Field(0, description="Total unique collaborators")

    # Issue interactions
    issues_with_collaboration: int = Field(0, description="Issues with multiple assignees")
    total_issues_analyzed: int = Field(0, description="Total issues analyzed")

    # Communication
    comments_received: int = Field(0, description="Comments received on assigned issues")
    comments_given: int = Field(0, description="Comments given on other issues")

    @computed_field
    @property
    def collaboration_rate(self) -> float:
        """Calculate collaboration rate percentage."""
        if self.total_issues_analyzed <= 0:
            return 0.0
        return (self.issues_with_collaboration / self.total_issues_analyzed) * 100


class MemberProductivityMetrics(BaseModel):
    """Main comprehensive productivity assessment for a member.

    Combines all metrics:
    - Time allocation (planning)
    - Adherence (4 custom dates + due_date)
    - Bug metrics (changelog)
    - Epic participation (child issues)
    - Collaboration (changelog + comments)
    - Spillover analysis
    - Overall scoring
    """

    # Member identification
    member_name: str = Field(..., description="Member name")
    cycle: str = Field(..., description="Cycle (e.g., Q4-C2)")
    cycle_start_date: date = Field(..., description="Cycle start date")
    cycle_end_date: date = Field(..., description="Cycle end date")

    # Core metrics
    time_allocation: TimeAllocation = Field(..., description="Time allocation from planning")
    adherence_summary: AdherenceSummary = Field(..., description="Adherence metrics")
    spillover_summary: SpilloverSummary = Field(..., description="Spillover analysis")
    bug_metrics: BugMetrics = Field(..., description="Bug work metrics")
    collaboration_metrics: CollaborationMetrics = Field(..., description="Collaboration metrics")

    # Epic details
    epics: list[EpicAllocation] = Field(default_factory=list, description="Epic allocations with adherence")
    epic_participations: list[EpicParticipation] = Field(
        default_factory=list, description="Epic participation via child issues"
    )

    # Overall assessment
    overall_score: float | None = Field(None, description="Overall productivity score (0-100)")
    performance_category: str | None = Field(None, description="Performance category (Excellent/Good/Fair/Poor)")

    # Metadata
    generated_at: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp when metrics were generated",
    )

    @field_validator("cycle")
    @classmethod
    def validate_cycle_format(cls, v: str) -> str:
        """Validate cycle format (Q1C1, Q2C2, etc.)."""
        import re

        pattern = r"^Q[1-4]C[1-2]$|^Q[1-4]-C[1-2]$"
        if not re.match(pattern, v, re.IGNORECASE):
            raise ValueError(f"Invalid cycle format: {v}. Expected format: Q1C1 or Q1-C1")
        return v.upper()

    @computed_field
    @property
    def cycle_duration_days(self) -> int:
        """Calculate cycle duration in days."""
        return (self.cycle_end_date - self.cycle_start_date).days + 1

    def calculate_overall_score(self) -> float:
        """Calculate overall productivity score (0-100).

        Weighted components:
        - Adherence: 30%
        - Utilization: 20%
        - Bug handling: 20%
        - Collaboration: 15%
        - Planning accuracy: 15%
        """
        adherence_score = self.adherence_summary.adherence_rate
        utilization_score = self.time_allocation.utilization_rate

        # Bug score: inverse of avg time per bug (faster is better, capped)
        bug_score = 100.0
        if self.bug_metrics.avg_time_per_bug_days > 0:
            # Assume ideal is ~2 days per bug, exponential penalty beyond that
            ideal_days = 2.0
            if self.bug_metrics.avg_time_per_bug_days > ideal_days:
                ratio = self.bug_metrics.avg_time_per_bug_days / ideal_days
                bug_score = max(0, 100 - (ratio - 1) * 50)

        collaboration_score = self.collaboration_metrics.collaboration_rate

        # Planning accuracy: how close actual work matches planned
        planning_score = 100.0
        if self.epic_participations:
            ratios = [ep.execution_vs_planning_ratio for ep in self.epic_participations if ep.planned_days > 0]
            if ratios:
                avg_ratio = sum(ratios) / len(ratios)
                # Penalize both under and over allocation
                deviation = abs(avg_ratio - 100)
                planning_score = max(0, 100 - deviation)

        # Weighted score
        overall = (
            adherence_score * 0.30
            + utilization_score * 0.20
            + bug_score * 0.20
            + collaboration_score * 0.15
            + planning_score * 0.15
        )

        self.overall_score = round(overall, 2)
        return self.overall_score

    def categorize_performance(self) -> str:
        """Categorize performance based on overall score.

        - Excellent: >= 85
        - Good: >= 70
        - Fair: >= 50
        - Poor: < 50
        """
        if self.overall_score is None:
            self.calculate_overall_score()

        score = self.overall_score or 0.0

        if score >= 85:
            category = "Excellent"
        elif score >= 70:
            category = "Good"
        elif score >= 50:
            category = "Fair"
        else:
            category = "Poor"

        self.performance_category = category
        return category


class EpicEnrichmentData(BaseModel):
    """JIRA enrichment data for an epic."""

    epic_key: str
    summary: str | None = None
    status: str | None = None
    planned_start_date: str | None = None  # ISO format: YYYY-MM-DD
    planned_end_date: str | None = None  # ISO format: YYYY-MM-DD
    actual_start_date: str | None = None  # ISO format: YYYY-MM-DD
    actual_end_date: str | None = None  # ISO format: YYYY-MM-DD
    due_date: str | None = None  # Fallback field
    resolution_date: str | None = None  # Fallback field


class EpicAdherenceMetrics(BaseModel):
    """Calculated adherence metrics for an epic."""

    epic_key: str
    summary: str | None = None
    status: str | None = None
    planned_end_date: str | None = None
    actual_end_date: str | None = None
    adherence_status: str  # on_time, early, late, in_progress, no_dates
    days_difference: int | None = None  # negative = early, positive = late, None = no_dates/in_progress


class MemberEpicAdherence(BaseModel):
    """Aggregate epic adherence statistics for a team member."""

    total_epics: int = 0
    on_time: int = 0
    early: int = 0
    late: int = 0
    in_progress: int = 0
    no_dates: int = 0
    adherence_rate: float = 0.0  # (on_time + early) / (on_time + early + late) * 100
    # REMOVED: epics_details (moved to TasksData.epics to separate raw data from metrics)


class EpicTaskData(BaseModel):
    """Complete epic data combining planning allocation and JIRA enrichment.

    This model represents raw epic task data, separating it from calculated metrics.
    """

    epic_key: str = Field(..., description="JIRA epic key")
    summary: str | None = Field(None, description="Epic summary from JIRA")
    status: str | None = Field(None, description="Current JIRA status")
    allocated_days: float = Field(..., description="Days allocated in planning")
    work_type: str = Field(..., description="Work type (Prod, Eng, etc)")
    category: str = Field(default="EPIC", description="Task category")

    # JIRA Enrichment
    jira_enrichment: dict | None = Field(None, description="JIRA adherence data")

    @staticmethod
    def from_task_and_adherence(
        task: dict, adherence: EpicAdherenceMetrics | None, enrichment: "EpicEnrichmentData | None" = None
    ) -> "EpicTaskData":
        """Create from planning task and JIRA adherence data.

        Args:
            task: Task dictionary with allocation data
            adherence: Calculated adherence metrics
            enrichment: Full JIRA enrichment data (optional, for complete date info)
        """
        jira_data = None
        if adherence:
            jira_data = {
                "planned_start_date": enrichment.planned_start_date if enrichment else None,
                "planned_end_date": adherence.planned_end_date,
                "actual_start_date": enrichment.actual_start_date if enrichment else None,
                "actual_end_date": adherence.actual_end_date,
                "adherence_status": adherence.adherence_status,
                "days_difference": adherence.days_difference,
            }

        return EpicTaskData(
            epic_key=task.get("jira_key", ""),
            summary=adherence.summary if adherence else task.get("description"),
            status=adherence.status if adherence else None,
            allocated_days=task.get("allocated_days", 0.0),
            work_type=task.get("type", ""),
            category=task.get("category", "EPIC"),
            jira_enrichment=jira_data,
        )


class TasksData(BaseModel):
    """Raw task data separated from metrics.

    This structure clearly separates raw data from calculated productivity metrics.
    """

    epics: list[EpicTaskData] = Field(default_factory=list, description="Epic tasks with JIRA enrichment")
    tasks_by_category: dict[str, list[dict]] = Field(default_factory=dict, description="Consolidated tasks by category")
    tasks_by_work_type: dict[str, list[dict]] = Field(
        default_factory=dict, description="Consolidated tasks by work type"
    )
