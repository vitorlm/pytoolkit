from datetime import date
from typing import List, Optional, Union

from pydantic import BaseModel, Field, model_validator


class IssueSummary(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    """
    Extends the Issue class to include planned and actual metrics for execution and adherence.
    """

    planned_start_date: Optional[date] = Field(None, description="Planned start date.")
    actual_start_date: Optional[date] = Field(None, description="Actual start date.")
    planned_end_date: Optional[date] = Field(None, description="Planned end date.")
    actual_end_date: Optional[date] = Field(None, description="Actual end date.")

    planned_issue_days: Optional[int] = Field(
        None, description="Planned duration in days."
    )
    executed_issue_days: Optional[int] = Field(
        None, description="Actual duration in days."
    )

    planned_resources: Optional[int] = Field(
        None, description="Planned number of resources."
    )
    actual_resources: Optional[int] = Field(
        None, description="Actual number of resources."
    )

    executed: Optional[bool] = Field(
        None, description="Indicates whether the issue was executed."
    )
    closed: Optional[bool] = Field(
        None, description="Indicates whether the issue is closed."
    )

    @model_validator(mode="after")
    def validate_summary_dates(self) -> "IssueSummary":
        """
        Validate the planned and actual start/end dates for logical consistency.
        """
        if (
            self.planned_start_date
            and self.planned_end_date
            and self.planned_start_date > self.planned_end_date
        ):
            raise ValueError("Planned end date must be after the planned start date.")
        if (
            self.actual_start_date
            and self.actual_end_date
            and self.actual_start_date > self.actual_end_date
        ):
            raise ValueError("Actual end date must be after the actual start date.")
        return self

    @property
    def planned_duration(self) -> int:
        """Calculates the planned duration in days."""
        if self.planned_start_date and self.planned_end_date:
            return (self.planned_end_date - self.planned_start_date).days + 1
        return 0

    @property
    def actual_duration(self) -> int:
        """Calculates the actual duration in days."""
        if self.actual_start_date and self.actual_end_date:
            return (self.actual_end_date - self.actual_start_date).days + 1
        return 0

    @property
    def adherence_to_dates(self) -> float:
        """
        Calculates adherence rate considering start and end dates with
        different weights, including a bonus for early completion and
        a quadratic penalty for delays.
        """

        start_weight = 0.4  # Weight of the start date in the adherence rate
        end_weight = 0.6  # Weight of the end date in the adherence rate
        early_bonus = 0.1  # Bonus per day of early completion

        start_adherence = 100  # Start with 100% adherence for start date
        end_adherence = 100  # Start with 100% adherence for end date

        if self.actual_start_date and self.planned_start_date:
            start_diff = (self.actual_start_date - self.planned_start_date).days
            if start_diff > 0:
                # Quadratic penalty for delays
                start_adherence -= (start_diff / self.planned_duration) ** 2 * 100
            else:
                start_adherence += abs(start_diff) * early_bonus

        if self.actual_end_date and self.planned_end_date:
            end_diff = (self.actual_end_date - self.planned_end_date).days
            if end_diff > 0:
                # Quadratic penalty for delays
                end_adherence -= (end_diff / self.planned_duration) ** 2 * 100
            else:
                end_adherence += abs(end_diff) * early_bonus

        total_adherence = (start_adherence * start_weight) + (
            end_adherence * end_weight
        )
        return max(0, min(total_adherence, 100))

    @property
    def categorize_adherence(self) -> str:
        """
        Categorize adherence to planned dates based on industry standards.

        Returns:
            str: Adherence category ('Excellent', 'Good', 'Fair', 'Poor').
        """
        adherence = self.adherence_to_dates / 100  # Normalize to a 0-1 scale

        if adherence >= 0.97:
            return "Excellent"
        elif adherence >= 0.9:
            return "Good"
        elif adherence >= 0.8:
            return "Fair"
        else:
            return "Poor"

    @property
    def schedule_adherence(self) -> float:
        """Calculates schedule adherence as a percentage."""
        if self.planned_issue_days:
            return (
                (self.executed_issue_days / self.planned_issue_days) * 100
                if self.executed_issue_days
                else 0
            )
        return 0.0

    @property
    def delay_in_days(self) -> int:
        """Calculates the delay in days."""
        if self.actual_end_date and self.planned_end_date:
            return (self.actual_end_date - self.planned_end_date).days
        return 0

    @property
    def date_deviation(self, unit: str = "days") -> Union[int, float]:
        """Calculates the total deviation from planned dates."""
        if unit not in ("days", "percentage"):
            raise ValueError("Invalid unit. Choose 'days' or 'percentage'.")

        start_deviation = abs((self.actual_start_date - self.planned_start_date).days)
        end_deviation = abs((self.actual_end_date - self.planned_end_date).days)
        total_deviation = start_deviation + end_deviation

        if unit == "percentage":
            if self.planned_duration:
                return (total_deviation / self.planned_duration) * 100
            return 0.0

        return total_deviation

    @property
    def duration_difference(self) -> int:
        """Calculates the difference between executed and planned duration."""
        if self.executed_issue_days and self.planned_issue_days:
            return self.executed_issue_days - self.planned_issue_days
        return 0

    @property
    def duration_deviation_percentage(self) -> float:
        """Calculates the duration deviation as a percentage."""
        if self.planned_issue_days:
            return (
                (self.executed_issue_days - self.planned_issue_days)
                / self.planned_issue_days
            ) * 100
        return 0.0

    @property
    def execution_ratio(self) -> float:
        """Calculates the ratio of executed to planned duration."""
        if self.planned_issue_days:
            return (
                self.executed_issue_days / self.planned_issue_days
                if self.executed_issue_days
                else 0.0
            )
        return 0.0

    @property
    def exceeded_time(self) -> int:
        """Calculates the amount of time exceeded."""
        if self.executed_issue_days and self.planned_issue_days:
            return max(0, self.executed_issue_days - self.planned_issue_days)
        return 0

    @property
    def saved_time(self) -> int:
        """Calculates the amount of time saved."""
        if self.executed_issue_days and self.planned_issue_days:
            return max(0, self.planned_issue_days - self.executed_issue_days)
        return 0

    @property
    def efficiency(
        self, duration_weight: float = 0.5, resource_weight: float = 0.5
    ) -> float:
        """
        Calculates the overall efficiency, considering both duration and resource utilization.
        The efficiency is capped at 100% to prevent values exceeding the maximum.
        """
        duration_efficiency = 0.0
        resource_efficiency = 0.0

        if self.executed_issue_days and self.planned_issue_days:
            duration_efficiency = min(
                (self.planned_issue_days / self.executed_issue_days) * 100, 100
            )  # Cap at 100%

        if self.actual_resources and self.planned_resources:
            resource_efficiency = min(
                (self.planned_resources / self.actual_resources) * 100, 100
            )  # Cap at 100%

        return min(
            (duration_efficiency * duration_weight)
            + (resource_efficiency * resource_weight),
            100,
        )

    @property
    def resource_utilization(self) -> Optional[float]:
        """Calculates resource utilization."""
        if self.planned_resources:
            return (
                self.actual_resources / self.planned_resources
                if self.actual_resources
                else None
            )
        return None

    @property
    def resource_allocation_accuracy(self) -> Optional[float]:
        """Calculates resource allocation accuracy."""
        if self.planned_resources and self.actual_resources is not None:
            deviation = abs(self.planned_resources - self.actual_resources)
            return (1 - deviation / self.planned_resources) * 100
        return None

    @property
    def resource_variance(self) -> Optional[int]:
        """Calculates resource variance."""
        if self.planned_resources is not None and self.actual_resources is not None:
            return self.planned_resources - self.actual_resources
        return None

    @property
    def resource_efficiency_index(self) -> Optional[float]:
        """Calculates resource efficiency index."""
        if self.actual_resources and self.executed_issue_days:
            return self.executed_issue_days / self.actual_resources
        return None


class IssueDetails(BaseModel):
    """
    Represents the details of an issue, either planned or executed.
    """

    start_date: Optional[date] = Field(None, description="Start date of the issue.")
    end_date: Optional[date] = Field(None, description="End date of the issue.")
    issue_total_days: Optional[int] = Field(
        0, description="Total days allocated for the issue."
    )
    member_list: List[str] = Field(
        default_factory=list, description="List of team members assigned to the issue."
    )


class Issue(BaseModel):
    """
    Represents an issue with metadata, dates, and related members.
    """

    code: str = Field("default_code", description="Unique identifier for the issue.")

    @model_validator(mode="before")
    def force_code_lowercase(cls, values):
        if "code" in values and values["code"]:
            values["code"] = values["code"].lower()
        return values

    type: str = Field(
        "default_type", description="Type of the issue (e.g., bug, task)."
    )
    jira: Optional[str] = Field(None, description="Jira ID associated with the issue.")
    description: Optional[str] = Field(None, description="Description of the issue.")

    closed: Optional[bool] = Field(
        False, description="Indicates whether the issue is closed."
    )

    planned: Optional[IssueDetails] = Field(
        None, description="Planned details for the issue."
    )
    executed: Optional[IssueDetails] = Field(
        None, description="Executed details for the issue."
    )

    summary: Optional[IssueSummary] = Field(
        None, description="Summary of planned and actual metrics."
    )

    @model_validator(mode="after")
    def validate_dates(self) -> "Issue":
        """
        Validate the start and end dates for logical consistency within planned and executed details
        """
        if self.planned and self.planned.start_date and self.planned.end_date:
            if self.planned.start_date > self.planned.end_date:
                raise ValueError(
                    "Planned end date must be after the planned start date."
                )
        if self.executed and self.executed.start_date and self.executed.end_date:
            if self.executed.start_date > self.executed.end_date:
                raise ValueError(
                    "Executed end date must be after the executed start date."
                )
        return self

    def summarize(self):
        """
        Calculates and updates the summary metrics for the issue.
        """

        if self.summary is None:
            self.summary = IssueSummary()

        planned_issue = self.planned
        executed_issue = self.executed

        self.summary.planned_start_date = (
            planned_issue.start_date if planned_issue else None
        )
        self.summary.planned_end_date = (
            planned_issue.end_date if planned_issue else None
        )
        self.summary.planned_issue_days = (
            planned_issue.issue_total_days if planned_issue else 0
        )
        self.summary.planned_resources = (
            len(planned_issue.member_list) if planned_issue else 0
        )

        self.summary.actual_start_date = (
            executed_issue.start_date if executed_issue else None
        )
        self.summary.actual_end_date = (
            executed_issue.end_date if executed_issue else None
        )
        self.summary.executed_issue_days = (
            executed_issue.issue_total_days if executed_issue else 0
        )
        self.summary.actual_resources = (
            len(executed_issue.member_list) if executed_issue else 0
        )

        if self.executed and self.executed.issue_total_days > 0:
            self.summary.executed = True
