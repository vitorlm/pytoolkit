from datetime import date
from typing import Optional, Union

from pydantic import BaseModel, model_validator


class Task(BaseModel):
    """
    Model for validating task data.

    Attributes:
        code (str): Task code.
        jira (Optional[str]): JIRA issue key.
        description (Optional[str]): Task description.
        type (str): Task type (e.g., Eng, Prod, Day-by-Day, Out, Bug).
    """

    code: str
    jira: Optional[str] = None
    description: Optional[str] = None
    type: str

    def __hash__(self) -> int:
        return hash((self.code, self.jira, self.description, self.type))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Task):
            return False
        return (
            self.code == other.code
            and self.jira == other.jira
            and self.description == other.description
            and self.type == other.type
        )


class TaskSummary(Task):
    """
    A class to represent the comparison between planned and actual execution of a task.
    """

    planned_start_date: Optional[date] = None
    actual_start_date: Optional[date] = None
    planned_end_date: Optional[date] = None
    actual_end_date: Optional[date] = None
    planned_duration: Optional[int] = None
    actual_duration: Optional[int] = None
    planned_resources: Optional[int] = None
    actual_resources: Optional[int] = None

    @property
    def adherence_to_dates(self) -> float:
        """
        Calculate adherence to planned dates, considering delays.
        This method uses a linear decrease in adherence based on the delay proportion.
        Future improvements could include using a weighted average based on the importance
        of starting and ending on time.

        Returns:
            float: Adherence percentage (0% to 100%).
        """
        adherence_start = 50
        adherence_end = 50

        if self.actual_start_date and self.planned_start_date:
            start_delay = (self.actual_start_date - self.planned_start_date).days
            planned_duration = (self.planned_end_date - self.planned_start_date).days
            start_delay_proportion = start_delay / planned_duration if planned_duration else 0
            adherence_start -= start_delay_proportion * 50

        if self.actual_end_date and self.planned_end_date:
            end_delay = (self.actual_end_date - self.planned_end_date).days
            planned_duration = (self.planned_end_date - self.planned_start_date).days
            end_delay_proportion = end_delay / planned_duration if planned_duration else 0
            adherence_end -= end_delay_proportion * 50

        return adherence_start + adherence_end

    @property
    def adherence_percentage(self) -> float:
        """
        Calculate the adherence percentage between the planned and actual durations.

        Returns:
            float: Adherence percentage (actual duration / planned duration * 100).
        """
        if self.planned_duration and self.planned_duration > 0:
            return (self.actual_duration / self.planned_duration) * 100
        return 0.0

    @property
    def categorize_adherence(self) -> str:
        """
        Categorize adherence to planned dates based on industry standards.

        Returns:
            str: Adherence category ('Excellent', 'Good', 'Fair', 'Poor').
        """
        adherence = self.adherence_to_dates / 100  # Normalize to a 0-1 scale

        if adherence > 1.1:
            return "Excellent"
        elif adherence > 0.9:
            return "Good"
        elif adherence > 0.8:
            return "Fair"
        else:
            return "Poor"

    @property
    def date_deviation(self, unit: str = "days") -> Union[int, float]:
        """
        Calculate the total deviation between planned and actual dates.

        Args:
            unit (str): The unit for the deviation ('days' or 'percentage').
                       Defaults to 'days'.

        Returns:
            Union[int, float]: The total deviation in days or as a percentage
                               of the planned duration.
        """
        if unit not in ("days", "percentage"):
            raise ValueError("Invalid unit. Choose 'days' or 'percentage'.")

        start_deviation = abs((self.actual_start_date - self.planned_start_date).days)
        end_deviation = abs((self.actual_end_date - self.planned_end_date).days)
        total_deviation = start_deviation + end_deviation

        if unit == "percentage":
            if self.planned_duration and self.planned_duration > 0:
                return (total_deviation / self.planned_duration) * 100
            return 0.0

        return total_deviation

    @property
    def delay_in_days(self) -> int:
        """
        Calculate the total delay in days between the planned and actual end dates.

        Returns:
            int: Delay in days (actual end date - planned end date).
        """
        if self.actual_end_date and self.planned_end_date:
            return (self.actual_end_date - self.planned_end_date).days
        return 0

    @property
    def relative_delay(self) -> float:
        """
        Calculate the relative delay as a percentage of the planned duration.

        Returns:
            float: Relative delay percentage (delay in days / planned duration * 100).
        """
        if self.planned_duration and self.planned_duration > 0:
            return (self.delay_in_days / self.planned_duration) * 100
        return 0

    @property
    def duration_difference(self) -> int:
        """
        Calculate the difference in days between actual and planned durations.

        Returns:
            int: Number of days the actual duration is above or below the planned duration
                 (actual duration - planned duration).
        """
        if self.actual_duration and self.planned_duration:
            return self.actual_duration - self.planned_duration
        return 0

    @property
    def duration_deviation_percentage(self) -> float:
        """
        Calculate the deviation as a percentage of the planned duration.

        Returns:
            float: Deviation percentage
                   ((actual duration - planned duration) / planned duration * 100).
        """
        if self.planned_duration and self.planned_duration > 0:
            return ((self.actual_duration - self.planned_duration) / self.planned_duration) * 100
        return 0.0

    @property
    def execution_ratio(self) -> float:
        """
        Calculate the ratio of actual duration to planned duration.

        Returns:
            float: Execution ratio (actual duration / planned duration).
        """
        if self.planned_duration and self.planned_duration > 0:
            return self.actual_duration / self.planned_duration
        return 0.0

    @property
    def exceeded_time(self) -> int:
        """
        Calculate the number of days the actual duration exceeded the planned duration.

        Returns:
            int: Exceeded time in days. Returns 0 if the task was completed on time or early.
        """
        if self.actual_duration and self.planned_duration:
            return max(0, self.actual_duration - self.planned_duration)
        return 0

    @property
    def saved_time(self) -> int:
        """
        Calculate the number of days saved if the actual duration was shorter than planned.

        Returns:
            int: Saved time in days. Returns 0 if the task took longer than planned.
        """
        if self.actual_duration and self.planned_duration:
            return max(0, self.planned_duration - self.actual_duration)
        return 0

    @property
    def efficiency(self, duration_weight: float = 0.5, resource_weight: float = 0.5) -> float:
        """
        Calculate the efficiency of the task considering both duration and resources.

        Args:
            duration_weight (float): The weight assigned to duration efficiency.
                                     Defaults to 0.5.
            resource_weight (float): The weight assigned to resource efficiency.
                                     Defaults to 0.5.

        Returns:
            float: Efficiency percentage. 100% means on time and budget,
                   >100% means better than planned, and <100% means over time or budget.
        """
        duration_efficiency = 0.0
        resource_efficiency = 0.0

        if self.actual_duration and self.planned_duration and self.actual_duration > 0:
            duration_efficiency = (self.planned_duration / self.actual_duration) * 100

        if self.actual_resources and self.planned_resources and self.actual_resources > 0:
            resource_efficiency = (self.planned_resources / self.actual_resources) * 100

        # Calculate the combined efficiency
        combined_efficiency = (duration_efficiency * duration_weight) + (
            resource_efficiency * resource_weight
        )

        return combined_efficiency

    @property
    def resource_utilization(self) -> Optional[float]:
        """
        Calculate the ratio of actual to planned resources.

        Returns:
            float: Utilization ratio. Values >1 indicate over-allocation of resources.
        """
        if self.planned_resources and self.planned_resources > 0:
            return self.actual_resources / self.planned_resources
        return None

    @property
    def resource_allocation_accuracy(self) -> Optional[float]:
        """
        Calculate the accuracy of resource allocation as a percentage.

        Returns:
            float: Accuracy percentage. Closer to 100% indicates better planning.
        """
        if self.planned_resources and self.planned_resources > 0:
            deviation = abs(self.planned_resources - self.actual_resources)
            return (1 - deviation / self.planned_resources) * 100
        return None

    @property
    def resource_variance(self) -> Optional[int]:
        """
        Calculate the variance in resources between planned and actual.

        Returns:
            int: Positive value indicates fewer resources used than planned.
                 Negative value indicates more resources used than planned.
        """
        if self.planned_resources is not None and self.actual_resources is not None:
            return self.planned_resources - self.actual_resources
        return None

    @property
    def resource_efficiency_index(self) -> Optional[float]:
        """
        Calculate the efficiency of resource usage based on actual results.

        Returns:
            float: Efficiency index. Values >1 indicate higher output per resource.
        """
        if self.actual_resources and self.actual_duration and self.actual_resources > 0:
            return self.actual_duration / self.actual_resources
        return None

    @model_validator(mode="after")
    def validate_dates(self) -> "TaskSummary":
        """
        Validate the planned and actual start/end dates.
        Ensure that:
        - Planned end date is after planned start date.
        - Actual end date is after actual start date.
        - Dates are provided in pairs (e.g., planned start and end).
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

        if (self.planned_start_date and not self.planned_end_date) or (
            self.planned_end_date and not self.planned_start_date
        ):
            raise ValueError("Both planned start and end dates must be provided.")

        if (self.actual_start_date and not self.actual_end_date) or (
            self.actual_end_date and not self.actual_start_date
        ):
            raise ValueError("Both actual start and end dates must be provided.")

        if self.planned_duration and self.planned_duration <= 0:
            raise ValueError("Planned duration must be a positive value.")

        if self.actual_duration and self.actual_duration <= 0:
            raise ValueError("Actual duration must be a positive value.")

        return self
