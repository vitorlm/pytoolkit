"""
Pydantic models for JIRA MCP tools validation.
"""

from typing import Optional
from enum import Enum
from pydantic import Field, field_validator

from .base import BaseMCPModel, OutputFileModel


class TimePeriodEnum(str, Enum):
    """Valid time periods for JIRA analysis."""

    LAST_WEEK = "last-week"
    LAST_2_WEEKS = "last-2-weeks"
    LAST_MONTH = "last-month"
    LAST_3_MONTHS = "last-3-months"
    LAST_6_MONTHS = "last-6-months"


class PriorityEnum(str, Enum):
    """JIRA priority levels."""

    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


class StatusCategoryEnum(str, Enum):
    """JIRA status categories."""

    TODO = "To Do"
    IN_PROGRESS = "In Progress"
    DONE = "Done"


class IssueTypeEnum(str, Enum):
    """Common JIRA issue types."""

    BUG = "Bug"
    STORY = "Story"
    TASK = "Task"
    EPIC = "Epic"
    TECHNICAL_DEBT = "Technical Debt"
    IMPROVEMENT = "Improvement"


class JiraEpicMonitoringArgs(BaseMCPModel, OutputFileModel):
    """Arguments for JIRA epic monitoring tool."""

    project_key: str = Field(
        ...,
        description="JIRA project key (e.g., 'CWS', 'DEV')",
        min_length=1,
        max_length=10,
    )

    team: Optional[str] = Field(
        None,
        description="Team name to filter (optional). If not provided, returns data from all teams",
        min_length=1,
    )


class JiraCycleTimeArgs(BaseMCPModel, OutputFileModel):
    """Arguments for JIRA cycle time analysis tool."""

    project_key: str = Field(
        ...,
        description="JIRA project key (e.g., 'CWS', 'DEV')",
        min_length=1,
        max_length=10,
    )

    time_period: str = Field(
        "last-week",
        description=(
            "Analysis period: 'last-week', 'last-2-weeks', 'last-month', "
            "'N-days' (e.g., '30-days'), date ranges (e.g., '2025-01-01 to 2025-01-31'), "
            "or single dates (e.g., '2025-01-15')"
        ),
    )

    issue_types: Optional[str] = Field(
        "Bug",
        description="Comma-separated list of issue types (e.g., 'Bug,Story,Task')",
    )

    team: Optional[str] = Field(
        None,
        description="Team name to filter using Squad[Dropdown] field (optional)",
        min_length=1,
    )

    priorities: Optional[str] = Field(
        None,
        description="Comma-separated list of priorities (e.g., 'Critical,High,Medium,Low')",
    )

    status_categories: Optional[str] = Field(
        "Done",
        description="Comma-separated list of status categories to include",
    )

    @field_validator("time_period")
    @classmethod
    def validate_time_period(cls, v):
        """Validate time period format."""
        if not v:
            raise ValueError("time_period cannot be empty")

        # Check if it's a predefined period
        predefined = ["last-week", "last-2-weeks", "last-month"]
        if v in predefined:
            return v

        # Check if it's N-days format
        if v.endswith("-days"):
            try:
                days = int(v.split("-")[0])
                if days <= 0:
                    raise ValueError("Days must be positive")
                return v
            except ValueError:
                raise ValueError(f"Invalid days format: {v}")

        # For date ranges and single dates, we'll let the service validate
        # since it's more complex (YYYY-MM-DD format validation)
        return v

    @field_validator("priorities")
    @classmethod
    def validate_priorities(cls, v):
        """Validate priority values."""
        if v is None:
            return v

        valid_priorities = {"Critical", "High", "Medium", "Low"}
        priorities = [p.strip() for p in v.split(",")]

        for priority in priorities:
            if priority not in valid_priorities:
                raise ValueError(
                    f"Invalid priority: {priority}. Valid: {valid_priorities}"
                )

        return v


class JiraTeamVelocityArgs(BaseMCPModel, OutputFileModel):
    """Arguments for JIRA team velocity analysis tool."""

    project_key: str = Field(
        ...,
        description="JIRA project key",
        min_length=1,
        max_length=10,
    )

    time_period: str = Field(
        "last-6-months",
        description="Analysis period: 'last-6-months', 'last-3-months', 'last-month', or date range",
    )

    issue_types: Optional[str] = Field(
        None,
        description="Comma-separated list of issue types (e.g., 'Story,Task,Epic,Technical Debt,Improvement')",
    )

    aggregation: Optional[str] = Field(
        "week",
        description="Aggregation level: 'day', 'week', 'month'",
    )

    team: Optional[str] = Field(
        None,
        description="Team name to filter (optional)",
        min_length=1,
    )


class JiraOpenIssuesArgs(BaseMCPModel, OutputFileModel):
    """Arguments for JIRA open issues analysis tool."""

    project_key: str = Field(
        ...,
        description="JIRA project key",
        min_length=1,
        max_length=10,
    )

    issue_types: Optional[str] = Field(
        "Bug,Story,Task",
        description="Comma-separated list of issue types to include",
    )

    team: Optional[str] = Field(
        None,
        description="Team name to filter (optional)",
        min_length=1,
    )

    status_categories: Optional[str] = Field(
        "To Do,In Progress",
        description="Comma-separated list of status categories (default: 'To Do,In Progress')",
    )
