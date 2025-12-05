"""Pydantic models for CircleCI MCP tools validation."""

from enum import Enum

from pydantic import Field, field_validator

from .base import BaseMCPModel, OutputFileModel


class StatusEnum(str, Enum):
    """CircleCI pipeline/workflow statuses."""

    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    CANCELED = "canceled"
    NOT_RUN = "not_run"


class CircleCIPipelineArgs(BaseMCPModel, OutputFileModel):
    """Arguments for CircleCI pipeline analysis tool."""

    project: str = Field(
        ...,
        description="Project name or slug (e.g., 'my-org/my-repo')",
        min_length=1,
    )

    branch: str | None = Field(
        None,
        description="Branch name to filter pipelines (optional, all branches if not specified)",
    )

    status: str | None = Field(
        None,
        description="Pipeline status to filter: 'success', 'failed', 'running', 'canceled' (optional)",
    )

    time_range: str = Field(
        "last-week",
        description="Time range for analysis: 'last-week', 'last-month', 'last-quarter'",
    )

    limit: int = Field(
        100,
        description="Maximum number of pipelines to retrieve (default: 100, max: 500)",
        ge=1,
        le=500,
    )

    @field_validator("status")
    @classmethod
    def validate_status(cls, v):
        """Validate status value."""
        if v is None:
            return v

        valid_statuses = {"success", "failed", "running", "canceled", "not_run"}
        if v not in valid_statuses:
            raise ValueError(f"Invalid status: {v}. Valid options: {valid_statuses}")
        return v


class CircleCIProjectArgs(BaseMCPModel, OutputFileModel):
    """Arguments for CircleCI project metrics tool."""

    project: str = Field(
        ...,
        description="Project name or slug",
        min_length=1,
    )

    include_workflows: bool = Field(
        True,
        description="Include workflow statistics (default: true)",
    )

    include_jobs: bool = Field(
        False,
        description="Include job-level statistics (default: false)",
    )

    time_range: str = Field(
        "last-month",
        description="Time range for metrics: 'last-week', 'last-month', 'last-quarter'",
    )


class CircleCIWorkflowArgs(BaseMCPModel, OutputFileModel):
    """Arguments for CircleCI workflow analysis tool."""

    project: str = Field(
        ...,
        description="Project name or slug",
        min_length=1,
    )

    workflow_name: str | None = Field(
        None,
        description="Specific workflow name to analyze (optional, all workflows if not specified)",
    )

    status: str | None = Field(
        None,
        description="Workflow status to filter (optional)",
    )

    branch: str | None = Field(
        None,
        description="Branch name to filter (optional)",
    )

    time_range: str = Field(
        "last-week",
        description="Time range for analysis",
    )

    include_jobs: bool = Field(
        False,
        description="Include individual job details (default: false)",
    )
