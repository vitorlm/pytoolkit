"""
Pydantic models for LinearB MCP tools validation.
"""

from typing import Optional
from enum import Enum
from pydantic import Field, field_validator

from .base import BaseMCPModel, OutputFileModel


class MetricsTypeEnum(str, Enum):
    """LinearB metrics types."""

    DELIVERY = "delivery"
    QUALITY = "quality"
    VELOCITY = "velocity"
    PLANNING = "planning"


class TimeRangeEnum(str, Enum):
    """LinearB time ranges."""

    LAST_WEEK = "last-week"
    LAST_MONTH = "last-month"
    LAST_QUARTER = "last-quarter"
    LAST_6_MONTHS = "last-6-months"


class LinearBTeamsArgs(BaseMCPModel, OutputFileModel):
    """Arguments for LinearB teams listing tool."""

    include_metrics: bool = Field(
        False,
        description="Include basic metrics for each team",
    )

    active_only: bool = Field(
        True,
        description="Only include active teams (default: true)",
    )


class LinearBMetricsArgs(BaseMCPModel, OutputFileModel):
    """Arguments for LinearB metrics tool."""

    teams: Optional[str] = Field(
        None,
        description="Comma-separated list of team names or IDs to analyze (optional, all teams if not specified)",
    )

    time_range: str = Field(
        "last-month",
        description="Time range for analysis: 'last-week', 'last-month', 'last-quarter', 'last-6-months'",
    )

    metrics_type: Optional[str] = Field(
        None,
        description="Specific metrics type: 'delivery', 'quality', 'velocity', 'planning' (optional, all if not specified)",
    )

    include_individuals: bool = Field(
        False,
        description="Include individual contributor metrics (default: false, team-level only)",
    )

    @field_validator("time_range")
    @classmethod
    def validate_time_range(cls, v):
        """Validate time range format."""
        valid_ranges = {"last-week", "last-month", "last-quarter", "last-6-months"}
        if v not in valid_ranges:
            raise ValueError(f"Invalid time_range: {v}. Valid options: {valid_ranges}")
        return v


class LinearBExportArgs(BaseMCPModel, OutputFileModel):
    """Arguments for LinearB export tool."""

    export_type: str = Field(
        ...,
        description="Export type: 'teams', 'metrics', 'reports'",
    )

    teams: Optional[str] = Field(
        None,
        description="Comma-separated list of team names to export (optional, all teams if not specified)",
    )

    date_range: Optional[str] = Field(
        None,
        description="Custom date range in format 'YYYY-MM-DD to YYYY-MM-DD' (optional)",
    )

    format_type: str = Field(
        "json",
        description="Export format: 'json', 'csv' (default: json)",
    )

    @field_validator("export_type")
    @classmethod
    def validate_export_type(cls, v):
        """Validate export type."""
        valid_types = {"teams", "metrics", "reports"}
        if v not in valid_types:
            raise ValueError(f"Invalid export_type: {v}. Valid options: {valid_types}")
        return v

    @field_validator("format_type")
    @classmethod
    def validate_format_type(cls, v):
        """Validate format type."""
        valid_formats = {"json", "csv"}
        if v not in valid_formats:
            raise ValueError(
                f"Invalid format_type: {v}. Valid options: {valid_formats}"
            )
        return v
