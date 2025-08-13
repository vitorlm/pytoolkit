"""
Pydantic models for SonarQube MCP tools validation.
"""

from typing import Optional
from enum import Enum
from pydantic import Field, field_validator

from .base import BaseMCPModel, OutputFileModel, CacheControlModel


class SonarQubeOperationEnum(str, Enum):
    """Valid SonarQube operations."""

    PROJECTS = "projects"
    MEASURES = "measures"
    ISSUES = "issues"
    METRICS = "metrics"
    BATCH_MEASURES = "batch-measures"
    LIST_PROJECTS = "list-projects"


class SonarQubeIssueTypeEnum(str, Enum):
    """SonarQube issue types."""

    BUG = "BUG"
    VULNERABILITY = "VULNERABILITY"
    CODE_SMELL = "CODE_SMELL"


class SeverityEnum(str, Enum):
    """SonarQube issue severities."""

    BLOCKER = "BLOCKER"
    CRITICAL = "CRITICAL"
    MAJOR = "MAJOR"
    MINOR = "MINOR"
    INFO = "INFO"


class SonarQubeProjectMetricsArgs(BaseMCPModel, OutputFileModel, CacheControlModel):
    """Arguments for SonarQube project metrics tool."""

    project_key: str = Field(
        ...,
        description="SonarQube project key",
        min_length=1,
    )

    organization: Optional[str] = Field(
        None,
        description="SonarQube organization (optional). If not provided, uses default configuration",
    )

    metrics: Optional[str] = Field(
        None,
        description=(
            "Comma-separated list of specific metrics to retrieve. "
            "If not provided, uses default quality metrics set"
        ),
    )

    @field_validator("metrics")
    @classmethod
    def validate_metrics(cls, v):
        """Validate metrics format."""
        if v is None:
            return v

        # List of commonly used metrics for validation
        valid_metrics = {
            "alert_status",
            "bugs",
            "reliability_rating",
            "vulnerabilities",
            "security_rating",
            "security_hotspots_reviewed",
            "security_review_rating",
            "code_smells",
            "sqale_rating",
            "duplicated_lines_density",
            "coverage",
            "ncloc",
            "ncloc_language_distribution",
            "lines",
            "complexity",
            "cognitive_complexity",
            "branch_coverage",
            "line_coverage",
        }

        metrics_list = [m.strip() for m in v.split(",")]

        # Only warn about unknown metrics, don't fail
        for metric in metrics_list:
            if metric not in valid_metrics:
                # Log warning but continue (some metrics might be project-specific)
                pass

        return v


class SonarQubeProjectIssuesArgs(BaseMCPModel, OutputFileModel, CacheControlModel):
    """Arguments for SonarQube project issues tool."""

    project_key: str = Field(
        ...,
        description="SonarQube project key",
        min_length=1,
    )

    issue_type: SonarQubeIssueTypeEnum = Field(
        SonarQubeIssueTypeEnum.BUG,
        description="Issue type: BUG, VULNERABILITY, or CODE_SMELL",
    )

    severities: Optional[str] = Field(
        None,
        description="Comma-separated list of severities to filter (BLOCKER,CRITICAL,MAJOR,MINOR,INFO)",
    )

    organization: Optional[str] = Field(
        None,
        description="SonarQube organization (optional)",
    )

    @field_validator("severities")
    @classmethod
    def validate_severities(cls, v):
        """Validate severity values."""
        if v is None:
            return v

        valid_severities = {"BLOCKER", "CRITICAL", "MAJOR", "MINOR", "INFO"}
        severities = [s.strip().upper() for s in v.split(",")]

        for severity in severities:
            if severity not in valid_severities:
                raise ValueError(
                    f"Invalid severity: {severity}. Valid: {valid_severities}"
                )

        return ",".join(severities)


class SonarQubeQualityOverviewArgs(BaseMCPModel, OutputFileModel, CacheControlModel):
    """Arguments for SonarQube quality overview tool."""

    organization: Optional[str] = Field(
        None,
        description="SonarQube organization (e.g., 'syngenta-digital'). If not provided, uses default configuration",
    )

    project_keys: Optional[str] = Field(
        None,
        description="Comma-separated list of specific project keys to analyze (optional)",
    )

    use_project_list: bool = Field(
        True,
        description="Use pre-defined Syngenta projects list (default: true). Ignored if project_keys is provided",
    )

    include_measures: bool = Field(
        True,
        description="Include detailed metrics for each project (default: true)",
    )

    metrics: Optional[str] = Field(
        None,
        description=(
            "Comma-separated list of specific metrics to include. "
            "If not provided, uses comprehensive default metrics set"
        ),
    )


class SonarQubeCompareProjectsArgs(BaseMCPModel, OutputFileModel, CacheControlModel):
    """Arguments for SonarQube projects comparison tool."""

    project_keys: str = Field(
        ...,
        description="Comma-separated list of project keys to compare (minimum 2 projects)",
        min_length=1,
    )

    metrics: Optional[str] = Field(
        None,
        description="Comma-separated list of metrics to compare. If not provided, uses default comparison set",
    )

    organization: Optional[str] = Field(
        None,
        description="SonarQube organization (optional)",
    )

    @field_validator("project_keys")
    @classmethod
    def validate_project_keys(cls, v):
        """Ensure at least 2 projects for comparison."""
        if not v:
            raise ValueError("project_keys cannot be empty")

        projects = [p.strip() for p in v.split(",") if p.strip()]
        if len(projects) < 2:
            raise ValueError("At least 2 projects are required for comparison")

        return ",".join(projects)
