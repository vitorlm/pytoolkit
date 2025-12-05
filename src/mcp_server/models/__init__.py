"""MCP Server Pydantic Models for Tool Validation.

This module provides Pydantic models for validating MCP tool arguments,
ensuring type safety and proper parameter validation for all PyToolkit integrations.
"""

from .base import BaseMCPModel
from .circleci_models import (
    CircleCIPipelineArgs,
    CircleCIProjectArgs,
    CircleCIWorkflowArgs,
)
from .jira_models import (
    JiraCycleTimeArgs,
    JiraEpicMonitoringArgs,
    JiraOpenIssuesArgs,
    JiraTeamVelocityArgs,
)
from .linearb_models import (
    LinearBExportArgs,
    LinearBMetricsArgs,
    LinearBTeamsArgs,
)
from .sonarqube_models import (
    SonarQubeCompareProjectsArgs,
    SonarQubeProjectIssuesArgs,
    SonarQubeProjectMetricsArgs,
    SonarQubeQualityOverviewArgs,
)

__all__ = [
    "BaseMCPModel",
    # Jira models
    "JiraEpicMonitoringArgs",
    "JiraCycleTimeArgs",
    "JiraTeamVelocityArgs",
    "JiraOpenIssuesArgs",
    # SonarQube models
    "SonarQubeProjectMetricsArgs",
    "SonarQubeProjectIssuesArgs",
    "SonarQubeQualityOverviewArgs",
    "SonarQubeCompareProjectsArgs",
    # LinearB models
    "LinearBTeamsArgs",
    "LinearBMetricsArgs",
    "LinearBExportArgs",
    # CircleCI models
    "CircleCIPipelineArgs",
    "CircleCIProjectArgs",
    "CircleCIWorkflowArgs",
]
