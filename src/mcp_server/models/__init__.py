"""
MCP Server Pydantic Models for Tool Validation.

This module provides Pydantic models for validating MCP tool arguments,
ensuring type safety and proper parameter validation for all PyToolkit integrations.
"""

from .base import BaseMCPModel
from .jira_models import (
    JiraEpicMonitoringArgs,
    JiraCycleTimeArgs,
    JiraTeamVelocityArgs,
    JiraOpenIssuesArgs,
)
from .sonarqube_models import (
    SonarQubeProjectMetricsArgs,
    SonarQubeProjectIssuesArgs,
    SonarQubeQualityOverviewArgs,
    SonarQubeCompareProjectsArgs,
)
from .linearb_models import (
    LinearBTeamsArgs,
    LinearBMetricsArgs,
    LinearBExportArgs,
)
from .circleci_models import (
    CircleCIPipelineArgs,
    CircleCIProjectArgs,
    CircleCIWorkflowArgs,
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
