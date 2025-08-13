"""
MCP Tool Argument Validators.

This module provides validation utilities for MCP tool arguments using Pydantic models.
"""

from typing import Type, Dict, Any, Optional
from pydantic import BaseModel, ValidationError

from mcp.types import TextContent
from utils.logging.logging_manager import LogManager

from .models import (
    JiraEpicMonitoringArgs,
    JiraCycleTimeArgs,
    JiraTeamVelocityArgs,
    JiraOpenIssuesArgs,
    SonarQubeProjectMetricsArgs,
    SonarQubeProjectIssuesArgs,
    SonarQubeQualityOverviewArgs,
    SonarQubeCompareProjectsArgs,
    LinearBTeamsArgs,
    LinearBMetricsArgs,
    LinearBExportArgs,
    CircleCIPipelineArgs,
    CircleCIProjectArgs,
    CircleCIWorkflowArgs,
)


class MCPToolValidator:
    """
    Validates MCP tool arguments using Pydantic models.

    Provides centralized validation logic for all MCP tools with
    structured error handling and logging.
    """

    # Mapping of tool names to their corresponding Pydantic models
    TOOL_MODELS: Dict[str, Type[BaseModel]] = {
        # Jira tools
        "jira_get_epic_monitoring": JiraEpicMonitoringArgs,
        "jira_get_cycle_time_metrics": JiraCycleTimeArgs,
        "jira_get_team_velocity": JiraTeamVelocityArgs,
        "jira_get_open_issues": JiraOpenIssuesArgs,
        # SonarQube tools
        "sonar_get_project_metrics": SonarQubeProjectMetricsArgs,
        "sonar_get_project_issues": SonarQubeProjectIssuesArgs,
        "sonar_get_quality_overview": SonarQubeQualityOverviewArgs,
        "sonar_compare_projects_quality": SonarQubeCompareProjectsArgs,
        # LinearB tools
        "linearb_get_teams": LinearBTeamsArgs,
        "linearb_get_metrics": LinearBMetricsArgs,
        "linearb_export_data": LinearBExportArgs,
        # CircleCI tools
        "circleci_get_pipelines": CircleCIPipelineArgs,
        "circleci_get_project_metrics": CircleCIProjectArgs,
        "circleci_get_workflows": CircleCIWorkflowArgs,
    }

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("MCPValidator")

    def validate_tool_args(
        self, tool_name: str, arguments: Dict[str, Any]
    ) -> BaseModel:
        """
        Validate tool arguments using the appropriate Pydantic model.

        Args:
            tool_name: Name of the MCP tool
            arguments: Dictionary of tool arguments to validate

        Returns:
            Validated Pydantic model instance

        Raises:
            ValidationError: If arguments are invalid
            ValueError: If tool name is not recognized
        """
        if tool_name not in self.TOOL_MODELS:
            raise ValueError(f"Unknown tool: {tool_name}")

        model_class = self.TOOL_MODELS[tool_name]

        try:
            # Validate and create model instance
            validated_model = model_class(**arguments)
            self.logger.debug(f"Successfully validated arguments for tool: {tool_name}")
            return validated_model

        except ValidationError as e:
            self.logger.error(f"Validation failed for tool {tool_name}: {e}")
            raise

    def format_validation_error(
        self, error: ValidationError, tool_name: str
    ) -> list[TextContent]:
        """
        Format a ValidationError into user-friendly MCP TextContent response.

        Args:
            error: The ValidationError to format
            tool_name: Name of the tool that failed validation

        Returns:
            List of TextContent with formatted error messages
        """
        error_messages = []

        # Main error message
        error_messages.append(f"❌ **Validation Error for tool '{tool_name}'**\n")

        # Individual field errors
        for err in error.errors():
            field_path = " → ".join(str(loc) for loc in err["loc"])
            message = err["msg"]
            value = err.get("input", "")

            error_messages.append(
                f"**Field:** `{field_path}`\n"
                f"**Error:** {message}\n"
                f"**Value:** `{value}`\n"
            )

        # Helpful suggestions
        error_messages.append(
            "\n💡 **Tips:**\n"
            "- Check parameter spelling and types\n"
            "- Ensure required fields are provided\n"
            "- Verify enum values are valid\n"
            "- Check date formats (YYYY-MM-DD)\n"
        )

        full_message = "\n".join(error_messages)

        return [TextContent(type="text", text=full_message)]

    def get_tool_schema(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the JSON schema for a tool's arguments.

        Args:
            tool_name: Name of the tool

        Returns:
            JSON schema dictionary or None if tool not found
        """
        if tool_name not in self.TOOL_MODELS:
            return None

        model_class = self.TOOL_MODELS[tool_name]
        return model_class.model_json_schema()

    def get_supported_tools(self) -> list[str]:
        """Get list of all supported tool names."""
        return list(self.TOOL_MODELS.keys())
