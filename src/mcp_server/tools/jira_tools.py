import json
from typing import Any
from pydantic import ValidationError

from mcp.types import TextContent, Tool

from ..adapters.jira_adapter import JiraAdapter
from ..validators import MCPToolValidator
from utils.logging.logging_manager import LogManager


class JiraTools:
    """MCP Tools for JIRA integration via PyToolkit."""

    def __init__(self):
        """
        Initialize JIRA Tools MCP handler.

        Sets up the JIRA adapter and logger for processing JIRA-related
        MCP tool requests.
        """
        self.adapter = JiraAdapter()
        self.logger = LogManager.get_instance().get_logger("JiraTools")
        self.validator = MCPToolValidator()

    @staticmethod
    def get_tool_definitions() -> list[Tool]:
        """Returns definitions of all JIRA tools."""
        return [
            Tool(
                name="jira_get_epic_monitoring",
                description=(
                    "Gets JIRA epic monitoring data with status, dates and identified issues"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "JIRA project key (ex: 'SCRUM', 'DEV')",
                        },
                        "team": {
                            "type": "string",
                            "description": "Team name to filter (optional). If not provided, returns data from all teams in the project.",
                        },
                        "output_file": {
                            "type": "string",
                            "description": "Optional file path to save results in JSON format",
                        },
                        "verbose": {
                            "type": "boolean",
                            "description": "Enable verbose output with detailed information",
                            "default": False,
                        },
                    },
                    "required": ["project_key"],
                },
            ),
            Tool(
                name="jira_get_cycle_time_metrics",
                description="Gets cycle time metrics for team performance analysis",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "JIRA project key",
                        },
                        "time_period": {
                            "type": "string",
                            "description": "Analysis period: 'last-week', 'last-2-weeks', 'last-month', 'N-days' (ex: '30-days'), or date range (ex: '2025-01-01,2025-01-31')",
                            "default": "last-week",
                        },
                        "issue_types": {
                            "type": "string",
                            "description": "Comma-separated list of issue types (ex: 'Bug,Story,Task'). If not provided, uses 'Bug,Story'",
                        },
                        "team": {
                            "type": "string",
                            "description": "Team name to filter (optional). If not provided, analyzes all teams in the project.",
                        },
                        "priorities": {
                            "type": "string",
                            "description": "Comma-separated list of priorities to filter (optional)",
                        },
                        "status_categories": {
                            "type": "string",
                            "description": "Comma-separated list of status categories to include (optional)",
                        },
                        "output_file": {
                            "type": "string",
                            "description": "Optional file path to save results in JSON format",
                        },
                        "verbose": {
                            "type": "boolean",
                            "description": "Enable verbose output with detailed information",
                            "default": False,
                        },
                    },
                    "required": ["project_key"],
                },
            ),
            Tool(
                name="jira_get_team_velocity",
                description="Gets team velocity data based on previous sprints",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "JIRA project key",
                        },
                        "time_period": {
                            "type": "string",
                            "description": "Analysis period: 'last-6-months', 'last-3-months', 'last-month', or date range",
                            "default": "last-6-months",
                        },
                        "issue_types": {
                            "type": "string",
                            "description": "Comma-separated list of issue types (ex: 'Story,Task,Epic,Technical Debt,Improvement')",
                        },
                        "aggregation": {
                            "type": "string",
                            "description": "Temporal aggregation: 'monthly', 'quarterly' (default: 'monthly')",
                            "default": "monthly",
                        },
                        "team": {
                            "type": "string",
                            "description": "Team name to filter (optional). If not provided, analyzes all teams in the project.",
                        },
                    },
                    "required": ["project_key"],
                },
            ),
            Tool(
                name="jira_get_issue_adherence",
                description="Analyzes team adherence to deadlines and delivery dates",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "JIRA project key",
                        },
                        "time_period": {
                            "type": "string",
                            "description": "Analysis period: 'last-week', 'last-2-weeks', 'last-month', 'N-days' (ex: '30-days'), or date range (ex: '2025-01-01,2025-01-31')",
                            "default": "last-month",
                        },
                        "issue_types": {
                            "type": "string",
                            "description": "Comma-separated list of issue types (ex: 'Bug,Support,Story,Task'). If not provided, uses 'Bug,Story'",
                        },
                        "team": {
                            "type": "string",
                            "description": "Team name to filter (optional). If not provided, analyzes all teams in the project.",
                        },
                        "status_categories": {
                            "type": "string",
                            "description": "Comma-separated list of status categories to include (ex: 'Done,In Progress,To Do')",
                        },
                        "include_no_due_date": {
                            "type": "boolean",
                            "description": "Include issues without due date in analysis (default: false)",
                            "default": False,
                        },
                    },
                    "required": ["project_key"],
                },
            ),
            Tool(
                name="jira_get_open_issues",
                description="Gets list of open issues in the project with optional filters",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_key": {
                            "type": "string",
                            "description": "JIRA project key",
                        },
                        "issue_types": {
                            "type": "string",
                            "description": "Comma-separated list of issue types (ex: 'Bug,Support,Story,Task')",
                        },
                        "team": {
                            "type": "string",
                            "description": "Team name to filter (optional). If not provided, returns issues from all teams in the project.",
                        },
                        "status_categories": {
                            "type": "string",
                            "description": "Comma-separated list of status categories to include (ex: 'To Do,In Progress'). If not provided, includes all non-'Done' categories",
                        },
                        "priorities": {
                            "type": "string",
                            "description": "Comma-separated list of priorities to filter (optional)",
                        },
                    },
                    "required": ["project_key"],
                },
            ),
        ]

    async def execute_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> list[TextContent]:
        """Executes specific JIRA tool with validation."""
        self.logger.info(f"Executing JIRA tool: {name} with args: {arguments}")

        try:
            # Validate arguments using Pydantic models
            try:
                validated_args = self.validator.validate_tool_args(name, arguments)
                self.logger.debug(f"Arguments validated successfully for {name}")
            except ValidationError as e:
                self.logger.warning(f"Validation failed for {name}: {e}")
                return self.validator.format_validation_error(e, name)
            except ValueError:
                # Tool not found in validator
                self.logger.debug(
                    f"No validator for {name}, proceeding without validation"
                )
                validated_args = None

            # Execute tool with validated arguments
            if name == "jira_get_epic_monitoring":
                return await self._get_epic_monitoring(
                    validated_args.model_dump() if validated_args else arguments
                )
            elif name == "jira_get_cycle_time_metrics":
                return await self._get_cycle_time_metrics(
                    validated_args.model_dump() if validated_args else arguments
                )
            elif name == "jira_get_team_velocity":
                return await self._get_team_velocity(
                    validated_args.model_dump() if validated_args else arguments
                )
            elif name == "jira_get_issue_adherence":
                return await self._get_issue_adherence(
                    arguments
                )  # No validator model yet
            elif name == "jira_get_open_issues":
                return await self._get_open_issues(
                    validated_args.model_dump() if validated_args else arguments
                )
            else:
                error_msg = f"Unknown JIRA tool '{name}'"
                self.logger.error(error_msg)
                return [TextContent(type="text", text=f"Error: {error_msg}")]
        except Exception as e:
            error_msg = f"Error executing JIRA tool '{name}': {str(e)}"
            self.logger.error(error_msg)
            return [TextContent(type="text", text=error_msg)]

    async def _get_epic_monitoring(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Execute epic monitoring analysis.

        Retrieves and formats epic monitoring data including status, dates,
        and identified problems for the specified project.

        Args:
            args: Dictionary containing project_key and optional team parameter

        Returns:
            list[TextContent]: Formatted epic monitoring results

        Raises:
            Exception: If epic monitoring data retrieval fails
        """
        project_key = args["project_key"]
        team = args.get("team")
        self.logger.info(
            f"Getting epic monitoring data for project: {project_key}, team: {team}"
        )

        try:
            if team:
                data = self.adapter.get_epic_monitoring_data(project_key, team)
            else:
                # Call without team parameter to get all teams
                data = self.adapter.get_epic_monitoring_data(project_key)

            formatted_result = {
                "project_key": project_key,
                "team": team,
                "epic_monitoring": data,
                "summary": {
                    "total_epics": (
                        len(data.get("epics", [])) if isinstance(data, dict) else "N/A"
                    ),
                    "timestamp": (
                        data.get("timestamp") if isinstance(data, dict) else None
                    ),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Epic Monitoring Data for {project_key} (team: {team or 'all'}):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get epic monitoring data: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve epic monitoring data for {project_key}: {str(e)}",
                )
            ]

    async def _get_cycle_time_metrics(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Execute cycle time analysis.

        Analyzes cycle time metrics for issues in the specified project,
        providing insights into team performance and delivery efficiency.

        Args:
            args: Dictionary containing project_key and optional analysis parameters

        Returns:
            list[TextContent]: Formatted cycle time analysis results

        Raises:
            Exception: If cycle time analysis fails
        """
        project_key = args["project_key"]
        time_period = args.get("time_period", "last-week")
        issue_types_str = args.get("issue_types", "Bug,Story")
        team = args.get("team")
        priorities_str = args.get("priorities")
        status_categories_str = args.get("status_categories")

        # Parse comma-separated strings into lists
        issue_types = (
            [t.strip() for t in issue_types_str.split(",")]
            if issue_types_str
            else ["Bug", "Story"]
        )
        priorities = (
            [p.strip() for p in priorities_str.split(",")] if priorities_str else None
        )
        status_categories = (
            [s.strip() for s in status_categories_str.split(",")]
            if status_categories_str
            else None
        )

        self.logger.info(
            f"Getting cycle time metrics for {project_key}, period: {time_period}, team: {team}"
        )

        try:
            data = self.adapter.get_cycle_time_analysis(
                project_key=project_key,
                time_period=time_period,
                issue_types=issue_types,
                team=team,
            )

            formatted_result = {
                "project_key": project_key,
                "time_period": time_period,
                "issue_types": issue_types,
                "team": team,
                "priorities": priorities,
                "status_categories": status_categories,
                "cycle_time_data": data,
                "summary": {
                    "total_issues_analyzed": (
                        len(data.get("issues", [])) if isinstance(data, dict) else "N/A"
                    ),
                    "timestamp": (
                        data.get("timestamp") if isinstance(data, dict) else None
                    ),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Cycle Time Metrics for {project_key} (period: {time_period}, team: {team or 'all'}):\n"
                    f"{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get cycle time metrics: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve cycle time metrics for {project_key}: {str(e)}",
                )
            ]

    async def _get_team_velocity(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Execute team velocity analysis.

        Analyzes team velocity based on historical sprint data, providing
        insights into team productivity and capacity planning.

        Args:
            args: Dictionary containing project_key and optional analysis parameters

        Returns:
            list[TextContent]: Formatted team velocity analysis results

        Raises:
            Exception: If velocity analysis fails
        """
        project_key = args["project_key"]
        time_period = args.get("time_period", "last-6-months")
        issue_types_str = args.get(
            "issue_types", "Story,Task,Epic,Technical Debt,Improvement"
        )
        aggregation = args.get("aggregation", "monthly")
        team = args.get("team")

        # Parse comma-separated string into list
        issue_types = (
            [t.strip() for t in issue_types_str.split(",")]
            if issue_types_str
            else ["Story", "Task", "Epic", "Technical Debt", "Improvement"]
        )

        self.logger.info(
            f"Getting team velocity for {project_key}, period: {time_period}, team: {team}"
        )

        try:
            data = self.adapter.get_velocity_analysis(
                project_key=project_key,
                time_period=time_period,
                issue_types=issue_types,
                aggregation=aggregation,
                team=team,
            )

            formatted_result = {
                "project_key": project_key,
                "time_period": time_period,
                "issue_types": issue_types,
                "aggregation": aggregation,
                "team": team,
                "velocity_data": data,
                "summary": {
                    "analysis_type": "team_velocity",
                    "timestamp": (
                        data.get("timestamp") if isinstance(data, dict) else None
                    ),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Team Velocity for {project_key} (period: {time_period}, team: {team or 'all'}):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get team velocity: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve team velocity for {project_key}: {str(e)}",
                )
            ]

    async def _get_issue_adherence(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Execute issue adherence analysis.

        Analyzes how well the team adheres to due dates and delivery commitments,
        providing insights into planning accuracy and delivery reliability.

        Args:
            args: Dictionary containing project_key and optional analysis parameters

        Returns:
            list[TextContent]: Formatted adherence analysis results

        Raises:
            Exception: If adherence analysis fails
        """
        project_key = args["project_key"]
        time_period = args.get("time_period", "last-month")
        issue_types_str = args.get("issue_types", "Bug,Story")
        team = args.get("team")
        status_categories_str = args.get("status_categories")
        include_no_due_date = args.get("include_no_due_date", False)

        # Parse comma-separated strings into lists
        issue_types = (
            [t.strip() for t in issue_types_str.split(",")]
            if issue_types_str
            else ["Bug", "Story"]
        )
        status_categories = (
            [s.strip() for s in status_categories_str.split(",")]
            if status_categories_str
            else None
        )

        self.logger.info(
            f"Getting issue adherence analysis for {project_key}, period: {time_period}, team: {team}"
        )

        try:
            data = self.adapter.get_adherence_analysis(
                project_key=project_key,
                time_period=time_period,
                issue_types=issue_types,
                team=team,
            )

            formatted_result = {
                "project_key": project_key,
                "time_period": time_period,
                "issue_types": issue_types,
                "team": team,
                "status_categories": status_categories,
                "include_no_due_date": include_no_due_date,
                "adherence_analysis": data,
                "summary": {
                    "analysis_type": "issue_adherence",
                    "timestamp": (
                        data.get("timestamp") if isinstance(data, dict) else None
                    ),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Issue Adherence Analysis for {project_key} (period: {time_period}, team: {team or 'all'}):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get issue adherence: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve issue adherence analysis for {project_key}: {str(e)}",
                )
            ]

    async def _get_open_issues(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Execute open issues analysis.

        Retrieves and formats currently open issues in the project with optional filtering.

        Args:
            args: Dictionary containing project_key and optional filtering parameters

        Returns:
            list[TextContent]: Formatted open issues results

        Raises:
            Exception: If open issues retrieval fails
        """
        project_key = args["project_key"]
        issue_types_str = args.get("issue_types", "Bug,Support,Story,Task")
        team = args.get("team")
        status_categories_str = args.get("status_categories", "To Do,In Progress")
        priorities_str = args.get("priorities")

        # Parse comma-separated strings into lists
        issue_types = (
            [t.strip() for t in issue_types_str.split(",")]
            if issue_types_str
            else ["Bug", "Support", "Story", "Task"]
        )
        status_categories = (
            [s.strip() for s in status_categories_str.split(",")]
            if status_categories_str
            else ["To Do", "In Progress"]
        )
        priorities = (
            [p.strip() for p in priorities_str.split(",")] if priorities_str else None
        )

        self.logger.info(f"Getting open issues for {project_key}, team: {team}")

        try:
            data = self.adapter.get_open_issues(
                project_key=project_key,
                issue_types=issue_types,
                team=team,
                status_categories=status_categories,
                priorities=priorities,
            )

            formatted_result = {
                "project_key": project_key,
                "issue_types": issue_types,
                "team": team,
                "status_categories": status_categories,
                "priorities": priorities,
                "open_issues_data": data,
                "summary": {
                    "analysis_type": "open_issues",
                    "total_open_issues": (
                        len(data.get("issues", [])) if isinstance(data, dict) else "N/A"
                    ),
                    "timestamp": (
                        data.get("timestamp") if isinstance(data, dict) else None
                    ),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Open Issues for {project_key} (team: {team or 'all'}):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get open issues: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve open issues for {project_key}: {str(e)}",
                )
            ]
