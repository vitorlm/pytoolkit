import json
from typing import Any

from mcp.types import TextContent, Tool

from utils.logging.logging_manager import LogManager

from ..adapters.circleci_adapter import CircleCIAdapter


class CircleCITools:
    """MCP Tools for CircleCI integration via PyToolkit."""

    def __init__(self):
        """Initialize CircleCI Tools MCP handler.

        Sets up the CircleCI adapter and logger for processing CircleCI-related
        MCP tool requests including pipeline status, build metrics, and deployment analysis.
        """
        self.adapter = CircleCIAdapter()
        self.logger = LogManager.get_instance().get_logger("CircleCITools")

    @staticmethod
    def get_tool_definitions() -> list[Tool]:
        """Returns definitions of all CircleCI tools."""
        return [
            Tool(
                name="circleci_get_pipeline_status",
                description="Gets status of the most recent pipelines of a project",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_slug": {
                            "type": "string",
                            "description": "CircleCI project slug (format: gh/org/repo)",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of pipelines to fetch (default: 10)",
                            "default": 10,
                        },
                    },
                    "required": ["project_slug"],
                },
            ),
            Tool(
                name="circleci_get_build_metrics",
                description="Gets build and deployment performance metrics",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_slug": {
                            "type": "string",
                            "description": "CircleCI project slug",
                        },
                        "days": {
                            "type": "integer",
                            "description": "Analysis period in days (default: 30)",
                            "default": 30,
                        },
                    },
                    "required": ["project_slug"],
                },
            ),
            Tool(
                name="circleci_analyze_deployment_frequency",
                description="Analyzes deployment frequency and success rates",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "project_slug": {
                            "type": "string",
                            "description": "CircleCI project slug",
                        },
                        "days": {
                            "type": "integer",
                            "description": "Analysis period (default: 30)",
                            "default": 30,
                        },
                    },
                    "required": ["project_slug"],
                },
            ),
        ]

    async def execute_tool(self, name: str, arguments: dict[str, Any]) -> list[TextContent]:
        """Executes specific CircleCI tool."""
        self.logger.info(f"Executing CircleCI tool: {name} with args: {arguments}")

        try:
            if name == "circleci_get_pipeline_status":
                return await self._get_pipeline_status(arguments)
            elif name == "circleci_get_build_metrics":
                return await self._get_build_metrics(arguments)
            elif name == "circleci_analyze_deployment_frequency":
                return await self._analyze_deployment_frequency(arguments)
            else:
                error_msg = f"Unknown CircleCI tool '{name}'"
                self.logger.error(error_msg)
                return [TextContent(type="text", text=f"Error: {error_msg}")]
        except Exception as e:
            error_msg = f"Error executing CircleCI tool '{name}': {e!s}"
            self.logger.error(error_msg)
            return [TextContent(type="text", text=error_msg)]

    async def _get_pipeline_status(self, args: dict[str, Any]) -> list[TextContent]:
        """Retrieve pipeline status information.

        Gets the status of the most recent pipelines for a specified project,
        including success rates and execution details.

        Args:
            args: Dictionary containing project_slug and optional limit

        Returns:
            list[TextContent]: Formatted pipeline status results

        Raises:
            Exception: If pipeline status retrieval fails
        """
        project_slug = args["project_slug"]
        limit = args.get("limit", 10)
        self.logger.info(f"Getting pipeline status for {project_slug}, limit: {limit}")

        try:
            data = self.adapter.get_pipeline_status(project_slug, limit)

            formatted_result = {
                "project_slug": project_slug,
                "limit": limit,
                "pipeline_data": data,
                "summary": {
                    "analysis_type": "pipeline_status",
                    "pipelines_analyzed": limit,
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Pipeline Status for {project_slug} (last {limit}):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get pipeline status: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve pipeline status for {project_slug}: {e!s}",
                )
            ]

    async def _get_build_metrics(self, args: dict[str, Any]) -> list[TextContent]:
        """Retrieve build performance metrics.

        Gets comprehensive build and deployment performance metrics for a project
        over a specified time period, including success rates and timing data.

        Args:
            args: Dictionary containing project_slug and optional days parameter

        Returns:
            list[TextContent]: Formatted build metrics results

        Raises:
            Exception: If build metrics retrieval fails
        """
        project_slug = args["project_slug"]
        days = args.get("days", 30)
        self.logger.info(f"Getting build metrics for {project_slug}, last {days} days")

        try:
            data = self.adapter.get_build_metrics(project_slug, days)

            formatted_result = {
                "project_slug": project_slug,
                "analysis_period_days": days,
                "build_metrics": data,
                "summary": {
                    "analysis_type": "build_metrics",
                    "period_days": days,
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Build Metrics for {project_slug} (last {days} days):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get build metrics: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve build metrics for {project_slug}: {e!s}",
                )
            ]

    async def _analyze_deployment_frequency(self, args: dict[str, Any]) -> list[TextContent]:
        """Analyze deployment frequency and success rates.

        Analyzes deployment patterns, frequency, and success rates over a specified
        time period to provide insights into delivery performance.

        Args:
            args: Dictionary containing project_slug and optional days parameter

        Returns:
            list[TextContent]: Formatted deployment frequency analysis results

        Raises:
            Exception: If deployment frequency analysis fails
        """
        project_slug = args["project_slug"]
        days = args.get("days", 30)
        self.logger.info(f"Analyzing deployment frequency for {project_slug}, last {days} days")

        try:
            # Reuse build metrics for deployment analysis
            # The CircleCI adapter can expand this in the future
            build_data = self.adapter.get_build_metrics(project_slug, days)

            # Create specific deployment analysis based on build data
            deployment_analysis = {
                "project_slug": project_slug,
                "analysis_period_days": days,
                "deployment_data": build_data,
                "frequency_analysis": {
                    "note": "Deployment frequency analysis based on build metrics",
                    "period": f"last {days} days",
                    "source": "build_metrics",
                },
            }

            formatted_result = {
                "project_slug": project_slug,
                "deployment_frequency": deployment_analysis,
                "summary": {
                    "analysis_type": "deployment_frequency",
                    "period_days": days,
                    "timestamp": (build_data.get("timestamp") if isinstance(build_data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Deployment Frequency Analysis for {project_slug}:\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to analyze deployment frequency: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to analyze deployment frequency for {project_slug}: {e!s}",
                )
            ]
