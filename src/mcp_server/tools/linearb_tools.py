import json
from typing import Any

from mcp.types import TextContent, Tool

from ..adapters.linearb_adapter import LinearBAdapter
from utils.logging.logging_manager import LogManager


class LinearBTools:
    """MCP Tools for LinearB integration via PyToolkit."""

    def __init__(self):
        """
        Initialize LinearB Tools MCP handler.

        Sets up the LinearB adapter and logger for processing LinearB-related
        MCP tool requests including engineering metrics, team performance,
        and delivery analytics.
        """
        self.adapter = LinearBAdapter()
        self.logger = LogManager.get_instance().get_logger("LinearBTools")

    @staticmethod
    def get_tool_definitions() -> list[Tool]:
        """Returns definitions of all LinearB tools."""
        return [
            Tool(
                name="linearb_get_engineering_metrics",
                description="Gets engineering and team productivity metrics",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "time_range": {
                            "type": "string",
                            "description": "Analysis period",
                            "enum": [
                                "last-week",
                                "last-2-weeks",
                                "last-month",
                                "last-quarter",
                            ],
                            "default": "last-week",
                        },
                        "team_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Team IDs for analysis (optional)",
                        },
                    },
                    "required": [],
                },
            ),
            Tool(
                name="linearb_get_team_performance",
                description="Gets specific performance analysis by teams",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "team_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Team IDs for analysis",
                        }
                    },
                    "required": [],
                },
            ),
            Tool(
                name="linearb_get_pr_metrics",
                description="Gets Pull Request metrics and review process",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "time_range": {
                            "type": "string",
                            "description": "Analysis period",
                            "enum": ["last-week", "last-month"],
                            "default": "last-week",
                        }
                    },
                    "required": [],
                },
            ),
            Tool(
                name="linearb_get_deployment_metrics",
                description="Gets deployment metrics and delivery performance",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "time_range": {
                            "type": "string",
                            "description": "Analysis period",
                            "default": "last-month",
                        }
                    },
                    "required": [],
                },
            ),
        ]

    async def execute_tool(self, name: str, arguments: dict[str, Any]) -> list[TextContent]:
        """Executes specific LinearB tool."""
        self.logger.info(f"Executing LinearB tool: {name} with args: {arguments}")

        try:
            if name == "linearb_get_engineering_metrics":
                return await self._get_engineering_metrics(arguments)
            elif name == "linearb_get_team_performance":
                return await self._get_team_performance(arguments)
            elif name == "linearb_get_pr_metrics":
                return await self._get_pr_metrics(arguments)
            elif name == "linearb_get_deployment_metrics":
                return await self._get_deployment_metrics(arguments)
            else:
                error_msg = f"Unknown LinearB tool '{name}'"
                self.logger.error(error_msg)
                return [TextContent(type="text", text=f"Error: {error_msg}")]
        except Exception as e:
            error_msg = f"Error executing LinearB tool '{name}': {str(e)}"
            self.logger.error(error_msg)
            return [TextContent(type="text", text=error_msg)]

    async def _get_engineering_metrics(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Retrieve engineering productivity metrics.

        Gets comprehensive engineering metrics including cycle time, throughput,
        and team productivity indicators over a specified time range.

        Args:
            args: Dictionary containing optional time_range and team_ids

        Returns:
            list[TextContent]: Formatted engineering metrics results

        Raises:
            Exception: If engineering metrics retrieval fails
        """
        time_range = args.get("time_range", "last-week")
        team_ids = args.get("team_ids")
        self.logger.info(f"Getting engineering metrics for range: {time_range}, teams: {team_ids}")

        try:
            data = self.adapter.get_engineering_metrics(time_range, team_ids)

            formatted_result = {
                "time_range": time_range,
                "team_ids": team_ids,
                "engineering_metrics": data,
                "summary": {
                    "analysis_type": "engineering_metrics",
                    "teams_analyzed": len(team_ids) if team_ids else "all",
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Engineering Metrics ({time_range}):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get engineering metrics: {e}")
            return [
                TextContent(
                    type="text",
                    text=f"Failed to retrieve engineering metrics: {str(e)}",
                )
            ]

    async def _get_team_performance(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Retrieve team-specific performance analysis.

        Gets detailed performance analysis for specific teams including
        velocity, quality metrics, and efficiency indicators.

        Args:
            args: Dictionary containing optional team_ids

        Returns:
            list[TextContent]: Formatted team performance results

        Raises:
            Exception: If team performance retrieval fails
        """
        team_ids = args.get("team_ids")
        self.logger.info(f"Getting team performance for teams: {team_ids}")

        try:
            data = self.adapter.get_team_performance(team_ids)

            formatted_result = {
                "team_ids": team_ids,
                "team_performance": data,
                "summary": {
                    "analysis_type": "team_performance",
                    "teams_analyzed": len(team_ids) if team_ids else "all",
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Team Performance Analysis:\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get team performance: {e}")
            return [TextContent(type="text", text=f"Failed to retrieve team performance: {str(e)}")]

    async def _get_pr_metrics(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Retrieve Pull Request metrics and review process data.

        Gets PR-specific metrics including review time, pickup time,
        and code review efficiency over a specified time range.

        Args:
            args: Dictionary containing optional time_range

        Returns:
            list[TextContent]: Formatted PR metrics results

        Raises:
            Exception: If PR metrics retrieval fails
        """
        time_range = args.get("time_range", "last-week")
        self.logger.info(f"Getting PR metrics for range: {time_range}")

        try:
            # Reuse engineering metrics for PR analysis
            # The LinearB adapter can expand this in the future
            data = self.adapter.get_engineering_metrics(time_range)

            # Create specific PR analysis based on engineering data
            pr_analysis = {
                "time_range": time_range,
                "pr_data": data,
                "focus": "pull_request_metrics",
                "note": "PR metrics derived from engineering metrics data",
            }

            formatted_result = {
                "time_range": time_range,
                "pr_metrics": pr_analysis,
                "summary": {
                    "analysis_type": "pr_metrics",
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"PR Metrics ({time_range}):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get PR metrics: {e}")
            return [TextContent(type="text", text=f"Failed to retrieve PR metrics: {str(e)}")]

    async def _get_deployment_metrics(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Retrieve deployment metrics and delivery performance data.

        Gets deployment-specific metrics including frequency, success rates,
        and delivery pipeline performance over a specified time range.

        Args:
            args: Dictionary containing optional time_range

        Returns:
            list[TextContent]: Formatted deployment metrics results

        Raises:
            Exception: If deployment metrics retrieval fails
        """
        time_range = args.get("time_range", "last-month")
        self.logger.info(f"Getting deployment metrics for range: {time_range}")

        try:
            # Reuse engineering metrics for deployment analysis
            # The LinearB adapter can expand this in the future
            data = self.adapter.get_engineering_metrics(time_range)

            # Create specific deployment analysis based on engineering data
            deployment_analysis = {
                "time_range": time_range,
                "deployment_data": data,
                "focus": "deployment_performance",
                "note": "Deployment metrics derived from engineering metrics data",
            }

            formatted_result = {
                "time_range": time_range,
                "deployment_metrics": deployment_analysis,
                "summary": {
                    "analysis_type": "deployment_metrics",
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"Deployment Metrics ({time_range}):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to get deployment metrics: {e}")
            return [TextContent(type="text", text=f"Failed to retrieve deployment metrics: {str(e)}")]
