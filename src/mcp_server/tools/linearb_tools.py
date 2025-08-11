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
                        "filter_type": {
                            "type": "string",
                            "description": "Filter type for grouping data",
                            "enum": [
                                "organization",
                                "contributor",
                                "team",
                                "repository",
                                "label",
                                "custom_metric",
                            ],
                            "default": "team",
                        },
                        "granularity": {
                            "type": "string",
                            "description": "Data granularity",
                            "enum": ["1d", "1w", "1mo", "custom"],
                            "default": "custom",
                        },
                        "aggregation": {
                            "type": "string",
                            "description": "Aggregation type for time-based metrics",
                            "enum": ["p75", "avg", "p50", "raw", "default"],
                            "default": "default",
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
                        },
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
                        "filter_type": {
                            "type": "string",
                            "description": "Filter type for grouping data",
                            "enum": [
                                "organization",
                                "contributor",
                                "team",
                                "repository",
                                "label",
                                "custom_metric",
                            ],
                            "default": "team",
                        },
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
                        },
                        "team_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Team IDs for analysis (optional)",
                        },
                        "filter_type": {
                            "type": "string",
                            "description": "Filter type for grouping data",
                            "enum": [
                                "organization",
                                "contributor",
                                "team",
                                "repository",
                                "label",
                                "custom_metric",
                            ],
                            "default": "team",
                        },
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
                        },
                        "team_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Team IDs for analysis (optional)",
                        },
                        "filter_type": {
                            "type": "string",
                            "description": "Filter type for grouping data",
                            "enum": [
                                "organization",
                                "contributor",
                                "team",
                                "repository",
                                "label",
                                "custom_metric",
                            ],
                            "default": "team",
                        },
                        "granularity": {
                            "type": "string",
                            "description": "Data granularity",
                            "enum": ["1d", "1w", "1mo", "custom"],
                            "default": "custom",
                        },
                        "aggregation": {
                            "type": "string",
                            "description": "Aggregation type for time-based metrics",
                            "enum": ["p75", "avg", "p50", "raw", "default"],
                            "default": "default",
                        },
                    },
                    "required": [],
                },
            ),
            Tool(
                name="linearb_export_report",
                description="Export comprehensive performance reports from LinearB",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "team_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Team IDs for analysis (optional)",
                        },
                        "time_range": {
                            "type": "string",
                            "description": "Time range for the report (last-week, last-2-weeks, last-month, N-days, or YYYY-MM-DD,YYYY-MM-DD)",
                        },
                        "filter_type": {
                            "type": "string",
                            "description": "Filter type for grouping data",
                            "enum": [
                                "organization",
                                "contributor",
                                "team",
                                "repository",
                                "label",
                                "custom_metric",
                            ],
                            "default": "team",
                        },
                        "granularity": {
                            "type": "string",
                            "description": "Data granularity",
                            "enum": ["1d", "1w", "1mo", "custom"],
                            "default": "custom",
                        },
                        "aggregation": {
                            "type": "string",
                            "description": "Aggregation type for time-based metrics",
                            "enum": ["p75", "avg", "p50", "raw", "default"],
                            "default": "default",
                        },
                        "format": {
                            "type": "string",
                            "description": "Export format",
                            "enum": ["csv", "json"],
                            "default": "csv",
                        },
                        "beautified": {
                            "type": "boolean",
                            "description": "Format data for better readability",
                            "default": False,
                        },
                        "return_no_data": {
                            "type": "boolean",
                            "description": "Include teams/contributors with no data",
                            "default": False,
                        },
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
            elif name == "linearb_export_report":
                return await self._export_report(arguments)
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
        and team productivity indicators over a specified time range with filtering.

        Args:
            args: Dictionary containing optional time_range, team_ids, filter_type,
                  granularity, and aggregation parameters

        Returns:
            list[TextContent]: Formatted engineering metrics results

        Raises:
            Exception: If engineering metrics retrieval fails
        """
        time_range = args.get("time_range", "last-week")
        team_ids = args.get("team_ids")
        filter_type = args.get("filter_type", "team")
        granularity = args.get("granularity", "custom")
        aggregation = args.get("aggregation", "default")

        self.logger.info(
            f"Getting engineering metrics - range: {time_range}, teams: {team_ids}, "
            f"filter: {filter_type}, granularity: {granularity}, aggregation: {aggregation}"
        )

        try:
            data = self.adapter.get_engineering_metrics(time_range, team_ids)

            formatted_result = {
                "time_range": time_range,
                "team_ids": team_ids,
                "filter_type": filter_type,
                "granularity": granularity,
                "aggregation": aggregation,
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
        velocity, quality metrics, and efficiency indicators with filtering options.

        Args:
            args: Dictionary containing optional team_ids, time_range, and filter_type

        Returns:
            list[TextContent]: Formatted team performance results

        Raises:
            Exception: If team performance retrieval fails
        """
        team_ids = args.get("team_ids")
        time_range = args.get("time_range", "last-week")
        filter_type = args.get("filter_type", "team")

        self.logger.info(
            f"Getting team performance - teams: {team_ids}, time_range: {time_range}, filter: {filter_type}"
        )

        try:
            data = self.adapter.get_team_performance(team_ids)

            formatted_result = {
                "team_ids": team_ids,
                "time_range": time_range,
                "filter_type": filter_type,
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
        and code review efficiency over a specified time range with filtering.

        Args:
            args: Dictionary containing optional time_range, team_ids, and filter_type

        Returns:
            list[TextContent]: Formatted PR metrics results

        Raises:
            Exception: If PR metrics retrieval fails
        """
        time_range = args.get("time_range", "last-week")
        team_ids = args.get("team_ids")
        filter_type = args.get("filter_type", "team")

        self.logger.info(f"Getting PR metrics - range: {time_range}, teams: {team_ids}, filter: {filter_type}")

        try:
            # Reuse engineering metrics for PR analysis
            # The LinearB adapter can expand this in the future
            data = self.adapter.get_engineering_metrics(time_range, team_ids)

            # Create specific PR analysis based on engineering data
            pr_analysis = {
                "time_range": time_range,
                "team_ids": team_ids,
                "filter_type": filter_type,
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
        and delivery pipeline performance over a specified time range with filtering.

        Args:
            args: Dictionary containing optional time_range, team_ids, filter_type,
                  granularity, and aggregation parameters

        Returns:
            list[TextContent]: Formatted deployment metrics results

        Raises:
            Exception: If deployment metrics retrieval fails
        """
        time_range = args.get("time_range", "last-month")
        team_ids = args.get("team_ids")
        filter_type = args.get("filter_type", "team")
        granularity = args.get("granularity", "custom")
        aggregation = args.get("aggregation", "default")

        self.logger.info(
            f"Getting deployment metrics - range: {time_range}, teams: {team_ids}, "
            f"filter: {filter_type}, granularity: {granularity}, aggregation: {aggregation}"
        )

        try:
            # Reuse engineering metrics for deployment analysis
            # The LinearB adapter can expand this in the future
            data = self.adapter.get_engineering_metrics(time_range, team_ids)

            # Create specific deployment analysis based on engineering data
            deployment_analysis = {
                "time_range": time_range,
                "team_ids": team_ids,
                "filter_type": filter_type,
                "granularity": granularity,
                "aggregation": aggregation,
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

    async def _export_report(self, args: dict[str, Any]) -> list[TextContent]:
        """
        Export comprehensive performance reports from LinearB.

        Exports comprehensive performance reports including review time,
        deploy metrics, PR analytics, and team productivity data.

        Args:
            args: Dictionary containing optional team_ids, time_range, filter_type,
                  granularity, aggregation, format, beautified, return_no_data parameters

        Returns:
            list[TextContent]: Formatted export report results

        Raises:
            Exception: If export report fails
        """
        team_ids = args.get("team_ids")
        time_range = args.get("time_range", "last-week")
        filter_type = args.get("filter_type", "team")
        granularity = args.get("granularity", "custom")
        aggregation = args.get("aggregation", "default")
        format_type = args.get("format", "csv")
        beautified = args.get("beautified", False)
        return_no_data = args.get("return_no_data", False)

        self.logger.info(
            f"Exporting LinearB report - teams: {team_ids}, time_range: {time_range}, "
            f"filter: {filter_type}, granularity: {granularity}, format: {format_type}"
        )

        try:
            # Note: This would typically call the export report functionality
            # For now, we'll return a consolidated engineering metrics report
            data = self.adapter.get_engineering_metrics(time_range, team_ids)

            formatted_result = {
                "export_parameters": {
                    "team_ids": team_ids,
                    "time_range": time_range,
                    "filter_type": filter_type,
                    "granularity": granularity,
                    "aggregation": aggregation,
                    "format": format_type,
                    "beautified": beautified,
                    "return_no_data": return_no_data,
                },
                "report_data": data,
                "summary": {
                    "analysis_type": "export_report",
                    "timestamp": (data.get("timestamp") if isinstance(data, dict) else None),
                },
            }

            return [
                TextContent(
                    type="text",
                    text=f"LinearB Export Report ({time_range}):\n{json.dumps(formatted_result, indent=2)}",
                )
            ]
        except Exception as e:
            self.logger.error(f"Failed to export LinearB report: {e}")
            return [TextContent(type="text", text=f"Failed to export LinearB report: {str(e)}")]
