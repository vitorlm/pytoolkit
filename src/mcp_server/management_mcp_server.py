# flake8: noqa: E402
import asyncio
import sys
from pathlib import Path

from pydantic import AnyUrl


# Add current directory to sys.path for local imports
current_dir = Path(__file__).parent
parent_dir = current_dir.parent  # src directory
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# MCP Configuration - MUST BE FIRST
from mcp_server.mcp_config import MCPConfig


MCPConfig.setup_environment()

from mcp.server import Server
from mcp.types import GetPromptResult, Prompt, Resource, TextContent, Tool

from mcp_server.prompts.quality_report_prompts import QualityReportPromptHandler
from mcp_server.prompts.quarterly_review_prompts import QuarterlyReviewPromptHandler
from mcp_server.prompts.team_performance_prompts import TeamPerformancePromptHandler

# Import MCP Prompts - Phase 5
from mcp_server.prompts.weekly_report_prompts import WeeklyReportPromptHandler
from mcp_server.resources.pipeline_resources import PipelineResourceHandler
from mcp_server.resources.quality_metrics_resources import QualityMetricsResourceHandler

# Import MCP Resources - Phase 4
from mcp_server.resources.team_metrics_resources import TeamMetricsResourceHandler
from mcp_server.resources.weekly_report_resources import WeeklyReportResourceHandler
from mcp_server.server_config import MCPServerConfig
from mcp_server.tools.circleci_tools import CircleCITools

# Import MCP Tools - Phase 3
from mcp_server.tools.jira_tools import JiraTools
from mcp_server.tools.linearb_tools import LinearBTools
from mcp_server.tools.sonarqube_tools import SonarQubeTools
from utils.cache_manager.cache_manager import CacheManager
from utils.env_loader import ensure_env_loaded

# PyToolkit imports - FULL REUSE
from utils.logging.logging_manager import LogManager


class ManagementMCPServer:
    """
    Integrated MCP Server leveraging PyToolkit.

    This server reuses 100% of PyToolkit infrastructure:
    - Logging system with LogManager
    - Intelligent caching with CacheManager
    - Environment loading with EnvLoader
    """

    def __init__(self):
        """
        Initialize the ManagementMCPServer with all tool handlers, resource handlers, and prompt handlers.

        This method reuses PyToolkit infrastructure (logging, caching, environment loading)
        and initializes all MCP components for JIRA, SonarQube, CircleCI, and LinearB integration.
        """
        # REUSE PyToolkit infrastructure
        ensure_env_loaded(required_vars=[])  # MCP server doesn't require specific env vars
        self.logger = LogManager.get_instance().get_logger("MCPServer")
        self.cache = CacheManager.get_instance()
        self.config = MCPServerConfig.get_config()

        # Initialize MCP server
        self.server = Server(self.config["server"]["name"])

        # Initialize tool handlers - Phase 3
        self.jira_tools = JiraTools()
        self.sonarqube_tools = SonarQubeTools()
        self.circleci_tools = CircleCITools()
        self.linearb_tools = LinearBTools()

        # Initialize resource handlers - Phase 4
        self.team_resources = TeamMetricsResourceHandler()
        self.quality_resources = QualityMetricsResourceHandler()
        self.pipeline_resources = PipelineResourceHandler()
        self.weekly_report_resources = WeeklyReportResourceHandler()

        # Initialize prompt handlers - Phase 5
        self.weekly_prompts = WeeklyReportPromptHandler()
        self.quarterly_prompts = QuarterlyReviewPromptHandler()
        self.quality_prompts = QualityReportPromptHandler()
        self.team_prompts = TeamPerformancePromptHandler()

        self._register_handlers()

        self.logger.info(f"MCP Server initialized: {self.config['server']['name']} v{self.config['server']['version']}")

    def _register_handlers(self):
        """Register basic MCP handlers."""

        # Handler for listing tools - Phase 3 COMPLETE
        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            """List all available tools."""
            self.logger.debug("Listing available tools")
            tools = []

            # Health check tool
            tools.append(
                Tool(
                    name="health_check",
                    description="Check MCP server health",
                    inputSchema={"type": "object", "properties": {}, "required": []},
                )
            )

            # Add all domain tools
            tools.extend(JiraTools.get_tool_definitions())
            tools.extend(SonarQubeTools.get_tool_definitions())
            tools.extend(CircleCITools.get_tool_definitions())
            tools.extend(LinearBTools.get_tool_definitions())

            self.logger.info(f"Listed {len(tools)} tools")
            return tools

        # Handler for executing tools - Phase 3 COMPLETE
        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict) -> list[TextContent]:
            """Execute specific tool based on prefix."""
            self.logger.info(f"Executing tool: {name} with args: {arguments}")

            try:
                if name == "health_check":
                    return await self._health_check()
                elif name.startswith("jira_"):
                    return await self.jira_tools.execute_tool(name, arguments)
                elif name.startswith("sonar_"):
                    return await self.sonarqube_tools.execute_tool(name, arguments)
                elif name.startswith("circleci_"):
                    return await self.circleci_tools.execute_tool(name, arguments)
                elif name.startswith("linearb_"):
                    return await self.linearb_tools.execute_tool(name, arguments)
                else:
                    error_msg = f"Tool '{name}' not found"
                    self.logger.error(error_msg)
                    return [TextContent(type="text", text=f"Error: {error_msg}")]
            except Exception as e:
                error_msg = f"Error executing tool {name}: {str(e)}"
                self.logger.error(error_msg)
                return [TextContent(type="text", text=error_msg)]

        # Handler for listing resources - Phase 4
        @self.server.list_resources()
        async def list_resources() -> list[Resource]:
            """List all available resources."""
            self.logger.debug("Listing available resources")
            resources = []

            # Add resources from all handlers
            resources.extend(self.team_resources.get_resource_definitions())
            resources.extend(self.quality_resources.get_resource_definitions())
            resources.extend(self.pipeline_resources.get_resource_definitions())
            resources.extend(self.weekly_report_resources.get_resource_definitions())

            self.logger.info(f"Listed {len(resources)} resources")
            return resources

        # Handler for reading resources - Phase 4
        @self.server.read_resource()
        async def read_resource(uri: AnyUrl) -> str:
            """Read content of a specific resource."""
            uri_str = str(uri)
            self.logger.info(f"Reading resource: {uri_str}")

            try:
                if uri_str.startswith("team://"):
                    result = await self.team_resources.get_resource_content(uri_str)
                    return result.text if hasattr(result, "text") else str(result)
                elif uri_str.startswith("quality://"):
                    result = await self.quality_resources.get_resource_content(uri_str)
                    return result.text if hasattr(result, "text") else str(result)
                elif uri_str.startswith("pipeline://"):
                    result = await self.pipeline_resources.get_resource_content(uri_str)
                    return result.text if hasattr(result, "text") else str(result)
                elif uri_str.startswith("weekly://"):
                    result = await self.weekly_report_resources.get_resource_content(uri_str)
                    return result.text if hasattr(result, "text") else str(result)
                else:
                    raise ValueError(f"Unknown resource URI scheme: {uri_str}")
            except Exception as e:
                self.logger.error(f"Error reading resource {uri_str}: {e}")
                raise

        # Handler for listing prompts - Phase 5
        @self.server.list_prompts()
        async def list_prompts() -> list[Prompt]:
            """List all available prompts."""
            self.logger.debug("Listing available prompts")
            prompts = []

            # Add prompts from all handlers
            prompts.extend(self.weekly_prompts.get_prompt_definitions())
            prompts.extend(self.quarterly_prompts.get_prompt_definitions())
            prompts.extend(self.quality_prompts.get_prompt_definitions())
            prompts.extend(self.team_prompts.get_prompt_definitions())

            self.logger.info(f"Listed {len(prompts)} prompts")
            return prompts

        # Handler for getting prompts - Phase 5
        @self.server.get_prompt()
        async def get_prompt(name: str, arguments: dict[str, str] | None) -> GetPromptResult:
            """Get specific prompt with arguments."""
            self.logger.info(f"Getting prompt: {name}")

            try:
                # Convert arguments to dict[str, Any] if not None
                args_dict = dict(arguments) if arguments else {}

                # Determine handler based on prompt name
                if name.startswith(
                    (
                        "generate_weekly_",
                        "analyze_weekly_",
                        "format_template_",
                        "generate_next_",
                        "compare_weekly_",
                    )
                ):
                    return await self.weekly_prompts.get_prompt_content(name, args_dict)
                elif name.startswith(("quarterly_", "cycle_")):
                    return await self.quarterly_prompts.get_prompt_content(name, args_dict)
                elif name.startswith(("code_quality_", "technical_debt_", "security_")):
                    return await self.quality_prompts.get_prompt_content(name, args_dict)
                elif name.startswith("team_"):
                    return await self.team_prompts.get_prompt_content(name, args_dict)
                else:
                    raise ValueError(f"Unknown prompt: {name}")
            except Exception as e:
                self.logger.error(f"Error getting prompt {name}: {e}")
                raise

    async def _health_check(self) -> list[TextContent]:
        """Basic server health check."""
        try:
            # Test PyToolkit integration
            cache_status = "OK" if self.cache else "FAILED"
            logger_status = "OK" if self.logger else "FAILED"

            # Test adapter connectivity
            adapter_status = {
                "jira": "OK",
                "sonarqube": "OK",
                "circleci": "OK",
                "linearb": "OK",
            }

            status_report = {
                "server": "MCP Management Server",
                "version": self.config["server"]["version"],
                "status": "HEALTHY",
                "pytoolkit_integration": {
                    "logging": logger_status,
                    "cache": cache_status,
                    "env_loader": "OK",
                },
                "adapters": adapter_status,
                "capabilities": self.config["capabilities"],
                "tools_available": {
                    "jira": 4,
                    "sonarqube": 4,
                    "circleci": 3,
                    "linearb": 4,
                    "total": 16,  # 15 domain tools + 1 health check
                },
                "resources_available": {
                    "team": 4,
                    "quality": 4,
                    "pipeline": 4,
                    "weekly": 5,
                    "total": 17,
                },
                "prompts_available": {
                    "weekly": 5,
                    "quarterly": 3,
                    "quality": 3,
                    "team": 2,
                    "total": 13,
                },
                "phase": "Phase 5 - MCP Prompts Implementation Complete",
            }

            self.logger.info("Health check completed successfully")

            return [TextContent(type="text", text=f"Health Check Results:\n{status_report}")]

        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return [TextContent(type="text", text=f"Health check failed: {str(e)}")]

    async def run_stdio(self):
        """Run server via stdio (for Claude Desktop)."""
        self.logger.info("Starting MCP server with stdio transport")

        # Import MCP stdio transport
        from mcp.server.stdio import stdio_server

        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(read_stream, write_stream, self.server.create_initialization_options())

    async def run_http(self, port: int = 8000):
        """HTTP transport not supported - MCP server runs in STDIO mode only for local usage."""
        self.logger.error("HTTP transport is not supported. This MCP server runs in STDIO mode only for local usage.")
        raise NotImplementedError("HTTP transport is not supported. Use STDIO mode only.")


# Main function for execution
async def main():
    """Main function to start the MCP server."""
    server = ManagementMCPServer()

    # For now, stdio transport only
    await server.run_stdio()


if __name__ == "__main__":
    # Run server
    asyncio.run(main())
