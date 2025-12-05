"""MCP Management Server - PyToolkit Integration.

This module implements a Model Context Protocol (MCP) Server that
reuses all existing PyToolkit infrastructure.
"""

from .server_config import MCPServerConfig

# Full MCP server will be available when MCP library is properly installed
FULL_MCP_AVAILABLE = False

__version__ = "1.0.0"
__author__ = "PyToolkit MCP Team"

__all__ = [
    "FULL_MCP_AVAILABLE",
    "MCPServerConfig",
    "get_management_server",
]


# Import servers only when needed to avoid dependency issues
def get_management_server():
    """Get the management MCP server instance (STDIO transport only)."""
    from .management_mcp_server import ManagementMCPServer

    return ManagementMCPServer
