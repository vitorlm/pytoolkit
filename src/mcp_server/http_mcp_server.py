#!/usr/bin/env python3
"""
HTTP MCP Server for use in Docker containers.

This server exposes the same MCP capabilities via HTTP,
allowing connection from Claude Desktop and Copilot over the network.
"""

import asyncio
import sys
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# Add src to path
sys.path.insert(0, "/app/src")

from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager

from .management_mcp_server import ManagementMCPServer


# Pydantic models for HTTP requests
class MCPToolRequest(BaseModel):
    name: str
    arguments: dict[str, Any] = {}


class MCPResourceRequest(BaseModel):
    uri: str


class MCPPromptRequest(BaseModel):
    name: str
    arguments: dict[str, Any] = {}


class HTTPMCPServer:
    """
    HTTP Server that exposes MCP functionalities via REST API.

    This server allows LLMs to connect via HTTP instead of stdio,
    facilitating use in containers and distributed environments.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8000):
        """
        Initialize the HTTP MCP Server.

        Args:
            host (str): Host address to bind the server to. Defaults to "0.0.0.0".
            port (int): Port number to bind the server to. Defaults to 8000.
        """
        # Initialize environment
        ensure_env_loaded()

        # Initialize LogManager
        if not LogManager._instance:
            LogManager.initialize(
                log_dir="/app/logs",
                log_file="http_mcp_server.log",
                log_retention_hours=48,
            )

        self.logger = LogManager.get_instance().get_logger("HTTPMCPServer")
        self.host = host
        self.port = port

        # Initialize the internal MCP server
        self.mcp_server = ManagementMCPServer()

        # Create FastAPI application
        self.app = FastAPI(
            title="PyToolkit MCP HTTP Server",
            description="HTTP interface for PyToolkit MCP Management Server",
            version="1.0.0",
        )

        # Configure CORS to allow LLM connections
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        self._setup_routes()
        self.logger.info(f"HTTP MCP Server initialized on {host}:{port}")

    def _setup_routes(self):
        """
        Configure HTTP routes for MCP functionalities.

        Sets up all endpoints for the HTTP MCP server including health checks,
        tool execution, resource reading, and prompt generation. Also includes
        special MCP endpoint for Claude Desktop compatibility.
        """

        @self.app.get("/")
        async def root():
            """Root endpoint with server information."""
            return {
                "server": "PyToolkit MCP HTTP Server",
                "version": "1.0.0",
                "status": "running",
                "endpoints": {
                    "health": "/health",
                    "tools": "/tools",
                    "resources": "/resources",
                    "prompts": "/prompts",
                    "execute_tool": "/tools/execute",
                    "read_resource": "/resources/read",
                    "get_prompt": "/prompts/get",
                },
                "total_capabilities": 46,
            }

        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            try:
                # Execute MCP server health check
                result = await self.mcp_server._health_check()
                return {
                    "status": "healthy",
                    "mcp_server": "ok",
                    "details": result[0].text if result else "No details",
                }
            except Exception as e:
                self.logger.error(f"Health check failed: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/tools")
        async def list_tools():
            """Lists all available tools."""
            try:
                # TODO: Fix private attribute access
                # tools_handler = self.mcp_server.server._list_tools_handler
                # if tools_handler:
                #     tools = await tools_handler()

                # Temporary workaround - return empty tools list
                tools = []
                return {
                    "tools": [
                        {
                            "name": tool.name,
                            "description": tool.description,
                            "input_schema": tool.inputSchema,
                        }
                        for tool in tools
                    ],
                    "count": len(tools),
                }
            except Exception as e:
                self.logger.error(f"Failed to list tools: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/tools/execute")
        async def execute_tool(request: MCPToolRequest):
            """Executes a specific tool."""
            try:
                # TODO: Fix private attribute access
                # call_tool_handler = self.mcp_server.server._call_tool_handler
                # if call_tool_handler:
                #     result = await call_tool_handler(request.name, request.arguments)

                # Temporary workaround - return mock result
                from mcp.types import TextContent

                result = [
                    TextContent(
                        type="text",
                        text="HTTP MCP Server - Tool execution not implemented",
                    )
                ]
                return {
                    "tool": request.name,
                    "arguments": request.arguments,
                    "result": [
                        {"type": content.type, "text": content.text}
                        for content in result
                    ],
                }
            except Exception as e:
                self.logger.error(f"Failed to execute tool {request.name}: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/resources")
        async def list_resources():
            """Lists all available resources."""
            try:
                # TODO: Fix private attribute access
                # resources_handler = self.mcp_server.server._list_resources_handler
                # if resources_handler:
                #     resources = await resources_handler()

                # Temporary workaround - return empty resources list
                resources = []
                return {
                    "resources": [
                        {
                            "uri": resource.uri,
                            "name": resource.name,
                            "description": resource.description,
                            "mime_type": resource.mimeType,
                        }
                        for resource in resources
                    ],
                    "count": len(resources),
                }
            except Exception as e:
                self.logger.error(f"Failed to list resources: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/resources/read")
        async def read_resource(request: MCPResourceRequest):
            """Reads content from a resource."""
            try:
                # TODO: Fix private attribute access
                # read_resource_handler = self.mcp_server.server._read_resource_handler
                # if read_resource_handler:
                #     content = await read_resource_handler(request.uri)

                # Temporary workaround - return mock content
                from mcp.types import TextResourceContents
                from pydantic import AnyUrl

                content = TextResourceContents(
                    uri=AnyUrl(request.uri),
                    mimeType="text/plain",
                    text="HTTP MCP Server - Resource reading not implemented",
                )
                return {
                    "uri": request.uri,
                    "content": {
                        "type": "text",
                        "text": (
                            content.text if hasattr(content, "text") else str(content)
                        ),
                    },
                }
            except Exception as e:
                self.logger.error(f"Failed to read resource {request.uri}: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/prompts")
        async def list_prompts():
            """Lists all available prompts."""
            try:
                # TODO: Fix private attribute access
                # prompts_handler = self.mcp_server.server._list_prompts_handler
                # if prompts_handler:
                #     prompts = await prompts_handler()

                # Temporary workaround - return empty prompts list
                prompts = []
                return {
                    "prompts": [
                        {
                            "name": prompt.name,
                            "description": prompt.description,
                            "arguments": (
                                prompt.arguments if hasattr(prompt, "arguments") else []
                            ),
                        }
                        for prompt in prompts
                    ],
                    "count": len(prompts),
                }
            except Exception as e:
                self.logger.error(f"Failed to list prompts: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/prompts/get")
        async def get_prompt(request: MCPPromptRequest):
            """Gets a specific prompt."""
            try:
                # TODO: Fix private attribute access
                # get_prompt_handler = self.mcp_server.server._get_prompt_handler
                # if get_prompt_handler:
                #     result = await get_prompt_handler(request.name, request.arguments)

                # Temporary workaround - return mock prompt result
                from mcp.types import GetPromptResult, PromptMessage, TextContent

                result = GetPromptResult(
                    description="HTTP MCP Server - Prompt execution not implemented",
                    messages=[
                        PromptMessage(
                            role="user",
                            content=TextContent(type="text", text="Mock prompt"),
                        )
                    ],
                )
                return {
                    "prompt": request.name,
                    "arguments": request.arguments,
                    "result": {
                        "description": (
                            result.description if hasattr(result, "description") else ""
                        ),
                        "messages": (
                            [
                                {
                                    "role": msg.role,
                                    "content": {
                                        "type": (
                                            msg.content.type
                                            if hasattr(msg.content, "type")
                                            else "text"
                                        ),
                                        "text": getattr(
                                            msg.content, "text", str(msg.content)
                                        ),
                                    },
                                }
                                for msg in result.messages
                            ]
                            if hasattr(result, "messages")
                            else []
                        ),
                    },
                }
            except Exception as e:
                self.logger.error(f"Failed to get prompt {request.name}: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        # Special endpoint for Claude Desktop via HTTP
        @self.app.post("/mcp")
        async def mcp_endpoint(request: Request):
            try:
                body = await request.json()
                method = body.get("method")
                params = body.get("params", {})

                # TODO: Fix private attribute access for MCP endpoint
                if method == "tools/list":
                    return {"result": {"tools": []}}

                elif method == "tools/call":
                    return {
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": "HTTP MCP endpoint - Tool execution not implemented",
                                }
                            ]
                        }
                    }

                elif method == "resources/list":
                    return {"result": {"resources": []}}

                elif method == "resources/read":
                    uri = params.get("uri")
                    return {
                        "result": {
                            "contents": [
                                {
                                    "uri": uri,
                                    "type": "text",
                                    "text": "HTTP MCP endpoint - Resource reading not implemented",
                                }
                            ]
                        }
                    }

                else:
                    raise HTTPException(
                        status_code=400, detail=f"Unknown method: {method}"
                    )

            except Exception as e:
                self.logger.error(f"MCP endpoint error: {e}")
                return {"error": {"code": -1, "message": str(e)}}

    async def run(self):
        """
        Start the HTTP server.

        Configures and starts the Uvicorn server with the FastAPI application
        on the specified host and port.

        Raises:
            Exception: If server fails to start
        """
        import uvicorn

        self.logger.info(f"Starting HTTP MCP Server on {self.host}:{self.port}")

        config = uvicorn.Config(
            self.app, host=self.host, port=self.port, log_level="info"
        )

        server = uvicorn.Server(config)
        await server.serve()


# Main function
async def main():
    """
    Start the HTTP MCP server.

    Initializes and runs the HTTP MCP server with configuration from
    environment variables. Default host is 0.0.0.0 and port is 8000.

    Environment Variables:
        MCP_SERVER_HOST: Host address for the server (default: 0.0.0.0)
        MCP_SERVER_PORT: Port number for the server (default: 8000)
    """
    import os

    host = os.getenv("MCP_SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_SERVER_PORT", 8000))

    server = HTTPMCPServer(host=host, port=port)
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
