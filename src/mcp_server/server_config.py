from typing import Any


class MCPServerConfig:
    """Centralized configuration for MCP Management Server."""

    # Server Identity
    SERVER_NAME = "management-server"
    SERVER_VERSION = "1.0.0"

    # Capabilities
    CAPABILITIES = {"tools": True, "resources": True, "prompts": True, "logging": True}

    # Transport Configuration
    TRANSPORTS = [
        {"type": "stdio"},  # For Claude Desktop (local only)
    ]

    # PyToolkit Integration
    REUSE_PYTOOLKIT_LOGGING = True
    REUSE_PYTOOLKIT_CACHE = True
    REUSE_PYTOOLKIT_ENV = True

    @classmethod
    def get_config(cls) -> dict[str, Any]:
        """Get complete server configuration.

        Returns:
            dict[str, Any]: Complete server configuration including server identity,
                          capabilities, transport options, and PyToolkit integration settings.
        """
        return {
            "server": {"name": cls.SERVER_NAME, "version": cls.SERVER_VERSION},
            "capabilities": cls.CAPABILITIES,
            "transports": cls.TRANSPORTS,
            "pytoolkit_integration": {
                "logging": cls.REUSE_PYTOOLKIT_LOGGING,
                "cache": cls.REUSE_PYTOOLKIT_CACHE,
                "env": cls.REUSE_PYTOOLKIT_ENV,
            },
        }
