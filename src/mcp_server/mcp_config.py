import os
import tempfile
from pathlib import Path


class MCPConfig:
    """MCP Server specific configuration.

    Resolves relative path issues when executed by Claude Desktop.
    """

    @classmethod
    def get_project_root(cls):
        """Get the project root directory based on this file's location.

        Returns:
            Path: The project root directory path.
        """
        current_file = Path(__file__).resolve()
        # Navigate until finding the directory that contains pyproject.toml or requirements.txt
        for parent in current_file.parents:
            if (parent / "pyproject.toml").exists() or (parent / "requirements.txt").exists():
                return parent
        # Fallback to 2 levels above mcp_server
        return current_file.parent.parent.parent

    @classmethod
    def get_log_dir(cls):
        """Get the logs directory - uses temp if cannot create in project.

        Returns:
            str: The logs directory path.
        """
        project_root = cls.get_project_root()
        log_dir = project_root / "logs"

        try:
            log_dir.mkdir(exist_ok=True)
            # Test if we can write
            test_file = log_dir / "test_write.tmp"
            test_file.touch()
            test_file.unlink()
            return str(log_dir)
        except (OSError, PermissionError):
            # Use temporary directory as fallback
            temp_log_dir = Path(tempfile.gettempdir()) / "pytoolkit_mcp" / "logs"
            temp_log_dir.mkdir(parents=True, exist_ok=True)
            return str(temp_log_dir)

    @classmethod
    def get_cache_dir(cls):
        """Get the cache directory.

        Attempts to create a cache directory in the project root. Falls back to
        a temporary directory if project cache directory cannot be created.

        Returns:
            str: The cache directory path.

        Raises:
            OSError: If neither project nor temp cache directory can be created.
        """
        project_root = cls.get_project_root()
        cache_dir = project_root / "cache"

        try:
            cache_dir.mkdir(exist_ok=True)
            return str(cache_dir)
        except (OSError, PermissionError):
            temp_cache_dir = Path(tempfile.gettempdir()) / "pytoolkit_mcp" / "cache"
            temp_cache_dir.mkdir(parents=True, exist_ok=True)
            return str(temp_cache_dir)

    @classmethod
    def setup_environment(cls):
        """Configure environment variables for the MCP server.

        Sets up logging, caching, and Python path configuration required
        for the MCP server to operate correctly with PyToolkit integration.

        Returns:
            Path: The project root directory path.
        """
        project_root = cls.get_project_root()

        # Define environment variables
        os.environ.setdefault("LOG_DIR", cls.get_log_dir())
        os.environ.setdefault("CACHE_DIR", cls.get_cache_dir())
        os.environ.setdefault("LOG_FILE", "pytoolkit_mcp.log")
        os.environ.setdefault("LOG_LEVEL", "INFO")
        os.environ.setdefault("LOG_OUTPUT", "file")  # File only for MCP
        os.environ.setdefault("LOG_RETENTION_HOURS", "24")
        os.environ.setdefault("CACHE_BACKEND", "file")
        os.environ.setdefault("CACHE_EXPIRATION_MINUTES", "60")
        os.environ.setdefault("USE_FILTER", "false")

        # Add src to PYTHONPATH if not already there
        src_path = str(project_root / "src")
        python_path = os.environ.get("PYTHONPATH", "")
        if src_path not in python_path:
            if python_path:
                os.environ["PYTHONPATH"] = f"{python_path}:{src_path}"
            else:
                os.environ["PYTHONPATH"] = src_path

        return project_root
