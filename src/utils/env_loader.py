"""Environment loading utility for the entire project.
Ensures environment variables are loaded from .env files.
"""

import os
from pathlib import Path


def load_env_file(env_file_path: str | None = None) -> None:
    """Load environment variables from .env file.

    Args:
        env_file_path: Path to the .env file. If None, searches common locations.
    """
    if env_file_path is None:
        # Search for .env files in common locations
        env_file_path = find_env_file()
        if not env_file_path:
            print("Warning: No .env file found in common locations")
            return

    env_file = Path(env_file_path)

    if not env_file.exists():
        print(f"Warning: Environment file not found at {env_file}")
        return

    try:
        with open(env_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith("#") or line.startswith("//"):
                    continue

                # Parse key=value pairs
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]

                    # Only set if not already in environment
                    if key not in os.environ:
                        os.environ[key] = value

        print(f"Loaded environment variables from {env_file}")

    except Exception as e:
        print(f"Error loading environment file {env_file}: {e}")


def find_env_file() -> str | None:
    """Find .env file in common locations.

    Returns:
        Path to the first .env file found, or None if not found.
    """
    # Get the project root (where src/ is located)
    current_file = Path(__file__)
    project_root = current_file.parent.parent.parent  # utils -> src -> project_root

    # Common locations to search for .env files
    search_paths = [
        # Project root
        project_root / ".env",
        # Domain-specific locations
        project_root / "src" / "domains" / "syngenta" / "jira" / ".env",
        project_root / "src" / "domains" / "linearb" / ".env",
        project_root / "src" / "domains" / "syngenta" / "datadog" / ".env",
        # Current working directory
        Path.cwd() / ".env",
        # User home directory
        Path.home() / ".env",
    ]

    for env_path in search_paths:
        if env_path.exists() and env_path.is_file():
            return str(env_path)

    return None


def load_domain_env(domain_path: str) -> None:
    """Load environment variables from a specific domain's .env file.

    Args:
        domain_path: Path to the domain directory (e.g., "domains/syngenta/jira")
    """
    current_file = Path(__file__)
    project_root = current_file.parent.parent.parent
    env_file = project_root / "src" / domain_path / ".env"

    if env_file.exists():
        load_env_file(str(env_file))
    else:
        print(f"Warning: Domain .env file not found at {env_file}")


def ensure_env_loaded(required_vars: list[str] | None = None) -> None:
    """Ensure environment variables are loaded.
    Call this at the beginning of commands that need environment variables.

    Args:
        required_vars: List of required environment variable names to check for.
                      If None, uses default Slack variables.
    """
    if required_vars is None:
        required_vars = ["SLACK_WEBHOOK_URL", "SLACK_BOT_TOKEN"]

    # Check if any required environment variables are missing
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        load_env_file()
        # Check again after loading
        still_missing = [var for var in required_vars if not os.getenv(var)]
        if still_missing:
            print(f"Warning: Required environment variables still missing: {still_missing}")


def ensure_jira_env_loaded() -> None:
    """Ensure JIRA-specific environment variables are loaded."""
    ensure_env_loaded(["JIRA_URL", "JIRA_USER_EMAIL", "JIRA_API_TOKEN"])


def ensure_slack_env_loaded() -> None:
    """Ensure Slack-specific environment variables are loaded."""
    ensure_env_loaded(["SLACK_WEBHOOK_URL", "SLACK_BOT_TOKEN", "SLACK_CHANNEL_ID"])


def ensure_linearb_env_loaded() -> None:
    """Ensure LinearB-specific environment variables are loaded."""
    # First load the general environment
    load_env_file()

    # Then load the domain-specific environment
    load_domain_env("domains/linearb")

    # Check if the required variables are available
    required_vars = ["LINEARB_API_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"Warning: Required LinearB environment variables missing: {missing_vars}")


def ensure_circleci_env_loaded() -> None:
    """Ensure CircleCI-specific environment variables are loaded."""
    # First load the general environment
    load_env_file()

    # Then load the domain-specific environment
    load_domain_env("domains/circleci")

    # Check if the required variables are available
    required_vars = ["CIRCLECI_TOKEN"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"Warning: Required CircleCI environment variables missing: {missing_vars}")


def ensure_sonarqube_env_loaded() -> None:
    """Ensure SonarQube-specific environment variables are loaded."""
    # First load the general environment
    load_env_file()

    # Then load the domain-specific environment
    load_domain_env("domains/syngenta/sonarqube")

    # Check if the required variables are available
    required_vars = ["SONAR_TOKEN", "SONAR_HOST_URL"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"Warning: Required SonarQube environment variables missing: {missing_vars}")


def ensure_github_env_loaded() -> None:
    """Ensure GitHub-specific environment variables are loaded."""
    # First load the general environment
    load_env_file()

    # Then load the domain-specific environment
    load_domain_env("domains/github")

    # Check if the required variables are available
    required_vars = ["GITHUB_TOKEN"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"Warning: Required GitHub environment variables missing: {missing_vars}")


def ensure_datadog_env_loaded() -> None:
    """Ensure Datadog-specific environment variables are loaded."""
    # Load general env and domain-specific env
    load_env_file()
    load_domain_env("domains/syngenta/datadog")

    # Check if the required variables are available
    required_vars = ["DD_API_KEY", "DD_APP_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"Warning: Required Datadog environment variables missing: {missing_vars}")
