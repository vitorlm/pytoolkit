"""Shared parsing logic for JIRA commands.

This module provides centralized parsing utilities used across multiple JIRA commands
to eliminate code duplication and ensure consistent argument handling.
"""

import re
from argparse import Namespace
from datetime import datetime, timedelta
from typing import Any

from utils.logging.logging_manager import LogManager


class TimeWindowParser:
    """Parser for time window specifications across JIRA commands."""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("TimeWindowParser")

    def parse_time_window(self, args: Namespace) -> dict[str, Any]:
        """Parse time window from various argument formats.

        Args:
            args: Command arguments containing time specifications

        Returns:
            Dictionary with parsed time window information
        """
        try:
            # Check for explicit date range
            if hasattr(args, "end_date") and hasattr(args, "window_days"):
                return self._parse_anchor_window(args.end_date, args.window_days)

            # Check for time_window argument
            if hasattr(args, "time_window") and args.time_window:
                return self._parse_time_window_string(args.time_window)

            # Check for start_date and end_date
            if hasattr(args, "start_date") and hasattr(args, "end_date"):
                return self._parse_date_range(args.start_date, args.end_date)

            # Default fallback
            return self._get_default_time_window()

        except Exception as e:
            self.logger.error(f"Failed to parse time window: {e}")
            return self._get_default_time_window()

    def _parse_anchor_window(self, end_date: str, window_days: int) -> dict[str, Any]:
        """Parse anchor date + window days format."""
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=window_days - 1)

            return {
                "start_date": start_dt.strftime("%Y-%m-%d"),
                "end_date": end_date,
                "window_days": window_days,
                "type": "anchor_window",
                "description": f"{window_days} days ending {end_date}",
            }
        except ValueError as e:
            self.logger.error(f"Invalid date format: {e}")
            return self._get_default_time_window()

    def _parse_time_window_string(self, time_window: str) -> dict[str, Any]:
        """Parse time window string formats."""
        if time_window == "last-week":
            return self._get_relative_window(7, "last week")
        elif time_window == "last-2-weeks":
            return self._get_relative_window(14, "last 2 weeks")
        elif time_window == "last-month":
            return self._get_relative_window(30, "last month")
        elif time_window.endswith("-days"):
            try:
                days = int(time_window.replace("-days", ""))
                return self._get_relative_window(days, f"last {days} days")
            except ValueError:
                return self._get_default_time_window()
        elif "," in time_window:
            # Date range format: "2024-01-01,2024-01-31"
            return self._parse_date_range_string(time_window)
        else:
            # Single date
            return self._parse_single_date(time_window)

    def _parse_date_range(self, start_date: str, end_date: str) -> dict[str, Any]:
        """Parse explicit start and end dates."""
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            days = (end_dt - start_dt).days + 1

            return {
                "start_date": start_date,
                "end_date": end_date,
                "window_days": days,
                "type": "date_range",
                "description": f"Date range: {start_date} to {end_date}",
            }
        except ValueError as e:
            self.logger.error(f"Invalid date range: {e}")
            return self._get_default_time_window()

    def _parse_date_range_string(self, date_range: str) -> dict[str, Any]:
        """Parse comma-separated date range."""
        try:
            start_date, end_date = date_range.split(",")
            return self._parse_date_range(start_date.strip(), end_date.strip())
        except ValueError:
            return self._get_default_time_window()

    def _parse_single_date(self, date_str: str) -> dict[str, Any]:
        """Parse single date specification."""
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return {
                "start_date": date_str,
                "end_date": date_str,
                "window_days": 1,
                "type": "single_date",
                "description": f"Single date: {date_str}",
            }
        except ValueError:
            return self._get_default_time_window()

    def _get_relative_window(self, days: int, description: str) -> dict[str, Any]:
        """Get relative time window from current date."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days - 1)

        return {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "window_days": days,
            "type": "relative",
            "description": description,
        }

    def _get_default_time_window(self) -> dict[str, Any]:
        """Get default time window (last 7 days)."""
        return self._get_relative_window(7, "last 7 days (default)")


class TeamParser:
    """Parser for team and squad specifications."""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("TeamParser")

    def parse_team_filters(self, args: Namespace) -> dict[str, Any]:
        """Parse team-related filter arguments.

        Args:
            args: Command arguments

        Returns:
            Dictionary with parsed team information
        """
        team_info = {"teams": [], "squad": None, "filter_type": "all"}

        try:
            # Single team
            if hasattr(args, "team") and args.team:
                team_info["teams"] = [args.team]
                team_info["filter_type"] = "single_team"

            # Multiple teams
            if hasattr(args, "teams") and args.teams:
                if isinstance(args.teams, str):
                    team_info["teams"] = [t.strip() for t in args.teams.split(",")]
                else:
                    team_info["teams"] = args.teams
                team_info["filter_type"] = "multiple_teams"

            # Squad filter
            if hasattr(args, "squad") and args.squad:
                team_info["squad"] = args.squad
                team_info["filter_type"] = "squad"

            return team_info

        except Exception as e:
            self.logger.error(f"Failed to parse team filters: {e}")
            return team_info

    def normalize_team_value(self, team_data: str | list[str] | None) -> str | None:
        """Normalize team value for consistent output.

        Args:
            team_data: Team data in various formats

        Returns:
            Normalized team string or None
        """
        if not team_data:
            return None

        if isinstance(team_data, list):
            return ",".join(str(t) for t in team_data if t)

        return str(team_data)


class ArgumentValidator:
    """Validator for common JIRA command arguments."""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("ArgumentValidator")

    def validate_project_key(self, project_key: str) -> bool:
        """Validate JIRA project key format.

        Args:
            project_key: Project key to validate

        Returns:
            True if valid, False otherwise
        """
        if not project_key:
            return False

        # Basic JIRA project key validation (2-10 uppercase letters)
        pattern = re.compile(r"^[A-Z]{2,10}$")
        return bool(pattern.match(project_key))

    def validate_issue_types(self, issue_types: str | list[str]) -> list[str]:
        """Validate and normalize issue types.

        Args:
            issue_types: Issue types to validate

        Returns:
            List of normalized issue types
        """
        if not issue_types:
            return []

        if isinstance(issue_types, str):
            types = [t.strip() for t in issue_types.split(",")]
        else:
            types = [str(t).strip() for t in issue_types]

        # Filter out empty values
        return [t for t in types if t]

    def validate_summary_mode(self, summary_mode: str) -> str:
        """Validate summary mode argument.

        Args:
            summary_mode: Summary mode to validate

        Returns:
            Valid summary mode ('auto', 'json', 'none')
        """
        valid_modes = ["auto", "json", "none"]

        if not summary_mode or summary_mode not in valid_modes:
            self.logger.warning(f"Invalid summary mode '{summary_mode}', defaulting to 'auto'")
            return "auto"

        return summary_mode

    def validate_output_format(self, output_format: str) -> str:
        """Validate output format argument.

        Args:
            output_format: Output format to validate

        Returns:
            Valid output format
        """
        valid_formats = ["json", "md", "csv", "console"]

        if not output_format or output_format not in valid_formats:
            self.logger.warning(f"Invalid output format '{output_format}', defaulting to 'console'")
            return "console"

        return output_format


class ErrorHandler:
    """Centralized error handling for JIRA commands."""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("JiraErrorHandler")

    def handle_api_error(self, error: Exception, context: str = "") -> None:
        """Handle JIRA API errors with appropriate user messaging.

        Args:
            error: Exception that occurred
            context: Context information for the error
        """
        error_str = str(error)

        if "400" in error_str and "does not exist for the field 'type'" in error_str:
            self._handle_invalid_issue_type_error(error, context)
        elif "401" in error_str or "authentication" in error_str.lower():
            self._handle_authentication_error(error, context)
        elif "403" in error_str or "permission" in error_str.lower():
            self._handle_permission_error(error, context)
        elif "404" in error_str:
            self._handle_not_found_error(error, context)
        else:
            self._handle_generic_error(error, context)

    def _handle_invalid_issue_type_error(self, error: Exception, context: str):
        """Handle invalid issue type errors."""
        print(f"\n{'=' * 50}")
        print("❌ ERROR: Invalid Issue Type Detected")
        print("=" * 50)
        print(f"Error: {error}")
        if context:
            print(f"Context: {context}")
        print("\nTo fix this issue:")
        print("1. Use the list-custom-fields command to see available issue types")
        print("2. Or try with common issue types: --issue-types 'Bug,Story,Task,Epic'")
        print("=" * 50)

    def _handle_authentication_error(self, error: Exception, context: str):
        """Handle authentication errors."""
        print(f"\n{'=' * 50}")
        print("❌ ERROR: Authentication Failed")
        print("=" * 50)
        print(f"Error: {error}")
        if context:
            print(f"Context: {context}")
        print("\nTo fix this issue:")
        print("1. Check your JIRA credentials in the .env file")
        print("2. Verify your API token is valid")
        print("3. Ensure your JIRA URL is correct")
        print("=" * 50)

    def _handle_permission_error(self, error: Exception, context: str):
        """Handle permission errors."""
        print(f"\n{'=' * 50}")
        print("❌ ERROR: Permission Denied")
        print("=" * 50)
        print(f"Error: {error}")
        if context:
            print(f"Context: {context}")
        print("\nTo fix this issue:")
        print("1. Check if you have access to the specified project")
        print("2. Verify your user permissions in JIRA")
        print("3. Contact your JIRA administrator if needed")
        print("=" * 50)

    def _handle_not_found_error(self, error: Exception, context: str):
        """Handle not found errors."""
        print(f"\n{'=' * 50}")
        print("❌ ERROR: Resource Not Found")
        print("=" * 50)
        print(f"Error: {error}")
        if context:
            print(f"Context: {context}")
        print("\nTo fix this issue:")
        print("1. Verify the project key exists")
        print("2. Check if the resource is accessible")
        print("3. Ensure all parameters are correct")
        print("=" * 50)

    def _handle_generic_error(self, error: Exception, context: str):
        """Handle generic errors."""
        print(f"\n{'=' * 50}")
        print("❌ UNEXPECTED ERROR OCCURRED")
        print("=" * 50)
        print(f"Error: {error}")
        print(f"Error Type: {type(error).__name__}")
        if context:
            print(f"Context: {context}")
        print("\nThis appears to be an unexpected error. Please check the logs for more details:")
        print("Check: logs/ directory for detailed error information")
        print("\nIf the problem persists, try:")
        print("1. Run with --verbose for more details")
        print("2. Use --output-format json to see raw data")
        print("3. Check if all parameters are correct")
        print("=" * 50)

        self.logger.error(f"Unexpected error in {context}: {error}", exc_info=True)
