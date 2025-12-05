"""Service Level Expectations (SLE) Configuration

This module provides configuration and utilities for SLE targets based on priority levels.
"""

import os


class SLEConfig:
    """Configuration class for Service Level Expectations."""

    def __init__(self):
        """Initialize SLE configuration from environment variables."""
        self._sle_targets = {
            "Critical": self._parse_hours(os.getenv("SLE_P1_HOURS", "48")),  # P1: 2 days
            "High": self._parse_hours(os.getenv("SLE_P2_HOURS", "72")),  # P2: 3 days
            "Medium": self._parse_hours(os.getenv("SLE_P3_HOURS", "168")),  # P3: 7 days
            "Low": self._parse_hours(os.getenv("SLE_P4_HOURS", "168")),  # P4: 7 days
        }
        self._default_sle = self._parse_hours(os.getenv("SLE_DEFAULT_HOURS", "168"))

    def _parse_hours(self, value: str) -> float:
        """Parse hours value from environment variable, removing comments.

        Args:
            value: Raw value from environment variable

        Returns:
            Parsed float value
        """
        if not value:
            return 168.0  # Default to 7 days

        # Remove comments (everything after #)
        cleaned_value = value.split("#")[0].strip()

        try:
            return float(cleaned_value)
        except ValueError:
            # If parsing fails, return default
            return 168.0

    def get_sle_target(self, priority: str | None) -> float:
        """Get SLE target in hours for a given priority.

        Args:
            priority: Priority level (Critical, High, Medium, Low)

        Returns:
            SLE target in hours
        """
        if not priority:
            return self._default_sle

        # Normalize priority name
        priority_normalized = priority.strip().title()
        return self._sle_targets.get(priority_normalized, self._default_sle)

    def get_sle_target_days(self, priority: str | None) -> float:
        """Get SLE target in days for a given priority.

        Args:
            priority: Priority level (Critical, High, Medium, Low)

        Returns:
            SLE target in days
        """
        return self.get_sle_target(priority) / 24

    def check_sle_compliance(self, priority: str | None, actual_hours: float) -> dict:
        """Check if actual time meets SLE target.

        Args:
            priority: Priority level
            actual_hours: Actual time taken in hours

        Returns:
            Dictionary with compliance information
        """
        target_hours = self.get_sle_target(priority)
        target_days = target_hours / 24
        actual_days = actual_hours / 24

        is_compliant = actual_hours <= target_hours
        variance_hours = actual_hours - target_hours
        variance_days = variance_hours / 24

        if variance_hours > 0:
            percentage_over = (variance_hours / target_hours) * 100
        else:
            percentage_over = 0

        return {
            "is_compliant": is_compliant,
            "target_hours": target_hours,
            "target_days": target_days,
            "actual_hours": actual_hours,
            "actual_days": actual_days,
            "variance_hours": variance_hours,
            "variance_days": variance_days,
            "percentage_over": percentage_over,
            "status": "✅ Within SLE" if is_compliant else f"❌ {variance_days:.1f}d over SLE",
        }

    def get_all_targets(self) -> dict[str, float]:
        """Get all SLE targets.

        Returns:
            Dictionary mapping priority to SLE target in hours
        """
        return self._sle_targets.copy()

    def get_sle_emoji(self, priority: str | None, actual_hours: float) -> str:
        """Get emoji indicator for SLE compliance.

        Args:
            priority: Priority level
            actual_hours: Actual time taken in hours

        Returns:
            Emoji indicating SLE status
        """
        compliance = self.check_sle_compliance(priority, actual_hours)

        if compliance["is_compliant"]:
            return "✅"
        elif compliance["percentage_over"] <= 50:
            return "⚠️"
        else:
            return "❌"
