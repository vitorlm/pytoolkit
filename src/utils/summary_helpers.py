"""Utilities for writing standardized summary metric outputs."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Optional


def _safe_write_summary(
    metrics: List[Dict[str, Any]],
    output_dir: str,
    command_name: str,
    file_name: Optional[str] = None,
) -> str:
    """Persist the summary metrics to the expected JSON file and return its absolute path."""
    os.makedirs(output_dir, exist_ok=True)
    summary_path = os.path.join(output_dir, file_name or f"{command_name}_summary.json")
    with open(summary_path, "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, ensure_ascii=False, indent=2)
    return os.path.abspath(summary_path)


def _isoz(dt_str: Optional[str]) -> Optional[str]:
    """Normalize ISO timestamps to ensure a trailing 'Z' for UTC."""
    if not dt_str:
        return dt_str
    if dt_str.endswith("Z"):
        return dt_str
    if dt_str.endswith("+00:00"):
        return dt_str[:-6] + "Z"
    return dt_str + "Z"


def _has_value(value: Any) -> bool:
    """Return True when a metric source value should be emitted."""
    return value is not None


def _extract_metric_value(source: Dict[str, Any], path: Iterable[str]) -> Any:
    """Safely traverse nested dictionaries returning None when any key is missing."""
    current: Any = source
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def build_standard_period(time_window: Optional[str] = None) -> Dict[str, Any]:
    """
    Build standardized period information for summary metrics.

    Args:
        time_window: Time window specification (e.g., 'last-week', 'last-month')

    Returns:
        Dictionary containing period information
    """
    if not time_window:
        return {"description": "No time window specified"}

    # Handle common time window formats
    if time_window == "last-week":
        return {"description": "Last 7 days", "type": "relative", "duration": "7d"}
    elif time_window == "last-2-weeks":
        return {"description": "Last 14 days", "type": "relative", "duration": "14d"}
    elif time_window == "last-month":
        return {"description": "Last 30 days", "type": "relative", "duration": "30d"}
    elif time_window.endswith("-days"):
        days = time_window.replace("-days", "")
        return {
            "description": f"Last {days} days",
            "type": "relative",
            "duration": f"{days}d",
        }
    elif "," in time_window:
        # Date range format
        return {
            "description": f"Date range: {time_window}",
            "type": "absolute",
            "range": time_window,
        }
    else:
        # Single date or other format
        return {
            "description": f"Period: {time_window}",
            "type": "custom",
            "value": time_window,
        }


def build_base_dimensions(
    args, additional_fields: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Build base dimensions from command arguments.

    Args:
        args: Command arguments namespace
        additional_fields: Additional field names to extract from args

    Returns:
        Dictionary containing dimension information
    """
    dimensions = {}

    # Standard fields
    standard_fields = [
        "time_window",
        "team",
        "project_key",
        "squad",
        "issue_types",
        "operation",
        "output_file",
        "verbose",
    ]

    # Add additional fields if specified
    if additional_fields:
        standard_fields.extend(additional_fields)

    for field in standard_fields:
        value = getattr(args, field, None)
        if value is not None:
            # Convert lists to comma-separated strings for readability
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value)
            dimensions[field] = value

    return dimensions


def append_metric_safe(
    metrics_list: List[Dict[str, Any]],
    name: str,
    value: Any,
    unit: str = "",
    description: str = "",
) -> None:
    """
    Safely append a metric to the metrics list with validation.

    This function provides backward compatibility and validation for metric addition.

    Args:
        metrics_list: List to append the metric to
        name: Metric name
        value: Metric value
        unit: Unit of measurement (optional)
        description: Metric description (optional)
    """
    if not name:
        return

    # Handle None or NaN values
    if value is None or (isinstance(value, (int, float)) and value != value):
        value = "N/A"

    metric = {
        "name": str(name),
        "value": value,
        "unit": str(unit) if unit else "",
        "description": str(description) if description else "",
    }

    metrics_list.append(metric)
