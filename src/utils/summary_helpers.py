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
