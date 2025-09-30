"""
SonarQube-specific summary metrics management.

This module provides SonarQube domain-specific implementation of the SummaryManager,
handling code quality metrics, security assessments, and project analysis with
full compatibility to existing command structures.
"""

import os
from argparse import Namespace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.summary.summary_manager import SummaryManager
from utils.output_manager import OutputManager


class SonarQubeSummaryManager(SummaryManager):
    """
    SonarQube-specific summary metrics management.

    Handles all SonarQube command summary generation with support for:
    - Code quality metrics (bugs, vulnerabilities, code smells)
    - Security ratings and coverage analysis
    - Project compliance and quality gate status
    - Batch project analysis
    - Technical debt metrics
    """

    def __init__(self):
        super().__init__("sonarqube")

    def build_metrics(self, data: Any, args: Namespace) -> Dict[str, Any]:
        """
        Build SonarQube-specific metrics from command result data.

        Args:
            data: SonarQube command result dictionary
            args: Command arguments for context

        Returns:
            Dictionary containing structured SonarQube metrics
        """
        try:
            # Extract components from SonarQube result structure
            result = data if isinstance(data, dict) else {}
            operation = result.get("operation", "unknown")
            metadata = result.get("metadata", {})

            # Build period information
            period_info = self._build_period_info(metadata, args)

            # Build base dimensions
            dimensions = self._build_sonarqube_dimensions(metadata, args)

            # Build metrics list based on operation type
            metrics_list = self._build_operation_metrics(
                result, operation, period_info, dimensions, args
            )

            return {
                "period": period_info,
                "dimensions": dimensions,
                "metrics": metrics_list,
                "source_command": getattr(args, "command_name", "sonarqube"),
                "generated_at": datetime.now().isoformat(),
                "raw_data_available": bool(getattr(args, "output_file", None)),
                "operation": operation,
            }

        except Exception as e:
            self.logger.error(f"Failed to build SonarQube metrics: {e}", exc_info=True)
            return {
                "error": str(e),
                "period": {"description": "Error occurred"},
                "dimensions": {},
                "metrics": [],
                "operation": "error",
            }

    def emit_summary_compatible(
        self,
        result: Dict[str, Any],
        summary_mode: str,
        existing_output_path: Optional[str],
        args: Namespace,
    ) -> Optional[str]:
        """
        Emit summary with full compatibility to existing SonarQube command structure.

        Args:
            result: SonarQube command result dictionary
            summary_mode: Summary mode ('auto', 'json', 'none')
            existing_output_path: Path to existing output file
            args: Command arguments

        Returns:
            Path to summary file if created, None otherwise
        """
        try:
            if summary_mode == "none":
                return None

            # Build metrics using standard method
            metrics = self.build_metrics(result, args)

            # Determine output path
            if existing_output_path:
                summary_path = self._summary_path_for_existing(existing_output_path)
            else:
                summary_path = self._generate_default_summary_path(args)

            # Write summary file
            if summary_mode in ["auto", "json"]:
                os.makedirs(os.path.dirname(summary_path), exist_ok=True)
                from utils.data.json_manager import JSONManager

                JSONManager.write_json(metrics, summary_path)
                return os.path.abspath(summary_path)

            return None

        except Exception as e:
            self.logger.error(f"Failed to emit SonarQube summary: {e}", exc_info=True)
            return None

    def _build_period_info(
        self, metadata: Dict[str, Any], args: Namespace
    ) -> Dict[str, Any]:
        """Build period information for the analysis."""
        return {
            "start_date": None,  # SonarQube doesn't use time windows like JIRA
            "end_date": None,
            "description": f"SonarQube analysis at {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "analysis_type": "code_quality_snapshot",
        }

    def _build_sonarqube_dimensions(
        self, metadata: Dict[str, Any], args: Namespace
    ) -> Dict[str, Any]:
        """Build SonarQube-specific dimensions."""
        dimensions = {}

        # Extract operation-specific dimensions
        if hasattr(args, "operation"):
            dimensions["operation"] = args.operation

        if hasattr(args, "organization") and args.organization:
            dimensions["organization"] = args.organization

        if hasattr(args, "project_key") and args.project_key:
            dimensions["project_key"] = args.project_key

        if hasattr(args, "project_keys") and args.project_keys:
            dimensions["project_keys"] = args.project_keys

        if hasattr(args, "metrics") and args.metrics:
            dimensions["requested_metrics"] = args.metrics

        return dimensions

    def _build_operation_metrics(
        self,
        result: Dict[str, Any],
        operation: str,
        period_info: Dict[str, Any],
        dimensions: Dict[str, Any],
        args: Namespace,
    ) -> List[Dict[str, Any]]:
        """Build metrics based on operation type."""
        if operation == "list-projects":
            return self._build_projects_metrics(result, args)
        elif operation == "measures":
            return self._build_measures_metrics(result, args)
        elif operation == "batch-measures":
            return self._build_batch_metrics(result, args)
        elif operation == "issues":
            return self._build_issues_metrics(result, args)
        elif operation == "projects":
            return self._build_all_projects_metrics(result, args)
        else:
            return self._build_generic_metrics(result, args)

    def _build_projects_metrics(
        self, result: Dict[str, Any], args: Namespace
    ) -> List[Dict[str, Any]]:
        """Build metrics for list-projects operation."""
        metrics = []
        data = result.get("data", {})
        projects = data.get("projects", [])

        # Basic project count
        self.append_metric_safe(
            metrics,
            "Total Projects",
            len(projects),
            "projects",
            "Number of projects analyzed",
        )

        # Quality gate analysis
        if projects and any("measures" in project for project in projects):
            quality_gate_pass = 0
            quality_gate_fail = 0
            total_bugs = 0
            total_vulnerabilities = 0
            total_code_smells = 0
            coverage_values = []

            for project in projects:
                measures = project.get("measures", {})

                # Quality gate status
                alert_status = measures.get("alert_status", {}).get("value")
                if alert_status == "OK":
                    quality_gate_pass += 1
                elif alert_status == "ERROR":
                    quality_gate_fail += 1

                # Quality metrics
                bugs = measures.get("bugs", {}).get("value")
                if bugs and bugs.isdigit():
                    total_bugs += int(bugs)

                vulnerabilities = measures.get("vulnerabilities", {}).get("value")
                if vulnerabilities and vulnerabilities.isdigit():
                    total_vulnerabilities += int(vulnerabilities)

                code_smells = measures.get("code_smells", {}).get("value")
                if code_smells and code_smells.isdigit():
                    total_code_smells += int(code_smells)

                coverage = measures.get("coverage", {}).get("value")
                if coverage:
                    try:
                        coverage_values.append(float(coverage))
                    except (ValueError, TypeError):
                        pass

            # Quality gate metrics
            total_with_quality_gate = quality_gate_pass + quality_gate_fail
            if total_with_quality_gate > 0:
                self.append_metric_safe(
                    metrics, "Quality Gate Pass", quality_gate_pass, "projects"
                )
                self.append_metric_safe(
                    metrics, "Quality Gate Fail", quality_gate_fail, "projects"
                )
                pass_rate = (quality_gate_pass / total_with_quality_gate) * 100
                self.append_metric_safe(
                    metrics, "Quality Gate Pass Rate", f"{pass_rate:.1f}", "%"
                )

            # Quality metrics
            self.append_metric_safe(metrics, "Total Bugs", total_bugs, "issues")
            self.append_metric_safe(
                metrics, "Total Vulnerabilities", total_vulnerabilities, "issues"
            )
            self.append_metric_safe(
                metrics, "Total Code Smells", total_code_smells, "issues"
            )

            # Coverage statistics
            if coverage_values:
                avg_coverage = sum(coverage_values) / len(coverage_values)
                self.append_metric_safe(
                    metrics, "Average Coverage", f"{avg_coverage:.1f}", "%"
                )
                self.append_metric_safe(
                    metrics, "Projects with Coverage", len(coverage_values), "projects"
                )

        return metrics

    def _build_measures_metrics(
        self, result: Dict[str, Any], args: Namespace
    ) -> List[Dict[str, Any]]:
        """Build metrics for single project measures operation."""
        metrics = []
        data = result.get("data", {})
        component = data.get("component", {})
        measures = component.get("measures", [])

        # Project info
        project_key = component.get("key", "unknown")
        self.append_metric_safe(
            metrics, "Project Key", project_key, "", "Analyzed project"
        )

        # Process measures
        for measure in measures:
            metric_key = measure.get("metric", "")
            value = measure.get("value", "")

            # Convert metric names to readable format
            readable_name = self._get_readable_metric_name(metric_key)
            unit = self._get_metric_unit(metric_key)

            self.append_metric_safe(metrics, readable_name, value, unit)

        return metrics

    def _build_batch_metrics(
        self, result: Dict[str, Any], args: Namespace
    ) -> List[Dict[str, Any]]:
        """Build metrics for batch measures operation."""
        metrics = []
        data = result.get("data", {})
        measures = data.get("measures", [])

        # Count projects
        projects = set()
        metric_totals = {}

        for measure in measures:
            project_key = measure.get("component", "")
            if project_key:
                projects.add(project_key)

            metric_key = measure.get("metric", "")
            value = measure.get("value", "")

            # Aggregate numeric metrics
            if metric_key and value:
                if metric_key not in metric_totals:
                    metric_totals[metric_key] = {"values": [], "total": 0}

                try:
                    numeric_value = float(value)
                    metric_totals[metric_key]["values"].append(numeric_value)
                    metric_totals[metric_key]["total"] += numeric_value
                except (ValueError, TypeError):
                    pass

        # Basic metrics
        self.append_metric_safe(metrics, "Projects Analyzed", len(projects), "projects")
        self.append_metric_safe(metrics, "Total Measures", len(measures), "measures")

        # Aggregated metrics
        for metric_key, data in metric_totals.items():
            values = data["values"]
            if values:
                readable_name = self._get_readable_metric_name(metric_key)
                unit = self._get_metric_unit(metric_key)

                # Total and average
                total = data["total"]
                average = total / len(values)

                self.append_metric_safe(
                    metrics, f"Total {readable_name}", f"{total:.1f}", unit
                )
                self.append_metric_safe(
                    metrics, f"Average {readable_name}", f"{average:.1f}", unit
                )

        return metrics

    def _build_issues_metrics(
        self, result: Dict[str, Any], args: Namespace
    ) -> List[Dict[str, Any]]:
        """Build metrics for issues operation."""
        metrics = []
        data = result.get("data", [])

        if isinstance(data, list):
            # Count by severity
            severity_counts = {}
            type_counts = {}

            for issue in data:
                severity = issue.get("severity", "UNKNOWN")
                issue_type = issue.get("type", "UNKNOWN")

                severity_counts[severity] = severity_counts.get(severity, 0) + 1
                type_counts[issue_type] = type_counts.get(issue_type, 0) + 1

            # Total issues
            self.append_metric_safe(metrics, "Total Issues", len(data), "issues")

            # By severity
            for severity, count in severity_counts.items():
                self.append_metric_safe(metrics, f"{severity} Issues", count, "issues")

            # By type
            for issue_type, count in type_counts.items():
                self.append_metric_safe(
                    metrics, f"{issue_type} Issues", count, "issues"
                )

        return metrics

    def _build_all_projects_metrics(
        self, result: Dict[str, Any], args: Namespace
    ) -> List[Dict[str, Any]]:
        """Build metrics for projects operation (list all projects)."""
        metrics = []
        data = result.get("data", [])

        if isinstance(data, list):
            self.append_metric_safe(metrics, "Total Projects", len(data), "projects")

            # Analyze visibility if available
            visibility_counts = {}
            for project in data:
                visibility = project.get("visibility", "UNKNOWN")
                visibility_counts[visibility] = visibility_counts.get(visibility, 0) + 1

            for visibility, count in visibility_counts.items():
                self.append_metric_safe(
                    metrics, f"{visibility} Projects", count, "projects"
                )

        return metrics

    def _build_generic_metrics(
        self, result: Dict[str, Any], args: Namespace
    ) -> List[Dict[str, Any]]:
        """Build generic metrics for unknown operations."""
        metrics = []

        # Basic result info
        operation = result.get("operation", "unknown")
        self.append_metric_safe(
            metrics, "Operation", operation, "", "SonarQube operation performed"
        )

        data = result.get("data")
        if isinstance(data, list):
            self.append_metric_safe(metrics, "Results Count", len(data), "items")
        elif isinstance(data, dict):
            self.append_metric_safe(metrics, "Result Keys", len(data.keys()), "keys")

        return metrics

    def _get_readable_metric_name(self, metric_key: str) -> str:
        """Convert SonarQube metric key to readable name."""
        name_mapping = {
            "alert_status": "Quality Gate Status",
            "bugs": "Bugs",
            "vulnerabilities": "Vulnerabilities",
            "code_smells": "Code Smells",
            "coverage": "Test Coverage",
            "duplicated_lines_density": "Duplicated Lines",
            "ncloc": "Lines of Code",
            "reliability_rating": "Reliability Rating",
            "security_rating": "Security Rating",
            "sqale_rating": "Maintainability Rating",
            "security_hotspots_reviewed": "Security Hotspots Reviewed",
            "security_review_rating": "Security Review Rating",
            "lines": "Total Lines",
            "complexity": "Cyclomatic Complexity",
            "cognitive_complexity": "Cognitive Complexity",
        }
        return name_mapping.get(metric_key, metric_key.replace("_", " ").title())

    def _get_metric_unit(self, metric_key: str) -> str:
        """Get unit for SonarQube metric."""
        unit_mapping = {
            "coverage": "%",
            "duplicated_lines_density": "%",
            "security_hotspots_reviewed": "%",
            "bugs": "issues",
            "vulnerabilities": "issues",
            "code_smells": "issues",
            "ncloc": "lines",
            "lines": "lines",
            "complexity": "complexity",
            "cognitive_complexity": "complexity",
        }
        return unit_mapping.get(metric_key, "")

    def _summary_path_for_existing(self, existing_path: str) -> str:
        """Generate summary path based on existing output file."""
        path = Path(existing_path)
        summary_name = path.stem + "_summary" + ".json"
        return str(path.parent / summary_name)

    def _generate_default_summary_path(self, args: Namespace) -> str:
        """Generate default summary path when no output file exists."""
        operation = getattr(args, "operation", "sonarqube")
        sub_dir = f"sonarqube_{datetime.now().strftime('%Y%m%d')}"
        filename = f"sonarqube_{operation}_summary"
        return OutputManager.get_output_path(sub_dir, filename, "json")
