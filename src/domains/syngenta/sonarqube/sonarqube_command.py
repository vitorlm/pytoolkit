"""SonarQube Data Command

This command provides access to SonarQube API data for code quality analysis with
caching support for improved performance.

OPERATIONS:
- projects: List all projects in SonarQube (supports predefined project list)
- measures: Get project measures/metrics for a single project
- issues: Get project issues with severity filtering
- metrics: List all available metrics
- batch-measures: Get measures for multiple projects in one request
- list-projects: Get projects from predefined list (projects_list.json) with optional measures

PREDEFINED PROJECT LIST:
The command uses a predefined list of 27 projects stored in:
src/domains/syngenta/sonarqube/projects_list.json

This includes core Syngenta Digital projects like:
- API services (Java, Node.js, Python)
- Web applications (React)
- Backend services and integrations

USAGE EXAMPLES:

1. List all projects from SonarQube:
   python src/main.py syngenta sonarqube --operation projects

2. List projects from predefined list:
   python src/main.py syngenta sonarqube --operation projects --use-project-list

3. Get quality metrics for predefined projects (RECOMMENDED):
   python src/main.py syngenta sonarqube --operation list-projects \
       --organization "syngenta-digital" --include-measures

4. Get quality metrics for predefined projects and save to file:
   python src/main.py syngenta sonarqube --operation list-projects \
       --organization "syngenta-digital" --include-measures \
       --output-file "syngenta_projects_metrics.json"

5. Get custom metrics for predefined projects:
   python src/main.py syngenta sonarqube --operation list-projects \
       --organization "syngenta-digital" --include-measures \
       --metrics "bugs,vulnerabilities,coverage,duplicated_lines_density"

6. Get project measures for a specific project:
   python src/main.py syngenta sonarqube --operation measures \
       --project-key "syngenta_digital_api_java_server_strix" \
       --metrics "lines,bugs,vulnerabilities,code_smells"

7. Get batch measures for multiple specific projects:
   python src/main.py syngenta sonarqube --operation batch-measures \
       --project-keys "project1,project2,project3" \
       --metrics "bugs,vulnerabilities,coverage"

8. Get project issues with severity filtering:
   python src/main.py syngenta sonarqube --operation issues \
       --project-key "syngenta_digital_api_java_server_strix" \
       --severities "MAJOR,CRITICAL,BLOCKER"

9. List all available metrics:
   python src/main.py syngenta sonarqube --operation metrics

CACHING:
- Results are cached for 1 hour to improve performance
- Cache is automatically managed and transparent to users
- Subsequent requests for the same data will be faster
- Use --clear-cache to force refresh and clear existing cache

CACHE MANAGEMENT:
10. Clear cache and get fresh data:
   python src/main.py syngenta sonarqube sonarqube --operation list-projects \
       --organization "syngenta-digital" --include-measures --clear-cache

ENVIRONMENT VARIABLES:
- SONARQUBE_URL: SonarQube instance URL (default: https://sonarcloud.io)
- SONARQUBE_TOKEN: Authentication token for API access
- SONARQUBE_ORGANIZATION: Organization key (default: syngenta-digital)

DEFAULT QUALITY METRICS (used when --include-measures without --metrics):
- alert_status: Overall quality gate status
- bugs: Number of bugs
- reliability_rating: Reliability rating (A-E)
- vulnerabilities: Number of vulnerabilities
- security_rating: Security rating (A-E)
- security_hotspots_reviewed: Security hotspots review percentage
- security_review_rating: Security review rating (A-E)
- code_smells: Number of code smells
- sqale_rating: Maintainability rating (A-E)
- duplicated_lines_density: Duplicated lines percentage
- coverage: Test coverage percentage
- ncloc: Non-commented lines of code
- ncloc_language_distribution: Language distribution
- security_issues: Number of security issues
- reliability_issues: Number of reliability issues
- maintainability_issues: Number of maintainability issues

COMMON METRICS FOR CUSTOM ANALYSIS:
- lines: Total lines of code
- complexity: Cyclomatic complexity
- cognitive_complexity: Cognitive complexity
- test_success_density: Test success density
- branch_coverage: Branch coverage percentage
- line_coverage: Line coverage percentage
- security_remediation_effort: Security remediation effort
- reliability_remediation_effort: Reliability remediation effort
- sqale_index: Technical debt ratio
"""

import json
from argparse import ArgumentParser, Namespace
from datetime import UTC, datetime
from statistics import mean, median

from domains.syngenta.sonarqube.sonarqube_service import SonarQubeService
from domains.syngenta.sonarqube.summary.sonarqube_summary_manager import (
    SonarQubeSummaryManager,
)
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


class SonarQubeCommand(BaseCommand):
    """Command to fetch data from SonarQube API."""

    @staticmethod
    def get_name() -> str:
        return "sonarqube"

    @staticmethod
    def get_description() -> str:
        return "Fetch code quality data from SonarQube API."

    @staticmethod
    def get_help() -> str:
        return "Access SonarQube API to retrieve project metrics, issues, and quality gate data."

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--operation",
            type=str,
            required=True,
            choices=[
                "projects",
                "measures",
                "issues",
                "metrics",
                "batch-measures",
                "list-projects",
            ],
            help=("The operation to perform: projects, measures, issues, metrics, batch-measures, or list-projects."),
        )
        parser.add_argument(
            "--project-key",
            type=str,
            required=False,
            help="The project key (required for measures and issues operations).",
        )
        parser.add_argument(
            "--project-keys",
            type=str,
            required=False,
            help="Comma-separated list of project keys (for batch-measures operation).",
        )
        parser.add_argument(
            "--organization",
            type=str,
            required=False,
            help="Organization key (for SonarCloud).",
        )
        parser.add_argument(
            "--use-project-list",
            action="store_true",
            help="Use predefined project list instead of fetching all projects.",
        )
        parser.add_argument(
            "--include-measures",
            action="store_true",
            help="Include measures when fetching projects from list.",
        )
        parser.add_argument(
            "--metrics",
            type=str,
            required=False,
            default=(
                "alert_status,bugs,reliability_rating,vulnerabilities,security_rating,"
                "security_hotspots_reviewed,security_review_rating,code_smells,sqale_rating,"
                "duplicated_lines_density,coverage,ncloc,ncloc_language_distribution,"
                "security_issues,reliability_issues,maintainability_issues"
            ),
            help="Comma-separated list of metrics to fetch (for measures operation).",
        )
        parser.add_argument(
            "--severities",
            type=str,
            required=False,
            help="Comma-separated list of issue severities to filter by " + "(INFO,MINOR,MAJOR,CRITICAL,BLOCKER).",
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional file path to save the results in JSON format.",
        )
        parser.add_argument(
            "--base-url",
            type=str,
            required=False,
            help="SonarQube instance URL (overrides SONARQUBE_URL environment variable).",
        )
        parser.add_argument(
            "--token",
            type=str,
            required=False,
            help="Authentication token (overrides SONARQUBE_TOKEN environment variable).",
        )
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear the cache before executing the operation.",
        )
        parser.add_argument(
            "--output-format",
            type=str,
            choices=["json", "md"],
            default="console",
            help="Output format: json (JSON file), md (Markdown file), console (display only)",
        )
        parser.add_argument(
            "--summary-output",
            type=str,
            choices=["auto", "json", "none"],
            default="auto",
            help="Control summary metrics persistence: 'auto' stores alongside reports, 'json' forces output, 'none' skips.",
        )

    @staticmethod
    def main(args: Namespace):
        """Main function to execute SonarQube operations.

        Args:
            args (Namespace): Command-line arguments.
        """
        # Ensure environment variables are loaded
        ensure_env_loaded()

        operation = args.operation
        _logger = LogManager.get_instance().get_logger("SonarQubeCommand")

        try:
            service = SonarQubeService(base_url=args.base_url, token=args.token)

            # Clear cache if requested
            if args.clear_cache:
                _logger.info("Clearing cache before operation")
                service.clear_cache()

            # Execute operation and get result
            result = None
            if operation == "projects":
                result = SonarQubeCommand._list_projects(args, service)
            elif operation == "measures":
                result = SonarQubeCommand._get_project_measures(args, service)
            elif operation == "issues":
                result = SonarQubeCommand._get_project_issues(args, service)
            elif operation == "metrics":
                result = SonarQubeCommand._list_available_metrics(args, service)
            elif operation == "batch-measures":
                result = SonarQubeCommand._get_batch_measures(args, service)
            elif operation == "list-projects":
                result = SonarQubeCommand._get_projects_from_list(args, service)

            # Always print executive summary to console
            if result:
                try:
                    SonarQubeCommand._print_executive_summary(result, args)
                except Exception as summary_err:
                    _logger.warning(f"Failed to print executive summary: {summary_err}")

            # Handle output formats (JSON/Markdown) if requested
            if result and args.output_format in ["json", "md"]:
                try:
                    output_path = SonarQubeCommand._handle_output_formats(result, args)
                    print(f"\nOutput file:\n- {output_path}")
                    if args.output_format == "md":
                        print("📄 Detailed report saved in MD format")
                    else:
                        print("✅ Detailed report saved in JSON format")
                except Exception as format_error:
                    _logger.error(f"Failed to save {args.output_format} report: {format_error}")

            # Generate summary if result is available
            if result:
                try:
                    summary_mode = getattr(args, "summary_output", "auto")
                    summary_manager = SonarQubeSummaryManager()
                    args.command_name = "sonarqube"
                    output_path = result.get("output_file") if isinstance(result, dict) else None
                    summary_path = summary_manager.emit_summary_compatible(result, summary_mode, output_path, args)
                    if summary_path:
                        print(f"[summary] wrote: {summary_path}")
                except Exception as summary_error:
                    _logger.warning(f"Failed to write summary metrics: {summary_error}")

        except Exception as e:
            _logger.error(f"Failed to execute {operation} operation: {e}")
            # Enhanced error handling with SonarQube-specific context
            if "authentication" in str(e).lower() or "401" in str(e):
                print("❌ Authentication failed. Check SONARQUBE_TOKEN environment variable.")
            elif "404" in str(e):
                print("❌ Resource not found. Check project key or organization.")
            elif "organization" in str(e).lower():
                print("❌ Organization not found. Check SONARQUBE_ORGANIZATION environment variable.")
            else:
                print(f"❌ Unexpected error: {e}")
            exit(1)

    @staticmethod
    def _list_projects(args: Namespace, service: SonarQubeService):
        """List all projects."""
        projects_data = service.get_projects(
            organization=args.organization,
            use_project_list=args.use_project_list,
            output_file=args.output_file,
        )

        return {
            "operation": "projects",
            "data": projects_data,
            "metadata": {
                "generated_at": "auto-filled",
                "organization": args.organization,
                "use_project_list": args.use_project_list,
                "total_projects": len(projects_data) if isinstance(projects_data, list) else 0,
            },
            "output_file": args.output_file,
        }

    @staticmethod
    def _get_project_measures(args: Namespace, service: SonarQubeService):
        """Get project measures."""
        if not args.project_key:
            logger = LogManager.get_instance().get_logger("SonarQubeCommand")
            logger.error("--project-key is required for measures operation")
            exit(1)

        metrics = [metric.strip() for metric in args.metrics.split(",")]
        measures_data = service.get_project_measures(
            project_key=args.project_key,
            metric_keys=metrics,
            output_file=args.output_file,
        )

        return {
            "operation": "measures",
            "data": measures_data,
            "metadata": {
                "generated_at": "auto-filled",
                "project_key": args.project_key,
                "requested_metrics": metrics,
                "total_metrics": len(metrics),
            },
            "output_file": args.output_file,
        }

    @staticmethod
    def _get_project_issues(args: Namespace, service: SonarQubeService):
        """Get project issues."""
        if not args.project_key:
            logger = LogManager.get_instance().get_logger("SonarQubeCommand")
            logger.error("--project-key is required for issues operation")
            exit(1)

        severities = None
        if args.severities:
            severities = [severity.strip() for severity in args.severities.split(",")]

        issues_data = service.get_project_issues(
            project_key=args.project_key,
            severities=severities,
            output_file=args.output_file,
        )

        return {
            "operation": "issues",
            "data": issues_data,
            "metadata": {
                "generated_at": "auto-filled",
                "project_key": args.project_key,
                "severities": severities,
                "total_issues": len(issues_data) if isinstance(issues_data, list) else 0,
            },
            "output_file": args.output_file,
        }

    @staticmethod
    def _list_available_metrics(args: Namespace, service: SonarQubeService):
        """List all available metrics."""
        metrics_data = service.get_available_metrics(output_file=args.output_file)

        return {
            "operation": "metrics",
            "data": metrics_data,
            "metadata": {
                "generated_at": "auto-filled",
                "total_metrics": len(metrics_data) if isinstance(metrics_data, list) else 0,
            },
            "output_file": args.output_file,
        }

    @staticmethod
    def _get_batch_measures(args: Namespace, service: SonarQubeService):
        """Get measures for multiple projects."""
        if not args.project_keys:
            logger = LogManager.get_instance().get_logger("SonarQubeCommand")
            logger.error("--project-keys is required for batch-measures operation")
            exit(1)

        project_keys = [key.strip() for key in args.project_keys.split(",")]
        metrics = [metric.strip() for metric in args.metrics.split(",")]

        batch_data = service.get_batch_measures(
            project_keys=project_keys, metric_keys=metrics, output_file=args.output_file
        )

        return {
            "operation": "batch-measures",
            "data": batch_data,
            "metadata": {
                "generated_at": "auto-filled",
                "project_keys": project_keys,
                "requested_metrics": metrics,
                "total_projects": len(project_keys),
                "total_metrics": len(metrics),
            },
            "output_file": args.output_file,
        }

    @staticmethod
    def _handle_output_formats(result: dict, args: Namespace) -> str:
        """Handle JSON and Markdown output formats."""
        operation = result.get("operation", "sonarqube")

        # Use OutputManager pattern similar to JIRA commands
        sub_dir = f"sonarqube_{datetime.now().strftime('%Y%m%d')}"
        base_filename = f"sonarqube_{operation}"

        if args.output_format == "json":
            output_path = OutputManager.get_output_path(sub_dir, base_filename, "json")
            from utils.data.json_manager import JSONManager

            JSONManager.write_json(result, output_path)
            return output_path

        elif args.output_format == "md":
            markdown_content = SonarQubeCommand._generate_markdown_report(result, args)
            output_path = OutputManager.save_markdown_report(markdown_content, sub_dir, base_filename)
            return output_path

        return ""

    @staticmethod
    def _generate_markdown_report(result: dict, args: Namespace) -> str:
        """Generate Markdown report from SonarQube result data."""
        operation = result.get("operation", "unknown")
        data = result.get("data", {})
        metadata = result.get("metadata", {})

        if operation == "projects":
            projects = data if isinstance(data, list) else []
            return SonarQubeCommand._build_projects_report(projects, metadata)

        md_content = []
        md_content.append(f"# SonarQube {operation.title()} Report")
        md_content.append("")
        md_content.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        md_content.append(f"**Operation:** {operation}")

        if metadata.get("organization"):
            md_content.append(f"**Organization:** {metadata['organization']}")

        md_content.append("")

        # Add operation-specific content
        if operation == "list-projects":
            md_content.extend(SonarQubeCommand._generate_projects_markdown(data, metadata))
        elif operation == "measures":
            md_content.extend(SonarQubeCommand._generate_measures_markdown(data, metadata))
        elif operation == "batch-measures":
            md_content.extend(SonarQubeCommand._generate_batch_measures_markdown(data, metadata))
        elif operation == "issues":
            md_content.extend(SonarQubeCommand._generate_issues_markdown(data, metadata))
        else:
            md_content.append("## Results")
            md_content.append("```json")
            md_content.append(str(data)[:1000] + "..." if len(str(data)) > 1000 else str(data))
            md_content.append("```")

        return "\n".join(md_content)

    @staticmethod
    def _generate_projects_markdown(data: dict, metadata: dict) -> list:
        """Generate markdown for list-projects operation."""
        md = []
        projects = data.get("projects", [])

        md.append("## Summary")
        md.append(f"- **Total Projects:** {len(projects)}")

        if metadata.get("include_measures"):
            # Quality analysis
            quality_gate_pass = 0
            quality_gate_fail = 0
            total_bugs = 0
            total_vulnerabilities = 0

            for project in projects:
                measures = project.get("measures", {})
                alert_status = measures.get("alert_status", {}).get("value")
                if alert_status == "OK":
                    quality_gate_pass += 1
                elif alert_status == "ERROR":
                    quality_gate_fail += 1

                bugs = measures.get("bugs", {}).get("value")
                if bugs and bugs.isdigit():
                    total_bugs += int(bugs)

                vulnerabilities = measures.get("vulnerabilities", {}).get("value")
                if vulnerabilities and vulnerabilities.isdigit():
                    total_vulnerabilities += int(vulnerabilities)

            md.append(f"- **Quality Gate Pass:** {quality_gate_pass}")
            md.append(f"- **Quality Gate Fail:** {quality_gate_fail}")
            md.append(f"- **Total Bugs:** {total_bugs}")
            md.append(f"- **Total Vulnerabilities:** {total_vulnerabilities}")

        md.append("")
        md.append("## Projects")
        md.append("")
        md.append("| Project | Key | Status | Bugs | Vulnerabilities | Coverage |")
        md.append("|---------|-----|--------|------|----------------|-----------|")

        for project in projects:
            key = project.get("key", "N/A")
            name = project.get("name", "N/A")
            measures = project.get("measures", {})

            alert_status = measures.get("alert_status", {}).get("value", "N/A")
            bugs = measures.get("bugs", {}).get("value", "N/A")
            vulnerabilities = measures.get("vulnerabilities", {}).get("value", "N/A")
            coverage = measures.get("coverage", {}).get("value", "N/A")
            if coverage != "N/A":
                coverage = f"{coverage}%"

            md.append(f"| {name} | {key} | {alert_status} | {bugs} | {vulnerabilities} | {coverage} |")

        return md

    @staticmethod
    def _generate_measures_markdown(data: dict, metadata: dict) -> list:
        """Generate markdown for measures operation."""
        md = []
        component = data.get("component", {})
        measures = component.get("measures", [])

        project_key = component.get("key", "N/A")
        md.append(f"## Project: {project_key}")
        md.append("")
        md.append("| Metric | Value |")
        md.append("|--------|-------|")

        for measure in measures:
            metric = measure.get("metric", "N/A")
            value = measure.get("value", "N/A")
            md.append(f"| {metric} | {value} |")

        return md

    @staticmethod
    def _generate_batch_measures_markdown(data: dict, metadata: dict) -> list:
        """Generate markdown for batch-measures operation."""
        md = []
        measures = data.get("measures", [])

        if not measures:
            md.append("## Batch Measures Overview")
            md.append("")
            md.append("No metrics were returned for the selected projects.")
            return md

        requested_metrics = metadata.get("requested_metrics") or []
        metric_order = (
            [metric.strip() for metric in requested_metrics if metric]
            if requested_metrics
            else sorted({measure.get("metric") for measure in measures})
        )

        project_metrics: dict[str, dict[str, object]] = {}
        display_names: dict[str, str] = {}

        def parse_value(raw_value: object) -> object:
            if isinstance(raw_value, str):
                stripped = raw_value.strip()
                if stripped in {"", "null", "None"}:
                    return None
                # Attempt JSON decode for complex structures
                if (stripped.startswith("{") and stripped.endswith("}")) or (
                    stripped.startswith("[") and stripped.endswith("]")
                ):
                    try:
                        return json.loads(stripped)
                    except json.JSONDecodeError:
                        pass
                try:
                    if "." in stripped:
                        return float(stripped)
                    return int(stripped)
                except ValueError:
                    return stripped
            return raw_value

        for measure in measures:
            project_key = measure.get("component", "unknown-project")
            metric_key = measure.get("metric")
            if not metric_key:
                continue

            metrics_map = project_metrics.setdefault(project_key, {})
            metrics_map[metric_key] = parse_value(measure.get("value"))

            component_name = measure.get("componentName")
            if component_name:
                display_names[project_key] = component_name

        # Ensure all metrics appear even if missing for some projects
        for metrics_map in project_metrics.values():
            for metric_key in metric_order:
                metrics_map.setdefault(metric_key, None)

        def format_metric(metric_key: str, value: object) -> str:
            if value is None:
                return "—"
            if isinstance(value, float):
                if metric_key.endswith("coverage") or metric_key == "coverage":
                    return f"{value:.1f}%"
                if metric_key.endswith("density"):
                    return f"{value:.1f}%"
                return f"{value:.2f}" if abs(value) < 1 else f"{value:.1f}"
            if isinstance(value, int):
                return f"{value}"
            if isinstance(value, dict):
                if "total" in value:
                    details = [f"total={value['total']}"]
                    for key in ["BLOCKER", "CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]:
                        if value.get(key):
                            details.append(f"{key.lower()}={value[key]}")
                    return ", ".join(details)
                return ", ".join(f"{k}={v}" for k, v in value.items())
            if isinstance(value, list):
                return ", ".join(str(item) for item in value)
            return str(value)

        def assess_project(metrics_map: dict[str, object]) -> tuple[str, str]:
            health_order = {"🟢": 0, "🟡": 1, "🟠": 2, "🔴": 3}
            health = "🟢"
            reasons: list[str] = []

            def downgrade(new_health: str, message: str):
                nonlocal health
                if health_order[new_health] > health_order[health]:
                    health = new_health
                reasons.append(message)

            alert_status = metrics_map.get("alert_status")
            if isinstance(alert_status, str):
                status_upper = alert_status.upper()
                if status_upper == "ERROR":
                    downgrade("🔴", "Quality Gate failed")
                elif status_upper not in {"", "OK", "SUCCESS"}:
                    downgrade("🟠", f"Quality Gate: {alert_status}")

            def to_number(value: object) -> float | None:
                if isinstance(value, (int, float)):
                    return float(value)
                if isinstance(value, str):
                    try:
                        return float(value)
                    except ValueError:
                        return None
                return None

            coverage_keys = [key for key in metric_order if key and (key == "coverage" or key.endswith("coverage"))]
            for key in coverage_keys:
                coverage_value = to_number(metrics_map.get(key))
                if coverage_value is None:
                    continue
                if coverage_value < 50:
                    downgrade("🔴", f"{key} {coverage_value:.1f}% (<50%)")
                elif coverage_value < 80:
                    downgrade("🟠", f"{key} {coverage_value:.1f}% (<80%)")

            for key in [
                "bugs",
                "vulnerabilities",
                "security_hotspots",
                "reliability_issues",
                "maintainability_issues",
            ]:
                value = metrics_map.get(key)
                if isinstance(value, dict) and value.get("total"):
                    total = to_number(value.get("total"))
                else:
                    total = to_number(value)
                if total is None:
                    continue
                if total >= 10:
                    downgrade("🔴", f"{key.replace('_', ' ').title()} {total:.0f}")
                elif total > 0:
                    downgrade("🟠", f"{key.replace('_', ' ').title()} {total:.0f}")

            duplication = to_number(metrics_map.get("duplicated_lines_density"))
            if duplication is not None:
                if duplication >= 10:
                    downgrade("🟠", f"Duplication {duplication:.1f}%")
                elif duplication >= 5:
                    downgrade("🟡", f"Duplication {duplication:.1f}%")

            if not reasons:
                reasons.append("All tracked metrics within target thresholds")

            # Keep the most critical two reasons for brevity
            reasons_sorted = reasons[:2]
            return health, "; ".join(reasons_sorted)

        project_rows = []
        for project_key, metrics_map in project_metrics.items():
            health_emoji, health_reason = assess_project(metrics_map)
            name = display_names.get(project_key, project_key)
            project_rows.append(
                {
                    "project_key": project_key,
                    "display_name": name,
                    "health": health_emoji,
                    "health_reason": health_reason,
                    "metrics": metrics_map,
                }
            )

        health_rank_sort = {"🔴": 0, "🟠": 1, "🟡": 2, "🟢": 3}
        project_rows.sort(
            key=lambda row: (
                health_rank_sort.get(row["health"], 4),
                row["display_name"].lower(),
            )
        )

        md.append("## 📊 Batch Metrics Overview")
        md.append("")
        md.append(f"- **Projects covered:** {len(project_rows)}")
        md.append(f"- **Metrics tracked:** {len(metric_order)}")

        quality_gate_counts = {"OK": 0, "ERROR": 0, "OTHER": 0}
        coverage_values = []
        for row in project_rows:
            status = row["metrics"].get("alert_status")
            if isinstance(status, str):
                status_upper = status.upper()
                if status_upper in quality_gate_counts:
                    quality_gate_counts[status_upper] += 1
                else:
                    quality_gate_counts["OTHER"] += 1
            for key in metric_order:
                if key and (key == "coverage" or key.endswith("coverage")):
                    coverage_value = row["metrics"].get(key)
                    if isinstance(coverage_value, (int, float)):
                        coverage_values.append(float(coverage_value))

        if quality_gate_counts["ERROR"] or quality_gate_counts["OTHER"]:
            md.append(
                f"- **Quality Gate:** {quality_gate_counts['OK']} ✅ | {quality_gate_counts['ERROR']} ❌ | {quality_gate_counts['OTHER']} ⚠️"
            )
        elif quality_gate_counts["OK"]:
            md.append(f"- **Quality Gate:** {quality_gate_counts['OK']} ✅")

        if coverage_values:
            avg_cover = sum(coverage_values) / len(coverage_values)
            md.append(f"- **Average coverage:** {avg_cover:.1f}%")
        md.append("")

        header_columns = ["Project", "Health"]
        header_columns.extend(metric_order)
        md.append("| " + " | ".join(header_columns) + " |")
        md.append("| " + " | ".join(["---"] * len(header_columns)) + " |")

        for row in project_rows:
            metrics_map = row["metrics"]
            metric_values = [format_metric(metric_key, metrics_map.get(metric_key)) for metric_key in metric_order]
            md.append(
                "| "
                + " | ".join(
                    [
                        f"{row['display_name']} (`{row['project_key']}`)",
                        f"{row['health']} {row['health_reason']}",
                    ]
                    + metric_values
                )
                + " |"
            )

        md.append("")

        md.append("## 🔎 Highlights")
        md.append("")
        critical_projects = [row for row in project_rows if row["health"] == "🔴"]
        at_risk_projects = [row for row in project_rows if row["health"] == "🟠"]

        if critical_projects:
            md.append("### 🔴 Quality Gate / High Risk")
            for row in critical_projects:
                md.append(f"- {row['display_name']} – {row['health_reason']}")
            md.append("")

        if at_risk_projects:
            md.append("### 🟠 Needs Follow-up")
            for row in at_risk_projects:
                md.append(f"- {row['display_name']} – {row['health_reason']}")
            md.append("")

        if not critical_projects and not at_risk_projects:
            md.append("- All tracked services meet the configured thresholds.")
            md.append("")

        return md

    @staticmethod
    def _generate_issues_markdown(data: list, metadata: dict) -> list:
        """Generate markdown for issues operation."""
        md = []

        if isinstance(data, list):
            md.append("## Issues Summary")
            md.append(f"- **Total Issues:** {len(data)}")

            # Count by severity
            severity_counts = {}
            for issue in data:
                severity = issue.get("severity", "UNKNOWN")
                severity_counts[severity] = severity_counts.get(severity, 0) + 1

            md.append("")
            md.append("### By Severity")
            for severity, count in severity_counts.items():
                md.append(f"- **{severity}:** {count}")

            md.append("")
            md.append("## Issues Detail")
            md.append("")
            md.append("| Key | Type | Severity | Status | Component |")
            md.append("|-----|------|----------|--------|-----------|")

            for issue in data[:50]:  # Limit to first 50 for readability
                key = issue.get("key", "N/A")
                issue_type = issue.get("type", "N/A")
                severity = issue.get("severity", "N/A")
                status = issue.get("status", "N/A")
                component = issue.get("component", "N/A")
                md.append(f"| {key} | {issue_type} | {severity} | {status} | {component} |")

            if len(data) > 50:
                md.append(f"*... and {len(data) - 50} more issues*")

        return md

    @staticmethod
    def _build_projects_report(projects: list, metadata: dict) -> str:
        """Generate a portfolio-style Markdown report for the SonarQube projects list."""
        projects = projects or []
        total_projects = len(projects)
        now = datetime.now(UTC)
        generated_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        organization = metadata.get("organization") or "Unknown"
        if organization == "Unknown" and projects:
            org_from_data = projects[0].get("organization")
            if org_from_data:
                organization = org_from_data
        scope = "Predefined project list" if metadata.get("use_project_list") else "Organization-wide scan"

        visibility_counts: dict[str, int] = {}
        analysis_records: list[dict[str, object]] = []
        project_rows: list[dict[str, object]] = []

        def parse_last_analysis(value: str | None) -> datetime | None:
            if not value:
                return None
            for fmt in (
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S",
            ):
                try:
                    parsed = datetime.strptime(value, fmt)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=UTC)
                    return parsed.astimezone(UTC)
                except (ValueError, TypeError):
                    continue
            return None

        def classify_freshness(days: float | None) -> dict[str, str]:
            if days is None:
                return {
                    "emoji": "⚠️",
                    "label": "No history",
                    "category": "critical",
                    "attention": "🚨 Action",
                }
            if days <= 30:
                return {
                    "emoji": "🟢",
                    "label": "Fresh",
                    "category": "healthy",
                    "attention": "✅ Stable",
                }
            if days <= 60:
                return {
                    "emoji": "🟡",
                    "label": "Aging",
                    "category": "monitor",
                    "attention": "👀 Monitor",
                }
            if days <= 90:
                return {
                    "emoji": "🟠",
                    "label": "Stale",
                    "category": "risk",
                    "attention": "⚠️ Schedule",
                }
            return {
                "emoji": "🔴",
                "label": "Overdue",
                "category": "critical",
                "attention": "🚨 Action",
            }

        for project in projects:
            visibility = (project.get("visibility") or "unknown").lower()
            visibility_counts[visibility] = visibility_counts.get(visibility, 0) + 1

            parsed_analysis = parse_last_analysis(project.get("lastAnalysisDate"))
            if parsed_analysis:
                days_since = (now - parsed_analysis).total_seconds() / 86400
                if days_since < 0:
                    days_since = 0.0
                analysis_records.append(
                    {
                        "project": project,
                        "last_analysis": parsed_analysis,
                        "days_since": days_since,
                    }
                )
            else:
                days_since = None

            freshness = classify_freshness(days_since)
            days_int = int(round(days_since)) if days_since is not None else None
            days_display = f"{days_int}d" if days_int is not None else "—"
            last_analysis_display = parsed_analysis.strftime("%Y-%m-%d") if parsed_analysis else "—"

            if days_since is None:
                reason = "No analysis history available."
            elif freshness["category"] == "healthy":
                reason = f"Scan {days_int} days ago – within the 30-day freshness target."
            elif freshness["category"] == "monitor":
                reason = f"{days_int} days since last scan – schedule within the next month."
            elif freshness["category"] == "risk":
                reason = f"{days_int} days since last scan – approaching 90-day limit."
            else:
                reason = f"{days_int} days since last scan – exceeds 90-day limit."

            name = project.get("name") or project.get("key") or "Unnamed project"
            key = project.get("key", "")
            display_name = f"{name} (`{key}`)" if key and key != name else name

            project_rows.append(
                {
                    "name": name,
                    "key": key,
                    "display_name": display_name,
                    "last_analysis": last_analysis_display,
                    "days_since": days_since,
                    "days_display": days_display,
                    "status_emoji": freshness["emoji"],
                    "status_label": freshness["label"],
                    "category": freshness["category"],
                    "attention": freshness["attention"],
                    "reason": reason,
                    "visibility": visibility.capitalize(),
                }
            )

        records_count = len(analysis_records)
        missing_analysis = total_projects - records_count

        days_values = [record["days_since"] for record in analysis_records]
        avg_days = mean(days_values) if days_values else None
        median_days = median(days_values) if days_values else None
        oldest_record = max(analysis_records, key=lambda record: record["days_since"]) if analysis_records else None
        newest_record = min(analysis_records, key=lambda record: record["days_since"]) if analysis_records else None

        category_counts = {"healthy": 0, "monitor": 0, "risk": 0, "critical": 0}
        for row in project_rows:
            category_counts[row["category"]] += 1

        fresh_count = category_counts["healthy"]
        monitor_count = category_counts["monitor"]
        risk_count = category_counts["risk"]
        critical_count = category_counts["critical"]
        critical_with_history = len(
            [row for row in project_rows if row["category"] == "critical" and row["days_since"] is not None]
        )

        coverage_ratio = records_count / total_projects if total_projects else 0.0
        fresh_ratio = fresh_count / total_projects if total_projects else 0.0
        critical_ratio = critical_count / total_projects if total_projects else 0.0

        if total_projects == 0:
            status_emoji = "⚪️"
            status_text = "No projects returned – verify organization filters"
        elif coverage_ratio == 0:
            status_emoji = "🔴"
            status_text = "Critical – no projects have analysis history"
        elif critical_ratio >= 0.4:
            status_emoji = "🔴"
            status_text = "Critical – large portion overdue or without history"
        elif (risk_count + missing_analysis) / total_projects >= 0.25:
            status_emoji = "🟠"
            status_text = "At Risk – aging scans require attention"
        elif fresh_ratio >= 0.65 and critical_with_history == 0 and missing_analysis == 0:
            status_emoji = "🟢"
            status_text = "Healthy – portfolio scans up to date"
        else:
            status_emoji = "🟡"
            status_text = "Watch – refresh aging projects soon"

        category_order = {"critical": 0, "risk": 1, "monitor": 2, "healthy": 3}
        project_rows_sorted = sorted(
            project_rows,
            key=lambda row: (
                category_order[row["category"]],
                -(row["days_since"] if row["days_since"] is not None else float("inf")),
            ),
        )

        lines: list[str] = []
        lines.append(f"# {status_emoji} SonarQube Portfolio Health Report")
        lines.append("")
        lines.append(f"**Organization:** {organization}")
        lines.append(f"**Generated:** {generated_at}")
        lines.append(f"**Scope:** {scope}")
        lines.append(f"**Total Projects:** {total_projects}")
        lines.append("")

        lines.append("## 📊 Executive Summary")
        lines.append("")
        lines.append(f"**Status:** {status_text}")
        if total_projects:
            lines.append(f"**Healthy (≤30d):** {fresh_count} | Monitor (31-60d): {monitor_count}")
            lines.append(f"**At Risk (61-90d):** {risk_count} | Critical (>90d/missing): {critical_count}")
        lines.append("")

        if total_projects:
            lines.append(f"- **Projects scanned ≤ 30 days:** {fresh_count}")
            lines.append(f"- **Projects overdue (> 90 days):** {critical_with_history}")
            if missing_analysis:
                lines.append(f"- **Projects without analysis history:** {missing_analysis}")
            if avg_days is not None:
                lines.append(f"- **Average days since last scan:** {avg_days:.1f} days")
            if median_days is not None:
                lines.append(f"- **Median days since last scan:** {median_days:.1f} days")
            if newest_record:
                project = newest_record["project"]
                lines.append(
                    "- **Most recent scan:** "
                    f"{project.get('name', 'N/A')}"
                    f" ({newest_record['last_analysis'].strftime('%Y-%m-%d')})"
                    f" – {newest_record['days_since']:.1f} days ago"
                )
            if oldest_record:
                project = oldest_record["project"]
                lines.append(
                    "- **Stalest scan:** "
                    f"{project.get('name', 'N/A')}"
                    f" ({oldest_record['last_analysis'].strftime('%Y-%m-%d')})"
                    f" – {oldest_record['days_since']:.1f} days ago"
                )
        else:
            lines.append("- No projects were retrieved from SonarQube.")
        lines.append("")

        lines.append("## 🗂️ Portfolio Overview")
        lines.append("")
        if project_rows_sorted:
            lines.append("| Project | Last Analysis | Days Since | Status | Visibility | Attention |")
            lines.append("|---------|---------------|------------|--------|------------|-----------|")
            for row in project_rows_sorted:
                status = f"{row['status_emoji']} {row['status_label']}"
                lines.append(
                    f"| {row['display_name']} | {row['last_analysis']} | {row['days_display']} | {status} | {row['visibility']} | {row['attention']} |"
                )
        else:
            lines.append("No projects were returned from SonarQube.")
        lines.append("")

        critical_rows = [row for row in project_rows_sorted if row["category"] == "critical"]
        risk_rows = [row for row in project_rows_sorted if row["category"] == "risk"]
        monitor_rows = [row for row in project_rows_sorted if row["category"] == "monitor"]
        healthy_rows = [row for row in project_rows_sorted if row["category"] == "healthy"]

        def emit_health_section(title: str, emoji: str, rows: list[dict[str, object]]):
            lines.append(f"### {emoji} {title}")
            lines.append("")
            if rows:
                for row in rows:
                    lines.append(f"- {row['display_name']} ({row['days_display']}) – {row['reason']}")
            else:
                lines.append("- None")
            lines.append("")

        lines.append("## 🚦 Health Breakdown")
        lines.append("")
        emit_health_section("Critical – Immediate Action", "🔴", critical_rows)
        emit_health_section("At Risk – Refresh Soon", "🟠", risk_rows)
        emit_health_section("Monitor – Getting Stale", "🟡", monitor_rows)
        emit_health_section("Healthy – Up to Date", "🟢", healthy_rows[:10])
        if len(healthy_rows) > 10:
            lines.append(f"- … {len(healthy_rows) - 10} additional healthy projects not shown")
            lines.append("")

        lines.append("## 🪟 Visibility Snapshot")
        lines.append("")
        if total_projects and visibility_counts:
            lines.append("| Visibility | Projects | Share |")
            lines.append("|------------|----------|-------|")
            for visibility, count in sorted(visibility_counts.items(), key=lambda item: item[1], reverse=True):
                label = visibility.capitalize()
                share = (count / total_projects) * 100
                lines.append(f"| {label} | {count} | {share:.1f}% |")
        else:
            lines.append("| N/A | 0 | 0.0% |")
        lines.append("")

        lines.append("## 💡 Recommendations")
        lines.append("")
        recommendations: list[str] = []
        if total_projects == 0:
            recommendations.append(
                "- Verify SonarQube credentials and organization filters; the API returned no projects."
            )
        else:
            if critical_rows:
                critical_names = ", ".join(row["display_name"] for row in critical_rows[:5])
                if len(critical_rows) > 5:
                    critical_names += ", …"
                recommendations.append(f"- Trigger fresh analyses for critical repositories ({critical_names}).")
            if risk_rows:
                recommendations.append("- Schedule quality gate runs this week for projects flagged as *At Risk*.")
            if missing_analysis:
                recommendations.append(
                    "- Investigate pipelines that never published results to SonarQube (missing history)."
                )
            if fresh_ratio < 0.6:
                recommendations.append(
                    "- Increase automated scanning cadence so at least 60% of services stay within 30 days."
                )
            if not recommendations:
                recommendations.append("- Maintain the current cadence and keep monitoring weekly checks.")

        lines.extend(recommendations)
        lines.append("")

        lines.append("## 📋 Data Quality & Methodology")
        lines.append("")
        lines.append(f"- **Data Source:** {scope} via SonarQube projects search API")
        lines.append("- **Timestamp Basis:** `lastAnalysisDate` provided by SonarQube (normalized to UTC)")
        lines.append("- **Health Buckets:** ≤30d (healthy), 31-60d (monitor), 61-90d (at risk), >90d (critical)")
        lines.append("- **No History:** Projects without `lastAnalysisDate` are treated as critical attention items")
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append(f"*Report generated on {generated_at} using PyToolkit SonarQube Portfolio Analysis Service*")

        return "\n".join(lines)

    @staticmethod
    def _print_executive_summary(result: dict, args: Namespace) -> None:
        """Print executive summary to console regardless of output mode."""
        operation = result.get("operation", "unknown")
        data = result.get("data", {})
        metadata = result.get("metadata", {})

        # Build operation-specific header
        if operation == "batch-measures":
            header = f"📊 SONARQUBE BATCH ANALYSIS - {len(metadata.get('project_keys', []))} Projects"
        elif operation == "list-projects":
            org = metadata.get("organization", "N/A")
            header = f"📊 SONARQUBE PROJECTS - {org}"
        elif operation == "measures":
            project = metadata.get("project_key", "N/A")
            header = f"📊 SONARQUBE MEASURES - {project}"
        elif operation == "issues":
            project = metadata.get("project_key", "N/A")
            header = f"📊 SONARQUBE ISSUES - {project}"
        else:
            header = f"📊 SONARQUBE {operation.upper()}"

        print("\n" + "=" * len(header))
        print(header)
        print("=" * len(header))

        # Operation-specific metrics
        if operation == "batch-measures":
            SonarQubeCommand._print_batch_measures_summary(data, metadata)
        elif operation == "list-projects":
            SonarQubeCommand._print_projects_summary(data, metadata)
        elif operation == "measures":
            SonarQubeCommand._print_single_project_summary(data, metadata)
        elif operation == "issues":
            SonarQubeCommand._print_issues_summary(data, metadata)
        elif operation == "projects":
            SonarQubeCommand._print_all_projects_summary(data, metadata)
        else:
            print(f"Operation: {operation}")
            if isinstance(data, list):
                print(f"Results: {len(data)} items")
            elif isinstance(data, dict):
                print(f"Results: {len(data.keys())} keys")

        print("=" * len(header))

    @staticmethod
    def _print_batch_measures_summary(data: dict, metadata: dict) -> None:
        """Print summary for batch-measures operation."""
        measures = data.get("measures", [])
        project_keys = metadata.get("project_keys", [])

        print(f"Total Projects: {len(project_keys)}")
        print(f"Total Measures: {len(measures)}")

        # Aggregate key metrics
        project_metrics = {}
        for measure in measures:
            project = measure.get("component", "")
            metric = measure.get("metric", "")
            value = measure.get("value", "")

            if project not in project_metrics:
                project_metrics[project] = {}
            project_metrics[project][metric] = value

        # Count quality gate statuses
        quality_gates = {"OK": 0, "ERROR": 0, "NONE": 0}
        total_bugs = 0
        total_vulnerabilities = 0
        coverage_values = []

        for project, metrics in project_metrics.items():
            # Quality gate
            alert_status = metrics.get("alert_status", "NONE")
            if alert_status in quality_gates:
                quality_gates[alert_status] += 1

            # Bugs
            bugs = metrics.get("bugs", "0")
            if bugs.isdigit():
                total_bugs += int(bugs)

            # Vulnerabilities
            vulns = metrics.get("vulnerabilities", "0")
            if vulns.isdigit():
                total_vulnerabilities += int(vulns)

            # Coverage
            coverage = metrics.get("coverage", "")
            if coverage:
                try:
                    coverage_values.append(float(coverage))
                except ValueError:
                    pass

        print(f"Quality Gates - PASS: {quality_gates['OK']}, FAIL: {quality_gates['ERROR']}")
        print(f"Total Bugs: {total_bugs}")
        print(f"Total Vulnerabilities: {total_vulnerabilities}")

        if coverage_values:
            avg_coverage = sum(coverage_values) / len(coverage_values)
            print(f"Average Coverage: {avg_coverage:.1f}%")

    @staticmethod
    def _print_projects_summary(data: dict, metadata: dict) -> None:
        """Print summary for list-projects operation."""
        projects = data.get("projects", [])
        print(f"Total Projects: {len(projects)}")

        if metadata.get("include_measures") and projects:
            quality_gate_pass = 0
            quality_gate_fail = 0
            total_bugs = 0

            for project in projects:
                measures = project.get("measures", {})
                alert_status = measures.get("alert_status", {}).get("value")
                if alert_status == "OK":
                    quality_gate_pass += 1
                elif alert_status == "ERROR":
                    quality_gate_fail += 1

                bugs = measures.get("bugs", {}).get("value")
                if bugs and bugs.isdigit():
                    total_bugs += int(bugs)

            print(f"Quality Gates - PASS: {quality_gate_pass}, FAIL: {quality_gate_fail}")
            print(f"Total Bugs: {total_bugs}")

    @staticmethod
    def _print_single_project_summary(data: dict, metadata: dict) -> None:
        """Print summary for single project measures."""
        component = data.get("component", {})
        measures = component.get("measures", [])
        project_key = component.get("key", "N/A")

        print(f"Project: {project_key}")
        print(f"Metrics Collected: {len(measures)}")

        # Extract key metrics
        key_metrics = {}
        for measure in measures:
            metric = measure.get("metric", "")
            value = measure.get("value", "")
            if metric in ["alert_status", "bugs", "vulnerabilities", "coverage"]:
                key_metrics[metric] = value

        for metric, value in key_metrics.items():
            if metric == "coverage" and value:
                print(f"{metric.title()}: {value}%")
            else:
                print(f"{metric.title().replace('_', ' ')}: {value}")

    @staticmethod
    def _print_issues_summary(data: list, metadata: dict) -> None:
        """Print summary for issues operation."""
        if isinstance(data, list):
            print(f"Total Issues: {len(data)}")

            # Count by severity
            severity_counts = {}
            for issue in data:
                severity = issue.get("severity", "UNKNOWN")
                severity_counts[severity] = severity_counts.get(severity, 0) + 1

            for severity, count in severity_counts.items():
                print(f"{severity}: {count}")

    @staticmethod
    def _print_all_projects_summary(data: list, metadata: dict) -> None:
        """Print summary for all projects operation."""
        if isinstance(data, list):
            print(f"Total Projects: {len(data)}")

            # Count by visibility
            visibility_counts = {}
            for project in data:
                visibility = project.get("visibility", "UNKNOWN")
                visibility_counts[visibility] = visibility_counts.get(visibility, 0) + 1

            for visibility, count in visibility_counts.items():
                print(f"{visibility.title()}: {count}")

    @staticmethod
    def _get_projects_from_list(args: Namespace, service: SonarQubeService):
        """Get projects from predefined list with optional measures and optional filtering
        by project keys. Automatically generate output file if not provided.
        """
        metric_keys = None
        project_keys = None

        # Parse project_keys if provided
        if args.project_keys:
            project_keys = [key.strip() for key in args.project_keys.split(",") if key.strip()]

        # Auto-generate output file path if not provided and output format requires it
        output_file = args.output_file
        if not output_file and args.output_format in ["json", "md"]:
            from datetime import datetime

            sub_dir = f"sonarqube_{datetime.now().strftime('%Y%m%d')}"
            base_filename = "sonarqube_list_projects"
            output_file = OutputManager.get_output_path(sub_dir, base_filename, "json")

        if args.include_measures:
            if args.metrics:
                metric_keys = [metric.strip() for metric in args.metrics.split(",")]
            else:
                # Default metrics for quality analysis
                metric_keys = [
                    "alert_status",
                    "bugs",
                    "reliability_rating",
                    "vulnerabilities",
                    "security_rating",
                    "security_hotspots_reviewed",
                    "security_review_rating",
                    "code_smells",
                    "sqale_rating",
                    "duplicated_lines_density",
                    "coverage",
                    "ncloc",
                    "ncloc_language_distribution",
                    "security_issues",
                    "reliability_issues",
                    "maintainability_issues",
                ]

        projects_list_data = service.get_projects_by_list(
            organization=args.organization,
            include_measures=args.include_measures,
            metric_keys=metric_keys,
            output_file=output_file,
            filter_project_keys=project_keys,
        )

        return {
            "operation": "list-projects",
            "data": projects_list_data,
            "metadata": {
                "generated_at": "auto-filled",
                "organization": args.organization,
                "include_measures": args.include_measures,
                "metric_keys": metric_keys,
                "filter_project_keys": project_keys,
                "total_projects": len(projects_list_data.get("projects", []))
                if isinstance(projects_list_data, dict)
                else 0,
            },
            "output_file": output_file,
        }
