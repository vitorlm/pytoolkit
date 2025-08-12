import os
import glob
import datetime
from typing import Dict, Any, List, Optional
from jinja2 import Environment, FileSystemLoader
import pandas as pd

from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager


class WeeklyReportService:
    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("WeeklyReportService")
        self.cache = CacheManager.get_instance()

        # Setup Jinja2 environment
        template_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        self.jinja_env = Environment(loader=FileSystemLoader(template_dir), trim_blocks=True, lstrip_blocks=True)

    def generate_report(self, args) -> str:
        """
        Loads, analyzes, and aggregates all required data, then generates the markdown report.
        Dynamically discovers files based on scope and adapts to different configurations.
        """
        scope = args.scope
        period = args.period
        team = getattr(args, "team", None)
        output_dir = args.output_dir

        self.logger.info(f"Generating {scope} report for period: {period}")
        if team:
            self.logger.info(f"Team: {team}")

        # Auto-discover latest output directory if not specified
        if not os.path.exists(output_dir) or output_dir == "output":
            output_dir = self._discover_latest_output_dir(scope, output_dir)

        if not output_dir or not os.path.exists(output_dir):
            raise FileNotFoundError(f"No valid output directory found. Expected pattern: {scope}_weekly_reports_*")

        # Dynamic file discovery
        file_mappings = self._discover_data_files(output_dir, scope)

        # Load data dynamically
        data = self._load_all_data(file_mappings)

        # Generate template context
        context = self._build_jinja_context(data, scope, period, team, output_dir)

        # Render template
        report_content = self._render_jinja_template(context)

        # Save report
        output_file = os.path.join(output_dir, "consolidated", f"{scope}-weekly-engineering-report.md")
        FileManager.create_folder(os.path.dirname(output_file))

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(report_content)

        self.logger.info(f"Weekly report generated at: {output_file}")
        return output_file

    def _render_jinja_template(self, context: Dict[str, Any]) -> str:
        """Renders the Jinja2 template with the provided context."""
        try:
            # Debug: Log the available context keys
            self.logger.info(f"Template context keys: {list(context.keys())}")
            
            template = self.jinja_env.get_template("report_template.j2")
            
            # Try to render the template
            result = template.render(**context)
            self.logger.info("Template rendered successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"Error rendering template: {e}")
            self.logger.error(f"Template error details: {type(e).__name__}: {str(e)}")
            
            # Log the full traceback for debugging
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Also log context structure for debugging
            for key, value in context.items():
                self.logger.error(f"Context[{key}]: {type(value)} = {str(value)[:200]}...")
            
            # Fallback to simple template if Jinja2 fails
            return self._generate_fallback_template(context)

    def _build_jinja_context(
        self, data: Dict[str, Any], scope: str, period: str, team: str | None = None, output_dir: str = ""
    ) -> Dict[str, Any]:
        """Builds the context for Jinja2 template rendering."""
        context = {
            # Basic info
            "report_week_range": self._get_week_range(period),
            "comparison_period": self._get_comparison_period(period),
            "report_date": datetime.datetime.now().strftime("%B %d, %Y"),
            "report_timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            # Data sources info
            "data_sources": {"jira_project": "CWS", "linearb_team_id": "19767", "sonar_org": "syngenta-digital"},
        }

        # Bugs & Support Overview
        context["bugs_support"] = self._build_bugs_support_context(output_dir)

        # Oldest Issues
        context["oldest_issues"] = self._build_oldest_issues_context(data["jira"].get("open-issues", {}))

        # Cycle Time
        context["cycle_time"] = self._build_cycle_time_context(data["jira"].get("cycle-time-bugs-lastweek", {}))

        # Adherence
        adherence_data = self._build_adherence_context(
            data["jira"].get("bugs-support-lastweek", {}), data["jira"].get("bugs-support-weekbefore", {})
        )
        context["adherence"] = adherence_data

        # Tasks
        context["tasks"] = self._build_tasks_context(data["jira"].get("tasks-2weeks", {}))

        # LinearB
        linearb_data = self._build_linearb_context(data["linearb"].get("metrics", []))
        context["linearb_metrics"] = linearb_data["metrics"]
        context["linearb_summary"] = linearb_data["summary"]

        # SonarQube
        sonar_data = self._build_sonar_context(data["sonarqube"].get("quality_metrics", {}))
        context["sonar_projects"] = sonar_data["projects"]
        context["sonar_summary"] = sonar_data["summary"]

        # Next Actions (default for now)
        context["next_actions"] = {
            "immediate": [
                "Review high-priority cycle time issues",
                "Address security hotspots in critical projects",
                "Follow up on overdue epics",
            ],
            "short_term": [
                "Improve code coverage for projects below 70%",
                "Optimize review process to reduce pickup time",
                "Plan technical debt reduction initiatives",
            ],
        }

        return context

    def _build_bugs_support_context(self, output_dir: str) -> dict:
        """Build context for bugs and support overview."""
        lastweek_file = os.path.join(output_dir, "jira", "tribe-bugs-support-lastweek.json")
        previousweek_file = os.path.join(output_dir, "jira", "tribe-bugs-support-previousweek.json")

        lastweek_data = JSONManager.read_json(lastweek_file, default={})
        previousweek_data = JSONManager.read_json(previousweek_file, default={})

        # Count bugs and support issues
        lastweek_issues = lastweek_data.get("issues", [])
        previousweek_issues = previousweek_data.get("issues", [])

        # Count by issue type
        lastweek_bugs = sum(1 for issue in lastweek_issues if issue.get("issue_type") == "Bug")
        lastweek_support = sum(1 for issue in lastweek_issues if issue.get("issue_type") == "Support")

        previousweek_bugs = sum(1 for issue in previousweek_issues if issue.get("issue_type") == "Bug")
        previousweek_support = sum(1 for issue in previousweek_issues if issue.get("issue_type") == "Support")

        return {
            "lastweek_bugs": lastweek_bugs,
            "lastweek_support": lastweek_support,
            "previousweek_bugs": previousweek_bugs,
            "previousweek_support": previousweek_support,
            "bugs_change": lastweek_bugs - previousweek_bugs,
            "support_change": lastweek_support - previousweek_support,
        }

    def _build_oldest_issues_context(self, open_issues_data: Dict) -> List[Dict[str, Any]]:
        """Builds oldest open issues context."""
        issues = open_issues_data.get("issues", [])
        oldest_issues = []

        for issue in issues[:5]:  # Get top 5 oldest
            oldest_issues.append(
                {
                    "type": issue.get("issue_type", "Support"),
                    "key": issue.get("issue_key", "N/A"),
                    "created_date": issue.get("created_date", "N/A"),
                    "summary": issue.get("summary", "No description available"),
                    "assignee": issue.get("assignee", "None"),
                }
            )

        return oldest_issues

    def _build_cycle_time_context(self, cycle_data: Dict) -> Dict[str, Any]:
        """Builds cycle time section context."""
        metrics = cycle_data.get("metrics", {})

        context = {
            "period": "July 28 - August 3, 2025",  # Default period
            "total_issues": metrics.get("total_issues", 0),
            "average_hours": metrics.get("average_cycle_time_hours", 0),
            "median_hours": metrics.get("median_cycle_time_hours", 0),
            "max_hours": metrics.get("max_cycle_time_hours", 0),
            "priority_breakdown": [],
        }

        # Priority breakdown
        priority_breakdown = metrics.get("priority_breakdown", {})
        for priority_key, priority_data in priority_breakdown.items():
            context["priority_breakdown"].append(
                {
                    "name": priority_key,
                    "count": priority_data.get("count", 0),
                    "avg_hours": priority_data.get("average_cycle_time_hours", 0),
                }
            )

        return context

    def _build_adherence_context(self, lastweek_data: Dict, weekbefore_data: Dict) -> Dict[str, Any]:
        """Builds adherence section context."""
        lastweek_metrics = lastweek_data.get("metrics", {})
        weekbefore_metrics = weekbefore_data.get("metrics", {})

        categories = []
        category_mapping = {"Early": "early", "On Time": "on_time", "Late": "late", "No Due Date": "no_due_date"}

        for display_name, metric_key in category_mapping.items():
            lw_count = lastweek_metrics.get(metric_key, 0)
            wb_count = weekbefore_metrics.get(metric_key, 0)
            lw_pct = lastweek_metrics.get(f"{metric_key}_percentage", 0)
            wb_pct = weekbefore_metrics.get(f"{metric_key}_percentage", 0)

            # Calculate change
            pct_change = lw_pct - wb_pct
            change_indicator = (
                f"⬆️ +{pct_change:.1f}pp" if pct_change > 0 else f"⬇️ {pct_change:.1f}pp" if pct_change < 0 else "➡️ 0.0pp"
            )

            categories.append(
                {
                    "name": display_name,
                    "current_count": lw_count,
                    "current_percentage": f"{lw_pct:.1f}",
                    "previous_count": wb_count,
                    "previous_percentage": f"{wb_pct:.1f}",
                    "change_indicator": change_indicator,
                }
            )

        # Overall adherence (on time + early)
        on_time_count = lastweek_metrics.get("on_time", 0) + lastweek_metrics.get("early", 0)
        total_with_dates = lastweek_metrics.get("issues_with_due_dates", 1)
        overall_adherence = (on_time_count / total_with_dates * 100) if total_with_dates > 0 else 0

        return {
            "resolution_period": "July 14 – July 21",  # Default period
            "bugs_support": categories,
            "overall_adherence": f"{overall_adherence:.1f}",
        }

    def _build_tasks_context(self, tasks_data: Dict) -> Dict[str, Any]:
        """Builds tasks section context."""
        metrics = tasks_data.get("metrics", {})

        categories = []
        category_mapping = {"Early": "early", "On Time": "on_time", "Late": "late", "No Due Date": "no_due_date"}

        for display_name, metric_key in category_mapping.items():
            count = metrics.get(metric_key, 0)
            percentage = metrics.get(f"{metric_key}_percentage", 0)

            categories.append({"name": display_name, "count": count, "percentage": f"{percentage:.1f}"})

        # Issue types breakdown
        issue_types = []
        issues = tasks_data.get("issues", [])
        issue_type_counts = {}

        for issue in issues:
            issue_type = issue.get("issue_type", "Unknown")
            issue_type_counts[issue_type] = issue_type_counts.get(issue_type, 0) + 1

        for issue_type, count in issue_type_counts.items():
            issue_types.append({"name": issue_type, "count": count})

        # Overall adherence
        on_time_count = metrics.get("on_time", 0) + metrics.get("early", 0)
        total_with_dates = metrics.get("issues_with_due_dates", 1)
        overall_adherence = (on_time_count / total_with_dates * 100) if total_with_dates > 0 else 0

        return {
            "period": "July 21 - August 3, 2025",  # Default period
            "total_issues": metrics.get("total_issues", 0),
            "categories": categories,
            "issue_types": issue_types,
            "overall_adherence": f"{overall_adherence:.1f}",
        }

    def _build_linearb_context(self, linearb_metrics: List[Dict]) -> Dict[str, Any]:
        """Builds LinearB section context."""
        if not linearb_metrics:
            return {
                "metrics": [],
                "summary": {
                    "review_depth": "N/A",
                    "review_depth_change": "N/A",
                    "prs_without_review": "N/A",
                    "prs_without_review_change": "N/A",
                    "pr_maturity": "N/A",
                    "pr_maturity_change": "N/A",
                },
            }

        # Extract current and previous week data
        current_week = linearb_metrics[0] if len(linearb_metrics) > 0 else {}
        previous_week = linearb_metrics[1] if len(linearb_metrics) > 1 else {}

        metrics = []
        metric_mappings = {
            "Cycle Time (avg, hours)": ("Cycle Time - avg (Minutes)", "h", 60),  # Convert minutes to hours
            "Pickup Time (avg, hours)": ("Pickup Time - avg (Minutes)", "h", 60),
            "Review Time (avg, hours)": ("Review Time - avg (Minutes)", "h", 60),
            "Deploy Frequency (count)": ("Deploy frequency (Deployments)", "", 1),
            "Deploy Time (avg, hours)": ("Deploy Time - avg (Minutes)", "h", 60),
        }

        for display_name, (field_name, unit, divisor) in metric_mappings.items():
            current_val = current_week.get(field_name, 0) / divisor if current_week.get(field_name, 0) else 0
            previous_val = previous_week.get(field_name, 0) / divisor if previous_week.get(field_name, 0) else 0

            # Calculate change percentage
            if previous_val > 0:
                change_pct = ((current_val - previous_val) / previous_val) * 100
                change_indicator = (
                    f"⬆️ {change_pct:.1f}%"
                    if change_pct > 0
                    else f"⬇️ {abs(change_pct):.1f}%"
                    if change_pct < 0
                    else "➡️ 0.0%"
                )
            else:
                change_indicator = "➡️ 0.0%"

            metrics.append(
                {
                    "name": display_name,
                    "current_value": f"{current_val:.1f} {unit}".strip()
                    if isinstance(current_val, float)
                    else f"{current_val} {unit}".strip(),
                    "previous_value": f"{previous_val:.1f} {unit}".strip()
                    if isinstance(previous_val, float)
                    else f"{previous_val} {unit}".strip(),
                    "change_indicator": change_indicator,
                }
            )

        # Summary metrics
        summary = {
            "review_depth": f"{current_week.get('Review Depth (Comments per review)', 0):.1f}",
            "review_depth_change": "⬇️",  # Default change indicators
            "prs_without_review": current_week.get("PRs merged w/o review (PRs)", 0),
            "prs_without_review_change": "⬇️",
            "pr_maturity": f"{current_week.get('PR Maturity', 0):.0f}",
            "pr_maturity_change": "⬆️",
        }

        return {"metrics": metrics, "summary": summary}

    def _build_sonar_context(self, sonar_data: Dict) -> Dict[str, Any]:
        """Builds SonarQube section context."""
        projects = []
        sonar_projects = sonar_data.get("projects", [])

        for project in sonar_projects:
            measures = project.get("measures", {})
            projects.append(
                {
                    "name": project.get("key", "Unknown").replace("syngenta-digital_", ""),
                    "quality_gate": measures.get("alert_status", {}).get("value", "UNKNOWN"),
                    "coverage": f"{float(measures.get('coverage', {}).get('value', 0)):.0f}",
                    "bugs": measures.get("bugs", {}).get("value", 0),
                    "reliability": f"{float(measures.get('reliability_rating', {}).get('value', 0)):.1f}",
                    "code_smells": measures.get("code_smells", {}).get("value", 0),
                    "security_hotspots": f"{float(measures.get('security_hotspots_reviewed', {}).get('value', 0)):.0f}",
                }
            )

        # Determine overall health status
        quality_gates = [p["quality_gate"] for p in projects]
        failed_gates = quality_gates.count("ERROR")

        if failed_gates > len(quality_gates) * 0.5:
            health_status = "CRITICAL"
        elif failed_gates > 0:
            health_status = "WARNING"
        else:
            health_status = "HEALTHY"

        return {
            "projects": projects,
            "summary": {
                "health_status": health_status,
                "observations": [
                    "Code coverage needs improvement across multiple projects",
                    "Security hotspots require immediate attention",
                    "Quality gates failing in several repositories",
                ],
            },
        }

    def _generate_fallback_template(self, context: Dict[str, Any]) -> str:
        """Generates a simple fallback template if Jinja2 fails."""
        return f"""# Weekly Engineering Report

**Report Date:** {context.get("report_date", "N/A")}

## Summary
- Total Issues: {context.get("cycle_time", {}).get("total_issues", 0)}
- Average Cycle Time: {context.get("cycle_time", {}).get("average_hours", 0):.1f}h

## Data Sources
- JIRA Project: {context.get("data_sources", {}).get("jira_project", "N/A")}
- LinearB Team: {context.get("data_sources", {}).get("linearb_team_id", "N/A")}

*Note: This is a fallback template. The full template failed to render.*
"""

    # Keep the existing utility methods
    def _discover_latest_output_dir(self, scope: str, base_output_dir: str = "output") -> str | None:
        """Discovers the latest output directory based on scope and date pattern."""
        pattern = os.path.join(base_output_dir, f"{scope}_weekly_reports_*")
        matching_dirs = glob.glob(pattern)

        if not matching_dirs:
            return None

        # Sort by directory name (which includes date) to get the latest
        latest_dir = sorted(matching_dirs)[-1]
        self.logger.info(f"Auto-discovered output directory: {latest_dir}")
        return latest_dir

    def _discover_data_files(self, output_dir: str, scope: str) -> dict:
        """Dynamically discovers available data files in the output directory."""
        file_mappings = {}

        # Define expected subdirectories and file patterns
        subdirs_patterns = {
            "jira": {
                "bugs-support-lastweek": f"{scope}-bugs-support-lastweek.json",
                "bugs-support-weekbefore": f"{scope}-bugs-support-weekbefore.json",
                "open-issues": f"{scope}-open-issues.json",
                "cycle-time-bugs-lastweek": f"{scope}-cycle-time-bugs-lastweek.json",
                "tasks-2weeks": f"{scope}-tasks-2weeks.json",
            },
            "sonarqube": {"quality_metrics": f"{scope}-quality-metrics.json"},
            "linearb": {"metrics": "linearb_report_*.csv"},
        }

        for subdir, patterns in subdirs_patterns.items():
            subdir_path = os.path.join(output_dir, subdir)
            if os.path.exists(subdir_path):
                file_mappings[subdir] = {}
                for key, pattern in patterns.items():
                    if "*" in pattern:
                        # Use glob for wildcard patterns
                        matches = glob.glob(os.path.join(subdir_path, pattern))
                        if matches:
                            file_mappings[subdir][key] = sorted(matches)[-1]  # Get latest
                    else:
                        # Direct file path
                        file_path = os.path.join(subdir_path, pattern)
                        if os.path.exists(file_path):
                            file_mappings[subdir][key] = file_path

        return file_mappings

    def _load_all_data(self, file_mappings: dict) -> dict:
        """Loads all data files based on the discovered mappings."""
        data = {}

        for subdir, files in file_mappings.items():
            data[subdir] = {}
            for key, file_path in files.items():
                try:
                    if file_path.endswith(".json"):
                        data[subdir][key] = JSONManager.read_json(file_path)
                        self.logger.info(f"Loaded {subdir.upper()} {key}: {file_path}")
                    elif file_path.endswith(".csv"):
                        # For CSV files (LinearB), convert to list of dicts
                        df = pd.read_csv(file_path)
                        data[subdir][key] = df.to_dict("records")
                        self.logger.info(f"Parsed {len(data[subdir][key])} LinearB records")
                        self.logger.info(f"Loaded {subdir.upper()} {key}: {file_path}")
                except Exception as e:
                    self.logger.error(f"Failed to load {file_path}: {e}")
                    data[subdir][key] = {}

        return data

    def _get_week_range(self, period: str) -> str:
        """Generates week range based on period."""
        if period == "last-week":
            today = datetime.date.today()
            start_week = today - datetime.timedelta(days=today.weekday() + 7)
            end_week = start_week + datetime.timedelta(days=6)
            return f"{start_week.strftime('%Y-%m-%d')} to {end_week.strftime('%Y-%m-%d')}"
        return "Custom Period Range"

    def _get_comparison_period(self, period: str) -> str:
        """Generates comparison period description."""
        if period == "last-week":
            today = datetime.date.today()
            current_start = today - datetime.timedelta(days=today.weekday() + 7)
            current_end = current_start + datetime.timedelta(days=6)
            previous_start = current_start - datetime.timedelta(days=7)
            previous_end = current_end - datetime.timedelta(days=7)

            return (
                f"{current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')} vs. "
                f"{previous_start.strftime('%Y-%m-%d')} to {previous_end.strftime('%Y-%m-%d')}"
            )
        return "Custom Comparison Period"
