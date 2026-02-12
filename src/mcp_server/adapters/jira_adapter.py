"""JIRA Adapter for PyToolkit MCP Integration.

This adapter reuses 100% of the existing PyToolkit JIRA services, providing
an MCP-compatible interface for JIRA operations.
"""

from datetime import datetime
from typing import Any

from domains.syngenta.jira.cycle_time_service import CycleTimeService

# Reusing existing PyToolkit JIRA services
from domains.syngenta.jira.epic_monitor_service import EpicMonitorService
from domains.syngenta.jira.issue_adherence_service import IssueAdherenceService
from domains.syngenta.jira.issue_resolution_time_service import (
    IssueResolutionTimeService,
)
from domains.syngenta.jira.issue_velocity_service import IssueVelocityService
from domains.syngenta.jira.open_issues_service import OpenIssuesService

from .base_adapter import BaseAdapter


class JiraAdapter(BaseAdapter):
    """JIRA adapter that reuses 100% of existing PyToolkit services.

    Services integrated:
    - EpicMonitorService: Epic monitoring with problem detection
    - CycleTimeService: Cycle time metrics (Started to Done)
    - IssueVelocityService: Creation vs resolution velocity
    - IssueAdherenceService: Due date adherence analysis
    - IssueResolutionTimeService: SLA and resolution time analysis
    """

    def __init__(self) -> None:
        super().__init__("JIRA")

        # Ensure JIRA-specific environment is loaded
        from utils.env_loader import ensure_jira_env_loaded

        ensure_jira_env_loaded()

        # Initialize services (lazy loading)
        self._services: dict[str, Any] = {}

    def initialize_service(self) -> dict[str, Any]:
        """Initialize all JIRA services from PyToolkit."""
        try:
            # EpicMonitorService requires squad parameter, using default
            # Individual instances will be created with specific squad when needed
            self._services = {
                "epic_monitor": None,  # Lazy initialization with squad parameter
                "cycle_time": CycleTimeService(),
                "velocity": IssueVelocityService(),
                "adherence": IssueAdherenceService(),
                "resolution_time": IssueResolutionTimeService(),
                "open_issues": OpenIssuesService(),
            }

            self.logger.info("All JIRA services initialized successfully")
            return self._services

        except Exception as e:
            self.logger.error(f"Failed to initialize JIRA services: {e}")
            raise

    @property
    def services(self):
        """Access to all JIRA services."""
        if not self._services:
            _ = self.service  # Trigger lazy loading
        return self._services

    def get_epic_monitoring_data(self, project_key: str, team: str | None = None) -> dict[str, Any]:
        """Get epic monitoring data with problem detection.

        Args:
            project_key: JIRA project key
            team: Team/squad name (default: "FarmOps")

        Returns:
            Epic monitoring data with problems identified
        """

        def _fetch_epic_data(**kwargs) -> dict[str, Any]:
            # Create EpicMonitorService with the specified squad/team
            squad_name = kwargs.get("team") or "FarmOps"
            epic_service = EpicMonitorService(squad=squad_name)

            # Get epics for the specified squad
            epics = epic_service.get_epics()

            # Analyze problems
            problematic_epics = epic_service.analyze_epic_problems(epics)

            return {
                "project_key": project_key,
                "squad": squad_name,
                "total_epics": len(epics),
                "problematic_epics_count": len(problematic_epics),
                "epics": [
                    {
                        "key": epic.key,
                        "summary": epic.summary,
                        "status": epic.status,
                        "assignee": epic.assignee_name,
                        "start_date": (epic.start_date.isoformat() if epic.start_date else None),
                        "due_date": (epic.due_date.isoformat() if epic.due_date else None),
                        "fix_version": epic.fix_version,
                        "problems": epic.problems,
                    }
                    for epic in epics
                ],
                "problematic_epics": [
                    {
                        "key": epic.key,
                        "summary": epic.summary,
                        "problems": epic.problems,
                    }
                    for epic in problematic_epics
                ],
                "generated_at": datetime.now().isoformat(),
            }

        return self.cached_operation(
            "epic_monitoring",
            _fetch_epic_data,
            expiration_minutes=30,  # Shorter cache for real-time monitoring
            project_key=project_key,
            team=team,
        )

    def get_cycle_time_analysis(
        self,
        project_key: str,
        time_period: str = "last-week",
        issue_types: list[str] | None = None,
        team: str | None = None,
    ) -> dict[str, Any]:
        """Get cycle time analysis (Started to Done).

        Args:
            project_key: JIRA project key
            time_period: Time period for analysis
            issue_types: list of issue types to analyze
            team: Team filter

        Returns:
            Cycle time analysis with metrics
        """
        if issue_types is None:
            issue_types = ["Bug", "Story"]

        def _fetch_cycle_time(**kwargs) -> dict[str, Any]:
            cycle_service = self.services["cycle_time"]

            # Adapt legacy single-team param to new multi-team API
            teams_arg = [kwargs["team"]] if kwargs.get("team") else None

            return cycle_service.analyze_cycle_time(
                project_key=kwargs["project_key"],
                time_period=kwargs["time_period"],
                issue_types=kwargs["issue_types"],
                teams=teams_arg,
                verbose=False,
            )

        return self.cached_operation(
            "cycle_time",
            _fetch_cycle_time,
            expiration_minutes=60,
            project_key=project_key,
            time_period=time_period,
            issue_types=issue_types,
            team=team,
        )

    def get_velocity_analysis(
        self,
        project_key: str,
        time_period: str = "last-6-months",
        issue_types: list[str] | None = None,
        aggregation: str = "monthly",
        team: str | None = None,
    ) -> dict[str, Any]:
        """Get issue velocity analysis (creation vs resolution).

        Args:
            project_key: JIRA project key
            time_period: Time period for analysis
            issue_types: list of issue types
            aggregation: Time aggregation (monthly/quarterly)
            team: Team filter

        Returns:
            Velocity analysis with trends
        """
        if issue_types is None:
            issue_types = ["Bug"]

        def _fetch_velocity(**kwargs) -> dict[str, Any]:
            velocity_service = self.services["velocity"]

            teams_arg = [kwargs["team"]] if kwargs.get("team") else None

            return velocity_service.analyze_issue_velocity(
                project_key=kwargs["project_key"],
                time_period=kwargs["time_period"],
                issue_types=kwargs["issue_types"],
                aggregation=kwargs["aggregation"],
                teams=teams_arg,
                include_summary=True,
                verbose=False,
            )

        return self.cached_operation(
            "velocity",
            _fetch_velocity,
            expiration_minutes=60,
            project_key=project_key,
            time_period=time_period,
            issue_types=issue_types,
            aggregation=aggregation,
            team=team,
        )

    def get_adherence_analysis(
        self,
        project_key: str,
        time_period: str = "last-month",
        issue_types: list[str] | None = None,
        team: str | None = None,
    ) -> dict[str, Any]:
        """Get due date adherence analysis.

        Args:
            project_key: JIRA project key
            time_period: Time period for analysis
            issue_types: list of issue types
            team: Team filter

        Returns:
            Adherence analysis with on-time completion rates
        """
        if issue_types is None:
            issue_types = ["Bug", "Story"]

        def _fetch_adherence(**kwargs) -> dict[str, Any]:
            adherence_service = self.services["adherence"]

            # Call with correct parameters based on the actual method signature
            teams_arg = [kwargs["team"]] if kwargs.get("team") else None

            return adherence_service.analyze_issue_adherence(
                project_key=kwargs["project_key"],
                time_period=kwargs["time_period"],
                issue_types=kwargs["issue_types"],
                teams=teams_arg,
                verbose=False,
            )

        return self.cached_operation(
            "adherence",
            _fetch_adherence,
            expiration_minutes=60,
            project_key=project_key,
            time_period=time_period,
            issue_types=issue_types,
            team=team,
        )

    def get_resolution_time_analysis(
        self,
        project_key: str,
        time_period: str = "last-3-months",
        issue_types: list[str] | None = None,
        squad: str | None = None,
    ) -> dict[str, Any]:
        """Get resolution time analysis with SLA metrics.

        Args:
            project_key: JIRA project key
            time_period: Time period for analysis
            issue_types: list of issue types
            squad: Squad filter (not team)

        Returns:
            Resolution time analysis with SLA recommendations
        """
        if issue_types is None:
            issue_types = ["Bug"]

        def _fetch_resolution_time(**kwargs) -> dict[str, Any]:
            resolution_service = self.services["resolution_time"]

            return resolution_service.analyze_resolution_time(
                project_key=kwargs["project_key"],
                time_period=kwargs["time_period"],
                issue_types=kwargs["issue_types"],
                squad=kwargs.get("squad"),
                verbose=False,
            )

        return self.cached_operation(
            "resolution_time",
            _fetch_resolution_time,
            expiration_minutes=120,  # Longer cache for historical analysis
            project_key=project_key,
            time_period=time_period,
            issue_types=issue_types,
            squad=squad,
        )

    def get_open_issues(
        self,
        project_key: str,
        issue_types: list[str] | None = None,
        team: str | None = None,
        status_categories: list[str] | None = None,
        priorities: list[str] | None = None,
    ) -> dict[str, Any]:
        """Get currently open issues with optional filtering.

        Args:
            project_key: JIRA project key
            issue_types: list of issue types to include
            team: Team filter
            status_categories: list of status categories to include
            priorities: list of priorities to filter (note: not currently supported by OpenIssuesService)

        Returns:
            Open issues data with breakdowns and metrics
        """
        if issue_types is None:
            issue_types = ["Bug", "Support", "Story", "Task"]

        if status_categories is None:
            status_categories = ["To Do", "In Progress"]

        def _fetch_open_issues(**kwargs) -> dict[str, Any]:
            open_issues_service = self.services["open_issues"]

            teams_arg = [kwargs["team"]] if kwargs.get("team") else None

            return open_issues_service.fetch_open_issues(
                project_key=kwargs["project_key"],
                issue_types=kwargs["issue_types"],
                teams=teams_arg,
                status_categories=kwargs["status_categories"],
                verbose=False,
                output_file=None,
            )

        return self.cached_operation(
            "open_issues",
            _fetch_open_issues,
            expiration_minutes=15,  # Shorter cache for current state
            project_key=project_key,
            issue_types=issue_types,
            team=team,
            status_categories=status_categories,
        )

    def get_comprehensive_dashboard(self, project_key: str, team: str | None = None) -> dict[str, Any]:
        """Get comprehensive dashboard with all key metrics.

        Args:
            project_key: JIRA project key
            team: Team filter

        Returns:
            Comprehensive dashboard with all metrics
        """
        try:
            self.logger.info(f"Generating comprehensive dashboard for {project_key}")

            # Collect all metrics in parallel (conceptually - Python is single-threaded)
            dashboard: dict[str, Any] = {
                "project_key": project_key,
                "team": team,
                "generated_at": datetime.now().isoformat(),
                "metrics": {},
            }

            # Epic monitoring (current state)
            try:
                dashboard["metrics"]["epic_monitoring"] = self.get_epic_monitoring_data(
                    project_key=project_key, team=team or "FarmOps"
                )
            except Exception as e:
                self.logger.warning(f"Epic monitoring failed: {e}")
                dashboard["metrics"]["epic_monitoring"] = {"error": str(e)}

            # Cycle time (last week)
            try:
                dashboard["metrics"]["cycle_time"] = self.get_cycle_time_analysis(
                    project_key=project_key, time_period="last-week", team=team
                )
            except Exception as e:
                self.logger.warning(f"Cycle time analysis failed: {e}")
                dashboard["metrics"]["cycle_time"] = {"error": str(e)}

            # Velocity (last 6 months)
            try:
                dashboard["metrics"]["velocity"] = self.get_velocity_analysis(
                    project_key=project_key, time_period="last-6-months", team=team
                )
            except Exception as e:
                self.logger.warning(f"Velocity analysis failed: {e}")
                dashboard["metrics"]["velocity"] = {"error": str(e)}

            # Adherence (last month)
            try:
                dashboard["metrics"]["adherence"] = self.get_adherence_analysis(
                    project_key=project_key, time_period="last-month", team=team
                )
            except Exception as e:
                self.logger.warning(f"Adherence analysis failed: {e}")
                dashboard["metrics"]["adherence"] = {"error": str(e)}

            self.logger.info("Comprehensive dashboard generated successfully")
            return dashboard

        except Exception as e:
            self.logger.error(f"Failed to generate comprehensive dashboard: {e}")
            raise

    def health_check(self) -> dict[str, Any]:
        """Enhanced health check for JIRA adapter."""
        base_health = super().health_check()

        try:
            # Test JIRA connectivity - create a temporary EpicMonitorService instance
            epic_service = EpicMonitorService(squad="FarmOps")

            # Try a simple JIRA operation
            test_result = epic_service.jira_assistant.fetch_project_issue_types("CWS")

            base_health.update(
                {
                    "jira_connectivity": "healthy",
                    "available_services": (list(self._services.keys()) if self._services else []),
                    "test_operation": "project_issue_types_fetch",
                    "test_result_count": len(test_result) if test_result else 0,
                }
            )

        except Exception as e:
            base_health.update({"jira_connectivity": "unhealthy", "connectivity_error": str(e)})

        return base_health
