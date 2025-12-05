"""Team Productivity Service - Team-level JIRA analysis for catalog team.

Created in Phase 3 to provide comprehensive team and squad-level productivity metrics.

This service aggregates individual member metrics into team-level insights including:
- Team velocity and throughput
- Epic delivery rates and adherence
- Bug resolution efficiency
- Squad comparison metrics
- Resource utilization
- Team health indicators
"""

from datetime import date, datetime
from typing import Any

from domains.syngenta.jira.workflow_config_service import WorkflowConfigService
from utils.cache_manager.cache_manager import CacheManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager

from ..core.assessment_report import EvaluationPeriod, TeamProductivityMetrics


class TeamProductivityService:
    """Service for calculating team-level productivity metrics from JIRA.

    Provides comprehensive team analysis including:
    - Epic delivery and adherence rates
    - Team velocity per cycle/quarter
    - Bug resolution metrics
    - Spillover tracking
    - Squad performance comparison
    - Resource utilization analysis
    """

    # Custom field IDs from cropwise_workflow.json
    CUSTOM_FIELDS = {
        "planned_start": "customfield_10357",
        "planned_end": "customfield_10487",
        "actual_start": "customfield_10015",
        "actual_end": "customfield_10233",
        "squad": "customfield_10265",
    }

    def __init__(self, cache_expiration: int = 60):
        """Initialize the service.

        Args:
            cache_expiration: Cache expiration in minutes (default: 60)
        """
        self.logger = LogManager.get_instance().get_logger("TeamProductivityService")
        self.cache = CacheManager.get_instance()
        self.cache_expiration = cache_expiration

        # Initialize dependencies
        self.jira_assistant = JiraAssistant(cache_expiration=cache_expiration)
        self.workflow_service = WorkflowConfigService(cache_expiration=cache_expiration)

        self.logger.info("TeamProductivityService initialized")

    def calculate_team_metrics(
        self,
        team_name: str,
        period_start: date,
        period_end: date,
        project_key: str = "CWS",
        squad_name: str | None = None,
    ) -> TeamProductivityMetrics:
        """Calculate comprehensive team productivity metrics for a period.

        Args:
            team_name: Team or squad name
            period_start: Period start date
            period_end: Period end date
            project_key: JIRA project key (default: CWS)
            squad_name: Optional squad filter

        Returns:
            TeamProductivityMetrics with comprehensive team analysis

        Example:
            >>> service = TeamProductivityService()
            >>> metrics = service.calculate_team_metrics(
            ...     "CWS Catalog",
            ...     date(2024, 10, 1),
            ...     date(2024, 12, 31),
            ...     squad_name="Catalog Squad"
            ... )
        """
        self.logger.info("=" * 80)
        self.logger.info(f"CALCULATING TEAM METRICS: {team_name}")
        self.logger.info(f"Period: {period_start} to {period_end}")
        self.logger.info(f"Project: {project_key}")
        if squad_name:
            self.logger.info(f"Squad Filter: {squad_name}")
        self.logger.info("=" * 80)

        try:
            # Create evaluation period
            evaluation_period = self._create_evaluation_period(period_start, period_end)

            # Fetch team epics
            self.logger.info("Step 1/5: Fetching team epics...")
            epics = self._fetch_team_epics(project_key, period_start, period_end, squad_name)
            self.logger.info(f"Found {len(epics)} epics")

            # Analyze epic delivery
            self.logger.info("Step 2/5: Analyzing epic delivery...")
            epic_metrics = self._analyze_epic_delivery(epics, period_start, period_end)

            # Analyze bug resolution
            self.logger.info("Step 3/5: Analyzing bug resolution...")
            bug_metrics = self._analyze_bug_resolution(project_key, period_start, period_end, squad_name)

            # Calculate team velocity
            self.logger.info("Step 4/5: Calculating team velocity...")
            velocity = self._calculate_team_velocity(epics)

            # Calculate resource utilization
            self.logger.info("Step 5/5: Calculating resource utilization...")
            utilization = self._calculate_resource_utilization(epics)

            # Build metrics object
            metrics = TeamProductivityMetrics(
                team_name=team_name,
                evaluation_period=evaluation_period,
                total_epics_planned=epic_metrics["planned"],
                total_epics_delivered=epic_metrics["delivered"],
                team_velocity=velocity,
                epic_adherence_rate=epic_metrics["adherence_rate"],
                bug_resolution_rate=bug_metrics["avg_resolution_days"],
                spillover_rate=epic_metrics["spillover_rate"],
                resource_utilization=utilization,
            )

            self.logger.info("=" * 80)
            self.logger.info("TEAM METRICS CALCULATED SUCCESSFULLY")
            self.logger.info(f"Epics Delivered: {metrics.total_epics_delivered}/{metrics.total_epics_planned}")
            self.logger.info(f"Adherence Rate: {metrics.epic_adherence_rate:.1f}%")
            self.logger.info(f"Team Velocity: {metrics.team_velocity:.1f} story points/cycle")
            self.logger.info(f"Spillover Rate: {metrics.spillover_rate:.1f}%")
            self.logger.info("=" * 80)

            return metrics

        except Exception as e:
            self.logger.error(f"Failed to calculate team metrics: {e}", exc_info=True)
            raise

    def _create_evaluation_period(self, period_start: date, period_end: date) -> EvaluationPeriod:
        """Create evaluation period from date range.

        Args:
            period_start: Period start date
            period_end: Period end date

        Returns:
            EvaluationPeriod instance
        """
        year = period_start.year

        # Determine if it's a quarter
        month_start = period_start.month
        month_end = period_end.month

        # Check for quarter patterns (Q1: Jan-Mar, Q2: Apr-Jun, etc.)
        if month_start in [1, 4, 7, 10] and (month_end - month_start) >= 2:
            quarter = (month_start - 1) // 3 + 1
            return EvaluationPeriod(year=year, quarter=quarter, period_label=f"{year}-Q{quarter}")
        elif month_start == month_end:
            # Single month
            return EvaluationPeriod(year=year, month=month_start, period_label=f"{year}-{month_start:02d}")
        else:
            # Date range
            return EvaluationPeriod(
                year=year,
                period_label=f"{year}-{period_start.strftime('%m%d')}-{period_end.strftime('%m%d')}",
            )

    def _fetch_team_epics(
        self,
        project_key: str,
        period_start: date,
        period_end: date,
        squad_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all epics for team during period.

        Args:
            project_key: JIRA project key
            period_start: Period start date
            period_end: Period end date
            squad_name: Optional squad filter

        Returns:
            List of epic issues with full details
        """
        # Build JQL query
        jql_parts = [
            f"project = {project_key}",
            "issuetype = Epic",
            f"created <= '{period_end.isoformat()}'",
        ]

        # Add squad filter if provided
        if squad_name:
            squad_field = self.CUSTOM_FIELDS["squad"]
            jql_parts.append(f'"{squad_field}" ~ "{squad_name}"')

        jql = " AND ".join(jql_parts)

        self.logger.debug(f"Epic JQL: {jql}")

        # Fetch epics with expanded changelog and fields
        epics = self.jira_assistant.search_issues(
            jql_query=jql,
            expand="changelog",
            fields=[
                "summary",
                "status",
                "created",
                "resolutiondate",
                self.CUSTOM_FIELDS["planned_start"],
                self.CUSTOM_FIELDS["planned_end"],
                self.CUSTOM_FIELDS["actual_start"],
                self.CUSTOM_FIELDS["actual_end"],
                self.CUSTOM_FIELDS["squad"],
            ],
        )

        return epics

    def _analyze_epic_delivery(
        self, epics: list[dict[str, Any]], period_start: date, period_end: date
    ) -> dict[str, Any]:
        """Analyze epic delivery metrics.

        Args:
            epics: List of epic issues
            period_start: Period start date
            period_end: Period end date

        Returns:
            Dict with epic delivery metrics
        """
        planned_count = 0
        delivered_count = 0
        on_time_count = 0
        spillover_count = 0

        for epic in epics:
            fields = epic.get("fields", {})

            # Get planned dates
            planned_end_str = fields.get(self.CUSTOM_FIELDS["planned_end"])
            actual_end_str = fields.get(self.CUSTOM_FIELDS["actual_end"])

            # Check if epic was planned for this period
            if planned_end_str:
                try:
                    planned_end = datetime.fromisoformat(planned_end_str.replace("Z", "+00:00")).date()

                    if period_start <= planned_end <= period_end:
                        planned_count += 1

                        # Check if delivered
                        status = fields.get("status", {}).get("name", "")
                        if status in ["Done", "Closed", "Resolved"]:
                            delivered_count += 1

                            # Check if on time
                            if actual_end_str:
                                actual_end = datetime.fromisoformat(actual_end_str.replace("Z", "+00:00")).date()
                                if actual_end <= planned_end:
                                    on_time_count += 1
                        # Not done but planned end date passed
                        elif planned_end < date.today():
                            spillover_count += 1

                except (ValueError, AttributeError) as e:
                    self.logger.warning(f"Failed to parse dates for epic {epic.get('key')}: {e}")
                    continue

        # Calculate rates
        adherence_rate = (on_time_count / planned_count * 100) if planned_count > 0 else 0.0
        spillover_rate = (spillover_count / planned_count * 100) if planned_count > 0 else 0.0

        return {
            "planned": planned_count,
            "delivered": delivered_count,
            "on_time": on_time_count,
            "spillover": spillover_count,
            "adherence_rate": adherence_rate,
            "spillover_rate": spillover_rate,
        }

    def _analyze_bug_resolution(
        self,
        project_key: str,
        period_start: date,
        period_end: date,
        squad_name: str | None = None,
    ) -> dict[str, Any]:
        """Analyze bug resolution metrics.

        Args:
            project_key: JIRA project key
            period_start: Period start date
            period_end: Period end date
            squad_name: Optional squad filter

        Returns:
            Dict with bug resolution metrics
        """
        # Build JQL for bugs resolved in period
        jql_parts = [
            f"project = {project_key}",
            "issuetype = Bug",
            f"resolutiondate >= '{period_start.isoformat()}'",
            f"resolutiondate <= '{period_end.isoformat()}'",
        ]

        if squad_name:
            squad_field = self.CUSTOM_FIELDS["squad"]
            jql_parts.append(f'"{squad_field}" ~ "{squad_name}"')

        jql = " AND ".join(jql_parts)

        # Fetch bugs
        bugs = self.jira_assistant.search_issues(
            jql_query=jql,
            fields=["created", "resolutiondate"],
        )

        if not bugs:
            return {
                "total_resolved": 0,
                "avg_resolution_days": 0.0,
            }

        # Calculate resolution times
        resolution_times = []
        for bug in bugs:
            fields = bug.get("fields", {})
            created_str = fields.get("created")
            resolved_str = fields.get("resolutiondate")

            if created_str and resolved_str:
                try:
                    created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                    resolved = datetime.fromisoformat(resolved_str.replace("Z", "+00:00"))
                    days = (resolved - created).days
                    resolution_times.append(days)
                except (ValueError, AttributeError):
                    continue

        avg_resolution_days = sum(resolution_times) / len(resolution_times) if resolution_times else 0.0

        return {
            "total_resolved": len(bugs),
            "avg_resolution_days": avg_resolution_days,
        }

    def _calculate_team_velocity(self, epics: list[dict[str, Any]]) -> float:
        """Calculate team velocity (story points per cycle).

        For now, returns number of completed epics as proxy for velocity.
        In future, can integrate story points if available.

        Args:
            epics: List of epic issues

        Returns:
            Team velocity metric
        """
        completed_count = 0

        for epic in epics:
            status = epic.get("fields", {}).get("status", {}).get("name", "")
            if status in ["Done", "Closed", "Resolved"]:
                completed_count += 1

        # Return as float for compatibility with metrics model
        return float(completed_count)

    def _calculate_resource_utilization(self, epics: list[dict[str, Any]]) -> float:
        """Calculate resource utilization percentage.

        Measures how efficiently team resources were used based on
        epic completion rate.

        Args:
            epics: List of epic issues

        Returns:
            Resource utilization percentage (0-100)
        """
        if not epics:
            return 0.0

        completed_count = 0
        total_count = len(epics)

        for epic in epics:
            status = epic.get("fields", {}).get("status", {}).get("name", "")
            if status in ["Done", "Closed", "Resolved"]:
                completed_count += 1

        utilization = (completed_count / total_count * 100) if total_count > 0 else 0.0
        return utilization

    def compare_squads(
        self,
        project_key: str,
        period_start: date,
        period_end: date,
        squad_names: list[str],
    ) -> dict[str, TeamProductivityMetrics]:
        """Compare productivity metrics across multiple squads.

        Args:
            project_key: JIRA project key
            period_start: Period start date
            period_end: Period end date
            squad_names: List of squad names to compare

        Returns:
            Dict mapping squad name to metrics

        Example:
            >>> service = TeamProductivityService()
            >>> comparison = service.compare_squads(
            ...     "CWS",
            ...     date(2024, 10, 1),
            ...     date(2024, 12, 31),
            ...     ["Catalog Squad", "Search Squad"]
            ... )
        """
        self.logger.info(f"Comparing {len(squad_names)} squads...")

        squad_metrics = {}

        for squad_name in squad_names:
            try:
                self.logger.info(f"Processing squad: {squad_name}")
                metrics = self.calculate_team_metrics(
                    team_name=squad_name,
                    period_start=period_start,
                    period_end=period_end,
                    project_key=project_key,
                    squad_name=squad_name,
                )
                squad_metrics[squad_name] = metrics
                self.logger.info(f"✓ {squad_name} metrics calculated")
            except Exception as e:
                self.logger.error(f"✗ Failed to process {squad_name}: {e}")
                continue

        self.logger.info(f"Squad comparison complete: {len(squad_metrics)} squads processed")
        return squad_metrics

    def calculate_team_health_score(self, metrics: TeamProductivityMetrics) -> dict[str, Any]:
        """Calculate team health indicators from productivity metrics.

        Args:
            metrics: Team productivity metrics

        Returns:
            Dict with health indicators and overall score
        """
        # Define health thresholds
        EXCELLENT_ADHERENCE = 80.0
        GOOD_ADHERENCE = 60.0
        MAX_SPILLOVER = 20.0
        MIN_UTILIZATION = 70.0

        # Calculate individual health scores (0-1)
        adherence_score = min(metrics.epic_adherence_rate / EXCELLENT_ADHERENCE, 1.0)
        spillover_score = max(1.0 - (metrics.spillover_rate / MAX_SPILLOVER), 0.0)
        utilization_score = min(metrics.resource_utilization / 100.0, 1.0)

        # Overall health score (weighted average)
        overall_score = adherence_score * 0.4 + spillover_score * 0.3 + utilization_score * 0.3

        # Determine health category
        if overall_score >= 0.8:
            health_category = "Excellent"
        elif overall_score >= 0.6:
            health_category = "Good"
        elif overall_score >= 0.4:
            health_category = "Fair"
        else:
            health_category = "Needs Improvement"

        return {
            "overall_score": overall_score,
            "health_category": health_category,
            "adherence_score": adherence_score,
            "spillover_score": spillover_score,
            "utilization_score": utilization_score,
            "indicators": {
                "delivery_predictability": adherence_score >= 0.6,
                "low_spillover": spillover_score >= 0.7,
                "efficient_utilization": utilization_score >= 0.7,
            },
        }
