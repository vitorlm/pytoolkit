"""Epic Enrichment Service.

Service for enriching EPIC tasks with JIRA planning and execution data.

Fetches 4 custom fields from JIRA:
- customfield_10357: Planned Start Date
- customfield_10487: Planned End Date
- customfield_10015: Actual Start Date
- customfield_10233: Actual End Date

Calculates adherence metrics by comparing planned_end_date vs actual_end_date.
Follows PyToolkit's Command-Service separation pattern.
"""

from datetime import datetime

from domains.syngenta.team_assessment.core.member_productivity_metrics import (
    EpicAdherenceMetrics,
    EpicEnrichmentData,
    MemberEpicAdherence,
)
from utils.cache_manager.cache_manager import CacheManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager


class EpicEnrichmentService:
    """Service for enriching EPIC tasks with JIRA planning and execution data."""

    def __init__(self):
        self.jira_assistant = JiraAssistant()
        self.logger = LogManager.get_instance().get_logger("EpicEnrichmentService")
        self.cache = CacheManager.get_instance()

    def enrich_epics(self, epic_keys: list[str]) -> dict[str, EpicEnrichmentData]:
        """Fetch JIRA data for epic keys and return enrichment data.

        Args:
            epic_keys: List of JIRA epic keys (e.g., ['CWS-1234', 'CWS-5678'])

        Returns:
            Dict mapping epic_key to EpicEnrichmentData
        """
        if not epic_keys:
            self.logger.info("No epic keys provided for enrichment")
            return {}

        self.logger.info(f"Enriching {len(epic_keys)} epics with JIRA data")

        # Build JQL query using IN clause (batch up to 50 keys at a time)
        # If more than 50 keys, we'll need to batch them
        enrichment_data = {}

        # Process in batches of 50
        batch_size = 50
        for i in range(0, len(epic_keys), batch_size):
            batch = epic_keys[i : i + batch_size]
            jql_query = f"key IN ({','.join(batch)})"

            # Fields to fetch (including custom fields)
            fields = (
                "key,summary,status,duedate,resolutiondate,"
                "customfield_10357,customfield_10487,customfield_10015,customfield_10233"
            )

            try:
                # Fetch issues from JIRA (with automatic pagination and caching)
                issues = self.jira_assistant.fetch_issues(
                    jql_query=jql_query, fields=fields, max_results=100, expand_changelog=False
                )

                self.logger.info(f"Fetched {len(issues)} epics from JIRA for batch {i // batch_size + 1}")

                # Parse response into EpicEnrichmentData objects
                for issue in issues:
                    epic_data = self._parse_epic_data(issue)
                    enrichment_data[epic_data.epic_key] = epic_data

            except Exception as e:
                self.logger.error(f"Failed to fetch epics batch {i // batch_size + 1}: {e}", exc_info=True)

        return enrichment_data

    def _parse_epic_data(self, issue: dict) -> EpicEnrichmentData:
        """Parse JIRA issue response into EpicEnrichmentData."""
        fields = issue.get("fields", {})

        return EpicEnrichmentData(
            epic_key=issue.get("key", ""),
            summary=fields.get("summary"),
            status=fields.get("status", {}).get("name") if isinstance(fields.get("status"), dict) else None,
            planned_start_date=fields.get("customfield_10357"),  # May be None
            planned_end_date=fields.get("customfield_10487"),  # May be None
            actual_start_date=fields.get("customfield_10015"),  # May be None
            actual_end_date=fields.get("customfield_10233"),  # May be None
            due_date=fields.get("duedate"),
            resolution_date=fields.get("resolutiondate"),
        )

    def calculate_adherence(self, epic_data: EpicEnrichmentData) -> EpicAdherenceMetrics:
        """Calculate adherence metrics for an epic.

        Args:
            epic_data: Epic enrichment data from JIRA

        Returns:
            EpicAdherenceMetrics with calculated adherence status
        """
        # Determine actual end date (prefer actual_end_date, fallback to resolution_date)
        actual_end = epic_data.actual_end_date or epic_data.resolution_date
        planned_end = epic_data.planned_end_date

        # Cannot calculate adherence without planned_end_date
        if not planned_end:
            return EpicAdherenceMetrics(
                epic_key=epic_data.epic_key,
                summary=epic_data.summary,
                status=epic_data.status,
                planned_end_date=None,
                actual_end_date=actual_end,
                adherence_status="no_dates",
                days_difference=None,
            )

        # Epic not completed yet
        if not actual_end:
            return EpicAdherenceMetrics(
                epic_key=epic_data.epic_key,
                summary=epic_data.summary,
                status=epic_data.status,
                planned_end_date=planned_end,
                actual_end_date=None,
                adherence_status="in_progress",
                days_difference=None,
            )

        # Calculate days difference (negative = early, positive = late)
        try:
            planned_dt = self._parse_date(planned_end)
            actual_dt = self._parse_date(actual_end)
            days_difference = (actual_dt - planned_dt).days

            # Determine adherence status with ±1 day tolerance
            if -1 <= days_difference <= 1:
                adherence_status = "on_time"
            elif days_difference < -1:
                adherence_status = "early"
            else:  # days_difference > 1
                adherence_status = "late"

            return EpicAdherenceMetrics(
                epic_key=epic_data.epic_key,
                summary=epic_data.summary,
                status=epic_data.status,
                planned_end_date=planned_end,
                actual_end_date=actual_end,
                adherence_status=adherence_status,
                days_difference=days_difference,
            )

        except Exception as e:
            self.logger.warning(f"Failed to parse dates for epic {epic_data.epic_key}: {e}")
            return EpicAdherenceMetrics(
                epic_key=epic_data.epic_key,
                summary=epic_data.summary,
                status=epic_data.status,
                planned_end_date=planned_end,
                actual_end_date=actual_end,
                adherence_status="no_dates",
                days_difference=None,
            )

    def _parse_date(self, date_string: str) -> datetime:
        """Parse date string to datetime object (date only, no time)."""
        # JIRA dates are typically in YYYY-MM-DD format
        # Handle both YYYY-MM-DD and YYYY-MM-DDTHH:MM:SS formats
        return datetime.fromisoformat(date_string[:10])

    def aggregate_epic_adherence(self, epic_metrics: list[EpicAdherenceMetrics]) -> MemberEpicAdherence:
        """Aggregate epic adherence metrics.

        Args:
            epic_metrics: List of EpicAdherenceMetrics for a member

        Returns:
            MemberEpicAdherence with aggregated statistics
        """
        if not epic_metrics:
            return MemberEpicAdherence()

        # Count by status
        on_time = sum(1 for m in epic_metrics if m.adherence_status == "on_time")
        early = sum(1 for m in epic_metrics if m.adherence_status == "early")
        late = sum(1 for m in epic_metrics if m.adherence_status == "late")
        in_progress = sum(1 for m in epic_metrics if m.adherence_status == "in_progress")
        no_dates = sum(1 for m in epic_metrics if m.adherence_status == "no_dates")

        # Calculate adherence rate (exclude in_progress and no_dates)
        completed_with_dates = on_time + early + late
        adherence_rate = ((on_time + early) / completed_with_dates * 100) if completed_with_dates > 0 else 0.0

        return MemberEpicAdherence(
            total_epics=len(epic_metrics),
            on_time=on_time,
            early=early,
            late=late,
            in_progress=in_progress,
            no_dates=no_dates,
            adherence_rate=round(adherence_rate, 2),
            epics_details=epic_metrics,
        )
