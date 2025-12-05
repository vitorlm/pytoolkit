"""Member Productivity Service

This service calculates comprehensive productivity metrics for team members by combining:
1. Planning data from Excel (time allocations)
2. JIRA execution data (adherence, bug metrics, epic participation)
3. Changelog analysis (assignee + status tracking)
4. Collaboration metrics (comments, shared work)

Key Features:
- Analyzes 4 custom date fields for epic adherence
- Tracks bug work via changelog (assignee + status in WIP)
- Calculates epic participation via child issues
- Detects collaboration patterns
- Generates overall productivity scores

Follows PyToolkit patterns:
- Command-Service separation (this is the SERVICE layer)
- Uses singleton managers (LogManager, CacheManager)
- Reuses existing utilities (JiraAssistant, WorkflowConfigService)
- Comprehensive error handling and logging
"""

from datetime import date, datetime, timedelta
from typing import Any

from domains.syngenta.jira.workflow_config_service import WorkflowConfigService
from utils.cache_manager.cache_manager import CacheManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager

from ..core.member_productivity_metrics import (
    AdherenceSummary,
    AssignmentPeriod,
    BugMetrics,
    CollaborationMetrics,
    EpicAllocation,
    EpicParticipation,
    MemberProductivityMetrics,
    SpilloverSummary,
    TimeAllocation,
)
from .absence_impact_service import AbsenceImpactService


class MemberProductivityService:
    """Service for calculating comprehensive member productivity metrics.

    Combines planning data with JIRA execution metrics to provide:
    - Time allocation analysis
    - Epic adherence metrics (using 4 custom dates)
    - Bug work analysis (changelog-based)
    - Epic participation (via child issues)
    - Collaboration metrics
    - Overall productivity scoring
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
            cache_expiration (int): Cache expiration in minutes (default: 60)
        """
        self.logger = LogManager.get_instance().get_logger("MemberProductivityService")
        self.cache = CacheManager.get_instance()
        self.cache_expiration = cache_expiration

        # Initialize dependencies
        self.jira_assistant = JiraAssistant(cache_expiration=cache_expiration)
        self.workflow_service = WorkflowConfigService(cache_expiration=cache_expiration)
        self.absence_service = AbsenceImpactService()

        self.logger.info("MemberProductivityService initialized")

    def calculate_member_metrics(
        self,
        member_name: str,
        cycle: str,
        cycle_start_date: date,
        cycle_end_date: date,
        planning_allocations: list[dict[str, Any]],
        project_key: str = "CWS",
    ) -> MemberProductivityMetrics:
        """Calculate comprehensive productivity metrics for a member.

        Main orchestration method that coordinates all sub-analyses.

        Args:
            member_name (str): Member's name (must match JIRA display name)
            cycle (str): Cycle identifier (e.g., "Q4-C2", "Q4C2")
            cycle_start_date (date): Cycle start date
            cycle_end_date (date): Cycle end date
            planning_allocations (List[Dict]): Planning data from Excel
                Expected structure:
                [
                    {
                        "epic_key": "CWS-123",
                        "allocated_days": 5.0,
                        "type": "epic"  # or "bug"
                    },
                    ...
                ]
            project_key (str): JIRA project key (default: "CWS")

        Returns:
            MemberProductivityMetrics: Comprehensive productivity assessment
        """
        self.logger.info(f"Calculating productivity metrics for {member_name} in {cycle}")

        try:
            # 1. Calculate time allocation from planning
            time_allocation = self._calculate_time_allocation(planning_allocations)
            self.logger.info(f"Time allocation calculated: {time_allocation.total_allocated_days} days")

            # 2. Extract bug keys from planning
            bug_keys = [
                item["epic_key"] for item in planning_allocations if item.get("type") == "bug" and "epic_key" in item
            ]

            # 3. Enrich epics with JIRA adherence data
            epics = []
            for item in planning_allocations:
                if item.get("type") == "epic" and "epic_key" in item:
                    epic = self._enrich_epic_adherence(item["epic_key"], item["allocated_days"], project_key)
                    if epic:
                        epics.append(epic)

            self.logger.info(f"Enriched {len(epics)} epics with adherence data")

            # 4. Calculate adherence summary
            adherence_summary = self._calculate_adherence_summary(epics)

            # 5. Calculate spillover summary
            spillover_summary = self._calculate_spillover_summary(epics, cycle_end_date)

            # 6. Calculate bug metrics
            bug_metrics = self._calculate_bug_metrics(
                member_name, bug_keys, cycle_start_date, cycle_end_date, project_key
            )
            self.logger.info(f"Bug metrics calculated: {bug_metrics.total_bugs_worked} bugs")

            # 7. Calculate epic participation via child issues
            epic_participations = []
            for item in planning_allocations:
                if item.get("type") == "epic" and "epic_key" in item:
                    participation = self._calculate_epic_participation(
                        item["epic_key"],
                        member_name,
                        item["allocated_days"],
                        cycle_start_date,
                        cycle_end_date,
                        project_key,
                    )
                    if participation:
                        epic_participations.append(participation)

            self.logger.info(f"Calculated participation in {len(epic_participations)} epics")

            # 8. Calculate collaboration metrics
            collaboration_metrics = self._calculate_collaboration_metrics(
                member_name, epics, epic_participations, project_key
            )

            # 9. Build comprehensive metrics object
            metrics = MemberProductivityMetrics(
                member_name=member_name,
                cycle=cycle,
                cycle_start_date=cycle_start_date,
                cycle_end_date=cycle_end_date,
                time_allocation=time_allocation,
                adherence_summary=adherence_summary,
                spillover_summary=spillover_summary,
                bug_metrics=bug_metrics,
                collaboration_metrics=collaboration_metrics,
                epics=epics,
                epic_participations=epic_participations,
            )

            # 10. Calculate overall score and categorize
            metrics.calculate_overall_score()
            metrics.categorize_performance()

            self.logger.info(
                f"Metrics calculated successfully: Score={metrics.overall_score}, "
                f"Category={metrics.performance_category}"
            )

            return metrics

        except Exception as e:
            self.logger.error(f"Failed to calculate member metrics: {e}", exc_info=True)
            raise

    def _calculate_time_allocation(self, planning_allocations: list[dict[str, Any]]) -> TimeAllocation:
        """Calculate time allocation from planning Excel data.

        Args:
            planning_allocations (List[Dict]): Planning data

        Returns:
            TimeAllocation: Time distribution metrics
        """
        epic_days = 0.0
        bug_days = 0.0
        other_days = 0.0
        epic_count = 0
        bug_count = 0

        for item in planning_allocations:
            allocated = item.get("allocated_days", 0.0)
            item_type = item.get("type", "other").lower()

            if item_type == "epic":
                epic_days += allocated
                epic_count += 1
            elif item_type == "bug":
                bug_days += allocated
                bug_count += 1
            else:
                other_days += allocated

        total_allocated = epic_days + bug_days + other_days

        return TimeAllocation(
            total_allocated_days=total_allocated,
            epic_days=epic_days,
            bug_days=bug_days,
            other_days=other_days,
            epic_count=epic_count,
            bug_count=bug_count,
        )

    def _enrich_epic_adherence(self, epic_key: str, allocated_days: float, project_key: str) -> EpicAllocation | None:
        """Enrich epic with JIRA adherence data using 4 custom dates + due_date.

        Custom fields:
        - planned_start: customfield_10357
        - planned_end: customfield_10487
        - actual_start: customfield_10015
        - actual_end: customfield_10233

        Due date changelog: Track changes to duedate field

        Args:
            epic_key (str): Epic key
            allocated_days (float): Days allocated in planning
            project_key (str): JIRA project key

        Returns:
            Optional[EpicAllocation]: Epic with adherence metrics, or None if fetch fails
        """
        try:
            # Fetch epic with changelog
            cache_key = f"epic_adherence_{epic_key}"
            cached_epic = self.cache.load(cache_key, expiration_minutes=self.cache_expiration)

            if cached_epic:
                self.logger.debug(f"Using cached epic data for {epic_key}")
                epic_data = cached_epic
            else:
                self.logger.debug(f"Fetching epic {epic_key} from JIRA")
                epic_data = self.jira_assistant.fetch_issues_by_keys([epic_key], expand_changelog=True)
                if not epic_data or len(epic_data) == 0:
                    self.logger.warning(f"Epic {epic_key} not found")
                    return None

                epic_data = epic_data[0]
                self.cache.save(cache_key, epic_data)

            fields = epic_data.get("fields", {})

            # Extract 4 custom dates
            planned_start_str = fields.get(self.CUSTOM_FIELDS["planned_start"])
            planned_end_str = fields.get(self.CUSTOM_FIELDS["planned_end"])
            actual_start_str = fields.get(self.CUSTOM_FIELDS["actual_start"])
            actual_end_str = fields.get(self.CUSTOM_FIELDS["actual_end"])

            planned_start = self._parse_date(planned_start_str)
            planned_end = self._parse_date(planned_end_str)
            actual_start = self._parse_date(actual_start_str)
            actual_end = self._parse_date(actual_end_str)

            # Extract due date and track changes
            current_due_date_str = fields.get("duedate")
            current_due_date = self._parse_date(current_due_date_str)

            # Track due date changes in changelog
            initial_due_date, due_date_changes = self._track_due_date_changes(epic_data.get("changelog", {}))

            # Calculate adherence: planned_end vs actual_end
            is_adherent = None
            days_difference = None

            if planned_end and actual_end:
                is_adherent = actual_end <= planned_end
                days_difference = (actual_end - planned_end).days

            # Extract metadata
            summary = fields.get("summary", "")
            status = fields.get("status", {}).get("name", "")
            assignee_obj = fields.get("assignee")
            assignee = assignee_obj.get("displayName", "") if assignee_obj else ""

            return EpicAllocation(
                epic_key=epic_key,
                epic_summary=summary,
                allocated_days=allocated_days,
                planned_start_date=planned_start,
                planned_end_date=planned_end,
                actual_start_date=actual_start,
                actual_end_date=actual_end,
                initial_due_date=initial_due_date,
                current_due_date=current_due_date,
                due_date_changes=due_date_changes,
                is_adherent=is_adherent,
                days_difference=days_difference,
                status=status,
                assignee=assignee,
            )

        except Exception as e:
            self.logger.error(f"Failed to enrich epic {epic_key}: {e}", exc_info=True)
            return None

    def _track_due_date_changes(self, changelog: dict) -> tuple[date | None, int]:
        """Track due date changes in changelog.

        Args:
            changelog (Dict): JIRA changelog

        Returns:
            Tuple[Optional[date], int]: (initial_due_date, number_of_changes)
        """
        initial_due_date = None
        changes = 0

        histories = changelog.get("histories", [])

        # Sort by created date to get chronological order
        sorted_histories = sorted(histories, key=lambda h: h.get("created", ""))

        for history in sorted_histories:
            items = history.get("items", [])
            for item in items:
                if item.get("field") == "duedate":
                    changes += 1
                    # First change has the initial value in 'from'
                    if changes == 1:
                        from_date_str = item.get("fromString")
                        if from_date_str:
                            initial_due_date = self._parse_date(from_date_str)

        return initial_due_date, changes

    def _calculate_adherence_summary(self, epics: list[EpicAllocation]) -> AdherenceSummary:
        """Calculate adherence summary across all epics.

        Args:
            epics (List[EpicAllocation]): Epics with adherence data

        Returns:
            AdherenceSummary: Aggregated adherence metrics
        """
        total_epics = len(epics)
        adherent_epics = 0
        non_adherent_epics = 0

        days_differences = []
        max_delay = 0
        max_early = 0

        for epic in epics:
            if epic.is_adherent is not None:
                if epic.is_adherent:
                    adherent_epics += 1
                else:
                    non_adherent_epics += 1

                if epic.days_difference is not None:
                    days_differences.append(epic.days_difference)

                    if epic.days_difference > 0:
                        max_delay = max(max_delay, epic.days_difference)
                    elif epic.days_difference < 0:
                        max_early = max(max_early, abs(epic.days_difference))

        avg_days_difference = sum(days_differences) / len(days_differences) if days_differences else 0.0

        return AdherenceSummary(
            total_epics=total_epics,
            adherent_epics=adherent_epics,
            non_adherent_epics=non_adherent_epics,
            avg_days_difference=round(avg_days_difference, 2),
            max_delay_days=max_delay,
            max_early_days=max_early,
        )

    def _calculate_spillover_summary(self, epics: list[EpicAllocation], cycle_end_date: date) -> SpilloverSummary:
        """Calculate spillover summary (epics extending beyond cycle).

        Spillover occurs when:
        - planned_end_date <= cycle_end_date (should finish in cycle)
        - actual_end_date > cycle_end_date (actually finished after cycle)

        Args:
            epics (List[EpicAllocation]): Epics with adherence data
            cycle_end_date (date): Cycle end date

        Returns:
            SpilloverSummary: Spillover analysis
        """
        spillover_epics = []
        spillover_days_list = []

        for epic in epics:
            if (
                epic.planned_end_date
                and epic.actual_end_date
                and epic.planned_end_date <= cycle_end_date
                and epic.actual_end_date > cycle_end_date
            ):
                spillover_epics.append(epic.epic_key)
                spillover_days = (epic.actual_end_date - cycle_end_date).days
                spillover_days_list.append(spillover_days)

        avg_spillover_days = sum(spillover_days_list) / len(spillover_days_list) if spillover_days_list else 0.0

        return SpilloverSummary(
            total_spillovers=len(spillover_epics),
            spillover_epics=spillover_epics,
            avg_spillover_days=round(avg_spillover_days, 2),
        )

    def _calculate_bug_metrics(
        self,
        member_name: str,
        bug_keys: list[str],
        cycle_start_date: date,
        cycle_end_date: date,
        project_key: str,
    ) -> BugMetrics:
        """Calculate bug metrics using changelog analysis.

        Pattern from cycle_time_service.py (lines 450-600):
        - Iterate changelog for BOTH assignee and status changes
        - Calculate assignment_periods: when member was assigned AND in WIP status
        - Handle handoffs: multiple people working on same bug
        - Use WorkflowConfigService to validate WIP statuses

        Args:
            member_name (str): Member name to match in assignee
            bug_keys (List[str]): Bug keys to analyze
            cycle_start_date (date): Cycle start date
            cycle_end_date (date): Cycle end date
            project_key (str): JIRA project key

        Returns:
            BugMetrics: Bug work analysis
        """
        if not bug_keys:
            self.logger.debug("No bugs to analyze")
            return BugMetrics()

        try:
            # Fetch bugs with changelog
            self.logger.debug(f"Fetching {len(bug_keys)} bugs for {member_name}")
            bugs = self.jira_assistant.fetch_issues_by_keys(bug_keys, expand_changelog=True)

            if not bugs:
                self.logger.warning(f"No bugs found for keys: {bug_keys}")
                return BugMetrics()

            total_bugs_worked = 0
            total_time_hours = 0.0
            bugs_with_handoff = 0
            bug_details = []

            for bug in bugs:
                bug_key = bug.get("key", "")
                self.logger.debug(f"Analyzing bug {bug_key}")

                # Calculate assignment periods for this member
                assignment_periods = self._calculate_assignment_periods(bug, member_name, project_key)

                if assignment_periods:
                    total_bugs_worked += 1

                    # Calculate total time
                    bug_time_hours = sum(period.duration_hours for period in assignment_periods)
                    total_time_hours += bug_time_hours

                    # Check for handoff (multiple assignees)
                    all_assignees = self._get_all_assignees(bug)
                    if len(all_assignees) > 1:
                        bugs_with_handoff += 1

                    bug_details.append(
                        {
                            "key": bug_key,
                            "time_hours": round(bug_time_hours, 2),
                            "time_days": round(bug_time_hours / 8, 2),
                            "assignment_periods": len(assignment_periods),
                            "handoff": len(all_assignees) > 1,
                            "assignees": list(all_assignees),
                        }
                    )

            total_time_days = total_time_hours / 8
            avg_time_per_bug_hours = total_time_hours / total_bugs_worked if total_bugs_worked > 0 else 0.0

            self.logger.info(
                f"Bug metrics: {total_bugs_worked} bugs, {total_time_hours:.2f} hours, {bugs_with_handoff} with handoff"
            )

            return BugMetrics(
                total_bugs_worked=total_bugs_worked,
                total_time_hours=round(total_time_hours, 2),
                total_time_days=round(total_time_days, 2),
                bugs_with_handoff=bugs_with_handoff,
                avg_time_per_bug_hours=round(avg_time_per_bug_hours, 2),
                bug_details=bug_details,
            )

        except Exception as e:
            self.logger.error(f"Failed to calculate bug metrics: {e}", exc_info=True)
            return BugMetrics()

    def _calculate_assignment_periods(self, issue: dict, member_name: str, project_key: str) -> list[AssignmentPeriod]:
        """Calculate assignment periods for a member on an issue.

        Assignment period = time when:
        - Issue is assigned to member (assignee == member_name)
        - AND issue is in WIP status

        Pattern from cycle_time_service.py:
        - Iterate changelog chronologically
        - Track both assignee and status changes
        - Calculate periods when both conditions are met

        Args:
            issue (Dict): JIRA issue with changelog
            member_name (str): Member name to match
            project_key (str): Project key for workflow validation

        Returns:
            List[AssignmentPeriod]: Assignment periods
        """
        changelog = issue.get("changelog", {})
        histories = changelog.get("histories", [])

        # Sort chronologically
        sorted_histories = sorted(histories, key=lambda h: h.get("created", ""))

        # Track state
        current_assignee = None
        current_status = None
        current_period_start = None
        assignment_periods = []

        for history in sorted_histories:
            timestamp_str = history.get("created")
            timestamp = self._parse_datetime(timestamp_str)

            items = history.get("items", [])

            for item in items:
                field = item.get("field")

                # Track assignee changes
                if field == "assignee":
                    to_assignee_str = item.get("toString", "")

                    # Check if assigned to our member
                    if current_assignee == member_name and to_assignee_str != member_name:
                        # Member unassigned - close current period if open
                        if current_period_start and current_status:
                            is_wip = self.workflow_service.is_wip_status(project_key, current_status)
                            if is_wip:
                                assignment_periods.append(
                                    AssignmentPeriod(
                                        start=current_period_start,
                                        end=timestamp,
                                        status=current_status,
                                    )
                                )
                                current_period_start = None

                    current_assignee = to_assignee_str

                    # Check if newly assigned to member
                    if current_assignee == member_name and current_status:
                        is_wip = self.workflow_service.is_wip_status(project_key, current_status)
                        if is_wip and not current_period_start:
                            current_period_start = timestamp

                # Track status changes
                elif field == "status":
                    to_status = item.get("toString", "")
                    from_status = item.get("fromString", "")

                    # Check if leaving WIP
                    if from_status:
                        was_wip = self.workflow_service.is_wip_status(project_key, from_status)
                        is_wip = self.workflow_service.is_wip_status(project_key, to_status)

                        if was_wip and not is_wip and current_assignee == member_name:
                            # Leaving WIP - close current period
                            if current_period_start:
                                assignment_periods.append(
                                    AssignmentPeriod(
                                        start=current_period_start,
                                        end=timestamp,
                                        status=from_status,
                                    )
                                )
                                current_period_start = None

                        elif not was_wip and is_wip and current_assignee == member_name:
                            # Entering WIP - start new period
                            if not current_period_start:
                                current_period_start = timestamp

                    current_status = to_status

        # Close any open period (shouldn't happen for resolved issues)
        if current_period_start and current_status:
            is_wip = self.workflow_service.is_wip_status(project_key, current_status)
            if is_wip and current_assignee == member_name:
                # Use current time as end
                assignment_periods.append(
                    AssignmentPeriod(
                        start=current_period_start,
                        end=datetime.now(),
                        status=current_status,
                    )
                )

        return assignment_periods

    def _get_all_assignees(self, issue: dict) -> set:
        """Get all unique assignees from issue changelog.

        Args:
            issue (Dict): JIRA issue with changelog

        Returns:
            set: Set of unique assignee names
        """
        assignees = set()

        # Current assignee
        current_assignee_obj = issue.get("fields", {}).get("assignee")
        if current_assignee_obj:
            assignees.add(current_assignee_obj.get("displayName", ""))

        # Historical assignees from changelog
        changelog = issue.get("changelog", {})
        histories = changelog.get("histories", [])

        for history in histories:
            items = history.get("items", [])
            for item in items:
                if item.get("field") == "assignee":
                    from_assignee = item.get("fromString", "")
                    to_assignee = item.get("toString", "")
                    if from_assignee:
                        assignees.add(from_assignee)
                    if to_assignee:
                        assignees.add(to_assignee)

        return assignees

    def _calculate_epic_participation(
        self,
        epic_key: str,
        member_name: str,
        planned_days: float,
        cycle_start_date: date,
        cycle_end_date: date,
        project_key: str,
    ) -> EpicParticipation | None:
        """Calculate member's participation in epic via child issues.

        CRITICAL: Don't analyze epic directly - analyze child issues!

        Steps:
        1. Fetch child issues (subtasks, stories with Parent Link = epic)
        2. For each child issue: apply same logic as bugs (changelog analysis)
        3. Aggregate all assignment_periods across child issues
        4. Calculate total_days_in_epic vs planning_days

        Args:
            epic_key (str): Epic key
            member_name (str): Member name
            planned_days (float): Days allocated in planning
            cycle_start_date (date): Cycle start date
            cycle_end_date (date): Cycle end date
            project_key (str): JIRA project key

        Returns:
            Optional[EpicParticipation]: Epic participation metrics
        """
        try:
            # Fetch child issues via JQL
            # NOTE: Using both subtasksOf and Parent Link to catch all children
            jql = f"""
                (issueFunction in subtasksOf("{epic_key}") OR "Parent Link" = {epic_key})
                AND type != Epic
            """

            self.logger.debug(f"Fetching child issues for epic {epic_key}")

            cache_key = f"epic_children_{epic_key}"
            cached_children = self.cache.load(cache_key, expiration_minutes=self.cache_expiration)

            if cached_children:
                child_issues = cached_children
            else:
                child_issues = self.jira_assistant.search_issues(
                    jql=jql,
                    expand_changelog=True,
                    max_results=500,  # Adjust if epics have many children
                )
                self.cache.save(cache_key, child_issues)

            if not child_issues:
                self.logger.warning(f"No child issues found for epic {epic_key}")
                return EpicParticipation(epic_key=epic_key, planned_days=planned_days)

            self.logger.debug(f"Found {len(child_issues)} child issues for {epic_key}")

            # Analyze each child issue
            all_assignment_periods = []
            child_issues_worked = 0

            for child in child_issues:
                child_key = child.get("key", "")

                # Calculate assignment periods for this child
                periods = self._calculate_assignment_periods(child, member_name, project_key)

                if periods:
                    child_issues_worked += 1
                    all_assignment_periods.extend(periods)
                    self.logger.debug(
                        f"Child {child_key}: {len(periods)} assignment periods, "
                        f"{sum(p.duration_hours for p in periods):.2f} hours"
                    )

            # Calculate total days
            total_hours = sum(period.duration_hours for period in all_assignment_periods)
            total_days = total_hours / 8

            self.logger.info(
                f"Epic {epic_key} participation: {total_days:.2f} days across {child_issues_worked} child issues"
            )

            return EpicParticipation(
                epic_key=epic_key,
                total_child_issues=len(child_issues),
                child_issues_worked=child_issues_worked,
                total_days_in_epic=round(total_days, 2),
                assignment_periods=all_assignment_periods,
                planned_days=planned_days,
            )

        except Exception as e:
            self.logger.error(
                f"Failed to calculate epic participation for {epic_key}: {e}",
                exc_info=True,
            )
            return None

    def _calculate_collaboration_metrics(
        self,
        member_name: str,
        epics: list[EpicAllocation],
        epic_participations: list[EpicParticipation],
        project_key: str,
    ) -> CollaborationMetrics:
        """Calculate collaboration metrics.

        Based on:
        - Changelog: assignees who worked on same issues
        - Comments: interactions on issues
        - Planning data: shared epic allocations

        Args:
            member_name (str): Member name
            epics (List[EpicAllocation]): Epics from planning
            epic_participations (List[EpicParticipation]): Epic participation data
            project_key (str): JIRA project key

        Returns:
            CollaborationMetrics: Collaboration analysis
        """
        unique_collaborators = set()
        issues_with_collaboration = 0
        total_issues_analyzed = 0
        comments_received = 0
        comments_given = 0

        try:
            # Analyze child issues from epic participations
            for participation in epic_participations:
                # Fetch child issues again (could optimize with caching)
                epic_key = participation.epic_key
                jql = f"""
                    (issueFunction in subtasksOf("{epic_key}")
                    OR "Parent Link" = {epic_key})
                    AND type != Epic
                """

                child_issues = self.jira_assistant.search_issues(
                    jql=jql,
                    expand_changelog=True,
                    fields=["comment", "assignee"],
                    max_results=500,
                )

                for child in child_issues:
                    total_issues_analyzed += 1

                    # Get all assignees
                    assignees = self._get_all_assignees(child)

                    # If more than one assignee, it's collaborative
                    if len(assignees) > 1:
                        issues_with_collaboration += 1

                        # Add collaborators (exclude self)
                        for assignee in assignees:
                            if assignee and assignee != member_name:
                                unique_collaborators.add(assignee)

                    # Analyze comments
                    comments = child.get("fields", {}).get("comment", {}).get("comments", [])
                    for comment in comments:
                        author_name = comment.get("author", {}).get("displayName", "")

                        # Count comments received (on issues assigned to member)
                        current_assignee = child.get("fields", {}).get("assignee", {})
                        if current_assignee and current_assignee.get("displayName") == member_name:
                            if author_name != member_name:
                                comments_received += 1

                        # Count comments given (by member on any issue)
                        if author_name == member_name:
                            comments_given += 1

            self.logger.info(
                f"Collaboration metrics: {len(unique_collaborators)} collaborators, "
                f"{issues_with_collaboration}/{total_issues_analyzed} collaborative issues"
            )

            return CollaborationMetrics(
                unique_collaborators=list(unique_collaborators),
                total_collaborators=len(unique_collaborators),
                issues_with_collaboration=issues_with_collaboration,
                total_issues_analyzed=total_issues_analyzed,
                comments_received=comments_received,
                comments_given=comments_given,
            )

        except Exception as e:
            self.logger.error(f"Failed to calculate collaboration metrics: {e}", exc_info=True)
            return CollaborationMetrics()

    # Utility methods

    def _parse_date(self, date_str: str | None) -> date | None:
        """Parse date string to date object.

        Handles multiple formats:
        - ISO 8601 with timezone: "2024-01-15T10:30:00.000+0000"
        - Simple date: "2024-01-15"

        Args:
            date_str (Optional[str]): Date string

        Returns:
            Optional[date]: Parsed date or None
        """
        if not date_str:
            return None

        try:
            # Handle ISO 8601 with timezone
            if "T" in date_str:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return dt.date()
            # Handle simple date format
            else:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None

    def _parse_datetime(self, datetime_str: str | None) -> datetime:
        """Parse datetime string to datetime object.

        Args:
            datetime_str (Optional[str]): Datetime string

        Returns:
            datetime: Parsed datetime
        """
        if not datetime_str:
            return datetime.now()

        try:
            return datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Failed to parse datetime '{datetime_str}': {e}")
            return datetime.now()

    def _calculate_business_days(self, start_date: date, end_date: date) -> int:
        """Calculate business days between two dates (excluding weekends).

        Args:
            start_date (date): Start date
            end_date (date): End date

        Returns:
            int: Number of business days
        """
        if start_date > end_date:
            return 0

        business_days = 0
        current_date = start_date

        while current_date <= end_date:
            # Check if weekday (Monday=0, Sunday=6)
            if current_date.weekday() < 5:
                business_days += 1
            current_date += timedelta(days=1)

        return business_days

    def calculate_availability_adjusted_metrics(
        self,
        base_metrics: MemberProductivityMetrics,
        absences: list[Any],
        normalization_method: str = "linear",
    ) -> dict[str, Any]:
        """Calculate availability-adjusted productivity metrics.

        Takes base productivity metrics and adjusts them based on member's
        actual availability during the period.

        Args:
            base_metrics: Base productivity metrics (unadjusted)
            absences: List of MemberAbsence objects for the period
            normalization_method: Adjustment method ("linear" or "scaled")

        Returns:
            Dict with adjusted metrics and availability info

        Example:
            >>> service = MemberProductivityService()
            >>> base_metrics = service.calculate_member_metrics(...)
            >>> adjusted = service.calculate_availability_adjusted_metrics(
            ...     base_metrics,
            ...     absence_list,
            ...     normalization_method="linear"
            ... )
        """
        self.logger.info(f"Calculating availability-adjusted metrics for {base_metrics.member}")

        # Calculate availability metrics
        availability = self.absence_service.calculate_availability(
            member_name=base_metrics.member,
            period_start=base_metrics.cycle_start_date,
            period_end=base_metrics.cycle_end_date,
            absences=absences,
            exclude_weekends=True,
        )

        # Adjust productivity scores
        adjusted_epic_productivity = self.absence_service.adjust_productivity_by_availability(
            base_metrics.overall_epic_productivity_score,
            availability.availability_percentage,
            normalization_method,
        )

        adjusted_bug_productivity = self.absence_service.adjust_productivity_by_availability(
            base_metrics.overall_bug_productivity_score,
            availability.availability_percentage,
            normalization_method,
        )

        adjusted_overall_score = self.absence_service.adjust_productivity_by_availability(
            base_metrics.overall_productivity_score,
            availability.availability_percentage,
            normalization_method,
        )

        self.logger.info(
            f"  Availability: {availability.availability_percentage:.1f}% ({availability.days_absent} days absent)"
        )
        self.logger.info(
            f"  Overall score: {base_metrics.overall_productivity_score:.2f} → {adjusted_overall_score:.2f}"
        )

        return {
            "member": base_metrics.member,
            "cycle": base_metrics.cycle,
            "availability_metrics": availability.model_dump(),
            "base_productivity": {
                "epic_score": base_metrics.overall_epic_productivity_score,
                "bug_score": base_metrics.overall_bug_productivity_score,
                "overall_score": base_metrics.overall_productivity_score,
            },
            "adjusted_productivity": {
                "epic_score": adjusted_epic_productivity,
                "bug_score": adjusted_bug_productivity,
                "overall_score": adjusted_overall_score,
            },
            "adjustment_method": normalization_method,
        }
