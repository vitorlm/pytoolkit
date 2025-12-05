"""Team Assessment Service - Main orchestration layer for assessment generation.

This service follows PyToolkit's Command-Service separation pattern, handling all
business logic for generating team member assessments.
"""

import os
from datetime import datetime
from pathlib import Path

from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager

from ..assessment_generator import AssessmentGenerator
from .absence_impact_service import AbsenceImpactService
from .member_productivity_service import MemberProductivityService
from .team_productivity_service import TeamProductivityService
from .visualization_service import VisualizationService


class TeamAssessmentService:
    """Main service for orchestrating team assessment generation.

    This service coordinates the workflow for generating comprehensive annual
    assessments by integrating:
    - Competency evaluations (feedback)
    - Planning data (task allocations)
    - Health checks (optional)
    - Productivity metrics (optional JIRA integration)
    """

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("TeamAssessmentService")

    def generate_assessment(
        self,
        competency_matrix_file: str,
        feedback_folder: str,
        output: str,
        planning_folder: str | None = None,
        health_check_folder: str | None = None,
        ignored_members: str | None = None,
        enable_productivity_metrics: bool = False,
        cycle: str | None = None,
        cycle_start_date: str | None = None,
        cycle_end_date: str | None = None,
        project_key: str = "CWS",
    ) -> dict:
        """Generate comprehensive team assessment report.

        Args:
            competency_matrix_file: Path to competency matrix Excel file
            feedback_folder: Path to directory with feedback Excel files
            output: Path to save generated assessment report (JSON)
            planning_folder: Optional path to planning Excel files
            health_check_folder: Optional path to health check Excel files
            ignored_members: Optional path to file with members to ignore
            enable_productivity_metrics: Whether to calculate JIRA productivity metrics
            cycle: Cycle identifier (e.g., Q4-C2) for productivity metrics
            cycle_start_date: Cycle start date (YYYY-MM-DD) for productivity metrics
            cycle_end_date: Cycle end date (YYYY-MM-DD) for productivity metrics
            project_key: JIRA project key (default: CWS)

        Returns:
            Dict with assessment results and output paths

        Raises:
            ValueError: If required parameters are missing or invalid
            FileNotFoundError: If input folders are invalid or inaccessible
        """
        self.logger.info("=" * 80)
        self.logger.info("STARTING TEAM ASSESSMENT GENERATION")
        self.logger.info("=" * 80)
        self.logger.info(f"Competency Matrix: {competency_matrix_file}")
        self.logger.info(f"Feedback Folder: {feedback_folder}")
        self.logger.info(f"Planning Folder: {planning_folder}")
        self.logger.info(f"Health Check Folder: {health_check_folder}")
        self.logger.info(f"Output: {output}")
        self.logger.info(f"Productivity Metrics: {enable_productivity_metrics}")
        self.logger.info("=" * 80)

        try:
            # Step 1: Generate base assessment (competency + feedback + planning)
            self.logger.info("Step 1/2: Generating base assessment...")
            orchestrator = AssessmentGenerator(
                competency_matrix_file=competency_matrix_file,
                feedback_folder=feedback_folder,
                planning_file=planning_folder,
                output_path=output,
                ignored_member_list=ignored_members,
            )

            orchestrator.run()
            self.logger.info(f"Base assessment successfully generated: {output}")

            result = {
                "success": True,
                "output_file": output,
                "members_processed": len(orchestrator.members),
                "productivity_metrics_enabled": enable_productivity_metrics,
            }

            # Step 2: Generate productivity metrics (optional)
            if enable_productivity_metrics:
                self.logger.info("Step 2/2: Generating productivity metrics...")
                productivity_results = self._generate_productivity_metrics(
                    orchestrator=orchestrator,
                    cycle=cycle,
                    cycle_start_date=cycle_start_date,
                    cycle_end_date=cycle_end_date,
                    planning_folder=planning_folder,
                    project_key=project_key,
                    output=output,
                )
                result.update(productivity_results)
            else:
                self.logger.info("Step 2/2: Productivity metrics disabled, skipping...")

            self.logger.info("=" * 80)
            self.logger.info("TEAM ASSESSMENT GENERATION COMPLETE")
            self.logger.info(f"Members Processed: {result['members_processed']}")
            if enable_productivity_metrics:
                self.logger.info(
                    f"Productivity Metrics: {result.get('productivity_members_processed', 0)} succeeded, "
                    f"{result.get('productivity_members_failed', 0)} failed"
                )
            self.logger.info("=" * 80)

            return result

        except Exception as e:
            self.logger.error(f"Team assessment generation failed: {e}", exc_info=True)
            raise

    def _generate_productivity_metrics(
        self,
        orchestrator: AssessmentGenerator,
        cycle: str | None,
        cycle_start_date: str | None,
        cycle_end_date: str | None,
        planning_folder: str | None,
        project_key: str,
        output: str,
    ) -> dict:
        """Generate productivity metrics for all members.

        Args:
            orchestrator: AssessmentGenerator with member data
            cycle: Cycle identifier
            cycle_start_date: Cycle start date (YYYY-MM-DD)
            cycle_end_date: Cycle end date (YYYY-MM-DD)
            planning_folder: Path to planning folder
            project_key: JIRA project key
            output: Output path

        Returns:
            Dict with productivity metrics results

        Raises:
            ValueError: If required parameters are missing
        """
        self.logger.info("Validating productivity metrics parameters...")

        # Validate required arguments
        if not cycle:
            raise ValueError("--cycle is required when --enable-productivity-metrics is used")
        if not cycle_start_date:
            raise ValueError("--cycle-start-date is required when --enable-productivity-metrics is used")
        if not cycle_end_date:
            raise ValueError("--cycle-end-date is required when --enable-productivity-metrics is used")
        if not planning_folder:
            raise ValueError("--planningFolder is required when --enable-productivity-metrics is used")

        # Parse dates
        try:
            cycle_start = datetime.strptime(cycle_start_date, "%Y-%m-%d").date()
            cycle_end = datetime.strptime(cycle_end_date, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")

        self.logger.info(f"Cycle: {cycle}")
        self.logger.info(f"Date Range: {cycle_start} to {cycle_end}")
        self.logger.info(f"Project Key: {project_key}")

        # Initialize productivity service
        try:
            productivity_service = MemberProductivityService()
        except ImportError as e:
            self.logger.error(f"Failed to import MemberProductivityService: {e}")
            raise ValueError("MemberProductivityService not available. Ensure JIRA dependencies are installed.")

        # Determine output directory
        output_path = output
        if output_path.endswith(".json"):
            output_dir = output_path[:-5]  # Remove .json extension
        else:
            output_dir = output_path

        members_output_path = os.path.join(output_dir, "members")

        # Ensure members output directory exists
        if not os.path.exists(members_output_path):
            os.makedirs(members_output_path)

        # Process each member
        members_processed = 0
        members_failed = 0

        for member_name, member_obj in orchestrator.members.items():
            try:
                self.logger.info(f"Processing productivity metrics for {member_name}...")

                # Extract planning allocations from member tasks
                planning_allocations = self._extract_planning_allocations(member_obj, cycle)

                if not planning_allocations:
                    self.logger.warning(f"No planning allocations found for {member_name}, skipping")
                    continue

                self.logger.info(f"Found {len(planning_allocations)} planning allocations for {member_name}")

                # Calculate metrics
                productivity_metrics = productivity_service.calculate_member_metrics(
                    member_name=member_name,
                    cycle=cycle,
                    cycle_start_date=cycle_start,
                    cycle_end_date=cycle_end,
                    planning_allocations=planning_allocations,
                    project_key=project_key,
                )

                # Save to member folder
                member_folder = os.path.join(members_output_path, member_name)
                if not os.path.exists(member_folder):
                    os.makedirs(member_folder)

                output_file = os.path.join(member_folder, "productivity_metrics.json")
                JSONManager.write_json(productivity_metrics.model_dump(), output_file)

                self.logger.info(
                    f"✓ {member_name}: Score={productivity_metrics.overall_score:.2f}, "
                    f"Category={productivity_metrics.performance_category}"
                )

                members_processed += 1

            except Exception as e:
                self.logger.error(f"✗ Failed to process {member_name}: {e}", exc_info=True)
                members_failed += 1
                continue

        return {
            "productivity_members_processed": members_processed,
            "productivity_members_failed": members_failed,
            "productivity_output_path": members_output_path,
        }

    def _extract_planning_allocations(self, member_obj, cycle: str) -> list[dict]:
        """Extract planning allocations from member object.

        Args:
            member_obj: Member object with tasks
            cycle: Cycle identifier

        Returns:
            List of planning allocation dictionaries
        """
        planning_allocations = []

        # Extract from member tasks
        if hasattr(member_obj, "tasks") and member_obj.tasks:
            for task in member_obj.tasks:
                # Extract epic/bug key from task
                issue_key = None
                if hasattr(task, "code"):
                    issue_key = task.code.upper()
                elif hasattr(task, "jira"):
                    issue_key = task.jira

                if not issue_key:
                    continue

                # Extract allocated days
                allocated_days = 0.0
                if hasattr(task, "planned") and task.planned:
                    if hasattr(task.planned, "issue_total_days"):
                        allocated_days = task.planned.issue_total_days or 0.0

                # Determine type (epic or bug)
                issue_type = "epic"  # Default to epic
                if hasattr(task, "type"):
                    task_type_lower = task.type.lower()
                    if "bug" in task_type_lower:
                        issue_type = "bug"

                planning_allocations.append(
                    {
                        "epic_key": issue_key,  # Using epic_key for both epics and bugs
                        "allocated_days": allocated_days,
                        "type": issue_type,
                    }
                )

        return planning_allocations

    def calculate_team_productivity(
        self,
        team_name: str,
        period_start: str,
        period_end: str,
        project_key: str = "CWS",
        squad_name: str | None = None,
    ):
        """Calculate team-level productivity metrics from JIRA.

        Args:
            team_name: Team or squad name
            period_start: Period start date (YYYY-MM-DD)
            period_end: Period end date (YYYY-MM-DD)
            project_key: JIRA project key (default: CWS)
            squad_name: Optional squad filter

        Returns:
            Dict with team productivity metrics
        """
        from datetime import datetime

        self.logger.info("=" * 80)
        self.logger.info("CALCULATING TEAM PRODUCTIVITY METRICS")
        self.logger.info(f"Team: {team_name}")
        self.logger.info(f"Period: {period_start} to {period_end}")
        self.logger.info("=" * 80)

        try:
            # Parse dates
            start_date = datetime.strptime(period_start, "%Y-%m-%d").date()
            end_date = datetime.strptime(period_end, "%Y-%m-%d").date()

            # Calculate metrics using TeamProductivityService
            team_service = TeamProductivityService()
            metrics = team_service.calculate_team_metrics(
                team_name=team_name,
                period_start=start_date,
                period_end=end_date,
                project_key=project_key,
                squad_name=squad_name,
            )

            # Calculate health score
            health_score = team_service.calculate_team_health_score(metrics)

            self.logger.info("✓ Team productivity metrics calculated successfully")
            self.logger.info("=" * 80)

            return {
                "metrics": metrics.model_dump(),
                "health_score": health_score,
            }

        except Exception as e:
            self.logger.error(f"Failed to calculate team productivity: {e}", exc_info=True)
            raise

    def calculate_team_absence_impact(
        self,
        planning_folder: str,
        period_start: str,
        period_end: str,
        team_size: int,
    ) -> dict[str, any]:
        """Calculate team-level absence impact and capacity analysis.

        Args:
            planning_folder: Path to folder with planning Excel files
            period_start: Period start date (YYYY-MM-DD)
            period_end: Period end date (YYYY-MM-DD)
            team_size: Total team size

        Returns:
            Dict with team absence impact analysis
        """
        from datetime import datetime

        from ..processors.members_task_processor import MembersTaskProcessor

        self.logger.info("=" * 80)
        self.logger.info("CALCULATING TEAM ABSENCE IMPACT")
        self.logger.info(f"Planning folder: {planning_folder}")
        self.logger.info(f"Period: {period_start} to {period_end}")
        self.logger.info("=" * 80)

        try:
            # Parse dates
            start_date = datetime.strptime(period_start, "%Y-%m-%d").date()
            end_date = datetime.strptime(period_end, "%Y-%m-%d").date()

            # Extract absences from planning files
            absence_service = AbsenceImpactService()
            task_processor = MembersTaskProcessor()

            # Process all planning files in folder
            planning_path = Path(planning_folder)
            all_member_absences = {}

            for file_path in planning_path.glob("*.xlsx"):
                if file_path.name.startswith("~"):  # Skip temp files
                    continue

                self.logger.info(f"Processing: {file_path.name}")
                absences_map = task_processor.extract_absences(file_path)

                # Merge absences for each member
                for member, absences in absences_map.items():
                    if member not in all_member_absences:
                        all_member_absences[member] = []
                    all_member_absences[member].extend(absences)

            # Calculate availability for each member
            from ..core.assessment_report import MemberAbsence

            team_availability = {}

            for member, absence_dicts in all_member_absences.items():
                # Convert dicts to MemberAbsence objects
                member_absences = [
                    MemberAbsence(
                        member=abs_dict["member"],
                        absence_type=abs_dict.get("absence_type", "vacation"),
                        start_date=abs_dict["start_date"],
                        end_date=abs_dict["end_date"],
                        days_count=abs_dict["days_count"],
                        notes=abs_dict.get("notes"),
                    )
                    for abs_dict in absence_dicts
                ]

                availability = absence_service.calculate_availability(
                    member_name=member,
                    period_start=start_date,
                    period_end=end_date,
                    absences=member_absences,
                    exclude_weekends=True,
                )

                team_availability[member] = availability

            # Calculate team capacity impact
            capacity_impact = absence_service.calculate_team_capacity_impact(team_availability, team_size)

            # Analyze absence patterns for team
            all_absences = []
            for absence_dicts in all_member_absences.values():
                all_absences.extend(
                    [
                        MemberAbsence(
                            member=abs_dict["member"],
                            absence_type=abs_dict.get("absence_type", "vacation"),
                            start_date=abs_dict["start_date"],
                            end_date=abs_dict["end_date"],
                            days_count=abs_dict["days_count"],
                            notes=abs_dict.get("notes"),
                        )
                        for abs_dict in absence_dicts
                    ]
                )

            absence_patterns = absence_service.analyze_absence_patterns(all_absences)

            self.logger.info("✓ Team absence impact calculated successfully")
            self.logger.info("=" * 80)

            return {
                "team_availability": {member: avail.model_dump() for member, avail in team_availability.items()},
                "capacity_impact": capacity_impact,
                "absence_patterns": absence_patterns,
                "period": {"start": period_start, "end": period_end},
            }

        except Exception as e:
            self.logger.error(f"Failed to calculate team absence impact: {e}", exc_info=True)
            raise

    def generate_visualizations(
        self,
        output_folder: str,
        member_assessments: dict[str, any] | None = None,
        team_metrics: dict[str, any] | None = None,
        availability_data: dict[str, any] | None = None,
    ) -> dict[str, list[str]]:
        """Generate comprehensive visualizations for assessment results.

        Args:
            output_folder: Path to save visualizations
            member_assessments: Dict with member assessment data
            team_metrics: Dict with team productivity metrics
            availability_data: Dict with availability metrics

        Returns:
            Dict mapping visualization type to list of generated file paths
        """
        self.logger.info("=" * 80)
        self.logger.info("GENERATING VISUALIZATIONS")
        self.logger.info(f"Output folder: {output_folder}")
        self.logger.info("=" * 80)

        try:
            viz_service = VisualizationService(output_path=output_folder)
            generated_charts = {
                "temporal_trends": [],
                "member_comparisons": [],
                "team_comparisons": [],
                "dashboards": [],
                "availability_charts": [],
            }

            # 1. Generate temporal trends (if multi-year data available)
            if member_assessments and any(isinstance(v, list) for v in member_assessments.values()):
                self.logger.info("Generating temporal trend charts...")
                for member_name, assessments in member_assessments.items():
                    if isinstance(assessments, list) and len(assessments) > 1:
                        # Generate trend for overall score
                        chart_path = viz_service.plot_temporal_trend(member_name, assessments, "overall_score")
                        if chart_path:
                            generated_charts["temporal_trends"].append(chart_path)

            # 2. Generate member comparisons
            if member_assessments and isinstance(next(iter(member_assessments.values())), dict):
                self.logger.info("Generating member comparison charts...")

                # Extract assessments (handle both dict and list formats)
                assessment_dict = {}
                for member_name, data in member_assessments.items():
                    if isinstance(data, list):
                        assessment_dict[member_name] = data[0] if data else None
                    else:
                        assessment_dict[member_name] = data

                # Generate comparison for key metrics
                for metric in [
                    "overall_score",
                    "adherence_rate",
                    "collaboration_score",
                ]:
                    try:
                        chart_path = viz_service.plot_multi_member_comparison(assessment_dict, metric)
                        if chart_path:
                            generated_charts["member_comparisons"].append(chart_path)
                    except Exception as e:
                        self.logger.warning(f"Could not generate comparison for {metric}: {e}")

            # 3. Generate team comparisons
            if team_metrics:
                self.logger.info("Generating team comparison charts...")
                for metric in [
                    "epic_adherence_rate",
                    "team_velocity",
                    "spillover_rate",
                ]:
                    try:
                        chart_path = viz_service.plot_team_comparison(team_metrics, metric)
                        if chart_path:
                            generated_charts["team_comparisons"].append(chart_path)
                    except Exception as e:
                        self.logger.warning(f"Could not generate team comparison for {metric}: {e}")

            # 4. Generate productivity dashboards
            if member_assessments:
                self.logger.info("Generating productivity dashboards...")
                for member_name, data in list(member_assessments.items())[:5]:  # Limit to first 5
                    try:
                        # Extract productivity metrics
                        assessment = data[0] if isinstance(data, list) else data
                        if isinstance(assessment, dict):
                            productivity_metrics = assessment.get("productivity_metrics", {})

                            # Get availability if present
                            availability = None
                            if availability_data and member_name in availability_data:
                                from ..core.assessment_report import AvailabilityMetrics

                                avail_dict = availability_data[member_name]
                                if isinstance(avail_dict, dict):
                                    availability = AvailabilityMetrics(**avail_dict)

                            chart_path = viz_service.plot_productivity_dashboard(
                                member_name, productivity_metrics, availability
                            )
                            if chart_path:
                                generated_charts["dashboards"].append(chart_path)
                    except Exception as e:
                        self.logger.warning(f"Could not generate dashboard for {member_name}: {e}")

            # 5. Generate availability impact charts
            if availability_data:
                self.logger.info("Generating availability impact charts...")
                try:
                    from ..core.assessment_report import AvailabilityMetrics

                    # Convert dict to AvailabilityMetrics objects
                    availability_metrics = {}
                    for member, data in availability_data.items():
                        if isinstance(data, dict):
                            availability_metrics[member] = AvailabilityMetrics(**data)

                    chart_path = viz_service.plot_availability_impact(availability_metrics)
                    if chart_path:
                        generated_charts["availability_charts"].append(chart_path)
                except Exception as e:
                    self.logger.warning(f"Could not generate availability chart: {e}")

            # Summary
            total_charts = sum(len(charts) for charts in generated_charts.values())
            self.logger.info("=" * 80)
            self.logger.info(f"✓ Generated {total_charts} visualizations")
            self.logger.info(f"  Temporal trends: {len(generated_charts['temporal_trends'])}")
            self.logger.info(f"  Member comparisons: {len(generated_charts['member_comparisons'])}")
            self.logger.info(f"  Team comparisons: {len(generated_charts['team_comparisons'])}")
            self.logger.info(f"  Dashboards: {len(generated_charts['dashboards'])}")
            self.logger.info(f"  Availability charts: {len(generated_charts['availability_charts'])}")
            self.logger.info("=" * 80)

            return generated_charts

        except Exception as e:
            self.logger.error(f"Failed to generate visualizations: {e}", exc_info=True)
            raise
