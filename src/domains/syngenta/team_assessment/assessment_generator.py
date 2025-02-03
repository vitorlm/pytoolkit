import os
from domains.syngenta.team_assessment.services.member_analyzer import MemberAnalyzer
from domains.syngenta.team_assessment.services.team_analyzer import TeamAnalyzer
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager
from utils.string_utils import StringUtils

from .processors.members_task_processor import MembersTaskProcessor
from .processors.competency_processor import CompetencyProcessor
from .processors.health_check_processor import HealthCheckProcessor
from .services.competency_analyzer import CompetencyAnalyzer
from .services.feedback_specialist import FeedbackSpecialist
from .core.member import Member


class AssessmentGenerator:
    """
    Handles the processing of competency matrix, health checks, and planning data
    to generate detailed feedback and assessment reports for team members and teams.
    """

    _logger = LogManager.get_instance().get_logger("AssessmentGenerator")

    def __init__(
        self, feedback_folder: str, planning_folder: str, health_check_folder: str, output_path: str
    ):
        self.feedback_folder = feedback_folder
        self.planning_folder = planning_folder
        self.health_check_folder = health_check_folder
        self.output_path = output_path

        self.members = {}
        self.task_processor = MembersTaskProcessor()
        self.competency_processor = CompetencyProcessor()
        self.health_check_processor = HealthCheckProcessor()
        self.feedback_specialist = FeedbackSpecialist()
        self.competency_analyzer = CompetencyAnalyzer(self.feedback_specialist)

    def run(self):
        """
        Executes the process to generate the assessment report.
        """
        self._logger.info("Starting assessment generation process.")
        self._validate_input_folders()
        self._process_tasks()
        self._process_health_checks()
        competency_matrix = self._process_feedback()
        self._update_members_with_feedback(competency_matrix)
        team_stats, members_stats = self.competency_analyzer.analyze(competency_matrix)
        self._update_members_with_stats(members_stats)
        for member_name, member_data in members_stats.items():
            member_analyzer = MemberAnalyzer(member_name, member_data, team_stats, self.output_path)
            member_analyzer.plot_all_charts()

        team_analyzer = TeamAnalyzer(team_stats, self.output_path)
        team_analyzer.plot_all_charts()

        self._generate_output(team_stats)

    def _validate_input_folders(self):
        """
        Validates the existence of the input folders.
        """
        self._logger.debug("Validating input folders.")
        for folder in [self.feedback_folder, self.planning_folder, self.health_check_folder]:
            FileManager.validate_folder(folder)

    def _process_tasks(self):
        """
        Processes the planning data to extract tasks for each member.
        """
        self._logger.info(f"Processing tasks from: {self.planning_folder}")
        members_tasks = self.task_processor.process_folder(self.planning_folder)
        for member_name, member_tasks in members_tasks.items():
            self._update_member_tasks(member_name, member_tasks)

    def _process_health_checks(self):
        """
        Processes health checks data for each member.
        """
        self._logger.info(f"Processing health checks from: {self.health_check_folder}")
        health_check_data = self.health_check_processor.process_folder(self.health_check_folder)
        for member_name, health_check in health_check_data.items():
            self._update_member_health_checks(member_name, health_check)

    def _process_feedback(self):
        """
        Processes feedback data and returns the competency matrix.
        """
        self._logger.info(f"Processing feedback from: {self.feedback_folder}")
        return self.competency_processor.process_folder(self.feedback_folder)

    def _update_member_tasks(self, member_name, tasks):
        """
        Updates or initializes member tasks.
        """
        member_name = StringUtils.remove_accents(member_name)
        if member_name not in self.members:
            self.members[member_name] = Member(
                name=member_name, tasks=list(tasks), health_check=None, feedback=None
            )
        else:
            self.members[member_name].tasks = list(tasks)

    def _update_member_health_checks(self, member_name, health_check):
        """
        Updates or initializes member health check data.
        """
        member_name = StringUtils.remove_accents(member_name)
        if member_name not in self.members:
            self.members[member_name] = Member(
                name=member_name, health_check=health_check, tasks=[], feedback={}
            )
        else:
            self.members[member_name].health_check = health_check

    def _update_members_with_feedback(self, competency_matrix):
        """
        Updates members with feedback data from the competency matrix.
        """
        for evaluatee_name, evaluations in competency_matrix.items():
            name, last_name = evaluatee_name.split(" ", 1)
            name = StringUtils.remove_accents(name)
            last_name = StringUtils.remove_accents(last_name)

            # Ensure the member exists in the dictionary
            if name not in self.members:
                self.members[name] = Member(
                    name=name,
                    last_name=last_name,
                    feedback={},  # Initialize feedback as an empty dictionary
                    feedback_stats=None,
                    tasks=[],
                    health_check=None,
                )

            # Safely handle feedback updates
            if self.members[name].feedback is None:
                self.members[name].feedback = {}  # Initialize feedback if it is None

            for evaluator_name, feedback in evaluations.items():
                evaluator_name = StringUtils.remove_accents(evaluator_name)
                self.members[name].feedback[evaluator_name] = feedback

    def _update_members_with_stats(self, members_stats):
        """
        Updates members with analyzed feedback stats.
        """
        for member_name, member_stats in members_stats.items():
            name, last_name = member_name.split(" ", 1)
            name = StringUtils.remove_accents(name)
            last_name = StringUtils.remove_accents(last_name)

            if name not in self.members:
                self.members[name] = Member(
                    name=name,
                    last_name=last_name,
                    feedback={},
                    feedback_stats=None,
                    tasks=[],
                    health_check=None,
                )

            # Update feedback_stats
            self.members[name].feedback_stats = member_stats

    def _generate_output(self, team_stats):
        """
        Generates the final output report.
        """
        self._logger.info(f"Generating output report at {self.output_path}")

        # Create a directory for member outputs
        members_output_path = os.path.join(self.output_path, "members")
        FileManager.create_folder(members_output_path)

        # Store each member's data in a separate file
        for member in self.members.values():
            member_data = {
                "name": member.name,
                "tasks": member.tasks,
                "health_check": member.health_check,
                "feedback": member.feedback,
                "feedback_stats": member.feedback_stats,
            }
            member_output_folder = os.path.join(members_output_path, member.name.split()[0])
            FileManager.create_folder(member_output_folder)
            member_file_path = os.path.join(member_output_folder, "stats.json")
            JSONManager.write_json(member_data, member_file_path)

        # Store team stats in a separate file
        team_file_path = os.path.join(self.output_path, "team_stats.json")
        JSONManager.write_json({"team": team_stats}, team_file_path)

        self._logger.info("Assessment report successfully generated.")
