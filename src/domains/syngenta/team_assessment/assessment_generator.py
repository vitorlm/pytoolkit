import os
from typing import Optional
from domains.syngenta.team_assessment.processors.criteria_processor import CriteriaProcessor
from domains.syngenta.team_assessment.services.member_analyzer import MemberAnalyzer
from domains.syngenta.team_assessment.services.team_analyzer import TeamAnalyzer
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager
from utils.string_utils import StringUtils

from .processors.members_task_processor import MembersTaskProcessor
from .processors.feedback_processor import FeedbackProcessor
from .processors.health_check_processor import HealthCheckProcessor

from .services.feedback_analyzer import FeedbackAnalyzer

# from .services.feedback_specialist import FeedbackSpecialist
from .core.member import Member
from .core.config import Config


class AssessmentGenerator:
    """
    Handles the processing of competency matrix, health checks, and planning data
    to generate detailed feedback and assessment reports for team members and teams.
    """

    _logger = LogManager.get_instance().get_logger("AssessmentGenerator")

    def __init__(
        self,
        competency_matrix_file: str,
        feedback_folder: str,
        planning_folder: str,
        health_check_folder: str,
        output_path: str,
        ignored_member_list: Optional[str] = None,
    ):
        self.competency_matrix_file = competency_matrix_file
        self.feedback_folder = feedback_folder
        self.planning_folder = planning_folder
        self.health_check_folder = health_check_folder
        self.output_path = output_path

        self.members = {}
        self.criteria_processor = CriteriaProcessor()
        self.task_processor = MembersTaskProcessor()
        self.feedback_processor = FeedbackProcessor()
        self.health_check_processor = HealthCheckProcessor()
        self.config = Config()
        # self.feedback_specialist = FeedbackSpecialist(
        #     host=self.config.ollama_host,
        #     model=self.config.ollama_model,
        #     **self.config.get_ollama_config(),
        # )
        self.feedback_analyzer = FeedbackAnalyzer()
        self.ignored_member_list = JSONManager.read_json(ignored_member_list, default=[])

    def run(self):
        """
        Executes the process to generate the assessment report.
        """
        self._logger.info("Starting assessment generation process.")
        self._validate_input_folders()
        competency_matrix = self._load_competency_matrix()
        self._process_tasks()
        self._process_health_checks()
        feedback = self._process_feedback()
        self._update_members_with_feedback(feedback)
        team_stats, members_stats = self.feedback_analyzer.analyze(competency_matrix, feedback)
        self._update_members_with_stats(members_stats)
        for member_name, member_data in members_stats.items():
            if self._is_member_ignored(member_name):
                continue
            member_analyzer = MemberAnalyzer(member_name, member_data, team_stats, self.output_path)
            member_analyzer.plot_all_charts()
            # self.feedback_specialist.generate_feedback(
            #     member_name, member_data, team_stats, competency_matrix
            # )

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

    def _load_competency_matrix(self):
        """
        Loads the default competency matrix from the specified file.
        """
        self._logger.info(f"Loading competency matrix from: {self.competency_matrix_file}")
        return self.criteria_processor.process_file(self.competency_matrix_file)

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
            if self._is_member_ignored(member_name):
                continue
            self._update_member_health_checks(member_name, health_check)

    def _process_feedback(self):
        """
        Processes feedback data and returns the competency matrix.
        """
        self._logger.info(f"Processing feedback from: {self.feedback_folder}")
        return self.feedback_processor.process_folder(self.feedback_folder)

    def _update_member_tasks(self, member_name, tasks):
        """
        Updates or initializes member tasks.
        """
        member_name = StringUtils.remove_accents(member_name)
        if self._is_member_ignored(member_name):
            return

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
        if self._is_member_ignored(member_name):
            return

        if member_name not in self.members:
            self.members[member_name] = Member(
                name=member_name, health_check=health_check, tasks=[], feedback={}
            )
        else:
            self.members[member_name].health_check = health_check

    def _update_members_with_feedback(self, competency_matrix):
        """
        Update members with feedback from the competency matrix.

        This method processes a competency matrix containing evaluations and updates
        the feedback for each member. It ensures that each member exists in the
        members dictionary and safely handles feedback updates.

        Args:
            competency_matrix (dict): A dictionary where the keys are evaluatee names
                                      (str) and the values are dictionaries containing
                                      evaluator names (str) and their feedback (any).

        Raises:
            ValueError: If the evaluatee name cannot be split into a first name and last name.
        """

        for evaluatee_name, evaluations in competency_matrix.items():
            if self._is_member_ignored(evaluatee_name):
                continue
            name, last_name = evaluatee_name.split(" ", 1)
            name = StringUtils.remove_accents(name)
            last_name = StringUtils.remove_accents(last_name)

            # Ensure the member exists in the dictionary
            if name not in self.members:
                self.members[name] = Member(
                    name=name,
                    last_name=last_name,
                    feedback={},
                    feedback_stats=None,
                    tasks=[],
                    health_check=None,
                )

            # Safely handle feedback updates
            if self.members[name].feedback is None:
                self.members[name].feedback = {}

            for evaluator_name, feedback in evaluations.items():
                evaluator_name = StringUtils.remove_accents(evaluator_name)
                self.members[name].feedback[evaluator_name] = feedback

    def _update_members_with_stats(self, members_stats):
        """
        Updates members with analyzed feedback stats.
        """
        for member_name, member_stats in members_stats.items():
            if self._is_member_ignored(member_name):
                continue
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

    def _is_member_ignored(self, member_name):
        """
        Checks if a member is in the ignored member list.

        Args:
            member_name (str): The name of the member to check.

        Returns:
            bool: True if the member is in the ignored member list, False otherwise.
        """
        name_parts = member_name.split(" ", 1)
        first_name = StringUtils.remove_accents(name_parts[0])
        last_name = StringUtils.remove_accents(name_parts[1]) if len(name_parts) > 1 else ""

        for ignored_member in self.ignored_member_list:
            ignored_parts = ignored_member.split(" ", 1)
            ignored_first_name = StringUtils.remove_accents(ignored_parts[0])
            ignored_last_name = (
                StringUtils.remove_accents(ignored_parts[1]) if len(ignored_parts) > 1 else ""
            )

            if first_name == ignored_first_name:
                if not last_name or last_name == ignored_last_name:
                    return True

        return False
